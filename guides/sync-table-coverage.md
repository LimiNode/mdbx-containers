# Sync Table Coverage Matrix

This document is the source-of-truth matrix for sync v0.1 table coverage. It
describes which public table wrappers emit `ChangeOp` records when a
`ThreadLocalChangeAccumulator` is attached to the writing `Connection`, and
which wrappers intentionally stay outside sync until their wire format and
round-trip semantics are defined.

Sync v0.1 replicates raw physical DBI operations. It does not deserialize table
values during transport, and remote apply replays the captured physical
`storage_key` / `value` bytes through `SyncEngine::handle_push()`.

## Matrix

| Table wrapper | v0.1 status | Captured operations | Apply semantics | Main tests |
| --- | --- | --- | --- | --- |
| `KeyValueTable<K, V>` | Supported | `Put`, `Delete`, `ClearTable`; point writes, bulk writes, range erase, and clear paths are implemented. | Raw key/value bytes are replayed into a destination DBI opened with captured DBI flags. | `test_sync_capture`, `test_sync_replication`, `test_sync_engine` |
| `KeyTable<K>` | Supported | `Put`, `Delete`, `ClearTable`; insert, erase, range erase, and clear paths are implemented. | Raw key bytes are replayed with empty values. | `test_sync_capture`, `test_sync_engine`, `test_sync_replication` |
| `ValueTable<V>` | Supported | `Put`, `Delete`, `ClearTable`; set, insert, update, erase, and clear paths are implemented. | The singleton physical key and serialized value bytes are replayed. | `test_sync_capture`, `test_sync_replication` |
| `SequenceTable<V>` | Supported | `Put`, `Delete`, `ClearTable`; append, `insert_or_assign`, erase, and clear paths are implemented. | Stable `uint64_t` sequence keys and value bytes are replayed. | `test_sync_capture`, `test_sync_replication` |
| `VectorStore` | Indirectly supported | Captured through its internal `SequenceTable` and `KeyValueTable` members. | The internal table operations are replicated; `VectorStore` has no separate wire type. This raw path requires one authoritative or externally serialized writer per collection. Already-open instances compare `Connection::sync_apply_generation()` and lazily rebuild their RAM index before index-dependent operations after remote apply. A connection apply/read barrier serializes remote `handle_push()` apply commits with cache-backed `VectorStore` operations. Each `VectorStore` instance serializes its own methods; C++17 builds let different readers share the connection read side, while C++11 builds use an exclusive connection mutex fallback. | `test_sync_capture`, `test_sync_replication` |
| `AnyValueTable<K>` | Deferred | No `ChangeOp` in v0.1. | Not applied by sync as a typed heterogeneous table. | `test_sync_capture` negative coverage |
| `KeyMultiValueTable<K, V>` | Limited logical adapter | No raw `ChangeOp` in v0.1. | `KeyMultiValueTableLogicalAdapter` explicitly captures unordered insert, key erase, all-matching-value erase, and clear for one writer or causally serialized updates. Raw calls, append, reconcile, range erase, and general multi-writer destructive convergence remain deferred. | `test_key_value_logical_adapter`, `test_sync_capture` negative coverage |
| `KeyOrderedMultiValueTable<K, V>` | Limited ordered logical adapter | No raw `ChangeOp` in v0.1. | Append-only logical changes require one ordered origin stream; direct logical frames and unordered delivery fail closed. Typed capture is pending; destructive operations remain deferred pending persistent element identity and tombstones. | `test_key_value_logical_adapter`; capture regressions pending |
| `HashedKeyValueStore<K, V, H, Layout>` | Deferred | No `ChangeOp` in v0.1. | Hash-index identity and logical-key mapping are deferred. | `test_sync_capture` negative coverage |

## Supported Capture Contract

For supported wrappers, application CRUD code does not need per-method sync
calls. Attach capture to the writing connection, preferably through
`SyncCaptureScope`, then use the normal table API:

- a standalone write transaction becomes one local sync batch;
- an explicit caller transaction spanning several supported tables becomes one
  atomic local sync batch;
- reads, scans, vector search, and other non-mutating APIs are not captured;
- remote apply commits one pulled page in one destination MDBX transaction.

The captured DBI name, DBI flags, physical key bytes, and value bytes must
remain sufficient to open or validate the destination DBI and replay the
operation without table-specific decoding.

Logical table sync is still a reserved extension. The scaffolding added for it
has three separate pieces that must not be treated as support by themselves:

- `_mdbxc_sync_schema` records an explicit application schema id, logical kind,
  schema version, and owned DBI names. Owned DBI names are treated as a
  canonical sorted unique set;
- `LogicalChange` describes an opaque adapter-owned payload;
- `LogicalTableRegistry` reserves a two-phase preflight/apply path and validates
  the full schema tuple plus reserved flags before invoking adapters.

`KeyValueTableLogicalAdapter` and `KeyTableLogicalAdapter` are the current
explicit logical apply helpers with opt-in typed capture sessions. They are not
connected to the transport pull/push path; callers own logical frame delivery.

Until a wrapper has a codec extension, a registered adapter, capture tests, and
round-trip tests, it must remain in the deferred rows below and must not emit
logical or raw `ChangeOp` records.

Focused capture and round-trip tests currently cover representative write,
delete, bulk, range-erase, wrapper-specific `ClearTable`, indirect
`VectorStore`, and deferred-table negative paths.

## VectorStore Raw Replication Boundary

`VectorStore` currently participates only through raw replication of its four
internal DBIs: ids, embeddings, text, and metadata. This supports a
leader/follower topology or an application that serializes writers externally.
It does not provide a distributed id allocator, cross-node conflict resolution,
or a logical `VectorStore` wire type.

Replicas must use the same collection name, vector metric, and compatible
embedding serialization. A remote apply invalidates an already-open store's
RAM index through the connection generation and it rebuilds lazily before the
next index-dependent operation. A future multi-DBI logical adapter needs a
global record identity scheme and explicit ordering/conflict policy before this
restriction can be relaxed.

## Deferred Table Rules

Do not add `record_op()` calls to deferred wrappers until the same PR also
defines:

- a persistent logical schema id and `SchemaRegistryStore` record, when the
  wrapper needs table-specific logical apply;
- the wire representation, including any table-specific metadata;
- the `ILogicalTableAdapter` implementation and preflight/apply rules;
- apply-side reconstruction or validation rules;
- a transaction owner that aborts the whole apply transaction if an adapter
  reports failure or throws after any mutation;
- capture tests for every mutating method that can change logical state;
- round-trip replication tests that compare source and destination logical
  state;
- negative tests for unsupported conflict or ordering scenarios.

A partial capture path is worse than no capture: it can make replication appear
successful while source and destination table semantics diverge.

## Deferred Designs

`KeyMultiValueTable<K, V>` has a limited explicit logical adapter for the
unordered multiset model. Repeated identical `(key, value)` pairs preserve
multiplicity under single-writer or causally serialized updates. Raw v0.1
capture, bulk/reconcile/range capture, and general concurrent destructive
updates remain deferred.
Before adding another `KeyMultiValueTableLogicalAdapter` opcode or capture
method, expand its malformed-payload regression matrix to cover truncated
pair payloads, oversized declared lengths, trailing bytes, payload-bearing
clear, and unknown opcodes. This is deferred coverage work for the next
logical-adapter extension, not a claim that those operations are supported.
The detailed deferred contract lives in
`include/mdbx_containers/sync/DESIGN.md`: it requires explicit multivalue wire
sub-operations and receiver-side logical apply helpers before capture can be
enabled. Order-sensitive histories belong to `KeyOrderedMultiValueTable<K, V>`;
that table exists for local storage, but its sync capture/apply contract is
still deferred.

`AnyValueTable<K>` needs value type-tag propagation or another explicit
compatibility policy. The current sync wire operation only carries raw value
bytes and cannot express which logical type was written.

`HashedKeyValueStore<K, V, H, Layout>` needs a logical-key identity model that
accounts for both its public key bytes and its internal hash-index storage.
Replicating only one side of the store would corrupt lookup semantics.

## Validation Baseline

When changing sync-facing table capture, run focused C++11 and C++17 tests:

```text
test_sync_capture
test_sync_replication
test_sync_engine
test_sync_randomized
```

For transport or worker changes that move captured batches between nodes, add:

```text
test_http_transport
test_websocket_transport
test_sync_worker
```

For broad table serialization changes, also include the table wrapper tests
that exercise the affected key/value type.
