# Sync Table Coverage Matrix

This document is the source-of-truth matrix for sync v0.1 table coverage. It
describes which public table wrappers emit `ChangeOp` records when a
`ThreadLocalChangeAccumulator` is attached to the writing `Connection`, and
which wrappers intentionally stay outside raw capture until their wire format
and round-trip semantics are defined.

The Sync v0.1 transport path replicates raw physical DBI operations. It does
not deserialize table values during transport, and remote apply replays the
captured physical `storage_key` / `value` bytes through
`SyncEngine::handle_push()`. Explicit logical adapters have their own ordered
delivery protocol and durable outbox; `SyncWorkerOptions::enable_logical_delivery`
can dispatch that outbox through a logical-capable peer after a raw round drains.

## Matrix

| Table wrapper | v0.1 status | Captured operations | Apply semantics | Main tests |
| --- | --- | --- | --- | --- |
| `KeyValueTable<K, V>` | Supported | `Put`, `Delete`, `ClearTable`; point writes, bulk writes, range erase, and clear paths are implemented. | Raw key/value bytes are replayed into a destination DBI opened with captured DBI flags. | `test_sync_capture`, `test_sync_replication`, `test_sync_engine` |
| `VersionedKeyValueTable<K, V>` | Narrow LWW v1 adapter | Only point `insert_or_assign` and `erase`, each with a non-empty application source version. | With `ConflictPolicy::LastWriterWins`, source-version bytes order competing operations; equal versions use origin `NodeId`; durable sidecar tombstones prevent stale puts from resurrecting deletes. | `test_sync_engine` |
| `KeyTable<K>` | Supported | `Put`, `Delete`, `ClearTable`; insert, erase, range erase, and clear paths are implemented. | Raw key bytes are replayed with empty values. | `test_sync_capture`, `test_sync_engine`, `test_sync_replication` |
| `ValueTable<V>` | Supported | `Put`, `Delete`, `ClearTable`; set, insert, update, erase, and clear paths are implemented. | The singleton physical key and serialized value bytes are replayed. | `test_sync_capture`, `test_sync_replication` |
| `SequenceTable<V>` | Supported | `Put`, `Delete`, `ClearTable`; append, `insert_or_assign`, erase, and clear paths are implemented. | Stable `uint64_t` sequence keys and value bytes are replayed. | `test_sync_capture`, `test_sync_replication` |
| `VectorStore` | Raw plus limited logical adapter | Raw capture flows through its internal `SequenceTable` and `KeyValueTable` members. The schema-v1 `VectorStoreLogicalAdapter` explicitly captures and applies add, erase, and clear across ids, embeddings, text, and metadata with explicit record ids. Erase retains the ids marker as allocation high-water; clear resets all four DBIs. | Raw apply replays physical member-table operations; logical apply validates the adapter marker, record state, and embedding dimension before mutation. `commit_to_outbox()` can publish the logical frame atomically and the worker can deliver it through a capable peer. Both paths require one authoritative or externally serialized writer per collection. The logical adapter is opt-in, not a generic multi-DBI primitive or distributed conflict resolver. | `test_sync_capture`, `test_sync_replication`, `test_vector_store_logical_adapter` |
| `AnyValueTable<K>` | Deferred | No `ChangeOp` in v0.1. | Not applied by sync as a typed heterogeneous table. | `test_sync_capture` negative coverage |
| `KeyMultiValueTable<K, V>` | Limited logical adapter | No raw `ChangeOp` in v0.1. | Schema v1 explicitly captures unordered insert, key erase, all-matching-value erase, and clear. Schema v2 additionally captures exact-one erase and typed `reconcile()`. Schema v3 adds bounded typed range erasure, expanded before mutation into canonical `EraseKey` changes; raw calls and general multi-writer destructive convergence remain deferred. | `test_key_value_logical_adapter`, `test_sync_capture` negative coverage |
| `KeyOrderedMultiValueTable<K, V>` | Limited ordered logical adapters | No raw `ChangeOp` in v0.1. Schema v1 captures append-only changes; schema v2 captures `AppendElement` and exact `EraseElement` by persistent id. | Both schemas require one authoritative ordered origin and fail closed for direct logical frames or unordered delivery. Schema v2 persists element identity and tombstones, and its typed capture atomically commits local mutations plus an ordered outbox envelope. Bounded `erase_at`, key/value erase, and clear resolve selectors to exact ids before mutation; `replace_with()` is also implemented for single-origin schema-v2 capture. The default resolver performs full reverse validation, while an opt-in transaction-bound proof uses a non-reusable session token, bounds its complete materialized ID set, and can reuse that validated set for trusted selector calls with a separate budget. Baseline import, multi-origin histories, and tombstone pruning/compaction remain separately deferred. | `test_key_value_logical_adapter`, `test_key_ordered_multi_value_destructive_state`, `test_key_ordered_multi_value_destructive_adapter` |
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

`VersionedKeyValueTable` is deliberately outside that general raw contract. It
owns one automatic write transaction per point operation, suppresses ordinary
capture for the table mutation, and emits one revisioned raw `ChangeOp`. It
requires initialized sync identity and an attached `ThreadLocalChangeAccumulator`.
All peers using `ConflictPolicy::LastWriterWins` accept only these revisioned
`Put`/`Delete` operations; `ClearTable`, ranges, bulk operations, identity
remapping, other table families, and full snapshots are excluded. Its durable
`_mdbxc_identity_index` sidecar consumes one additional named DBI and retains
delete tombstones.

Logical table sync is still a reserved extension. The scaffolding added for it
has three separate pieces that must not be treated as support by themselves:

- `_mdbxc_sync_schema` records an explicit application schema id, logical kind,
  schema version, and owned DBI names. Owned DBI names are treated as a
  canonical sorted unique set;
- `LogicalChange` describes an opaque adapter-owned payload;
- `LogicalTableRegistry` reserves a two-phase preflight/apply path and validates
  the full schema tuple plus reserved flags before invoking adapters.

`KeyValueTableLogicalAdapter`, `KeyTableLogicalAdapter`, and
`VectorStoreLogicalAdapter` are explicit logical apply helpers with opt-in
typed capture sessions. Their `commit_to_outbox()` paths atomically publish a
logical envelope; a worker with `enable_logical_delivery` sends that envelope
through the separate capability-gated logical peer protocol. Direct logical
frames remain caller-delivered and are not placed in raw pull/push messages.

Until a wrapper has a codec extension, a registered adapter, capture tests, and
round-trip tests, it must remain in the deferred rows below and must not emit
logical or raw `ChangeOp` records.

Focused capture and round-trip tests currently cover representative write,
delete, bulk, range-erase, wrapper-specific `ClearTable`, indirect
`VectorStore`, and deferred-table negative paths.

## VectorStore Sync Boundary

`VectorStore` participates through raw replication of its four internal DBIs:
ids, embeddings, text, and metadata. The opt-in schema-v1
`VectorStoreLogicalAdapter` provides a separate logical contract for add, erase,
and clear, with explicit ids and a four-DBI preflight. Both paths support a
leader/follower topology or an application that serializes writers externally.
Neither provides a distributed id allocator or cross-node conflict resolution;
the logical adapter is an application-owned schema rather than a generic
multi-DBI primitive.

For a collection with an existing logical schema marker, reopen through
`VectorStoreLogicalAdapter::open_store_for_schema()`. It opens all four DBIs
without `MDBX_CREATE`, so missing or incompatible storage fails closed; direct
`VectorStore` construction remains create-by-default for non-schema use.

Replicas must use the same collection name and compatible embedding
serialization. The vector metric is local query configuration rather than
logical schema state; applications that require identical search ranking must
configure every replica with the same metric. A remote apply invalidates an
already-open store's RAM index through the connection generation and it rebuilds
lazily before the next index-dependent operation. The logical adapter remains
opt-in and does not relax the writer-serialization requirement.

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
capture, direct bulk/range table calls, and general concurrent destructive
updates remain deferred. The typed session supports batch `append()` in all
supported schema versions, schema-v2 `reconcile()`, and schema-v3 bounded
`erase_range()` capture.
The adapter rejects truncated pair payloads, oversized declared lengths,
trailing bytes, payload-bearing clear, and unknown opcodes before mutation.
The detailed typed contract and its remaining deferred boundaries live in
`include/mdbx_containers/sync/DESIGN.md`. Typed capture is enabled for the
documented schema-v1/v2/v3 operations; raw capture, direct bulk/range table
calls, and general multi-writer destructive convergence remain deferred.

`KeyOrderedMultiValueTable<K, V>` has two ordered logical schemas. Schema v1 is
append-only. Schema v2 adds `AppendElement` and exact `EraseElement` by a
persistent `OrderedElementId`, with Live/Tombstone state, per-origin introduced
high-water integrity, and typed capture that atomically commits the mutation and
an ordered outbox envelope. Both require one authoritative ordered origin.
Bounded `erase_at`, key/value erase, and clear expand a complete
canonical-codec selector result into deterministic exact-id operations before
mutation. Candidate-expansion and inspected-record budgets apply to the entire
operation, including remaining mutation-time reads. Single-origin
`replace_with()` is implemented; baseline import, multi-origin histories,
tombstone pruning, and compaction remain deferred. The
transaction wrapper's native
commit-error cleanup path has deterministic test-only coverage after ordered
capture preparation: the injected path aborts the native handle before
returning an MDBX error, notifies the attached capture sink of the discard, and
requires the physical table, state, and outbox to all roll back. This is not a
claim of real storage-I/O fault injection.

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
