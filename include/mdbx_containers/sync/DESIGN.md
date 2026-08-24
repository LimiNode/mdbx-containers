# Sync Subsystem Design (target: v0.1)

> Reading order for any agent considering changes to the sync subsystem.
> This document locks in decisions that have wire- or disk-format consequences
> so future agents do not silently change them. It also distinguishes
> **already implemented** from **planned for v0.1** to keep the contract
> honest as the codebase evolves.

## Scope

Multi-master replication of logical MDBX tables between node-local envs.
Wire is transport-agnostic, codec is versioned, storage uses named DBIs.

## Already implemented (merged into main)

- Public sync modules are grouped behind `sync/core.hpp`, `sync/protocol.hpp`,
  `sync/storage.hpp`, `sync/capture.hpp`, `sync/logical.hpp`,
  `sync/adapters.hpp`, `sync/engine.hpp`, and `sync/transport.hpp`.
  Their leaves contain `ChangeAccumulator`, `ChangeBatch`, `ChangeOp`,
  `CancellationToken`, `CodecBounds`, `CodecFlags`, `ConflictPolicy`,
  `DirectSyncPeer`, `IdentityProvider`, `ISyncCaptureSink`, `ISyncPeer`,
  `SyncCaptureScope`, `SyncCursor`, `SyncEngine`, and `SyncWorker`.
- Five raw system stores under `include/mdbx_containers/sync/stores/`:
  `MetaStore`, `ChangeLogStore`, `OriginIndexStore`, `AppliedStore`, and
  `IdentityIndexStore`. Durable logical state is separate under
  `include/mdbx_containers/sync/logical/stores/`: `SchemaRegistryStore`,
  `LogicalDeliveryStore`, `LogicalDeliveryOrderStore`, `LogicalJournalStore`,
  and `LogicalOutboxStore`.
- `ChangeBatchCodec` strict versioned wire format (magic, codec version,
  batch version, batch flags, then payload); rejects unknown mandatory
  flags and version mismatches at both encode and decode.
- `TransportMessageCodec` strict versioned envelope for transport DTOs:
  `PullRequest`, `PullResponse`, `PushRequest`, and `PushResponse`.
  It length-prefixes nested `ChangeBatchCodec` payloads and does not
  serialize operation-local `CancellationToken` state.
- Framework-neutral HTTP-shaped adapter seam: `HttpSyncPeer`,
  `IHttpSyncClient`, `HttpSyncServer`, and `HttpSyncRoutes`. It defines
  route/content-type/body/status mapping over `TransportMessageCodec` but does
  not open sockets or depend on an HTTP framework.
- Framework-neutral WebSocket-shaped adapter seam: `WebSocketSyncPeer`,
  `IWebSocketSyncChannel`, and `WebSocketSyncServer`. It defines a complete
  binary-message request/response contract over `TransportMessageCodec` but
  does not open sockets, own sessions, or depend on a WebSocket framework.
- `sync/transport.hpp` umbrella for framework-neutral transport seams and
  middleware.
- Optional ready-made Simple-Web HTTP/WebSocket bindings under
  `sync/transports/simple_web/` plus socket-backed examples over Simple-Web-Server /
  Simple-WebSocket-Server, standalone Asio, and a process-supervised HTTP
  node-fleet example over tiny-process-library. These integrations are behind
  explicit CMake options, not mandatory runtime dependencies. The backend
  umbrella is `sync/transports/simple_web.hpp`; HTTP-only or WebSocket-only
  targets can include the narrower backend-specific header. Targets that link
  these dependency backends receive `MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT` and/or
  `MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT`.
- Optional ready-made Kurlyk/libcurl HTTP client binding under
  `sync/transports/kurlyk/`. It implements only the client-side
  `IHttpSyncClient` seam and can be paired with any server binding that exposes
  the framework-neutral `HttpSyncServer` contract. Targets that link the
  backend receive `MDBXC_HAS_KURLYK_HTTP_TRANSPORT`.
- Transport middleware helpers: `SyncPeerMiddleware`,
  `HttpSyncClientMiddleware`, allow-list policies, fixed-budget rate limiting,
  HTTP request-context bearer/remote-address/fixed-window policies,
  WebSocket session identity policy, bearer token to `NodeId` binding,
  HTTP retry status classification,
  `Retry-After`/`WWW-Authenticate` rejection headers, and a metrics observer.
  These wrap transport adapters and do not change the sync DTO wire format.
- Change capture hooks: `Connection::attach_sync_capture()`,
  `BaseTable::record_op()`, and the transaction pre-commit hook route table
  writes into `ThreadLocalChangeAccumulator`, which appends one local
  `ChangeBatch` per committing write transaction.
- Selective-replication foundation: `SyncEngine::register_selective_replication_scope()`
  durably registers an immutable raw-DBI manifest and its designated writer.
  `ThreadLocalChangeAccumulator` keeps the global `ChangeBatch` complete and,
  for one scoped transaction, atomically writes its scope-local projection and
  sequence. A non-designated local write, raw external writable transaction,
  or public capture suppression fails closed before commit. Scoped wire
  delivery, snapshots/resume, and retention are not implemented yet.
- `SyncEngine` pull / push / apply protocol logic, `DirectSyncPeer`
  in-process transport, detailed apply conflict diagnostics, multi-origin
  pagination, origin-index fallback for legacy changelogs, and
  `make_push_request()` for example / transport code.
- Replicated table operation capture for `KeyValueTable`, `KeyTable`,
  `ValueTable`, and `SequenceTable` normal write paths, including bulk
  upserts, reconcile deletes, singleton value writes, and range erases.
  `SequenceTable` `append()` remains a local single-writer operation with
  existing external synchronisation requirements. `VectorStore` is covered
  indirectly because its persistent write path uses `SequenceTable` and
  `KeyValueTable`. It also has an explicit schema-v1 logical adapter for
  add, erase, and clear operations over its four owned DBIs.
- End-to-end replication tests cover `ValueTable`, `KeyValueTable`,
  `KeyTable`, `SequenceTable` insert/update/delete including empty serialized
  values, and `VectorStore` add/erase/rebuild through its public API. A
  separate logical-adapter regression covers VectorStore add/erase/clear
  round-trip and fail-closed duplicate, schema, malformed-payload, and
  capture-rollback cases.
- `ConflictPolicy::Reject` is the default. Narrow `ConflictPolicy::LastWriterWins`
  v1 applies to DBIs durably registered by `VersionedKeyValueTable`. Registered
  DBIs accept only revisioned raw `Put`/`Delete` operations; ordinary DBIs keep
  raw sync on the same engine. It orders non-empty application source-version
  bytes lexicographically, then origin `NodeId`, and persists winner/tombstone
  markers in `_mdbxc_identity_index` with the user mutation.
- `SyncWorker` background pull/apply lifecycle, cooperative cancellation
  tokens, best-effort peer cancellation hook, and focused worker tests.
- Manual hub-style benchmark (`sync_tick_hub_benchmark`) plus opt-in and
  scheduled stress coverage for multi-origin sync paths.

## v0.1 table support matrix

| Type | Sync v0.1 status | Notes |
|------|------------------|-------|
| `KeyValueTable` | Supported | Captures normal put/delete paths, bulk append, reconcile puts/deletes, range erase, and clear-table ops. |
| `KeyTable` | Supported | Captures insert/delete, range erase, reconcile/clear paths that operate on physical keys. |
| `ValueTable` | Supported | Captures singleton put/delete/clear using its fixed physical key. |
| `SequenceTable` | Supported | Captures set/append/delete/clear against stable `uint64_t` record ids. `append()` remains a local single-writer helper; external synchronization is still required for concurrent appenders. |
| `VectorStore` | Raw plus limited logical adapter | Raw replication covers its `SequenceTable` and `KeyValueTable` member writes. The explicit schema-v1 logical adapter captures and applies add, erase, and clear over the ids, embeddings, text, and metadata DBIs with explicit record ids. Erase retains the ids marker as the persistent allocation high-water; clear resets all four DBIs. Both paths require one authoritative or application-serialized writer per collection; the logical adapter is not a multi-writer conflict resolver or automatic transport path. Already-open instances refresh their RAM index lazily after completed remote apply when the connection sync-apply generation changes. |
| `AnyValueTable` | Not supported in v0.1 | Deferred until heterogeneous value type tags are part of the sync wire format. |
| `KeyMultiValueTable` | Schema-v3 logical adapter plus automatic capture | Raw v0.1 capture remains unsupported. `SyncEngine::bind_key_multi_value_logical_capture()` durably binds one DBI to a receiver-neutral logical dataset and makes ordinary supported writes publish schema-v3 frames atomically into `LogicalJournalStore`. `erase_range()` remains fail-closed because its normal API is unbounded. All destructive modes require one-writer or causally serialized updates. |
| `KeyOrderedMultiValueTable` | Schema-v1 automatic append capture plus ordered adapters | `SyncEngine::bind_key_ordered_multi_value_logical_capture()` durably binds schema v1 to a receiver-neutral logical dataset and atomically journals ordinary `append`/`insert` calls. The bound v1 API rejects destructive table mutations. Schema v2 provides explicit `AppendElement` and `EraseElement` by immutable id through ordered delivery for one authoritative origin. Bounded key/index/value/clear capture expands selectors to exact ids, and single-origin `replace_with()` expands replacement state into exact changes. |
| `HashedKeyValueStore` | Not supported in v0.1 | Deferred until hash-index and identity-key mapping semantics are specified. |

Do not add `record_op()` paths for unsupported table types without first
updating this design document and adding round-trip replication tests for the
new wire-format semantics.

`VectorStore` collection names are validated instead of sanitized. Names must
be non-empty and contain only ASCII letters, digits, `_`, and `-`; unsupported
characters are rejected before internal DBI names are built. This prevents
different logical collections from collapsing to the same physical DBI names.

`VectorStore` has two explicit sync paths. Raw physical replication covers its
ids, embeddings, text, and metadata DBIs; replicas must agree on collection
name and embedding serialization. The vector metric is local query
configuration and is not stored in the logical schema marker; applications
that require identical search ranking configure the same metric on every
replica. `VectorStore::add()` assigns
ids from local table state, so concurrent independent writers can collide and
have no conflict-resolution rule. The schema-v1
`VectorStoreLogicalAdapter` instead captures explicit record ids and canonical
embedding/text/metadata payloads for add, erase, and clear across all four DBIs.
When reopening a collection with an existing logical schema marker, use
`VectorStoreLogicalAdapter::open_store_for_schema()` so all four DBIs open
without `MDBX_CREATE`; a missing or incompatible DBI then fails closed. Direct
`VectorStore` construction remains the create-by-default non-schema API.
Logical erase removes the three payload rows but retains the ids marker, so an
erased id cannot be reused by a later local append. Clear is the explicit
allocator reset and removes every marker and payload row. Incoming adds validate
their embedding dimension against the persisted collection before mutation.
It is an opt-in application-owned logical recipe, not a generic multi-DBI
primitive, distributed id allocator, conflict resolver, or automatic
pull/push capability. Both paths therefore require a leader/follower or
application-serialized writer topology per collection.

`Connection::sync_apply_generation()` is a coarse invalidation marker for sync
apply commits. `SyncEngine::handle_push()` increments it after a successful
commit that applied at least one incoming raw operation, and
`SyncEngine::apply_logical_changes()` increments it after committed logical
changes. `VectorStore` stores the last seen generation and rebuilds its
in-memory index before index-dependent operations when the value changes.
Applications and future cached wrappers may also register
`ISyncApplyObserver` instances on the connection. Observers are called after
the apply transaction commits and after the generation increments. The
connection apply/write barrier is released before callbacks run, so observers
may use table and cache-backed APIs on the same connection for invalidation.
Observer exceptions are swallowed because the database state has already
changed. Registrations are non-owning lifecycle hooks. A successful
`remove_sync_apply_observer()` call is a callback drain barrier for that token,
so the observer can be destroyed after removal returns. Apply events also carry
the unique DBI names touched by applied operations in first-seen order. The
names are local physical DBI names for invalidation; they are not transport
identity, auth metadata, or a substitute for table-specific sync semantics.
`add_sync_apply_observer_for_dbis()` optionally schedules a callback only when
one of its registered DBI names was affected. It filters local post-commit
callback delivery only; capture, transport delivery, and remote apply remain
unchanged.

A connection-level apply/read barrier serializes sync apply commits with
cache-backed `VectorStore` operations. C++17 builds use `std::shared_mutex` so
different cache-backed readers can share the connection read side. Each
`VectorStore` instance still serializes its own public operations with an
instance mutex to preserve the existing table wrapper thread-safety contract.
C++11 builds fall back to an exclusive connection mutex model.

## `KeyMultiValueTable` logical sync contract

This section documents the logical replication contract for
`KeyMultiValueTable`. Raw v0.1 capture remains unsupported:
`KeyMultiValueTable` emits no raw `ChangeOp` records. Its explicit typed
capture session and schema-v3 automatic binding emit only the operations
defined by this contract.

`KeyMultiValueTable` cannot safely reuse the v0.1 raw DBI put/delete model as
an undocumented implementation detail. The table stores one MDBX DUPSORT record
per logical pair, but the duplicate value is not just the serialized public
value. It is:

```text
stored duplicate value = sequence-prefix || serialized-value
```

The sequence prefix preserves repeated identical `(key, value)` pairs as
separate physical records and also affects iteration order for values under the
same key. Public reads strip the prefix. Public `erase(key, value)` removes all
duplicate records whose stripped payload equals `serialized-value`. Therefore a
future `KeyMultiValueTable` wire format must choose and test an explicit
multiset model instead of assuming that a physical key, a stripped value, or a
locally assigned sequence prefix uniquely identifies one cross-node record.

The first sync design for `KeyMultiValueTable` targets unordered multiset
preservation under single-writer or causally serialized updates. Schema v1
contains `InsertOne`, `EraseKey`, `EraseAllValues`, and `Clear`; schema v2 adds
`EraseOneValue` and typed `reconcile()`; schema v3 adds bounded typed
range-erasure capture:

```text
for every serialized key and serialized public value:
    count(key, value) is preserved after replaying the same ordered operation history
```

General concurrent multi-writer convergence is not guaranteed by the operation
set below. Destructive operations are not commutative with concurrent inserts:
`EraseAllValues`, `EraseKey`, bounded range erasure, and `Clear` can produce different
final counts when different replicas observe local writes and remote deletes in
different orders. Supporting that case requires an explicit conflict model,
such as a single authoritative writer per key/range, a deterministic global
operation order with history replay, CRDT tagged occurrences with tombstones, or
an LWW/version policy for destructive operations. That design is deferred.

Order-sensitive APIs such as `find(key)`, `retrieve_all_vector()`, and
`range_vector()` may expose different value order on different nodes when
multiple nodes write values for the same key. The order and multiplicity
converge only when the application uses one authoritative writer for the
relevant key/logical dataset or otherwise serializes conflicting updates before
they are replicated. Ordered distributed history belongs to the separate
`KeyOrderedMultiValueTable` API and still needs its own sync contract.

The complete logical operation model is:

| Operation | Required payload | Apply semantics |
|-----------|------------------|-----------------|
| `InsertOne` | serialized key, serialized public value | Add one logical pair. The replica assigns its own local duplicate sequence prefix. |
| `EraseKey` | serialized key | Remove all values for the key. |
| `EraseOneValue` | serialized key, serialized public value | Remove one matching repeated value for the key, if one exists. This is needed for `reconcile()` surplus deletes. |
| `EraseAllValues` | serialized key, serialized public value | Remove all exact matching repeated values for the key, matching current public `erase(key, value)` semantics. |
| `Clear` | no key/value payload | Remove all pairs in the table. |

These operations use the existing `LogicalChange` envelope with
`LogicalTableKind::KeyMultiValue`, an adapter-local opcode, and an
adapter-owned payload. They must not be encoded as plain v0.1
`ChangeOpType::Put` / `Delete` records against the DUPSORT DBI: that would leak
the local sequence-prefixed duplicate value and make remote replay depend on
another node's private prefix allocator. Receivers without a registered
matching adapter fail closed before mutation through logical schema-marker and
adapter validation.

The first implementation scope is intentionally smaller than the complete
model. Schema v1 typed capture supports `InsertOne`, batch `append()` expanded
in input order into `InsertOne`, `EraseKey`, `EraseAllValues`, and `Clear`.
Schema v2 additionally supports `EraseOneValue` and `reconcile()`.
Reconciliation matches canonical logical pairs by multiplicity, emits one
`EraseOneValue` per surplus occurrence, then emits missing `InsertOne` changes
in desired-vector order. Schema v3 additionally supports bounded typed
`erase_range()`, expanded into `EraseKey` changes before local mutation. These
typed session methods are version-neutral where stated. Raw capture remains
local-only; direct table calls publish logical changes only after the explicit
schema-v3 automatic binding described below. This is not partial raw capture:
callers opt into the typed session or durable automatic binding, and only their
documented methods publish logical changes.

Schema v3 typed range erasure is an inclusive logical-key interval, never a raw
MDBX cursor key. Capture scans the complete local range before mutation under a
mandatory `max_pairs` bound. It builds canonical `EraseKey` changes for the
distinct selected public keys, reserves pending-frame storage, and only then
removes the keys locally. The wire frame therefore uses the already validated
`EraseKey` opcode rather than a broad remote cursor-delete operation. A receiver
replays those exact key erasures through its public table API. The operation is
still limited to one writer or causally serialized destructive updates; it
provides no multi-writer convergence. `append()` needs no schema-v3 opcode:
typed capture expands it into `InsertOne` changes in input order.

Schema-v3 also provides receiver-neutral automatic capture for an explicitly
bound `KeyMultiValueTable` DBI. Bind it through
`SyncEngine::bind_key_multi_value_logical_capture(adapter, destination, record)`.
The binding is durable in `_mdbxc_logical_dbi_bindings`; it records a logical
dataset `destination`, never a receiver route. Supported normal table writes
accumulate typed changes in their `Connection` transaction, and pre-commit
appends one frame to `LogicalJournalStore` atomically with the physical table
mutation. Routing later fans that journal out with
`materialize_logical_journal(destination, receiver)`.

The binding fails closed. A process that opens a bound DBI without installing
the matching runtime capture cannot mutate it through the wrapper, and a
caller-created writable `MDBX_txn*` is rejected before physical mutation.
The public raw `SyncCaptureSuppressionScope` cannot bypass this contract: a
bound-table write while it is active fails and rolls back. Only the private
`SyncEngine` logical-apply scope suppresses automatic capture for an incoming
frame. Direct public adapter apply on a bound DBI fails closed. Legacy typed
capture sessions are rejected for a bound DBI, so there is
no second caller-selected outbox route. Runtime capture registrations are
cleared after a successful `Connection::disconnect()` and rechecked against the
current environment's durable binding before each use. The normal `erase_range()`
API remains unavailable for a bound DBI
because it has no caller-supplied bound; `insert`, `append`, `erase(key)`,
`erase(key,value)`, `erase_one`, `clear`, and `reconcile` map to the existing
schema-v3 operations.

Implementation phases:

1. Implement adapter payload encoding and preflight through the existing
   logical schema marker and adapter registry. A receiver without the adapter
   must reject the change before table mutation.
2. Implement apply helpers that call the public/logical multivalue semantics:
   assign a fresh local duplicate prefix on `InsertOne`; match stripped public
   values for `EraseOneValue` and `EraseAllValues`; use public key ordering for
   range erasure.
3. Add the scoped typed capture session only for methods that map directly to
   the implementation operations. Raw capture stays disabled.
4. Add negative compatibility tests: receivers without the matching adapter or
   persistent marker must fail before table mutation.
5. Add round-trip tests before documenting the wrapper as supported. Tests must
   compare logical multiset counts, not raw duplicate bytes or local iteration
   order.

Typed `append()` is available for schema v1, v2, and v3 because it is
represented as a sequence of `InsertOne` operations in the same local batch.
`erase(key, value)` emits `EraseAllValues`. Typed
`reconcile()` emits one `EraseOneValue` per surplus occurrence and one
`InsertOne` per missing occurrence so that repeated identical pairs preserve
their final multiplicity. If a future implementation captures lower-level
physical deletes during `reconcile()` or range erase, it must copy cursor keys
and duplicate values before `mdbx_cursor_del()` and must still publish logical
operations whose replay is deterministic on another node.

The wire payload should carry the stripped public value, not the local
sequence-prefixed duplicate bytes. This keeps replicas free to assign local
duplicate sequence prefixes while preserving observable multiset state for the
supported single-writer or causally serialized case:

```text
count(key), count(key, value), and per-pair multiplicity
```

Neither order-sensitive iteration nor concurrent destructive-update convergence
is guaranteed for general multi-writer `KeyMultiValueTable` replication. The
sequence prefix remains a local storage detail and is not a cross-node identity.

### `KeyOrderedMultiValueTable` ordered-delivery contract

`KeyOrderedMultiValueTable<K, V>` exists as a local table API for replicated
per-key histories, event timelines, queues, and other local models where
per-key append order is part of the contract. Its first logical adapter is
append-only and accepts changes only through
`SyncEngine::apply_ordered_logical_delivery_envelope()`. Direct logical frames
and unordered delivery must fail before adapter callbacks or table mutation.

Schema v1 may also use receiver-neutral automatic capture at its authoritative
origin. `SyncEngine::bind_key_ordered_multi_value_logical_capture(adapter,
destination, record)` persists the dataset binding in
`_mdbxc_logical_dbi_bindings` and requires `record` to name the local node as
the ordered origin. Ordinary `append` and `insert` calls, including a vector
batch append, accumulate `AppendOne` changes in the current transaction and
append one durable frame to `LogicalJournalStore` at pre-commit. The binding
does not contain a receiver node; delivery later calls
`materialize_logical_journal(destination, receiver)`. `erase`, `erase_at`,
`clear`, and `replace_with` are rejected for a bound schema-v1 DBI, because
their semantics require the existing schema-v2 element-id/tombstone protocol.
There is currently no automatic schema-v2 binding or in-place migration from a
bound v1 DBI: assigning ids to existing v1 occurrences, creating the v2 state
and key-index DBIs, migrating the durable logical DBI binding, and defining the
journal/delivery-frontier transition require one explicit future migration
procedure. A v1→v2 change is therefore not a schema-version edit. Missing
runtime capture, legacy typed sessions, public capture suppression, and
caller-created writable raw transactions fail closed, as for other automatic
logical bindings.
The local storage format is:

```text
MDBX key                 = serialized-key
ordered duplicate value  = local-order-prefix || serialized-value
local-order-prefix       = per-key uint64_t assigned by the destination DBI
```

The local-order prefix is a storage detail, not a cross-node identity. The
append-only adapter obtains its stable history order from one ordered delivery
stream: envelope origin sequence followed by logical-change position within the
frame. The persistent logical schema marker binds an ordered adapter to one
non-zero authoritative origin. A caller must register that binding before
capture or delivery; a missing or mismatched origin fails before marker
insertion, adapter callbacks, or table mutation. Existing ordered markers
without the binding require an explicit schema-marker migration. The receiver
replays changes in that stream order and assigns local prefixes while preserving
the observable per-key append order.

#### Controlled authoritative-origin cutover

Changing the authoritative origin is an application-coordinated operational
cutover, not an automatic failover mechanism. The schema-marker migration
changes only the admission rule for future ordered deliveries; it does not
move table data, sender outbox entries, receiver frontiers, or replay markers.
Applications must use this sequence:

1. Quiesce new capture at the old origin, drain its ordered outbox through
   controlled dispatch to every participating replica, and then stop old-origin
   dispatch before marker migration. The application chooses the delivery
   acknowledgement boundary because the engine has no global peer registry or
   distributed cutover coordinator.
2. Keep replay markers for the old origin through the application retry
   horizon. After marker migration, a committed exact old-origin delivery can
   still acknowledge as a no-op, but a new old-origin sequence is rejected.
   Pruning the exact marker ends that retry guarantee.
3. On every participating database environment, including the old origin, new
   origin, and all replicas, call `migrate_logical_schema()` with the exact old
   record and a replacement record whose
   `ordered_delivery_origin_node_id` names the new origin. The replacement must
   keep the same schema kind, version, and owned DBI set unless this is a
   separately designed schema migration. The old origin must receive the same
   replacement marker so a restarted or stale writer fails capture locally.
4. Before enabling new-origin capture, verify that every participating marker
   names the new origin, the old origin has no active capture session or pending
   ordered outbox entry, and old-origin replay-marker retention covers the
   chosen retry horizon. Keep both origins quiesced while marker values are
   mixed; this migration is not a distributed transaction.
5. Only then may the new origin start typed capture and enqueue ordered
   deliveries. Its stream is independent from the retired origin stream and
   therefore starts from its own persisted outbox sequence.

A simple reverse marker migration is safe only before a new-origin
`LogicalCaptureSession::commit()` or `commit_to_outbox()` has succeeded. An
uncommitted session may be rolled back or destroyed normally. After either
commit succeeds, the new origin already has durable local ordered data, and
`commit_to_outbox()` also has a durable sender entry, even if no delivery has
been sent. Marker rollback alone is then insufficient. Recovery must stop new
capture and dispatch, preserve or isolate its pending outbox entries, and use a
separate application-specific plan to reconcile or compensate the local ordered
data before the old origin is enabled again. The protocol intentionally does
not accept concurrent old and new origins as a shortcut for availability.

Multiple independent origins writing the same ordered dataset are not supported
by this contract. A future multi-writer format must carry an explicit globally
stable element order identity, for example:

```text
ordered duplicate value = global-order-key || serialized-value
global-order-key        = origin-node-id || origin-local-sequence || op-index
```

The exact fields are deferred, but the important property is that the order key
is carried on the wire and replayed unchanged by replicas. With the illustrative
fields above, this is only a deterministic presentation order: lexicographic
comparison groups operations by `origin-node-id` before the origin-local
sequence. It is not wall-clock insertion time and does not express causal order
between nodes. A causally meaningful distributed history would need an explicit
Lamport/HLC-style component or another causal-ordering model.

The initial adapter supports only `append(key, value)`. `insert` is an alias for
that operation and uses the same typed capture method. Its capture session owns
one writable transaction and can atomically commit local appends plus an ordered
outbox envelope through `commit_to_outbox()`.

#### Destructive ordered-history contract

The implemented initial destructive extension is versioned separately. The existing
append-only `KeyOrderedMultiValueTableLogicalAdapter` is logically schema
version 1 only and must reject every configured version other than 1. The
destructive `KeyOrderedMultiValueTableDestructiveLogicalAdapter` is logically
schema version 2 only with persistent layout version 1 and must likewise reject
every other version. The generic registry continues to compare the exact
registered schema reference with a change and persistent marker; it does not
infer format semantics from table kind alone. These fixed adapter versions make
a version-1 adapter or marker reject version-2 traffic before callbacks, and
conversely. This does not enable raw `ChangeOp` capture or make multiple
independent writers converge.

The existing local duplicate prefix cannot identify an element across nodes:

```text
stored duplicate = local-order-prefix || serialized-value
```

The prefix is allocated independently by every replica, and identical public
values may occur more than once under a key. A delete that carries only a key,
a value, or a local index can therefore remove a different occurrence on another
replica. The destructive extension must use an immutable logical element id:

```text
OrderedElementId = origin-node-id || origin-element-sequence
```

`origin-element-sequence` is a non-zero monotonically increasing `uint64_t`
allocated by the authoritative origin inside the same writable transaction as
the local append. It is not the outbox delivery sequence: one frame can contain
many changes, and an element id must exist before `commit_to_outbox()` allocates
its envelope sequence. A new origin after a completed cutover uses its own node
id and its own persistent element sequence; old element ids remain unchanged.

##### Durable destructive capture

The destructive schema has a stricter local capture rule than the current
append-only helper: it may commit only through `commit_to_outbox()`. Its typed
session must not expose `commit(out)` or any equivalent path that commits table
state and returns an unpublished frame to the caller. The local physical append,
element-state and key-index updates, counter advance, and one ordered outbox
envelope must therefore commit or abort in the same caller-owned writable MDBX
transaction. This prevents a process crash between introducing an immutable id
locally and durably publishing its `AppendElement` frame.

When all operations of one destructive session coalesce locally, the session
still commits one ordered outbox envelope containing an empty
`LogicalChangeFrame`. The already allocated element counter advance and the
empty envelope commit atomically; the envelope consumes one ordered delivery
sequence and is replayed as a no-op without adapter callbacks. This keeps every
destructive capture commit on the same outbox-backed lifecycle rather than
introducing a special local-only commit path.

The first destructive implementation uses one authoritative origin and ordered
delivery, as the append-only adapter does. It does not provide a later baseline
recovery path for a locally committed but unpublished destructive frame. The
existing append-only `LogicalCaptureSession::commit(out)` remains available only
for its existing append-only schema and must not be reused by the destructive
schema version.

##### Origin admission

The destructive marker has one non-zero `ordered_delivery_origin_node_id`.
Before allocating an element id, typed capture verifies that the local sync
`node_id` equals that marker origin. For a received `AppendElement`, the sync
core first validates `delivery-envelope.origin_node_id` against the marker
before adapter preflight. Destructive-adapter preflight then validates
`element-id.origin` against that same marker before mutation. Together these
checks require all three origins to be identical:

```text
element-id.origin == delivery-envelope.origin_node_id
                  == marker.ordered_delivery_origin_node_id
```

This rule prevents an origin from introducing an id in another node's sequence
namespace. `EraseElement` deliberately has no such id-origin equality rule: the
current authoritative origin may erase an element introduced by a historical
origin after a completed cutover. An authoritative baseline migration is the
only trusted non-delivery path that may install valid historical ids from more
than one origin; it still rejects a zero origin or sequence and duplicate ids.

##### Destructive operation wire format

The shared `LogicalChangeFrame` codec is unchanged: `opcode` is adapter-local,
and the schema reference selects its meaning. In destructive schema version 2,
`AppendElement` is opcode `1` and `EraseElement` is opcode `2`. Their payloads
are exactly:

```text
AppendElement = NodeId[16] || sequence-le64
              || key-size-le32 || canonical-key-bytes
              || value-size-le32 || canonical-value-bytes
EraseElement  = NodeId[16] || sequence-le64
```

`canonical-key-bytes` and `canonical-value-bytes` are the typed `KeyCodec` and
`ValueCodec` representations, not a physical MDBX key or local duplicate.
The codecs used by this schema must be canonical: decoding and re-encoding an
accepted byte sequence produces the same byte sequence. Decoding validates every
size before arithmetic or allocation, enforces the applicable `CodecBounds` and
frame bounds, and consumes the payload exactly. Missing adapters, schema-ref
mismatches, reserved generic change flags, and unsupported delivery modes fail
in the sync core before any adapter preflight callback. The payload is
adapter-local, so unknown opcodes, a zero id component, truncated or oversized
fields, integer overflow, non-canonical codec bytes, and trailing bytes fail in
the adapter's `preflight()` or `preflight_batch()` before any `apply()` callback
or MDBX mutation. Empty key or value bytes are valid only when the relevant codec
accepts that value. Version-1 opcode `1` can never be interpreted as version-2
opcode `1`, because the fixed adapter and marker/schema checks precede adapter
preflight.

##### Batch preflight and duplicate identities

Destructive schema version 2 uses the source-compatible non-pure batch hook on
`ILogicalTableAdapter`:

```cpp
virtual LogicalApplyResult preflight_batch(
    MDBX_txn* txn,
    const LogicalChangeBatchView& changes) const;
```

Its default implementation invokes the existing `preflight()` for every change
in the supplied order, so existing adapters remain source-compatible.
`LogicalChangeBatchView` is a synchronous, non-owning view of references to the
caller-provided changes; adapters must not retain the view or references to its
elements after `preflight_batch()` returns. The registry first validates all
schema references, then constructs one stable-order view per registered adapter,
runs `preflight_batch()` for every view, and only then calls any `apply()`
callback in the original frame order. This avoids copying bounded but potentially
large logical payloads while guaranteeing that a batch-level failure cannot
follow another adapter's mutation.

For destructive schema version 2, one batch contains every change for one exact
registered `LogicalSchemaRef`. `OrderedElementId` must be unique within that
batch: a remote frame containing the same id in more than one operation is
rejected by `preflight_batch()` before any `apply()` callback or MDBX mutation,
including an `AppendElement(X)` plus `EraseElement(X)` pair. The uniqueness
scope is not the entire frame across independent schemas. An exact retry is a
separately delivered, already committed frame and remains idempotent.

Typed local capture has the complementary rule. When it appends a newly
allocated `OrderedElementId` and erases that same new element before its
`commit_to_outbox()`, it coalesces both physical mutations and both pending
logical operations to an empty ordered envelope. The per-origin counter remains
advanced, and the empty envelope consumes one delivery sequence: element
sequences are never reused and need not be contiguous. A capture that erases an
element which existed before the session emits only `EraseElement`.

##### Persistent layout and DBI ownership

The schema-v2 marker for persistent-layout-v1 owns exactly three
application-named DBIs: the existing ordered table is the primary DBI, followed
by `element-state` and `elements-by-key`. Their names are chosen by the
application, must be unique to this logical schema, and are canonical members
of `affected_dbis()`. They are not `_mdbxc_` system DBIs and cannot be shared
with another logical schema. The marker primary remains the ordered table; the
marker must contain precisely this canonical three-name set.

Setup calls `SyncEngine::initialize_logical_adapter_schema()` in one writable
transaction. Without a marker, it requires an empty primary DBI, creates empty
auxiliary DBIs, and commits the marker. With an existing matching marker, it
only reopens and validates the existing primary and auxiliary DBIs without
`MDBX_CREATE`. A missing auxiliary DBI or incompatible persistent flags are
schema corruption, not an empty state to recreate during setup or delivery.
Recovery or rebaseline of an existing v2 schema requires a separate explicit
procedure; rerunning setup is intentionally not a repair operation.

Before constructing a schema-v2 primary table accessor, callers use
`KeyOrderedMultiValueTableDestructiveLogicalAdapter::open_primary_for_schema()`.
The helper first reads the persistent marker. It preserves the requested
`MDBX_CREATE` flag only when the marker is absent; once a matching marker
exists, it opens the primary without creation. Thus a missing primary DBI
remains detectable corruption instead of becoming a new empty table. Direct
`KeyOrderedMultiValueTable` construction remains the ordinary non-schema-bound
table API and is outside this lifecycle.

`OrderedElementId` has two encodings. Its logical wire encoding is exactly
`NodeId[16] || sequence-le64`. Its bytewise ordered DBI-key suffix is
`NodeId[16] || sequence-be64`; the fixed-width `NodeId` bytes precede the
big-endian sequence so records of one origin retain numeric sequence order.
The all-zero `NodeId` and `sequence == 0` are invalid. A counter stores the last
allocated sequence, starts at zero, allocates one through `UINT64_MAX`, and
fails before mutation on overflow. A separate per-origin introduced high-water
mark advances with every newly persisted `AppendElement`, remains after a
tombstone, and ensures new ids are strictly increasing in append order. Local
allocation uses the larger of its counter and the introduced high-water mark,
so a later local writer cannot reuse a sequence introduced by remote delivery.
For every origin that has a persisted Live or Tombstone record, the high-water
record is mandatory and must be greater than or equal to that origin's largest
persisted element sequence. Existing-schema verification, local allocation and
admission of a new append check this invariant before mutation; a missing or
lower high-water record is corruption, not an implicit zero value.

For every `AppendElement`, the sync core validates the ordered envelope origin
against the marker before adapter preflight, and destructive-adapter preflight
validates the id origin against the marker before state or table mutation.
`EraseElement` deliberately does not impose that admission check: it addresses
an already persisted id and must remain able to remove legacy elements during a
controlled migration.

`element-state` uses ascending bytewise keys. It rejects `MDBX_REVERSEKEY`,
`MDBX_DUPSORT`, `MDBX_INTEGERKEY`, `MDBX_INTEGERDUP`, `MDBX_REVERSEDUP`, and
`MDBX_DUPFIXED`; no comparator flag is inherited from the primary table. Its
reserved key namespace and persistent-layout-v1 values are:

```text
0x00 || NodeId[16]                         -> last-allocated-sequence-le64
0x01 || NodeId[16] || sequence-be64        -> Live:
                                                 0x01 || logical-key-size-le32
                                                      || exact-KeyCodec-bytes
                                                      || logical-value-size-le32
                                                      || exact-ValueCodec-bytes
                                              Tombstone:
                                                 0x02
0x02 || NodeId[16]                         -> introduced-high-water-le64
```

The `0x00` counter, `0x01` element, and `0x02` introduced-high-water namespaces
must never be interpreted as one another. Counter and high-water values must be
exactly eight bytes. An element key of any other length or tag, a zero
node/sequence, a truncated length/value field, non-canonical codec bytes, or
trailing bytes is corruption and fails closed.
Live state stores exactly the canonical logical codec bytes accepted from the
`AppendElement` payload. Retry equality and the baseline digest compare those
logical bytes. To validate or mutate the primary DBI, the adapter decodes the
logical bytes to `KeyT`/`ValueT` and uses the table's serializer to obtain the
physical key/value bytes; it never assumes the two representations are equal.
Persistent-layout-v1 tombstones deliberately contain no deletion metadata:
their only meaning is permanent non-resurrection of the immutable id. Deletion
metadata, pruning and compaction require a later persistent layout with a
separately designed global recovery horizon.

`elements-by-key` is a DUPSORT DBI. Its key is the exact canonical logical
`KeyCodec` byte representation. Its fixed-format duplicate is:

```text
NodeId[16] || sequence-be64
```

For the initial one-origin implementation, the introduced high-water mark
requires every new append id to be strictly increasing in frame order. Therefore
bytewise id order is the append order of live elements for one key. The DBI uses
`MDBX_DUPSORT` with ascending bytewise comparison and rejects incompatible
duplicate flags. It is a logical index, not a physical mirror of the primary key
comparator. Every index duplicate is exactly 24 bytes and contains a non-zero
element id.

After a destructive schema marker is installed, every local table mutation must use
the typed adapter session. Mixing direct `KeyOrderedMultiValueTable` mutators
with the adapter is outside the schema contract. The implementation cannot
intercept an arbitrary direct raw write at the instant it occurs, but it must
fail closed before a later destructive mutation can ignore it. For every touched
key, preflight and post-mutation validation require exact parity:

```text
physical duplicate in current public per-key order
    <=> one Live element-state record whose decoded logical bytes serialize to
        that key/value
    <=> one elements-by-key duplicate under that canonical logical key with the
        same element id
```

Validation obtains the index side by keyed DUPSORT lookup and the state side by
a reverse scan of all Live state records for that canonical key; the two exact
id sets must agree before their values are compared with the physical table.
An orphan physical row, key-index entry, or Live state record is corruption.
`clear()` and baseline migration must validate the complete three-DBI parity;
persistent-layout-v1 does not substitute an unchecked count, generation, or
hash shortcut. The adapter must never infer or synthesize an element id for a
raw row during a replicated destructive write.

The reverse scan is the default correctness-first implementation. Its per-key
cost is linear in all persisted element-state records, and the ordered
reverse-scan benchmark reports that cost separately from the DUPSORT lookup.
This path remains the fail-closed fallback and must not be removed merely to
avoid the scan. High-water integrity validation likewise scans the persisted
records for one origin before allocation or admission of a new id; no cached
shortcut may hide durable state corruption.

An explicit opt-in fast path is available after a complete validation through
`LogicalCaptureSession::validate_key_index()`. It returns a transaction-bound
`OrderedElementKeyIndexProof` for one canonical key. The proof carries a
non-reusable session lifetime token in addition to the transaction, key, and
mutation revision. It is valid only for that capture session and rejects after
the session is destroyed, even if object or MDBX transaction addresses are
reused. `max_selected_elements` also bounds the complete materialized proof ID
set; validation fails before adding another ID when that bound is exceeded.
The trusted selector methods `erase_at_trusted()`, `erase_value_trusted()`, and
`erase_key_trusted()` reject a stale or foreign proof and reuse its validated
id set instead of repeating the full reverse state scan. They still apply the
candidate and inspected-record bounds and retain physical primary, key-index,
and state point checks before mutation. Proof validation and each later trusted
selector call have separate `BroadEraseBounds` budgets; the scan count is not
carried from one call to the next. Callers must not modify the underlying table
outside the session between proof creation and trusted use. Proofs are
ephemeral and must never be persisted or treated as a corruption-recovery
mechanism; when no proof is available, the ordinary full-validation selectors
remain the required path.

The initial v2 adapter resolves an element through its persistent state and uses
the current per-key live-id order to call `erase_at()` inside the same ordered
delivery transaction. This is valid only for the fixed single-authoritative-
origin contract: no concurrent writer may shift that order. A future multi-origin
or baseline-import v2 extension requires a narrow physical-prefix erase primitive
and a layout revision; it is not implied by this initial adapter.

The versioned logical operations are:

| Operation | Required payload | Apply semantics |
|-----------|------------------|-----------------|
| `AppendElement` | Exact schema-v2 payload above | A new id appends once and persists matching live-state and key-index records. An exact committed envelope retry is deduplicated by the delivery layer before adapter callbacks. A later byte-identical append of an already Live id is a successful no-op; a differing payload or a tombstoned id is a permanent conflict. Repeating any `OrderedElementId` within one schema-local batch is malformed and fails during batch preflight. |
| `EraseElement` | Exact schema-v2 payload above | Delete the exact live occurrence addressed by the state record, remove its key-index record, and persist its tombstone. An exact committed envelope retry is deduplicated before adapter callbacks; an unknown or tombstoned id, or a missing physical live record, fails closed. |

`erase(key, value)`, `erase(key)`, `erase_at(key, index)`, `clear()`, and
`replace_with()` are not independent broad wire operations. The initial typed
capture surface exposes only `append()` and exact `erase(OrderedElementId)`.
Broad local erasure extends that same wire contract by resolving local
selectors into exact ids; it must not add key-, value-, index-, or clear-level
opcodes.

##### Bounded broad local erasure contract

The typed capture-session API is:

```text
struct BroadEraseBounds {
    size_t max_selected_elements;
    size_t max_scanned_records;
};

bool erase_at(key, index, const BroadEraseBounds& bounds)
size_t erase_value(key, value, const BroadEraseBounds& bounds)
size_t erase_key(key, const BroadEraseBounds& bounds)
size_t clear(const BroadEraseBounds& bounds)
```

The implemented `erase_at()`, `erase_value()`, `erase_key()`, and `clear()` methods
distinguish potentially broad selectors from `erase(OrderedElementId)`. They
preserve the corresponding local table semantics: `erase_at()` returns false
for a missing index, `erase_value()` removes every repeated exact value under
the key, `erase_key()` removes every value under the key, and `clear()` removes
every current value. `clear()` scans and reconciles the complete bounded
primary/state/key-index set, validates introduced high-water marks for every
Live or Tombstone origin, and admits each Live id before it materializes the
complete candidate set or creates the first tombstone.

After that prevalidation, bounded selector mutation reuses resolved key/id
metadata and deletes positions in descending per-key order. It does not repeat
the full reverse state scan after each id. Physical cursor reads and all state
or index point reads remain part of the same `max_scanned_records` budget. An
opt-in transaction-bound proof can also reuse the complete validated id set
across selector calls; proof creation itself is bounded by the full ID-set
materialization limit, the proof path is never implicit, and the ordinary
fail-closed resolver remains the fallback.
Committing a session with no pending changes retains the existing empty-envelope
semantics and consumes one ordered delivery sequence.

Broad selectors are encoded exactly once through the logical schema's
`KeyCodec` and `ValueCodec`. `erase_key()` and `erase_at()` resolve the
canonical `KeyCodec` bytes. `erase_value()` selects Live state records whose
stored canonical key and value bytes exactly equal the canonical encodings of
the caller's `KeyT` and `ValueT`. Physical primary bytes are used only for
three-DBI parity validation after the stored logical bytes are decoded and
serialized through the table wrapper; they are never compared directly with
logical selector bytes. Typed `operator==` is not the replicated selector
contract.

Every broad method runs inside the capture session's existing writable
transaction and follows a resolve-then-mutate lifecycle:

1. Validate the persistent marker, authoritative origin, and exact three-DBI
   parity for the selector scope under one cumulative
   `max_scanned_records` budget. `clear()` validates the complete schema.
2. Resolve every selected Live record to its existing immutable
   `OrderedElementId`, including elements appended earlier in the same session.
   Repeated equal values remain separate selected ids.
3. Stop before inspecting a persisted record that would exceed
   `max_scanned_records`, or before materializing a selected id that would
   exceed `max_selected_elements`. Exceeding either caller-supplied bound throws
   `std::length_error`, rolls back and deactivates the complete session, and
   leaves the table, both state DBIs, and outbox unchanged. The implementation
   must not mutate while validating parity or discovering the candidate set.
4. Sort the complete selected set by origin bytes and then numeric element
   sequence, equivalently by the persistent id-index encoding with its
   big-endian sequence. This gives `clear()` a deterministic order independent
   of a local key serializer or MDBX comparator. The little-endian logical wire
   encoding must not be used as a numeric sort key.
5. Apply exact erasure for each id in that order. A selected id appended in the
   same session coalesces with its pending `AppendElement`; a pre-existing id
   adds one `EraseElement`. `max_selected_elements` counts both kinds before
   coalescing, so it bounds candidate memory, mutations, and potential
   `EraseElement` expansion rather than only final frame size.

Both fields of `BroadEraseBounds` are mandatory; there is no default unbounded
overload. `max_scanned_records` is cumulative across parity validation and
selection. Every primary, element-state, or elements-by-key record returned and
inspected counts, including Tombstones and records revisited by separate
passes. Fixed-size schema-marker and local-identity lookups are not part of this
record-scan count. A cursor may fetch one additional record as lookahead to
distinguish exact-bound completion from overflow, but must reject before
decoding, comparing, or materializing that record. The budget therefore bounds
record inspection with one constant cursor-step allowance; it does not bound
wall-clock time, MDBX tree lookup latency, or operating-system I/O.

`CodecBounds::max_ops_per_batch`, `max_value_len`, and
`max_transport_message_bytes` independently bound the final encoded frame.
Encoding or outbox failure still rolls back the transaction and deactivates the
session even when it occurs after in-transaction physical mutation.

`erase_at()` selects at most one id but still takes `BroadEraseBounds`, because
its per-key parity validation may scan persisted state. A zero selected-element
budget succeeds only when the selector resolves no Live id; a zero scan budget
succeeds only when no primary/state/index record needs inspection. No method
may derive ids from raw physical rows, skip tombstone creation, or publish a
partial frame. Receiver apply remains unchanged because it sees only exact
`EraseElement` operations.

`replace_with()` is implemented for the single-authoritative-origin schema-v2
capture session. It first prepares canonical logical bytes, fresh immutable
ids, and exact `AppendElement` payloads, then resolves the complete old live
set under one cumulative existing-state bound. It applies the exact
`EraseElement` and prepared `AppendElement` changes without rescanning the
complete state. Its existing-state and desired-size bounds are mandatory; no
replacement-specific wire opcode is introduced.
Baseline import, multi-origin histories, physical-prefix optimization, and
tombstone pruning likewise remain separate extensions.

Implementation acceptance requires C++11/C++17 coverage for empty and non-empty
selectors under a zero selected-element budget; exact scan-budget success and
scan-budget-plus-one rejection; repeated equal values; canonical selector bytes
that intentionally differ from physical serialization; exact index targeting;
pending-append coalescing; deterministic clear order; tombstone-heavy clear;
selected-element exact-bound success and bound-plus-one rejection; codec/outbox
rollback; native commit-error rollback; restart/replay; and corrupt parity
before selection. A failed broad operation must never leave a partial table
mutation, state transition, tombstone set, or outbox envelope.

Tombstones are retained indefinitely in the first destructive implementation.
They must not be pruned by time or by a local outbox acknowledgement. Safe
compaction needs a separately specified global delivery/recovery horizon that
covers every participating replica and any snapshot/bootstrap path; it is not
part of this extension. Persistent-layout-v1 Tombstones are not compactable:
they retain only an element id and no identity for the ordered delivery that
published `EraseElement`. An element id sequence is not an outbox delivery
sequence and must never be compared with an applied-delivery frontier or a
snapshot watermark.

A future compaction API needs a later Tombstone layout or a separate persisted
compaction certificate/index. For each Tombstone it must bind the erase to its
ordered delivery identity, then receive, persist, and validate a global horizon
in that delivery sequence space. Safe deletion requires every participating
replica's durable applied frontier, every retained snapshot/bootstrap coverage
record, and the sender's retained retry horizon to be beyond that erase
delivery. Local time, one peer's ACK, local outbox emptiness, or the current
table contents are insufficient evidence. Horizon validation and Tombstone
deletion must commit atomically with a restart-safe accepted-horizon record;
otherwise compaction remains disabled.

The destructive format requires a new logical schema version and an explicit
persistent-marker migration. Existing append-only data has no stable element
ids, so there is no automatic in-place upgrade. An application must either start
a fresh destructive schema/table or use this authoritative baseline procedure:

1. Quiesce new append-only capture at the old origin, drain its ordered outbox
   through controlled dispatch to every participating replica, then stop that
   dispatch and record the retained retry/recovery horizon.
2. Freeze the old writer and obtain one authoritative logical baseline. Assign
   each immutable id exactly once in that baseline; a local scan on each replica
   must not independently invent ids. The manifest includes both the complete
   live tuple set and a canonical per-origin allocation map. The map has one
   entry for every origin with an element, allocation-counter, or introduced
   high-water record, including origins represented only by Tombstones or
   coalesced local ids that are absent from the live set.
3. On every replica, use one writable transaction to build its primary ordered
   table, element-state and elements-by-key DBIs from that canonical baseline,
   validate full parity, install the exact manifest allocation-counter and
   introduced-high-water values, and install the new persistent marker. The
   logical `id -> exact KeyCodec/ValueCodec bytes` mapping is identical
   everywhere, but each replica allocates its own local prefixes and therefore
   its physical Live rows and key-index duplicates need not be byte-identical
   to another replica.
4. Verify the marker, local three-DBI parity and the exact per-origin allocation
   map on every participant. Verification must include the required canonical
   SHA-256 digest. Sort every live tuple by `NodeId` bytes and then the numeric
   `uint64_t` element sequence; do not use the little-endian logical wire bytes
   as a numeric sort key. After sorting, serialize each id as its logical wire
   representation and append the tuple data:

   ```text
   OrderedElementId || key-size-le32 || key-bytes || value-size-le32 || value-bytes
   ```

   Append the canonical allocation map in lexicographic `NodeId` order, using
   `NodeId || introduced-high-water-le64 || allocation-counter-le64` for each
   origin. The source values are exact: the counter retains spent but
   unintroduced sequences, while the introduced high-water retains every Live
   or Tombstone id. Local allocation continues from their maximum, but a
   baseline installer must persist both manifest values rather than invent a
   local substitute. Local prefixes are intentionally excluded. Counts are
   diagnostic only and cannot replace the digest. Only then enable the
   destructive writer and its `commit_to_outbox()` path.

Changing the authoritative origin and changing this schema version are separate
controlled operations. They may be combined only by a separately specified
protocol that preserves both the old ordered outbox horizon and every immutable
id. After the first committed destructive envelope, replacing the marker with
the append-only version cannot roll back durable element ids or outbox state; it
requires recovery or re-baselining instead.

The baseline procedure is an authoritative replacement, not a merge. The
source must publish one manifest and digest for the complete sorted live set
and canonical allocation map; every participating receiver verifies that
identity, the exact three-DBI scope, and each per-origin counter/high-water
pair before accepting replacement. A partial baseline, a local-only scan, or a
digest mismatch fails closed before the first physical mutation. The current
library documents this procedure but does not expose a baseline-import API yet.

Schema-v2 has no implicit multi-origin conflict resolution. A concurrent
append from an origin other than the persisted authoritative origin is a
permanent schema conflict, while an `EraseElement` may target a historical id
only after an explicit controlled cutover. Last-writer-wins, vector clocks,
CRDT merge, and independent multi-writer outboxes remain future protocols; they
must not be inferred from the existing ordered-delivery sequence or marker.

Remaining hardening before this initial v2 adapter can grow into a broader
destructive surface includes:

- proof invalidation and recovery policy for future baseline replacement or
  broader multi-writer selectors;
- a duplicate `AppendElement` with identical bytes, the same id with different
  bytes, append of a tombstoned id, duplicate ids in one frame, an
  append-then-erase remote duplicate pair, and unknown destructive opcodes;
- batch preflight rejects all duplicate-id cases before every `apply()` callback
  while preserving original cross-schema apply order after all batches succeed;
  local append-then-erase of one new id commits an empty ordered envelope,
  advances the delivery sequence, replays as a no-op after restart, and does not
  reuse its allocated element sequence;
- fixed schema-v2 encode/decode vectors for both opcodes; wrong schema version,
  missing adapter, unsupported delivery mode and reserved flags rejected before
  adapter preflight; unknown opcode, truncated/oversized fields, arithmetic
  overflow and trailing payload bytes rejected by preflight before `apply()` or
  mutation;
- the append-only adapter rejects every schema version other than 1, the
  destructive adapter rejects every version other than 2, and v1/v2 payloads
  under the opposite marker fail before callbacks;
- local capture at a foreign or stale marker origin; received append ids whose
  origin differs from the envelope or marker origin; zero, overflowed and
  malformed-length ids; corrupt counter records; malformed state values; and
  logical codec bytes whose decode/re-encode form differs from the payload;
- direct raw append and erase after the v2 marker, plus orphan physical rows,
  orphan key-index entries and orphan Live state records;
- missing state/index DBIs and incompatible flags, plus v1/v2 marker or DBI-set
  mismatches, all rejected before adapter callbacks or mutation;
- broad-operation exact-bound and bound-plus-one failures without changing any
  of the three DBIs or the outbox; restart/retry after the durable outbox
  commit;
- restart between baseline construction and marker activation, migration parity
  and high-water verification, an authoritative-baseline SHA-256 mismatch, and
  a later origin cutover with pre-existing ids.

Direct logical frames and unordered delivery must continue to reject every
ordered-table destructive change before adapter callbacks.

## What v0.1 does NOT cover (deferred to v0.2)

For the table-by-table support status, capture coverage, and negative test
anchors, see the
[Sync table coverage matrix](/guides/sync-table-coverage.md).

- `HashedKeyValueStore` — internal hash index layout complicates the wire
  format; deferred until an explicit identity-mapping scheme lands.
- `KeyMultiValueTable` — raw capture, unbounded direct `erase_range()`, and
  general multi-writer destructive convergence remain deferred beyond the
  schema-v3 automatic capture model described above.
- `KeyOrderedMultiValueTable` — raw capture, baseline import, multi-origin
  history and tombstone compaction remain deferred beyond the implemented
  single-origin v2 capture contract. That contract includes `replace_with()`,
  bounded `erase_at`, key/value erase, and clear, each expanded to exact ids.
- `AnyValueTable` — heterogeneous values need type-tag propagation on the wire.
- `IdentityProvider` integration in `BaseTable` — declared in v0.1, no
  write path until HashedKeyValueStore.
- Specialized table types not listed in the implemented scope are not treated
  as sync-covered without explicit wire-format design and round-trip tests.
- Automatic remap of physical `storage_key` from logical `identity_key`.
- HLC or another general production authority for logical-key conflict
  resolution. `time_unix_ns` remains metadata only. The narrow LWW v1 uses an
  application-owned source version for `VersionedKeyValueTable` point writes;
  it is not a generic resolver for raw tables or logical identities.
- `Custom` conflict resolver — schema-level callback; deferred until the
  first real consumer needs it.
- Production-grade deployment wrappers for concrete socket-bound HTTP and
  WebSocket transports (`Boost.Beast`, libcurl, Simple-Web-Server, or another
  framework) with deployment-specific lifecycle, TLS, reconnect, and
  observability policy. The optional Simple-Web bindings are convenience and
  reference integrations; production services may still wrap or replace them
  for their own operations model.
- `zstd` compression — reserved flag, encoder throws, decoder rejects.

## Endianness policy (do not change)

| Layer | Endianness | Reason |
|-------|------------|--------|
| Payload integer fields | little-endian | Native on Windows/Linux/ARM; round-trip is platform-agnostic and cheap. |
| Wire magic, codec version, batch version | ASCII (`MDBXCSYN`) | Independent of byte order. |
| Codec integer fields (`codec_version`, `batch_version`, `batch_flags`, `seq` field, `time_unix_ns`, `ops_count`, op fields) | little-endian | Never sorted on the wire. |
| `ChangeLogStore` key `seq` part | **big-endian** | Range scans (`prune_up_to`, future gap detection) must preserve numeric order under MDBX bytewise compare. LE would sort 256 before 1. |
| `IdentityIndexStore` length prefix `u32 dbi_name_len` | little-endian | Read and written as a single u32, never sorted. |
| All other key bytes (origins, raw `storage_key`, raw `identity_key`, dbi_name bytes) | opaque bytes | Layout defined by the application, not by the codec. |

Sync v0.1 replicates application table keys as raw physical `storage_key`
bytes. For `MDBX_INTEGERKEY` tables this targets ordinary Windows/Linux/macOS
deployments on little-endian x86_64/aarch64-class platforms. It is not a
cross-endian typed key wire format. Use fixed-width key types for schemas that
must be replicated between different C++ ABIs; ABI-dependent source types such
as `long` and `wchar_t` have canonical storage widths, but their value domain
is still the local C++ type domain.

If you add a new ordered numeric key in the future: big-endian. If you add
any new payload integer: little-endian.

## Stores

The `_mdbxc_` DBI prefix is reserved for library-owned stores. Application
tables must not use that prefix, and `SyncEngine` rejects incoming `ChangeOp`
entries whose `dbi_name` targets the reserved namespace before applying any
operation from the containing batch. A multi-batch `PushRequest` is still
atomic: earlier batches in the same request are rolled back when a later batch
is rejected.

### `_mdbxc_meta` (MetaStore)

| Tag | Field | Type | Notes |
|-----|-------|------|-------|
| `0x01` | `db_uuid` | 16 bytes | Stable for the lifetime of this logical database. |
| `0x02` | `node_id` | 16 bytes | Stable for the lifetime of this replication node. |
| `0x03` | `schema_version` | u32 LE | Bumped only when `ChangeBatch` layout or semantics change incompatibly. |
| `0x04` | `local_seq` | u64 LE | Monotonic per-node counter; `increment_local_seq` is the only mutator. |
| `0x05` | `created_at_ms` | u64 LE | Wall-clock metadata, not authority for any conflict. |

### `_mdbxc_changelog` (ChangeLogStore)

| | |
|---|---|
| Key | `origin_node_id (16 raw) ‖ seq (8 BE)` |
| Value | raw `ChangeBatchCodec::encode()` bytes, opaque to this store |
| Insertion | `MDBX_NOOVERWRITE` so accidental `(origin, seq)` reuse throws |
| Retention | explicit `prune_up_to(origin, up_to)` removes records where `seq <= up_to` |

`prune_up_to` opens a cursor, walks from `(origin, 0)` until `mdbx_cmp > (origin, up_to)`,
deletes each hit, then closes. The boundary comparison is on the bytewise
key, which is why `seq` is big-endian in the key.
Pull detects when `request.have + 1` is older than the earliest retained
changelog record for a known origin and returns
`PullResponse{ok=false, error_code=SnapshotRequired}` instead of streaming a
later non-contiguous batch. The full snapshot protocol provides an explicit
fresh-replica importer, and `SyncWorker` can use it for
`CompleteUserDatabase` recovery. Its importer can opt into durable resume for
an unexpired complete-snapshot source session; selective partial-scope
continuation remains outside the implemented contract.

### `_mdbxc_origins` (OriginIndexStore)

| | |
|---|---|
| Key | `origin_node_id` (16 raw bytes) |
| Value | u64 LE - max known changelog `seq` for that origin |

This index is a discovery accelerator for hub-style pull. `ChangeLogStore::append`
updates it atomically with the changelog row. Pull uses the indexed tail to skip
origins where the requester already has `last_seq`, then still seeks exact
`have_seq + 1` changelog keys for origins with possible new batches. When an
upgraded database has legacy changelog rows but no `_mdbxc_origins`, the first
writable append backfills the index by scanning existing changelog keys.
Read-only pull keeps a compatibility fallback: if `_mdbxc_origins` is absent or
empty, origin discovery scans `_mdbxc_changelog`.

`ChangeLogStore::origin_index_matches_changelog()` compares the index against
the changelog-derived origin tails. `ChangeLogStore::rebuild_origin_index()`
is the explicit maintenance path for a manually damaged or otherwise partial
`_mdbxc_origins` DBI; ordinary pull does not rebuild metadata in a read-only
transaction.

These maintenance operations scan the changelog. Use them for startup
diagnostics, manual repair, or rare integrity checks; do not place them in the
normal background-sync loop or per-pull hot path.

### `_mdbxc_applied` (AppliedStore)

| | |
|---|---|
| Key | `origin_node_id` (16 raw bytes) |
| Value | u64 LE — last contiguous applied `seq` |

`SyncEngine` invariant: writes to this store are always contiguous. If
`incoming.seq > last + 1`, the current v0.1 apply path reports a
`SequenceGap` conflict and stores no pending queue; the caller must retry after
missing batches arrive. If `incoming.seq <= last`, the batch is a redundant
replay and is skipped.

### `_mdbxc_identity_index` (IdentityIndexStore) — LWW v1 sidecar

| | |
|---|---|
| Key | `u32 dbi_name_len le ‖ dbi_name bytes ‖ identity_key bytes` |
| Value | `IdentityIndexValue { storage_key, origin_node_id, seq, revision_key, flags }` |

Length-prefix on `dbi_name` is mandatory: without it, `("ab","c")` and
`("a","bc")` collide on the same record. The value layout is opaque and
must contain enough metadata to resolve an `(dbi, identity_key)` entry
back to a physical `storage_key` without re-reading the user table.

`IDENTITY_TOMBSTONE` flag bit marks deleted logical records while keeping
the row readable for older incoming batches that still reference the key.
For LWW v1 the index key is the physical `storage_key`: `VersionedKeyValueTable`
and `SyncEngine` atomically write the source version, origin, and tombstone with
the user mutation. Incoming LWW batches may not carry a different
`identity_key`. Tombstones are not compacted by v1; real removal requires a
separately specified replica horizon.

### `_mdbxc_versioned_dbis` (VersionedDbiStore) — LWW v1 contract registry

Each key is one non-reserved user DBI name with an empty value. Constructing a
`VersionedKeyValueTable` registers an empty user DBI in the same environment;
every replica must make the same registration before it receives revisioned
operations. The registration makes direct raw writes, clear, and bulk/range
mutations fail closed through a `Connection` lookup of the durable registry;
the result does not depend on `SyncEngine` lifetime or capture attachment.
Capture suppression prevents raw re-publication only and does not bypass this
check, so generic logical adapters cannot mutate a registered DBI. It also
determines apply semantics per operation: a registered DBI requires
`LastWriterWins` and revisioned point put/delete; an unregistered DBI rejects
revision metadata and uses ordinary raw apply. Full snapshot manifests cannot
include registered DBIs.

### `_mdbxc_sync_schema` (SchemaRegistryStore) — logical adapter marker

| | |
|---|---|
| Key | application-defined logical schema id string |
| Value | `schema_id` repeated in the versioned value envelope, followed by `LogicalSchemaRecord { kind, schema_version, flags, dbi_name, dbi_names[] }`; owned `dbi_names[]` are stored as a sorted unique set and must include the primary `dbi_name` |

This store is a persistent compatibility marker for future logical table
adapters. It does not enable logical sync by itself. Normal application setup
should call `SyncEngine::register_logical_schema(schema_id, record)` so the
marker is written through a committed sync-system lifecycle transaction. A
wrapper or adapter may call `SchemaRegistryStore::register_or_verify()` only
when it already owns the setup transaction, for example in tests, migrations,
or repair tools. Both paths ensure that an existing database was opened with
the same logical table kind, application schema version, and owned physical DBI
set.

Schema marker evolution is explicit. `register_or_verify()` creates or checks
an exact immutable marker; it does not update an existing record.
`SchemaRegistryStore::migrate_or_verify()` and
`SyncEngine::migrate_logical_schema()` replace a marker only when the current
record first matches an expected old record exactly. This marker operation does
not migrate user table contents or old changelog entries.

`SyncEngine::initialize_local_identity()` creates this DBI together with the
required sync metadata stores in a committed setup transaction. Standalone
maintenance code may still use `SchemaRegistryStore` directly; public store
operations reopen the DBI in the supplied transaction, while explicit
`open(txn)` and `handle(txn)` remain available for low-level raw-handle
maintenance code. `handle(txn)` also reopens the DBI in the supplied
transaction before returning the raw DBI handle.

The registry intentionally uses an explicit application schema id instead of
`typeid`, C++ type names, or ABI-dependent layout data. Unknown logical changes
must still be rejected by the apply path until a matching logical adapter is
registered.

`LogicalChange.hpp` defines the public in-memory model for the future logical
domain:

- `ChangeDomain::RawDbi` is the current `ChangeOp`/`ChangeBatchCodec` domain.
- `ChangeDomain::LogicalTable` is reserved for adapter-owned logical payloads.
- `LogicalSchemaRef` repeats the expected schema id, logical table kind, and
  schema version on each logical operation.
- `LogicalChange::payload` is opaque to the sync core; only a registered
  adapter for the referenced schema may decode it.

The v0.1 `ChangeBatchCodec` still serializes raw DBI operations only. Adding
the logical domain to the wire format requires an explicit codec version bump
and compatibility tests.

`LogicalChangeFrameCodec.hpp` defines the separate explicit logical frame
boundary. It is a strict little-endian codec for ordered `LogicalChange`
payloads, with its own magic/version/mandatory-flags header. This frame is not
embedded into `TransportMessageCodec` yet; receivers must opt into logical
apply explicitly and raw pull/push transports remain raw-DBI only. The frame is
not a delivery envelope: it carries no destination database id, origin node id,
monotonic sequence, frame id, or replay marker. Retrying transports must provide
delivery routing, ordering, and replay protection outside this payload layer
until a dedicated logical delivery envelope is used.

`LogicalDeliveryEnvelopeCodec.hpp` defines that outer retry-safe layer for
explicit logical frame delivery. The envelope carries the destination database
uuid, origin node id, origin sequence, stable frame id, and nested
`LogicalChangeFrame`. `SyncEngine::apply_logical_delivery_envelope()` validates
the destination against local sync metadata, inserts a persistent marker into
`_mdbxc_logical_delivery` with `MDBX_NOOVERWRITE`, and applies adapter mutations
in the same writable transaction. If the transaction rolls back, both user data
and the replay marker roll back. If the same delivery key is seen again after a
committed apply, the engine treats it as a successful no-op and does not invoke
adapters again. Envelopes whose origin is the local node are also treated as
successful no-ops before marker insertion, matching the raw sync self-origin
guard.

The marker key is fixed-size: origin node id, origin sequence, and a stable
digest of `frame_id`. The marker value stores the full delivery identity,
destination database id, and canonical encoded nested frame bytes; reusing the
same delivery key with different frame content is an explicit identity conflict.
When `apply_logical_delivery_envelope_bytes()` receives custom `CodecBounds`,
the same bounds are used for decode and marker frame fingerprinting.

This delivery envelope provides routing validation and replay deduplication, but
not ordered delivery. The receiver currently accepts envelopes from the same
origin out of sequence, and it treats different `frame_id` values with the same
`origin_sequence` as different delivery identities. Applications or transports
that require ordered effects must serialize delivery externally.
`check_logical_delivery_order()` provides a stateless helper for classifying an
envelope against a caller-owned expected next sequence as in-order,
behind the caller's watermark, or ahead with a sequence gap. It only classifies
the numeric `origin_sequence`: it does not decide whether an envelope is an
exact duplicate delivery identity, persist a watermark, buffer missing frames,
or decide whether it is safe to advance one.

`LogicalJournalStore` is the receiver-neutral durable source for locally
originated ordered logical frames. It stores the destination database id,
origin node id, monotonic `origin_sequence`, stable frame id, and frame bytes
in `_mdbxc_logical_journal`; it contains no receiver node id or acknowledgement
state. `SyncEngine::append_logical_journal()` records one such frame atomically
with an optional caller-owned table mutation, without selecting a peer.

`LogicalOutboxStore` remains the sender-side receiver-specific delivery queue.
`SyncEngine::materialize_logical_journal()` projects a contiguous prefix of one
local journal stream into one selected receiver route. Projection preserves the
journal-assigned envelope bytes and sequence, is transactional and retry-safe,
and can be repeated independently for multiple receivers. The route owns only
its pending entries, `acknowledged_through`, and known tail. Its MDBX entry
keys contain the destination, receiver node id, and a big-endian event-sequence
suffix, so `pending_logical_deliveries()` reads one route in numeric order. A
receiver hello must match the selected receiver node id before its queue can be
dispatched; an acknowledgement can therefore advance only that receiver's
durable frontier. A rollback neither consumes a journal sequence nor leaves a
projected queue entry behind.

`SyncEngine::enqueue_logical_delivery()` remains source-compatible for existing
typed capture sessions. It appends to the journal and projects the same
envelope to its supplied receiver in one transaction. Its legacy behavior for
a newly selected receiver is retained: that route begins at the current event
sequence and does not automatically receive earlier frames. New fan-out code
must instead append once and materialize the journal for every receiver from
the desired retained prefix.

`_mdbxc_logical_outbox` is created during normal committed sync-system
initialization. `_mdbxc_logical_journal` is created lazily only by
`append_logical_journal()` or legacy `enqueue_logical_delivery()`. Raw-only
deployments therefore retain their existing DBI footprint; deployments that
publish journal-backed logical frames need one additional named-DBI slot in
`Config::max_dbs`.

The first journal append atomically writes a persistent layout-v1 marker with
its entry. Subsequent append-time migration guards use only that constant-size
marker lookup; they do not scan or decode the accumulated journal. The deeper
`has_persistent_state()` inspection remains reserved for snapshot, recovery,
and diagnostics paths where validating every durable journal record is needed.

This first journal layout deliberately has no migration of pre-journal outbox
records. A process opening an existing non-empty outbox without journal state
fails closed before it appends a new logical frame. Operators must drain or
replace that legacy logical state with an explicit recovery plan before enabling
journal-backed capture; silently creating a new sequence allocator could reuse
an existing `(destination, origin, sequence)` identity.

Persisted outbox entries always satisfy the library-default `CodecBounds`, so a
later worker can decode them without carrying a caller-specific bounds object.
A non-default bound passed to enqueue is an additional validation constraint; it
cannot widen the durable outbox format.

`LogicalDeliveryProtocolCodec` defines a separate logical wire protocol with its
own magic and version. It carries `Hello`, stateless `HelloRequest`, legacy
envelope-only `Delivery`, capability-bearing `DeliveryRequest`, and cumulative
`Acknowledgement` messages. It does not change `TransportMessageCodec` or the
raw `ChangeBatchCodec` version. HTTP maps the request messages to dedicated
logical routes, while WebSocket chooses the protocol by its magic before raw
transport decoding. `DeliveryRequest` carries the selected receiver node id;
the acknowledgement repeats the actual receiver node id. The receiver verifies
that binding before ordered replay, frontier, or adapter work. HTTP and
WebSocket reject the legacy envelope-only `Delivery` message for ordered
delivery, so a routed request cannot be applied by another node with the same
database id.

`ISyncPeer` is also an `ILogicalDeliveryPeer`. Its default logical methods keep
raw-only peers source-compatible: they report no support and never acknowledge a
logical envelope. `DirectSyncPeer`, `HttpSyncPeer`, and `WebSocketSyncPeer`
implement hello plus capability-bearing delivery. `SyncEngine::deliver_pending_logical_deliveries()` negotiates ordered delivery and cumulative acknowledgement,
validates every acknowledgement against the durable sender tail, and commits
each acknowledged outbox prefix before continuing. The older
`apply_logical_delivery_envelope()` API remains intentionally unordered and is
not implicitly redirected through this outbox.

`LogicalDeliveryProtocol.hpp` defines the separate versioned wire boundary for
that exchange. Its `Hello` message carries optional capability bits; an
unknown bit is preserved but does not become negotiated unless both peers expose
a known capability. `Delivery` wraps a strict `LogicalDeliveryEnvelope`, and
`Acknowledgement` carries a destination-scoped cumulative lower bound with
explicit success/retryability. `OrderedDelivery` alone keeps the conservative
contract: a success acknowledges exactly the delivery it answers. When both
peers negotiate `CumulativeAcknowledgement`, the receiver may return its higher
persisted contiguous frontier for a duplicate. The sender validates that value
against its own durable outbox known tail, then atomically removes the verified
contiguous local prefix. This supports a sender restart after the receiver
committed an earlier delivery but before the sender persisted cleanup. Existing
peers that implement only the envelope-only delivery virtual method cannot
receive ordered `DeliveryRequest` messages. Direct, HTTP, and WebSocket peers
expose receiver-bound logical routes; the raw pull/push protocol remains
unchanged and raw-only peers remain source-compatible.

`SyncEngine::apply_ordered_logical_delivery_envelope()` is the receiver-side
implementation of `OrderedDelivery`. `_mdbxc_logical_delivery_order` stores the
highest committed contiguous sequence for each remote origin. A sequence at or
below that frontier is a successful no-op only when its exact persisted delivery
marker matches the incoming frame. A reused sequence with a different frame or
payload, or a replay whose marker has been pruned, fails closed. Exact replay
validation happens before runtime adapter or schema-origin validation, so a
committed delivery can acknowledge a lost-ACK retry after restart or an
administrative ordered-origin migration. A valid duplicate acknowledges through
the attempted sequence by default, or through the persisted frontier when the
sender advertises `CumulativeAcknowledgement`. Only the exact next sequence
reaches schema validation and adapters; a gap is a retryable acknowledgement and
leaves both user data and markers untouched. The generic unordered delivery API
remains separate, so applications do not acquire ordering merely by changing a
call site. Order-state advance, replay marker, and adapter mutations commit in
one transaction.

`ILogicalDeliveryPeer` and `DirectLogicalDeliveryPeer` provide the capability-
gated dispatch boundary. `SyncEngine::deliver_pending_logical_deliveries()`
checks destination database, expected receiver node identity, and negotiated
`OrderedDelivery` before sending its outbox prefix. The receiver independently
checks the receiver id carried by the request before it advances ordered state.
A negotiated cumulative success is bounded by the sender's
durable known tail, so it can safely clean a prefix that was delivered before a
sender restart; entries already removed by that acknowledgement are skipped from
the in-memory pending snapshot. A retryable negative acknowledgement can still
acknowledge only an earlier prefix. Receiver marker retention remains an explicit
lifecycle choice: `prune_ordered_logical_delivery_markers(origin)` prunes only
through the persisted contiguous frontier. It must be used only for origins that
do not mix legacy unordered delivery with the ordered protocol.

`LogicalDeliveryStore` persists one monotonic per-origin watermark in the
optional `_mdbxc_logical_delivery_watermarks` DBI. The DBI is created lazily by
the first `SyncEngine::prune_logical_delivery_markers()` call; normal logical
delivery and read-only inspection work with older layouts where it is absent
and report a zero watermark. Environments that use pruning must reserve one
additional named-DBI slot through `Config::max_dbs`. A caller may advance it
through `SyncEngine::prune_logical_delivery_markers(origin,
safe_through_sequence)`;
the operation atomically deletes the matching replay markers through that
sequence and persists the new boundary. Replays at or below the boundary become
successful stale no-ops without adapter callbacks, even after restart. This is
not an ordering, buffering, or acknowledgement protocol: the caller must
advance the watermark only after its external delivery protocol has established
that no unseen envelope at or below the boundary can arrive later. Time-based
deletion remains unsafe.
`LogicalDeliveryStore::count()` and `list_markers()` are read-only inspection
helpers for diagnostics, repair tooling, and future lifecycle work. They expose
delivery identity and stored frame metadata after validating that the persisted
marker key matches the marker value identity and `frame_id` digest. They do not
define an acknowledgement horizon and must not be used as a standalone pruning
signal. `LogicalDeliveryStore::contains()` likewise reports only the presence
of an exact physical marker; it does not treat a delivery below a watermark as
present. `try_mark_applied()` applies the watermark when deciding whether a
delivery is a stale no-op.

`LogicalTableAdapter.hpp` reserves the apply-side extension point:

- `ILogicalTableAdapter::preflight()` validates one logical change without
  mutating user tables.
- `ILogicalTableAdapter::preflight_batch()` receives a transient, non-owning
  `LogicalChangeBatchView` for one registered schema. Its default invokes
  per-change `preflight()` in view order; an adapter must not retain the view or
  references to its changes after the callback returns.
- `ILogicalTableAdapter::apply()` mutates user tables only after every logical
  batch in the same apply transaction passed preflight.
- `ILogicalTableAdapter::primary_dbi()` names the stable primary physical DBI
  for the logical schema. It defaults to the only affected DBI for
  source-compatible single-DBI adapters. Multi-DBI adapters must override it
  explicitly, and the returned DBI must be included in `affected_dbis()`.
- `LogicalTableRegistry` is a non-owning lifecycle registry keyed by
  `LogicalSchemaRef::schema_id`, but dispatch validates the full
  `(schema_id, kind, schema_version)` tuple and rejects non-zero reserved
  logical flags before any adapter callback.
- `LogicalTableRegistry::preflight_then_apply()` runs core validation for every
  change before any adapter preflight, then runs every schema-local batch
  preflight before any apply. Adapter-local payload validation therefore happens
  in preflight, before apply or MDBX mutation. If an adapter reports failure from
  `apply()` or throws after mutating data, the helper returns failure and the
  caller that owns the MDBX write transaction must abort that transaction; the
  registry cannot roll back a transaction it does not own.

`adapters/KeyValueTableLogicalAdapter.hpp`,
`adapters/KeyTableLogicalAdapter.hpp`, and
`adapters/VectorStoreLogicalAdapter.hpp` provide concrete adapter
helpers. They translate typed table operations into adapter-owned logical
payloads and apply them through
`SyncEngine::apply_logical_changes()` or
`LogicalTableRegistry::preflight_then_apply()` inside a caller-owned write
transaction. Their `commit_to_outbox()` paths atomically commit local table
mutations and a durable ordered envelope. The envelope uses the separate logical
peer protocol rather than the automatic raw pull/push wire pipeline.
`KeyValueTableLogicalAdapter` supports upsert/delete/clear,
`KeyTableLogicalAdapter` supports insert/delete/clear, and
`VectorStoreLogicalAdapter` supports add/erase/clear over its four owned DBIs.
These adapters expose opt-in `LogicalCaptureSession` types. The session owns a writable transaction, suppresses
raw capture for its typed writes, buffers logical changes privately, and copies
them to the caller only from `commit(out)`. Rollback, destruction, or commit
failure discards the pending logical changes. An exception after physical
mutation, outbox enqueue, or native commit processing begins requests
transaction rollback and deactivates the session, so later `commit()` or
`commit_to_outbox()` calls are rejected. Ordinary operation preparation or
encoding failures before transaction mutation leave the session active.
Persistent-storage integrity failures are session-fatal: detecting corruption
during pre-mutation reconciliation rolls back and deactivates the session.
Session construction validates
the adapter against the persistent schema marker in the same writable
transaction, before any local mutation can be performed. For `KeyTable`, only
successful membership changes are captured: inserting an existing key or
erasing an absent key leaves the pending logical frame unchanged; an explicit
clear request is captured even when the table is already empty.

`KeyOrderedMultiValueTableLogicalAdapter` additionally requires its persistent
schema marker to name the local node as the authoritative ordered origin before
it starts capture. This prevents a local append from being committed into an
outbox stream that the ordered receiver must reject. Its capture factory is
lvalue-qualified because a session holds a reference to its adapter.

The adapter payload codec is not the table storage serializer. The initial
codec surface is deliberately small and explicit: callers provide key/value
codec tags such as `KeyValueLogicalInt64Codec<long>` and
`KeyValueLogicalStringCodec<std::string>`. The codec tag, not the native C++
source spelling, defines the logical wire type; for example, local `long` may
be mapped to signed 64-bit wire integers and range-checked on decode. Codec
tags are part of the logical schema contract, so changing them requires a new
schema id, or an explicit schema-marker migration. A plain registration call
still detects `schema_version` changes under an already registered schema id as
a mismatch. Integer logical payload bytes are little-endian, matching the
project-wide payload integer rule above. Apply uses a
transaction-scoped sync-capture suppression guard so an incoming logical change
written through public table methods is not re-published as a local raw
`ChangeOp`.

`SyncEngine::apply_logical_changes()` owns the write transaction, routes the
changes through its registered logical adapters, re-checks the persistent
schema marker for each schema before adapter preflight, validates that the
marker primary DBI matches `adapter.primary_dbi()` and that the canonical owned
DBI set matches `adapter.affected_dbis()`, suppresses raw capture for the
transaction, commits only after the two-phase registry preflight/apply succeeds,
and emits the normal sync apply observer event after commit.
`SyncEngine::handle_push()` remains raw-DBI only. Unknown logical payloads and
stale adapter/schema-marker combinations must not fall back to raw DBI apply.
`SyncEngine::apply_logical_frame()` and `apply_logical_frame_bytes()` are the
explicit frame-level apply helpers for callers that have already opted into
logical sync. Malformed frame bytes are converted to a failure result before
adapter preflight or physical mutation.

Logical-table support therefore has a staged contract:

1. Register a persistent schema marker for the table kind and physical DBI set.
2. Add a versioned wire frame that distinguishes raw DBI operations from
   logical operations and fails closed for unsupported capability bits.
3. Register an adapter that can preflight every logical operation in the
   incoming transaction before applying any of them.
4. Enable capture only after every mutating public method maps to a tested
   logical operation.

The current logical adapter capture sessions are intentionally manual and
opt-in; they do not replace the normal table APIs or claim automatic coverage
for every mutating table method. `VectorStoreLogicalAdapter` likewise does not
turn the store's ordinary `add()`, `erase()`, or `clear()` calls into logical
capture automatically.

Until a later causal-context PR defines dependency cursors, Lamport/HLC order,
or another conflict-resolution model, logical table adapters may claim only
single-writer or application-serialized conflicting writes for the affected
logical dataset. General concurrent multi-writer convergence is out of scope.

## Codec — `ChangeBatchCodec`

See `ChangeBatchCodec.hpp` layout comment for the full byte layout. Locked
contract:

- Magic: 8 bytes `MDBXCSYN`.
- Mandatory unknown batch flag bits → decoder throws.
- `BATCH_COMPRESSED_ZSTD` is reserved and rejected at both encode and
  decode paths in v0.1.
- Encoder rejects `ChangeBatch::version != 1`, `op_type > ClearTable`,
  unknown op flag bits, `OP_TOMBSTONE` with non-empty value, and the
  `OP_HAS_*_KEY` flags with empty payloads.
- Decoder rejects trailing bytes when called with `bytes_read == nullptr`
  or via `decode_exact`.

## Codec - `TransportMessageCodec`

Transport DTOs use a separate envelope from individual `ChangeBatch` records.
This lets HTTP, WebSocket, IPC, or message-queue adapters exchange one
request/response object without inventing per-adapter framing for the sync
payload itself.

Locked contract:

- Magic: 8 bytes `MDBXCPRT`.
- Version: u16 little-endian, currently `5`.
- Message type: u8 (`1=PullRequest`, `2=PullResponse`, `3=PushRequest`,
  `4=PushResponse`).
- Message flags: u32 little-endian, currently zero. Unknown non-zero flags are
  rejected.
- Payload integers are little-endian.
- `SyncCursor` is encoded as `u32 count` followed by `(NodeId, u64 seq)`
  entries.
- `ChangeBatch` values inside pull/push messages are encoded as
  `u32 byte_length` plus exact `ChangeBatchCodec` bytes.
- `PullResponse` carries both `remote_have` (responder applied cursor) and
  optional `remote_tail` (responder changelog tail) so receivers can report
  catch-up progress without changing pagination semantics.
- Full-snapshot pull pages use the same `PullRequest`/`PullResponse` envelope,
  but are explicit: a request has `request_full_snapshot=true`; the first page
  has empty `full_snapshot_id` and `full_snapshot_continuation`; later requests
  repeat both opaque values exactly. A response with `is_full_snapshot=true`
  contains one length-prefixed `FullSnapshotCodec` chunk and no incremental
  batches. Its `has_more` value must equal the chunk continuation flag.
- `PullRequest::max_bytes` is a soft page budget. A responder may return one
  retained changelog batch whose encoded size exceeds `max_bytes` when that
  batch is the next required batch. `PullRequest::max_single_batch_bytes` is
  the hard per-batch budget; exceeding it returns `BatchTooLarge`.
- `PullResponse` and `PushResponse` carry a structured
  `SyncResponseErrorCode` plus an `error_retryable` boolean after their
  human-readable error string. `None` means no structured sync-level
  classification is available. `error_retryable` describes protocol-level
  recovery, not blind replay of the identical request: for example a
  `SequenceGap` apply conflict is retryable after the caller catches up from a
  fresher cursor, while DBI flag conflicts and an unconfigured snapshot source
  are permanent until the caller changes behavior. `SnapshotRequired` means the
  requested changelog range was pruned and cannot be recovered through
  incremental pull. `BatchTooLarge` means a retained changelog entry exceeds
  the requester's hard per-batch limit and is permanent until the requester
  raises that limit or obtains the data through another path. Transport-local
  errors remain represented by adapter status, close codes, response headers,
  and `SyncTransportRetryHint`.
- `CancellationToken` fields in request DTOs are local call-control state and
  are never serialized. Decoded request DTOs contain default non-cancellable
  tokens.
- Decoders reject trailing bytes, truncated messages, invalid boolean values,
  wrong message types, unsupported versions, and configured size-limit
  violations.
- `peek_message_type()` validates the shared envelope and returns only the
  message kind. It is for message-oriented adapters such as WebSocket servers
  that dispatch complete binary messages before decoding the type-specific
  payload.
- A null `CodecBounds` argument uses the default `CodecBounds` limits at this
  transport layer; adapters may pass stricter limits for their deployment.

## Sync flow (current v0.1 core behavior)

Default round shape, single-writer friendly and the base case for
multi-master:

```
origin A writes
    -> Transaction::commit pre-commit hook
        -> ChangeAccumulator.flush (writes ChangeLogStore; OriginIndexStore
           is updated by ChangeLogStore::append)
            -> mdbx_txn_commit() — changes land atomically

receiver B
    -> pull / push via ISyncPeer
        -> SyncEngine.handle_push / handle_pull
            -> begin write txn
                if already_applied(origin, seq): skip
                if sequence gap: conflict / rollback
                for op in batch.ops: apply raw dbi_op
                mark_applied(origin, seq)
            -> commit
```

Application integration contract:

- supported table write methods keep the same public API with or without sync;
- callers do not wrap each individual `insert`, `insert_or_assign`, `erase`,
  `reconcile`, or range erase in a sync-specific call;
- `Connection::attach_sync_capture()` installs the capture sink for commits on
  that connection; `SyncCaptureScope` is the RAII helper for temporary
  attachments and restores the previous sink when the scope ends; supported
  write operations are recorded by table code and flushed by the transaction
  pre-commit hook;
- `BaseTable::record_op()` constructs a full `ChangeOp` and forwards it through
  `ISyncCaptureSink::record_change_op(txn, change)`. The older raw-field callback
  remains the source-compatible abstract sink contract for existing custom
  sinks; new full-`ChangeOp` sinks may derive from `FullChangeSyncCaptureSink`;
- if `record_change_op()` throws after the user-table MDBX write succeeded
  including from legacy `record_change()` forwarding, or `flush_in_txn()`
  throws during pre-commit capture flush, the transaction is marked as failed
  for sync capture. A later commit is rejected before another flush attempt, so
  the caller must roll back or let the transaction guard abort it;
- mutating supported table calls must use connection-managed transactions while
  capture is attached. Caller-created raw writable `MDBX_txn*` handles are
  rejected before mutation because native `mdbx_txn_commit()` cannot invoke the
  capture pre-commit hook. Caller-created raw read-only transactions remain
  valid for read/search snapshot operations;
- choose the scope helper for bounded write phases owned by one stack frame;
  choose explicit attach/detach only for a wider component lifecycle where the
  caller can prove no concurrent table operation or active transaction races
  with capture attachment changes;
- nested `SyncCaptureScope` objects must be detached or destroyed in strict
  LIFO order. Explicit out-of-order `detach()` is rejected, and raw
  attach/detach calls must not replace the connection sink while a scope owns
  it;
- a standalone write method call that opens its own transaction commits as one
  local change batch;
- an explicit transaction passed through several supported table calls commits
  those writes atomically and flushes one local change batch;
- reads, searches, range scans, and failed/rolled-back transactions emit no
  change batch;
- local commit never contacts a remote node. Pull/push delivery is performed
  later by explicit protocol code or by `SyncWorker` through an `ISyncPeer`.

Cold replica sync currently uses changelog replay:

```
B: empty cursor
    -> pull request, have = empty
A: handle_pull treats it as a full changelog replay across known origins
    -> streams persisted ChangeBatches from seq=1 with pagination limits
B: applies each page as above
    -> onward sync is incremental pull-from-have
```

The reserved `seq=0, BATCH_HAS_MORE` full snapshot format is separate from
retained changelog replay. `FullSnapshotProtocol.hpp` defines and validates its
chunk codec: every chunk carries a source identity, immutable per-origin
replication tail, stable `snapshot_id`, chunk index, replacement scope, opaque next-page
token, manifest version, immutable named-user-DBI manifest, and a nested raw
batch with `seq=0`. Transport codec v5 carries the explicit session request and
one snapshot chunk in `PullResponse`; it rejects mixed incremental/snapshot
pages and malformed session state.

`SyncEngine` exports two explicit snapshot scopes. `ManifestOnly` materializes
the caller's configured user-DBI manifest in one read transaction, bounds active
sessions and materialized data, and returns stable pages. It is a manual
physical replacement mode: the final import replaces only manifest DBIs and
never changes `_mdbxc_applied`. `CompleteUserDatabase` requires no caller
manifest; the engine inventories every named non-reserved user DBI from MainDB
under the same source read transaction. Its fresh-replica importer rejects a
destination user DBI outside the exported inventory and, only after the complete
replacement plan commits, atomically writes the immutable source tail to
`_mdbxc_applied`. An empty configured `ManifestOnly` source returns
`SnapshotNotConfigured`; a complete-source inventory with no user DBIs is
rejected. Unknown, expired, or mismatched continuations or a different requester
return `SnapshotSessionInvalid`; bounded session capacity returns retryable
`SnapshotSessionBusy`.

Raw `CompleteUserDatabase` is a raw-sync-only recovery scope. Before a
session is materialized, the source rejects any persistent logical-sync state
with `SnapshotLogicalStateUnsupported`: schema markers, replay markers or
pruning watermarks, ordered-delivery frontiers, and durable outbox metadata or
entries. A physical copy of logical adapter DBIs without this state cannot
safely continue logical delivery.
`ManifestOnly` is likewise only a manual physical replacement tool: it does
not claim to repair or bootstrap logical replication.

`LogicalRecoveryRequest` / `LogicalRecoveryResponse` define the separate
logical-aware fresh-replica path. It reuses bounded physical snapshot pages but
delivers their final page with an immutable baseline containing schema markers,
replay markers and watermarks, ordered receiver frontiers, and the source
outbox tail plus pending envelopes for the requesting receiver node. The
pending suffix must be contiguous and end at that receiver's known tail. The
importer creates replay markers for those pending source envelopes, restores
the source frontier, and never copies the source outbox as receiver-local work.
The final physical replacement, raw cursor bootstrap, and logical metadata
restoration share one MDBX transaction. Missing matching destination adapters,
corrupt baseline metadata, or existing logical receiver state abort that
transaction. Source materialization and receiver staging use one shared budget
for physical operations and logical baseline records; source enumeration spends
that budget before retaining each logical record. `LogicalRecoveryPeer` also
accepts a cooperative cancellation token: a cancelled materialization returns a
retryable failure and publishes no snapshot session. `LogicalRecoveryProtocolCodec`
is a distinct strict versioned wire contract (`MDBXCLRP`): every request carries
the requester and target `DbId`; it rejects trailing bytes, incompatible codec
versions, and inconsistent page shapes, and nests the existing bounded
`FullSnapshotCodec` for physical pages. Sources reject a mismatched `DbId`
before snapshot materialization. HTTP bearer and WebSocket authenticated-node
policies apply their existing per-principal DB access checks to this request,
as they do for raw pull and push. `DirectSyncPeer`, `HttpSyncPeer`, and
`WebSocketSyncPeer` implement this capability. HTTP and WebSocket forward a
caller cancellation token to their client-side socket/post operation;
propagation of a remote disconnect into source materialization remains
concrete-server-backend work and is deferred.

`SyncEngine::apply_full_snapshot_chunk()` validates immutable page-zero metadata
on every continuation. Its default staging is bounded process memory. With
`FullSnapshotImportOptions::persist_complete_staging=true`, non-final
`CompleteUserDatabase` pages are instead additionally recorded in one lazy
reserved DBI, so a newly constructed engine can reconstruct the exact validated
replacement plan and resume with the stored source continuation. This durable
mode excludes `ManifestOnly` and logical-aware recovery in v1. Only the final
page opens the replacement write transaction: it requires zero local changelog
sequence, an empty applied cursor, and empty manifest DBIs, then applies the
staged `ClearTable` / `Put` plan. That same transaction removes persisted
staging and, for a complete replacement, bootstraps `_mdbxc_applied` from the
immutable source tail. Complete replacement additionally requires a destination
node identity absent from the source tail, so a restored database cannot resume
local writes with an origin sequence already used by the source. Any
interruption, malformed continuation, bound failure, or non-fresh target fails
before a user-DBI commit.
Starting a new non-persistent `CompleteUserDatabase` import, or explicitly
disabling persisted staging, abandons any existing durable session.

`SyncWorker` can opt in to `SnapshotRequired` recovery only with a fresh-replica
`CompleteUserDatabase` session. It starts a new empty-cursor source session and
drains every page through the final import commit. When the engine enables
`persist_complete_staging`, a later worker instance resumes an unexpired source
session from the durable continuation after a transport failure or restart. A
source `SnapshotSessionInvalid` discards that durable staging because the
continuation can no longer be trusted, and a later fallback starts a new source
session. It never treats `ManifestOnly` as a raw-sync fallback, because that
scope has no global cursor bootstrap. The worker does not repair an existing
partial replica; a failed fresh-target preflight remains a reported sync error.
If changelog pruning removed entries needed by the requester's cursor,
`handle_pull()` returns `SnapshotRequired` with no batches. This is also a
valid sync response, not a transport failure.
`SyncWorkerPermanentFailurePolicy` is transport-only; workers still expose
sync-level response errors through round results, stage events, and status
snapshots without treating them as permanent transport failures.

### Remaining full snapshot work

The full snapshot protocol is explicit rather than another spelling of retained
changelog replay. The reserved request shape is
`PullRequest::request_full_snapshot=true`; responders return snapshot chunks
only when the caller requested that mode, never as an implicit fallback from a
normal incremental pull.

The implemented source session, fresh-replica importer, worker fallback, and
complete-scope persisted resume preserve these properties. Any future
selective-scope extension must preserve them too:

- A full snapshot export is a named snapshot session. The first response must
  return an opaque `snapshot_id` and all later pages must present the same id;
  pages with an unknown, expired, or mismatched id are rejected instead of being
  merged into the receiver state.
- The session is tied to one stable source view. All snapshot data, the exported
  DBI manifest, the source `NodeId`, the `db_id`, and the advertised changelog
  tail must come from one MDBX read transaction or from a materialized snapshot
  created from that read transaction. The advertised tail is immutable for the
  whole session.
- The first page carries a manifest before any data is considered complete. The
  manifest lists the selected user DBIs, DBI flags, empty source DBIs, the fixed
  source identity, the immutable changelog tail, the replacement scope, and a
  manifest version/hash that later pages repeat. A receiver must reject chunks
  whose manifest identity differs from the first page.
- Receiver-only DBIs are an explicit policy decision, not an accidental side
  effect. `ManifestOnly` preserves DBIs outside its manifest and does not
  bootstrap global cursor state. `CompleteUserDatabase` inventories all named
  non-reserved source DBIs and fails closed if a fresh destination contains a
  user DBI outside that manifest; it does not silently drop receiver-only DBIs.
- Snapshot chunks are distinguishable from ordinary changelog batches. The
  implemented snapshot-specific envelope carries a nested
  `ChangeBatch{seq=0, batch_flags=BATCH_HAS_MORE...}`; ordinary local changelog
  batches keep `seq > 0`.
- A chunk carries physical `ClearTable` / `Put` operations for user DBIs only.
  It must not export or overwrite reserved `_mdbxc_` sync metadata DBIs through
  the normal user-operation path.
- Snapshot apply is a replacement operation for the selected database content:
  the receiver must clear each exported user DBI before applying that DBI's
  first snapshot entries, then apply later chunks idempotently or reject
  ambiguous resume attempts.
- Metadata bootstrap is separate from user data import. Only after a complete
  user-database snapshot commits does the receiver record applied cursors
  consistent with the responder's advertised replication tail. A manifest-only
  import intentionally leaves global cursor state unchanged.
- Chunk pagination must use the existing pull limits: `max_bytes` as a soft page
  budget and `max_single_batch_bytes` as a hard limit for a single encoded
  snapshot chunk. If one logical DBI chunk cannot fit under the hard limit, the
  snapshot encoder must split it further or return a structured permanent sync
  error.
- Multi-page sessions must carry an explicit continuation token. The token is
  opaque to callers, but it must identify the next physical position precisely
  enough to resume within duplicate-value tables, for example by encoding the
  DBI name, storage key, duplicate position, and manifest identity. Tokens may
  expire; expired or foreign tokens are permanent sync-level rejections and do
  not advance receiver state.
- A failed snapshot apply must not leave the receiver advertising a partially
  advanced applied cursor. Persisted complete-import staging contains only
  validated non-final pages and the next opaque source continuation; final
  user-DBI replacement, applied-cursor bootstrap, and staging removal commit
  atomically. The receiver remains a fresh replica until that commit.
- `SnapshotRequired` remains the incremental-pull recovery signal. It tells the
  caller that retained changelog replay cannot satisfy the request; the caller
  may then make a separate `request_full_snapshot=true` request. The worker
  uses this only for `CompleteUserDatabase` fresh-replica recovery.

Scope-aware partial snapshot continuation is still deferred. A partial manifest
cannot share the global per-origin cursor: that architecture requires stable
scope identity, per-scope or per-DBI applied progress, scope-filtered changelog
pull and retention, and explicit DBI membership-change handling.

## Background worker lifecycle

`SyncWorker` is the minimal background driver for `SyncEngine + ISyncPeer`.
It owns its thread but does not own the engine, peer, or connection. The
runtime state shape is:

```
Stopped -> Starting -> Idle -> Pulling -> Applying -> Idle
                              -> Backoff -> Idle
                              -> Stopping -> Stopped
                              -> Failed
```

Worker invariants:

- no local MDBX transaction is held while waiting in a peer pull or logical
  delivery call;
- no local MDBX transaction is held during idle or backoff sleeps;
- pulled pages are applied through `SyncEngine::handle_push()`, so each page
  uses one short local write transaction;
- stop requests cancel the active raw request token and call
  `ISyncPeer::request_cancel()` at most once for each observed in-flight
  peer call. A page returned after stop was requested is not applied, and a
  stop request recorded between logical envelopes prevents the next logical
  delivery from starting;
- `stop()`, `join()`, and destruction may wait for an in-flight peer call to
  return when the concrete transport does not support cancellation;
- `SyncWorkerGuard` may own a single background run session for an existing
  worker; it starts the worker on construction and stops it on destruction,
  while the `SyncWorker` object itself must still outlive the guard;
- `SyncNodeSession` may own one application-level sync session around an
  existing worker: it can attach capture, start the worker, register a remote
  apply observer, then stop and release those hooks in reverse order;
- lifecycle mutations (`start`, `stop`, `join`, `run_once`) are caller-serialized,
  while `request_stop`, `state`, `last_error`, `last_observer_error`, and
  `wait_until_state` are thread-safe;
- optional `ISyncWorkerObserver` callbacks report coarse sync stages
  (`round`, `pull`, `apply`, `logical delivery`, `backoff`), page application,
  round completion, and backoff entry synchronously on the thread that runs the
  sync round;
  observer implementations must outlive the worker, return quickly, and avoid
  caller-serialized lifecycle calls from worker-thread callbacks;
- worker stage and round events include a best-effort progress estimate derived
  from the latest `PullResponse::remote_tail` cursor and the receiver cursor;
  it reports known catch-up progress only, not future writes or wall-clock ETA;
- observer exceptions never fail the sync round and are reported through
  `last_observer_error`; they do not overwrite `last_error`, which remains
  reserved for pull/apply, cancellation, and lifecycle failures;
- the `SyncWorker` object must outlive its background thread and must not be
  destroyed from callbacks running on that worker thread;
- `Transaction`, raw `MDBX_txn*`, and cursors stay on the thread that opened
  them and never cross the worker boundary.

The worker is a lifecycle/concurrency helper, not a transport. HTTP/WebSocket
peers remain separate adapters over `ISyncPeer`. Transport adapters that can
interrupt blocking I/O should poll the request `cancel_token` where possible
and implement `request_cancel()` by using their own timeout, socket shutdown,
or equivalent mechanism when polling alone cannot unblock the operation.

`SyncWorkerOptions::enable_logical_delivery` is false by default. When enabled,
the worker attempts logical dispatch only after its normal raw pull loop has
drained the current round (`has_more == false`). It first checks whether the
replication `DbId` has any pending logical entry. An empty logical outbox does
not require capability negotiation, so a raw-only peer still completes the
round. Pending logical state requires `ISyncPeer::supports_logical_delivery()`;
the worker then obtains the remote hello and checks its receiver route. The
worker dispatches at most one
envelope in each peer call, reports `LogicalDeliveryStarted` and
`LogicalDeliveryFinished` stages, and stores delivered and acknowledged counts
in its round result. `max_logical_deliveries == 0` drains the pending prefix;
any positive value bounds one round. Unsupported peers and failed
acknowledgements leave the unacknowledged outbox suffix durable for retry.

Cancellation intentionally stays minimal in the core API: operation-scoped
tokens plus the existing best-effort peer hook. Do not add callback
registration, generation-based token reuse, or allocation-free state reuse
without evidence from a real transport adapter or benchmark. The preferred next
step for HTTP/WebSocket transports is an adapter-local bridge from
`cancel_token` / `request_cancel()` to the transport library's native timeout,
socket shutdown, or cancellation primitive. Revisit the core API only if that
adapter-local approach proves insufficient, or if benchmark data shows
per-operation cancellation-state allocation on the `pull()` path is a measured
hot spot.

## Transport boundary contract

The `ISyncPeer` interface is the single boundary between the sync core and
any external transport (in-process, HTTP, WebSocket, IPC, message queue).
This section locks in the rules a transport adapter must satisfy, the
places where cancellation must be wired through, and the policy decisions
that stay out of the core API. The design is intentionally minimal: locks
in place until a real adapter shows that core cannot support it.

### Boundary rules

- Only `PullRequest`, `PullResponse`, `PushRequest`, and `PushResponse`
  cross the boundary. Binary adapters should use `TransportMessageCodec`
  for those DTOs unless they deliberately define another documented
  content type. `MDBX_txn*`, `MDBX_cursor*`, `Connection`,
  `SyncEngine`, table objects, the `ISyncCaptureSink`, and the system
  stores (MetaStore, ChangeLogStore, AppliedStore, OriginIndexStore,
  IdentityIndexStore) are thread-owned and never enter a transport
  payload.
- `DirectSyncPeer` forwards request DTOs to another `SyncEngine` in the same
  process and is suitable for tests, examples, and in-process demos.
  `HttpSyncPeer` is a framework-neutral HTTP-shaped adapter over an abstract
  `IHttpSyncClient`; it defines the route/body contract but does not own a
  socket. `WebSocketSyncPeer` is a framework-neutral binary-message adapter
  over an abstract `IWebSocketSyncChannel`; it defines the complete-message
  request/response contract but does not own a socket or session. Production
  code that crosses a process boundary must bind these seams to a transport
  implementation that owns its own connection, threading, and lifecycle.
- A transport adapter does not own the caller's `SyncEngine`. The
  receiver-side `SyncEngine` (the one whose state changed because of a
  remote write) must live on the thread that owns the receiver
  `Connection`. Cross-thread writes are not supported.
- A pull page and the matching apply round are owned by `SyncWorker`
  (background) or by the caller of `run_once()` (foreground). The
  transport's `pull()` returns detached `ChangeBatch` values; the worker
  then calls `SyncEngine::handle_push()` which opens its own short
  write transaction.

### Where socket / RPC timeouts live

Timeouts are an adapter-local concern. The core API exposes no
`timeout` field on `PullRequest`/`PushRequest` and no `set_timeout`
on `ISyncPeer` because transports differ about how a deadline is
expressed (HTTP, libcurl, gRPC, Boost.Asio, raw sockets, ...). The
adapter owns its timeout configuration and is responsible for
honoring it. An adapter that cannot bound a blocking call without
the core's help must say so explicitly in its documentation; do not
silently block forever on the worker thread.
The ready-made Simple-WebSocket client binding exposes
`WebSocketSyncChannelConfig::exchange_timeout` as a whole-exchange deadline
covering connect, request send, and response wait. Zero disables the deadline;
negative values are rejected.

The timeout policy that does belong to the core is the worker
backoff loop: repeated pull failures increase the wait between
attempts up to `SyncWorkerOptions::max_backoff`. That is a cooldown
for failed *rounds*, not a per-call deadline.

### Where the cancellation bridge lives

A transport adapter must implement two cancellation channels, both of
which are already in the public API:

1. `PullRequest::cancel_token` and `PushRequest::cancel_token`.
   `SyncWorker` sets a cancellable token before every `pull()` call and
   requests cancellation on it when `request_stop()` runs. The adapter
   receives the token by value and may poll it during any
   interruptible wait. A `CancellationToken` is cheap to copy and
   non-owning; copies share state with their `CancellationSource`.
2. `ISyncPeer::request_cancel()`. `SyncWorker` calls this hook
   at most once for each observed in-flight `pull()`. The default
   implementation is a no-op so token-only peers stay valid. Adapters
   whose underlying call ignores the token (TCP read, blocking RPC,
   legacy client) must override `request_cancel()` to close their
   socket, shutdown their transport, or call the library's native
   cancellation primitive so the in-flight call returns promptly.

The default adapter-local bridge combines both channels: the adapter
sets the same deadline it would use for a normal timeout, polls the
`cancel_token` while waiting on the underlying socket or future, and
also overrides `request_cancel()` to force the socket into a closed or
half-closed state so any blocking call returns. The bridge must be
documented adapter-side; the core does not assume a specific
mechanism.

Cancellation is best-effort. `SyncWorker::stop()`, `join()`, and
destruction may wait for an in-flight peer call to return when the
adapter cannot interrupt that call. This is acknowledged in
`SyncWorker`'s class docstring and is not a contract violation by
the adapter.

### Where reconnect policy lives

Reconnect policy is transport-local. There is no `reconnect_interval`
on `ISyncPeer` and no retry counter on `SyncWorker` because retry
shapes differ across transports (immediate retry after transient
connection reset, exponential backoff across peer failures, jittered
reconnect for peer discovery, ...). An adapter may own its own
`std::thread` that holds the transport connection and surfaces peer
state through a small `ISyncPeer`-like façade; that façade must still
honor the cancellation bridge above.

The one retry shape that lives in core is the worker backoff loop
on repeated pull failures. It is intentionally distinct from
connection-level reconnect and does not attempt to model
connection-state separately from transport-call failures.

### Where auth, rate limits, TLS, and compression live

Auth, rate limits, allow/deny lists, TLS, and compression are adapter-local.
The core has no
`credentials` field on `PullRequest`/`PushRequest` and no TLS
configuration on `ISyncPeer`. A real adapter typically wraps the
transport with the platform's TLS layer (OpenSSL, Schannel, NSURLSession)
or delegates to a server framework that owns the secure channel.
Identity at the sync layer is `NodeId` (16 bytes); the adapter decides
whether TLS terminates before or after the sync payload is parsed.
Authorization and rate limiting should be implemented as wrappers around
request handling: inspect transport metadata and, when needed, the decoded
DTO header fields (`requester`, `sender`, `db_id`) before dispatching to
`SyncEngine`. Do not put bearer tokens, ACL decisions, or rate-limit counters
inside the sync DTO wire format.

`SyncPeerMiddleware`, `HttpSyncClientMiddleware`, and
`HttpSyncServerMiddleware` are the v0.1 framework-free
building blocks for these wrappers. `NodeDbAllowListPolicy` checks decoded
`requester` / `sender` and `db_id` values. `HttpRouteAllowListPolicy` checks
HTTP-shaped targets before a concrete HTTP client sends bytes.
`HttpSyncRequest` carries adapter-local `headers` and `remote_address` fields;
they are not serialized by `TransportMessageCodec`. `HttpBearerTokenPolicy`,
`HttpBearerNodeIdentityPolicy`, `HttpRemoteAddressAllowListPolicy`, and
`FixedWindowHttpRateLimitPolicy` run on that context before dispatch.
`HttpBearerNodeIdentityPolicy` is the v0.1 authenticated-identity contract:
the bearer token maps to one `NodeId`; a pull request is allowed only when
`PullRequest::requester` matches that authenticated node, and a push request is
allowed only when `PushRequest::sender` matches that authenticated node.
Optional per-token DB access rules validate `db_id` before dispatch.
`SyncDbAccess` makes the intent explicit: a token binding may allow any DB,
deny every DB, or allow only listed DB ids. `HttpBearerNodeIdentityPolicy`
keeps token bindings in `allow any DB` mode until
`allow_db_id_for_token()` switches that token to a restricted list. This keeps
the sync DTOs self-describing while preventing a transport principal from
claiming another node id inside the binary payload.
Rejections may carry response headers such as `WWW-Authenticate` or
`Retry-After`; concrete HTTP bindings must write those headers to the real
response. `HttpSyncHeaders::request_id()` and
`HttpSyncHeaders::trace_id()` define optional adapter-local correlation
headers. They are copied from request to response by the framework-neutral HTTP
server/middleware, but they are not serialized inside sync DTOs.
`FixedBudgetSyncTransportPolicy` is a deterministic fixed-budget limiter useful
for tests, examples, and simple adapters; production adapters can replace it
with a time-window or token-bucket policy while keeping the same middleware
shape. `FixedWindowHttpRateLimitPolicy` accepts an optional non-zero bucket cap;
expired identity buckets are evicted before a new identity is rejected with
`429` and `Retry-After`. `SyncTransportMetricsObserver` records basic call,
rejection, failure, cancel, and batch counters without changing transport
behavior.
`TransportMessageSizePolicy` is a pre-decode guard for HTTP bodies and
WebSocket binary messages. It complements `CodecBounds`: adapters can reject
oversized transport frames before decoding, while the codec still validates the
structured payload.
Ready-made concrete bindings also expose `CodecBounds` in their config objects.
Simple-Web HTTP rejects oversized `Content-Length` before copying the request
body into an adapter DTO and checks the actual buffered body as a fallback.
Simple-WebSocket and Kurlyk/libcurl check their buffered messages before
calling the framework-neutral sync decoder.

These helpers do not replace server-framework authentication or
per-remote-client rate limiting before `HttpSyncServer::handle()`. They also
count middleware hook invocations: if one observer is installed at both the
peer layer and HTTP client layer, a forwarded `request_cancel()` can be counted
once per layer.
For WebSocket, decoded DTO policy can wrap `WebSocketSyncPeer` through
`SyncPeerMiddleware`. Server-side bindings can pass a
`WebSocketSyncRequestContext` to `WebSocketSyncServerMiddleware` after the
concrete WebSocket framework authenticates the session. The framework-specific
token, cookie, mTLS principal, or remote address stays outside the sync DTO;
`WebSocketAuthenticatedNodeIdentityPolicy` receives only the resulting
authenticated `NodeId`, an explicit `SyncDbAccess` rule, and one complete
binary message. WebSocket request contexts default to `deny every DB`, so a
binding must opt into `SyncDbAccess::any()` or allow concrete DB ids for that
session. The policy then requires `PullRequest::requester` or
`PushRequest::sender` to match that authenticated node before dispatching to
`WebSocketSyncServer`. `WebSocketSyncServerMiddleware` preserves policy close
codes by throwing `WebSocketSyncRejected`; concrete bindings should catch it
and send `close_code()` as the WebSocket close frame status.
Backpressure, reconnects, ping/pong, and pre-DTO rate limits remain
binding-local.

Transport retry classification is adapter-level. HTTP 2xx means transport
success; the decoded sync response still needs its own `ok/error` handling.
HTTP `408`, `425`, `429`, `500`, `502`, `503`, and `504` are retryable
transport statuses by default. Auth, authorization, routing, content-type,
payload-size, and malformed-body errors (`400`, `401`, `403`, `404`, `405`,
`413`, `415`) are permanent for the current request unless a higher-level
adapter refreshes credentials or changes the request. `Retry-After` is advisory
transport metadata; `http_sync_retry_hint()` exposes relative
`Retry-After: <delta-seconds>` values without parsing HTTP-date clock policy.
`SyncWorker` backoff remains the core retry loop for failed
sync rounds. For WebSocket, close code `1000` is success; `1001`, local
observations `1005`/`1006`, and server/transient codes `1011`, `1012`, `1013`,
and `1014` are retryable by default. Policy, malformed payload, and oversized
message codes such as `1007`, `1008`, and `1009` are permanent for the current
request. `websocket_sync_retry_hint()` exposes the same retryable flag for
concrete bindings that report close codes.

Peers that can classify adapter failures expose the last observed advice
through `ISyncPeer::last_retry_hint()`. The default implementation returns an
unavailable hint, so existing peers are source-compatible and callers can keep
using their fallback retry policy. Concrete peers should set
`SyncTransportRetryHint::available` when a transport failure was actually
classified, clear stale retry advice after successful operations, and update it
after transport-level failures. With `available=true`, `retryable=false` means
the transport classified the failure as permanent for the current request.
Successful transport responses are not failures and should clear back to an
unavailable hint. The hint is advisory: callers may still use their own retry
scheduler, but `SyncWorker` and examples consume the same transport-neutral
shape without depending on HTTP or WebSocket headers. Worker round and stage
events carry the latest hint for failed pulls. `SyncWorker::status()` exposes a
thread-safe snapshot for polling UIs, health endpoints, and structured logging
code that does not want to reconstruct state from observer callbacks. In
background mode, an available retryable hint with relative `Retry-After`
overrides the current exponential delay for that backoff wait, capped by
`SyncWorkerOptions::max_backoff`. Permanent hints remain advisory by default:
`SyncWorkerPermanentFailurePolicy::KeepRetrying` keeps using the normal
backoff loop. Applications that want a classified permanent transport failure
to stop the background loop can set
`SyncWorkerPermanentFailurePolicy::StopWorker`, which leaves the worker in
`SyncWorkerState::Failed` instead of entering backoff.

`ChangeBatchCodec` already rejects `BATCH_COMPRESSED_ZSTD` at both
encode and decode paths. Adding a real `zstd` backend is a codec
change and belongs in a separate design pass; it is not part of the
transport boundary.

### What the core API explicitly does not do

- It does not own a transport connection.
- It does not expose a per-call timeout on the public DTOs.
- It does not expose authentication, credentials, or tokens.
- It does not own a worker thread for the transport itself; only the
  `SyncWorker` pull/apply loop is provided, and it owns no transport
  state.
- It does not promise graceful shutdown of an in-flight peer call;
  `request_cancel()` is best-effort.

### Adapter-local extension pattern

A concrete socket-bound transport adapter ships as a separate pair of headers
(one client, one server) and is gated by its own build option
(`MDBXC_HTTP_SYNC`, `MDBXC_WEBSOCKET_SYNC`, ...). The adapter:

- implements or reuses `ISyncPeer` on the client side;
- wraps `SyncEngine::handle_pull()` / `handle_push()` on the server
  side, directly, through `HttpSyncServer`, or through `WebSocketSyncServer`;
- owns its own threading model (one acceptor thread, thread pool,
  per-connection thread, ...);
- owns its own timeout configuration;
- documents how its `request_cancel()` translates into the
  underlying transport's interrupt primitive.

The framework-neutral `HttpSyncPeer` / `HttpSyncServer` and
`WebSocketSyncPeer` / `WebSocketSyncServer` seams are part of v0.1. Concrete
server/client bindings remain separate optional integrations; the ready-made
Simple-Web bindings live under `sync/transports/simple_web/` and are not
included by the main sync umbrella header.

The Kurlyk HTTP client binding lives under `sync/transports/kurlyk/` and is
also excluded from the main sync umbrella header. It exists to validate that
HTTP client backends can be swapped at the `IHttpSyncClient` boundary without
changing `SyncEngine`, `HttpSyncPeer`, transport DTOs, or auth/rate-limit
middleware. On Windows/MinGW, its optional example can fetch a pinned
ready-made libcurl package; that fallback is a build-time convenience for the
example target, not a dependency of the sync core.

The `MDBXC_SIMPLE_WEB_HTTP_TRANSPORT`,
`MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT`, and `MDBXC_KURLYK_HTTP_TRANSPORT`
CMake options enable the optional backend dependency targets and their backend
smoke tests. The `MDBXC_HTTP_SYNC_EXAMPLE`,
`MDBXC_WEBSOCKET_SYNC_EXAMPLE`, and `MDBXC_KURLYK_HTTP_SYNC_EXAMPLE` options
only add repository examples on top of those backends; with
`MDBXC_BUILD_EXAMPLES=ON`, they still enable the matching backend for
compatibility with older example build commands. Application code should use
`MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT`,
`MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT`, and
`MDBXC_HAS_KURLYK_HTTP_TRANSPORT` when it needs conditional includes for
concrete backend headers. Consumers that wire the same third-party dependencies
manually may define the corresponding `MDBXC_HAS_*` macro themselves.

Installed packages export provider functions instead of exporting raw
FetchContent build-tree targets:

```cmake
mdbx_containers_simple_web_http_transport_provide(OUT_TARGET http_target)
mdbx_containers_simple_web_websocket_transport_provide(OUT_TARGET ws_target)
mdbx_containers_kurlyk_http_transport_provide(OUT_TARGET kurlyk_target)
```

The returned targets are
`mdbx_containers::simple_web_http_transport`,
`mdbx_containers::simple_web_websocket_transport`, and
`mdbx_containers::kurlyk_http_transport`. They link the core package target,
enable `MDBXC_SYNC_ENABLED`, fetch or find the optional third-party backend,
and propagate the matching `MDBXC_HAS_*_TRANSPORT` macro. This keeps installed
packages relocatable while preserving the same target-based usage model as the
source-tree examples and tests.

Production deployment details for TLS/WSS, token rotation, graceful shutdown,
structured logging, and offline dependency management are kept in
`guides/sync-transport-production.md`.

Request and trace ids are adapter-local metadata. HTTP bindings carry them in
`X-MDBXC-Sync-Request-Id` and `X-MDBXC-Sync-Trace-Id`; WebSocket bindings may
copy equivalent handshake or session metadata into
`WebSocketSyncRequestContext`. `SyncTransportTraceContext` and the
`*_sync_trace_context()` helpers expose those fields to observers without
adding them to `TransportMessageCodec` DTOs.

## Why `prune_up_to` uses cursor walk + `MDBX_NEXT`

MDBX has no batch "delete by key range" primitive. The supported pattern
for walking and deleting a range is:

```
cursor open
cursor get(MDBX_SET_RANGE, lo) -> k
while cmp(k, hi) <= 0:
    cursor_del(MDBX_CURRENT)
    cursor get(MDBX_NEXT) -> k
cursor close
```

After `cursor_del`, the cursor stays logically on the deleted position; the
next `MDBX_NEXT` advances to the next live record. The loop terminates
when the current key compares greater than `hi` (or when the cursor runs
out of records). This is the only documented way; alternatives either
don't exist or fail on the first record.

## Why `IdentityIndexValue` does not get an extra outer value prefix

Only the **key** is subject to MDBX bytewise uniqueness — different logical
keys can collapse to the same key bytes if the composite key is not encoded
unambiguously. That is why the identity-index key uses:

    u32 dbi_name_len le ‖ dbi_name bytes ‖ identity_key bytes

The **value** for a given key is single-valued and never participates in MDBX
key comparison. It is still a structured payload, so variable-size fields
inside the value (`storage_key`, `revision_key`) are length-prefixed where
needed for decoding.

Do not add an extra outer MDBX-value prefix around `IdentityIndexValue`;
the MDBX value length is already known from `MDBX_val::iov_len`.

When HLC or similar lands in v0.2, it goes in as opaque bytes inside
`revision_key`.

## Deferred to v0.2 with no open issue yet

- `meta_schema_version()` currently returns 1; bump rule + migration
  procedure not defined.
- Logical data migration policy for existing logical table contents and
  retained changelog entries after a schema-marker migration.
- Public sync API stability after the first external transport adapter.
- `PeerRegistry` for multi-peer fan-out — single peer per sync invocation
  in v0.1.

In v0.1 one capture/session commit atomically publishes an ordered envelope for
exactly one receiver; atomic multi-recipient fan-out is deferred. Moving that
single receiver to another replica requires logical-aware recovery first: the
new receiver must import the origin frontier before it can accept the next
globally sequenced event. Sending a new route to a fresh receiver fails closed
with an ordered-delivery gap.

Before the first external sync release, logical-store layouts and logical wire
codec versions do not carry a persistent compatibility or migration guarantee.
