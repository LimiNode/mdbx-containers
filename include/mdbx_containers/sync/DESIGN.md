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

- Public types in `include/mdbx_containers/sync/`:
  `Common`, `ChangeAccumulator`, `ChangeBatch`, `ChangeOp`, `Cancellation`,
  `CodecBounds`, `CodecFlags`, `ConflictPolicy`, `DirectSyncPeer`,
  `IdentityProvider`, `ISyncCaptureSink`, `ISyncPeer`, `Protocol`,
  `SyncCaptureScope`, `SyncCursor`, `SyncEngine`, `SyncWorker`.
- Five system stores under `include/mdbx_containers/sync/stores/`:
  `MetaStore`, `ChangeLogStore`, `OriginIndexStore`, `AppliedStore`,
  `IdentityIndexStore`.
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
  `KeyValueTable`.
- End-to-end replication tests cover `ValueTable`, `KeyValueTable`,
  `KeyTable`, `SequenceTable` insert/update/delete including empty serialized
  values, and `VectorStore` add/erase/rebuild through its public API.
- `ConflictPolicy::Reject` is the default. `ConflictPolicy::LastWriterWins`
  is declared for future logical-key conflict resolution; `SyncEngine`
  rejects it until a reliable conflict authority exists.
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
| `VectorStore` | Supported indirectly | Does not own a separate wire format. Its persistent writes go through `SequenceTable` and `KeyValueTable` member tables. Raw replication requires one authoritative or externally serialized writer per collection. Already-open instances refresh their RAM index lazily after completed remote apply when the connection sync-apply generation changes. |
| `AnyValueTable` | Not supported in v0.1 | Deferred until heterogeneous value type tags are part of the sync wire format. |
| `KeyMultiValueTable` | Limited logical adapter | Raw v0.1 capture remains unsupported. `KeyMultiValueTableLogicalAdapter` explicitly captures unordered insert, key erase, all-matching-value erase, and clear under one-writer or causally serialized updates. |
| `KeyOrderedMultiValueTable` | Append-only logical adapter | `KeyOrderedMultiValueTableLogicalAdapter` applies append-only changes through ordered delivery for one origin stream. Typed capture and destructive operations remain deferred. |
| `HashedKeyValueStore` | Not supported in v0.1 | Deferred until hash-index and identity-key mapping semantics are specified. |

Do not add `record_op()` paths for unsupported table types without first
updating this design document and adding round-trip replication tests for the
new wire-format semantics.

`VectorStore` collection names are validated instead of sanitized. Names must
be non-empty and contain only ASCII letters, digits, `_`, and `-`; unsupported
characters are rejected before internal DBI names are built. This prevents
different logical collections from collapsing to the same physical DBI names.

`VectorStore` sync is currently a raw physical replication path over its ids,
embeddings, text, and metadata DBIs. Replicas must agree on collection name,
vector metric, and embedding serialization. `VectorStore::add()` assigns ids
from local table state, so concurrent independent writers can collide and have
no conflict-resolution rule. The supported topology is therefore
leader/follower or application-serialized writers per collection. A future
multi-DBI logical adapter must introduce a global record identity scheme and
explicit ordering/conflict semantics; raw capture must not be presented as that
adapter.

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
capture session emits only the operations defined by this contract.

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
preservation under single-writer or causally serialized updates:

```text
for every serialized key and serialized public value:
    count(key, value) is preserved after replaying the same ordered operation history
```

General concurrent multi-writer convergence is not guaranteed by the operation
set below. Destructive operations are not commutative with concurrent inserts:
`EraseAllValues`, `EraseKey`, `EraseRange`, and `Clear` can produce different
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
| `EraseRange` | serialized inclusive key range | Remove every physical pair whose key is in the range. |
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
model. Its explicit typed capture session supports `InsertOne`, `EraseKey`,
`EraseAllValues`, and `Clear`. It does not capture raw table calls, `append()`,
`reconcile()`, `erase_range()`, or `EraseOneValue`. Those paths remain local
until a later extension can preserve their exact multiset semantics. This is
not partial raw capture: callers opt into the typed session and only its
documented methods publish logical changes.

Implementation phases:

1. Implement adapter payload encoding and preflight through the existing
   logical schema marker and adapter registry. A receiver without the adapter
   must reject the change before table mutation.
2. Implement apply helpers that call the public/logical multivalue semantics:
   assign a fresh local duplicate prefix on `InsertOne`; match stripped public
   values for `EraseOneValue` and `EraseAllValues`; use public key ordering for
   range erasure.
3. Add the scoped typed capture session only for methods that map directly to
   the first implementation operations. Raw capture stays disabled.
4. Add negative compatibility tests: receivers without the matching adapter or
   persistent marker must fail before table mutation.
5. Add round-trip tests before documenting the wrapper as supported. Tests must
   compare logical multiset counts, not raw duplicate bytes or local iteration
   order.

`append()` can be represented as a sequence of `InsertOne` operations in the
same local batch. `erase(key, value)` should emit `EraseAllValues`.
`reconcile()` should emit one `EraseOneValue` per surplus occurrence and one
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
that operation and may use the same capture method. `erase`, `erase_at`,
`clear`, and `replace_with` remain local-only until a persistent element
identity, delete/tombstone semantics, and a new round-trip contract are
specified.

Required tests before enabling capture:

- codec tests proving unknown multivalue operation bits/subtypes are rejected by
  older or capability-limited decoders;
- sink-level tests for append and its `insert` alias;
- ordered-delivery round-trip tests that preserve repeated identical `(key,
  value)` pairs and per-key append order for one origin stream;
- negative tests proving direct logical frames and unordered delivery reject
  ordered-table changes before adapter callbacks;
- rollback, retry, and restart tests for append-only capture and ordered
  delivery;
- multi-writer same-key tests that prove independent origins are not claimed to
  converge unless a future explicit conflict policy is selected;
- pagination and restart tests, including persisted applied cursor behavior;
- idempotent replay checks for already-applied batches;
- negative tests proving unsupported `AnyValueTable` and `HashedKeyValueStore`
  still emit no `ChangeOp` until their own wire formats are defined.

## Planned before v0.1 release (NOT YET implemented)

- Full export/import via `seq=0, BATCH_HAS_MORE` chunks for empty replicas.

## What v0.1 does NOT cover (deferred to v0.2)

For the table-by-table support status, capture coverage, and negative test
anchors, see the
[Sync table coverage matrix](/guides/sync-table-coverage.md).

- `HashedKeyValueStore` — internal hash index layout complicates the wire
  format; deferred until an explicit identity-mapping scheme lands.
- `KeyMultiValueTable` — DUPSORT duplicate values need the unordered multiset
  model described above before capture can be enabled.
- `KeyOrderedMultiValueTable` — local ordered storage exists, but ordered
  distributed histories need the explicit sync wire identity described above
  before capture can be enabled.
- `AnyValueTable` — heterogeneous values need type-tag propagation on the wire.
- `IdentityProvider` integration in `BaseTable` — declared in v0.1, no
  write path until HashedKeyValueStore.
- Specialized table types not listed in the implemented scope are not treated
  as sync-covered without explicit wire-format design and round-trip tests.
- Automatic remap of physical `storage_key` from logical `identity_key`.
- HLC or other production authority for logical-key `LastWriterWins`.
  `time_unix_ns` is metadata only, not a reliable conflict authority. The
  current apply path does not maintain enough identity-index conflict state to
  use `LastWriterWins`, so `SyncEngine` rejects that policy.
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
later non-contiguous batch. Until the reserved full snapshot protocol exists,
callers must provision a fresh replica or use an application-defined snapshot
outside sync v0.1.

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

### `_mdbxc_identity_index` (IdentityIndexStore) — declared in v0.1, write path deferred

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
Real removal is explicit via `erase()`.

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

`LogicalOutboxStore` is the sender-side durable foundation for a future ordered
delivery protocol. `SyncEngine::enqueue_logical_delivery()` persists an envelope
in `_mdbxc_logical_outbox` and allocates a monotonic `origin_sequence` per
destination database, not globally across unrelated destinations. Its MDBX entry
keys contain the destination and a big-endian sequence suffix, so
`pending_logical_deliveries()` reads the stream in numeric order. The generated
frame id is stable for that stored sequence, and allocating a frame, updating the
next sequence, acknowledging a prefix, and deleting acknowledged entries are all
transactional. A rollback therefore neither consumes a sequence nor leaves a
queue entry behind. `_mdbxc_logical_outbox` is created during the normal
committed sync-system initialization lifecycle, so deployments need one
additional named-DBI slot in `Config::max_dbs` compared with a layout that did
not use the outbox.

`acknowledge_logical_deliveries()` currently exposes only a local cumulative
frontier and deletes its acknowledged outbox prefix. It does not establish a
wire acknowledgement, receiver-side ordering, capability negotiation, or retry
scheduling. The older `apply_logical_delivery_envelope()` API remains intentionally
unordered and is not implicitly redirected through this outbox.

`LogicalDeliveryProtocol.hpp` reserves a separate versioned wire boundary for
that future exchange. Its `Hello` message carries optional capability bits; an
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
peers that implement only the envelope-only delivery virtual method remain on
the conservative contract. No existing HTTP, WebSocket, or raw pull/push
endpoint advertises or accepts this protocol yet, so current transports remain
backward-compatible raw-sync-only.

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
checks destination identity and negotiated `OrderedDelivery` before sending its
outbox prefix. A negotiated cumulative success is bounded by the sender's
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
- `ILogicalTableAdapter::apply()` mutates user tables only after every logical
  change in the same apply transaction passed preflight.
- `ILogicalTableAdapter::primary_dbi()` names the stable primary physical DBI
  for the logical schema. It defaults to the only affected DBI for
  source-compatible single-DBI adapters. Multi-DBI adapters must override it
  explicitly, and the returned DBI must be included in `affected_dbis()`.
- `LogicalTableRegistry` is a non-owning lifecycle registry keyed by
  `LogicalSchemaRef::schema_id`, but dispatch validates the full
  `(schema_id, kind, schema_version)` tuple and rejects non-zero reserved
  logical flags before any adapter callback.
- `LogicalTableRegistry::preflight_then_apply()` runs validation for every
  change before any preflight, then runs every preflight before any apply. If
  an adapter reports failure from `apply()` or throws after mutating data, the
  helper returns failure and the caller that owns the MDBX write transaction
  must abort that transaction; the registry cannot roll back a transaction it
  does not own.

`adapters/KeyValueTableLogicalAdapter.hpp` and
`adapters/KeyTableLogicalAdapter.hpp` provide the first concrete adapter
helpers. They translate typed table operations into adapter-owned logical
payloads and apply them through
`SyncEngine::apply_logical_changes()` or
`LogicalTableRegistry::preflight_then_apply()` inside a caller-owned write
transaction. These helpers prove the adapter contract and payload shape for
simple tables, but they are not yet connected to the automatic pull/push wire
pipeline.
`KeyValueTableLogicalAdapter` supports upsert/delete/clear and
`KeyTableLogicalAdapter` supports insert/delete/clear. Both expose an opt-in
`LogicalCaptureSession`. The session owns a writable transaction, suppresses
raw capture for its typed writes, buffers logical changes privately, and copies
them to the caller only from `commit(out)`. Rollback, destruction, or commit
failure discards the pending logical changes. An exception after physical
mutation, outbox enqueue, or native commit processing begins requests
transaction rollback and deactivates the session, so later `commit()` or
`commit_to_outbox()` calls are rejected. Preparation or encoding failures
before transaction mutation leave the session active.
Session construction validates
the adapter against the persistent schema marker in the same writable
transaction, before any local mutation can be performed. For `KeyTable`, only
successful membership changes are captured: inserting an existing key or
erasing an absent key leaves the pending logical frame unchanged; an explicit
clear request is captured even when the table is already empty.

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

The current `KeyValueTableLogicalAdapter` capture session is intentionally
manual and opt-in; it does not replace the normal `KeyValueTable` API or claim
automatic coverage for every mutating table method.

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
- Version: u16 little-endian, currently `4`.
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
  fresher cursor, while DBI flag conflicts and unsupported full snapshots are
  permanent until the caller changes behavior. `SnapshotRequired` means the
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

The reserved `seq=0, BATCH_HAS_MORE` full export/import format remains planned
for v0.1 and is not the current cold-replica implementation.
`PullRequest::request_full_snapshot=true` is rejected explicitly until that
format is implemented. In v0.1 this is a sync-level protocol rejection carried
as `PullResponse{ok=false, error=..., error_code=UnsupportedFullSnapshot}`; it
does not produce a transport retry hint because the server returned a valid
sync response rather than a transport failure.
If changelog pruning removed entries needed by the requester's cursor,
`handle_pull()` returns `SnapshotRequired` with no batches. This is also a
valid sync response, not a transport failure.
`SyncWorkerPermanentFailurePolicy` is transport-only; workers still expose
sync-level response errors through round results, stage events, and status
snapshots without treating them as permanent transport failures.

### Deferred full snapshot protocol

The future full snapshot protocol must be explicit rather than another spelling
of retained changelog replay. The reserved request shape is
`PullRequest::request_full_snapshot=true`; responders that implement it should
return snapshot chunks only when the caller requested that mode, never as an
implicit fallback from a normal incremental pull.

Required protocol properties before enabling the flag:

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
  effect. The manifest must say whether the snapshot replaces the complete
  database scope or only the listed DBIs; complete replacement may clear/drop
  receiver-only user DBIs only when the caller opted into that scope. Otherwise
  receiver-only DBIs are preserved or the import fails closed before data apply.
- Snapshot chunks must be distinguishable from ordinary changelog batches.
  The reserved shape is `ChangeBatch{seq=0, batch_flags=BATCH_HAS_MORE...}`
  with a snapshot-specific flag or versioned envelope added before release.
  Ordinary local changelog batches keep `seq > 0`.
- A chunk carries physical `ClearTable` / `Put` operations for user DBIs only.
  It must not export or overwrite reserved `_mdbxc_` sync metadata DBIs through
  the normal user-operation path.
- Snapshot apply is a replacement operation for the selected database content:
  the receiver must clear each exported user DBI before applying that DBI's
  first snapshot entries, then apply later chunks idempotently or reject
  ambiguous resume attempts.
- Metadata bootstrap is separate from user data import. After the snapshot data
  is committed, the receiver records applied cursors consistent with the
  responder's advertised changelog tail so the next round can continue through
  ordinary incremental pull.
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
  advanced applied cursor. Either each chunk is independently resumable with
  explicit snapshot state, or the initial implementation must require a fresh
  replica directory and fail closed on interruption.
- `SnapshotRequired` remains the incremental-pull recovery signal. It tells the
  caller that retained changelog replay cannot satisfy the request; the caller
  may then make a separate `request_full_snapshot=true` request once this
  protocol exists.

Until these details are implemented and covered by round-trip tests,
`request_full_snapshot=true` remains a permanent sync-level rejection.

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

- no local MDBX transaction is held while waiting in `ISyncPeer::pull()`;
- no local MDBX transaction is held during idle or backoff sleeps;
- pulled pages are applied through `SyncEngine::handle_push()`, so each page
  uses one short local write transaction;
- stop requests cancel the active `PullRequest::cancel_token` and call
  `ISyncPeer::request_cancel()` at most once for each observed in-flight
  peer pull call, and a page returned after stop was requested is not applied;
- a stop request recorded before peer-call activation prevents the next pull
  call from starting;
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
  (`round`, `pull`, `apply`, `backoff`), page application, round completion,
  and backoff entry synchronously on the thread that runs the sync round;
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
