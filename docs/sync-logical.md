# Logical And Ordered Sync

Logical sync is an opt-in, application-owned protocol for tables whose public
semantics cannot be represented safely as raw MDBX puts and deletes. It uses
an explicit schema marker and adapter-owned payloads. It is not part of the
normal raw `PullRequest` / `PushRequest` wire path.

Read [Sync Replication](sync.md) first. Russian version:
[sync-logical-RU.md](sync-logical-RU.md).

## Lifecycle

1. Choose a stable application `schema_id`, logical table kind, schema version,
   codec tags, and the complete set of owned DBI names.
2. Initialize or verify the schema marker through `SyncEngine`. Existing
   markers are verified, not recreated as an implicit recovery mechanism.
3. Construct matching adapters on the source and destination. Register the
   destination adapter with the destination `SyncEngine`.
4. Perform source writes through the adapter's typed capture session. A
   successful session commit publishes logical changes only after the local
   table mutation succeeds.
5. Deliver either a `LogicalChangeFrame` through an application protocol or an
   ordered delivery envelope when the adapter requires it.
6. Apply on the destination through the matching `SyncEngine` method. Marker
   validation and adapter preflight happen before adapter mutation; failures
   roll back the engine-owned transaction.

```mermaid
sequenceDiagram
    participant S as Source adapter session
    participant Schema as _mdbxc_sync_schema
    participant Wire as Application delivery
    participant E as Receiver SyncEngine
    participant A as Receiver adapter

    S->>Schema: verify registered schema
    S->>S: mutate local table and collect typed changes
    S-->>Wire: LogicalChangeFrame or delivery envelope
    Wire->>E: explicit apply call
    E->>Schema: verify persistent marker
    E->>A: preflight every change
    E->>A: apply in one MDBX transaction
```

The schema marker records the application schema id, kind, version, primary
DBI, and owned DBI set. It protects receivers from accepting a stale in-memory
adapter after a marker migration. Changing codec semantics, schema version, or
owned DBIs requires a new schema identity or an explicit compatible migration;
ordinary registration does not overwrite an existing incompatible marker.

## Direct Logical Frames

`LogicalChangeFrameCodec` serializes an explicit collection of logical
changes. `SyncEngine::apply_logical_frame_bytes()` applies it on the receiver.
This route is appropriate when the application already owns destination
routing, delivery order, and retry policy.

It is intentionally not a retry-safe ordered transport by itself. A frame has
no destination identity, global order, or durable replay identity. Do not use
it as an implicit substitute for raw pull/push or for the ordered-delivery
envelope described below.

The runnable [`sync_23_key_value_logical_frame.cpp`](../examples/sync_23_key_value_logical_frame.cpp)
example shows the complete capture, encode, decode, and apply path for a
`KeyValueTable` adapter.

## Ordered Logical Delivery

`KeyOrderedMultiValueTable` uses a different route because observable append
order is part of its API. A direct logical frame and unordered delivery are
rejected. The schema marker names one non-zero authoritative origin.

```mermaid
sequenceDiagram
    participant O as Authoritative origin
    participant Outbox as Durable ordered outbox
    participant D as Application dispatcher
    participant R as Replica SyncEngine
    participant State as Replay marker and frontier

    O->>Outbox: commit local change and envelope atomically
    D->>R: deliver envelope for the next origin sequence
    R->>State: validate destination, replay identity, and order
    R->>R: preflight and apply exact changes
    R->>State: commit marker and contiguous frontier
    R-->>D: acknowledgement
    D->>Outbox: acknowledge delivered envelope
```

The receiver treats a committed redelivery as a successful no-op. A gap or a
mismatched origin is rejected before table mutation. The sender outbox lets an
application retry delivery after process or transport failure without losing
the local ordered history that was committed with the envelope.

Changing the authoritative origin is an application-coordinated cutover. The
application must quiesce old capture, drain the old outbox, retain replay state
through its retry horizon, migrate the marker on every participant, and only
then enable the new origin. It is not automatic failover and it does not make
two origins concurrently valid for one ordered dataset.

## Implemented Adapter Contracts

| Adapter | Implemented typed contract | Boundary |
| --- | --- | --- |
| `KeyValueTableLogicalAdapter` | Explicit typed capture and apply. | Application owns frame delivery. |
| `KeyTableLogicalAdapter` | Explicit typed capture and apply. | Application owns frame delivery. |
| `VectorStoreLogicalAdapter` | Schema v1 add, erase, and clear across ids, embeddings, text, and metadata DBIs. Explicit IDs are validated before mutation. | One authoritative or externally serialized writer per collection. |
| `KeyMultiValueTableLogicalAdapter` | v1: insert, version-neutral batch `append()`, key erase, all-matching-value erase, clear. v2 adds exact-one erase and `reconcile()`. v3 adds bounded typed `erase_range()` expanded into exact key erasures. | Unordered multiset semantics; one writer or causally serialized destructive updates. |
| `KeyOrderedMultiValueTableLogicalAdapter` | v1 append-only ordered delivery. | One authoritative origin. |
| `KeyOrderedMultiValueTableDestructiveLogicalAdapter` | v2 exact append/erase by persistent element ID, bounded selector erasure, clear, and single-origin `replace_with()`. | One authoritative origin; baseline import, multi-origin history, and tombstone compaction are deferred. |

For `KeyMultiValueTable`, repeated equal pairs are a multiset: multiplicity is
preserved, but local iteration order of duplicates is not a cross-node
guarantee. Its direct table calls, raw capture, and unsupported bulk paths stay
local-only. For `KeyOrderedMultiValueTable`, the physical local duplicate
prefix is not a distributed identity; the ordered envelope is the source of
history order.

## Failure And Concurrency Rules

- Validation, planning, or encoding failures before mutation leave a typed
  capture session active unless the adapter documents a storage-integrity
  failure as session-fatal.
- Once a capture/apply failure occurs after mutation, the affected engine or
  session rolls back its MDBX transaction and refuses a later commit when its
  contract requires deactivation.
- There is no general multi-writer convergence contract for destructive logical
  updates. Use one authoritative writer for the affected logical dataset, or
  externally serialize conflicting updates before delivery.
- The logical stores are durable compatibility and delivery state. Do not edit
  `_mdbxc_sync_schema`, replay markers, ordered frontiers, or outbox records
  through application MDBX calls.

## Next Reading

- [Recovery and full snapshots](sync-recovery.md) explains why a raw complete
  snapshot cannot recover a database with persistent logical-sync state.
- [Sync table coverage matrix](../guides/sync-table-coverage.md) contains the
  exhaustive operation-level matrix and deferred boundaries.
