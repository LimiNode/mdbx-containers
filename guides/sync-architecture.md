# Sync Architecture Map

Use this guide to orient yourself before opening implementation headers or
choosing a sync API. It is a map of responsibilities and entry points; wire,
on-disk, and recovery invariants remain specified in
[`sync/DESIGN.md`](../include/mdbx_containers/sync/DESIGN.md).

[Русская версия](sync-architecture-RU.md)

## Choose the Pipeline First

Sync has two independent replication pipelines. They share node identity,
transactions, worker scheduling, and transport seams, but they do not convert
messages between one another.

```text
                            +---------------------------+
                            | Shared sync foundation    |
                            | Connection / transaction  |
                            | node identity / schema     |
                            | SyncEngine / SyncWorker    |
                            +-------------+-------------+
                                          /   \
                                         /     \
      RAW REPLICATION                     /       \  LOGICAL REPLICATION
                                         v         v
 +---------------------------+     +------------------------------+
 | Supported table mutation  |     | Bound-table or adapter       |
 | (physical DBI semantics)  |     | mutation (declared schema)   |
 +-------------+-------------+     +--------------+---------------+
               |                                  |
               v                                  v
 +---------------------------+     +------------------------------+
 | Raw capture accumulator   |     | Logical change frame         |
 +-------------+-------------+     +--------------+---------------+
               |                                  |
               v                                  v
 +---------------------------+     +------------------------------+
 | Durable raw changelog     |     | Receiver-neutral journal     |
 +-------------+-------------+     +--------------+---------------+
               |                                  |
               v                                  v
 +---------------------------+     +------------------------------+
 | Pull / push page          |     | Per-receiver route / outbox  |
 +-------------+-------------+     +--------------+---------------+
               |                                  |
               v                                  v
 +---------------------------+     +------------------------------+
 | Raw DBI apply             |     | Ordered logical delivery     |
 +---------------------------+     +--------------+---------------+
                                                    |
                                                    v
                                     +-----------------------------+
                                     | Registered adapter apply    |
                                     +-----------------------------+
```

Use the raw pipeline when physical DBI key/value replay is sufficient. Use the
logical pipeline only when a table's public semantics need a declared schema,
custom apply logic, ordering, or receiver-specific delivery. A logical frame
is never embedded in a raw `ChangeBatch`.

## Public Entry Points

Include the narrowest aggregate that owns the API being used.

| Need | Include | Owns |
| --- | --- | --- |
| Complete optional subsystem | `mdbx_containers/sync.hpp` | All sync domains. |
| Raw protocol DTOs and codecs | `mdbx_containers/sync/protocol.hpp` | Batches, cursors, snapshots, bounds. |
| Durable raw state | `mdbx_containers/sync/storage.hpp` | Changelog, origin, applied-cursor, LWW sidecars. |
| Raw capture lifetime | `mdbx_containers/sync/capture.hpp` | Capture sink, scope, accumulator, Connection hook. |
| Logical contracts and durable state | `mdbx_containers/sync/logical.hpp` | Schemas, frames, journal, delivery, recovery DTOs. |
| Table-bound logical adapters | `mdbx_containers/sync/adapters.hpp` | Table prerequisites and adapter implementations. |
| Engine and worker | `mdbx_containers/sync/engine.hpp` | Peers, engine, worker, in-process peers. |
| Transport-neutral HTTP/WS seams | `mdbx_containers/sync/transport.hpp` | Codec, middleware, policy, HTTP and WebSocket adapters. |
| Concrete optional backends | `mdbx_containers/sync/transports/*.hpp` | Backend-specific provider aggregates. |

Concrete headers under `sync/transports/...` are documented integration entry
points for applications that intentionally select a backend. Other headers
below `sync/` are internal leaves; include their owning aggregate instead.

## Layer Responsibilities

| Layer | Directory | Responsibility |
| --- | --- | --- |
| Shared primitives | `sync/core/`, `sync/common.hpp` | Identity, cancellation, conflict and observer contracts. |
| Raw protocol | `sync/protocol/` | Wire DTOs and validation for raw replication. |
| Raw persistence | `sync/stores/` | Metadata, changelog and apply state. |
| Capture | `sync/capture/` | Records local raw mutations in the committing transaction. |
| Logical state | `sync/logical/` | Schema references, logical frames, journal, route and replay state. |
| Logical adapters | `sync/logical/adapters/` | Table-specific capture and apply semantics. |
| Orchestration | `sync/engine/` | Pull/push, transactions, worker rounds, recovery coordination. |
| Transport | `sync/transport/` | Framework-neutral DTO adaptation, policy and observability. |
| Backend bindings | `sync/transports/` | Optional Simple-Web and Kurlyk integrations. |

`Connection` is the one intentional bridge from the storage core into sync.
`sync/connection_hooks.hpp` provides its logical capture hook and durable
binding prerequisites before `Connection` is defined. This header is an
internal seam, not an application include point.

## Header Ownership Rule

An internal leaf includes standard-library, third-party, and same-domain
implementation dependencies only. Its domain aggregate supplies cross-domain
prerequisites in a visible order. This preserves one-way dependency flow and
keeps supported aggregate headers self-sufficient without making every leaf a
standalone API.

## Find the Detailed Contract

| Question | Reference |
| --- | --- |
| Which table is supported and under what semantics? | [Sync table coverage matrix](sync-table-coverage.md) |
| Is a feature ready for v0.1 and what is deferred? | [Sync v0.1 readiness checklist](sync-v0.1-readiness.md) |
| How should production transport be secured and operated? | [Sync transport production notes](sync-transport-production.md) |
| What are the complete wire, storage, and recovery invariants? | [`sync/DESIGN.md`](../include/mdbx_containers/sync/DESIGN.md) |
| Which sync contract fits a dataset and writer model? | [`docs/sync-use-cases.md`](../docs/sync-use-cases.md) |
| What are the detailed multi-origin deployment patterns? | [`docs/sync-deployment-patterns.md`](../docs/sync-deployment-patterns.md) |
