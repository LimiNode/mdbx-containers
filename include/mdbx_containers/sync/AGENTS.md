# Sync Instructions

These instructions apply to sync capture, stores, protocols, logical adapters,
recovery, workers, and transport bindings under this directory.

## Required Context

Start with `DESIGN.md`, then load only the guide matching the changed contract:

| Surface | Guide |
| --- | --- |
| Supported table capture/apply behavior | `guides/sync-table-coverage.md` |
| Architecture and dependency direction | `guides/sync-architecture.md` |
| Recovery, readiness, or remaining scope | `guides/sync-v0.1-readiness.md` |
| Selective replication | `guides/sync-selective-replication-design.md` |
| Production transports and middleware | `guides/sync-transport-production.md` |
| Audit history and deferred follow-ups | `guides/sync-audit-followups.md` |

Each listed user guide has a `-RU.md` counterpart. Update both when the
normative contract changes.

## Core Invariants

- Raw, versioned, logical, ordered, and selective paths have distinct authority
  and conflict contracts. Do not infer one path's semantics from another.
- Raw global history remains complete. Selective history is an additional
  atomic projection, not a replacement for the global batch.
- A selective v1 scope has one designated local writer. Reject a foreign local
  mutation before commit; incoming full-global raw apply remains allowed and
  must not turn a replica into a scoped relay.
- Publish capture records, schema/order/replay state, outbox records, and user
  mutations in the same MDBX transaction required by their contract.
- Do not advance progress, cursor, frontier, or acknowledgement before the
  corresponding state is durably committed.
- Optional sync stores must not be created or consume a named-DBI handle merely
  to determine that optional state is absent.
- Validate environment ownership before using caller-supplied `MDBX_txn*`.
- Wire and durable format changes require an explicit version, bounded decode,
  malformed-input coverage, and a compatibility or migration decision.
- Recovery is state replacement plus cursor/bootstrap state, not blind resend.
  Source outbox state does not become the receiver's local outbox.

For an invariant, document the forbidden path, the supported alternative, and
the regression that proves it.

## Test Selection

Use CTest and run relevant targets in C++11 and C++17:

| Change | Minimum targets |
| --- | --- |
| Raw capture or table apply | `test_sync_capture`, `test_sync_replication` |
| Engine, cursor, snapshot, recovery, selective scope | `test_sync_engine`, `test_sync_replication` |
| Stores or durable codecs | `test_sync_stores` and the affected codec/protocol target |
| Logical adapters/delivery | Adapter-specific target plus `test_sync_engine` |
| Public sync include surface | `header_sync_umbrella_test` and affected domain umbrella |
| HTTP/WebSocket/Kurlyk transport | Matching provider/transport target and middleware tests |

Run the full sync-related CTest subset when a shared protocol, storage, or
capture seam affects more than one row.
