# Sync v0.1 Readiness Checklist

This checklist records the current sync surface after the transport, worker,
and example hardening pass. It is a release-readiness map, not a stability
promise: sync remains experimental and opt-in through `MDBXC_SYNC_ENABLED=1`.
For the table-by-table support status, capture paths, and deferred wrapper
rules, see [Sync table coverage matrix](sync-table-coverage.md).

## Ready For v0.1 Use

- Supported table capture paths are explicit: `KeyValueTable`, `KeyTable`,
  `ValueTable`, and `SequenceTable` writes are captured when a
  `ThreadLocalChangeAccumulator` is attached to the writing `Connection`;
  `SyncCaptureScope` provides RAII attach/restore for write phases.
- `VectorStore` is covered indirectly through its internal `SequenceTable` and
  `KeyValueTable` members. This is raw physical replication only: use one
  authoritative or externally serialized writer per collection. It is not a
  multi-writer logical `VectorStore` contract.
- Standalone writes become standalone sync batches; an explicit transaction
  spanning multiple supported tables becomes one local atomic batch.
- Reads, scans, vector search, and other non-mutating APIs are not captured.
- Remote apply uses `SyncEngine::handle_push()` and commits one pulled page per
  local transaction.
- `SyncWorker` owns the background pull loop, pagination, cancellation tokens,
  observer callbacks, retry backoff, and optional `Retry-After` backoff hints.
- `SyncWorkerGuard` is available when an application wants one RAII-owned
  background worker session instead of a manual `start()` / `stop()` pair.
- `SyncNodeSession` is available for one common application wiring shape:
  attach capture, start one existing worker, and register an optional remote
  apply observer for the session lifetime.
- `SyncWorker::status()` exposes a thread-safe snapshot for polling UIs,
  health endpoints, and structured logging code that do not subscribe to
  observer callbacks.
- `Connection::add_sync_apply_observer()` provides a post-commit remote apply
  hook for cache invalidation, metrics, and structured logging. Events include
  the unique affected DBI names in first-seen order.
- HTTP and WebSocket framework-neutral seams use `TransportMessageCodec`; DTOs
  do not carry bearer tokens, cookies, remote addresses, request ids, or trace
  ids.
- Ready-made optional backends exist for Simple-Web HTTP, Simple-WebSocket, and
  Kurlyk/libcurl HTTP through feature-gated provider targets.
- Installed-package smoke tests cover exported transport provider targets and
  provider argument validation.
- Negative wire/transport tests cover malformed DTOs, oversized payloads,
  response/request mixups, auth/policy rejection, retry classification, and
  selected cancellation paths.

## Operational Contracts To Preserve

- `SyncWorker`, `SyncEngine`, `ISyncPeer`, and table objects are caller-owned;
  they must outlive callbacks and in-flight transport calls that can reference
  them.
- `SyncWorker` lifecycle methods `start()`, `stop()`, `join()`, and
  `run_once()` are caller-serialized. State and diagnostics readers are
  thread-safe.
- Transport cancellation is best-effort. `request_stop()` cancels the active
  token and calls `ISyncPeer::request_cancel()`, but shutdown can still wait
  for a transport that ignores cancellation.
- `SyncTransportRetryHint` is advisory. `available=false` means the peer did
  not classify the failure. `available=true, retryable=false` means the peer
  classified the current transport failure as permanent.
- `SyncWorker` keeps retrying permanent transport hints by default for backward
  compatibility. Set `SyncWorkerPermanentFailurePolicy::StopWorker` when a
  permanent hint should stop the background loop in `Failed`.
- Authentication, DB allow-list decisions, request ids, trace ids, rate-limit
  headers, HTTP status, and WebSocket close codes remain adapter-local
  metadata.
- `PullRequest::requester` and `PushRequest::sender` still need to match the
  authenticated transport identity before a production endpoint dispatches to
  `SyncEngine`.

## Deferred Table Work

These table families intentionally emit no `ChangeOp` in v0.1:

- `AnyValueTable`, until a wire-level type tag and compatibility policy is
  specified.
- raw `KeyMultiValueTable` capture, bulk/range operations, and
  general multi-writer destructive convergence. The explicit unordered logical
  adapter schema v1 covers insert, key erase, all-matching-value erase, and
  clear; schema v2 additionally covers exact-one erase and typed `reconcile()`.
  Schema v3 adds bounded typed range erasure under the same one-writer or
  causally serialized update contract; capture expands the selected keys into
  existing `EraseKey` logical changes before local mutation.
- `KeyOrderedMultiValueTable` raw capture, replace, baseline import, and
  multi-origin histories. Schema v1 remains append-only. Schema v2 supports
  logical `AppendElement` and exact `EraseElement` by persistent element id,
  plus bounded `erase_at`, key/value erase, and clear capture through one
  authoritative ordered origin.
- `HashedKeyValueStore`, until the relationship between logical key bytes,
  physical storage keys, and hash-index entries is specified.

Do not add `record_op()` calls to these tables until their wire format and
round-trip tests exist. A partial capture path is worse than no capture because
it can make replication appear successful while logical state diverges.

The raw `handle_push()` transport path remains separate from explicit logical
delivery. The logical core uses the following contracts:

- `_mdbxc_sync_schema` is a persistent compatibility marker, not an apply path.
- `LogicalChange` payloads are opaque and are not serialized by the current
  `ChangeBatchCodec`.
- `LogicalTableRegistry` defines the two-phase preflight/apply contract,
  including full schema tuple validation before adapter callbacks.
- `SyncEngine::apply_logical_changes()` owns the MDBX write transaction around
  explicit logical apply and aborts it if an adapter reports failure or throws
  after any mutation. The transport `handle_push()` path still applies raw DBI
  operations only.
- `KeyValueTableLogicalAdapter` and `KeyTableLogicalAdapter` are explicit
  apply helpers with opt-in typed capture sessions. Neither is connected to
  the transport pull/push path yet; callers own logical frame delivery.
- `KeyMultiValueTableLogicalAdapter` follows the same explicit logical-frame
  path. Schema v1 provides unordered insert, key erase, all-matching-value
  erase, and clear; schema v2 adds exact-one erase and typed `reconcile()`.
  It does not enable raw `ChangeOp` capture for the table wrapper.
- `KeyOrderedMultiValueTableLogicalAdapter` applies schema-v1 append-only
  changes only through ordered delivery for one origin stream.
  `KeyOrderedMultiValueTableDestructiveLogicalAdapter` applies schema-v2
  `AppendElement` and exact `EraseElement` changes with persistent element
  identity and tombstones; its typed capture session atomically commits local
  mutations and an ordered outbox envelope. Bounded `erase_at`, key/value
  erase, and clear capture resolve canonical logical-codec selectors to exact
  ids with separate candidate and inspected-record bounds. Replace and other
  broad state construction remain separately deferred. Transferring the
  authoritative origin is an
  application-coordinated schema-marker cutover; it is not automatic failover
  and requires old-outbox drain plus replay-marker retention through the chosen
  retry horizon.
- Logical delivery replay markers can be pruned through a persisted per-origin
  watermark. The watermark DBI is created lazily on the first pruning call, so
  deployments that use pruning must reserve one additional named-DBI slot in
  `Config::max_dbs`; older layouts without it remain readable with a zero
  watermark. Callers must advance it only from an external delivery/acknowledge
  protocol that rules out later unseen frames at or below the boundary; the
  engine does not supply ordering, buffering, or acknowledgements itself.

Until a causal context or another conflict model is implemented, future logical
table support should document either one authoritative writer for the affected
logical dataset or application-level serialization of conflicting writes.
General concurrent multi-writer convergence must not be claimed for deferred
tables.

## Suggested Next PRs

- Add optional table identity filters on top of the affected DBI names already
  reported by `ISyncApplyObserver`, if more cached wrappers need narrower
  subscriptions.
- Extend logical-frame capability negotiation only when a new adapter requires
  a compatibility distinction beyond the existing schema marker and adapter
  registry fail-closed checks.
- Integrate `LogicalTableRegistry` with `SyncEngine::handle_push()` only after
  logical changes can be parsed separately from raw DBI operations.
- Extend `KeyMultiValueTable` logical capture only after every added bulk or
  reconcile operation has explicit multiset replay semantics and round-trip
  coverage. Raw capture remains disabled.
- Implement the deferred full snapshot protocol before treating
  `SnapshotRequired` as automatically recoverable by sync itself.
- Define explicit conflict/CRDT semantics before claiming general concurrent
  multi-writer convergence for `KeyMultiValueTable`.
- Design `replace_with()`, baseline import, and multi-origin histories for
  schema v2 separately; none is implied by the implemented bounded erasure.
- Evaluate whether any `KeyMultiValueTable` framing ideas carry over to
  `AnyValueTable` and `HashedKeyValueStore`.

## Validation Baseline

Before declaring a sync-facing change ready, run the narrow affected tests in
both C++11 and C++17. For shared sync headers and transport contracts, include:

```text
header_sync_umbrella_test
header_sync_transport_umbrella_test
test_transport_middleware
test_http_transport
test_websocket_transport
test_sync_worker
test_sync_replication
```

For benchmark or backend-provider changes, also run the dedicated benchmark,
installed-package, and backend smoke jobs or their local equivalents.
