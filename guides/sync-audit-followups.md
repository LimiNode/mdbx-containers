# Sync Audit Follow-ups

This note records the follow-up order from the repository-wide sync audit at
`main` commit `60a5b32479f2d69a78ec9f9e404ab717fb59e92c`. Keep the fixes split
into small PRs so behavior, storage format, and build hygiene remain reviewable.

## Completed Audit Fixes

- PR #150 hardened reserved `_mdbxc_` DBI handling for public table names and
  incoming sync apply operations.
- PR #97 added standalone public header smoke coverage.
- PR #128 exposed transport backend feature macros.
- PR #131 covered installed transport provider targets.
- PR #152 tightened the `SyncWorker` stop-before-apply boundary.
- PR #154 rejected foreign external transactions before table, sync engine,
  sync store, or accumulator code can use handles from another environment.
- PR #155 made unsupported sync protocol modes explicit:
  `PullRequest::request_full_snapshot=true` is rejected, and
  `ConflictPolicy::LastWriterWins` cannot be selected until the apply semantics
  exist.
- Current status: narrow LWW v1 is now available through
  `VersionedKeyValueTable` with application-provided source versions. Its DBI
  is durably registered and fail-closed for direct raw writes; ordinary raw
  DBIs remain supported on the same engine. The historical PR #155 statement
  remains applicable to ordinary raw operations without a versioned contract.
- PR #156 renamed the changelog pull internals enough to clarify that empty
  cursors replay retained changelog history rather than exporting a database
  snapshot.
- PR #158 fixed signed `MDBX_INTEGERKEY` range/cursor ordering by storing signed
  integral keys through an order-preserving unsigned rank.
- PR #159 made integral key storage canonical for supported integer-key widths
  and documented the storage-format break for development DBIs.
- PR #160 canonicalized floating-point zero keys and rejected NaN keys.
- PR #161 added fast-math floating-key regression coverage.
- PR #162 added the sync table coverage matrix.
- PR #163 refreshed this follow-up ledger after the first audit sequence.
- PR #164 added wrapper-specific `clear()` capture and replication coverage.
- PR #165 removed the misleading public `pull_full_snapshot()` API name and
  documented retained changelog replay semantics.
- PR #166 added machine-readable sync response error metadata for permanent
  protocol rejections.
- PR #167 detects cursors older than retained changelog history and reports
  `SnapshotRequired`.
- PR #168 rejects invalid `VectorStore` collection names before building
  internal DBI names.
- PR #169 exposed `Connection::sync_apply_generation()` and made already-open
  `VectorStore` instances lazily rebuild after remote apply.
- PR #170 added concrete binding body-limit knobs for Simple-Web HTTP,
  Simple-WebSocket, and Kurlyk HTTP.
- PR #171 serialized remote apply commits with cache-backed `VectorStore`
  operations through a connection apply/read barrier.
- PR #172 capped and evicted HTTP fixed-window rate-limit buckets.
- PR #173 added whole-exchange deadlines for the Simple-WebSocket sync client.
- PR #174 supports installed packages that provide lowercase `mdbxConfig`.
- PR #175 split pull page byte budgets from per-batch byte budgets.
- PR #180 added connection-level remote apply observer hooks for cache
  invalidation, metrics, and logging after successful remote apply commits.
- PR #181 added `SyncWorkerGuard` as an RAII helper for one background worker
  session.
- PR #182 added an application-facing example that combines
  `SyncCaptureScope`, `SyncWorkerGuard`, and `ISyncApplyObserver`.
- PR #184 added `SyncNodeSession` for one common application wiring shape:
  capture attachment, worker session lifetime, and optional apply observer
  registration.
- PR #185 extended `ISyncApplyObserver` events with affected DBI names for
  more precise cache invalidation.

## Current PR Sequence

No active audit PR sequence is queued in this ledger.

## Later Medium-Risk Follow-ups

- Framework-level pre-buffer limits: where the concrete HTTP/WebSocket library
  supports it, configure request, response, and frame caps before the complete
  payload is retained in memory. PR #170 added binding-side limits after the
  framework has surfaced the payload.
- Table-filtered apply observer subscriptions: `ISyncApplyObserver` now reports
  affected DBI names, but every observer still receives every committed remote
  apply event. Add registration-side filters only if more cached wrappers need
  narrower callback delivery.
- Higher-level transport helpers: after the node ergonomics helper, reduce
  repeated setup code around concrete transport peer construction, identity
  policy, retry/backoff settings, and common production policies.
