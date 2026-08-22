# Selective Replication Design

This document specifies the proposed contract for replicating a selected set
of raw user DBIs. It is a design for a later implementation, not a v0.1 API.
Current table-filtered apply observers filter local callback delivery only;
they do not filter capture, transport, apply, snapshots, or retention.

Read [Sync recovery and full snapshots](../docs/sync-recovery.md) and the
[sync table coverage matrix](sync-table-coverage.md) first. Russian version:
[sync-selective-replication-design-RU.md](sync-selective-replication-design-RU.md).

## Goal And Boundary

A replication scope lets an application replicate an explicit, stable set of
named user DBIs independently of the complete raw database. A scope is useful
when one receiver needs only particular tables, or when independent receiver
groups own different table sets.

It is not a row, key-range, predicate, tenant, or callback filter. A DBI is
either wholly inside one raw replication scope or outside selective raw
replication. Table-level filtering of `ISyncApplyObserver` remains an
invalidation convenience and does not imply delivery isolation.

The first scoped protocol is limited to raw supported table DBIs. It excludes:

- reserved `_mdbxc_` DBIs;
- a DBI owned by a logical schema, ordered delivery, or a logical adapter;
- a `VersionedKeyValueTable` registered DBI;
- a DBI already owned by another active raw replication scope.

Logical datasets need their own scope semantics because their state includes
schema, replay, ordering, and receiver-specific delivery records. They must
not be copied as a raw subset.

## Required Invariants

1. **Scope identity is durable and explicit.** `ScopeId` is an opaque,
   application-assigned, non-empty canonical byte string. It is not derived
   from a DBI list, a process-local DBI handle, or a receiver route.
2. **Membership is immutable for a scope identity.** A durable descriptor maps
   one `ScopeId` to a sorted, unique manifest of named DBIs and their required
   DBI flags. The source and receiver compare the complete descriptor before
   any mutation.
3. **One raw DBI has one replication authority.** During capture and apply, a
   scoped DBI cannot also enter the existing unscoped raw changelog. A write
   transaction that changes DBIs from different raw authorities fails closed;
   the implementation must never split one caller transaction into unrelated
   replicated commits.
4. **Progress is per scope and origin.** The existing `_mdbxc_applied` cursor
   remains the progress record for complete raw replication only. Scoped
   progress is keyed by `(ScopeId, origin_node_id)` in a separate durable
   store, and advances only after a contiguous scoped batch commits.
5. **Retention is per scope.** A source retains and prunes scoped history using
   the scoped cursor contract. A receiver behind that retained history gets a
   scope-specific `SnapshotRequired`; it must not consume later batches.
6. **A scoped baseline is atomic.** A completed baseline replaces only the
   descriptor manifest and writes its scoped per-origin tail in the same MDBX
   transaction. It neither writes nor validates the global raw cursor.
7. **Descriptor mismatch fails closed.** Different membership, DBI flags,
   descriptor revision, source `DbId`, scope identity, or immutable snapshot
   metadata invalidates the request/session. The receiver does not attempt a
   best-effort intersection.

## Descriptor And Membership Evolution

The implementation should persist a `ScopedReplicationDescriptor` containing:

```text
ScopeId
descriptor_revision
sorted unique [(dbi_name, dbi_flags)]
```

`descriptor_revision` is a source-issued, monotonically increasing value for
diagnostics and snapshot-session validation; it is not a substitute for the
full manifest comparison.

Membership does not change in place. Adding, removing, renaming, or changing
the flags of a DBI creates a new `ScopeId` and descriptor. The application must
drain or retire the old scope according to its retention policy, bootstrap the
new scope, and only then route receivers to it. This prevents a receiver from
silently interpreting a cursor for `{orders}` as a cursor for
`{orders, invoices}`.

An application may intentionally configure two immutable scopes with different
DBI sets. They must remain disjoint. A table move is therefore a controlled
cutover, not an automatic membership update.

## Capture, Wire, And Apply

Scoped replication needs a protocol family separate from existing raw
`PullRequest`, `PushRequest`, `ChangeBatch`, and `FullSnapshotChunk`. Extending
those DTOs with an optional DBI filter would make a global cursor appear valid
after only a subset of its operations was applied.

The proposed family has these conceptual records:

```text
ScopedPullRequest     = DbId + requester + ScopeId + scoped cursor
ScopedPullResponse    = descriptor + contiguous scoped batches or error
ScopedChangeBatch     = ScopeId + origin + scope-local sequence + operations
ScopedSnapshotRequest = ScopeId + empty scoped cursor
ScopedSnapshotChunk   = immutable descriptor + scoped source tail + page data
```

The concrete codec version, capability names, and public C++ types are deferred
to the implementation PR. A peer without the scoped capability rejects the
request; it must not silently fall back to complete raw pull/push.

Every operation in `ScopedChangeBatch` names a DBI in the descriptor manifest.
The source emits a batch only for a transaction whose captured changes all
belong to that same scope. The receiver validates the descriptor and every DBI
before opening its write transaction, then applies the batch and advances the
matching scoped cursor atomically. Duplicate, gap, foreign-origin, and
out-of-scope batches fail closed under the same contiguous-delivery principle
as the existing raw cursor.

Unscoped raw replication continues unchanged for DBIs with no scope owner.
The first implementation must define a capture-time authority lookup, rather
than filtering the global changelog after the fact.

## Scoped Baseline And Resume

The scoped baseline exists only for recovery of one scope. It is not an
extension of `ManifestOnly`, and `ManifestOnly` remains a manual physical
replacement tool with no replication-progress effect.

For a scoped `SnapshotRequired` the source captures, in one stable read
transaction:

- the immutable descriptor and all DBI data in its manifest;
- the scope-local per-origin source tail; and
- an opaque snapshot session id and immutable page-zero metadata.

The receiver validates and stages all pages before mutating live DBIs. On the
final page it replaces the descriptor manifest, writes scoped progress equal to
the captured tail, and discards staging in one transaction. DBIs outside the
scope and `_mdbxc_applied` remain unchanged.

Persisted resume, when added, must key staging by at least `ScopeId`, source
`DbId`, and snapshot id. It may resume only a session whose descriptor and
page-zero metadata still match. Starting a different scoped baseline for the
same `ScopeId`, disabling persisted staging, cancellation, or a source
`SnapshotSessionInvalid` discards that staging. A session for one scope can
never resume another scope.

The initial receiver eligibility rule should be conservative: every DBI in the
descriptor must be absent or explicitly replaceable by the scoped-baseline API,
and no conflicting unscoped or logical progress may claim it. The detailed
overwrite policy belongs with the implementation API, but it must be explicit;
a partial baseline is not an implicit repair of unrelated replica state.

## Retention And Operational Rules

The source needs a scoped changelog and a retention watermark per scope and
origin. It cannot infer safe pruning from `_mdbxc_applied`, because a receiver
may subscribe to one scope but not another. Operators retain scoped history
until every relevant receiver has advanced past it or the declared recovery
policy permits a scoped baseline.

Changing a receiver's scope membership requires an application-coordinated
cutover: stop the old scoped worker, ensure the old route is drained or its
recovery horizon is retained, bootstrap the new scope, and start its worker
from the new scoped cursor. No worker may reinterpret old scoped progress as
progress for a new descriptor.

Authentication and authorization remain transport-local, but a production
adapter must authorize the requested `ScopeId` in addition to `DbId` and node
identity. Scope names can expose application topology and are not an access
control mechanism by themselves.

## Non-Goals

This design deliberately does not provide:

- predicate, row, key-range, or tenant filtering inside a DBI;
- cross-scope atomic writes or automatic transaction splitting;
- automatic scope membership discovery or migration;
- logical-table selective replication;
- multi-peer fan-out, conflict resolution, or CRDT semantics; or
- reuse of global complete-database recovery state for a partial scope.

## Implementation Sequence And Acceptance Tests

Implementation should be split into reviewable PRs:

1. Persist and validate immutable scope descriptors, including exclusive raw
   DBI ownership and negative registration tests.
2. Add scope-local capture/changelog/cursor storage and prove that mixed-scope
   or scoped-plus-unscoped write transactions reject before commit.
3. Add capability-gated scoped pull/push and tests for contiguous delivery,
   duplicates, gaps, foreign scope, descriptor mismatch, and restart.
4. Add scoped snapshot sessions, staging, and optional persisted resume with
   atomic final replacement and scoped cursor bootstrap.
5. Add retention/pruning and controlled membership-cutover tests.

Each phase must run the relevant C++11 and C++17 sync tests. End-to-end tests
must demonstrate that two receivers can converge different disjoint scopes,
that an out-of-scope DBI never appears at the receiver, and that a scoped
baseline cannot modify global raw progress.
