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

A selective scope is an additional projection of the complete raw stream, not
a replacement replication authority. Every supported raw DBI continues to
enter the existing global changelog and `CompleteUserDatabase` snapshot exactly
as it does today. A scope adds its own capture, progress, retention, and
delivery path for selected DBIs. This preserves existing full-raw receivers and
their `_mdbxc_applied` cursors while allowing a different receiver to subscribe
only to one or more scopes. A scoped receiver does not also consume global raw
pulls for that `DbId`.

```text
source transaction { orders in Scope X, catalog unscoped }
                    |                         |
                    +---- global batch --------+
                    |     { orders, catalog }  | --> full receiver / global cursor
                    |
                    +---- scoped batch --------+ --> Scope X receiver / scoped cursor
                          { orders }
```

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
2. **Membership and write authority are immutable for a scope identity.** A
   durable descriptor maps one `ScopeId` to one non-zero designated writer
   origin and a sorted, unique manifest of named DBIs and their required DBI
   flags. The source and receiver compare the complete descriptor before any
   mutation.
3. **Global history stays complete; selective membership is exclusive.** A
   scoped DBI continues to enter the global raw changelog. It can belong to at
   most one selective scope. The implementation captures the global batch and
   any scoped projection in the same committing MDBX transaction.
4. **Progress is one stream per scope.** The existing `_mdbxc_applied` cursor
   remains the progress record for complete raw replication only. Scoped
   progress is keyed by `ScopeId` in a separate durable store, represents the
   designated writer origin's sequence, and advances only after a contiguous
   scoped batch commits.
5. **Retention is per scope.** The designated writer retains and prunes scoped
   history using the scoped cursor contract. A receiver behind that retained
   history gets a scope-specific `SnapshotRequired`; it must not consume later
   batches.
6. **A scoped baseline is atomic.** A completed baseline replaces only the
   descriptor manifest and writes the designated writer origin's scoped tail in
   the same MDBX transaction. It neither writes nor validates the global raw
   cursor.
7. **Descriptor mismatch fails closed.** Different membership, DBI flags,
   designated writer origin, source `DbId`, scope identity, or immutable
   snapshot metadata invalidates the request/session. The receiver does not
   attempt a best-effort intersection.

## Descriptor And Membership Evolution

The implementation should persist a `ScopedReplicationDescriptor` containing:

```text
ScopeId
designated_writer_origin (non-zero NodeId)
sorted unique [(dbi_name, dbi_flags)]
```

**Global writer invariant.** Selective scope v1 requires one globally enforced
writer for every DBI in its manifest. Before selective replication activates,
the application must install and validate this same descriptor on every known
origin for the `DbId` that can open a manifest DBI for local application
writes. The designated writer accepts those writes; every other such origin
must reject them before mutation and global capture. An origin without the
descriptor must be configured without local write access to scoped DBIs;
otherwise the topology is invalid and the scope must not activate. The
application or installed scope guards enforce this rule. Descriptor deployment
and write-access revocation are application-coordinated; this v1 design
provides no dynamic writer discovery.

Neither membership nor write authority changes in place. Adding, removing,
renaming, or changing the flags of a DBI, or changing the designated writer
origin, creates a new `ScopeId` and descriptor. The application must drain or
retire the old scope according to its retention policy, bootstrap the new
scope, and only then route receivers to it. This prevents a receiver from
silently interpreting a cursor for `{orders}` as a cursor for
`{orders, invoices}`, or treating a new writer's sequence as a continuation of
the old writer's stream.

An application may intentionally configure two immutable scopes with different
DBI sets. They must remain disjoint. A table move is therefore a controlled
cutover, not an automatic membership update. A writer-origin move is the same
kind of controlled cutover, not a failover within an existing scope.

## Capture, Wire, And Apply

Scoped replication needs a protocol family separate from existing raw
`PullRequest`, `PushRequest`, `ChangeBatch`, and `FullSnapshotChunk`. Extending
those DTOs with an optional DBI filter would make a global cursor appear valid
after only a subset of its operations was applied.

The existing global raw protocol remains complete and unchanged: global pull
and push carry every supported raw DBI, and `CompleteUserDatabase` continues to
export every named non-reserved user DBI. Scoped peers use only the new protocol
family for their manifests. A receiving environment chooses one durable raw
delivery mode for a `DbId`:

- **full-global mode** consumes the existing complete raw stream and no scoped
  streams; or
- **selective mode** consumes one or more disjoint scopes and no global raw
  stream for that `DbId`.

The mode is receiver-wide rather than per DBI because one global `ChangeBatch`
can atomically contain operations for both scoped and unscoped DBIs. Filtering
those operations at a selective receiver would again make an incomplete global
cursor appear valid. A full-global receiver and one or more selective receivers
can coexist for the same source database.

The proposed family has these conceptual records:

```text
ScopedPullRequest     = DbId + requester + ScopeId + scoped cursor
ScopedPullResponse    = descriptor + contiguous scoped batches or error
ScopedChangeBatch     = ScopeId + designated writer origin + scope-local sequence + operations
ScopedSnapshotRequest = ScopeId + empty scoped cursor
ScopedSnapshotChunk   = immutable descriptor + scoped source tail + page data
```

The concrete codec version, capability names, and public C++ types are deferred
to the implementation PR. A peer without the scoped capability rejects the
request; it must not silently fall back to complete raw pull/push. A scoped
request must target the descriptor's designated writer origin; another peer
rejects it rather than relaying scoped history.

Every operation in `ScopedChangeBatch` names a DBI in the descriptor manifest,
and its origin must equal the descriptor's designated writer origin. Only that
origin may make local application mutations to a scoped DBI or create its
scoped projection; a local mutation by another origin fails closed before
commit. This does not change full-global apply: a node may still apply raw
history received from another origin, but it must not present that history as a
scoped relay.

The global batch always contains the complete caller transaction. If a
transaction at the designated writer changes DBIs from one selective scope, the
source atomically adds one scoped batch containing that scope's operations; it
may also change unscoped DBIs. A transaction that changes DBIs from two
selective scopes fails closed before commit, rather than splitting its scoped
effects. The receiver validates the descriptor, designated writer origin, and
every DBI before opening its write transaction, then applies the batch and
advances the matching scoped cursor atomically. Duplicate, gap, wrong-writer,
and out-of-scope batches fail closed under the same contiguous-delivery
principle as the existing raw cursor.

The first implementation must define a capture-time scope-membership and
designated-writer lookup and write the scoped projection beside, not instead
of, the global capture. It must not filter the global changelog after the fact.

## Scoped Baseline And Resume

The scoped baseline exists only for recovery of one scope. It is not an
extension of `ManifestOnly`, and `ManifestOnly` remains a manual physical
replacement tool with no replication-progress effect.

For a scoped `SnapshotRequired` the source captures, in one stable read
transaction:

- the immutable descriptor and all DBI data in its manifest;
- the designated writer origin's scope-local source tail; and
- an opaque snapshot session id and immutable page-zero metadata.

The scoped baseline is served directly by the designated writer origin, whose
DBI state in the captured read transaction is authoritative for that scope. A
node that merely received its global raw history is not a scoped baseline
source.
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
the receiver must not have an active global raw cursor for that `DbId`, and no
logical apply state may claim those DBIs. The detailed overwrite policy belongs
with the implementation API, but it must be explicit; a partial baseline is not
an implicit repair of unrelated replica state. Moving an existing full-global
receiver to selective mode requires an explicit fresh-receiver or reset-and-
bootstrap procedure; it is not an in-place cursor conversion.

## Retention And Operational Rules

The designated writer retains the existing complete global changelog under its
current contract and additionally needs a scoped changelog and retention
watermark per scope. It cannot infer safe scoped pruning from `_mdbxc_applied`,
because a receiver may subscribe to one scope but not another. Operators retain
scoped history until every relevant receiver has advanced past it or the
declared recovery policy permits a scoped baseline.

Changing a receiver's scope membership requires an application-coordinated
cutover: stop the old scoped worker, ensure the old route is drained or its
recovery horizon is retained, bootstrap the new scope, and start its worker
from the new scoped cursor. No worker may reinterpret old scoped progress as
progress for a new descriptor. Switching full-global and selective receiver
modes is a distinct cutover and uses the fresh-receiver or reset-and-bootstrap
procedure above.

Authentication and authorization remain transport-local, but a production
adapter must authorize the requested `ScopeId` in addition to `DbId` and node
identity. Scope names can expose application topology and are not an access
control mechanism by themselves.

## Non-Goals

This design deliberately does not provide:

- predicate, row, key-range, or tenant filtering inside a DBI;
- cross-scope atomic writes or automatic transaction splitting;
- a hybrid receiver that consumes global raw for some DBIs and scoped delivery
  for others;
- automatic scope membership discovery or migration;
- logical-table selective replication;
- multi-origin selective replication, designated-writer failover, or relay of
  scoped history/baselines through a node that only received global raw
  history;
- multi-peer fan-out, conflict resolution, or CRDT semantics; or
- reuse of global complete-database recovery state for a partial scope.

## Implementation Sequence And Acceptance Tests

Implementation should be split into reviewable PRs:

1. Persist and validate immutable scope descriptors, including the non-zero
   designated writer origin, exclusive selective membership, and negative
   registration tests.
2. Add scope-local capture/changelog/cursor storage and prove that a
   scoped-plus-unscoped transaction publishes an atomic full global batch plus
   its scoped projection, while mixed-scope transactions reject before commit.
3. Add capability-gated scoped pull/push and tests for contiguous delivery,
   duplicates, gaps, wrong writer, foreign scope, descriptor mismatch, and
   restart.
4. Add scoped snapshot sessions, staging, and optional persisted resume with
   atomic final replacement and scoped cursor bootstrap.
5. Add retention/pruning and controlled membership-cutover tests.

Each phase must run the relevant C++11 and C++17 sync tests. End-to-end tests
must demonstrate that two receivers can converge different disjoint scopes,
that an out-of-scope DBI never appears at the receiver, and that a scoped
baseline cannot modify global raw progress. They must also prove that adding a
scope never removes its DBIs from an existing full-raw receiver's changelog or
complete-database baseline, that an unauthorized local writer is rejected
before mutation and global capture, that after restart the same non-designated
origin restores its descriptor and still rejects the write, that a scoped
baseline is accepted only from the designated writer, that changing the writer
uses a new `ScopeId`, and that a receiver rejects any attempt to mix a global
raw stream with scoped delivery for the same `DbId`. In particular, a test with
designated writer `A` and non-designated writer `B` using the same descriptor
must prove that `B` cannot write a scoped DBI before or after restart.
