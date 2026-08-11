# Sync Deployment Patterns

`mdbx-containers` sync supports multi-origin transport: a replica can receive
committed change histories from multiple durable `NodeId` origins. This makes
several writer nodes practical when their writes do not compete for the same
logical record.

`ConflictPolicy::Reject` remains the default. For one narrow mutable-register
case, `ConflictPolicy::LastWriterWins` is available with
`VersionedKeyValueTable`: it compares application-provided source-version bytes,
not node wall-clock time. Every other raw table path still has no concurrent
conflict-resolution semantics. Multi-origin transport is not a generic CRDT
contract.

Russian version: [sync-deployment-patterns-RU.md](sync-deployment-patterns-RU.md).

## One Receiver Database, Multiple Origins

Multiple origins do not require multiple user databases at a receiver. Their
histories, progress, and replay state are maintained by the sync subsystem;
incoming operations are applied to the same user MDBX tables selected by their
DBI names. A storage node can therefore receive from redundant collectors into
one canonical database.

```text
collector A ──┐
              ├── one MDBX database
collector B ──┘       user table: trades
```

The application data model, not the origin id, determines whether records are
the same logical event. Origin identity remains relevant for delivery and
diagnostics, and can also be retained explicitly as provenance data.

## Select A Data Model Before Selecting A Topology

Use one of these two patterns for a replicated dataset:

1. Assign each writer a disjoint logical-key range.
2. Store immutable records with globally unambiguous identities.

The second pattern is often the better fit for event data. It permits several
writers without requiring them to decide which concurrent update to one mutable
record wins.

| Scenario | Current sync contract |
| --- | --- |
| Different nodes write different logical keys | Supported multi-origin topology |
| Multiple nodes create uniquely identified immutable records | Practical multi-writer topology |
| Two nodes concurrently mutate one logical key | No defined conflict-resolution semantics |
| One node erases while another writes the same logical key | No guaranteed convergence semantics |
| Versioned `KeyValueTable` put/erase with source authority | Narrow source-version-wins register |
| Independent `SequenceTable::append()` calls | Single-writer only; serialize appenders externally |
| Destructive multi-writer `KeyMultiValueTable` operations | Require one writer or application-level causal serialization |

Do not use wall-clock time alone as an ordering authority. Clocks can differ
between nodes. When an application must select a winner, define and persist an
application-owned authority, such as an upstream sequence number or a
deterministic version tuple, before it treats the data as mutable replicated
state.

## Fault-Tolerant Market Data

### Primary And Hot Standby

One active collector publishes a dataset while a standby follows the upstream
feed and can take over after application-coordinated failover. This is the
simplest topology because only one writer is active for each key range. The
application still needs a failover procedure that resumes the upstream feed
without an unaccounted gap.

### Active-Active Collectors For Immutable Events

For trades and other immutable upstream events, use the exchange's canonical
identity as the user-table key, for example:

```text
(exchange, symbol, trade_id) -> Trade
```

Two collectors that receive the same trade should produce the same logical key
and the same serialized payload. This allows either collector to fill an event
gap observed by another collector in one receiver database.

Current raw sync does not yet implement canonical-event deduplication or an
integrity error for equal keys with different payloads. An active-active
deployment must therefore validate such divergence in application code and
must not describe raw upsert ordering as an integrity or conflict-resolution
guarantee. A same event identity with different payload is a data-integrity
anomaly, not a case for silently selecting a writer.

### Preserve Observation Provenance When Needed

Keep canonical events and collector observations separate when operations need
to measure feed coverage or latency:

```text
trades:       trade_id -> Trade
observations: (trade_id, collector_id) -> Observation
```

This keeps one market history while preserving enough data to calculate missing
coverage, collector lag, or source divergence.

### Mutable Bars And Snapshots

An open bar, latest quote, or upstream snapshot is not an immutable event. Two
collectors may legitimately observe different states for one key. For this
narrow case, configure every participant with
`ConflictPolicy::LastWriterWins` and write only through
`VersionedKeyValueTable<K, V>`. Its `insert_or_assign` and `erase` each take a
non-empty, canonical source-version byte sequence. The version is sync metadata,
not part of `V`; the receiver durably keeps the winner and a delete tombstone in
`_mdbxc_identity_index` in the same transaction as user data.

The winner is the lexicographically greatest source version; equal versions are
resolved by origin `NodeId`. A version must describe one state for one source:
reusing it for different local state is rejected. A broker timestamp may be one
component, but an upstream sequence or deterministic tuple must resolve timestamp
ties. Writer wall-clock and packet-receipt time are not suitable authorities.

This v1 register intentionally excludes normal raw `KeyValueTable` writes,
`clear`, bulk/range operations, logical identity remapping, other table types,
and full-snapshot bootstrap. Do not mix ordinary raw capture with an LWW engine:
it rejects unversioned incoming operations. Tombstones are retained; compaction
needs a separately specified replica horizon. Reserve one additional MDBX DBI
slot for `_mdbxc_identity_index` in `Config::max_dbs`.

## Supported Table Paths

Raw v0.1 capture records normal writes for `KeyValueTable`, `KeyTable`,
`ValueTable`, and `SequenceTable` when a
`ThreadLocalChangeAccumulator` is attached to the writing `Connection`.

`KeyMultiValueTable` uses an explicit logical adapter rather than raw capture.
Its destructive operations require one writer or application-level causal
serialization. `VectorStore` raw replication and its logical adapter likewise
require one authoritative or externally serialized writer per collection.
See the [sync table coverage matrix](../guides/sync-table-coverage.md) for the
complete table-by-table contract.

## Market Data Collectors

Model the raw market-data layer as immutable events. Each collector can write
its own records and synchronize them with aggregation or storage nodes.

For feeds with an authoritative upstream sequence, a record identity can be:

```text
(exchange, symbol, stream, exchange_timestamp, exchange_sequence)
```

If two collectors intentionally observe the same feed for redundancy, choose
one of two explicit models:

- retain both observations with `(source_node, exchange, symbol,
  exchange_event_id)`;
- deduplicate by `(exchange, symbol, exchange_event_id)` when the exchange
  provides a stable event identity.

Do not use ordinary raw capture for concurrent mutable records such as
`latest_quote["BTCUSDT"]` or an in-progress OHLCV candle. Either rebuild them
from immutable ticks and trades, assign one authoritative projector, or use the
narrow versioned register with an exchange-authoritative source version.

```text
collector A ── immutable events ──┐
                                  ├── aggregation or storage node
collector B ── immutable events ──┘
```

This pattern also supports ownership or sharding, for example by exchange,
symbol range, market type, or collector identity.

## Trader And Platform Records

Represent trades, fills, ledger events, and audit records as immutable events
with globally unique identities. An identity can include a platform-issued
trade or execution id, or an application-generated id scoped by its origin.
Assign ownership explicitly when a platform can originate the same business
record from more than one node.

Replicate event history, then derive balances, positions, and reporting views
from that history or update those views through one authoritative writer. Do
not rely on raw sync to coordinate concurrent mutations of the same account
balance, order state, or risk limit; such a decision needs application-level
ownership, serialization, or a separately designed conflict protocol.

## Knowledge Bases And RAG Chunks

Store source material and chunk revisions as immutable, versioned records. A
chunk identity can combine a stable document id, source revision, and chunk
ordinal, or use a content hash together with the codec and embedding versions.
This lets different ingestion nodes add distinct document revisions without
competing for one mutable key.

Treat mutable projections as derived state. Examples include a current-document
pointer, a search index, cached retrieval scores, and a vector collection built
from chunks. Rebuild such projections from the immutable corpus or assign a
single authoritative writer. In particular, the current `VectorStore` sync
paths do not provide independent multi-writer id allocation or conflict
resolution for one collection.

## Deployment Checklist

Before enabling sync for a dataset, define:

- the durable `NodeId` for every writer and the shared `DbId` for the
  replication set;
- the logical identity of every replicated record and which writer owns it;
- whether a record is immutable, or which application authority serializes its
  mutations and deletions;
- for a versioned register, the canonical source-version encoding and its
  upstream authority, including timestamp tie-breaking;
- the rebuild or ownership strategy for derived state;
- the table-specific sync path from the
  [coverage matrix](../guides/sync-table-coverage.md).

For capture setup, transport deployment, and recovery behavior, start with the
[sync overview](sync.md),
[transport production notes](../guides/sync-transport-production.md), and
[recovery guide](sync-recovery.md).
