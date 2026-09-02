# Sync Use Cases

Use this guide to choose a sync contract from the shape of the data rather
than from the network topology. It gives small application-level recipes; the
wire, persistence, and recovery contracts remain defined in
[`sync/DESIGN.md`](../include/mdbx_containers/sync/DESIGN.md).

Russian version: [sync-use-cases-RU.md](sync-use-cases-RU.md).

## Choose By Write Semantics

| Dataset and writer model | Use | Do not assume |
| --- | --- | --- |
| One writer, many read replicas | Raw replication of a supported table | A replica may also change the same records. |
| Several writers own disjoint keys | Raw multi-origin replication | Raw sync resolves concurrent writes to one key. |
| Several collectors create immutable observations | Raw multi-origin replication with distinct event identities | A same-key raw upsert validates or merges different payloads. |
| Several nodes update one current value, and the source supplies an ordering authority | Narrow LWW v1 with `VersionedKeyValueTable` | Node wall-clock or receive time is an authority. |
| Unordered multiset with one writer or externally serialized destructive updates | `KeyMultiValueTableLogicalAdapter` | General destructive multi-writer convergence. |
| One authoritative ordered history | Ordered logical delivery for `KeyOrderedMultiValueTable` | Multi-origin ordered history or automatic failover. |
| RAG corpus ingested by several nodes | Immutable chunk revisions through raw replication; one owner for mutable projections | Independent multi-writer `VectorStore` allocation or conflict resolution. |

All snippets below assume serializable application types, a durable unique
`NodeId` per process, and one shared `DbId` per replication set. The snippets
show data ownership, not HTTP/WebSocket wiring; see
[sync_22_node_session_minimal.cpp](../examples/sync_22_node_session_minimal.cpp)
for a runnable writer/worker lifecycle.

## Common Writer Setup

Attach capture once for the lifetime of the writer service, or use a shorter
`SyncCaptureScope` around a bounded write phase. Normal supported table writes
then become changelog entries atomically with their MDBX transaction.

```cpp
mdbxc::sync::SyncEngine engine(connection);
engine.initialize_local_identity(local_node_id, replicated_db_id);

mdbxc::sync::ThreadLocalChangeAccumulator capture(connection);
mdbxc::sync::SyncCaptureScope capture_scope(connection, capture);

// Normal supported table writes below are captured on commit.
```

Each receiver owns its own `SyncWorker` and pulls from a peer. It may read its
replicated tables, but it should not make competing writes to records owned by
the writer. One primary can therefore fan out to any number of read replicas;
each replica retains its own applied cursor.

## Case: One Writer, Many Readers

This is the default choice for a service that publishes a canonical stream or
reference dataset. Use a raw-supported table and let one node own the table's
write side.

```cpp
mdbxc::KeyValueTable<std::string, Quote> quotes(connection, "quotes");

// Only the designated writer performs this mutation.
quotes.insert_or_assign("binance/BTCUSDT", latest_quote);

// A replica uses the ordinary read API after its SyncWorker catches up.
const Quote visible_quote = replica_quotes.at("binance/BTCUSDT");
```

`KeyValueTable`, `KeyTable`, `ValueTable`, `SequenceTable`, and `MetadataTable`
have raw capture support. `SequenceTable::append()` remains single-writer:
independent appenders must be serialized externally.

## Case: Market Ticks, Trades, And Other Immutable Events

For event data, make the key identify the upstream event rather than the local
collector or only its timestamp. A timestamp alone can collide; combine it
with the exchange's stable event id or sequence.

```cpp
mdbxc::KeyValueTable<std::string, Trade> observations(
    connection, "trade_observations");

const std::string observation_key =
    collector_id + "/" + exchange + "/" + symbol + "/" + exchange_trade_id;
observations.insert_or_assign(observation_key, trade);
```

Several collectors can safely contribute distinct observation keys to one
receiver database. For a canonical trade table, choose exactly one of these
models explicitly:

- partition ownership, so exactly one collector writes each canonical key;
- retain collector observations and use one deterministic normalizer to write
  `(exchange, symbol, trade_id) -> Trade`;
- accept duplicate observations as distinct data for feed-coverage and latency
  analysis.

Raw sync does not verify that two concurrent upserts of one key carry the same
payload. If two collectors observe the same exchange event, do not use raw
same-key writes as a deduplication or integrity mechanism.

Open OHLCV bars and `latest_quote` records are mutable state, not immutable
events. Derive them from the event stream under one owner, or use the narrow
LWW contract below when the upstream supplies a real revision authority.

## Case: One Current Value With A Source Authority

Use LWW v1 only for point `insert_or_assign` and `erase` on an initially empty
`KeyValueTable`. Every participant must use `ConflictPolicy::LastWriterWins`,
initialize identity, attach capture, and construct the versioned wrapper
before it receives revisioned changes.

```cpp
mdbxc::sync::SyncEngine engine(
    connection, mdbxc::sync::ConflictPolicy::LastWriterWins);
engine.initialize_local_identity(local_node_id, replicated_db_id);

mdbxc::sync::ThreadLocalChangeAccumulator capture(connection);
mdbxc::sync::SyncCaptureScope capture_scope(connection, capture);

mdbxc::KeyValueTable<std::string, Bar> bars(connection, "bars");
mdbxc::sync::VersionedKeyValueTable<std::string, Bar> versioned_bars(
    bars, capture);

const std::vector<std::uint8_t> version =
    source_version_big_endian(broker_time, broker_sequence);
versioned_bars.insert_or_assign(bar_key, bar, version);
```

`source_version_big_endian()` is application code. It must return a non-empty,
canonical byte encoding whose lexicographic order is the source's desired
order. A fixed-width big-endian tuple such as `(broker_time, broker_sequence)`
is suitable when both components are authoritative. `broker_time` is sync
metadata; it need not become a field of `Bar`. Equal versions are resolved by
origin `NodeId` only to make replicas converge, not to determine which source
observation is objectively newer.

The wrapper durably registers its DBI. Direct `KeyValueTable` writes, clear,
bulk, range operations, generic capture, and full snapshots for that DBI are
rejected. Keep ordinary raw tables and LWW tables separate even when they share
one `SyncEngine`.

## Case: Platform Trades And Ledger Records

Treat platform-issued fills, executions, ledger entries, and audit events as
immutable records first. Give every record a globally unambiguous business
identity and replicate the history; derive balances, positions, and reporting
views from it under one owner.

```cpp
mdbxc::KeyValueTable<std::string, Fill> fills(connection, "fills");

const std::string fill_key = platform + "/" + account_id + "/" + execution_id;
fills.insert_or_assign(fill_key, fill);
```

Do not use raw replication to coordinate concurrent mutation of an account
balance, risk limit, or order state. Assign one authoritative writer, serialize
the operation in application code, or give that particular register an explicit
source-version contract.

## Case: RAG Corpus And Derived Search State

Replicate immutable document/chunk revisions as ordinary records. A key should
include the stable document identity, source revision, and chunk ordinal (or a
content hash plus codec and embedding versions).

```cpp
mdbxc::KeyValueTable<std::string, Chunk> chunks(connection, "chunks");

const std::string chunk_key =
    document_id + "/" + source_revision + "/" + chunk_ordinal;
chunks.insert_or_assign(chunk_key, chunk);
```

Several ingestion nodes can write different revisions or owned key ranges.
Treat the current-document pointer, cached retrieval scores, and vector index
as derived state. Rebuild them from the corpus or assign one writer. The
current `VectorStore` paths do not provide distributed id allocation or
multi-writer conflict resolution for one collection.

## When Not To Select Sync Yet

Do not enable a path merely because nodes can exchange batches. First define a
record identity and write authority when any of these are true:

- two nodes can concurrently put or delete one ordinary raw key;
- a `KeyMultiValueTable` needs destructive multi-writer updates;
- an ordered table needs multiple independent origins or automatic failover;
- a table is `AnyValueTable` or `HashedKeyValueStore`, whose sync contracts
  remain deferred;
- a partial snapshot would be used to repair an arbitrary existing replica.

For full constraints and topology details, continue with
[deployment patterns](sync-deployment-patterns.md), the
[logical sync guide](sync-logical.md), and the
[table coverage matrix](../guides/sync-table-coverage.md).
