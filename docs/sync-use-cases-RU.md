# Сценарии использования sync

Этот документ помогает выбрать контракт sync по структуре данных, а не по
топологии сети. В нём приведены короткие recipes уровня приложения; инварианты
wire-формата, persistent state и recovery определены в
[`sync/DESIGN.md`](../include/mdbx_containers/sync/DESIGN.md).

English version: [sync-use-cases.md](sync-use-cases.md).

## Выбор по семантике записи

| Набор данных и модель writers | Использовать | Не предполагать |
| --- | --- | --- |
| Один writer и много read replicas | Raw replication поддерживаемой таблицы | Что replica также может менять те же records. |
| Несколько writers владеют непересекающимися keys | Raw multi-origin replication | Что raw sync разрешает конкурентные записи в один key. |
| Несколько collectors создают immutable observations | Raw multi-origin replication с разными event identities | Что raw upsert одного key валидирует или сливает разные payloads. |
| Несколько nodes обновляют одно текущее значение, а источник даёт authority порядка | Узкий LWW v1 через `VersionedKeyValueTable` | Что wall-clock node или время приёма является authority. |
| Unordered multiset с одним writer или внешне сериализованными destructive updates | `KeyMultiValueTableLogicalAdapter` | Общую destructive multi-writer convergence. |
| Одна authoritative ordered history | Ordered logical delivery для `KeyOrderedMultiValueTable` | Multi-origin ordered history или automatic failover. |
| RAG corpus от нескольких ingestion nodes | Immutable chunk revisions через raw replication; один owner mutable projections | Независимое multi-writer выделение id или conflict resolution в `VectorStore`. |

Все фрагменты ниже предполагают serializable application types, устойчивый
уникальный `NodeId` у каждого процесса и один общий `DbId` у replication set.
Они показывают ownership данных, а не HTTP/WebSocket wiring. Полный runnable
lifecycle writer/worker приведён в
[sync_22_node_session_minimal.cpp](../examples/sync_22_node_session_minimal.cpp).

## Общая настройка writer

Подключите capture на всё время жизни writer service либо используйте короткий
`SyncCaptureScope` вокруг ограниченной фазы записи. Тогда обычные операции
поддерживаемых таблиц атомарно с MDBX transaction становятся changelog entries.

```cpp
mdbxc::sync::SyncEngine engine(connection);
engine.initialize_local_identity(local_node_id, replicated_db_id);

mdbxc::sync::ThreadLocalChangeAccumulator capture(connection);
mdbxc::sync::SyncCaptureScope capture_scope(connection, capture);

// Обычные записи в поддерживаемые таблицы ниже capture'ятся на commit.
```

Каждый receiver владеет своим `SyncWorker` и выполняет pull у peer. Он может
читать реплицированные таблицы, но не должен конкурировать за records, которыми
владеет writer. Поэтому один primary может fan-out'ить данные на любое число
read replicas; каждая replica хранит собственный applied cursor.

## Сценарий: один writer и много readers

Это стандартный выбор для сервиса, публикующего canonical stream или reference
dataset. Используйте raw-supported table и назначьте одному node write-side
этой таблицы.

```cpp
mdbxc::KeyValueTable<std::string, Quote> quotes(connection, "quotes");

// Эту мутацию делает только назначенный writer.
quotes.insert_or_assign("binance/BTCUSDT", latest_quote);

// Replica использует обычный read API после catch-up своего SyncWorker.
const Quote visible_quote = replica_quotes.at("binance/BTCUSDT");
```

`KeyValueTable`, `KeyTable`, `ValueTable` и `SequenceTable` поддерживают raw
capture. `SequenceTable::append()` остаётся single-writer: независимые
appenders нужно сериализовать вне библиотеки.

## Сценарий: рыночные тики, сделки и другие immutable events

Для event data пусть key идентифицирует upstream event, а не local collector и
не только timestamp. Один timestamp может совпасть; добавьте устойчивый event
id или sequence биржи.

```cpp
mdbxc::KeyValueTable<std::string, Trade> observations(
    connection, "trade_observations");

const std::string observation_key =
    collector_id + "/" + exchange + "/" + symbol + "/" + exchange_trade_id;
observations.insert_or_assign(observation_key, trade);
```

Несколько collectors могут безопасно добавлять разные observation keys в одну
БД receiver. Для canonical таблицы сделок явно выберите одну модель:

- разделите ownership: каждый canonical key пишет ровно один collector;
- храните observations collectors и поручите одному deterministic normalizer
  записывать `(exchange, symbol, trade_id) -> Trade`;
- принимайте duplicate observations как отдельные данные для анализа coverage
  и latency фидов.

Raw sync не проверяет, что два конкурентных upsert одного key несут один и тот
же payload. Если два collectors увидели один exchange event, raw same-key write
нельзя использовать как механизм deduplication или проверки целостности.

Открытые OHLCV bars и `latest_quote` — mutable state, а не immutable events.
Вычисляйте их из event stream под одним owner либо используйте узкий LWW
контракт ниже, если upstream даёт реальный revision authority.

## Сценарий: одно текущее значение с authority источника

Используйте LWW v1 только для point `insert_or_assign` и `erase` в изначально
пустой `KeyValueTable`. Каждый participant должен использовать
`ConflictPolicy::LastWriterWins`, инициализировать identity, подключить capture
и сконструировать versioned wrapper до приёма revisioned changes.

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

`source_version_big_endian()` — application code. Она должна вернуть
непустое canonical byte encoding, у которого lexicographic order совпадает с
нужным source order. Fixed-width big-endian tuple, например
`(broker_time, broker_sequence)`, подходит, когда оба компонента authoritative.
`broker_time` — sync metadata; его не требуется добавлять полем в `Bar`.
Равные versions разрешаются по `NodeId` origin только для convergence replicas,
а не для определения объективно более нового наблюдения.

Wrapper durable-регистрирует свой DBI. Прямые записи через `KeyValueTable`,
`clear`, bulk и range операции, generic capture и full snapshots этого DBI
отклоняются. Обычные raw tables и LWW tables нужно оставлять раздельными, даже
когда они используют один `SyncEngine`.

## Сценарий: сделки трейдеров и platform ledger records

Сначала моделируйте platform fills, executions, ledger entries и audit events
как immutable records. Дайте каждой записи globally unambiguous business
identity и реплицируйте историю; balances, positions и reporting views
вычисляйте из неё под одним owner.

```cpp
mdbxc::KeyValueTable<std::string, Fill> fills(connection, "fills");

const std::string fill_key = platform + "/" + account_id + "/" + execution_id;
fills.insert_or_assign(fill_key, fill);
```

Не используйте raw replication для координации concurrent mutation account
balance, risk limit или order state. Назначьте authoritative writer,
сериализуйте операцию на уровне приложения или задайте явный source-version
контракт конкретному register.

## Сценарий: RAG corpus и производное search state

Реплицируйте immutable document/chunk revisions как обычные records. Key должен
включать stable document identity, source revision и chunk ordinal (либо content
hash вместе с codec и embedding versions).

```cpp
mdbxc::KeyValueTable<std::string, Chunk> chunks(connection, "chunks");

const std::string chunk_key =
    document_id + "/" + source_revision + "/" + chunk_ordinal;
chunks.insert_or_assign(chunk_key, chunk);
```

Несколько ingestion nodes могут писать разные revisions или выделенные им key
ranges. Рассматривайте current-document pointer, cached retrieval scores и
vector index как derived state. Перестраивайте их из corpus либо назначьте
одного writer. Текущие пути `VectorStore` не дают distributed id allocation или
multi-writer conflict resolution одной collection.

## Когда sync пока не следует выбирать

Не включайте path только потому, что nodes умеют обмениваться batches. Сначала
определите record identity и write authority, если верно хотя бы одно условие:

- два nodes могут конкурентно делать put или delete одного ordinary raw key;
- `KeyMultiValueTable` требует destructive multi-writer updates;
- ordered table требует несколько независимых origins или automatic failover;
- используется `AnyValueTable` или `HashedKeyValueStore`, чьи sync contracts
  пока отложены;
- partial snapshot предполагается для repair произвольной существующей replica.

Полные ограничения и детали topology описаны в
[deployment patterns](sync-deployment-patterns-RU.md),
[руководстве по logical sync](sync-logical-RU.md) и
[матрице покрытия таблиц](../guides/sync-table-coverage.md).
