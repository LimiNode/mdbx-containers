# Матрица покрытия таблиц sync

Это источник истины для sync v0.1. Raw transport переносит physical DBI
operations, не десериализует table value и применяет captured `storage_key` /
`value` через `SyncEngine::handle_push()`. Explicit logical adapter-ы имеют
собственный ordered delivery protocol и durable outbox. После завершения raw
round worker с `SyncWorkerOptions::enable_logical_delivery` может передать этот
outbox через peer, поддерживающий logical delivery.

## Матрица

| Wrapper | Статус v0.1 | Операции захвата | Семантика применения | Основные tests |
| --- | --- | --- | --- | --- |
| `KeyValueTable<K, V>` | Поддержан | `Put`, `Delete`, `ClearTable`; point/bulk/range erase/clear. | Raw key/value bytes replay-ятся в destination DBI с captured DBI flags. | `test_sync_capture`, `test_sync_replication`, `test_sync_engine` |
| `VersionedKeyValueTable<K, V>` | Узкий LWW v1 | Только point `insert_or_assign` и `erase`, каждая с non-empty application source version. | DBI постоянно регистрируется; именно при `ConflictPolicy::LastWriterWins` source-version bytes упорядочивают конкурирующие операции, равные версии разрешаются origin `NodeId`. Tombstone не даёт stale put воскресить delete. | `test_sync_engine` |
| `KeyTable<K>` | Поддержан | `Put`, `Delete`, `ClearTable`; insert/erase/range erase/clear. | Raw key bytes replay-ятся с empty value. | `test_sync_capture`, `test_sync_engine`, `test_sync_replication` |
| `ValueTable<V>` | Поддержан | `Put`, `Delete`, `ClearTable`; set/insert/update/erase/clear. | Replay singleton physical key и serialized value bytes. | `test_sync_capture`, `test_sync_replication` |
| `SequenceTable<V>` | Поддержан | `Put`, `Delete`, `ClearTable`; append/`insert_or_assign`/erase/clear. | Replay stable `uint64_t` sequence key и value bytes. | `test_sync_capture`, `test_sync_replication` |
| `VectorStore` | Raw + limited logical adapter | Raw через internal `SequenceTable`/`KeyValueTable`; logical schema-v1: add/erase/clear ids, embeddings, text, metadata с explicit record id. | Raw replay physical member-table operations; logical apply проверяет adapter marker, record state и embedding dimension. `commit_to_outbox()` atomically публикует frame для capable peer. Один назначенный/external-serialized writer. | `test_sync_capture`, `test_sync_replication`, `test_vector_store_logical_adapter` |
| `AnyValueTable<K>` | Отложен | Нет `ChangeOp`. | Не применяется как typed heterogeneous table. | negative `test_sync_capture` |
| `KeyMultiValueTable<K, V>` | Logical schema-v3 + automatic capture | Нет raw `ChangeOp`. | `bind_key_multi_value_logical_capture()` постоянно привязывает DBI к receiver-neutral dataset; supported calls atomically добавляют schema-v3 frame в `LogicalJournalStore`, routing materializes receiver route позднее. Missing runtime binding, public suppression, legacy sessions и caller raw writable txn fail closed. | `test_key_value_logical_adapter`, negative `test_sync_capture` |
| `KeyOrderedMultiValueTable<K, V>` | Automatic schema-v1 + limited ordered adapters | Нет raw `ChangeOp`. Schema-v1 capture: append/insert; schema-v2: `AppendElement`, `EraseElement`. | Один назначенный ordered origin. Schema-v1 журналирует обычный append/insert и отвергает destructive calls; schema-v2 хранит element id/tombstone и поддерживает bounded destructive capture. | `test_key_value_logical_adapter`, `test_key_ordered_multi_value_destructive_state`, `test_key_ordered_multi_value_destructive_adapter` |
| `HashedKeyValueStore<K,V,H,Layout>` | Отложен | Нет `ChangeOp`. | Отложена identity mapping logical key и hash index. | negative `test_sync_capture` |

## Контракт поддержанного capture

Подключите capture к пишущему connection, предпочтительно `SyncCaptureScope`,
и используйте обычный table API. Standalone transaction создаёт один local
batch, явная transaction через поддержанные таблицы — один atomic batch.
Read/scan/search не захватываются; remote apply фиксирует одну page в одной MDBX
transaction. Captured DBI name/flags и physical key/value bytes должны быть
достаточны для открытия или проверки destination DBI без table-specific decode.

`VersionedKeyValueTable` вне общего raw contract: каждая point operation владеет
automatic write transaction, подавляет ordinary capture и создаёт один
versioned raw `ChangeOp`. DBI постоянно зарегистрирован в
`_mdbxc_versioned_dbis`; direct raw writes, clear/range/bulk и generic logical
writes fail closed. Full snapshot не включает registered DBI; registry и
`_mdbxc_identity_index` занимают два named DBI, index удерживает delete
tombstone. Операции требуют initialized sync identity и attached
`ThreadLocalChangeAccumulator`; передача одной source version без этих условий
не образует применимую LWW операцию.

Durable lookup в `Connection` продолжает действовать и без живого `SyncEngine`:
его нельзя обойти подавлением capture. Остальные DBI того же LWW engine работают
по обычному raw contract.

## Logical и отложенные правила

`_mdbxc_sync_schema` хранит явные application schema id, logical kind, schema
version и owned DBI names как canonical sorted unique set. `LogicalChange`
несёт opaque adapter-owned payload, а `LogicalTableRegistry` до вызова adapter-а
проверяет полный schema tuple и reserved flags в two-phase preflight/apply.
Direct logical frames доставляет сам caller: они не попадают в raw pull/push.
`KeyValueTableLogicalAdapter`, `KeyTableLogicalAdapter` и
`VectorStoreLogicalAdapter` — opt-in helpers с typed capture; их outbox worker
доставляет только logical-capable peer-у, не raw pull/push.

Новой deferred таблице нельзя добавлять `record_op()` без persistent schema,
wire representation, adapter, apply validation/reconstruction, transaction
rollback owner, capture/round-trip/negative tests. Partial capture создаёт
скрытое logical divergence.

`KeyMultiValueTable` поддерживает ограниченные unordered schema v1/v2/v3,
включая batch append, reconcile и bounded range erase, но raw capture и general
concurrent destructive updates отложены. Обычный неограниченный
`erase_range()` для bound DBI не поддерживается. `KeyOrderedMultiValueTable`
schema-v1 append-only: durable binding журналирует обычные `append`/`insert`, в
том числе batch append, а destructive table calls отвергаются. Schema-v2
использует `OrderedElementId`, Live/Tombstone и single-origin destructive
capture; она также поддерживает `replace_with()` для одного назначенного
origin. Direct logical frame и unordered delivery для обеих схем fail closed.
Baseline import, multi-origin history, tombstone pruning и compaction остаются
отложенными. Migration v1→v2 требует отдельной миграции occurrence, auxiliary
state, durable binding и journal/delivery state; marker version сам по себе её
не выполняет.

`KeyMultiValueTableLogicalAdapter` до мутации отвергает truncated pair payload,
oversized declared length, trailing bytes, payload-bearing clear и unknown
opcode. Для любой новой deferred table требуется persistent logical schema и
`SchemaRegistryStore` record, wire representation со всеми table-specific
metadata, `ILogicalTableAdapter` с preflight/apply, reconstruction/validation
на receiving стороне и owner transaction, который rollback-ит whole apply при
failure или exception.

Для `VectorStore` реплики используют одинаковое collection name и compatible
embedding serialization. Vector metric — local query configuration, а не
logical schema state; для одинакового search ranking приложения задают тот же
metric на каждой реплике. Remote apply invalidates RAM index уже открытого
store через connection generation; rebuild выполняется лениво перед следующим
index-dependent operation. Collection с existing logical schema marker нужно
открывать через `VectorStoreLogicalAdapter::open_store_for_schema()`: он не
создаёт отсутствующие DBI и fail-closed для incompatible storage.

Raw путь реплицирует четыре internal DBI `VectorStore`: ids, embeddings, text и
metadata. Logical schema-v1 add/erase/clear работает с явными record id и
проверяет все четыре DBI до мутации; erase сохраняет ids marker как high-water
allocation, а clear очищает все четыре DBI. Ни raw, ни logical путь не являются
distributed id allocator-ом или conflict resolver-ом: для collection нужен один
назначенный либо внешне сериализованный пишущий узел.

Schema-v2 `KeyOrderedMultiValueTable` bounded `erase_at`, key/value erase и
clear разворачивают полный canonical-codec selector result в deterministic
exact-id operations до мутации. Candidate-expansion и inspected-record bounds
охватывают всю операцию, включая оставшиеся reads во время mutation. Default
resolver выполняет complete reverse validation; opt-in transaction-bound proof
ограничивает полный materialized ID set и может повторно использоваться только
доверенными selector calls с отдельным budget; session token непереиспользуем.
Stale/foreign proof fail closed. Native
commit-error cleanup после ordered capture имеет deterministic test-only
coverage; это не заявление о real storage-I/O fault injection.

## Проверка

При table capture запускайте в C++11/C++17 `test_sync_capture`,
`test_sync_replication`, `test_sync_engine`, `test_sync_randomized`; для
transport/worker изменений добавляйте `test_http_transport`,
`test_websocket_transport`, `test_sync_worker` и wrapper tests затронутых
типов.
