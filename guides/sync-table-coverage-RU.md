# Матрица покрытия таблиц sync

Это источник истины для sync v0.1. Raw transport переносит physical DBI
operations, не десериализует table value и применяет captured `storage_key` /
`value` через `SyncEngine::handle_push()`. Explicit logical adapter-ы имеют
собственный ordered delivery protocol и durable outbox.

## Матрица

| Wrapper | Статус v0.1 | Захват и применение |
| --- | --- | --- |
| `KeyValueTable<K, V>` | Поддержан | `Put`, `Delete`, `ClearTable`; point/bulk/range/clear. Raw key/value replay в DBI с captured flags. |
| `VersionedKeyValueTable<K, V>` | Узкий LWW v1 | Point `insert_or_assign`/`erase` с non-empty source version. Version order и origin разрешают конфликт, tombstone не даёт stale put воскресить delete. |
| `KeyTable<K>` | Поддержан | `Put`, `Delete`, `ClearTable`; replay raw key с empty value. |
| `ValueTable<V>` | Поддержан | `Put`, `Delete`, `ClearTable`; replay singleton key и serialized value. |
| `SequenceTable<V>` | Поддержан | `Put`, `Delete`, `ClearTable`; replay stable `uint64_t` key и value. |
| `VectorStore` | Raw + limited logical | Raw через internal tables; schema-v1 adapter atomically add/erase/clear ids, embedding, text, metadata с explicit id. Один назначенный/external-serialized writer. |
| `AnyValueTable<K>` | Отложен | Нет `ChangeOp`: нужен type tag и compatibility policy. |
| `KeyMultiValueTable<K, V>` | Logical schema-v3 + automatic capture | Нет raw `ChangeOp`. Durable receiver-neutral binding journal-ит supported calls; raw writable txn, suppression и unsupported range fail closed. General multi-writer destructive convergence отложен. |
| `KeyOrderedMultiValueTable<K, V>` | Automatic v1 + ordered adapters | Нет raw `ChangeOp`. Schema-v1 journal-ит append/insert одного назначенного origin; schema-v2 даёт element id, tombstone и bounded destructive capture. Baseline/multi-origin/compaction отложены. |
| `HashedKeyValueStore<K,V,H,Layout>` | Отложен | Нужна модель logical key и hash index. |

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
tombstone.

## Logical и отложенные правила

`_mdbxc_sync_schema` хранит schema id, kind, version и canonical owned DBI set;
`LogicalChange` непрозрачен; `LogicalTableRegistry` выполняет two-phase
preflight/apply. `KeyValueTableLogicalAdapter`, `KeyTableLogicalAdapter` и
`VectorStoreLogicalAdapter` — opt-in helpers с typed capture; их outbox worker
доставляет только logical-capable peer-у, не raw pull/push.

Новой deferred таблице нельзя добавлять `record_op()` без persistent schema,
wire representation, adapter, apply validation/reconstruction, transaction
rollback owner, capture/round-trip/negative tests. Partial capture создаёт
скрытое logical divergence.

`KeyMultiValueTable` поддерживает ограниченные unordered schema v1/v2/v3,
включая batch append, reconcile и bounded range erase, но raw capture и general
concurrent destructive updates отложены. `KeyOrderedMultiValueTable` schema-v1
append-only; schema-v2 использует `OrderedElementId`, Live/Tombstone и
single-origin destructive capture. Migration v1→v2 требует отдельной миграции
occurrence, auxiliary state, durable binding и journal/delivery state; marker
version сам по себе её не выполняет.

## Проверка

При table capture запускайте в C++11/C++17 `test_sync_capture`,
`test_sync_replication`, `test_sync_engine`, `test_sync_randomized`; для
transport/worker изменений добавляйте `test_http_transport`,
`test_websocket_transport`, `test_sync_worker` и wrapper tests затронутых
типов.
