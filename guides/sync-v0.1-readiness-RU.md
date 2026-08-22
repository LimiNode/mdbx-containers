# Чек-лист готовности sync v0.1

Sync экспериментален и включается через `MDBXC_SYNC_ENABLED=1`. Для карты
слоёв см. [архитектуру sync](sync-architecture-RU.md), для статуса wrapper-ов —
[матрицу покрытия](sync-table-coverage-RU.md).

## Готово для v0.1

- `KeyValueTable`, `KeyTable`, `ValueTable` и `SequenceTable` захватываются
  `ThreadLocalChangeAccumulator` через `SyncCaptureScope`.
- Самостоятельная запись образует один batch; явная транзакция через несколько
  поддержанных таблиц — один атомарный batch. Чтения и scan не захватываются.
- `SyncEngine::handle_push()` применяет одну полученную страницу в одной MDBX
  transaction; `SyncWorker` ведёт pull loop, pagination, отмену и backoff.
- `SyncWorkerGuard`, `SyncNodeSession` и `SyncWorker::status()` покрывают
  типичное владение worker-ом, application wiring и status polling.
- `Connection::add_sync_apply_observer()` сообщает post-commit affected DBI
  names. `add_sync_apply_observer_for_dbis()` ограничивает только callback
  delivery указанными локальными DBI, не capture, transport или apply.
- Raw `VectorStore` реплицируется через внутренние таблицы. Opt-in
  `VectorStoreLogicalAdapter` schema-v1 применяет add/erase/clear четырёх DBI
  с явным record id; оба режима требуют одного назначенного либо внешне
  сериализованного пишущего узла.
- `DirectSyncPeer`, `HttpSyncPeer` и `WebSocketSyncPeer` поддерживают
  logical-aware fresh recovery: raw baseline, logical schema, replay, ordering
  и pending-delivery state получателя; см. [восстановление](../docs/sync-recovery-RU.md).
- Simple-Web HTTP/WebSocket и Kurlyk/libcurl доступны как optional provider
  targets. Installed-provider и negative wire/transport tests покрывают их
  базовые контракты.

## Эксплуатационные контракты

- Объекты `SyncWorker`, `SyncEngine`, `ISyncPeer` и таблицы принадлежат
  вызывающему коду и должны жить дольше callback-ов и in-flight calls.
- `start()`, `stop()`, `join()` и `run_once()` caller-serialized; status и
  diagnostics thread-safe.
- Cancellation best-effort. `request_stop()` отменяет token и вызывает
  `request_cancel()`, но shutdown может ждать неотменяемый transport call.
- `SyncTransportRetryHint` рекомендателен. По умолчанию worker повторяет и
  permanent transport failures; `StopWorker` переводит его в `Failed`.
- Аутентификация, DB allow-list, trace/request id, rate limits и HTTP/WS
  статусы остаются adapter-local. `requester`/`sender` сверяются с
  authenticated transport identity до dispatch в engine.

## Отложенные таблицы и границы

`AnyValueTable`, raw `KeyMultiValueTable`, raw `KeyOrderedMultiValueTable` и
`HashedKeyValueStore` не должны создавать `ChangeOp`, пока не определены codec,
schema, adapter, preflight/apply и round-trip/negative tests. Частичный capture
опасен: он делает репликацию внешне успешной при расхождении logical state.

`KeyMultiValueTableLogicalAdapter` покрывает unordered schema-v1/v2/v3
операции с одним пишущим узлом или causal serialization; raw capture и general
multi-writer destructive convergence отложены. `KeyOrderedMultiValueTable`
имеет schema-v1 automatic append journal и schema-v2 destructive adapter с
persistent element id; baseline import, multi-origin history, compaction и
automatic origin failover отложены.

Raw `handle_push()` не является logical delivery. `_mdbxc_sync_schema` —
compatibility marker; `LogicalTableRegistry` выполняет preflight/apply; logical
apply владеет одной MDBX write transaction и откатывает её при failure. Logical
frames не входят в raw pull/push. Replay watermark создаётся лениво и требует
дополнительного named DBI при включённом pruning.

## Следующие задачи

- Новые logical-frame capability различия — только при реальном adapter-е.
- Новые `KeyMultiValueTable` операции — только с explicit multiset replay и
  round-trip coverage.
- Scope-aware partial snapshot continuation реализуется только по
  [проектированию выборочной репликации](sync-selective-replication-design-RU.md):
  нужны отдельные scope identity, progress, changelog, retention и правила
  membership cutover.
- General multi-writer для `KeyMultiValueTable`, baseline/multi-origin schema-v2
  и модели `AnyValueTable`/`HashedKeyValueStore` требуют отдельных дизайнов.

## Базовая проверка

Для shared sync header-ов и transport contract-ов запускайте в C++11/C++17:

```text
header_sync_umbrella_test
header_sync_transport_umbrella_test
test_transport_middleware
test_http_transport
test_websocket_transport
test_sync_worker
test_sync_replication
```
