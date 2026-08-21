# Последующие действия по аудиту sync

Заметка фиксирует результаты общерепозиторного аудита sync на `main`
`60a5b32479f2d69a78ec9f9e404ab717fb59e92c`. Исправления держатся небольшими
PR, чтобы отдельно проверять поведение, storage format и build hygiene.

## Завершённые исправления

- PR #150 усилил обработку зарезервированных `_mdbxc_` DBI в публичных именах
  таблиц и входящих sync apply operations.
- PR #97 добавил standalone public-header smoke coverage; PR #128 объявил
  transport backend feature macros, а PR #131 покрыл installed provider targets.
- PR #152 уточнил границу `SyncWorker` stop-before-apply, а PR #154 отвергает
  foreign external transaction до использования handle таблицей, engine, store
  или accumulator другой environment.
- PR #155 сделал unsupported protocol modes явными. Узкий LWW v1 теперь
  доступен через `VersionedKeyValueTable` с source version от приложения: DBI
  постоянно зарегистрирован и fail-closed для direct raw writes, в то время как
  ordinary raw DBI на том же engine остаются поддержанными.
- PR #156 уточнил, что пустой cursor replay-ит retained changelog, а не
  экспортирует database snapshot. PR #167 возвращает `SnapshotRequired` для
  cursor старше retained history.
- PR #158 исправил signed `MDBX_INTEGERKEY` range/cursor ordering; #159 сделал
  integral storage canonical; #160 canonicalized floating zero и отверг NaN;
  #161 добавил fast-math regression coverage.
- PR #162 добавил matrix покрытия таблиц; #164 — wrapper-specific `clear()`
  capture/replication; #165 удалил вводящее в заблуждение
  `pull_full_snapshot()`; #166 добавил machine-readable response errors.
- PR #168 отвергает invalid `VectorStore` collection name; #169 добавил
  `Connection::sync_apply_generation()` и lazy rebuild открытых `VectorStore`
  после remote apply; #170 добавил binding-side body limits.
- PR #171 сериализовал remote apply commits с cache-backed `VectorStore`; #172
  ограничил и evict-ит HTTP fixed-window rate-limit buckets; #173 добавил
  whole-exchange deadline Simple-WebSocket client; #174 поддержал lowercase
  `mdbxConfig`; #175 разделил pull page и per-batch byte budgets.
- PR #180 добавил connection-level remote apply observers; #181 —
  `SyncWorkerGuard`; #182 — application example для `SyncCaptureScope`, guard
  и `ISyncApplyObserver`; #184 — `SyncNodeSession`; #185 — affected DBI names.

## Текущая последовательность PR

Активной audit-последовательности нет.

## Более поздние задачи среднего риска

- Настраивать framework-level pre-buffer limits до удержания полного
  HTTP/WebSocket payload там, где это позволяет concrete backend.
- Registration-side DBI filters apply observer-ов ограничивают только local
  callback delivery; они не фильтруют capture, transport delivery или apply.
- Сократить повторяющийся setup concrete peer-ов: identity policy, retry/backoff
  и production policy.
