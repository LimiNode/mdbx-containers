# Последующие действия по аудиту sync

Заметка фиксирует результаты общерепозиторного аудита sync на `main`
`60a5b32479f2d69a78ec9f9e404ab717fb59e92c`. Исправления держатся небольшими
PR, чтобы отдельно проверять поведение, storage format и build hygiene.

## Завершённые исправления

PR #150–#185 последовательно закрыли reserved DBI names, standalone header
coverage, transport providers, stop-before-apply, foreign transactions,
unsupported protocol modes, canonical key layout, `SnapshotRequired`,
`VectorStore` validation/invalidation, transport limits, retry/budget rules,
apply observer hooks, `SyncWorkerGuard`, `SyncNodeSession` и affected DBI
names. Узкий LWW v1 доступен через `VersionedKeyValueTable` с source version
от приложения; ordinary raw DBI на том же engine остаются поддержанными.

## Текущая последовательность PR

Активной audit-последовательности нет.

## Более поздние задачи среднего риска

- Настраивать framework-level pre-buffer limits до удержания полного
  HTTP/WebSocket payload там, где это позволяет concrete backend.
- Registration-side DBI filters apply observer-ов ограничивают только local
  callback delivery; они не фильтруют capture, transport delivery или apply.
- Сократить повторяющийся setup concrete peer-ов: identity policy, retry/backoff
  и production policy.
