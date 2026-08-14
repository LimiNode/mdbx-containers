# Карта архитектуры синхронизации

Этот документ предназначен для первичной навигации по подсистеме sync перед
выбором API или чтением implementation headers. Здесь описаны границы
ответственности и точки входа. Инварианты wire-формата, persistent state и
recovery определены в
[`sync/DESIGN.md`](../include/mdbx_containers/sync/DESIGN.md).

[English version](sync-architecture.md)

## Сначала выберите pipeline

В sync есть два независимых replication pipeline. Они используют общие node
identity, transaction, планирование worker и transport seams, но не преобразуют
сообщения одного pipeline в сообщения другого.

```text
                            +---------------------------+
                            | Общая основа sync          |
                            | Connection / transaction   |
                            | node identity / schema     |
                            | SyncEngine / SyncWorker    |
                            +-------------+-------------+
                                          /   \
                                         /     \
       RAW REPLICATION                    /       \  LOGICAL REPLICATION
                                         v         v
 +---------------------------+     +------------------------------+
 | Мутация поддерживаемой    |     | Мутация bound-table или      |
 | таблицы (физическая DBI)  |     | adapter (объявленная schema) |
 +-------------+-------------+     +--------------+---------------+
               |                                  |
               v                                  v
 +---------------------------+     +------------------------------+
 | Raw capture accumulator   |     | Logical change frame         |
 +-------------+-------------+     +--------------+---------------+
               |                                  |
               v                                  v
 +---------------------------+     +------------------------------+
 | Durable raw changelog     |     | Receiver-neutral journal     |
 +-------------+-------------+     +--------------+---------------+
               |                                  |
               v                                  v
 +---------------------------+     +------------------------------+
 | Pull / push page          |     | Route / outbox для receiver  |
 +-------------+-------------+     +--------------+---------------+
               |                                  |
               v                                  v
 +---------------------------+     +------------------------------+
 | Raw DBI apply             |     | Ordered logical delivery     |
 +---------------------------+     +--------------+---------------+
                                                    |
                                                    v
                                     +-----------------------------+
                                     | Apply зарегистрированным    |
                                     | adapter                     |
                                     +-----------------------------+
```

Используйте raw pipeline, когда достаточно физического replay пар
ключ/значение в DBI. Используйте logical pipeline только когда public semantics
таблицы требуют объявленной schema, custom apply-логики, порядка или
receiver-specific delivery. Logical frame никогда не вкладывается в raw
`ChangeBatch`.

## Публичные точки входа

Подключайте наиболее узкий aggregate header, которому принадлежит нужный API.

| Задача | Include | Что предоставляет |
| --- | --- | --- |
| Полная optional-подсистема | `mdbx_containers/sync.hpp` | Все домены sync. |
| Raw protocol DTO и codecs | `mdbx_containers/sync/protocol.hpp` | Batches, cursors, snapshots, bounds. |
| Durable raw state | `mdbx_containers/sync/storage.hpp` | Changelog, origin, applied-cursor, LWW sidecars. |
| Время жизни raw capture | `mdbx_containers/sync/capture.hpp` | Capture sink, scope, accumulator, Connection hook. |
| Logical contracts и durable state | `mdbx_containers/sync/logical.hpp` | Schemas, frames, journal, delivery, recovery DTOs. |
| Table-bound logical adapters | `mdbx_containers/sync/adapters.hpp` | Prerequisites таблиц и реализации adapters. |
| Engine и worker | `mdbx_containers/sync/engine.hpp` | Peers, engine, worker, in-process peers. |
| Transport-neutral HTTP/WS seams | `mdbx_containers/sync/transport.hpp` | Codec, middleware, policy, HTTP и WebSocket adapters. |
| Конкретные optional backends | `mdbx_containers/sync/transports/*.hpp` | Backend-specific provider aggregates. |

Concrete headers в `sync/transports/...` являются документированными точками
integration для приложений, сознательно выбравших backend. Остальные headers
ниже `sync/` являются internal leaves; вместо них подключайте владеющий ими
aggregate.

## Ответственность слоёв

| Слой | Каталог | Ответственность |
| --- | --- | --- |
| Shared primitives | `sync/core/`, `sync/common.hpp` | Identity, cancellation, conflict и observer contracts. |
| Raw protocol | `sync/protocol/` | Wire DTO и validation raw replication. |
| Raw persistence | `sync/stores/` | Metadata, changelog и apply state. |
| Capture | `sync/capture/` | Фиксирует локальные raw mutations в committing transaction. |
| Logical state | `sync/logical/` | Schema references, logical frames, journal, route и replay state. |
| Logical adapters | `sync/logical/adapters/` | Table-specific capture и apply semantics. |
| Orchestration | `sync/engine/` | Pull/push, transactions, worker rounds и recovery coordination. |
| Transport | `sync/transport/` | Framework-neutral DTO adaptation, policy и observability. |
| Backend bindings | `sync/transports/` | Optional Simple-Web и Kurlyk integrations. |

`Connection` — единственный намеренный bridge из storage core в sync.
`sync/connection_hooks.hpp` перед определением `Connection` предоставляет его
logical capture hook и prerequisites durable binding. Этот header является
internal seam, а не точкой include для приложения.

## Правило владения include

Internal leaf может непосредственно подключать standard-library, third-party и
локальные same-domain implementation dependencies, которыми он владеет
напрямую. Он также может опираться на prerequisites, намеренно установленные
владеющим domain aggregate или явным integration seam, когда прямой project
include потребовал бы traversal вверх или создал обратную зависимость.
Standalone-компиляция требуется только от поддерживаемых public entry points,
а не от internal leaves.

## Где искать подробный контракт

| Вопрос | Документ |
| --- | --- |
| Какие таблицы поддержаны и с какой семантикой? | [Матрица покрытия таблиц sync](sync-table-coverage.md) |
| Готова ли функция для v0.1 и что отложено? | [Чек-лист готовности sync v0.1](sync-v0.1-readiness.md) |
| Как безопасно эксплуатировать production transport? | [Заметки о production transport](sync-transport-production.md) |
| Каковы полные инварианты wire, storage и recovery? | [`sync/DESIGN.md`](../include/mdbx_containers/sync/DESIGN.md) |
| Какой sync contract подходит dataset и модели writers? | [`docs/sync-use-cases-RU.md`](../docs/sync-use-cases-RU.md) |
| Каковы подробные multi-origin deployment patterns? | [`docs/sync-deployment-patterns-RU.md`](../docs/sync-deployment-patterns-RU.md) |
| Как использовать logical и ordered sync? | [`docs/sync-logical-RU.md`](../docs/sync-logical-RU.md) |
| Как работают recovery и full snapshot? | [`docs/sync-recovery-RU.md`](../docs/sync-recovery-RU.md) |
