# Примеры синхронизации

Эти примеры показывают API sync v0.1 как набор блоков, не привязанных к
конкретному транспорту. Большинство примеров используют `DirectSyncPeer` или
небольшой буфер в памяти, чтобы поток протокола был виден без HTTP, WebSocket
или IPC-кода. Опциональные HTTP- и WebSocket-примеры используют готовые
заголовки привязки Simple-Web из `mdbx_containers/sync/transports/simple_web/`
поверх Simple-Web-Server, Simple-WebSocket-Server и автономного Asio. Пример
Kurlyk использует подключаемую привязку HTTP-клиента из
`mdbx_containers/sync/transports/kurlyk/` поверх libcurl.

Синхронизация включается явно. Примеры собираются с `MDBXC_SYNC_ENABLED=1`;
приложениям, которые используют sync, тоже нужно компилировать соответствующий
код с этим макросом.

## Сборка и запуск

```bash
cmake -S . -B tmp/build-examples \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=OFF \
    -DMDBXC_BUILD_EXAMPLES=ON \
    -DCMAKE_CXX_STANDARD=11

cmake --build tmp/build-examples --target sync_01_lifecycle_direct_peer
tmp/build-examples/bin/examples/sync_01_lifecycle_direct_peer
```

Замените имя цели сборки на любой пример из таблицы ниже. На Windows имя
исполняемого файла заканчивается на `.exe`, например:

```powershell
.\tmp\build-examples\bin\examples\sync_01_lifecycle_direct_peer.exe
```

Реальные примеры с HTTP включаются отдельно, потому что они скачивают
заголовки автономного Asio, Simple-Web-Server и для демонстрации с управлением
процессом
tiny-process-library:

```bash
cmake -S . -B tmp/build-http-example \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=OFF \
    -DMDBXC_BUILD_EXAMPLES=ON \
    -DMDBXC_HTTP_SYNC_EXAMPLE=ON \
    -DCMAKE_CXX_STANDARD=11

cmake --build tmp/build-http-example --target sync_13_http_simple_web_server
tmp/build-http-example/bin/examples/sync_13_http_simple_web_server \
    demo 127.0.0.1 18080
tmp/build-http-example/bin/examples/sync_13_http_simple_web_server \
    worker-demo 127.0.0.1 18080

cmake --build tmp/build-http-example --target sync_18_http_node_fleet
tmp/build-http-example/bin/examples/sync_18_http_node_fleet \
    master-replica 3
tmp/build-http-example/bin/examples/sync_18_http_node_fleet \
    mesh 3
```

Готовая привязка HTTP подключается так:

```cpp
#if defined(MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT) && \
        MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT
#include <mdbx_containers/sync/transports/simple_web/HttpTransport.hpp>
#endif
```

Привязка HTTP-клиента Kurlyk/libcurl тоже включается отдельно. Она использует
тот же независимый от фреймворка API `HttpSyncPeer` и меняет только реализацию
`IHttpSyncClient`. На Windows/MinGW пример может скачать зафиксированный
готовый пакет Win64 libcurl, когда
`MDBXC_KURLYK_HTTP_SYNC_MINGW_CURL_FALLBACK=ON` (по умолчанию):

```bash
cmake -S . -B tmp/build-kurlyk-http \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=OFF \
    -DMDBXC_BUILD_EXAMPLES=ON \
    -DMDBXC_KURLYK_HTTP_SYNC_EXAMPLE=ON \
    -DCMAKE_CXX_STANDARD=17

cmake --build tmp/build-kurlyk-http --target sync_19_kurlyk_http_client
tmp/build-kurlyk-http/bin/examples/sync_19_kurlyk_http_client
```

Готовая привязка HTTP-клиента Kurlyk подключается так:

```cpp
#if defined(MDBXC_HAS_KURLYK_HTTP_TRANSPORT) && \
        MDBXC_HAS_KURLYK_HTTP_TRANSPORT
#include <mdbx_containers/sync/transports/kurlyk/HttpTransport.hpp>
#endif
```

Цели, которым нужны и привязка Simple-Web HTTP, и WebSocket, могут
подключать агрегирующий заголовок backend:

```cpp
#if defined(MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT) && \
        MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT && \
        defined(MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT) && \
        MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT
#include <mdbx_containers/sync/transports/simple_web.hpp>
#endif
```

`MDBXC_SIMPLE_WEB_HTTP_TRANSPORT`,
`MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT` и `MDBXC_KURLYK_HTTP_TRANSPORT`
включают подключаемые цели зависимостей и их быстрые проверки backend.
Параметры `MDBXC_*_SYNC_EXAMPLE` только добавляют примеры из этого репозитория
поверх этих backend; когда примеры включены, старые параметры примеров всё ещё
включают соответствующий backend для совместимости со старыми командами.
Цели потребителя получают `MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT`,
`MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT` или
`MDBXC_HAS_KURLYK_HTTP_TRANSPORT` от конкретной цели зависимости, с которой
они линкуются.

При использовании установленного пакета эти цели можно создать через
экспортированные функции provider:

```cmake
find_package(mdbx_containers CONFIG REQUIRED)

mdbx_containers_simple_web_http_transport_provide(OUT_TARGET http_target)
target_link_libraries(my_http_node PRIVATE ${http_target})

mdbx_containers_simple_web_websocket_transport_provide(OUT_TARGET ws_target)
target_link_libraries(my_ws_node PRIVATE ${ws_target})

mdbx_containers_kurlyk_http_transport_provide(OUT_TARGET kurlyk_target)
target_link_libraries(my_kurlyk_client PRIVATE ${kurlyk_target})
```

Возвращаемые цели включают `MDBXC_SYNC_ENABLED`, линкуют цель пакета ядра
и распространяют соответствующий `MDBXC_HAS_*_TRANSPORT` macro.

Реальный пример с WebSocket тоже включается отдельно, потому что он скачивает
заголовки автономного Asio и Simple-WebSocket-Server.
Simple-WebSocket-Server использует OpenSSL Crypto для WebSocket handshake,
поэтому задайте `OPENSSL_ROOT_DIR`, если CMake не найдёт OpenSSL автоматически.
На Windows/MinGW пример может скачать зафиксированный готовый Win64 OpenSSL
пакет, когда `MDBXC_WEBSOCKET_SYNC_MINGW_OPENSSL_FALLBACK=ON` (по умолчанию):

Зафиксированный резервный пакет предоставляет `lib/VC/x64/MD/libcrypto.lib`;
такой layout библиотеки импорта проверен с toolchain MinGW-w64, который используется
в CI проекта. Пример использует обычный `ws://`, а не WSS/TLS, поэтому рядом с
исполняемым файлом копируется только `libcrypto-3-x64.dll`.

```bash
cmake -S . -B tmp/build-ws-example \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=OFF \
    -DMDBXC_BUILD_EXAMPLES=ON \
    -DMDBXC_WEBSOCKET_SYNC_EXAMPLE=ON \
    -DCMAKE_CXX_STANDARD=11

cmake --build tmp/build-ws-example --target sync_17_websocket_simple_web_server
tmp/build-ws-example/bin/examples/sync_17_websocket_simple_web_server \
    127.0.0.1 18194
```

Готовая привязка WebSocket подключается так:

```cpp
#if defined(MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT) && \
        MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT
#include <mdbx_containers/sync/transports/simple_web/WebSocketTransport.hpp>
#endif
```

Чтобы использовать свой готовый бинарный пакет OpenSSL, укажите CMake корень
пакета, в котором лежат `include`, `lib` и `bin`, например:

```powershell
cmake -S . -B tmp/build-ws-example `
    -DMDBXC_DEPS_MODE=BUNDLED `
    -DMDBXC_BUILD_TESTS=OFF `
    -DMDBXC_BUILD_EXAMPLES=ON `
    -DMDBXC_WEBSOCKET_SYNC_EXAMPLE=ON `
    -DOPENSSL_ROOT_DIR=C:/deps/openssl-win64-v3.4.0 `
    -DCMAKE_CXX_STANDARD=11
```

## Порядок чтения

| Пример | Что показывает | Уровень |
| --- | --- | --- |
| `sync_01_lifecycle_direct_peer.cpp` | Один явный цикл write -> pull -> push -> read. | Начальный |
| `sync_02_incremental_direct_peer.cpp` | Курсор получателя и инкрементальные pull-запросы. | Начальный |
| `sync_03_multi_table.cpp` | Поддерживаемые типы таблиц и постраничный pull. | Средний |
| `sync_04_primary_to_replicas.cpp` | Один основной узел и несколько реплик с независимыми курсорами. | Средний |
| `sync_05_three_node_mesh.cpp` | Попарный обмен без пересылки batches, полученных от других origin-узлов. | Продвинутый |
| `sync_06_threaded_transport.cpp` | Владение объектами по потокам и буфер запросов и ответов в памяти. | Продвинутый |
| `sync_07_worker_observer.cpp` | Фоновый `SyncWorker` и уведомления о прогрессе через `ISyncWorkerObserver`. | Продвинутый |
| `sync_08_transport_boundary.cpp` | Псевдотранспорт, который проверяет контракт границы `ISyncPeer` (`CancellationToken` + `request_cancel()`). | Продвинутый |
| `sync_09_transport_codec.cpp` | Версионированный бинарный `TransportMessageCodec` для DTO запросов и ответов. | Продвинутый |
| `sync_10_custom_transport.cpp` | Минимальный кастомный `ISyncPeer` поверх закодированных byte buffers. | Продвинутый |
| `sync_11_http_adapter.cpp` | Фреймворк-независимый HTTP-адаптер поверх `TransportMessageCodec`. | Продвинутый |
| `sync_12_transport_middleware.cpp` | Allow-list, fixed-budget rate limit и metrics middleware вокруг транспортных адаптеров. | Продвинутый |
| `sync_13_http_simple_web_server.cpp` | Готовый Simple-Web HTTP binding, прямой pull/push и worker-demo через реальные loopback HTTP sockets. | Продвинутый |
| `sync_14_websocket_adapter.cpp` | Framework-neutral WebSocket binary-message seam поверх `TransportMessageCodec`. | Продвинутый |
| `sync_15_http_policy_context.cpp` | Bearer-token, remote-address и `Retry-After` HTTP policy context. | Продвинутый |
| `sync_16_worker_http_transport.cpp` | `SyncWorker` поверх `HttpSyncPeer` и HTTP request-context policy. | Продвинутый |
| `sync_17_websocket_simple_web_server.cpp` | Готовый Simple-WebSocket binding поверх standalone Asio. | Продвинутый |
| `sync_18_http_node_fleet.cpp` | Fleet из нескольких процессов поверх готового Simple-Web HTTP binding. | Продвинутый |
| `sync_19_kurlyk_http_client.cpp` | Готовый Kurlyk/libcurl HTTP client binding против Simple-Web sync listener. | Продвинутый |
| `sync_20_observability.cpp` | Логи worker/transport observer-ов с trace IDs, progress и retry hints. | Продвинутый |
| `sync_21_worker_guard_apply_hooks.cpp` | Жизненный цикл через `SyncNodeSession` и remote apply observer hooks. | Средний |
| `sync_22_node_session_minimal.cpp` | Минимальный application-facing recipe для `SyncNodeSession`. | Начальный |
| `sync_23_key_value_logical_frame.cpp` | Явный путь `KeyValueTable`: logical capture -> frame codec -> apply на реплике. | Продвинутый |

## Общие правила

- `NodeId` идентифицирует конкретный узел репликации. Сгенерируйте его один
  раз, сохраните и используйте повторно после перезапуска.
- `DbId` идентифицирует одну логическую реплицируемую БД. Все узлы,
  реплицирующие эту логическую БД, должны использовать одинаковый `DbId`.
- Перед локальными записями прикрепите `ThreadLocalChangeAccumulator` через
  `SyncCaptureScope` или более низкоуровневую пару `attach_sync_capture()` /
  `detach_sync_capture()`. Scope-вариант автоматически восстанавливает
  предыдущий capture sink после завершения локальной фазы записи.
- Предпочитайте `SyncCaptureScope`, когда функция владеет ограниченной фазой
  записи: например, seed-данными, одной локальной бизнес-операцией или
  временной подменой sink в тестах. Ручные `attach_sync_capture()` /
  `detach_sync_capture()` лучше оставлять для более широкого lifecycle
  компонента, например службы записи в рамках процесса, которая прикрепляет
  захват один раз после старта и отсоединяет его при остановке.
- Вложенные `SyncCaptureScope` должны завершаться строго в обратном порядке.
  Явный out-of-order `detach()` отклоняется, а raw-вызовы
  `attach_sync_capture()` / `detach_sync_capture()` не должны подменять sink
  соединения, пока им владеет активный scope.
- Методы поддерживаемых таблиц сохраняют обычный API. Не нужно оборачивать
  каждый `insert`, `insert_or_assign`, `erase`, `reconcile` или range erase в
  отдельный sync-вызов. Capture sink записывает поддерживаемые write operations
  при commit MDBX-транзакции. Если вы передаёте явную транзакцию через
  несколько вызовов поддерживаемых таблиц, эти записи коммитятся и
  реплицируются как один локальный batch. Независимые вызовы без явной
  транзакции остаются независимыми локальными транзакциями и независимыми sync
  batches.
- Logical capture через `KeyValueTableLogicalAdapter` сейчас является явным
  opt-in путём. `LogicalChangeFrameCodec` может переносить такие typed logical
  changes между компонентами приложения, а получатель применяет их через
  `SyncEngine::apply_logical_frame_bytes()`. Обычные transport DTO
  `PullRequest`, `PullResponse`, `PushRequest` и `PushResponse` остаются
  raw-DBI only до появления negotiation для logical transport capabilities.
  `LogicalChangeFrame` является payload-контейнером, а не протоколом доставки:
  retrying transports должны оборачивать его в `LogicalDeliveryEnvelope`, где
  есть destination identity и persistent replay protection.
- Чтение, поиск и range scans не запускают sync. Локальный commit также не
  обращается к другой ноде; `SyncWorker` или явный pull/push-код позже
  отправляет уже закоммиченные batches через `ISyncPeer`.
- Минимальный recipe с `SyncNodeSession` использует `DirectSyncPeer`, чтобы не
  требовать optional dependencies. Реальная HTTP-нода сохраняет ту же форму
  session и заменяет только peer на готовый HTTP transport binding, например
  Simple-Web HTTP или Kurlyk HTTP.
- `PullRequest`, `PullResponse`, `PushRequest` и `PushResponse` - структуры
  данных протокола. Именно эти значения нужно сериализовать и передавать через
  транспорт приложения.
- `TransportMessageCodec` - встроенный версионированный бинарный codec для
  этих DTO. Он сериализует данные запросов и ответов, но не локальное состояние
  отмены.
- Не передавайте между потоками или процессами объекты `Connection`,
  `Transaction` и таблиц, а также необёрнутые дескрипторы MDBX и курсоры.
- Получатель применяет страницу через `SyncEngine::handle_push()`. Одна
  страница применяется в одной локальной транзакции; многостраничный pull не
  является одной глобальной транзакцией.
- Курсор применённых изменений получателя сохраняется в MDBX metadata.
  Следующий pull должен использовать этот курсор, чтобы не применять уже
  обработанные batches повторно.
- Применение удалённых изменений обновляет пользовательские таблицы, но не
  записывает удалённый batch в локальный changelog получателя. Обычный узел не
  становится транзитным relay-узлом.
- Sync v0.1 поддерживает `KeyValueTable`, `KeyTable`, `ValueTable`,
  `SequenceTable` и `VectorStore` через его внутренние поддерживаемые таблицы.
  `AnyValueTable`, `KeyMultiValueTable` и `HashedKeyValueStore` не
  реплицируются в v0.1. Для `KeyMultiValueTable` в `sync/DESIGN.md` описан
  отложенная семантика неупорядоченного мультимножества для одного пишущего
  узла либо причинно сериализованных обновлений; конкурентные конфликты
  нескольких писателей и сохраняющие порядок распределённые истории отложены
  в будущую работу, например `KeyOrderedMultiValueTable<K, V>`. Захват остаётся
  выключенным до появления реализации и тестов полного цикла.

## Граница транспорта

`DirectSyncPeer` удобен для тестов, примеров и демонстраций внутри одного
процесса. Рабочий код приложения должен рассматривать структуры запросов и
ответов как данные, которые передаются транспортом:

```text
реплика формирует PullRequest
-> транспорт передаёт его исходному узлу
-> исходный узел вызывает SyncEngine::handle_pull()
-> транспорт возвращает PullResponse
-> реплика помещает пакеты в PushRequest
-> реплика вызывает SyncEngine::handle_push()
```

Многопоточный пример держит каждый `Connection` и `SyncEngine` в том потоке,
который владеет соответствующей БД. Общий буфер передаёт только значения
запросов и ответов протокола. Эту границу стоит сохранить при замене буфера на
реальный транспорт.

`sync_07_worker_observer.cpp` показывает сторону приложения у фоновой реплики:
`SyncWorker` выполняет pull/apply циклы, а `ISyncWorkerObserver` сообщает
основному коду, когда меняются стадии pull/apply, страницы применены или
завершён очередной цикл. События стадий содержат best-effort оценку прогресса
догоняющей синхронизации на основе последнего remote cursor.

`sync_08_transport_boundary.cpp` фиксирует контракт, которому должен следовать
транспортный адаптер, реализующий `ISyncPeer`. Пример показывает, где
наблюдается `CancellationToken` из `PullRequest` и где
`ISyncPeer::request_cancel()` закрывает выполняющийся вызов. Готовые
Simple-Web HTTP/WebSocket bindings используют ту же схему, заменяя очереди в
памяти на сокеты.

`sync_09_transport_codec.cpp` показывает следующий слой: `PullRequest`,
`PullResponse`, `PushRequest` и `PushResponse` кодируются в byte buffer через
`TransportMessageCodec` до пересечения границы. Политики адаптера, такие как
авторизация, rate limit, allow/deny lists, маршрутизация и TLS, должны
оборачивать этот обмен bytes, а не попадать внутрь sync DTO.

`sync_10_custom_transport.cpp` показывает минимальную форму кастомного
транспорта: реализовать `ISyncPeer`, закодировать DTO запроса, отправить bytes
через канал приложения, декодировать DTO ответа и реализовать
`request_cancel()` через собственный механизм прерывания канала.

`sync_11_http_adapter.cpp` показывает границу HTTP-адаптера. `HttpSyncPeer`
реализует `ISyncPeer` поверх абстрактного `IHttpSyncClient`, а
`HttpSyncServer` передаёт уже разобранные HTTP method/target/content-type/body
значения в `SyncEngine`. `mdbxc::sync::simple_web::HttpSyncClient` и
`mdbxc::sync::simple_web::HttpSyncListener` - готовая опциональная реализация
этой границы на Simple-Web-Server.

`sync_14_websocket_adapter.cpp` показывает границу WebSocket-адаптера.
`WebSocketSyncPeer` реализует `ISyncPeer` поверх абстрактного
`IWebSocketSyncChannel`, а `WebSocketSyncServer` передаёт полные бинарные
сообщения в `SyncEngine`. `mdbxc::sync::simple_web::WebSocketSyncChannel` и
`mdbxc::sync::simple_web::WebSocketSyncListener` - готовая опциональная
реализация этой границы на Simple-WebSocket-Server.

`sync_12_transport_middleware.cpp` показывает adapter-local policy wrappers.
`SyncPeerMiddleware` может проверять декодированные `NodeId` / `DbId` перед
вызовом peer-а, а `HttpSyncClientMiddleware` может применять route-level policy
вокруг HTTP-shaped обмена bytes. В такие обёртки помещаются простые allow-lists,
fixed-budget rate limits и metrics hooks; они не добавляют auth tokens или
счётчики в wire format sync DTO.
Они не заменяют server-framework authentication или per-remote-client rate
limits перед `HttpSyncServer::handle()`. Метрики считают вызовы middleware
hooks, поэтому общий observer на нескольких слоях может посчитать одно
логическое действие по одному разу на каждом слое.

`sync_15_http_policy_context.cpp` показывает request-context слой, который
конкретный HTTP server binding может запускать перед `HttpSyncServer::handle()`.
Он извлекает bearer token из headers, проверяет remote address и возвращает
header `Retry-After`, когда fixed-window limiter отклоняет запрос.

`sync_16_worker_http_transport.cpp` объединяет фоновый worker с HTTP-shaped
adapter внутри одного процесса. Replica владеет `SyncWorker` и `HttpSyncPeer`;
middleware на стороне основного узла аутентифицирует bearer token как `NodeId`
реплики перед тем, как `HttpSyncServer` передаст запрос в `SyncEngine`.

`sync_13_http_simple_web_server.cpp worker-demo` запускает тот же worker path
через настоящие loopback HTTP sockets. Он использует bearer identity,
message-size guard, страничные pull-запросы и callbacks observer-а worker-а
поверх `mdbxc::sync::simple_web::HttpSyncClient` и
`mdbxc::sync::simple_web::HttpSyncListener`.

`sync_18_http_node_fleet.cpp` запускает несколько копий одного исполняемого
файла через tiny-process-library. Каждый дочерний процесс владеет собственным
MDBX environment, готовым Simple-Web HTTP listener/client, sync engine,
capture sink и application loop;
родительский процесс только стартует ноды и отправляет команды `pause` / `stop`
через stdin. Режим `master-replica` показывает одного активного писателя и
одного пассивного получателя. Режим `mesh` позволяет обеим нодам писать
локально, пока каждая нода подтягивает изменения другой по HTTP.

`sync_19_kurlyk_http_client.cpp` оставляет серверную сторону на готовом
Simple-Web HTTP listener и меняет клиентский backend на
`mdbxc::sync::kurlyk::HttpSyncClient`. Это показывает форму для других HTTP
клиентов: реализовать `IHttpSyncClient`, передать его в `HttpSyncPeer` и не
менять `SyncEngine`, DTO encoding, auth policy и локальный apply.

`sync_20_observability.cpp` показывает production-style logging hooks без
создания socket-ов. Он запускает `SyncWorker` поверх `HttpSyncPeer`, добавляет
request/trace IDs в HTTP-shaped client, логирует request context через
`ISyncTransportObserver`, логирует progress worker-а через
`ISyncWorkerObserver` и показывает, как однократный `429 Retry-After`
превращается в worker backoff hint.

`sync_17_websocket_simple_web_server.cpp` - socket-backed WebSocket-вариант.
Он использует `mdbxc::sync::simple_web::WebSocketSyncChannel` и
`mdbxc::sync::simple_web::WebSocketSyncListener`, отправляет binary frames,
проверяет bearer token во
время WebSocket handshake, передаёт аутентифицированный `NodeId` реплики вместе
с явным `SyncDbAccess` в `WebSocketSyncServerMiddleware`, отклоняет слишком
большие сообщения до decode и классифицирует close codes для диагностики
клиента.
