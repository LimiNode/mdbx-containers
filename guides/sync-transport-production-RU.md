# Эксплуатация transport sync в production

Это руководство описывает deployment contracts optional HTTP/WebSocket
transport-ов. Оно не заменяет API reference: до публикации sync endpoint за
пределами доверенного test process приложение само принимает эти решения.

## Граница безопасности транспорта

Wire format содержит только sync DTO. Bearer token-ы, cookie, mTLS principal,
remote address, HTTP headers, WebSocket handshake/close metadata, request id и
trace id остаются adapter-local. До вызова server adapter аутентифицируйте
transport session и привяжите authenticated identity к ожидаемому `NodeId` и
DB access policy.

## TLS и WSS

Готовые Simple-Web examples используют plain HTTP/WS только для простых local
smoke tests. В production используйте TLS termination в reverse proxy/service
mesh, HTTPS/WSS-capable binding либо mTLS на edge. Forwarded identity metadata
после внешнего TLS допустима только от доверенного proxy: произвольные
`X-Forwarded-*` из публичной сети не являются authenticated facts.

Обычные схемы: edge TLS termination с internal authenticated traffic; native
TLS binding, владеющий certificate/cipher/session state; либо mTLS, где subject,
SAN, SPIFFE id или иной principal отображается в `NodeId`. Mapping остаётся вне
serialized сообщений. Transport policy сверяет `PullRequest::requester` и
`PushRequest::sender` с authenticated session до dispatch в `SyncEngine`.

## Ротация token-ов

Во время rotation window держите минимум две активные credentials: добавьте
новый token для того же `NodeId` и DB policy, переведите clients, затем удалите
старый после максимальной задержки restart/reconnect. При revoke закрывайте
long-lived WebSocket sessions policy close code-ом (например `1008`). HTTP
retry с тем же revoked token — permanent failure; credential provider должен
выбрать новый token до следующего request.

WebSocket client обычно продолжает старую session до конца rotation window,
предпочитает новый token для новых session, а после revoke получает policy close
и reconnect-ится с новой credential. Concrete server отклоняет новые handshake
и закрывает уже существующие session через свой policy close code.

## Graceful shutdown

1. Прекратите принимать новые transport request-ы.
2. Запросите остановку sync worker-ов.
3. Дождитесь in-flight `pull()`/`push()` либо их timeout.
4. Join worker и listener threads.
5. Уничтожьте `SyncEngine`, таблицы и `Connection`.

`request_stop()` передаёт отмену через active `CancellationToken` и
`ISyncPeer::request_cancel()`, но cancellation best-effort. Binding-ам нужны
finite timeout либо socket-level cancellation. До остановки listener-а новые
HTTP request-ы обычно получают retryable `503`/`Retry-After`; WebSocket
handshake прекращается, idle session закрываются `1001` или `1012`. Не
уничтожайте engine/connection, пока transport callback ещё может до них дойти.
Для active WebSocket exchange завершите текущий message, когда это возможно,
либо используйте cancellation/close mechanism backend-а, если shutdown должен
быть ограничен по времени.

## Retry policy и limits

HTTP transport success (`2xx`) отделён от sync DTO success (`PullResponse::ok`
или `PushResponse::ok`). Sync error может требовать protocol recovery, а не
blind resend. HTTP `408`, `425`, `429`, `500`, `502`, `503`, `504` retryable по
умолчанию; `400`, `401`, `403`, `413`, `415` permanent до изменения policy,
credential, route или payload. `http_sync_retry_hint()` разбирает только
relative `Retry-After: <delta-seconds>`.

Для WebSocket `1001`, `1005`, `1006`, `1011`–`1014` retryable; `1007`–`1009`
обычно permanent. `exchange_timeout` Simple-WebSocket покрывает connect, send и
wait; ноль отключает limit, отрицательное значение отвергается.

`CodecBounds` в ready-made bindings отвергает oversized request/response до
decode. `PullRequest::max_bytes` — soft page target; большой следующий batch
может всё же вернуться для progress. `max_single_batch_bytes` — hard limit;
превышение возвращает `BatchTooLarge` без page data.
Caller в этом случае увеличивает limit, уменьшает writer-side batch либо
использует out-of-band snapshot path.

`SyncWorkerPermanentFailurePolicy` относится только к classified transport
failure из `SyncTransportRetryHint`. Sync-level `SyncResponseErrorCode`
попадает в worker round result, stage event и status snapshot; retryability на
этом уровне означает protocol recovery (например, повторный pull от persistent
cursor после sequence gap), а не blind resend того же request.

`FixedWindowHttpRateLimitPolicy` третьим constructor argument может ограничить
число tracked client-identity bucket-ов. Нулевой cap сохраняет прежнее
unbounded поведение. При ненулевом cap сначала evict-ятся expired window-ы; если
освободить bucket нельзя, request отклоняется с `429` и `Retry-After`.

Concrete ready-made binding-и передают `CodecBounds` и отвергают oversized
request/response body до передачи байтов в `TransportMessageCodec`. Simple-Web
HTTP может отвергнуть oversized `Content-Length` ещё до копирования body в DTO;
при отсутствии пригодного header body всё равно проверяется после buffering.
Simple-WebSocket и Kurlyk/libcurl также проверяют до sync decode, однако их
underlying library уже могла полностью буферизовать frame либо response.

`ISyncPeer::last_retry_hint()` выдаёт последний transport advice. Успех очищает
старый hint; `available=true, retryable=false` означает classified permanent
transport failure. Hint рекомендателен: приложение может его ограничить либо
объединить с circuit breaker. Worker передаёт его в events/status и для
retryable relative `Retry-After` временно заменяет exponential delay в пределах
`max_backoff`.
Default unavailable hint означает, что peer не дал advice и caller использует
свою fallback retry policy. `KeepRetrying` сохраняет normal worker backoff;
`StopWorker` при classified permanent transport failure переводит background
worker в `Failed` вместо следующего backoff.

## Структурированное логирование

Логируйте стабильные поля: local/remote authenticated node, `db_id`, direction,
request/trace id, HTTP status/WS close code, числа pulled/applied/skipped/
rejected batches, worker stage и observed progress. Не логируйте raw key/value,
bearer token или целое serialized body. Полезные boundaries: transport receive,
policy decision, response/close, worker round, page received/applied, backoff,
cancellation и stop. Catch-up progress — estimate, а не ETA contract.
HTTP middleware сообщает incoming request context observer-ам до policy dispatch,
поэтому accepted и rejected request-ы могут нести request/trace id. WebSocket
middleware делает то же через `WebSocketSyncRequestContext`, когда concrete
binding заполнил adapter-local trace fields.

## Offline и corporate builds

`find_package(mdbx_containers)` сам transport dependency не fetch-ит; fetching
начинается только после `*_transport_provide()`. Для контролируемых builds
задавайте `FETCHCONTENT_SOURCE_DIR_<NAME>`, mirror Git URL вне пакета, vendor-те
dependency в parent project либо patch/fork helper. Application code линкует
только provider target, чтобы feature macro, include path, libraries и C++
requirements оставались в одном месте.

Provider helper-ы дают cache variables для pinned tag, но не для repository
URL. Предварительно созданного compatible dependency target недостаточно для
пропуска `FetchContent`: helper всё равно materializes ожидаемые source tree
до wiring final usage target.
