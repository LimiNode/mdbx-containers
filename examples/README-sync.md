# Sync Examples

These examples show the sync v0.1 API as transport-agnostic building blocks.
Most use `DirectSyncPeer` or a small in-memory buffer so the protocol flow is
visible without adding HTTP, WebSocket, or IPC code. The optional HTTP and
WebSocket examples use ready-made Simple-Web binding headers from
`mdbx_containers/sync/transports/simple_web/` over Simple-Web-Server,
Simple-WebSocket-Server, and standalone Asio. The Kurlyk example uses the
optional `mdbx_containers/sync/transports/kurlyk/` HTTP client binding over
libcurl.

Sync is opt-in. The examples are built with `MDBXC_SYNC_ENABLED=1`; applications
must also compile sync users with that macro enabled.

## Build and Run

```bash
cmake -S . -B tmp/build-examples \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=OFF \
    -DMDBXC_BUILD_EXAMPLES=ON \
    -DCMAKE_CXX_STANDARD=11

cmake --build tmp/build-examples --target sync_01_lifecycle_direct_peer
tmp/build-examples/bin/examples/sync_01_lifecycle_direct_peer
```

Replace the target name with any example from the table below. On Windows the
executable has the `.exe` suffix, for example:

```powershell
.\tmp\build-examples\bin\examples\sync_01_lifecycle_direct_peer.exe
```

The real HTTP binding examples are opt-in because they fetch standalone Asio,
Simple-Web-Server headers, and, for the process-supervised demo,
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

The reusable HTTP binding lives in:

```cpp
#if defined(MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT) && \
        MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT
#include <mdbx_containers/sync/transports/simple_web/HttpTransport.hpp>
#endif
```

The Kurlyk/libcurl HTTP client binding is also opt-in. It reuses the
framework-neutral `HttpSyncPeer` API and swaps only the concrete
`IHttpSyncClient` implementation. On Windows/MinGW, the example can fetch a
pinned ready-made Win64 libcurl package when
`MDBXC_KURLYK_HTTP_SYNC_MINGW_CURL_FALLBACK=ON` (the default):

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

The reusable Kurlyk HTTP client binding lives in:

```cpp
#if defined(MDBXC_HAS_KURLYK_HTTP_TRANSPORT) && \
        MDBXC_HAS_KURLYK_HTTP_TRANSPORT
#include <mdbx_containers/sync/transports/kurlyk/HttpTransport.hpp>
#endif
```

Targets that intentionally use both Simple-Web HTTP and WebSocket bindings can
include the backend umbrella:

```cpp
#if defined(MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT) && \
        MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT && \
        defined(MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT) && \
        MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT
#include <mdbx_containers/sync/transports/simple_web.hpp>
#endif
```

`MDBXC_SIMPLE_WEB_HTTP_TRANSPORT`,
`MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT`, and `MDBXC_KURLYK_HTTP_TRANSPORT`
enable the optional dependency targets and their backend smoke tests. The
`MDBXC_*_SYNC_EXAMPLE` options only add repository examples on top of those
backends; when examples are enabled, the old example options still enable the
matching backend for compatibility with older commands. Consumer targets get
`MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT`,
`MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT`, or
`MDBXC_HAS_KURLYK_HTTP_TRANSPORT` from the concrete dependency target they link.

With an installed package, use the exported provider functions to create those
targets:

```cmake
find_package(mdbx_containers CONFIG REQUIRED)

mdbx_containers_simple_web_http_transport_provide(OUT_TARGET http_target)
target_link_libraries(my_http_node PRIVATE ${http_target})

mdbx_containers_simple_web_websocket_transport_provide(OUT_TARGET ws_target)
target_link_libraries(my_ws_node PRIVATE ${ws_target})

mdbx_containers_kurlyk_http_transport_provide(OUT_TARGET kurlyk_target)
target_link_libraries(my_kurlyk_client PRIVATE ${kurlyk_target})
```

The returned targets enable `MDBXC_SYNC_ENABLED`, link the core package target,
and propagate the matching `MDBXC_HAS_*_TRANSPORT` macro.

The real WebSocket binding example is also opt-in because it fetches standalone
Asio and Simple-WebSocket-Server headers. Simple-WebSocket-Server uses OpenSSL
Crypto for the WebSocket handshake, so set `OPENSSL_ROOT_DIR` if CMake cannot
find OpenSSL automatically. On Windows/MinGW, the example can fetch a pinned
ready-made Win64 OpenSSL package when
`MDBXC_WEBSOCKET_SYNC_MINGW_OPENSSL_FALLBACK=ON` (the default):

The pinned fallback package exposes `lib/VC/x64/MD/libcrypto.lib`; this import
library layout has been validated with the MinGW-w64 toolchain used by the
project CI. The example uses plain `ws://`, not WSS/TLS, so only
`libcrypto-3-x64.dll` is copied next to the executable.

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

The reusable WebSocket binding lives in:

```cpp
#if defined(MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT) && \
        MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT
#include <mdbx_containers/sync/transports/simple_web/WebSocketTransport.hpp>
#endif
```

To use your own ready-made OpenSSL package instead, point CMake at the package
root that contains `include`, `lib`, and `bin`, for example:

```powershell
cmake -S . -B tmp/build-ws-example `
    -DMDBXC_DEPS_MODE=BUNDLED `
    -DMDBXC_BUILD_TESTS=OFF `
    -DMDBXC_BUILD_EXAMPLES=ON `
    -DMDBXC_WEBSOCKET_SYNC_EXAMPLE=ON `
    -DOPENSSL_ROOT_DIR=C:/deps/openssl-win64-v3.4.0 `
    -DCMAKE_CXX_STANDARD=11
```

## Reading Order

| Example | What it demonstrates | Level |
| --- | --- | --- |
| `sync_01_lifecycle_direct_peer.cpp` | One explicit write -> pull -> push -> read lifecycle. | Beginner |
| `sync_02_incremental_direct_peer.cpp` | Receiver cursor and incremental pulls. | Beginner |
| `sync_03_multi_table.cpp` | Supported table types and paginated pulls. | Intermediate |
| `sync_04_primary_to_replicas.cpp` | One primary, multiple independent replica cursors. | Intermediate |
| `sync_05_three_node_mesh.cpp` | Pairwise exchange without forwarding remote-origin batches. | Advanced |
| `sync_06_threaded_transport.cpp` | Thread ownership and an in-memory request/response buffer. | Advanced |
| `sync_07_worker_observer.cpp` | Background `SyncWorker` progress notifications through `ISyncWorkerObserver`. | Advanced |
| `sync_08_transport_boundary.cpp` | Pseudo-transport that exercises the `ISyncPeer` boundary contract (cancellation token + `request_cancel()`). | Advanced |
| `sync_09_transport_codec.cpp` | Versioned binary `TransportMessageCodec` for request/response DTOs. | Advanced |
| `sync_10_custom_transport.cpp` | Minimal custom `ISyncPeer` over encoded byte buffers. | Advanced |
| `sync_11_http_adapter.cpp` | Framework-neutral HTTP-shaped adapter over `TransportMessageCodec`. | Advanced |
| `sync_12_transport_middleware.cpp` | Allow-list, fixed-budget, and metrics middleware around transport adapters. | Advanced |
| `sync_13_http_simple_web_server.cpp` | Ready-made Simple-Web HTTP binding, direct pull/push, and socket-backed worker demo. | Advanced |
| `sync_14_websocket_adapter.cpp` | Framework-neutral WebSocket binary-message seam over `TransportMessageCodec`. | Advanced |
| `sync_15_http_policy_context.cpp` | Bearer-token, remote-address, and `Retry-After` HTTP policy context. | Advanced |
| `sync_16_worker_http_transport.cpp` | `SyncWorker` running through `HttpSyncPeer` and HTTP request-context policy. | Advanced |
| `sync_17_websocket_simple_web_server.cpp` | Ready-made Simple-WebSocket binding over standalone Asio. | Advanced |
| `sync_18_http_node_fleet.cpp` | Multi-process node fleet using the ready-made Simple-Web HTTP binding. | Advanced |
| `sync_19_kurlyk_http_client.cpp` | Ready-made Kurlyk/libcurl HTTP client binding against the Simple-Web sync listener. | Advanced |
| `sync_20_observability.cpp` | Worker and transport observer logs with trace IDs, progress, and retry hints. | Advanced |
| `sync_21_worker_guard_apply_hooks.cpp` | `SyncNodeSession` lifecycle plus remote apply observer hooks. | Intermediate |
| `sync_22_node_session_minimal.cpp` | Minimal application-facing `SyncNodeSession` recipe. | Beginner |
| `sync_23_key_value_logical_frame.cpp` | Explicit `KeyValueTable` logical capture -> frame codec -> replica apply path. | Advanced |

## Common Rules

- `NodeId` identifies one replication node. Generate it once, persist it, and
  reuse it after restart.
- `DbId` identifies one logical replicated database. All nodes replicating the
  same logical database must use the same `DbId`.
- Before local writes, attach `ThreadLocalChangeAccumulator` with
  `SyncCaptureScope` or the lower-level `attach_sync_capture()` /
  `detach_sync_capture()` pair. The scope form restores the previous capture
  sink automatically when the local write phase ends.
- Prefer `SyncCaptureScope` when a function owns a bounded write phase:
  examples include seeding rows, applying one local business operation, or
  temporarily overriding an existing sink in tests. Prefer manual
  `attach_sync_capture()` / `detach_sync_capture()` only when capture is part of
  a wider component lifecycle, such as a process-local writer service that
  attaches once after startup and detaches during shutdown.
- Nested `SyncCaptureScope` objects must end in strict LIFO order. An explicit
  out-of-order `detach()` is rejected, and raw `attach_sync_capture()` /
  `detach_sync_capture()` calls must not replace the connection sink while a
  scope still owns it.
- Supported table methods keep their normal API. You do not wrap each
  `insert`, `insert_or_assign`, `erase`, `reconcile`, or range erase in a sync
  call. The capture sink records supported write operations when their MDBX
  transaction commits. If you pass an explicit transaction through several
  supported table calls, those writes commit and replicate as one local batch.
  Independent calls without an explicit transaction remain independent local
  transactions and independent sync batches.
- Sync is not triggered by reads, searches, or range scans. It also does not
  contact another node during the local commit; a `SyncWorker` or explicit
  pull/push code sends already committed batches later through an `ISyncPeer`.
- `KeyValueTableLogicalAdapter` logical capture is currently an explicit opt-in
  path. `LogicalChangeFrameCodec` can carry those typed logical changes between
  application components, and the receiver applies them with
  `SyncEngine::apply_logical_frame_bytes()`. The normal `PullRequest`,
  `PullResponse`, `PushRequest`, and `PushResponse` transport DTOs remain
  raw-DBI only until logical transport capability negotiation is added.
  `LogicalChangeFrame` is a payload container, not a delivery protocol: retrying
  transports still need an outer contract for destination routing, ordering, and
  replay protection.
- The minimal `SyncNodeSession` recipe uses `DirectSyncPeer` to stay
  dependency-free. A real HTTP node keeps the same session shape and replaces
  only the peer with a ready-made HTTP transport binding, for example
  Simple-Web HTTP or Kurlyk HTTP.
- `PullRequest`, `PullResponse`, `PushRequest`, and `PushResponse` are
  transport payload structs. Serialize and deliver these values through the
  transport used by your application.
- `TransportMessageCodec` is the built-in versioned binary codec for those
  DTOs. It serializes request/response data, not local cancellation state.
- Do not pass `Connection`, `Transaction`, table objects, raw MDBX handles, or
  cursors across threads or processes.
- A receiver applies a page with `SyncEngine::handle_push()`. One page is
  applied in one local transaction; a multi-page pull is not one global
  transaction.
- The receiver's applied cursor is persisted in its MDBX metadata. The next
  pull should use that cursor to avoid replaying already applied batches.
- Remote apply updates user tables but does not rewrite the remote batch into
  the receiver's local changelog. A simple node is not a forwarding relay.
- Sync v0.1 supports `KeyValueTable`, `KeyTable`, `ValueTable`,
  `SequenceTable`, and `VectorStore` through its internal supported tables.
  `AnyValueTable`, `KeyMultiValueTable`, and `HashedKeyValueStore` are not
  replicated in v0.1. `KeyMultiValueTable` has a deferred unordered multiset
  design for single-writer or causally serialized updates in `sync/DESIGN.md`;
  concurrent multi-writer conflicts and ordered distributed histories are
  deferred to future work such as `KeyOrderedMultiValueTable<K, V>`. Capture
  remains disabled until the implementation and round-trip tests land.

## Transport Boundary

`DirectSyncPeer` is useful for tests, examples, and in-process demos. Production
code should treat the request/response structs as transport payloads:

```text
replica builds PullRequest
-> transport sends it to the source node
-> source calls SyncEngine::handle_pull()
-> transport returns PullResponse
-> replica wraps batches in PushRequest
-> replica calls SyncEngine::handle_push()
```

The threaded example keeps each `Connection` and `SyncEngine` on the thread that
owns its database. The shared buffer carries only sync request/response values.
That is the boundary to preserve when replacing the buffer with a real
transport.

`sync_07_worker_observer.cpp` shows the application side of a background
replica: `SyncWorker` performs pull/apply rounds, while `ISyncWorkerObserver`
notifies foreground code when pull/apply stages change, pages are applied, or
rounds finish. Stage events include a best-effort catch-up progress estimate
based on the latest remote cursor.

`sync_08_transport_boundary.cpp` formalises the contract that any transport
adapter over `ISyncPeer` must respect. It implements a tiny in-memory
adapter that owns no MDBX handles and demonstrates where the
`CancellationToken` from `PullRequest` is observed and where
`ISyncPeer::request_cancel()` closes the in-flight call. The ready-made
Simple-Web HTTP/WebSocket bindings use the same pattern, swapping the in-memory
queues for sockets.

`sync_09_transport_codec.cpp` shows the next layer down: `PullRequest`,
`PullResponse`, `PushRequest`, and `PushResponse` are encoded into byte buffers
with `TransportMessageCodec` before they cross the boundary. Adapter policy
such as authorization, rate limits, allow/deny lists, routing, and TLS belongs
around that byte exchange, not inside the sync DTOs themselves.

`sync_10_custom_transport.cpp` shows the minimal custom transport shape:
implement `ISyncPeer`, encode request DTOs, send bytes through the application
channel, decode response DTOs, and implement `request_cancel()` with the
channel's own interruption primitive.

`sync_11_http_adapter.cpp` shows the HTTP-shaped adapter seam. `HttpSyncPeer`
implements `ISyncPeer` over an abstract `IHttpSyncClient`, and
`HttpSyncServer` dispatches already-parsed HTTP method/target/content-type/body
values to `SyncEngine`. A real HTTP library supplies the socket layer around
that seam; `mdbxc::sync::simple_web::HttpSyncClient` and
`mdbxc::sync::simple_web::HttpSyncListener` are the optional Simple-Web
implementation shipped with these examples.

`sync_14_websocket_adapter.cpp` shows the WebSocket-shaped adapter seam.
`WebSocketSyncPeer` implements `ISyncPeer` over an abstract
`IWebSocketSyncChannel`, and `WebSocketSyncServer` dispatches complete binary
messages to `SyncEngine`. A real WebSocket library supplies connection setup,
fragment reassembly, ping/pong, backpressure, and close/error mapping around
that seam; `mdbxc::sync::simple_web::WebSocketSyncChannel` and
`mdbxc::sync::simple_web::WebSocketSyncListener` are the optional
Simple-WebSocket implementation shipped with these examples.

`sync_12_transport_middleware.cpp` shows adapter-local policy wrappers.
`SyncPeerMiddleware` can inspect decoded `NodeId` / `DbId` values before a peer
call, while `HttpSyncClientMiddleware` can enforce route-level policy around an
HTTP-shaped byte exchange. These wrappers are where simple allow-lists,
fixed-budget rate limits, and metrics hooks belong; they do not add auth tokens
or counters to the sync DTO wire format.
They do not replace server-framework authentication or per-remote-client rate
limits before `HttpSyncServer::handle()`. Metrics count middleware hook
invocations, so a shared observer installed at several stacked layers can count
one logical action once per layer.

`sync_15_http_policy_context.cpp` shows the request-context layer a concrete
HTTP server binding can run before `HttpSyncServer::handle()`. It extracts a
bearer token from headers, checks the remote address, and returns a `Retry-After`
header when the fixed-window limiter rejects a request.

`sync_16_worker_http_transport.cpp` combines the background worker with the
HTTP-shaped adapter inside one process. The replica owns `SyncWorker` and
`HttpSyncPeer`; the primary-side middleware authenticates the bearer token as
the replica `NodeId` before `HttpSyncServer` dispatches the request.

`sync_13_http_simple_web_server.cpp worker-demo` runs the same worker shape
through real loopback HTTP sockets. It uses bearer identity, a message-size
guard, paginated pulls, and worker observer callbacks over
`mdbxc::sync::simple_web::HttpSyncClient` and
`mdbxc::sync::simple_web::HttpSyncListener`.

`sync_18_http_node_fleet.cpp` runs several copies of the same executable through
tiny-process-library. Each child process owns its own MDBX environment, ready
Simple-Web HTTP listener/client, sync engine, capture sink, and application
loop; the parent process only starts nodes and sends `pause` / `stop` commands
through stdin. The `master-replica` mode shows one active writer and one passive
receiver. The `mesh` mode lets both nodes write locally while each node pulls
from the other over HTTP.

`sync_19_kurlyk_http_client.cpp` keeps the server side on the ready
Simple-Web HTTP listener and swaps the client-side backend to
`mdbxc::sync::kurlyk::HttpSyncClient`. This demonstrates the intended
adapter shape for additional HTTP clients: implement `IHttpSyncClient`, pass it
to `HttpSyncPeer`, and leave `SyncEngine`, DTO encoding, auth policy, and local
apply unchanged.

`sync_20_observability.cpp` demonstrates production-style logging hooks without
opening sockets. It runs `SyncWorker` over `HttpSyncPeer`, adds request and
trace IDs in the HTTP-shaped client, logs incoming request context through
`ISyncTransportObserver`, logs worker progress through `ISyncWorkerObserver`,
and shows a one-time `429 Retry-After` rejection turning into a worker backoff
hint.

`sync_17_websocket_simple_web_server.cpp` is the socket-backed WebSocket
counterpart. It uses `mdbxc::sync::simple_web::WebSocketSyncChannel` and
`mdbxc::sync::simple_web::WebSocketSyncListener`, sends binary frames, checks
the bearer token during the WebSocket handshake, passes the authenticated
replica `NodeId` plus explicit `SyncDbAccess` to
`WebSocketSyncServerMiddleware`, rejects oversized messages before decode, and
classifies close codes for client diagnostics.
