#include <mdbx_containers.hpp>
#include <mdbx_containers/sync.hpp>

#include <chrono>
#include <cstdio>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId node{};
    for (int i = 0; i < 16; ++i) {
        node[i] = static_cast<std::uint8_t>(seed + i);
    }
    return node;
}

mdbxc::sync::ChangeBatch make_batch(std::uint8_t seed, std::uint64_t seq) {
    mdbxc::sync::ChangeBatch batch;
    batch.origin_node_id = make_node(seed);
    batch.seq = seq;
    return batch;
}

void require_true(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void cleanup(const std::string& path) {
    std::remove(path.c_str());
    std::remove((path + "-lck").c_str());
}

mdbxc::Config config(const std::string& path) {
    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.no_subdir = true;
    cfg.max_dbs = 32;
    return cfg;
}

std::shared_ptr<mdbxc::Connection> open_db(const std::string& path) {
    return mdbxc::Connection::create(config(path));
}

class RecordingPeer : public mdbxc::sync::ISyncPeer {
public:
    RecordingPeer()
        : m_pull_count(0),
          m_push_count(0),
          m_cancel_count(0),
          m_logical_recovery_count(0),
          m_logical_recovery_token_cancellable(false) {}

    mdbxc::sync::PullResponse pull(
            const mdbxc::sync::PullRequest& request) override {
        ++m_pull_count;
        m_last_pull = request;
        mdbxc::sync::PullResponse response;
        response.batches.push_back(make_batch(0xA0, m_pull_count));
        return response;
    }

    mdbxc::sync::PushResponse push(
            const mdbxc::sync::PushRequest& request) override {
        ++m_push_count;
        m_last_push = request;
        mdbxc::sync::PushResponse response;
        response.receiver_have.last_seq_by_origin[request.sender] =
            request.batches.size();
        return response;
    }

    void request_cancel() override {
        ++m_cancel_count;
    }

    bool supports_logical_delivery() const override { return true; }

    bool supports_logical_recovery() const override { return true; }

    mdbxc::sync::LogicalRecoveryResponse logical_recovery(
            const mdbxc::sync::LogicalRecoveryRequest& request) override {
        return logical_recovery_with_cancel(request, nullptr);
    }

    mdbxc::sync::LogicalRecoveryResponse logical_recovery_with_cancel(
            const mdbxc::sync::LogicalRecoveryRequest& request,
            const mdbxc::sync::CancellationToken* cancel_token = nullptr) override {
        ++m_logical_recovery_count;
        m_last_logical_recovery = request;
        m_logical_recovery_token_cancellable =
            cancel_token != nullptr && cancel_token->can_be_cancelled();
        mdbxc::sync::LogicalRecoveryResponse response;
        response.ok = false;
        response.error = "recorded logical recovery";
        response.error_code =
            mdbxc::sync::SyncResponseErrorCode::SnapshotNotConfigured;
        return response;
    }

    std::size_t pull_count() const { return m_pull_count; }
    std::size_t push_count() const { return m_push_count; }
    std::size_t cancel_count() const { return m_cancel_count; }
    std::size_t logical_recovery_count() const {
        return m_logical_recovery_count;
    }
    bool logical_recovery_token_cancellable() const {
        return m_logical_recovery_token_cancellable;
    }

private:
    std::size_t m_pull_count;
    std::size_t m_push_count;
    std::size_t m_cancel_count;
    std::size_t m_logical_recovery_count;
    bool m_logical_recovery_token_cancellable;
    mdbxc::sync::PullRequest m_last_pull;
    mdbxc::sync::PushRequest m_last_push;
    mdbxc::sync::LogicalRecoveryRequest m_last_logical_recovery;
};

class RecordingHttpClient : public mdbxc::sync::IHttpSyncClient {
public:
    RecordingHttpClient() : m_post_count(0), m_cancel_count(0) {}

    mdbxc::sync::HttpSyncResponse post(
            const std::string& target,
            const std::string& content_type,
            const std::vector<std::uint8_t>& body,
            const mdbxc::sync::CancellationToken& cancel_token) override {
        (void)cancel_token;
        ++m_post_count;
        m_last_target = target;
        mdbxc::sync::HttpSyncResponse response;
        response.status_code = 200;
        response.content_type = content_type;
        response.body = body;
        return response;
    }

    void request_cancel() override {
        ++m_cancel_count;
    }

    std::size_t post_count() const { return m_post_count; }
    std::size_t cancel_count() const { return m_cancel_count; }
    const std::string& last_target() const { return m_last_target; }

private:
    std::size_t m_post_count;
    std::size_t m_cancel_count;
    std::string m_last_target;
};

class ThrowingObserver : public mdbxc::sync::ISyncTransportObserver {
public:
    void on_sync_transport_http_request(
            const mdbxc::sync::HttpSyncRequest& request) override {
        (void)request;
        throw std::runtime_error("observer request failure");
    }

    void on_sync_transport_pull_result(
            const mdbxc::sync::PullRequest& request,
            const mdbxc::sync::PullResponse& response) override {
        (void)request;
        (void)response;
        throw std::runtime_error("observer failure");
    }

    void on_sync_transport_rejected(
            mdbxc::sync::SyncTransportOperation operation,
            const std::string& error) override {
        (void)operation;
        (void)error;
        throw std::runtime_error("observer rejection failure");
    }
};

class RequestContextObserver : public mdbxc::sync::ISyncTransportObserver {
public:
    RequestContextObserver()
        : m_http_requests(0), m_websocket_messages(0) {}

    void on_sync_transport_http_request(
            const mdbxc::sync::HttpSyncRequest& request) override {
        ++m_http_requests;
        m_http_trace = mdbxc::sync::http_sync_trace_context(request);
    }

    void on_sync_transport_websocket_message(
            const mdbxc::sync::WebSocketSyncRequestContext& request) override {
        ++m_websocket_messages;
        m_websocket_trace =
            mdbxc::sync::websocket_sync_trace_context(request);
    }

    std::size_t http_requests() const { return m_http_requests; }
    std::size_t websocket_messages() const {
        return m_websocket_messages;
    }
    const mdbxc::sync::SyncTransportTraceContext& http_trace() const {
        return m_http_trace;
    }
    const mdbxc::sync::SyncTransportTraceContext& websocket_trace() const {
        return m_websocket_trace;
    }

private:
    std::size_t m_http_requests;
    std::size_t m_websocket_messages;
    mdbxc::sync::SyncTransportTraceContext m_http_trace;
    mdbxc::sync::SyncTransportTraceContext m_websocket_trace;
};

void test_peer_middleware_allows_limits_and_observes() {
    const mdbxc::sync::NodeId requester = make_node(0x10);
    const mdbxc::sync::DbId db_id = make_node(0x20);

    mdbxc::sync::NodeDbAllowListPolicy allow_list;
    allow_list.allow_node_id(requester);
    allow_list.allow_db_id(db_id);

    mdbxc::sync::FixedBudgetSyncTransportPolicy budget(1, 1);
    mdbxc::sync::CompositeSyncTransportPolicy policy;
    policy.add(allow_list);
    policy.add(budget);

    mdbxc::sync::SyncTransportMetricsObserver metrics;
    RecordingPeer peer;
    mdbxc::sync::SyncPeerMiddleware wrapped(peer, &policy, &metrics);

    mdbxc::sync::PullRequest pull;
    pull.requester = requester;
    pull.db_id = db_id;
    mdbxc::sync::PullResponse pulled = wrapped.pull(pull);
    require_true(pulled.ok, "allowed pull was rejected");
    require_true(peer.pull_count() == 1u, "allowed pull was not forwarded");

    pulled = wrapped.pull(pull);
    require_true(!pulled.ok, "rate-limited pull was allowed");
    require_true(peer.pull_count() == 1u,
                 "rate-limited pull reached downstream peer");

    mdbxc::sync::PushRequest push;
    push.sender = requester;
    push.db_id = db_id;
    push.batches.push_back(make_batch(0x10, 1));
    mdbxc::sync::PushResponse pushed = wrapped.push(push);
    require_true(pushed.ok, "allowed push was rejected");
    require_true(peer.push_count() == 1u, "allowed push was not forwarded");

    push.db_id = make_node(0x99);
    pushed = wrapped.push(push);
    require_true(!pushed.ok, "db-denied push was allowed");
    require_true(peer.push_count() == 1u,
                 "db-denied push reached downstream peer");

    wrapped.request_cancel();
    require_true(peer.cancel_count() == 1u,
                 "request_cancel was not forwarded");

    const mdbxc::sync::SyncTransportMetricsSnapshot snapshot =
        metrics.snapshot();
    require_true(snapshot.pull_calls == 1u, "pull metric mismatch");
    require_true(snapshot.push_calls == 1u, "push metric mismatch");
    require_true(snapshot.rejected_calls == 2u, "rejection metric mismatch");
    require_true(snapshot.request_cancel_calls == 1u,
                 "cancel metric mismatch");
    require_true(snapshot.pulled_batches == 1u,
                 "pulled batch metric mismatch");
    require_true(snapshot.pushed_batches == 1u,
                 "pushed batch metric mismatch");
}

void test_peer_middleware_forwards_logical_capabilities() {
    RecordingPeer peer;
    mdbxc::sync::SyncTransportMetricsObserver metrics;
    mdbxc::sync::SyncPeerMiddleware wrapped(peer, nullptr, &metrics);
    require_true(wrapped.supports_logical_delivery(),
                 "peer middleware lost logical delivery capability");
    require_true(wrapped.supports_logical_recovery(),
                 "peer middleware lost logical recovery capability");

    mdbxc::sync::LogicalRecoveryRequest request;
    request.requester = make_node(0x33);
    request.db_id = make_node(0xD3);
    mdbxc::sync::CancellationSource cancellation;
    const mdbxc::sync::CancellationToken token = cancellation.token();
    const mdbxc::sync::LogicalRecoveryResponse response =
        wrapped.logical_recovery_with_cancel(request, &token);

    require_true(!response.ok, "recording logical recovery unexpectedly succeeded");
    require_true(peer.logical_recovery_count() == 1u,
                 "peer middleware did not forward logical recovery");
    require_true(peer.logical_recovery_token_cancellable(),
                 "peer middleware did not preserve logical recovery cancellation");

    const mdbxc::sync::SyncTransportMetricsSnapshot snapshot =
        metrics.snapshot();
    require_true(snapshot.logical_recovery_calls == 1u,
                 "logical recovery metric mismatch");
    require_true(snapshot.failed_calls == 1u,
                 "logical recovery failure metric mismatch");
}

void test_peer_middleware_enforces_logical_recovery_policy() {
    const mdbxc::sync::NodeId requester = make_node(0x34);
    const mdbxc::sync::DbId allowed_db_id = make_node(0xD4);

    mdbxc::sync::NodeDbAllowListPolicy allow_list;
    allow_list.allow_node_id(requester);
    allow_list.allow_db_id(allowed_db_id);
    mdbxc::sync::CompositeSyncTransportPolicy policy;
    policy.add(allow_list);

    RecordingPeer peer;
    mdbxc::sync::SyncTransportMetricsObserver metrics;
    mdbxc::sync::SyncPeerMiddleware wrapped(peer, &policy, &metrics);

    mdbxc::sync::LogicalRecoveryRequest request;
    request.requester = requester;
    request.db_id = make_node(0xD5);
    const mdbxc::sync::LogicalRecoveryResponse rejected =
        wrapped.logical_recovery(request);
    require_true(!rejected.ok, "db-denied logical recovery was allowed");
    require_true(peer.logical_recovery_count() == 0u,
                 "db-denied logical recovery reached downstream peer");

    request.db_id = allowed_db_id;
    const mdbxc::sync::LogicalRecoveryResponse allowed =
        wrapped.logical_recovery(request);
    require_true(!allowed.ok,
                 "recording logical recovery unexpectedly succeeded");
    require_true(peer.logical_recovery_count() == 1u,
                 "allowed logical recovery was not forwarded");

    const mdbxc::sync::SyncTransportMetricsSnapshot snapshot =
        metrics.snapshot();
    require_true(snapshot.rejected_calls == 1u,
                 "logical recovery rejection metric mismatch");
    require_true(snapshot.logical_recovery_calls == 1u,
                 "allowed logical recovery was not observed");
}

void test_transport_trace_context_helpers() {
    mdbxc::sync::HttpSyncRequest request;
    mdbxc::sync::http_add_header(
        request.headers, mdbxc::sync::HttpSyncHeaders::request_id(),
        "http-request");
    mdbxc::sync::http_add_header(
        request.headers, mdbxc::sync::HttpSyncHeaders::trace_id(),
        "http-trace");

    mdbxc::sync::SyncTransportTraceContext trace =
        mdbxc::sync::http_sync_trace_context(request);
    require_true(!trace.empty(), "HTTP trace context must not be empty");
    require_true(trace.request_id == "http-request",
                 "HTTP request id was not extracted");
    require_true(trace.trace_id == "http-trace",
                 "HTTP trace id was not extracted");

    mdbxc::sync::WebSocketSyncRequestContext websocket;
    websocket.request_id = "ws-request";
    websocket.trace_id = "ws-trace";
    trace = mdbxc::sync::websocket_sync_trace_context(websocket);
    require_true(trace.request_id == "ws-request",
                 "WebSocket request id was not extracted");
    require_true(trace.trace_id == "ws-trace",
                 "WebSocket trace id was not extracted");
}

void test_transport_observer_receives_request_context() {
    const std::string http_path =
        "test_transport_middleware_http_trace.mdbx";
    cleanup(http_path);

    std::shared_ptr<mdbxc::Connection> http_db = open_db(http_path);
    mdbxc::sync::SyncEngine http_engine(http_db);
    mdbxc::sync::HttpSyncServer http_server(http_engine);
    RequestContextObserver observer;
    mdbxc::sync::HttpSyncServerMiddleware http_middleware(
        http_server, nullptr, &observer);

    mdbxc::sync::HttpSyncRequest http_request;
    http_request.method = "GET";
    http_request.target = mdbxc::sync::HttpSyncRoutes::pull_target();
    mdbxc::sync::http_add_header(
        http_request.headers, mdbxc::sync::HttpSyncHeaders::request_id(),
        "middleware-http-request");
    mdbxc::sync::http_add_header(
        http_request.headers, mdbxc::sync::HttpSyncHeaders::trace_id(),
        "middleware-http-trace");

    const mdbxc::sync::HttpSyncResponse http_response =
        http_middleware.handle(http_request);
    require_true(http_response.status_code == 405,
                 "HTTP middleware trace test expected method rejection");
    require_true(observer.http_requests() == 1u,
                 "observer did not see HTTP request context");
    require_true(observer.http_trace().request_id ==
                     "middleware-http-request",
                 "observer did not receive HTTP request id");
    require_true(observer.http_trace().trace_id ==
                     "middleware-http-trace",
                 "observer did not receive HTTP trace id");

    http_db->disconnect();
    cleanup(http_path);

    const std::string ws_path =
        "test_transport_middleware_ws_trace.mdbx";
    cleanup(ws_path);

    std::shared_ptr<mdbxc::Connection> ws_db = open_db(ws_path);
    mdbxc::sync::SyncEngine ws_engine(ws_db);
    mdbxc::sync::WebSocketSyncServer ws_server(ws_engine);
    mdbxc::sync::TransportMessageSizePolicy reject_oversized(1u);
    mdbxc::sync::WebSocketSyncServerMiddleware ws_middleware(
        ws_server, &reject_oversized, &observer);

    mdbxc::sync::WebSocketSyncRequestContext ws_request;
    ws_request.request_id = "middleware-ws-request";
    ws_request.trace_id = "middleware-ws-trace";
    ws_request.binary_message.push_back(0x42u);
    ws_request.binary_message.push_back(0x43u);

    bool rejected = false;
    try {
        (void)ws_middleware.handle_binary_message(ws_request);
    } catch (const mdbxc::sync::WebSocketSyncRejected&) {
        rejected = true;
    }
    require_true(rejected,
                 "WebSocket middleware trace test expected rejection");
    require_true(observer.websocket_messages() == 1u,
                 "observer did not see WebSocket message context");
    require_true(observer.websocket_trace().request_id ==
                     "middleware-ws-request",
                 "observer did not receive WebSocket request id");
    require_true(observer.websocket_trace().trace_id ==
                     "middleware-ws-trace",
                 "observer did not receive WebSocket trace id");

    ws_db->disconnect();
    cleanup(ws_path);
}

void test_observer_exceptions_are_swallowed() {
    RecordingPeer peer;
    ThrowingObserver observer;
    mdbxc::sync::SyncPeerMiddleware wrapped(peer, nullptr, &observer);

    mdbxc::sync::PullRequest pull;
    pull.requester = make_node(0x50);
    pull.db_id = make_node(0x51);

    const mdbxc::sync::PullResponse response = wrapped.pull(pull);
    require_true(response.ok, "observer exception changed pull result");
    require_true(peer.pull_count() == 1u,
                 "observer exception prevented downstream pull");

    mdbxc::sync::NodeDbAllowListPolicy deny_db;
    deny_db.allow_db_id(make_node(0x99));
    mdbxc::sync::SyncPeerMiddleware denying(peer, &deny_db, &observer);
    const mdbxc::sync::PullResponse denied = denying.pull(pull);
    require_true(!denied.ok, "observer rejection exception allowed pull");
    require_true(peer.pull_count() == 1u,
                 "denied pull reached downstream peer");
}

void test_http_client_middleware_route_policy() {
    mdbxc::sync::HttpRouteAllowListPolicy route_policy;
    route_policy.allow_target(mdbxc::sync::HttpSyncRoutes::pull_target());

    mdbxc::sync::SyncTransportMetricsObserver metrics;
    RecordingHttpClient client;
    mdbxc::sync::HttpSyncClientMiddleware wrapped(
        client, &route_policy, &metrics);

    const std::vector<std::uint8_t> body(3u, 0x42u);
    mdbxc::sync::HttpSyncResponse response = wrapped.post(
        mdbxc::sync::HttpSyncRoutes::pull_target(),
        mdbxc::sync::HttpSyncRoutes::content_type(),
        body,
        mdbxc::sync::CancellationToken());
    require_true(response.status_code == 200, "allowed HTTP post failed");
    require_true(client.post_count() == 1u, "HTTP post was not forwarded");

    response = wrapped.post(
        "/forbidden",
        mdbxc::sync::HttpSyncRoutes::content_type(),
        body,
        mdbxc::sync::CancellationToken());
    require_true(response.status_code == 403,
                 "forbidden HTTP route was not rejected");
    require_true(client.post_count() == 1u,
                 "forbidden HTTP route reached downstream client");

    wrapped.request_cancel();
    require_true(client.cancel_count() == 1u,
                 "HTTP request_cancel was not forwarded");

    const mdbxc::sync::SyncTransportMetricsSnapshot snapshot =
        metrics.snapshot();
    require_true(snapshot.http_post_calls == 1u,
                 "HTTP post metric mismatch");
    require_true(snapshot.rejected_calls == 1u,
                 "HTTP rejection metric mismatch");
    require_true(snapshot.request_cancel_calls == 1u,
                 "HTTP cancel metric mismatch");
}

void test_http_client_middleware_budget_policy() {
    mdbxc::sync::FixedBudgetSyncTransportPolicy budget(
        mdbxc::sync::FixedBudgetSyncTransportPolicy::unlimited_budget(),
        mdbxc::sync::FixedBudgetSyncTransportPolicy::unlimited_budget(),
        1);
    RecordingHttpClient client;
    mdbxc::sync::HttpSyncClientMiddleware wrapped(client, &budget);

    const std::vector<std::uint8_t> body(1u, 0x11u);
    mdbxc::sync::HttpSyncResponse response = wrapped.post(
        mdbxc::sync::HttpSyncRoutes::pull_target(),
        mdbxc::sync::HttpSyncRoutes::content_type(),
        body,
        mdbxc::sync::CancellationToken());
    require_true(response.status_code == 200,
                 "first budgeted HTTP post failed");

    response = wrapped.post(
        mdbxc::sync::HttpSyncRoutes::pull_target(),
        mdbxc::sync::HttpSyncRoutes::content_type(),
        body,
        mdbxc::sync::CancellationToken());
    require_true(response.status_code == 429,
                 "rate-limited HTTP post was not rejected");
    require_true(client.post_count() == 1u,
                 "rate-limited HTTP post reached downstream client");
}

void test_http_context_policies() {
    mdbxc::sync::HttpSyncRequest request;
    request.method = mdbxc::sync::HttpSyncRoutes::method_post();
    request.target = mdbxc::sync::HttpSyncRoutes::pull_target();
    request.content_type = mdbxc::sync::HttpSyncRoutes::content_type();
    request.remote_address = "127.0.0.1";
    mdbxc::sync::http_add_header(
        request.headers, "Authorization", "Bearer token-a");

    require_true(mdbxc::sync::http_header_value(
                     request.headers, "authorization") == "Bearer token-a",
                 "HTTP header lookup must be case-insensitive");
    require_true(mdbxc::sync::http_bearer_token(request) == "token-a",
                 "bearer token extraction mismatch");

    mdbxc::sync::HttpBearerTokenPolicy bearer;
    bearer.allow_token("token-a");
    mdbxc::sync::HttpRemoteAddressAllowListPolicy remote;
    remote.allow_remote_address("127.0.0.1");
    mdbxc::sync::FixedWindowHttpRateLimitPolicy rate(
        1, std::chrono::seconds(10));

    mdbxc::sync::CompositeSyncTransportPolicy policy;
    policy.add(bearer);
    policy.add(remote);
    policy.add(rate);

    mdbxc::sync::SyncTransportDecision decision =
        policy.check_http_request(request);
    require_true(decision.allowed, "allowed HTTP context was rejected");

    decision = policy.check_http_request(request);
    require_true(!decision.allowed && decision.status_code == 429,
                 "HTTP rate limit did not reject second request");
    require_true(mdbxc::sync::http_header_value(
                     decision.response_headers, "Retry-After") !=
                     std::string(),
                 "HTTP rate-limit rejection must include Retry-After");

    mdbxc::sync::HttpSyncRequest missing_token = request;
    missing_token.headers.clear();
    decision = bearer.check_http_request(missing_token);
    require_true(!decision.allowed && decision.status_code == 401,
                 "missing bearer token must be unauthorized");
    require_true(mdbxc::sync::http_header_value(
                     decision.response_headers, "WWW-Authenticate") !=
                     std::string(),
                 "bearer rejection must include WWW-Authenticate");

    request.remote_address = "203.0.113.7";
    decision = remote.check_http_request(request);
    require_true(!decision.allowed && decision.status_code == 403,
                 "denied remote address was not rejected");
}

void test_http_correlation_copy_is_idempotent() {
    std::vector<mdbxc::sync::HttpSyncHeader> source;
    std::vector<mdbxc::sync::HttpSyncHeader> destination;

    mdbxc::sync::http_add_header(
        source, mdbxc::sync::HttpSyncHeaders::request_id(),
        "source-request");
    mdbxc::sync::http_add_header(
        source, mdbxc::sync::HttpSyncHeaders::trace_id(),
        "source-trace");
    mdbxc::sync::http_add_header(
        destination, "x-mdbxc-sync-request-id",
        "existing-request");

    mdbxc::sync::http_copy_sync_correlation_headers(source, destination);
    mdbxc::sync::http_copy_sync_correlation_headers(source, destination);

    std::size_t request_id_count = 0;
    std::size_t trace_id_count = 0;
    for (std::size_t i = 0; i < destination.size(); ++i) {
        if (mdbxc::sync::http_header_name_equals(
                destination[i].name,
                mdbxc::sync::HttpSyncHeaders::request_id())) {
            ++request_id_count;
        }
        if (mdbxc::sync::http_header_name_equals(
                destination[i].name,
                mdbxc::sync::HttpSyncHeaders::trace_id())) {
            ++trace_id_count;
        }
    }

    require_true(request_id_count == 1u,
                 "correlation copy duplicated request id");
    require_true(trace_id_count == 1u,
                 "correlation copy duplicated trace id");
    require_true(mdbxc::sync::http_header_value(
                     destination,
                     mdbxc::sync::HttpSyncHeaders::request_id()) ==
                     "existing-request",
                 "correlation copy replaced existing request id");
    require_true(mdbxc::sync::http_header_value(
                     destination,
                     mdbxc::sync::HttpSyncHeaders::trace_id()) ==
                     "source-trace",
                 "correlation copy did not add trace id");
}

void test_http_bearer_node_identity_policy() {
    const mdbxc::sync::NodeId node_a = make_node(0x11);
    const mdbxc::sync::NodeId node_b = make_node(0x22);
    const mdbxc::sync::DbId db_a = make_node(0xD1);
    const mdbxc::sync::DbId db_b = make_node(0xD2);

    mdbxc::sync::HttpBearerNodeIdentityPolicy policy;
    policy.allow_token_for_node("token-a", node_a);

    mdbxc::sync::PullRequest pull;
    pull.requester = node_a;
    pull.db_id = db_a;

    mdbxc::sync::HttpSyncRequest request;
    request.method = mdbxc::sync::HttpSyncRoutes::method_post();
    request.target = mdbxc::sync::HttpSyncRoutes::pull_target();
    request.content_type = mdbxc::sync::HttpSyncRoutes::content_type();
    request.body = mdbxc::sync::TransportMessageCodec::encode_pull_request(
        pull);
    mdbxc::sync::http_add_header(
        request.headers, "Authorization", "Bearer token-a");

    mdbxc::sync::SyncTransportDecision decision =
        policy.check_http_request(request);
    require_true(decision.allowed,
                 "matching bearer identity was rejected");

    pull.db_id = db_b;
    request.body = mdbxc::sync::TransportMessageCodec::encode_pull_request(
        pull);
    decision = policy.check_http_request(request);
    require_true(decision.allowed,
                 "default bearer DB access should allow any db_id");

    policy.allow_db_id_for_token("token-a", db_a);
    pull.db_id = db_a;
    request.body = mdbxc::sync::TransportMessageCodec::encode_pull_request(
        pull);
    decision = policy.check_http_request(request);
    require_true(decision.allowed,
                 "restricted bearer DB access rejected allowed db_id");

    pull.requester = node_b;
    request.body = mdbxc::sync::TransportMessageCodec::encode_pull_request(
        pull);
    decision = policy.check_http_request(request);
    require_true(!decision.allowed && decision.status_code == 403,
                 "requester mismatch was not rejected");

    pull.requester = node_a;
    pull.db_id = db_b;
    request.body = mdbxc::sync::TransportMessageCodec::encode_pull_request(
        pull);
    decision = policy.check_http_request(request);
    require_true(!decision.allowed && decision.status_code == 403,
                 "db_id mismatch was not rejected");

    mdbxc::sync::PushRequest push;
    push.sender = node_b;
    push.db_id = db_a;
    request.target = mdbxc::sync::HttpSyncRoutes::push_target();
    request.body = mdbxc::sync::TransportMessageCodec::encode_push_request(
        push);
    decision = policy.check_http_request(request);
    require_true(!decision.allowed && decision.status_code == 403,
                 "sender mismatch was not rejected");

    mdbxc::sync::LogicalRecoveryRequest recovery;
    recovery.requester = node_a;
    recovery.db_id = db_a;
    request.target = mdbxc::sync::HttpSyncRoutes::logical_recovery_target();
    request.body = mdbxc::sync::LogicalRecoveryProtocolCodec::encode_request(
        recovery);
    decision = policy.check_http_request(request);
    require_true(decision.allowed,
                 "matching logical recovery requester was rejected");

    recovery.db_id = db_b;
    request.body = mdbxc::sync::LogicalRecoveryProtocolCodec::encode_request(
        recovery);
    decision = policy.check_http_request(request);
    require_true(!decision.allowed && decision.status_code == 403,
                 "logical recovery db_id mismatch was not rejected");

    recovery.requester = node_b;
    recovery.db_id = db_a;
    request.body = mdbxc::sync::LogicalRecoveryProtocolCodec::encode_request(
        recovery);
    decision = policy.check_http_request(request);
    require_true(!decision.allowed && decision.status_code == 403,
                 "logical recovery requester mismatch was not rejected");

    request.headers.clear();
    decision = policy.check_http_request(request);
    require_true(!decision.allowed && decision.status_code == 401,
                 "missing bearer identity was not rejected");
    require_true(mdbxc::sync::http_header_value(
                     decision.response_headers, "WWW-Authenticate") !=
                     std::string(),
                 "identity rejection must include WWW-Authenticate");
}

void test_http_bearer_node_identity_policy_rejects_invalid_body() {
    const mdbxc::sync::NodeId node = make_node(0x33);
    const mdbxc::sync::DbId db_id = make_node(0xD3);

    mdbxc::sync::CodecBounds bounds;
    bounds.max_transport_message_bytes = 8;

    mdbxc::sync::HttpBearerNodeIdentityPolicy bounded_policy(bounds);
    bounded_policy.allow_token_for_node("token-a", node);
    bounded_policy.allow_db_id_for_token("token-a", db_id);

    mdbxc::sync::HttpSyncRequest request;
    request.method = mdbxc::sync::HttpSyncRoutes::method_post();
    request.target = mdbxc::sync::HttpSyncRoutes::pull_target();
    request.content_type = mdbxc::sync::HttpSyncRoutes::content_type();
    mdbxc::sync::http_add_header(
        request.headers, "Authorization", "Bearer token-a");

    const std::uint8_t malformed_byte_1 = 0x01;
    const std::uint8_t malformed_byte_2 = 0x02;
    request.body.push_back(malformed_byte_1);
    request.body.push_back(malformed_byte_2);

    mdbxc::sync::SyncTransportDecision decision =
        bounded_policy.check_http_request(request);
    require_true(!decision.allowed && decision.status_code == 400,
                 "malformed pull body was not rejected as HTTP 400");

    mdbxc::sync::PullRequest pull;
    pull.requester = node;
    pull.db_id = db_id;
    request.body = mdbxc::sync::TransportMessageCodec::encode_pull_request(
        pull);

    decision = bounded_policy.check_http_request(request);
    require_true(!decision.allowed && decision.status_code == 413,
                 "oversized pull body was not rejected as HTTP 413");
}

void test_http_retry_status_classification() {
    require_true(mdbxc::sync::classify_http_sync_status(200) ==
                     mdbxc::sync::HttpSyncRetryClass::Success,
                 "HTTP 200 must be success");
    require_true(mdbxc::sync::http_sync_status_is_retryable(429),
                 "HTTP 429 must be retryable");
    require_true(mdbxc::sync::http_sync_status_is_retryable(503),
                 "HTTP 503 must be retryable");
    require_true(!mdbxc::sync::http_sync_status_is_retryable(401),
                 "HTTP 401 must be permanent");
    require_true(!mdbxc::sync::http_sync_status_is_retryable(413),
                 "HTTP 413 must be permanent");
}

void test_http_retry_hint() {
    mdbxc::sync::HttpSyncResponse response;
    response.status_code = 429;
    mdbxc::sync::http_add_header(
        response.headers, "Retry-After", "17");

    mdbxc::sync::SyncTransportRetryHint hint =
        mdbxc::sync::http_sync_retry_hint(response);
    require_true(hint.available, "HTTP 429 hint must be available");
    require_true(hint.retryable, "HTTP 429 hint must be retryable");
    require_true(hint.has_retry_after,
                 "HTTP 429 hint must preserve Retry-After");
    require_true(hint.retry_after_seconds == 17u,
                 "HTTP Retry-After seconds parsed incorrectly");

    response.headers.clear();
    mdbxc::sync::http_add_header(
        response.headers, "Retry-After", "soon");
    hint = mdbxc::sync::http_sync_retry_hint(response);
    require_true(hint.available,
                 "invalid Retry-After hint must stay available");
    require_true(hint.retryable,
                 "invalid Retry-After must not change retryable status");
    require_true(!hint.has_retry_after,
                 "invalid Retry-After must be ignored");

    response.status_code = 200;
    response.headers.clear();
    mdbxc::sync::http_add_header(
        response.headers, "Retry-After", "5");
    hint = mdbxc::sync::http_sync_retry_hint(response);
    require_true(!hint.available, "HTTP 200 hint must be unavailable");
    require_true(!hint.retryable,
                 "HTTP 200 hint must not be retryable");
    require_true(!hint.has_retry_after,
                 "HTTP 200 hint must ignore Retry-After");

    response.status_code = 401;
    hint = mdbxc::sync::http_sync_retry_hint(response);
    require_true(hint.available, "HTTP 401 hint must be available");
    require_true(!hint.retryable,
                 "HTTP 401 hint must not be retryable");

    std::uint64_t seconds = 0;
    require_true(mdbxc::sync::parse_http_retry_after_delta_seconds(
                     "0", seconds),
                 "Retry-After zero seconds must parse");
    require_true(seconds == 0u,
                 "Retry-After zero seconds parsed incorrectly");
    require_true(!mdbxc::sync::parse_http_retry_after_delta_seconds(
                     "", seconds),
                 "empty Retry-After must not parse");
    require_true(!mdbxc::sync::parse_http_retry_after_delta_seconds(
                     "18446744073709551616", seconds),
                 "overflowing Retry-After must not parse");
}

void test_websocket_close_code_classification() {
    require_true(mdbxc::sync::classify_websocket_sync_close_code(1000) ==
                     mdbxc::sync::WebSocketSyncCloseRetryClass::Success,
                 "WebSocket 1000 must be success");
    require_true(mdbxc::sync::websocket_sync_close_code_is_retryable(1006),
                 "WebSocket 1006 must be retryable");
    require_true(mdbxc::sync::websocket_sync_close_code_is_retryable(1011),
                 "WebSocket 1011 must be retryable");
    require_true(mdbxc::sync::websocket_sync_close_code_is_retryable(1012),
                 "WebSocket 1012 must be retryable");
    require_true(mdbxc::sync::websocket_sync_close_code_is_retryable(1013),
                 "WebSocket 1013 must be retryable");
    require_true(!mdbxc::sync::websocket_sync_close_code_is_retryable(1008),
                 "WebSocket 1008 must be permanent");
    require_true(!mdbxc::sync::websocket_sync_close_code_is_retryable(1009),
                 "WebSocket 1009 must be permanent");
}

void test_websocket_retry_hint() {
    mdbxc::sync::SyncTransportRetryHint hint =
        mdbxc::sync::websocket_sync_retry_hint(1011);
    require_true(hint.available,
                 "WebSocket 1011 hint must be available");
    require_true(hint.retryable,
                 "WebSocket 1011 hint must be retryable");
    require_true(!hint.has_retry_after,
                 "WebSocket hint must not invent Retry-After");

    hint = mdbxc::sync::websocket_sync_retry_hint(1008);
    require_true(hint.available,
                 "WebSocket 1008 hint must be available");
    require_true(!hint.retryable,
                 "WebSocket 1008 hint must be permanent");

    hint = mdbxc::sync::websocket_sync_retry_hint(1000);
    require_true(!hint.available,
                 "WebSocket 1000 hint must be unavailable");
    require_true(!hint.retryable,
                 "WebSocket 1000 hint must not be retryable");
}

void test_transport_message_size_policy() {
    bool threw = false;
    try {
        (void)mdbxc::sync::TransportMessageSizePolicy(0u);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    require_true(threw, "zero message size limit must be rejected");

    mdbxc::sync::TransportMessageSizePolicy policy(3u);

    mdbxc::sync::SyncTransportDecision decision =
        policy.check_http_post(
            mdbxc::sync::HttpSyncRoutes::pull_target(),
            mdbxc::sync::HttpSyncRoutes::content_type(),
            std::vector<std::uint8_t>(4u, 0x11u));
    require_true(!decision.allowed && decision.status_code == 413,
                 "oversized HTTP body was not rejected as 413");

    mdbxc::sync::WebSocketSyncRequestContext context;
    context.binary_message.assign(4u, 0x22u);
    decision = policy.check_websocket_message(context);
    require_true(!decision.allowed && decision.status_code == 1009,
                 "oversized WebSocket message was not rejected as 1009");

    context.binary_message.assign(3u, 0x33u);
    decision = policy.check_websocket_message(context);
    require_true(decision.allowed,
                 "size policy rejected message at the configured limit");
}

void test_transport_zero_limit_policies() {
    bool threw = false;
    try {
        (void)mdbxc::sync::FixedWindowHttpRateLimitPolicy(
            1u, std::chrono::seconds(0));
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    require_true(threw, "zero HTTP rate-limit window must be rejected");

    mdbxc::sync::FixedWindowHttpRateLimitPolicy rate(
        0u, std::chrono::seconds(5));
    mdbxc::sync::HttpSyncRequest request;
    request.remote_address = "127.0.0.1";
    mdbxc::sync::SyncTransportDecision decision =
        rate.check_http_request(request);
    require_true(!decision.allowed && decision.status_code == 429,
                 "zero HTTP request limit must reject immediately");
    require_true(mdbxc::sync::http_header_value(
                     decision.response_headers, "Retry-After") !=
                     std::string(),
                 "zero HTTP request limit must include Retry-After");

    mdbxc::sync::FixedBudgetSyncTransportPolicy budget(0u, 0u, 0u, 0u);
    mdbxc::sync::PullRequest pull;
    decision = budget.check_pull(pull);
    require_true(!decision.allowed && decision.status_code == 429,
                 "zero pull budget must reject");

    mdbxc::sync::PushRequest push;
    decision = budget.check_push(push);
    require_true(!decision.allowed && decision.status_code == 429,
                 "zero push budget must reject");

    mdbxc::sync::LogicalRecoveryRequest recovery;
    decision = budget.check_logical_recovery(recovery);
    require_true(!decision.allowed && decision.status_code == 429,
                 "zero logical recovery budget must reject");

    decision = budget.check_http_post(
        mdbxc::sync::HttpSyncRoutes::pull_target(),
        mdbxc::sync::HttpSyncRoutes::content_type(),
        std::vector<std::uint8_t>());
    require_true(!decision.allowed && decision.status_code == 429,
                 "zero HTTP post budget must reject");
}

void test_http_rate_limit_bucket_cap_and_eviction() {
    mdbxc::sync::FixedWindowHttpRateLimitPolicy rate(
        1u, std::chrono::seconds(1), 1u);

    mdbxc::sync::HttpSyncRequest first;
    first.remote_address = "192.0.2.1";
    mdbxc::sync::SyncTransportDecision decision =
        rate.check_http_request(first);
    require_true(decision.allowed, "first capped rate-limit bucket rejected");
    require_true(rate.bucket_count() == 1u,
                 "rate-limit bucket cap did not track first identity");

    mdbxc::sync::HttpSyncRequest second;
    second.remote_address = "192.0.2.2";
    decision = rate.check_http_request(second);
    require_true(!decision.allowed && decision.status_code == 429,
                 "rate-limit bucket cap did not reject new identity");
    require_true(mdbxc::sync::http_header_value(
                     decision.response_headers, "Retry-After") !=
                     std::string(),
                 "rate-limit bucket cap rejection omitted Retry-After");
    require_true(rate.bucket_count() == 1u,
                 "rate-limit bucket cap allowed bucket growth");

    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    decision = rate.check_http_request(second);
    require_true(decision.allowed,
                 "expired rate-limit bucket was not evicted for new identity");
    require_true(rate.bucket_count() == 1u,
                 "rate-limit bucket eviction left stale buckets");
}

void test_http_client_middleware_copies_rejection_headers() {
    mdbxc::sync::FixedWindowHttpRateLimitPolicy rate(
        0, std::chrono::seconds(7));
    RecordingHttpClient client;
    mdbxc::sync::HttpSyncClientMiddleware wrapped(client, &rate);

    const std::vector<std::uint8_t> body(1u, 0x22u);
    const mdbxc::sync::HttpSyncResponse response = wrapped.post(
        mdbxc::sync::HttpSyncRoutes::pull_target(),
        mdbxc::sync::HttpSyncRoutes::content_type(),
        body,
        mdbxc::sync::CancellationToken());

    require_true(response.status_code == 429,
                 "client middleware did not return rate-limit status");
    require_true(mdbxc::sync::http_header_value(
                     response.headers, "Retry-After") != std::string(),
                 "client middleware dropped Retry-After");
    require_true(client.post_count() == 0u,
                 "rate-limited HTTP post reached downstream client");
}

void test_http_server_middleware_copies_rejection_headers() {
    const std::string path = "test_transport_middleware_http_server.mdbx";
    cleanup(path);

    std::shared_ptr<mdbxc::Connection> db = open_db(path);
    mdbxc::sync::SyncEngine engine(db);
    engine.initialize_local_identity(make_node(0x70), make_node(0x71));
    mdbxc::sync::HttpSyncServer server(engine);
    mdbxc::sync::FixedWindowHttpRateLimitPolicy rate(
        0, std::chrono::seconds(7));
    mdbxc::sync::HttpSyncServerMiddleware wrapped(server, &rate);

    mdbxc::sync::HttpSyncRequest request;
    request.method = mdbxc::sync::HttpSyncRoutes::method_post();
    request.target = mdbxc::sync::HttpSyncRoutes::pull_target();
    request.content_type = mdbxc::sync::HttpSyncRoutes::content_type();
    request.remote_address = "127.0.0.1";
    mdbxc::sync::http_add_header(
        request.headers, mdbxc::sync::HttpSyncHeaders::request_id(),
        "middleware-request");
    mdbxc::sync::http_add_header(
        request.headers, mdbxc::sync::HttpSyncHeaders::trace_id(),
        "middleware-trace");

    const mdbxc::sync::HttpSyncResponse response = wrapped.handle(request);
    require_true(response.status_code == 429,
                 "server middleware did not return rate-limit status");
    require_true(mdbxc::sync::http_header_value(
                     response.headers, "Retry-After") != std::string(),
                 "server middleware dropped Retry-After");
    require_true(mdbxc::sync::http_header_value(
                     response.headers,
                     mdbxc::sync::HttpSyncHeaders::request_id()) ==
                     "middleware-request",
                 "server middleware dropped request id");
    require_true(mdbxc::sync::http_header_value(
                     response.headers,
                     mdbxc::sync::HttpSyncHeaders::trace_id()) ==
                     "middleware-trace",
                 "server middleware dropped trace id");

    db->disconnect();
    cleanup(path);
}

} // namespace

int main() {
    test_peer_middleware_allows_limits_and_observes();
    test_peer_middleware_forwards_logical_capabilities();
    test_peer_middleware_enforces_logical_recovery_policy();
    test_transport_trace_context_helpers();
    test_transport_observer_receives_request_context();
    test_observer_exceptions_are_swallowed();
    test_http_client_middleware_route_policy();
    test_http_client_middleware_budget_policy();
    test_http_context_policies();
    test_http_correlation_copy_is_idempotent();
    test_http_bearer_node_identity_policy();
    test_http_bearer_node_identity_policy_rejects_invalid_body();
    test_http_retry_status_classification();
    test_http_retry_hint();
    test_websocket_close_code_classification();
    test_websocket_retry_hint();
    test_transport_message_size_policy();
    test_transport_zero_limit_policies();
    test_http_rate_limit_bucket_cap_and_eviction();
    test_http_client_middleware_copies_rejection_headers();
    test_http_server_middleware_copies_rejection_headers();
    return 0;
}
