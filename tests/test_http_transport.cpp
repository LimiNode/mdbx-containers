#include <mdbx_containers.hpp>
#include <mdbx_containers/sync.hpp>

#include <cstdio>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId node{};
    for (int i = 0; i < 16; ++i) {
        node[i] = static_cast<std::uint8_t>(seed + i);
    }
    return node;
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

void require_true(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

std::string get_value(const std::shared_ptr<mdbxc::Connection>& conn,
                      mdbxc::KeyValueTable<int, std::string>& table,
                      int key) {
    std::string out;
    auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
    if (!table.try_get(key, out, txn.handle())) {
        throw std::runtime_error("missing replicated value");
    }
    return out;
}

class LoopbackHttpClient : public mdbxc::sync::IHttpSyncClient {
public:
    explicit LoopbackHttpClient(mdbxc::sync::HttpSyncServer& server)
        : m_server(server),
          m_cancel_count(0),
          m_last_token_cancellable(false) {}

    mdbxc::sync::HttpSyncResponse post(
            const std::string& target,
            const std::string& content_type,
            const std::vector<std::uint8_t>& body,
            const mdbxc::sync::CancellationToken& cancel_token) override {
        m_last_target = target;
        m_last_content_type = content_type;
        m_last_token_cancellable = cancel_token.can_be_cancelled();

        mdbxc::sync::HttpSyncRequest request;
        request.method = mdbxc::sync::HttpSyncRoutes::method_post();
        request.target = target;
        request.content_type = content_type;
        request.body = body;
        return m_server.handle(request);
    }

    void request_cancel() override {
        ++m_cancel_count;
    }

    const std::string& last_target() const { return m_last_target; }
    const std::string& last_content_type() const {
        return m_last_content_type;
    }
    bool last_token_cancellable() const { return m_last_token_cancellable; }
    std::size_t cancel_count() const { return m_cancel_count; }

private:
    mdbxc::sync::HttpSyncServer& m_server;
    std::size_t m_cancel_count;
    bool m_last_token_cancellable;
    std::string m_last_target;
    std::string m_last_content_type;
};

class ErrorHttpClient : public mdbxc::sync::IHttpSyncClient {
public:
    mdbxc::sync::HttpSyncResponse post(
            const std::string& target,
            const std::string& content_type,
            const std::vector<std::uint8_t>& body,
            const mdbxc::sync::CancellationToken& cancel_token) override {
        (void)target;
        (void)content_type;
        (void)body;
        (void)cancel_token;
        mdbxc::sync::HttpSyncResponse response;
        response.status_code = 503;
        response.content_type = "text/plain; charset=utf-8";
        mdbxc::sync::http_add_header(response.headers, "Retry-After", "3");
        const std::string text = "upstream unavailable";
        response.body.assign(text.begin(), text.end());
        return response;
    }
};

void test_http_peer_pull_and_push_roundtrip() {
    const std::string primary_path = "test_http_transport_primary.mdbx";
    const std::string replica_path = "test_http_transport_replica.mdbx";
    cleanup(primary_path);
    cleanup(replica_path);

    std::shared_ptr<mdbxc::Connection> primary = open_db(primary_path);
    std::shared_ptr<mdbxc::Connection> replica = open_db(replica_path);

    const mdbxc::sync::NodeId primary_node = make_node(0x10);
    const mdbxc::sync::NodeId replica_node = make_node(0x20);
    const mdbxc::sync::DbId db_id = make_node(0xD0);

    mdbxc::sync::SyncEngine primary_engine(primary);
    mdbxc::sync::SyncEngine replica_engine(replica);
    primary_engine.initialize_local_identity(primary_node, db_id);
    replica_engine.initialize_local_identity(replica_node, db_id);

    mdbxc::sync::ThreadLocalChangeAccumulator capture(primary);
    mdbxc::KeyValueTable<int, std::string> primary_ticks(primary, "ticks");

    primary->attach_sync_capture(&capture);
    primary_ticks.insert_or_assign(1, "BTC/USD");
    primary_ticks.insert_or_assign(2, "ETH/USD");
    primary->detach_sync_capture();

    mdbxc::sync::HttpSyncServer primary_server(primary_engine);
    LoopbackHttpClient primary_http(primary_server);
    mdbxc::sync::HttpSyncPeer primary_peer(primary_http);

    mdbxc::sync::CancellationSource pull_cancel;
    mdbxc::sync::PullRequest pull;
    pull.requester = replica_node;
    pull.db_id = db_id;
    pull.have = replica_engine.applied_cursor();
    pull.max_batches = 100;
    pull.cancel_token = pull_cancel.token();

    const mdbxc::sync::PullResponse pulled = primary_peer.pull(pull);
    require_true(pulled.ok, "HTTP pull failed: " + pulled.error);
    require_true(pulled.batches.size() == 2u,
                 "HTTP pull expected two batches");
    require_true(primary_http.last_target() ==
                     mdbxc::sync::HttpSyncRoutes::pull_target(),
                 "HTTP pull target mismatch");
    require_true(primary_http.last_content_type() ==
                     mdbxc::sync::HttpSyncRoutes::content_type(),
                 "HTTP pull content type mismatch");
    require_true(primary_http.last_token_cancellable(),
                 "HTTP client did not receive cancellation token");

    mdbxc::sync::PushRequest local_apply;
    local_apply.sender = primary_node;
    local_apply.db_id = db_id;
    local_apply.batches = pulled.batches;
    const mdbxc::sync::PushResponse applied =
        replica_engine.handle_push(local_apply);
    require_true(applied.ok, "local apply failed: " + applied.error);

    primary->attach_sync_capture(&capture);
    primary_ticks.insert_or_assign(3, "SOL/USD");
    primary->detach_sync_capture();

    mdbxc::sync::HttpSyncServer replica_server(replica_engine);
    LoopbackHttpClient replica_http(replica_server);
    mdbxc::sync::HttpSyncPeer replica_peer(replica_http);

    mdbxc::sync::PushRequest push = primary_engine.make_push_request(3, 0);
    require_true(push.batches.size() == 1u,
                 "HTTP push expected one batch");
    const mdbxc::sync::PushResponse pushed = replica_peer.push(push);
    require_true(pushed.ok, "HTTP push failed: " + pushed.error);
    require_true(pushed.receiver_have.last_seq_for(primary_node) == 3u,
                 "HTTP push cursor mismatch");
    require_true(replica_http.last_target() ==
                     mdbxc::sync::HttpSyncRoutes::push_target(),
                 "HTTP push target mismatch");

    replica_peer.request_cancel();
    require_true(replica_http.cancel_count() == 1u,
                 "HTTP peer did not forward request_cancel()");

    mdbxc::KeyValueTable<int, std::string> replica_ticks(replica, "ticks");
    require_true(get_value(replica, replica_ticks, 1) == "BTC/USD",
                 "replica value 1 mismatch");
    require_true(get_value(replica, replica_ticks, 2) == "ETH/USD",
                 "replica value 2 mismatch");
    require_true(get_value(replica, replica_ticks, 3) == "SOL/USD",
                 "replica value 3 mismatch");

    primary->disconnect();
    replica->disconnect();
    cleanup(primary_path);
    cleanup(replica_path);
}

void test_http_server_status_mapping() {
    const std::string path = "test_http_transport_status.mdbx";
    cleanup(path);

    std::shared_ptr<mdbxc::Connection> db = open_db(path);
    mdbxc::sync::SyncEngine engine(db);
    engine.initialize_local_identity(make_node(0x30), make_node(0xD1));
    mdbxc::sync::HttpSyncServer server(engine);

    mdbxc::sync::HttpSyncRequest request;
    request.method = "GET";
    request.target = mdbxc::sync::HttpSyncRoutes::pull_target();
    request.content_type = mdbxc::sync::HttpSyncRoutes::content_type();
    mdbxc::sync::http_add_header(
        request.headers, mdbxc::sync::HttpSyncHeaders::request_id(),
        "request-123");
    mdbxc::sync::http_add_header(
        request.headers, mdbxc::sync::HttpSyncHeaders::trace_id(),
        "trace-abc");
    mdbxc::sync::HttpSyncResponse response = server.handle(request);
    require_true(response.status_code == 405, "GET must be rejected");
    require_true(!response.body.empty(), "GET rejection must carry body text");
    require_true(mdbxc::sync::http_header_value(
                     response.headers,
                     mdbxc::sync::HttpSyncHeaders::request_id()) ==
                     "request-123",
                 "HTTP server did not echo request id");
    require_true(mdbxc::sync::http_header_value(
                     response.headers,
                     mdbxc::sync::HttpSyncHeaders::trace_id()) ==
                     "trace-abc",
                 "HTTP server did not echo trace id");

    request.method = mdbxc::sync::HttpSyncRoutes::method_post();
    request.content_type = "application/octet-stream";
    response = server.handle(request);
    require_true(response.status_code == 415,
                 "wrong content type must be rejected");

    request.content_type = mdbxc::sync::HttpSyncRoutes::content_type();
    request.target = "/unknown";
    response = server.handle(request);
    require_true(response.status_code == 404,
                 "unknown route must be rejected");

    request.target = mdbxc::sync::HttpSyncRoutes::pull_target();
    request.body.assign(3u, 0xFFu);
    response = server.handle(request);
    require_true(response.status_code == 400,
                 "malformed body must be rejected");

    request.target = mdbxc::sync::HttpSyncRoutes::logical_delivery_target();
    response = server.handle(request);
    require_true(response.status_code == 400,
                 "malformed logical delivery body must be rejected");

    mdbxc::sync::CodecBounds bounds;
    bounds.max_transport_message_bytes = 16;
    mdbxc::sync::HttpSyncServer bounded_server(engine, bounds);
    request.body = mdbxc::sync::TransportMessageCodec::encode_pull_request(
        mdbxc::sync::PullRequest());
    response = bounded_server.handle(request);
    require_true(response.status_code == 413,
                 "oversized body must be rejected");

    db->disconnect();
    cleanup(path);
}

void test_http_peer_delivers_ordered_logical_outbox() {
    const std::string source_path = "test_http_transport_logical_source.mdbx";
    const std::string replica_path = "test_http_transport_logical_replica.mdbx";
    cleanup(source_path);
    cleanup(replica_path);

    std::shared_ptr<mdbxc::Connection> source = open_db(source_path);
    std::shared_ptr<mdbxc::Connection> replica = open_db(replica_path);
    const mdbxc::sync::DbId db_id = make_node(0xD5);
    const mdbxc::sync::NodeId replica_node = make_node(0x61);
    mdbxc::sync::SyncEngine source_engine(source);
    mdbxc::sync::SyncEngine replica_engine(replica);
    source_engine.initialize_local_identity(make_node(0x51), db_id);
    replica_engine.initialize_local_identity(replica_node, db_id);

    mdbxc::sync::LogicalChangeFrame frame;
    source_engine.enqueue_logical_delivery(db_id, replica_node, frame);

    mdbxc::sync::HttpSyncServer replica_server(replica_engine);
    LoopbackHttpClient client(replica_server);
    mdbxc::sync::HttpSyncPeer peer(client);
    mdbxc::sync::CancellationSource logical_cancel;
    const mdbxc::sync::CancellationToken logical_token = logical_cancel.token();
    const mdbxc::sync::LogicalDeliveryHello hello =
        peer.logical_delivery_hello_with_cancel(&logical_token);
    require_true(hello.db_uuid == db_id,
                 "HTTP logical hello db uuid mismatch");
    require_true(client.last_target() ==
                     mdbxc::sync::HttpSyncRoutes::logical_hello_target(),
                 "HTTP logical hello target mismatch");
    require_true(client.last_token_cancellable(),
                 "HTTP logical hello did not receive cancellation token");
    const mdbxc::sync::LogicalDeliveryDispatchResult result =
        source_engine.deliver_pending_logical_deliveries(peer, db_id,
                                                         replica_node);
    require_true(result.ok, "HTTP logical delivery failed: " + result.error);
    require_true(result.delivered == 1u,
                 "HTTP logical delivery count mismatch");
    require_true(result.acknowledged_through == 1u,
                 "HTTP logical acknowledgement mismatch");
    require_true(client.last_target() ==
                     mdbxc::sync::HttpSyncRoutes::logical_delivery_target(),
                 "HTTP logical delivery target mismatch");
    require_true(source_engine.pending_logical_deliveries(db_id, replica_node).empty(),
                 "HTTP logical delivery did not remove acknowledged outbox entry");

    source->disconnect();
    replica->disconnect();
    cleanup(source_path);
    cleanup(replica_path);
}

void test_http_peer_rejects_transport_error() {
    ErrorHttpClient client;
    mdbxc::sync::HttpSyncPeer peer(client);
    bool caught = false;
    try {
        (void)peer.pull(mdbxc::sync::PullRequest());
    } catch (const std::runtime_error& e) {
        const std::string message = e.what();
        caught = message.find("503") != std::string::npos &&
                 message.find("upstream unavailable") != std::string::npos;
    }
    require_true(caught, "HTTP peer must surface non-200 status");
    const mdbxc::sync::SyncTransportRetryHint hint =
        peer.last_retry_hint();
    require_true(hint.available, "HTTP peer must publish available retry hint");
    require_true(hint.retryable, "HTTP peer must publish retryable hint");
    require_true(hint.has_retry_after,
                 "HTTP peer must preserve Retry-After hint");
    require_true(hint.retry_after_seconds == 3u,
                 "HTTP peer Retry-After hint mismatch");
}

} // namespace

int main() {
    test_http_peer_pull_and_push_roundtrip();
    test_http_server_status_mapping();
    test_http_peer_rejects_transport_error();
    test_http_peer_delivers_ordered_logical_outbox();
    return 0;
}
