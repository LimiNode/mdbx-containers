#include <mdbx_containers.hpp>
#include <mdbx_containers/sync.hpp>

#include <cstdio>
#include <cstdint>
#include <cstring>
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

class LoopbackWebSocketChannel : public mdbxc::sync::IWebSocketSyncChannel {
public:
    explicit LoopbackWebSocketChannel(
            mdbxc::sync::WebSocketSyncServer& server)
        : m_server(server),
           m_exchange_count(0),
           m_cancel_count(0),
           m_last_token_cancellable(false),
           m_last_was_logical(false),
           m_last_type(mdbxc::sync::TransportMessageType::PullRequest) {}

    std::vector<std::uint8_t> exchange_binary(
            const std::vector<std::uint8_t>& binary_message,
            const mdbxc::sync::CancellationToken& cancel_token) override {
        ++m_exchange_count;
        m_last_token_cancellable = cancel_token.can_be_cancelled();
        m_last_was_logical = binary_message.size() >=
                mdbxc::sync::LogicalDeliveryProtocolCodec::magic_size() &&
            std::memcmp(&binary_message[0],
                        mdbxc::sync::LogicalDeliveryProtocolCodec::magic(),
                        mdbxc::sync::LogicalDeliveryProtocolCodec::magic_size()) == 0;
        m_last_was_logical = m_last_was_logical ||
            (binary_message.size() >=
                mdbxc::sync::LogicalRecoveryProtocolCodec::magic_size() &&
             std::memcmp(&binary_message[0],
                         mdbxc::sync::LogicalRecoveryProtocolCodec::magic(),
                         mdbxc::sync::LogicalRecoveryProtocolCodec::magic_size()) == 0);
        if (!m_last_was_logical) {
            m_last_type =
                mdbxc::sync::TransportMessageCodec::peek_message_type(
                    binary_message);
        }
        return m_server.handle_binary_message(binary_message);
    }

    void request_cancel() override {
        ++m_cancel_count;
    }

    mdbxc::sync::SyncTransportRetryHint last_retry_hint() const override {
        return m_last_retry_hint;
    }

    void set_last_retry_hint(
            const mdbxc::sync::SyncTransportRetryHint& hint) {
        m_last_retry_hint = hint;
    }

    std::size_t exchange_count() const { return m_exchange_count; }
    std::size_t cancel_count() const { return m_cancel_count; }
    bool last_token_cancellable() const { return m_last_token_cancellable; }
    bool last_was_logical() const { return m_last_was_logical; }
    mdbxc::sync::TransportMessageType last_type() const {
        return m_last_type;
    }

private:
    mdbxc::sync::WebSocketSyncServer& m_server;
    std::size_t m_exchange_count;
    std::size_t m_cancel_count;
    bool m_last_token_cancellable;
    bool m_last_was_logical;
    mdbxc::sync::TransportMessageType m_last_type;
    mdbxc::sync::SyncTransportRetryHint m_last_retry_hint;
};

void test_websocket_peer_pull_and_push_roundtrip() {
    const std::string primary_path = "test_websocket_transport_primary.mdbx";
    const std::string replica_path = "test_websocket_transport_replica.mdbx";
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

    mdbxc::sync::WebSocketSyncServer primary_server(primary_engine);
    LoopbackWebSocketChannel primary_channel(primary_server);
    mdbxc::sync::WebSocketSyncPeer primary_peer(primary_channel);

    mdbxc::sync::CancellationSource pull_cancel;
    mdbxc::sync::PullRequest pull;
    pull.requester = replica_node;
    pull.db_id = db_id;
    pull.have = replica_engine.applied_cursor();
    pull.max_batches = 100;
    pull.cancel_token = pull_cancel.token();

    const mdbxc::sync::PullResponse pulled = primary_peer.pull(pull);
    require_true(pulled.ok, "WebSocket pull failed: " + pulled.error);
    require_true(pulled.batches.size() == 2u,
                 "WebSocket pull expected two batches");
    require_true(primary_channel.last_type() ==
                     mdbxc::sync::TransportMessageType::PullRequest,
                 "WebSocket pull sent wrong message type");
    require_true(primary_channel.exchange_count() == 1u,
                 "WebSocket pull exchange count mismatch");
    require_true(primary_channel.last_token_cancellable(),
                 "WebSocket channel did not receive cancellation token");

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

    mdbxc::sync::WebSocketSyncServer replica_server(replica_engine);
    LoopbackWebSocketChannel replica_channel(replica_server);
    mdbxc::sync::WebSocketSyncPeer replica_peer(replica_channel);

    mdbxc::sync::PushRequest push = primary_engine.make_push_request(3, 0);
    require_true(push.batches.size() == 1u,
                 "WebSocket push expected one batch");
    const mdbxc::sync::PushResponse pushed = replica_peer.push(push);
    require_true(pushed.ok, "WebSocket push failed: " + pushed.error);
    require_true(pushed.receiver_have.last_seq_for(primary_node) == 3u,
                 "WebSocket push cursor mismatch");
    require_true(replica_channel.last_type() ==
                     mdbxc::sync::TransportMessageType::PushRequest,
                 "WebSocket push sent wrong message type");
    require_true(replica_channel.exchange_count() == 1u,
                 "WebSocket push exchange count mismatch");

    replica_peer.request_cancel();
    require_true(replica_channel.cancel_count() == 1u,
                 "WebSocket peer did not forward request_cancel()");
    mdbxc::sync::SyncTransportRetryHint retry_hint;
    retry_hint.available = true;
    retry_hint.retryable = true;
    retry_hint.has_retry_after = false;
    primary_channel.set_last_retry_hint(retry_hint);
    require_true(primary_peer.last_retry_hint().available,
                 "WebSocket peer did not expose available channel retry hint");
    require_true(primary_peer.last_retry_hint().retryable,
                 "WebSocket peer did not expose channel retry hint");

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

void test_websocket_server_rejects_response_messages() {
    const std::string path = "test_websocket_transport_reject.mdbx";
    cleanup(path);

    std::shared_ptr<mdbxc::Connection> db = open_db(path);
    mdbxc::sync::SyncEngine engine(db);
    engine.initialize_local_identity(make_node(0x30), make_node(0xD1));
    mdbxc::sync::WebSocketSyncServer server(engine);

    const std::vector<std::uint8_t> response_message =
        mdbxc::sync::TransportMessageCodec::encode_pull_response(
            mdbxc::sync::PullResponse());

    bool caught = false;
    try {
        (void)server.handle_binary_message(response_message);
    } catch (const std::runtime_error& e) {
        caught = std::string(e.what()).find("response message") !=
                 std::string::npos;
    }
    require_true(caught,
                 "WebSocket server must reject response messages");

    mdbxc::sync::LogicalRecoveryResponse recovery_response;
    recovery_response.ok = false;
    recovery_response.error = "client response";
    const std::vector<std::uint8_t> logical_response =
        mdbxc::sync::LogicalRecoveryProtocolCodec::encode_response(
            recovery_response);
    caught = false;
    try {
        (void)server.handle_binary_message(logical_response);
    } catch (const std::runtime_error& e) {
        caught = std::string(e.what()).find("response message") !=
                 std::string::npos;
    }
    require_true(caught,
                 "WebSocket server must reject logical recovery responses");

    db->disconnect();
    cleanup(path);
}

void test_websocket_peer_delivers_ordered_logical_outbox() {
    const std::string source_path =
        "test_websocket_transport_logical_source.mdbx";
    const std::string replica_path =
        "test_websocket_transport_logical_replica.mdbx";
    cleanup(source_path);
    cleanup(replica_path);

    std::shared_ptr<mdbxc::Connection> source = open_db(source_path);
    std::shared_ptr<mdbxc::Connection> replica = open_db(replica_path);
    const mdbxc::sync::DbId db_id = make_node(0xD6);
    const mdbxc::sync::NodeId replica_node = make_node(0x62);
    mdbxc::sync::SyncEngine source_engine(source);
    mdbxc::sync::SyncEngine replica_engine(replica);
    source_engine.initialize_local_identity(make_node(0x52), db_id);
    replica_engine.initialize_local_identity(replica_node, db_id);

    mdbxc::sync::LogicalChangeFrame frame;
    source_engine.enqueue_logical_delivery(db_id, replica_node, frame);

    mdbxc::sync::WebSocketSyncServer replica_server(replica_engine);
    LoopbackWebSocketChannel channel(replica_server);
    mdbxc::sync::WebSocketSyncPeer peer(channel);
    mdbxc::sync::CancellationSource logical_cancel;
    const mdbxc::sync::CancellationToken logical_token = logical_cancel.token();
    const mdbxc::sync::LogicalDeliveryHello hello =
        peer.logical_delivery_hello_with_cancel(&logical_token);
    require_true(hello.db_uuid == db_id,
                 "WebSocket logical hello db uuid mismatch");
    require_true(channel.last_was_logical(),
                 "WebSocket logical hello used raw protocol magic");
    require_true(channel.last_token_cancellable(),
                 "WebSocket logical hello did not receive cancellation token");
    const mdbxc::sync::LogicalDeliveryDispatchResult result =
        source_engine.deliver_pending_logical_deliveries(peer, db_id,
                                                         replica_node);
    require_true(result.ok, "WebSocket logical delivery failed: " + result.error);
    require_true(result.delivered == 1u,
                 "WebSocket logical delivery count mismatch");
    require_true(result.acknowledged_through == 1u,
                 "WebSocket logical acknowledgement mismatch");
    require_true(channel.last_was_logical(),
                 "WebSocket logical delivery used raw protocol magic");
    require_true(source_engine.pending_logical_deliveries(db_id, replica_node).empty(),
                 "WebSocket logical delivery did not remove acknowledged outbox entry");

    source->disconnect();
    replica->disconnect();
    cleanup(source_path);
    cleanup(replica_path);
}

void test_websocket_server_rejects_malformed_messages() {
    const std::string path = "test_websocket_transport_malformed.mdbx";
    cleanup(path);

    std::shared_ptr<mdbxc::Connection> db = open_db(path);
    mdbxc::sync::SyncEngine engine(db);
    engine.initialize_local_identity(make_node(0x40), make_node(0xD2));
    mdbxc::sync::WebSocketSyncServer server(engine);

    const std::uint8_t malformed_byte_1 = 0x01;
    const std::uint8_t malformed_byte_2 = 0x02;
    std::vector<std::uint8_t> malformed_message;
    malformed_message.push_back(malformed_byte_1);
    malformed_message.push_back(malformed_byte_2);

    bool caught = false;
    try {
        (void)server.handle_binary_message(malformed_message);
    } catch (const std::runtime_error&) {
        caught = true;
    }

    db->disconnect();
    cleanup(path);

    require_true(caught,
                 "WebSocket server must reject malformed messages");
}

void test_websocket_authenticated_node_policy() {
    const mdbxc::sync::NodeId node_a = make_node(0x11);
    const mdbxc::sync::NodeId node_b = make_node(0x22);
    const mdbxc::sync::DbId db_a = make_node(0xD1);
    const mdbxc::sync::DbId db_b = make_node(0xD2);

    mdbxc::sync::WebSocketAuthenticatedNodeIdentityPolicy policy;
    mdbxc::sync::WebSocketSyncRequestContext context;
    context.has_authenticated_node = true;
    context.authenticated_node = node_a;

    mdbxc::sync::PullRequest pull;
    pull.requester = node_a;
    pull.db_id = db_a;
    context.binary_message =
        mdbxc::sync::TransportMessageCodec::encode_pull_request(pull);

    mdbxc::sync::SyncTransportDecision decision =
        policy.check_websocket_message(context);
    require_true(!decision.allowed && decision.status_code == 1008,
                 "default WebSocket DB access should deny db_id");

    context.db_access.allow_db_id(db_a);
    decision = policy.check_websocket_message(context);
    require_true(decision.allowed,
                 "matching WebSocket node identity was rejected");

    pull.requester = node_b;
    context.binary_message =
        mdbxc::sync::TransportMessageCodec::encode_pull_request(pull);
    decision = policy.check_websocket_message(context);
    require_true(!decision.allowed && decision.status_code == 1008,
                 "WebSocket requester mismatch was not rejected");

    pull.requester = node_a;
    pull.db_id = db_b;
    context.binary_message =
        mdbxc::sync::TransportMessageCodec::encode_pull_request(pull);
    decision = policy.check_websocket_message(context);
    require_true(!decision.allowed && decision.status_code == 1008,
                 "WebSocket db_id mismatch was not rejected");

    mdbxc::sync::PushRequest push;
    push.sender = node_b;
    push.db_id = db_a;
    context.binary_message =
        mdbxc::sync::TransportMessageCodec::encode_push_request(push);
    decision = policy.check_websocket_message(context);
    require_true(!decision.allowed && decision.status_code == 1008,
                 "WebSocket sender mismatch was not rejected");

    mdbxc::sync::LogicalRecoveryRequest recovery;
    recovery.requester = node_a;
    recovery.db_id = db_a;
    context.binary_message =
        mdbxc::sync::LogicalRecoveryProtocolCodec::encode_request(recovery);
    decision = policy.check_websocket_message(context);
    require_true(decision.allowed,
                 "matching WebSocket logical recovery requester was rejected");

    recovery.db_id = db_b;
    context.binary_message =
        mdbxc::sync::LogicalRecoveryProtocolCodec::encode_request(recovery);
    decision = policy.check_websocket_message(context);
    require_true(!decision.allowed && decision.status_code == 1008,
                 "WebSocket logical recovery db_id mismatch was not rejected");

    recovery.requester = node_b;
    recovery.db_id = db_a;
    context.binary_message =
        mdbxc::sync::LogicalRecoveryProtocolCodec::encode_request(recovery);
    decision = policy.check_websocket_message(context);
    require_true(!decision.allowed && decision.status_code == 1008,
                 "WebSocket logical recovery requester mismatch was not rejected");

    context.has_authenticated_node = false;
    push.sender = node_a;
    context.binary_message =
        mdbxc::sync::TransportMessageCodec::encode_push_request(push);
    decision = policy.check_websocket_message(context);
    require_true(!decision.allowed && decision.status_code == 1008,
                 "missing WebSocket authenticated node was not rejected");
}

void test_websocket_authenticated_node_policy_rejects_invalid_body() {
    const mdbxc::sync::NodeId node = make_node(0x33);
    const mdbxc::sync::DbId db_id = make_node(0xD3);

    mdbxc::sync::CodecBounds bounds;
    bounds.max_transport_message_bytes = 8;
    mdbxc::sync::WebSocketAuthenticatedNodeIdentityPolicy policy(bounds);

    mdbxc::sync::WebSocketSyncRequestContext context;
    context.has_authenticated_node = true;
    context.authenticated_node = node;
    context.db_access.allow_db_id(db_id);

    const std::uint8_t malformed_byte_1 = 0x01;
    const std::uint8_t malformed_byte_2 = 0x02;
    context.binary_message.push_back(malformed_byte_1);
    context.binary_message.push_back(malformed_byte_2);

    mdbxc::sync::SyncTransportDecision decision =
        policy.check_websocket_message(context);
    require_true(!decision.allowed && decision.status_code == 1007,
                 "malformed WebSocket sync body was not rejected as 1007");

    mdbxc::sync::PullRequest pull;
    pull.requester = node;
    pull.db_id = db_id;
    context.binary_message =
        mdbxc::sync::TransportMessageCodec::encode_pull_request(pull);

    decision = policy.check_websocket_message(context);
    require_true(!decision.allowed && decision.status_code == 1009,
                 "oversized WebSocket sync body was not rejected as 1009");
}

void test_websocket_server_middleware_rejects_spoofed_identity() {
    const std::string path = "test_websocket_transport_policy.mdbx";
    cleanup(path);

    std::shared_ptr<mdbxc::Connection> db = open_db(path);
    mdbxc::sync::SyncEngine engine(db);
    engine.initialize_local_identity(make_node(0x44), make_node(0xD4));

    mdbxc::sync::WebSocketSyncServer server(engine);
    mdbxc::sync::WebSocketAuthenticatedNodeIdentityPolicy policy;
    mdbxc::sync::WebSocketSyncServerMiddleware wrapped(server, &policy);

    const mdbxc::sync::NodeId authenticated = make_node(0x55);
    mdbxc::sync::PullRequest pull;
    pull.requester = make_node(0x66);
    pull.db_id = make_node(0xD4);

    mdbxc::sync::WebSocketSyncRequestContext context;
    context.has_authenticated_node = true;
    context.authenticated_node = authenticated;
    context.binary_message =
        mdbxc::sync::TransportMessageCodec::encode_pull_request(pull);

    bool caught = false;
    try {
        (void)wrapped.handle_binary_message(context);
    } catch (const std::runtime_error& e) {
        caught = std::string(e.what()).find("requester does not match") !=
                 std::string::npos;
    }

    db->disconnect();
    cleanup(path);

    require_true(caught,
                 "WebSocket server middleware allowed spoofed requester");
}

void test_websocket_server_middleware_preserves_close_code() {
    const std::string path = "test_websocket_transport_close_code.mdbx";
    cleanup(path);

    std::shared_ptr<mdbxc::Connection> db = open_db(path);
    mdbxc::sync::SyncEngine engine(db);
    engine.initialize_local_identity(make_node(0x77), make_node(0xD7));

    mdbxc::sync::CodecBounds bounds;
    bounds.max_transport_message_bytes = 8;
    mdbxc::sync::WebSocketSyncServer server(engine, bounds);
    mdbxc::sync::WebSocketAuthenticatedNodeIdentityPolicy policy(bounds);
    mdbxc::sync::SyncTransportMetricsObserver metrics;
    mdbxc::sync::WebSocketSyncServerMiddleware wrapped(
        server, &policy, &metrics);

    mdbxc::sync::PullRequest pull;
    pull.requester = make_node(0x88);
    pull.db_id = make_node(0xD7);

    mdbxc::sync::WebSocketSyncRequestContext context;
    context.has_authenticated_node = true;
    context.authenticated_node = pull.requester;
    context.binary_message =
        mdbxc::sync::TransportMessageCodec::encode_pull_request(pull);

    bool caught = false;
    try {
        (void)wrapped.handle_binary_message(context);
    } catch (const mdbxc::sync::WebSocketSyncRejected& e) {
        caught = true;
        require_true(e.close_code() == 1009u,
                     "WebSocket middleware lost oversized close code");
    }

    db->disconnect();
    cleanup(path);

    require_true(caught,
                 "WebSocket middleware did not throw typed rejection");

    const mdbxc::sync::SyncTransportMetricsSnapshot snapshot =
        metrics.snapshot();
    require_true(snapshot.rejected_calls == 1u,
                 "WebSocket rejection metric was not recorded");
    require_true(snapshot.failed_calls == 0u,
                 "WebSocket policy rejection was counted as exception");
}

void test_websocket_peer_logical_recovery_route() {
    const std::string source_path = "test_websocket_logical_recovery_source.mdbx";
    cleanup(source_path);

    std::shared_ptr<mdbxc::Connection> source = open_db(source_path);
    mdbxc::sync::FullSnapshotExportOptions options;
    options.replacement_scope =
        mdbxc::sync::FullSnapshotScope::CompleteUserDatabase;
    mdbxc::sync::SyncEngine engine(
        source, mdbxc::sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(make_node(0x81), make_node(0x91));
    mdbxc::KeyValueTable<int, std::string> source_values(source, "values");
    source_values.insert_or_assign(1, "recovery-value");
    mdbxc::sync::WebSocketSyncServer server(engine);
    LoopbackWebSocketChannel channel(server);
    mdbxc::sync::WebSocketSyncPeer peer(channel);

    mdbxc::sync::LogicalRecoveryRequest request;
    request.requester = make_node(0xA1);
    request.db_id = make_node(0x91);
    mdbxc::sync::CancellationSource cancellation;
    const mdbxc::sync::CancellationToken token = cancellation.token();
    const mdbxc::sync::LogicalRecoveryResponse response =
        peer.logical_recovery_with_cancel(request, &token);

    require_true(peer.supports_logical_recovery(),
                 "WebSocket peer did not advertise logical recovery");
    require_true(response.ok && response.has_baseline &&
                     !response.snapshot_chunk.has_more,
                 "WebSocket logical recovery success response mismatch");
    require_true(channel.last_was_logical(),
                 "WebSocket logical recovery did not use logical wire family");
    require_true(channel.last_token_cancellable(),
                 "WebSocket logical recovery did not receive cancellation token");

    source->disconnect();
    cleanup(source_path);
}

} // namespace

int main() {
    test_websocket_peer_pull_and_push_roundtrip();
    test_websocket_server_rejects_response_messages();
    test_websocket_peer_delivers_ordered_logical_outbox();
    test_websocket_server_rejects_malformed_messages();
    test_websocket_authenticated_node_policy();
    test_websocket_authenticated_node_policy_rejects_invalid_body();
    test_websocket_server_middleware_rejects_spoofed_identity();
    test_websocket_server_middleware_preserves_close_code();
    test_websocket_peer_logical_recovery_route();
    return 0;
}
