#include <mdbx_containers/sync.hpp>

#include "test_assert.hpp"

#include <cstdint>
#include <string>
#include <vector>

#ifndef MDBXC_SYNC_ENABLED
#error "MDBXC_SYNC_ENABLED must be defined by the test target"
#endif

#if !MDBXC_SYNC_ENABLED
#error "header_sync_umbrella_test must be compiled with sync enabled"
#endif

class HeaderSyncPeer : public mdbxc::sync::ISyncPeer {
public:
    mdbxc::sync::PullResponse pull(
            const mdbxc::sync::PullRequest& request) override {
        (void)request;
        return mdbxc::sync::PullResponse();
    }

    mdbxc::sync::PushResponse push(
            const mdbxc::sync::PushRequest& request) override {
        (void)request;
        return mdbxc::sync::PushResponse();
    }
};

class HeaderSyncSink : public mdbxc::sync::ISyncCaptureSink {
public:
    void record_change(MDBX_txn* txn,
                       const std::string& dbi_name,
                       mdbxc::sync::ChangeOpType op_type,
                       std::uint32_t dbi_flags,
                       const std::vector<std::uint8_t>& storage_key,
                       const std::vector<std::uint8_t>& value) override {
        (void)txn;
        (void)dbi_name;
        (void)op_type;
        (void)dbi_flags;
        (void)storage_key;
        (void)value;
    }

    void flush_in_txn(MDBX_txn* txn) override {
        (void)txn;
    }
};

class HeaderFullChangeSyncSink : public mdbxc::sync::FullChangeSyncCaptureSink {
public:
    void record_change_op(MDBX_txn* txn,
                          const mdbxc::sync::ChangeOp& change) override {
        (void)txn;
        last_change = change;
    }

    void flush_in_txn(MDBX_txn* txn) override {
        (void)txn;
    }

    mdbxc::sync::ChangeOp last_change;
};

class HeaderSyncApplyObserver : public mdbxc::sync::ISyncApplyObserver {
public:
    HeaderSyncApplyObserver() : calls(0) {}

    void on_sync_apply_committed(
        const mdbxc::sync::SyncApplyEvent& event) override {
        ++calls;
        last_event = event;
    }

    unsigned calls;
    mdbxc::sync::SyncApplyEvent last_event;
};

int main() {
    mdbxc::sync::NodeId node = mdbxc::sync::make_zero_node();
    MDBXC_TEST_ASSERT(node.size() == 16u);

    mdbxc::sync::ChangeOp op;
    op.op_type = mdbxc::sync::ChangeOpType::Put;
    op.dbi_name = "sync_umbrella_table";
    op.storage_key.push_back(1u);
    op.value.push_back(2u);

    mdbxc::sync::ChangeBatch batch;
    batch.origin_node_id = node;
    batch.seq = 1;
    batch.ops.push_back(op);

    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::ChangeBatchCodec::encode(batch);
    const mdbxc::sync::ChangeBatch decoded =
        mdbxc::sync::ChangeBatchCodec::decode_exact(encoded);
    MDBXC_TEST_ASSERT(decoded.seq == 1u);
    MDBXC_TEST_ASSERT(decoded.ops.size() == 1u);
    MDBXC_TEST_ASSERT(decoded.ops[0].dbi_name == "sync_umbrella_table");

    mdbxc::sync::PullRequest pull;
    pull.requester = node;
    pull.db_id = node;
    pull.have.last_seq_by_origin[node] = 1u;
    const std::vector<std::uint8_t> wire =
        mdbxc::sync::TransportMessageCodec::encode_pull_request(pull);
    const mdbxc::sync::PullRequest decoded_pull =
        mdbxc::sync::TransportMessageCodec::decode_pull_request(wire);
    MDBXC_TEST_ASSERT(decoded_pull.requester == node);
    MDBXC_TEST_ASSERT(decoded_pull.have.last_seq_for(node) == 1u);
    MDBXC_TEST_ASSERT(
        mdbxc::sync::TransportMessageCodec::peek_message_type(wire) ==
        mdbxc::sync::TransportMessageType::PullRequest);
    MDBXC_TEST_ASSERT(std::string(mdbxc::sync::HttpSyncRoutes::pull_target())
                      == "/mdbxc/sync/v1/pull");
    MDBXC_TEST_ASSERT(std::string(mdbxc::sync::HttpSyncHeaders::trace_id())
                      == "X-MDBXC-Sync-Trace-Id");
    MDBXC_TEST_ASSERT(mdbxc::sync::http_sync_status_is_retryable(503));
    MDBXC_TEST_ASSERT(
        mdbxc::sync::websocket_sync_close_code_is_retryable(1013u));
    HeaderSyncPeer header_peer;
    const mdbxc::sync::SyncTransportRetryHint default_retry_hint =
        header_peer.last_retry_hint();
    MDBXC_TEST_ASSERT(!default_retry_hint.available);
    MDBXC_TEST_ASSERT(!default_retry_hint.retryable);
    MDBXC_TEST_ASSERT(!default_retry_hint.has_retry_after);
    mdbxc::sync::SyncWorkerStatus worker_status;
    MDBXC_TEST_ASSERT(worker_status.state ==
                      mdbxc::sync::SyncWorkerState::Stopped);
    MDBXC_TEST_ASSERT(!worker_status.last_round_known);
    mdbxc::sync::SyncWorkerOptions worker_options;
    worker_options.permanent_failure_policy =
        mdbxc::sync::SyncWorkerPermanentFailurePolicy::StopWorker;
    MDBXC_TEST_ASSERT(
        worker_options.permanent_failure_policy ==
        mdbxc::sync::SyncWorkerPermanentFailurePolicy::StopWorker);
    MDBXC_TEST_ASSERT(
        std::string(mdbxc::sync::sync_response_error_code_name(
            mdbxc::sync::SyncResponseErrorCode::UnsupportedFullSnapshot)) ==
        "unsupported_full_snapshot");
    MDBXC_TEST_ASSERT(
        std::string(mdbxc::sync::sync_response_error_code_name(
            mdbxc::sync::SyncResponseErrorCode::SnapshotRequired)) ==
        "snapshot_required");
    MDBXC_TEST_ASSERT(
        std::string(mdbxc::sync::sync_response_error_code_name(
            mdbxc::sync::SyncResponseErrorCode::BatchTooLarge)) ==
        "batch_too_large");
    mdbxc::sync::SyncCaptureScope* capture_scope = nullptr;
    mdbxc::sync::SyncWorkerGuard* worker_guard = nullptr;
    mdbxc::sync::SyncNodeSession* node_session = nullptr;
    mdbxc::sync::SyncNodeSessionOptions node_session_options;
    HeaderSyncSink header_sink;
    HeaderFullChangeSyncSink header_full_sink;
    HeaderSyncApplyObserver apply_observer;
    mdbxc::sync::SyncApplyEvent apply_event;
    apply_event.generation = 1u;
    apply_event.applied_batches = 1u;
    apply_event.applied_ops = 1u;
    apply_event.affected_dbi_names.push_back("header_table");
    apply_observer.on_sync_apply_committed(apply_event);
    MDBXC_TEST_ASSERT(apply_observer.calls == 1u);
    MDBXC_TEST_ASSERT(apply_observer.last_event.applied_ops == 1u);
    MDBXC_TEST_ASSERT(
        apply_observer.last_event.affected_dbi_names.size() == 1u);
    (void)capture_scope;
    (void)worker_guard;
    (void)node_session;
    (void)node_session_options;
    (void)header_sink;
    (void)header_full_sink;
    mdbxc::sync::TransportMessageSizePolicy size_policy(1024u);
    (void)size_policy;
    mdbxc::sync::IWebSocketSyncChannel* websocket_channel = nullptr;
    mdbxc::sync::WebSocketSyncServer* websocket_server = nullptr;
    (void)websocket_channel;
    (void)websocket_server;
    mdbxc::sync::WebSocketSyncRequestContext websocket_context;
    websocket_context.has_authenticated_node = true;
    websocket_context.authenticated_node = node;
    websocket_context.db_access = mdbxc::sync::SyncDbAccess::only(node);
    websocket_context.binary_message = wire;
    mdbxc::sync::WebSocketAuthenticatedNodeIdentityPolicy websocket_policy;
    MDBXC_TEST_ASSERT(
        websocket_policy.check_websocket_message(websocket_context).allowed);
    mdbxc::sync::WebSocketSyncRejected websocket_rejection(
        1008u, "sync websocket rejected");
    MDBXC_TEST_ASSERT(websocket_rejection.close_code() == 1008u);
    mdbxc::sync::NodeDbAllowListPolicy allow_list;
    allow_list.allow_node_id(node);
    allow_list.allow_db_id(node);
    MDBXC_TEST_ASSERT(allow_list.check_pull(pull).allowed);

    mdbxc::sync::CancellationSource source;
    const mdbxc::sync::CancellationToken token = source.token();
    MDBXC_TEST_ASSERT(token.can_be_cancelled());
    source.request_cancel();
    MDBXC_TEST_ASSERT(token.is_cancellation_requested());

    return 0;
}
