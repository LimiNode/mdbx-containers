#include <mdbx_containers/sync.hpp>

#include <cstdint>
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

mdbxc::sync::ChangeBatch make_batch(std::uint8_t seed, std::uint64_t seq) {
    using namespace mdbxc::sync;
    ChangeBatch batch;
    batch.origin_node_id = make_node(seed);
    batch.seq = seq;
    batch.time_unix_ns = 1700000000000000000ULL + seq;

    ChangeOp op;
    op.op_type = ChangeOpType::Put;
    op.dbi_name = "ticks";
    op.storage_key = { static_cast<std::uint8_t>(seq & 0xFFu) };
    op.value = { 0x10, 0x20, static_cast<std::uint8_t>(seed) };
    batch.ops.push_back(op);
    return batch;
}

mdbxc::sync::FullSnapshotChunk make_snapshot_chunk() {
    using namespace mdbxc::sync;
    FullSnapshotChunk chunk;
    chunk.source_node_id = make_node(0x50);
    chunk.source_db_uuid = make_node(0x70);
    chunk.snapshot_id = "snapshot-session";
    chunk.source_tail.last_seq_by_origin[chunk.source_node_id] = 11u;
    chunk.chunk_index = 0u;
    chunk.has_more = true;
    chunk.continuation = "next-page";
    FullSnapshotManifestEntry entry;
    entry.dbi_name = "documents";
    chunk.manifest.push_back(entry);
    chunk.batch = make_batch(0x50, 0u);
    chunk.batch.origin_node_id = chunk.source_node_id;
    chunk.batch.batch_flags = BATCH_HAS_MORE;
    chunk.batch.ops[0].dbi_name = "documents";
    return chunk;
}

template<class Fn>
void expect_throw(const std::string& label, Fn&& fn) {
    bool caught = false;
    try {
        fn();
    } catch (...) {
        caught = true;
    }
    if (!caught) {
        throw std::runtime_error(label + ": expected throw");
    }
}

void require_true(bool value, const char* message) {
    if (!value) {
        throw std::runtime_error(message);
    }
}

void test_pull_request_roundtrip() {
    using namespace mdbxc::sync;
    PullRequest request;
    request.requester = make_node(0x10);
    request.db_id = make_node(0x20);
    request.have.last_seq_by_origin[make_node(0xA0)] = 7;
    request.have.last_seq_by_origin[make_node(0xB0)] = 9;
    request.max_batches = 17;
    request.max_bytes = 4096;
    request.request_full_snapshot = true;
    request.full_snapshot_id = "snapshot-session";
    request.full_snapshot_continuation = "next-page";
    request.max_single_batch_bytes = 8192;
    CancellationSource source;
    request.cancel_token = source.token();

    const std::vector<std::uint8_t> bytes =
        TransportMessageCodec::encode_pull_request(request);
    const PullRequest decoded =
        TransportMessageCodec::decode_pull_request(bytes);

    require_true(decoded.requester == request.requester,
                 "PullRequest requester mismatch");
    require_true(decoded.db_id == request.db_id,
                 "PullRequest db_id mismatch");
    require_true(decoded.have.last_seq_for(make_node(0xA0)) == 7,
                 "PullRequest cursor A mismatch");
    require_true(decoded.have.last_seq_for(make_node(0xB0)) == 9,
                 "PullRequest cursor B mismatch");
    require_true(decoded.max_batches == 17,
                 "PullRequest max_batches mismatch");
    require_true(decoded.max_bytes == 4096,
                 "PullRequest max_bytes mismatch");
    require_true(decoded.request_full_snapshot,
                 "PullRequest full snapshot mismatch");
    require_true(decoded.full_snapshot_id == request.full_snapshot_id,
                 "PullRequest full snapshot id mismatch");
    require_true(decoded.full_snapshot_continuation ==
                     request.full_snapshot_continuation,
                 "PullRequest full snapshot continuation mismatch");
    require_true(decoded.max_single_batch_bytes == 8192,
                 "PullRequest max_single_batch_bytes mismatch");
    require_true(!decoded.cancel_token.can_be_cancelled(),
                 "PullRequest cancel token must not be serialized");
}

void test_full_snapshot_pull_response_roundtrip() {
    using namespace mdbxc::sync;
    PullResponse response;
    response.is_full_snapshot = true;
    response.snapshot_chunk = make_snapshot_chunk();
    response.has_more = true;
    response.remote_tail_known = true;
    response.remote_tail = response.snapshot_chunk.source_tail;

    const std::vector<std::uint8_t> bytes =
        TransportMessageCodec::encode_pull_response(response);
    const PullResponse decoded =
        TransportMessageCodec::decode_pull_response(bytes);

    require_true(decoded.is_full_snapshot,
                 "PullResponse full snapshot flag mismatch");
    require_true(decoded.batches.empty(),
                 "PullResponse full snapshot has incremental batches");
    require_true(decoded.has_more && decoded.snapshot_chunk.has_more,
                 "PullResponse full snapshot continuation mismatch");
    require_true(decoded.snapshot_chunk.snapshot_id == "snapshot-session",
                 "PullResponse full snapshot id mismatch");
    require_true(decoded.snapshot_chunk.continuation == "next-page",
                 "PullResponse full snapshot token mismatch");
    require_true(decoded.snapshot_chunk.source_tail.last_seq_for(
                     make_node(0x50)) == 11u,
                 "PullResponse full snapshot tail mismatch");

    expect_throw("mixed pull response", [response] {
        PullResponse invalid = response;
        invalid.batches.push_back(make_batch(0x50, 1u));
        (void)TransportMessageCodec::encode_pull_response(invalid);
    });
}

void test_pull_response_roundtrip() {
    using namespace mdbxc::sync;
    PullResponse response;
    response.remote_have.last_seq_by_origin[make_node(0xA0)] = 4;
    response.remote_have.last_seq_by_origin[make_node(0xB0)] = 8;
    response.remote_tail.last_seq_by_origin[make_node(0xA0)] = 5;
    response.remote_tail.last_seq_by_origin[make_node(0xB0)] = 9;
    response.remote_tail_known = true;
    response.batches.push_back(make_batch(0xA0, 5));
    response.batches.push_back(make_batch(0xB0, 9));
    response.has_more = true;
    response.ok = false;
    response.error = "temporary upstream timeout";
    response.error_code = SyncResponseErrorCode::BatchTooLarge;
    response.error_retryable = false;

    const std::vector<std::uint8_t> bytes =
        TransportMessageCodec::encode_pull_response(response);
    const PullResponse decoded =
        TransportMessageCodec::decode_pull_response(bytes);

    require_true(decoded.remote_have.last_seq_for(make_node(0xA0)) == 4,
                 "PullResponse cursor A mismatch");
    require_true(decoded.remote_have.last_seq_for(make_node(0xB0)) == 8,
                 "PullResponse cursor B mismatch");
    require_true(decoded.remote_tail.last_seq_for(make_node(0xA0)) == 5,
                 "PullResponse tail A mismatch");
    require_true(decoded.remote_tail.last_seq_for(make_node(0xB0)) == 9,
                 "PullResponse tail B mismatch");
    require_true(decoded.remote_tail_known,
                 "PullResponse tail-known mismatch");
    require_true(decoded.batches.size() == 2u,
                 "PullResponse batch count mismatch");
    require_true(decoded.batches[0].origin_node_id == make_node(0xA0),
                 "PullResponse batch A origin mismatch");
    require_true(decoded.batches[1].origin_node_id == make_node(0xB0),
                 "PullResponse batch B origin mismatch");
    require_true(decoded.has_more, "PullResponse has_more mismatch");
    require_true(!decoded.ok, "PullResponse ok mismatch");
    require_true(decoded.error == response.error, "PullResponse error mismatch");
    require_true(decoded.error_code == SyncResponseErrorCode::BatchTooLarge,
                 "PullResponse error_code mismatch");
    require_true(decoded.error_retryable == response.error_retryable,
                 "PullResponse error_retryable mismatch");
}

void test_push_request_roundtrip() {
    using namespace mdbxc::sync;
    PushRequest request;
    request.sender = make_node(0x30);
    request.db_id = make_node(0x40);
    request.batches.push_back(make_batch(0xC0, 1));
    request.batches.push_back(make_batch(0xC0, 2));
    CancellationSource source;
    request.cancel_token = source.token();

    const std::vector<std::uint8_t> bytes =
        TransportMessageCodec::encode_push_request(request);
    const PushRequest decoded =
        TransportMessageCodec::decode_push_request(bytes);

    require_true(decoded.sender == request.sender,
                 "PushRequest sender mismatch");
    require_true(decoded.db_id == request.db_id,
                 "PushRequest db_id mismatch");
    require_true(decoded.batches.size() == 2u,
                 "PushRequest batch count mismatch");
    require_true(decoded.batches[0].seq == 1,
                 "PushRequest batch 1 seq mismatch");
    require_true(decoded.batches[1].seq == 2,
                 "PushRequest batch 2 seq mismatch");
    require_true(!decoded.cancel_token.can_be_cancelled(),
                 "PushRequest cancel token must not be serialized");
}

void test_push_response_roundtrip() {
    using namespace mdbxc::sync;
    PushResponse response;
    response.receiver_have.last_seq_by_origin[make_node(0xD0)] = 42;
    response.ok = false;
    response.error = "sequence gap";
    response.error_code = SyncResponseErrorCode::ApplyConflict;
    response.error_retryable = true;

    const std::vector<std::uint8_t> bytes =
        TransportMessageCodec::encode_push_response(response);
    const PushResponse decoded =
        TransportMessageCodec::decode_push_response(bytes);

    require_true(decoded.receiver_have.last_seq_for(make_node(0xD0)) == 42,
                 "PushResponse cursor mismatch");
    require_true(!decoded.ok, "PushResponse ok mismatch");
    require_true(decoded.error == response.error, "PushResponse error mismatch");
    require_true(decoded.error_code == response.error_code,
                 "PushResponse error_code mismatch");
    require_true(decoded.error_retryable == response.error_retryable,
                 "PushResponse error_retryable mismatch");
}

void test_peek_message_type() {
    using namespace mdbxc::sync;

    require_true(
        TransportMessageCodec::peek_message_type(
            TransportMessageCodec::encode_pull_request(PullRequest())) ==
            TransportMessageType::PullRequest,
        "PullRequest peek mismatch");
    require_true(
        TransportMessageCodec::peek_message_type(
            TransportMessageCodec::encode_pull_response(PullResponse())) ==
            TransportMessageType::PullResponse,
        "PullResponse peek mismatch");
    require_true(
        TransportMessageCodec::peek_message_type(
            TransportMessageCodec::encode_push_request(PushRequest())) ==
            TransportMessageType::PushRequest,
        "PushRequest peek mismatch");
    require_true(
        TransportMessageCodec::peek_message_type(
            TransportMessageCodec::encode_push_response(PushResponse())) ==
            TransportMessageType::PushResponse,
        "PushResponse peek mismatch");
}

void test_message_header_rejections() {
    using namespace mdbxc::sync;
    const std::vector<std::uint8_t> bytes =
        TransportMessageCodec::encode_pull_request(PullRequest());

    expect_throw("empty input", [] {
        (void)TransportMessageCodec::decode_pull_request(
            std::vector<std::uint8_t>());
    });

    expect_throw("magic mismatch", [bytes] {
        std::vector<std::uint8_t> bad = bytes;
        bad[0] = 0xFF;
        (void)TransportMessageCodec::decode_pull_request(bad);
    });

    expect_throw("version mismatch", [bytes] {
        std::vector<std::uint8_t> bad = bytes;
        bad[8] = 0xFF;
        bad[9] = 0xFF;
        (void)TransportMessageCodec::decode_pull_request(bad);
    });

    expect_throw("unknown flags", [bytes] {
        std::vector<std::uint8_t> bad = bytes;
        bad[11] = 0x01;
        (void)TransportMessageCodec::decode_pull_request(bad);
    });

    expect_throw("wrong message type", [] {
        const std::vector<std::uint8_t> bad =
            TransportMessageCodec::encode_pull_response(PullResponse());
        (void)TransportMessageCodec::decode_pull_request(bad);
    });

    expect_throw("trailing bytes", [bytes] {
        std::vector<std::uint8_t> bad = bytes;
        bad.push_back(0xEE);
        (void)TransportMessageCodec::decode_pull_request(bad);
    });

    expect_throw("invalid bool", [bytes] {
        std::vector<std::uint8_t> bad = bytes;
        bad[bad.size() - 9u] = 2u;
        (void)TransportMessageCodec::decode_pull_request(bad);
    });

    expect_throw("duplicate cursor origin", [] {
        PullRequest request;
        request.have.last_seq_by_origin[make_node(0xA0)] = 1;
        request.have.last_seq_by_origin[make_node(0xB0)] = 2;
        std::vector<std::uint8_t> bad =
            TransportMessageCodec::encode_pull_request(request);

        const std::size_t envelope_size =
            TransportMessageCodec::magic_size() + 2u + 1u + 4u;
        const std::size_t node_size = request.requester.size();
        const std::size_t cursor_count_size = 4u;
        const std::size_t seq_size = 8u;
        const std::size_t first_origin_offset =
            envelope_size + node_size + node_size + cursor_count_size;
        const std::size_t second_origin_offset =
            first_origin_offset + node_size + seq_size;

        for (std::size_t i = 0; i < node_size; ++i) {
            bad[second_origin_offset + i] = bad[first_origin_offset + i];
        }
        (void)TransportMessageCodec::decode_pull_request(bad);
    });
}

void test_bounds_rejections() {
    using namespace mdbxc::sync;

    expect_throw("cursor origins bound", [] {
        CodecBounds bounds;
        bounds.max_cursor_origins = 1;
        PullRequest request;
        request.have.last_seq_by_origin[make_node(0xA0)] = 1;
        request.have.last_seq_by_origin[make_node(0xB0)] = 2;
        (void)TransportMessageCodec::encode_pull_request(request, &bounds);
    });

    expect_throw("batches per message bound", [] {
        CodecBounds bounds;
        bounds.max_batches_per_message = 1;
        PullResponse response;
        response.batches.push_back(make_batch(0xA0, 1));
        response.batches.push_back(make_batch(0xA0, 2));
        (void)TransportMessageCodec::encode_pull_response(response, &bounds);
    });

    expect_throw("error string bound", [] {
        CodecBounds bounds;
        bounds.max_error_len = 4;
        PushResponse response;
        response.ok = false;
        response.error = "too long";
        (void)TransportMessageCodec::encode_push_response(response, &bounds);
    });

    expect_throw("transport message bytes bound", [] {
        CodecBounds bounds;
        bounds.max_transport_message_bytes = 16;
        PullRequest request;
        (void)TransportMessageCodec::encode_pull_request(request, &bounds);
    });

    expect_throw("incremental pull snapshot state", [] {
        PullRequest request;
        request.full_snapshot_id = "unexpected";
        (void)TransportMessageCodec::encode_pull_request(request);
    });

    expect_throw("partial snapshot session state", [] {
        PullRequest request;
        request.request_full_snapshot = true;
        request.full_snapshot_id = "session";
        (void)TransportMessageCodec::encode_pull_request(request);
    });
}

void test_response_error_code_rejections() {
    using namespace mdbxc::sync;

    expect_throw("pull response error code", [] {
        std::vector<std::uint8_t> bad =
            TransportMessageCodec::encode_pull_response(PullResponse());
        bad[bad.size() - 3u] = 0xFFu;
        bad[bad.size() - 2u] = 0xFFu;
        (void)TransportMessageCodec::decode_pull_response(bad);
    });

    expect_throw("push response error code", [] {
        std::vector<std::uint8_t> bad =
            TransportMessageCodec::encode_push_response(PushResponse());
        bad[bad.size() - 3u] = 0xFFu;
        bad[bad.size() - 2u] = 0xFFu;
        (void)TransportMessageCodec::decode_push_response(bad);
    });
}

void test_logical_snapshot_error_roundtrip() {
    using namespace mdbxc::sync;
    PullResponse response;
    response.ok = false;
    response.error = "logical snapshot state is unsupported";
    response.error_code =
        SyncResponseErrorCode::SnapshotLogicalStateUnsupported;

    const std::vector<std::uint8_t> bytes =
        TransportMessageCodec::encode_pull_response(response);
    const PullResponse decoded =
        TransportMessageCodec::decode_pull_response(bytes);
    require_true(decoded.error_code ==
                     SyncResponseErrorCode::SnapshotLogicalStateUnsupported,
                 "logical snapshot error code mismatch");
    require_true(!decoded.error_retryable,
                 "logical snapshot error unexpectedly retryable");
}

void test_logical_recovery_protocol_roundtrip() {
    using namespace mdbxc::sync;

    LogicalRecoveryRequest request;
    request.requester = make_node(0x31);
    request.db_id = make_node(0xD1);
    request.max_bytes = 8192u;
    request.max_single_batch_bytes = 4096u;
    const LogicalRecoveryRequest decoded_request =
        LogicalRecoveryProtocolCodec::decode_request(
            LogicalRecoveryProtocolCodec::encode_request(request));
    require_true(decoded_request.requester == request.requester,
                 "logical recovery requester mismatch");
    require_true(decoded_request.db_id == request.db_id,
                 "logical recovery db_id mismatch");
    require_true(decoded_request.max_bytes == request.max_bytes,
                 "logical recovery max bytes mismatch");

    LogicalRecoveryResponse intermediate;
    intermediate.has_more = true;
    intermediate.snapshot_chunk = make_snapshot_chunk();
    const LogicalRecoveryResponse decoded_intermediate =
        LogicalRecoveryProtocolCodec::decode_response(
            LogicalRecoveryProtocolCodec::encode_response(intermediate));
    require_true(decoded_intermediate.ok && decoded_intermediate.has_more &&
                     !decoded_intermediate.has_baseline,
                 "logical recovery intermediate response mismatch");

    LogicalRecoveryResponse final_response;
    final_response.snapshot_chunk = make_snapshot_chunk();
    final_response.snapshot_chunk.has_more = false;
    final_response.snapshot_chunk.continuation.clear();
    final_response.snapshot_chunk.batch.batch_flags = BATCH_NONE;
    final_response.has_baseline = true;
    final_response.baseline.source_node_id =
        final_response.snapshot_chunk.source_node_id;
    final_response.baseline.source_db_uuid =
        final_response.snapshot_chunk.source_db_uuid;
    final_response.baseline.snapshot_id =
        final_response.snapshot_chunk.snapshot_id;
    LogicalSchemaRegistryEntry schema;
    schema.schema_id = "app.recovery.v1";
    schema.record.dbi_name = "documents";
    schema.record.kind = LogicalTableKind::KeyValue;
    schema.record.schema_version = 1u;
    schema.record.dbi_names.push_back("documents");
    final_response.baseline.schemas.push_back(schema);
    LogicalSchemaRef frame_schema;
    frame_schema.schema_id = schema.schema_id;
    frame_schema.kind = schema.record.kind;
    frame_schema.schema_version = schema.record.schema_version;
    LogicalChangeFrame frame;
    frame.changes.push_back(LogicalChange(
        frame_schema, 1u, 0u, std::vector<std::uint8_t>(1u, 0x5Au)));
    LogicalDeliveryMarkerInfo marker;
    marker.destination_db_uuid = final_response.snapshot_chunk.source_db_uuid;
    marker.origin_node_id = make_node(0x60);
    marker.origin_sequence = 7u;
    marker.frame_id = "marker-7";
    marker.frame_codec_version = LogicalChangeFrameCodec::codec_version();
    marker.encoded_frame = LogicalChangeFrameCodec::encode(frame);
    marker.frame_bytes_size = static_cast<std::uint32_t>(
        marker.encoded_frame.size());
    final_response.baseline.delivery_markers.push_back(marker);
    LogicalDeliveryWatermarkInfo watermark;
    watermark.origin_node_id = make_node(0x61);
    watermark.sequence = 3u;
    final_response.baseline.delivery_watermarks.push_back(watermark);
    LogicalDeliveryOrderEntry order;
    order.origin_node_id = make_node(0x62);
    order.acknowledged_through = 5u;
    final_response.baseline.delivery_order.push_back(order);
    LogicalDeliveryEnvelope pending;
    pending.destination_db_uuid = final_response.snapshot_chunk.source_db_uuid;
    pending.origin_node_id = final_response.snapshot_chunk.source_node_id;
    pending.origin_sequence = 1u;
    pending.frame_id = "pending-1";
    pending.frame = frame;
    final_response.baseline.source_outbox_pending.push_back(pending);
    final_response.baseline.source_outbox_known_tail = 1u;
    const LogicalRecoveryResponse decoded_final =
        LogicalRecoveryProtocolCodec::decode_response(
            LogicalRecoveryProtocolCodec::encode_response(final_response));
    require_true(decoded_final.ok && !decoded_final.has_more &&
                     decoded_final.has_baseline &&
                     decoded_final.baseline.snapshot_id == "snapshot-session" &&
                     decoded_final.baseline.schemas.size() == 1u &&
                     decoded_final.baseline.delivery_markers.size() == 1u &&
                     decoded_final.baseline.delivery_watermarks.size() == 1u &&
                     decoded_final.baseline.delivery_order.size() == 1u &&
                     decoded_final.baseline.source_outbox_pending.size() == 1u,
                 "logical recovery final response mismatch");

    LogicalRecoveryResponse failure;
    failure.ok = false;
    failure.error = "logical recovery unavailable";
    failure.error_code = SyncResponseErrorCode::SnapshotSessionBusy;
    failure.error_retryable = true;
    const LogicalRecoveryResponse decoded_failure =
        LogicalRecoveryProtocolCodec::decode_response(
            LogicalRecoveryProtocolCodec::encode_response(failure));
    require_true(!decoded_failure.ok && decoded_failure.error_retryable &&
                     decoded_failure.error_code ==
                         SyncResponseErrorCode::SnapshotSessionBusy,
                 "logical recovery failure response mismatch");
}

void test_logical_recovery_protocol_rejections() {
    using namespace mdbxc::sync;

    LogicalRecoveryResponse response;
    response.snapshot_chunk = make_snapshot_chunk();
    response.snapshot_chunk.has_more = false;
    response.snapshot_chunk.continuation.clear();
    response.snapshot_chunk.batch.batch_flags = BATCH_NONE;

    expect_throw("logical recovery missing baseline", [response] {
        (void)LogicalRecoveryProtocolCodec::encode_response(response);
    });

    response.has_baseline = true;
    response.baseline.source_node_id = response.snapshot_chunk.source_node_id;
    response.baseline.source_db_uuid = response.snapshot_chunk.source_db_uuid;
    response.baseline.snapshot_id = response.snapshot_chunk.snapshot_id;
    std::vector<std::uint8_t> encoded =
        LogicalRecoveryProtocolCodec::encode_response(response);
    encoded.push_back(0xFFu);
    expect_throw("logical recovery trailing bytes", [encoded] {
        (void)LogicalRecoveryProtocolCodec::decode_response(encoded);
    });

    CodecBounds bounds;
    bounds.max_transport_message_bytes = 16u;
    expect_throw("logical recovery transport bound", [&bounds] {
        LogicalRecoveryRequest request;
        request.requester = make_node(0x44);
        request.db_id = make_node(0xD4);
        (void)LogicalRecoveryProtocolCodec::encode_request(request, &bounds);
    });

    LogicalRecoveryResponse invalid_error_code;
    invalid_error_code.ok = false;
    invalid_error_code.error_code =
        static_cast<SyncResponseErrorCode>(0xFFFFu);
    expect_throw("logical recovery unknown error code", [invalid_error_code] {
        (void)LogicalRecoveryProtocolCodec::encode_response(invalid_error_code);
    });

    LogicalRecoveryResponse oversized_pending = response;
    LogicalDeliveryEnvelope pending;
    pending.destination_db_uuid = response.snapshot_chunk.source_db_uuid;
    pending.origin_node_id = make_node(0x55);
    pending.origin_sequence = 1u;
    pending.frame_id = "pending-bound";
    oversized_pending.baseline.source_outbox_pending.push_back(pending);
    CodecBounds envelope_bounds;
    envelope_bounds.max_batch_total_bytes = 1u;
    expect_throw("logical recovery pending envelope bound", [oversized_pending,
                                                               &envelope_bounds] {
        (void)LogicalRecoveryProtocolCodec::encode_response(
            oversized_pending, &envelope_bounds);
    });

    response.baseline.schemas.push_back(LogicalSchemaRegistryEntry());
    response.baseline.schemas.back().schema_id = "app.recovery.bound";
    response.baseline.schemas.back().record.dbi_name = "documents";
    response.baseline.schemas.back().record.kind = LogicalTableKind::KeyValue;
    response.baseline.schemas.back().record.schema_version = 1u;
    response.baseline.schemas.back().record.dbi_names.push_back("documents");
    response.baseline.schemas.back().record.dbi_names.push_back("metadata");
    CodecBounds schema_bounds;
    schema_bounds.max_snapshot_manifest_entries = 1u;
    expect_throw("logical recovery schema DBI count bound", [response, &schema_bounds] {
        (void)LogicalRecoveryProtocolCodec::encode_response(response, &schema_bounds);
    });
}

void test_golden_header_shape() {
    using namespace mdbxc::sync;
    const std::vector<std::uint8_t> bytes =
        TransportMessageCodec::encode_pull_request(PullRequest());
    static const std::uint8_t expected_magic[8] =
        { 'M','D','B','X','C','P','R','T' };
    for (int i = 0; i < 8; ++i) {
        require_true(bytes[i] == expected_magic[i],
                     "TransportMessageCodec magic mismatch");
    }
    require_true(bytes[8] == 5u && bytes[9] == 0u,
                 "TransportMessageCodec version mismatch");
    require_true(bytes[10] == 1u,
                 "TransportMessageCodec pull request type mismatch");
    require_true(bytes[11] == 0u && bytes[12] == 0u &&
                 bytes[13] == 0u && bytes[14] == 0u,
                 "TransportMessageCodec flags mismatch");
}

} // namespace

int main() {
    test_pull_request_roundtrip();
    test_pull_response_roundtrip();
    test_full_snapshot_pull_response_roundtrip();
    test_push_request_roundtrip();
    test_push_response_roundtrip();
    test_peek_message_type();
    test_message_header_rejections();
    test_bounds_rejections();
    test_response_error_code_rejections();
    test_logical_snapshot_error_roundtrip();
    test_logical_recovery_protocol_roundtrip();
    test_logical_recovery_protocol_rejections();
    test_golden_header_shape();
    return 0;
}
