#include <mdbx_containers.hpp>
#include <mdbx_containers/sync.hpp>

#include "test_assert.hpp"

#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId out{};
    for (std::size_t i = 0u; i < out.size(); ++i) {
        out[i] = static_cast<std::uint8_t>(seed + i);
    }
    return out;
}

template<class Fn>
void expect_throw(const char* name, Fn fn) {
    bool threw = false;
    try {
        fn();
    } catch (const std::exception&) {
        threw = true;
    }
    if (!threw) throw std::runtime_error(name);
}

class ProtocolFixture {
public:
    ProtocolFixture()
        : path("test_selective_replication_protocol.mdbx") {
        std::remove(path.c_str());
        mdbxc::Config config;
        config.pathname = path;
        config.no_subdir = true;
        config.max_dbs = 8u;
        connection = mdbxc::Connection::create(config);
        alpha.reset(new mdbxc::KeyValueTable<int, int>(connection, "alpha12"));
        bravo.reset(new mdbxc::KeyValueTable<int, int>(connection, "bravo22"));
    }

    ~ProtocolFixture() {
        bravo.reset();
        alpha.reset();
        if (connection) connection->disconnect();
        connection.reset();
        std::remove(path.c_str());
    }

    mdbxc::sync::SelectiveReplicationDescriptor descriptor() const {
        mdbxc::sync::SelectiveReplicationDescriptor out;
        out.scope_id = "orders.eu";
        out.designated_writer_origin = make_node(0x20);
        out.manifest.push_back(
            mdbxc::sync::SelectiveReplicationDbi::from(*alpha));
        out.manifest.push_back(
            mdbxc::sync::SelectiveReplicationDbi::from(*bravo));
        return out;
    }

private:
    std::string path;
    std::shared_ptr<mdbxc::Connection> connection;
    std::unique_ptr<mdbxc::KeyValueTable<int, int> > alpha;
    std::unique_ptr<mdbxc::KeyValueTable<int, int> > bravo;
};

mdbxc::sync::ScopedChangeBatch make_batch(
        const mdbxc::sync::SelectiveReplicationDescriptor& descriptor,
        std::uint64_t sequence) {
    mdbxc::sync::ChangeOp op;
    op.op_type = mdbxc::sync::ChangeOpType::Put;
    op.dbi_name = descriptor.manifest[0].dbi_name();
    op.dbi_flags = descriptor.manifest[0].dbi_flags();
    op.storage_key.push_back(static_cast<std::uint8_t>(sequence));
    op.value.push_back(static_cast<std::uint8_t>(sequence + 1u));

    mdbxc::sync::ScopedChangeBatch out;
    out.scope_id = descriptor.scope_id;
    out.designated_writer_origin = descriptor.designated_writer_origin;
    out.scope_sequence = sequence;
    out.time_unix_ns = 1234u + sequence;
    out.ops.push_back(op);
    return out;
}

std::size_t find_bytes(const std::vector<std::uint8_t>& bytes,
                       const std::string& value) {
    if (value.empty() || bytes.size() < value.size()) return bytes.size();
    for (std::size_t i = 0u; i + value.size() <= bytes.size(); ++i) {
        bool equal = true;
        for (std::size_t j = 0u; j < value.size(); ++j) {
            if (bytes[i + j] != static_cast<std::uint8_t>(value[j])) {
                equal = false;
                break;
            }
        }
        if (equal) return i;
    }
    return bytes.size();
}

void replace_bytes(std::vector<std::uint8_t>& bytes,
                   const std::string& from,
                   const std::string& to) {
    MDBXC_TEST_ASSERT(from.size() == to.size());
    const std::size_t position = find_bytes(bytes, from);
    MDBXC_TEST_ASSERT(position != bytes.size());
    for (std::size_t i = 0u; i < to.size(); ++i) {
        bytes[position + i] = static_cast<std::uint8_t>(to[i]);
    }
}

std::size_t find_last_bytes(const std::vector<std::uint8_t>& bytes,
                            const std::string& value) {
    std::size_t position = bytes.size();
    for (std::size_t i = 0u; i + value.size() <= bytes.size(); ++i) {
        bool equal = true;
        for (std::size_t j = 0u; j < value.size(); ++j) {
            if (bytes[i + j] != static_cast<std::uint8_t>(value[j])) {
                equal = false;
                break;
            }
        }
        if (equal) position = i;
    }
    return position;
}

void replace_last_bytes(std::vector<std::uint8_t>& bytes,
                        const std::string& from,
                        const std::string& to) {
    MDBXC_TEST_ASSERT(from.size() == to.size());
    const std::size_t position = find_last_bytes(bytes, from);
    MDBXC_TEST_ASSERT(position != bytes.size());
    for (std::size_t i = 0u; i < to.size(); ++i) {
        bytes[position + i] = static_cast<std::uint8_t>(to[i]);
    }
}

void test_hello_and_capabilities() {
    using namespace mdbxc::sync;
    SelectiveReplicationHello hello;
    hello.node_id = make_node(0x10);
    hello.db_id = make_node(0x30);
    hello.capabilities.flags =
        selective_replication_supported_capability_flags() |
        (UINT64_C(1) << 48);

    const std::vector<std::uint8_t> bytes =
        SelectiveReplicationProtocolCodec::encode_hello(hello);
    const SelectiveReplicationHello decoded =
        SelectiveReplicationProtocolCodec::decode_hello(bytes);
    MDBXC_TEST_ASSERT(decoded.node_id == hello.node_id);
    MDBXC_TEST_ASSERT(decoded.db_id == hello.db_id);
    MDBXC_TEST_ASSERT(decoded.capabilities.supports(
        SelectiveReplicationCapability::ScopedPull));
    MDBXC_TEST_ASSERT(decoded.capabilities.supports(
        SelectiveReplicationCapability::ScopedPush));
    MDBXC_TEST_ASSERT((decoded.capabilities.flags &
                       (UINT64_C(1) << 48)) != 0u);
    MDBXC_TEST_ASSERT(selective_replication_capability_negotiated(
        hello.capabilities, decoded.capabilities,
        SelectiveReplicationCapability::ScopedPull));
    MDBXC_TEST_ASSERT(
        SelectiveReplicationProtocolCodec::peek_message_type(bytes) ==
        SelectiveReplicationProtocolCodec::MessageType::Hello);

    expect_throw("zero selective hello node accepted", [hello] {
        SelectiveReplicationHello bad = hello;
        bad.node_id = NodeId();
        (void)SelectiveReplicationProtocolCodec::encode_hello(bad);
    });
}

void test_pull_round_trip(ProtocolFixture& fixture) {
    using namespace mdbxc::sync;
    const SelectiveReplicationDescriptor descriptor = fixture.descriptor();

    ScopedPullRequest request;
    request.requester = make_node(0x70);
    request.db_id = make_node(0x80);
    request.scope_id = descriptor.scope_id;
    request.have_sequence = 10u;
    request.max_batches = 64u;
    request.max_bytes = 4096u;
    request.max_single_batch_bytes = 2048u;
    CancellationSource cancellation;
    request.cancel_token = cancellation.token();
    const ScopedPullRequest decoded_request =
        SelectiveReplicationProtocolCodec::decode_pull_request(
            SelectiveReplicationProtocolCodec::encode_pull_request(request));
    MDBXC_TEST_ASSERT(decoded_request.requester == request.requester);
    MDBXC_TEST_ASSERT(decoded_request.db_id == request.db_id);
    MDBXC_TEST_ASSERT(decoded_request.scope_id == request.scope_id);
    MDBXC_TEST_ASSERT(decoded_request.have_sequence == 10u);
    MDBXC_TEST_ASSERT(decoded_request.max_single_batch_bytes == 2048u);
    MDBXC_TEST_ASSERT(!decoded_request.cancel_token.can_be_cancelled());

    ScopedPullResponse response;
    response.descriptor = descriptor;
    response.remote_tail = 14u;
    response.remote_tail_known = true;
    response.batches.push_back(make_batch(descriptor, 11u));
    response.batches.push_back(make_batch(descriptor, 12u));
    response.has_more = true;
    const ScopedPullResponse decoded_response =
        SelectiveReplicationProtocolCodec::decode_pull_response(
            SelectiveReplicationProtocolCodec::encode_pull_response(response));
    MDBXC_TEST_ASSERT(decoded_response.ok);
    MDBXC_TEST_ASSERT(selective_replication_descriptors_equal(
        decoded_response.descriptor, descriptor));
    MDBXC_TEST_ASSERT(decoded_response.remote_tail == 14u);
    MDBXC_TEST_ASSERT(decoded_response.remote_tail_known);
    MDBXC_TEST_ASSERT(decoded_response.batches.size() == 2u);
    MDBXC_TEST_ASSERT(decoded_response.batches[0].scope_sequence == 11u);
    MDBXC_TEST_ASSERT(decoded_response.batches[1].scope_sequence == 12u);
    MDBXC_TEST_ASSERT(decoded_response.batches[0].ops[0].dbi_name ==
                      descriptor.manifest[0].dbi_name());
    MDBXC_TEST_ASSERT(decoded_response.has_more);

    ScopedPullResponse failure;
    failure.ok = false;
    failure.error = "descriptor mismatch";
    failure.error_code =
        SelectiveReplicationErrorCode::ScopeDescriptorMismatch;
    const ScopedPullResponse decoded_failure =
        SelectiveReplicationProtocolCodec::decode_pull_response(
            SelectiveReplicationProtocolCodec::encode_pull_response(failure));
    MDBXC_TEST_ASSERT(!decoded_failure.ok);
    MDBXC_TEST_ASSERT(decoded_failure.error_code == failure.error_code);
    MDBXC_TEST_ASSERT(decoded_failure.batches.empty());
}

void test_push_round_trip(ProtocolFixture& fixture) {
    using namespace mdbxc::sync;
    const SelectiveReplicationDescriptor descriptor = fixture.descriptor();
    ScopedPushRequest request;
    request.sender = descriptor.designated_writer_origin;
    request.db_id = make_node(0x90);
    request.descriptor = descriptor;
    request.batches.push_back(make_batch(descriptor, 21u));
    request.batches.push_back(make_batch(descriptor, 22u));
    CancellationSource cancellation;
    request.cancel_token = cancellation.token();

    const ScopedPushRequest decoded_request =
        SelectiveReplicationProtocolCodec::decode_push_request(
            SelectiveReplicationProtocolCodec::encode_push_request(request));
    MDBXC_TEST_ASSERT(decoded_request.sender == request.sender);
    MDBXC_TEST_ASSERT(decoded_request.db_id == request.db_id);
    MDBXC_TEST_ASSERT(selective_replication_descriptors_equal(
        decoded_request.descriptor, descriptor));
    MDBXC_TEST_ASSERT(decoded_request.batches.size() == 2u);
    MDBXC_TEST_ASSERT(!decoded_request.cancel_token.can_be_cancelled());

    ScopedPushResponse response;
    response.scope_id = descriptor.scope_id;
    response.receiver_sequence = 22u;
    const ScopedPushResponse decoded_response =
        SelectiveReplicationProtocolCodec::decode_push_response(
            SelectiveReplicationProtocolCodec::encode_push_response(response));
    MDBXC_TEST_ASSERT(decoded_response.ok);
    MDBXC_TEST_ASSERT(decoded_response.scope_id == descriptor.scope_id);
    MDBXC_TEST_ASSERT(decoded_response.receiver_sequence == 22u);

    ScopedPushResponse failure;
    failure.scope_id = descriptor.scope_id;
    failure.ok = false;
    failure.error = "gap";
    failure.error_code = SelectiveReplicationErrorCode::ScopedSequenceGap;
    failure.error_retryable = true;
    failure.receiver_sequence = 20u;
    const ScopedPushResponse decoded_failure =
        SelectiveReplicationProtocolCodec::decode_push_response(
            SelectiveReplicationProtocolCodec::encode_push_response(failure));
    MDBXC_TEST_ASSERT(!decoded_failure.ok);
    MDBXC_TEST_ASSERT(decoded_failure.error_retryable);
    MDBXC_TEST_ASSERT(decoded_failure.receiver_sequence == 20u);
}

void test_descriptor_and_batch_rejections(ProtocolFixture& fixture) {
    using namespace mdbxc::sync;
    const SelectiveReplicationDescriptor descriptor = fixture.descriptor();

    expect_throw("empty selective scope accepted", [descriptor] {
        ScopedPushRequest bad;
        bad.sender = descriptor.designated_writer_origin;
        bad.db_id = make_node(0xA0);
        bad.descriptor = descriptor;
        bad.descriptor.scope_id.clear();
        bad.batches.push_back(make_batch(descriptor, 1u));
        (void)SelectiveReplicationProtocolCodec::encode_push_request(bad);
    });
    expect_throw("zero designated writer accepted", [descriptor] {
        ScopedPullResponse bad;
        bad.descriptor = descriptor;
        bad.descriptor.designated_writer_origin = NodeId();
        (void)SelectiveReplicationProtocolCodec::encode_pull_response(bad);
    });
    expect_throw("empty manifest accepted", [descriptor] {
        ScopedPullResponse bad;
        bad.descriptor = descriptor;
        bad.descriptor.manifest.clear();
        (void)SelectiveReplicationProtocolCodec::encode_pull_response(bad);
    });
    expect_throw("wrong scoped writer accepted", [descriptor] {
        ScopedPushRequest bad;
        bad.sender = descriptor.designated_writer_origin;
        bad.db_id = make_node(0xA1);
        bad.descriptor = descriptor;
        bad.batches.push_back(make_batch(descriptor, 1u));
        bad.batches[0].designated_writer_origin = make_node(0xF0);
        (void)SelectiveReplicationProtocolCodec::encode_push_request(bad);
    });
    expect_throw("foreign scoped operation accepted", [descriptor] {
        ScopedPushRequest bad;
        bad.sender = descriptor.designated_writer_origin;
        bad.db_id = make_node(0xA2);
        bad.descriptor = descriptor;
        bad.batches.push_back(make_batch(descriptor, 1u));
        bad.batches[0].ops[0].dbi_name = "outside";
        (void)SelectiveReplicationProtocolCodec::encode_push_request(bad);
    });
    expect_throw("scoped DBI flags mismatch accepted", [descriptor] {
        ScopedPushRequest bad;
        bad.sender = descriptor.designated_writer_origin;
        bad.db_id = make_node(0xA3);
        bad.descriptor = descriptor;
        bad.batches.push_back(make_batch(descriptor, 1u));
        ++bad.batches[0].ops[0].dbi_flags;
        (void)SelectiveReplicationProtocolCodec::encode_push_request(bad);
    });
    expect_throw("zero scoped sequence accepted", [descriptor] {
        ScopedPushRequest bad;
        bad.sender = descriptor.designated_writer_origin;
        bad.db_id = make_node(0xA4);
        bad.descriptor = descriptor;
        bad.batches.push_back(make_batch(descriptor, 0u));
        (void)SelectiveReplicationProtocolCodec::encode_push_request(bad);
    });
    expect_throw("scoped sequence gap accepted", [descriptor] {
        ScopedPushRequest bad;
        bad.sender = descriptor.designated_writer_origin;
        bad.db_id = make_node(0xA5);
        bad.descriptor = descriptor;
        bad.batches.push_back(make_batch(descriptor, 1u));
        bad.batches.push_back(make_batch(descriptor, 3u));
        (void)SelectiveReplicationProtocolCodec::encode_push_request(bad);
    });
    expect_throw("foreign push sender accepted", [descriptor] {
        ScopedPushRequest bad;
        bad.sender = make_node(0xF1);
        bad.db_id = make_node(0xA6);
        bad.descriptor = descriptor;
        bad.batches.push_back(make_batch(descriptor, 1u));
        (void)SelectiveReplicationProtocolCodec::encode_push_request(bad);
    });

    ScopedPushRequest valid;
    valid.sender = descriptor.designated_writer_origin;
    valid.db_id = make_node(0xA7);
    valid.descriptor = descriptor;
    valid.batches.push_back(make_batch(descriptor, 1u));
    std::vector<std::uint8_t> reserved =
        SelectiveReplicationProtocolCodec::encode_push_request(valid);
    replace_bytes(reserved, "alpha12", "_mdbxc_");
    expect_throw("reserved manifest DBI decoded", [reserved] {
        (void)SelectiveReplicationProtocolCodec::decode_push_request(reserved);
    });

    std::vector<std::uint8_t> duplicate =
        SelectiveReplicationProtocolCodec::encode_push_request(valid);
    replace_bytes(duplicate, "bravo22", "alpha12");
    expect_throw("duplicate manifest DBI decoded", [duplicate] {
        (void)SelectiveReplicationProtocolCodec::decode_push_request(duplicate);
    });

    std::vector<std::uint8_t> foreign_operation =
        SelectiveReplicationProtocolCodec::encode_push_request(valid);
    replace_last_bytes(foreign_operation, "alpha12", "outside");
    expect_throw("foreign scoped operation decoded", [foreign_operation] {
        (void)SelectiveReplicationProtocolCodec::decode_push_request(
            foreign_operation);
    });

    std::vector<std::uint8_t> wrong_flags =
        SelectiveReplicationProtocolCodec::encode_push_request(valid);
    const std::size_t operation_name = find_last_bytes(wrong_flags, "alpha12");
    MDBXC_TEST_ASSERT(operation_name != wrong_flags.size());
    MDBXC_TEST_ASSERT(operation_name >= 8u);
    wrong_flags[operation_name - 8u] ^= 1u;
    expect_throw("scoped DBI flag mismatch decoded", [wrong_flags] {
        (void)SelectiveReplicationProtocolCodec::decode_push_request(
            wrong_flags);
    });
}

void test_envelope_and_bounds_rejections(ProtocolFixture& fixture) {
    using namespace mdbxc::sync;
    ScopedPullRequest request;
    request.requester = make_node(0xB0);
    request.db_id = make_node(0xC0);
    request.scope_id = fixture.descriptor().scope_id;
    const std::vector<std::uint8_t> encoded =
        SelectiveReplicationProtocolCodec::encode_pull_request(request);

    expect_throw("selective magic mismatch accepted", [encoded] {
        std::vector<std::uint8_t> bad = encoded;
        bad[0] ^= 0xFFu;
        (void)SelectiveReplicationProtocolCodec::decode_pull_request(bad);
    });
    expect_throw("selective version mismatch accepted", [encoded] {
        std::vector<std::uint8_t> bad = encoded;
        bad[8] = 0xFFu;
        (void)SelectiveReplicationProtocolCodec::decode_pull_request(bad);
    });
    expect_throw("selective mandatory flags accepted", [encoded] {
        std::vector<std::uint8_t> bad = encoded;
        bad[11] = 1u;
        (void)SelectiveReplicationProtocolCodec::decode_pull_request(bad);
    });
    expect_throw("selective unknown message type accepted", [encoded] {
        std::vector<std::uint8_t> bad = encoded;
        bad[10] = 0xFFu;
        (void)SelectiveReplicationProtocolCodec::decode_pull_request(bad);
    });
    expect_throw("selective trailing bytes accepted", [encoded] {
        std::vector<std::uint8_t> bad = encoded;
        bad.push_back(0xEEu);
        (void)SelectiveReplicationProtocolCodec::decode_pull_request(bad);
    });
    expect_throw("selective truncation accepted", [encoded] {
        std::vector<std::uint8_t> bad = encoded;
        bad.pop_back();
        (void)SelectiveReplicationProtocolCodec::decode_pull_request(bad);
    });

    ScopedPushResponse response;
    response.scope_id = fixture.descriptor().scope_id;
    std::vector<std::uint8_t> invalid_bool =
        SelectiveReplicationProtocolCodec::encode_push_response(response);
    invalid_bool[15u] = 2u;
    expect_throw("invalid selective bool accepted", [invalid_bool] {
        (void)SelectiveReplicationProtocolCodec::decode_push_response(
            invalid_bool);
    });

    ScopedPushResponse failure_wire;
    failure_wire.scope_id = fixture.descriptor().scope_id;
    failure_wire.ok = false;
    failure_wire.error_code =
        SelectiveReplicationErrorCode::ScopedSequenceGap;
    std::vector<std::uint8_t> invalid_error =
        SelectiveReplicationProtocolCodec::encode_push_response(failure_wire);
    invalid_error[16u] = 0xFFu;
    invalid_error[17u] = 0xFFu;
    expect_throw("invalid selective error code decoded", [invalid_error] {
        (void)SelectiveReplicationProtocolCodec::decode_push_response(
            invalid_error);
    });

    CodecBounds scope_bounds;
    scope_bounds.max_selective_scope_id_len = 3u;
    expect_throw("selective scope bound ignored", [request, &scope_bounds] {
        (void)SelectiveReplicationProtocolCodec::encode_pull_request(
            request, &scope_bounds);
    });

    CodecBounds message_bounds;
    message_bounds.max_transport_message_bytes = 16u;
    expect_throw("selective message bound ignored", [request, &message_bounds] {
        (void)SelectiveReplicationProtocolCodec::encode_pull_request(
            request, &message_bounds);
    });

    CodecBounds manifest_bounds;
    manifest_bounds.max_selective_manifest_entries = 1u;
    expect_throw("selective manifest bound ignored", [&fixture,
                                                         &manifest_bounds] {
        ScopedPullResponse response;
        response.descriptor = fixture.descriptor();
        (void)SelectiveReplicationProtocolCodec::encode_pull_response(
            response, &manifest_bounds);
    });

    CodecBounds batch_bounds;
    batch_bounds.max_batches_per_message = 1u;
    expect_throw("selective batch count bound ignored", [&fixture,
                                                            &batch_bounds] {
        ScopedPullResponse response;
        response.descriptor = fixture.descriptor();
        response.batches.push_back(make_batch(response.descriptor, 1u));
        response.batches.push_back(make_batch(response.descriptor, 2u));
        (void)SelectiveReplicationProtocolCodec::encode_pull_response(
            response, &batch_bounds);
    });

    CodecBounds nested_batch_bounds;
    nested_batch_bounds.max_batch_total_bytes = 1u;
    expect_throw("selective nested batch bound ignored", [&fixture,
                                                     &nested_batch_bounds] {
        ScopedPullResponse bounded;
        bounded.descriptor = fixture.descriptor();
        bounded.batches.push_back(make_batch(bounded.descriptor, 1u));
        (void)SelectiveReplicationProtocolCodec::encode_pull_response(
            bounded, &nested_batch_bounds);
    });

    CodecBounds error_bounds;
    error_bounds.max_error_len = 3u;
    expect_throw("selective error bound ignored", [&fixture, &error_bounds] {
        ScopedPushResponse bounded;
        bounded.scope_id = fixture.descriptor().scope_id;
        bounded.ok = false;
        bounded.error = "long";
        bounded.error_code =
            SelectiveReplicationErrorCode::ScopeDescriptorMismatch;
        (void)SelectiveReplicationProtocolCodec::encode_push_response(
            bounded, &error_bounds);
    });

    ScopedPullResponse success_with_error;
    success_with_error.descriptor = fixture.descriptor();
    success_with_error.error = "unexpected";
    expect_throw("successful pull error state accepted", [success_with_error] {
        (void)SelectiveReplicationProtocolCodec::encode_pull_response(
            success_with_error);
    });

    ScopedPullResponse unknown_tail;
    unknown_tail.descriptor = fixture.descriptor();
    unknown_tail.remote_tail = 1u;
    expect_throw("unknown selective tail progress accepted", [unknown_tail] {
        (void)SelectiveReplicationProtocolCodec::encode_pull_response(
            unknown_tail);
    });

    ScopedPullResponse failed_with_batches;
    failed_with_batches.ok = false;
    failed_with_batches.error_code =
        SelectiveReplicationErrorCode::ScopedSequenceGap;
    failed_with_batches.descriptor = fixture.descriptor();
    failed_with_batches.batches.push_back(
        make_batch(failed_with_batches.descriptor, 1u));
    expect_throw("failed pull success state accepted", [failed_with_batches] {
        (void)SelectiveReplicationProtocolCodec::encode_pull_response(
            failed_with_batches);
    });

    ScopedPushResponse bad_error_code;
    bad_error_code.scope_id = fixture.descriptor().scope_id;
    bad_error_code.ok = false;
    bad_error_code.error_code =
        static_cast<SelectiveReplicationErrorCode>(0xFFFFu);
    expect_throw("unknown selective error code accepted", [bad_error_code] {
        (void)SelectiveReplicationProtocolCodec::encode_push_response(
            bad_error_code);
    });
}

} // namespace

int main() {
    ProtocolFixture fixture;
    test_hello_and_capabilities();
    test_pull_round_trip(fixture);
    test_push_round_trip(fixture);
    test_descriptor_and_batch_rejections(fixture);
    test_envelope_and_bounds_rejections(fixture);
    return 0;
}
