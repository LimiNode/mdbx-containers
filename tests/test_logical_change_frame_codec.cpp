#include <mdbx_containers/sync.hpp>

#include "test_assert.hpp"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

mdbxc::sync::LogicalChange make_change(
        const std::string& schema_id,
        mdbxc::sync::LogicalTableKind kind,
        std::uint32_t schema_version,
        std::uint32_t opcode,
        const std::vector<std::uint8_t>& payload) {
    mdbxc::sync::LogicalSchemaRef ref;
    ref.schema_id = schema_id;
    ref.kind = kind;
    ref.schema_version = schema_version;
    return mdbxc::sync::LogicalChange(ref, opcode, 0, payload);
}

void write_u16_le(std::vector<std::uint8_t>& bytes,
                  std::size_t offset,
                  std::uint16_t value) {
    MDBXC_TEST_ASSERT(offset + 2u <= bytes.size());
    bytes[offset] = static_cast<std::uint8_t>(value & 0xffu);
    bytes[offset + 1u] = static_cast<std::uint8_t>((value >> 8) & 0xffu);
}

void write_u32_le(std::vector<std::uint8_t>& bytes,
                  std::size_t offset,
                  std::uint32_t value) {
    MDBXC_TEST_ASSERT(offset + 4u <= bytes.size());
    bytes[offset] = static_cast<std::uint8_t>(value & 0xffu);
    bytes[offset + 1u] = static_cast<std::uint8_t>((value >> 8) & 0xffu);
    bytes[offset + 2u] = static_cast<std::uint8_t>((value >> 16) & 0xffu);
    bytes[offset + 3u] = static_cast<std::uint8_t>((value >> 24) & 0xffu);
}

std::vector<std::uint8_t> make_encoded_single_change() {
    mdbxc::sync::LogicalChangeFrame frame;
    std::vector<std::uint8_t> payload;
    payload.push_back(42);
    frame.changes.push_back(make_change(
        "app.logical.fail_closed.v1",
        mdbxc::sync::LogicalTableKind::KeyValue,
        1,
        10,
        payload));
    return mdbxc::sync::LogicalChangeFrameCodec::encode(frame);
}

std::size_t first_change_offset() {
    return mdbxc::sync::LogicalChangeFrameCodec::magic_size() + 2u + 4u + 4u;
}

std::size_t schema_id_offset() {
    return first_change_offset() + 4u;
}

std::size_t kind_offset() {
    return schema_id_offset() + std::string("app.logical.fail_closed.v1").size();
}

std::size_t schema_version_offset() {
    return kind_offset() + 2u;
}

std::size_t opcode_offset() {
    return schema_version_offset() + 4u;
}

std::size_t change_flags_offset() {
    return opcode_offset() + 4u;
}

std::size_t payload_length_offset() {
    return change_flags_offset() + 4u;
}

void expect_decode_failure(const std::vector<std::uint8_t>& bytes,
                           const mdbxc::sync::CodecBounds* bounds = nullptr) {
    bool caught = false;
    try {
        (void)mdbxc::sync::LogicalChangeFrameCodec::decode(bytes, bounds);
    } catch (const std::exception&) {
        caught = true;
    }
    MDBXC_TEST_ASSERT(caught);
}

void expect_encode_failure(
        const mdbxc::sync::LogicalChangeFrame& frame,
        const mdbxc::sync::CodecBounds* bounds = nullptr) {
    bool caught = false;
    try {
        (void)mdbxc::sync::LogicalChangeFrameCodec::encode(frame, bounds);
    } catch (const std::exception&) {
        caught = true;
    }
    MDBXC_TEST_ASSERT(caught);
}

void test_logical_frame_roundtrip() {
    mdbxc::sync::LogicalChangeFrame frame;
    std::vector<std::uint8_t> first_payload;
    first_payload.push_back(1);
    first_payload.push_back(2);
    first_payload.push_back(3);
    frame.changes.push_back(make_change(
        "app.logical.items.v1",
        mdbxc::sync::LogicalTableKind::KeyValue,
        1,
        10,
        first_payload));

    std::vector<std::uint8_t> second_payload;
    second_payload.push_back(0);
    second_payload.push_back(255);
    frame.changes.push_back(make_change(
        "app.logical.items.v2",
        mdbxc::sync::LogicalTableKind::KeyMultiValue,
        2,
        11,
        second_payload));

    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalChangeFrameCodec::encode(frame);
    MDBXC_TEST_ASSERT(encoded.size() >
                      mdbxc::sync::LogicalChangeFrameCodec::magic_size());

    const mdbxc::sync::LogicalChangeFrame decoded =
        mdbxc::sync::LogicalChangeFrameCodec::decode(encoded);
    MDBXC_TEST_ASSERT(decoded.changes.size() == 2u);
    MDBXC_TEST_ASSERT(decoded.changes[0].schema.schema_id ==
                      "app.logical.items.v1");
    MDBXC_TEST_ASSERT(decoded.changes[0].schema.kind ==
                      mdbxc::sync::LogicalTableKind::KeyValue);
    MDBXC_TEST_ASSERT(decoded.changes[0].schema.schema_version == 1u);
    MDBXC_TEST_ASSERT(decoded.changes[0].opcode == 10u);
    MDBXC_TEST_ASSERT(decoded.changes[0].flags == 0u);
    MDBXC_TEST_ASSERT(decoded.changes[0].payload == first_payload);

    MDBXC_TEST_ASSERT(decoded.changes[1].schema.schema_id ==
                      "app.logical.items.v2");
    MDBXC_TEST_ASSERT(decoded.changes[1].schema.kind ==
                      mdbxc::sync::LogicalTableKind::KeyMultiValue);
    MDBXC_TEST_ASSERT(decoded.changes[1].schema.schema_version == 2u);
    MDBXC_TEST_ASSERT(decoded.changes[1].opcode == 11u);
    MDBXC_TEST_ASSERT(decoded.changes[1].payload == second_payload);
}

void test_logical_frame_matches_golden_vector() {
    mdbxc::sync::LogicalChangeFrame frame;
    std::vector<std::uint8_t> payload;
    payload.push_back(0xAAu);
    payload.push_back(0xBBu);
    frame.changes.push_back(make_change(
        "a",
        mdbxc::sync::LogicalTableKind::KeyValue,
        1,
        9,
        payload));

    const std::uint8_t expected_raw[] = {
        static_cast<std::uint8_t>('M'),
        static_cast<std::uint8_t>('D'),
        static_cast<std::uint8_t>('B'),
        static_cast<std::uint8_t>('X'),
        static_cast<std::uint8_t>('C'),
        static_cast<std::uint8_t>('L'),
        static_cast<std::uint8_t>('G'),
        static_cast<std::uint8_t>('F'),
        1, 0,
        0, 0, 0, 0,
        1, 0, 0, 0,
        1, 0, 0, 0,
        static_cast<std::uint8_t>('a'),
        5, 0,
        1, 0, 0, 0,
        9, 0, 0, 0,
        0, 0, 0, 0,
        2, 0, 0, 0,
        0xAAu, 0xBBu
    };
    const std::vector<std::uint8_t> expected(
        expected_raw,
        expected_raw + sizeof(expected_raw) / sizeof(expected_raw[0]));

    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalChangeFrameCodec::encode(frame);
    MDBXC_TEST_ASSERT(encoded == expected);

    const mdbxc::sync::LogicalChangeFrame decoded =
        mdbxc::sync::LogicalChangeFrameCodec::decode(expected);
    MDBXC_TEST_ASSERT(decoded.changes.size() == 1u);
    MDBXC_TEST_ASSERT(decoded.changes[0].schema.schema_id == "a");
    MDBXC_TEST_ASSERT(decoded.changes[0].opcode == 9u);
    MDBXC_TEST_ASSERT(decoded.changes[0].payload == payload);
}

void test_empty_logical_frame_roundtrip() {
    mdbxc::sync::LogicalChangeFrame frame;
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalChangeFrameCodec::encode(frame);
    const mdbxc::sync::LogicalChangeFrame decoded =
        mdbxc::sync::LogicalChangeFrameCodec::decode(encoded);
    MDBXC_TEST_ASSERT(decoded.changes.empty());
}

void test_logical_frame_encode_rejects_too_many_changes() {
    mdbxc::sync::LogicalChangeFrame frame;
    std::vector<std::uint8_t> payload;
    frame.changes.push_back(make_change(
        "app.logical.items.v1",
        mdbxc::sync::LogicalTableKind::KeyValue,
        1,
        10,
        payload));
    frame.changes.push_back(make_change(
        "app.logical.items.v1",
        mdbxc::sync::LogicalTableKind::KeyValue,
        1,
        11,
        payload));

    mdbxc::sync::CodecBounds bounds;
    bounds.max_ops_per_batch = 1;
    expect_encode_failure(frame, &bounds);
}

void test_logical_frame_encode_rejects_projected_message_size() {
    mdbxc::sync::LogicalChangeFrame frame;
    std::vector<std::uint8_t> payload;
    payload.push_back(1);
    frame.changes.push_back(make_change(
        "a",
        mdbxc::sync::LogicalTableKind::KeyValue,
        1,
        10,
        payload));

    mdbxc::sync::CodecBounds bounds;
    bounds.max_transport_message_bytes =
        mdbxc::sync::LogicalChangeFrameCodec::magic_size() + 2u + 4u;
    expect_encode_failure(frame, &bounds);
}

void test_logical_frame_encode_rejects_reserved_change_flags() {
    mdbxc::sync::LogicalChangeFrame frame;
    std::vector<std::uint8_t> payload;
    frame.changes.push_back(make_change(
        "app.logical.items.v1",
        mdbxc::sync::LogicalTableKind::KeyValue,
        1,
        10,
        payload));
    frame.changes[0].flags = 1u;

    bool caught = false;
    try {
        (void)mdbxc::sync::LogicalChangeFrameCodec::encode(frame);
    } catch (const std::logic_error&) {
        caught = true;
    }
    MDBXC_TEST_ASSERT(caught);
}

void test_logical_frame_decode_rejects_bad_magic() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    bytes[0] ^= 0xffu;
    expect_decode_failure(bytes);
}

void test_logical_frame_decode_rejects_bad_version() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    write_u16_le(bytes, mdbxc::sync::LogicalChangeFrameCodec::magic_size(), 2);
    expect_decode_failure(bytes);
}

void test_logical_frame_decode_rejects_mandatory_flags() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    write_u32_le(bytes,
                 mdbxc::sync::LogicalChangeFrameCodec::magic_size() + 2u,
                 1);
    expect_decode_failure(bytes);
}

void test_logical_frame_decode_rejects_too_many_changes_before_reserve() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    mdbxc::sync::CodecBounds bounds;
    bounds.max_ops_per_batch = 1;
    write_u32_le(bytes,
                 mdbxc::sync::LogicalChangeFrameCodec::magic_size() + 2u + 4u,
                 2);
    expect_decode_failure(bytes, &bounds);
}

void test_logical_frame_decode_rejects_empty_schema_id() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    write_u32_le(bytes, first_change_offset(), 0);
    expect_decode_failure(bytes);
}

void test_logical_frame_decode_rejects_invalid_kind() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    write_u16_le(bytes, kind_offset(), 0xffffu);
    expect_decode_failure(bytes);
}

void test_logical_frame_decode_rejects_zero_schema_version() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    write_u32_le(bytes, schema_version_offset(), 0);
    expect_decode_failure(bytes);
}

void test_logical_frame_decode_rejects_reserved_change_flags() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    write_u32_le(bytes, change_flags_offset(), 1);
    expect_decode_failure(bytes);
}

void test_logical_frame_decode_rejects_trailing_bytes() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    bytes.push_back(0);
    expect_decode_failure(bytes);
}

void test_logical_frame_decode_rejects_truncated_payload() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    bytes.resize(bytes.size() - 1u);
    expect_decode_failure(bytes);
}

void test_logical_frame_decode_rejects_schema_id_bound() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    mdbxc::sync::CodecBounds bounds;
    bounds.max_logical_schema_id_len = 4;
    expect_decode_failure(bytes, &bounds);
}

void test_logical_frame_schema_id_bound_is_not_error_bound() {
    mdbxc::sync::LogicalChangeFrame frame;
    std::vector<std::uint8_t> payload;
    frame.changes.push_back(make_change(
        "schema-id-longer-than-four",
        mdbxc::sync::LogicalTableKind::KeyValue,
        1,
        10,
        payload));

    mdbxc::sync::CodecBounds bounds;
    bounds.max_error_len = 4;
    bounds.max_logical_schema_id_len = 64;
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalChangeFrameCodec::encode(frame, &bounds);
    const mdbxc::sync::LogicalChangeFrame decoded =
        mdbxc::sync::LogicalChangeFrameCodec::decode(encoded, &bounds);
    MDBXC_TEST_ASSERT(decoded.changes.size() == 1u);
    MDBXC_TEST_ASSERT(decoded.changes[0].schema.schema_id ==
                      "schema-id-longer-than-four");
}

void test_logical_frame_accepts_exact_schema_and_payload_bounds() {
    mdbxc::sync::LogicalChangeFrame frame;
    std::vector<std::uint8_t> payload;
    payload.push_back(1);
    payload.push_back(2);
    frame.changes.push_back(make_change(
        "abc",
        mdbxc::sync::LogicalTableKind::KeyValue,
        1,
        10,
        payload));

    mdbxc::sync::CodecBounds bounds;
    bounds.max_logical_schema_id_len = 3;
    bounds.max_value_len = 2;
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalChangeFrameCodec::encode(frame, &bounds);
    const mdbxc::sync::LogicalChangeFrame decoded =
        mdbxc::sync::LogicalChangeFrameCodec::decode(encoded, &bounds);
    MDBXC_TEST_ASSERT(decoded.changes.size() == 1u);
    MDBXC_TEST_ASSERT(decoded.changes[0].schema.schema_id == "abc");
    MDBXC_TEST_ASSERT(decoded.changes[0].payload == payload);
}

void test_logical_frame_decode_rejects_payload_bound() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    mdbxc::sync::CodecBounds bounds;
    bounds.max_value_len = 0;
    expect_decode_failure(bytes, &bounds);
}

void test_logical_frame_decode_rejects_payload_length_overflow() {
    std::vector<std::uint8_t> bytes = make_encoded_single_change();
    write_u32_le(bytes, payload_length_offset(), 0xffffffffu);
    expect_decode_failure(bytes);
}

} // namespace

int main() {
    test_logical_frame_roundtrip();
    test_logical_frame_matches_golden_vector();
    test_empty_logical_frame_roundtrip();
    test_logical_frame_encode_rejects_too_many_changes();
    test_logical_frame_encode_rejects_projected_message_size();
    test_logical_frame_encode_rejects_reserved_change_flags();
    test_logical_frame_decode_rejects_bad_magic();
    test_logical_frame_decode_rejects_bad_version();
    test_logical_frame_decode_rejects_mandatory_flags();
    test_logical_frame_decode_rejects_too_many_changes_before_reserve();
    test_logical_frame_decode_rejects_empty_schema_id();
    test_logical_frame_decode_rejects_invalid_kind();
    test_logical_frame_decode_rejects_zero_schema_version();
    test_logical_frame_decode_rejects_reserved_change_flags();
    test_logical_frame_decode_rejects_trailing_bytes();
    test_logical_frame_decode_rejects_truncated_payload();
    test_logical_frame_decode_rejects_schema_id_bound();
    test_logical_frame_schema_id_bound_is_not_error_bound();
    test_logical_frame_accepts_exact_schema_and_payload_bounds();
    test_logical_frame_decode_rejects_payload_bound();
    test_logical_frame_decode_rejects_payload_length_overflow();
    return 0;
}
