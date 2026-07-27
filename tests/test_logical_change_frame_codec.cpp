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

void test_empty_logical_frame_roundtrip() {
    mdbxc::sync::LogicalChangeFrame frame;
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalChangeFrameCodec::encode(frame);
    const mdbxc::sync::LogicalChangeFrame decoded =
        mdbxc::sync::LogicalChangeFrameCodec::decode(encoded);
    MDBXC_TEST_ASSERT(decoded.changes.empty());
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

} // namespace

int main() {
    test_logical_frame_roundtrip();
    test_empty_logical_frame_roundtrip();
    test_logical_frame_encode_rejects_reserved_change_flags();
    return 0;
}
