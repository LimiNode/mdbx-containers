#include <mdbx_containers/sync.hpp>

#include "test_assert.hpp"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId out{};
    for (std::size_t i = 0; i < out.size(); ++i) {
        out[i] = static_cast<std::uint8_t>(seed + i);
    }
    return out;
}

mdbxc::sync::LogicalDeliveryEnvelope make_envelope() {
    mdbxc::sync::LogicalSchemaRef schema;
    schema.schema_id = "app.protocol.v1";
    schema.kind = mdbxc::sync::LogicalTableKind::KeyValue;
    schema.schema_version = 1u;
    std::vector<std::uint8_t> payload;
    payload.push_back(0x5Au);

    mdbxc::sync::LogicalDeliveryEnvelope out;
    out.destination_db_uuid = make_node(0x10);
    out.origin_node_id = make_node(0x20);
    out.origin_sequence = 7u;
    out.frame_id = "frame-7";
    out.frame.changes.push_back(
        mdbxc::sync::LogicalChange(schema, 1u, 0u, payload));
    return out;
}

template <typename Fn>
bool throws_runtime_error(Fn fn) {
    try {
        fn();
    } catch (const std::runtime_error&) {
        return true;
    }
    return false;
}

void test_hello_round_trip_and_capability_negotiation() {
    mdbxc::sync::LogicalDeliveryHello hello;
    hello.node_id = make_node(0x30);
    hello.db_uuid = make_node(0x40);
    hello.capabilities.flags =
        static_cast<std::uint64_t>(
            mdbxc::sync::LogicalDeliveryCapability::OrderedDelivery) |
        (UINT64_C(1) << 48);

    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalDeliveryProtocolCodec::encode_hello(hello);
    MDBXC_TEST_ASSERT(
        mdbxc::sync::LogicalDeliveryProtocolCodec::peek_message_type(encoded) ==
        mdbxc::sync::LogicalDeliveryProtocolCodec::MessageType::Hello);
    const mdbxc::sync::LogicalDeliveryHello decoded =
        mdbxc::sync::LogicalDeliveryProtocolCodec::decode_hello(encoded);
    MDBXC_TEST_ASSERT(decoded.node_id == hello.node_id);
    MDBXC_TEST_ASSERT(decoded.db_uuid == hello.db_uuid);
    MDBXC_TEST_ASSERT(decoded.capabilities.flags == hello.capabilities.flags);

    mdbxc::sync::LogicalDeliveryCapabilities local;
    local.flags = static_cast<std::uint64_t>(
        mdbxc::sync::LogicalDeliveryCapability::OrderedDelivery);
    MDBXC_TEST_ASSERT(mdbxc::sync::logical_delivery_capability_negotiated(
        local, decoded.capabilities,
        mdbxc::sync::LogicalDeliveryCapability::OrderedDelivery));
}

void test_delivery_and_acknowledgement_round_trip() {
    const mdbxc::sync::LogicalDeliveryEnvelope envelope = make_envelope();
    const std::vector<std::uint8_t> delivery =
        mdbxc::sync::LogicalDeliveryProtocolCodec::encode_delivery(envelope);
    const mdbxc::sync::LogicalDeliveryEnvelope decoded_envelope =
        mdbxc::sync::LogicalDeliveryProtocolCodec::decode_delivery(delivery);
    MDBXC_TEST_ASSERT(decoded_envelope.destination_db_uuid ==
                      envelope.destination_db_uuid);
    MDBXC_TEST_ASSERT(decoded_envelope.origin_node_id ==
                      envelope.origin_node_id);
    MDBXC_TEST_ASSERT(decoded_envelope.origin_sequence ==
                      envelope.origin_sequence);
    MDBXC_TEST_ASSERT(decoded_envelope.frame_id == envelope.frame_id);
    MDBXC_TEST_ASSERT(decoded_envelope.frame.changes.size() == 1u);

    mdbxc::sync::LogicalDeliveryAcknowledgement acknowledgement;
    acknowledgement.destination_db_uuid = envelope.destination_db_uuid;
    acknowledgement.origin_node_id = envelope.origin_node_id;
    acknowledgement.acknowledged_through = 6u;
    acknowledgement.ok = false;
    acknowledgement.retryable = true;
    acknowledgement.error = "delivery sequence gap";
    const std::vector<std::uint8_t> encoded_ack =
        mdbxc::sync::LogicalDeliveryProtocolCodec::encode_acknowledgement(
            acknowledgement);
    const mdbxc::sync::LogicalDeliveryAcknowledgement decoded_ack =
        mdbxc::sync::LogicalDeliveryProtocolCodec::decode_acknowledgement(
            encoded_ack);
    MDBXC_TEST_ASSERT(decoded_ack.destination_db_uuid ==
                      acknowledgement.destination_db_uuid);
    MDBXC_TEST_ASSERT(decoded_ack.origin_node_id ==
                      acknowledgement.origin_node_id);
    MDBXC_TEST_ASSERT(decoded_ack.acknowledged_through == 6u);
    MDBXC_TEST_ASSERT(!decoded_ack.ok);
    MDBXC_TEST_ASSERT(decoded_ack.retryable);
    MDBXC_TEST_ASSERT(decoded_ack.error == acknowledgement.error);
}

void test_protocol_rejects_invalid_header_and_acknowledgement() {
    mdbxc::sync::LogicalDeliveryHello hello;
    hello.node_id = make_node(0x50);
    hello.db_uuid = make_node(0x60);
    std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalDeliveryProtocolCodec::encode_hello(hello);
    encoded[10] = 0x7Fu;
    MDBXC_TEST_ASSERT(throws_runtime_error([&encoded]() {
        mdbxc::sync::LogicalDeliveryProtocolCodec::decode_hello(encoded);
    }));

    mdbxc::sync::LogicalDeliveryAcknowledgement acknowledgement;
    acknowledgement.destination_db_uuid = make_node(0x70);
    acknowledgement.origin_node_id = make_node(0x80);
    acknowledgement.ok = true;
    acknowledgement.retryable = true;
    MDBXC_TEST_ASSERT(throws_runtime_error([&acknowledgement]() {
        mdbxc::sync::LogicalDeliveryProtocolCodec::encode_acknowledgement(
            acknowledgement);
    }));
}

} // namespace

int main() {
    test_hello_round_trip_and_capability_negotiation();
    test_delivery_and_acknowledgement_round_trip();
    test_protocol_rejects_invalid_header_and_acknowledgement();
    return 0;
}
