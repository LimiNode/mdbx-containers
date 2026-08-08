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

template <typename Fn>
bool throws_length_error(Fn fn) {
    try {
        fn();
    } catch (const std::length_error&) {
        return true;
    }
    return false;
}

class LegacyLogicalDeliveryPeer : public mdbxc::sync::ILogicalDeliveryPeer {
public:
    explicit LegacyLogicalDeliveryPeer(const mdbxc::sync::LogicalDeliveryHello& hello)
        : m_hello(hello),
          m_calls(0u) {}

    mdbxc::sync::LogicalDeliveryHello logical_delivery_hello() override {
        return m_hello;
    }

    mdbxc::sync::LogicalDeliveryAcknowledgement
    deliver_ordered_logical_delivery(
            const mdbxc::sync::LogicalDeliveryEnvelope& envelope,
            const mdbxc::sync::CodecBounds* /* bounds */ = nullptr) override {
        ++m_calls;
        mdbxc::sync::LogicalDeliveryAcknowledgement out;
        out.destination_db_uuid = envelope.destination_db_uuid;
        out.origin_node_id = envelope.origin_node_id;
        out.acknowledged_through = envelope.origin_sequence;
        return out;
    }

    std::size_t calls() const { return m_calls; }

private:
    mdbxc::sync::LogicalDeliveryHello m_hello;
    std::size_t m_calls;
};

void test_hello_round_trip_and_capability_negotiation() {
    mdbxc::sync::LogicalDeliveryHello hello;
    hello.node_id = make_node(0x30);
    hello.db_uuid = make_node(0x40);
    hello.capabilities.flags =
        static_cast<std::uint64_t>(
            mdbxc::sync::LogicalDeliveryCapability::OrderedDelivery) |
        static_cast<std::uint64_t>(
            mdbxc::sync::LogicalDeliveryCapability::CumulativeAcknowledgement) |
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
        mdbxc::sync::LogicalDeliveryCapability::OrderedDelivery) |
        static_cast<std::uint64_t>(
            mdbxc::sync::LogicalDeliveryCapability::CumulativeAcknowledgement);
    MDBXC_TEST_ASSERT(mdbxc::sync::logical_delivery_capability_negotiated(
        local, decoded.capabilities,
        mdbxc::sync::LogicalDeliveryCapability::OrderedDelivery));
    MDBXC_TEST_ASSERT(mdbxc::sync::logical_delivery_capability_negotiated(
        local, decoded.capabilities,
        mdbxc::sync::LogicalDeliveryCapability::CumulativeAcknowledgement));
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

void test_stateless_requests_round_trip() {
    const std::vector<std::uint8_t> hello_request =
        mdbxc::sync::LogicalDeliveryProtocolCodec::encode_hello_request();
    MDBXC_TEST_ASSERT(
        mdbxc::sync::LogicalDeliveryProtocolCodec::peek_message_type(
            hello_request) ==
        mdbxc::sync::LogicalDeliveryProtocolCodec::MessageType::HelloRequest);
    mdbxc::sync::LogicalDeliveryProtocolCodec::decode_hello_request(
        hello_request);

    mdbxc::sync::LogicalDeliveryRequest request;
    request.envelope = make_envelope();
    request.sender_capabilities.flags = static_cast<std::uint64_t>(
        mdbxc::sync::LogicalDeliveryCapability::OrderedDelivery) |
        static_cast<std::uint64_t>(
            mdbxc::sync::LogicalDeliveryCapability::CumulativeAcknowledgement);
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalDeliveryProtocolCodec::encode_delivery_request(
            request);
    MDBXC_TEST_ASSERT(
        mdbxc::sync::LogicalDeliveryProtocolCodec::peek_message_type(encoded) ==
        mdbxc::sync::LogicalDeliveryProtocolCodec::MessageType::DeliveryRequest);
    const mdbxc::sync::LogicalDeliveryRequest decoded =
        mdbxc::sync::LogicalDeliveryProtocolCodec::decode_delivery_request(
            encoded);
    MDBXC_TEST_ASSERT(decoded.envelope.origin_sequence ==
                      request.envelope.origin_sequence);
    MDBXC_TEST_ASSERT(decoded.envelope.frame_id == request.envelope.frame_id);
    MDBXC_TEST_ASSERT(decoded.sender_capabilities.flags ==
                      request.sender_capabilities.flags);

    std::vector<std::uint8_t> trailing_hello = hello_request;
    trailing_hello.push_back(0u);
    MDBXC_TEST_ASSERT(throws_runtime_error([&trailing_hello]() {
        mdbxc::sync::LogicalDeliveryProtocolCodec::decode_hello_request(
            trailing_hello);
    }));
    std::vector<std::uint8_t> truncated = encoded;
    truncated.pop_back();
    MDBXC_TEST_ASSERT(throws_runtime_error([&truncated]() {
        mdbxc::sync::LogicalDeliveryProtocolCodec::decode_delivery_request(
            truncated);
    }));
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

void test_acknowledgement_matches_its_delivery() {
    const mdbxc::sync::LogicalDeliveryEnvelope envelope = make_envelope();
    mdbxc::sync::LogicalDeliveryAcknowledgement acknowledgement;
    acknowledgement.destination_db_uuid = envelope.destination_db_uuid;
    acknowledgement.origin_node_id = envelope.origin_node_id;
    acknowledgement.acknowledged_through = envelope.origin_sequence;
    mdbxc::sync::validate_logical_delivery_acknowledgement_for_delivery(
        acknowledgement, envelope);

    acknowledgement.ok = false;
    acknowledgement.retryable = true;
    acknowledgement.error = "retry";
    MDBXC_TEST_ASSERT(throws_runtime_error([&acknowledgement, &envelope]() {
        mdbxc::sync::validate_logical_delivery_acknowledgement_for_delivery(
            acknowledgement, envelope);
    }));

    acknowledgement.acknowledged_through = envelope.origin_sequence - 1u;
    mdbxc::sync::validate_logical_delivery_acknowledgement_for_delivery(
        acknowledgement, envelope);
}

void test_cumulative_acknowledgement_respects_sender_tail() {
    const mdbxc::sync::LogicalDeliveryEnvelope envelope = make_envelope();
    mdbxc::sync::LogicalDeliveryAcknowledgement acknowledgement;
    acknowledgement.destination_db_uuid = envelope.destination_db_uuid;
    acknowledgement.origin_node_id = envelope.origin_node_id;
    acknowledgement.acknowledged_through = envelope.origin_sequence + 2u;

    mdbxc::sync::validate_logical_delivery_acknowledgement_for_sender(
        acknowledgement, envelope, envelope.origin_sequence + 2u, true);
    MDBXC_TEST_ASSERT(throws_runtime_error([&acknowledgement, &envelope]() {
        mdbxc::sync::validate_logical_delivery_acknowledgement_for_sender(
            acknowledgement, envelope, envelope.origin_sequence + 1u, true);
    }));
    MDBXC_TEST_ASSERT(throws_runtime_error([&acknowledgement, &envelope]() {
        mdbxc::sync::validate_logical_delivery_acknowledgement_for_sender(
            acknowledgement, envelope, envelope.origin_sequence + 2u, false);
    }));
}

void test_default_outer_message_bound_includes_protocol_overhead() {
    mdbxc::sync::CodecBounds defaults;
    mdbxc::sync::LogicalDeliveryEnvelope exact = make_envelope();
    const mdbxc::sync::LogicalChange change = exact.frame.changes[0];
    exact.frame.changes.clear();
    const std::size_t base_size =
        mdbxc::sync::LogicalDeliveryProtocolCodec::encode_delivery(exact).size();
    mdbxc::sync::LogicalDeliveryEnvelope one_empty_change = exact;
    mdbxc::sync::LogicalChange empty_change = change;
    empty_change.payload.clear();
    one_empty_change.frame.changes.push_back(empty_change);
    const std::size_t change_overhead =
        mdbxc::sync::LogicalDeliveryProtocolCodec::encode_delivery(
            one_empty_change).size() - base_size;
    const std::size_t remaining =
        defaults.max_transport_message_bytes - base_size;
    const std::size_t change_count = 1u +
        (remaining + defaults.max_value_len + change_overhead - 1u) /
        (defaults.max_value_len + change_overhead);
    std::size_t payload_left = remaining - change_count * change_overhead;
    MDBXC_TEST_ASSERT(payload_left <= change_count * defaults.max_value_len);
    for (std::size_t i = 0u; i < change_count; ++i) {
        mdbxc::sync::LogicalChange part = change;
        const std::size_t payload_size = payload_left > defaults.max_value_len
            ? defaults.max_value_len
            : payload_left;
        part.payload.assign(payload_size, 0x5Au);
        exact.frame.changes.push_back(part);
        payload_left -= payload_size;
    }
    MDBXC_TEST_ASSERT(payload_left == 0u);
    const std::vector<std::uint8_t> exact_encoded =
        mdbxc::sync::LogicalDeliveryProtocolCodec::encode_delivery(exact);
    MDBXC_TEST_ASSERT(exact_encoded.size() ==
                      defaults.max_transport_message_bytes);

    mdbxc::sync::LogicalDeliveryEnvelope too_large = exact;
    too_large.frame.changes[too_large.frame.changes.size() - 1u]
        .payload.push_back(0x5Bu);
    MDBXC_TEST_ASSERT(throws_length_error([&too_large]() {
        mdbxc::sync::LogicalDeliveryProtocolCodec::encode_delivery(too_large);
    }));

    mdbxc::sync::CodecBounds larger = defaults;
    ++larger.max_transport_message_bytes;
    const std::vector<std::uint8_t> oversized =
        mdbxc::sync::LogicalDeliveryProtocolCodec::encode_delivery(
            too_large, &larger);
    MDBXC_TEST_ASSERT(throws_length_error([&oversized]() {
        mdbxc::sync::LogicalDeliveryProtocolCodec::decode_delivery(oversized);
    }));
}

void test_legacy_peer_receives_request_through_default_forwarding() {
    const mdbxc::sync::LogicalDeliveryEnvelope envelope = make_envelope();
    mdbxc::sync::LogicalDeliveryHello hello;
    hello.node_id = make_node(0x91);
    hello.db_uuid = envelope.destination_db_uuid;
    LegacyLogicalDeliveryPeer peer(hello);
    mdbxc::sync::LogicalDeliveryRequest request;
    request.envelope = envelope;
    request.sender_capabilities.flags = static_cast<std::uint64_t>(
        mdbxc::sync::LogicalDeliveryCapability::OrderedDelivery);

    const mdbxc::sync::LogicalDeliveryAcknowledgement acknowledgement =
        peer.deliver_ordered_logical_request(request);
    MDBXC_TEST_ASSERT(peer.calls() == 1u);
    MDBXC_TEST_ASSERT(acknowledgement.destination_db_uuid ==
                      envelope.destination_db_uuid);
    MDBXC_TEST_ASSERT(acknowledgement.origin_node_id ==
                      envelope.origin_node_id);
    MDBXC_TEST_ASSERT(acknowledgement.acknowledged_through ==
                      envelope.origin_sequence);
}

} // namespace

int main() {
    test_hello_round_trip_and_capability_negotiation();
    test_delivery_and_acknowledgement_round_trip();
    test_stateless_requests_round_trip();
    test_protocol_rejects_invalid_header_and_acknowledgement();
    test_acknowledgement_matches_its_delivery();
    test_cumulative_acknowledgement_respects_sender_tail();
    test_default_outer_message_bound_includes_protocol_overhead();
    test_legacy_peer_receives_request_through_default_forwarding();
    return 0;
}
