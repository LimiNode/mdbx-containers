#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_DELIVERY_PROTOCOL_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_DELIVERY_PROTOCOL_HPP_INCLUDED

/// \file logical/LogicalDeliveryProtocol.hpp
/// \brief Capability-gated wire messages for ordered logical delivery.

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "LogicalDeliveryEnvelopeCodec.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Optional features understood by a logical-delivery peer.
    enum class LogicalDeliveryCapability : std::uint64_t {
        None = 0u,
        OrderedDelivery = UINT64_C(1) << 0,
        CumulativeAcknowledgement = UINT64_C(1) << 1
    };

    /// \brief Capability set exchanged before logical delivery.
    struct LogicalDeliveryCapabilities {
        std::uint64_t flags = 0u;

        bool supports(LogicalDeliveryCapability capability) const {
            const std::uint64_t bit =
                static_cast<std::uint64_t>(capability);
            return bit != 0u && (flags & bit) == bit;
        }
    };

    /// \brief Stable hello message for logical-delivery capability exchange.
    struct LogicalDeliveryHello {
        NodeId node_id{};
        DbId db_uuid{};
        LogicalDeliveryCapabilities capabilities;
    };

    /// \brief One ordered delivery attempt with sender feature context.
    /// \details The receiver identity binds this exchange to one sender-side
    /// ordered outbox route. The envelope remains the stable persisted object.
    struct LogicalDeliveryRequest {
        NodeId receiver_node_id{};
        LogicalDeliveryEnvelope envelope;
        LogicalDeliveryCapabilities sender_capabilities;
    };

    /// \brief Cumulative response to one delivery attempt.
    /// \details Without negotiated
    /// \c CumulativeAcknowledgement a successful response acknowledges exactly
    /// the delivery it answers. With that capability, a receiver can return a
    /// higher persisted contiguous frontier, bounded by the sender's durable
    /// known tail during validation.
    struct LogicalDeliveryAcknowledgement {
        DbId destination_db_uuid{};
        NodeId receiver_node_id{};
        NodeId origin_node_id{};
        std::uint64_t acknowledged_through = 0u;
        bool ok = true;
        bool retryable = false;
        std::string error;
    };

    /// \brief Validates the structural acknowledgement wire contract.
    inline void validate_logical_delivery_acknowledgement(
            const LogicalDeliveryAcknowledgement& acknowledgement,
            const CodecBounds* bounds = nullptr) {
        if (is_zero_sync_id(acknowledgement.destination_db_uuid) ||
            is_zero_sync_id(acknowledgement.receiver_node_id) ||
            is_zero_sync_id(acknowledgement.origin_node_id)) {
            throw std::runtime_error(
                "Logical delivery acknowledgement identity is incomplete");
        }
        const CodecBounds& effective =
            logical_delivery_effective_bounds(bounds);
        if (acknowledgement.error.size() > effective.max_error_len) {
            throw std::length_error(
                "logical delivery acknowledgement error exceeds max_error_len");
        }
        if (acknowledgement.ok &&
            (acknowledgement.retryable || !acknowledgement.error.empty())) {
            throw std::runtime_error(
                "Successful logical delivery acknowledgement is inconsistent");
        }
    }

    /// \brief Validates the receiver-bound ordered delivery request.
    inline void validate_logical_delivery_request(
            const LogicalDeliveryRequest& request,
            const CodecBounds* bounds = nullptr) {
        if (is_zero_sync_id(request.receiver_node_id)) {
            throw std::runtime_error(
                "Logical delivery request receiver identity is incomplete");
        }
        validate_logical_delivery_envelope(request.envelope, bounds);
    }

    /// \brief Validates an acknowledgement against the delivery it answers.
    /// \details A successful response acknowledges exactly the delivered
    /// sequence. A failed response may acknowledge only an earlier prefix.
    inline void validate_logical_delivery_acknowledgement_for_delivery(
            const LogicalDeliveryAcknowledgement& acknowledgement,
            const LogicalDeliveryEnvelope& delivery,
            const NodeId& receiver,
            const CodecBounds* bounds = nullptr) {
        validate_logical_delivery_acknowledgement(acknowledgement, bounds);
        if (compare_node_id(acknowledgement.destination_db_uuid,
                            delivery.destination_db_uuid) != 0 ||
            compare_node_id(acknowledgement.receiver_node_id, receiver) != 0 ||
            compare_node_id(acknowledgement.origin_node_id,
                            delivery.origin_node_id) != 0) {
            throw std::runtime_error(
                "Logical delivery acknowledgement identity does not match delivery");
        }
        if (acknowledgement.acknowledged_through >
            delivery.origin_sequence) {
            throw std::runtime_error(
                "Logical delivery acknowledgement exceeds delivered sequence");
        }
        if (acknowledgement.ok) {
            if (acknowledgement.acknowledged_through !=
                delivery.origin_sequence) {
                throw std::runtime_error(
                    "Successful logical delivery acknowledgement is incomplete");
            }
        } else if (acknowledgement.acknowledged_through >=
                   delivery.origin_sequence) {
            throw std::runtime_error(
                "Failed logical delivery acknowledgement includes delivery");
        }
    }

    /// \brief Validates an acknowledgement against sender recovery state.
    /// \details A negotiated cumulative success may acknowledge a known local
    /// prefix beyond the delivery that triggered it. Failed acknowledgements
    /// remain restricted to an earlier prefix, so they never report success for
    /// the delivery that failed.
    inline void validate_logical_delivery_acknowledgement_for_sender(
            const LogicalDeliveryAcknowledgement& acknowledgement,
            const LogicalDeliveryEnvelope& delivery,
            const NodeId& receiver,
            std::uint64_t known_tail,
            bool cumulative_acknowledgement_negotiated,
            const CodecBounds* bounds = nullptr) {
        validate_logical_delivery_acknowledgement(acknowledgement, bounds);
        if (compare_node_id(acknowledgement.destination_db_uuid,
                            delivery.destination_db_uuid) != 0 ||
            compare_node_id(acknowledgement.receiver_node_id, receiver) != 0 ||
            compare_node_id(acknowledgement.origin_node_id,
                            delivery.origin_node_id) != 0) {
            throw std::runtime_error(
                "Logical delivery acknowledgement identity does not match delivery");
        }
        if (known_tail < delivery.origin_sequence) {
            throw std::invalid_argument(
                "Logical delivery sender known tail precedes delivery");
        }
        if (acknowledgement.acknowledged_through > known_tail) {
            throw std::runtime_error(
                "Logical delivery acknowledgement exceeds sender known tail");
        }
        if (acknowledgement.ok) {
            if (acknowledgement.acknowledged_through <
                delivery.origin_sequence) {
                throw std::runtime_error(
                    "Successful logical delivery acknowledgement is incomplete");
            }
            if (!cumulative_acknowledgement_negotiated &&
                acknowledgement.acknowledged_through !=
                delivery.origin_sequence) {
                throw std::runtime_error(
                    "Logical delivery cumulative acknowledgement was not negotiated");
            }
        } else if (acknowledgement.acknowledged_through >=
                   delivery.origin_sequence) {
            throw std::runtime_error(
                "Failed logical delivery acknowledgement includes delivery");
        }
    }

    /// \brief Truncates an acknowledgement diagnostic to its wire bound.
    inline std::string bounded_logical_delivery_acknowledgement_error(
            const std::string& error,
            const CodecBounds* bounds = nullptr) {
        const CodecBounds& effective =
            logical_delivery_effective_bounds(bounds);
        return error.size() <= effective.max_error_len
            ? error
            : error.substr(0u, effective.max_error_len);
    }

    /// \brief Returns the capabilities this protocol version understands.
    inline std::uint64_t logical_delivery_supported_capability_flags() {
        return static_cast<std::uint64_t>(
            LogicalDeliveryCapability::OrderedDelivery) |
            static_cast<std::uint64_t>(
                LogicalDeliveryCapability::CumulativeAcknowledgement);
    }

    /// \brief Returns true when both peers offer \p capability.
    inline bool logical_delivery_capability_negotiated(
            const LogicalDeliveryCapabilities& local,
            const LogicalDeliveryCapabilities& remote,
            LogicalDeliveryCapability capability) {
        return local.supports(capability) && remote.supports(capability);
    }

    /// \brief Strict binary codec for logical-delivery protocol messages.
    class LogicalDeliveryProtocolCodec {
    public:
        enum class MessageType : std::uint8_t {
            Hello = 1u,
            Delivery = 2u,
            Acknowledgement = 3u,
            HelloRequest = 4u,
            DeliveryRequest = 5u
        };

        static const std::uint8_t* magic() {
            static const std::uint8_t value[8] =
                { 'M', 'D', 'B', 'X', 'C', 'L', 'D', 'P' };
            return value;
        }

        static std::size_t magic_size() { return 8u; }
        static std::uint16_t codec_version() { return 2u; }

        static std::vector<std::uint8_t> encode_hello(
                const LogicalDeliveryHello& hello,
                const CodecBounds* bounds = nullptr) {
            validate_hello(hello);
            std::vector<std::uint8_t> out = make_header(MessageType::Hello);
            append_id(out, hello.node_id, bounds);
            append_id(out, hello.db_uuid, bounds);
            detail::append_u64_le(out, hello.capabilities.flags);
            validate_size(out, bounds);
            return out;
        }

        static LogicalDeliveryHello decode_hello(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            Cursor cur = make_cursor(encoded, bounds);
            check_header(cur, MessageType::Hello);
            LogicalDeliveryHello out;
            read_id(cur, out.node_id);
            read_id(cur, out.db_uuid);
            out.capabilities.flags = read_u64_le(cur);
            validate_hello(out);
            check_consumed(cur);
            return out;
        }

        /// \brief Encodes a stateless request for the remote hello.
        static std::vector<std::uint8_t> encode_hello_request(
                const CodecBounds* bounds = nullptr) {
            std::vector<std::uint8_t> out =
                make_header(MessageType::HelloRequest);
            validate_size(out, bounds);
            return out;
        }

        /// \brief Strictly decodes a stateless remote-hello request.
        static void decode_hello_request(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            Cursor cur = make_cursor(encoded, bounds);
            check_header(cur, MessageType::HelloRequest);
            check_consumed(cur);
        }

        static std::vector<std::uint8_t> encode_delivery(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr) {
            const std::vector<std::uint8_t> nested =
                LogicalDeliveryEnvelopeCodec::encode(envelope, bounds);
            std::vector<std::uint8_t> out =
                make_header(MessageType::Delivery);
            append_u32_size(out, nested.size(),
                            "logical delivery envelope exceeds u32");
            append_bytes(out, nested.empty() ? nullptr : &nested[0],
                         nested.size(), bounds);
            validate_size(out, bounds);
            return out;
        }

        static LogicalDeliveryEnvelope decode_delivery(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            Cursor cur = make_cursor(encoded, bounds);
            check_header(cur, MessageType::Delivery);
            const std::uint32_t size = read_u32_le(cur);
            const std::uint8_t* data = read_bytes(cur, size);
            std::vector<std::uint8_t> nested(size);
            if (size != 0u) {
                std::memcpy(&nested[0], data, size);
            }
            const LogicalDeliveryEnvelope out =
                LogicalDeliveryEnvelopeCodec::decode(nested, bounds);
            check_consumed(cur);
            return out;
        }

        /// \brief Encodes an ordered delivery with sender capabilities.
        static std::vector<std::uint8_t> encode_delivery_request(
                const LogicalDeliveryRequest& request,
                const CodecBounds* bounds = nullptr) {
            validate_logical_delivery_request(request, bounds);
            const std::vector<std::uint8_t> nested =
                LogicalDeliveryEnvelopeCodec::encode(request.envelope, bounds);
            std::vector<std::uint8_t> out =
                make_header(MessageType::DeliveryRequest);
            append_id(out, request.receiver_node_id, bounds);
            detail::append_u64_le(out, request.sender_capabilities.flags);
            append_u32_size(out, nested.size(),
                            "logical delivery envelope exceeds u32");
            append_bytes(out, nested.empty() ? nullptr : &nested[0],
                         nested.size(), bounds);
            validate_size(out, bounds);
            return out;
        }

        /// \brief Strictly decodes an ordered delivery with sender capabilities.
        static LogicalDeliveryRequest decode_delivery_request(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            Cursor cur = make_cursor(encoded, bounds);
            check_header(cur, MessageType::DeliveryRequest);
            LogicalDeliveryRequest out;
            read_id(cur, out.receiver_node_id);
            out.sender_capabilities.flags = read_u64_le(cur);
            const std::uint32_t size = read_u32_le(cur);
            const std::uint8_t* data = read_bytes(cur, size);
            std::vector<std::uint8_t> nested(size);
            if (size != 0u) {
                std::memcpy(&nested[0], data, size);
            }
            out.envelope = LogicalDeliveryEnvelopeCodec::decode(nested, bounds);
            validate_logical_delivery_request(out, bounds);
            check_consumed(cur);
            return out;
        }

        static std::vector<std::uint8_t> encode_acknowledgement(
                const LogicalDeliveryAcknowledgement& acknowledgement,
                const CodecBounds* bounds = nullptr) {
            validate_logical_delivery_acknowledgement(acknowledgement, bounds);
            std::vector<std::uint8_t> out =
                make_header(MessageType::Acknowledgement);
            append_id(out, acknowledgement.destination_db_uuid, bounds);
            append_id(out, acknowledgement.receiver_node_id, bounds);
            append_id(out, acknowledgement.origin_node_id, bounds);
            detail::append_u64_le(out, acknowledgement.acknowledged_through);
            append_bool(out, acknowledgement.ok);
            append_bool(out, acknowledgement.retryable);
            append_string(out, acknowledgement.error, bounds);
            validate_size(out, bounds);
            return out;
        }

        static LogicalDeliveryAcknowledgement decode_acknowledgement(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            Cursor cur = make_cursor(encoded, bounds);
            check_header(cur, MessageType::Acknowledgement);
            LogicalDeliveryAcknowledgement out;
            read_id(cur, out.destination_db_uuid);
            read_id(cur, out.receiver_node_id);
            read_id(cur, out.origin_node_id);
            out.acknowledged_through = read_u64_le(cur);
            out.ok = read_bool(cur);
            out.retryable = read_bool(cur);
            out.error = read_string(cur, bounds);
            validate_logical_delivery_acknowledgement(out, bounds);
            check_consumed(cur);
            return out;
        }

        static MessageType peek_message_type(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            Cursor cur = make_cursor(encoded, bounds);
            return read_header_type(cur);
        }

    private:
        struct Cursor {
            const std::uint8_t* data;
            std::size_t size;
            std::size_t pos;
        };

        static std::vector<std::uint8_t> make_header(MessageType type) {
            std::vector<std::uint8_t> out;
            out.reserve(64u);
            append_bytes(out, magic(), magic_size(), nullptr);
            detail::append_u16_le(out, codec_version());
            out.push_back(static_cast<std::uint8_t>(type));
            detail::append_u32_le(out, 0u);
            return out;
        }

        static Cursor make_cursor(const std::vector<std::uint8_t>& encoded,
                                  const CodecBounds* bounds) {
            const CodecBounds& effective = logical_delivery_effective_bounds(
                bounds);
            if (encoded.size() > effective.max_transport_message_bytes) {
                throw std::length_error(
                    "logical delivery protocol exceeds max_transport_message_bytes");
            }
            Cursor out = {
                encoded.empty() ? nullptr : &encoded[0],
                encoded.size(),
                0u
            };
            return out;
        }

        static void validate_hello(const LogicalDeliveryHello& hello) {
            if (is_zero_sync_id(hello.node_id) ||
                is_zero_sync_id(hello.db_uuid)) {
                throw std::runtime_error(
                    "Logical delivery hello identity is incomplete");
            }
        }

        static void validate_size(const std::vector<std::uint8_t>& out,
                                  const CodecBounds* bounds) {
            const CodecBounds& effective = logical_delivery_effective_bounds(
                bounds);
            if (out.size() > effective.max_transport_message_bytes) {
                throw std::length_error(
                    "logical delivery protocol exceeds max_transport_message_bytes");
            }
        }

        static void append_bytes(std::vector<std::uint8_t>& out,
                                 const std::uint8_t* bytes,
                                 std::size_t size,
                                 const CodecBounds* bounds) {
            if (size == 0u) {
                return;
            }
            if (bytes == nullptr ||
                size > (std::numeric_limits<std::size_t>::max)() - out.size()) {
                throw std::length_error("logical delivery protocol size overflow");
            }
            const CodecBounds& effective = logical_delivery_effective_bounds(
                bounds);
            if (out.size() + size > effective.max_transport_message_bytes) {
                throw std::length_error(
                    "logical delivery protocol exceeds max_transport_message_bytes");
            }
            const std::size_t begin = out.size();
            out.resize(begin + size);
            std::memcpy(&out[begin], bytes, size);
        }

        static void append_id(std::vector<std::uint8_t>& out,
                              const NodeId& id,
                              const CodecBounds* bounds) {
            append_bytes(out, id.data(), id.size(), bounds);
        }

        static void append_bool(std::vector<std::uint8_t>& out, bool value) {
            out.push_back(value ? 1u : 0u);
        }

        static void append_u32_size(std::vector<std::uint8_t>& out,
                                    std::size_t value,
                                    const char* message) {
            if (value > static_cast<std::size_t>(
                    (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error(message);
            }
            detail::append_u32_le(out, static_cast<std::uint32_t>(value));
        }

        static void append_string(std::vector<std::uint8_t>& out,
                                  const std::string& value,
                                  const CodecBounds* bounds) {
            const CodecBounds& effective =
                logical_delivery_effective_bounds(bounds);
            if (value.size() > effective.max_error_len) {
                throw std::length_error(
                    "logical delivery acknowledgement error exceeds max_error_len");
            }
            append_u32_size(out, value.size(),
                            "logical delivery acknowledgement error exceeds u32");
            append_bytes(out,
                         value.empty() ? nullptr :
                         reinterpret_cast<const std::uint8_t*>(value.data()),
                         value.size(), bounds);
        }

        static void check_bounds(const Cursor& cur, std::size_t count) {
            if (cur.pos > cur.size || count > cur.size - cur.pos) {
                throw std::runtime_error(
                    "Logical delivery protocol buffer underrun");
            }
        }

        static const std::uint8_t* read_bytes(Cursor& cur,
                                              std::size_t count) {
            check_bounds(cur, count);
            if (count == 0u) {
                return nullptr;
            }
            const std::uint8_t* out = cur.data + cur.pos;
            cur.pos += count;
            return out;
        }

        static std::uint8_t read_u8(Cursor& cur) {
            check_bounds(cur, 1u);
            return cur.data[cur.pos++];
        }

        static bool read_bool(Cursor& cur) {
            const std::uint8_t value = read_u8(cur);
            if (value > 1u) {
                throw std::runtime_error(
                    "Invalid logical delivery protocol bool");
            }
            return value != 0u;
        }

        static std::uint16_t read_u16_le(Cursor& cur) {
            check_bounds(cur, 2u);
            const std::uint16_t out = detail::read_u16_le(cur.data + cur.pos);
            cur.pos += 2u;
            return out;
        }

        static std::uint32_t read_u32_le(Cursor& cur) {
            check_bounds(cur, 4u);
            const std::uint32_t out = detail::read_u32_le(cur.data + cur.pos);
            cur.pos += 4u;
            return out;
        }

        static std::uint64_t read_u64_le(Cursor& cur) {
            check_bounds(cur, 8u);
            const std::uint64_t out = detail::read_u64_le(cur.data + cur.pos);
            cur.pos += 8u;
            return out;
        }

        static void read_id(Cursor& cur, NodeId& out) {
            const std::uint8_t* bytes = read_bytes(cur, out.size());
            std::memcpy(out.data(), bytes, out.size());
        }

        static std::string read_string(Cursor& cur,
                                       const CodecBounds* bounds) {
            const std::uint32_t size = read_u32_le(cur);
            const CodecBounds& effective =
                logical_delivery_effective_bounds(bounds);
            if (size > effective.max_error_len) {
                throw std::length_error(
                    "logical delivery acknowledgement error exceeds max_error_len");
            }
            const std::uint8_t* bytes = read_bytes(cur, size);
            return size == 0u ? std::string() :
                std::string(reinterpret_cast<const char*>(bytes), size);
        }

        static MessageType read_header_type(Cursor& cur) {
            check_bounds(cur, magic_size());
            if (std::memcmp(cur.data + cur.pos, magic(), magic_size()) != 0) {
                throw std::runtime_error("Logical delivery protocol magic mismatch");
            }
            cur.pos += magic_size();
            if (read_u16_le(cur) != codec_version()) {
                throw std::runtime_error(
                    "Unsupported logical delivery protocol codec version");
            }
            const std::uint8_t raw_type = read_u8(cur);
            if (read_u32_le(cur) != 0u) {
                throw std::runtime_error(
                    "Unknown mandatory logical delivery protocol flags");
            }
            switch (raw_type) {
                case static_cast<std::uint8_t>(MessageType::Hello):
                    return MessageType::Hello;
                case static_cast<std::uint8_t>(MessageType::Delivery):
                    return MessageType::Delivery;
                case static_cast<std::uint8_t>(MessageType::Acknowledgement):
                    return MessageType::Acknowledgement;
                case static_cast<std::uint8_t>(MessageType::HelloRequest):
                    return MessageType::HelloRequest;
                case static_cast<std::uint8_t>(MessageType::DeliveryRequest):
                    return MessageType::DeliveryRequest;
                default:
                    throw std::runtime_error(
                        "Unexpected logical delivery protocol message type");
            }
        }

        static void check_header(Cursor& cur, MessageType expected) {
            if (read_header_type(cur) != expected) {
                throw std::runtime_error(
                    "Unexpected logical delivery protocol message type");
            }
        }

        static void check_consumed(const Cursor& cur) {
            if (cur.pos != cur.size) {
                throw std::runtime_error(
                    "Trailing bytes after logical delivery protocol message");
            }
        }
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_DELIVERY_PROTOCOL_HPP_INCLUDED
