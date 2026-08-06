#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_DELIVERY_ENVELOPE_CODEC_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_DELIVERY_ENVELOPE_CODEC_HPP_INCLUDED

/// \file logical/LogicalDeliveryEnvelopeCodec.hpp
/// \brief Stable little-endian codec for logical delivery envelopes.

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx_containers/sync/protocol/CodecBounds.hpp>
#include "LogicalChangeFrameCodec.hpp"
#include "LogicalDeliveryEnvelope.hpp"
#include <mdbx_containers/sync/common.hpp>

namespace mdbxc {
namespace sync {

    /// \brief Stable binary codec for \c LogicalDeliveryEnvelope.
    /// \details The envelope supplies routing and replay identity around a
    /// nested \c LogicalChangeFrame. The nested frame is decoded with the same
    /// bounds so malformed payloads fail before adapter preflight.
    class LogicalDeliveryEnvelopeCodec {
    public:
        /// \brief Encoded magic prefix (8 bytes, no NUL terminator).
        static const std::uint8_t* magic() {
            static const std::uint8_t m[8] =
                { 'M','D','B','X','C','L','D','E' };
            return m;
        }

        /// \brief Magic prefix length in bytes.
        static std::size_t magic_size() { return 8; }

        /// \brief Supported delivery envelope codec version.
        static std::uint16_t codec_version() { return 1; }

        /// \brief Encodes \p envelope.
        static std::vector<std::uint8_t> encode(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            validate_envelope(envelope, bounds);
            const std::vector<std::uint8_t> frame_bytes =
                LogicalChangeFrameCodec::encode(envelope.frame, bounds);

            std::vector<std::uint8_t> out;
            out.reserve(128);
            append_bytes(out, magic(), magic_size(), bounds);
            append_u16_le(out, codec_version(), bounds);
            append_u32_le(out, 0, bounds);
            append_bytes(out, envelope.destination_db_uuid.data(),
                         envelope.destination_db_uuid.size(), bounds);
            append_bytes(out, envelope.origin_node_id.data(),
                         envelope.origin_node_id.size(), bounds);
            append_u64_le(out, envelope.origin_sequence, bounds);
            append_string(out, envelope.frame_id, bounds);
            append_u32_size(out, frame_bytes.size(), bounds,
                            "logical delivery frame length exceeds u32");
            append_bytes(out,
                         frame_bytes.empty() ? nullptr : &frame_bytes[0],
                         frame_bytes.size(), bounds);
            return out;
        }

        /// \brief Strictly decodes \p data.
        static LogicalDeliveryEnvelope decode(
                const std::vector<std::uint8_t>& data,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            Cursor cur = make_cursor(data, bounds);
            check_header(cur);

            LogicalDeliveryEnvelope envelope;
            read_id(cur, envelope.destination_db_uuid);
            read_id(cur, envelope.origin_node_id);
            envelope.origin_sequence = read_u64_le(cur);
            envelope.frame_id = read_string(cur, bounds);
            const std::uint32_t frame_size = read_u32_le(cur);
            const std::uint8_t* frame_bytes = read_bytes(cur, frame_size);
            std::vector<std::uint8_t> nested(frame_size);
            if (frame_size != 0u) {
                std::memcpy(&nested[0], frame_bytes, frame_size);
            }
            envelope.frame = LogicalChangeFrameCodec::decode(nested, bounds);
            validate_envelope(envelope, bounds);
            check_consumed(cur);
            return envelope;
        }

    private:
        struct Cursor {
            const std::uint8_t* data;
            std::size_t size;
            std::size_t pos;
        };

        static const CodecBounds* effective_bounds(
                const CodecBounds* bounds) {
            static const CodecBounds defaults;
            return bounds != nullptr ? bounds : &defaults;
        }

        static void validate_envelope(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds) {
            validate_logical_delivery_envelope(envelope, bounds);
        }

        static Cursor make_cursor(const std::vector<std::uint8_t>& data,
                                  const CodecBounds* bounds) {
            if (data.size() > bounds->max_transport_message_bytes) {
                throw std::length_error(
                    "logical delivery envelope exceeds "
                    "max_transport_message_bytes");
            }
            Cursor cur;
            cur.data = data.empty() ? nullptr : &data[0];
            cur.size = data.size();
            cur.pos = 0;
            return cur;
        }

        static void check_header(Cursor& cur) {
            check_bounds(cur, magic_size());
            if (std::memcmp(cur.data + cur.pos, magic(), magic_size()) != 0) {
                throw std::runtime_error(
                    "Logical delivery envelope magic mismatch");
            }
            cur.pos += magic_size();

            const std::uint16_t version = read_u16_le(cur);
            if (version != codec_version()) {
                throw std::runtime_error(
                    "Unsupported logical delivery envelope codec_version");
            }
            const std::uint32_t flags = read_u32_le(cur);
            if (flags != 0) {
                throw std::runtime_error(
                    "Unknown mandatory logical delivery envelope flags");
            }
        }

        static void check_consumed(const Cursor& cur) {
            if (cur.pos != cur.size) {
                throw std::runtime_error(
                    "Trailing bytes after logical delivery envelope");
            }
        }

        static void check_bounds(const Cursor& cur, std::size_t n) {
            if (cur.pos > cur.size || n > cur.size - cur.pos) {
                throw std::runtime_error(
                    "Logical delivery envelope codec buffer underrun");
            }
        }

        static void ensure_append_size(
                const std::vector<std::uint8_t>& out,
                std::size_t n,
                const CodecBounds* bounds) {
            if (n > std::numeric_limits<std::size_t>::max() - out.size()) {
                throw std::length_error(
                    "logical delivery envelope size overflow");
            }
            if (out.size() + n > bounds->max_transport_message_bytes) {
                throw std::length_error(
                    "logical delivery envelope exceeds "
                    "max_transport_message_bytes");
            }
        }

        static void append_bytes(std::vector<std::uint8_t>& out,
                                 const std::uint8_t* src,
                                 std::size_t n,
                                 const CodecBounds* bounds) {
            if (n == 0) {
                return;
            }
            ensure_append_size(out, n, bounds);
            const std::size_t base = out.size();
            out.resize(base + n);
            std::memcpy(&out[base], src, n);
        }

        static void append_u16_le(std::vector<std::uint8_t>& out,
                                  std::uint16_t value,
                                  const CodecBounds* bounds) {
            ensure_append_size(out, 2, bounds);
            detail::append_u16_le(out, value);
        }

        static void append_u32_le(std::vector<std::uint8_t>& out,
                                  std::uint32_t value,
                                  const CodecBounds* bounds) {
            ensure_append_size(out, 4, bounds);
            detail::append_u32_le(out, value);
        }

        static void append_u64_le(std::vector<std::uint8_t>& out,
                                  std::uint64_t value,
                                  const CodecBounds* bounds) {
            ensure_append_size(out, 8, bounds);
            detail::append_u64_le(out, value);
        }

        static void append_u32_size(std::vector<std::uint8_t>& out,
                                    std::size_t value,
                                    const CodecBounds* bounds,
                                    const char* label) {
            if (value > static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max())) {
                throw std::length_error(label);
            }
            append_u32_le(out, static_cast<std::uint32_t>(value), bounds);
        }

        static void append_string(std::vector<std::uint8_t>& out,
                                  const std::string& value,
                                  const CodecBounds* bounds) {
            if (value.empty()) {
                throw std::runtime_error(
                    "Logical delivery envelope frame id is empty");
            }
            if (value.size() >
                bounds->max_logical_delivery_frame_id_len) {
                throw std::length_error(
                    "logical delivery frame id exceeds "
                    "max_logical_delivery_frame_id_len");
            }
            append_u32_size(out, value.size(), bounds,
                            "logical delivery frame id length exceeds u32");
            append_bytes(out,
                         reinterpret_cast<const std::uint8_t*>(value.data()),
                         value.size(), bounds);
        }

        static std::uint16_t read_u16_le(Cursor& cur) {
            check_bounds(cur, 2);
            const std::uint16_t value =
                detail::read_u16_le(cur.data + cur.pos);
            cur.pos += 2;
            return value;
        }

        static std::uint32_t read_u32_le(Cursor& cur) {
            check_bounds(cur, 4);
            const std::uint32_t value =
                detail::read_u32_le(cur.data + cur.pos);
            cur.pos += 4;
            return value;
        }

        static std::uint64_t read_u64_le(Cursor& cur) {
            check_bounds(cur, 8);
            const std::uint64_t value =
                detail::read_u64_le(cur.data + cur.pos);
            cur.pos += 8;
            return value;
        }

        static const std::uint8_t* read_bytes(Cursor& cur, std::size_t n) {
            check_bounds(cur, n);
            if (n == 0) {
                return nullptr;
            }
            const std::uint8_t* out = cur.data + cur.pos;
            cur.pos += n;
            return out;
        }

        static void read_id(Cursor& cur, NodeId& out) {
            const std::uint8_t* bytes = read_bytes(cur, out.size());
            std::memcpy(out.data(), bytes, out.size());
        }

        static std::string read_string(Cursor& cur,
                                       const CodecBounds* bounds) {
            const std::uint32_t len = read_u32_le(cur);
            if (len == 0) {
                throw std::runtime_error(
                    "Logical delivery envelope frame id is empty");
            }
            if (len > bounds->max_logical_delivery_frame_id_len) {
                throw std::length_error(
                    "logical delivery frame id exceeds "
                    "max_logical_delivery_frame_id_len");
            }
            const std::uint8_t* bytes = read_bytes(cur, len);
            return std::string(reinterpret_cast<const char*>(bytes), len);
        }
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_DELIVERY_ENVELOPE_CODEC_HPP_INCLUDED
