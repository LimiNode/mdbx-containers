#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_CHANGE_FRAME_CODEC_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_CHANGE_FRAME_CODEC_HPP_INCLUDED

/// \file LogicalChangeFrameCodec.hpp
/// \brief Stable little-endian codec for explicit logical change frames.

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "CodecBounds.hpp"
#include "LogicalChange.hpp"
#include "common.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Stable binary codec for \c LogicalChangeFrame.
    /// \details This is not yet embedded in \c TransportMessageCodec. It gives
    /// explicit logical sync paths a strict fail-closed frame boundary while
    /// raw pull/push transport DTOs remain unchanged. The frame is only a
    /// payload boundary; routing, ordering, and replay protection belong to a
    /// separate delivery envelope or caller-owned transport contract.
    class LogicalChangeFrameCodec {
    public:
        /// \brief Encoded magic prefix (8 bytes, no NUL terminator).
        static const std::uint8_t* magic() {
            static const std::uint8_t m[8] =
                { 'M','D','B','X','C','L','G','F' };
            return m;
        }

        /// \brief Magic prefix length in bytes.
        static std::size_t magic_size() { return 8; }

        /// \brief Supported logical frame codec version.
        static std::uint16_t codec_version() { return 1; }

        /// \brief Encodes \p frame.
        static std::vector<std::uint8_t> encode(
                const LogicalChangeFrame& frame,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            if (frame.changes.size() > bounds->max_ops_per_batch) {
                throw std::length_error(
                    "logical changes exceed max_ops_per_batch");
            }
            std::vector<std::uint8_t> out;
            out.reserve(64);
            append_bytes(out, magic(), magic_size(), bounds);
            append_u16_le(out, codec_version(), bounds);
            append_u32_le(out, 0, bounds);
            append_u32_size(out, frame.changes.size(), bounds,
                            "logical change count exceeds u32");
            for (std::size_t i = 0; i < frame.changes.size(); ++i) {
                append_change(out, frame.changes[i], bounds);
            }
            validate_message_size(out, bounds);
            return out;
        }

        /// \brief Strictly decodes \p data.
        static LogicalChangeFrame decode(
                const std::vector<std::uint8_t>& data,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            Cursor cur = make_cursor(data, bounds);
            check_header(cur);
            const std::uint32_t count = read_u32_le(cur);
            if (count > bounds->max_ops_per_batch) {
                throw std::length_error(
                    "logical changes exceed max_ops_per_batch");
            }
            LogicalChangeFrame frame;
            frame.changes.reserve(count);
            for (std::uint32_t i = 0; i < count; ++i) {
                frame.changes.push_back(read_change(cur, bounds));
            }
            check_consumed(cur);
            return frame;
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

        static Cursor make_cursor(const std::vector<std::uint8_t>& data,
                                  const CodecBounds* bounds) {
            if (data.size() > bounds->max_transport_message_bytes) {
                throw std::length_error(
                    "logical frame exceeds max_transport_message_bytes");
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
                throw std::runtime_error("Logical frame magic mismatch");
            }
            cur.pos += magic_size();

            const std::uint16_t version = read_u16_le(cur);
            if (version != codec_version()) {
                throw std::runtime_error(
                    "Unsupported logical frame codec_version");
            }
            const std::uint32_t flags = read_u32_le(cur);
            if (flags != 0) {
                throw std::runtime_error(
                    "Unknown mandatory logical frame flags");
            }
        }

        static void check_consumed(const Cursor& cur) {
            if (cur.pos != cur.size) {
                throw std::runtime_error(
                    "Trailing bytes after logical frame");
            }
        }

        static void check_bounds(const Cursor& cur, std::size_t n) {
            if (cur.pos > cur.size || n > cur.size - cur.pos) {
                throw std::runtime_error(
                    "Logical frame codec buffer underrun");
            }
        }

        static void append_bytes(std::vector<std::uint8_t>& out,
                                 const std::uint8_t* src,
                                 std::size_t n,
                                 const CodecBounds* bounds) {
            if (n == 0) {
                return;
            }
            const std::size_t base = out.size();
            if (n > std::numeric_limits<std::size_t>::max() - base) {
                throw std::length_error("logical frame size overflow");
            }
            if (base + n > bounds->max_transport_message_bytes) {
                throw std::length_error(
                    "logical frame exceeds max_transport_message_bytes");
            }
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

        static void ensure_append_size(
                const std::vector<std::uint8_t>& out,
                std::size_t n,
                const CodecBounds* bounds) {
            if (n > std::numeric_limits<std::size_t>::max() - out.size()) {
                throw std::length_error("logical frame size overflow");
            }
            if (out.size() + n > bounds->max_transport_message_bytes) {
                throw std::length_error(
                    "logical frame exceeds max_transport_message_bytes");
            }
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
                                  const CodecBounds* bounds,
                                  const char* label) {
            if (value.empty()) {
                throw std::runtime_error(label);
            }
            if (value.size() > bounds->max_logical_schema_id_len) {
                throw std::length_error(
                    "logical schema id exceeds max_logical_schema_id_len");
            }
            append_u32_size(out, value.size(), bounds,
                            "string length exceeds u32");
            append_bytes(out,
                         reinterpret_cast<const std::uint8_t*>(value.data()),
                         value.size(), bounds);
        }

        static void append_payload(std::vector<std::uint8_t>& out,
                                   const std::vector<std::uint8_t>& payload,
                                   const CodecBounds* bounds) {
            if (payload.size() > bounds->max_value_len) {
                throw std::length_error(
                    "logical payload exceeds max_value_len");
            }
            append_u32_size(out, payload.size(), bounds,
                            "logical payload length exceeds u32");
            append_bytes(out, payload.empty() ? nullptr : &payload[0],
                         payload.size(), bounds);
        }

        static void append_kind(std::vector<std::uint8_t>& out,
                                LogicalTableKind kind,
                                const CodecBounds* bounds) {
            if (!is_known_logical_table_kind(kind)) {
                throw std::logic_error(
                    "LogicalChange has unknown table kind");
            }
            append_u16_le(out, static_cast<std::uint16_t>(kind),
                          bounds);
        }

        static void append_change(std::vector<std::uint8_t>& out,
                                  const LogicalChange& change,
                                  const CodecBounds* bounds) {
            if (!is_logical_schema_ref_complete(change.schema)) {
                throw std::logic_error(
                    "LogicalChange schema ref is incomplete");
            }
            if (change.flags != 0) {
                throw std::logic_error(
                    "LogicalChange flags are reserved and must be zero");
            }
            append_string(out, change.schema.schema_id, bounds,
                          "LogicalChange schema id is empty");
            append_kind(out, change.schema.kind, bounds);
            append_u32_le(out, change.schema.schema_version, bounds);
            append_u32_le(out, change.opcode, bounds);
            append_u32_le(out, change.flags, bounds);
            append_payload(out, change.payload, bounds);
        }

        static void validate_message_size(
                const std::vector<std::uint8_t>& out,
                const CodecBounds* bounds) {
            if (out.size() > bounds->max_transport_message_bytes) {
                throw std::length_error(
                    "logical frame exceeds max_transport_message_bytes");
            }
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

        static const std::uint8_t* read_bytes(Cursor& cur, std::size_t n) {
            check_bounds(cur, n);
            if (n == 0) {
                return nullptr;
            }
            const std::uint8_t* out = cur.data + cur.pos;
            cur.pos += n;
            return out;
        }

        static std::string read_string(Cursor& cur,
                                       const CodecBounds* bounds,
                                       const char* label) {
            const std::uint32_t len = read_u32_le(cur);
            if (len == 0) {
                throw std::runtime_error(label);
            }
            if (len > bounds->max_logical_schema_id_len) {
                throw std::length_error(
                    "logical schema id exceeds max_logical_schema_id_len");
            }
            const std::uint8_t* bytes = read_bytes(cur, len);
            return std::string(reinterpret_cast<const char*>(bytes), len);
        }

        static LogicalTableKind read_kind(Cursor& cur) {
            const std::uint16_t value = read_u16_le(cur);
            switch (value) {
                case static_cast<std::uint16_t>(
                        LogicalTableKind::AnyValue):
                    return LogicalTableKind::AnyValue;
                case static_cast<std::uint16_t>(
                        LogicalTableKind::HashedKeyValue):
                    return LogicalTableKind::HashedKeyValue;
                case static_cast<std::uint16_t>(
                        LogicalTableKind::KeyValue):
                    return LogicalTableKind::KeyValue;
                case static_cast<std::uint16_t>(
                        LogicalTableKind::KeyTable):
                    return LogicalTableKind::KeyTable;
                case static_cast<std::uint16_t>(
                        LogicalTableKind::KeyMultiValue):
                    return LogicalTableKind::KeyMultiValue;
                case static_cast<std::uint16_t>(
                        LogicalTableKind::KeyOrderedMultiValue):
                    return LogicalTableKind::KeyOrderedMultiValue;
            }
            throw std::runtime_error("Invalid logical table kind");
        }

        static std::vector<std::uint8_t> read_payload(
                Cursor& cur,
                const CodecBounds* bounds) {
            const std::uint32_t len = read_u32_le(cur);
            if (len > bounds->max_value_len) {
                throw std::length_error(
                    "logical payload exceeds max_value_len");
            }
            const std::uint8_t* bytes = read_bytes(cur, len);
            std::vector<std::uint8_t> payload(len);
            if (len != 0u) {
                std::memcpy(&payload[0], bytes, len);
            }
            return payload;
        }

        static LogicalChange read_change(Cursor& cur,
                                         const CodecBounds* bounds) {
            LogicalChange change;
            change.schema.schema_id = read_string(
                cur, bounds, "LogicalChange schema id is empty");
            change.schema.kind = read_kind(cur);
            change.schema.schema_version = read_u32_le(cur);
            if (change.schema.schema_version == 0) {
                throw std::runtime_error(
                    "LogicalChange schema version is zero");
            }
            change.opcode = read_u32_le(cur);
            change.flags = read_u32_le(cur);
            if (change.flags != 0) {
                throw std::runtime_error(
                    "LogicalChange flags are reserved and must be zero");
            }
            change.payload = read_payload(cur, bounds);
            return change;
        }
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_CHANGE_FRAME_CODEC_HPP_INCLUDED
