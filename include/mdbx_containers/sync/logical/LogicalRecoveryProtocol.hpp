#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_RECOVERY_PROTOCOL_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_RECOVERY_PROTOCOL_HPP_INCLUDED

/// \file logical/LogicalRecoveryProtocol.hpp
/// \brief Strict wire codec for paged logical-aware recovery.

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "LogicalDeliveryEnvelopeCodec.hpp"
#include "logical_recovery.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Strict binary codec for logical recovery requests and responses.
    /// \details The final successful response carries the logical baseline;
    /// intermediate pages carry only one physical snapshot chunk. The codec
    /// rejects inconsistent response shapes before a peer can apply them.
    class LogicalRecoveryProtocolCodec {
    public:
        enum class MessageType : std::uint8_t {
            Request = 1u,
            Response = 2u
        };

        static const std::uint8_t* magic() {
            static const std::uint8_t value[8] =
                { 'M', 'D', 'B', 'X', 'C', 'L', 'R', 'P' };
            return value;
        }

        static std::size_t magic_size() { return 8u; }
        static std::uint16_t codec_version() { return 1u; }

        static std::vector<std::uint8_t> encode_request(
                const LogicalRecoveryRequest& request,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            validate_request(request, &effective);
            std::vector<std::uint8_t> out = make_header(MessageType::Request);
            append_id(out, request.requester);
            append_snapshot_token(out, request.snapshot_id, effective);
            append_snapshot_token(out, request.continuation, effective);
            append_u64(out, request.max_bytes);
            append_u64(out, request.max_single_batch_bytes);
            validate_size(out, effective);
            return out;
        }

        static LogicalRecoveryRequest decode_request(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            Cursor cur = make_cursor(encoded, effective);
            check_header(cur, MessageType::Request);
            LogicalRecoveryRequest out;
            read_id(cur, out.requester);
            out.snapshot_id = read_snapshot_token(cur, effective);
            out.continuation = read_snapshot_token(cur, effective);
            out.max_bytes = read_u64(cur);
            out.max_single_batch_bytes = read_u64(cur);
            check_consumed(cur);
            validate_request(out, &effective);
            return out;
        }

        static std::vector<std::uint8_t> encode_response(
                const LogicalRecoveryResponse& response,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            validate_response(response, &effective);
            std::vector<std::uint8_t> out = make_header(MessageType::Response);
            append_bool(out, response.ok);
            append_bool(out, response.has_more);
            append_bool(out, response.has_baseline);
            append_u16(out, static_cast<std::uint16_t>(response.error_code));
            append_bool(out, response.error_retryable);
            append_error(out, response.error, effective);
            if (response.ok) {
                append_snapshot_chunk(out, response.snapshot_chunk, effective);
                if (response.has_baseline) {
                    append_baseline(out, response.baseline, effective);
                }
            }
            validate_size(out, effective);
            return out;
        }

        static LogicalRecoveryResponse decode_response(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            Cursor cur = make_cursor(encoded, effective);
            check_header(cur, MessageType::Response);
            LogicalRecoveryResponse out;
            out.ok = read_bool(cur);
            out.has_more = read_bool(cur);
            out.has_baseline = read_bool(cur);
            out.error_code = read_error_code(cur);
            out.error_retryable = read_bool(cur);
            out.error = read_error(cur, effective);
            if (out.ok) {
                out.snapshot_chunk = read_snapshot_chunk(cur, effective);
                if (out.has_baseline) {
                    out.baseline = read_baseline(cur, effective);
                }
            }
            check_consumed(cur);
            validate_response(out, &effective);
            return out;
        }

        static MessageType peek_message_type(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            Cursor cur = make_cursor(encoded, effective);
            check_magic_and_version(cur);
            const std::uint8_t value = read_u8(cur);
            switch (value) {
                case static_cast<std::uint8_t>(MessageType::Request):
                    return MessageType::Request;
                case static_cast<std::uint8_t>(MessageType::Response):
                    return MessageType::Response;
                default:
                    throw std::runtime_error(
                        "Invalid logical recovery message type");
            }
        }

        static void validate_request(const LogicalRecoveryRequest& request,
                                     const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            if (is_zero_sync_id(request.requester)) {
                throw std::invalid_argument(
                    "Logical recovery requester identity is incomplete");
            }
            if (request.snapshot_id.size() > effective.max_snapshot_id_len ||
                request.continuation.size() > effective.max_snapshot_id_len) {
                throw std::length_error(
                    "logical recovery token exceeds max_snapshot_id_len");
            }
            if ((request.snapshot_id.empty()) != request.continuation.empty()) {
                throw std::invalid_argument(
                    "logical recovery snapshot id and continuation must both be empty or set");
            }
            if (request.max_bytes == 0u || request.max_single_batch_bytes == 0u) {
                throw std::invalid_argument(
                    "logical recovery byte limits must be nonzero");
            }
        }

        static void validate_response(const LogicalRecoveryResponse& response,
                                      const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            if (response.error.size() > effective.max_error_len) {
                throw std::length_error(
                    "logical recovery error exceeds max_error_len");
            }
            if (!response.ok) {
                if (response.has_more || response.has_baseline) {
                    throw std::invalid_argument(
                        "failed logical recovery response contains recovery state");
                }
                return;
            }
            if (!response.error.empty() ||
                response.error_code != SyncResponseErrorCode::None ||
                response.error_retryable) {
                throw std::invalid_argument(
                    "successful logical recovery response contains an error");
            }
            FullSnapshotCodec::validate(response.snapshot_chunk, &effective);
            if (response.has_more != response.snapshot_chunk.has_more) {
                throw std::invalid_argument(
                    "logical recovery continuation flags differ");
            }
            if (response.has_more == response.has_baseline) {
                throw std::invalid_argument(
                    "logical recovery baseline must appear exactly on the final page");
            }
            if (response.has_baseline) {
                validate_baseline(response.baseline, response.snapshot_chunk,
                                  effective);
            }
        }

    private:
        struct Cursor {
            const std::uint8_t* data;
            std::size_t size;
            std::size_t pos;
        };

        static const CodecBounds& effective_bounds(const CodecBounds* bounds) {
            static const CodecBounds defaults;
            return bounds != nullptr ? *bounds : defaults;
        }

        static std::vector<std::uint8_t> make_header(MessageType type) {
            std::vector<std::uint8_t> out;
            append_bytes(out, magic(), magic_size());
            append_u16(out, codec_version());
            append_u8(out, static_cast<std::uint8_t>(type));
            return out;
        }

        static Cursor make_cursor(const std::vector<std::uint8_t>& encoded,
                                  const CodecBounds& bounds) {
            if (encoded.size() > bounds.max_transport_message_bytes) {
                throw std::length_error(
                    "logical recovery message exceeds max_transport_message_bytes");
            }
            Cursor cur = { encoded.empty() ? nullptr : &encoded[0],
                           encoded.size(), 0u };
            return cur;
        }

        static void check_header(Cursor& cur, MessageType expected) {
            check_magic_and_version(cur);
            if (read_u8(cur) != static_cast<std::uint8_t>(expected)) {
                throw std::runtime_error("Unexpected logical recovery message type");
            }
        }

        static void check_magic_and_version(Cursor& cur) {
            require(cur, magic_size());
            if (std::memcmp(cur.data + cur.pos, magic(), magic_size()) != 0) {
                throw std::runtime_error("Invalid logical recovery magic");
            }
            cur.pos += magic_size();
            if (read_u16(cur) != codec_version()) {
                throw std::runtime_error("Unsupported logical recovery codec version");
            }
        }

        static void check_consumed(const Cursor& cur) {
            if (cur.pos != cur.size) {
                throw std::runtime_error("Trailing bytes after logical recovery message");
            }
        }

        static void require(const Cursor& cur, std::size_t size) {
            if (size > cur.size - cur.pos) {
                throw std::runtime_error("Truncated logical recovery message");
            }
        }

        static void append_bytes(std::vector<std::uint8_t>& out,
                                 const std::uint8_t* data,
                                 std::size_t size) {
            if (size == 0u) return;
            if (data == nullptr ||
                size > (std::numeric_limits<std::size_t>::max)() - out.size()) {
                throw std::length_error("logical recovery message size overflow");
            }
            const std::size_t old_size = out.size();
            out.resize(old_size + size);
            std::memcpy(&out[old_size], data, size);
        }

        static void append_u8(std::vector<std::uint8_t>& out, std::uint8_t value) {
            out.push_back(value);
        }

        static void append_bool(std::vector<std::uint8_t>& out, bool value) {
            append_u8(out, value ? 1u : 0u);
        }

        static void append_u16(std::vector<std::uint8_t>& out, std::uint16_t value) {
            out.push_back(static_cast<std::uint8_t>(value & 0xffu));
            out.push_back(static_cast<std::uint8_t>((value >> 8u) & 0xffu));
        }

        static void append_u32(std::vector<std::uint8_t>& out, std::uint32_t value) {
            for (std::size_t i = 0u; i < 4u; ++i) {
                out.push_back(static_cast<std::uint8_t>(value >> (8u * i)));
            }
        }

        static void append_u64(std::vector<std::uint8_t>& out, std::uint64_t value) {
            for (std::size_t i = 0u; i < 8u; ++i) {
                out.push_back(static_cast<std::uint8_t>(value >> (8u * i)));
            }
        }

        static std::uint8_t read_u8(Cursor& cur) {
            require(cur, 1u);
            return cur.data[cur.pos++];
        }

        static bool read_bool(Cursor& cur) {
            const std::uint8_t value = read_u8(cur);
            if (value > 1u) {
                throw std::runtime_error("Invalid logical recovery bool value");
            }
            return value != 0u;
        }

        static std::uint16_t read_u16(Cursor& cur) {
            require(cur, 2u);
            const std::uint16_t out = static_cast<std::uint16_t>(cur.data[cur.pos]) |
                static_cast<std::uint16_t>(cur.data[cur.pos + 1u] << 8u);
            cur.pos += 2u;
            return out;
        }

        static std::uint32_t read_u32(Cursor& cur) {
            require(cur, 4u);
            std::uint32_t out = 0u;
            for (std::size_t i = 0u; i < 4u; ++i) {
                out |= static_cast<std::uint32_t>(cur.data[cur.pos + i]) <<
                    (8u * i);
            }
            cur.pos += 4u;
            return out;
        }

        static std::uint64_t read_u64(Cursor& cur) {
            require(cur, 8u);
            std::uint64_t out = 0u;
            for (std::size_t i = 0u; i < 8u; ++i) {
                out |= static_cast<std::uint64_t>(cur.data[cur.pos + i]) <<
                    (8u * i);
            }
            cur.pos += 8u;
            return out;
        }

        static const std::uint8_t* read_bytes(Cursor& cur, std::size_t size) {
            require(cur, size);
            const std::uint8_t* out = size == 0u ? nullptr : cur.data + cur.pos;
            cur.pos += size;
            return out;
        }

        static void append_id(std::vector<std::uint8_t>& out, const NodeId& id) {
            append_bytes(out, id.data(), id.size());
        }

        static void read_id(Cursor& cur, NodeId& out) {
            const std::uint8_t* bytes = read_bytes(cur, out.size());
            std::memcpy(out.data(), bytes, out.size());
        }

        static void append_u32_size(std::vector<std::uint8_t>& out,
                                    std::size_t size,
                                    const char* error) {
            if (size > (std::numeric_limits<std::uint32_t>::max)()) {
                throw std::length_error(error);
            }
            append_u32(out, static_cast<std::uint32_t>(size));
        }

        static void append_string(std::vector<std::uint8_t>& out,
                                  const std::string& value,
                                  std::uint32_t max_size,
                                  const char* error) {
            if (value.size() > max_size) {
                throw std::length_error(error);
            }
            append_u32_size(out, value.size(), error);
            append_bytes(out,
                         value.empty() ? nullptr :
                             reinterpret_cast<const std::uint8_t*>(value.data()),
                         value.size());
        }

        static std::string read_string(Cursor& cur, std::uint32_t max_size,
                                       const char* error) {
            const std::uint32_t size = read_u32(cur);
            if (size > max_size) {
                throw std::length_error(error);
            }
            const std::uint8_t* bytes = read_bytes(cur, size);
            return size == 0u ? std::string() :
                std::string(reinterpret_cast<const char*>(bytes), size);
        }

        static void append_snapshot_token(std::vector<std::uint8_t>& out,
                                          const std::string& value,
                                          const CodecBounds& bounds) {
            append_string(out, value, bounds.max_snapshot_id_len,
                          "logical recovery token exceeds max_snapshot_id_len");
        }

        static std::string read_snapshot_token(Cursor& cur,
                                               const CodecBounds& bounds) {
            return read_string(cur, bounds.max_snapshot_id_len,
                               "logical recovery token exceeds max_snapshot_id_len");
        }

        static void append_error(std::vector<std::uint8_t>& out,
                                 const std::string& value,
                                 const CodecBounds& bounds) {
            append_string(out, value, bounds.max_error_len,
                          "logical recovery error exceeds max_error_len");
        }

        static std::string read_error(Cursor& cur, const CodecBounds& bounds) {
            return read_string(cur, bounds.max_error_len,
                               "logical recovery error exceeds max_error_len");
        }

        static SyncResponseErrorCode read_error_code(Cursor& cur) {
            const std::uint16_t value = read_u16(cur);
            switch (value) {
                case static_cast<std::uint16_t>(SyncResponseErrorCode::None):
                    return SyncResponseErrorCode::None;
                case static_cast<std::uint16_t>(SyncResponseErrorCode::DbIdMismatch):
                    return SyncResponseErrorCode::DbIdMismatch;
                case static_cast<std::uint16_t>(SyncResponseErrorCode::UnsupportedFullSnapshot):
                    return SyncResponseErrorCode::UnsupportedFullSnapshot;
                case static_cast<std::uint16_t>(SyncResponseErrorCode::ApplyConflict):
                    return SyncResponseErrorCode::ApplyConflict;
                case static_cast<std::uint16_t>(SyncResponseErrorCode::SnapshotRequired):
                    return SyncResponseErrorCode::SnapshotRequired;
                case static_cast<std::uint16_t>(SyncResponseErrorCode::BatchTooLarge):
                    return SyncResponseErrorCode::BatchTooLarge;
                case static_cast<std::uint16_t>(SyncResponseErrorCode::SnapshotNotConfigured):
                    return SyncResponseErrorCode::SnapshotNotConfigured;
                case static_cast<std::uint16_t>(SyncResponseErrorCode::SnapshotSessionInvalid):
                    return SyncResponseErrorCode::SnapshotSessionInvalid;
                case static_cast<std::uint16_t>(SyncResponseErrorCode::SnapshotSessionBusy):
                    return SyncResponseErrorCode::SnapshotSessionBusy;
                case static_cast<std::uint16_t>(SyncResponseErrorCode::SnapshotLogicalStateUnsupported):
                    return SyncResponseErrorCode::SnapshotLogicalStateUnsupported;
            }
            throw std::runtime_error("Invalid logical recovery error code");
        }

        static void append_snapshot_chunk(std::vector<std::uint8_t>& out,
                                          const FullSnapshotChunk& chunk,
                                          const CodecBounds& bounds) {
            const std::vector<std::uint8_t> nested =
                FullSnapshotCodec::encode(chunk, &bounds);
            if (nested.size() > bounds.max_snapshot_chunk_bytes) {
                throw std::length_error(
                    "logical recovery snapshot exceeds max_snapshot_chunk_bytes");
            }
            append_u32_size(out, nested.size(),
                            "logical recovery snapshot length exceeds u32");
            append_bytes(out, nested.empty() ? nullptr : &nested[0], nested.size());
        }

        static FullSnapshotChunk read_snapshot_chunk(Cursor& cur,
                                                     const CodecBounds& bounds) {
            const std::uint32_t size = read_u32(cur);
            if (size > bounds.max_snapshot_chunk_bytes) {
                throw std::length_error(
                    "logical recovery snapshot exceeds max_snapshot_chunk_bytes");
            }
            const std::uint8_t* bytes = read_bytes(cur, size);
            std::vector<std::uint8_t> nested(size);
            if (size != 0u) std::memcpy(&nested[0], bytes, size);
            return FullSnapshotCodec::decode(nested, &bounds);
        }

        static void append_baseline(std::vector<std::uint8_t>& out,
                                    const LogicalRecoveryBaseline& baseline,
                                    const CodecBounds& bounds) {
            append_id(out, baseline.source_node_id);
            append_id(out, baseline.source_db_uuid);
            append_snapshot_token(out, baseline.snapshot_id, bounds);
            append_u32_size(out, baseline.schemas.size(),
                            "logical recovery schema count exceeds u32");
            for (std::size_t i = 0u; i < baseline.schemas.size(); ++i) {
                append_schema(out, baseline.schemas[i], bounds);
            }
            append_u32_size(out, baseline.delivery_markers.size(),
                            "logical recovery marker count exceeds u32");
            for (std::size_t i = 0u; i < baseline.delivery_markers.size(); ++i) {
                append_marker(out, baseline.delivery_markers[i], bounds);
            }
            append_u32_size(out, baseline.delivery_watermarks.size(),
                            "logical recovery watermark count exceeds u32");
            for (std::size_t i = 0u; i < baseline.delivery_watermarks.size(); ++i) {
                append_id(out, baseline.delivery_watermarks[i].origin_node_id);
                append_u64(out, baseline.delivery_watermarks[i].sequence);
            }
            append_u32_size(out, baseline.delivery_order.size(),
                            "logical recovery order count exceeds u32");
            for (std::size_t i = 0u; i < baseline.delivery_order.size(); ++i) {
                append_id(out, baseline.delivery_order[i].origin_node_id);
                append_u64(out, baseline.delivery_order[i].acknowledged_through);
            }
            append_u32_size(out, baseline.source_outbox_pending.size(),
                            "logical recovery pending count exceeds u32");
            for (std::size_t i = 0u; i < baseline.source_outbox_pending.size(); ++i) {
                const std::vector<std::uint8_t> envelope =
                    LogicalDeliveryEnvelopeCodec::encode(
                        baseline.source_outbox_pending[i], &bounds);
                append_u32_size(out, envelope.size(),
                                "logical recovery envelope length exceeds u32");
                append_bytes(out, envelope.empty() ? nullptr : &envelope[0],
                             envelope.size());
            }
            append_u64(out, baseline.source_outbox_known_tail);
        }

        static LogicalRecoveryBaseline read_baseline(Cursor& cur,
                                                      const CodecBounds& bounds) {
            LogicalRecoveryBaseline out;
            read_id(cur, out.source_node_id);
            read_id(cur, out.source_db_uuid);
            out.snapshot_id = read_snapshot_token(cur, bounds);
            out.schemas = read_schemas(cur, bounds);
            out.delivery_markers = read_markers(cur, bounds);
            const std::uint32_t watermark_count = read_collection_count(cur, bounds,
                "logical recovery watermark count exceeds max_snapshot_manifest_entries");
            out.delivery_watermarks.reserve(watermark_count);
            for (std::uint32_t i = 0u; i < watermark_count; ++i) {
                LogicalDeliveryWatermarkInfo item;
                read_id(cur, item.origin_node_id);
                item.sequence = read_u64(cur);
                out.delivery_watermarks.push_back(item);
            }
            const std::uint32_t order_count = read_collection_count(cur, bounds,
                "logical recovery order count exceeds max_snapshot_manifest_entries");
            out.delivery_order.reserve(order_count);
            for (std::uint32_t i = 0u; i < order_count; ++i) {
                LogicalDeliveryOrderEntry item;
                read_id(cur, item.origin_node_id);
                item.acknowledged_through = read_u64(cur);
                out.delivery_order.push_back(item);
            }
            const std::uint32_t pending_count = read_collection_count(cur, bounds,
                "logical recovery pending count exceeds max_snapshot_manifest_entries");
            out.source_outbox_pending.reserve(pending_count);
            for (std::uint32_t i = 0u; i < pending_count; ++i) {
                const std::uint32_t size = read_u32(cur);
                if (size > bounds.max_batch_total_bytes) {
                    throw std::length_error(
                        "logical recovery envelope exceeds max_batch_total_bytes");
                }
                const std::uint8_t* bytes = read_bytes(cur, size);
                std::vector<std::uint8_t> nested(size);
                if (size != 0u) std::memcpy(&nested[0], bytes, size);
                out.source_outbox_pending.push_back(
                    LogicalDeliveryEnvelopeCodec::decode(nested, &bounds));
            }
            out.source_outbox_known_tail = read_u64(cur);
            return out;
        }

        static void append_schema(std::vector<std::uint8_t>& out,
                                  const LogicalSchemaRegistryEntry& entry,
                                  const CodecBounds& bounds) {
            append_string(out, entry.schema_id, bounds.max_logical_schema_id_len,
                          "logical recovery schema id exceeds max_logical_schema_id_len");
            append_string(out, entry.record.dbi_name, bounds.max_dbi_name_len,
                          "logical recovery schema DBI exceeds max_dbi_name_len");
            if (!is_known_logical_table_kind(entry.record.kind)) {
                throw std::invalid_argument("logical recovery schema kind is unknown");
            }
            append_u16(out, static_cast<std::uint16_t>(entry.record.kind));
            append_u32(out, entry.record.schema_version);
            append_u32(out, entry.record.flags);
            append_u32_size(out, entry.record.dbi_names.size(),
                            "logical recovery schema DBI count exceeds u32");
            for (std::size_t i = 0u; i < entry.record.dbi_names.size(); ++i) {
                append_string(out, entry.record.dbi_names[i], bounds.max_dbi_name_len,
                              "logical recovery schema DBI exceeds max_dbi_name_len");
            }
            append_id(out, entry.record.ordered_delivery_origin_node_id);
        }

        static std::vector<LogicalSchemaRegistryEntry> read_schemas(
                Cursor& cur, const CodecBounds& bounds) {
            const std::uint32_t count = read_collection_count(cur, bounds,
                "logical recovery schema count exceeds max_snapshot_manifest_entries");
            std::vector<LogicalSchemaRegistryEntry> out;
            out.reserve(count);
            for (std::uint32_t i = 0u; i < count; ++i) {
                LogicalSchemaRegistryEntry entry;
                entry.schema_id = read_string(cur, bounds.max_logical_schema_id_len,
                    "logical recovery schema id exceeds max_logical_schema_id_len");
                entry.record.dbi_name = read_string(cur, bounds.max_dbi_name_len,
                    "logical recovery schema DBI exceeds max_dbi_name_len");
                const std::uint16_t kind = read_u16(cur);
                entry.record.kind = static_cast<LogicalTableKind>(kind);
                if (!is_known_logical_table_kind(entry.record.kind)) {
                    throw std::runtime_error("logical recovery schema kind is unknown");
                }
                entry.record.schema_version = read_u32(cur);
                entry.record.flags = read_u32(cur);
                const std::uint32_t dbi_count = read_collection_count(cur, bounds,
                    "logical recovery schema DBI count exceeds max_snapshot_manifest_entries");
                entry.record.dbi_names.reserve(dbi_count);
                for (std::uint32_t j = 0u; j < dbi_count; ++j) {
                    entry.record.dbi_names.push_back(read_string(
                        cur, bounds.max_dbi_name_len,
                        "logical recovery schema DBI exceeds max_dbi_name_len"));
                }
                read_id(cur, entry.record.ordered_delivery_origin_node_id);
                out.push_back(entry);
            }
            return out;
        }

        static void append_marker(std::vector<std::uint8_t>& out,
                                  const LogicalDeliveryMarkerInfo& marker,
                                  const CodecBounds& bounds) {
            if (marker.encoded_frame.size() != marker.frame_bytes_size) {
                throw std::invalid_argument(
                    "logical recovery marker frame size is inconsistent");
            }
            if (marker.encoded_frame.size() > bounds.max_batch_total_bytes) {
                throw std::length_error(
                    "logical recovery marker frame exceeds max_batch_total_bytes");
            }
            append_id(out, marker.destination_db_uuid);
            append_id(out, marker.origin_node_id);
            append_u64(out, marker.origin_sequence);
            append_string(out, marker.frame_id,
                          bounds.max_logical_delivery_frame_id_len,
                          "logical recovery marker frame id exceeds max_logical_delivery_frame_id_len");
            append_u16(out, marker.frame_codec_version);
            append_u32(out, marker.frame_bytes_size);
            append_u32_size(out, marker.encoded_frame.size(),
                            "logical recovery marker frame length exceeds u32");
            append_bytes(out, marker.encoded_frame.empty() ? nullptr :
                         &marker.encoded_frame[0], marker.encoded_frame.size());
        }

        static std::vector<LogicalDeliveryMarkerInfo> read_markers(
                Cursor& cur, const CodecBounds& bounds) {
            const std::uint32_t count = read_collection_count(cur, bounds,
                "logical recovery marker count exceeds max_snapshot_manifest_entries");
            std::vector<LogicalDeliveryMarkerInfo> out;
            out.reserve(count);
            for (std::uint32_t i = 0u; i < count; ++i) {
                LogicalDeliveryMarkerInfo marker;
                read_id(cur, marker.destination_db_uuid);
                read_id(cur, marker.origin_node_id);
                marker.origin_sequence = read_u64(cur);
                marker.frame_id = read_string(cur,
                    bounds.max_logical_delivery_frame_id_len,
                    "logical recovery marker frame id exceeds max_logical_delivery_frame_id_len");
                marker.frame_codec_version = read_u16(cur);
                marker.frame_bytes_size = read_u32(cur);
                const std::uint32_t size = read_u32(cur);
                if (size != marker.frame_bytes_size) {
                    throw std::runtime_error(
                        "logical recovery marker frame size is inconsistent");
                }
                if (size > bounds.max_batch_total_bytes) {
                    throw std::length_error(
                        "logical recovery marker frame exceeds max_batch_total_bytes");
                }
                const std::uint8_t* bytes = read_bytes(cur, size);
                marker.encoded_frame.resize(size);
                if (size != 0u) std::memcpy(&marker.encoded_frame[0], bytes, size);
                out.push_back(marker);
            }
            return out;
        }

        static std::uint32_t read_collection_count(Cursor& cur,
                                                   const CodecBounds& bounds,
                                                   const char* error) {
            const std::uint32_t count = read_u32(cur);
            if (count > bounds.max_snapshot_manifest_entries) {
                throw std::length_error(error);
            }
            return count;
        }

        static void validate_baseline(const LogicalRecoveryBaseline& baseline,
                                      const FullSnapshotChunk& chunk,
                                      const CodecBounds& bounds) {
            if (is_zero_sync_id(baseline.source_node_id) ||
                is_zero_sync_id(baseline.source_db_uuid) ||
                baseline.snapshot_id.empty() ||
                compare_node_id(baseline.source_node_id, chunk.source_node_id) != 0 ||
                compare_node_id(baseline.source_db_uuid, chunk.source_db_uuid) != 0 ||
                baseline.snapshot_id != chunk.snapshot_id) {
                throw std::invalid_argument(
                    "logical recovery baseline does not match snapshot identity");
            }
            if (baseline.schemas.size() > bounds.max_snapshot_manifest_entries ||
                baseline.delivery_markers.size() > bounds.max_snapshot_manifest_entries ||
                baseline.delivery_watermarks.size() > bounds.max_snapshot_manifest_entries ||
                baseline.delivery_order.size() > bounds.max_snapshot_manifest_entries ||
                baseline.source_outbox_pending.size() > bounds.max_snapshot_manifest_entries) {
                throw std::length_error(
                    "logical recovery baseline collection exceeds max_snapshot_manifest_entries");
            }
            for (std::size_t i = 0u; i < baseline.schemas.size(); ++i) {
                const LogicalSchemaRegistryEntry& entry = baseline.schemas[i];
                if (entry.schema_id.empty() || entry.record.dbi_name.empty() ||
                    !is_known_logical_table_kind(entry.record.kind)) {
                    throw std::invalid_argument(
                        "logical recovery baseline schema is incomplete");
                }
            }
            for (std::size_t i = 0u; i < baseline.delivery_markers.size(); ++i) {
                const LogicalDeliveryMarkerInfo& marker = baseline.delivery_markers[i];
                if (is_zero_sync_id(marker.destination_db_uuid) ||
                    is_zero_sync_id(marker.origin_node_id) ||
                    marker.origin_sequence == 0u || marker.frame_id.empty() ||
                    marker.encoded_frame.size() != marker.frame_bytes_size) {
                    throw std::invalid_argument(
                        "logical recovery baseline marker is incomplete");
                }
            }
            for (std::size_t i = 0u; i < baseline.source_outbox_pending.size(); ++i) {
                validate_logical_delivery_envelope(
                    baseline.source_outbox_pending[i], &bounds);
            }
        }

        static void validate_size(const std::vector<std::uint8_t>& out,
                                  const CodecBounds& bounds) {
            if (out.size() > bounds.max_transport_message_bytes) {
                throw std::length_error(
                    "logical recovery message exceeds max_transport_message_bytes");
            }
        }
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_RECOVERY_PROTOCOL_HPP_INCLUDED
