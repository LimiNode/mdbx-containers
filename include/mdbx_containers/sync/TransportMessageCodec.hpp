#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_MESSAGE_CODEC_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_MESSAGE_CODEC_HPP_INCLUDED

/// \file TransportMessageCodec.hpp
/// \brief Stable little-endian binary codec for sync transport DTOs.
/// \details
/// Envelope layout for all messages:
/// \code
///   magic             "MDBXCPRT"   8 bytes
///   codec_version     u16 le       = 5
///   message_type      u8           1=pull request, 2=pull response,
///                                  3=push request, 4=push response
///   message_flags     u32 le       = 0 in v0.1
///   payload           type-specific fields
/// \endcode
///
/// \c CancellationToken values are local call-control state and are never
/// serialized. Decoded request DTOs always contain default non-cancellable
/// tokens. \c ChangeBatch payloads are length-prefixed byte strings encoded by
/// \c ChangeBatchCodec. Passing a null \c CodecBounds pointer uses the
/// default bounds from \c CodecBounds; callers may pass stricter bounds for a
/// specific transport.

#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "ChangeBatchCodec.hpp"
#include "CodecBounds.hpp"
#include "common.hpp"
#include "protocol.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Type tag stored in the transport message envelope.
    enum class TransportMessageType : std::uint8_t {
        PullRequest  = 1,
        PullResponse = 2,
        PushRequest  = 3,
        PushResponse = 4,
    };

    /// \brief Stable binary codec for \c PullRequest, \c PullResponse,
    /// \c PushRequest, and \c PushResponse.
    class TransportMessageCodec {
    public:
        /// \brief Encoded magic prefix (8 bytes, no NUL terminator).
        static const std::uint8_t* magic() {
            static const std::uint8_t m[8] =
                { 'M','D','B','X','C','P','R','T' };
            return m;
        }

        /// \brief Magic prefix length in bytes.
        static std::size_t magic_size() { return 8; }

        /// \brief Supported transport codec version.
        static std::uint16_t codec_version() { return 5; }

        /// \brief Reads the message type from a transport envelope.
        /// \details Validates magic, codec version, and mandatory flags but
        /// does not decode or validate the type-specific payload.
        static TransportMessageType peek_message_type(
                const std::vector<std::uint8_t>& data,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            Cursor cur = make_cursor(data, bounds);
            return read_header_type(cur);
        }

        /// \brief Encodes a pull request.
        static std::vector<std::uint8_t> encode_pull_request(
                const PullRequest& request,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            std::vector<std::uint8_t> out =
                make_header(TransportMessageType::PullRequest);
            append_node(out, request.requester);
            append_node(out, request.db_id);
            append_cursor(out, request.have, bounds);
            detail::append_u64_le(out, request.max_batches);
            detail::append_u64_le(out, request.max_bytes);
            append_bool(out, request.request_full_snapshot);
            validate_pull_request_snapshot(request, bounds);
            append_snapshot_token(out, request.full_snapshot_id, bounds);
            append_snapshot_token(out, request.full_snapshot_continuation, bounds);
            detail::append_u64_le(out, request.max_single_batch_bytes);
            validate_message_size(out, bounds);
            return out;
        }

        /// \brief Encodes a pull response.
        static std::vector<std::uint8_t> encode_pull_response(
                const PullResponse& response,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            std::vector<std::uint8_t> out =
                make_header(TransportMessageType::PullResponse);
            append_cursor(out, response.remote_have, bounds);
            append_cursor(out, response.remote_tail, bounds);
            append_bool(out, response.remote_tail_known);
            append_batches(out, response.batches, bounds);
            append_bool(out, response.has_more);
            validate_pull_response_snapshot(response, bounds);
            append_bool(out, response.is_full_snapshot);
            if (response.is_full_snapshot) {
                append_snapshot_chunk(out, response.snapshot_chunk, bounds);
            }
            append_bool(out, response.ok);
            append_string(out, response.error, bounds);
            append_response_error_code(out, response.error_code);
            append_bool(out, response.error_retryable);
            validate_message_size(out, bounds);
            return out;
        }

        /// \brief Encodes a push request.
        static std::vector<std::uint8_t> encode_push_request(
                const PushRequest& request,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            std::vector<std::uint8_t> out =
                make_header(TransportMessageType::PushRequest);
            append_node(out, request.sender);
            append_node(out, request.db_id);
            append_batches(out, request.batches, bounds);
            validate_message_size(out, bounds);
            return out;
        }

        /// \brief Encodes a push response.
        static std::vector<std::uint8_t> encode_push_response(
                const PushResponse& response,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            std::vector<std::uint8_t> out =
                make_header(TransportMessageType::PushResponse);
            append_cursor(out, response.receiver_have, bounds);
            append_bool(out, response.ok);
            append_string(out, response.error, bounds);
            append_response_error_code(out, response.error_code);
            append_bool(out, response.error_retryable);
            validate_message_size(out, bounds);
            return out;
        }

        /// \brief Strictly decodes a pull request.
        static PullRequest decode_pull_request(
                const std::vector<std::uint8_t>& data,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            Cursor cur = make_cursor(data, bounds);
            check_header(cur, TransportMessageType::PullRequest);
            PullRequest request;
            read_node(cur, request.requester);
            read_node(cur, request.db_id);
            request.have = read_cursor(cur, bounds);
            request.max_batches = read_u64_le(cur);
            request.max_bytes = read_u64_le(cur);
            request.request_full_snapshot = read_bool(cur);
            request.full_snapshot_id = read_snapshot_token(cur, bounds);
            request.full_snapshot_continuation = read_snapshot_token(cur, bounds);
            request.max_single_batch_bytes = read_u64_le(cur);
            validate_pull_request_snapshot(request, bounds);
            check_consumed(cur);
            return request;
        }

        /// \brief Strictly decodes a pull response.
        static PullResponse decode_pull_response(
                const std::vector<std::uint8_t>& data,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            Cursor cur = make_cursor(data, bounds);
            check_header(cur, TransportMessageType::PullResponse);
            PullResponse response;
            response.remote_have = read_cursor(cur, bounds);
            response.remote_tail = read_cursor(cur, bounds);
            response.remote_tail_known = read_bool(cur);
            response.batches = read_batches(cur, bounds);
            response.has_more = read_bool(cur);
            response.is_full_snapshot = read_bool(cur);
            if (response.is_full_snapshot) {
                response.snapshot_chunk = read_snapshot_chunk(cur, bounds);
            }
            response.ok = read_bool(cur);
            response.error = read_string(cur, bounds);
            response.error_code = read_response_error_code(cur);
            response.error_retryable = read_bool(cur);
            check_consumed(cur);
            validate_pull_response_snapshot(response, bounds);
            return response;
        }

        /// \brief Strictly decodes a push request.
        static PushRequest decode_push_request(
                const std::vector<std::uint8_t>& data,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            Cursor cur = make_cursor(data, bounds);
            check_header(cur, TransportMessageType::PushRequest);
            PushRequest request;
            read_node(cur, request.sender);
            read_node(cur, request.db_id);
            request.batches = read_batches(cur, bounds);
            check_consumed(cur);
            return request;
        }

        /// \brief Strictly decodes a push response.
        static PushResponse decode_push_response(
                const std::vector<std::uint8_t>& data,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            Cursor cur = make_cursor(data, bounds);
            check_header(cur, TransportMessageType::PushResponse);
            PushResponse response;
            response.receiver_have = read_cursor(cur, bounds);
            response.ok = read_bool(cur);
            response.error = read_string(cur, bounds);
            response.error_code = read_response_error_code(cur);
            response.error_retryable = read_bool(cur);
            check_consumed(cur);
            return response;
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

        static std::vector<std::uint8_t> make_header(
                TransportMessageType type) {
            std::vector<std::uint8_t> out;
            out.reserve(64);
            append_bytes(out, magic(), magic_size());
            detail::append_u16_le(out, codec_version());
            append_u8(out, static_cast<std::uint8_t>(type));
            detail::append_u32_le(out, 0);
            return out;
        }

        static Cursor make_cursor(const std::vector<std::uint8_t>& data,
                                  const CodecBounds* bounds) {
            if (bounds != nullptr &&
                data.size() > bounds->max_transport_message_bytes) {
                throw std::length_error(
                    "transport message exceeds max_transport_message_bytes");
            }
            Cursor cur;
            cur.data = data.empty() ? nullptr : &data[0];
            cur.size = data.size();
            cur.pos = 0;
            return cur;
        }

        static TransportMessageType read_header_type(Cursor& cur) {
            check_bounds(cur, magic_size());
            if (std::memcmp(cur.data + cur.pos, magic(), magic_size()) != 0) {
                throw std::runtime_error("Transport codec magic mismatch");
            }
            cur.pos += magic_size();

            const std::uint16_t version = read_u16_le(cur);
            if (version != codec_version()) {
                throw std::runtime_error("Unsupported transport codec_version");
            }
            const std::uint8_t type = read_u8(cur);
            const std::uint32_t flags = read_u32_le(cur);
            if (flags != 0) {
                throw std::runtime_error(
                    "Unknown mandatory transport message flags");
            }
            switch (type) {
                case static_cast<std::uint8_t>(
                        TransportMessageType::PullRequest):
                    return TransportMessageType::PullRequest;
                case static_cast<std::uint8_t>(
                        TransportMessageType::PullResponse):
                    return TransportMessageType::PullResponse;
                case static_cast<std::uint8_t>(
                        TransportMessageType::PushRequest):
                    return TransportMessageType::PushRequest;
                case static_cast<std::uint8_t>(
                        TransportMessageType::PushResponse):
                    return TransportMessageType::PushResponse;
                default:
                    throw std::runtime_error(
                        "Unexpected transport message type");
            }
        }

        static void check_header(Cursor& cur, TransportMessageType expected) {
            const TransportMessageType type = read_header_type(cur);
            if (type != expected) {
                throw std::runtime_error("Unexpected transport message type");
            }
        }

        static void check_consumed(const Cursor& cur) {
            if (cur.pos != cur.size) {
                throw std::runtime_error("Trailing bytes after transport message");
            }
        }

        static void check_bounds(const Cursor& cur, std::size_t n) {
            if (cur.pos > cur.size || n > cur.size - cur.pos) {
                throw std::runtime_error("Transport codec buffer underrun");
            }
        }

        static void append_u8(std::vector<std::uint8_t>& out,
                              std::uint8_t value) {
            out.push_back(value);
        }

        static void append_bool(std::vector<std::uint8_t>& out, bool value) {
            append_u8(out, value ? 1u : 0u);
        }

        static void append_response_error_code(
                std::vector<std::uint8_t>& out,
                SyncResponseErrorCode code) {
            switch (code) {
                case SyncResponseErrorCode::None:
                case SyncResponseErrorCode::DbIdMismatch:
                case SyncResponseErrorCode::UnsupportedFullSnapshot:
                case SyncResponseErrorCode::ApplyConflict:
                case SyncResponseErrorCode::SnapshotRequired:
                case SyncResponseErrorCode::BatchTooLarge:
                case SyncResponseErrorCode::SnapshotNotConfigured:
                case SyncResponseErrorCode::SnapshotSessionInvalid:
                case SyncResponseErrorCode::SnapshotSessionBusy:
                case SyncResponseErrorCode::SnapshotLogicalStateUnsupported:
                    detail::append_u16_le(out,
                        static_cast<std::uint16_t>(code));
                    return;
            }
            throw std::logic_error("unknown SyncResponseErrorCode");
        }

        static void append_bytes(std::vector<std::uint8_t>& out,
                                 const std::uint8_t* src,
                                 std::size_t n) {
            if (n == 0) {
                return;
            }
            const std::size_t base = out.size();
            if (n > std::numeric_limits<std::size_t>::max() - base) {
                throw std::length_error("transport message size overflow");
            }
            out.resize(base + n);
            std::memcpy(&out[base], src, n);
        }

        static void append_node(std::vector<std::uint8_t>& out,
                                const NodeId& node) {
            append_bytes(out, node.data(), node.size());
        }

        static void append_u32_size(std::vector<std::uint8_t>& out,
                                    std::size_t value,
                                    const char* label) {
            if (value > static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max())) {
                throw std::length_error(label);
            }
            detail::append_u32_le(out, static_cast<std::uint32_t>(value));
        }

        static void append_cursor(std::vector<std::uint8_t>& out,
                                  const SyncCursor& cursor,
                                  const CodecBounds* bounds) {
            if (bounds != nullptr &&
                cursor.last_seq_by_origin.size() > bounds->max_cursor_origins) {
                throw std::length_error(
                    "cursor origins exceed max_cursor_origins");
            }
            append_u32_size(out, cursor.last_seq_by_origin.size(),
                            "cursor origins exceed u32");
            std::map<NodeId, std::uint64_t>::const_iterator it =
                cursor.last_seq_by_origin.begin();
            for (; it != cursor.last_seq_by_origin.end(); ++it) {
                append_node(out, it->first);
                detail::append_u64_le(out, it->second);
            }
        }

        static void append_string(std::vector<std::uint8_t>& out,
                                  const std::string& value,
                                  const CodecBounds* bounds) {
            if (bounds != nullptr && value.size() > bounds->max_error_len) {
                throw std::length_error("error string exceeds max_error_len");
            }
            append_u32_size(out, value.size(), "string length exceeds u32");
            if (!value.empty()) {
                append_bytes(out,
                             reinterpret_cast<const std::uint8_t*>(value.data()),
                             value.size());
            }
        }

        static void append_snapshot_token(std::vector<std::uint8_t>& out,
                                          const std::string& value,
                                          const CodecBounds* bounds) {
            if (value.size() > bounds->max_snapshot_id_len) {
                throw std::length_error(
                    "snapshot token exceeds max_snapshot_id_len");
            }
            append_u32_size(out, value.size(),
                            "snapshot token length exceeds u32");
            if (!value.empty()) {
                append_bytes(out,
                             reinterpret_cast<const std::uint8_t*>(value.data()),
                             value.size());
            }
        }

        static void append_snapshot_chunk(
                std::vector<std::uint8_t>& out,
                const FullSnapshotChunk& chunk,
                const CodecBounds* bounds) {
            const std::vector<std::uint8_t> encoded =
                FullSnapshotCodec::encode(chunk, bounds);
            append_u32_size(out, encoded.size(),
                            "full snapshot chunk length exceeds u32");
            append_bytes(out, encoded.empty() ? nullptr : &encoded[0],
                         encoded.size());
        }

        static void append_batches(std::vector<std::uint8_t>& out,
                                   const std::vector<ChangeBatch>& batches,
                                   const CodecBounds* bounds) {
            if (bounds != nullptr &&
                batches.size() > bounds->max_batches_per_message) {
                throw std::length_error(
                    "batches exceed max_batches_per_message");
            }
            append_u32_size(out, batches.size(), "batches count exceeds u32");
            for (std::size_t i = 0; i < batches.size(); ++i) {
                const std::vector<std::uint8_t> encoded =
                    ChangeBatchCodec::encode(batches[i], bounds);
                if (bounds != nullptr &&
                    encoded.size() > bounds->max_batch_total_bytes) {
                    throw std::length_error(
                        "batch bytes exceed max_batch_total_bytes");
                }
                append_u32_size(out, encoded.size(),
                                "batch bytes length exceeds u32");
                append_bytes(out, encoded.empty() ? nullptr : &encoded[0],
                             encoded.size());
            }
        }

        static void validate_message_size(
                const std::vector<std::uint8_t>& out,
                const CodecBounds* bounds) {
            if (bounds != nullptr &&
                out.size() > bounds->max_transport_message_bytes) {
                throw std::length_error(
                    "transport message exceeds max_transport_message_bytes");
            }
        }

        static std::uint8_t read_u8(Cursor& cur) {
            check_bounds(cur, 1);
            const std::uint8_t value = cur.data[cur.pos];
            cur.pos += 1;
            return value;
        }

        static bool read_bool(Cursor& cur) {
            const std::uint8_t value = read_u8(cur);
            if (value > 1u) {
                throw std::runtime_error("Invalid transport bool value");
            }
            return value != 0;
        }

        static std::uint16_t read_u16_le(Cursor& cur) {
            check_bounds(cur, 2);
            const std::uint16_t value = detail::read_u16_le(cur.data + cur.pos);
            cur.pos += 2;
            return value;
        }

        static SyncResponseErrorCode read_response_error_code(Cursor& cur) {
            const std::uint16_t value = read_u16_le(cur);
            switch (value) {
                case static_cast<std::uint16_t>(SyncResponseErrorCode::None):
                    return SyncResponseErrorCode::None;
                case static_cast<std::uint16_t>(
                        SyncResponseErrorCode::DbIdMismatch):
                    return SyncResponseErrorCode::DbIdMismatch;
                case static_cast<std::uint16_t>(
                        SyncResponseErrorCode::UnsupportedFullSnapshot):
                    return SyncResponseErrorCode::UnsupportedFullSnapshot;
                case static_cast<std::uint16_t>(
                        SyncResponseErrorCode::ApplyConflict):
                    return SyncResponseErrorCode::ApplyConflict;
                case static_cast<std::uint16_t>(
                        SyncResponseErrorCode::SnapshotRequired):
                    return SyncResponseErrorCode::SnapshotRequired;
                case static_cast<std::uint16_t>(
                        SyncResponseErrorCode::BatchTooLarge):
                    return SyncResponseErrorCode::BatchTooLarge;
                case static_cast<std::uint16_t>(
                        SyncResponseErrorCode::SnapshotNotConfigured):
                    return SyncResponseErrorCode::SnapshotNotConfigured;
                case static_cast<std::uint16_t>(
                        SyncResponseErrorCode::SnapshotSessionInvalid):
                    return SyncResponseErrorCode::SnapshotSessionInvalid;
                case static_cast<std::uint16_t>(
                        SyncResponseErrorCode::SnapshotSessionBusy):
                    return SyncResponseErrorCode::SnapshotSessionBusy;
                case static_cast<std::uint16_t>(
                        SyncResponseErrorCode::SnapshotLogicalStateUnsupported):
                    return SyncResponseErrorCode::SnapshotLogicalStateUnsupported;
            }
            throw std::runtime_error("Invalid SyncResponseErrorCode");
        }

        static std::uint32_t read_u32_le(Cursor& cur) {
            check_bounds(cur, 4);
            const std::uint32_t value = detail::read_u32_le(cur.data + cur.pos);
            cur.pos += 4;
            return value;
        }

        static std::uint64_t read_u64_le(Cursor& cur) {
            check_bounds(cur, 8);
            const std::uint64_t value = detail::read_u64_le(cur.data + cur.pos);
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

        static void read_node(Cursor& cur, NodeId& node) {
            const std::uint8_t* bytes = read_bytes(cur, node.size());
            std::memcpy(node.data(), bytes, node.size());
        }

        static SyncCursor read_cursor(Cursor& cur,
                                      const CodecBounds* bounds) {
            const std::uint32_t count = read_u32_le(cur);
            if (bounds != nullptr && count > bounds->max_cursor_origins) {
                throw std::length_error(
                    "cursor origins exceed max_cursor_origins");
            }
            SyncCursor cursor;
            for (std::uint32_t i = 0; i < count; ++i) {
                NodeId origin{};
                read_node(cur, origin);
                const std::uint64_t seq = read_u64_le(cur);
                if (cursor.last_seq_by_origin.find(origin) !=
                    cursor.last_seq_by_origin.end()) {
                    throw std::runtime_error(
                        "Duplicate origin in transport cursor");
                }
                cursor.last_seq_by_origin[origin] = seq;
            }
            return cursor;
        }

        static std::string read_string(Cursor& cur,
                                       const CodecBounds* bounds) {
            const std::uint32_t len = read_u32_le(cur);
            if (bounds != nullptr && len > bounds->max_error_len) {
                throw std::length_error("error string exceeds max_error_len");
            }
            if (len == 0) {
                return std::string();
            }
            const std::uint8_t* bytes = read_bytes(cur, len);
            return std::string(reinterpret_cast<const char*>(bytes), len);
        }

        static std::string read_snapshot_token(Cursor& cur,
                                               const CodecBounds* bounds) {
            const std::uint32_t len = read_u32_le(cur);
            if (len > bounds->max_snapshot_id_len) {
                throw std::length_error(
                    "snapshot token exceeds max_snapshot_id_len");
            }
            if (len == 0u) {
                return std::string();
            }
            const std::uint8_t* bytes = read_bytes(cur, len);
            return std::string(reinterpret_cast<const char*>(bytes), len);
        }

        static FullSnapshotChunk read_snapshot_chunk(
                Cursor& cur,
                const CodecBounds* bounds) {
            const std::uint32_t len = read_u32_le(cur);
            if (len > bounds->max_snapshot_chunk_bytes) {
                throw std::length_error(
                    "full snapshot chunk exceeds max_snapshot_chunk_bytes");
            }
            const std::uint8_t* bytes = read_bytes(cur, len);
            std::vector<std::uint8_t> encoded(len);
            if (len != 0u) {
                std::memcpy(&encoded[0], bytes, len);
            }
            return FullSnapshotCodec::decode(encoded, bounds);
        }

        static void validate_pull_request_snapshot(
                const PullRequest& request,
                const CodecBounds* bounds) {
            if (request.full_snapshot_id.size() > bounds->max_snapshot_id_len ||
                request.full_snapshot_continuation.size() >
                    bounds->max_snapshot_id_len) {
                throw std::length_error(
                    "snapshot token exceeds max_snapshot_id_len");
            }
            if (!request.request_full_snapshot &&
                (!request.full_snapshot_id.empty() ||
                 !request.full_snapshot_continuation.empty())) {
                throw std::invalid_argument(
                    "incremental pull contains full snapshot session state");
            }
            if (request.request_full_snapshot &&
                (request.full_snapshot_id.empty() !=
                 request.full_snapshot_continuation.empty())) {
                throw std::invalid_argument(
                    "full snapshot session id and continuation must both be empty or set");
            }
        }

        static void validate_pull_response_snapshot(
                const PullResponse& response,
                const CodecBounds* bounds) {
            if (!response.is_full_snapshot) {
                return;
            }
            if (!response.batches.empty()) {
                throw std::invalid_argument(
                    "full snapshot response also contains incremental batches");
            }
            if (!response.ok) {
                throw std::invalid_argument(
                    "failed pull response contains a full snapshot chunk");
            }
            FullSnapshotCodec::validate(response.snapshot_chunk, bounds);
            if (response.has_more != response.snapshot_chunk.has_more) {
                throw std::invalid_argument(
                    "pull and full snapshot continuation flags differ");
            }
        }

        static std::vector<ChangeBatch> read_batches(
                Cursor& cur,
                const CodecBounds* bounds) {
            const std::uint32_t count = read_u32_le(cur);
            if (bounds != nullptr && count > bounds->max_batches_per_message) {
                throw std::length_error(
                    "batches exceed max_batches_per_message");
            }
            std::vector<ChangeBatch> batches;
            batches.reserve(count);
            for (std::uint32_t i = 0; i < count; ++i) {
                const std::uint32_t len = read_u32_le(cur);
                if (bounds != nullptr && len > bounds->max_batch_total_bytes) {
                    throw std::length_error(
                        "batch bytes exceed max_batch_total_bytes");
                }
                const std::uint8_t* bytes = read_bytes(cur, len);
                std::vector<std::uint8_t> encoded(len);
                if (len > 0) {
                    std::memcpy(&encoded[0], bytes, len);
                }
                batches.push_back(ChangeBatchCodec::decode_exact(encoded, bounds));
            }
            return batches;
        }
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_MESSAGE_CODEC_HPP_INCLUDED
