#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_SELECTIVE_REPLICATION_PROTOCOL_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_SELECTIVE_REPLICATION_PROTOCOL_HPP_INCLUDED

/// \file protocol/SelectiveReplicationProtocol.hpp
/// \brief Capability-gated wire DTOs and codec for selective raw replication.
/// \details This is a separate protocol family from the complete global raw
/// stream. Cancellation tokens are local call-control state and are never
/// serialized. All integers are little-endian. Every message starts with:
/// \code
///   magic          "MDBXCSRP"  8 bytes
///   codec_version  u16         = 1
///   message_type   u8          1=hello, 2=pull request, 3=pull response,
///                                4=push request, 5=push response
///   message_flags  u32         = 0
/// \endcode
/// Strings and nested byte payloads use a u32 byte length. Collections use a
/// u32 element count. A descriptor is ScopeId, designated writer NodeId, then
/// sorted `(DBI name, DBI flags)` manifest entries. A scoped batch is ScopeId
/// plus one length-prefixed `ChangeBatchCodec` payload whose origin and seq are
/// interpreted as the designated writer and scope-local sequence.
///
/// Pull requests carry requester, DbId, ScopeId, cursor, and page bounds.
/// Successful pull responses carry status, the complete descriptor, source
/// tail metadata, contiguous scoped batches, and `has_more`. Push requests
/// carry sender, DbId, complete descriptor, and contiguous scoped batches.
/// Push responses carry status, ScopeId, and the last durable receiver cursor
/// even on failure. A failure pull response carries status only. Response
/// status is `ok`, structured error code, retryable flag, and bounded text.
///
/// An incompatible layout requires a codec version change. Unknown envelope
/// flags, message types, and structured error codes fail closed. Unknown hello
/// capability bits are optional advertisements: they round-trip but grant no
/// known capability unless both peers advertise the corresponding known bit.

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "ChangeBatchCodec.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Optional features understood by a selective-replication peer.
    enum class SelectiveReplicationCapability : std::uint64_t {
        None       = 0u,
        ScopedPull = UINT64_C(1) << 0,
        ScopedPush = UINT64_C(1) << 1
    };

    /// \brief Capability set exchanged before scoped pull or push.
    struct SelectiveReplicationCapabilities {
        std::uint64_t flags = 0u;

        bool supports(SelectiveReplicationCapability capability) const {
            const std::uint64_t bit =
                static_cast<std::uint64_t>(capability);
            return bit != 0u && (flags & bit) == bit;
        }
    };

    /// \brief Stable peer identity and selective protocol capabilities.
    struct SelectiveReplicationHello {
        NodeId node_id{};
        DbId db_id{};
        SelectiveReplicationCapabilities capabilities;
    };

    /// \brief Stable machine-readable selective protocol failure.
    enum class SelectiveReplicationErrorCode : std::uint16_t {
        None                            = 0u,
        UnsupportedSelectiveReplication = 1u,
        DbIdMismatch                    = 2u,
        ScopeDescriptorMismatch         = 3u,
        WrongDesignatedWriter           = 4u,
        ScopedSequenceGap               = 5u,
        OutOfScopeOperation             = 6u,
        ReceiverModeConflict            = 7u,
        ScopedSnapshotRequired          = 8u,
        BatchTooLarge                   = 9u
    };

    /// \brief Returns a stable diagnostic name for a selective error code.
    inline const char* selective_replication_error_code_name(
            SelectiveReplicationErrorCode code) {
        switch (code) {
            case SelectiveReplicationErrorCode::None:
                return "none";
            case SelectiveReplicationErrorCode::UnsupportedSelectiveReplication:
                return "unsupported_selective_replication";
            case SelectiveReplicationErrorCode::DbIdMismatch:
                return "db_id_mismatch";
            case SelectiveReplicationErrorCode::ScopeDescriptorMismatch:
                return "scope_descriptor_mismatch";
            case SelectiveReplicationErrorCode::WrongDesignatedWriter:
                return "wrong_designated_writer";
            case SelectiveReplicationErrorCode::ScopedSequenceGap:
                return "scoped_sequence_gap";
            case SelectiveReplicationErrorCode::OutOfScopeOperation:
                return "out_of_scope_operation";
            case SelectiveReplicationErrorCode::ReceiverModeConflict:
                return "receiver_mode_conflict";
            case SelectiveReplicationErrorCode::ScopedSnapshotRequired:
                return "scoped_snapshot_required";
            case SelectiveReplicationErrorCode::BatchTooLarge:
                return "batch_too_large";
        }
        return "unknown";
    }

    /// \brief One atomic scope-local projection of a source transaction.
    struct ScopedChangeBatch {
        std::uint32_t version = 1u;
        std::uint32_t batch_flags = BATCH_NONE;
        std::string scope_id;
        NodeId designated_writer_origin{};
        std::uint64_t scope_sequence = 0u;
        std::uint64_t time_unix_ns = 0u;
        std::vector<ChangeOp> ops;
    };

    /// \brief Requests a contiguous page after one scope-local cursor.
    struct ScopedPullRequest {
        NodeId requester{};
        DbId db_id{};
        std::string scope_id;
        std::uint64_t have_sequence = 0u;
        std::uint64_t max_batches = 1000u;
        std::uint64_t max_bytes = 4ULL * 1024ULL * 1024ULL;
        std::uint64_t max_single_batch_bytes = 4ULL * 1024ULL * 1024ULL;
        CancellationToken cancel_token;
    };

    /// \brief Returns one descriptor-bound page of scoped history.
    struct ScopedPullResponse {
        SelectiveReplicationDescriptor descriptor;
        std::uint64_t remote_tail = 0u;
        bool remote_tail_known = false;
        std::vector<ScopedChangeBatch> batches;
        bool has_more = false;
        bool ok = true;
        std::string error;
        SelectiveReplicationErrorCode error_code =
            SelectiveReplicationErrorCode::None;
        bool error_retryable = false;
    };

    /// \brief Sends descriptor-bound scoped history to a receiver.
    struct ScopedPushRequest {
        NodeId sender{};
        DbId db_id{};
        SelectiveReplicationDescriptor descriptor;
        std::vector<ScopedChangeBatch> batches;
        CancellationToken cancel_token;
    };

    /// \brief Reports the receiver's committed scope-local cursor.
    /// \details Failure responses retain the last durable cursor so the
    /// sender can recover from a gap; they never claim the rejected batch was
    /// committed.
    struct ScopedPushResponse {
        std::string scope_id;
        std::uint64_t receiver_sequence = 0u;
        bool ok = true;
        std::string error;
        SelectiveReplicationErrorCode error_code =
            SelectiveReplicationErrorCode::None;
        bool error_retryable = false;
    };

    inline std::uint64_t selective_replication_supported_capability_flags() {
        return static_cast<std::uint64_t>(
                   SelectiveReplicationCapability::ScopedPull) |
               static_cast<std::uint64_t>(
                   SelectiveReplicationCapability::ScopedPush);
    }

    inline bool selective_replication_capability_negotiated(
            const SelectiveReplicationCapabilities& local,
            const SelectiveReplicationCapabilities& remote,
            SelectiveReplicationCapability capability) {
        return local.supports(capability) && remote.supports(capability);
    }

    /// \brief Strict binary codec for selective-replication messages.
    class SelectiveReplicationProtocolCodec {
    public:
        enum class MessageType : std::uint8_t {
            Hello = 1u,
            PullRequest = 2u,
            PullResponse = 3u,
            PushRequest = 4u,
            PushResponse = 5u
        };

        static const std::uint8_t* magic() {
            static const std::uint8_t value[8] =
                { 'M', 'D', 'B', 'X', 'C', 'S', 'R', 'P' };
            return value;
        }

        static std::size_t magic_size() { return 8u; }
        static std::uint16_t codec_version() { return 1u; }

        static MessageType peek_message_type(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            Cursor cur = make_cursor(encoded, effective_bounds(bounds));
            return read_header_type(cur);
        }

        static std::vector<std::uint8_t> encode_hello(
                const SelectiveReplicationHello& hello,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            validate_hello(hello);
            std::vector<std::uint8_t> out = make_header(MessageType::Hello);
            append_id(out, hello.node_id);
            append_id(out, hello.db_id);
            detail::append_u64_le(out, hello.capabilities.flags);
            validate_size(out, effective);
            return out;
        }

        static SelectiveReplicationHello decode_hello(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            Cursor cur = make_cursor(encoded, effective);
            check_header(cur, MessageType::Hello);
            SelectiveReplicationHello out;
            read_id(cur, out.node_id);
            read_id(cur, out.db_id);
            out.capabilities.flags = read_u64(cur);
            validate_hello(out);
            check_consumed(cur);
            return out;
        }

        static std::vector<std::uint8_t> encode_pull_request(
                const ScopedPullRequest& request,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            validate_pull_request(request, effective);
            std::vector<std::uint8_t> out =
                make_header(MessageType::PullRequest);
            append_id(out, request.requester);
            append_id(out, request.db_id);
            append_scope_id(out, request.scope_id, effective);
            detail::append_u64_le(out, request.have_sequence);
            detail::append_u64_le(out, request.max_batches);
            detail::append_u64_le(out, request.max_bytes);
            detail::append_u64_le(out, request.max_single_batch_bytes);
            validate_size(out, effective);
            return out;
        }

        static ScopedPullRequest decode_pull_request(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            Cursor cur = make_cursor(encoded, effective);
            check_header(cur, MessageType::PullRequest);
            ScopedPullRequest out;
            read_id(cur, out.requester);
            read_id(cur, out.db_id);
            out.scope_id = read_scope_id(cur, effective);
            out.have_sequence = read_u64(cur);
            out.max_batches = read_u64(cur);
            out.max_bytes = read_u64(cur);
            out.max_single_batch_bytes = read_u64(cur);
            check_consumed(cur);
            validate_pull_request(out, effective);
            return out;
        }

        static std::vector<std::uint8_t> encode_pull_response(
                const ScopedPullResponse& response,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            validate_pull_response(response, effective);
            std::vector<std::uint8_t> out =
                make_header(MessageType::PullResponse);
            append_response_status(out, response.ok, response.error,
                                   response.error_code,
                                   response.error_retryable, effective);
            if (response.ok) {
                append_descriptor(out, response.descriptor, effective);
                detail::append_u64_le(out, response.remote_tail);
                append_bool(out, response.remote_tail_known);
                append_scoped_batches(out, response.batches,
                                      response.descriptor, effective);
                append_bool(out, response.has_more);
            }
            validate_size(out, effective);
            return out;
        }

        static ScopedPullResponse decode_pull_response(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            Cursor cur = make_cursor(encoded, effective);
            check_header(cur, MessageType::PullResponse);
            ScopedPullResponse out;
            read_response_status(cur, out.ok, out.error, out.error_code,
                                 out.error_retryable, effective);
            if (out.ok) {
                out.descriptor = read_descriptor(cur, effective);
                out.remote_tail = read_u64(cur);
                out.remote_tail_known = read_bool(cur);
                out.batches = read_scoped_batches(cur, out.descriptor,
                                                  effective);
                out.has_more = read_bool(cur);
            }
            check_consumed(cur);
            validate_pull_response(out, effective);
            return out;
        }

        static std::vector<std::uint8_t> encode_push_request(
                const ScopedPushRequest& request,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            validate_push_request(request, effective);
            std::vector<std::uint8_t> out =
                make_header(MessageType::PushRequest);
            append_id(out, request.sender);
            append_id(out, request.db_id);
            append_descriptor(out, request.descriptor, effective);
            append_scoped_batches(out, request.batches,
                                  request.descriptor, effective);
            validate_size(out, effective);
            return out;
        }

        static ScopedPushRequest decode_push_request(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            Cursor cur = make_cursor(encoded, effective);
            check_header(cur, MessageType::PushRequest);
            ScopedPushRequest out;
            read_id(cur, out.sender);
            read_id(cur, out.db_id);
            out.descriptor = read_descriptor(cur, effective);
            out.batches = read_scoped_batches(cur, out.descriptor, effective);
            check_consumed(cur);
            validate_push_request(out, effective);
            return out;
        }

        static std::vector<std::uint8_t> encode_push_response(
                const ScopedPushResponse& response,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            validate_push_response(response, effective);
            std::vector<std::uint8_t> out =
                make_header(MessageType::PushResponse);
            append_response_status(out, response.ok, response.error,
                                   response.error_code,
                                   response.error_retryable, effective);
            append_scope_id(out, response.scope_id, effective);
            detail::append_u64_le(out, response.receiver_sequence);
            validate_size(out, effective);
            return out;
        }

        static ScopedPushResponse decode_push_response(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            const CodecBounds& effective = effective_bounds(bounds);
            Cursor cur = make_cursor(encoded, effective);
            check_header(cur, MessageType::PushResponse);
            ScopedPushResponse out;
            read_response_status(cur, out.ok, out.error, out.error_code,
                                 out.error_retryable, effective);
            out.scope_id = read_scope_id(cur, effective);
            out.receiver_sequence = read_u64(cur);
            check_consumed(cur);
            validate_push_response(out, effective);
            return out;
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
            out.reserve(128u);
            append_bytes(out, magic(), magic_size());
            detail::append_u16_le(out, codec_version());
            out.push_back(static_cast<std::uint8_t>(type));
            detail::append_u32_le(out, 0u);
            return out;
        }

        static Cursor make_cursor(const std::vector<std::uint8_t>& encoded,
                                  const CodecBounds& bounds) {
            if (encoded.size() > bounds.max_transport_message_bytes) {
                throw std::length_error(
                    "selective message exceeds max_transport_message_bytes");
            }
            Cursor cur = { encoded.empty() ? nullptr : &encoded[0],
                           encoded.size(), 0u };
            return cur;
        }

        static MessageType read_header_type(Cursor& cur) {
            check_bounds(cur, magic_size());
            if (std::memcmp(cur.data + cur.pos, magic(), magic_size()) != 0) {
                throw std::runtime_error("Selective protocol magic mismatch");
            }
            cur.pos += magic_size();
            if (read_u16(cur) != codec_version()) {
                throw std::runtime_error(
                    "Unsupported selective protocol codec version");
            }
            const std::uint8_t raw = read_u8(cur);
            if (read_u32(cur) != 0u) {
                throw std::runtime_error(
                    "Unknown mandatory selective protocol flags");
            }
            switch (raw) {
                case 1u: return MessageType::Hello;
                case 2u: return MessageType::PullRequest;
                case 3u: return MessageType::PullResponse;
                case 4u: return MessageType::PushRequest;
                case 5u: return MessageType::PushResponse;
                default:
                    throw std::runtime_error(
                        "Unknown selective protocol message type");
            }
        }

        static void check_header(Cursor& cur, MessageType expected) {
            if (read_header_type(cur) != expected) {
                throw std::runtime_error(
                    "Unexpected selective protocol message type");
            }
        }

        static void validate_hello(const SelectiveReplicationHello& hello) {
            if (is_zero_sync_id(hello.node_id) ||
                is_zero_sync_id(hello.db_id)) {
                throw std::invalid_argument(
                    "selective hello identity is incomplete");
            }
        }

        static bool is_reserved_dbi(const std::string& name) {
            static const char prefix[] = "_mdbxc_";
            return name.size() >= sizeof(prefix) - 1u &&
                   name.compare(0u, sizeof(prefix) - 1u, prefix) == 0;
        }

        static void validate_scope_id(const std::string& scope_id,
                                      const CodecBounds& bounds) {
            if (scope_id.empty()) {
                throw std::invalid_argument("selective scope_id is empty");
            }
            if (scope_id.size() > bounds.max_selective_scope_id_len) {
                throw std::length_error(
                    "scope_id exceeds max_selective_scope_id_len");
            }
        }

        static void validate_descriptor(
                const SelectiveReplicationDescriptor& descriptor,
                const CodecBounds& bounds) {
            validate_scope_id(descriptor.scope_id, bounds);
            if (is_zero_sync_id(descriptor.designated_writer_origin)) {
                throw std::invalid_argument(
                    "selective descriptor writer is zero");
            }
            if (descriptor.manifest.empty()) {
                throw std::invalid_argument(
                    "selective descriptor manifest is empty");
            }
            if (descriptor.manifest.size() >
                bounds.max_selective_manifest_entries) {
                throw std::length_error(
                    "manifest exceeds max_selective_manifest_entries");
            }
            for (std::size_t i = 0u; i < descriptor.manifest.size(); ++i) {
                const std::string& name = descriptor.manifest[i].dbi_name();
                if (name.empty() || is_reserved_dbi(name)) {
                    throw std::invalid_argument(
                        "selective manifest contains invalid DBI name");
                }
                if (name.size() > bounds.max_dbi_name_len) {
                    throw std::length_error(
                        "manifest DBI exceeds max_dbi_name_len");
                }
                if (i != 0u &&
                    descriptor.manifest[i - 1u].dbi_name() >= name) {
                    throw std::invalid_argument(
                        "selective manifest must be sorted and unique");
                }
            }
        }

        static const SelectiveReplicationDbi* find_manifest_dbi(
                const SelectiveReplicationDescriptor& descriptor,
                const std::string& dbi_name) {
            for (std::size_t i = 0u; i < descriptor.manifest.size(); ++i) {
                if (descriptor.manifest[i].dbi_name() == dbi_name) {
                    return &descriptor.manifest[i];
                }
            }
            return nullptr;
        }

        static void validate_scoped_batch(
                const ScopedChangeBatch& batch,
                const SelectiveReplicationDescriptor& descriptor,
                const CodecBounds& bounds) {
            validate_scope_id(batch.scope_id, bounds);
            if (batch.scope_id != descriptor.scope_id) {
                throw std::invalid_argument("scoped batch scope mismatch");
            }
            if (compare_node_id(batch.designated_writer_origin,
                                descriptor.designated_writer_origin) != 0) {
                throw std::invalid_argument("scoped batch writer mismatch");
            }
            if (batch.scope_sequence == 0u) {
                throw std::invalid_argument(
                    "scoped batch sequence must not be zero");
            }
            if (batch.version != 1u || batch.batch_flags != BATCH_NONE) {
                throw std::invalid_argument(
                    "scoped batch version or flags are unsupported");
            }
            if (batch.ops.empty()) {
                throw std::invalid_argument(
                    "scoped batch operations must not be empty");
            }
            ChangeBatch nested = to_change_batch(batch);
            (void)ChangeBatchCodec::encode(nested, &bounds);
            for (std::size_t i = 0u; i < batch.ops.size(); ++i) {
                const SelectiveReplicationDbi* manifest_dbi =
                    find_manifest_dbi(descriptor, batch.ops[i].dbi_name);
                if (manifest_dbi == nullptr) {
                    throw std::invalid_argument(
                        "scoped batch operation is outside manifest");
                }
                if (manifest_dbi->dbi_flags() != batch.ops[i].dbi_flags) {
                    throw std::invalid_argument(
                        "scoped batch DBI flags mismatch");
                }
            }
        }

        static void validate_scoped_batches(
                const std::vector<ScopedChangeBatch>& batches,
                const SelectiveReplicationDescriptor& descriptor,
                const CodecBounds& bounds) {
            if (batches.size() > bounds.max_batches_per_message) {
                throw std::length_error(
                    "scoped batch count exceeds max_batches_per_message");
            }
            for (std::size_t i = 0u; i < batches.size(); ++i) {
                validate_scoped_batch(batches[i], descriptor, bounds);
                if (i != 0u &&
                    batches[i].scope_sequence !=
                        batches[i - 1u].scope_sequence + 1u) {
                    throw std::invalid_argument(
                        "scoped batches are not contiguous");
                }
            }
        }

        static void validate_pull_request(const ScopedPullRequest& request,
                                          const CodecBounds& bounds) {
            if (is_zero_sync_id(request.requester) ||
                is_zero_sync_id(request.db_id)) {
                throw std::invalid_argument(
                    "scoped pull request identity is incomplete");
            }
            validate_scope_id(request.scope_id, bounds);
            if (request.max_batches == 0u || request.max_bytes == 0u ||
                request.max_single_batch_bytes == 0u) {
                throw std::invalid_argument(
                    "scoped pull request bounds must not be zero");
            }
        }

        static void validate_response_status(
                bool ok, const std::string& error,
                SelectiveReplicationErrorCode error_code,
                bool retryable, const CodecBounds& bounds) {
            if (error.size() > bounds.max_error_len) {
                throw std::length_error(
                    "selective error exceeds max_error_len");
            }
            validate_error_code(error_code);
            if (ok) {
                if (!error.empty() || retryable ||
                    error_code != SelectiveReplicationErrorCode::None) {
                    throw std::invalid_argument(
                        "successful selective response has error state");
                }
            } else if (error_code == SelectiveReplicationErrorCode::None) {
                throw std::invalid_argument(
                    "failed selective response has no error code");
            }
        }

        static void validate_pull_response(
                const ScopedPullResponse& response,
                const CodecBounds& bounds) {
            validate_response_status(response.ok, response.error,
                                     response.error_code,
                                     response.error_retryable, bounds);
            if (!response.ok) {
                if (!response.descriptor.scope_id.empty() ||
                    !response.batches.empty() || response.remote_tail != 0u ||
                    response.remote_tail_known || response.has_more) {
                    throw std::invalid_argument(
                        "failed scoped pull response carries success state");
                }
                return;
            }
            validate_descriptor(response.descriptor, bounds);
            validate_scoped_batches(response.batches, response.descriptor,
                                    bounds);
            if (response.has_more && response.batches.empty()) {
                throw std::invalid_argument(
                    "scoped pull has_more without batches");
            }
            if (!response.remote_tail_known && response.remote_tail != 0u) {
                throw std::invalid_argument(
                    "unknown scoped pull tail carries progress");
            }
            if (response.remote_tail_known && !response.batches.empty() &&
                response.remote_tail <
                    response.batches.back().scope_sequence) {
                throw std::invalid_argument(
                    "scoped pull tail precedes returned batches");
            }
        }

        static void validate_push_request(const ScopedPushRequest& request,
                                          const CodecBounds& bounds) {
            if (is_zero_sync_id(request.sender) ||
                is_zero_sync_id(request.db_id)) {
                throw std::invalid_argument(
                    "scoped push request identity is incomplete");
            }
            validate_descriptor(request.descriptor, bounds);
            if (compare_node_id(request.sender,
                                request.descriptor.designated_writer_origin) !=
                0) {
                throw std::invalid_argument(
                    "scoped push sender is not designated writer");
            }
            if (request.batches.empty()) {
                throw std::invalid_argument(
                    "scoped push request has no batches");
            }
            validate_scoped_batches(request.batches, request.descriptor,
                                    bounds);
        }

        static void validate_push_response(
                const ScopedPushResponse& response,
                const CodecBounds& bounds) {
            validate_scope_id(response.scope_id, bounds);
            validate_response_status(response.ok, response.error,
                                     response.error_code,
                                     response.error_retryable, bounds);
        }

        static ChangeBatch to_change_batch(const ScopedChangeBatch& scoped) {
            ChangeBatch out;
            out.version = scoped.version;
            out.batch_flags = scoped.batch_flags;
            out.origin_node_id = scoped.designated_writer_origin;
            out.seq = scoped.scope_sequence;
            out.time_unix_ns = scoped.time_unix_ns;
            out.ops = scoped.ops;
            return out;
        }

        static ScopedChangeBatch from_change_batch(
                const std::string& scope_id, const ChangeBatch& batch) {
            ScopedChangeBatch out;
            out.version = batch.version;
            out.batch_flags = batch.batch_flags;
            out.scope_id = scope_id;
            out.designated_writer_origin = batch.origin_node_id;
            out.scope_sequence = batch.seq;
            out.time_unix_ns = batch.time_unix_ns;
            out.ops = batch.ops;
            return out;
        }

        static void append_descriptor(
                std::vector<std::uint8_t>& out,
                const SelectiveReplicationDescriptor& descriptor,
                const CodecBounds& bounds) {
            validate_descriptor(descriptor, bounds);
            append_scope_id(out, descriptor.scope_id, bounds);
            append_id(out, descriptor.designated_writer_origin);
            append_u32_size(out, descriptor.manifest.size(),
                            "selective manifest count exceeds u32");
            for (std::size_t i = 0u; i < descriptor.manifest.size(); ++i) {
                append_string(out, descriptor.manifest[i].dbi_name(),
                              bounds.max_dbi_name_len,
                              "manifest DBI exceeds max_dbi_name_len");
                detail::append_u32_le(out,
                                      descriptor.manifest[i].dbi_flags());
            }
        }

        static SelectiveReplicationDescriptor read_descriptor(
                Cursor& cur, const CodecBounds& bounds) {
            SelectiveReplicationDescriptor out;
            out.scope_id = read_scope_id(cur, bounds);
            read_id(cur, out.designated_writer_origin);
            const std::uint32_t count = read_u32(cur);
            if (count > bounds.max_selective_manifest_entries) {
                throw std::length_error(
                    "manifest exceeds max_selective_manifest_entries");
            }
            out.manifest.reserve(count);
            for (std::uint32_t i = 0u; i < count; ++i) {
                SelectiveReplicationDbi dbi;
                dbi.m_dbi_name = read_string(
                    cur, bounds.max_dbi_name_len,
                    "manifest DBI exceeds max_dbi_name_len");
                dbi.m_dbi_flags = read_u32(cur);
                out.manifest.push_back(dbi);
            }
            validate_descriptor(out, bounds);
            return out;
        }

        static void append_scoped_batches(
                std::vector<std::uint8_t>& out,
                const std::vector<ScopedChangeBatch>& batches,
                const SelectiveReplicationDescriptor& descriptor,
                const CodecBounds& bounds) {
            validate_scoped_batches(batches, descriptor, bounds);
            append_u32_size(out, batches.size(),
                            "scoped batch count exceeds u32");
            for (std::size_t i = 0u; i < batches.size(); ++i) {
                append_scope_id(out, batches[i].scope_id, bounds);
                const std::vector<std::uint8_t> nested =
                    ChangeBatchCodec::encode(to_change_batch(batches[i]),
                                             &bounds);
                if (nested.size() > bounds.max_batch_total_bytes) {
                    throw std::length_error(
                        "scoped batch exceeds max_batch_total_bytes");
                }
                append_u32_size(out, nested.size(),
                                "scoped batch length exceeds u32");
                append_bytes(out, nested.empty() ? nullptr : &nested[0],
                             nested.size());
            }
        }

        static std::vector<ScopedChangeBatch> read_scoped_batches(
                Cursor& cur,
                const SelectiveReplicationDescriptor& descriptor,
                const CodecBounds& bounds) {
            const std::uint32_t count = read_u32(cur);
            if (count > bounds.max_batches_per_message) {
                throw std::length_error(
                    "scoped batch count exceeds max_batches_per_message");
            }
            std::vector<ScopedChangeBatch> out;
            out.reserve(count);
            for (std::uint32_t i = 0u; i < count; ++i) {
                const std::string scope_id = read_scope_id(cur, bounds);
                const std::uint32_t size = read_u32(cur);
                if (size > bounds.max_batch_total_bytes) {
                    throw std::length_error(
                        "scoped batch exceeds max_batch_total_bytes");
                }
                const std::uint8_t* bytes = read_bytes(cur, size);
                std::vector<std::uint8_t> nested(size);
                if (size != 0u) std::memcpy(&nested[0], bytes, size);
                out.push_back(from_change_batch(
                    scope_id, ChangeBatchCodec::decode_exact(nested, &bounds)));
            }
            validate_scoped_batches(out, descriptor, bounds);
            return out;
        }

        static void append_response_status(
                std::vector<std::uint8_t>& out, bool ok,
                const std::string& error,
                SelectiveReplicationErrorCode error_code,
                bool retryable, const CodecBounds& bounds) {
            validate_response_status(ok, error, error_code, retryable, bounds);
            append_bool(out, ok);
            detail::append_u16_le(out,
                                  static_cast<std::uint16_t>(error_code));
            append_bool(out, retryable);
            append_string(out, error, bounds.max_error_len,
                          "selective error exceeds max_error_len");
        }

        static void read_response_status(
                Cursor& cur, bool& ok, std::string& error,
                SelectiveReplicationErrorCode& error_code,
                bool& retryable, const CodecBounds& bounds) {
            ok = read_bool(cur);
            error_code = static_cast<SelectiveReplicationErrorCode>(
                read_u16(cur));
            validate_error_code(error_code);
            retryable = read_bool(cur);
            error = read_string(cur, bounds.max_error_len,
                                "selective error exceeds max_error_len");
            validate_response_status(ok, error, error_code, retryable, bounds);
        }

        static void validate_error_code(SelectiveReplicationErrorCode code) {
            switch (code) {
                case SelectiveReplicationErrorCode::None:
                case SelectiveReplicationErrorCode::UnsupportedSelectiveReplication:
                case SelectiveReplicationErrorCode::DbIdMismatch:
                case SelectiveReplicationErrorCode::ScopeDescriptorMismatch:
                case SelectiveReplicationErrorCode::WrongDesignatedWriter:
                case SelectiveReplicationErrorCode::ScopedSequenceGap:
                case SelectiveReplicationErrorCode::OutOfScopeOperation:
                case SelectiveReplicationErrorCode::ReceiverModeConflict:
                case SelectiveReplicationErrorCode::ScopedSnapshotRequired:
                case SelectiveReplicationErrorCode::BatchTooLarge:
                    return;
            }
            throw std::runtime_error(
                "Unknown selective replication error code");
        }

        static void append_scope_id(std::vector<std::uint8_t>& out,
                                    const std::string& value,
                                    const CodecBounds& bounds) {
            validate_scope_id(value, bounds);
            append_string(out, value, bounds.max_selective_scope_id_len,
                          "scope_id exceeds max_selective_scope_id_len");
        }

        static std::string read_scope_id(Cursor& cur,
                                         const CodecBounds& bounds) {
            const std::string out = read_string(
                cur, bounds.max_selective_scope_id_len,
                "scope_id exceeds max_selective_scope_id_len");
            validate_scope_id(out, bounds);
            return out;
        }

        static void append_string(std::vector<std::uint8_t>& out,
                                  const std::string& value,
                                  std::uint32_t maximum,
                                  const char* error) {
            if (value.size() > maximum) throw std::length_error(error);
            append_u32_size(out, value.size(),
                            "selective string length exceeds u32");
            append_bytes(out,
                         value.empty() ? nullptr :
                             reinterpret_cast<const std::uint8_t*>(value.data()),
                         value.size());
        }

        static std::string read_string(Cursor& cur, std::uint32_t maximum,
                                       const char* error) {
            const std::uint32_t size = read_u32(cur);
            if (size > maximum) throw std::length_error(error);
            const std::uint8_t* bytes = read_bytes(cur, size);
            return std::string(reinterpret_cast<const char*>(bytes), size);
        }

        static void append_id(std::vector<std::uint8_t>& out,
                              const NodeId& id) {
            append_bytes(out, id.data(), id.size());
        }

        static void read_id(Cursor& cur, NodeId& id) {
            const std::uint8_t* bytes = read_bytes(cur, id.size());
            std::memcpy(id.data(), bytes, id.size());
        }

        static void append_bool(std::vector<std::uint8_t>& out, bool value) {
            out.push_back(value ? 1u : 0u);
        }

        static bool read_bool(Cursor& cur) {
            const std::uint8_t value = read_u8(cur);
            if (value > 1u) {
                throw std::runtime_error("Invalid selective protocol bool");
            }
            return value != 0u;
        }

        static void append_u32_size(std::vector<std::uint8_t>& out,
                                    std::size_t size, const char* error) {
            if (size > static_cast<std::size_t>(
                           std::numeric_limits<std::uint32_t>::max())) {
                throw std::length_error(error);
            }
            detail::append_u32_le(out, static_cast<std::uint32_t>(size));
        }

        static void append_bytes(std::vector<std::uint8_t>& out,
                                 const std::uint8_t* bytes,
                                 std::size_t size) {
            if (size == 0u) return;
            if (bytes == nullptr ||
                size > (std::numeric_limits<std::size_t>::max)() - out.size()) {
                throw std::length_error(
                    "selective protocol size overflow");
            }
            out.insert(out.end(), bytes, bytes + size);
        }

        static const std::uint8_t* read_bytes(Cursor& cur,
                                              std::size_t size) {
            check_bounds(cur, size);
            const std::uint8_t* out = cur.data + cur.pos;
            cur.pos += size;
            return out;
        }

        static std::uint8_t read_u8(Cursor& cur) {
            return *read_bytes(cur, 1u);
        }

        static std::uint16_t read_u16(Cursor& cur) {
            const std::uint8_t* bytes = read_bytes(cur, 2u);
            return detail::read_u16_le(bytes);
        }

        static std::uint32_t read_u32(Cursor& cur) {
            const std::uint8_t* bytes = read_bytes(cur, 4u);
            return detail::read_u32_le(bytes);
        }

        static std::uint64_t read_u64(Cursor& cur) {
            const std::uint8_t* bytes = read_bytes(cur, 8u);
            return detail::read_u64_le(bytes);
        }

        static void check_bounds(const Cursor& cur, std::size_t size) {
            if (cur.pos > cur.size || size > cur.size - cur.pos) {
                throw std::runtime_error(
                    "Selective protocol buffer underrun");
            }
        }

        static void check_consumed(const Cursor& cur) {
            if (cur.pos != cur.size) {
                throw std::runtime_error(
                    "Trailing bytes after selective protocol message");
            }
        }

        static void validate_size(const std::vector<std::uint8_t>& out,
                                  const CodecBounds& bounds) {
            if (out.size() > bounds.max_transport_message_bytes) {
                throw std::length_error(
                    "selective message exceeds max_transport_message_bytes");
            }
        }
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_SELECTIVE_REPLICATION_PROTOCOL_HPP_INCLUDED
