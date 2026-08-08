#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_FULLSNAPSHOTPROTOCOL_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_FULLSNAPSHOTPROTOCOL_HPP_INCLUDED

/// \file protocol/FullSnapshotProtocol.hpp
/// \brief Explicit full-snapshot chunk contract and codec.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "ChangeBatchCodec.hpp"
#include "CodecBounds.hpp"
#include "SyncCursor.hpp"

namespace mdbxc {
namespace sync {

    /// \brief One named user DBI in an immutable snapshot manifest.
    struct FullSnapshotManifestEntry {
        std::string dbi_name;
        std::uint32_t dbi_flags = 0u;
    };

    /// \brief Declares which user-DBI content a snapshot may replace.
    /// \details \c ManifestOnly replaces only manifest DBIs and never
    /// bootstraps global raw-sync progress. \c CompleteUserDatabase replaces
    /// every named user DBI and is the only scope that may bootstrap the
    /// per-origin applied cursor for subsequent incremental replication. The
    /// current complete scope is raw-only and rejects sources with persistent
    /// logical-sync state until a separate logical snapshot protocol exists.
    enum class FullSnapshotScope : std::uint8_t {
        ManifestOnly = 0u,
        CompleteUserDatabase = 1u
    };

    /// \brief Explicit source-side configuration for materialized snapshots.
    /// \details For \c ManifestOnly, c manifest explicitly declares the
    /// exportable DBIs, including empty tables. For
    /// \c CompleteUserDatabase, c manifest must be empty: the engine
    /// enumerates every named non-reserved DBI in MainDB under one read
    /// transaction. Both modes bound one-session materialization before
    /// enabling full-snapshot pull.
    struct FullSnapshotExportOptions {
        std::vector<FullSnapshotManifestEntry> manifest;
        FullSnapshotScope replacement_scope = FullSnapshotScope::ManifestOnly;
        std::uint64_t max_materialized_operations = 1000000u;
        std::uint64_t max_materialized_bytes =
            256ULL * 1024ULL * 1024ULL;
        std::uint32_t max_active_sessions = 4u;
        std::chrono::seconds session_idle_timeout =
            std::chrono::seconds(300);
    };

    /// \brief Receiver-side bounds for one staged full snapshot import.
    /// \details The importer keeps pages only in process memory until the
    /// final page. These bounds prevent a peer from accumulating an unbounded
    /// replacement plan before the one atomic destination commit. Logical-aware
    /// recovery spends the same budget on its final logical baseline, including
    /// schema, replay, ordering, and pending-delivery records.
    struct FullSnapshotImportOptions {
        std::uint64_t max_staged_operations = 1000000u;
        std::uint64_t max_staged_bytes =
            256ULL * 1024ULL * 1024ULL;
    };

    /// \brief Progress returned after accepting one full snapshot chunk.
    struct FullSnapshotImportResult {
        bool completed = false;
        std::uint64_t next_chunk_index = 0u;
    };

    /// \brief One chunk of a full database export.
    /// \details Snapshot chunks are not changelog batches. They carry a
    /// stable source identity, an immutable manifest, and a nested raw batch
    /// whose sequence is always zero. The transport validates the complete
    /// manifest and snapshot continuation before any user-DBI mutation.
    struct FullSnapshotChunk {
        NodeId source_node_id{};
        DbId source_db_uuid{};
        std::string snapshot_id;
        /// \brief Immutable per-origin replication tail captured with this snapshot.
        /// \details This is the componentwise maximum of source changelog and
        /// already-applied remote-origin progress, so a fresh receiver can
        /// continue incremental pull from every represented origin.
        SyncCursor source_tail;
        std::uint64_t chunk_index = 0u;
        bool has_more = false;
        /// \brief Explicit replacement scope for the receiver.
        FullSnapshotScope replacement_scope = FullSnapshotScope::ManifestOnly;
        /// \brief Opaque token that requests the immediate next chunk.
        /// \details Non-empty exactly when \c has_more is true. Callers return
        /// this value unchanged in PullRequest::full_snapshot_continuation.
        std::string continuation;
        /// \brief Version of the immutable manifest contract.
        std::uint32_t manifest_version = 1u;
        std::vector<FullSnapshotManifestEntry> manifest;
        ChangeBatch batch;
    };

    /// \brief Strict codec for the explicit full-snapshot wire boundary.
    /// \details Configured sources export bounded snapshot sessions through
    /// \c PullRequest::request_full_snapshot. Receivers stage pages and
    /// commit the validated replacement plan atomically at the final page.
    class FullSnapshotCodec {
    public:
        static const std::uint8_t* magic() {
            static const std::uint8_t value[8] =
                { 'M', 'D', 'B', 'X', 'C', 'S', 'N', 'P' };
            return value;
        }

        static std::size_t magic_size() { return 8u; }
        static std::uint16_t codec_version() { return 2u; }

        static std::vector<std::uint8_t> encode(
                const FullSnapshotChunk& chunk,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            validate(chunk, bounds);
            const std::vector<std::uint8_t> nested =
                ChangeBatchCodec::encode(chunk.batch, bounds);
            std::vector<std::uint8_t> out;
            append_bytes(out, magic(), magic_size());
            detail::append_u16_le(out, codec_version());
            append_bytes(out, chunk.source_node_id.data(),
                         chunk.source_node_id.size());
            append_bytes(out, chunk.source_db_uuid.data(),
                         chunk.source_db_uuid.size());
            append_string(out, chunk.snapshot_id);
            append_cursor(out, chunk.source_tail, bounds);
            detail::append_u64_le(out, chunk.chunk_index);
            out.push_back(chunk.has_more ? 1u : 0u);
            append_scope(out, chunk.replacement_scope);
            append_string(out, chunk.continuation);
            detail::append_u32_le(out, chunk.manifest_version);
            detail::append_u32_le(
                out, static_cast<std::uint32_t>(chunk.manifest.size()));
            for (std::size_t i = 0u; i < chunk.manifest.size(); ++i) {
                append_string(out, chunk.manifest[i].dbi_name);
                detail::append_u32_le(out, chunk.manifest[i].dbi_flags);
            }
            if (nested.size() > (std::numeric_limits<std::uint32_t>::max)()) {
                throw std::length_error("full snapshot nested batch exceeds u32");
            }
            detail::append_u32_le(out, static_cast<std::uint32_t>(nested.size()));
            append_bytes(out, nested.empty() ? nullptr : &nested[0], nested.size());
            validate_encoded_size(out, bounds);
            return out;
        }

        static FullSnapshotChunk decode(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            validate_encoded_size(encoded, bounds);
            Cursor cursor = make_cursor(encoded);
            check_bytes(cursor, magic(), magic_size());
            if (read_u16_le(cursor) != codec_version()) {
                throw std::runtime_error("Unsupported full snapshot codec version");
            }
            FullSnapshotChunk out;
            read_bytes(cursor, out.source_node_id.data(), out.source_node_id.size());
            read_bytes(cursor, out.source_db_uuid.data(), out.source_db_uuid.size());
            out.snapshot_id = read_string(
                cursor, bounds == nullptr ? 256u : bounds->max_snapshot_id_len,
                "full snapshot id exceeds max_snapshot_id_len");
            out.source_tail = read_cursor(cursor, bounds);
            out.chunk_index = read_u64_le(cursor);
            const std::uint8_t has_more = read_u8(cursor);
            if (has_more > 1u) {
                throw std::runtime_error("Invalid full snapshot continuation flag");
            }
            out.has_more = has_more != 0u;
            out.replacement_scope = read_scope(cursor);
            out.continuation = read_string(
                cursor, bounds == nullptr ? 256u : bounds->max_snapshot_id_len,
                "full snapshot continuation exceeds max_snapshot_id_len");
            out.manifest_version = read_u32_le(cursor);
            const std::uint32_t manifest_count = read_u32_le(cursor);
            if (bounds != nullptr &&
                manifest_count > bounds->max_snapshot_manifest_entries) {
                throw std::length_error(
                    "full snapshot manifest exceeds max_snapshot_manifest_entries");
            }
            out.manifest.resize(manifest_count);
            for (std::uint32_t i = 0u; i < manifest_count; ++i) {
                out.manifest[i].dbi_name = read_string(
                    cursor, bounds == nullptr ? 256u : bounds->max_dbi_name_len,
                    "full snapshot manifest DBI name exceeds max_dbi_name_len");
                out.manifest[i].dbi_flags = read_u32_le(cursor);
            }
            const std::uint32_t nested_size = read_u32_le(cursor);
            const std::uint8_t* nested_data = read_pointer(cursor, nested_size);
            std::vector<std::uint8_t> nested(nested_size);
            if (nested_size != 0u) {
                std::memcpy(&nested[0], nested_data, nested_size);
            }
            out.batch = ChangeBatchCodec::decode_exact(nested, bounds);
            if (cursor.pos != cursor.size) {
                throw std::runtime_error("Trailing bytes after full snapshot chunk");
            }
            validate(out, bounds);
            return out;
        }

        static void validate(const FullSnapshotChunk& chunk,
                             const CodecBounds* bounds = nullptr) {
            bounds = effective_bounds(bounds);
            if (is_zero_id(chunk.source_node_id) ||
                is_zero_id(chunk.source_db_uuid)) {
                throw std::runtime_error(
                    "Full snapshot source identity is incomplete");
            }
            if (chunk.snapshot_id.empty()) {
                throw std::invalid_argument("Full snapshot id is empty");
            }
            if (chunk.snapshot_id.size() > bounds->max_snapshot_id_len) {
                throw std::length_error(
                    "full snapshot id exceeds max_snapshot_id_len");
            }
            if (chunk.continuation.size() > bounds->max_snapshot_id_len) {
                throw std::length_error(
                    "full snapshot continuation exceeds max_snapshot_id_len");
            }
            if (chunk.has_more != !chunk.continuation.empty()) {
                throw std::invalid_argument(
                    "Full snapshot continuation does not match has_more");
            }
            if (chunk.manifest_version != 1u) {
                throw std::invalid_argument(
                    "Unsupported full snapshot manifest version");
            }
            validate_scope(chunk.replacement_scope);
            validate_cursor(chunk.source_tail, bounds);
            if (chunk.manifest.size() > bounds->max_snapshot_manifest_entries) {
                throw std::length_error(
                    "full snapshot manifest exceeds max_snapshot_manifest_entries");
            }
            if (chunk.manifest.empty()) {
                throw std::invalid_argument("Full snapshot manifest is empty");
            }
            for (std::size_t i = 0u; i < chunk.manifest.size(); ++i) {
                const FullSnapshotManifestEntry& entry = chunk.manifest[i];
                if (entry.dbi_name.empty() || is_reserved_dbi(entry.dbi_name)) {
                    throw std::invalid_argument(
                        "Full snapshot manifest contains a reserved or empty DBI");
                }
                if (i != 0u &&
                    chunk.manifest[i - 1u].dbi_name >= entry.dbi_name) {
                    throw std::invalid_argument(
                        "Full snapshot manifest must be sorted and unique");
                }
                if (entry.dbi_name.size() > bounds->max_dbi_name_len) {
                    throw std::length_error(
                        "full snapshot manifest DBI name exceeds max_dbi_name_len");
                }
            }
            if (chunk.batch.version != ChangeBatchCodec::batch_version() ||
                chunk.batch.seq != 0u ||
                chunk.batch.origin_node_id != chunk.source_node_id) {
                throw std::invalid_argument(
                    "Full snapshot batch must use source identity and seq=0");
            }
            const std::uint32_t expected_flags =
                chunk.has_more ? static_cast<std::uint32_t>(BATCH_HAS_MORE) :
                    static_cast<std::uint32_t>(BATCH_NONE);
            if (chunk.batch.batch_flags != expected_flags) {
                throw std::invalid_argument(
                    "Full snapshot continuation does not match batch flags");
            }
            if (chunk.batch.ops.empty()) {
                throw std::invalid_argument(
                    "Full snapshot chunk must contain at least one operation");
            }
            for (std::size_t i = 0u; i < chunk.batch.ops.size(); ++i) {
                const ChangeOp& op = chunk.batch.ops[i];
                validate_snapshot_operation(op);
                if (is_reserved_dbi(op.dbi_name)) {
                    throw std::invalid_argument(
                        "Full snapshot contains a reserved DBI operation");
                }
                std::uint32_t manifest_flags = 0u;
                if (!manifest_entry_flags(chunk.manifest,
                                          op.dbi_name,
                                          manifest_flags)) {
                    throw std::invalid_argument(
                        "Full snapshot operation is outside its manifest");
                }
                if (op.dbi_flags != manifest_flags) {
                    throw std::invalid_argument(
                        "Full snapshot operation DBI flags differ from manifest");
                }
            }
        }

    private:
        struct Cursor {
            const std::uint8_t* data;
            std::size_t size;
            std::size_t pos;
        };

        static bool is_reserved_dbi(const std::string& name) {
            return name.size() >= 7u && name.compare(0u, 7u, "_mdbxc_") == 0;
        }

        static const CodecBounds* effective_bounds(
                const CodecBounds* bounds) {
            static const CodecBounds defaults;
            return bounds != nullptr ? bounds : &defaults;
        }

        static void validate_snapshot_operation(const ChangeOp& op) {
            if (op.op_type != ChangeOpType::Put &&
                op.op_type != ChangeOpType::ClearTable) {
                throw std::invalid_argument(
                    "Full snapshot operation must be Put or ClearTable");
            }
            if (op.op_flags != OP_NONE || !op.identity_key.empty() ||
                !op.revision_key.empty()) {
                throw std::invalid_argument(
                    "Full snapshot operation contains non-physical metadata");
            }
            if (op.op_type == ChangeOpType::ClearTable &&
                (!op.storage_key.empty() || !op.value.empty())) {
                throw std::invalid_argument(
                    "Full snapshot ClearTable operation contains key or value bytes");
            }
        }

        static bool is_zero_id(const NodeId& id) {
            for (std::size_t i = 0u; i < id.size(); ++i) {
                if (id[i] != 0u) return false;
            }
            return true;
        }

        static bool manifest_entry_flags(
                const std::vector<FullSnapshotManifestEntry>& manifest,
                const std::string& name,
                std::uint32_t& flags) {
            for (std::size_t i = 0u; i < manifest.size(); ++i) {
                if (manifest[i].dbi_name == name) {
                    flags = manifest[i].dbi_flags;
                    return true;
                }
            }
            return false;
        }

        static void validate_scope(FullSnapshotScope scope) {
            switch (scope) {
                case FullSnapshotScope::ManifestOnly:
                case FullSnapshotScope::CompleteUserDatabase:
                    return;
            }
            throw std::invalid_argument("Invalid full snapshot replacement scope");
        }

        static void append_scope(std::vector<std::uint8_t>& out,
                                 FullSnapshotScope scope) {
            validate_scope(scope);
            out.push_back(static_cast<std::uint8_t>(scope));
        }

        static FullSnapshotScope read_scope(Cursor& cursor) {
            const std::uint8_t raw = read_u8(cursor);
            switch (raw) {
                case static_cast<std::uint8_t>(FullSnapshotScope::ManifestOnly):
                    return FullSnapshotScope::ManifestOnly;
                case static_cast<std::uint8_t>(
                        FullSnapshotScope::CompleteUserDatabase):
                    return FullSnapshotScope::CompleteUserDatabase;
                default:
                    throw std::runtime_error(
                        "Invalid full snapshot replacement scope");
            }
        }

        static void append_cursor(std::vector<std::uint8_t>& out,
                                  const SyncCursor& value,
                                  const CodecBounds* bounds) {
            validate_cursor(value, bounds);
            detail::append_u32_le(
                out, static_cast<std::uint32_t>(value.last_seq_by_origin.size()));
            std::map<NodeId, std::uint64_t>::const_iterator it =
                value.last_seq_by_origin.begin();
            for (; it != value.last_seq_by_origin.end(); ++it) {
                append_bytes(out, it->first.data(), it->first.size());
                detail::append_u64_le(out, it->second);
            }
        }

        static SyncCursor read_cursor(Cursor& cursor, const CodecBounds* bounds) {
            const std::uint32_t count = read_u32_le(cursor);
            if (count > bounds->max_cursor_origins) {
                throw std::length_error(
                    "full snapshot tail exceeds max_cursor_origins");
            }
            SyncCursor out;
            for (std::uint32_t i = 0u; i < count; ++i) {
                NodeId origin{};
                read_bytes(cursor, origin.data(), origin.size());
                const std::uint64_t sequence = read_u64_le(cursor);
                if (out.last_seq_by_origin.find(origin) !=
                    out.last_seq_by_origin.end()) {
                    throw std::runtime_error(
                        "Duplicate origin in full snapshot tail");
                }
                out.last_seq_by_origin[origin] = sequence;
            }
            return out;
        }

        static void validate_cursor(const SyncCursor& value,
                                    const CodecBounds* bounds) {
            if (value.last_seq_by_origin.size() > bounds->max_cursor_origins) {
                throw std::length_error(
                    "full snapshot tail exceeds max_cursor_origins");
            }
        }

        static void append_bytes(std::vector<std::uint8_t>& out,
                                 const std::uint8_t* data,
                                 std::size_t size) {
            if (size == 0u) return;
            if (data == nullptr ||
                size > (std::numeric_limits<std::size_t>::max)() - out.size()) {
                throw std::length_error("full snapshot size overflow");
            }
            const std::size_t old_size = out.size();
            out.resize(old_size + size);
            std::memcpy(&out[old_size], data, size);
        }

        static void append_string(std::vector<std::uint8_t>& out,
                                  const std::string& value) {
            if (value.size() > (std::numeric_limits<std::uint32_t>::max)()) {
                throw std::length_error("full snapshot string exceeds u32");
            }
            detail::append_u32_le(out, static_cast<std::uint32_t>(value.size()));
            append_bytes(out,
                         value.empty() ? nullptr :
                             reinterpret_cast<const std::uint8_t*>(value.data()),
                         value.size());
        }

        static Cursor make_cursor(const std::vector<std::uint8_t>& encoded) {
            Cursor out = { encoded.empty() ? nullptr : &encoded[0],
                           encoded.size(), 0u };
            return out;
        }

        static void require(Cursor& cursor, std::size_t size) {
            if (size > cursor.size - cursor.pos) {
                throw std::runtime_error("Truncated full snapshot chunk");
            }
        }

        static void check_bytes(Cursor& cursor, const std::uint8_t* expected,
                                std::size_t size) {
            require(cursor, size);
            if (std::memcmp(cursor.data + cursor.pos, expected, size) != 0) {
                throw std::runtime_error("Invalid full snapshot magic");
            }
            cursor.pos += size;
        }

        static std::uint8_t read_u8(Cursor& cursor) {
            require(cursor, 1u);
            return cursor.data[cursor.pos++];
        }

        static std::uint16_t read_u16_le(Cursor& cursor) {
            require(cursor, 2u);
            const std::uint16_t out =
                static_cast<std::uint16_t>(cursor.data[cursor.pos]) |
                static_cast<std::uint16_t>(cursor.data[cursor.pos + 1u] << 8u);
            cursor.pos += 2u;
            return out;
        }

        static std::uint32_t read_u32_le(Cursor& cursor) {
            require(cursor, 4u);
            std::uint32_t out = 0u;
            for (std::size_t i = 0u; i < 4u; ++i) {
                out |= static_cast<std::uint32_t>(cursor.data[cursor.pos + i])
                    << (8u * i);
            }
            cursor.pos += 4u;
            return out;
        }

        static std::uint64_t read_u64_le(Cursor& cursor) {
            require(cursor, 8u);
            std::uint64_t out = 0u;
            for (std::size_t i = 0u; i < 8u; ++i) {
                out |= static_cast<std::uint64_t>(cursor.data[cursor.pos + i])
                    << (8u * i);
            }
            cursor.pos += 8u;
            return out;
        }

        static const std::uint8_t* read_pointer(Cursor& cursor,
                                                 std::size_t size) {
            require(cursor, size);
            const std::uint8_t* out = cursor.data + cursor.pos;
            cursor.pos += size;
            return out;
        }

        static void read_bytes(Cursor& cursor, std::uint8_t* out,
                               std::size_t size) {
            const std::uint8_t* data = read_pointer(cursor, size);
            if (size != 0u) std::memcpy(out, data, size);
        }

        static std::string read_string(Cursor& cursor,
                                       std::size_t max_size,
                                       const char* error) {
            const std::uint32_t size = read_u32_le(cursor);
            if (size > max_size) {
                throw std::length_error(error);
            }
            const std::uint8_t* data = read_pointer(cursor, size);
            return size == 0u
                ? std::string()
                : std::string(reinterpret_cast<const char*>(data), size);
        }

        static void validate_encoded_size(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds) {
            if (encoded.size() > bounds->max_snapshot_chunk_bytes) {
                throw std::length_error(
                    "full snapshot chunk exceeds max_snapshot_chunk_bytes");
            }
        }
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_FULLSNAPSHOTPROTOCOL_HPP_INCLUDED
