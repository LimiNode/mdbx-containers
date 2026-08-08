#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CHANGEBATCHCODEC_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CHANGEBATCHCODEC_HPP_INCLUDED

/// \file protocol/ChangeBatchCodec.hpp
/// \brief Stable little-endian binary codec for \c ChangeBatch.
/// \details
/// Wire layout:
/// \code
///   magic            "MDBXCSYN"   8 bytes
///   codec_version    u16 le       = 1
///   batch_version    u32 le       = 1
///   batch_flags      u32 le
///   origin_node_id   16 bytes
///   seq              u64 le
///   time_unix_ns     u64 le
///   ops_count        u32 le
///   for each op:
///     op_type        u8
///     op_flags       u32 le
///     dbi_flags      u32 le
///     dbi_name_len   u32 le
///     dbi_name       [u8; ...]
///     storage_key_len u32 le
///     storage_key    [u8; ...]
///     value_len      u32 le       (0xFFFFFFFF = absent)
///     value          [u8; ...]    (omitted if value_len == 0xFFFFFFFF)
///     identity_key_len u32 le     (omitted if !(op_flags & OP_HAS_IDENTITY_KEY))
///     identity_key   [u8; ...]
///     revision_key_len u32 le     (omitted if !(op_flags & OP_HAS_REVISION_KEY))
///     revision_key   [u8; ...]
/// \endcode
/// Mandatory unknown bits in either \c batch_flags or \c op_flags trigger a
/// decode error. \c BATCH_COMPRESSED_ZSTD is reserved and explicitly
/// rejected.

#include "ChangeBatch.hpp"
#include "CodecBounds.hpp"
#include "codec_flags.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Stable binary codec for \c ChangeBatch.
    class ChangeBatchCodec {
    public:
        /// \brief Encoded magic prefix (8 bytes, no NUL terminator).
        static const std::uint8_t* magic() {
            static const std::uint8_t m[8] = { 'M','D','B','X','C','S','Y','N' };
            return m;
        }

        /// \brief Magic prefix length in bytes.
        static std::size_t magic_size() { return 8; }

        /// \brief Supported codec version.
        static std::uint16_t codec_version() { return 1; }

        /// \brief Supported batch schema version.
        static std::uint32_t batch_version() { return 1; }

        /// \brief Encodes a batch into a byte vector.
        /// \param batch Source batch.
        /// \param bounds Structural limits; null disables validation.
        /// \throws std::length_error if any bound is exceeded.
        /// \throws std::logic_error for invalid combinations.
        static std::vector<std::uint8_t> encode(const ChangeBatch& batch,
                                                const CodecBounds* bounds = nullptr) {
            if (bounds != nullptr) {
                validate_bounds(batch, *bounds);
            }
            if (batch.version != batch_version()) {
                throw std::logic_error("Unsupported ChangeBatch::version");
            }
            if ((batch.batch_flags & BATCH_COMPRESSED_ZSTD) != 0) {
                throw std::logic_error("BATCH_COMPRESSED_ZSTD is not supported in v0.1");
            }
            const std::uint32_t known_batch_mask = static_cast<std::uint32_t>(
                BATCH_COMPRESSED_ZSTD | BATCH_HAS_MORE);
            if ((batch.batch_flags & ~known_batch_mask) != 0) {
                throw std::logic_error("ChangeBatch has unknown mandatory batch flags");
            }

            std::vector<std::uint8_t> out;
            out.reserve(256 + batch.ops.size() * 32);
            append_bytes(out, reinterpret_cast<const char*>(magic()), magic_size());
            detail::append_u16_le(out, codec_version());
            detail::append_u32_le(out, batch_version());
            detail::append_u32_le(out, batch.batch_flags);
            append_bytes(out, reinterpret_cast<const char*>(batch.origin_node_id.data()), 16);
            detail::append_u64_le(out, batch.seq);
            detail::append_u64_le(out, batch.time_unix_ns);
            detail::append_u32_le(out, static_cast<std::uint32_t>(batch.ops.size()));

            for (std::size_t i = 0; i < batch.ops.size(); ++i) {
                const ChangeOp& op = batch.ops[i];
                if (op.op_type > ChangeOpType::ClearTable) {
                    throw std::logic_error("Unknown ChangeOpType");
                }
                if ((op.op_flags & ~static_cast<std::uint32_t>(
                        OP_HAS_IDENTITY_KEY | OP_HAS_REVISION_KEY | OP_TOMBSTONE)) != 0) {
                    throw std::logic_error("ChangeOp has unknown mandatory flags");
                }
                if ((op.op_flags & OP_HAS_IDENTITY_KEY) != 0 && op.identity_key.empty()) {
                    throw std::logic_error("OP_HAS_IDENTITY_KEY set with empty identity_key");
                }
                if ((op.op_flags & OP_HAS_REVISION_KEY) != 0 && op.revision_key.empty()) {
                    throw std::logic_error("OP_HAS_REVISION_KEY set with empty revision_key");
                }
                if ((op.op_flags & OP_TOMBSTONE) != 0 && !op.value.empty()) {
                    throw std::logic_error("OP_TOMBSTONE set with non-empty value");
                }

                append_u8(out, static_cast<std::uint8_t>(op.op_type));
                detail::append_u32_le(out, op.op_flags);
                detail::append_u32_le(out, op.dbi_flags);
                detail::append_u32_le(out, static_cast<std::uint32_t>(op.dbi_name.size()));
                if (!op.dbi_name.empty()) {
                    append_bytes(out, op.dbi_name.data(), op.dbi_name.size());
                }
                detail::append_u32_le(out,
                                      static_cast<std::uint32_t>(op.storage_key.size()));
                if (!op.storage_key.empty()) {
                    append_bytes(out, reinterpret_cast<const char*>(op.storage_key.data()),
                                 op.storage_key.size());
                }
                if (op.value.empty()) {
                    detail::append_u32_le(out, 0xFFFFFFFFu);
                } else {
                    detail::append_u32_le(out, static_cast<std::uint32_t>(op.value.size()));
                    append_bytes(out, reinterpret_cast<const char*>(op.value.data()),
                                 op.value.size());
                }
                if ((op.op_flags & OP_HAS_IDENTITY_KEY) != 0) {
                    detail::append_u32_le(out,
                                          static_cast<std::uint32_t>(op.identity_key.size()));
                    append_bytes(out, reinterpret_cast<const char*>(op.identity_key.data()),
                                 op.identity_key.size());
                }
                if ((op.op_flags & OP_HAS_REVISION_KEY) != 0) {
                    detail::append_u32_le(out,
                                          static_cast<std::uint32_t>(op.revision_key.size()));
                    append_bytes(out, reinterpret_cast<const char*>(op.revision_key.data()),
                                 op.revision_key.size());
                }
            }
            if (bounds != nullptr &&
                out.size() > static_cast<std::size_t>(
                    bounds->max_batch_total_bytes)) {
                throw std::length_error(
                    "encoded change batch exceeds max_batch_total_bytes");
            }
            return out;
        }

        /// \brief Decodes a batch from a byte span.
        /// \param data Source bytes.
        /// \param bytes_read Optional output of bytes consumed. When null, any
        ///        trailing bytes after a valid batch cause a decode error; pass
        ///        a non-null pointer to use \c decode() as a stream parser.
        /// \param bounds Structural limits; null disables validation.
        /// \throws std::runtime_error on any format violation.
        /// \throws std::length_error when structural bounds are exceeded.
        static ChangeBatch decode(const std::vector<std::uint8_t>& data,
                                  std::size_t* bytes_read = nullptr,
                                  const CodecBounds* bounds = nullptr) {
            Cursor cur;
            cur.data = data.empty() ? nullptr : &data[0];
            cur.size = data.size();
            cur.pos = 0;

            ChangeBatch batch;
            check_magic(cur);
            const std::uint16_t cv = read_u16_le(cur);
            if (cv != codec_version()) {
                throw std::runtime_error("Unsupported codec_version");
            }
            const std::uint32_t bv = read_u32_le(cur);
            if (bv != batch_version()) {
                throw std::runtime_error("Unsupported batch_version");
            }
            batch.version = bv;
            batch.batch_flags = read_u32_le(cur);
            if ((batch.batch_flags & BATCH_COMPRESSED_ZSTD) != 0) {
                throw std::runtime_error("BATCH_COMPRESSED_ZSTD is not supported in v0.1");
            }
            const std::uint32_t known_batch_mask =
                static_cast<std::uint32_t>(BATCH_COMPRESSED_ZSTD | BATCH_HAS_MORE);
            if ((batch.batch_flags & ~known_batch_mask) != 0) {
                throw std::runtime_error("Unknown mandatory batch flag bits set");
            }

            read_bytes(cur, reinterpret_cast<char*>(batch.origin_node_id.data()), 16);
            batch.seq = read_u64_le(cur);
            batch.time_unix_ns = read_u64_le(cur);
            const std::uint32_t ops_count = read_u32_le(cur);

            if (bounds != nullptr) {
                if (ops_count > bounds->max_ops_per_batch) {
                    throw std::length_error("ops_count exceeds max_ops_per_batch");
                }
            }

            batch.ops.resize(ops_count);
            for (std::uint32_t i = 0; i < ops_count; ++i) {
                ChangeOp& op = batch.ops[i];
                const std::uint8_t op_type = read_u8(cur);
                if (op_type > static_cast<std::uint8_t>(ChangeOpType::ClearTable)) {
                    throw std::runtime_error("Unknown ChangeOpType");
                }
                op.op_type = static_cast<ChangeOpType>(op_type);
                op.op_flags = read_u32_le(cur);
                const std::uint32_t known_op_mask = static_cast<std::uint32_t>(
                    OP_HAS_IDENTITY_KEY | OP_HAS_REVISION_KEY | OP_TOMBSTONE);
                if ((op.op_flags & ~known_op_mask) != 0) {
                    throw std::runtime_error("Unknown mandatory op flag bits set");
                }
                op.dbi_flags = read_u32_le(cur);

                const std::uint32_t dbi_name_len = read_u32_le(cur);
                if (bounds != nullptr && dbi_name_len > bounds->max_dbi_name_len) {
                    throw std::length_error("dbi_name_len exceeds max_dbi_name_len");
                }
                op.dbi_name.assign(read_bytes_ptr(cur, dbi_name_len), dbi_name_len);

                const std::uint32_t storage_key_len = read_u32_le(cur);
                if (bounds != nullptr && storage_key_len > bounds->max_storage_key_len) {
                    throw std::length_error("storage_key_len exceeds max_storage_key_len");
                }
                const char* sk = read_bytes_ptr(cur, storage_key_len);
                op.storage_key.resize(storage_key_len);
                if (storage_key_len > 0) {
                    std::memcpy(op.storage_key.data(), sk, storage_key_len);
                }

                const std::uint32_t value_len = read_u32_le(cur);
                if (value_len == 0xFFFFFFFFu) {
                    // value absent (e.g. delete or tombstone)
                } else {
                    if (bounds != nullptr && value_len > bounds->max_value_len) {
                        throw std::length_error("value_len exceeds max_value_len");
                    }
                    const char* v = read_bytes_ptr(cur, value_len);
                    op.value.resize(value_len);
                    if (value_len > 0) {
                        std::memcpy(op.value.data(), v, value_len);
                    }
                }

                if ((op.op_flags & OP_HAS_IDENTITY_KEY) != 0) {
                    const std::uint32_t id_len = read_u32_le(cur);
                    if (bounds != nullptr && id_len > bounds->max_identity_key_len) {
                        throw std::length_error("identity_key_len exceeds max_identity_key_len");
                    }
                    const char* id = read_bytes_ptr(cur, id_len);
                    op.identity_key.resize(id_len);
                    if (id_len > 0) {
                        std::memcpy(op.identity_key.data(), id, id_len);
                    }
                }
                if ((op.op_flags & OP_HAS_REVISION_KEY) != 0) {
                    const std::uint32_t rv_len = read_u32_le(cur);
                    if (bounds != nullptr && rv_len > bounds->max_revision_key_len) {
                        throw std::length_error("revision_key_len exceeds max_revision_key_len");
                    }
                    const char* rv = read_bytes_ptr(cur, rv_len);
                    op.revision_key.resize(rv_len);
                    if (rv_len > 0) {
                        std::memcpy(op.revision_key.data(), rv, rv_len);
                    }
                }

                if ((op.op_flags & OP_TOMBSTONE) != 0 && !op.value.empty()) {
                    throw std::runtime_error("OP_TOMBSTONE with non-empty value");
                }
                if ((op.op_flags & OP_HAS_IDENTITY_KEY) != 0 && op.identity_key.empty()) {
                    throw std::runtime_error("OP_HAS_IDENTITY_KEY with empty identity_key");
                }
                if ((op.op_flags & OP_HAS_REVISION_KEY) != 0 && op.revision_key.empty()) {
                    throw std::runtime_error("OP_HAS_REVISION_KEY with empty revision_key");
                }
            }

            if (bounds != nullptr && cur.pos > bounds->max_batch_total_bytes) {
                throw std::length_error("total batch bytes exceeds max_batch_total_bytes");
            }

            if (bytes_read != nullptr) {
                *bytes_read = cur.pos;
            } else if (cur.pos != cur.size) {
                throw std::runtime_error("Trailing bytes after ChangeBatch");
            }
            return batch;
        }

        /// \brief Strict decoder: the input buffer must contain exactly one
        /// batch with no trailing bytes.
        /// \throws std::runtime_error on any format violation, including
        ///         trailing bytes.
        static ChangeBatch decode_exact(const std::vector<std::uint8_t>& data,
                                       const CodecBounds* bounds = nullptr) {
            std::size_t consumed = 0;
            ChangeBatch batch = decode(data, &consumed, bounds);
            if (consumed != data.size()) {
                throw std::runtime_error("Trailing bytes after ChangeBatch");
            }
            return batch;
        }

        /// \brief Validates encoder input against the given bounds.
        static void validate_bounds(const ChangeBatch& batch, const CodecBounds& bounds) {
            if (batch.ops.size() > bounds.max_ops_per_batch) {
                throw std::length_error("ops_count exceeds max_ops_per_batch");
            }
            for (std::size_t i = 0; i < batch.ops.size(); ++i) {
                const ChangeOp& op = batch.ops[i];
                if (op.dbi_name.size() > bounds.max_dbi_name_len) {
                    throw std::length_error("dbi_name_len exceeds max_dbi_name_len");
                }
                if (op.storage_key.size() > bounds.max_storage_key_len) {
                    throw std::length_error("storage_key_len exceeds max_storage_key_len");
                }
                if (op.value.size() > bounds.max_value_len) {
                    throw std::length_error("value_len exceeds max_value_len");
                }
                if (op.identity_key.size() > bounds.max_identity_key_len) {
                    throw std::length_error("identity_key_len exceeds max_identity_key_len");
                }
                if (op.revision_key.size() > bounds.max_revision_key_len) {
                    throw std::length_error("revision_key_len exceeds max_revision_key_len");
                }
            }
        }

    private:
        struct Cursor {
            const std::uint8_t* data = nullptr;
            std::size_t size = 0;
            std::size_t pos = 0;
        };

        static void check_bounds(const Cursor& cur, std::size_t n) {
            if (cur.pos + n > cur.size) {
                throw std::runtime_error("Codec buffer underrun");
            }
        }

        static void append_bytes(std::vector<std::uint8_t>& out, const char* src, std::size_t n) {
            const std::size_t base = out.size();
            out.resize(base + n);
            std::memcpy(&out[base], src, n);
        }

        static void append_u8(std::vector<std::uint8_t>& out, std::uint8_t v) {
            out.push_back(v);
        }

        static void check_magic(Cursor& cur) {
            check_bounds(cur, magic_size());
            if (std::memcmp(cur.data + cur.pos, magic(), magic_size()) != 0) {
                throw std::runtime_error("Codec magic mismatch");
            }
            cur.pos += magic_size();
        }

        static std::uint8_t read_u8(Cursor& cur) {
            check_bounds(cur, 1);
            const std::uint8_t v = cur.data[cur.pos];
            cur.pos += 1;
            return v;
        }

        static std::uint16_t read_u16_le(Cursor& cur) {
            check_bounds(cur, 2);
            const std::uint16_t v = detail::read_u16_le(cur.data + cur.pos);
            cur.pos += 2;
            return v;
        }

        static std::uint32_t read_u32_le(Cursor& cur) {
            check_bounds(cur, 4);
            const std::uint32_t v = detail::read_u32_le(cur.data + cur.pos);
            cur.pos += 4;
            return v;
        }

        static std::uint64_t read_u64_le(Cursor& cur) {
            check_bounds(cur, 8);
            const std::uint64_t v = detail::read_u64_le(cur.data + cur.pos);
            cur.pos += 8;
            return v;
        }

        static void read_bytes(Cursor& cur, char* dst, std::size_t n) {
            check_bounds(cur, n);
            std::memcpy(dst, cur.data + cur.pos, n);
            cur.pos += n;
        }

        static const char* read_bytes_ptr(Cursor& cur, std::size_t n) {
            check_bounds(cur, n);
            const char* p = reinterpret_cast<const char*>(cur.data + cur.pos);
            cur.pos += n;
            return p;
        }
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CHANGEBATCHCODEC_HPP_INCLUDED
