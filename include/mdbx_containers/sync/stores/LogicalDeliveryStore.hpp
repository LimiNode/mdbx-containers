#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_STORES_LOGICAL_DELIVERY_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_STORES_LOGICAL_DELIVERY_STORE_HPP_INCLUDED

/// \file LogicalDeliveryStore.hpp
/// \brief Persistent replay marker store for logical delivery envelopes.

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>

#include "../../common/MdbxException.hpp"
#include "../../detail/utils.hpp"
#include "../LogicalChangeFrameCodec.hpp"
#include "../LogicalDeliveryEnvelope.hpp"
#include "../common.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Read-only metadata decoded from an applied delivery marker.
    /// \details The inspection record intentionally exposes only delivery
    /// identity and stored frame metadata. It is not a pruning contract and
    /// does not expose or decode the nested logical frame payload.
    struct LogicalDeliveryMarkerInfo {
        DbId destination_db_uuid{};        ///< Destination database id.
        NodeId origin_node_id{};           ///< Delivery origin node.
        std::uint64_t origin_sequence = 0; ///< Origin delivery sequence.
        std::string frame_id;              ///< Stable sender frame id.
        std::uint16_t frame_codec_version = 0; ///< Nested frame codec version.
        std::uint32_t frame_bytes_size = 0;    ///< Stored nested frame bytes.
    };

    /// \brief Tracks applied logical delivery envelopes.
    /// \details Key = fixed-size origin node id + origin sequence + frame id
    /// digest. Value = full delivery identity, destination database id, and
    /// canonical encoded logical frame bytes.
    /// The caller writes this marker in the same MDBX transaction that applies
    /// the logical mutations, making duplicate delivery detection atomic with
    /// apply rollback/commit. Reusing the same delivery key with different
    /// frame content fails closed as an identity conflict.
    class LogicalDeliveryStore {
    public:
        explicit LogicalDeliveryStore(
                MDBX_env* env,
                const std::string& dbi_name = "_mdbxc_logical_delivery")
            : m_env(env),
              m_dbi_name(dbi_name),
              m_watermark_dbi_name(dbi_name + "_watermarks"),
              m_dbi(0),
              m_watermark_dbi(0),
              m_marker_open(false) {}

        /// \brief Opens the store DBI inside the supplied transaction.
        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "LogicalDeliveryStore::open");
            open_marker_checked(txn);
        }

        bool is_open() const { return m_marker_open; }

        MDBX_dbi handle(MDBX_txn* txn) const {
            txn = checked_txn(txn, "LogicalDeliveryStore::handle");
            open_marker_checked(txn);
            return m_dbi;
        }

        void reset_open() {
            m_marker_open = false;
        }

        void ensure_open() const {
            if (!m_marker_open) {
                throw std::logic_error("LogicalDeliveryStore is not open");
            }
        }

        /// \brief Counts persisted logical delivery markers.
        std::size_t count(MDBX_txn* txn) const {
            txn = checked_txn(txn, "LogicalDeliveryStore::count");
            open_const(txn);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalDeliveryStore cursor open failed");
            std::size_t out = 0;
            try {
                MDBX_val key;
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    (void)decode_and_validate_marker(key, value);
                    ++out;
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "LogicalDeliveryStore cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            return out;
        }

        /// \brief Lists persisted logical delivery marker identities.
        /// \param limit Maximum number of markers to return, or zero for all.
        std::vector<LogicalDeliveryMarkerInfo> list_markers(
                MDBX_txn* txn,
                std::size_t limit = 0) const {
            txn = checked_txn(txn, "LogicalDeliveryStore::list_markers");
            open_const(txn);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalDeliveryStore cursor open failed");
            std::vector<LogicalDeliveryMarkerInfo> out;
            try {
                MDBX_val key;
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    out.push_back(decode_and_validate_marker(key, value));
                    if (limit != 0 && out.size() >= limit) {
                        break;
                    }
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND && (limit == 0 || out.size() < limit)) {
                    check_mdbx(rc, "LogicalDeliveryStore cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            return out;
        }

        /// \brief Returns the persisted replay-pruning watermark for \p origin.
        /// \return Zero when no markers for \p origin have been pruned or
        /// the optional watermark DBI has not been created yet.
        std::uint64_t watermark(MDBX_txn* txn,
                                const NodeId& origin) const {
            txn = checked_txn(txn, "LogicalDeliveryStore::watermark");
            return read_watermark(txn, origin);
        }

        /// \brief Prunes markers through \p safe_through_sequence for one origin.
        /// \details This persists a monotonic replay watermark in the same
        /// transaction as marker removal. Future envelopes from \p origin
        /// whose sequence is at or below the watermark are treated as stale
        /// successful no-ops. The caller must advance this boundary only after
        /// its delivery protocol guarantees that no unseen envelope at or
        /// below the boundary can arrive later.
        /// The first call creates the optional watermark DBI and therefore
        /// requires one additional named-DBI slot in the MDBX environment.
        /// \return Number of removed delivery markers.
        std::size_t prune_up_to(MDBX_txn* txn,
                                const NodeId& origin,
                                std::uint64_t safe_through_sequence) {
            txn = checked_txn(txn, "LogicalDeliveryStore::prune_up_to");
            if (is_zero_sync_id(origin)) {
                throw std::invalid_argument(
                    "LogicalDeliveryStore prune origin is zero");
            }
            if (safe_through_sequence == 0u) {
                throw std::invalid_argument(
                    "LogicalDeliveryStore prune watermark is zero");
            }
            open_marker_checked(txn);
            open_watermark_for_write(txn);
            const std::uint64_t previous = read_watermark(txn, origin);
            if (safe_through_sequence < previous) {
                throw std::invalid_argument(
                    "LogicalDeliveryStore prune watermark moved backwards");
            }
            if (safe_through_sequence == previous) {
                return 0u;
            }

            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalDeliveryStore prune cursor open failed");
            std::size_t removed = 0u;
            try {
                MDBX_val key;
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    const LogicalDeliveryMarkerInfo marker =
                        decode_and_validate_marker(key, value);
                    if (compare_node_id(marker.origin_node_id, origin) == 0 &&
                        marker.origin_sequence <= safe_through_sequence) {
                        check_mdbx(mdbx_cursor_del(cursor, MDBX_CURRENT),
                                   "LogicalDeliveryStore prune cursor delete failed");
                        ++removed;
                    }
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc,
                               "LogicalDeliveryStore prune cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            write_watermark(txn, origin, safe_through_sequence);
            return removed;
        }

        /// \brief Returns true when \p envelope has an exact applied marker.
        /// \details A persisted replay watermark is intentionally not included
        /// in this inspection result. Use \c watermark() to inspect the
        /// pruning boundary; \c try_mark_applied() applies that boundary when
        /// deciding whether a delivery may mutate local data.
        bool contains(MDBX_txn* txn,
                      const LogicalDeliveryEnvelope& envelope,
                      const CodecBounds* bounds = nullptr) const {
            txn = checked_txn(txn, "LogicalDeliveryStore::contains");
            open_const(txn);
            validate_logical_delivery_envelope(envelope, bounds);
            const std::vector<std::uint8_t> key =
                make_key(envelope, bounds);
            MDBX_val k = {
                const_cast<std::uint8_t*>(key.empty() ? nullptr : &key[0]),
                key.size()
            };
            MDBX_val v;
            const int rc = mdbx_get(txn, m_dbi, &k, &v);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "LogicalDeliveryStore read failed");
            assert_marker_value_matches(envelope, v, bounds);
            return true;
        }

        /// \brief Inserts an applied marker.
        /// \return false when the exact delivery key already exists.
        bool try_mark_applied(MDBX_txn* txn,
                              const LogicalDeliveryEnvelope& envelope,
                              const CodecBounds* bounds = nullptr) {
            txn = checked_txn(txn, "LogicalDeliveryStore::try_mark_applied");
            open_marker_checked(txn);
            validate_logical_delivery_envelope(envelope, bounds);
            if (is_at_or_below_watermark(txn, envelope)) {
                return false;
            }
            const std::vector<std::uint8_t> key =
                make_key(envelope, bounds);
            MDBX_val k = {
                const_cast<std::uint8_t*>(key.empty() ? nullptr : &key[0]),
                key.size()
            };
            const std::vector<std::uint8_t> value =
                make_marker_value(envelope, bounds);
            MDBX_val v = {
                const_cast<std::uint8_t*>(
                    value.empty() ? nullptr : &value[0]),
                value.size()
            };
            const int rc = mdbx_put(txn, m_dbi, &k, &v, MDBX_NOOVERWRITE);
            if (rc == MDBX_KEYEXIST) {
                MDBX_val existing;
                check_mdbx(mdbx_get(txn, m_dbi, &k, &existing),
                           "LogicalDeliveryStore duplicate read failed");
                assert_marker_value_matches(envelope, existing, bounds);
                return false;
            }
            check_mdbx(rc, "LogicalDeliveryStore write failed");
            return true;
        }

    private:
        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        void open_const(MDBX_txn* txn) const {
            txn = checked_txn(txn, "LogicalDeliveryStore::open");
            open_marker_checked(txn);
        }

        void open_marker_checked(MDBX_txn* txn) const {
            open_dbi_checked(txn, m_dbi_name, m_dbi);
            m_marker_open = true;
        }

        bool open_watermark_if_exists(MDBX_txn* txn) const {
            const int rc = mdbx_dbi_open(
                txn, m_watermark_dbi_name.c_str(),
                static_cast<MDBX_db_flags_t>(0), &m_watermark_dbi);
            if (rc == MDBX_NOTFOUND) {
                return false;
            }
            check_mdbx(rc, "Failed to open LogicalDeliveryStore watermark DBI");
            return true;
        }

        void open_watermark_for_write(MDBX_txn* txn) const {
            open_dbi_checked(txn, m_watermark_dbi_name, m_watermark_dbi);
        }

        static void open_dbi_checked(MDBX_txn* txn,
                                     const std::string& name,
                                     MDBX_dbi& dbi) {
            int rc = mdbx_dbi_open(txn, name.c_str(), MDBX_CREATE, &dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0), &dbi);
            }
            check_mdbx(rc, "Failed to open LogicalDeliveryStore DBI");
        }

        std::uint64_t read_watermark(MDBX_txn* txn,
                                     const NodeId& origin) const {
            if (!open_watermark_if_exists(txn)) {
                return 0u;
            }
            MDBX_val key = {
                const_cast<std::uint8_t*>(&origin[0]),
                origin.size()
            };
            MDBX_val value;
            const int rc = mdbx_get(txn, m_watermark_dbi, &key, &value);
            if (rc == MDBX_NOTFOUND) {
                return 0u;
            }
            check_mdbx(rc, "LogicalDeliveryStore watermark read failed");
            return decode_watermark(value);
        }

        void write_watermark(MDBX_txn* txn,
                             const NodeId& origin,
                             std::uint64_t sequence) const {
            open_watermark_for_write(txn);
            MDBX_val key = {
                const_cast<std::uint8_t*>(&origin[0]),
                origin.size()
            };
            const std::vector<std::uint8_t> bytes =
                encode_watermark(sequence);
            MDBX_val value = {
                const_cast<std::uint8_t*>(
                    bytes.empty() ? nullptr : &bytes[0]),
                bytes.size()
            };
            check_mdbx(mdbx_put(txn, m_watermark_dbi, &key, &value,
                                MDBX_UPSERT),
                       "LogicalDeliveryStore watermark write failed");
        }

        bool is_at_or_below_watermark(
                MDBX_txn* txn,
                const LogicalDeliveryEnvelope& envelope) const {
            return envelope.origin_sequence <=
                read_watermark(txn, envelope.origin_node_id);
        }

        static std::vector<std::uint8_t> make_key(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds) {
            validate_logical_delivery_envelope(envelope, bounds);
            if (envelope.frame_id.size() >
                static_cast<std::size_t>(
                    (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error(
                    "Logical delivery frame id length exceeds u32");
            }
            const std::vector<std::uint8_t> frame_id_digest =
                make_frame_id_digest(envelope.frame_id);
            std::vector<std::uint8_t> key;
            key.reserve(2u + envelope.origin_node_id.size() + 8u +
                        frame_id_digest.size());
            detail::append_u16_le(key, marker_key_version());
            key.insert(key.end(),
                       envelope.origin_node_id.begin(),
                       envelope.origin_node_id.end());
            detail::append_u64_le(key, envelope.origin_sequence);
            key.insert(key.end(), frame_id_digest.begin(),
                       frame_id_digest.end());
            return key;
        }

        static std::vector<std::uint8_t> make_marker_value(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds) {
            validate_logical_delivery_envelope(envelope, bounds);
            const std::vector<std::uint8_t> frame_bytes =
                LogicalChangeFrameCodec::encode(envelope.frame, bounds);
            if (frame_bytes.size() >
                static_cast<std::size_t>(
                    (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error(
                    "Logical delivery frame bytes exceed u32");
            }
            if (envelope.frame_id.size() >
                static_cast<std::size_t>(
                    (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error(
                    "Logical delivery frame id length exceeds u32");
            }
            std::vector<std::uint8_t> out;
            out.reserve(2u + envelope.destination_db_uuid.size() +
                        envelope.origin_node_id.size() + 8u + 4u +
                        envelope.frame_id.size() + 2u + 4u +
                        frame_bytes.size());
            detail::append_u16_le(out, marker_value_version());
            out.insert(out.end(),
                       envelope.destination_db_uuid.begin(),
                       envelope.destination_db_uuid.end());
            out.insert(out.end(),
                       envelope.origin_node_id.begin(),
                       envelope.origin_node_id.end());
            detail::append_u64_le(out, envelope.origin_sequence);
            detail::append_u32_le(
                out, static_cast<std::uint32_t>(envelope.frame_id.size()));
            out.insert(out.end(),
                       envelope.frame_id.begin(),
                       envelope.frame_id.end());
            detail::append_u16_le(
                out, LogicalChangeFrameCodec::codec_version());
            detail::append_u32_le(
                out, static_cast<std::uint32_t>(frame_bytes.size()));
            out.insert(out.end(), frame_bytes.begin(), frame_bytes.end());
            return out;
        }

        static void assert_marker_value_matches(
                const LogicalDeliveryEnvelope& envelope,
                const MDBX_val& value,
                const CodecBounds* bounds) {
            const std::vector<std::uint8_t> expected =
                make_marker_value(envelope, bounds);
            if (value.iov_len != expected.size()) {
                throw std::runtime_error(
                    "LogicalDeliveryStore delivery identity conflict");
            }
            if (expected.empty()) {
                return;
            }
            if (std::memcmp(value.iov_base, &expected[0],
                            expected.size()) != 0) {
                throw std::runtime_error(
                    "LogicalDeliveryStore delivery identity conflict");
            }
        }

        static std::vector<std::uint8_t> make_frame_id_digest(
                const std::string& frame_id) {
            std::uint64_t h0 = UINT64_C(0xcbf29ce484222325);
            std::uint64_t h1 = UINT64_C(0x9ae16a3b2f90404f);
            for (std::size_t i = 0; i < frame_id.size(); ++i) {
                const std::uint8_t b =
                    static_cast<std::uint8_t>(frame_id[i]);
                h0 ^= static_cast<std::uint64_t>(b);
                h0 *= UINT64_C(0x100000001b3);
                h1 ^= static_cast<std::uint64_t>(b) +
                      static_cast<std::uint64_t>(i & 0xffu);
                h1 *= UINT64_C(0x100000001b3);
            }
            std::vector<std::uint8_t> out;
            out.reserve(16u);
            detail::append_u64_le(out, h0);
            detail::append_u64_le(out, h1);
            return out;
        }

        static std::uint16_t marker_key_version() { return 1; }

        static std::uint16_t marker_value_version() { return 1; }

        static std::uint16_t watermark_value_version() { return 1; }

        static std::vector<std::uint8_t> encode_watermark(
                std::uint64_t sequence) {
            std::vector<std::uint8_t> out;
            out.reserve(2u + 8u);
            detail::append_u16_le(out, watermark_value_version());
            detail::append_u64_le(out, sequence);
            return out;
        }

        static std::uint64_t decode_watermark(const MDBX_val& value) {
            if (value.iov_len != 2u + 8u || value.iov_base == nullptr) {
                throw std::runtime_error(
                    "LogicalDeliveryStore watermark has invalid size");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(value.iov_base);
            if (detail::read_u16_le(bytes) != watermark_value_version()) {
                throw std::runtime_error(
                    "Unsupported LogicalDeliveryStore watermark version");
            }
            const std::uint64_t sequence = detail::read_u64_le(bytes + 2u);
            if (sequence == 0u) {
                throw std::runtime_error(
                    "LogicalDeliveryStore watermark is zero");
            }
            return sequence;
        }

        struct ValueCursor {
            const std::uint8_t* data;
            std::size_t size;
            std::size_t pos;
        };

        struct MarkerKeyInfo {
            NodeId origin_node_id{};
            std::uint64_t origin_sequence = 0;
            std::vector<std::uint8_t> frame_id_digest;
        };

        static void require_bytes(ValueCursor& cur,
                                  std::size_t n,
                                  const char* context) {
            if (n > cur.size || cur.pos > cur.size - n) {
                throw std::runtime_error(context);
            }
        }

        static std::uint16_t read_marker_u16(ValueCursor& cur) {
            require_bytes(cur, 2u,
                          "LogicalDeliveryStore marker value underrun");
            const std::uint16_t out = detail::read_u16_le(cur.data + cur.pos);
            cur.pos += 2u;
            return out;
        }

        static std::uint32_t read_marker_u32(ValueCursor& cur) {
            require_bytes(cur, 4u,
                          "LogicalDeliveryStore marker value underrun");
            const std::uint32_t out = detail::read_u32_le(cur.data + cur.pos);
            cur.pos += 4u;
            return out;
        }

        static std::uint64_t read_marker_u64(ValueCursor& cur) {
            require_bytes(cur, 8u,
                          "LogicalDeliveryStore marker value underrun");
            const std::uint64_t out = detail::read_u64_le(cur.data + cur.pos);
            cur.pos += 8u;
            return out;
        }

        static void read_marker_id(ValueCursor& cur, NodeId& out) {
            require_bytes(cur, out.size(),
                          "LogicalDeliveryStore marker value underrun");
            for (std::size_t i = 0; i < out.size(); ++i) {
                out[i] = cur.data[cur.pos + i];
            }
            cur.pos += out.size();
        }

        static std::string read_marker_string(ValueCursor& cur,
                                              std::uint32_t len) {
            require_bytes(cur, len,
                          "LogicalDeliveryStore marker value underrun");
            const char* begin =
                reinterpret_cast<const char*>(cur.data + cur.pos);
            cur.pos += len;
            return std::string(begin, begin + len);
        }

        static LogicalDeliveryMarkerInfo decode_marker_value(
                const MDBX_val& value) {
            ValueCursor cur = {
                static_cast<const std::uint8_t*>(value.iov_base),
                value.iov_len,
                0u
            };
            LogicalDeliveryMarkerInfo out;
            const std::uint16_t version = read_marker_u16(cur);
            if (version != marker_value_version()) {
                throw std::runtime_error(
                    "Unsupported LogicalDeliveryStore marker value version");
            }
            read_marker_id(cur, out.destination_db_uuid);
            read_marker_id(cur, out.origin_node_id);
            out.origin_sequence = read_marker_u64(cur);
            const std::uint32_t frame_id_len = read_marker_u32(cur);
            out.frame_id = read_marker_string(cur, frame_id_len);
            out.frame_codec_version = read_marker_u16(cur);
            out.frame_bytes_size = read_marker_u32(cur);
            require_bytes(cur, out.frame_bytes_size,
                          "LogicalDeliveryStore marker value underrun");
            cur.pos += out.frame_bytes_size;
            if (cur.pos != cur.size) {
                throw std::runtime_error(
                    "Trailing bytes after LogicalDeliveryStore marker value");
            }
            return out;
        }

        static MarkerKeyInfo decode_marker_key(const MDBX_val& key) {
            ValueCursor cur = {
                static_cast<const std::uint8_t*>(key.iov_base),
                key.iov_len,
                0u
            };
            const std::size_t digest_size = 16u;
            const std::size_t expected_size =
                2u + NodeId().size() + 8u + digest_size;
            if (cur.size != expected_size) {
                throw std::runtime_error(
                    "LogicalDeliveryStore marker key has unexpected size");
            }
            const std::uint16_t version = read_marker_u16(cur);
            if (version != marker_key_version()) {
                throw std::runtime_error(
                    "Unsupported LogicalDeliveryStore marker key version");
            }
            MarkerKeyInfo out;
            read_marker_id(cur, out.origin_node_id);
            out.origin_sequence = read_marker_u64(cur);
            require_bytes(cur, digest_size,
                          "LogicalDeliveryStore marker key underrun");
            out.frame_id_digest.assign(cur.data + cur.pos,
                                       cur.data + cur.pos + digest_size);
            cur.pos += digest_size;
            if (cur.pos != cur.size) {
                throw std::runtime_error(
                    "Trailing bytes after LogicalDeliveryStore marker key");
            }
            return out;
        }

        static LogicalDeliveryMarkerInfo decode_and_validate_marker(
                const MDBX_val& key,
                const MDBX_val& value) {
            const MarkerKeyInfo key_info = decode_marker_key(key);
            const LogicalDeliveryMarkerInfo info =
                decode_marker_value(value);
            if (compare_node_id(key_info.origin_node_id,
                                info.origin_node_id) != 0 ||
                key_info.origin_sequence != info.origin_sequence) {
                throw std::runtime_error(
                    "LogicalDeliveryStore marker key/value identity mismatch");
            }
            const std::vector<std::uint8_t> expected_digest =
                make_frame_id_digest(info.frame_id);
            if (key_info.frame_id_digest != expected_digest) {
                throw std::runtime_error(
                    "LogicalDeliveryStore marker key/value frame id mismatch");
            }
            return info;
        }

        MDBX_env*   m_env;
        std::string m_dbi_name;
        std::string m_watermark_dbi_name;
        mutable MDBX_dbi m_dbi;
        mutable MDBX_dbi m_watermark_dbi;
        mutable bool     m_marker_open;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_STORES_LOGICAL_DELIVERY_STORE_HPP_INCLUDED
