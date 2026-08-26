#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_STORES_CHANGE_LOG_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_STORES_CHANGE_LOG_STORE_HPP_INCLUDED

/// \file ChangeLogStore.hpp
/// \brief Per-origin changelog of raw \c ChangeBatch bytes.

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>

#include "OriginIndexStore.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Raw bytestore for replicated \c ChangeBatch records keyed by
    /// \c (origin_node_id, seq).
    /// \details Values are \c ChangeBatchCodec::encode() output, opaque to
    /// this layer. The store makes no assumption about seq monotonicity; the
    /// caller (SyncEngine) is responsible for gap detection and ordering.
    class ChangeLogStore {
    public:
        /// \brief Constructs a wrapper bound to \p env.
        ChangeLogStore(MDBX_env* env,
                       const std::string& dbi_name = "_mdbxc_changelog")
            : m_env(env),
              m_dbi_name(dbi_name),
              m_dbi(0),
              m_open(false),
              m_origin_index_ready(false),
              m_origins(env) {}

        /// \brief Opens the DBI inside the supplied transaction.
        /// \details Tries \c MDBX_CREATE first; falls back to a plain open
        /// when the transaction is read-only and the DBI already exists.
        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "ChangeLogStore::open");
            if (m_open) return;
            int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE, &m_dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0), &m_dbi);
            }
            check_mdbx(rc, "Failed to open ChangeLogStore DBI");
            m_open = true;
        }

        bool is_open() const { return m_open; }
        MDBX_dbi handle() const { return m_dbi; }

        /// \brief Resets the open flag so the next \c open() reopens the DBI
        /// inside the supplied transaction. Use between transactions to avoid
        /// using a stale handle from a prior committed/closed transaction.
        void reset_open() {
            m_open = false;
            m_origin_index_ready = false;
            m_origins.reset_open();
        }

        /// \brief Throws when the DBI has not been opened yet.
        void ensure_open() const {
            if (!m_open) {
                throw std::logic_error("ChangeLogStore is not open");
            }
        }

        /// \brief Appends a single batch for \p origin at \p seq.
        /// \details Uses \c MDBX_NOOVERWRITE so accidental re-use of a
        /// (origin, seq) key surfaces immediately.
        void append(MDBX_txn* txn, const NodeId& origin,
                    std::uint64_t seq, const std::vector<std::uint8_t>& bytes) {
            txn = checked_txn(txn, "ChangeLogStore::append");
            ensure_open();
            ensure_origin_index_ready(txn);
            std::vector<std::uint8_t> key_buf;
            encode_key(origin, seq, key_buf);
            MDBX_val k = { key_buf.empty() ? nullptr : &key_buf[0], key_buf.size() };
            MDBX_val v = { bytes.empty() ? nullptr : const_cast<std::uint8_t*>(&bytes[0]),
                           bytes.size() };
            check_mdbx(
                mdbx_put(txn, m_dbi, &k, &v, MDBX_NOOVERWRITE),
                "ChangeLogStore append failed"
            );
            m_origins.note_origin(txn, origin, seq);
        }

        /// \brief Returns true when a record exists for (\p origin, \p seq).
        bool contains(MDBX_txn* txn, const NodeId& origin, std::uint64_t seq) const {
            txn = checked_txn(txn, "ChangeLogStore::contains");
            ensure_open();
            std::vector<std::uint8_t> key_buf;
            encode_key(origin, seq, key_buf);
            MDBX_val k = { key_buf.empty() ? nullptr : &key_buf[0], key_buf.size() };
            MDBX_val v;
            const int rc = mdbx_get(txn, m_dbi, &k, &v);
            if (rc == MDBX_SUCCESS) return true;
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "ChangeLogStore contains failed");
            return false;
        }

        /// \brief Reads raw batch bytes for (\p origin, \p seq).
        /// \return true when present, false when absent.
        bool get(MDBX_txn* txn, const NodeId& origin, std::uint64_t seq,
                 std::vector<std::uint8_t>& out) const {
            txn = checked_txn(txn, "ChangeLogStore::get");
            ensure_open();
            std::vector<std::uint8_t> key_buf;
            encode_key(origin, seq, key_buf);
            MDBX_val k = { key_buf.empty() ? nullptr : &key_buf[0], key_buf.size() };
            MDBX_val v;
            const int rc = mdbx_get(txn, m_dbi, &k, &v);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "ChangeLogStore get failed");
            out.resize(v.iov_len);
            if (v.iov_len > 0) {
                std::memcpy(out.data(), v.iov_base, v.iov_len);
            }
            return true;
        }

        /// \brief Removes the record at (\p origin, \p seq).
        /// \return true when a record was removed, false when absent.
        bool erase(MDBX_txn* txn, const NodeId& origin, std::uint64_t seq) {
            txn = checked_txn(txn, "ChangeLogStore::erase");
            ensure_open();
            std::vector<std::uint8_t> key_buf;
            encode_key(origin, seq, key_buf);
            MDBX_val k = { key_buf.empty() ? nullptr : &key_buf[0], key_buf.size() };
            const int rc = mdbx_del(txn, m_dbi, &k, nullptr);
            if (rc == MDBX_SUCCESS) return true;
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "ChangeLogStore erase failed");
            return false;
        }

        /// \brief Removes every record with seq <= \p up_to for \p origin.
        /// \return Number of records removed.
        std::size_t prune_up_to(MDBX_txn* txn, const NodeId& origin,
                                std::uint64_t up_to) {
            txn = checked_txn(txn, "ChangeLogStore::prune_up_to");
            if (!m_open) {
                throw std::logic_error("ChangeLogStore is not open");
            }
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &raw), "cursor open failed");
            std::size_t removed = 0;
            try {
                std::vector<std::uint8_t> lo_key, hi_key;
                encode_key(origin, 0, lo_key);
                encode_key(origin, up_to, hi_key);
                MDBX_val lo = { lo_key.empty() ? nullptr : &lo_key[0], lo_key.size() };
                MDBX_val hi = { hi_key.empty() ? nullptr : &hi_key[0], hi_key.size() };
                MDBX_val k = lo;
                MDBX_val v;
                int rc = mdbx_cursor_get(raw, &k, &v, MDBX_SET_RANGE);
                while (rc == MDBX_SUCCESS) {
                    if (k.iov_len < 24) break;
                    if (mdbx_cmp(txn, m_dbi, &k, &hi) > 0) break;
                    rc = mdbx_cursor_del(raw, MDBX_CURRENT);
                    if (rc == MDBX_SUCCESS) {
                        ++removed;
                        rc = mdbx_cursor_get(raw, &k, &v, MDBX_NEXT);
                    } else if (rc == MDBX_NOTFOUND) {
                        break;
                    } else {
                        check_mdbx(rc, "ChangeLogStore prune cursor_del failed");
                    }
                }
                if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "ChangeLogStore prune cursor_get failed");
                }
            } catch (...) {
                mdbx_cursor_close(raw);
                throw;
            }
            mdbx_cursor_close(raw);
            return removed;
        }

        /// \brief Checks whether \c _mdbxc_origins exactly mirrors changelog tails.
        /// \param txn Active transaction.
        /// \return \c true when every changelog origin has one matching index
        /// entry with the same max seq, and the index has no extra origins.
        /// \complexity O(changelog entries + indexed origins).
        /// \note Intended for startup diagnostics, manual repair, or rare
        /// integrity checks. Do not call from the normal pull/sync hot path.
        bool origin_index_matches_changelog(MDBX_txn* txn) const {
            txn = checked_txn(txn, "ChangeLogStore::origin_index_matches_changelog");
            ensure_open();
            const std::vector<OriginTail> expected =
                collect_changelog_origin_tails(txn);

            OriginIndexStore origins(m_env);
            if (!origins.open_existing(txn)) {
                return expected.empty();
            }

            const std::vector<NodeId> actual_origins = origins.origins(txn);
            if (actual_origins.size() != expected.size()) {
                return false;
            }
            for (std::size_t i = 0; i < expected.size(); ++i) {
                if (actual_origins[i] != expected[i].origin) {
                    return false;
                }
                std::uint64_t actual_seq = 0;
                if (!origins.last_seq(txn, actual_origins[i], actual_seq) ||
                    actual_seq != expected[i].seq) {
                    return false;
                }
            }
            return true;
        }

        /// \brief Rebuilds \c _mdbxc_origins from changelog keys.
        /// \param txn Active transaction.
        /// \return Number of origins written to the rebuilt index.
        /// \pre Transaction must be writable.
        /// \complexity O(changelog entries + indexed origins).
        /// \note Explicit maintenance operation; ordinary pull does not call it.
        std::size_t rebuild_origin_index(MDBX_txn* txn) {
            txn = checked_txn(txn, "ChangeLogStore::rebuild_origin_index");
            ensure_open();
            const std::vector<OriginTail> tails =
                collect_changelog_origin_tails(txn);
            m_origins.open(txn);
            m_origins.clear(txn);
            write_origin_tails(txn, tails);
            m_origin_index_ready = true;
            return tails.size();
        }

    private:
        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        struct OriginTail {
            NodeId origin;
            std::uint64_t seq;
        };

        static void encode_key(const NodeId& origin, std::uint64_t seq,
                               std::vector<std::uint8_t>& out) {
            out.resize(24);
            std::memcpy(out.data(), origin.data(), 16);
            // Big-endian seq preserves numeric order under MDBX bytewise scans.
            detail::write_u64_be(seq, out.data() + 16);
        }

        static NodeId decode_key_origin(const MDBX_val& key) {
            if (key.iov_len != 24) {
                throw std::runtime_error("ChangeLogStore key has invalid size");
            }
            NodeId origin{};
            std::memcpy(origin.data(), key.iov_base, 16);
            return origin;
        }

        static std::uint64_t decode_key_seq(const MDBX_val& key) {
            if (key.iov_len != 24) {
                throw std::runtime_error("ChangeLogStore key has invalid size");
            }
            const std::uint8_t* bytes = static_cast<const std::uint8_t*>(key.iov_base);
            return detail::read_u64_be(bytes + 16);
        }

        std::vector<OriginTail> collect_changelog_origin_tails(MDBX_txn* txn) const {
            std::vector<OriginTail> out;
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &raw),
                       "ChangeLogStore origin scan cursor open failed");
            try {
                MDBX_val k, v;
                int rc = mdbx_cursor_get(raw, &k, &v, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    const NodeId origin = decode_key_origin(k);
                    const std::uint64_t seq = decode_key_seq(k);
                    if (out.empty() ||
                        compare_node_id(out.back().origin, origin) != 0) {
                        OriginTail tail;
                        tail.origin = origin;
                        tail.seq = seq;
                        out.push_back(tail);
                    } else {
                        out.back().seq = seq;
                    }
                    rc = mdbx_cursor_get(raw, &k, &v, MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "ChangeLogStore origin scan cursor walk failed");
                }
            } catch (...) {
                mdbx_cursor_close(raw);
                throw;
            }
            mdbx_cursor_close(raw);
            return out;
        }

        void write_origin_tails(MDBX_txn* txn,
                                const std::vector<OriginTail>& tails) {
            for (std::size_t i = 0; i < tails.size(); ++i) {
                m_origins.note_origin(txn, tails[i].origin, tails[i].seq);
            }
        }

        void ensure_origin_index_ready(MDBX_txn* txn) {
            if (m_origin_index_ready) {
                return;
            }
            m_origins.open(txn);
            if (!m_origins.empty(txn)) {
                m_origin_index_ready = true;
                return;
            }
            backfill_origin_index(txn);
            m_origin_index_ready = true;
        }

        void backfill_origin_index(MDBX_txn* txn) {
            const std::vector<OriginTail> tails =
                collect_changelog_origin_tails(txn);
            write_origin_tails(txn, tails);
        }

        MDBX_env*     m_env;
        std::string   m_dbi_name;
        MDBX_dbi      m_dbi;
        bool          m_open;
        bool          m_origin_index_ready;
        OriginIndexStore m_origins;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_STORES_CHANGE_LOG_STORE_HPP_INCLUDED
