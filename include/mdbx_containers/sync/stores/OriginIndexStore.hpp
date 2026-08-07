#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_STORES_ORIGIN_INDEX_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_STORES_ORIGIN_INDEX_STORE_HPP_INCLUDED

/// \file OriginIndexStore.hpp
/// \brief Compact index of origins present in the local changelog.

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>


namespace mdbxc {
namespace sync {

    /// \brief Thin wrapper around \c _mdbxc_origins.
    /// \details Key = \c origin_node_id (16 raw bytes), value = max known
    /// changelog seq for that origin as \c u64 little-endian. Pull pagination
    /// uses the value as a cheap tail check before seeking changelog keys for
    /// exact batch iteration.
    class OriginIndexStore {
    public:
        /// \brief Indexed origin and its max known changelog sequence.
        struct OriginTail {
            NodeId origin;
            std::uint64_t last_seq;
        };

        /// \brief Constructs a wrapper bound to \p env.
        OriginIndexStore(MDBX_env* env,
                         const std::string& dbi_name = "_mdbxc_origins")
            : m_env(env), m_dbi_name(dbi_name), m_dbi(0), m_open(false) {}

        /// \brief Opens or creates the DBI inside the supplied transaction.
        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "OriginIndexStore::open");
            if (m_open) return;
            int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE, &m_dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0), &m_dbi);
            }
            check_mdbx(rc, "Failed to open OriginIndexStore DBI");
            m_open = true;
        }

        /// \brief Opens the DBI only if it already exists.
        /// \return \c true when opened, \c false when the DBI is absent.
        bool open_existing(MDBX_txn* txn) {
            txn = checked_txn(txn, "OriginIndexStore::open_existing");
            if (m_open) return true;
            const int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                         static_cast<MDBX_db_flags_t>(0), &m_dbi);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to open existing OriginIndexStore DBI");
            m_open = true;
            return true;
        }

        bool is_open() const { return m_open; }
        MDBX_dbi handle() const { return m_dbi; }

        /// \brief Resets the open flag so the next \c open() reopens the DBI.
        void reset_open() { m_open = false; }

        /// \brief Throws when the DBI has not been opened yet.
        void ensure_open() const {
            if (!m_open) {
                throw std::logic_error("OriginIndexStore is not open");
            }
        }

        /// \brief Returns true when the index contains no origins.
        bool empty(MDBX_txn* txn) const {
            txn = checked_txn(txn, "OriginIndexStore::empty");
            ensure_open();
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &raw),
                       "OriginIndexStore empty cursor open failed");
            MDBX_val k, v;
            const int rc = mdbx_cursor_get(raw, &k, &v, MDBX_FIRST);
            mdbx_cursor_close(raw);
            if (rc == MDBX_NOTFOUND) return true;
            check_mdbx(rc, "OriginIndexStore empty cursor get failed");
            return false;
        }

        /// \brief Removes all indexed origins.
        /// \param txn Active transaction.
        /// \return Number of removed origin entries.
        /// \pre Transaction must be writable.
        std::size_t clear(MDBX_txn* txn) {
            txn = checked_txn(txn, "OriginIndexStore::clear");
            ensure_open();
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &raw),
                       "OriginIndexStore clear cursor open failed");
            std::size_t removed = 0;
            try {
                MDBX_val k, v;
                int rc = mdbx_cursor_get(raw, &k, &v, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    check_mdbx(mdbx_cursor_del(raw, MDBX_CURRENT),
                               "OriginIndexStore clear cursor_del failed");
                    ++removed;
                    rc = mdbx_cursor_get(raw, &k, &v, MDBX_NEXT);
                }
                if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "OriginIndexStore clear cursor walk failed");
                }
            } catch (...) {
                mdbx_cursor_close(raw);
                throw;
            }
            mdbx_cursor_close(raw);
            return removed;
        }

        /// \brief Records \p origin with at least \p seq as its known tail.
        void note_origin(MDBX_txn* txn, const NodeId& origin, std::uint64_t seq) {
            txn = checked_txn(txn, "OriginIndexStore::note_origin");
            ensure_open();
            std::uint64_t current = 0;
            if (last_seq(txn, origin, current) && current >= seq) {
                return;
            }
            std::uint8_t value[8];
            detail::write_u64_le(seq, value);
            MDBX_val k = { const_cast<std::uint8_t*>(origin.data()), origin.size() };
            MDBX_val v = { value, sizeof(value) };
            check_mdbx(mdbx_put(txn, m_dbi, &k, &v, MDBX_UPSERT),
                       "OriginIndexStore note_origin failed");
        }

        /// \brief Reads the max known changelog seq for \p origin.
        /// \return true when the origin exists.
        bool last_seq(MDBX_txn* txn, const NodeId& origin,
                      std::uint64_t& out) const {
            txn = checked_txn(txn, "OriginIndexStore::last_seq");
            ensure_open();
            MDBX_val k = { const_cast<std::uint8_t*>(origin.data()), origin.size() };
            MDBX_val v;
            const int rc = mdbx_get(txn, m_dbi, &k, &v);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "OriginIndexStore last_seq failed");
            if (v.iov_len != 8) {
                throw std::runtime_error("OriginIndexStore value has invalid size");
            }
            out = detail::read_u64_le(static_cast<const std::uint8_t*>(v.iov_base));
            return true;
        }

        /// \brief Returns all indexed origins with their tails in DB key order.
        std::vector<OriginTail> origin_tails(MDBX_txn* txn) const {
            txn = checked_txn(txn, "OriginIndexStore::origin_tails");
            ensure_open();
            std::vector<OriginTail> out;
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &raw),
                       "OriginIndexStore origin_tails cursor open failed");
            try {
                MDBX_val k, v;
                int rc = mdbx_cursor_get(raw, &k, &v, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    if (k.iov_len != 16) {
                        throw std::runtime_error("OriginIndexStore key has invalid size");
                    }
                    if (v.iov_len != 8) {
                        throw std::runtime_error("OriginIndexStore value has invalid size");
                    }
                    OriginTail tail;
                    tail.origin = NodeId();
                    std::memcpy(tail.origin.data(), k.iov_base, 16);
                    tail.last_seq =
                        detail::read_u64_le(static_cast<const std::uint8_t*>(v.iov_base));
                    out.push_back(tail);
                    rc = mdbx_cursor_get(raw, &k, &v, MDBX_NEXT);
                }
                if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "OriginIndexStore origin_tails cursor walk failed");
                }
            } catch (...) {
                mdbx_cursor_close(raw);
                throw;
            }
            mdbx_cursor_close(raw);
            return out;
        }

        /// \brief Returns all indexed origins in DB key order.
        std::vector<NodeId> origins(MDBX_txn* txn) const {
            const std::vector<OriginTail> tails = origin_tails(txn);
            std::vector<NodeId> out;
            out.reserve(tails.size());
            for (std::vector<OriginTail>::const_iterator it = tails.begin();
                 it != tails.end(); ++it) {
                out.push_back(it->origin);
            }
            return out;
        }

    private:
        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        MDBX_env*     m_env;
        std::string   m_dbi_name;
        MDBX_dbi      m_dbi;
        bool          m_open;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_STORES_ORIGIN_INDEX_STORE_HPP_INCLUDED
