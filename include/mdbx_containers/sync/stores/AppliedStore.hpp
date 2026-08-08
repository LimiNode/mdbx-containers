#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_STORES_APPLIED_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_STORES_APPLIED_STORE_HPP_INCLUDED

/// \file AppliedStore.hpp
/// \brief Tracks the last contiguous applied \c seq per origin \c NodeId.

#include <cstdint>
#include <stdexcept>
#include <string>

#include <mdbx.h>


namespace mdbxc {
namespace sync {

    /// \brief Records the last contiguous applied \c seq per origin.
    /// \details Key = 16 bytes of \c NodeId. Value = 8 bytes little-endian
    /// \c seq. The store assumes the caller only writes contiguous updates
    /// (i.e. applying batch \c seq means \c last_seq = \c seq, never gaps).
    class AppliedStore {
    public:
        AppliedStore(MDBX_env* env,
                     const std::string& dbi_name = "_mdbxc_applied")
            : m_env(env), m_dbi_name(dbi_name), m_dbi(0), m_open(false) {}

        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "AppliedStore::open");
            if (m_open) return;
            check_mdbx(
                mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE, &m_dbi),
                "Failed to open AppliedStore DBI"
            );
            m_open = true;
        }

        bool is_open() const { return m_open; }
        MDBX_dbi handle() const { return m_dbi; }

        /// \brief Throws when the DBI has not been opened yet.
        void ensure_open() const {
            if (!m_open) {
                throw std::logic_error("AppliedStore is not open");
            }
        }

        /// \brief Returns the last contiguous applied \c seq for \p origin.
        /// \details Returns 0 when no record exists.
        std::uint64_t last_applied_seq(MDBX_txn* txn, const NodeId& origin) const {
            txn = checked_txn(txn, "AppliedStore::last_applied_seq");
            ensure_open();
            MDBX_val k = { const_cast<std::uint8_t*>(origin.data()), 16 };
            MDBX_val v;
            const int rc = mdbx_get(txn, m_dbi, &k, &v);
            if (rc == MDBX_NOTFOUND) return 0;
            check_mdbx(rc, "AppliedStore read failed");
            if (v.iov_len != 8) return 0;
            return detail::read_u64_le(static_cast<const std::uint8_t*>(v.iov_base));
        }

        /// \brief Records \p seq as the new last contiguous applied value for
        /// \p origin.
        void set_last_applied_seq(MDBX_txn* txn, const NodeId& origin,
                                  std::uint64_t seq) {
            txn = checked_txn(txn, "AppliedStore::set_last_applied_seq");
            ensure_open();
            std::uint8_t buf[8];
            detail::write_u64_le(seq, buf);
            MDBX_val k = { const_cast<std::uint8_t*>(origin.data()), 16 };
            MDBX_val v = { buf, 8 };
            check_mdbx(
                mdbx_put(txn, m_dbi, &k, &v, MDBX_UPSERT),
                "AppliedStore write failed"
            );
        }

        /// \brief Removes the record for \p origin if present.
        /// \return true when a record was removed.
        bool clear(MDBX_txn* txn, const NodeId& origin) {
            txn = checked_txn(txn, "AppliedStore::clear");
            ensure_open();
            MDBX_val k = { const_cast<std::uint8_t*>(origin.data()), 16 };
            const int rc = mdbx_del(txn, m_dbi, &k, nullptr);
            if (rc == MDBX_SUCCESS) return true;
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "AppliedStore clear failed");
            return false;
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

#endif // MDBX_CONTAINERS_HEADER_SYNC_STORES_APPLIED_STORE_HPP_INCLUDED
