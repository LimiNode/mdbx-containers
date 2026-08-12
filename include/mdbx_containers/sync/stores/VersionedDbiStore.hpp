#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_STORES_VERSIONED_DBI_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_STORES_VERSIONED_DBI_STORE_HPP_INCLUDED

/// \file VersionedDbiStore.hpp
/// \brief Durable registry of DBIs using source-version-wins replication.

#include <stdexcept>
#include <string>

namespace mdbxc {
namespace sync {

    /// \brief Thin wrapper around \c _mdbxc_versioned_dbis.
    /// \details Each key is one user DBI name and has an empty value. A key
    /// selects the narrow \c VersionedKeyValueTable replication contract.
    class VersionedDbiStore {
    public:
        /// \brief Binds the store to an MDBX environment.
        /// \param env Live MDBX environment.
        explicit VersionedDbiStore(MDBX_env* env)
            : m_env(env), m_dbi(0), m_open(false) {}

        /// \brief Registers a user DBI.
        /// \param txn Active writable MDBX transaction.
        /// \param dbi_name User DBI name.
        /// \return True when this call added the marker, false when present.
        bool register_dbi(MDBX_txn* txn, const std::string& dbi_name) {
            txn = checked_txn(txn, "VersionedDbiStore::register_dbi");
            validate_name(dbi_name);
            open_for_write(txn);
            MDBX_val key = { const_cast<char*>(dbi_name.data()), dbi_name.size() };
            MDBX_val value = { nullptr, 0u };
            const int rc = mdbx_put(txn, m_dbi, &key, &value, MDBX_NOOVERWRITE);
            if (rc == MDBX_SUCCESS) return true;
            if (rc == MDBX_KEYEXIST) return false;
            check_mdbx(rc, "VersionedDbiStore register failed");
            return false;
        }

        /// \brief Tests whether a user DBI is registered.
        /// \param txn Active MDBX transaction.
        /// \param dbi_name User DBI name.
        /// \return True only for a registered source-version-wins DBI.
        bool contains(MDBX_txn* txn, const std::string& dbi_name) {
            txn = checked_txn(txn, "VersionedDbiStore::contains");
            if (dbi_name.empty() || !open_existing(txn)) return false;
            MDBX_val key = { const_cast<char*>(dbi_name.data()), dbi_name.size() };
            MDBX_val value;
            const int rc = mdbx_get(txn, m_dbi, &key, &value);
            if (rc == MDBX_SUCCESS) return true;
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "VersionedDbiStore lookup failed");
            return false;
        }

    private:
        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        static void validate_name(const std::string& dbi_name) {
            if (dbi_name.empty() || is_reserved_dbi_name(dbi_name)) {
                throw std::invalid_argument(
                    "VersionedDbiStore requires a non-reserved user DBI name");
            }
        }

        void open_for_write(MDBX_txn* txn) {
            if (m_open) return;
            check_mdbx(mdbx_dbi_open(txn, "_mdbxc_versioned_dbis", MDBX_CREATE,
                                     &m_dbi),
                       "Failed to open VersionedDbiStore DBI");
            m_open = true;
        }

        bool open_existing(MDBX_txn* txn) {
            if (m_open) return true;
            const int rc = mdbx_dbi_open(txn, "_mdbxc_versioned_dbis",
                                         static_cast<MDBX_db_flags_t>(0), &m_dbi);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to open VersionedDbiStore DBI");
            m_open = true;
            return true;
        }

        MDBX_env* m_env;
        MDBX_dbi  m_dbi;
        bool      m_open;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_STORES_VERSIONED_DBI_STORE_HPP_INCLUDED
