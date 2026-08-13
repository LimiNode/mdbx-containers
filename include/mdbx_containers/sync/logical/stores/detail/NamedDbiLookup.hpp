#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_DETAIL_NAMED_DBI_LOOKUP_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_DETAIL_NAMED_DBI_LOOKUP_HPP_INCLUDED

/// \file logical/stores/detail/NamedDbiLookup.hpp
/// \brief Constant-size named-DBI existence lookup.

#include <string>

#include <mdbx.h>

namespace mdbxc {
namespace sync {
namespace detail {

    inline bool named_dbi_exists(MDBX_txn* txn, const std::string& name) {
        MDBX_dbi main_dbi = 0;
        check_mdbx(
            mdbx_dbi_open(
                txn,
                static_cast<const char*>(MDBX_CHK_MAIN),
                static_cast<MDBX_db_flags_t>(0),
                &main_dbi),
            "Failed to open main DBI while checking named DBI");
        MDBX_val key = {
            const_cast<char*>(name.data()),
            name.size()
        };
        MDBX_val value;
        const int rc = mdbx_get(txn, main_dbi, &key, &value);
        if (rc == MDBX_NOTFOUND) return false;
        check_mdbx(rc, "Failed to inspect named DBI");
        return true;
    }

} // namespace detail
} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_DETAIL_NAMED_DBI_LOOKUP_HPP_INCLUDED
