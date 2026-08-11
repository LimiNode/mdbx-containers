#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_CORE_CONFLICTPOLICY_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_CORE_CONFLICTPOLICY_HPP_INCLUDED

/// \file core/ConflictPolicy.hpp
/// \brief How a replica resolves conflicting updates to the same logical key.

namespace mdbxc {
namespace sync {

    /// \brief Conflict resolution policy applied during replication.
    enum class ConflictPolicy {
        /// \brief Reject the incoming change; surface an error to the caller.
        Reject,
        /// \brief Applies the greatest application-provided source version.
        /// \details Narrow v1 support applies to DBIs registered by
        /// \c VersionedKeyValueTable. Those DBIs accept only revisioned raw
        /// \c Put and \c Delete operations. Ordinary DBIs keep raw sync on
        /// the same engine. Source version bytes are compared lexicographically,
        /// then \c NodeId breaks an equal-version tie deterministically.
        /// It is not wall-clock LWW.
        LastWriterWins,
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_CORE_CONFLICTPOLICY_HPP_INCLUDED
