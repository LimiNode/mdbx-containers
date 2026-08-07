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
        /// \brief Reserved for future timestamp/version based resolution.
        /// \details v0.1 \c SyncEngine rejects this policy because raw batch
        /// apply does not yet have a reliable logical-key conflict authority.
        LastWriterWins,
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_CORE_CONFLICTPOLICY_HPP_INCLUDED
