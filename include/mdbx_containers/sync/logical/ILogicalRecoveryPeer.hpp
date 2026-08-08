#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ILOGICAL_RECOVERY_PEER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ILOGICAL_RECOVERY_PEER_HPP_INCLUDED

/// \file logical/ILogicalRecoveryPeer.hpp
/// \brief Optional peer contract for logical-aware fresh-replica recovery.

#include <stdexcept>

#include "logical_recovery.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Capability-gated peer for the separate logical recovery route.
    class ILogicalRecoveryPeer {
    public:
        virtual ~ILogicalRecoveryPeer() {}

        virtual bool supports_logical_recovery() const { return false; }

        virtual LogicalRecoveryResponse logical_recovery(
                const LogicalRecoveryRequest&) {
            throw std::logic_error(
                "Sync peer does not support logical recovery");
        }
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ILOGICAL_RECOVERY_PEER_HPP_INCLUDED
