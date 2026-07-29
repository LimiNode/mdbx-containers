#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_I_LOGICAL_DELIVERY_PEER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_I_LOGICAL_DELIVERY_PEER_HPP_INCLUDED

/// \file ILogicalDeliveryPeer.hpp
/// \brief Capability-gated peer interface for ordered logical delivery.

#include <cstddef>
#include <string>

#include "LogicalDeliveryProtocol.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Optional transport boundary for ordered logical delivery.
    class ILogicalDeliveryPeer {
    public:
        virtual ~ILogicalDeliveryPeer() {}

        virtual LogicalDeliveryHello logical_delivery_hello() = 0;

        virtual LogicalDeliveryAcknowledgement deliver_ordered_logical_delivery(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr) = 0;
    };

    /// \brief Outcome of sending pending deliveries to one peer.
    struct LogicalDeliveryDispatchResult {
        bool ok = true;
        bool retryable = false;
        std::size_t delivered = 0u;
        std::uint64_t acknowledged_through = 0u;
        std::string error;

        static LogicalDeliveryDispatchResult failure(
                const std::string& message,
                bool is_retryable = false) {
            LogicalDeliveryDispatchResult out;
            out.ok = false;
            out.retryable = is_retryable;
            out.error = message;
            return out;
        }
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_I_LOGICAL_DELIVERY_PEER_HPP_INCLUDED
