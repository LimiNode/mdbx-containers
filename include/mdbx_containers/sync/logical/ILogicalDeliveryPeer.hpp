#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_I_LOGICAL_DELIVERY_PEER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_I_LOGICAL_DELIVERY_PEER_HPP_INCLUDED

/// \file logical/ILogicalDeliveryPeer.hpp
/// \brief Capability-gated peer interface for ordered logical delivery.

#include <cstddef>
#include <string>

#include "LogicalDeliveryProtocol.hpp"

namespace mdbxc {
namespace sync {

    class CancellationToken;

    /// \brief Optional transport boundary for ordered logical delivery.
    class ILogicalDeliveryPeer {
    public:
        virtual ~ILogicalDeliveryPeer() {}

        virtual LogicalDeliveryHello logical_delivery_hello() = 0;

        virtual LogicalDeliveryAcknowledgement deliver_ordered_logical_delivery(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr) = 0;

        /// \brief Retrieves peer capabilities with optional cancellation.
        /// \details The default preserves existing logical peers that do not
        /// own an interruptible transport call.
        virtual LogicalDeliveryHello logical_delivery_hello_with_cancel(
                const CancellationToken* cancel_token = nullptr) {
            (void)cancel_token;
            return logical_delivery_hello();
        }

        /// \brief Delivers an envelope with optional cancellation.
        /// \details The default preserves the original envelope-only peer
        /// contract for existing source implementations.
        virtual LogicalDeliveryAcknowledgement
        deliver_ordered_logical_delivery_with_cancel(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr,
                const CancellationToken* cancel_token = nullptr) {
            (void)cancel_token;
            return deliver_ordered_logical_delivery(envelope, bounds);
        }

        /// \brief Delivers one request with sender feature context.
        /// \details The default preserves existing peers that implemented the
        /// earlier envelope-only virtual method. Such peers remain on the
        /// conservative acknowledgement path until they override this method.
        virtual LogicalDeliveryAcknowledgement deliver_ordered_logical_request(
                const LogicalDeliveryRequest& request,
                const CodecBounds* bounds = nullptr) {
            return deliver_ordered_logical_delivery(request.envelope, bounds);
        }

        /// \brief Delivers a request with optional cancellation.
        virtual LogicalDeliveryAcknowledgement
        deliver_ordered_logical_request_with_cancel(
                const LogicalDeliveryRequest& request,
                const CodecBounds* bounds = nullptr,
                const CancellationToken* cancel_token = nullptr) {
            (void)cancel_token;
            return deliver_ordered_logical_request(request, bounds);
        }
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

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_I_LOGICAL_DELIVERY_PEER_HPP_INCLUDED
