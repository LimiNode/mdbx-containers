#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ENGINE_ISYNCPEER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ENGINE_ISYNCPEER_HPP_INCLUDED

/// \file engine/ISyncPeer.hpp
/// \brief Abstract peer used by \c SyncWorker and \c SyncEngine clients.

#include <cstdint>
#include <stdexcept>

namespace mdbxc {
namespace sync {

    /// \brief Adapter-level retry hint for transport failures.
    /// \details \c available distinguishes a peer that did not classify the
    /// last failure from a peer that explicitly classified it as permanent.
    /// \c retry_after_seconds is present only when a transport supplied a
    /// supported relative retry delay such as HTTP
    /// \c Retry-After: <delta-seconds>. Absolute HTTP-date values are
    /// intentionally left to concrete bindings that own clock policy.
    struct SyncTransportRetryHint {
        bool available = false;
        bool retryable = false;
        bool has_retry_after = false;
        std::uint64_t retry_after_seconds = 0;
    };

    /// \brief Abstract peer that exchanges pull and push requests.
    /// \details Concrete implementations (in-process, HTTP, WebSocket) live
    /// outside the core sync layer and depend on optional transport headers.
    /// Transport operations receive cooperative cancellation tokens through
    /// \c PullRequest::cancel_token and \c PushRequest::cancel_token. Blocking
    /// implementations may also override \c request_cancel() to interrupt
    /// socket waits or other transport-owned blocking primitives. Overrides
    /// must allow \c request_cancel() to be called concurrently with
    /// \c pull() / \c push() and tolerate calls that race with operation
    /// startup or completion.
    class ISyncPeer : public ILogicalDeliveryPeer {
    public:
        virtual ~ISyncPeer() {}

        /// \brief Sends a pull request and returns the response.
        virtual PullResponse pull(const PullRequest& request) = 0;

        /// \brief Sends a push request and returns the response.
        virtual PushResponse push(const PushRequest& request) = 0;

        /// \brief Requests cancellation of in-flight transport operations.
        /// \details Best-effort hook for blocking transports. The default
        /// implementation is a no-op, so token-only and non-interruptible
        /// peers remain valid. Overrides should return quickly and should not
        /// throw.
        virtual void request_cancel() {}

        /// \brief Returns retry advice for the most recent transport failure.
        /// \details Peers that can classify adapter failures may override this
        /// method after a failed \c pull() or \c push(). The default returns an
        /// unavailable hint. Successful operations should normally clear any
        /// previous retry advice.
        /// \return Retry hint for the last observed transport failure.
        virtual SyncTransportRetryHint last_retry_hint() const {
            return SyncTransportRetryHint();
        }

        /// \brief Returns whether this peer can exchange ordered logical frames.
        /// \details Raw-only peers preserve the v0.1 contract by returning
        /// \c false. Callers must opt in before treating logical outbox state
        /// as a transport responsibility.
        virtual bool supports_logical_delivery() const {
            return false;
        }

        LogicalDeliveryHello logical_delivery_hello() override {
            throw std::logic_error(
                "Sync peer does not support logical delivery");
        }

        LogicalDeliveryAcknowledgement deliver_ordered_logical_delivery(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr) override {
            (void)bounds;
            LogicalDeliveryAcknowledgement acknowledgement;
            acknowledgement.destination_db_uuid = envelope.destination_db_uuid;
            acknowledgement.origin_node_id = envelope.origin_node_id;
            acknowledgement.ok = false;
            acknowledgement.error =
                "Sync peer does not support logical delivery";
            return acknowledgement;
        }
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ENGINE_ISYNCPEER_HPP_INCLUDED
