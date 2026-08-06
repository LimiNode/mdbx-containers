#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_ISYNCPEER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_ISYNCPEER_HPP_INCLUDED

/// \file transport/ISyncPeer.hpp
/// \brief Abstract transport-level peer used by \c SyncEngine.

#include <cstdint>

#include <mdbx_containers/sync/protocol.hpp>

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
    class ISyncPeer {
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
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_ISYNCPEER_HPP_INCLUDED
