#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_CORE_CANCELLATION_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_CORE_CANCELLATION_HPP_INCLUDED

/// \file core/cancellation.hpp
/// \brief Cooperative cancellation primitives for sync transports.

namespace mdbxc {
namespace sync {

    namespace detail {
        /// \brief Shared cancellation flag observed by sources and tokens.
        /// \details Kept behind \c shared_ptr because cancellation handles are
        /// copyable while \c std::atomic<bool> is not copyable by value.
        struct CancellationState {
            CancellationState() : requested(false) {}

            std::atomic<bool> requested;
        };
    } // namespace detail

    /// \brief Read-only view of a cooperative cancellation request.
    /// \details Tokens are cheap to copy and can be stored by transport code
    /// for the duration of one operation. A default-constructed token is not
    /// cancellable and never reports cancellation.
    class CancellationToken {
    public:
        CancellationToken() {}

        /// \brief Returns whether this token is backed by a source.
        bool can_be_cancelled() const {
            return static_cast<bool>(m_state);
        }

        /// \brief Returns whether cancellation has been requested.
        bool is_cancellation_requested() const {
            return m_state &&
                m_state->requested.load(std::memory_order_acquire);
        }

    private:
        friend class CancellationSource;

        explicit CancellationToken(
                const std::shared_ptr<detail::CancellationState>& state)
            : m_state(state) {}

        std::shared_ptr<detail::CancellationState> m_state;
    };

    /// \brief Owner side of a cooperative cancellation token.
    /// \details Sources are cheap to copy and copied sources share one
    /// cancellation state. Calling \c request_cancel() on any copy is
    /// thread-safe and wakes only transports that actively observe the paired
    /// \c CancellationToken. It does not interrupt arbitrary blocking system
    /// calls by itself; transport adapters should combine it with socket
    /// shutdown, deadlines, or their native cancellation primitive when needed.
    class CancellationSource {
    public:
        CancellationSource()
            : m_state(new detail::CancellationState()) {}

        /// \brief Returns a token associated with this source.
        CancellationToken token() const {
            return CancellationToken(m_state);
        }

        /// \brief Requests cancellation for all tokens from this source.
        void request_cancel() const {
            m_state->requested.store(true, std::memory_order_release);
        }

        /// \brief Returns whether cancellation has been requested.
        bool is_cancellation_requested() const {
            return m_state->requested.load(std::memory_order_acquire);
        }

    private:
        std::shared_ptr<detail::CancellationState> m_state;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_CORE_CANCELLATION_HPP_INCLUDED
