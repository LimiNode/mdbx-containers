#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SYNCNODESESSION_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SYNCNODESESSION_HPP_INCLUDED

/// \file engine/SyncNodeSession.hpp
/// \brief RAII helper for common sync node application wiring.

#if MDBXC_SYNC_ENABLED

#include "SyncWorkerGuard.hpp"

#include <cstdint>
#include <memory>
#include <stdexcept>

namespace mdbxc {
namespace sync {

    /// \brief Optional pieces owned by a \c SyncNodeSession.
    /// \details The helper is intentionally transport-neutral: callers still
    /// construct \c SyncEngine, \c ISyncPeer, and \c SyncWorker explicitly.
    struct SyncNodeSessionOptions {
        /// \brief Connection whose writes should be captured for the session.
        std::shared_ptr<Connection> capture_connection;

        /// \brief Non-owning capture sink attached for the session lifetime.
        ISyncCaptureSink* capture_sink = nullptr;

        /// \brief Connection used to register \c apply_observer.
        std::shared_ptr<Connection> apply_observer_connection;

        /// \brief Non-owning observer registered for the session lifetime.
        ISyncApplyObserver* apply_observer = nullptr;
    };

    /// \brief Owns a common application-facing sync session.
    /// \details Starts one existing \c SyncWorker, optionally attaches a
    /// capture sink, and optionally registers a remote-apply observer. The
    /// helper does not own the worker, peer, engine, sink, or observer objects;
    /// those objects must outlive the session. Destruction stops the worker
    /// first, then removes the observer registration, then detaches capture.
    class SyncNodeSession {
    public:
        /// \brief Creates a session and starts \p worker immediately.
        /// \throws std::invalid_argument when only one side of an optional
        /// capture or observer pair is provided.
        explicit SyncNodeSession(SyncWorker& worker,
                                 const SyncNodeSessionOptions& options)
            : m_worker_guard(),
              m_capture_scope(),
              m_apply_observer_connection(options.apply_observer_connection),
              m_apply_observer_token(0),
              m_apply_observer_registered(false) {
            validate_options(options);
            try {
                if (options.capture_sink != nullptr) {
                    m_capture_scope.reset(new SyncCaptureScope(
                        options.capture_connection, options.capture_sink));
                }
                if (options.apply_observer != nullptr) {
                    m_apply_observer_token =
                        options.apply_observer_connection
                            ->add_sync_apply_observer(
                                options.apply_observer);
                    m_apply_observer_registered = true;
                }
                m_worker_guard.reset(new SyncWorkerGuard(worker));
            } catch (...) {
                cleanup_noexcept();
                throw;
            }
        }

        ~SyncNodeSession() {
            cleanup_noexcept();
        }

        SyncNodeSession(const SyncNodeSession&) = delete;
        SyncNodeSession& operator=(const SyncNodeSession&) = delete;
        SyncNodeSession(SyncNodeSession&&) = delete;
        SyncNodeSession& operator=(SyncNodeSession&&) = delete;

        /// \brief Stops the worker and releases observer/capture hooks.
        /// \throws std::logic_error if called from the worker thread.
        void stop() {
            if (m_worker_guard) {
                m_worker_guard->stop();
                m_worker_guard.reset();
            }
            remove_apply_observer();
            detach_capture();
        }

        /// \brief Returns whether the worker session is still active.
        bool active() const {
            return m_worker_guard && m_worker_guard->active();
        }

        /// \brief Returns whether a capture scope is still attached.
        bool capture_active() const {
            return m_capture_scope && m_capture_scope->active();
        }

        /// \brief Returns whether an apply observer is still registered.
        bool apply_observer_registered() const {
            return m_apply_observer_registered;
        }

        /// \brief Returns the observer token, or zero when none was registered.
        std::uint64_t apply_observer_token() const {
            return m_apply_observer_token;
        }

        /// \brief Detaches the session capture scope if one is active.
        void detach_capture() {
            if (m_capture_scope) {
                m_capture_scope->detach();
                m_capture_scope.reset();
            }
        }

        /// \brief Removes the session apply observer if one is registered.
        void remove_apply_observer() {
            if (m_apply_observer_registered &&
                m_apply_observer_connection) {
                m_apply_observer_connection->remove_sync_apply_observer(
                    m_apply_observer_token);
            }
            m_apply_observer_registered = false;
            m_apply_observer_token = 0;
        }

    private:
        static void validate_options(const SyncNodeSessionOptions& options) {
            if (static_cast<bool>(options.capture_connection) !=
                (options.capture_sink != nullptr)) {
                throw std::invalid_argument(
                    "SyncNodeSession capture connection and sink must be "
                    "provided together");
            }
            if (static_cast<bool>(options.apply_observer_connection) !=
                (options.apply_observer != nullptr)) {
                throw std::invalid_argument(
                    "SyncNodeSession observer connection and observer must be "
                    "provided together");
            }
        }

        void cleanup_noexcept() noexcept {
            try {
                if (m_worker_guard) {
                    m_worker_guard->stop();
                    m_worker_guard.reset();
                }
            } catch (...) {
                try {
                    if (m_worker_guard) {
                        m_worker_guard->worker().request_stop();
                    }
                } catch (...) {
                }
            }
            try {
                remove_apply_observer();
            } catch (...) {
            }
            try {
                detach_capture();
            } catch (...) {
            }
        }

        std::unique_ptr<SyncWorkerGuard> m_worker_guard;
        std::unique_ptr<SyncCaptureScope> m_capture_scope;
        std::shared_ptr<Connection> m_apply_observer_connection;
        std::uint64_t m_apply_observer_token;
        bool m_apply_observer_registered;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBXC_SYNC_ENABLED

#endif // MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SYNCNODESESSION_HPP_INCLUDED
