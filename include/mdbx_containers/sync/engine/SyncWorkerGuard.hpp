#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SYNCWORKERGUARD_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SYNCWORKERGUARD_HPP_INCLUDED

/// \file engine/SyncWorkerGuard.hpp
/// \brief RAII helper for a background \c SyncWorker session.

#include "SyncWorker.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Starts a \c SyncWorker and stops it when the guard is destroyed.
    /// \details This helper does not own the worker. It only owns one
    /// background run session and is intended for examples and application
    /// scopes where forgetting a matching \c stop() would be easy.
    class SyncWorkerGuard {
    public:
        /// \brief Starts \p worker immediately.
        explicit SyncWorkerGuard(SyncWorker& worker)
            : m_worker(&worker), m_active(false) {
            m_worker->start();
            m_active = true;
        }

        /// \brief Requests stop and joins the worker if the guard is active.
        ~SyncWorkerGuard() {
            stop_noexcept();
        }

        SyncWorkerGuard(const SyncWorkerGuard&) = delete;
        SyncWorkerGuard& operator=(const SyncWorkerGuard&) = delete;
        SyncWorkerGuard(SyncWorkerGuard&&) = delete;
        SyncWorkerGuard& operator=(SyncWorkerGuard&&) = delete;

        /// \brief Explicitly stops the guarded background session.
        /// \throws std::logic_error if called from the worker thread.
        void stop() {
            if (m_active && m_worker != nullptr) {
                m_worker->stop();
                m_active = false;
            }
        }

        /// \brief Returns whether this guard still owns a running session.
        bool active() const noexcept {
            return m_active;
        }

        /// \brief Returns the guarded worker.
        SyncWorker& worker() const {
            return *m_worker;
        }

    private:
        void stop_noexcept() noexcept {
            if (!m_active || m_worker == nullptr) {
                return;
            }
            try {
                m_worker->stop();
            } catch (...) {
                try {
                    m_worker->request_stop();
                } catch (...) {
                }
            }
            m_active = false;
        }

        SyncWorker* m_worker;
        bool m_active;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SYNCWORKERGUARD_HPP_INCLUDED
