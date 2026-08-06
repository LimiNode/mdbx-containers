#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_CAPTURE_SYNCCAPTURESCOPE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_CAPTURE_SYNCCAPTURESCOPE_HPP_INCLUDED

/// \file capture/SyncCaptureScope.hpp
/// \brief RAII helper for temporarily attaching an \c ISyncCaptureSink.

#include <mdbx_containers/sync/sync_module.hpp>

#if MDBXC_SYNC_ENABLED

#include <mdbx_containers/common.hpp>
#include <mdbx_containers/sync/capture/ISyncCaptureSink.hpp>

#include <cstdint>
#include <exception>
#include <memory>
#include <stdexcept>

namespace mdbxc {
namespace sync {

    /// \brief Temporarily attaches a sync capture sink to a \c Connection.
    /// \details The scope stores the previously attached sink, attaches
    /// \p sink, and restores the previous sink on destruction or \c detach().
    /// Nested scopes must be detached or destroyed in strict LIFO order. An
    /// explicit out-of-order \c detach() throws \c std::logic_error. The sink
    /// pointer is non-owning and must outlive the scope. This is a lifecycle
    /// helper; do not create or destroy it concurrently with table operations
    /// or active transactions on the same connection.
    class SyncCaptureScope {
    public:
        SyncCaptureScope(const std::shared_ptr<Connection>& connection,
                         ISyncCaptureSink& sink)
            : m_connection(connection),
              m_sink(nullptr),
              m_token(0),
              m_previous(nullptr),
              m_previous_token(0),
              m_active(false) {
            attach(&sink);
        }

        SyncCaptureScope(const std::shared_ptr<Connection>& connection,
                         ISyncCaptureSink* sink)
            : m_connection(connection),
              m_sink(nullptr),
              m_token(0),
              m_previous(nullptr),
              m_previous_token(0),
              m_active(false) {
            if (sink == nullptr) {
                throw std::invalid_argument("SyncCaptureScope sink is null");
            }
            attach(sink);
        }

        ~SyncCaptureScope() noexcept {
            if (!restore()) {
                std::terminate();
            }
        }

        SyncCaptureScope(const SyncCaptureScope&) = delete;
        SyncCaptureScope& operator=(const SyncCaptureScope&) = delete;

        /// \brief Restores the sink that was attached before this scope.
        /// \details Safe to call more than once. Throws \c std::logic_error if
        /// another active scope or a raw attach call currently owns the
        /// connection attachment.
        void detach() {
            if (!restore()) {
                throw std::logic_error(
                    "SyncCaptureScope detach called out of LIFO order");
            }
        }

        /// \brief Returns whether this scope still owns the attachment.
        bool active() const {
            return m_active;
        }

    private:
        bool restore() {
            if (!m_active) {
                return true;
            }
            if (!m_connection->restore_sync_capture_if_current(
                    m_sink, m_token, m_previous, m_previous_token)) {
                return false;
            }
            m_active = false;
            return true;
        }
        void attach(ISyncCaptureSink* sink) {
            if (!m_connection) {
                throw std::invalid_argument(
                    "SyncCaptureScope connection is null");
            }
            m_previous = m_connection->sync_capture();
            m_previous_token = m_connection->sync_capture_token();
            m_sink = sink;
            m_connection->attach_sync_capture(sink);
            m_token = m_connection->sync_capture_token();
            m_active = true;
        }

        std::shared_ptr<Connection> m_connection;
        ISyncCaptureSink* m_sink;
        std::uint64_t m_token;
        ISyncCaptureSink* m_previous;
        std::uint64_t m_previous_token;
        bool m_active;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBXC_SYNC_ENABLED

#endif // MDBX_CONTAINERS_HEADER_SYNC_CAPTURE_SYNCCAPTURESCOPE_HPP_INCLUDED
