#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_DETAIL_CAPTURED_LOGICAL_TRANSACTION_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_DETAIL_CAPTURED_LOGICAL_TRANSACTION_HPP_INCLUDED

/// \file logical/adapters/detail/captured_logical_transaction.hpp
/// \brief Internal transaction lifecycle shared by typed logical capture sessions.

#include <cstddef>
#include <stdexcept>
#include <vector>

namespace mdbxc {
namespace sync {
namespace detail {

    /// \brief Owns the transaction and pending frame of one capture session.
    class CapturedLogicalTransaction {
    protected:
        explicit CapturedLogicalTransaction(Connection& connection)
            : m_txn(connection.transaction(TransactionMode::WRITABLE)),
              m_active(true) {}

        void commit_pending(std::vector<LogicalChange>& out) {
            ensure_active("Logical capture session is not active");
            const std::size_t old_size = out.size();
            out.insert(out.end(), m_pending.begin(), m_pending.end());
            try {
                m_txn.commit();
            } catch (...) {
                out.erase(out.begin() + static_cast<std::ptrdiff_t>(old_size),
                          out.end());
                rollback_and_deactivate();
                throw;
            }
            m_pending.clear();
            m_active = false;
        }

        LogicalDeliveryEnvelope commit_pending_to_outbox(
                ILogicalDeliveryOutbox& outbox,
                const DbId& destination,
                const NodeId& receiver,
                const CodecBounds* bounds = nullptr) {
            ensure_active("Logical capture session is not active");
            LogicalChangeFrame frame;
            frame.changes = m_pending;
            try {
                const LogicalDeliveryEnvelope envelope =
                    outbox.enqueue_logical_delivery(
                        m_txn.handle(), destination, receiver, frame, bounds);
                m_txn.commit();
                m_pending.clear();
                m_active = false;
                return envelope;
            } catch (...) {
                rollback_and_deactivate();
                throw;
            }
        }

        void rollback_and_deactivate() noexcept {
            try {
                m_pending.clear();
            } catch (...) {
            }
            try {
                m_txn.rollback();
            } catch (...) {
            }
            m_active = false;
        }

        void ensure_active(const char* message) const {
            if (!m_active) {
                throw std::logic_error(message);
            }
        }

        Transaction m_txn;
        std::vector<LogicalChange> m_pending;
        bool m_active;
    };

} // namespace detail
} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_DETAIL_CAPTURED_LOGICAL_TRANSACTION_HPP_INCLUDED
