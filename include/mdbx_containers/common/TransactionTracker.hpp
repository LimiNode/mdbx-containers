#pragma once
#ifndef MDBX_CONTAINERS_HEADER_COMMON_TRANSACTION_TRACKER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_COMMON_TRANSACTION_TRACKER_HPP_INCLUDED

/// \file TransactionTracker.hpp
/// \brief Tracks MDBX transactions per thread for reuse and cleanup.

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <thread>
#include <unordered_map>

#ifndef MDBXC_SYNC_ENABLED
#define MDBXC_SYNC_ENABLED 0
#endif

#if MDBXC_SYNC_ENABLED
#include <mdbx.h>
#endif

namespace mdbxc {

    /// \class TransactionTracker
    /// \ingroup mdbxc_core
    /// \brief Associates MDBX transactions with threads.
    ///
    /// Manages a map from thread IDs to MDBX transaction pointers,
    /// allowing reuse and cleanup of transactions for specific threads.
    ///
    /// \thread_safety Internally synchronized only for registry access. This
    /// class does not make `MDBX_txn*` handles safe to use from another thread.
    class TransactionTracker {
        friend class Transaction;
    protected:

        /// \brief Registers a transaction for a specific thread.
        /// \param txn Pointer to the MDBX transaction.
        void bind_txn(MDBX_txn* txn);

        /// \brief Registers a newly created MDBX transaction handle.
        void register_txn_handle();

        /// \brief Unregisters a closed MDBX transaction handle.
        void unregister_txn_handle();

        /// \brief Unregisters the expected transaction for the current thread.
        /// \param expected_txn Transaction pointer that must match the current
        ///        thread's registered transaction.
        void unbind_txn(MDBX_txn* expected_txn);

        /// \brief Retrieves the transaction associated with the current thread.
        /// \return Pointer to the MDBX transaction, or nullptr if not found.
        MDBX_txn* thread_txn() const;

        /// \brief Checks whether the current thread owns an active transaction.
        /// \return true if a transaction is registered for this thread.
        bool current_thread_has_txn() const;

        /// \brief Checks whether the current thread owns any transaction handle.
        /// \return true if this thread owns an MDBX transaction handle.
        bool current_thread_has_txn_handle() const;

        /// \brief Checks whether any MDBX transaction handle is still open.
        /// \return true if any transaction handle is still open.
        bool has_txn_handles() const;

        /// \brief Waits until no transaction handle is open.
        void wait_for_no_txn_handles() const;

        /// \brief Waits for transaction handles to close.
        /// \return true if there are no open transaction handles, false on timeout.
        template<class Rep, class Period>
        bool wait_for_no_txn_handles_for(
            const std::chrono::duration<Rep, Period>& timeout) const {
            std::unique_lock<std::mutex> lock(m_mutex);
            return m_txn_cv.wait_for(lock, timeout, [this]() {
                return m_open_txn_handles == 0;
            });
        }
        
        virtual ~TransactionTracker() = default;

#       if MDBXC_SYNC_ENABLED
        /// \brief Pre-commit hook for sync capture.
        /// \details Called by \c Transaction::commit before \c mdbx_txn_commit
        /// for write transactions. Default is no-op; \c Connection overrides.
        virtual void on_pre_commit(MDBX_txn* txn) noexcept(false) {
            (void)txn;
        }

        /// \brief Discard hook for sync capture.
        /// \details Called by \c Transaction::release / \c rollback paths
        /// before a write transaction is aborted, and after a terminal native
        /// commit error has already terminated it. \p txn is an opaque identity
        /// token only; implementations must not dereference it or pass it to
        /// MDBX. Default is no-op; \c Connection forwards to the attached
        /// \c ISyncCaptureSink.
        virtual void on_discard(MDBX_txn* txn) noexcept {
            (void)txn;
        }
#       endif

    private:
        mutable std::mutex m_mutex;  ///< Protects access to m_thread_txns.
        mutable std::condition_variable m_txn_cv; ///< Notifies waiters when transactions end.
        std::unordered_map<std::thread::id, MDBX_txn*> m_thread_txns; ///< Map of thread IDs to transaction pointers.
        std::unordered_map<std::thread::id, std::size_t> m_thread_txn_handle_counts; ///< Open transaction handles per thread.
        std::size_t m_open_txn_handles = 0; ///< Total number of open transaction handles.
    };

} // namespace mdbxc

#ifdef MDBX_CONTAINERS_HEADER_ONLY
#include "TransactionTracker.ipp"
#endif

#endif // MDBX_CONTAINERS_HEADER_COMMON_TRANSACTION_TRACKER_HPP_INCLUDED
