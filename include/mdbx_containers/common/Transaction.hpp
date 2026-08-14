#pragma once
#ifndef MDBX_CONTAINERS_HEADER_COMMON_TRANSACTION_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_COMMON_TRANSACTION_HPP_INCLUDED

/// \file Transaction.hpp
/// \brief Declares the Transaction class, a wrapper for managing MDBX transactions.

#include "TransactionTracker.hpp"

namespace mdbxc {
    
    /// \enum TransactionMode
    /// \brief Specifies the access mode of a transaction.
    ///
    /// Defines whether the transaction is read-only or writable.
    enum class TransactionMode {
        READ_ONLY,  ///< Read-only transaction (no write operations allowed).
        WRITABLE    ///< Writable transaction (allows inserts, updates, deletes).
    };

    /// \class Transaction
    /// \ingroup mdbxc_core
    /// \brief Manages MDBX transactions with automatic cleanup and error handling.
    ///
    /// Supports both read-only and writable modes. Provides methods for beginning,
    /// committing, and rolling back transactions, with integration of MDBX-specific behavior.
    ///
    /// \thread_safety Not thread-safe. A transaction and its MDBX cursors belong
    /// to the thread that created or currently owns the guard. Do not use,
    /// commit, roll back, destroy, or move it for use by another thread.
    ///
    /// \note mdbx-containers does not enable `MDBX_NOSTICKYTHREADS`; follow the
    /// default MDBX transaction ownership rules.
    /// \see https://libmdbx.dqdkfa.ru/group__c__transactions.html
    class Transaction {
        friend class Connection;
    public:

        /// \brief Destructor that safely releases the transaction handle.
        ///
        /// If the guard still owns an MDBX transaction handle, the handle is
        /// aborted/released.
        ///
        /// \warning Must run on the same thread that owns the transaction.
        virtual ~Transaction();

        /// \brief Starts the transaction.
        ///
        /// For read-only transactions, renews this guard's reset handle when
        /// available. For writable transactions, begins a new transaction using
        /// the MDBX environment.
        ///
        /// \throws MdbxException if the transaction is already started or if beginning fails.
        /// \warning The started MDBX transaction is owned by the calling thread.
        void begin();

        /// \brief Commits the transaction.
        ///
        /// For read-only transactions, resets the handle for reuse.
        /// For writable transactions, commits the changes and closes the handle.
        ///
        /// \throws MdbxException if no transaction is active or commit/reset fails.
        /// \warning Must be called on the owning thread.
        void commit();

        /// \brief Rolls back the transaction.
        ///
        /// For read-only transactions, resets the handle.
        /// For writable transactions, aborts the transaction.
        ///
        /// \throws MdbxException if no transaction is active or rollback/reset fails.
        /// \warning Must be called on the owning thread.
        void rollback();

        /// \brief Returns the internal MDBX transaction handle.
        /// \return Raw pointer to MDBX_txn, or nullptr if not active.
        /// \warning The returned handle must stay on the owning thread.
        MDBX_txn *handle() const noexcept;
        
        /// \brief Constructs a new transaction object.
        /// \param registry Transaction tracker used to associate the
        ///        transaction with the current thread.
        /// \param env Pointer to the MDBX environment handle.
        /// \param mode Access mode of the transaction.
        Transaction(TransactionTracker* registry,
                    MDBX_env* env,
                    TransactionMode mode);
        
        /// \brief Move-constructs a transaction guard, transferring ownership.
        /// \param other Source transaction guard.
        /// \warning Transfer only within the owning thread. Moving a live
        /// transaction for use by another thread violates MDBX rules.
        Transaction(Transaction&& other) noexcept;

        /// \brief Move-assigns a transaction guard, transferring ownership.
        /// \param other Source transaction guard.
        /// \return Reference to this transaction.
        /// \warning Transfer only within the owning thread. Moving a live
        /// transaction for use by another thread violates MDBX rules.
        Transaction& operator=(Transaction&& other) noexcept;
        
    private:

        Transaction(const Transaction&) = delete;
        Transaction& operator=(const Transaction&) = delete;

        TransactionTracker* m_registry = nullptr;
        MDBX_env*       m_env = nullptr;            ///< Pointer to the MDBX environment handle.
        MDBX_txn*       m_txn = nullptr;            ///< MDBX transaction handle.
        TransactionMode m_mode = TransactionMode::WRITABLE; ///< Current transaction mode.
        bool            m_started = false;
        bool            m_handle_registered = false; ///< Tracker owns a live or opening handle slot.

        Transaction(TransactionTracker* registry,
                    MDBX_env* env,
                    TransactionMode mode,
                    bool handle_reserved);

        /// \brief Releases any owned transaction without throwing.
        void release() noexcept;

        /// \brief Transfers ownership from another transaction object.
        void move_from(Transaction& other) noexcept;

        /// \brief Best-effort unbind of a transaction from the tracker.
        /// Never throws; asserts in debug if the tracker call fails.
        void safe_unbind_txn(TransactionTracker* registry, MDBX_txn* txn) noexcept;

        /// \brief Best-effort unregister of a transaction handle from the tracker.
        /// Never throws; asserts in debug if the tracker call fails.
        void safe_unregister_txn_handle(TransactionTracker* registry) noexcept;
    }; // Transaction

}; // namespace mdbxc

#ifdef MDBX_CONTAINERS_HEADER_ONLY
#include "Transaction.ipp"
#endif

#endif // MDBX_CONTAINERS_HEADER_COMMON_TRANSACTION_HPP_INCLUDED
