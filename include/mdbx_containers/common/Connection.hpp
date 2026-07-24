#pragma once
#ifndef MDBX_CONTAINERS_HEADER_COMMON_CONNECTION_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_COMMON_CONNECTION_HPP_INCLUDED

/// \file Connection.hpp
/// \brief Manages an MDBX database connection using a provided configuration.

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#if __cplusplus >= 201703L
#   include <optional>
#   include <shared_mutex>
#endif

#include <mdbx.h>

#include "Backup.hpp"
#include "Config.hpp"
#include "Transaction.hpp"

namespace mdbxc {
    class VectorStore;

    namespace sync {
        class ISyncCaptureSink;
        class ISyncApplyObserver;
        class SyncCaptureScope;
        class SyncEngine;
    }

    /// \class Connection
    /// \ingroup mdbxc_core
    /// \brief Manages a single MDBX environment and per-thread transaction tracking.
    ///
    /// \thread_safety Conditionally thread-safe: table operations may run on
    /// different threads while the connection lifecycle is stable and every
    /// thread uses only its own transaction handles. Lifecycle operations
    /// (`configure()`, `connect()`, `disconnect()`, and destruction) must not
    /// run concurrently with table operations or active transactions. Use
    /// `shutdown()` or `shutdown_for()` to request a coordinated stop.
    ///
    /// \note Internal mutexes protect connection state and manual transaction
    /// registries. They do not make `MDBX_txn*` or MDBX cursors safe to use
    /// from another thread.
    /// \see https://libmdbx.dqdkfa.ru/group__c__transactions.html
    /// \see https://libmdbx.dqdkfa.ru/group__c__opening.html
    class Connection : private TransactionTracker {
#   if MDBXC_SYNC_ENABLED
    public:
        void on_pre_commit(MDBX_txn* txn) override;
        void on_discard(MDBX_txn* txn) noexcept override;
#   endif
    private:
        friend class BaseTable;
#   if MDBXC_SYNC_ENABLED
        friend class sync::SyncCaptureScope;
#   endif
    public:

        /// \brief Default constructor.
        Connection() = default;

        /// \brief Constructs a connection using the given MDBX configuration.
        /// \param config Configuration used to initialize the environment.
        explicit Connection(const Config& config);

        /// \brief Destructor. Closes the MDBX environment when no transaction handle is open.
        ///
        /// \warning Lifecycle-only. Destroy the connection after all worker
        /// threads have stopped and all transactions/cursors using the
        /// environment are gone.
        ~Connection();
        
        /// \brief Creates and connects a new shared Connection instance.
        /// \param config Configuration to use for initialization.
        /// \return Shared pointer to the created Connection.
        static std::shared_ptr<Connection> create(const Config& config);

        /// \brief Sets the MDBX configuration (must be called before connect()).
        /// \param config New configuration to apply.
        /// \warning Lifecycle-only. Do not call concurrently with table
        /// operations or active transactions.
        void configure(const Config& config);

        /// \brief Connects to the database using the current configuration.
        /// \throws MdbxException on configuration or environment errors.
        /// \warning Lifecycle-only. Call before concurrent table activity.
        void connect();
        
        /// \brief Sets configuration and connects in one step.
        /// \param config Configuration to use.
        /// \warning Lifecycle-only. Call before concurrent table activity.
        void connect(const Config& config);

        /// \brief Disconnects from the MDBX environment and releases resources.
        /// \throws MdbxException if closing the environment fails.
        /// \throws MdbxException with MDBX_BUSY if any transaction handle is open.
        /// \warning Lifecycle-only. Call after all concurrent table activity,
        /// transactions, and MDBX cursors have ended. This method does not wait
        /// for worker threads and does not abort transactions owned by another
        /// thread. Use `shutdown()`/`shutdown_for()` for coordinated closing.
        void disconnect();

        /// \brief Requests shutdown, waits for transaction handles, then disconnects.
        /// \throws std::logic_error if the calling thread owns a transaction handle.
        /// \throws MdbxException if MDBX close fails.
        ///
        /// New transactions are rejected after shutdown is requested. Existing
        /// transaction handles must be closed on their owning threads.
        void shutdown();

        /// \brief Requests shutdown and waits up to \p timeout before disconnecting.
        /// \param timeout Maximum time to wait for open transaction handles.
        /// \return true if the environment was disconnected, false on timeout.
        /// \throws std::logic_error if the calling thread owns a transaction handle.
        /// \throws MdbxException if MDBX close fails after transactions finish.
        ///
        /// When this returns false, shutdown remains requested: new transactions
        /// are still rejected, and the caller may retry `shutdown_for()` or
        /// `shutdown()` after worker threads finish.
        template<class Rep, class Period>
        bool shutdown_for(const std::chrono::duration<Rep, Period>& timeout) {
            if (!request_shutdown()) {
                return true;
            }
            if (!wait_for_no_txn_handles_for(timeout)) {
                return false;
            }
            disconnect();
            return true;
        }

        /// \brief Checks whether the environment is currently open.
        /// \return true if connected, false otherwise.
        bool is_connected() const;

        /// \brief Checks whether the environment is configured read-only.
        /// \return true if the current configuration opens MDBX with MDBX_RDONLY.
        bool is_read_only() const;

        /// \brief Creates a RAII transaction object.
        /// \param mode Transaction mode to open (default: WRITABLE).
        /// \throws MdbxException on MDBX errors.
        /// \return Transaction guard managing the MDBX_txn handle.
        /// \warning The returned transaction belongs to the calling thread.
        /// Do not use or destroy it from another thread.
        Transaction transaction(TransactionMode mode = TransactionMode::WRITABLE);
        
        /// \brief Begins a manual transaction (must be committed or rolled back later).
        /// \param mode The transaction mode (default: WRITABLE).
        /// \throws MdbxException if the transaction is already started or if beginning fails.
        /// \throws std::logic_error
        /// \warning Manual transactions are bound to the calling thread.
        void begin(TransactionMode mode = TransactionMode::WRITABLE);
        
        /// \brief Commits the current manual transaction.
        /// \throws MdbxException if no transaction is active or commit/reset fails.
        void commit();

        /// \brief Rolls back the current manual transaction.
        /// \throws MdbxException if no transaction is active or rollback/reset fails.
        void rollback();
        
        /// \brief Returns the transaction associated with the current thread.
        /// \return Shared pointer to the active Transaction or nullptr.
        std::shared_ptr<Transaction> current_txn() const;

        /// \brief Returns the environment handle.
        /// \return MDBX environment pointer.
        MDBX_env* env_handle() noexcept;

        /// \brief Returns the configured proactive DUPSORT duplicate value limit.
        /// \return Maximum duplicate value size, or a non-positive value when disabled.
        int64_t max_dupsort_value_size() const;

#       if MDBXC_SYNC_ENABLED
        /// \brief Attaches a non-owning \c ISyncCaptureSink.
        /// \details Pass \c nullptr to disable change capture. The pointer is
        /// non-owning; the sink must outlive the \c Connection period during
        /// which it is attached.
        /// \warning Lifecycle-only. Do not call concurrently with table
        ///          operations or active transactions.
        void attach_sync_capture(sync::ISyncCaptureSink* sink);

        /// \brief Detaches the currently attached \c ISyncCaptureSink.
        /// \details Safe to call when no sink is attached.
        /// \warning Lifecycle-only. Do not call concurrently with table
        ///          operations or active transactions.
        void detach_sync_capture();

        /// \brief Returns the currently attached \c ISyncCaptureSink or \c nullptr.
        sync::ISyncCaptureSink* sync_capture() const;

        /// \brief Returns the remote sync-apply invalidation generation.
        /// \details The value increments after a successful
        /// \c SyncEngine::handle_push() commit that applied at least one
        /// incoming batch. Table/view wrappers with RAM caches may compare a
        /// stored generation with this value and rebuild when it changes.
        std::uint64_t sync_apply_generation() const;

        /// \brief Registers a non-owning remote apply observer.
        /// \details The observer is called after successful remote sync apply
        /// commits that changed user data. The pointer is non-owning; the
        /// observer must outlive the registration. Observer callbacks run
        /// after the connection apply/write barrier is released, and observer
        /// exceptions are swallowed.
        /// \return Token that can be passed to
        /// \c remove_sync_apply_observer().
        /// \throws std::invalid_argument if \p observer is null.
        std::uint64_t add_sync_apply_observer(
            sync::ISyncApplyObserver* observer);

        /// \brief Removes a previously registered remote apply observer.
        /// \details When this returns \c true, no callback for \p token is
        /// still running or will start later. If this is called from that
        /// observer's own callback, it waits for other in-flight callbacks for
        /// the same token but not for the current callback.
        /// \return true when a matching registration was removed.
        bool remove_sync_apply_observer(std::uint64_t token);
#       endif

        /// \brief Copies the whole MDBX environment to a file or directory path.
        /// \param path Destination path. The target must not already exist;
        ///        the caller is responsible for removing a stale target before
        ///        invoking this method.
        /// \param options Backup behavior; see \ref BackupOptions.
        /// \throws MdbxException on any MDBX error.
        /// \warning Copies the entire environment, not a single logical table.
        /// \warning Do not call concurrently with \c configure(), \c connect(),
        ///          \c disconnect(), or destruction. Do not call while shutdown
        ///          has been requested.
        /// \note The backup may be performed while readers exist, but a long
        ///       backup keeps an MVCC snapshot alive and may delay page reuse.
        ///       Use \c BackupOptions::throttle_mvcc to mitigate this.
        /// \note Holds the connection mutex for the duration of the copy.
        ///       Existing raw MDBX transactions may continue, but new
        ///       \c Connection::transaction(), \c Connection::begin(), and
        ///       other state-mutating calls are blocked until backup completes.
        void backup_to(const std::string& path, const BackupOptions& options = BackupOptions());

        /// \brief Flushes the environment to disk.
        /// \param force When \c true forces a synchronous durable flush.
        /// \param nonblock When \c true returns immediately if a flush is
        ///        already in progress on another thread.
        /// \throws MdbxException on any MDBX error.
        /// \warning Not valid for read-only connections; MDBX may return
        ///          \c MDBX_EACCES.
        /// \note Durability flush is not database replication; see
        ///       \c sync.hpp for changelog-based replication.
        void sync_to_disk(bool force = true, bool nonblock = false);

    private:
        friend class Transaction;

        MDBX_env *m_env = nullptr;          ///< Pointer to the MDBX environment handle.
        mutable std::mutex m_mdbx_mutex;    ///< Protects lifecycle state and manual transaction map.
        std::unordered_map<std::thread::id, std::shared_ptr<Transaction>> m_transactions;
        bool m_shutdown_requested = false;  ///< Rejects new transactions during coordinated shutdown.
#       if __cplusplus >= 201703L
        using config_t = std::optional<Config>;
#       else
        using config_t = std::unique_ptr<Config>;
#       endif
        config_t m_config;                  ///< Database configuration object.

        /// \brief Initializes the MDBX environment.
        void initialize();
        
        /// \brief Safely closes the environment and aborts tracked transactions if needed.
        /// \param use_throw If true, throw on error; otherwise suppress exceptions.
        void cleanup(bool use_throw = true);

        /// \brief Marks the connection as shutting down.
        /// \return true if the connection is open and needs waiting/closing.
        bool request_shutdown();

        /// \brief Initializes and opens the MDBX environment.
        void db_init();

#       if MDBXC_SYNC_ENABLED
        std::uint64_t sync_capture_token() const;
        bool restore_sync_capture_if_current(sync::ISyncCaptureSink* expected_sink,
                                             std::uint64_t expected_token,
                                             sync::ISyncCaptureSink* restore_sink,
                                             std::uint64_t restore_token);
        void mark_sync_capture_failed(MDBX_txn* txn) noexcept;
        bool sync_capture_failed(MDBX_txn* txn) const noexcept;
        void clear_sync_capture_failed(MDBX_txn* txn) noexcept;
        void ensure_sync_capture_txn_supported(MDBX_txn* txn,
                                               const char* context) const;
        struct SyncApplyObserverState;

        struct SyncApplyObserverCallback {
            std::shared_ptr<SyncApplyObserverState> state;
            sync::ISyncApplyObserver* observer;
        };

        struct SyncApplyNotification {
            std::uint64_t generation = 0;
            std::size_t applied_batches = 0;
            std::size_t applied_ops = 0;
            std::vector<std::string> affected_dbi_names;
            std::vector<SyncApplyObserverCallback> callbacks;
        };

        SyncApplyNotification mark_sync_apply_committed(
            std::size_t applied_batches,
            std::size_t applied_ops,
            const std::vector<std::string>& affected_dbi_names);
        void notify_sync_apply_observers(
            const SyncApplyNotification& notification);
        void begin_sync_apply_observer_callback(
            const std::shared_ptr<SyncApplyObserverState>& state);
        void finish_sync_apply_observer_callback(
            const std::shared_ptr<SyncApplyObserverState>& state);

        friend class sync::SyncEngine;
        friend class VectorStore;
#           if __cplusplus >= 201703L
        using SyncApplyMutex = std::shared_mutex;
        using SyncApplyReadGuard = std::shared_lock<SyncApplyMutex>;
#           else
        using SyncApplyMutex = std::mutex;
        using SyncApplyReadGuard = std::unique_lock<SyncApplyMutex>;
#           endif
        using SyncApplyWriteGuard = std::unique_lock<SyncApplyMutex>;

        SyncApplyReadGuard sync_apply_read_guard() const;
        SyncApplyWriteGuard sync_apply_write_guard() const;

        struct SyncApplyObserverState {
            std::uint64_t token;
            sync::ISyncApplyObserver* observer;
            std::size_t in_flight;
            bool removed;
            std::vector<std::thread::id> active_callback_threads;
        };

        sync::ISyncCaptureSink* m_sync_capture = nullptr;
        std::uint64_t m_sync_capture_token = 0;
        std::uint64_t m_next_sync_capture_token = 0;
        MDBX_txn* m_sync_capture_failed_txn = nullptr;
        mutable std::mutex m_sync_capture_failure_mutex;
        std::uint64_t m_next_sync_apply_observer_token = 0;
        std::uint64_t m_sync_apply_generation = 0;
        std::vector<std::shared_ptr<SyncApplyObserverState>> m_sync_apply_observers;
        std::condition_variable m_sync_apply_observer_cv;
        mutable SyncApplyMutex m_sync_apply_mutex;
#       endif

    }; // Connection

} // namespace mdbxc

#ifdef MDBX_CONTAINERS_HEADER_ONLY
#include "Connection.ipp"
#endif

#endif // MDBX_CONTAINERS_HEADER_COMMON_CONNECTION_HPP_INCLUDED
