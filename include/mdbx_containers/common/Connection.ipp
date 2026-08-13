#include <string>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <cassert>

#if __cplusplus >= 201703L
# include <filesystem>
#endif

#include <mdbx.h>

#ifndef MDBX_CONTAINERS_HEADER_ONLY
#if MDBXC_SYNC_ENABLED
#include <mdbx_containers/sync/capture/ISyncCaptureSink.hpp>
#include <mdbx_containers/sync/core/SyncApplyObserver.hpp>
#endif
#endif

#if MDBXC_SYNC_ENABLED
#include <mdbx_containers/sync/capture/ILogicalDbiCapture.hpp>
#endif

namespace mdbxc {

    inline Connection::Connection(const Config& config) {
        connect(config);
    }

    inline Connection::~Connection() {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        assert(!has_txn_handles() && "Destroying Connection with live transaction handles");
        cleanup(false);
    }

    inline std::shared_ptr<Connection> Connection::create(const Config& config) {
        auto conn = std::make_shared<Connection>();
        conn->connect(config);
        return conn;
    }

    inline void Connection::configure(const Config& config) {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
#       if __cplusplus >= 201703L
        m_config = config;
#       else
        m_config.reset(new Config(config));
#       endif
    }

    inline void Connection::connect() {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        if (m_env) return;
        if (!m_config) throw std::logic_error("No configuration provided.");
        m_shutdown_requested = false;
        initialize();
    }

    inline void Connection::connect(const Config& config) {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        if (m_env) return;
#       if __cplusplus >= 201703L
        m_config = config;
#       else
        m_config.reset(new Config(config));
#       endif
        m_shutdown_requested = false;
        initialize();
    }

    inline void Connection::disconnect() {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        cleanup();
    }

    inline void Connection::shutdown() {
        if (!request_shutdown()) {
            return;
        }
        wait_for_no_txn_handles();
        disconnect();
    }

    inline bool Connection::is_connected() const {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        return m_env != nullptr;
    }

    inline bool Connection::is_read_only() const {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        return m_config ? m_config->read_only : false;
    }

    inline Transaction Connection::transaction(TransactionMode mode) {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        if (m_shutdown_requested) {
            throw std::logic_error("Connection shutdown is in progress.");
        }
        if (!m_env) {
            throw std::logic_error("Connection is not connected.");
        }
        if (current_thread_has_txn()) {
            throw std::logic_error(
                "A transaction is already active on this connection's thread. "
                "Reuse it through table operations or pass the active transaction explicitly."
            );
        }
        return Transaction(static_cast<TransactionTracker*>(this), m_env, mode);
    }

    inline void Connection::begin(TransactionMode mode) {
        std::lock_guard<std::mutex> lock(m_mdbx_mutex);
        if (m_shutdown_requested) {
            throw std::logic_error("Connection shutdown is in progress.");
        }
        if (!m_env) {
            throw std::logic_error("Connection is not connected.");
        }
        auto tid = std::this_thread::get_id();
        auto it = m_transactions.find(tid);
        if (it != m_transactions.end()) {
            throw std::logic_error("Transaction already started for this thread.");
        }
        if (current_thread_has_txn()) {
            throw std::logic_error(
                "A transaction is already active on this connection's thread. "
                "Commit or roll it back before starting a manual transaction."
            );
        }
        auto txn = std::make_shared<Transaction>(static_cast<TransactionTracker*>(this), m_env, mode);
        m_transactions[tid] = txn;
    }

    inline void Connection::commit() {
        std::lock_guard<std::mutex> lock(m_mdbx_mutex);
        auto tid = std::this_thread::get_id();
        auto it = m_transactions.find(tid);
        if (it == m_transactions.end()) {
            throw std::logic_error("No transaction for this thread.");
        }
        try {
            it->second->commit();
        } catch (...) {
            m_transactions.erase(it);
            throw;
        }
        m_transactions.erase(it);
    }

    inline void Connection::rollback() {
        std::lock_guard<std::mutex> lock(m_mdbx_mutex);
        auto tid = std::this_thread::get_id();
        auto it = m_transactions.find(tid);
        if (it == m_transactions.end()) {
            throw std::logic_error("No transaction for this thread.");
        }
        it->second->rollback();
        m_transactions.erase(it);
    }

    inline std::shared_ptr<Transaction> Connection::current_txn() const {
        std::lock_guard<std::mutex> lock(m_mdbx_mutex);
        auto it = m_transactions.find(std::this_thread::get_id());
        return (it != m_transactions.end()) ? it->second : nullptr;
    }

    inline MDBX_env* Connection::env_handle() noexcept {
        return m_env;
    }

    inline int64_t Connection::max_dupsort_value_size() const {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        return m_config ? m_config->max_dupsort_value_size : Config().max_dupsort_value_size;
    }

#   if MDBXC_SYNC_ENABLED
    inline void Connection::attach_sync_capture(sync::ISyncCaptureSink* sink) {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        m_sync_capture = sink;
        m_sync_capture_token = ++m_next_sync_capture_token;
    }

    inline void Connection::detach_sync_capture() {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        m_sync_capture = nullptr;
        m_sync_capture_token = ++m_next_sync_capture_token;
    }

    inline sync::ISyncCaptureSink* Connection::sync_capture() const {
        return m_sync_capture;
    }

    inline void Connection::validate_sync_dbi_write(
            MDBX_txn* txn,
            const std::string& dbi_name,
            sync::ChangeOpType op_type) {
        try {
            (void)op_type;
            if (!is_sync_versioned_dbi(txn, dbi_name) &&
                !is_sync_logical_dbi(txn, dbi_name)) {
                return;
            }
            if (is_sync_logical_dbi(txn, dbi_name)) {
                throw std::logic_error(
                    "registered logical DBI must be mutated through "
                    "KeyMultiValueTable logical capture");
            }
            throw std::logic_error(
                "registered source-version-wins DBI must be mutated through "
                "VersionedKeyValueTable");
        } catch (...) {
            mark_sync_capture_failed(txn);
            throw;
        }
    }

    inline Connection::SyncCaptureSuppressionScope::
    SyncCaptureSuppressionScope(Connection& connection, MDBX_txn* txn)
        : m_connection(&connection),
          m_txn(txn) {
        m_connection->begin_sync_capture_suppression(m_txn);
    }

    inline Connection::SyncCaptureSuppressionScope::
    ~SyncCaptureSuppressionScope() noexcept {
        if (m_connection != nullptr) {
            m_connection->end_sync_capture_suppression(m_txn);
        }
    }

    inline std::uint64_t Connection::sync_apply_generation() const {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        return m_sync_apply_generation;
    }

    inline std::uint64_t Connection::add_sync_apply_observer(
        sync::ISyncApplyObserver* observer) {
        if (observer == nullptr) {
            throw std::invalid_argument(
                "Connection::add_sync_apply_observer observer cannot be null");
        }
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        std::shared_ptr<SyncApplyObserverState> entry(
            new SyncApplyObserverState);
        entry->token = ++m_next_sync_apply_observer_token;
        entry->observer = observer;
        entry->in_flight = 0;
        entry->removed = false;
        m_sync_apply_observers.push_back(entry);
        return entry->token;
    }

    inline bool Connection::remove_sync_apply_observer(std::uint64_t token) {
        std::unique_lock<std::mutex> locker(m_mdbx_mutex);
        std::shared_ptr<SyncApplyObserverState> removed;
        for (std::vector<std::shared_ptr<SyncApplyObserverState>>::iterator it =
                 m_sync_apply_observers.begin();
             it != m_sync_apply_observers.end(); ++it) {
            if ((*it)->token == token) {
                removed = *it;
                removed->removed = true;
                m_sync_apply_observers.erase(it);
                break;
            }
        }
        if (!removed) {
            return false;
        }
        std::size_t own_in_flight = 0;
        for (std::size_t i = 0; i < removed->active_callback_threads.size();
             ++i) {
            if (removed->active_callback_threads[i] ==
                std::this_thread::get_id()) {
                ++own_in_flight;
            }
        }
        while (removed->in_flight > own_in_flight) {
            m_sync_apply_observer_cv.wait(locker);
            own_in_flight = 0;
            for (std::size_t i = 0;
                 i < removed->active_callback_threads.size(); ++i) {
                if (removed->active_callback_threads[i] ==
                    std::this_thread::get_id()) {
                    ++own_in_flight;
                }
            }
        }
        return true;
    }

    inline std::uint64_t Connection::sync_capture_token() const {
        return m_sync_capture_token;
    }

    inline Connection::SyncApplyReadGuard Connection::sync_apply_read_guard() const {
        return SyncApplyReadGuard(m_sync_apply_mutex);
    }

    inline Connection::SyncApplyWriteGuard Connection::sync_apply_write_guard() const {
        return SyncApplyWriteGuard(m_sync_apply_mutex);
    }

    inline bool Connection::restore_sync_capture_if_current(
        sync::ISyncCaptureSink* expected_sink,
        std::uint64_t expected_token,
        sync::ISyncCaptureSink* restore_sink,
        std::uint64_t restore_token) {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        if (m_sync_capture != expected_sink ||
            m_sync_capture_token != expected_token) {
            return false;
        }
        m_sync_capture = restore_sink;
        m_sync_capture_token = restore_token;
        return true;
    }

    inline void Connection::mark_sync_capture_failed(MDBX_txn* txn) noexcept {
        if (txn == nullptr) {
            return;
        }
        std::lock_guard<std::mutex> locker(m_sync_capture_failure_mutex);
        m_sync_capture_failed_txn = txn;
    }

    inline bool Connection::sync_capture_failed(MDBX_txn* txn) const noexcept {
        if (txn == nullptr) {
            return false;
        }
        std::lock_guard<std::mutex> locker(m_sync_capture_failure_mutex);
        return m_sync_capture_failed_txn == txn;
    }

    inline void Connection::clear_sync_capture_failed(MDBX_txn* txn) noexcept {
        if (txn == nullptr) {
            return;
        }
        std::lock_guard<std::mutex> locker(m_sync_capture_failure_mutex);
        if (m_sync_capture_failed_txn == txn) {
            m_sync_capture_failed_txn = nullptr;
        }
    }

    inline void Connection::begin_sync_capture_suppression(MDBX_txn* txn) {
        if (txn == nullptr) {
            throw std::invalid_argument(
                "Connection::SyncCaptureSuppressionScope transaction cannot be null");
        }
        checked_txn_env(txn, m_env,
                        "Connection::SyncCaptureSuppressionScope");
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        m_sync_capture_suppressed_txns.push_back(txn);
    }

    inline void Connection::end_sync_capture_suppression(
            MDBX_txn* txn) noexcept {
        if (txn == nullptr) {
            return;
        }
        try {
            std::lock_guard<std::mutex> locker(m_mdbx_mutex);
            for (std::vector<MDBX_txn*>::iterator it =
                     m_sync_capture_suppressed_txns.begin();
                 it != m_sync_capture_suppressed_txns.end(); ++it) {
                if (*it == txn) {
                    m_sync_capture_suppressed_txns.erase(it);
                    return;
                }
            }
        } catch (...) {
            // Destructors must not throw; a missing suppression entry is benign.
        }
    }

    inline bool Connection::sync_capture_suppressed(MDBX_txn* txn) const {
        if (txn == nullptr) {
            return false;
        }
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        for (std::size_t i = 0; i < m_sync_capture_suppressed_txns.size();
             ++i) {
            if (m_sync_capture_suppressed_txns[i] == txn) {
                return true;
            }
        }
        return false;
    }

    inline void Connection::ensure_sync_capture_txn_supported(
            MDBX_txn* txn,
            const char* context) const {
        bool capture_attached = false;
        {
            std::lock_guard<std::mutex> locker(m_mdbx_mutex);
            capture_attached = m_sync_capture != nullptr;
        }
        if (!capture_attached) {
            return;
        }
        if (thread_txn() == txn) {
            return;
        }
        const MDBX_txn_flags_t flags = mdbx_txn_flags(txn);
        if ((static_cast<int>(flags) &
             static_cast<int>(MDBX_TXN_RDONLY)) != 0) {
            return;
        }
        throw std::logic_error(
            std::string(context) +
            " cannot use caller-created raw writable MDBX_txn* while sync capture is attached; "
            "use mdbx_containers::Transaction or Connection::begin()/commit()");
    }

    inline void Connection::ensure_sync_dbi_external_txn_supported(
            MDBX_txn* txn,
            const std::string& dbi_name,
            const char* context) const {
        if (thread_txn() == txn) {
            return;
        }
        const MDBX_txn_flags_t flags = mdbx_txn_flags(txn);
        if ((static_cast<int>(flags) &
             static_cast<int>(MDBX_TXN_RDONLY)) != 0) {
            return;
        }
        if (!is_sync_versioned_dbi(txn, dbi_name) &&
            !is_sync_logical_dbi(txn, dbi_name)) {
            return;
        }
        if (is_sync_logical_dbi(txn, dbi_name)) {
            throw std::logic_error(
                std::string(context) +
                " cannot use caller-created raw writable MDBX_txn* with a "
                "registered logical DBI; use Connection::transaction() and "
                "KeyMultiValueTable");
        }
        throw std::logic_error(
            std::string(context) +
            " cannot use caller-created raw writable MDBX_txn* with a "
            "registered source-version-wins DBI; use VersionedKeyValueTable");
    }

    inline bool Connection::is_sync_versioned_dbi(
            MDBX_txn* txn,
            const std::string& dbi_name) const {
        checked_txn_env(txn, m_env, "Connection::is_sync_versioned_dbi");
        static const char versioned_dbi_registry_name[] =
            "_mdbxc_versioned_dbis";
        MDBX_val registry_name = {
            const_cast<char*>(versioned_dbi_registry_name),
            sizeof(versioned_dbi_registry_name) - 1u
        };
        MDBX_dbi main_dbi = 0;
        check_mdbx(mdbx_dbi_open(
                       txn, nullptr, static_cast<MDBX_db_flags_t>(0),
                       &main_dbi),
                   "Connection failed to open main DBI");
        MDBX_val registry_value;
        const int registry_lookup_rc = mdbx_get(
            txn, main_dbi, &registry_name, &registry_value);
        if (registry_lookup_rc == MDBX_NOTFOUND) {
            return false;
        }
        check_mdbx(registry_lookup_rc,
                   "Connection failed to find versioned DBI registry");
        MDBX_dbi registry_dbi = 0;
        const int open_rc = mdbx_dbi_open(
            txn, versioned_dbi_registry_name,
            static_cast<MDBX_db_flags_t>(0),
            &registry_dbi);
        if (open_rc == MDBX_NOTFOUND) {
            return false;
        }
        check_mdbx(open_rc,
                   "Connection failed to open versioned DBI registry");
        MDBX_val key = {
            dbi_name.empty() ? nullptr : const_cast<char*>(dbi_name.data()),
            dbi_name.size()
        };
        MDBX_val value;
        const int get_rc = mdbx_get(txn, registry_dbi, &key, &value);
        if (get_rc == MDBX_NOTFOUND) {
            return false;
        }
        check_mdbx(get_rc,
                   "Connection failed to query versioned DBI registry");
        return true;
    }

    inline bool Connection::is_sync_logical_dbi(
            MDBX_txn* txn,
            const std::string& dbi_name) const {
        checked_txn_env(txn, m_env, "Connection::is_sync_logical_dbi");
        static const char binding_registry_name[] =
            "_mdbxc_logical_dbi_bindings";
        MDBX_dbi main_dbi = 0;
        check_mdbx(mdbx_dbi_open(txn, nullptr, static_cast<MDBX_db_flags_t>(0),
                                 &main_dbi),
                   "Connection failed to open main DBI");
        MDBX_val registry_name = {
            const_cast<char*>(binding_registry_name),
            sizeof(binding_registry_name) - 1u
        };
        MDBX_val registry_value;
        const int registry_lookup_rc = mdbx_get(
            txn, main_dbi, &registry_name, &registry_value);
        if (registry_lookup_rc == MDBX_NOTFOUND) return false;
        check_mdbx(registry_lookup_rc,
                   "Connection failed to find logical DBI binding registry");
        MDBX_dbi bindings_dbi = 0;
        const int open_rc = mdbx_dbi_open(
            txn, binding_registry_name, static_cast<MDBX_db_flags_t>(0),
            &bindings_dbi);
        if (open_rc == MDBX_NOTFOUND) return false;
        check_mdbx(open_rc,
                   "Connection failed to open logical DBI binding registry");
        MDBX_val key = {
            dbi_name.empty() ? nullptr : const_cast<char*>(dbi_name.data()),
            dbi_name.size()
        };
        MDBX_val value;
        const int get_rc = mdbx_get(txn, bindings_dbi, &key, &value);
        if (get_rc == MDBX_NOTFOUND) return false;
        check_mdbx(get_rc,
                   "Connection failed to query logical DBI binding registry");
        if (value.iov_len < 26u) {
            throw std::runtime_error(
                "Connection found invalid logical DBI binding");
        }
        return true;
    }

    inline void Connection::attach_logical_dbi_capture(
            const std::shared_ptr<sync::ILogicalDbiCapture>& capture) {
        if (!capture) {
            throw std::invalid_argument("Connection logical DBI capture is null");
        }
        if (capture->dbi_name().empty()) {
            throw std::invalid_argument("Connection logical DBI capture name is empty");
        }
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        std::map<std::string, std::shared_ptr<sync::ILogicalDbiCapture>>::iterator
            it = m_logical_dbi_captures.find(capture->dbi_name());
        if (it != m_logical_dbi_captures.end()) {
            if (it->second->schema_id() != capture->schema_id() ||
                it->second->schema_version() != capture->schema_version()) {
                throw std::logic_error(
                    "Connection logical DBI capture schema does not match existing binding");
            }
            return;
        }
        m_logical_dbi_captures[capture->dbi_name()] = capture;
    }

    inline std::shared_ptr<sync::ILogicalDbiCapture>
    Connection::logical_dbi_capture_for(MDBX_txn* txn,
                                        const std::string& dbi_name) const {
        std::shared_ptr<sync::ILogicalDbiCapture> capture;
        {
            std::lock_guard<std::mutex> locker(m_mdbx_mutex);
            std::map<std::string, std::shared_ptr<sync::ILogicalDbiCapture>>::
                const_iterator it = m_logical_dbi_captures.find(dbi_name);
            if (it != m_logical_dbi_captures.end()) capture = it->second;
        }
        if (capture) return capture;
        if (is_sync_logical_dbi(txn, dbi_name)) {
            throw std::logic_error(
                "registered logical DBI has no active logical capture binding");
        }
        return capture;
    }

    inline void Connection::record_logical_dbi_insert(
            MDBX_txn* txn, const std::string& dbi_name,
            const void* key, const void* value) {
        try {
            const std::shared_ptr<sync::ILogicalDbiCapture> capture =
                logical_dbi_capture_for(txn, dbi_name);
            if (capture) capture->record_insert(txn, key, value);
        } catch (...) {
            mark_sync_capture_failed(txn);
            throw;
        }
    }

    inline void Connection::record_logical_dbi_erase_key(
            MDBX_txn* txn, const std::string& dbi_name, const void* key) {
        try {
            const std::shared_ptr<sync::ILogicalDbiCapture> capture =
                logical_dbi_capture_for(txn, dbi_name);
            if (capture) capture->record_erase_key(txn, key);
        } catch (...) {
            mark_sync_capture_failed(txn);
            throw;
        }
    }

    inline void Connection::record_logical_dbi_erase_all_values(
            MDBX_txn* txn, const std::string& dbi_name,
            const void* key, const void* value) {
        try {
            const std::shared_ptr<sync::ILogicalDbiCapture> capture =
                logical_dbi_capture_for(txn, dbi_name);
            if (capture) capture->record_erase_all_values(txn, key, value);
        } catch (...) {
            mark_sync_capture_failed(txn);
            throw;
        }
    }

    inline void Connection::record_logical_dbi_erase_one_value(
            MDBX_txn* txn, const std::string& dbi_name,
            const void* key, const void* value) {
        try {
            const std::shared_ptr<sync::ILogicalDbiCapture> capture =
                logical_dbi_capture_for(txn, dbi_name);
            if (capture) capture->record_erase_one_value(txn, key, value);
        } catch (...) {
            mark_sync_capture_failed(txn);
            throw;
        }
    }

    inline void Connection::record_logical_dbi_clear(
            MDBX_txn* txn, const std::string& dbi_name) {
        try {
            const std::shared_ptr<sync::ILogicalDbiCapture> capture =
                logical_dbi_capture_for(txn, dbi_name);
            if (capture) capture->record_clear(txn);
        } catch (...) {
            mark_sync_capture_failed(txn);
            throw;
        }
    }

    inline void Connection::ensure_logical_dbi_operation_supported(
            MDBX_txn* txn, const std::string& dbi_name,
            const char* operation) const {
        if (!is_sync_logical_dbi(txn, dbi_name)) return;
        throw std::logic_error(std::string("registered logical DBI does not support ") +
                               operation + "; use a supported KeyMultiValueTable mutation");
    }

    inline Connection::SyncApplyNotification Connection::mark_sync_apply_committed(
        std::size_t applied_batches,
        std::size_t applied_ops,
        const std::vector<std::string>& affected_dbi_names) {
        SyncApplyNotification notification;
        try {
            notification.affected_dbi_names = affected_dbi_names;
        } catch (...) {
            notification.affected_dbi_names.clear();
        }
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        try {
            notification.callbacks.reserve(m_sync_apply_observers.size());
        } catch (...) {
            ++m_sync_apply_generation;
            notification.generation = m_sync_apply_generation;
            notification.applied_batches = applied_batches;
            notification.applied_ops = applied_ops;
            return notification;
        }
        ++m_sync_apply_generation;
        notification.generation = m_sync_apply_generation;
        notification.applied_batches = applied_batches;
        notification.applied_ops = applied_ops;
        for (std::size_t i = 0; i < m_sync_apply_observers.size(); ++i) {
            const std::shared_ptr<SyncApplyObserverState>& state =
                m_sync_apply_observers[i];
            if (!state->removed && state->observer != nullptr) {
                SyncApplyObserverCallback callback;
                callback.state = state;
                callback.observer = state->observer;
                try {
                    notification.callbacks.push_back(callback);
                    ++state->in_flight;
                } catch (...) {
                    break;
                }
            }
        }
        return notification;
    }

    inline void Connection::notify_sync_apply_observers(
        const SyncApplyNotification& notification) noexcept {
        try {
            if (notification.callbacks.empty()) {
                return;
            }
            sync::SyncApplyEvent event;
            event.generation = notification.generation;
            event.applied_batches = notification.applied_batches;
            event.applied_ops = notification.applied_ops;
            try {
                event.affected_dbi_names = notification.affected_dbi_names;
            } catch (...) {
                event.affected_dbi_names.clear();
            }
            for (std::size_t i = 0; i < notification.callbacks.size(); ++i) {
                try {
                    begin_sync_apply_observer_callback(
                        notification.callbacks[i].state);
                    if (notification.callbacks[i].observer != nullptr) {
                        notification.callbacks[i].observer
                            ->on_sync_apply_committed(event);
                    }
                } catch (...) {
                    // Sync apply has already committed; observers are best-effort.
                }
                try {
                    finish_sync_apply_observer_callback(
                        notification.callbacks[i].state);
                } catch (...) {
                }
            }
        } catch (...) {
            // Sync apply has already committed; notification is best-effort.
        }
    }

    inline void Connection::begin_sync_apply_observer_callback(
        const std::shared_ptr<SyncApplyObserverState>& state) {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        state->active_callback_threads.push_back(std::this_thread::get_id());
    }

    inline void Connection::finish_sync_apply_observer_callback(
        const std::shared_ptr<SyncApplyObserverState>& state) {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        for (std::vector<std::thread::id>::iterator it =
                 state->active_callback_threads.begin();
             it != state->active_callback_threads.end(); ++it) {
            if (*it == std::this_thread::get_id()) {
                state->active_callback_threads.erase(it);
                break;
            }
        }
        if (state->in_flight != 0u) {
            --state->in_flight;
        }
        if (state->in_flight == 0u) {
            m_sync_apply_observer_cv.notify_all();
        }
    }

    inline void Connection::on_pre_commit(MDBX_txn* txn) {
        if (sync_capture_failed(txn)) {
            throw std::logic_error(
                "Cannot commit transaction after sync capture failure");
        }
        sync::ISyncCaptureSink* sink = sync_capture();
        if (sink != nullptr) {
            try {
                sink->flush_in_txn(txn);
            } catch (...) {
                mark_sync_capture_failed(txn);
                throw;
            }
        }
        std::vector<std::shared_ptr<sync::ILogicalDbiCapture>> captures;
        {
            std::lock_guard<std::mutex> locker(m_mdbx_mutex);
            captures.reserve(m_logical_dbi_captures.size());
            for (std::map<std::string,
                           std::shared_ptr<sync::ILogicalDbiCapture>>::
                     const_iterator it = m_logical_dbi_captures.begin();
                 it != m_logical_dbi_captures.end(); ++it) {
                captures.push_back(it->second);
            }
        }
        try {
            for (std::size_t i = 0; i < captures.size(); ++i) {
                captures[i]->flush_in_txn(txn);
            }
        } catch (...) {
            mark_sync_capture_failed(txn);
            throw;
        }
    }

    inline void Connection::on_discard(MDBX_txn* txn) noexcept {
        try {
            // txn is an identity token here. A terminal commit error may have
            // already invalidated the native handle.
            clear_sync_capture_failed(txn);
            sync::ISyncCaptureSink* sink = sync_capture();
            if (sink != nullptr) {
                sink->discard_txn(txn);
            }
            std::vector<std::shared_ptr<sync::ILogicalDbiCapture>> captures;
            {
                std::lock_guard<std::mutex> locker(m_mdbx_mutex);
                captures.reserve(m_logical_dbi_captures.size());
                for (std::map<std::string,
                               std::shared_ptr<sync::ILogicalDbiCapture>>::
                         const_iterator it = m_logical_dbi_captures.begin();
                     it != m_logical_dbi_captures.end(); ++it) {
                    captures.push_back(it->second);
                }
            }
            for (std::size_t i = 0; i < captures.size(); ++i) {
                captures[i]->discard_txn(txn);
            }
        } catch (...) {
            // discard must not throw; sink contract is noexcept.
        }
    }
#   endif

    inline void Connection::backup_to(const std::string& path, const BackupOptions& options) {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        if (!m_env) {
            throw MdbxException("Connection is not connected.", MDBX_EINVAL);
        }
        if (m_shutdown_requested) {
            throw std::logic_error("Cannot backup during connection shutdown.");
        }

        MDBX_copy_flags_t flags = MDBX_CP_DEFAULTS;
        if (options.mode == BackupMode::Compact) {
            flags = static_cast<MDBX_copy_flags_t>(flags | MDBX_CP_COMPACT);
        }
        if (options.throttle_mvcc) {
            flags = static_cast<MDBX_copy_flags_t>(flags | MDBX_CP_THROTTLE_MVCC);
        }
        if (options.dont_flush) {
            flags = static_cast<MDBX_copy_flags_t>(flags | MDBX_CP_DONT_FLUSH);
        }
        if (options.force_dynamic_size) {
            flags = static_cast<MDBX_copy_flags_t>(flags | MDBX_CP_FORCE_DYNAMIC_SIZE);
        }

        const int rc = mdbx_env_copy(m_env, path.c_str(), flags);
        if (rc != MDBX_SUCCESS) {
            check_mdbx(rc, "mdbx_env_copy failed");
        }
    }

    inline void Connection::sync_to_disk(bool force, bool nonblock) {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        if (!m_env) {
            throw MdbxException("Connection is not connected.", MDBX_EINVAL);
        }
        const int rc = mdbx_env_sync_ex(m_env, force, nonblock);
        // mdbx_env_sync_ex returns MDBX_SUCCESS on flushed data and
        // MDBX_RESULT_TRUE when nothing was pending; both are success.
        if (rc != MDBX_SUCCESS && rc != MDBX_RESULT_TRUE) {
            check_mdbx(rc, "mdbx_env_sync_ex failed");
        }
    }

    inline void Connection::initialize() {
        try {
            db_init();
        } catch (...) {
            if (m_env && mdbx_env_close(m_env) == MDBX_SUCCESS) {
                m_env = nullptr;
            }
            throw;
        }
    }

    inline void Connection::cleanup(bool use_throw) {
        if (has_txn_handles()) {
            if (use_throw) {
                throw MdbxException("Cannot disconnect while transaction handles are open.", MDBX_BUSY);
            }
            return;
        }
        m_transactions.clear();
        if (m_env) {
            int rc = mdbx_env_close(m_env);
            if (rc != MDBX_SUCCESS && use_throw) {
                check_mdbx(rc, "Failed to close environment");
            }
            if (rc == MDBX_SUCCESS) {
                m_env = nullptr;
                m_shutdown_requested = false;
            }
        }
    }

    inline bool Connection::request_shutdown() {
        std::lock_guard<std::mutex> locker(m_mdbx_mutex);
        if (!m_env) {
            m_shutdown_requested = false;
            return false;
        }
        if (current_thread_has_txn_handle()) {
            throw std::logic_error("Cannot shutdown from a thread with an open transaction handle.");
        }
        m_shutdown_requested = true;
        return true;
    }
    
    inline void Connection::db_init() {
#       if __cplusplus >= 201703L
        namespace fs = std::filesystem;
#       endif

        int rc = 0;
        check_mdbx(
            mdbx_env_create(&m_env),
            "Failed to create environment"
        );

        check_mdbx(
            mdbx_env_set_geometry(
                m_env,
                m_config->size_lower,
                m_config->size_now,
                m_config->size_upper,
                m_config->growth_step,
                m_config->shrink_threshold,
                m_config->page_size
            ),
            "Failed to set environment geometry"
        );

        check_mdbx(
            mdbx_env_set_maxdbs(m_env, m_config->max_dbs),
            "Failed to set max databases"
        );

        int readers = m_config->max_readers > 0
            ? static_cast<int>(m_config->max_readers)
            : static_cast<int>(std::thread::hardware_concurrency()) * 2;
        check_mdbx(
            mdbx_env_set_maxreaders(m_env, readers),
            "Failed to set max readers"
        );

        MDBX_env_flags_t env_flags = MDBX_ACCEDE;
        if (m_config->no_subdir)     env_flags |= MDBX_NOSUBDIR;
        if (m_config->sync_durable)  env_flags |= MDBX_SYNC_DURABLE;
        if (m_config->read_only)     env_flags |= MDBX_RDONLY;
        if (!m_config->readahead)    env_flags |= MDBX_NORDAHEAD;
        if (m_config->writemap_mode) env_flags |= MDBX_WRITEMAP;

        std::string pathname = m_config->pathname;
        if (m_config->relative_to_exe && 
            !is_absolute_path(pathname) &&
            !is_explicitly_relative(pathname)) {
#       if __cplusplus >= 201703L
#           if __cplusplus >= 202002L
            pathname = u8string_to_string((fs::u8path(get_exec_dir()) / fs::u8path(pathname)).u8string());
#           else
            pathname = (fs::u8path(get_exec_dir()) / fs::u8path(pathname)).u8string();
#           endif
#       else
#           ifdef _WIN32
            pathname = get_exec_dir() + "\\" + pathname;
#           else
            pathname = get_exec_dir() + "/" + pathname;
#           endif
#       endif
        }
        if (!m_config->read_only) {
            create_directories(pathname);
        }

#ifdef _WIN32
#   if __cplusplus >= 201703L
#       if __cplusplus >= 202002L
        fs::path file_path = fs::u8path(pathname);
#       else
        std::wstring wide_path = utf8_to_wide(pathname);
        fs::path file_path = fs::path(wide_path);
#       endif
        file_path = file_path.lexically_normal();
        check_mdbx(
            mdbx_env_openW(m_env, file_path.c_str(), env_flags, 0664),
            "Failed to open environment"
        );
#   else
        pathname = lexically_normal_compat(pathname);
        std::wstring wide_path = utf8_to_wide(pathname);
        check_mdbx(
            mdbx_env_openW(m_env, wide_path.c_str(), env_flags, 0664),
            "Failed to open environment"
        );
#   endif

#else
        check_mdbx(
            mdbx_env_open(m_env, pathname.c_str(), env_flags, 0664),
            "Failed to open environment"
        );
#endif
    }

} // namespace mdbxc
