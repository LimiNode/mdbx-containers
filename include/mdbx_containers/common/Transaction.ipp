namespace mdbxc {

    inline Transaction::Transaction(TransactionTracker* registry, MDBX_env* env, TransactionMode mode)
        : m_registry(registry), m_env(env), m_mode(mode) {
        begin();
    }

    inline Transaction::Transaction(Transaction&& other) noexcept {
        move_from(other);
    }

    inline Transaction& Transaction::operator=(Transaction&& other) noexcept {
        if (this != &other) {
            release();
            move_from(other);
        }
        return *this;
    }

    inline Transaction::~Transaction() {
        release();
    }

    inline void Transaction::safe_unbind_txn(TransactionTracker* registry, MDBX_txn* txn) noexcept {
        if (!registry || !txn) return;
        try {
            registry->unbind_txn(txn);
        } catch (...) {
            assert(!"TransactionTracker::unbind_txn() failed during Transaction cleanup");
        }
    }

    inline void Transaction::safe_unregister_txn_handle(TransactionTracker* registry) noexcept {
        if (!registry) return;
        try {
            registry->unregister_txn_handle();
        } catch (...) {
            assert(!"TransactionTracker::unregister_txn_handle() failed during Transaction cleanup");
        }
    }

    inline void Transaction::release() noexcept {
        TransactionTracker* registry = m_registry;
        MDBX_txn* txn = m_txn;
        const TransactionMode mode = m_mode;
        const bool was_started = m_started;

        m_registry = nullptr;
        m_env = nullptr;
        m_txn = nullptr;
        m_started = false;

#       if MDBXC_SYNC_ENABLED
        if (registry && txn && was_started && mode == TransactionMode::WRITABLE) {
            registry->on_discard(txn);
        }
#       endif

        if (txn) {
            const int rc = mdbx_txn_abort(txn);
            assert((rc == MDBX_SUCCESS || rc == MDBX_THREAD_MISMATCH) &&
                   "mdbx_txn_abort() failed in Transaction::release()");
            (void)rc;
        }

        if (registry && txn && was_started) {
            safe_unbind_txn(registry, txn);
        }

        if (registry && txn) {
            safe_unregister_txn_handle(registry);
        }
    }

    inline void Transaction::move_from(Transaction& other) noexcept {
        m_registry = other.m_registry;
        m_env = other.m_env;
        m_txn = other.m_txn;
        m_mode = other.m_mode;
        m_started = other.m_started;

        other.m_registry = nullptr;
        other.m_env = nullptr;
        other.m_txn = nullptr;
        other.m_started = false;
    }

    inline void Transaction::begin() {
        if (m_started) return;
        bool new_handle = false;
        bool registered_handle = false;
        if (m_txn && m_mode == TransactionMode::READ_ONLY) {
            check_mdbx(mdbx_txn_renew(m_txn), "Failed to renew transaction");
        } else {
            MDBX_txn_flags_t flags = (m_mode == TransactionMode::READ_ONLY) ? MDBX_TXN_RDONLY : MDBX_TXN_READWRITE;
            check_mdbx(mdbx_txn_begin(m_env, nullptr, flags, &m_txn), "Failed to begin transaction");
            new_handle = true;
        }
        try {
            if (new_handle) {
                m_registry->register_txn_handle();
                registered_handle = true;
            }
            m_registry->bind_txn(m_txn);
            m_started = true;
        } catch (...) {
            if (new_handle && m_txn) {
                mdbx_txn_abort(m_txn);
                if (registered_handle) {
                    m_registry->unregister_txn_handle();
                }
                m_txn = nullptr;
            } else if (m_txn && m_mode == TransactionMode::READ_ONLY) {
                mdbx_txn_reset(m_txn);
            }
            throw;
        }
    }

    inline void Transaction::commit() {
        if (!m_txn || !m_started) throw MdbxException("No active transaction to commit.");

        switch (m_mode) {
        case TransactionMode::READ_ONLY:
        {
            MDBX_txn* txn = m_txn;
            check_mdbx(mdbx_txn_reset(txn), "Failed to reset read-only transaction");
            m_started = false;
            safe_unbind_txn(m_registry, txn);
            break;
        }
        case TransactionMode::WRITABLE:
        {
            MDBX_txn* txn = m_txn;

#           if MDBXC_SYNC_ENABLED
            // Pre-commit hook for changelog capture.
            // Runs inside the write transaction, before mdbx_txn_commit. 
            // MdbxException raised here aborts the commit so 
            // user-visible writes and changelog rows stay atomic.
            m_registry->on_pre_commit(txn);
#           endif

            const int rc = mdbx_txn_commit(txn);

            if (rc == MDBX_THREAD_MISMATCH) {
                check_mdbx(rc, "Failed to commit writable transaction");
            }

            // MDBX_SUCCESS or any other error: the native handle is already
            // terminated (or we threw above for THREAD_MISMATCH). Null the
            // wrapper state before tracker cleanup so that a tracker throw
            // does not cause a double-abort in the destructor.
            TransactionTracker* registry = m_registry;

            m_txn = nullptr;
            m_started = false;

            safe_unbind_txn(registry, txn);
            safe_unregister_txn_handle(registry);

            check_mdbx(rc, "Failed to commit writable transaction");
            break;
        }
        };
    }

    inline void Transaction::rollback() {
        if (!m_txn || !m_started) throw MdbxException("No active transaction to rollback.");

        switch (m_mode) {
        case TransactionMode::READ_ONLY:
        {
            MDBX_txn* txn = m_txn;
            const int rc = mdbx_txn_reset(txn);

            if (rc == MDBX_THREAD_MISMATCH) {
                check_mdbx(rc, "Failed to reset read-only transaction");
            }
            check_mdbx(rc, "Failed to reset read-only transaction");

            m_started = false;
            safe_unbind_txn(m_registry, txn);
            break;
        }
        case TransactionMode::WRITABLE:
        {
            MDBX_txn* txn = m_txn;

#           if MDBXC_SYNC_ENABLED
            m_registry->on_discard(txn);
#           endif
            const int rc = mdbx_txn_abort(txn);

            if (rc == MDBX_THREAD_MISMATCH) {
                check_mdbx(rc, "Failed to abort writable transaction");
            }
            check_mdbx(rc, "Failed to abort writable transaction");

            // Only MDBX_SUCCESS reaches this point; the native handle is terminated.
            TransactionTracker* registry = m_registry;

            m_txn = nullptr;
            m_started = false;

            safe_unbind_txn(registry, txn);
            safe_unregister_txn_handle(registry);

            break;
        }
        };
    }

    inline MDBX_txn* Transaction::handle() const noexcept {
        return m_txn;
    }

} // namespace mdbxc
