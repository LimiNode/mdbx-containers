namespace mdbxc {

    inline void TransactionTracker::bind_txn(MDBX_txn* txn) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_thread_txns[std::this_thread::get_id()] = txn;
    }

    inline void TransactionTracker::register_txn_handle() {
        std::lock_guard<std::mutex> lock(m_mutex);
        const std::thread::id tid = std::this_thread::get_id();
        std::unordered_map<std::thread::id, std::size_t>::iterator it =
            m_thread_txn_handle_counts.find(tid);
        if (it == m_thread_txn_handle_counts.end()) {
            it = m_thread_txn_handle_counts.insert(
                std::unordered_map<std::thread::id, std::size_t>::value_type(
                    tid, std::size_t(0))).first;
        }
        ++m_open_txn_handles;
        ++it->second;
    }

    inline void TransactionTracker::unregister_txn_handle() {
        bool empty = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_open_txn_handles > 0) {
                --m_open_txn_handles;
            }
            auto it = m_thread_txn_handle_counts.find(std::this_thread::get_id());
            if (it != m_thread_txn_handle_counts.end()) {
                if (it->second > 1) {
                    --it->second;
                } else {
                    m_thread_txn_handle_counts.erase(it);
                }
            }
            empty = (m_open_txn_handles == 0);
        }
        if (empty) {
            m_txn_cv.notify_all();
        }
    }

    inline void TransactionTracker::unbind_txn(MDBX_txn* expected_txn) {
        bool erased = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            auto it = m_thread_txns.find(std::this_thread::get_id());
            if (it != m_thread_txns.end() && it->second == expected_txn) {
                m_thread_txns.erase(it);
                erased = true;
            }
        }
        if (erased) {
            m_txn_cv.notify_all();
        }
    }

    inline MDBX_txn* TransactionTracker::thread_txn() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_thread_txns.find(std::this_thread::get_id());
        return (it != m_thread_txns.end()) ? it->second : nullptr;
    }

    inline bool TransactionTracker::current_thread_has_txn() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_thread_txns.find(std::this_thread::get_id()) != m_thread_txns.end();
    }

    inline bool TransactionTracker::current_thread_has_txn_handle() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_thread_txn_handle_counts.find(std::this_thread::get_id()) !=
               m_thread_txn_handle_counts.end();
    }

    inline bool TransactionTracker::has_txn_handles() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_open_txn_handles != 0;
    }

    inline void TransactionTracker::wait_for_no_txn_handles() const {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_txn_cv.wait(lock, [this]() {
            return m_open_txn_handles == 0;
        });
    }

} // namespace mdbxc
