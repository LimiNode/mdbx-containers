/// \file test_sync_worker.cpp
/// \brief Background SyncWorker lifecycle tests.

#include <mdbx_containers.hpp>
#include <mdbx_containers/sync.hpp>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

void cleanup(const std::string& path) {
    std::remove(path.c_str());
    std::remove((path + "-lck").c_str());
}

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId node{};
    for (int i = 0; i < 16; ++i) {
        node[i] = static_cast<std::uint8_t>(seed + i);
    }
    return node;
}

mdbxc::sync::ChangeBatch make_raw_batch(const mdbxc::sync::NodeId& origin,
                                        std::uint64_t seq,
                                        const std::string& dbi_name,
                                        std::uint8_t key_seed) {
    mdbxc::sync::ChangeBatch batch;
    batch.origin_node_id = origin;
    batch.seq = seq;
    mdbxc::sync::ChangeOp op;
    op.op_type = mdbxc::sync::ChangeOpType::Put;
    op.dbi_name = dbi_name;
    op.storage_key.push_back(key_seed);
    op.storage_key.push_back(static_cast<std::uint8_t>(seq & 0xffu));
    op.value.push_back(static_cast<std::uint8_t>(0x80u | key_seed));
    op.value.push_back(static_cast<std::uint8_t>(seq & 0xffu));
    batch.ops.push_back(op);
    return batch;
}

std::shared_ptr<mdbxc::Connection> open_env(const std::string& path) {
    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    return mdbxc::Connection::create(config);
}

template<class KVT>
typename KVT::value_type::second_type kv_or_throw(
        const std::shared_ptr<mdbxc::Connection>& conn,
        KVT& kv,
        const typename KVT::value_type::first_type& key,
        const char* what) {
    typename KVT::value_type::second_type out{};
    auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
    if (!kv.try_get(key, out, txn.handle())) {
        throw std::runtime_error(std::string("missing: ") + what);
    }
    return out;
}

class EmptyPeer : public mdbxc::sync::ISyncPeer {
public:
    EmptyPeer() : m_pull_count(0), m_cancel_count(0) {}

    mdbxc::sync::PullResponse pull(
            const mdbxc::sync::PullRequest& request) override {
        (void)request;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_pull_count;
        }
        m_changed.notify_all();
        return mdbxc::sync::PullResponse();
    }

    mdbxc::sync::PushResponse push(
            const mdbxc::sync::PushRequest& request) override {
        (void)request;
        return mdbxc::sync::PushResponse();
    }

    void request_cancel() override {
        std::lock_guard<std::mutex> lock(m_mutex);
        ++m_cancel_count;
    }

    bool wait_for_pulls(int count, std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_changed.wait_for(
            lock, timeout,
            [this, count] { return m_pull_count >= count; });
    }

    int cancel_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_cancel_count;
    }

private:
    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;
    int m_pull_count;
    int m_cancel_count;
};

class NodeSessionApplyObserver : public mdbxc::sync::ISyncApplyObserver {
public:
    NodeSessionApplyObserver() : m_events(0), m_generation(0) {}

    void on_sync_apply_committed(
            const mdbxc::sync::SyncApplyEvent& event) override {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_events;
            m_generation = event.generation;
        }
        m_changed.notify_all();
    }

    bool wait_for_events(std::size_t count,
                         std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_changed.wait_for(
            lock, timeout,
            [this, count] { return m_events >= count; });
    }

    std::size_t events() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_events;
    }

    std::uint64_t generation() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_generation;
    }

private:
    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;
    std::size_t m_events;
    std::uint64_t m_generation;
};

class NodeSessionCaptureSink : public mdbxc::sync::ISyncCaptureSink {
public:
    NodeSessionCaptureSink() : m_records(0), m_flushes(0), m_discards(0) {}

    void record_change(MDBX_txn* txn,
                       const std::string& dbi_name,
                       mdbxc::sync::ChangeOpType op_type,
                       std::uint32_t dbi_flags,
                       const std::vector<std::uint8_t>& storage_key,
                       const std::vector<std::uint8_t>& value) override {
        (void)txn;
        (void)dbi_name;
        (void)op_type;
        (void)dbi_flags;
        (void)storage_key;
        (void)value;
        ++m_records;
    }

    void flush_in_txn(MDBX_txn* txn) override {
        (void)txn;
        ++m_flushes;
    }

    void discard_txn(MDBX_txn* txn) noexcept override {
        (void)txn;
        ++m_discards;
    }

    int records() const {
        return m_records;
    }

private:
    int m_records;
    int m_flushes;
    int m_discards;
};

class FixedPeer : public mdbxc::sync::ISyncPeer {
public:
    explicit FixedPeer(const mdbxc::sync::PullResponse& response)
        : m_response(response) {}

    mdbxc::sync::PullResponse pull(
            const mdbxc::sync::PullRequest& request) override {
        (void)request;
        return m_response;
    }

    mdbxc::sync::PushResponse push(
            const mdbxc::sync::PushRequest& request) override {
        (void)request;
        return mdbxc::sync::PushResponse();
    }

private:
    mdbxc::sync::PullResponse m_response;
};

class BlockingPeer : public mdbxc::sync::ISyncPeer {
public:
    explicit BlockingPeer(const mdbxc::sync::PullResponse& response)
        : m_response(response), m_entered(false), m_release(false),
          m_cancel_count(0) {}

    mdbxc::sync::PullResponse pull(
            const mdbxc::sync::PullRequest& request) override {
        (void)request;
        std::unique_lock<std::mutex> lock(m_mutex);
        m_entered = true;
        m_changed.notify_all();
        m_changed.wait(lock, [this] { return m_release; });
        return m_response;
    }

    mdbxc::sync::PushResponse push(
            const mdbxc::sync::PushRequest& request) override {
        (void)request;
        return mdbxc::sync::PushResponse();
    }

    void request_cancel() override {
        std::lock_guard<std::mutex> lock(m_mutex);
        ++m_cancel_count;
    }

    bool wait_until_entered(std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_changed.wait_for(
            lock, timeout,
            [this] { return m_entered; });
    }

    int cancel_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_cancel_count;
    }

    void release() {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_release = true;
        }
        m_changed.notify_all();
    }

private:
    mdbxc::sync::PullResponse m_response;
    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;
    bool m_entered;
    bool m_release;
    int  m_cancel_count;
};

class CancelableBlockingPeer : public mdbxc::sync::ISyncPeer {
public:
    explicit CancelableBlockingPeer(const mdbxc::sync::PullResponse& response)
        : m_response(response), m_entered(false), m_cancelled(false),
          m_cancel_count(0) {}

    mdbxc::sync::PullResponse pull(
            const mdbxc::sync::PullRequest& request) override {
        (void)request;
        std::unique_lock<std::mutex> lock(m_mutex);
        m_entered = true;
        m_changed.notify_all();
        m_changed.wait(lock, [this] { return m_cancelled; });
        return m_response;
    }

    mdbxc::sync::PushResponse push(
            const mdbxc::sync::PushRequest& request) override {
        (void)request;
        return mdbxc::sync::PushResponse();
    }

    void request_cancel() override {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_cancelled = true;
            ++m_cancel_count;
        }
        m_changed.notify_all();
    }

    bool wait_until_entered(std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_changed.wait_for(
            lock, timeout,
            [this] { return m_entered; });
    }

    int cancel_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_cancel_count;
    }

private:
    mdbxc::sync::PullResponse m_response;
    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;
    bool m_entered;
    bool m_cancelled;
    int  m_cancel_count;
};

class TokenBlockingPeer : public mdbxc::sync::ISyncPeer {
public:
    explicit TokenBlockingPeer(const mdbxc::sync::PullResponse& response)
        : m_response(response), m_entered(false), m_saw_token(false),
          m_saw_token_cancel(false) {}

    mdbxc::sync::PullResponse pull(
            const mdbxc::sync::PullRequest& request) override {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_entered = true;
        m_saw_token = request.cancel_token.can_be_cancelled();
        m_changed.notify_all();
        while (!request.cancel_token.is_cancellation_requested()) {
            m_changed.wait_for(lock, std::chrono::milliseconds(10));
        }
        m_saw_token_cancel = true;
        return m_response;
    }

    mdbxc::sync::PushResponse push(
            const mdbxc::sync::PushRequest& request) override {
        (void)request;
        return mdbxc::sync::PushResponse();
    }

    bool wait_until_entered(std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_changed.wait_for(
            lock, timeout,
            [this] { return m_entered; });
    }

    bool saw_token() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_saw_token;
    }

    bool saw_token_cancel() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_saw_token_cancel;
    }

private:
    mdbxc::sync::PullResponse m_response;
    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;
    bool m_entered;
    bool m_saw_token;
    bool m_saw_token_cancel;
};

class FailingPeer : public mdbxc::sync::ISyncPeer {
public:
    FailingPeer() : m_cancel_count(0) {}

    mdbxc::sync::PullResponse pull(
            const mdbxc::sync::PullRequest& request) override {
        (void)request;
        mdbxc::sync::PullResponse response;
        response.ok = false;
        response.error = "synthetic pull failure";
        return response;
    }

    mdbxc::sync::PushResponse push(
            const mdbxc::sync::PushRequest& request) override {
        (void)request;
        return mdbxc::sync::PushResponse();
    }

    void request_cancel() override {
        std::lock_guard<std::mutex> lock(m_mutex);
        ++m_cancel_count;
    }

    int cancel_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_cancel_count;
    }

private:
    mutable std::mutex m_mutex;
    int m_cancel_count;
};

class RetryAfterPeer : public mdbxc::sync::ISyncPeer {
public:
    RetryAfterPeer() : m_cancel_count(0) {
        m_hint.available = true;
        m_hint.retryable = true;
        m_hint.has_retry_after = true;
        m_hint.retry_after_seconds = 1;
    }

    mdbxc::sync::PullResponse pull(
            const mdbxc::sync::PullRequest& request) override {
        (void)request;
        mdbxc::sync::PullResponse response;
        response.ok = false;
        response.error = "retry later";
        return response;
    }

    mdbxc::sync::PushResponse push(
            const mdbxc::sync::PushRequest& request) override {
        (void)request;
        return mdbxc::sync::PushResponse();
    }

    mdbxc::sync::SyncTransportRetryHint last_retry_hint() const override {
        return m_hint;
    }

    void request_cancel() override {
        std::lock_guard<std::mutex> lock(m_mutex);
        ++m_cancel_count;
    }

    int cancel_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_cancel_count;
    }

private:
    mutable std::mutex m_mutex;
    mdbxc::sync::SyncTransportRetryHint m_hint;
    int m_cancel_count;
};

class PermanentHintPeer : public mdbxc::sync::ISyncPeer {
public:
    PermanentHintPeer() {
        m_hint.available = true;
        m_hint.retryable = false;
    }

    mdbxc::sync::PullResponse pull(
            const mdbxc::sync::PullRequest& request) override {
        (void)request;
        mdbxc::sync::PullResponse response;
        response.ok = false;
        response.error = "permanent transport failure";
        return response;
    }

    mdbxc::sync::PushResponse push(
            const mdbxc::sync::PushRequest& request) override {
        (void)request;
        return mdbxc::sync::PushResponse();
    }

    mdbxc::sync::SyncTransportRetryHint last_retry_hint() const override {
        return m_hint;
    }

private:
    mdbxc::sync::SyncTransportRetryHint m_hint;
};

class SelfStoppingPeer : public mdbxc::sync::ISyncPeer {
public:
    SelfStoppingPeer()
        : m_worker(nullptr),
          m_entered_pull(false),
          m_allow_stop(false),
          m_saw_logic_error(false) {}

    void set_worker(mdbxc::sync::SyncWorker* worker) {
        m_worker = worker;
    }

    mdbxc::sync::PullResponse pull(
            const mdbxc::sync::PullRequest& request) override {
        (void)request;
        if (m_worker == nullptr) {
            throw std::runtime_error("self-stopping peer has no worker");
        }
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_entered_pull = true;
            m_changed.notify_all();
            while (!m_allow_stop) {
                m_changed.wait(lock);
            }
        }
        try {
            m_worker->stop();
        } catch (const std::logic_error& e) {
            if (std::string(e.what()).find("cannot join itself") ==
                std::string::npos) {
                throw;
            }
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_saw_logic_error = true;
            }
            m_changed.notify_all();
        }
        return mdbxc::sync::PullResponse();
    }

    mdbxc::sync::PushResponse push(
            const mdbxc::sync::PushRequest& request) override {
        (void)request;
        return mdbxc::sync::PushResponse();
    }

    bool saw_logic_error() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_saw_logic_error;
    }

    bool wait_until_pull_entered(std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_changed.wait_for(
                lock, timeout,
                [this]() { return m_entered_pull; });
    }

    void allow_stop() {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_allow_stop = true;
        }
        m_changed.notify_all();
    }

private:
    mdbxc::sync::SyncWorker* m_worker;
    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;
    bool m_entered_pull;
    bool m_allow_stop;
    bool m_saw_logic_error;
};

class RecordingWorkerObserver : public mdbxc::sync::ISyncWorkerObserver {
public:
    struct BackoffRecord {
        mdbxc::sync::SyncWorkerRoundResult result;
        std::chrono::milliseconds delay;
    };

    void on_sync_worker_page_applied(
            const mdbxc::sync::SyncWorkerPageEvent& event) override {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_pages.push_back(event);
        }
        m_changed.notify_all();
    }

    void on_sync_worker_stage_changed(
            const mdbxc::sync::SyncWorkerStageEvent& event) override {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_stages.push_back(event);
        }
        m_changed.notify_all();
    }

    void on_sync_worker_round_completed(
            const mdbxc::sync::SyncWorkerRoundResult& result) override {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_rounds.push_back(result);
        }
        m_changed.notify_all();
    }

    void on_sync_worker_backoff(
            const mdbxc::sync::SyncWorkerRoundResult& result,
            std::chrono::milliseconds delay) override {
        BackoffRecord record;
        record.result = result;
        record.delay = delay;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_backoffs.push_back(record);
        }
        m_changed.notify_all();
    }

    std::vector<mdbxc::sync::SyncWorkerPageEvent> pages() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_pages;
    }

    std::vector<mdbxc::sync::SyncWorkerRoundResult> rounds() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_rounds;
    }

    std::vector<BackoffRecord> backoffs() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_backoffs;
    }

    std::vector<mdbxc::sync::SyncWorkerStageEvent> stages() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_stages;
    }

    bool wait_for_backoffs(std::size_t count,
                           std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_changed.wait_for(
            lock, timeout,
            [this, count] { return m_backoffs.size() >= count; });
    }

    bool wait_for_batches(std::size_t count,
                          std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_changed.wait_for(
            lock, timeout,
            [this, count] { return applied_batches() >= count; });
    }

private:
    std::size_t applied_batches() const {
        std::size_t total = 0;
        for (std::size_t i = 0; i < m_pages.size(); ++i) {
            total += m_pages[i].batches_applied;
        }
        return total;
    }

    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;
    std::vector<mdbxc::sync::SyncWorkerPageEvent> m_pages;
    std::vector<mdbxc::sync::SyncWorkerRoundResult> m_rounds;
    std::vector<BackoffRecord> m_backoffs;
    std::vector<mdbxc::sync::SyncWorkerStageEvent> m_stages;
};

class StopOnApplyStartedObserver : public RecordingWorkerObserver {
public:
    StopOnApplyStartedObserver() : m_worker(nullptr) {}

    void set_worker(mdbxc::sync::SyncWorker* worker) {
        m_worker = worker;
    }

    void on_sync_worker_stage_changed(
            const mdbxc::sync::SyncWorkerStageEvent& event) override {
        RecordingWorkerObserver::on_sync_worker_stage_changed(event);
        if (event.stage == mdbxc::sync::SyncWorkerStage::ApplyStarted) {
            if (m_worker == nullptr) {
                throw std::runtime_error("stop observer has no worker");
            }
            m_worker->request_stop();
        }
    }

private:
    mdbxc::sync::SyncWorker* m_worker;
};

class ThreadedStopOnApplyStartedObserver : public RecordingWorkerObserver {
public:
    ThreadedStopOnApplyStartedObserver() : m_worker(nullptr) {}

    void set_worker(mdbxc::sync::SyncWorker* worker) {
        m_worker = worker;
    }

    void on_sync_worker_stage_changed(
            const mdbxc::sync::SyncWorkerStageEvent& event) override {
        RecordingWorkerObserver::on_sync_worker_stage_changed(event);
        if (event.stage == mdbxc::sync::SyncWorkerStage::ApplyStarted) {
            if (m_worker == nullptr) {
                throw std::runtime_error("threaded stop observer has no worker");
            }
            std::thread stopper(
                &mdbxc::sync::SyncWorker::request_stop, m_worker);
            stopper.join();
        }
    }

private:
    mdbxc::sync::SyncWorker* m_worker;
};

class ThrowingRoundObserver : public mdbxc::sync::ISyncWorkerObserver {
public:
    void on_sync_worker_round_completed(
            const mdbxc::sync::SyncWorkerRoundResult& result) override {
        (void)result;
        throw std::runtime_error("observer boom");
    }
};

class ThrowingPageObserver : public mdbxc::sync::ISyncWorkerObserver {
public:
    ThrowingPageObserver() : m_page_count(0) {}

    void on_sync_worker_page_applied(
            const mdbxc::sync::SyncWorkerPageEvent& event) override {
        (void)event;
        ++m_page_count;
        throw std::runtime_error("page observer boom");
    }

    int page_count() const {
        return m_page_count;
    }

private:
    int m_page_count;
};

class ThrowingBackoffObserver : public mdbxc::sync::ISyncWorkerObserver {
public:
    ThrowingBackoffObserver() : m_backoff_count(0) {}

    void on_sync_worker_backoff(
            const mdbxc::sync::SyncWorkerRoundResult& result,
            std::chrono::milliseconds delay) override {
        (void)result;
        (void)delay;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_backoff_count;
        }
        m_changed.notify_all();
        throw std::runtime_error("backoff observer boom");
    }

    bool wait_for_backoffs(int count,
                           std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_changed.wait_for(
            lock, timeout,
            [this, count] { return m_backoff_count >= count; });
    }

private:
    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;
    int m_backoff_count;
};

class ThrowingStageObserver : public mdbxc::sync::ISyncWorkerObserver {
public:
    ThrowingStageObserver() : m_stage_count(0) {}

    void on_sync_worker_stage_changed(
            const mdbxc::sync::SyncWorkerStageEvent& event) override {
        (void)event;
        ++m_stage_count;
        throw std::runtime_error("stage observer boom");
    }

    int stage_count() const {
        return m_stage_count;
    }

private:
    int m_stage_count;
};

class ContextHttpClient : public mdbxc::sync::IHttpSyncClient {
public:
    ContextHttpClient(mdbxc::sync::HttpSyncServerMiddleware& server,
                      const std::string& bearer_token,
                      const std::string& remote_address)
        : m_server(server),
          m_bearer_token(bearer_token),
          m_remote_address(remote_address),
          m_post_count(0),
          m_cancel_count(0),
          m_saw_cancellable_token(false) {}

    mdbxc::sync::HttpSyncResponse post(
            const std::string& target,
            const std::string& content_type,
            const std::vector<std::uint8_t>& body,
            const mdbxc::sync::CancellationToken& cancel_token) override {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_post_count;
            m_saw_cancellable_token =
                m_saw_cancellable_token ||
                cancel_token.can_be_cancelled();
        }
        m_changed.notify_all();

        mdbxc::sync::HttpSyncRequest request;
        request.method = mdbxc::sync::HttpSyncRoutes::method_post();
        request.target = target;
        request.content_type = content_type;
        request.remote_address = m_remote_address;
        request.body = body;
        mdbxc::sync::http_add_header(
            request.headers, "Authorization",
            std::string("Bearer ") + m_bearer_token);
        return m_server.handle(request);
    }

    void request_cancel() override {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_cancel_count;
        }
        m_changed.notify_all();
    }

    bool wait_for_posts(std::size_t count,
                        std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_changed.wait_for(
            lock, timeout,
            [this, count] { return m_post_count >= count; });
    }

    std::size_t post_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_post_count;
    }

    std::size_t cancel_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_cancel_count;
    }

    bool saw_cancellable_token() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_saw_cancellable_token;
    }

private:
    mdbxc::sync::HttpSyncServerMiddleware& m_server;
    std::string m_bearer_token;
    std::string m_remote_address;
    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;
    std::size_t m_post_count;
    std::size_t m_cancel_count;
    bool m_saw_cancellable_token;
};

class BlockingHttpClient : public mdbxc::sync::IHttpSyncClient {
public:
    BlockingHttpClient()
        : m_entered(false),
          m_cancel_requested(false),
          m_saw_cancellable_token(false),
          m_saw_token_cancel(false),
          m_cancel_count(0) {}

    mdbxc::sync::HttpSyncResponse post(
            const std::string& target,
            const std::string& content_type,
            const std::vector<std::uint8_t>& body,
            const mdbxc::sync::CancellationToken& cancel_token) override {
        (void)target;
        (void)content_type;
        (void)body;

        std::unique_lock<std::mutex> lock(m_mutex);
        m_entered = true;
        m_saw_cancellable_token = cancel_token.can_be_cancelled();
        m_changed.notify_all();
        while (!m_cancel_requested &&
               !cancel_token.is_cancellation_requested()) {
            m_changed.wait_for(lock, std::chrono::milliseconds(10));
        }
        m_saw_token_cancel = cancel_token.is_cancellation_requested();

        mdbxc::sync::HttpSyncResponse response;
        response.status_code = 503;
        response.content_type = "text/plain; charset=utf-8";
        response.error = "transport cancelled";
        response.body.assign(response.error.begin(), response.error.end());
        return response;
    }

    void request_cancel() override {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_cancel_requested = true;
            ++m_cancel_count;
        }
        m_changed.notify_all();
    }

    bool wait_until_entered(std::chrono::milliseconds timeout) const {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_changed.wait_for(
            lock, timeout,
            [this] { return m_entered; });
    }

    std::size_t cancel_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_cancel_count;
    }

    bool saw_cancellable_token() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_saw_cancellable_token;
    }

    bool saw_token_cancel() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_saw_token_cancel;
    }

private:
    mutable std::mutex m_mutex;
    mutable std::condition_variable m_changed;
    bool m_entered;
    bool m_cancel_requested;
    bool m_saw_cancellable_token;
    bool m_saw_token_cancel;
    std::size_t m_cancel_count;
};

void expect_invalid_options(mdbxc::sync::SyncEngine& engine,
                            mdbxc::sync::ISyncPeer& peer,
                            const mdbxc::sync::SyncWorkerOptions& options,
                            const char* name) {
    bool threw = false;
    try {
        mdbxc::sync::SyncWorker worker(engine, peer, options);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    if (!threw) {
        throw std::runtime_error(
            std::string("SyncWorker accepted invalid options: ") + name);
    }
}

void test_worker_run_once_drains_paginated_pull() {
    using namespace mdbxc;
    const std::string primary_path = "test_worker_primary.mdbx";
    const std::string replica_path = "test_worker_replica.mdbx";
    cleanup(primary_path);
    cleanup(replica_path);

    std::shared_ptr<Connection> primary_conn = open_env(primary_path);
    std::shared_ptr<Connection> replica_conn = open_env(replica_path);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_id = make_node(0xD0);

    sync::SyncEngine primary_engine(primary_conn);
    sync::SyncEngine replica_engine(replica_conn);
    primary_engine.initialize_local_identity(primary_node, db_id);
    replica_engine.initialize_local_identity(replica_node, db_id);

    sync::ThreadLocalChangeAccumulator sink(primary_conn);
    primary_conn->attach_sync_capture(&sink);
    {
        KeyValueTable<int, int> kv(primary_conn, "kv");
        for (int i = 1; i <= 5; ++i) {
            kv.insert_or_assign(i, i * 10);
        }
    }
    primary_conn->detach_sync_capture();

    sync::DirectSyncPeer peer(&primary_engine);
    RecordingWorkerObserver observer;
    sync::SyncWorkerOptions options;
    options.max_batches = 2;
    options.idle_interval = std::chrono::milliseconds(1);
    options.observer = &observer;

    sync::SyncWorker worker(replica_engine, peer, options);
    const sync::SyncWorkerRoundResult result = worker.run_once();
    if (!result.ok) {
        throw std::runtime_error("worker run_once failed: " + result.error);
    }
    if (result.pages_pulled != 3u || result.batches_applied != 5u) {
        throw std::runtime_error("worker did not drain paginated pull");
    }
    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("worker run_once should finish Stopped");
    }
    const std::vector<sync::SyncWorkerPageEvent> pages = observer.pages();
    if (pages.size() != 3u) {
        throw std::runtime_error("worker observer did not see all pages");
    }
    if (pages[0].batches_applied != 2u || !pages[0].has_more ||
        pages[0].applied_cursor.last_seq_for(primary_node) != 2u) {
        throw std::runtime_error("worker observer first page mismatch");
    }
    if (pages[1].batches_applied != 2u || !pages[1].has_more ||
        pages[1].applied_cursor.last_seq_for(primary_node) != 4u) {
        throw std::runtime_error("worker observer second page mismatch");
    }
    if (pages[2].batches_applied != 1u || pages[2].has_more ||
        pages[2].applied_cursor.last_seq_for(primary_node) != 5u) {
        throw std::runtime_error("worker observer final page mismatch");
    }
    const std::vector<sync::SyncWorkerRoundResult> rounds = observer.rounds();
    if (rounds.size() != 1u || !rounds[0].ok ||
        rounds[0].pages_pulled != 3u ||
        rounds[0].batches_applied != 5u) {
        throw std::runtime_error("worker observer round mismatch");
    }
    const std::vector<sync::SyncWorkerStageEvent> stages =
        observer.stages();
    if (stages.size() != 14u) {
        throw std::runtime_error("worker observer stage count mismatch");
    }
    if (stages.front().stage != sync::SyncWorkerStage::RoundStarted ||
        stages.back().stage != sync::SyncWorkerStage::RoundCompleted ||
        !stages.back().ok || stages.back().pages_pulled != 3u ||
        stages.back().batches_applied != 5u) {
        throw std::runtime_error("worker observer round stage mismatch");
    }
    std::size_t pull_started = 0;
    std::size_t pull_finished = 0;
    std::size_t apply_started = 0;
    std::size_t apply_finished = 0;
    for (std::size_t i = 0; i < stages.size(); ++i) {
        if (stages[i].stage == sync::SyncWorkerStage::PullStarted) {
            ++pull_started;
        } else if (stages[i].stage ==
                   sync::SyncWorkerStage::PullFinished) {
            ++pull_finished;
        } else if (stages[i].stage ==
                   sync::SyncWorkerStage::ApplyStarted) {
            ++apply_started;
        } else if (stages[i].stage ==
                   sync::SyncWorkerStage::ApplyFinished) {
            ++apply_finished;
        }
    }
    if (pull_started != 3u || pull_finished != 3u ||
        apply_started != 3u || apply_finished != 3u) {
        throw std::runtime_error("worker observer page stage mismatch");
    }
    if (!stages[4].progress.remote_tail_known ||
        stages[4].progress.batches_applied != 2u ||
        stages[4].progress.batches_remaining != 3u ||
        stages[4].progress.batches_total != 5u) {
        throw std::runtime_error("worker observer first progress mismatch");
    }
    if (!stages[12].progress.remote_tail_known ||
        stages[12].progress.batches_applied != 5u ||
        stages[12].progress.batches_remaining != 0u ||
        stages[12].progress.batches_total != 5u) {
        throw std::runtime_error("worker observer final progress mismatch");
    }
    if (!rounds[0].progress.remote_tail_known ||
        rounds[0].progress.batches_applied != 5u ||
        rounds[0].progress.batches_remaining != 0u ||
        rounds[0].progress.batches_total != 5u) {
        throw std::runtime_error("worker observer round progress mismatch");
    }
    const sync::SyncWorkerStatus status = worker.status();
    if (status.state != sync::SyncWorkerState::Stopped ||
        !status.last_stage_known || !status.last_round_known ||
        status.round_active || status.backoff_active) {
        throw std::runtime_error("worker status lifecycle mismatch");
    }
    if (status.rounds_started != 1u ||
        status.rounds_completed != 1u ||
        status.rounds_succeeded != 1u ||
        status.rounds_failed != 0u) {
        throw std::runtime_error("worker status counters mismatch");
    }
    if (status.current_stage != sync::SyncWorkerStage::RoundCompleted ||
        status.last_stage.stage != sync::SyncWorkerStage::RoundCompleted ||
        !status.last_round.ok ||
        status.last_round.pages_pulled != 3u ||
        status.last_round.batches_applied != 5u) {
        throw std::runtime_error("worker status round snapshot mismatch");
    }
    if (!status.last_progress.remote_tail_known ||
        status.last_progress.batches_applied != 5u ||
        status.last_progress.batches_remaining != 0u ||
        status.last_backoff_delay != std::chrono::milliseconds::zero() ||
        status.last_round_finished_at < status.last_round_started_at) {
        throw std::runtime_error("worker status progress snapshot mismatch");
    }

    KeyValueTable<int, int> replica_kv(replica_conn, "kv");
    for (int i = 1; i <= 5; ++i) {
        if (kv_or_throw(replica_conn, replica_kv, i, "replica kv") != i * 10) {
            throw std::runtime_error("replica value mismatch after worker sync");
        }
    }

    primary_conn->disconnect();
    replica_conn->disconnect();
    cleanup(primary_path);
    cleanup(replica_path);
}

void test_worker_start_stop_idle() {
    using namespace mdbxc;
    const std::string path = "test_worker_idle.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    EmptyPeer peer;
    sync::SyncWorkerOptions options;
    options.idle_interval = std::chrono::milliseconds(10000);
    sync::SyncWorker worker(engine, peer, options);

    worker.start();
    if (!peer.wait_for_pulls(1, std::chrono::milliseconds(2000))) {
        throw std::runtime_error("worker did not perform initial pull");
    }
    if (!worker.wait_until_state(sync::SyncWorkerState::Idle,
                                 std::chrono::milliseconds(2000))) {
        throw std::runtime_error("worker did not enter Idle");
    }
    worker.stop();
    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("worker did not stop");
    }
    if (peer.cancel_count() != 0) {
        throw std::runtime_error("idle stop should not cancel peer transport");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_background_pull_over_http_transport() {
    using namespace mdbxc;
    const std::string primary_path = "test_worker_http_primary.mdbx";
    const std::string replica_path = "test_worker_http_replica.mdbx";
    cleanup(primary_path);
    cleanup(replica_path);

    std::shared_ptr<Connection> primary_conn = open_env(primary_path);
    std::shared_ptr<Connection> replica_conn = open_env(replica_path);

    const sync::NodeId primary_node = make_node(0xC0);
    const sync::NodeId replica_node = make_node(0xC1);
    const sync::DbId db_id = make_node(0xD3);

    sync::SyncEngine primary_engine(primary_conn);
    sync::SyncEngine replica_engine(replica_conn);
    primary_engine.initialize_local_identity(primary_node, db_id);
    replica_engine.initialize_local_identity(replica_node, db_id);

    sync::ThreadLocalChangeAccumulator sink(primary_conn);
    primary_conn->attach_sync_capture(&sink);
    {
        KeyValueTable<int, int> kv(primary_conn, "kv");
        kv.insert_or_assign(1, 10);
        kv.insert_or_assign(2, 20);
    }
    primary_conn->detach_sync_capture();

    sync::HttpBearerNodeIdentityPolicy identity;
    identity.allow_token_for_node("replica-token", replica_node);
    identity.allow_db_id_for_token("replica-token", db_id);
    sync::HttpSyncServer primary_server(primary_engine);
    sync::HttpSyncServerMiddleware guarded_server(primary_server, &identity);
    ContextHttpClient http_client(
        guarded_server, "replica-token", "127.0.0.1");
    sync::HttpSyncPeer http_peer(http_client);

    RecordingWorkerObserver observer;
    sync::SyncWorkerOptions options;
    options.max_batches = 1;
    options.idle_interval = std::chrono::milliseconds(10000);
    options.observer = &observer;

    sync::SyncWorker worker(replica_engine, http_peer, options);
    worker.start();
    if (!observer.wait_for_batches(2u, std::chrono::milliseconds(5000))) {
        throw std::runtime_error("HTTP worker did not apply pulled batches");
    }
    if (!http_client.wait_for_posts(2u, std::chrono::milliseconds(5000))) {
        throw std::runtime_error("HTTP worker did not perform paginated posts");
    }
    worker.stop();

    if (!http_client.saw_cancellable_token()) {
        throw std::runtime_error("HTTP worker did not pass cancellation token");
    }
    if (http_client.cancel_count() != 0u) {
        throw std::runtime_error("idle HTTP worker stop should not cancel post");
    }

    KeyValueTable<int, int> replica_kv(replica_conn, "kv");
    if (kv_or_throw(replica_conn, replica_kv, 1, "replica kv[1]") != 10 ||
        kv_or_throw(replica_conn, replica_kv, 2, "replica kv[2]") != 20) {
        throw std::runtime_error("HTTP worker replica values mismatch");
    }

    primary_conn->disconnect();
    replica_conn->disconnect();
    cleanup(primary_path);
    cleanup(replica_path);
}

void test_worker_stop_cancels_http_transport_peer() {
    using namespace mdbxc;
    const std::string path = "test_worker_http_cancel.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xC2), make_node(0xD4));

    BlockingHttpClient client;
    sync::HttpSyncPeer peer(client);
    sync::SyncWorkerOptions options;
    options.idle_interval = std::chrono::milliseconds(10000);
    sync::SyncWorker worker(engine, peer, options);

    worker.start();
    if (!client.wait_until_entered(std::chrono::milliseconds(2000))) {
        throw std::runtime_error("HTTP worker did not enter blocking post");
    }
    if (!client.saw_cancellable_token()) {
        throw std::runtime_error("HTTP blocking client did not receive token");
    }
    worker.request_stop();
    worker.join();

    if (client.cancel_count() != 1u) {
        throw std::runtime_error("HTTP worker did not request client cancel");
    }
    if (!client.saw_token_cancel()) {
        throw std::runtime_error("HTTP worker did not cancel request token");
    }
    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("HTTP cancelled worker did not stop");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_backoff_on_pull_error() {
    using namespace mdbxc;
    const std::string path = "test_worker_backoff.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    FailingPeer peer;
    RecordingWorkerObserver observer;
    sync::SyncWorkerOptions options;
    options.initial_backoff = std::chrono::milliseconds(10000);
    options.max_backoff = std::chrono::milliseconds(10000);
    options.observer = &observer;
    sync::SyncWorker worker(engine, peer, options);

    worker.start();
    if (!worker.wait_until_state(sync::SyncWorkerState::Backoff,
                                 std::chrono::milliseconds(2000))) {
        throw std::runtime_error("worker did not enter Backoff after pull error");
    }
    if (worker.last_error().find("synthetic pull failure") == std::string::npos) {
        throw std::runtime_error("worker did not record pull error");
    }
    if (!observer.wait_for_backoffs(1u, std::chrono::milliseconds(2000))) {
        throw std::runtime_error("worker observer did not see backoff");
    }
    const sync::SyncWorkerStatus status = worker.status();
    if (status.state != sync::SyncWorkerState::Backoff ||
        !status.backoff_active ||
        status.last_backoff_delay != std::chrono::milliseconds(10000) ||
        status.current_stage != sync::SyncWorkerStage::BackoffStarted ||
        !status.last_round_known ||
        status.last_round.ok ||
        status.rounds_failed != 1u) {
        throw std::runtime_error("worker backoff status snapshot mismatch");
    }
    const std::vector<sync::SyncWorkerRoundResult> rounds = observer.rounds();
    if (rounds.empty() || rounds[0].ok ||
        rounds[0].error.find("synthetic pull failure") == std::string::npos) {
        throw std::runtime_error("worker observer round error mismatch");
    }
    const std::vector<RecordingWorkerObserver::BackoffRecord> backoffs =
        observer.backoffs();
    if (backoffs[0].delay != std::chrono::milliseconds(10000) ||
        backoffs[0].result.ok ||
        backoffs[0].result.error.find("synthetic pull failure") ==
            std::string::npos) {
        throw std::runtime_error("worker observer backoff mismatch");
    }
    worker.stop();
    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("backoff worker did not stop");
    }
    if (peer.cancel_count() != 0) {
        throw std::runtime_error("backoff stop should not cancel peer transport");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_backoff_uses_retry_after_hint() {
    using namespace mdbxc;
    const std::string path = "test_worker_retry_after.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    RetryAfterPeer peer;
    RecordingWorkerObserver observer;
    sync::SyncWorkerOptions options;
    options.initial_backoff = std::chrono::milliseconds(10000);
    options.max_backoff = std::chrono::milliseconds(10000);
    options.observer = &observer;
    sync::SyncWorker worker(engine, peer, options);

    worker.start();
    if (!observer.wait_for_backoffs(1u, std::chrono::milliseconds(2000))) {
        throw std::runtime_error("worker observer did not see retry backoff");
    }
    const std::vector<RecordingWorkerObserver::BackoffRecord> backoffs =
        observer.backoffs();
    if (backoffs[0].delay != std::chrono::milliseconds(1000)) {
        throw std::runtime_error("worker did not use Retry-After backoff");
    }
    if (!backoffs[0].result.retry_hint.available ||
        !backoffs[0].result.retry_hint.retryable ||
        !backoffs[0].result.retry_hint.has_retry_after ||
        backoffs[0].result.retry_hint.retry_after_seconds != 1u) {
        throw std::runtime_error("worker did not publish retry hint");
    }
    worker.stop();
    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("retry-after worker did not stop");
    }
    if (peer.cancel_count() != 0) {
        throw std::runtime_error(
            "retry-after backoff stop should not cancel peer transport");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_permanent_hint_keeps_retrying_by_default() {
    using namespace mdbxc;
    const std::string path = "test_worker_permanent_default.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    PermanentHintPeer peer;
    RecordingWorkerObserver observer;
    sync::SyncWorkerOptions options;
    options.initial_backoff = std::chrono::milliseconds(10000);
    options.max_backoff = std::chrono::milliseconds(10000);
    options.observer = &observer;
    sync::SyncWorker worker(engine, peer, options);

    worker.start();
    if (!observer.wait_for_backoffs(1u, std::chrono::milliseconds(2000))) {
        throw std::runtime_error(
            "permanent hint default worker did not enter backoff");
    }
    const sync::SyncWorkerStatus status = worker.status();
    if (status.state != sync::SyncWorkerState::Backoff ||
        !status.backoff_active ||
        status.last_backoff_delay != std::chrono::milliseconds(10000) ||
        !status.last_round_known ||
        status.last_round.ok ||
        !status.last_round.retry_hint.available ||
        status.last_round.retry_hint.retryable ||
        status.rounds_failed != 1u) {
        throw std::runtime_error(
            "permanent hint default status snapshot mismatch");
    }
    worker.stop();
    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("permanent hint default worker did not stop");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_permanent_hint_can_stop_worker() {
    using namespace mdbxc;
    const std::string path = "test_worker_permanent_stop.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    PermanentHintPeer peer;
    RecordingWorkerObserver observer;
    sync::SyncWorkerOptions options;
    options.initial_backoff = std::chrono::milliseconds(10000);
    options.max_backoff = std::chrono::milliseconds(10000);
    options.observer = &observer;
    options.permanent_failure_policy =
        sync::SyncWorkerPermanentFailurePolicy::StopWorker;
    sync::SyncWorker worker(engine, peer, options);

    worker.start();
    if (!worker.wait_until_state(sync::SyncWorkerState::Failed,
                                 std::chrono::milliseconds(2000))) {
        throw std::runtime_error(
            "permanent hint worker did not enter Failed state");
    }
    worker.join();

    const sync::SyncWorkerStatus status = worker.status();
    if (status.state != sync::SyncWorkerState::Failed ||
        status.backoff_active ||
        status.last_backoff_delay != std::chrono::milliseconds(0) ||
        !status.last_round_known ||
        status.last_round.ok ||
        !status.last_round.retry_hint.available ||
        status.last_round.retry_hint.retryable ||
        status.rounds_failed != 1u ||
        status.last_error.find("permanent transport failure") ==
            std::string::npos) {
        throw std::runtime_error("permanent hint stop status mismatch");
    }
    if (!observer.backoffs().empty()) {
        throw std::runtime_error(
            "permanent hint stop worker should not enter backoff");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_observer_exception_does_not_fail_round() {
    using namespace mdbxc;
    const std::string path = "test_worker_observer_exception.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    EmptyPeer peer;
    ThrowingRoundObserver observer;
    sync::SyncWorkerOptions options;
    options.observer = &observer;
    sync::SyncWorker worker(engine, peer, options);

    const sync::SyncWorkerRoundResult result = worker.run_once();
    if (!result.ok) {
        throw std::runtime_error("observer exception failed sync round");
    }
    if (!worker.last_error().empty()) {
        throw std::runtime_error("observer exception polluted last_error");
    }
    if (worker.last_observer_error().find("observer boom") ==
        std::string::npos) {
        throw std::runtime_error("observer exception was not recorded");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_stage_observer_exception_does_not_fail_round() {
    using namespace mdbxc;
    const std::string path = "test_worker_stage_observer_exception.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    EmptyPeer peer;
    ThrowingStageObserver observer;
    sync::SyncWorkerOptions options;
    options.observer = &observer;
    sync::SyncWorker worker(engine, peer, options);

    const sync::SyncWorkerRoundResult result = worker.run_once();
    if (!result.ok) {
        throw std::runtime_error("stage observer exception failed sync round");
    }
    if (observer.stage_count() == 0) {
        throw std::runtime_error("stage observer was not called");
    }
    if (!worker.last_error().empty()) {
        throw std::runtime_error("stage observer exception polluted last_error");
    }
    if (worker.last_observer_error().find("stage observer boom") ==
        std::string::npos) {
        throw std::runtime_error("stage observer exception was not recorded");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_observer_exception_keeps_pull_error() {
    using namespace mdbxc;
    const std::string path = "test_worker_observer_pull_error.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    FailingPeer peer;
    ThrowingRoundObserver observer;
    sync::SyncWorkerOptions options;
    options.observer = &observer;
    sync::SyncWorker worker(engine, peer, options);

    const sync::SyncWorkerRoundResult result = worker.run_once();
    if (result.ok ||
        result.error.find("synthetic pull failure") == std::string::npos) {
        throw std::runtime_error("failing peer result was not preserved");
    }
    if (worker.state() != sync::SyncWorkerState::Failed) {
        throw std::runtime_error("failing observer round should leave Failed");
    }
    if (worker.last_error().find("synthetic pull failure") ==
        std::string::npos) {
        throw std::runtime_error("pull error was not kept in last_error");
    }
    if (worker.last_observer_error().find("observer boom") ==
        std::string::npos) {
        throw std::runtime_error("round observer error was not recorded");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_backoff_observer_exception_keeps_pull_error() {
    using namespace mdbxc;
    const std::string path = "test_worker_observer_backoff_error.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    FailingPeer peer;
    ThrowingBackoffObserver observer;
    sync::SyncWorkerOptions options;
    options.initial_backoff = std::chrono::milliseconds(10000);
    options.max_backoff = std::chrono::milliseconds(10000);
    options.observer = &observer;
    sync::SyncWorker worker(engine, peer, options);

    worker.start();
    if (!observer.wait_for_backoffs(1, std::chrono::milliseconds(2000))) {
        throw std::runtime_error("backoff observer was not called");
    }
    if (worker.last_error().find("synthetic pull failure") ==
        std::string::npos) {
        throw std::runtime_error("backoff observer overwrote pull error");
    }
    bool observer_error_recorded = false;
    for (int i = 0; i < 200; ++i) {
        if (worker.last_observer_error().find("backoff observer boom") !=
            std::string::npos) {
            observer_error_recorded = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!observer_error_recorded) {
        throw std::runtime_error("backoff observer error was not recorded");
    }
    worker.stop();
    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("backoff observer worker did not stop");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_page_observer_exception_does_not_fail_round() {
    using namespace mdbxc;
    const std::string primary_path = "test_worker_page_observer_primary.mdbx";
    const std::string replica_path = "test_worker_page_observer_replica.mdbx";
    cleanup(primary_path);
    cleanup(replica_path);

    std::shared_ptr<Connection> primary_conn = open_env(primary_path);
    std::shared_ptr<Connection> replica_conn = open_env(replica_path);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_id = make_node(0xD0);

    sync::SyncEngine primary_engine(primary_conn);
    sync::SyncEngine replica_engine(replica_conn);
    primary_engine.initialize_local_identity(primary_node, db_id);
    replica_engine.initialize_local_identity(replica_node, db_id);

    sync::ThreadLocalChangeAccumulator sink(primary_conn);
    primary_conn->attach_sync_capture(&sink);
    {
        KeyValueTable<int, int> kv(primary_conn, "kv");
        kv.insert_or_assign(1, 10);
    }
    primary_conn->detach_sync_capture();

    sync::DirectSyncPeer peer(&primary_engine);
    ThrowingPageObserver observer;
    sync::SyncWorkerOptions options;
    options.observer = &observer;
    sync::SyncWorker worker(replica_engine, peer, options);

    const sync::SyncWorkerRoundResult result = worker.run_once();
    if (!result.ok) {
        throw std::runtime_error("page observer exception failed sync round");
    }
    if (observer.page_count() != 1) {
        throw std::runtime_error("page observer was not called once");
    }
    if (!worker.last_error().empty()) {
        throw std::runtime_error("page observer exception polluted last_error");
    }
    if (worker.last_observer_error().find("page observer boom") ==
        std::string::npos) {
        throw std::runtime_error("page observer error was not recorded");
    }

    KeyValueTable<int, int> replica_kv(replica_conn, "kv");
    if (kv_or_throw(replica_conn, replica_kv, 1, "replica kv") != 10) {
        throw std::runtime_error("replica value missing after page observer error");
    }

    primary_conn->disconnect();
    replica_conn->disconnect();
    cleanup(primary_path);
    cleanup(replica_path);
}

void test_worker_rejects_invalid_options() {
    using namespace mdbxc;
    const std::string path = "test_worker_invalid_options.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    EmptyPeer peer;
    sync::SyncWorkerOptions options;

    sync::SyncWorkerOptions invalid = options;
    invalid.max_batches = 0;
    expect_invalid_options(engine, peer, invalid, "max_batches");

    invalid = options;
    invalid.max_bytes = 0;
    expect_invalid_options(engine, peer, invalid, "max_bytes");

    invalid = options;
    invalid.max_single_batch_bytes = 0;
    expect_invalid_options(engine, peer, invalid, "max_single_batch_bytes");

    invalid = options;
    invalid.idle_interval = std::chrono::milliseconds(-1);
    expect_invalid_options(engine, peer, invalid, "idle_interval");

    invalid = options;
    invalid.initial_backoff = std::chrono::milliseconds(-1);
    expect_invalid_options(engine, peer, invalid, "initial_backoff");

    invalid = options;
    invalid.max_backoff = std::chrono::milliseconds(-1);
    expect_invalid_options(engine, peer, invalid, "max_backoff");

    invalid = options;
    invalid.initial_backoff = std::chrono::milliseconds(10);
    invalid.max_backoff = std::chrono::milliseconds(9);
    expect_invalid_options(engine, peer, invalid, "max_backoff ordering");

    conn->disconnect();
    cleanup(path);
}

void test_worker_propagates_pull_sync_error() {
    using namespace mdbxc;
    const std::string path = "test_worker_pull_sync_error.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xD0));

    sync::PullResponse response;
    response.ok = false;
    response.error = "db_id mismatch";
    response.error_code = sync::SyncResponseErrorCode::DbIdMismatch;
    response.error_retryable = false;

    FixedPeer peer(response);
    RecordingWorkerObserver observer;
    sync::SyncWorkerOptions options;
    options.observer = &observer;
    sync::SyncWorker worker(engine, peer, options);

    const sync::SyncWorkerRoundResult result = worker.run_once();
    if (result.ok ||
        result.sync_error_code != sync::SyncResponseErrorCode::DbIdMismatch ||
        result.sync_error_retryable) {
        throw std::runtime_error("worker did not propagate pull sync error");
    }

    const std::vector<sync::SyncWorkerStageEvent> stages = observer.stages();
    bool saw_pull_finished = false;
    for (std::size_t i = 0; i < stages.size(); ++i) {
        if (stages[i].stage == sync::SyncWorkerStage::PullFinished) {
            saw_pull_finished = true;
            if (stages[i].ok ||
                stages[i].sync_error_code !=
                    sync::SyncResponseErrorCode::DbIdMismatch ||
                stages[i].sync_error_retryable) {
                throw std::runtime_error(
                    "worker pull stage sync error mismatch");
            }
        }
    }
    if (!saw_pull_finished) {
        throw std::runtime_error("worker did not report PullFinished");
    }

    const sync::SyncWorkerStatus status = worker.status();
    if (!status.last_round_known ||
        status.last_round.sync_error_code !=
            sync::SyncResponseErrorCode::DbIdMismatch ||
        status.last_sync_error_code !=
            sync::SyncResponseErrorCode::DbIdMismatch ||
        status.last_sync_error_retryable) {
        throw std::runtime_error("worker status lost pull sync error");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_propagates_apply_sync_error() {
    using namespace mdbxc;
    const std::string path = "test_worker_apply_sync_error.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    const sync::NodeId replica_node = make_node(0x11);
    const sync::NodeId remote_origin = make_node(0x21);
    const sync::NodeId db_id = make_node(0xD1);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);

    sync::ChangeBatch batch;
    batch.origin_node_id = remote_origin;
    batch.seq = 3;

    sync::PullResponse response;
    response.batches.push_back(batch);

    FixedPeer peer(response);
    RecordingWorkerObserver observer;
    sync::SyncWorkerOptions options;
    options.observer = &observer;
    sync::SyncWorker worker(engine, peer, options);

    const sync::SyncWorkerRoundResult result = worker.run_once();
    if (result.ok ||
        result.sync_error_code != sync::SyncResponseErrorCode::ApplyConflict ||
        !result.sync_error_retryable) {
        throw std::runtime_error("worker did not propagate apply sync error");
    }

    const std::vector<sync::SyncWorkerStageEvent> stages = observer.stages();
    bool saw_apply_finished = false;
    for (std::size_t i = 0; i < stages.size(); ++i) {
        if (stages[i].stage == sync::SyncWorkerStage::ApplyFinished) {
            saw_apply_finished = true;
            if (stages[i].ok ||
                stages[i].sync_error_code !=
                    sync::SyncResponseErrorCode::ApplyConflict ||
                !stages[i].sync_error_retryable) {
                throw std::runtime_error(
                    "worker apply stage sync error mismatch");
            }
        }
    }
    if (!saw_apply_finished) {
        throw std::runtime_error("worker did not report ApplyFinished");
    }

    const sync::SyncWorkerStatus status = worker.status();
    if (!status.last_round_known ||
        status.last_round.sync_error_code !=
            sync::SyncResponseErrorCode::ApplyConflict ||
        status.last_sync_error_code !=
            sync::SyncResponseErrorCode::ApplyConflict ||
        !status.last_sync_error_retryable) {
        throw std::runtime_error("worker status lost apply sync error");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_stop_while_pull_blocked_does_not_apply_returned_page() {
    using namespace mdbxc;
    const std::string path = "test_worker_blocked_stop.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId remote_origin = make_node(0xA0);
    const sync::NodeId db_id = make_node(0xD0);

    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);

    sync::ChangeBatch batch;
    batch.origin_node_id = remote_origin;
    batch.seq = 1;

    sync::PullResponse response;
    response.batches.push_back(batch);

    BlockingPeer peer(response);
    sync::SyncWorkerOptions options;
    options.idle_interval = std::chrono::milliseconds(10000);
    sync::SyncWorker worker(engine, peer, options);

    worker.start();
    if (!peer.wait_until_entered(std::chrono::milliseconds(2000))) {
        throw std::runtime_error("worker did not enter blocking pull");
    }
    worker.request_stop();
    if (peer.cancel_count() != 1) {
        throw std::runtime_error("blocked worker did not request cancellation");
    }
    peer.release();
    worker.join();

    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("blocked worker did not stop");
    }
    if (engine.applied_cursor().last_seq_for(remote_origin) != 0u) {
        throw std::runtime_error("worker applied a page returned after stop");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_stop_from_apply_started_observer_skips_apply() {
    using namespace mdbxc;
    const std::string path = "test_worker_stop_before_apply.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    const sync::NodeId replica_node = make_node(0xB3);
    const sync::NodeId remote_origin = make_node(0xA3);
    const sync::NodeId db_id = make_node(0xD3);

    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);

    sync::ChangeBatch batch;
    batch.origin_node_id = remote_origin;
    batch.seq = 1;

    sync::PullResponse response;
    response.batches.push_back(batch);

    FixedPeer peer(response);
    StopOnApplyStartedObserver observer;
    sync::SyncWorkerOptions options;
    options.observer = &observer;
    sync::SyncWorker guarded_worker(engine, peer, options);
    observer.set_worker(&guarded_worker);

    const sync::SyncWorkerRoundResult result = guarded_worker.run_once();
    if (!result.ok || result.pages_pulled != 1u ||
        result.batches_applied != 0u) {
        throw std::runtime_error("stop before apply round result mismatch");
    }
    if (guarded_worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("stop-before-apply worker did not stop");
    }
    if (engine.applied_cursor().last_seq_for(remote_origin) != 0u) {
        throw std::runtime_error("worker applied page after ApplyStarted stop");
    }
    if (!observer.pages().empty()) {
        throw std::runtime_error("page-applied observer ran for skipped apply");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_threaded_stop_from_apply_started_observer_skips_apply() {
    using namespace mdbxc;
    const std::string path = "test_worker_threaded_stop_before_apply.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    const sync::NodeId replica_node = make_node(0xB4);
    const sync::NodeId remote_origin = make_node(0xA4);
    const sync::NodeId db_id = make_node(0xD4);

    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);

    sync::ChangeBatch batch;
    batch.origin_node_id = remote_origin;
    batch.seq = 1;

    sync::PullResponse response;
    response.batches.push_back(batch);

    FixedPeer peer(response);
    ThreadedStopOnApplyStartedObserver observer;
    sync::SyncWorkerOptions options;
    options.observer = &observer;
    sync::SyncWorker guarded_worker(engine, peer, options);
    observer.set_worker(&guarded_worker);

    const sync::SyncWorkerRoundResult result = guarded_worker.run_once();
    if (!result.ok || result.pages_pulled != 1u ||
        result.batches_applied != 0u) {
        throw std::runtime_error(
            "threaded stop before apply round result mismatch");
    }
    if (guarded_worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error(
            "threaded stop-before-apply worker did not stop");
    }
    if (engine.applied_cursor().last_seq_for(remote_origin) != 0u) {
        throw std::runtime_error(
            "worker applied page after threaded ApplyStarted stop");
    }
    if (!observer.pages().empty()) {
        throw std::runtime_error(
            "page-applied observer ran for threaded skipped apply");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_repeated_stop_cancels_blocking_peer_once() {
    using namespace mdbxc;
    const std::string path = "test_worker_cancel_once.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    const sync::NodeId replica_node = make_node(0xB1);
    const sync::NodeId remote_origin = make_node(0xA1);
    const sync::NodeId db_id = make_node(0xD1);

    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);

    sync::ChangeBatch batch;
    batch.origin_node_id = remote_origin;
    batch.seq = 1;

    sync::PullResponse response;
    response.batches.push_back(batch);

    BlockingPeer peer(response);
    sync::SyncWorkerOptions options;
    options.idle_interval = std::chrono::milliseconds(10000);
    sync::SyncWorker worker(engine, peer, options);

    worker.start();
    if (!peer.wait_until_entered(std::chrono::milliseconds(2000))) {
        throw std::runtime_error("worker did not enter blocking pull");
    }
    worker.request_stop();
    worker.request_stop();

    if (peer.cancel_count() != 1) {
        throw std::runtime_error("repeated stop cancelled active peer more than once");
    }

    peer.release();
    worker.join();

    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("cancel-once worker did not stop");
    }
    if (engine.applied_cursor().last_seq_for(remote_origin) != 0u) {
        throw std::runtime_error("worker applied a page after repeated stop");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_stop_cancels_blocking_peer() {
    using namespace mdbxc;
    const std::string path = "test_worker_cancel_peer.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId remote_origin = make_node(0xA0);
    const sync::NodeId db_id = make_node(0xD0);

    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);

    sync::ChangeBatch batch;
    batch.origin_node_id = remote_origin;
    batch.seq = 1;

    sync::PullResponse response;
    response.batches.push_back(batch);

    CancelableBlockingPeer peer(response);
    sync::SyncWorkerOptions options;
    options.idle_interval = std::chrono::milliseconds(10000);
    sync::SyncWorker worker(engine, peer, options);

    worker.start();
    if (!peer.wait_until_entered(std::chrono::milliseconds(2000))) {
        throw std::runtime_error("worker did not enter cancellable pull");
    }
    worker.request_stop();
    worker.join();

    if (peer.cancel_count() == 0) {
        throw std::runtime_error("worker did not request peer cancellation");
    }
    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("cancelled worker did not stop");
    }
    if (engine.applied_cursor().last_seq_for(remote_origin) != 0u) {
        throw std::runtime_error("worker applied a page returned after cancel");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_stop_cancels_request_token() {
    using namespace mdbxc;
    const std::string path = "test_worker_cancel_token.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    const sync::NodeId replica_node = make_node(0xB2);
    const sync::NodeId remote_origin = make_node(0xA2);
    const sync::NodeId db_id = make_node(0xD2);

    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);

    sync::ChangeBatch batch;
    batch.origin_node_id = remote_origin;
    batch.seq = 1;

    sync::PullResponse response;
    response.batches.push_back(batch);

    TokenBlockingPeer peer(response);
    sync::SyncWorkerOptions options;
    options.idle_interval = std::chrono::milliseconds(10000);
    sync::SyncWorker worker(engine, peer, options);

    worker.start();
    if (!peer.wait_until_entered(std::chrono::milliseconds(2000))) {
        throw std::runtime_error("worker did not enter token-blocking pull");
    }
    if (!peer.saw_token()) {
        throw std::runtime_error("worker did not pass a cancellable token");
    }
    worker.request_stop();
    worker.join();

    if (!peer.saw_token_cancel()) {
        throw std::runtime_error("worker did not cancel request token");
    }
    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("token-cancelled worker did not stop");
    }
    if (engine.applied_cursor().last_seq_for(remote_origin) != 0u) {
        throw std::runtime_error("worker applied a page returned after token cancel");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_rejects_self_join() {
    using namespace mdbxc;
    const std::string path = "test_worker_self_join.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    SelfStoppingPeer peer;
    sync::SyncWorker worker(engine, peer);
    peer.set_worker(&worker);

    worker.start();
    if (!peer.wait_until_pull_entered(std::chrono::seconds(5))) {
        worker.request_stop();
        peer.allow_stop();
        worker.join();
        throw std::runtime_error("self-join worker did not enter pull");
    }
    peer.allow_stop();
    worker.join();

    if (!peer.saw_logic_error()) {
        throw std::runtime_error("worker did not reject self-join");
    }
    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("self-join worker did not stop");
    }

    conn->disconnect();
    cleanup(path);
}

void test_worker_guard_starts_and_stops_background_session() {
    using namespace mdbxc;
    const std::string path = "test_worker_guard_session.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x71), make_node(0xD1));

    EmptyPeer peer;
    sync::SyncWorkerOptions options;
    options.idle_interval = std::chrono::milliseconds(50);
    sync::SyncWorker worker(engine, peer, options);

    {
        sync::SyncWorkerGuard guard(worker);
        if (!guard.active() || &guard.worker() != &worker) {
            throw std::runtime_error("SyncWorkerGuard did not become active");
        }
        if (!peer.wait_for_pulls(1, std::chrono::milliseconds(2000))) {
            throw std::runtime_error("guarded worker did not run");
        }
    }

    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("SyncWorkerGuard did not stop worker");
    }

    conn->disconnect();
    cleanup(path);
}

void test_sync_node_session_wires_capture_worker_and_observer() {
    using namespace mdbxc;
    const std::string primary_path = "test_sync_node_session_primary.mdbx";
    const std::string replica_path = "test_sync_node_session_replica.mdbx";
    cleanup(primary_path);
    cleanup(replica_path);

    std::shared_ptr<Connection> primary_conn = open_env(primary_path);
    std::shared_ptr<Connection> replica_conn = open_env(replica_path);

    const sync::NodeId primary_node = make_node(0x91);
    const sync::NodeId replica_node = make_node(0x92);
    const sync::NodeId db_id = make_node(0xD9);

    sync::SyncEngine primary_engine(primary_conn);
    sync::SyncEngine replica_engine(replica_conn);
    primary_engine.initialize_local_identity(primary_node, db_id);
    replica_engine.initialize_local_identity(replica_node, db_id);

    sync::ThreadLocalChangeAccumulator capture(primary_conn);
    sync::DirectSyncPeer primary_peer(&primary_engine);
    NodeSessionApplyObserver observer;

    sync::SyncWorkerOptions worker_options;
    worker_options.idle_interval = std::chrono::milliseconds(20);
    worker_options.max_batches = 4;
    sync::SyncWorker worker(replica_engine, primary_peer, worker_options);

    KeyValueTable<int, std::string> primary_items(primary_conn, "items");
    KeyValueTable<int, std::string> replica_items(replica_conn, "items");

    std::uint64_t observer_token = 0;
    {
        sync::SyncNodeSessionOptions session_options;
        session_options.capture_connection = primary_conn;
        session_options.capture_sink = &capture;
        session_options.apply_observer_connection = replica_conn;
        session_options.apply_observer = &observer;

        sync::SyncNodeSession session(worker, session_options);
        observer_token = session.apply_observer_token();
        if (!session.active() || !session.capture_active() ||
            !session.apply_observer_registered() ||
            observer_token == 0u) {
            throw std::runtime_error("SyncNodeSession did not activate");
        }
        if (primary_conn->sync_capture() != &capture) {
            throw std::runtime_error("SyncNodeSession did not attach capture");
        }

        primary_items.insert_or_assign(11, "eleven");

        if (!observer.wait_for_events(1u, std::chrono::milliseconds(3000))) {
            throw std::runtime_error("SyncNodeSession observer did not fire");
        }
        const std::string value = kv_or_throw(
            replica_conn, replica_items, 11, "replica item 11");
        if (value != "eleven") {
            throw std::runtime_error("replica item mismatch");
        }
        if (observer.generation() != replica_conn->sync_apply_generation()) {
            throw std::runtime_error("observer generation mismatch");
        }

        session.stop();
        if (session.active() || session.capture_active() ||
            session.apply_observer_registered()) {
            throw std::runtime_error("SyncNodeSession stop did not release hooks");
        }
    }

    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error("SyncNodeSession did not stop worker");
    }
    if (primary_conn->sync_capture() != nullptr) {
        throw std::runtime_error("SyncNodeSession did not detach capture");
    }
    if (replica_conn->remove_sync_apply_observer(observer_token)) {
        throw std::runtime_error(
            "SyncNodeSession did not remove observer registration");
    }

    primary_conn->disconnect();
    replica_conn->disconnect();
    cleanup(primary_path);
    cleanup(replica_path);
}

void test_sync_node_session_destructor_releases_hooks() {
    using namespace mdbxc;
    const std::string primary_path =
        "test_sync_node_session_destructor_primary.mdbx";
    const std::string replica_path =
        "test_sync_node_session_destructor_replica.mdbx";
    cleanup(primary_path);
    cleanup(replica_path);

    std::shared_ptr<Connection> primary_conn = open_env(primary_path);
    std::shared_ptr<Connection> replica_conn = open_env(replica_path);

    const sync::NodeId primary_node = make_node(0xA1);
    const sync::NodeId replica_node = make_node(0xA2);
    const sync::NodeId db_id = make_node(0xDA);

    sync::SyncEngine primary_engine(primary_conn);
    sync::SyncEngine replica_engine(replica_conn);
    primary_engine.initialize_local_identity(primary_node, db_id);
    replica_engine.initialize_local_identity(replica_node, db_id);

    sync::ThreadLocalChangeAccumulator capture(primary_conn);
    sync::DirectSyncPeer primary_peer(&primary_engine);
    NodeSessionApplyObserver observer;

    sync::SyncWorkerOptions worker_options;
    worker_options.idle_interval = std::chrono::milliseconds(20);
    sync::SyncWorker worker(replica_engine, primary_peer, worker_options);

    KeyValueTable<int, std::string> primary_items(primary_conn, "items");
    KeyValueTable<int, std::string> replica_items(replica_conn, "items");

    std::uint64_t observer_token = 0;
    {
        sync::SyncNodeSessionOptions session_options;
        session_options.capture_connection = primary_conn;
        session_options.capture_sink = &capture;
        session_options.apply_observer_connection = replica_conn;
        session_options.apply_observer = &observer;

        sync::SyncNodeSession session(worker, session_options);
        observer_token = session.apply_observer_token();
        primary_items.insert_or_assign(21, "twenty one");
        if (!observer.wait_for_events(1u, std::chrono::milliseconds(3000))) {
            throw std::runtime_error(
                "SyncNodeSession destructor test did not observe apply");
        }
        if (kv_or_throw(replica_conn, replica_items, 21, "replica item 21") !=
            "twenty one") {
            throw std::runtime_error(
                "SyncNodeSession destructor test replicated wrong value");
        }
    }

    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error(
            "SyncNodeSession destructor did not stop worker");
    }
    if (primary_conn->sync_capture() != nullptr) {
        throw std::runtime_error(
            "SyncNodeSession destructor did not detach capture");
    }
    if (replica_conn->remove_sync_apply_observer(observer_token)) {
        throw std::runtime_error(
            "SyncNodeSession destructor did not remove observer");
    }

    primary_conn->disconnect();
    replica_conn->disconnect();
    cleanup(primary_path);
    cleanup(replica_path);
}

void test_sync_node_session_supports_optional_hook_subsets() {
    using namespace mdbxc;
    const std::string primary_path =
        "test_sync_node_session_optional_primary.mdbx";
    const std::string replica_path =
        "test_sync_node_session_optional_replica.mdbx";
    cleanup(primary_path);
    cleanup(replica_path);

    std::shared_ptr<Connection> primary_conn = open_env(primary_path);
    std::shared_ptr<Connection> replica_conn = open_env(replica_path);

    const sync::NodeId primary_node = make_node(0xB1);
    const sync::NodeId replica_node = make_node(0xB2);
    const sync::NodeId db_id = make_node(0xDB);

    sync::SyncEngine primary_engine(primary_conn);
    sync::SyncEngine replica_engine(replica_conn);
    primary_engine.initialize_local_identity(primary_node, db_id);
    replica_engine.initialize_local_identity(replica_node, db_id);

    sync::DirectSyncPeer primary_peer(&primary_engine);
    sync::SyncWorkerOptions worker_options;
    worker_options.idle_interval = std::chrono::milliseconds(20);

    {
        sync::SyncWorker worker(replica_engine, primary_peer, worker_options);
        sync::SyncNodeSessionOptions session_options;
        sync::SyncNodeSession session(worker, session_options);
        if (!session.active() || session.capture_active() ||
            session.apply_observer_registered()) {
            throw std::runtime_error(
                "SyncNodeSession worker-only mode exposed wrong state");
        }
    }

    sync::ThreadLocalChangeAccumulator capture(primary_conn);
    {
        sync::SyncWorker worker(replica_engine, primary_peer, worker_options);
        sync::SyncNodeSessionOptions session_options;
        session_options.capture_connection = primary_conn;
        session_options.capture_sink = &capture;
        sync::SyncNodeSession session(worker, session_options);
        if (!session.active() || !session.capture_active() ||
            session.apply_observer_registered()) {
            throw std::runtime_error(
                "SyncNodeSession capture-only mode exposed wrong state");
        }
    }
    if (primary_conn->sync_capture() != nullptr) {
        throw std::runtime_error(
            "SyncNodeSession capture-only mode did not detach");
    }

    NodeSessionApplyObserver observer;
    std::uint64_t observer_token = 0;
    {
        sync::SyncWorker worker(replica_engine, primary_peer, worker_options);
        sync::SyncNodeSessionOptions session_options;
        session_options.apply_observer_connection = replica_conn;
        session_options.apply_observer = &observer;
        sync::SyncNodeSession session(worker, session_options);
        observer_token = session.apply_observer_token();
        if (!session.active() || session.capture_active() ||
            !session.apply_observer_registered() ||
            observer_token == 0u) {
            throw std::runtime_error(
                "SyncNodeSession observer-only mode exposed wrong state");
        }
    }
    if (replica_conn->remove_sync_apply_observer(observer_token)) {
        throw std::runtime_error(
            "SyncNodeSession observer-only mode did not unregister");
    }

    primary_conn->disconnect();
    replica_conn->disconnect();
    cleanup(primary_path);
    cleanup(replica_path);
}

void test_sync_node_session_rejects_invalid_option_pairs() {
    using namespace mdbxc;
    const std::string path = "test_sync_node_session_invalid.mdbx";
    cleanup(path);

    std::shared_ptr<Connection> conn = open_env(path);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xC1), make_node(0xDC));

    EmptyPeer peer;
    sync::SyncWorker worker(engine, peer);
    NodeSessionCaptureSink capture_sink;
    NodeSessionApplyObserver observer;

    bool saw_capture_conn_only = false;
    try {
        sync::SyncNodeSessionOptions options;
        options.capture_connection = conn;
        sync::SyncNodeSession session(worker, options);
    } catch (const std::invalid_argument&) {
        saw_capture_conn_only = true;
    }
    if (!saw_capture_conn_only) {
        throw std::runtime_error(
            "SyncNodeSession accepted capture connection without sink");
    }

    bool saw_capture_sink_only = false;
    try {
        sync::SyncNodeSessionOptions options;
        options.capture_sink = &capture_sink;
        sync::SyncNodeSession session(worker, options);
    } catch (const std::invalid_argument&) {
        saw_capture_sink_only = true;
    }
    if (!saw_capture_sink_only) {
        throw std::runtime_error(
            "SyncNodeSession accepted capture sink without connection");
    }

    bool saw_observer_conn_only = false;
    try {
        sync::SyncNodeSessionOptions options;
        options.apply_observer_connection = conn;
        sync::SyncNodeSession session(worker, options);
    } catch (const std::invalid_argument&) {
        saw_observer_conn_only = true;
    }
    if (!saw_observer_conn_only) {
        throw std::runtime_error(
            "SyncNodeSession accepted observer connection without observer");
    }

    bool saw_observer_only = false;
    try {
        sync::SyncNodeSessionOptions options;
        options.apply_observer = &observer;
        sync::SyncNodeSession session(worker, options);
    } catch (const std::invalid_argument&) {
        saw_observer_only = true;
    }
    if (!saw_observer_only) {
        throw std::runtime_error(
            "SyncNodeSession accepted observer without connection");
    }

    if (worker.state() != sync::SyncWorkerState::Stopped) {
        throw std::runtime_error(
            "invalid SyncNodeSession options started worker");
    }
    if (conn->sync_capture() != nullptr) {
        throw std::runtime_error(
            "invalid SyncNodeSession options attached capture");
    }

    conn->disconnect();
    cleanup(path);
}

void test_sync_node_session_rolls_back_hooks_when_worker_start_fails() {
    using namespace mdbxc;
    const std::string primary_path =
        "test_sync_node_session_rollback_primary.mdbx";
    const std::string replica_path =
        "test_sync_node_session_rollback_replica.mdbx";
    cleanup(primary_path);
    cleanup(replica_path);

    std::shared_ptr<Connection> primary_conn = open_env(primary_path);
    std::shared_ptr<Connection> replica_conn = open_env(replica_path);

    sync::SyncEngine replica_engine(replica_conn);
    replica_engine.initialize_local_identity(make_node(0xD1), make_node(0xDD));

    EmptyPeer peer;
    sync::SyncWorkerOptions worker_options;
    worker_options.idle_interval = std::chrono::milliseconds(50);
    sync::SyncWorker worker(replica_engine, peer, worker_options);

    NodeSessionCaptureSink previous_sink;
    sync::ThreadLocalChangeAccumulator capture(primary_conn);
    NodeSessionApplyObserver observer;

    primary_conn->attach_sync_capture(&previous_sink);
    worker.start();
    if (!peer.wait_for_pulls(1, std::chrono::milliseconds(2000))) {
        worker.request_stop();
        worker.join();
        throw std::runtime_error(
            "SyncNodeSession rollback precondition worker did not run");
    }

    bool saw_start_failure = false;
    try {
        sync::SyncNodeSessionOptions session_options;
        session_options.capture_connection = primary_conn;
        session_options.capture_sink = &capture;
        session_options.apply_observer_connection = replica_conn;
        session_options.apply_observer = &observer;
        sync::SyncNodeSession session(worker, session_options);
    } catch (const std::logic_error&) {
        saw_start_failure = true;
    }

    worker.stop();
    worker.join();

    if (!saw_start_failure) {
        throw std::runtime_error(
            "SyncNodeSession did not propagate worker start failure");
    }
    if (primary_conn->sync_capture() != &previous_sink) {
        throw std::runtime_error(
            "SyncNodeSession failed-start path did not restore capture");
    }

    sync::PushRequest push;
    push.sender = make_node(0xE1);
    push.db_id = make_node(0xDD);
    push.batches.push_back(
        make_raw_batch(make_node(0xE2), 1, "rollback_observer_probe", 0x45));
    const sync::PushResponse pushed = replica_engine.handle_push(push);
    if (!pushed.ok) {
        throw std::runtime_error(
            "SyncNodeSession rollback observer probe failed: " +
            pushed.error);
    }
    if (observer.events() != 0u) {
        throw std::runtime_error(
            "SyncNodeSession failed-start path leaked observer");
    }

    primary_conn->detach_sync_capture();
    primary_conn->disconnect();
    replica_conn->disconnect();
    cleanup(primary_path);
    cleanup(replica_path);
}

void test_worker_recovers_fresh_replica_with_full_snapshot() {
    using namespace mdbxc;
    const std::string source_path = "test_worker_snapshot_source.mdbx";
    const std::string replica_path = "test_worker_snapshot_replica.mdbx";
    cleanup(source_path);
    cleanup(replica_path);

    const sync::NodeId source_node = make_node(0xA9);
    const sync::NodeId replica_node = make_node(0xB9);
    const sync::DbId db_id = make_node(0xD9);
    sync::FullSnapshotExportOptions snapshot_options;
    sync::FullSnapshotManifestEntry entry;
    entry.dbi_name = "documents";
    snapshot_options.manifest.push_back(entry);
    snapshot_options.max_materialized_operations = 16u;
    snapshot_options.max_materialized_bytes = 4096u;

    std::shared_ptr<Connection> source_conn = open_env(source_path);
    sync::SyncEngine source_engine(
        source_conn, sync::ConflictPolicy::Reject, snapshot_options);
    source_engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> source_documents(
        source_conn, "documents");
    source_documents.insert_or_assign("one", "1");
    source_documents.insert_or_assign("two", "2");
    {
        auto txn = source_conn->transaction(TransactionMode::WRITABLE);
        sync::ChangeLogStore changelog(source_conn->env_handle());
        changelog.open(txn.handle());
        const std::vector<std::uint8_t> encoded =
            sync::ChangeBatchCodec::encode(
                make_raw_batch(source_node, 3u, "documents", 0x21u));
        changelog.append(txn.handle(), source_node, 3u, encoded);
        txn.commit();
    }

    std::shared_ptr<Connection> replica_conn = open_env(replica_path);
    sync::SyncEngine replica_engine(replica_conn);
    replica_engine.initialize_local_identity(replica_node, db_id);
    sync::DirectSyncPeer peer(&source_engine);
    {
        sync::SyncWorker disabled_worker(replica_engine, peer);
        const sync::SyncWorkerRoundResult disabled =
            disabled_worker.run_once();
        if (disabled.ok ||
            disabled.sync_error_code != sync::SyncResponseErrorCode::SnapshotRequired) {
            throw std::runtime_error(
                "worker enabled full snapshot fallback without opt-in");
        }
    }
    sync::SyncWorkerOptions options;
    options.enable_full_snapshot_fallback = true;
    options.max_bytes = 1u;
    options.max_single_batch_bytes = 8192u;
    sync::SyncWorker worker(replica_engine, peer, options);

    const sync::SyncWorkerRoundResult result = worker.run_once();
    if (!result.ok || result.pages_pulled < 2u ||
        result.batches_applied != 0u || result.has_more ||
        result.sync_error_code != sync::SyncResponseErrorCode::None) {
        throw std::runtime_error(
            "worker did not complete fresh-replica full snapshot fallback");
    }
    KeyValueTable<std::string, std::string> replica_documents(
        replica_conn, "documents");
    if (kv_or_throw(replica_conn, replica_documents, std::string("one"),
                    "snapshot one") != "1" ||
        kv_or_throw(replica_conn, replica_documents, std::string("two"),
                    "snapshot two") != "2" ||
        replica_engine.applied_cursor().last_seq_for(source_node) != 3u) {
        throw std::runtime_error(
            "worker full snapshot fallback did not bootstrap replica state");
    }

    source_conn->disconnect();
    replica_conn->disconnect();
    cleanup(source_path);
    cleanup(replica_path);
}

void test_worker_snapshot_recovery_preserves_remote_origin_cursor() {
    using namespace mdbxc;
    const std::string snapshot_source_path =
        "test_worker_snapshot_remote_source.mdbx";
    const std::string remote_origin_path =
        "test_worker_snapshot_remote_origin.mdbx";
    const std::string replica_path =
        "test_worker_snapshot_remote_replica.mdbx";
    cleanup(snapshot_source_path);
    cleanup(remote_origin_path);
    cleanup(replica_path);

    const sync::NodeId snapshot_source_node = make_node(0xAA);
    const sync::NodeId remote_origin_node = make_node(0xBA);
    const sync::NodeId replica_node = make_node(0xCA);
    const sync::DbId db_id = make_node(0xDA);
    sync::FullSnapshotExportOptions snapshot_options;
    sync::FullSnapshotManifestEntry entry;
    entry.dbi_name = "documents";
    snapshot_options.manifest.push_back(entry);
    snapshot_options.max_materialized_operations = 16u;
    snapshot_options.max_materialized_bytes = 4096u;

    std::shared_ptr<Connection> snapshot_source_conn =
        open_env(snapshot_source_path);
    sync::SyncEngine snapshot_source(
        snapshot_source_conn, sync::ConflictPolicy::Reject, snapshot_options);
    snapshot_source.initialize_local_identity(snapshot_source_node, db_id);
    sync::PushRequest applied_remote;
    applied_remote.sender = remote_origin_node;
    applied_remote.db_id = db_id;
    applied_remote.batches.push_back(
        make_raw_batch(remote_origin_node, 1u, "documents", 0x41u));
    if (!snapshot_source.handle_push(applied_remote).ok) {
        throw std::runtime_error(
            "snapshot source did not apply remote origin sequence one");
    }
    {
        auto txn = snapshot_source_conn->transaction(TransactionMode::WRITABLE);
        sync::ChangeLogStore changelog(snapshot_source_conn->env_handle());
        changelog.open(txn.handle());
        const std::vector<std::uint8_t> encoded =
            sync::ChangeBatchCodec::encode(
                make_raw_batch(snapshot_source_node, 3u, "documents", 0x31u));
        changelog.append(txn.handle(), snapshot_source_node, 3u, encoded);
        txn.commit();
    }

    std::shared_ptr<Connection> remote_origin_conn = open_env(remote_origin_path);
    sync::SyncEngine remote_origin(remote_origin_conn);
    remote_origin.initialize_local_identity(remote_origin_node, db_id);
    {
        auto txn = remote_origin_conn->transaction(TransactionMode::WRITABLE);
        sync::ChangeLogStore changelog(remote_origin_conn->env_handle());
        changelog.open(txn.handle());
        const std::vector<std::uint8_t> first =
            sync::ChangeBatchCodec::encode(
                make_raw_batch(remote_origin_node, 1u, "documents", 0x41u));
        const std::vector<std::uint8_t> second =
            sync::ChangeBatchCodec::encode(
                make_raw_batch(remote_origin_node, 2u, "documents", 0x42u));
        changelog.append(txn.handle(), remote_origin_node, 1u, first);
        changelog.append(txn.handle(), remote_origin_node, 2u, second);
        txn.commit();
    }

    std::shared_ptr<Connection> replica_conn = open_env(replica_path);
    sync::SyncEngine replica(replica_conn);
    replica.initialize_local_identity(replica_node, db_id);
    sync::DirectSyncPeer snapshot_peer(&snapshot_source);
    sync::SyncWorkerOptions snapshot_worker_options;
    snapshot_worker_options.enable_full_snapshot_fallback = true;
    snapshot_worker_options.max_bytes = 1u;
    snapshot_worker_options.max_single_batch_bytes = 8192u;
    sync::SyncWorker snapshot_worker(
        replica, snapshot_peer, snapshot_worker_options);
    const sync::SyncWorkerRoundResult snapshot_result =
        snapshot_worker.run_once();
    if (!snapshot_result.ok ||
        replica.applied_cursor().last_seq_for(remote_origin_node) != 1u) {
        throw std::runtime_error(
            "snapshot recovery did not preserve remote origin progress");
    }

    sync::DirectSyncPeer remote_origin_peer(&remote_origin);
    sync::SyncWorkerOptions incremental_options;
    incremental_options.max_bytes = 8192u;
    incremental_options.max_single_batch_bytes = 8192u;
    sync::SyncWorker incremental_worker(
        replica, remote_origin_peer, incremental_options);
    const sync::SyncWorkerRoundResult incremental_result =
        incremental_worker.run_once();
    if (!incremental_result.ok || incremental_result.batches_applied != 1u ||
        replica.applied_cursor().last_seq_for(remote_origin_node) != 2u) {
        throw std::runtime_error(
            "incremental remote origin delivery failed after snapshot recovery");
    }

    snapshot_source_conn->disconnect();
    remote_origin_conn->disconnect();
    replica_conn->disconnect();
    cleanup(snapshot_source_path);
    cleanup(remote_origin_path);
    cleanup(replica_path);
}

} // namespace

int main() {
    struct Case { const char* name; void (*fn)(); };
    const Case cases[] = {
        { "test_worker_recovers_fresh_replica_with_full_snapshot",
          &test_worker_recovers_fresh_replica_with_full_snapshot },
        { "test_worker_snapshot_recovery_preserves_remote_origin_cursor",
          &test_worker_snapshot_recovery_preserves_remote_origin_cursor },
        { "test_worker_run_once_drains_paginated_pull",
          &test_worker_run_once_drains_paginated_pull },
        { "test_worker_start_stop_idle", &test_worker_start_stop_idle },
        { "test_worker_background_pull_over_http_transport",
          &test_worker_background_pull_over_http_transport },
        { "test_worker_stop_cancels_http_transport_peer",
          &test_worker_stop_cancels_http_transport_peer },
        { "test_worker_backoff_on_pull_error", &test_worker_backoff_on_pull_error },
        { "test_worker_backoff_uses_retry_after_hint",
          &test_worker_backoff_uses_retry_after_hint },
        { "test_worker_permanent_hint_keeps_retrying_by_default",
          &test_worker_permanent_hint_keeps_retrying_by_default },
        { "test_worker_permanent_hint_can_stop_worker",
          &test_worker_permanent_hint_can_stop_worker },
        { "test_worker_observer_exception_does_not_fail_round",
          &test_worker_observer_exception_does_not_fail_round },
        { "test_worker_stage_observer_exception_does_not_fail_round",
          &test_worker_stage_observer_exception_does_not_fail_round },
        { "test_worker_observer_exception_keeps_pull_error",
          &test_worker_observer_exception_keeps_pull_error },
        { "test_worker_backoff_observer_exception_keeps_pull_error",
          &test_worker_backoff_observer_exception_keeps_pull_error },
        { "test_worker_page_observer_exception_does_not_fail_round",
          &test_worker_page_observer_exception_does_not_fail_round },
        { "test_worker_rejects_invalid_options",
          &test_worker_rejects_invalid_options },
        { "test_worker_propagates_pull_sync_error",
          &test_worker_propagates_pull_sync_error },
        { "test_worker_propagates_apply_sync_error",
          &test_worker_propagates_apply_sync_error },
        { "test_worker_stop_while_pull_blocked",
          &test_worker_stop_while_pull_blocked_does_not_apply_returned_page },
        { "test_worker_stop_from_apply_started_observer_skips_apply",
          &test_worker_stop_from_apply_started_observer_skips_apply },
        { "test_worker_threaded_stop_from_apply_started_observer_skips_apply",
          &test_worker_threaded_stop_from_apply_started_observer_skips_apply },
        { "test_worker_repeated_stop_cancels_blocking_peer_once",
          &test_worker_repeated_stop_cancels_blocking_peer_once },
        { "test_worker_stop_cancels_blocking_peer",
          &test_worker_stop_cancels_blocking_peer },
        { "test_worker_stop_cancels_request_token",
          &test_worker_stop_cancels_request_token },
        { "test_worker_rejects_self_join", &test_worker_rejects_self_join },
        { "test_worker_guard_starts_and_stops_background_session",
          &test_worker_guard_starts_and_stops_background_session },
        { "test_sync_node_session_wires_capture_worker_observer",
          &test_sync_node_session_wires_capture_worker_and_observer },
        { "test_sync_node_session_destructor_releases_hooks",
          &test_sync_node_session_destructor_releases_hooks },
        { "test_sync_node_session_supports_optional_hook_subsets",
          &test_sync_node_session_supports_optional_hook_subsets },
        { "test_sync_node_session_rejects_invalid_option_pairs",
          &test_sync_node_session_rejects_invalid_option_pairs },
        { "test_sync_node_session_rolls_back_hooks_when_worker_start_fails",
          &test_sync_node_session_rolls_back_hooks_when_worker_start_fails },
    };

    int rc = 0;
    for (std::size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        try {
            cases[i].fn();
            std::printf("PASS %s\n", cases[i].name);
        } catch (const std::exception& e) {
            std::printf("FAIL %s: %s\n", cases[i].name, e.what());
            rc = static_cast<int>(i + 1);
        } catch (...) {
            std::printf("FAIL %s: non-std exception\n", cases[i].name);
            rc = static_cast<int>(i + 1);
        }
    }
    return rc;
}
