#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SYNCWORKER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SYNCWORKER_HPP_INCLUDED

/// \file engine/SyncWorker.hpp
/// \brief Background pull/apply loop for \c SyncEngine.

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

namespace mdbxc {
namespace sync {

    class ISyncWorkerObserver;

    /// \brief Runtime state of a \c SyncWorker.
    enum class SyncWorkerState {
        Stopped,  ///< No worker thread is running.
        Starting, ///< Worker thread has been created but has not entered the loop.
        Idle,     ///< Worker is sleeping between sync rounds.
        Pulling,  ///< Worker is waiting for a remote peer call.
        Applying, ///< Worker is applying a pulled page locally.
        Backoff,  ///< Last round failed; worker is waiting before retry.
        Stopping, ///< Stop has been requested; current operation is draining.
        Failed,   ///< Worker thread exited after an unexpected exception.
    };

    /// \brief Fine-grained sync round stage reported to observers.
    enum class SyncWorkerStage {
        RoundStarted,   ///< A foreground or background sync round started.
        PullStarted,    ///< A pull page request is about to be sent.
        PullFinished,   ///< A pull page request returned.
        ApplyStarted,   ///< A non-empty pulled page is about to be applied.
        ApplyFinished,  ///< A non-empty pulled page finished applying.
        LogicalDeliveryStarted,  ///< A queued logical delivery is about to be sent.
        LogicalDeliveryFinished, ///< A queued logical delivery returned an acknowledgement.
        RoundCompleted, ///< The round finished with a final result.
        BackoffStarted, ///< Background worker entered retry backoff.
    };

    /// \brief Policy for classified permanent transport failures.
    enum class SyncWorkerPermanentFailurePolicy {
        KeepRetrying, ///< Keep using normal background retry/backoff.
        StopWorker    ///< Stop the background loop in \c Failed state.
    };

    /// \brief Timing and pagination settings for \c SyncWorker.
    struct SyncWorkerOptions {
        /// \brief Max batches requested from the peer per pull page.
        std::uint64_t max_batches = 1000;

        /// \brief Max encoded batch bytes requested from the peer per pull page.
        std::uint64_t max_bytes = 4ULL * 1024ULL * 1024ULL;

        /// \brief Hard limit for one encoded retained changelog batch.
        /// \details \c max_bytes is a soft page budget; a peer may still
        /// return one batch larger than \c max_bytes when that batch fits
        /// this per-batch limit.
        std::uint64_t max_single_batch_bytes = 4ULL * 1024ULL * 1024ULL;

        /// \brief Delay between successful background sync rounds.
        std::chrono::milliseconds idle_interval =
            std::chrono::milliseconds(1000);

        /// \brief Initial delay after a failed background sync round.
        std::chrono::milliseconds initial_backoff =
            std::chrono::milliseconds(100);

        /// \brief Maximum delay after repeated failed background sync rounds.
        std::chrono::milliseconds max_backoff =
            std::chrono::milliseconds(5000);

        /// \brief Whether one round drains all \c has_more pages immediately.
        bool drain_pages = true;

        /// \brief Requests an explicit fresh-replica snapshot after
        /// \c SnapshotRequired.
        /// \details Disabled by default. When enabled, the worker starts a
        /// new \c CompleteUserDatabase snapshot session with an empty cursor
        /// and drains it through the final page in the current round.
        /// \c ManifestOnly is a manual physical replacement mode and never a
        /// raw-sync recovery fallback. The receiver importer still rejects
        /// non-fresh targets; this is not an in-place repair path for a
        /// partially replicated database.
        bool enable_full_snapshot_fallback = false;

        /// \brief Requests logical-aware fresh-replica recovery after
        /// \c SnapshotRequired.
        /// \details Disabled by default. This is a separate peer capability
        /// and commits the final physical snapshot page together with schema,
        /// replay, and ordered-delivery metadata. It never relaxes the raw
        /// CompleteUserDatabase logical-state guard.
        bool enable_logical_recovery_fallback = false;

        /// \brief Sends this engine's queued logical deliveries after raw sync.
        /// \details Disabled by default, preserving the raw-only worker contract.
        /// When enabled, the worker sends only the outbox prefix addressed to
        /// this worker's remote database and only after it has drained raw pull
        /// pages for the round. The peer must explicitly support logical
        /// delivery whenever the destination has pending logical entries.
        bool enable_logical_delivery = false;

        /// \brief Maximum logical envelopes delivered in one round.
        /// \details Zero drains the pending prefix. Deliveries are dispatched
        /// one at a time so a stop request can prevent the next envelope from
        /// starting after the current peer call returns.
        std::size_t max_logical_deliveries = 0u;

        /// \brief How the background worker handles permanent transport hints.
        /// \details The default keeps v0.1 retry hints advisory. Set to
        /// \c StopWorker when a classified permanent transport failure should
        /// leave the worker in \c SyncWorkerState::Failed instead of entering
        /// retry backoff. Sync-level response errors are reported separately
        /// through \c SyncWorkerRoundResult and status snapshots; this policy
        /// does not stop on them.
        SyncWorkerPermanentFailurePolicy permanent_failure_policy =
            SyncWorkerPermanentFailurePolicy::KeepRetrying;

        /// \brief Optional observer for stage, page, round, and backoff events.
        /// \details The worker does not own the observer. When set, the
        /// observer must outlive the worker.
        ISyncWorkerObserver* observer = nullptr;
    };

    /// \brief Best-effort progress estimate for a worker catch-up round.
    /// \details The estimate is based on the latest successful
    /// \c PullResponse::remote_tail cursor and the receiver cursor known to
    /// the worker. It counts sequence numbers, which correspond to captured
    /// change batches. Unknown future writes and origins not yet advertised by
    /// the peer are outside this estimate.
    struct SyncWorkerProgressEstimate {
        bool remote_tail_known = false; ///< Whether \c remote_tail was seen.
        std::uint64_t batches_applied = 0; ///< Batches applied in this round.
        std::uint64_t batches_remaining = 0; ///< Known remaining batches.
        std::uint64_t batches_total = 0; ///< Applied plus known remaining.
        double completion_ratio = 0.0; ///< 0.0 to 1.0 when total is known.
    };

    /// \brief Result of one \c SyncWorker sync round.
    struct SyncWorkerRoundResult {
        bool        ok = true; ///< Whether the round completed without error.
        std::size_t pages_pulled = 0; ///< Number of successful pull pages.
        std::size_t batches_applied = 0; ///< Number of batches applied locally.
        std::size_t logical_deliveries_delivered = 0; ///< Logical envelopes ACKed this round.
        std::uint64_t logical_acknowledged_through = 0; ///< Latest logical ACK frontier.
        bool        has_more = false; ///< Whether the peer reported more pages.
        SyncWorkerProgressEstimate progress; ///< Last known progress.
        std::string error; ///< Failure detail when \c ok is false.
        SyncTransportRetryHint retry_hint; ///< Transport retry advice.
        /// \brief Sync-level response error classification.
        /// \details Distinct from transport retry advice. \c None means the
        /// peer did not return a structured sync error.
        SyncResponseErrorCode sync_error_code =
            SyncResponseErrorCode::None;
        bool sync_error_retryable = false; ///< Sync-level recovery hint.
    };

    /// \brief Fine-grained observer event for sync round stages.
    struct SyncWorkerStageEvent {
        SyncWorkerStage stage =
            SyncWorkerStage::RoundStarted; ///< Reported stage.
        SyncWorkerState state =
            SyncWorkerState::Stopped; ///< Worker state near the callback.
        std::size_t pages_pulled = 0; ///< Pages pulled so far in the round.
        std::size_t batches_in_page = 0; ///< Batches in the current page.
        std::size_t batches_applied = 0; ///< Batches applied so far.
        std::size_t logical_deliveries_delivered = 0; ///< Logical envelopes ACKed so far.
        std::uint64_t logical_acknowledged_through = 0; ///< Latest logical ACK frontier.
        bool        has_more = false; ///< Latest peer pagination flag.
        bool        ok = true; ///< Whether the current stage succeeded.
        SyncWorkerProgressEstimate progress; ///< Last known progress.
        std::string error; ///< Failure detail when \c ok is false.
        SyncTransportRetryHint retry_hint; ///< Transport retry advice.
        /// \brief Sync-level response error classification.
        SyncResponseErrorCode sync_error_code =
            SyncResponseErrorCode::None;
        bool sync_error_retryable = false; ///< Sync-level recovery hint.
    };

    /// \brief Thread-safe snapshot of the current \c SyncWorker status.
    /// \details The snapshot is intended for polling UIs, health endpoints,
    /// and structured logging code that does not want to reconstruct state
    /// from observer callbacks. Time points use the process-local steady
    /// clock and are meaningful only for elapsed-time calculations inside the
    /// current process.
    struct SyncWorkerStatus {
        SyncWorkerState state =
            SyncWorkerState::Stopped; ///< Current lifecycle state.
        SyncWorkerStage current_stage =
            SyncWorkerStage::RoundCompleted; ///< Last observed stage.
        bool last_stage_known = false; ///< Whether \c last_stage is valid.
        bool last_round_known = false; ///< Whether \c last_round is valid.
        bool round_active = false; ///< A round has started and not completed.
        bool backoff_active = false; ///< Worker is currently in backoff wait.
        std::uint64_t rounds_started = 0; ///< Rounds started in this session.
        std::uint64_t rounds_completed = 0; ///< Rounds completed in this session.
        std::uint64_t rounds_succeeded = 0; ///< Successful completed rounds.
        std::uint64_t rounds_failed = 0; ///< Failed completed rounds.
        SyncWorkerStageEvent last_stage; ///< Last reported stage event.
        SyncWorkerRoundResult last_round; ///< Last completed round result.
        SyncWorkerProgressEstimate last_progress; ///< Last known progress.
        SyncTransportRetryHint last_retry_hint; ///< Last known retry hint.
        SyncResponseErrorCode last_sync_error_code =
            SyncResponseErrorCode::None; ///< Last sync-level error code.
        bool last_sync_error_retryable = false; ///< Last sync-level hint.
        std::chrono::milliseconds last_backoff_delay =
            std::chrono::milliseconds::zero(); ///< Last scheduled backoff.
        std::chrono::steady_clock::time_point last_round_started_at;
        std::chrono::steady_clock::time_point last_round_finished_at;
        std::string last_error; ///< Most recent worker failure message.
        std::string last_observer_error; ///< Most recent observer failure.
    };

    /// \brief Details for a successfully applied page.
    struct SyncWorkerPageEvent {
        std::size_t pages_pulled = 0; ///< 1-based page count in the round.
        std::size_t batches_applied = 0; ///< Batches applied by this page.
        bool        has_more = false; ///< Whether the peer reported more pages.
        SyncCursor  applied_cursor; ///< Receiver cursor after the page commit.
    };

    /// \brief Optional observer for \c SyncWorker progress and stage changes.
    /// \details Callbacks are invoked synchronously on the thread that runs the
    /// sync round: the caller thread for \c run_once() and the worker thread for
    /// \c start(). Implementations should return quickly and must not call
    /// caller-serialized lifecycle methods such as \c stop(), \c join(), or
    /// \c run_once() from a worker callback. Exceptions are caught and recorded
    /// in \c last_observer_error(); they do not fail the sync round and do not
    /// overwrite \c last_error().
    class ISyncWorkerObserver {
    public:
        virtual ~ISyncWorkerObserver() {}

        /// \brief Called when a sync round enters a coarse-grained stage.
        /// \details This callback is intended for logging, metrics, and UI
        /// status updates. It complements the more specific page, round, and
        /// backoff callbacks below.
        virtual void on_sync_worker_stage_changed(
                const SyncWorkerStageEvent& event) {
            (void)event;
        }

        /// \brief Called after a pulled page was applied locally.
        virtual void on_sync_worker_page_applied(
                const SyncWorkerPageEvent& event) {
            (void)event;
        }

        /// \brief Called after one sync round finishes.
        virtual void on_sync_worker_round_completed(
                const SyncWorkerRoundResult& result) {
            (void)result;
        }

        /// \brief Called when the background worker enters backoff.
        virtual void on_sync_worker_backoff(
                const SyncWorkerRoundResult& result,
                std::chrono::milliseconds delay) {
            (void)result;
            (void)delay;
        }
    };

    /// \brief Drives pull/apply sync in a caller-owned lifecycle.
    /// \details The worker owns only its std::thread. It does not own the
    /// supplied \c SyncEngine or \c ISyncPeer; both must outlive the worker.
    /// The \c SyncWorker object itself must outlive its worker thread and must
    /// not be destroyed from callbacks running on that worker thread.
    /// Lifecycle mutations (\c start(), \c stop(), \c join(), \c run_once())
    /// must be serialized by the caller. State observers and stop signalling
    /// (\c state(), \c last_error(), \c last_observer_error(),
    /// \c wait_until_state(), \c request_stop()) are thread-safe.
    /// Optional \c ISyncWorkerObserver callbacks run synchronously on the
    /// thread that executes the sync round.
    ///
    /// The implementation never keeps a local MDBX transaction open while
        /// waiting in a remote peer call, idle sleep, or backoff sleep. Pulled
    /// batches are applied through \c SyncEngine::handle_push(), which opens
    /// and commits a short local write transaction for each pulled page. With
    /// opt-in full snapshot fallback, the worker accepts only a
    /// \c CompleteUserDatabase session, stages all pages, and the engine
    /// commits its fresh-replica replacement only after the final page.
    /// Stop requests cancel the active request token and call
    /// \c ISyncPeer::request_cancel() at most once for each observed in-flight
    /// peer call. Cancellation is best-effort: \c stop(), \c join(), and the
    /// destructor may still wait until the peer returns.
    class SyncWorker {
    public:
        /// \brief Constructs a worker over a local engine and remote peer.
        /// \param engine Local engine that applies pulled batches.
        /// \param peer Remote peer that serves pull requests.
        /// \param options Worker timing and pagination options.
        SyncWorker(SyncEngine& engine,
                   ISyncPeer& peer,
                   const SyncWorkerOptions& options = SyncWorkerOptions())
            : m_engine(engine),
              m_peer(peer),
              m_options(options),
              m_state(SyncWorkerState::Stopped),
              m_stop_requested(false),
              m_peer_call_active(false),
              m_peer_cancel_requested(false),
              m_peer_cancel_source() {
            validate_options(m_options);
        }

        /// \brief Requests stop and waits for the background worker to finish.
        /// \details May block until an in-flight remote peer call returns.
        /// The worker object must not be destroyed from its own worker thread.
        ~SyncWorker() {
            request_stop();
            join();
        }

        SyncWorker(const SyncWorker&) = delete;
        SyncWorker& operator=(const SyncWorker&) = delete;

        /// \brief Starts the background worker thread.
        /// \throws std::logic_error if a worker thread is already running.
        void start() {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_thread.joinable()) {
                    throw std::logic_error("SyncWorker is already running");
                }
                if (m_state != SyncWorkerState::Stopped &&
                    m_state != SyncWorkerState::Failed) {
                    throw std::logic_error("SyncWorker is not stopped");
                }
                m_stop_requested = false;
                m_peer_call_active = false;
                m_peer_cancel_requested = false;
                m_last_error.clear();
                m_last_observer_error.clear();
                m_state = SyncWorkerState::Starting;
                reset_status_locked();
            }
            m_state_changed.notify_all();
            try {
                m_thread = std::thread(&SyncWorker::thread_main, this);
            } catch (...) {
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    m_stop_requested = true;
                    m_peer_call_active = false;
                    m_peer_cancel_requested = false;
                    m_state = SyncWorkerState::Stopped;
                }
                m_state_changed.notify_all();
                throw;
            }
        }

        /// \brief Requests background worker shutdown.
        /// \details Cancels the active raw request token and calls
        /// \c ISyncPeer::request_cancel() outside the worker mutex at most
        /// once for each observed in-flight peer call.
        /// Cancellation is best-effort; the worker exits before applying a
        /// page returned after stop was requested unless that page has already
        /// passed the final apply gate immediately before \c handle_push().
        void request_stop() {
            bool cancel_peer = false;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_stop_requested = true;
                if (m_peer_call_active && !m_peer_cancel_requested) {
                    m_peer_cancel_source.request_cancel();
                    m_peer_cancel_requested = true;
                    cancel_peer = true;
                }
                if (m_state != SyncWorkerState::Stopped &&
                    m_state != SyncWorkerState::Failed) {
                    m_state = SyncWorkerState::Stopping;
                }
            }
            if (cancel_peer) {
                request_peer_cancel();
            }
            m_state_changed.notify_all();
        }

        /// \brief Joins the background worker thread if it is running.
        /// \throws std::logic_error if called from the worker thread.
        void join() {
            if (m_thread.joinable()) {
                if (m_thread.get_id() == std::this_thread::get_id()) {
                    throw std::logic_error("SyncWorker cannot join itself");
                }
                m_thread.join();
            }
        }

        /// \brief Requests stop and joins the background worker thread.
        /// \details May block until an in-flight \c ISyncPeer::pull() returns.
        /// \throws std::logic_error if called from the worker thread.
        void stop() {
            request_stop();
            join();
        }

        /// \brief Runs one pull/apply round on the calling thread.
        /// \return Round result with page and batch counts.
        SyncWorkerRoundResult run_once() {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_thread.joinable()) {
                    throw std::logic_error("SyncWorker is already running");
                }
                if (m_state != SyncWorkerState::Stopped &&
                    m_state != SyncWorkerState::Failed) {
                    throw std::logic_error("SyncWorker is not stopped");
                }
                m_stop_requested = false;
                m_peer_call_active = false;
                m_peer_cancel_requested = false;
                m_last_error.clear();
                m_last_observer_error.clear();
                reset_status_locked();
            }
            const SyncWorkerRoundResult result = run_once_impl();
            notify_round_completed(result);
            if (stop_requested()) {
                set_state(SyncWorkerState::Stopped);
                return result;
            }
            if (result.ok) {
                set_state(SyncWorkerState::Stopped);
            } else {
                set_last_error(result.error);
                set_state(SyncWorkerState::Failed);
            }
            return result;
        }

        /// \brief Returns the current worker state.
        SyncWorkerState state() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_state;
        }

        /// \brief Returns the most recent sync worker failure message.
        /// \details Observer callback exceptions are reported separately via
        /// \c last_observer_error() so they do not overwrite pull, apply,
        /// cancellation, or lifecycle errors.
        std::string last_error() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_last_error;
        }

        /// \brief Returns the most recent observer callback failure message.
        /// \details This diagnostic is independent from \c last_error().
        /// Observer exceptions never fail the current sync round. In
        /// background mode, a non-empty value may describe an earlier round;
        /// it is cleared when \c start() begins a new background session or
        /// when foreground \c run_once() begins a new round.
        std::string last_observer_error() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_last_observer_error;
        }

        /// \brief Returns a thread-safe snapshot of worker status.
        SyncWorkerStatus status() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            SyncWorkerStatus snapshot = m_status;
            snapshot.state = m_state;
            snapshot.last_error = m_last_error;
            snapshot.last_observer_error = m_last_observer_error;
            return snapshot;
        }

        /// \brief Waits until \p desired is observed or \p timeout expires.
        /// \param desired State to wait for.
        /// \param timeout Maximum wait duration.
        /// \return \c true when \p desired was observed.
        bool wait_until_state(SyncWorkerState desired,
                              std::chrono::milliseconds timeout) const {
            std::unique_lock<std::mutex> lock(m_mutex);
            return m_state_changed.wait_for(
                lock, timeout,
                [this, desired] { return m_state == desired; });
        }

    private:
        class PeerCallGuard {
        public:
            PeerCallGuard(SyncWorker& worker, CancellationToken& token)
                : m_worker(worker),
                  m_active(worker.begin_peer_call(token)) {
            }

            ~PeerCallGuard() {
                if (m_active) {
                    m_worker.end_peer_call();
                }
            }

            bool active() const {
                return m_active;
            }

            PeerCallGuard(const PeerCallGuard&) = delete;
            PeerCallGuard& operator=(const PeerCallGuard&) = delete;

        private:
            SyncWorker& m_worker;
            bool        m_active;
        };

        class FullSnapshotImportResetGuard {
        public:
            explicit FullSnapshotImportResetGuard(SyncEngine& engine)
                : m_engine(engine), m_active(true) {
            }

            ~FullSnapshotImportResetGuard() {
                if (!m_active) return;
                try {
                    m_engine.discard_full_snapshot_import();
                } catch (...) {
                }
            }

            void dismiss() {
                m_active = false;
            }

            FullSnapshotImportResetGuard(const FullSnapshotImportResetGuard&) = delete;
            FullSnapshotImportResetGuard& operator=(
                const FullSnapshotImportResetGuard&) = delete;

        private:
            SyncEngine& m_engine;
            bool        m_active;
        };

        static void validate_options(const SyncWorkerOptions& options) {
            if (options.max_batches == 0) {
                throw std::invalid_argument(
                    "SyncWorkerOptions::max_batches must be greater than zero");
            }
            if (options.max_bytes == 0) {
                throw std::invalid_argument(
                    "SyncWorkerOptions::max_bytes must be greater than zero");
            }
            if (options.max_single_batch_bytes == 0) {
                throw std::invalid_argument(
                    "SyncWorkerOptions::max_single_batch_bytes must be greater than zero");
            }
            if (options.idle_interval < std::chrono::milliseconds::zero()) {
                throw std::invalid_argument(
                    "SyncWorkerOptions::idle_interval must not be negative");
            }
            if (options.initial_backoff < std::chrono::milliseconds::zero()) {
                throw std::invalid_argument(
                    "SyncWorkerOptions::initial_backoff must not be negative");
            }
            if (options.max_backoff < std::chrono::milliseconds::zero()) {
                throw std::invalid_argument(
                    "SyncWorkerOptions::max_backoff must not be negative");
            }
            if (options.max_backoff < options.initial_backoff) {
                throw std::invalid_argument(
                    "SyncWorkerOptions::max_backoff must be at least initial_backoff");
            }
        }

        SyncWorkerRoundResult run_once_impl() {
            SyncWorkerRoundResult result;
            SyncWorkerProgressEstimate progress;
            try {
                notify_stage_changed(make_stage_event(
                    SyncWorkerStage::RoundStarted, result));

                PullRequest request;
                request.requester = m_engine.local_node_id();
                request.db_id = m_engine.db_uuid();
                request.have = m_engine.applied_cursor();
                request.max_batches = m_options.max_batches;
                request.max_bytes = m_options.max_bytes;
                request.max_single_batch_bytes =
                    m_options.max_single_batch_bytes;

                bool has_more = false;
                do {
                    if (stop_requested()) {
                        result.has_more = has_more;
                        return result;
                    }

                    const SyncCursor before = request.have;
                    PullResponse response;
                    {
                        CancellationToken cancel_token;
                        PeerCallGuard peer_call(*this, cancel_token);
                        if (!peer_call.active()) {
                            result.has_more = has_more;
                            return result;
                        }
                        notify_stage_changed(make_stage_event(
                            SyncWorkerStage::PullStarted, result));
                        request.cancel_token = cancel_token;
                        response = m_peer.pull(request);
                    }
                    {
                        SyncWorkerStageEvent event = make_stage_event(
                            SyncWorkerStage::PullFinished, result);
                        event.batches_in_page = response.batches.size();
                        event.has_more = response.has_more;
                        event.ok = response.ok;
                        event.error = response.error;
                        event.sync_error_code = response.error_code;
                        event.sync_error_retryable =
                            response.error_retryable;
                        if (response.ok) {
                            if (response.remote_tail_known) {
                                progress = make_progress_estimate(
                                    response.remote_tail,
                                    request.have,
                                    result.batches_applied);
                            } else {
                                progress = SyncWorkerProgressEstimate();
                            }
                            result.progress = progress;
                            event.progress = progress;
                        } else {
                            event.retry_hint = m_peer.last_retry_hint();
                        }
                        notify_stage_changed(event);
                    }
                    if (!response.ok) {
                        if (response.error_code ==
                                SyncResponseErrorCode::SnapshotRequired) {
                            if (m_options.enable_logical_recovery_fallback &&
                                m_peer.supports_logical_recovery()) {
                                return run_logical_recovery_fallback(request,
                                                                     result);
                            }
                            if (m_options.enable_full_snapshot_fallback) {
                                return run_full_snapshot_fallback(request, result);
                            }
                        }
                        result.ok = false;
                        result.error = response.error.empty()
                            ? "pull failed"
                            : response.error;
                        result.retry_hint = m_peer.last_retry_hint();
                        result.sync_error_code = response.error_code;
                        result.sync_error_retryable =
                            response.error_retryable;
                        return result;
                    }
                    ++result.pages_pulled;

                    has_more = response.has_more;
                    if (stop_requested()) {
                        result.has_more = has_more;
                        return result;
                    }

                    if (!response.batches.empty()) {
                        PushRequest apply;
                        apply.db_id = request.db_id;
                        apply.batches = response.batches;

                        if (!begin_apply_stage()) {
                            result.has_more = has_more;
                            return result;
                        }
                        {
                            SyncWorkerStageEvent event = make_stage_event(
                                SyncWorkerStage::ApplyStarted, result);
                            event.batches_in_page = response.batches.size();
                            event.has_more = has_more;
                            event.progress = progress;
                            notify_stage_changed(event);
                        }
                        PushResponse applied;
                        if (!enter_apply_gate()) {
                            result.has_more = has_more;
                            return result;
                        }
                        applied = m_engine.handle_push(apply);
                        SyncCursor after_apply = request.have;
                        if (applied.ok) {
                            after_apply = m_engine.applied_cursor();
                            if (response.remote_tail_known) {
                                progress = make_progress_estimate(
                                    response.remote_tail,
                                    after_apply,
                                    result.batches_applied +
                                        response.batches.size());
                            }
                        }
                        {
                            SyncWorkerStageEvent event = make_stage_event(
                                SyncWorkerStage::ApplyFinished, result);
                            event.batches_in_page = response.batches.size();
                            event.has_more = has_more;
                            event.ok = applied.ok;
                            event.error = applied.error;
                            event.sync_error_code = applied.error_code;
                            event.sync_error_retryable =
                                applied.error_retryable;
                            if (applied.ok) {
                                event.batches_applied =
                                    result.batches_applied +
                                    response.batches.size();
                                if (response.remote_tail_known) {
                                    event.progress = progress;
                                }
                            }
                            notify_stage_changed(event);
                        }
                        if (!applied.ok) {
                            result.ok = false;
                            result.error = applied.error.empty()
                                ? "apply failed"
                                : applied.error;
                            result.sync_error_code = applied.error_code;
                            result.sync_error_retryable =
                                applied.error_retryable;
                            return result;
                        }
                        result.batches_applied += response.batches.size();
                        result.progress = progress;
                        request.have = after_apply;
                    } else if (response.has_more) {
                        result.ok = false;
                        result.error = "pull reported has_more without batches";
                        return result;
                    }

                    if (!response.batches.empty()) {
                        SyncWorkerPageEvent event;
                        event.pages_pulled = result.pages_pulled;
                        event.batches_applied = response.batches.size();
                        event.has_more = has_more;
                        event.applied_cursor = request.have;
                        notify_page_applied(event);
                    }
                    if (has_more &&
                        request.have.last_seq_by_origin == before.last_seq_by_origin) {
                        result.ok = false;
                        result.error = "pull pagination made no cursor progress";
                        return result;
                    }
                    result.has_more = has_more;
                } while (has_more && m_options.drain_pages);

                if (!has_more && m_options.enable_logical_delivery &&
                    !stop_requested()) {
                    deliver_pending_logical_deliveries(result);
                }
            } catch (const std::exception& e) {
                result.ok = false;
                result.error = e.what();
                result.retry_hint = m_peer.last_retry_hint();
            } catch (...) {
                result.ok = false;
                result.error = "unknown sync worker error";
                result.retry_hint = m_peer.last_retry_hint();
            }
            return result;
        }

        void deliver_pending_logical_deliveries(
                SyncWorkerRoundResult& result) {
            const DbId destination = m_engine.db_uuid();
            std::size_t remaining = m_options.max_logical_deliveries;
            for (;;) {
                if (stop_requested()) {
                    return;
                }
                if (m_options.max_logical_deliveries != 0u &&
                    remaining == 0u) {
                    return;
                }

                if (!m_peer.supports_logical_delivery()) {
                    if (!m_engine.has_pending_logical_deliveries(destination)) {
                        return;
                    }
                    result.ok = false;
                    result.error =
                        "Sync peer does not support configured logical delivery";
                    return;
                }

                LogicalDeliveryHello remote_hello;
                {
                    CancellationToken cancel_token;
                    PeerCallGuard peer_call(*this, cancel_token);
                    if (!peer_call.active()) {
                        return;
                    }
                    remote_hello =
                        m_peer.logical_delivery_hello_with_cancel(&cancel_token);
                }
                const std::vector<LogicalDeliveryEnvelope> pending =
                    m_engine.pending_logical_deliveries(
                        destination, remote_hello.node_id, 1u);
                if (pending.empty()) {
                    return;
                }

                LogicalDeliveryDispatchResult dispatch;
                {
                    CancellationToken cancel_token;
                    PeerCallGuard peer_call(*this, cancel_token);
                    if (!peer_call.active()) {
                        return;
                    }
                    notify_stage_changed(make_stage_event(
                        SyncWorkerStage::LogicalDeliveryStarted, result));
                    dispatch = m_engine.deliver_pending_logical_deliveries(
                        m_peer, destination, remote_hello.node_id, 1u,
                        nullptr, &cancel_token);
                }
                result.logical_deliveries_delivered += dispatch.delivered;
                result.logical_acknowledged_through =
                    dispatch.acknowledged_through;
                {
                    SyncWorkerStageEvent event = make_stage_event(
                        SyncWorkerStage::LogicalDeliveryFinished, result);
                    event.ok = dispatch.ok;
                    event.error = dispatch.error;
                    event.sync_error_retryable = dispatch.retryable;
                    notify_stage_changed(event);
                }
                if (!dispatch.ok) {
                    result.ok = false;
                    result.error = dispatch.error.empty()
                        ? "logical delivery failed"
                        : dispatch.error;
                    result.sync_error_retryable = dispatch.retryable;
                    return;
                }
                if (dispatch.delivered == 0u) {
                    return;
                }
                if (remaining != 0u) {
                    --remaining;
                }
            }
        }

        SyncWorkerRoundResult run_logical_recovery_fallback(
                const PullRequest& incremental_request,
                SyncWorkerRoundResult result) {
            m_engine.discard_full_snapshot_import();
            FullSnapshotImportResetGuard reset_guard(m_engine);
            LogicalRecoveryRequest request;
            request.requester = incremental_request.requester;
            request.max_bytes = incremental_request.max_bytes;
            request.max_single_batch_bytes =
                incremental_request.max_single_batch_bytes;

            for (;;) {
                if (stop_requested()) {
                    result.has_more = true;
                    return result;
                }

                LogicalRecoveryResponse response;
                {
                    CancellationToken cancel_token;
                    PeerCallGuard peer_call(*this, cancel_token);
                    if (!peer_call.active()) {
                        result.has_more = true;
                        return result;
                    }
                    notify_stage_changed(make_stage_event(
                        SyncWorkerStage::PullStarted, result));
                    response = m_peer.logical_recovery(request);
                }
                {
                    SyncWorkerStageEvent event = make_stage_event(
                        SyncWorkerStage::PullFinished, result);
                    event.has_more = response.has_more;
                    event.ok = response.ok;
                    event.error = response.error;
                    event.sync_error_code = response.error_code;
                    event.sync_error_retryable = response.error_retryable;
                    notify_stage_changed(event);
                }
                if (!response.ok) {
                    m_engine.discard_full_snapshot_import();
                    result.ok = false;
                    result.error = response.error.empty()
                        ? "logical recovery pull failed"
                        : response.error;
                    result.retry_hint = m_peer.last_retry_hint();
                    result.sync_error_code = response.error_code;
                    result.sync_error_retryable = response.error_retryable;
                    return result;
                }
                if (response.snapshot_chunk.replacement_scope !=
                        FullSnapshotScope::CompleteUserDatabase ||
                    response.has_more != response.snapshot_chunk.has_more ||
                    response.has_baseline == response.has_more) {
                    m_engine.discard_full_snapshot_import();
                    result.ok = false;
                    result.error = "logical recovery returned an invalid response shape";
                    result.sync_error_code =
                        SyncResponseErrorCode::SnapshotSessionInvalid;
                    return result;
                }
                ++result.pages_pulled;
                if (stop_requested() || !begin_apply_stage() ||
                    !enter_apply_gate()) {
                    m_engine.discard_full_snapshot_import();
                    result.has_more = response.has_more;
                    return result;
                }
                {
                    SyncWorkerStageEvent event = make_stage_event(
                        SyncWorkerStage::ApplyStarted, result);
                    event.has_more = response.has_more;
                    notify_stage_changed(event);
                }
                const LogicalRecoveryBaseline* baseline = response.has_baseline
                    ? &response.baseline : nullptr;
                const FullSnapshotImportResult imported =
                    m_engine.apply_logical_recovery_chunk(
                        response.snapshot_chunk, baseline);
                {
                    SyncWorkerStageEvent event = make_stage_event(
                        SyncWorkerStage::ApplyFinished, result);
                    event.has_more = response.has_more;
                    event.ok = true;
                    notify_stage_changed(event);
                }
                if (response.has_more && imported.completed) {
                    m_engine.discard_full_snapshot_import();
                    result.ok = false;
                    result.error =
                        "logical recovery importer completed before final page";
                    return result;
                }
                if (!response.has_more && !imported.completed) {
                    m_engine.discard_full_snapshot_import();
                    result.ok = false;
                    result.error =
                        "logical recovery importer did not complete final page";
                    return result;
                }

                result.has_more = response.has_more;
                if (!response.has_more) {
                    result.progress = make_progress_estimate(
                        response.snapshot_chunk.source_tail,
                        m_engine.applied_cursor(), result.batches_applied);
                    result.sync_error_code = SyncResponseErrorCode::None;
                    result.sync_error_retryable = false;
                    reset_guard.dismiss();
                    return result;
                }
                request.snapshot_id = response.snapshot_chunk.snapshot_id;
                request.continuation = response.snapshot_chunk.continuation;
            }
        }

        SyncWorkerRoundResult run_full_snapshot_fallback(
                const PullRequest& incremental_request,
                SyncWorkerRoundResult result) {
            m_engine.discard_full_snapshot_import();
            FullSnapshotImportResetGuard reset_guard(m_engine);
            PullRequest request = incremental_request;
            request.request_full_snapshot = true;
            request.have = SyncCursor();
            request.full_snapshot_id.clear();
            request.full_snapshot_continuation.clear();
            request.cancel_token = CancellationToken();

            for (;;) {
                if (stop_requested()) {
                    result.has_more = true;
                    return result;
                }

                PullResponse response;
                {
                    CancellationToken cancel_token;
                    PeerCallGuard peer_call(*this, cancel_token);
                    if (!peer_call.active()) {
                        result.has_more = true;
                        return result;
                    }
                    notify_stage_changed(make_stage_event(
                        SyncWorkerStage::PullStarted, result));
                    request.cancel_token = cancel_token;
                    response = m_peer.pull(request);
                }
                {
                    SyncWorkerStageEvent event = make_stage_event(
                        SyncWorkerStage::PullFinished, result);
                    event.has_more = response.has_more;
                    event.ok = response.ok;
                    event.error = response.error;
                    event.sync_error_code = response.error_code;
                    event.sync_error_retryable = response.error_retryable;
                    notify_stage_changed(event);
                }
                if (!response.ok) {
                    m_engine.discard_full_snapshot_import();
                    result.ok = false;
                    result.error = response.error.empty()
                        ? "full snapshot pull failed"
                        : response.error;
                    result.retry_hint = m_peer.last_retry_hint();
                    result.sync_error_code = response.error_code;
                    result.sync_error_retryable = response.error_retryable;
                    return result;
                }
                if (!response.is_full_snapshot || !response.batches.empty() ||
                    response.has_more != response.snapshot_chunk.has_more) {
                    m_engine.discard_full_snapshot_import();
                    result.ok = false;
                    result.error =
                        "full snapshot pull returned an invalid response shape";
                    return result;
                }
                if (response.snapshot_chunk.replacement_scope !=
                    FullSnapshotScope::CompleteUserDatabase) {
                    m_engine.discard_full_snapshot_import();
                    result.ok = false;
                    result.error =
                        "full snapshot fallback requires CompleteUserDatabase scope";
                    result.sync_error_code =
                        SyncResponseErrorCode::SnapshotSessionInvalid;
                    result.sync_error_retryable = false;
                    return result;
                }
                ++result.pages_pulled;
                if (stop_requested() || !begin_apply_stage() ||
                    !enter_apply_gate()) {
                    m_engine.discard_full_snapshot_import();
                    result.has_more = response.has_more;
                    return result;
                }
                {
                    SyncWorkerStageEvent event = make_stage_event(
                        SyncWorkerStage::ApplyStarted, result);
                    event.has_more = response.has_more;
                    notify_stage_changed(event);
                }

                const FullSnapshotImportResult imported =
                    m_engine.apply_full_snapshot_chunk(response.snapshot_chunk);
                {
                    SyncWorkerStageEvent event = make_stage_event(
                        SyncWorkerStage::ApplyFinished, result);
                    event.has_more = response.has_more;
                    event.ok = true;
                    notify_stage_changed(event);
                }
                if (response.has_more && imported.completed) {
                    m_engine.discard_full_snapshot_import();
                    result.ok = false;
                    result.error =
                        "full snapshot importer completed before final page";
                    return result;
                }
                if (!response.has_more && !imported.completed) {
                    m_engine.discard_full_snapshot_import();
                    result.ok = false;
                    result.error =
                        "full snapshot importer did not complete final page";
                    return result;
                }

                result.has_more = response.has_more;
                if (!response.has_more) {
                    result.progress = response.remote_tail_known
                        ? make_progress_estimate(response.remote_tail,
                                                 m_engine.applied_cursor(),
                                                 result.batches_applied)
                        : SyncWorkerProgressEstimate();
                    result.sync_error_code = SyncResponseErrorCode::None;
                    result.sync_error_retryable = false;
                    reset_guard.dismiss();
                    return result;
                }
                request.full_snapshot_id =
                    response.snapshot_chunk.snapshot_id;
                request.full_snapshot_continuation =
                    response.snapshot_chunk.continuation;
            }
        }

        SyncWorkerStageEvent make_stage_event(
                SyncWorkerStage stage,
                const SyncWorkerRoundResult& result) const {
            SyncWorkerStageEvent event;
            event.stage = stage;
            event.state = state();
            event.pages_pulled = result.pages_pulled;
            event.batches_applied = result.batches_applied;
            event.logical_deliveries_delivered =
                result.logical_deliveries_delivered;
            event.logical_acknowledged_through =
                result.logical_acknowledged_through;
            event.has_more = result.has_more;
            event.ok = result.ok;
            event.progress = result.progress;
            event.error = result.error;
            event.retry_hint = result.retry_hint;
            event.sync_error_code = result.sync_error_code;
            event.sync_error_retryable = result.sync_error_retryable;
            return event;
        }

        static std::uint64_t saturating_add(std::uint64_t lhs,
                                            std::uint64_t rhs) {
            const std::uint64_t max =
                (std::numeric_limits<std::uint64_t>::max)();
            if (lhs > max - rhs) {
                return max;
            }
            return lhs + rhs;
        }

        static std::uint64_t size_to_u64(std::size_t value) {
            const std::uint64_t max =
                (std::numeric_limits<std::uint64_t>::max)();
            if (value > static_cast<std::size_t>(max)) {
                return max;
            }
            return static_cast<std::uint64_t>(value);
        }

        static std::uint64_t cursor_distance(const SyncCursor& from,
                                             const SyncCursor& to) {
            std::uint64_t distance = 0;
            std::map<NodeId, std::uint64_t>::const_iterator it =
                to.last_seq_by_origin.begin();
            for (; it != to.last_seq_by_origin.end(); ++it) {
                const std::uint64_t have = from.last_seq_for(it->first);
                if (it->second > have) {
                    distance = saturating_add(distance, it->second - have);
                }
            }
            return distance;
        }

        static SyncWorkerProgressEstimate make_progress_estimate(
                const SyncCursor& remote,
                const SyncCursor& have,
                std::size_t applied_in_round) {
            SyncWorkerProgressEstimate estimate;
            estimate.remote_tail_known = true;
            estimate.batches_applied = size_to_u64(applied_in_round);
            estimate.batches_remaining = cursor_distance(have, remote);
            estimate.batches_total =
                saturating_add(estimate.batches_applied,
                               estimate.batches_remaining);
            estimate.completion_ratio = estimate.batches_total == 0
                ? 1.0
                : static_cast<double>(estimate.batches_applied) /
                    static_cast<double>(estimate.batches_total);
            return estimate;
        }

        std::chrono::milliseconds retry_delay(
                const SyncWorkerRoundResult& result,
                std::chrono::milliseconds fallback) const {
            if (!result.retry_hint.available ||
                !result.retry_hint.retryable ||
                !result.retry_hint.has_retry_after) {
                return fallback;
            }
            const std::chrono::milliseconds hinted =
                retry_after_to_milliseconds(
                    result.retry_hint.retry_after_seconds);
            return hinted > m_options.max_backoff
                ? m_options.max_backoff
                : hinted;
        }

        static std::chrono::milliseconds retry_after_to_milliseconds(
                std::uint64_t seconds) {
            const std::chrono::milliseconds max_delay =
                (std::chrono::milliseconds::max)();
            const std::uint64_t max_count =
                static_cast<std::uint64_t>(max_delay.count());
            if (seconds > max_count / 1000u) {
                return max_delay;
            }
            return std::chrono::milliseconds(
                static_cast<std::chrono::milliseconds::rep>(
                    seconds * 1000u));
        }

        void notify_stage_changed(const SyncWorkerStageEvent& event) const {
            record_stage_status(event);
            if (m_options.observer == nullptr) {
                return;
            }
            try {
                m_options.observer->on_sync_worker_stage_changed(event);
            } catch (const std::exception& e) {
                record_observer_error("on_sync_worker_stage_changed", e.what());
            } catch (...) {
                record_observer_error("on_sync_worker_stage_changed", nullptr);
            }
        }

        void notify_page_applied(const SyncWorkerPageEvent& event) const {
            if (m_options.observer == nullptr) {
                return;
            }
            try {
                m_options.observer->on_sync_worker_page_applied(event);
            } catch (const std::exception& e) {
                record_observer_error("on_sync_worker_page_applied", e.what());
            } catch (...) {
                record_observer_error("on_sync_worker_page_applied", nullptr);
            }
        }

        void notify_round_completed(
                const SyncWorkerRoundResult& result) const {
            record_round_status(result);
            notify_stage_changed(make_stage_event(
                SyncWorkerStage::RoundCompleted, result));
            if (m_options.observer == nullptr) {
                return;
            }
            try {
                m_options.observer->on_sync_worker_round_completed(result);
            } catch (const std::exception& e) {
                record_observer_error("on_sync_worker_round_completed", e.what());
            } catch (...) {
                record_observer_error("on_sync_worker_round_completed", nullptr);
            }
        }

        void notify_backoff(const SyncWorkerRoundResult& result,
                            std::chrono::milliseconds delay) const {
            record_backoff_status(delay);
            SyncWorkerStageEvent event = make_stage_event(
                SyncWorkerStage::BackoffStarted, result);
            event.state = SyncWorkerState::Backoff;
            notify_stage_changed(event);
            if (m_options.observer == nullptr) {
                return;
            }
            try {
                m_options.observer->on_sync_worker_backoff(result, delay);
            } catch (const std::exception& e) {
                record_observer_error("on_sync_worker_backoff", e.what());
            } catch (...) {
                record_observer_error("on_sync_worker_backoff", nullptr);
            }
        }

        void record_observer_error(const char* callback,
                                   const char* detail) const {
            std::string error = "SyncWorker observer ";
            error += callback;
            error += " failed";
            if (detail != nullptr && detail[0] != '\0') {
                error += ": ";
                error += detail;
            }
            set_last_observer_error(error);
        }

        void reset_status_locked() const {
            m_status = SyncWorkerStatus();
            m_status.state = m_state;
        }

        void record_stage_status(
                const SyncWorkerStageEvent& event) const {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_status.state = m_state;
            m_status.current_stage = event.stage;
            m_status.last_stage_known = true;
            m_status.last_stage = event;
            m_status.last_progress = event.progress;
            m_status.last_retry_hint = event.retry_hint;
            m_status.last_sync_error_code = event.sync_error_code;
            m_status.last_sync_error_retryable =
                event.sync_error_retryable;
            if (event.stage == SyncWorkerStage::RoundStarted) {
                m_status.round_active = true;
                m_status.backoff_active = false;
                m_status.last_backoff_delay =
                    std::chrono::milliseconds::zero();
                m_status.last_round_started_at =
                    std::chrono::steady_clock::now();
                ++m_status.rounds_started;
            }
        }

        void record_round_status(
                const SyncWorkerRoundResult& result) const {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_status.last_round_known = true;
            m_status.last_round = result;
            m_status.last_progress = result.progress;
            m_status.last_retry_hint = result.retry_hint;
            m_status.last_sync_error_code = result.sync_error_code;
            m_status.last_sync_error_retryable =
                result.sync_error_retryable;
            m_status.round_active = false;
            m_status.last_round_finished_at =
                std::chrono::steady_clock::now();
            ++m_status.rounds_completed;
            if (result.ok) {
                ++m_status.rounds_succeeded;
            } else {
                ++m_status.rounds_failed;
            }
        }

        void record_backoff_status(
                std::chrono::milliseconds delay) const {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_status.backoff_active = true;
            m_status.last_backoff_delay = delay;
        }

        bool begin_peer_call(CancellationToken& token) const {
            bool started = false;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (!m_stop_requested) {
                    m_peer_cancel_source = CancellationSource();
                    token = m_peer_cancel_source.token();
                    m_state = SyncWorkerState::Pulling;
                    m_peer_call_active = true;
                    m_peer_cancel_requested = false;
                    started = true;
                }
            }
            if (started) {
                m_state_changed.notify_all();
            }
            return started;
        }

        void end_peer_call() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_peer_call_active = false;
            m_peer_cancel_requested = false;
        }

        bool begin_apply_stage() const {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_stop_requested) {
                    return false;
                }
                m_state = SyncWorkerState::Applying;
                m_status.state = m_state;
                m_status.backoff_active = false;
            }
            m_state_changed.notify_all();
            return true;
        }

        bool enter_apply_gate() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return !m_stop_requested;
        }

        void request_peer_cancel() const {
            try {
                m_peer.request_cancel();
            } catch (const std::exception& e) {
                set_last_error(std::string("peer cancellation failed: ") +
                               e.what());
            } catch (...) {
                set_last_error("peer cancellation failed");
            }
        }

        void thread_main() {
            std::chrono::milliseconds backoff = m_options.initial_backoff;
            bool failed = false;
            try {
                set_state(SyncWorkerState::Idle);
                while (!stop_requested()) {
                    const SyncWorkerRoundResult result = run_once_impl();
                    notify_round_completed(result);
                    if (stop_requested()) {
                        break;
                    }
                    if (result.ok) {
                        backoff = m_options.initial_backoff;
                        set_state(SyncWorkerState::Idle);
                        if (wait_for_stop(m_options.idle_interval)) {
                            break;
                        }
                    } else {
                        set_last_error(result.error);
                        if (should_stop_on_permanent_failure(result)) {
                            failed = true;
                            set_state(SyncWorkerState::Failed);
                            break;
                        }
                        set_state(SyncWorkerState::Backoff);
                        const std::chrono::milliseconds delay =
                            retry_delay(result, backoff);
                        notify_backoff(result, delay);
                        if (wait_for_stop(delay)) {
                            break;
                        }
                        backoff = next_backoff(backoff);
                    }
                }
                if (!failed) {
                    set_state(SyncWorkerState::Stopped);
                }
            } catch (const std::exception& e) {
                set_last_error(e.what());
                set_state(SyncWorkerState::Failed);
            } catch (...) {
                set_last_error("unknown sync worker thread error");
                set_state(SyncWorkerState::Failed);
            }
        }

        bool wait_for_stop(std::chrono::milliseconds duration) const {
            std::unique_lock<std::mutex> lock(m_mutex);
            return m_state_changed.wait_for(
                lock, duration,
                [this] { return m_stop_requested; });
        }

        std::chrono::milliseconds next_backoff(
                std::chrono::milliseconds current) const {
            if (current >= m_options.max_backoff) {
                return m_options.max_backoff;
            }
            if (current.count() >= m_options.max_backoff.count() / 2) {
                return m_options.max_backoff;
            }
            return current + current;
        }

        bool should_stop_on_permanent_failure(
                const SyncWorkerRoundResult& result) const {
            return m_options.permanent_failure_policy ==
                SyncWorkerPermanentFailurePolicy::StopWorker &&
                result.retry_hint.available &&
                !result.retry_hint.retryable;
        }

        bool stop_requested() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_stop_requested;
        }

        void set_state(SyncWorkerState state) const {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_state = state;
                m_status.state = state;
                if (state != SyncWorkerState::Backoff) {
                    m_status.backoff_active = false;
                }
            }
            m_state_changed.notify_all();
        }

        void set_last_error(const std::string& error) const {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_last_error = error;
        }

        void set_last_observer_error(const std::string& error) const {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_last_observer_error = error;
        }

        SyncEngine&                 m_engine;
        ISyncPeer&                  m_peer;
        SyncWorkerOptions           m_options;
        std::thread                 m_thread;
        mutable std::mutex          m_mutex;
        mutable std::condition_variable m_state_changed;
        mutable SyncWorkerState     m_state;
        mutable bool                m_stop_requested;
        mutable bool                m_peer_call_active;
        mutable bool                m_peer_cancel_requested;
        mutable CancellationSource  m_peer_cancel_source;
        mutable std::string         m_last_error;
        mutable std::string         m_last_observer_error;
        mutable SyncWorkerStatus    m_status;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SYNCWORKER_HPP_INCLUDED
