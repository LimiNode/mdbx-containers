#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SELECTIVE_SYNC_WORKER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SELECTIVE_SYNC_WORKER_HPP_INCLUDED

/// \file SelectiveSyncWorker.hpp
/// \brief Foreground orchestration for one selective-replication scope.

#include <cstdint>
#include <stdexcept>
#include <string>

namespace mdbxc {
namespace sync {

    /// \brief Bounded paging options for one selective worker.
    struct SelectiveSyncWorkerOptions {
        std::uint64_t max_batches = 1000u;
        std::uint64_t max_bytes = 4ULL * 1024ULL * 1024ULL;
        std::uint64_t max_single_batch_bytes = 4ULL * 1024ULL * 1024ULL;
        bool drain_pages = true;
    };

    /// \brief Result of one selective pull/apply round.
    struct SelectiveSyncWorkerRoundResult {
        std::size_t pages_pulled = 0u;
        std::size_t batches_applied = 0u;
        std::uint64_t receiver_sequence = 0u;
        std::uint64_t remote_tail = 0u;
        bool remote_tail_known = false;
        bool has_more = false;
        bool ok = true;
        std::string error;
        SelectiveReplicationErrorCode error_code =
            SelectiveReplicationErrorCode::None;
        bool error_retryable = false;
    };

    /// \brief Pulls and applies one immutable selective scope through a peer.
    /// \details One instance never invokes global raw pull/push. The durable
    /// scope cursor is owned by c SyncEngine; recreating a worker therefore
    /// resumes from the last committed sequence. c request_stop() may be
    /// called concurrently with c run_once() and forwards best-effort peer
    /// cancellation in addition to the request token.
    class SelectiveSyncWorker {
    public:
        SelectiveSyncWorker(SyncEngine& engine, ISyncPeer& peer,
                            const std::string& scope_id,
                            const SelectiveSyncWorkerOptions& options =
                                SelectiveSyncWorkerOptions())
            : m_engine(engine),
              m_peer(peer),
              m_scope_id(scope_id),
              m_options(options),
              m_cancel_source() {
            if (m_scope_id.empty()) {
                throw std::invalid_argument(
                    "SelectiveSyncWorker scope_id must not be empty");
            }
            if (m_options.max_batches == 0u ||
                m_options.max_bytes == 0u ||
                m_options.max_single_batch_bytes == 0u) {
                throw std::invalid_argument(
                    "SelectiveSyncWorker page bounds must be non-zero");
            }
        }

        SelectiveSyncWorker(const SelectiveSyncWorker&) = delete;
        SelectiveSyncWorker& operator=(const SelectiveSyncWorker&) = delete;

        /// \brief Requests cancellation of the current or next round.
        void request_stop() {
            m_cancel_source.request_cancel();
            try {
                m_peer.request_cancel();
            } catch (...) {
            }
        }

        /// \brief Runs one bounded pull/apply round on the calling thread.
        SelectiveSyncWorkerRoundResult run_once() {
            SelectiveSyncWorkerRoundResult result;
            if (!m_peer.supports_selective_replication()) {
                return fail(result,
                            "peer does not support selective replication",
                            SelectiveReplicationErrorCode::
                                UnsupportedSelectiveReplication,
                            false);
            }

            SelectiveReplicationHello remote;
            try {
                remote = m_peer.selective_replication_hello();
            } catch (const std::exception& error) {
                return fail(result, error.what(),
                            SelectiveReplicationErrorCode::
                                UnsupportedSelectiveReplication,
                            false);
            }
            const DbId local_db = m_engine.db_uuid();
            if (compare_node_id(remote.db_id, local_db) != 0) {
                return fail(result, "selective peer db_id mismatch",
                            SelectiveReplicationErrorCode::DbIdMismatch,
                            false);
            }
            if (!remote.capabilities.supports(
                    SelectiveReplicationCapability::ScopedPull)) {
                return fail(result, "peer does not advertise scoped pull",
                            SelectiveReplicationErrorCode::
                                UnsupportedSelectiveReplication,
                            false);
            }

            ScopedPullRequest request;
            request.requester = m_engine.local_node_id();
            request.db_id = local_db;
            request.scope_id = m_scope_id;
            request.have_sequence =
                m_engine.scoped_applied_sequence(m_scope_id);
            request.max_batches = m_options.max_batches;
            request.max_bytes = m_options.max_bytes;
            request.max_single_batch_bytes =
                m_options.max_single_batch_bytes;
            request.cancel_token = m_cancel_source.token();

            do {
                if (request.cancel_token.is_cancellation_requested()) {
                    return fail(result, "selective worker cancelled",
                                SelectiveReplicationErrorCode::None, true);
                }
                ScopedPullResponse pulled;
                try {
                    pulled = m_peer.scoped_pull(request);
                } catch (const std::exception& error) {
                    return fail(result, error.what(),
                                SelectiveReplicationErrorCode::None, true);
                }
                if (!pulled.ok) {
                    return fail(result,
                                pulled.error.empty()
                                    ? "selective pull failed" : pulled.error,
                                pulled.error_code,
                                pulled.error_retryable);
                }
                ++result.pages_pulled;
                result.remote_tail = pulled.remote_tail;
                result.remote_tail_known = pulled.remote_tail_known;
                result.has_more = pulled.has_more;
                if (compare_node_id(remote.node_id,
                        pulled.descriptor.designated_writer_origin) != 0) {
                    return fail(result,
                                "selective peer is not the designated writer",
                                SelectiveReplicationErrorCode::
                                    WrongDesignatedWriter,
                                false);
                }
                if (pulled.batches.empty()) {
                    if (pulled.has_more) {
                        return fail(result,
                                    "scoped pull made no pagination progress",
                                    SelectiveReplicationErrorCode::
                                        ScopedSequenceGap,
                                    true);
                    }
                    break;
                }

                ScopedPushRequest apply;
                apply.sender = remote.node_id;
                apply.db_id = local_db;
                apply.descriptor = pulled.descriptor;
                apply.batches = pulled.batches;
                apply.cancel_token = request.cancel_token;
                const ScopedPushResponse applied =
                    m_engine.handle_scoped_push(apply);
                if (!applied.ok) {
                    return fail(result,
                                applied.error.empty()
                                    ? "selective apply failed" : applied.error,
                                applied.error_code,
                                applied.error_retryable);
                }
                if (applied.receiver_sequence <= request.have_sequence) {
                    return fail(result,
                                "selective apply made no cursor progress",
                                SelectiveReplicationErrorCode::
                                    ScopedSequenceGap,
                                true);
                }
                result.batches_applied += pulled.batches.size();
                result.receiver_sequence = applied.receiver_sequence;
                request.have_sequence = applied.receiver_sequence;
            } while (result.has_more && m_options.drain_pages);

            result.receiver_sequence =
                m_engine.scoped_applied_sequence(m_scope_id);
            return result;
        }

    private:
        static SelectiveSyncWorkerRoundResult fail(
                SelectiveSyncWorkerRoundResult result,
                const std::string& error,
                SelectiveReplicationErrorCode error_code,
                bool retryable) {
            result.ok = false;
            result.error = error;
            result.error_code = error_code;
            result.error_retryable = retryable;
            return result;
        }

        SyncEngine& m_engine;
        ISyncPeer& m_peer;
        std::string m_scope_id;
        SelectiveSyncWorkerOptions m_options;
        CancellationSource m_cancel_source;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SELECTIVE_SYNC_WORKER_HPP_INCLUDED
