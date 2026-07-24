#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_SYNC_ENGINE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_SYNC_ENGINE_HPP_INCLUDED

/// \file SyncEngine.hpp
/// \brief Pull/push/apply coordinator for replication.
///
/// Lifecycle: call \c initialize_local_identity once per database, then drive
/// replication through \c handle_pull / \c handle_push / \c apply_batch.
/// \c handle_pull and \c handle_push open their own transactions on the
/// engine's \c Connection; \c apply_batch uses a caller-supplied write
/// transaction so multiple batches can be applied atomically.
///
/// Apply rules (v0.1):
///  - \c seq <= last_applied_seq: \c Skipped (redundant replay).
///  - \c seq == last_applied_seq + 1: \c Applied, ops applied in order.
///  - \c seq >  last_applied_seq + 1: \c Conflict (gap; caller must re-pull).
///  - incompatible persistent DBI flags: \c Conflict.
///
/// Self-origin batches are always \c Skipped (the receiver already has them).
///
/// Stores (MetaStore, ChangeLogStore, AppliedStore) are opened inside the
/// supplied transaction and are therefore valid only for its lifetime.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <mdbx.h>

#include "../common.hpp"
#include "common.hpp"
#include "ConflictPolicy.hpp"
#include "ChangeBatch.hpp"
#include "ChangeBatchCodec.hpp"
#include "ChangeOp.hpp"
#include "protocol.hpp"
#include "SyncCursor.hpp"
#include "stores/AppliedStore.hpp"
#include "stores/ChangeLogStore.hpp"
#include "stores/MetaStore.hpp"
#include "stores/OriginIndexStore.hpp"
#include "stores/SchemaRegistryStore.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Outcome of a single \c apply_batch call.
    enum class ApplyResult {
        Applied,  ///< Batch was applied to local DBIs.
        Skipped,  ///< Batch was redundant (seq <= last contiguous applied)
                  ///< or originated from the local node.
        Conflict, ///< Conflict detected; batch was not applied.
    };

    /// \brief More specific reason when \c ApplyResult::Conflict is returned.
    enum class ApplyConflictReason {
        None,                     ///< No conflict; result is not Conflict.
        SequenceGap,              ///< Batch seq is not last_applied_seq + 1.
        InconsistentBatchDbiFlags,///< One batch carries contradictory flags for one DBI.
        ExistingDbiFlagsMismatch, ///< Existing destination DBI rejects captured flags.
        ReservedDbiName,          ///< Incoming ChangeOp targets an internal DBI name.
    };

    /// \brief Detailed result for callers that need conflict diagnostics.
    /// \details \c apply_batch() preserves the original compact API. New code
    /// can use \c apply_batch_ex() when it needs to distinguish retryable
    /// sequence gaps from schema/DBI incompatibilities.
    struct ApplyOutcome {
        /// \brief Compact apply result, matching \c apply_batch().
        ApplyResult          result = ApplyResult::Applied;

        /// \brief Specific reason when \c result is \c ApplyResult::Conflict.
        /// \details Remains \c ApplyConflictReason::None for \c Applied and
        /// \c Skipped outcomes.
        ApplyConflictReason  conflict_reason = ApplyConflictReason::None;

        /// \brief Origin of the incoming batch for all outcome kinds.
        NodeId               origin_node_id{};

        /// \brief Last contiguous seq for \c origin_node_id before apply.
        /// \details For \c SequenceGap this is the receiver-side seq that
        /// made \c batch_seq non-contiguous. For successful \c Applied
        /// outcomes this is updated to the applied \c batch_seq.
        std::uint64_t        last_applied_seq = 0;

        /// \brief Incoming batch seq for all outcome kinds.
        std::uint64_t        batch_seq = 0;

        /// \brief DBI name for DBI-related conflicts.
        /// \details Set for \c InconsistentBatchDbiFlags and
        /// \c ExistingDbiFlagsMismatch, and \c ReservedDbiName.
        std::string          dbi_name;

        /// \brief Previously seen flags for \c InconsistentBatchDbiFlags.
        /// \details Not used for \c ExistingDbiFlagsMismatch; use
        /// \c actual_dbi_flags when available.
        std::uint32_t        expected_dbi_flags = 0;

        /// \brief Incoming persistent DBI flags for DBI-related conflicts.
        std::uint32_t        incoming_dbi_flags = 0;

        /// \brief Whether \c actual_dbi_flags contains probed existing flags.
        /// \details Only meaningful for \c ExistingDbiFlagsMismatch.
        bool                 actual_dbi_flags_available = false;

        /// \brief Existing persistent DBI flags when probe succeeds.
        /// \details Valid only when \c actual_dbi_flags_available is true.
        std::uint32_t        actual_dbi_flags = 0;

        /// \brief MDBX error code that caused a DBI preflight mismatch.
        /// \details Currently set for \c ExistingDbiFlagsMismatch.
        int                  mdbx_error_code = MDBX_SUCCESS;
    };

    /// \brief Pull/push/apply coordinator bound to a single \c Connection.
    class SyncEngine {
        struct PullOrigin {
            NodeId origin;
            std::uint64_t last_seq;
            bool has_last_seq;
        };

    public:
        /// \brief Constructs an engine bound to \p conn.
        /// \param conn Shared connection that owns the env and stores.
        /// \param policy Conflict resolution policy (default: \c Reject).
        explicit SyncEngine(std::shared_ptr<Connection> conn,
                            ConflictPolicy policy = ConflictPolicy::Reject)
            : m_conn(std::move(conn)), m_policy(policy) {
            if (m_policy == ConflictPolicy::LastWriterWins) {
                throw std::invalid_argument(
                    "ConflictPolicy::LastWriterWins is not implemented");
            }
        }

        /// \brief Initialises the local \c node_id and \c db_uuid.
        /// \details Throws when already initialised with different values.
        /// \param node_id Stable 16-byte identifier for this node.
        /// \param db_uuid Stable 16-byte identifier for the replicated database.
        void initialize_local_identity(const NodeId& node_id,
                                      const NodeId& db_uuid) {
            auto txn = m_conn->transaction(TransactionMode::WRITABLE);
            MetaStore meta(m_conn->env_handle());
            meta.open(txn.handle());
            initialize_system_stores(txn.handle());
            const NodeId existing_node = meta.get_node_id(txn.handle());
            const NodeId zero{};
            if (compare_node_id(existing_node, zero) != 0 &&
                compare_node_id(existing_node, node_id) != 0) {
                throw std::logic_error(
                    "SyncEngine already initialised with a different node_id");
            }
            const NodeId existing_db = meta.get_db_uuid(txn.handle());
            if (compare_node_id(existing_db, zero) != 0 &&
                compare_node_id(existing_db, db_uuid) != 0) {
                throw std::logic_error(
                    "SyncEngine already initialised with a different db_uuid");
            }
            meta.set_node_id(txn.handle(), node_id);
            meta.set_db_uuid(txn.handle(), db_uuid);
            meta.set_schema_version(txn.handle(), meta_schema_version());
            txn.commit();
        }

        /// \brief Returns the local \c node_id.
        NodeId local_node_id() const {
            auto txn = m_conn->transaction(TransactionMode::READ_ONLY);
            MetaStore meta(m_conn->env_handle());
            meta.open(txn.handle());
            return meta.get_node_id(txn.handle());
        }

        /// \brief Returns the database \c db_uuid.
        NodeId db_uuid() const {
            auto txn = m_conn->transaction(TransactionMode::READ_ONLY);
            MetaStore meta(m_conn->env_handle());
            meta.open(txn.handle());
            return meta.get_db_uuid(txn.handle());
        }

        /// \brief Returns the conflict resolution policy.
        ConflictPolicy policy() const noexcept { return m_policy; }

        /// \brief Applies a single \c ChangeBatch to local DBIs inside \p txn.
        /// \details See class-level docs for the seq / apply rules. The
        /// caller commits the transaction. User DBIs are opened lazily by
        /// name with the captured \c ChangeOp::dbi_flags plus
        /// \c MDBX_CREATE so destination tables keep compatible MDBX flags.
        ApplyResult apply_batch(MDBX_txn* txn, const ChangeBatch& batch) {
            return apply_batch_ex(txn, batch).result;
        }

        /// \brief Applies a batch and returns detailed conflict diagnostics.
        /// \details This is the diagnostic form of \c apply_batch(). It keeps
        /// the same write semantics while exposing whether a conflict was a
        /// sequence gap, an inconsistent batch schema, or a destination DBI
        /// flag mismatch.
        ApplyOutcome apply_batch_ex(MDBX_txn* txn, const ChangeBatch& batch) {
            txn = checked_external_txn(txn, "SyncEngine::apply_batch_ex");
            ApplyOutcome outcome = make_apply_outcome(ApplyResult::Applied, batch, 0);
            MetaStore meta(m_conn->env_handle());
            AppliedStore applied(m_conn->env_handle());
            meta.open(txn);
            applied.open(txn);

            const NodeId local_node = meta.get_node_id(txn);
            if (compare_node_id(batch.origin_node_id, local_node) == 0) {
                outcome.result = ApplyResult::Skipped;
                return outcome;
            }

            const std::uint64_t last = applied.last_applied_seq(txn, batch.origin_node_id);
            outcome.last_applied_seq = last;
            if (batch.seq <= last) {
                outcome.result = ApplyResult::Skipped;
                return outcome;
            }
            if (batch.seq != last + 1) {
                outcome.result = ApplyResult::Conflict;
                outcome.conflict_reason = ApplyConflictReason::SequenceGap;
                return outcome;
            }
            std::vector<BatchDbiFlags> batch_dbis;
            if (!collect_batch_dbi_flags(batch, batch_dbis, &outcome)) return outcome;

            std::unordered_map<std::string, MDBX_dbi> dbi_cache;
            if (!preflight_batch_user_dbis(txn, batch_dbis, dbi_cache, &outcome)) {
                return outcome;
            }
            for (const ChangeOp& op : batch.ops) {
                apply_one_op(txn, op, dbi_cache);
            }
            applied.set_last_applied_seq(txn, batch.origin_node_id, batch.seq);
            outcome.result = ApplyResult::Applied;
            outcome.conflict_reason = ApplyConflictReason::None;
            outcome.last_applied_seq = batch.seq;
            return outcome;
        }

        /// \brief Returns a stable short name for an apply conflict reason.
        static const char* apply_conflict_reason_name(ApplyConflictReason reason) {
            switch (reason) {
                case ApplyConflictReason::None:
                    return "none";
                case ApplyConflictReason::SequenceGap:
                    return "sequence_gap";
                case ApplyConflictReason::InconsistentBatchDbiFlags:
                    return "inconsistent_batch_dbi_flags";
                case ApplyConflictReason::ExistingDbiFlagsMismatch:
                    return "existing_dbi_flags_mismatch";
                case ApplyConflictReason::ReservedDbiName:
                    return "reserved_dbi_name";
            }
            return "unknown";
        }

        /// \brief Handles a pull request: reads the local changelog
        /// for batches newer than the requester's cursor.
        /// \details When \c request.have is empty, replays all retained
        /// changelog batches from seq=1 for all known origins. This is not
        /// a full database snapshot. Non-empty cursors still
        /// consider all known origins so newly discovered origins and
        /// multi-origin pagination are not stranded. Validates
        /// \c request.db_id against the local \c db_uuid; mismatched peers
        /// receive an empty response with \c ok=false.
        /// \c has_more is set to \c true when the loop stopped because of
        /// \c request.max_batches or the soft page budget
        /// \c request.max_bytes rather than running out of changelog entries.
        /// A single retained batch may exceed \c max_bytes but is rejected
        /// when it exceeds \c request.max_single_batch_bytes.
        PullResponse handle_pull(const PullRequest& request) {
            PullResponse out;
            if (!db_id_matches(request.db_id)) {
                out.ok = false;
                out.error = "db_id mismatch";
                out.error_code = SyncResponseErrorCode::DbIdMismatch;
                return out;
            }
            if (request.request_full_snapshot) {
                out.ok = false;
                out.error = "PullRequest::request_full_snapshot is not implemented";
                out.error_code =
                    SyncResponseErrorCode::UnsupportedFullSnapshot;
                return out;
            }

            MDBX_txn* txn = nullptr;
            check_mdbx(mdbx_txn_begin(m_conn->env_handle(), nullptr,
                                      MDBX_TXN_RDONLY, &txn),
                       "SyncEngine: failed to begin read txn for pull");
            /// RAII guard: aborts (releases handle + reader slot) instead of
            /// reset, because we never renew the same transaction here — reset
            /// would leak the MDBX_txn object across calls.
            struct Guard {
                MDBX_txn* t;
                ~Guard() { if (t) mdbx_txn_abort(t); }
            } guard{txn};

            MDBX_dbi changelog_dbi = open_changelog_ro(txn);
            if (changelog_dbi == 0) {
                out.remote_have = read_applied_cursor(txn, out.remote_have);
                out.remote_tail_known = true;
                return out;
            }

            return pull_changelog_page(txn, changelog_dbi, request);
        }

        /// \brief Returns retained changelog batches newer than
        /// \c request.have.
        /// \details Empty \c request.have replays all retained changelog
        /// batches from seq=1 for all known origins. This is not a full
        /// database snapshot. Non-empty cursors filter each origin
        /// independently and still include origins missing from the cursor.
        /// Origin discovery uses \c _mdbxc_origins when available, with a
        /// changelog scan fallback for pre-index databases. Indexed origin
        /// tails skip origins that have no new batches; changelog keys are
        /// still used for exact \c have_seq+1 seeks so old values are not
        /// decoded.
        /// Sets \c has_more=true when the walk stopped because of
        /// \c request.max_batches or the soft page budget
        /// \c request.max_bytes. A single retained batch may exceed
        /// \c max_bytes but is rejected when it exceeds
        /// \c request.max_single_batch_bytes.
        PullResponse pull_changelog_page(MDBX_txn* txn, MDBX_dbi dbi,
                                        const PullRequest& request) {
            PullResponse out;
            if (request.request_full_snapshot) {
                out.ok = false;
                out.error = "PullRequest::request_full_snapshot is not implemented";
                out.error_code =
                    SyncResponseErrorCode::UnsupportedFullSnapshot;
                return out;
            }
            txn = checked_external_txn(txn, "SyncEngine::pull_changelog_page");
            out.remote_have = read_applied_cursor(txn, out.remote_have);
            const std::vector<PullOrigin> origins = collect_known_origins(txn, dbi);
            out.remote_tail_known = copy_known_tail(origins, out.remote_tail);
            for (std::size_t i = 0; i < origins.size(); ++i) {
                if (!request_has_retained_start(txn, dbi, origins[i], request,
                                                out)) {
                    return out;
                }
            }
            std::size_t total_bytes = 0;
            bool truncated = false;
            for (std::size_t i = 0; i < origins.size(); ++i) {
                if (origin_is_at_tail(origins[i], request)) {
                    continue;
                }
                if (pull_origin_batches(txn, dbi, origins[i].origin,
                                        request, out, total_bytes)) {
                    if (!out.ok) {
                        return out;
                    }
                    truncated = true;
                    break;
                }
            }
            out.has_more = truncated;
            return out;
        }

        /// \brief Handles a push request: applies each batch in order.
        /// \details Atomic: when \c apply_batch returns \c Conflict for any
        /// batch, the transaction is rolled back (no partial commit) and
        /// \c ok is set to \c false. Validates \c request.db_id against the
        /// local \c db_uuid; mismatched peers receive \c ok=false with no
        /// side effects.
        PushResponse handle_push(const PushRequest& request) {
            PushResponse out;
            if (!db_id_matches(request.db_id)) {
                out.ok = false;
                out.error = "db_id mismatch";
                out.error_code = SyncResponseErrorCode::DbIdMismatch;
                out.receiver_have = applied_cursor();
                return out;
            }
            Connection::SyncApplyNotification notification;
            std::size_t applied_batches = 0;
            std::size_t applied_ops = 0;
            std::vector<std::string> affected_dbi_names;
            {
                const Connection::SyncApplyWriteGuard sync_apply_guard =
                    m_conn->sync_apply_write_guard();
                auto txn = m_conn->transaction(TransactionMode::WRITABLE);
                for (const ChangeBatch& batch : request.batches) {
                    const ApplyOutcome outcome =
                        apply_batch_ex(txn.handle(), batch);
                    if (outcome.result == ApplyResult::Conflict) {
                        txn.rollback();
                        out.ok = false;
                        out.error = apply_conflict_message(outcome);
                        out.error_code = SyncResponseErrorCode::ApplyConflict;
                        out.error_retryable =
                            outcome.conflict_reason ==
                                ApplyConflictReason::SequenceGap;
                        out.receiver_have = applied_cursor();
                        return out;
                    }
                    if (outcome.result == ApplyResult::Applied &&
                        !batch.ops.empty()) {
                        ++applied_batches;
                        applied_ops += batch.ops.size();
                        for (std::size_t i = 0; i < batch.ops.size(); ++i) {
                            add_unique_dbi_name(affected_dbi_names,
                                                batch.ops[i].dbi_name);
                        }
                    }
                }
                txn.commit();
                if (applied_batches != 0u) {
                    notification =
                        m_conn->mark_sync_apply_committed(applied_batches,
                                                          applied_ops,
                                                          affected_dbi_names);
                }
            }
            m_conn->notify_sync_apply_observers(notification);
            out.ok = true;
            out.receiver_have = applied_cursor();
            return out;
        }

        /// \brief Returns the current applied cursor across all known origins.
        SyncCursor applied_cursor() const {
            SyncCursor cur;
            auto txn = m_conn->transaction(TransactionMode::READ_ONLY);
            return read_applied_cursor(txn.handle(), cur);
        }

        /// \brief Reads the applied cursor using the caller-supplied txn.
        /// \details Used inside \c handle_pull to avoid opening a second
        /// sticky-thread transaction on the same thread.
        SyncCursor applied_cursor(MDBX_txn* txn) const {
            txn = checked_external_txn(txn, "SyncEngine::applied_cursor");
            SyncCursor cur;
            return read_applied_cursor(txn, cur);
        }

        /// \brief Builds a \c PushRequest that carries every local batch with
        /// \c seq in \c [from_seq, to_seq] (inclusive).
        /// \details Hides the system stores from callers: example code and
        /// future transports can call this instead of touching
        /// \c MetaStore / \c ChangeLogStore directly. The sender is always
        /// the local node id (derived from \c _mdbxc_meta); sending batches
        /// of other origins is not supported at this layer.
        ///
        /// Opens its own short-lived read-only transaction on the bound
        /// connection. The caller must not have another active transaction
        /// for the same connection on the current thread (Mdbx would return
        /// \c MDBX_BUSY). Right after a writable commit is fine.
        /// \param from_seq First \c seq to include (use 1 to send from the
        /// beginning; use the peer's \c applied_cursor + 1 to send a delta).
        /// \param to_seq Last \c seq to include (use 0 to send up to the
        /// current local tail, inclusive).
        /// \return A \c PushRequest ready to send to the peer. Empty
        /// \c batches when the requested range is empty (or past the tail).
        /// \throws std::runtime_error if the requested range is not
        /// contiguous in the local changelog (a hole means pruning,
        /// corruption, or a wrong origin).
        /// \throws MdbxException on database error.
        PushRequest make_push_request(std::uint64_t from_seq,
                                      std::uint64_t to_seq) const {
            PushRequest req;
            req.db_id  = db_uuid();
            if (to_seq != 0 && to_seq < from_seq) return req;
            auto txn = m_conn->transaction(TransactionMode::READ_ONLY);
            MetaStore meta(m_conn->env_handle());
            meta.open(txn.handle());
            ChangeLogStore log(m_conn->env_handle());
            log.open(txn.handle());
            req.sender = meta.get_node_id(txn.handle());
            if (to_seq == 0) {
                to_seq = meta.get_local_seq(txn.handle());
            }
            if (to_seq < from_seq) return req;
            std::vector<std::uint8_t> buf;
            for (std::uint64_t s = from_seq;; ++s) {
                if (!log.get(txn.handle(), req.sender, s, buf)) {
                    throw std::runtime_error(
                        "SyncEngine::make_push_request: changelog gap at seq " +
                        std::to_string(s) +
                        " (pruning, corruption, or wrong origin)");
                }
                req.batches.push_back(ChangeBatchCodec::decode_exact(buf));
                if (s == to_seq) break;
            }
            return req;
        }

    private:
        struct CursorGuard {
            explicit CursorGuard(MDBX_cursor* cursor) : raw(cursor) {}
            ~CursorGuard() { if (raw) mdbx_cursor_close(raw); }
            CursorGuard(const CursorGuard&) = delete;
            CursorGuard& operator=(const CursorGuard&) = delete;

            MDBX_cursor* raw;
        };

        /// \brief Returns true when \p request_db_id matches the local
        /// \c db_uuid. A zero \p request_db_id is rejected: callers must
        /// know the database identity before issuing pull/push requests.
        bool db_id_matches(const NodeId& request_db_id) const {
            const NodeId zero{};
            if (compare_node_id(request_db_id, zero) == 0) return false;
            return compare_node_id(request_db_id, db_uuid()) == 0;
        }

        void initialize_system_stores(MDBX_txn* txn) {
            txn = checked_external_txn(txn, "SyncEngine::initialize_system_stores");
            MetaStore meta(m_conn->env_handle());
            ChangeLogStore change_log(m_conn->env_handle());
            AppliedStore applied(m_conn->env_handle());
            SchemaRegistryStore schemas(m_conn->env_handle());
            meta.open(txn);
            change_log.open(txn);
            applied.open(txn);
            schemas.open(txn);
        }

        static ApplyOutcome make_apply_outcome(ApplyResult result,
                                               const ChangeBatch& batch,
                                               std::uint64_t last_applied_seq) {
            ApplyOutcome outcome;
            outcome.result = result;
            outcome.conflict_reason = ApplyConflictReason::None;
            outcome.origin_node_id = batch.origin_node_id;
            outcome.last_applied_seq = last_applied_seq;
            outcome.batch_seq = batch.seq;
            return outcome;
        }

        MDBX_txn* checked_external_txn(MDBX_txn* txn,
                                       const char* context) const {
            return checked_txn_env(txn, m_conn->env_handle(), context);
        }

        static std::string apply_conflict_message(const ApplyOutcome& outcome) {
            std::string message = std::string(apply_conflict_reason_name(outcome.conflict_reason)) +
                                  " while applying pushed batch";
            if (outcome.conflict_reason == ApplyConflictReason::SequenceGap) {
                message += " (last_applied_seq=" +
                           std::to_string(outcome.last_applied_seq) +
                           ", batch_seq=" + std::to_string(outcome.batch_seq) + ")";
            } else if (outcome.conflict_reason ==
                       ApplyConflictReason::InconsistentBatchDbiFlags) {
                message += " (dbi='" + outcome.dbi_name +
                           "', expected_flags=" +
                           std::to_string(outcome.expected_dbi_flags) +
                           ", incoming_flags=" +
                           std::to_string(outcome.incoming_dbi_flags) + ")";
            } else if (outcome.conflict_reason ==
                       ApplyConflictReason::ExistingDbiFlagsMismatch) {
                message += " (dbi='" + outcome.dbi_name +
                           "', incoming_flags=" +
                           std::to_string(outcome.incoming_dbi_flags);
                if (outcome.actual_dbi_flags_available) {
                    message += ", actual_flags=" +
                               std::to_string(outcome.actual_dbi_flags);
                } else {
                    message += ", actual_flags=unknown";
                }
                message += ", mdbx_error_code=" +
                           std::to_string(outcome.mdbx_error_code) + ")";
            } else if (outcome.conflict_reason ==
                       ApplyConflictReason::ReservedDbiName) {
                message += " (dbi='" + outcome.dbi_name + "')";
            }
            return message;
        }

        static void add_unique_dbi_name(std::vector<std::string>& names,
                                        const std::string& dbi_name) {
            for (std::size_t i = 0; i < names.size(); ++i) {
                if (names[i] == dbi_name) {
                    return;
                }
            }
            names.push_back(dbi_name);
        }

        static std::uint32_t persistent_dbi_flags_mask() {
            return static_cast<std::uint32_t>(MDBX_REVERSEKEY) |
                   static_cast<std::uint32_t>(MDBX_DUPSORT) |
                   static_cast<std::uint32_t>(MDBX_INTEGERKEY) |
                   static_cast<std::uint32_t>(MDBX_DUPFIXED) |
                   static_cast<std::uint32_t>(MDBX_INTEGERDUP) |
                   static_cast<std::uint32_t>(MDBX_REVERSEDUP);
        }

        static std::uint32_t persistent_dbi_flags(std::uint32_t dbi_flags) {
            return dbi_flags & persistent_dbi_flags_mask();
        }

        struct BatchDbiFlags {
            std::string name;
            std::uint32_t flags;
        };

        static bool collect_batch_dbi_flags(const ChangeBatch& batch,
                                            std::vector<BatchDbiFlags>& dbis,
                                            ApplyOutcome* outcome) {
            dbis.clear();
            std::unordered_map<std::string, std::vector<BatchDbiFlags>::size_type> index_by_name;
            for (const ChangeOp& op : batch.ops) {
                if (is_reserved_dbi_name(op.dbi_name)) {
                    if (outcome != nullptr) {
                        outcome->result = ApplyResult::Conflict;
                        outcome->conflict_reason =
                            ApplyConflictReason::ReservedDbiName;
                        outcome->dbi_name = op.dbi_name;
                        outcome->incoming_dbi_flags =
                            persistent_dbi_flags(op.dbi_flags);
                    }
                    return false;
                }
                const std::uint32_t flags = persistent_dbi_flags(op.dbi_flags);
                const std::pair<
                    std::unordered_map<std::string, std::vector<BatchDbiFlags>::size_type>::iterator,
                    bool> inserted =
                    index_by_name.insert(std::make_pair(op.dbi_name, dbis.size()));
                if (inserted.second) {
                    BatchDbiFlags entry;
                    entry.name = op.dbi_name;
                    entry.flags = flags;
                    dbis.push_back(entry);
                    continue;
                }
                const BatchDbiFlags& existing = dbis[inserted.first->second];
                if (existing.flags != flags) {
                    if (outcome != nullptr) {
                        outcome->result = ApplyResult::Conflict;
                        outcome->conflict_reason =
                            ApplyConflictReason::InconsistentBatchDbiFlags;
                        outcome->dbi_name = op.dbi_name;
                        outcome->expected_dbi_flags = existing.flags;
                        outcome->incoming_dbi_flags = flags;
                    }
                    return false;
                }
            }
            return true;
        }

        static bool open_existing_user_dbi(MDBX_txn* txn,
                                           const std::string& name,
                                           std::uint32_t dbi_flags,
                                           MDBX_dbi& dbi,
                                           int& error_code) {
            error_code = MDBX_SUCCESS;
            const std::uint32_t open_dbi_flags = persistent_dbi_flags(dbi_flags);
            MDBX_db_flags_t open_flags = static_cast<MDBX_db_flags_t>(open_dbi_flags);
            int rc = mdbx_dbi_open(txn, name.c_str(), open_flags, &dbi);
            if (rc == MDBX_INCOMPATIBLE &&
                (open_dbi_flags & static_cast<std::uint32_t>(MDBX_INTEGERKEY)) == 0) {
                const MDBX_db_flags_t integer_flags = static_cast<MDBX_db_flags_t>(
                    open_dbi_flags | static_cast<std::uint32_t>(MDBX_INTEGERKEY));
                rc = mdbx_dbi_open(txn, name.c_str(), integer_flags, &dbi);
            }
            if (rc == MDBX_NOTFOUND) {
                dbi = 0;
                return true;
            }
            if (rc == MDBX_INCOMPATIBLE || rc == MDBX_EINVAL) {
                dbi = 0;
                error_code = rc;
                return false;
            }
            check_mdbx(rc, "SyncEngine: failed to preflight user DBI '" + name + "'");
            return true;
        }

        static bool read_existing_user_dbi_flags(MDBX_txn* txn,
                                                 const std::string& name,
                                                 std::uint32_t& actual_flags) {
            actual_flags = 0;
            MDBX_dbi dbi = 0;
            const int rc = mdbx_dbi_open(txn, name.c_str(), MDBX_DB_ACCEDE, &dbi);
            if (rc == MDBX_NOTFOUND ||
                rc == MDBX_INCOMPATIBLE ||
                rc == MDBX_EINVAL) {
                return false;
            }
            check_mdbx(rc, "SyncEngine: failed to probe existing user DBI '" + name + "'");

            unsigned raw_flags = 0;
            check_mdbx(mdbx_dbi_flags(txn, dbi, &raw_flags),
                       "SyncEngine: failed to read flags for existing user DBI '" + name + "'");
            actual_flags = persistent_dbi_flags(raw_flags);
            return true;
        }

        static bool preflight_batch_user_dbis(MDBX_txn* txn,
                                              const std::vector<BatchDbiFlags>& dbis,
                                              std::unordered_map<std::string, MDBX_dbi>& cache,
                                              ApplyOutcome* outcome) {
            for (std::vector<BatchDbiFlags>::const_iterator it = dbis.begin();
                 it != dbis.end(); ++it) {
                MDBX_dbi dbi = 0;
                int error_code = MDBX_SUCCESS;
                if (!open_existing_user_dbi(txn, it->name, it->flags, dbi, error_code)) {
                    if (outcome != nullptr) {
                        outcome->result = ApplyResult::Conflict;
                        outcome->conflict_reason =
                            ApplyConflictReason::ExistingDbiFlagsMismatch;
                        outcome->dbi_name = it->name;
                        outcome->incoming_dbi_flags = it->flags;
                        outcome->actual_dbi_flags_available =
                            read_existing_user_dbi_flags(txn,
                                                         it->name,
                                                         outcome->actual_dbi_flags);
                        outcome->mdbx_error_code = error_code;
                    }
                    return false;
                }
                if (dbi != 0) {
                    cache[it->name] = dbi;
                }
            }
            return true;
        }

        static std::vector<std::uint8_t> make_changelog_key(const NodeId& origin,
                                                            std::uint64_t seq) {
            std::vector<std::uint8_t> out(24);
            std::memcpy(out.data(), origin.data(), 16);
            detail::write_u64_be(seq, out.data() + 16);
            return out;
        }

        static NodeId changelog_key_origin(const MDBX_val& key) {
            if (key.iov_len != 24) {
                throw std::runtime_error("SyncEngine: invalid changelog key size");
            }
            NodeId origin{};
            std::memcpy(origin.data(), key.iov_base, 16);
            return origin;
        }

        static std::uint64_t changelog_key_seq(const MDBX_val& key) {
            if (key.iov_len != 24) {
                throw std::runtime_error("SyncEngine: invalid changelog key size");
            }
            const std::uint8_t* bytes = static_cast<const std::uint8_t*>(key.iov_base);
            return detail::read_u64_be(bytes + 16);
        }

        static bool changelog_key_matches_origin(const MDBX_val& key,
                                                 const NodeId& origin) {
            return compare_node_id(changelog_key_origin(key), origin) == 0;
        }

        static PullOrigin make_pull_origin(const NodeId& origin) {
            PullOrigin out;
            out.origin = origin;
            out.last_seq = 0;
            out.has_last_seq = false;
            return out;
        }

        static PullOrigin make_pull_origin(const NodeId& origin,
                                           std::uint64_t last_seq) {
            PullOrigin out;
            out.origin = origin;
            out.last_seq = last_seq;
            out.has_last_seq = true;
            return out;
        }

        static bool origin_is_at_tail(const PullOrigin& origin,
                                      const PullRequest& request) {
            return origin.has_last_seq &&
                   request.have.last_seq_for(origin.origin) >= origin.last_seq;
        }

        static void set_snapshot_required(PullResponse& out,
                                          const PullOrigin& origin,
                                          std::uint64_t have_seq,
                                          bool earliest_known,
                                          std::uint64_t earliest_seq) {
            out.ok = false;
            out.batches.clear();
            out.has_more = false;
            out.error_code = SyncResponseErrorCode::SnapshotRequired;
            out.error_retryable = false;
            out.error = "requested changelog history was pruned for origin";
            out.error += " (have_seq=" + std::to_string(have_seq);
            if (origin.has_last_seq) {
                out.error += ", tail_seq=" + std::to_string(origin.last_seq);
            }
            if (earliest_known) {
                out.error += ", earliest_retained_seq=" +
                             std::to_string(earliest_seq);
            } else {
                out.error += ", earliest_retained_seq=none";
            }
            out.error += ")";
        }

        static void set_batch_too_large(PullResponse& out,
                                        const NodeId& origin,
                                        std::uint64_t seq,
                                        std::uint64_t batch_bytes,
                                        std::uint64_t limit) {
            (void)origin;
            out.ok = false;
            out.batches.clear();
            out.has_more = false;
            out.error_code = SyncResponseErrorCode::BatchTooLarge;
            out.error_retryable = false;
            out.error = "retained changelog batch exceeds max_single_batch_bytes";
            out.error += " (seq=" + std::to_string(seq);
            out.error += ", batch_bytes=" + std::to_string(batch_bytes);
            out.error += ", max_single_batch_bytes=" + std::to_string(limit);
            out.error += ")";
        }

        static bool request_has_retained_start(MDBX_txn* txn,
                                               MDBX_dbi dbi,
                                               const PullOrigin& origin,
                                               const PullRequest& request,
                                               PullResponse& out) {
            if (origin_is_at_tail(origin, request)) {
                return true;
            }
            const std::uint64_t have_seq =
                request.have.last_seq_for(origin.origin);
            if (have_seq == std::numeric_limits<std::uint64_t>::max()) {
                return true;
            }
            std::uint64_t earliest_seq = 0;
            const bool earliest_known =
                changelog_earliest_seq(txn, dbi, origin.origin, earliest_seq);
            if (!earliest_known) {
                if (origin.has_last_seq && have_seq < origin.last_seq) {
                    set_snapshot_required(out, origin, have_seq,
                                          false, earliest_seq);
                    return false;
                }
                return true;
            }
            if (have_seq + 1 < earliest_seq) {
                set_snapshot_required(out, origin, have_seq,
                                      true, earliest_seq);
                return false;
            }
            return true;
        }

        static bool copy_known_tail(const std::vector<PullOrigin>& origins,
                                    SyncCursor& out) {
            for (std::size_t i = 0; i < origins.size(); ++i) {
                if (!origins[i].has_last_seq) {
                    out.last_seq_by_origin.clear();
                    return false;
                }
                out.last_seq_by_origin[origins[i].origin] =
                    origins[i].last_seq;
            }
            return true;
        }

        static std::vector<PullOrigin> collect_changelog_origins(MDBX_txn* txn,
                                                                 MDBX_dbi dbi) {
            std::vector<PullOrigin> origins;
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, dbi, &raw),
                       "pull_full: origin cursor open failed");
            CursorGuard guard(raw);

            MDBX_val k, v;
            std::vector<std::uint8_t> owned_key;
            int rc = mdbx_cursor_get(raw, &k, &v, MDBX_FIRST);
            while (rc == MDBX_SUCCESS) {
                const NodeId origin = changelog_key_origin(k);
                origins.push_back(make_pull_origin(origin));

                std::vector<std::uint8_t> next_key_buf =
                    make_changelog_key(origin, std::numeric_limits<std::uint64_t>::max());
                MDBX_val next_key = { next_key_buf.empty() ? nullptr : &next_key_buf[0],
                                      next_key_buf.size() };
                rc = mdbx_cursor_get(raw, &next_key, &v, MDBX_SET_RANGE);
                if (rc == MDBX_SUCCESS &&
                    compare_node_id(changelog_key_origin(next_key), origin) == 0) {
                    rc = mdbx_cursor_get(raw, &k, &v, MDBX_NEXT);
                } else if (rc == MDBX_SUCCESS) {
                    owned_key.resize(next_key.iov_len);
                    if (next_key.iov_len > 0) {
                        std::memcpy(owned_key.data(), next_key.iov_base, next_key.iov_len);
                    }
                    k.iov_base = owned_key.empty() ? nullptr : &owned_key[0];
                    k.iov_len = owned_key.size();
                }
            }
            if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "pull_full: origin cursor walk failed");
            }
            return origins;
        }

        std::vector<PullOrigin> collect_indexed_origins(MDBX_txn* txn) const {
            OriginIndexStore origins(m_conn->env_handle());
            if (!origins.open_existing(txn)) {
                return std::vector<PullOrigin>();
            }
            const std::vector<OriginIndexStore::OriginTail> tails =
                origins.origin_tails(txn);
            std::vector<PullOrigin> out;
            out.reserve(tails.size());
            for (std::vector<OriginIndexStore::OriginTail>::const_iterator it =
                     tails.begin();
                 it != tails.end(); ++it) {
                out.push_back(make_pull_origin(it->origin, it->last_seq));
            }
            return out;
        }

        std::vector<PullOrigin> collect_known_origins(MDBX_txn* txn,
                                                      MDBX_dbi changelog_dbi) const {
            const std::vector<PullOrigin> indexed = collect_indexed_origins(txn);
            if (!indexed.empty()) {
                return indexed;
            }
            return collect_changelog_origins(txn, changelog_dbi);
        }

        static bool pull_origin_batches(MDBX_txn* txn,
                                        MDBX_dbi dbi,
                                        const NodeId& origin,
                                        const PullRequest& request,
                                        PullResponse& out,
                                        std::size_t& total_bytes) {
            const std::uint64_t have_seq = request.have.last_seq_for(origin);
            if (have_seq == std::numeric_limits<std::uint64_t>::max()) {
                return false;
            }

            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, dbi, &raw),
                       "pull_full: origin batch cursor open failed");
            CursorGuard guard(raw);

            std::vector<std::uint8_t> key_buf = make_changelog_key(origin, have_seq + 1);
            MDBX_val k = { key_buf.empty() ? nullptr : &key_buf[0], key_buf.size() };
            MDBX_val v;
            int rc = mdbx_cursor_get(raw, &k, &v, MDBX_SET_RANGE);
            while (rc == MDBX_SUCCESS && changelog_key_matches_origin(k, origin)) {
                if (out.batches.size() >= request.max_batches ||
                    total_bytes >= request.max_bytes) {
                    return true;
                }

                const std::uint64_t key_seq = changelog_key_seq(k);
                if (v.iov_len > request.max_single_batch_bytes) {
                    set_batch_too_large(
                        out, origin, key_seq,
                        static_cast<std::uint64_t>(v.iov_len),
                        request.max_single_batch_bytes);
                    return true;
                }
                std::vector<std::uint8_t> buf(v.iov_len);
                if (v.iov_len > 0) {
                    std::memcpy(buf.data(), v.iov_base, v.iov_len);
                }
                const ChangeBatch batch = ChangeBatchCodec::decode_exact(buf);
                if (compare_node_id(batch.origin_node_id, origin) != 0 ||
                    batch.seq != key_seq) {
                    throw std::runtime_error("SyncEngine: changelog key/value mismatch");
                }

                out.batches.push_back(batch);
                total_bytes += v.iov_len;
                rc = mdbx_cursor_get(raw, &k, &v, MDBX_NEXT);
            }
            if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "pull_full: origin batch cursor walk failed");
            }
            return false;
        }

        static bool changelog_earliest_seq(MDBX_txn* txn,
                                           MDBX_dbi dbi,
                                           const NodeId& origin,
                                           std::uint64_t& out) {
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, dbi, &raw),
                       "pull_full: earliest retained cursor open failed");
            CursorGuard guard(raw);

            std::vector<std::uint8_t> key_buf = make_changelog_key(origin, 0);
            MDBX_val k = {
                key_buf.empty() ? nullptr : &key_buf[0],
                key_buf.size()
            };
            MDBX_val v;
            const int rc = mdbx_cursor_get(raw, &k, &v, MDBX_SET_RANGE);
            if (rc == MDBX_NOTFOUND) {
                return false;
            }
            check_mdbx(rc, "pull_full: earliest retained cursor get failed");
            if (!changelog_key_matches_origin(k, origin)) {
                return false;
            }
            out = changelog_key_seq(k);
            return true;
        }

        /// \brief Opens \c _mdbxc_changelog read-only when it exists; 0 otherwise.
        static MDBX_dbi open_changelog_ro(MDBX_txn* txn) {
            return open_store_ro(txn, "_mdbxc_changelog");
        }

        /// \brief Opens \c _mdbxc_applied read-only when it exists; 0 otherwise.
        static MDBX_dbi open_applied_ro(MDBX_txn* txn) {
            return open_store_ro(txn, "_mdbxc_applied");
        }

        static MDBX_dbi open_store_ro(MDBX_txn* txn, const char* name) {
            MDBX_dbi dbi = 0;
            const int rc = mdbx_dbi_open(txn, name, static_cast<MDBX_db_flags_t>(0), &dbi);
            if (rc == MDBX_NOTFOUND) return 0;
            check_mdbx(rc, std::string("SyncEngine: failed to open store '") + name + "'");
            return dbi;
        }

        static SyncCursor read_applied_cursor(MDBX_txn* txn, SyncCursor cur) {
            MDBX_dbi applied_dbi = open_applied_ro(txn);
            if (applied_dbi == 0) return cur;
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, applied_dbi, &raw),
                       "applied_cursor: cursor open failed");
            try {
                MDBX_val k, v;
                int rc = mdbx_cursor_get(raw, &k, &v, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    if (k.iov_len == 16) {
                        NodeId origin{};
                        std::memcpy(origin.data(), k.iov_base, 16);
                        if (v.iov_len == 8) {
                            const std::uint64_t seq = detail::read_u64_le(
                                static_cast<const std::uint8_t*>(v.iov_base));
                            cur.last_seq_by_origin[origin] = seq;
                        }
                    }
                    rc = mdbx_cursor_get(raw, &k, &v, MDBX_NEXT);
                }
                if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "applied_cursor: cursor walk failed");
                }
            } catch (...) {
                mdbx_cursor_close(raw);
                throw;
            }
            mdbx_cursor_close(raw);
            return cur;
        }

        static MDBX_dbi resolve_user_dbi(MDBX_txn* txn,
                                         const std::string& name,
                                         std::uint32_t dbi_flags,
                                         std::unordered_map<std::string, MDBX_dbi>& cache) {
            auto it = cache.find(name);
            if (it != cache.end() && it->second != 0) {
                return it->second;
            }
            MDBX_dbi dbi = 0;
            const std::uint32_t open_dbi_flags = persistent_dbi_flags(dbi_flags);
            const MDBX_db_flags_t open_flags = static_cast<MDBX_db_flags_t>(
                open_dbi_flags | static_cast<std::uint32_t>(MDBX_CREATE));
            int rc = mdbx_dbi_open(txn, name.c_str(), open_flags, &dbi);
            if (rc == MDBX_INCOMPATIBLE &&
                (open_dbi_flags & static_cast<std::uint32_t>(MDBX_INTEGERKEY)) == 0) {
                // Compatibility path for batches produced before dbi_flags
                // capture was implemented. Existing integer-key DBIs may
                // reject open_flags=MDBX_CREATE with MDBX_INCOMPATIBLE.
                const MDBX_db_flags_t integer_flags = static_cast<MDBX_db_flags_t>(
                    static_cast<std::uint32_t>(open_flags) |
                    static_cast<std::uint32_t>(MDBX_INTEGERKEY));
                rc = mdbx_dbi_open(txn, name.c_str(), integer_flags, &dbi);
            }
            check_mdbx(rc, "SyncEngine: failed to open user DBI '" + name + "'");
            cache[name] = dbi;
            return dbi;
        }

        static void apply_one_op(MDBX_txn* txn,
                                 const ChangeOp& op,
                                 std::unordered_map<std::string, MDBX_dbi>& cache) {
            MDBX_dbi dbi = resolve_user_dbi(txn, op.dbi_name, op.dbi_flags, cache);
            switch (op.op_type) {
                case ChangeOpType::Put: {
                    MDBX_val k = { op.storage_key.empty() ? nullptr
                                                           : const_cast<std::uint8_t*>(op.storage_key.data()),
                                   op.storage_key.size() };
                    MDBX_val v = { op.value.empty() ? nullptr
                                                     : const_cast<std::uint8_t*>(op.value.data()),
                                   op.value.size() };
                    check_mdbx(mdbx_put(txn, dbi, &k, &v, MDBX_UPSERT),
                               "SyncEngine: mdbx_put failed for DBI '" + op.dbi_name + "'");
                    return;
                }
                case ChangeOpType::Delete: {
                    MDBX_val k = { op.storage_key.empty() ? nullptr
                                                           : const_cast<std::uint8_t*>(op.storage_key.data()),
                                   op.storage_key.size() };
                    const int rc = mdbx_del(txn, dbi, &k, nullptr);
                    if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND) {
                        check_mdbx(rc, "SyncEngine: mdbx_del failed for DBI '" + op.dbi_name + "'");
                    }
                    cache.erase(op.dbi_name);
                    return;
                }
                case ChangeOpType::ClearTable: {
                    check_mdbx(mdbx_drop(txn, dbi, 0),
                               "SyncEngine: mdbx_drop failed for DBI '" + op.dbi_name + "'");
                    cache.erase(op.dbi_name);
                    return;
                }
            }
            throw std::logic_error("SyncEngine: unknown ChangeOpType");
        }

        std::shared_ptr<Connection> m_conn;
        ConflictPolicy              m_policy;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_SYNC_ENGINE_HPP_INCLUDED
