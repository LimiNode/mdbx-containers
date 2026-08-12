#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SYNCENGINE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SYNCENGINE_HPP_INCLUDED

/// \file engine/SyncEngine.hpp
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
#include <chrono>
#include <cstdint>
#include <cstring>
#if defined(MDBXC_TEST_LOGICAL_RECOVERY_MATERIALIZATION_CHECKPOINT)
#include <functional>
#endif
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <mdbx.h>

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
        MissingLwwRevision,       ///< LWW operation lacks a non-empty source revision.
        UnsupportedLwwOperation,  ///< LWW accepts only raw put/delete without identity remapping.
        LwwPolicyDisabled,        ///< A registered LWW DBI requires LastWriterWins policy.
        UnexpectedLwwRevision,    ///< A normal DBI received source-version metadata.
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
    class SyncEngine : public ILogicalDeliveryOutbox {
        struct PullOrigin {
            NodeId origin;
            std::uint64_t last_seq;
            bool has_last_seq;
        };

        struct FullSnapshotContinuation {
            std::size_t next_operation = 0u;
            std::uint64_t chunk_index = 0u;
        };

        struct MaterializationBudget {
            explicit MaterializationBudget(
                    const FullSnapshotExportOptions& options)
                : operations_left(options.max_materialized_operations),
                  bytes_left(options.max_materialized_bytes) {}

            void consume(std::uint64_t operations, std::uint64_t bytes,
                         const char* context) {
                if (operations > operations_left || bytes > bytes_left) {
                    throw std::length_error(context);
                }
                operations_left -= operations;
                bytes_left -= bytes;
            }

            std::uint64_t operations_left;
            std::uint64_t bytes_left;
        };

        class FullSnapshotCancelled : public std::runtime_error {
        public:
            FullSnapshotCancelled()
                : std::runtime_error("full snapshot materialization cancelled") {}
        };

        struct FullSnapshotSession {
            NodeId source_node_id{};
            DbId source_db_uuid{};
            NodeId requester{};
            std::string snapshot_id;
            SyncCursor source_tail;
            FullSnapshotScope replacement_scope =
                FullSnapshotScope::ManifestOnly;
            std::vector<FullSnapshotManifestEntry> manifest;
            std::vector<ChangeOp> operations;
            bool logical_recovery = false;
            LogicalRecoveryBaseline logical_recovery_baseline;
            std::map<std::string, FullSnapshotContinuation> continuations;
            std::chrono::steady_clock::time_point last_access;
            std::mutex mutex;
        };

        class FullSnapshotLogicalStateUnsupported : public std::runtime_error {
        public:
            explicit FullSnapshotLogicalStateUnsupported(
                    const std::string& message =
                        "complete full snapshot does not support persistent logical-sync state")
                : std::runtime_error(message) {}
        };

        class FullSnapshotVersionedDbiUnsupported : public std::runtime_error {
        public:
            explicit FullSnapshotVersionedDbiUnsupported(
                    const std::string& dbi_name)
                : std::runtime_error(
                    "full snapshot does not support registered versioned DBI: " +
                    dbi_name) {}
        };

        struct FullSnapshotImportSession {
            enum class ReplacementState {
                NotSeen,
                Cleared,
                ReceivingPuts
            };

            NodeId source_node_id{};
            DbId source_db_uuid{};
            std::string snapshot_id;
            SyncCursor source_tail;
            FullSnapshotScope replacement_scope =
                FullSnapshotScope::ManifestOnly;
            std::uint32_t manifest_version = 0u;
            std::vector<FullSnapshotManifestEntry> manifest;
            std::uint64_t next_chunk_index = 0u;
            std::uint64_t staged_bytes = 0u;
            std::vector<ChangeOp> operations;
            std::map<std::string, ReplacementState> replacement_state;
            bool logical_recovery = false;
        };

    public:
        /// \brief Constructs an engine bound to \p conn.
        /// \param conn Shared connection that owns the env and stores.
        /// \param policy Conflict resolution policy (default: \c Reject).
        explicit SyncEngine(std::shared_ptr<Connection> conn,
                            ConflictPolicy policy = ConflictPolicy::Reject,
                            const FullSnapshotExportOptions&
                                full_snapshot_options =
                                    FullSnapshotExportOptions())
            : m_conn(std::move(conn)),
              m_policy(policy),
              m_full_snapshot_options(full_snapshot_options) {
            validate_full_snapshot_export_options(m_full_snapshot_options);
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

        /// \brief Replaces the explicit source manifest used for full snapshots.
        /// \details An empty manifest disables source export. Reconfiguration
        /// invalidates all in-memory snapshot sessions, so callers must not
        /// change it while an export is in progress.
        void set_full_snapshot_export_options(
                const FullSnapshotExportOptions& options) {
            validate_full_snapshot_export_options(options);
            std::lock_guard<std::mutex> lock(m_full_snapshot_mutex);
            m_full_snapshot_options = options;
            m_full_snapshot_sessions.clear();
        }

        /// \brief Replaces the bounds for in-memory full snapshot import staging.
        /// \details Reconfiguration discards an incomplete import because its
        /// pending pages were never durable and must not outlive their bounds.
        void set_full_snapshot_import_options(
                const FullSnapshotImportOptions& options) {
            validate_full_snapshot_import_options(options);
            std::lock_guard<std::mutex> lock(m_full_snapshot_import_mutex);
            m_full_snapshot_import_options = options;
            m_full_snapshot_import_session.reset();
        }

        /// \brief Discards an incomplete in-memory full snapshot import.
        /// \details No user-DBI mutation has occurred before a snapshot final
        /// page, so this is safe after cancellation, transport failure, or a
        /// worker restart. It never alters durable replication metadata.
        void discard_full_snapshot_import() {
            std::lock_guard<std::mutex> lock(m_full_snapshot_import_mutex);
            m_full_snapshot_import_session.reset();
        }

        /// \brief Stages or atomically applies one full snapshot chunk.
        /// \details Every page is validated against immutable metadata from
        /// page zero. No destination user DBI changes before the final page;
        /// that page verifies a fresh manifest target and applies the complete
        /// staged plan in one write transaction. \c ManifestOnly replaces only
        /// its manifest DBIs and leaves global raw-sync progress unchanged.
        /// \c CompleteUserDatabase replaces the complete named-user-DBI
        /// inventory and bootstraps the applied cursor from its source tail.
        FullSnapshotImportResult apply_full_snapshot_chunk(
                const FullSnapshotChunk& chunk) {
            FullSnapshotCodec::validate(chunk);
            reject_versioned_full_snapshot_manifest(chunk.manifest);
            FullSnapshotImportResult result;
            Connection::SyncApplyNotification notification;
            bool notification_ready = false;
            {
                std::lock_guard<std::mutex> lock(
                    m_full_snapshot_import_mutex);
                try {
                    result = apply_full_snapshot_chunk_locked(
                        chunk, nullptr, false, notification, notification_ready);
                } catch (...) {
                    m_full_snapshot_import_session.reset();
                    throw;
                }
            }
            if (result.completed && notification_ready) {
                m_conn->notify_sync_apply_observers(notification);
            }
            return result;
        }

        /// \brief Stages or atomically applies one logical-aware recovery page.
        /// \details The final page carries its immutable logical baseline. The
        /// user-DBI replacement and logical schema/replay/order state commit in
        /// the same MDBX transaction; malformed metadata therefore leaves the
        /// destination unchanged.
        FullSnapshotImportResult apply_logical_recovery_chunk(
                const FullSnapshotChunk& chunk,
                const LogicalRecoveryBaseline* baseline = nullptr) {
            FullSnapshotCodec::validate(chunk);
            reject_versioned_full_snapshot_manifest(chunk.manifest);
            if (chunk.has_more ? baseline != nullptr : baseline == nullptr) {
                throw std::invalid_argument(
                    "logical recovery baseline must accompany only the final page");
            }
            FullSnapshotImportResult result;
            Connection::SyncApplyNotification notification;
            bool notification_ready = false;
            {
                std::lock_guard<std::mutex> lock(
                    m_full_snapshot_import_mutex);
                try {
                    result = apply_full_snapshot_chunk_locked(
                        chunk, baseline, true, notification, notification_ready);
                } catch (...) {
                    m_full_snapshot_import_session.reset();
                    throw;
                }
            }
            if (result.completed && notification_ready) {
                m_conn->notify_sync_apply_observers(notification);
            }
            return result;
        }

        /// \brief Serves one page of the explicit logical-aware recovery path.
        /// \details This does not alter raw \c handle_pull semantics. It is a
        /// distinct API so only a peer that opted into logical recovery can
        /// receive a CompleteUserDatabase baseline from a logical source.
        LogicalRecoveryResponse handle_logical_recovery(
                const LogicalRecoveryRequest& request,
                const CancellationToken* cancel_token = nullptr) {
            if (!db_id_matches(request.db_id)) {
                LogicalRecoveryResponse mismatch;
                mismatch.ok = false;
                mismatch.error = "db_id mismatch";
                mismatch.error_code = SyncResponseErrorCode::DbIdMismatch;
                return mismatch;
            }
            if (cancel_token != nullptr &&
                cancel_token->is_cancellation_requested()) {
                LogicalRecoveryResponse cancelled;
                cancelled.ok = false;
                cancelled.error = "logical recovery cancelled";
                cancelled.error_retryable = true;
                return cancelled;
            }
            PullRequest snapshot_request;
            snapshot_request.requester = request.requester;
            snapshot_request.db_id = request.db_id;
            snapshot_request.request_full_snapshot = true;
            snapshot_request.full_snapshot_id = request.snapshot_id;
            snapshot_request.full_snapshot_continuation = request.continuation;
            snapshot_request.max_bytes = request.max_bytes;
            snapshot_request.max_single_batch_bytes =
                request.max_single_batch_bytes;

            LogicalRecoveryBaseline baseline;
            const PullResponse snapshot_response = handle_full_snapshot_pull(
                snapshot_request, true, &baseline, cancel_token);
            LogicalRecoveryResponse out;
            out.ok = snapshot_response.ok;
            out.has_more = snapshot_response.has_more;
            out.error = snapshot_response.error;
            out.error_code = snapshot_response.error_code;
            out.error_retryable = snapshot_response.error_retryable;
            if (snapshot_response.ok) {
                out.snapshot_chunk = snapshot_response.snapshot_chunk;
                out.has_baseline = !snapshot_response.has_more;
                if (out.has_baseline) {
                    out.baseline = baseline;
                }
            }
            return out;
        }

#if defined(MDBXC_TEST_LOGICAL_RECOVERY_MATERIALIZATION_CHECKPOINT)
        /// \brief Installs a test-only barrier before logical recovery scans.
        void set_logical_recovery_materialization_checkpoint_for_testing(
                const std::function<void()>& checkpoint) {
            m_logical_recovery_materialization_checkpoint = checkpoint;
        }

        /// \brief Removes the test-only logical recovery barrier.
        void clear_logical_recovery_materialization_checkpoint_for_testing() {
            m_logical_recovery_materialization_checkpoint = std::function<void()>();
        }
#endif

        /// \brief Registers or verifies a persistent logical table schema.
        /// \details This is the normal lifecycle entry point for application
        /// logical schema markers. It opens the sync system DBIs and commits
        /// the schema registry update atomically. Direct
        /// \c SchemaRegistryStore usage remains available for tests,
        /// migrations, and repair utilities that already own a transaction.
        void register_logical_schema(const std::string& schema_id,
                                     const LogicalSchemaRecord& record) {
            auto txn = m_conn->transaction(TransactionMode::WRITABLE);
            initialize_system_stores(txn.handle());
            SchemaRegistryStore schemas(m_conn->env_handle());
            schemas.register_or_verify(txn.handle(), schema_id, record);
            txn.commit();
        }

        /// \brief Initializes fresh adapter storage or verifies marked storage.
        /// \details Adapters with auxiliary DBIs override
        /// \c ILogicalTableAdapter::initialize_storage() and
        /// \c ILogicalTableAdapter::verify_storage(). Fresh setup and the
        /// persistent marker belong to one transaction. Existing markers only
        /// verify their storage, so an apply path never treats missing
        /// auxiliary state as empty.
        void initialize_logical_adapter_schema(
                const ILogicalTableAdapter& adapter,
                const LogicalSchemaRecord& record) {
            const LogicalSchemaRef ref = adapter.schema_ref();
            if (!is_logical_schema_ref_complete(ref) ||
                record.kind != ref.kind ||
                record.schema_version != ref.schema_version) {
                throw std::invalid_argument(
                    "Logical adapter schema marker does not match adapter");
            }
            auto txn = m_conn->transaction(TransactionMode::WRITABLE);
            initialize_system_stores(txn.handle());
            SchemaRegistryStore schemas(m_conn->env_handle());
            LogicalSchemaRecord existing;
            if (schemas.get(txn.handle(), ref.schema_id, existing)) {
                // An existing marker describes durable layout. Verify it first
                // and never recreate an auxiliary DBI as implicit recovery.
                schemas.register_or_verify(txn.handle(), ref.schema_id, record);
                adapter.verify_storage(txn.handle());
            } else {
                adapter.initialize_storage(txn.handle());
                schemas.register_or_verify(txn.handle(), ref.schema_id, record);
            }
            const LogicalApplyResult marker_result =
                validate_logical_adapter_marker(
                    txn.handle(), m_conn->env_handle(), adapter);
            if (!marker_result.ok) {
                throw std::runtime_error(marker_result.error);
            }
            txn.commit();
        }

        /// \brief Migrates an existing persistent logical table schema marker.
        /// \details This is an explicit marker lifecycle operation. It does
        /// not migrate user table contents or changelog data. The current
        /// marker must match \p expected_existing exactly unless it already
        /// equals \p replacement, making retries idempotent.
        void migrate_logical_schema(
                const std::string& schema_id,
                const LogicalSchemaRecord& expected_existing,
                const LogicalSchemaRecord& replacement) {
            auto txn = m_conn->transaction(TransactionMode::WRITABLE);
            initialize_system_stores(txn.handle());
            SchemaRegistryStore schemas(m_conn->env_handle());
            schemas.migrate_or_verify(txn.handle(), schema_id,
                                      expected_existing, replacement);
            txn.commit();
        }

        /// \brief Registers a logical table adapter for engine-owned apply.
        /// \details Registration is a lifecycle operation and is not
        /// synchronized with concurrent \c apply_logical_changes() calls.
        void register_logical_adapter(ILogicalTableAdapter& adapter) {
            m_logical_registry.register_adapter(&adapter);
        }

        /// \brief Removes a logical table adapter by schema id.
        bool unregister_logical_adapter(const std::string& schema_id) {
            return m_logical_registry.unregister_adapter(schema_id);
        }

        /// \brief Returns the number of registered logical table adapters.
        std::size_t logical_adapter_count() const {
            return m_logical_registry.size();
        }

        /// \brief Applies logical table changes in an engine-owned write txn.
        /// \details This is the first engine integration point for
        /// \c LogicalTableRegistry. It does not change the transport wire
        /// format and is not called by \c handle_push() yet. Unknown schemas
        /// fail closed, all adapter preflights run before any apply, and any
        /// failure rolls back the transaction.
        LogicalApplyResult apply_logical_changes(
                const std::vector<LogicalChange>& changes) {
            if (changes.empty()) {
                return LogicalApplyResult::success();
            }

            std::vector<std::string> affected_dbi_names;
            collect_logical_affected_dbis(changes, affected_dbi_names);

            Connection::SyncApplyNotification notification;
            LogicalApplyResult result;
            {
                const Connection::SyncApplyWriteGuard sync_apply_guard =
                    m_conn->sync_apply_write_guard();
                auto txn = m_conn->transaction(TransactionMode::WRITABLE);
                result = validate_logical_schema_markers(
                    txn.handle(), changes);
                if (result.ok) {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_conn, txn.handle());
                    result = m_logical_registry.preflight_then_apply(
                        txn.handle(), changes, false);
                }
                if (!result.ok) {
                    txn.rollback();
                    return result;
                }
                txn.commit();
                try {
                    notification =
                        m_conn->mark_sync_apply_committed(
                            1u, changes.size(), affected_dbi_names);
                } catch (...) {
                    return result;
                }
            }
            m_conn->notify_sync_apply_observers(notification);
            return result;
        }

        /// \brief Applies a decoded logical change frame.
        /// \details This helper is an explicit logical sync path. Raw
        /// pull/push transport DTOs remain raw-DBI only.
        LogicalApplyResult apply_logical_frame(
                const LogicalChangeFrame& frame) {
            return apply_logical_changes(frame.changes);
        }

        /// \brief Decodes and applies a logical change frame.
        /// \details Malformed frame bytes fail closed as a logical apply
        /// failure before any adapter preflight or physical mutation.
        LogicalApplyResult apply_logical_frame_bytes(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            LogicalChangeFrame frame;
            try {
                frame = LogicalChangeFrameCodec::decode(encoded, bounds);
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("Logical change frame decode failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "Logical change frame decode failed");
            }

            try {
                return apply_logical_frame(frame);
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("Logical change frame apply failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "Logical change frame apply failed");
            }
        }

        /// \brief Applies a logical delivery envelope with atomic replay dedup.
        /// \details The envelope destination must match this engine's
        /// \c db_uuid. Envelopes whose origin is this engine's local
        /// \c node_id are treated as successful loopback no-ops before marker
        /// insertion. The delivery marker is written in the same MDBX write
        /// transaction as adapter mutations, so a failed apply rolls back both
        /// data and replay identity. Re-delivery of an already committed
        /// envelope is treated as a successful no-op.
        LogicalApplyResult apply_logical_delivery_envelope(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr) {
            try {
                validate_logical_delivery_envelope(envelope, bounds);
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "Logical delivery envelope validation failed");
            }

            const std::vector<LogicalChange>& changes =
                envelope.frame.changes;
            std::vector<std::string> affected_dbi_names;

            Connection::SyncApplyNotification notification;
            LogicalApplyResult result;
            bool notify = false;
            {
                const Connection::SyncApplyWriteGuard sync_apply_guard =
                    m_conn->sync_apply_write_guard();
                auto txn = m_conn->transaction(TransactionMode::WRITABLE);

                MetaStore meta(m_conn->env_handle());
                LogicalDeliveryStore delivery(m_conn->env_handle());
                meta.open(txn.handle());
                delivery.open(txn.handle());

                const DbId local_db_uuid = meta.get_db_uuid(txn.handle());
                const NodeId local_node_id = meta.get_node_id(txn.handle());
                if (compare_node_id(local_db_uuid,
                                    envelope.destination_db_uuid) != 0) {
                    txn.rollback();
                    return LogicalApplyResult::failure(
                        "Logical delivery envelope destination db_uuid "
                        "mismatch");
                }
                if (compare_node_id(local_node_id,
                                    envelope.origin_node_id) == 0) {
                    txn.rollback();
                    return LogicalApplyResult::success();
                }

                if (!delivery.try_mark_applied(
                        txn.handle(), envelope, bounds)) {
                    txn.rollback();
                    return LogicalApplyResult::success();
                }

                collect_logical_affected_dbis(
                    changes, affected_dbi_names);

                result = validate_logical_schema_markers(
                    txn.handle(), changes);
                if (result.ok) {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_conn, txn.handle());
                    result = m_logical_registry.preflight_then_apply(
                        txn.handle(), changes, false);
                }
                if (!result.ok) {
                    txn.rollback();
                    return result;
                }

                txn.commit();
                if (!changes.empty()) {
                    try {
                        notification =
                            m_conn->mark_sync_apply_committed(
                                1u, changes.size(), affected_dbi_names);
                        notify = true;
                    } catch (...) {
                        return result;
                    }
                }
            }
            if (notify) {
                m_conn->notify_sync_apply_observers(notification);
            }
            return result;
        }

        /// \brief Returns logical delivery features implemented by this engine.
        LogicalDeliveryCapabilities logical_delivery_capabilities() const {
            LogicalDeliveryCapabilities out;
            out.flags = static_cast<std::uint64_t>(
                LogicalDeliveryCapability::OrderedDelivery) |
                static_cast<std::uint64_t>(
                    LogicalDeliveryCapability::CumulativeAcknowledgement);
            return out;
        }

        /// \brief Builds this engine's capability hello after local init.
        LogicalDeliveryHello logical_delivery_hello() const {
            auto txn = m_conn->transaction(TransactionMode::READ_ONLY);
            MetaStore meta(m_conn->env_handle());
            meta.open(txn.handle());
            LogicalDeliveryHello out;
            out.node_id = meta.get_node_id(txn.handle());
            out.db_uuid = meta.get_db_uuid(txn.handle());
            if (is_zero_sync_id(out.node_id) || is_zero_sync_id(out.db_uuid)) {
                throw std::logic_error(
                    "SyncEngine local identity is not initialised");
            }
            out.capabilities = logical_delivery_capabilities();
            return out;
        }

        /// \brief Applies one strictly ordered logical delivery envelope.
        /// \details This is distinct from the legacy unordered delivery API.
        /// It accepts only the next contiguous origin sequence, acknowledges
        /// persisted exact duplicate and self-origin no-ops through their own
        /// sequence, and
        /// reports a gap as a retryable acknowledgement without invoking
        /// logical adapters. Exact duplicate validation does not require a
        /// currently registered adapter or schema marker.
        LogicalDeliveryAcknowledgement apply_ordered_logical_delivery_envelope(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr) {
            return apply_ordered_logical_delivery_envelope(
                envelope,
                static_cast<const LogicalDeliveryCapabilities*>(nullptr),
                bounds);
        }

        /// \brief Applies one ordered delivery with sender feature context.
        /// \details The envelope-only overload preserves the earlier public
        /// contract and therefore uses conservative acknowledgements.
        LogicalDeliveryAcknowledgement apply_ordered_logical_delivery_envelope(
                const LogicalDeliveryEnvelope& envelope,
                const LogicalDeliveryCapabilities* sender_capabilities,
                const CodecBounds* bounds = nullptr,
                const NodeId* receiver = nullptr) {
            LogicalDeliveryAcknowledgement acknowledgement;
            acknowledgement.destination_db_uuid = envelope.destination_db_uuid;
            acknowledgement.origin_node_id = envelope.origin_node_id;
            try {
                validate_logical_delivery_envelope(envelope, bounds);
            } catch (const std::exception& e) {
                set_logical_delivery_acknowledgement_failure(
                    acknowledgement, e.what(), false, bounds);
                return acknowledgement;
            } catch (...) {
                set_logical_delivery_acknowledgement_failure(
                    acknowledgement,
                    "Logical ordered delivery envelope validation failed",
                    false, bounds);
                return acknowledgement;
            }

            const std::vector<LogicalChange>& changes = envelope.frame.changes;
            std::vector<std::string> affected_dbi_names;
            Connection::SyncApplyNotification notification;
            bool notify = false;
            {
                const Connection::SyncApplyWriteGuard sync_apply_guard =
                    m_conn->sync_apply_write_guard();
                auto txn = m_conn->transaction(TransactionMode::WRITABLE);
                MetaStore meta(m_conn->env_handle());
                LogicalDeliveryStore delivery(m_conn->env_handle());
                LogicalDeliveryOrderStore order(m_conn->env_handle());
                meta.open(txn.handle());
                delivery.open(txn.handle());
                order.open(txn.handle());

                const DbId local_db_uuid = meta.get_db_uuid(txn.handle());
                const NodeId local_node_id = meta.get_node_id(txn.handle());
                acknowledgement.destination_db_uuid = local_db_uuid;
                acknowledgement.receiver_node_id = local_node_id;
                if (compare_node_id(local_db_uuid,
                                    envelope.destination_db_uuid) != 0) {
                    set_logical_delivery_acknowledgement_failure(
                        acknowledgement,
                        "Logical ordered delivery destination db_uuid mismatch",
                        false, bounds);
                    txn.rollback();
                    return acknowledgement;
                }
                if (receiver != nullptr &&
                    compare_node_id(local_node_id, *receiver) != 0) {
                    set_logical_delivery_acknowledgement_failure(
                        acknowledgement,
                        "Logical ordered delivery receiver node_id mismatch",
                        false, bounds);
                    txn.rollback();
                    return acknowledgement;
                }
                if (compare_node_id(local_node_id,
                                    envelope.origin_node_id) == 0) {
                    acknowledgement.acknowledged_through =
                        envelope.origin_sequence;
                    txn.rollback();
                    return acknowledgement;
                }

                const std::uint64_t last = order.last_applied(
                    txn.handle(), envelope.origin_node_id);
                acknowledgement.acknowledged_through = last;
                if (envelope.origin_sequence <= last) {
                    bool exact_replay = false;
                    try {
                        exact_replay = delivery.contains(
                            txn.handle(), envelope, bounds);
                    } catch (const std::exception& e) {
                        set_logical_delivery_acknowledgement_failure(
                            acknowledgement, e.what(), false, bounds);
                        txn.rollback();
                        return acknowledgement;
                    } catch (...) {
                        set_logical_delivery_acknowledgement_failure(
                            acknowledgement,
                            "Logical ordered delivery replay validation failed",
                            false, bounds);
                        txn.rollback();
                        return acknowledgement;
                    }
                    if (!exact_replay) {
                        set_logical_delivery_acknowledgement_failure(
                            acknowledgement,
                            "Logical ordered delivery replay identity is missing or conflicts",
                            false, bounds);
                        txn.rollback();
                        return acknowledgement;
                    }
                    if (sender_capabilities != nullptr &&
                        sender_capabilities->supports(
                            LogicalDeliveryCapability::CumulativeAcknowledgement)) {
                        acknowledgement.acknowledged_through = last;
                    } else {
                        acknowledgement.acknowledged_through =
                            envelope.origin_sequence;
                    }
                    txn.rollback();
                    return acknowledgement;
                }
                if (last == (std::numeric_limits<std::uint64_t>::max)() ||
                    envelope.origin_sequence != last + 1u) {
                    set_logical_delivery_acknowledgement_failure(
                        acknowledgement,
                        "Logical ordered delivery sequence gap", true, bounds);
                    txn.rollback();
                    return acknowledgement;
                }

                const LogicalApplyResult origin_validation =
                    validate_ordered_logical_schema_origins(
                        txn.handle(), changes, envelope.origin_node_id);
                if (!origin_validation.ok) {
                    set_logical_delivery_acknowledgement_failure(
                        acknowledgement, origin_validation.error,
                        origin_validation.retryable, bounds);
                    txn.rollback();
                    return acknowledgement;
                }
                if (!delivery.try_mark_applied(txn.handle(), envelope, bounds)) {
                    set_logical_delivery_acknowledgement_failure(
                        acknowledgement,
                        "Logical ordered delivery marker conflicts with order state",
                        false, bounds);
                    txn.rollback();
                    return acknowledgement;
                }

                collect_logical_affected_dbis(changes, affected_dbi_names);
                const LogicalApplyResult validation =
                    validate_logical_schema_markers(txn.handle(), changes);
                if (!validation.ok) {
                    set_logical_delivery_acknowledgement_failure(
                        acknowledgement, validation.error,
                        validation.retryable, bounds);
                    txn.rollback();
                    return acknowledgement;
                }
                LogicalApplyResult apply_result;
                {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_conn, txn.handle());
                    apply_result = m_logical_registry.preflight_then_apply(
                        txn.handle(), changes, true);
                }
                if (!apply_result.ok) {
                    set_logical_delivery_acknowledgement_failure(
                        acknowledgement, apply_result.error,
                        apply_result.retryable, bounds);
                    txn.rollback();
                    return acknowledgement;
                }
                order.advance(txn.handle(), envelope.origin_node_id,
                              envelope.origin_sequence);
                txn.commit();
                acknowledgement.acknowledged_through =
                    envelope.origin_sequence;
                if (!changes.empty()) {
                    try {
                        notification = m_conn->mark_sync_apply_committed(
                            1u, changes.size(), affected_dbi_names);
                        notify = true;
                    } catch (...) {
                        return acknowledgement;
                    }
                }
            }
            if (notify) {
                m_conn->notify_sync_apply_observers(notification);
            }
            return acknowledgement;
        }

        /// \brief Applies a receiver-bound ordered delivery request.
        LogicalDeliveryAcknowledgement apply_ordered_logical_delivery_request(
                const LogicalDeliveryRequest& request,
                const CodecBounds* bounds = nullptr) {
            return apply_ordered_logical_delivery_envelope(
                request.envelope, &request.sender_capabilities, bounds,
                &request.receiver_node_id);
        }

        /// \brief Decodes and applies a logical delivery envelope.
        LogicalApplyResult apply_logical_delivery_envelope_bytes(
                const std::vector<std::uint8_t>& encoded,
                const CodecBounds* bounds = nullptr) {
            LogicalDeliveryEnvelope envelope;
            try {
                envelope =
                    LogicalDeliveryEnvelopeCodec::decode(encoded, bounds);
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string(
                        "Logical delivery envelope decode failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "Logical delivery envelope decode failed");
            }

            try {
                return apply_logical_delivery_envelope(envelope, bounds);
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string(
                        "Logical delivery envelope apply failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "Logical delivery envelope apply failed");
            }
        }

        /// \brief Prunes acknowledged logical delivery replay markers.
        /// \details Persists a per-origin watermark and removes markers whose
        /// sequence is at or below \p safe_through_sequence in one write
        /// transaction. The caller must supply this boundary only after its
        /// delivery/acknowledgement protocol guarantees that no unseen
        /// envelope at or below it can arrive later. Future replay at or below
        /// the persisted watermark is a successful stale no-op.
        /// \return Number of removed replay markers.
        std::size_t prune_logical_delivery_markers(
                const NodeId& origin,
                std::uint64_t safe_through_sequence) {
            const Connection::SyncApplyWriteGuard sync_apply_guard =
                m_conn->sync_apply_write_guard();
            auto txn = m_conn->transaction(TransactionMode::WRITABLE);
            initialize_system_stores(txn.handle());
            LogicalDeliveryStore delivery(m_conn->env_handle());
            const std::size_t removed = delivery.prune_up_to(
                txn.handle(), origin, safe_through_sequence);
            txn.commit();
            return removed;
        }

        /// \brief Persists a locally-originated ordered logical delivery.
        /// \details Sequences are allocated per destination database and
        /// origin; receiver routes retain their own pending state.
        /// This only enqueues the envelope; transport dispatch and receiver
        /// acknowledgements are separate protocol layers.
        LogicalDeliveryEnvelope enqueue_logical_delivery(
                const DbId& destination,
                const NodeId& receiver,
                const LogicalChangeFrame& frame,
                const CodecBounds* bounds = nullptr) {
            auto txn = m_conn->transaction(TransactionMode::WRITABLE);
            const LogicalDeliveryEnvelope envelope = enqueue_logical_delivery(
                txn.handle(), destination, receiver, frame, bounds);
            txn.commit();
            return envelope;
        }

        /// \brief Persists one delivery in a caller-owned write transaction.
        /// \details This does not commit or roll back \p txn. It is the
        /// transaction-bound outbox API used by logical capture sessions to
        /// atomically commit their table mutations and queued delivery.
        LogicalDeliveryEnvelope enqueue_logical_delivery(
                MDBX_txn* txn,
                const DbId& destination,
                const NodeId& receiver,
                const LogicalChangeFrame& frame,
                const CodecBounds* bounds = nullptr) override {
            txn = checked_external_txn(
                txn, "SyncEngine::enqueue_logical_delivery");
            initialize_system_stores(txn);
            MetaStore meta(m_conn->env_handle());
            LogicalOutboxStore outbox(m_conn->env_handle());
            meta.open(txn);
            const NodeId origin = meta.get_node_id(txn);
            if (is_zero_sync_id(origin)) {
                throw std::logic_error(
                    "SyncEngine local node identity is not initialised");
            }
            return outbox.enqueue(txn, destination, receiver, origin, frame,
                                  bounds);
        }

        /// \brief Lists locally queued ordered deliveries for one receiver.
        std::vector<LogicalDeliveryEnvelope> pending_logical_deliveries(
                const DbId& destination,
                const NodeId& receiver,
                std::size_t limit = 0u,
                const CodecBounds* bounds = nullptr) const {
            auto txn = m_conn->transaction(TransactionMode::READ_ONLY);
            LogicalOutboxStore outbox(m_conn->env_handle());
            return outbox.list_pending(txn.handle(), destination, receiver,
                                       limit, bounds);
        }

        /// \brief Returns whether any receiver route has pending delivery.
        /// \details A raw-only worker uses this before it can obtain a
        /// receiver-specific logical hello.
        bool has_pending_logical_deliveries(
                const DbId& destination,
                const CodecBounds* bounds = nullptr) const {
            auto txn = m_conn->transaction(TransactionMode::READ_ONLY);
            LogicalOutboxStore outbox(m_conn->env_handle());
            return outbox.has_pending_for_destination(txn.handle(), destination,
                                                      bounds);
        }

        /// \brief Persists a cumulative acknowledgement and removes its prefix.
        std::size_t acknowledge_logical_deliveries(
                const DbId& destination,
                const NodeId& receiver,
                std::uint64_t acknowledged_through) {
            auto txn = m_conn->transaction(TransactionMode::WRITABLE);
            initialize_system_stores(txn.handle());
            LogicalOutboxStore outbox(m_conn->env_handle());
            const std::size_t removed = outbox.acknowledge_through(
                txn.handle(), destination, receiver, acknowledged_through);
            txn.commit();
            return removed;
        }

        /// \brief Returns the persisted cumulative acknowledgement frontier.
        std::uint64_t logical_delivery_acknowledged_through(
                const DbId& destination,
                const NodeId& receiver) const {
            auto txn = m_conn->transaction(TransactionMode::READ_ONLY);
            LogicalOutboxStore outbox(m_conn->env_handle());
            return outbox.acknowledged_through(txn.handle(), destination,
                                               receiver);
        }

        /// \brief Returns the highest locally persisted delivery sequence.
        /// \details This durable value bounds any cumulative acknowledgement
        /// accepted from a peer, including after a sender restart.
        std::uint64_t logical_delivery_known_tail(
                const DbId& destination,
                const NodeId& receiver) const {
            auto txn = m_conn->transaction(TransactionMode::READ_ONLY);
            LogicalOutboxStore outbox(m_conn->env_handle());
            return outbox.known_tail(txn.handle(), destination, receiver);
        }

        /// \brief Delivers a pending ordered outbox prefix to one capable peer.
        /// \details A negotiated cumulative acknowledgement can advance through
        /// the sender's persisted known tail, including after restart. A failed
        /// acknowledgement can advance only an earlier prefix; it can never
        /// acknowledge the delivery that failed.
        /// Existing raw sync peers do not implement \c ILogicalDeliveryPeer and
        /// therefore cannot be passed to this opt-in API.
        LogicalDeliveryDispatchResult deliver_pending_logical_deliveries(
                ILogicalDeliveryPeer& peer,
                const DbId& destination,
                const NodeId& receiver,
                std::size_t limit = 0u,
                const CodecBounds* bounds = nullptr,
                const CancellationToken* cancel_token = nullptr) {
            try {
                if (cancel_token != nullptr &&
                    cancel_token->is_cancellation_requested()) {
                    return LogicalDeliveryDispatchResult::failure(
                        "Logical delivery dispatch cancelled", true);
                }
                const LogicalDeliveryHello local = logical_delivery_hello();
                const LogicalDeliveryHello remote =
                    peer.logical_delivery_hello_with_cancel(cancel_token);
                if (compare_node_id(remote.db_uuid, destination) != 0) {
                    return LogicalDeliveryDispatchResult::failure(
                        "Logical delivery peer destination db_uuid mismatch");
                }
                if (compare_node_id(remote.node_id, receiver) != 0) {
                    return LogicalDeliveryDispatchResult::failure(
                        "Logical delivery peer receiver node_id mismatch");
                }
                if (!logical_delivery_capability_negotiated(
                        local.capabilities, remote.capabilities,
                        LogicalDeliveryCapability::OrderedDelivery)) {
                    return LogicalDeliveryDispatchResult::failure(
                        "Logical delivery peer does not support OrderedDelivery");
                }
                const bool cumulative_acknowledgement_negotiated =
                    logical_delivery_capability_negotiated(
                        local.capabilities, remote.capabilities,
                        LogicalDeliveryCapability::CumulativeAcknowledgement);

                LogicalDeliveryDispatchResult out;
                const std::vector<LogicalDeliveryEnvelope> pending =
                    pending_logical_deliveries(destination, receiver, limit,
                                               bounds);
                out.acknowledged_through =
                    logical_delivery_acknowledged_through(destination, receiver);
                const std::uint64_t known_tail =
                    logical_delivery_known_tail(destination, receiver);
                for (std::size_t i = 0; i < pending.size(); ++i) {
                    if (cancel_token != nullptr &&
                        cancel_token->is_cancellation_requested()) {
                        return LogicalDeliveryDispatchResult::failure(
                            "Logical delivery dispatch cancelled", true);
                    }
                    if (pending[i].origin_sequence <=
                        out.acknowledged_through) {
                        continue;
                    }
                    LogicalDeliveryRequest request;
                    request.receiver_node_id = receiver;
                    request.envelope = pending[i];
                    request.sender_capabilities = local.capabilities;
                    const LogicalDeliveryAcknowledgement acknowledgement =
                        peer.deliver_ordered_logical_request_with_cancel(
                            request, bounds, cancel_token);
                    try {
                        validate_logical_delivery_acknowledgement_for_sender(
                            acknowledgement, pending[i], receiver, known_tail,
                            cumulative_acknowledgement_negotiated, bounds);
                    } catch (const std::exception& e) {
                        return LogicalDeliveryDispatchResult::failure(
                            e.what());
                    }
                    if (acknowledgement.acknowledged_through >
                        out.acknowledged_through) {
                        acknowledge_logical_deliveries(
                            destination,
                            receiver,
                            acknowledgement.acknowledged_through);
                        out.acknowledged_through =
                            acknowledgement.acknowledged_through;
                    }
                    if (!acknowledgement.ok) {
                        out.ok = false;
                        out.retryable = acknowledgement.retryable;
                        out.error = acknowledgement.error;
                        return out;
                    }
                    ++out.delivered;
                }
                return out;
            } catch (const std::exception& e) {
                return LogicalDeliveryDispatchResult::failure(e.what(), true);
            } catch (...) {
                return LogicalDeliveryDispatchResult::failure(
                    "Logical delivery dispatch failed", true);
            }
        }

        /// \brief Prunes ordered-delivery replay markers through its frontier.
        /// \details This is safe only after the sender can no longer retry the
        /// pruned prefix. Ordered duplicate delivery requires the exact persisted
        /// marker to validate replay identity; a replay below the frontier whose
        /// marker was pruned is rejected fail-closed. Do not mix unordered
        /// envelopes from the same origin with this pruning lifecycle.
        std::size_t prune_ordered_logical_delivery_markers(
                const NodeId& origin) {
            const Connection::SyncApplyWriteGuard sync_apply_guard =
                m_conn->sync_apply_write_guard();
            auto txn = m_conn->transaction(TransactionMode::WRITABLE);
            initialize_system_stores(txn.handle());
            LogicalDeliveryOrderStore order(m_conn->env_handle());
            LogicalDeliveryStore delivery(m_conn->env_handle());
            const std::uint64_t frontier =
                order.last_applied(txn.handle(), origin);
            if (frontier == 0u) {
                txn.rollback();
                return 0u;
            }
            const std::size_t removed = delivery.prune_up_to(
                txn.handle(), origin, frontier);
            txn.commit();
            return removed;
        }

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
            VersionedDbiStore versioned_dbis(m_conn->env_handle());
            std::vector<bool> versioned_ops;
            if (!validate_versioned_dbi_batch(
                    txn, batch, versioned_dbis, versioned_ops, &outcome)) {
                return outcome;
            }
            std::vector<BatchDbiFlags> batch_dbis;
            if (!collect_batch_dbi_flags(batch, batch_dbis, &outcome)) return outcome;

            std::unordered_map<std::string, MDBX_dbi> dbi_cache;
            if (!preflight_batch_user_dbis(txn, batch_dbis, dbi_cache, &outcome)) {
                return outcome;
            }
            IdentityIndexStore identity_index(m_conn->env_handle());
            if (std::find(versioned_ops.begin(), versioned_ops.end(), true) !=
                versioned_ops.end()) {
                identity_index.open(txn);
            }
            for (std::size_t i = 0u; i < batch.ops.size(); ++i) {
                const ChangeOp& op = batch.ops[i];
                if (versioned_ops[i]) {
                    apply_lww_one_op(txn, op, batch.origin_node_id,
                                     batch.seq, dbi_cache, identity_index);
                } else {
                    apply_one_op(txn, op, dbi_cache);
                }
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
                case ApplyConflictReason::MissingLwwRevision:
                    return "missing_lww_revision";
                case ApplyConflictReason::UnsupportedLwwOperation:
                    return "unsupported_lww_operation";
                case ApplyConflictReason::LwwPolicyDisabled:
                    return "lww_policy_disabled";
                case ApplyConflictReason::UnexpectedLwwRevision:
                    return "unexpected_lww_revision";
            }
            return "unknown";
        }

        /// \brief Handles an incremental pull or explicitly configured
        /// full-snapshot export request.
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
        /// when it exceeds \c request.max_single_batch_bytes. Full snapshots
        /// require an empty receiver cursor and an explicit source manifest;
        /// they materialize stable, bounded source pages, while import remains
        /// a separate lifecycle operation.
        PullResponse handle_pull(const PullRequest& request) {
            PullResponse out;
            if (!db_id_matches(request.db_id)) {
                out.ok = false;
                out.error = "db_id mismatch";
                out.error_code = SyncResponseErrorCode::DbIdMismatch;
                return out;
            }
            if (request.request_full_snapshot) {
                return handle_full_snapshot_pull(request);
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
                out.error =
                    "request_full_snapshot is not handled by pull_changelog_page";
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
        static void set_logical_delivery_acknowledgement_failure(
                LogicalDeliveryAcknowledgement& acknowledgement,
                const std::string& error,
                bool retryable,
                const CodecBounds* bounds) {
            acknowledgement.ok = false;
            acknowledgement.retryable = retryable;
            acknowledgement.error =
                bounded_logical_delivery_acknowledgement_error(error, bounds);
        }

        struct CursorGuard {
            explicit CursorGuard(MDBX_cursor* cursor) : raw(cursor) {}
            ~CursorGuard() { if (raw) mdbx_cursor_close(raw); }
            CursorGuard(const CursorGuard&) = delete;
            CursorGuard& operator=(const CursorGuard&) = delete;

            MDBX_cursor* raw;
        };

        static bool is_reserved_snapshot_dbi(const std::string& name) {
            return name.size() >= 7u &&
                name.compare(0u, 7u, "_mdbxc_") == 0;
        }

        static void validate_full_snapshot_export_options(
                const FullSnapshotExportOptions& options) {
            switch (options.replacement_scope) {
                case FullSnapshotScope::ManifestOnly:
                case FullSnapshotScope::CompleteUserDatabase:
                    break;
                default:
                    throw std::invalid_argument(
                        "invalid full snapshot replacement scope");
            }
            if (options.replacement_scope ==
                    FullSnapshotScope::CompleteUserDatabase &&
                !options.manifest.empty()) {
                throw std::invalid_argument(
                    "complete full snapshot export enumerates its manifest");
            }
            if (options.replacement_scope == FullSnapshotScope::ManifestOnly &&
                options.manifest.empty()) {
                return;
            }
            if (options.max_materialized_operations == 0u ||
                options.max_materialized_bytes == 0u ||
                options.max_active_sessions == 0u ||
                options.session_idle_timeout <= std::chrono::seconds::zero()) {
                throw std::invalid_argument(
                    "full snapshot export limits must be non-zero");
            }
            for (std::size_t i = 0u; i < options.manifest.size(); ++i) {
                const FullSnapshotManifestEntry& entry = options.manifest[i];
                if (entry.dbi_name.empty() ||
                    is_reserved_snapshot_dbi(entry.dbi_name)) {
                    throw std::invalid_argument(
                        "full snapshot manifest contains a reserved or empty DBI");
                }
                if (i != 0u &&
                    options.manifest[i - 1u].dbi_name >= entry.dbi_name) {
                    throw std::invalid_argument(
                        "full snapshot manifest must be sorted and unique");
                }
            }
        }

        static bool full_snapshot_export_is_configured(
                const FullSnapshotExportOptions& options) {
            return options.replacement_scope ==
                FullSnapshotScope::CompleteUserDatabase ||
                !options.manifest.empty();
        }

        std::vector<FullSnapshotManifestEntry>
        enumerate_user_snapshot_manifest(MDBX_txn* txn) const {
            MDBX_dbi main_dbi = 0;
            check_mdbx(
                mdbx_dbi_open(
                    txn, static_cast<const char*>(MDBX_CHK_MAIN),
                    static_cast<MDBX_db_flags_t>(0), &main_dbi),
                "full snapshot failed to open MainDB for complete inventory");

            std::vector<FullSnapshotManifestEntry> manifest;
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, main_dbi, &raw),
                       "full snapshot failed to open MainDB cursor");
            CursorGuard cursor(raw);
            MDBX_val key;
            MDBX_val value;
            int rc = mdbx_cursor_get(raw, &key, &value, MDBX_FIRST);
            while (rc == MDBX_SUCCESS) {
                MDBX_dbi dbi = 0;
                const int open_rc = mdbx_dbi_open2(
                    txn, &key, MDBX_DB_ACCEDE, &dbi);
                if (open_rc == MDBX_SUCCESS) {
                    const char* name_data =
                        static_cast<const char*>(key.iov_base);
                    const std::string name(name_data, key.iov_len);
                    if (!is_reserved_snapshot_dbi(name)) {
                        unsigned raw_flags = 0u;
                        check_mdbx(
                            mdbx_dbi_flags(txn, dbi, &raw_flags),
                            "full snapshot failed to read complete DBI flags");
                        FullSnapshotManifestEntry entry;
                        entry.dbi_name = name;
                        entry.dbi_flags = persistent_dbi_flags(
                            static_cast<std::uint32_t>(raw_flags));
                        manifest.push_back(entry);
                    }
                } else if (open_rc != MDBX_INCOMPATIBLE &&
                           open_rc != MDBX_NOTFOUND) {
                    check_mdbx(
                        open_rc,
                        "full snapshot failed to inspect MainDB entry");
                }
                rc = mdbx_cursor_get(raw, &key, &value, MDBX_NEXT);
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc,
                           "full snapshot failed to scan complete DBI inventory");
            }
            std::sort(manifest.begin(), manifest.end(),
                [](const FullSnapshotManifestEntry& left,
                   const FullSnapshotManifestEntry& right) {
                    return left.dbi_name < right.dbi_name;
                });
            return manifest;
        }

        static std::uint64_t snapshot_materialized_bytes_for(
                const std::string& dbi_name,
                const MDBX_val& key,
                const MDBX_val& value) {
            const std::uint64_t max =
                (std::numeric_limits<std::uint64_t>::max)();
            if (dbi_name.size() > max || key.iov_len > max || value.iov_len > max) {
                throw std::length_error("full snapshot materialization size overflow");
            }
            const std::uint64_t name_size =
                static_cast<std::uint64_t>(dbi_name.size());
            const std::uint64_t key_size = static_cast<std::uint64_t>(key.iov_len);
            const std::uint64_t value_size = static_cast<std::uint64_t>(value.iov_len);
            const std::uint64_t operation_size =
                static_cast<std::uint64_t>(sizeof(ChangeOp));
            if (operation_size > max - name_size ||
                key_size > max - name_size - operation_size ||
                value_size > max - name_size - operation_size - key_size) {
                throw std::length_error("full snapshot materialization size overflow");
            }
            return name_size + operation_size + key_size + value_size;
        }

        static void append_full_snapshot_operation(
                FullSnapshotSession& session,
                MaterializationBudget& budget,
                const std::string& dbi_name,
                std::uint32_t dbi_flags,
                ChangeOpType op_type,
                const MDBX_val* key,
                const MDBX_val* value) {
            MDBX_val empty = { nullptr, 0u };
            const MDBX_val& actual_key = key == nullptr ? empty : *key;
            const MDBX_val& actual_value = value == nullptr ? empty : *value;
            const std::uint64_t add = snapshot_materialized_bytes_for(
                dbi_name, actual_key, actual_value);
            budget.consume(1u, add, "full snapshot exceeds materialization budget");

            ChangeOp op;
            op.op_type = op_type;
            op.dbi_name = dbi_name;
            op.dbi_flags = dbi_flags;
            if (actual_key.iov_len != 0u) {
                const std::uint8_t* bytes =
                    static_cast<const std::uint8_t*>(actual_key.iov_base);
                op.storage_key.assign(bytes, bytes + actual_key.iov_len);
            }
            if (actual_value.iov_len != 0u) {
                const std::uint8_t* bytes =
                    static_cast<const std::uint8_t*>(actual_value.iov_base);
                op.value.assign(bytes, bytes + actual_value.iov_len);
            }
            session.operations.push_back(std::move(op));
        }

        static void throw_if_cancelled(const CancellationToken* cancel_token) {
            if (cancel_token != nullptr &&
                cancel_token->is_cancellation_requested()) {
                throw FullSnapshotCancelled();
            }
        }

        std::string next_full_snapshot_id_locked() {
            ++m_next_full_snapshot_session_id;
            return std::string("snapshot-") +
                std::to_string(m_next_full_snapshot_session_id);
        }

        void prune_expired_full_snapshot_sessions_locked() {
            const std::chrono::steady_clock::time_point now =
                std::chrono::steady_clock::now();
            std::map<std::string,
                     std::shared_ptr<FullSnapshotSession>>::iterator it =
                m_full_snapshot_sessions.begin();
            while (it != m_full_snapshot_sessions.end()) {
                const std::chrono::steady_clock::duration idle =
                    now - it->second->last_access;
                if (idle >= m_full_snapshot_options.session_idle_timeout) {
                    m_full_snapshot_sessions.erase(it++);
                } else {
                    ++it;
                }
            }
        }

        void require_raw_only_complete_snapshot_source(MDBX_txn* txn) const {
            try {
                SchemaRegistryStore schemas(m_conn->env_handle());
                LogicalDeliveryStore delivery(m_conn->env_handle());
                LogicalDeliveryOrderStore order(m_conn->env_handle());
                LogicalOutboxStore outbox(m_conn->env_handle());
                if (schemas.has_entries(txn) ||
                    delivery.has_persistent_state(txn) ||
                    order.has_entries(txn) ||
                    outbox.has_persistent_state(txn)) {
                    throw FullSnapshotLogicalStateUnsupported();
                }
            } catch (const FullSnapshotLogicalStateUnsupported&) {
                throw;
            } catch (const std::exception& e) {
                throw FullSnapshotLogicalStateUnsupported(
                    std::string(
                        "complete full snapshot cannot validate persistent logical-sync state: ") +
                    e.what());
            }
        }

        void reject_versioned_full_snapshot_manifest(
                MDBX_txn* txn,
                const std::vector<FullSnapshotManifestEntry>& manifest) const {
            VersionedDbiStore registry(m_conn->env_handle());
            for (std::size_t i = 0u; i < manifest.size(); ++i) {
                if (registry.contains(txn, manifest[i].dbi_name)) {
                    throw FullSnapshotVersionedDbiUnsupported(
                        manifest[i].dbi_name);
                }
            }
        }

        void reject_versioned_full_snapshot_manifest(
                const std::vector<FullSnapshotManifestEntry>& manifest) const {
            auto txn = m_conn->transaction(TransactionMode::READ_ONLY);
            reject_versioned_full_snapshot_manifest(txn.handle(), manifest);
        }

        void collect_logical_recovery_baseline(
                MDBX_txn* txn,
                FullSnapshotSession& session,
                MaterializationBudget& budget,
                const CancellationToken* cancel_token) const {
            SchemaRegistryStore schemas(m_conn->env_handle());
            LogicalDeliveryStore delivery(m_conn->env_handle());
            LogicalDeliveryOrderStore order(m_conn->env_handle());
            LogicalOutboxStore outbox(m_conn->env_handle());
            session.logical_recovery_baseline.source_node_id =
                session.source_node_id;
            session.logical_recovery_baseline.source_db_uuid =
                session.source_db_uuid;
            session.logical_recovery_baseline.snapshot_id = session.snapshot_id;
            budget.consume(0u,
                logical_recovery_baseline_header_bytes(
                    session.logical_recovery_baseline),
                "logical recovery baseline exceeds materialization budget");
            schemas.for_each_entry(txn,
                [&session, &budget, cancel_token](
                        const LogicalSchemaRegistryEntry& entry) {
                    throw_if_cancelled(cancel_token);
                    budget.consume(1u, logical_recovery_schema_bytes(entry),
                        "logical recovery baseline exceeds materialization budget");
                    session.logical_recovery_baseline.schemas.push_back(entry);
                });
            delivery.for_each_marker(txn,
                [&session, &budget, cancel_token](
                        const LogicalDeliveryMarkerInfo& marker) {
                    throw_if_cancelled(cancel_token);
                    budget.consume(1u, logical_recovery_marker_bytes(marker),
                        "logical recovery baseline exceeds materialization budget");
                    session.logical_recovery_baseline.delivery_markers.push_back(marker);
                });
            delivery.for_each_watermark(txn,
                [&session, &budget, cancel_token](
                        const LogicalDeliveryWatermarkInfo& watermark) {
                    throw_if_cancelled(cancel_token);
                    budget.consume(1u, logical_recovery_watermark_bytes(watermark),
                        "logical recovery baseline exceeds materialization budget");
                    session.logical_recovery_baseline.delivery_watermarks.push_back(
                        watermark);
                });
            order.for_each_entry(txn,
                [&session, &budget, cancel_token](
                        const LogicalDeliveryOrderEntry& entry) {
                    throw_if_cancelled(cancel_token);
                    budget.consume(1u, logical_recovery_order_bytes(entry),
                        "logical recovery baseline exceeds materialization budget");
                    session.logical_recovery_baseline.delivery_order.push_back(entry);
                });
            outbox.for_each_pending(txn, session.source_db_uuid,
                session.requester,
                [&session, &budget, cancel_token](
                        const LogicalDeliveryEnvelope& envelope) {
                    throw_if_cancelled(cancel_token);
                    budget.consume(1u, logical_recovery_outbox_bytes(envelope),
                        "logical recovery baseline exceeds materialization budget");
                    session.logical_recovery_baseline.source_outbox_pending.push_back(
                        envelope);
                });
            session.logical_recovery_baseline.source_outbox_known_tail =
                outbox.known_tail(txn, session.source_db_uuid,
                                  session.requester);
        }

        static void add_logical_recovery_baseline_bytes(
                std::uint64_t& total,
                std::size_t additional) {
            const std::uint64_t max =
                (std::numeric_limits<std::uint64_t>::max)();
            if (additional > max || total > max -
                    static_cast<std::uint64_t>(additional)) {
                throw std::length_error(
                    "logical recovery baseline size overflow");
            }
            total += static_cast<std::uint64_t>(additional);
        }

        static void add_logical_recovery_baseline_element_bytes(
                std::uint64_t& total,
                std::size_t count,
                std::size_t element_size) {
            const std::uint64_t max =
                (std::numeric_limits<std::uint64_t>::max)();
            const std::uint64_t element_count =
                static_cast<std::uint64_t>(count);
            const std::uint64_t bytes_per_element =
                static_cast<std::uint64_t>(element_size);
            if (bytes_per_element != 0u &&
                element_count > max / bytes_per_element) {
                throw std::length_error(
                    "logical recovery baseline size overflow");
            }
            const std::uint64_t additional =
                element_count * bytes_per_element;
            if (total > max - additional) {
                throw std::length_error(
                    "logical recovery baseline size overflow");
            }
            total += additional;
        }

        static std::uint64_t logical_recovery_schema_bytes(
                const LogicalSchemaRegistryEntry& entry) {
            std::uint64_t bytes = 0u;
            add_logical_recovery_baseline_bytes(bytes,
                                                sizeof(LogicalSchemaRegistryEntry));
            add_logical_recovery_baseline_element_bytes(bytes,
                entry.record.dbi_names.size(), sizeof(std::string));
            add_logical_recovery_baseline_bytes(bytes, entry.schema_id.size());
            add_logical_recovery_baseline_bytes(bytes,
                                                entry.record.dbi_name.size());
            for (std::size_t i = 0u; i < entry.record.dbi_names.size(); ++i) {
                add_logical_recovery_baseline_bytes(bytes,
                    entry.record.dbi_names[i].size());
            }
            return bytes;
        }

        static std::uint64_t logical_recovery_marker_bytes(
                const LogicalDeliveryMarkerInfo& marker) {
            std::uint64_t bytes = 0u;
            add_logical_recovery_baseline_bytes(bytes,
                                                sizeof(LogicalDeliveryMarkerInfo));
            add_logical_recovery_baseline_bytes(bytes, marker.frame_id.size());
            add_logical_recovery_baseline_bytes(bytes,
                                                marker.encoded_frame.size());
            return bytes;
        }

        static std::uint64_t logical_recovery_outbox_bytes(
                const LogicalDeliveryEnvelope& envelope) {
            const std::vector<std::uint8_t> encoded =
                LogicalDeliveryEnvelopeCodec::encode(envelope);
            std::uint64_t bytes = 0u;
            add_logical_recovery_baseline_bytes(bytes,
                                                sizeof(LogicalDeliveryEnvelope));
            add_logical_recovery_baseline_element_bytes(bytes,
                envelope.frame.changes.size(), sizeof(LogicalChange));
            add_logical_recovery_baseline_bytes(bytes, encoded.size());
            return bytes;
        }

        static std::uint64_t logical_recovery_watermark_bytes(
                const LogicalDeliveryWatermarkInfo&) {
            return sizeof(LogicalDeliveryWatermarkInfo);
        }

        static std::uint64_t logical_recovery_order_bytes(
                const LogicalDeliveryOrderEntry&) {
            return sizeof(LogicalDeliveryOrderEntry);
        }

        static std::uint64_t logical_recovery_baseline_header_bytes(
                const LogicalRecoveryBaseline& baseline) {
            std::uint64_t bytes = 0u;
            add_logical_recovery_baseline_bytes(bytes,
                                                sizeof(LogicalRecoveryBaseline));
            add_logical_recovery_baseline_bytes(bytes, baseline.snapshot_id.size());
            return bytes;
        }

        static std::uint64_t logical_recovery_baseline_entry_count(
                const LogicalRecoveryBaseline& baseline) {
            const std::uint64_t max =
                (std::numeric_limits<std::uint64_t>::max)();
            const std::size_t counts[] = {
                baseline.schemas.size(),
                baseline.delivery_markers.size(),
                baseline.delivery_watermarks.size(),
                baseline.delivery_order.size(),
                baseline.source_outbox_pending.size()
            };
            std::uint64_t total = 0u;
            for (std::size_t i = 0u; i < sizeof(counts) / sizeof(counts[0]); ++i) {
                if (counts[i] > max || total > max -
                        static_cast<std::uint64_t>(counts[i])) {
                    throw std::length_error("logical recovery baseline size overflow");
                }
                total += static_cast<std::uint64_t>(counts[i]);
            }
            return total;
        }

        static std::uint64_t logical_recovery_baseline_bytes(
                const LogicalRecoveryBaseline& baseline) {
            std::uint64_t bytes = logical_recovery_baseline_header_bytes(baseline);
            for (std::size_t i = 0u; i < baseline.schemas.size(); ++i) {
                const std::uint64_t entry_bytes =
                    logical_recovery_schema_bytes(baseline.schemas[i]);
                if (bytes > (std::numeric_limits<std::uint64_t>::max)() -
                        entry_bytes) {
                    throw std::length_error("logical recovery baseline size overflow");
                }
                bytes += entry_bytes;
            }
            for (std::size_t i = 0u; i < baseline.delivery_markers.size(); ++i) {
                const std::uint64_t entry_bytes =
                    logical_recovery_marker_bytes(baseline.delivery_markers[i]);
                if (bytes > (std::numeric_limits<std::uint64_t>::max)() -
                        entry_bytes) {
                    throw std::length_error("logical recovery baseline size overflow");
                }
                bytes += entry_bytes;
            }
            for (std::size_t i = 0u;
                 i < baseline.delivery_watermarks.size(); ++i) {
                const std::uint64_t entry_bytes = logical_recovery_watermark_bytes(
                    baseline.delivery_watermarks[i]);
                if (bytes > (std::numeric_limits<std::uint64_t>::max)() -
                        entry_bytes) {
                    throw std::length_error("logical recovery baseline size overflow");
                }
                bytes += entry_bytes;
            }
            for (std::size_t i = 0u; i < baseline.delivery_order.size(); ++i) {
                const std::uint64_t entry_bytes = logical_recovery_order_bytes(
                    baseline.delivery_order[i]);
                if (bytes > (std::numeric_limits<std::uint64_t>::max)() -
                        entry_bytes) {
                    throw std::length_error("logical recovery baseline size overflow");
                }
                bytes += entry_bytes;
            }
            for (std::size_t i = 0u;
                 i < baseline.source_outbox_pending.size(); ++i) {
                const std::uint64_t entry_bytes = logical_recovery_outbox_bytes(
                    baseline.source_outbox_pending[i]);
                if (bytes > (std::numeric_limits<std::uint64_t>::max)() -
                        entry_bytes) {
                    throw std::length_error("logical recovery baseline size overflow");
                }
                bytes += entry_bytes;
            }
            return bytes;
        }

        static bool logical_schema_record_matches_adapter(
                const LogicalSchemaRegistryEntry& entry,
                const ILogicalTableAdapter& adapter) {
            const LogicalSchemaRef ref = adapter.schema_ref();
            if (!is_logical_schema_ref_complete(ref) ||
                ref.schema_id != entry.schema_id ||
                ref.kind != entry.record.kind ||
                ref.schema_version != entry.record.schema_version ||
                entry.record.dbi_name != adapter.primary_dbi()) {
                return false;
            }
            std::vector<std::string> adapter_dbis;
            std::vector<std::string> record_dbis;
            return canonical_logical_dbi_names(adapter.affected_dbis(),
                                               adapter_dbis) &&
                canonical_logical_dbi_names(entry.record.dbi_names,
                                            record_dbis) &&
                adapter_dbis == record_dbis;
        }

        void validate_logical_recovery_baseline(
                const FullSnapshotImportSession& session,
                const LogicalRecoveryBaseline& baseline) const {
            if (compare_node_id(baseline.source_node_id,
                                session.source_node_id) != 0 ||
                compare_node_id(baseline.source_db_uuid,
                                session.source_db_uuid) != 0 ||
                baseline.snapshot_id != session.snapshot_id) {
                throw std::invalid_argument(
                    "logical recovery baseline does not match snapshot identity");
            }
            std::map<std::string, bool> seen_schemas;
            for (std::size_t i = 0u; i < baseline.schemas.size(); ++i) {
                const LogicalSchemaRegistryEntry& entry = baseline.schemas[i];
                if (entry.schema_id.empty() ||
                    !seen_schemas.insert(std::make_pair(entry.schema_id, true)).second) {
                    throw std::invalid_argument(
                        "logical recovery baseline has duplicate or empty schema id");
                }
                ILogicalTableAdapter* adapter =
                    m_logical_registry.find(entry.schema_id);
                if (adapter == nullptr ||
                    !logical_schema_record_matches_adapter(entry, *adapter)) {
                    throw std::logic_error(
                        "logical recovery destination adapter does not match baseline");
                }
            }
            for (std::size_t i = 0u; i < baseline.delivery_markers.size(); ++i) {
                const LogicalDeliveryMarkerInfo& marker =
                    baseline.delivery_markers[i];
                if (compare_node_id(marker.destination_db_uuid,
                                    session.source_db_uuid) != 0) {
                    throw std::invalid_argument(
                        "logical recovery marker destination does not match snapshot db_uuid");
                }
            }
            for (std::size_t i = 0u; i < baseline.delivery_order.size(); ++i) {
                if (compare_node_id(baseline.delivery_order[i].origin_node_id,
                                    session.source_node_id) == 0) {
                    throw std::invalid_argument(
                        "logical recovery baseline contains a source self frontier");
                }
            }
            std::uint64_t previous_pending = 0u;
            for (std::size_t i = 0u;
                 i < baseline.source_outbox_pending.size(); ++i) {
                const LogicalDeliveryEnvelope& envelope =
                    baseline.source_outbox_pending[i];
                validate_logical_delivery_envelope(envelope);
                if (compare_node_id(envelope.destination_db_uuid,
                                    session.source_db_uuid) != 0 ||
                    compare_node_id(envelope.origin_node_id,
                                    session.source_node_id) != 0 ||
                    envelope.origin_sequence >
                        baseline.source_outbox_known_tail ||
                    (previous_pending != 0u && envelope.origin_sequence !=
                        previous_pending + 1u)) {
                    throw std::invalid_argument(
                        "logical recovery source outbox baseline is invalid");
                }
                previous_pending = envelope.origin_sequence;
            }
            if (!baseline.source_outbox_pending.empty() &&
                previous_pending != baseline.source_outbox_known_tail) {
                throw std::invalid_argument(
                    "logical recovery pending outbox does not reach known tail");
            }
        }

        void validate_logical_recovery_import_bounds(
                const FullSnapshotImportSession& session,
                const LogicalRecoveryBaseline& baseline) const {
            const std::uint64_t baseline_entries =
                logical_recovery_baseline_entry_count(baseline);
            const std::uint64_t baseline_bytes =
                logical_recovery_baseline_bytes(baseline);
            const std::uint64_t staged_operations =
                static_cast<std::uint64_t>(session.operations.size());
            const std::uint64_t max_operations =
                m_full_snapshot_import_options.max_staged_operations;
            const std::uint64_t max_bytes =
                m_full_snapshot_import_options.max_staged_bytes;
            if (staged_operations > max_operations ||
                session.staged_bytes > max_bytes ||
                baseline_entries > max_operations - staged_operations ||
                baseline_bytes > max_bytes - session.staged_bytes) {
                throw std::length_error(
                    "logical recovery import exceeds staged materialization budget");
            }
        }

        void require_fresh_logical_recovery_target(MDBX_txn* txn) const {
            SchemaRegistryStore schemas(m_conn->env_handle());
            LogicalDeliveryStore delivery(m_conn->env_handle());
            LogicalDeliveryOrderStore order(m_conn->env_handle());
            LogicalOutboxStore outbox(m_conn->env_handle());
            if (schemas.has_entries(txn) || delivery.has_persistent_state(txn) ||
                order.has_entries(txn) || outbox.has_persistent_state(txn)) {
                throw std::logic_error(
                    "logical recovery destination has persistent logical state");
            }
        }

        void apply_logical_recovery_baseline(
                MDBX_txn* txn,
                const FullSnapshotImportSession& session,
                const LogicalRecoveryBaseline& baseline) {
            validate_logical_recovery_baseline(session, baseline);
            require_fresh_logical_recovery_target(txn);

            SchemaRegistryStore schemas(m_conn->env_handle());
            LogicalDeliveryStore delivery(m_conn->env_handle());
            LogicalDeliveryOrderStore order(m_conn->env_handle());
            for (std::size_t i = 0u; i < baseline.schemas.size(); ++i) {
                const LogicalSchemaRegistryEntry& entry = baseline.schemas[i];
                ILogicalTableAdapter* adapter =
                    m_logical_registry.find(entry.schema_id);
                adapter->verify_storage(txn);
                schemas.register_or_verify(txn, entry.schema_id, entry.record);
            }
            for (std::size_t i = 0u; i < baseline.delivery_markers.size(); ++i) {
                delivery.restore_marker(txn, baseline.delivery_markers[i]);
            }
            for (std::size_t i = 0u; i < baseline.delivery_watermarks.size(); ++i) {
                delivery.restore_watermark(txn,
                                           baseline.delivery_watermarks[i]);
            }
            for (std::size_t i = 0u;
                 i < baseline.source_outbox_pending.size(); ++i) {
                if (!delivery.try_mark_applied(
                        txn, baseline.source_outbox_pending[i])) {
                    throw std::runtime_error(
                        "logical recovery source outbox marker conflicts");
                }
            }
            for (std::size_t i = 0u; i < baseline.delivery_order.size(); ++i) {
                order.restore_entry(txn, baseline.delivery_order[i]);
            }
            if (baseline.source_outbox_known_tail != 0u) {
                LogicalDeliveryOrderEntry source_frontier;
                source_frontier.origin_node_id = session.source_node_id;
                source_frontier.acknowledged_through =
                    baseline.source_outbox_known_tail;
                order.restore_entry(txn, source_frontier);
            }
        }

        std::shared_ptr<FullSnapshotSession> materialize_full_snapshot(
                const FullSnapshotExportOptions& options,
                const NodeId& requester,
                bool logical_recovery,
                const std::string& snapshot_id,
                const CancellationToken* cancel_token) const {
            throw_if_cancelled(cancel_token);
            std::shared_ptr<FullSnapshotSession> session(
                new FullSnapshotSession());
            session->requester = requester;
            session->snapshot_id = snapshot_id;
            session->replacement_scope = options.replacement_scope;
            session->logical_recovery = logical_recovery;
            session->last_access = std::chrono::steady_clock::now();

            auto txn = m_conn->transaction(TransactionMode::READ_ONLY);
            throw_if_cancelled(cancel_token);
            MetaStore meta(m_conn->env_handle());
            meta.open(txn.handle());
            session->source_node_id = meta.get_node_id(txn.handle());
            session->source_db_uuid = meta.get_db_uuid(txn.handle());
            if (compare_node_id(session->source_node_id, NodeId()) == 0 ||
                compare_node_id(session->source_db_uuid, NodeId()) == 0) {
                throw std::logic_error(
                    "full snapshot source identity is not initialized");
            }

            if (!logical_recovery && options.replacement_scope ==
                FullSnapshotScope::CompleteUserDatabase) {
                require_raw_only_complete_snapshot_source(txn.handle());
            }

            session->manifest = options.replacement_scope ==
                    FullSnapshotScope::CompleteUserDatabase
                ? enumerate_user_snapshot_manifest(txn.handle())
                : options.manifest;
            reject_versioned_full_snapshot_manifest(
                txn.handle(), session->manifest);
            if (session->replacement_scope ==
                    FullSnapshotScope::CompleteUserDatabase &&
                session->manifest.empty()) {
                throw std::runtime_error(
                    "complete full snapshot source has no user DBIs");
            }

            throw_if_cancelled(cancel_token);

            if (options.replacement_scope ==
                    FullSnapshotScope::CompleteUserDatabase) {
                const MDBX_dbi changelog_dbi = open_changelog_ro(txn.handle());
                session->source_tail = read_applied_cursor(
                    txn.handle(), SyncCursor());
                if (changelog_dbi != 0) {
                    const std::vector<PullOrigin> origins =
                        collect_known_origins(txn.handle(), changelog_dbi);
                    SyncCursor changelog_tail;
                    if (!copy_known_tail(origins, changelog_tail)) {
                        throw std::runtime_error(
                            "full snapshot source changelog tail is incomplete");
                    }
                    merge_sync_cursor_max(session->source_tail, changelog_tail);
                }
            }

            MaterializationBudget materialization_budget(options);
            if (logical_recovery) {
#if defined(MDBXC_TEST_LOGICAL_RECOVERY_MATERIALIZATION_CHECKPOINT)
                if (m_logical_recovery_materialization_checkpoint) {
                    m_logical_recovery_materialization_checkpoint();
                }
#endif
                collect_logical_recovery_baseline(
                    txn.handle(), *session, materialization_budget, cancel_token);
            }
            for (std::size_t i = 0u; i < session->manifest.size(); ++i) {
                throw_if_cancelled(cancel_token);
                const FullSnapshotManifestEntry& entry = session->manifest[i];
                MDBX_dbi dbi = 0;
                const MDBX_db_flags_t open_flags =
                    static_cast<MDBX_db_flags_t>(
                        persistent_dbi_flags(entry.dbi_flags));
                int rc = mdbx_dbi_open(txn.handle(), entry.dbi_name.c_str(),
                                       open_flags, &dbi);
                if (rc == MDBX_NOTFOUND) {
                    throw std::runtime_error(
                        "full snapshot manifest DBI does not exist: " +
                        entry.dbi_name);
                }
                check_mdbx(rc, "full snapshot failed to open DBI '" +
                            entry.dbi_name + "'");

                unsigned actual_raw = 0u;
                check_mdbx(mdbx_dbi_flags(txn.handle(), dbi, &actual_raw),
                           "full snapshot failed to read DBI flags");
                const std::uint32_t actual_flags = persistent_dbi_flags(
                    static_cast<std::uint32_t>(actual_raw));
                if (actual_flags != entry.dbi_flags) {
                    throw std::runtime_error(
                        "full snapshot manifest DBI flags mismatch: " +
                        entry.dbi_name);
                }

                append_full_snapshot_operation(*session, materialization_budget,
                    entry.dbi_name, actual_flags, ChangeOpType::ClearTable,
                    nullptr, nullptr);

                MDBX_cursor* raw = nullptr;
                check_mdbx(mdbx_cursor_open(txn.handle(), dbi, &raw),
                           "full snapshot failed to open DBI cursor");
                CursorGuard cursor(raw);
                MDBX_val key;
                MDBX_val value;
                rc = mdbx_cursor_get(raw, &key, &value, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    throw_if_cancelled(cancel_token);
                    append_full_snapshot_operation(*session, materialization_budget,
                        entry.dbi_name, actual_flags, ChangeOpType::Put,
                        &key, &value);
                    rc = mdbx_cursor_get(raw, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "full snapshot failed to scan DBI '" +
                               entry.dbi_name + "'");
                }
            }
            return session;
        }

        static std::string continuation_for(
                FullSnapshotSession& session,
                std::size_t next_operation,
                std::uint64_t next_chunk_index) {
            std::map<std::string, FullSnapshotContinuation>::const_iterator it =
                session.continuations.begin();
            for (; it != session.continuations.end(); ++it) {
                if (it->second.next_operation == next_operation &&
                    it->second.chunk_index == next_chunk_index) {
                    return it->first;
                }
            }
            const std::string token = session.snapshot_id + ":" +
                std::to_string(next_chunk_index) + ":" +
                std::to_string(next_operation);
            FullSnapshotContinuation continuation;
            continuation.next_operation = next_operation;
            continuation.chunk_index = next_chunk_index;
            session.continuations[token] = continuation;
            return token;
        }

        static PullResponse make_full_snapshot_page(
                FullSnapshotSession& session,
                const PullRequest& request,
                std::size_t start_operation,
                std::uint64_t chunk_index) {
            if (request.max_single_batch_bytes == 0u) {
                throw std::length_error(
                    "full snapshot max_single_batch_bytes must be non-zero");
            }
            const std::uint64_t hard_limit = request.max_single_batch_bytes;
            const std::uint64_t soft_limit = request.max_bytes;
            FullSnapshotChunk chunk;
            chunk.source_node_id = session.source_node_id;
            chunk.source_db_uuid = session.source_db_uuid;
            chunk.snapshot_id = session.snapshot_id;
            chunk.source_tail = session.source_tail;
            chunk.chunk_index = chunk_index;
            chunk.replacement_scope = session.replacement_scope;
            chunk.manifest = session.manifest;
            chunk.batch.origin_node_id = session.source_node_id;
            chunk.batch.seq = 0u;
            chunk.batch.version = ChangeBatchCodec::batch_version();

            std::size_t next_operation = start_operation;
            std::vector<std::uint8_t> last_encoded;
            FullSnapshotChunk last_chunk;
            while (next_operation < session.operations.size()) {
                chunk.batch.ops.push_back(session.operations[next_operation]);
                const std::size_t candidate_next = next_operation + 1u;
                chunk.has_more = candidate_next < session.operations.size();
                chunk.continuation = chunk.has_more
                    ? continuation_for(session, candidate_next, chunk_index + 1u)
                    : std::string();
                chunk.batch.batch_flags = chunk.has_more
                    ? static_cast<std::uint32_t>(BATCH_HAS_MORE)
                    : static_cast<std::uint32_t>(BATCH_NONE);
                const std::vector<std::uint8_t> encoded =
                    FullSnapshotCodec::encode(chunk);
                if (encoded.size() > hard_limit) {
                    if (last_encoded.empty()) {
                        throw std::length_error(
                            "full snapshot operation exceeds max_single_batch_bytes");
                    }
                    break;
                }
                if (!last_encoded.empty() && soft_limit != 0u &&
                    encoded.size() > soft_limit) {
                    break;
                }
                last_chunk = chunk;
                last_encoded = encoded;
                next_operation = candidate_next;
            }
            if (last_encoded.empty()) {
                throw std::length_error("full snapshot page contains no operations");
            }

            PullResponse out;
            out.ok = true;
            out.is_full_snapshot = true;
            out.snapshot_chunk = last_chunk;
            out.has_more = last_chunk.has_more;
            out.remote_tail = session.source_tail;
            out.remote_tail_known = true;
            return out;
        }

        PullResponse handle_full_snapshot_pull(
                const PullRequest& request,
                bool logical_recovery = false,
                LogicalRecoveryBaseline* final_baseline = nullptr,
                const CancellationToken* cancel_token = nullptr) {
            PullResponse out;
            if (cancel_token != nullptr &&
                cancel_token->is_cancellation_requested()) {
                out.ok = false;
                out.error = "full snapshot materialization cancelled";
                out.error_retryable = true;
                return out;
            }
            if (request.full_snapshot_id.empty() !=
                request.full_snapshot_continuation.empty()) {
                out.ok = false;
                out.error =
                    "full snapshot session id and continuation must both be empty or set";
                out.error_code = SyncResponseErrorCode::SnapshotSessionInvalid;
                return out;
            }
            if (!request.have.last_seq_by_origin.empty()) {
                out.ok = false;
                out.error = "full snapshot requires an empty receiver cursor";
                out.error_code = SyncResponseErrorCode::SnapshotSessionInvalid;
                return out;
            }

            std::shared_ptr<FullSnapshotSession> session;
            std::size_t start_operation = 0u;
            std::uint64_t chunk_index = 0u;
            if (request.full_snapshot_id.empty()) {
                FullSnapshotExportOptions options;
                std::string snapshot_id;
                {
                    std::lock_guard<std::mutex> lock(m_full_snapshot_mutex);
                    prune_expired_full_snapshot_sessions_locked();
                    options = m_full_snapshot_options;
                    if (!full_snapshot_export_is_configured(options)) {
                        out.ok = false;
                        out.error = "full snapshot source export is not configured";
                        out.error_code =
                            SyncResponseErrorCode::SnapshotNotConfigured;
                        return out;
                    }
                    if (logical_recovery && options.replacement_scope !=
                            FullSnapshotScope::CompleteUserDatabase) {
                        out.ok = false;
                        out.error =
                            "logical recovery requires CompleteUserDatabase snapshot export";
                        out.error_code =
                            SyncResponseErrorCode::SnapshotNotConfigured;
                        return out;
                    }
                    if (m_full_snapshot_sessions.size() +
                        m_full_snapshot_creating >= options.max_active_sessions) {
                        out.ok = false;
                        out.error = "full snapshot session capacity is exhausted";
                        out.error_code = SyncResponseErrorCode::SnapshotSessionBusy;
                        out.error_retryable = true;
                        return out;
                    }
                    ++m_full_snapshot_creating;
                    snapshot_id = next_full_snapshot_id_locked();
                }
                try {
                    session = materialize_full_snapshot(
                        options, request.requester, logical_recovery, snapshot_id,
                        cancel_token);
                    throw_if_cancelled(cancel_token);
                } catch (const FullSnapshotCancelled& e) {
                    std::lock_guard<std::mutex> lock(m_full_snapshot_mutex);
                    --m_full_snapshot_creating;
                    out.ok = false;
                    out.error = e.what();
                    out.error_retryable = true;
                    return out;
                } catch (const FullSnapshotLogicalStateUnsupported& e) {
                    std::lock_guard<std::mutex> lock(m_full_snapshot_mutex);
                    --m_full_snapshot_creating;
                    out.ok = false;
                    out.error = e.what();
                    out.error_code =
                        SyncResponseErrorCode::SnapshotLogicalStateUnsupported;
                    return out;
                } catch (const FullSnapshotVersionedDbiUnsupported& e) {
                    std::lock_guard<std::mutex> lock(m_full_snapshot_mutex);
                    --m_full_snapshot_creating;
                    out.ok = false;
                    out.error = e.what();
                    out.error_code = SyncResponseErrorCode::UnsupportedFullSnapshot;
                    return out;
                } catch (const std::length_error& e) {
                    std::lock_guard<std::mutex> lock(m_full_snapshot_mutex);
                    --m_full_snapshot_creating;
                    out.ok = false;
                    out.error = e.what();
                    out.error_code = SyncResponseErrorCode::BatchTooLarge;
                    return out;
                } catch (...) {
                    std::lock_guard<std::mutex> lock(m_full_snapshot_mutex);
                    --m_full_snapshot_creating;
                    throw;
                }
                {
                    std::lock_guard<std::mutex> lock(m_full_snapshot_mutex);
                    --m_full_snapshot_creating;
                    prune_expired_full_snapshot_sessions_locked();
                    m_full_snapshot_sessions[session->snapshot_id] = session;
                }
            } else {
                std::lock_guard<std::mutex> lock(m_full_snapshot_mutex);
                prune_expired_full_snapshot_sessions_locked();
                std::map<std::string,
                         std::shared_ptr<FullSnapshotSession>>::const_iterator it =
                    m_full_snapshot_sessions.find(request.full_snapshot_id);
                if (it == m_full_snapshot_sessions.end()) {
                    out.ok = false;
                    out.error = "full snapshot session is unknown or expired";
                    out.error_code = SyncResponseErrorCode::SnapshotSessionInvalid;
                    return out;
                }
                session = it->second;
                if (session->logical_recovery != logical_recovery) {
                    out.ok = false;
                    out.error = "full snapshot session belongs to another protocol";
                    out.error_code = SyncResponseErrorCode::SnapshotSessionInvalid;
                    return out;
                }
            }

            std::lock_guard<std::mutex> lock(session->mutex);
            if (compare_node_id(session->requester, request.requester) != 0) {
                out.ok = false;
                out.error = "full snapshot requester does not own this session";
                out.error_code = SyncResponseErrorCode::SnapshotSessionInvalid;
                return out;
            }
            if (!request.full_snapshot_id.empty()) {
                std::map<std::string, FullSnapshotContinuation>::const_iterator it =
                    session->continuations.find(request.full_snapshot_continuation);
                if (it == session->continuations.end()) {
                    out.ok = false;
                    out.error = "full snapshot continuation is invalid";
                    out.error_code = SyncResponseErrorCode::SnapshotSessionInvalid;
                    return out;
                }
                start_operation = it->second.next_operation;
                chunk_index = it->second.chunk_index;
            }
            {
                std::lock_guard<std::mutex> sessions_lock(
                    m_full_snapshot_mutex);
                std::map<std::string,
                         std::shared_ptr<FullSnapshotSession>>::const_iterator it =
                    m_full_snapshot_sessions.find(session->snapshot_id);
                if (it == m_full_snapshot_sessions.end() ||
                    it->second != session) {
                    out.ok = false;
                    out.error = "full snapshot session is unknown or expired";
                    out.error_code = SyncResponseErrorCode::SnapshotSessionInvalid;
                    return out;
                }
                session->last_access = std::chrono::steady_clock::now();
            }
            try {
                PullResponse page = make_full_snapshot_page(
                    *session, request, start_operation, chunk_index);
                if (logical_recovery && page.ok && !page.has_more &&
                    final_baseline != nullptr) {
                    *final_baseline = session->logical_recovery_baseline;
                }
                return page;
            } catch (const std::length_error& e) {
                out.ok = false;
                out.error = e.what();
                out.error_code = SyncResponseErrorCode::BatchTooLarge;
                return out;
            }
        }

        static bool full_snapshot_metadata_matches(
                const FullSnapshotImportSession& session,
                const FullSnapshotChunk& chunk) {
            return compare_node_id(session.source_node_id,
                                   chunk.source_node_id) == 0 &&
                compare_node_id(session.source_db_uuid,
                                chunk.source_db_uuid) == 0 &&
                session.snapshot_id == chunk.snapshot_id &&
                session.source_tail.last_seq_by_origin ==
                    chunk.source_tail.last_seq_by_origin &&
                session.replacement_scope == chunk.replacement_scope &&
                session.manifest_version == chunk.manifest_version &&
                full_snapshot_manifest_equal(session.manifest, chunk.manifest);
        }

        void append_full_snapshot_import_chunk(
                FullSnapshotImportSession& session,
                const FullSnapshotChunk& chunk) const {
            std::map<std::string, FullSnapshotImportSession::ReplacementState>
                replacement_state = session.replacement_state;
            for (std::size_t i = 0u; i < chunk.batch.ops.size(); ++i) {
                const ChangeOp& op = chunk.batch.ops[i];
                std::map<std::string,
                         FullSnapshotImportSession::ReplacementState>::iterator
                    state = replacement_state.find(op.dbi_name);
                if (state == replacement_state.end()) {
                    throw std::invalid_argument(
                        "full snapshot operation is outside the replacement plan");
                }
                if (op.op_type == ChangeOpType::ClearTable) {
                    if (state->second !=
                        FullSnapshotImportSession::ReplacementState::NotSeen) {
                        throw std::invalid_argument(
                            "full snapshot replacement DBI was cleared twice");
                    }
                    state->second =
                        FullSnapshotImportSession::ReplacementState::Cleared;
                } else if (op.op_type == ChangeOpType::Put) {
                    if (state->second ==
                        FullSnapshotImportSession::ReplacementState::NotSeen) {
                        throw std::invalid_argument(
                            "full snapshot Put precedes its replacement clear");
                    }
                    state->second =
                        FullSnapshotImportSession::ReplacementState::ReceivingPuts;
                } else {
                    throw std::invalid_argument(
                        "full snapshot replacement operation is unsupported");
                }
            }
            const std::uint64_t operation_count =
                static_cast<std::uint64_t>(session.operations.size());
            const std::uint64_t incoming_count =
                static_cast<std::uint64_t>(chunk.batch.ops.size());
            if (operation_count >
                    m_full_snapshot_import_options.max_staged_operations ||
                incoming_count >
                    m_full_snapshot_import_options.max_staged_operations -
                        operation_count) {
                throw std::length_error(
                    "full snapshot import exceeds max_staged_operations");
            }

            std::uint64_t additional_bytes = 0u;
            for (std::size_t i = 0u; i < chunk.batch.ops.size(); ++i) {
                const std::uint64_t operation_bytes =
                    full_snapshot_operation_bytes(chunk.batch.ops[i]);
                if (operation_bytes >
                        m_full_snapshot_import_options.max_staged_bytes -
                            session.staged_bytes - additional_bytes) {
                    throw std::length_error(
                        "full snapshot import exceeds max_staged_bytes");
                }
                additional_bytes += operation_bytes;
            }
            session.operations.insert(session.operations.end(),
                                      chunk.batch.ops.begin(),
                                      chunk.batch.ops.end());
            session.staged_bytes += additional_bytes;
            session.replacement_state.swap(replacement_state);
        }

        static void require_complete_full_snapshot_replacement_plan(
                const FullSnapshotImportSession& session) {
            for (std::map<std::string,
                          FullSnapshotImportSession::ReplacementState>::const_iterator
                    it = session.replacement_state.begin();
                 it != session.replacement_state.end(); ++it) {
                if (it->second ==
                    FullSnapshotImportSession::ReplacementState::NotSeen) {
                    throw std::invalid_argument(
                        "full snapshot replacement plan omits manifest DBI: " +
                        it->first);
                }
            }
        }

        static bool full_snapshot_manifest_flags(
                const std::vector<FullSnapshotManifestEntry>& manifest,
                const std::string& name,
                std::uint32_t& flags) {
            for (std::size_t i = 0u; i < manifest.size(); ++i) {
                if (manifest[i].dbi_name == name) {
                    flags = manifest[i].dbi_flags;
                    return true;
                }
            }
            return false;
        }

        void require_fresh_full_snapshot_target(
                MDBX_txn* txn,
                MetaStore& meta,
                const FullSnapshotImportSession& session) const {
            if (compare_node_id(meta.get_db_uuid(txn),
                                session.source_db_uuid) != 0) {
                throw std::invalid_argument(
                    "full snapshot source db_uuid does not match destination");
            }
            const NodeId local_node_id = meta.get_node_id(txn);
            if (is_zero_sync_id(local_node_id)) {
                throw std::logic_error(
                    "full snapshot destination node identity is not initialized");
            }
            if (compare_node_id(local_node_id, session.source_node_id) == 0) {
                throw std::logic_error(
                    "full snapshot source and destination node identities match");
            }
            if (session.replacement_scope ==
                    FullSnapshotScope::CompleteUserDatabase &&
                session.source_tail.last_seq_for(local_node_id) != 0u) {
                throw std::logic_error(
                    "full snapshot destination node identity appears in source tail");
            }
            if (meta.get_local_seq(txn) != 0u) {
                throw std::logic_error(
                    "full snapshot destination has local changelog history");
            }
            SyncCursor applied;
            if (!read_applied_cursor(txn, applied).last_seq_by_origin.empty()) {
                throw std::logic_error(
                    "full snapshot destination has an applied cursor");
            }

            if (session.replacement_scope ==
                    FullSnapshotScope::CompleteUserDatabase) {
                const std::vector<FullSnapshotManifestEntry> destination =
                    enumerate_user_snapshot_manifest(txn);
                for (std::size_t i = 0u; i < destination.size(); ++i) {
                    std::uint32_t manifest_flags = 0u;
                    if (!full_snapshot_manifest_flags(
                            session.manifest, destination[i].dbi_name,
                            manifest_flags) ||
                        manifest_flags != destination[i].dbi_flags) {
                        throw std::logic_error(
                            "complete full snapshot destination has a user DBI "
                            "outside the source manifest: " +
                            destination[i].dbi_name);
                    }
                }
            }

            for (std::size_t i = 0u; i < session.manifest.size(); ++i) {
                const FullSnapshotManifestEntry& entry = session.manifest[i];
                MDBX_dbi dbi = 0;
                int error_code = MDBX_SUCCESS;
                if (!open_existing_user_dbi(txn, entry.dbi_name,
                                            entry.dbi_flags, dbi,
                                            error_code)) {
                    throw std::runtime_error(
                        "full snapshot destination DBI flags mismatch: " +
                        entry.dbi_name);
                }
                if (dbi == 0) continue;

                unsigned actual_raw = 0u;
                check_mdbx(mdbx_dbi_flags(txn, dbi, &actual_raw),
                           "full snapshot failed to read destination DBI flags");
                if (persistent_dbi_flags(static_cast<std::uint32_t>(actual_raw)) !=
                    entry.dbi_flags) {
                    throw std::runtime_error(
                        "full snapshot destination DBI flags mismatch: " +
                        entry.dbi_name);
                }
                MDBX_stat stat;
                check_mdbx(mdbx_dbi_stat(txn, dbi, &stat, sizeof(stat)),
                           "full snapshot failed to inspect destination DBI");
                if (stat.ms_entries != 0u) {
                    throw std::logic_error(
                        "full snapshot destination DBI is not empty: " +
                        entry.dbi_name);
                }
            }
        }

        FullSnapshotImportResult apply_full_snapshot_chunk_locked(
                const FullSnapshotChunk& chunk,
                const LogicalRecoveryBaseline* logical_baseline,
                bool logical_recovery,
                Connection::SyncApplyNotification& notification,
                bool& notification_ready) {
            if (chunk.replacement_scope != FullSnapshotScope::ManifestOnly &&
                chunk.replacement_scope !=
                    FullSnapshotScope::CompleteUserDatabase) {
                throw std::invalid_argument(
                    "full snapshot importer received an unsupported replacement scope");
            }
            if (!m_full_snapshot_import_session) {
                if (chunk.chunk_index != 0u) {
                    throw std::invalid_argument(
                        "full snapshot import must begin at chunk zero");
                }
                std::unique_ptr<FullSnapshotImportSession> session(
                    new FullSnapshotImportSession());
                session->source_node_id = chunk.source_node_id;
                session->source_db_uuid = chunk.source_db_uuid;
                session->snapshot_id = chunk.snapshot_id;
                session->source_tail = chunk.source_tail;
                session->replacement_scope = chunk.replacement_scope;
                session->manifest_version = chunk.manifest_version;
                session->manifest = chunk.manifest;
                session->logical_recovery = logical_recovery;
                for (std::size_t i = 0u; i < session->manifest.size(); ++i) {
                    session->replacement_state[session->manifest[i].dbi_name] =
                        FullSnapshotImportSession::ReplacementState::NotSeen;
                }
                m_full_snapshot_import_session = std::move(session);
            }

            FullSnapshotImportSession& session =
                *m_full_snapshot_import_session;
            if (chunk.chunk_index != session.next_chunk_index ||
                !full_snapshot_metadata_matches(session, chunk) ||
                session.logical_recovery != logical_recovery) {
                throw std::invalid_argument(
                    "full snapshot chunk does not match the active import session");
            }
            if ((!logical_recovery && logical_baseline != nullptr) ||
                (logical_recovery &&
                 (chunk.has_more ? logical_baseline != nullptr :
                                   logical_baseline == nullptr))) {
                throw std::invalid_argument(
                    "logical recovery baseline does not match snapshot finality");
            }
            append_full_snapshot_import_chunk(session, chunk);
            ++session.next_chunk_index;

            FullSnapshotImportResult result;
            result.next_chunk_index = session.next_chunk_index;
            if (chunk.has_more) return result;

            require_complete_full_snapshot_replacement_plan(session);

            {
                const Connection::SyncApplyWriteGuard sync_apply_guard =
                    m_conn->sync_apply_write_guard();
                auto txn = m_conn->transaction(TransactionMode::WRITABLE);
                MetaStore meta(m_conn->env_handle());
                meta.open(txn.handle());
                require_fresh_full_snapshot_target(txn.handle(), meta, session);
                if (logical_recovery) {
                    validate_logical_recovery_baseline(session,
                                                       *logical_baseline);
                    validate_logical_recovery_import_bounds(session,
                                                            *logical_baseline);
                    require_fresh_logical_recovery_target(txn.handle());
                }

                std::vector<std::string> affected_dbi_names;
                for (std::size_t i = 0u; i < session.manifest.size(); ++i) {
                    add_unique_dbi_name(affected_dbi_names,
                                        session.manifest[i].dbi_name);
                }
                const std::size_t applied_operations = session.operations.size();

                std::unordered_map<std::string, MDBX_dbi> dbi_cache;
                for (std::size_t i = 0u; i < session.operations.size(); ++i) {
                    apply_one_op(txn.handle(), session.operations[i], dbi_cache);
                }

                if (session.replacement_scope ==
                        FullSnapshotScope::CompleteUserDatabase) {
                    AppliedStore applied(m_conn->env_handle());
                    applied.open(txn.handle());
                    for (std::map<NodeId, std::uint64_t>::const_iterator it =
                            session.source_tail.last_seq_by_origin.begin();
                         it != session.source_tail.last_seq_by_origin.end(); ++it) {
                        applied.set_last_applied_seq(txn.handle(), it->first,
                                                     it->second);
                    }
                }
                if (logical_recovery) {
                    apply_logical_recovery_baseline(txn.handle(), session,
                                                    *logical_baseline);
                }
                txn.commit();
                result.completed = true;
                m_full_snapshot_import_session.reset();
                try {
                    notification = m_conn->mark_sync_apply_committed(
                        1u, applied_operations, affected_dbi_names);
                    notification_ready = true;
                } catch (...) {
                    // Native commit is durable; observer bookkeeping is best effort.
                }
            }
            return result;
        }

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
            LogicalDeliveryStore logical_delivery(m_conn->env_handle());
            LogicalDeliveryOrderStore logical_delivery_order(m_conn->env_handle());
            LogicalOutboxStore logical_outbox(m_conn->env_handle());
            meta.open(txn);
            change_log.open(txn);
            applied.open(txn);
            schemas.open(txn);
            logical_delivery.open(txn);
            logical_delivery_order.open(txn);
            logical_outbox.open(txn);
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

        void collect_logical_affected_dbis(
                const std::vector<LogicalChange>& changes,
                std::vector<std::string>& names) const {
            for (std::size_t i = 0; i < changes.size(); ++i) {
                ILogicalTableAdapter* adapter =
                    m_logical_registry.find(changes[i].schema.schema_id);
                if (adapter == nullptr) {
                    continue;
                }
                const std::vector<std::string> dbis =
                    adapter->affected_dbis();
                for (std::size_t j = 0; j < dbis.size(); ++j) {
                    add_unique_dbi_name(names, dbis[j]);
                }
            }
        }

        LogicalApplyResult validate_logical_schema_markers(
                MDBX_txn* txn,
                const std::vector<LogicalChange>& changes) const {
            std::vector<std::string> checked_schema_ids;

            for (std::size_t i = 0; i < changes.size(); ++i) {
                const std::string& schema_id = changes[i].schema.schema_id;
                bool already_checked = false;
                for (std::size_t j = 0; j < checked_schema_ids.size(); ++j) {
                    if (checked_schema_ids[j] == schema_id) {
                        already_checked = true;
                        break;
                    }
                }
                if (already_checked) {
                    continue;
                }

                ILogicalTableAdapter* adapter =
                    m_logical_registry.find(schema_id);
                if (adapter == nullptr) {
                    return LogicalApplyResult::failure(
                        "No logical adapter registered for schema id");
                }

                const LogicalApplyResult marker_result =
                    validate_logical_adapter_marker(
                        txn, m_conn->env_handle(), *adapter);
                if (!marker_result.ok) return marker_result;

                checked_schema_ids.push_back(schema_id);
            }

            return LogicalApplyResult::success();
        }

        LogicalApplyResult validate_ordered_logical_schema_origins(
                MDBX_txn* txn,
                const std::vector<LogicalChange>& changes,
                const NodeId& origin) const {
            SchemaRegistryStore schemas(m_conn->env_handle());
            std::vector<std::string> checked_schema_ids;

            for (std::size_t i = 0; i < changes.size(); ++i) {
                const std::string& schema_id = changes[i].schema.schema_id;
                if (std::find(checked_schema_ids.begin(),
                              checked_schema_ids.end(),
                              schema_id) != checked_schema_ids.end()) {
                    continue;
                }

                ILogicalTableAdapter* adapter = m_logical_registry.find(schema_id);
                if (adapter == nullptr) {
                    return LogicalApplyResult::failure(
                        "No logical adapter registered for schema id");
                }
                if (!adapter->requires_ordered_delivery()) {
                    checked_schema_ids.push_back(schema_id);
                    continue;
                }

                LogicalSchemaRecord record;
                if (!schemas.get(txn, schema_id, record)) {
                    return LogicalApplyResult::failure(
                        "Persistent logical schema marker is missing");
                }
                if (record.kind != changes[i].schema.kind ||
                    record.schema_version != changes[i].schema.schema_version) {
                    return LogicalApplyResult::failure(
                        "Persistent logical schema marker does not match ordered delivery");
                }
                if (is_zero_sync_id(record.ordered_delivery_origin_node_id)) {
                    return LogicalApplyResult::failure(
                        "Persistent logical schema marker has no ordered delivery origin");
                }
                if (compare_node_id(record.ordered_delivery_origin_node_id,
                                    origin) != 0) {
                    return LogicalApplyResult::failure(
                        "Logical ordered delivery origin does not match schema marker");
                }
                checked_schema_ids.push_back(schema_id);
            }

            return LogicalApplyResult::success();
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

        static void validate_full_snapshot_import_options(
                const FullSnapshotImportOptions& options) {
            if (options.max_staged_operations == 0u ||
                options.max_staged_bytes == 0u) {
                throw std::invalid_argument(
                    "full snapshot import bounds must be non-zero");
            }
        }

        static bool full_snapshot_manifest_equal(
                const std::vector<FullSnapshotManifestEntry>& lhs,
                const std::vector<FullSnapshotManifestEntry>& rhs) {
            if (lhs.size() != rhs.size()) return false;
            for (std::size_t i = 0u; i < lhs.size(); ++i) {
                if (lhs[i].dbi_name != rhs[i].dbi_name ||
                    lhs[i].dbi_flags != rhs[i].dbi_flags) {
                    return false;
                }
            }
            return true;
        }

        static std::uint64_t full_snapshot_operation_bytes(
                const ChangeOp& op) {
            const std::uint64_t fixed =
                static_cast<std::uint64_t>(sizeof(ChangeOp));
            const std::uint64_t name =
                static_cast<std::uint64_t>(op.dbi_name.size());
            const std::uint64_t key =
                static_cast<std::uint64_t>(op.storage_key.size());
            const std::uint64_t value =
                static_cast<std::uint64_t>(op.value.size());
            if (name > (std::numeric_limits<std::uint64_t>::max)() - fixed ||
                key > (std::numeric_limits<std::uint64_t>::max)() - fixed - name ||
                value > (std::numeric_limits<std::uint64_t>::max)() - fixed - name - key) {
                throw std::length_error(
                    "full snapshot operation size overflow");
            }
            return fixed + name + key + value;
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

        static void merge_sync_cursor_max(SyncCursor& target,
                                          const SyncCursor& source) {
            for (std::map<NodeId, std::uint64_t>::const_iterator it =
                    source.last_seq_by_origin.begin();
                 it != source.last_seq_by_origin.end(); ++it) {
                const std::uint64_t current = target.last_seq_for(it->first);
                if (it->second > current) {
                    target.last_seq_by_origin[it->first] = it->second;
                }
            }
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

        static void apply_lww_one_op(
                MDBX_txn* txn,
                const ChangeOp& op,
                const NodeId& origin,
                std::uint64_t sequence,
                std::unordered_map<std::string, MDBX_dbi>& cache,
                IdentityIndexStore& identity_index) {
            IdentityIndexValue current;
            if (identity_index.get(txn, op.dbi_name, op.storage_key, current)) {
                if (op.revision_key < current.revision_key) return;
                if (op.revision_key == current.revision_key &&
                    compare_node_id(origin, current.origin_node_id) <= 0) {
                    return;
                }
            }
            apply_one_op(txn, op, cache);
            IdentityIndexValue marker;
            marker.storage_key = op.storage_key;
            marker.origin_node_id = origin;
            marker.seq = sequence;
            marker.revision_key = op.revision_key;
            if (op.op_type == ChangeOpType::Delete) {
                identity_index.tombstone(txn, op.dbi_name, op.storage_key, marker);
            } else {
                identity_index.put(txn, op.dbi_name, op.storage_key, marker);
            }
        }

        bool validate_versioned_dbi_batch(
                MDBX_txn* txn,
                const ChangeBatch& batch,
                VersionedDbiStore& registry,
                std::vector<bool>& versioned_ops,
                ApplyOutcome* outcome) const {
            versioned_ops.clear();
            versioned_ops.reserve(batch.ops.size());
            for (std::size_t i = 0u; i < batch.ops.size(); ++i) {
                const ChangeOp& op = batch.ops[i];
                const bool is_versioned = registry.contains(txn, op.dbi_name);
                versioned_ops.push_back(is_versioned);
                const bool has_revision =
                    (op.op_flags & OP_HAS_REVISION_KEY) != 0 ||
                    !op.revision_key.empty();
                if (!is_versioned) {
                    if (!has_revision) {
                        continue;
                    }
                    if (outcome != nullptr) {
                        outcome->result = ApplyResult::Conflict;
                        outcome->conflict_reason =
                            ApplyConflictReason::UnexpectedLwwRevision;
                        outcome->dbi_name = op.dbi_name;
                    }
                    return false;
                }
                if (m_policy != ConflictPolicy::LastWriterWins) {
                    if (outcome != nullptr) {
                        outcome->result = ApplyResult::Conflict;
                        outcome->conflict_reason =
                            ApplyConflictReason::LwwPolicyDisabled;
                        outcome->dbi_name = op.dbi_name;
                    }
                    return false;
                }
                if (!has_revision || op.revision_key.empty()) {
                    if (outcome != nullptr) {
                        outcome->result = ApplyResult::Conflict;
                        outcome->conflict_reason =
                            ApplyConflictReason::MissingLwwRevision;
                        outcome->dbi_name = op.dbi_name;
                    }
                    return false;
                }
                if ((op.op_flags & OP_HAS_IDENTITY_KEY) != 0 ||
                    (op.op_type != ChangeOpType::Put &&
                     op.op_type != ChangeOpType::Delete)) {
                    if (outcome != nullptr) {
                        outcome->result = ApplyResult::Conflict;
                        outcome->conflict_reason =
                            ApplyConflictReason::UnsupportedLwwOperation;
                        outcome->dbi_name = op.dbi_name;
                    }
                    return false;
                }
            }
            return true;
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
        LogicalTableRegistry        m_logical_registry;
        FullSnapshotExportOptions   m_full_snapshot_options;
        mutable std::mutex          m_full_snapshot_mutex;
        std::map<std::string, std::shared_ptr<FullSnapshotSession>>
                                    m_full_snapshot_sessions;
        std::uint64_t               m_next_full_snapshot_session_id = 0u;
        std::size_t                  m_full_snapshot_creating = 0u;
#if defined(MDBXC_TEST_LOGICAL_RECOVERY_MATERIALIZATION_CHECKPOINT)
        std::function<void()>        m_logical_recovery_materialization_checkpoint;
#endif
        FullSnapshotImportOptions    m_full_snapshot_import_options;
        mutable std::mutex           m_full_snapshot_import_mutex;
        std::unique_ptr<FullSnapshotImportSession>
                                    m_full_snapshot_import_session;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ENGINE_SYNCENGINE_HPP_INCLUDED
