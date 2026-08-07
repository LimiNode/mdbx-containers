#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_VECTOR_STORE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_VECTOR_STORE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file logical/adapters/VectorStoreLogicalAdapter.hpp
/// \brief Explicit logical adapter for one \c VectorStore collection.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>


namespace mdbxc {
namespace sync {

    /// \brief Adapter-local operations for a vector record collection.
    enum class VectorStoreLogicalOpcode : std::uint32_t {
        Add   = 1u,
        Erase = 2u,
        Clear = 3u
    };

    constexpr std::uint32_t opcode_value(VectorStoreLogicalOpcode opcode) {
        return static_cast<std::uint32_t>(opcode);
    }

    /// \brief Logical adapter for one persistent \c VectorStore collection.
    /// \details The adapter owns no transport. It provides an explicit typed
    /// capture session and apply callbacks for the four physical DBIs owned by
    /// the bound store: ids, embeddings, text, and metadata.
    ///
    /// Add payloads carry the generated record id and the canonical embedding
    /// bytes, so a receiver never allocates a local id while applying a remote
    /// record. The contract is intended for one authoritative writer or
    /// causally serialized delivery; it does not provide distributed id
    /// allocation or conflict resolution.
    class VectorStoreLogicalAdapter : public ILogicalTableAdapter {
    private:
        static const std::uint32_t supported_schema_version = 1u;

    public:

        /// \brief Constructs an adapter bound to \p store.
        /// \param store Existing vector collection.
        /// \param schema_id Persistent logical schema identifier.
        /// \throws std::invalid_argument for an empty schema id or unsupported version.
        explicit VectorStoreLogicalAdapter(
                VectorStore& store,
                const std::string& schema_id = "mdbxc.vector_store",
                std::uint32_t version = supported_schema_version)
            : m_store(store),
              m_schema_id(schema_id),
              m_schema_version(version) {
            if (m_schema_id.empty()) {
                throw std::invalid_argument(
                    "VectorStore logical adapter schema id is empty");
            }
            if (m_schema_version != supported_schema_version) {
                throw std::invalid_argument(
                    "VectorStore logical adapter schema version is unsupported");
            }
            verify_table_names();
        }

        /// \brief Opens a VectorStore with marker-aware DBI creation policy.
        /// \details A fresh schema creates the four collection DBIs. When a
        /// matching schema-v1 marker already exists, all four DBIs are opened
        /// without \c MDBX_CREATE so missing or incompatible storage fails
        /// closed. Direct \c VectorStore construction remains create-by-default.
        static std::unique_ptr<VectorStore> open_store_for_schema(
                const std::shared_ptr<Connection>& connection,
                const std::string& schema_id = "mdbxc.vector_store",
                const std::string& collection = "default",
                VectorMetric metric = VectorMetric::COSINE) {
            if (!connection || schema_id.empty()) {
                throw std::invalid_argument(
                    "VectorStore logical schema store configuration is invalid");
            }

            const std::string valid_collection =
                VectorStore::validate_collection_name(collection);
            const std::string expected_primary_dbi =
                VectorStore::make_table_name(valid_collection, "ids");
            std::vector<std::string> expected_dbis;
            expected_dbis.push_back(expected_primary_dbi);
            expected_dbis.push_back(
                VectorStore::make_table_name(valid_collection, "embeddings"));
            expected_dbis.push_back(
                VectorStore::make_table_name(valid_collection, "texts"));
            expected_dbis.push_back(
                VectorStore::make_table_name(valid_collection, "metadata"));
            std::sort(expected_dbis.begin(), expected_dbis.end());

            bool marker_exists = false;
            {
                Transaction txn = connection->transaction(TransactionMode::READ_ONLY);
                MDBX_dbi schema_dbi = 0;
                const int open_rc = mdbx_dbi_open(
                    txn.handle(), "_mdbxc_sync_schema",
                    static_cast<MDBX_db_flags_t>(0), &schema_dbi);
                if (open_rc != MDBX_NOTFOUND) {
                    check_mdbx(open_rc,
                               "VectorStore logical schema registry open failed");
                    SchemaRegistryStore schemas(connection->env_handle());
                    LogicalSchemaRecord marker;
                    if (schemas.get(txn.handle(), schema_id, marker)) {
                        if (marker.kind != LogicalTableKind::VectorStore ||
                            marker.schema_version != supported_schema_version ||
                            marker.flags != 0u ||
                            marker.dbi_name != expected_primary_dbi ||
                            marker.dbi_names != expected_dbis) {
                            throw std::runtime_error(
                                "VectorStore logical schema marker does not match collection DBIs");
                        }
                        marker_exists = true;
                    }
                }
                txn.rollback();
            }

            const MDBX_db_flags_t table_flags = marker_exists
                ? MDBX_DB_DEFAULTS
                : static_cast<MDBX_db_flags_t>(MDBX_DB_DEFAULTS | MDBX_CREATE);
            return std::unique_ptr<VectorStore>(
                new VectorStore(connection, valid_collection, metric, table_flags));
        }

        /// \brief Returns the adapter's persistent logical schema reference.
        LogicalSchemaRef schema_ref() const override {
            return LogicalSchemaRef(m_schema_id, LogicalTableKind::VectorStore,
                                    m_schema_version);
        }

        /// \brief Returns the four physical DBIs owned by the collection.
        std::vector<std::string> affected_dbis() const override {
            std::vector<std::string> out;
            out.push_back(m_store.m_ids.dbi_name());
            out.push_back(m_store.m_embeddings.dbi_name());
            out.push_back(m_store.m_texts.dbi_name());
            out.push_back(m_store.m_metadata.dbi_name());
            return out;
        }

        /// \brief Returns the id DBI as the schema's primary DBI.
        std::string primary_dbi() const override {
            return m_store.m_ids.dbi_name();
        }

        /// \brief Returns a marker record matching this adapter's DBI layout.
        /// \param ordered_origin Authoritative origin, or a zero id for direct
        /// logical apply without ordered delivery.
        LogicalSchemaRecord schema_record(
                const NodeId& ordered_origin = make_zero_node()) const {
            LogicalSchemaRecord record;
            record.dbi_name = primary_dbi();
            record.kind = LogicalTableKind::VectorStore;
            record.schema_version = m_schema_version;
            record.flags = 0u;
            record.dbi_names = affected_dbis();
            record.ordered_delivery_origin_node_id = ordered_origin;
            return record;
        }

        /// \brief Builds an add-record logical change with an explicit id.
        LogicalChange make_add(std::uint64_t id,
                               const Embedding& embedding,
                               const std::string& text,
                               const std::string& metadata_json) const {
            embedding.validate();
            const std::vector<std::uint8_t> embedding_bytes =
                embedding.to_bytes();
            return make_add_from_bytes(id, embedding_bytes, text,
                                        metadata_json);
        }

        /// \brief Builds an erase-record logical change.
        LogicalChange make_erase(std::uint64_t id) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode_value(VectorStoreLogicalOpcode::Erase);
            append_u8(change.payload, payload_version);
            detail::append_u64_le(change.payload, id);
            return change;
        }

        /// \brief Builds a clear-collection logical change.
        LogicalChange make_clear() const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode_value(VectorStoreLogicalOpcode::Clear);
            return change;
        }

        /// \brief Transaction-bound typed capture session.
        /// \details Local mutations are suppressed from generic raw capture.
        /// The session must be committed or rolled back by its owner; failures
        /// after physical mutation deactivate it and roll back its transaction.
        class LogicalCaptureSession {
        public:
            explicit LogicalCaptureSession(
                    const VectorStoreLogicalAdapter& adapter)
                : m_adapter(adapter),
                  m_sync_apply_guard(
                      adapter.m_store.m_connection->sync_apply_write_guard()),
                  m_store_lock(adapter.m_store.m_store_mutex),
                  m_txn(adapter.m_store.m_connection->transaction(
                      TransactionMode::WRITABLE)),
                  m_active(true) {
                const LogicalApplyResult marker_result =
                    validate_logical_adapter_marker(
                        m_txn.handle(),
                        adapter.m_store.m_connection->env_handle(),
                        adapter);
                if (!marker_result.ok) {
                    throw std::runtime_error(marker_result.error);
                }
            }

            ~LogicalCaptureSession() noexcept {
                rollback();
            }

            LogicalCaptureSession(const LogicalCaptureSession&) = delete;
            LogicalCaptureSession& operator=(
                    const LogicalCaptureSession&) = delete;

            /// \brief Adds a record and captures its explicit id.
            /// \return Allocated id, unique in this transaction's collection.
            std::uint64_t add(const Embedding& embedding,
                              const std::string& text,
                              const std::string& metadata_json = "{}") {
                ensure_active();
                embedding.validate();
                m_adapter.m_store.ensure_index_fresh_locked();
                if (m_adapter.m_store.m_index.dim() != 0u &&
                    embedding.dim != m_adapter.m_store.m_index.dim()) {
                    throw std::invalid_argument(
                        "Embedding dimension does not match index dimension");
                }
                const std::vector<std::uint8_t> embedding_bytes =
                    embedding.to_bytes();
                validate_blob_size(embedding_bytes.size(),
                                   "embedding payload");
                validate_blob_size(text.size(), "text payload");
                validate_blob_size(metadata_json.size(), "metadata payload");

                try {
                    const std::uint64_t id = m_adapter.m_store.m_ids.append(
                        std::uint64_t(0), m_txn.handle());
                    const LogicalChange change =
                        m_adapter.make_add_from_bytes(
                            id, embedding_bytes, text, metadata_json);
                    m_pending.push_back(change);
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_store.m_connection, m_txn.handle());
                    m_adapter.m_store.m_embeddings.insert_or_assign(
                        id, embedding, m_txn.handle());
                    m_adapter.m_store.m_texts.insert_or_assign(
                        id, text, m_txn.handle());
                    m_adapter.m_store.m_metadata.insert_or_assign(
                        id, metadata_json, m_txn.handle());
                    m_adapter.m_store.m_index.add(id, embedding);
                    return id;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Erases one live record while retaining its id marker.
            /// \return Whether a live record existed and was removed.
            bool erase(std::uint64_t id) {
                ensure_active();
                const LogicalChange change = m_adapter.make_erase(id);
                const RecordState state =
                    m_adapter.record_state(m_txn.handle(), id);
                const RecordStateKind state_kind = state.kind();
                if (state_kind == RecordStateKind::Unused ||
                    state_kind == RecordStateKind::Erased) {
                    return false;
                }
                if (state_kind == RecordStateKind::Corrupt) {
                    throw std::runtime_error(
                        "VectorStore logical record is partially present");
                }
                try {
                    m_pending.push_back(change);
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_store.m_connection, m_txn.handle());
                    erase_record(id, m_txn.handle());
                    m_adapter.m_store.m_index.erase(id);
                    return true;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Clears all four collection DBIs.
            void clear() {
                ensure_active();
                try {
                    m_adapter.validate_collection_state(m_txn.handle());
                    m_pending.push_back(m_adapter.make_clear());
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_store.m_connection, m_txn.handle());
                    m_adapter.m_store.m_ids.clear(m_txn.handle());
                    m_adapter.m_store.m_embeddings.clear(m_txn.handle());
                    m_adapter.m_store.m_texts.clear(m_txn.handle());
                    m_adapter.m_store.m_metadata.clear(m_txn.handle());
                    m_adapter.m_store.m_index.clear();
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Commits and appends pending logical changes to p out.
            void commit(std::vector<LogicalChange>& out) {
                ensure_active();
                const std::size_t old_size = out.size();
                try {
                    out.insert(out.end(), m_pending.begin(), m_pending.end());
                    m_txn.commit();
                } catch (...) {
                    out.erase(out.begin() +
                              static_cast<std::ptrdiff_t>(old_size),
                              out.end());
                    rollback_and_deactivate();
                    throw;
                }
                m_pending.clear();
                m_active = false;
            }

            /// \brief Commits local changes and enqueues one logical envelope.
            LogicalDeliveryEnvelope commit_to_outbox(
                    ILogicalDeliveryOutbox& outbox,
                    const DbId& destination,
                    const CodecBounds* bounds = nullptr) {
                ensure_active();
                LogicalChangeFrame frame;
                frame.changes = m_pending;
                try {
                    const LogicalDeliveryEnvelope envelope =
                        outbox.enqueue_logical_delivery(
                            m_txn.handle(), destination, frame, bounds);
                    m_txn.commit();
                    m_pending.clear();
                    m_active = false;
                    return envelope;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Rolls back the session and makes it inactive.
            void rollback() noexcept {
                if (!m_active) return;
                rollback_and_deactivate();
            }

            /// \brief Returns the number of pending logical changes.
            std::size_t pending_size() const { return m_pending.size(); }

        private:
            void erase_record(std::uint64_t id, MDBX_txn* txn) {
                m_adapter.m_store.m_embeddings.erase(id, txn);
                m_adapter.m_store.m_texts.erase(id, txn);
                m_adapter.m_store.m_metadata.erase(id, txn);
            }

            void rollback_and_deactivate() noexcept {
                try {
                    m_pending.clear();
                    m_txn.rollback();
                    m_adapter.m_store.rebuild_index_impl_locked();
                } catch (...) {
                }
                m_active = false;
            }

            void ensure_active() const {
                if (!m_active) {
                    throw std::logic_error(
                        "VectorStore logical capture session is not active");
                }
            }

            const VectorStoreLogicalAdapter& m_adapter;
            Connection::SyncApplyWriteGuard m_sync_apply_guard;
            std::unique_lock<std::mutex> m_store_lock;
            Transaction m_txn;
            std::vector<LogicalChange> m_pending;
            bool m_active;
        };

        std::unique_ptr<LogicalCaptureSession> begin_capture_session() const {
            return std::unique_ptr<LogicalCaptureSession>(
                new LogicalCaptureSession(*this));
        }

        LogicalApplyResult preflight(
                MDBX_txn* txn,
                const LogicalChange& change) const override {
            try {
                std::lock_guard<std::mutex> store_lock(m_store.m_store_mutex);
                const DecodedChange decoded = decode_change(change);
                return preflight_decoded_change(txn, decoded);
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("VectorStore logical preflight failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "VectorStore logical preflight failed");
            }
        }

        LogicalApplyResult preflight_batch(
                MDBX_txn* txn,
                const LogicalChangeBatchView& changes) const override {
            try {
                std::lock_guard<std::mutex> store_lock(m_store.m_store_mutex);
                return preflight_batch_locked(txn, changes);
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("VectorStore logical batch preflight failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "VectorStore logical batch preflight failed");
            }
        }

        LogicalApplyResult apply(
                MDBX_txn* txn,
                const LogicalChange& change) override {
            try {
                std::lock_guard<std::mutex> store_lock(m_store.m_store_mutex);
                const DecodedChange decoded = decode_change(change);
                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_store.m_connection, txn);
                if (decoded.opcode == opcode_value(VectorStoreLogicalOpcode::Add)) {
                    m_store.m_ids.insert_or_assign(decoded.id,
                                                   std::uint64_t(0), txn);
                    m_store.m_embeddings.insert_or_assign(
                        decoded.id, decoded.embedding, txn);
                    m_store.m_texts.insert_or_assign(
                        decoded.id, decoded.text, txn);
                    m_store.m_metadata.insert_or_assign(
                        decoded.id, decoded.metadata_json, txn);
                } else if (decoded.opcode == opcode_value(VectorStoreLogicalOpcode::Erase)) {
                    m_store.m_embeddings.erase(decoded.id, txn);
                    m_store.m_texts.erase(decoded.id, txn);
                    m_store.m_metadata.erase(decoded.id, txn);
                } else {
                    m_store.m_ids.clear(txn);
                    m_store.m_embeddings.clear(txn);
                    m_store.m_texts.clear(txn);
                    m_store.m_metadata.clear(txn);
                }
                return LogicalApplyResult::success();
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("VectorStore logical apply failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "VectorStore logical apply failed");
            }
        }

    private:
#include "VectorStoreLogicalAdapter.ipp"

    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_VECTOR_STORE_LOGICAL_ADAPTER_HPP_INCLUDED
