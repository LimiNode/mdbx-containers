#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_VECTOR_STORE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_VECTOR_STORE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file logical/adapters/VectorStoreLogicalAdapter.hpp
/// \brief Explicit logical adapter for one \c VectorStore collection.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>


namespace mdbxc {
namespace sync {

    /// \brief Adapter-local operations for a vector record collection.
    enum VectorStoreLogicalOpcode {
        VectorStoreLogicalAdd   = 1,
        VectorStoreLogicalErase = 2,
        VectorStoreLogicalClear = 3
    };

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
    public:
        static const std::uint32_t schema_version = 1u;

        /// \brief Constructs an adapter bound to \p store.
        /// \param store Existing vector collection.
        /// \param schema_id Persistent logical schema identifier.
        /// \throws std::invalid_argument for an empty schema id or zero version.
        explicit VectorStoreLogicalAdapter(
                VectorStore& store,
                const std::string& schema_id = "mdbxc.vector_store",
                std::uint32_t version = schema_version)
            : m_store(store),
              m_schema_id(schema_id),
              m_schema_version(version) {
            if (m_schema_id.empty()) {
                throw std::invalid_argument(
                    "VectorStore logical adapter schema id is empty");
            }
            if (m_schema_version == 0u) {
                throw std::invalid_argument(
                    "VectorStore logical adapter schema version is zero");
            }
            verify_table_names();
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
            change.opcode = VectorStoreLogicalErase;
            append_u8(change.payload, payload_version);
            detail::append_u64_le(change.payload, id);
            return change;
        }

        /// \brief Builds a clear-collection logical change.
        LogicalChange make_clear() const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = VectorStoreLogicalClear;
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
                if (m_adapter.m_store.m_index.dim() != 0 &&
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

            /// \brief Erases one record when it exists.
            /// \return Whether a complete record existed and was removed.
            bool erase(std::uint64_t id) {
                ensure_active();
                const LogicalChange change = m_adapter.make_erase(id);
                const RecordState state =
                    m_adapter.record_state(m_txn.handle(), id);
                if (!state.any()) {
                    return false;
                }
                if (!state.complete()) {
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
                m_pending.push_back(m_adapter.make_clear());
                try {
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
                m_adapter.m_store.m_ids.erase(id, txn);
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
                const RecordState state = record_state(txn, decoded.id);
                if (decoded.opcode == VectorStoreLogicalAdd) {
                    if (state.any()) {
                        return LogicalApplyResult::failure(
                            state.complete()
                                ? "VectorStore logical record id already exists"
                                : "VectorStore logical record is partially present");
                    }
                } else if (decoded.opcode == VectorStoreLogicalErase &&
                           state.any() && !state.complete()) {
                    return LogicalApplyResult::failure(
                        "VectorStore logical record is partially present");
                }
                return LogicalApplyResult::success();
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("VectorStore logical preflight failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "VectorStore logical preflight failed");
            }
        }

        LogicalApplyResult apply(
                MDBX_txn* txn,
                const LogicalChange& change) override {
            const LogicalApplyResult validation = preflight(txn, change);
            if (!validation.ok) return validation;
            try {
                std::lock_guard<std::mutex> store_lock(m_store.m_store_mutex);
                const DecodedChange decoded = decode_change(change);
                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_store.m_connection, txn);
                if (decoded.opcode == VectorStoreLogicalAdd) {
                    m_store.m_ids.insert_or_assign(decoded.id,
                                                   std::uint64_t(0), txn);
                    m_store.m_embeddings.insert_or_assign(
                        decoded.id, decoded.embedding, txn);
                    m_store.m_texts.insert_or_assign(
                        decoded.id, decoded.text, txn);
                    m_store.m_metadata.insert_or_assign(
                        decoded.id, decoded.metadata_json, txn);
                } else if (decoded.opcode == VectorStoreLogicalErase) {
                    m_store.m_ids.erase(decoded.id, txn);
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
        static const std::uint8_t payload_version = 1u;

        struct PayloadCursor {
            const std::uint8_t* data;
            std::size_t size;
            std::size_t pos;
        };

        struct DecodedChange {
            std::uint32_t opcode;
            std::uint64_t id;
            Embedding embedding;
            std::string text;
            std::string metadata_json;
        };

        struct RecordState {
            bool ids;
            bool embeddings;
            bool texts;
            bool metadata;

            RecordState()
                : ids(false), embeddings(false), texts(false), metadata(false) {}

            bool any() const {
                return ids || embeddings || texts || metadata;
            }

            bool complete() const {
                return ids && embeddings && texts && metadata;
            }
        };

        static void require(PayloadCursor& cursor, std::size_t size) {
            if (cursor.pos > cursor.size || size > cursor.size - cursor.pos) {
                throw std::runtime_error(
                    "VectorStore logical payload underrun");
            }
        }

        static std::uint8_t read_u8(PayloadCursor& cursor) {
            require(cursor, 1u);
            return cursor.data[cursor.pos++];
        }

        static std::uint64_t read_u64(PayloadCursor& cursor) {
            require(cursor, 8u);
            const std::uint64_t value =
                detail::read_u64_le(cursor.data + cursor.pos);
            cursor.pos += 8u;
            return value;
        }

        static std::vector<std::uint8_t> read_blob(PayloadCursor& cursor) {
            require(cursor, 4u);
            const std::uint32_t size =
                detail::read_u32_le(cursor.data + cursor.pos);
            cursor.pos += 4u;
            require(cursor, size);
            std::vector<std::uint8_t> value;
            if (size != 0u) {
                value.assign(cursor.data + cursor.pos,
                             cursor.data + cursor.pos + size);
            }
            cursor.pos += size;
            return value;
        }

        static void validate_blob_size(std::size_t size, const char* label) {
            if (size > static_cast<std::size_t>(
                           (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error(
                    std::string("VectorStore logical ") + label +
                    " is too large");
            }
        }

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::vector<std::uint8_t>& value) {
            validate_blob_size(value.size(), "blob");
            detail::append_u32_le(out, static_cast<std::uint32_t>(value.size()));
            out.insert(out.end(), value.begin(), value.end());
        }

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::string& value) {
            validate_blob_size(value.size(), "string");
            detail::append_u32_le(out, static_cast<std::uint32_t>(value.size()));
            out.insert(out.end(), value.begin(), value.end());
        }

        static void append_u8(std::vector<std::uint8_t>& out,
                              std::uint8_t value) {
            out.push_back(value);
        }

        static LogicalChange make_add_from_bytes_impl(
                const LogicalSchemaRef& schema,
                std::uint64_t id,
                const std::vector<std::uint8_t>& embedding_bytes,
                const std::string& text,
                const std::string& metadata_json) {
            validate_blob_size(embedding_bytes.size(), "embedding payload");
            validate_blob_size(text.size(), "text payload");
            validate_blob_size(metadata_json.size(), "metadata payload");
            LogicalChange change;
            change.schema = schema;
            change.opcode = VectorStoreLogicalAdd;
            append_u8(change.payload, payload_version);
            detail::append_u64_le(change.payload, id);
            append_blob(change.payload, embedding_bytes);
            append_blob(change.payload, text);
            append_blob(change.payload, metadata_json);
            return change;
        }

        LogicalChange make_add_from_bytes(
                std::uint64_t id,
                const std::vector<std::uint8_t>& embedding_bytes,
                const std::string& text,
                const std::string& metadata_json) const {
            return make_add_from_bytes_impl(schema_ref(), id, embedding_bytes,
                                            text, metadata_json);
        }

        DecodedChange decode_change(const LogicalChange& change) const {
            if (change.flags != 0u || change.schema.schema_id != m_schema_id ||
                change.schema.kind != LogicalTableKind::VectorStore ||
                change.schema.schema_version != m_schema_version) {
                throw std::runtime_error(
                    "VectorStore logical schema or flags mismatch");
            }
            DecodedChange decoded;
            decoded.opcode = change.opcode;
            decoded.id = 0u;
            PayloadCursor cursor = {
                change.payload.empty() ? nullptr : &change.payload[0],
                change.payload.size(), 0u};
            if (change.opcode == VectorStoreLogicalClear) {
                if (!change.payload.empty()) {
                    throw std::runtime_error(
                        "VectorStore logical clear payload is not empty");
                }
                return decoded;
            }
            if (change.opcode != VectorStoreLogicalAdd &&
                change.opcode != VectorStoreLogicalErase) {
                throw std::runtime_error(
                    "VectorStore logical opcode is unsupported");
            }
            if (read_u8(cursor) != payload_version) {
                throw std::runtime_error(
                    "VectorStore logical payload version is unsupported");
            }
            decoded.id = read_u64(cursor);
            if (change.opcode == VectorStoreLogicalAdd) {
                const std::vector<std::uint8_t> embedding_bytes =
                    read_blob(cursor);
                decoded.embedding = Embedding::from_bytes(
                    embedding_bytes.empty() ? nullptr : &embedding_bytes[0],
                    embedding_bytes.size());
                const std::vector<std::uint8_t> text_bytes = read_blob(cursor);
                const std::vector<std::uint8_t> metadata_bytes =
                    read_blob(cursor);
                decoded.text = std::string(text_bytes.begin(), text_bytes.end());
                decoded.metadata_json = std::string(
                    metadata_bytes.begin(), metadata_bytes.end());
            }
            if (cursor.pos != cursor.size) {
                throw std::runtime_error(
                    "VectorStore logical payload has trailing bytes");
            }
            return decoded;
        }

        RecordState record_state(MDBX_txn* txn, std::uint64_t id) const {
            RecordState state;
            state.ids = m_store.m_ids.contains(id, txn);
            state.embeddings = m_store.m_embeddings.contains(id, txn);
            state.texts = m_store.m_texts.contains(id, txn);
            state.metadata = m_store.m_metadata.contains(id, txn);
            return state;
        }

        void verify_table_names() const {
            const std::vector<std::string> names = affected_dbis();
            for (std::size_t i = 0; i < names.size(); ++i) {
                if (names[i].empty()) {
                    throw std::invalid_argument(
                        "VectorStore logical adapter DBI name is empty");
                }
            }
        }

        void initialize_storage(MDBX_txn* txn) const override {
            (void)txn;
            verify_table_names();
        }

        void verify_storage(MDBX_txn* txn) const override {
            (void)txn;
            verify_table_names();
        }

        VectorStore& m_store;
        std::string m_schema_id;
        std::uint32_t m_schema_version;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_VECTOR_STORE_LOGICAL_ADAPTER_HPP_INCLUDED
