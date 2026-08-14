#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file logical/adapters/KeyOrderedMultiValueTableLogicalAdapter.hpp
/// \brief Ordered logical adapter for append-only multi-value tables.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "KeyValueTableLogicalAdapter.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Opcodes understood by \c KeyOrderedMultiValueTableLogicalAdapter.
    enum class KeyOrderedMultiValueLogicalOpcode : std::uint32_t {
        AppendOne = 1u
    };

    constexpr std::uint32_t opcode_value(KeyOrderedMultiValueLogicalOpcode opcode) {
        return static_cast<std::uint32_t>(opcode);
    }

    /// \brief Logical adapter for append-only \c KeyOrderedMultiValueTable.
    /// \details The adapter transports public key/value bytes and requires the
    /// ordered logical-delivery entry point. The receiver uses \c append() so
    /// its private duplicate prefix remains local while one origin stream
    /// preserves observable append order. Destructive operations are excluded
    /// until a persistent element-identity and tombstone contract exists.
    template<class KeyT, class ValueT,
             class KeyCodec, class ValueCodec,
             class Options = DefaultTableOptions>
    class KeyOrderedMultiValueTableLogicalAdapter
            : public ILogicalTableAdapter {
    private:
        class AutomaticCapture;
        static const std::uint32_t supported_schema_version = 1u;

    public:
        typedef KeyOrderedMultiValueTable<KeyT, ValueT, Options> table_type;

        static_assert(std::is_same<typename KeyCodec::value_type,
                                   KeyT>::value,
                      "KeyOrderedMultiValueTableLogicalAdapter key codec local type must match KeyT");
        static_assert(std::is_same<typename ValueCodec::value_type,
                                   ValueT>::value,
                      "KeyOrderedMultiValueTableLogicalAdapter value codec local type must match ValueT");

        /// \brief Creates the append-only schema-version-1 adapter.
        /// \details The optional argument is retained for source compatibility,
        /// but this adapter owns only the version-1 append payload contract.
        KeyOrderedMultiValueTableLogicalAdapter(
                table_type& table,
                const std::string& schema_id,
                std::uint32_t schema_version = supported_schema_version)
            : m_table(table),
              m_schema_id(schema_id),
              m_schema_version(schema_version) {
            if (m_schema_id.empty()) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTableLogicalAdapter schema id is empty");
            }
            if (m_table.dbi_name().empty()) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTableLogicalAdapter DBI name is empty");
            }
            if (m_schema_version != supported_schema_version) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTableLogicalAdapter supports only schema version 1");
            }
        }

        LogicalSchemaRef schema_ref() const override {
            return LogicalSchemaRef(m_schema_id,
                                    LogicalTableKind::KeyOrderedMultiValue,
                                    m_schema_version);
        }

        std::string primary_dbi() const override {
            return m_table.dbi_name();
        }

        std::vector<std::string> affected_dbis() const override {
            std::vector<std::string> out;
            out.push_back(m_table.dbi_name());
            return out;
        }

        bool requires_ordered_delivery() const override {
            return true;
        }

        /// \brief Creates one append-only logical operation.
        LogicalChange make_append(const KeyT& key, const ValueT& value) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode_value(KeyOrderedMultiValueLogicalOpcode::AppendOne);
            encode_pair(key, value, change.payload);
            return change;
        }

        /// \brief Creates one append-only logical operation for \c insert.
        LogicalChange make_insert(const KeyT& key, const ValueT& value) const {
            return make_append(key, value);
        }

        /// \brief Creates connection-owned automatic capture for this adapter.
        /// \details Schema-v1 ordered capture is append-only. Ordinary
        /// \c append and \c insert calls append receiver-neutral frames to the
        /// durable logical journal at transaction pre-commit; receiver routes
        /// are selected later by \c materialize_logical_journal().
        std::shared_ptr<ILogicalDbiCapture> make_automatic_capture(
                const DbId& destination) const {
            return std::shared_ptr<ILogicalDbiCapture>(
                new AutomaticCapture(m_table.connection()->env_handle(),
                                     m_table.dbi_name(), schema_ref(),
                                     destination));
        }

        /// \brief Transaction-bound typed capture session for append history.
        /// \details The session owns one writable transaction, suppresses raw
        /// capture, and records only \c append and its \c insert alias. A
        /// failure after physical mutation, outbox enqueue, or native commit
        /// processing begins rolls back and deactivates the session.
        class LogicalCaptureSession
                : private detail::CapturedLogicalTransaction {
        public:
            explicit LogicalCaptureSession(
                    const KeyOrderedMultiValueTableLogicalAdapter& adapter)
                : detail::CapturedLogicalTransaction(
                      *adapter.m_table.connection()),
                  m_adapter(adapter) {
                adapter.m_table.connection()
                    ->ensure_logical_dbi_capture_session_supported(
                        m_txn.handle(), adapter.m_table.dbi_name());
                const LogicalApplyResult marker_result =
                    validate_logical_adapter_marker(
                        m_txn.handle(),
                        adapter.m_table.connection()->env_handle(),
                        adapter);
                if (!marker_result.ok) {
                    throw std::runtime_error(marker_result.error);
                }
                const LogicalApplyResult origin_result =
                    validate_ordered_logical_adapter_origin(
                        m_txn.handle(),
                        adapter.m_table.connection()->env_handle(),
                        adapter);
                if (!origin_result.ok) {
                    throw std::runtime_error(origin_result.error);
                }
            }

            ~LogicalCaptureSession() noexcept {
                rollback();
            }

            LogicalCaptureSession(const LogicalCaptureSession&) = delete;
            LogicalCaptureSession& operator=(
                    const LogicalCaptureSession&) = delete;

            void append(const KeyT& key, const ValueT& value) {
                ensure_active();
                append_then_mutate(m_adapter.make_append(key, value),
                    [this, &key, &value]() {
                        m_adapter.m_table.append(key, value, m_txn.handle());
                    });
            }

            void insert(const KeyT& key, const ValueT& value) {
                append(key, value);
            }

            /// \brief Commits local appends and returns their logical changes.
            /// \warning Returned changes are not atomically published. Use
            /// \c commit_to_outbox() when durable delivery is required.
            void commit(std::vector<LogicalChange>& out) {
                ensure_active();
                commit_pending(out);
            }

            /// \brief Commits appends and an ordered delivery atomically.
            LogicalDeliveryEnvelope commit_to_outbox(
                    ILogicalDeliveryOutbox& outbox,
                    const DbId& destination,
                    const NodeId& receiver,
                    const CodecBounds* bounds = nullptr) {
                ensure_active();
                return commit_pending_to_outbox(outbox, destination, receiver,
                                                bounds);
            }

            void rollback() noexcept {
                if (!m_active) {
                    return;
                }
                rollback_and_deactivate();
            }

            std::size_t pending_size() const {
                return m_pending.size();
            }

        private:
            template<class Mutation>
            void append_then_mutate(const LogicalChange& change,
                                    const Mutation& mutation) {
                m_pending.push_back(change);
                try {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    mutation();
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            void ensure_active() const {
                detail::CapturedLogicalTransaction::ensure_active(
                    "KeyOrderedMultiValue logical capture session is not active");
            }

            const KeyOrderedMultiValueTableLogicalAdapter& m_adapter;
        };

        /// \brief Starts a capture session bound to this adapter instance.
        /// \details The session stores an adapter reference, so construction is
        /// restricted to an lvalue adapter that outlives the session.
        std::unique_ptr<LogicalCaptureSession> begin_capture_session() const & {
            return std::unique_ptr<LogicalCaptureSession>(
                new LogicalCaptureSession(*this));
        }

        std::unique_ptr<LogicalCaptureSession> begin_capture_session() const && = delete;

        LogicalApplyResult preflight(
                MDBX_txn* txn,
                const LogicalChange& change) const override {
            (void)txn;
            return validate_payload(change);
        }

        LogicalApplyResult apply(
                MDBX_txn* txn,
                const LogicalChange& change) override {
            const LogicalApplyResult validation = validate_payload(change);
            if (!validation.ok) {
                return validation;
            }

            try {
                const std::pair<KeyT, ValueT> pair =
                    decode_pair(change.payload);
                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_table.connection(), txn);
                m_table.append(pair.first, pair.second, txn);
                return LogicalApplyResult::success();
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string(
                        "KeyOrderedMultiValue logical adapter apply failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyOrderedMultiValue logical adapter apply failed");
            }
        }

    private:
        class AutomaticCapture : public ILogicalDbiCapture {
        public:
            AutomaticCapture(MDBX_env* env,
                             const std::string& dbi_name,
                             const LogicalSchemaRef& schema,
                             const DbId& destination)
                : m_env(env),
                  m_dbi_name(dbi_name),
                  m_schema(schema),
                  m_destination(destination) {
                if (m_env == nullptr ||
                    !is_logical_schema_ref_complete(m_schema) ||
                    m_schema.kind != LogicalTableKind::KeyOrderedMultiValue ||
                    m_schema.schema_version != supported_schema_version ||
                    is_zero_sync_id(m_destination)) {
                    throw std::invalid_argument(
                        "KeyOrderedMultiValue automatic logical capture is invalid");
                }
            }

            const std::string& dbi_name() const override { return m_dbi_name; }
            LogicalSchemaRef schema_ref() const override { return m_schema; }
            const DbId& destination() const override { return m_destination; }

            void record_insert(MDBX_txn* txn, const void* key,
                               const void* value) override {
                LogicalChange change;
                change.schema = m_schema;
                change.opcode = opcode_value(
                    KeyOrderedMultiValueLogicalOpcode::AppendOne);
                encode_pair(checked_key(key), checked_value(value), change.payload);
                append(txn, change);
            }

            void record_erase_key(MDBX_txn*, const void*) override {
                reject_destructive_operation();
            }

            void record_erase_all_values(MDBX_txn*, const void*,
                                         const void*) override {
                reject_destructive_operation();
            }

            void record_erase_one_value(MDBX_txn*, const void*,
                                        const void*) override {
                reject_destructive_operation();
            }

            void record_clear(MDBX_txn*) override {
                reject_destructive_operation();
            }

            void flush_in_txn(MDBX_txn* txn) override {
                LogicalChangeFrame frame;
                {
                    std::lock_guard<std::mutex> locker(m_mutex);
                    typename std::map<MDBX_txn*, LogicalChangeFrame>::iterator it =
                        m_pending.find(txn);
                    if (it == m_pending.end() || it->second.changes.empty()) return;
                    frame.changes.swap(it->second.changes);
                    m_pending.erase(it);
                }
                MetaStore meta(m_env);
                LogicalJournalStore journal(m_env);
                LogicalOutboxStore outbox(m_env);
                meta.open(txn);
                const NodeId origin = meta.get_node_id(txn);
                if (is_zero_sync_id(origin)) {
                    throw std::logic_error(
                        "KeyOrderedMultiValue automatic logical capture requires local node identity");
                }
                SchemaRegistryStore schemas(m_env);
                LogicalSchemaRecord record;
                if (!schemas.get(txn, m_schema.schema_id, record) ||
                    record.kind != m_schema.kind ||
                    record.schema_version != m_schema.schema_version ||
                    record.dbi_name != m_dbi_name ||
                    record.dbi_names.size() != 1u ||
                    record.dbi_names[0] != m_dbi_name) {
                    throw std::logic_error(
                        "KeyOrderedMultiValue automatic logical capture schema marker is invalid");
                }
                const LogicalApplyResult authority_result =
                    validate_ordered_logical_schema_record_origin(
                        txn, m_env, record);
                if (!authority_result.ok) {
                    throw std::logic_error(authority_result.error);
                }
                if (!journal.is_initialized(txn)) {
                    if (outbox.has_persistent_state(txn)) {
                        throw std::logic_error(
                            "logical journal cannot start with legacy outbox state");
                    }
                    journal.initialize(txn);
                }
                (void)journal.append(txn, m_destination, origin, frame);
            }

            void discard_txn(MDBX_txn* txn) noexcept override {
                try {
                    std::lock_guard<std::mutex> locker(m_mutex);
                    m_pending.erase(txn);
                } catch (...) {
                }
            }

        private:
            static const KeyT& checked_key(const void* value) {
                if (value == nullptr) {
                    throw std::invalid_argument(
                        "KeyOrderedMultiValue automatic capture key is null");
                }
                return *static_cast<const KeyT*>(value);
            }

            static const ValueT& checked_value(const void* value) {
                if (value == nullptr) {
                    throw std::invalid_argument(
                        "KeyOrderedMultiValue automatic capture value is null");
                }
                return *static_cast<const ValueT*>(value);
            }

            static void reject_destructive_operation() {
                throw std::logic_error(
                    "KeyOrderedMultiValue automatic logical capture is append-only");
            }

            void append(MDBX_txn* txn, const LogicalChange& change) {
                if (txn == nullptr) {
                    throw std::invalid_argument(
                        "KeyOrderedMultiValue automatic capture transaction is null");
                }
                std::lock_guard<std::mutex> locker(m_mutex);
                m_pending[txn].changes.push_back(change);
            }

            MDBX_env* m_env;
            std::string m_dbi_name;
            LogicalSchemaRef m_schema;
            DbId m_destination;
            std::mutex m_mutex;
            std::map<MDBX_txn*, LogicalChangeFrame> m_pending;
        };

#include "KeyOrderedMultiValueTableLogicalAdapter.ipp"

    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
