#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file logical/adapters/KeyTableLogicalAdapter.hpp
/// \brief Minimal logical adapter for \c KeyTable.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "KeyValueTableLogicalAdapter.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Opcodes understood by \c KeyTableLogicalAdapter.
    enum class KeyTableLogicalOpcode : std::uint32_t {
        Insert = 1u,
        Delete = 2u,
        Clear  = 3u
    };

    constexpr std::uint32_t opcode_value(KeyTableLogicalOpcode opcode) {
        return static_cast<std::uint32_t>(opcode);
    }

    /// \brief Logical adapter for typed \c KeyTable insert/delete/clear ops.
    /// \details The adapter reuses the explicit key codec tags used by
    /// \c KeyValueTableLogicalAdapter.
    template<class KeyT,
             class KeyCodec,
             class Options = DefaultTableOptions>
    class KeyTableLogicalAdapter : public ILogicalTableAdapter {
    private:
        static const std::uint32_t default_schema_version = 1u;

    public:
        typedef KeyTable<KeyT, Options> table_type;

        static_assert(std::is_same<typename KeyCodec::value_type,
                                   KeyT>::value,
                      "KeyTableLogicalAdapter key codec local type must match KeyT");

        KeyTableLogicalAdapter(table_type& table,
                               const std::string& schema_id,
                               std::uint32_t schema_version = default_schema_version)
            : m_table(table),
              m_schema_id(schema_id),
              m_schema_version(schema_version) {
            if (m_schema_id.empty()) {
                throw std::invalid_argument(
                    "KeyTableLogicalAdapter schema id is empty");
            }
            if (m_table.dbi_name().empty()) {
                throw std::invalid_argument(
                    "KeyTableLogicalAdapter DBI name is empty");
            }
            if (m_schema_version < default_schema_version) {
                throw std::invalid_argument(
                    "KeyTableLogicalAdapter schema version is zero");
            }
        }

        LogicalSchemaRef schema_ref() const override {
            LogicalSchemaRef ref;
            ref.schema_id = m_schema_id;
            ref.kind = LogicalTableKind::KeyTable;
            ref.schema_version = m_schema_version;
            return ref;
        }

        std::string primary_dbi() const override {
            return m_table.dbi_name();
        }

        std::vector<std::string> affected_dbis() const override {
            std::vector<std::string> out;
            out.push_back(m_table.dbi_name());
            return out;
        }

        LogicalChange make_insert(const KeyT& key) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode_value(KeyTableLogicalOpcode::Insert);
            encode_key_only(key, change.payload);
            return change;
        }

        LogicalChange make_delete(const KeyT& key) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode_value(KeyTableLogicalOpcode::Delete);
            encode_key_only(key, change.payload);
            return change;
        }

        LogicalChange make_clear() const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode_value(KeyTableLogicalOpcode::Clear);
            return change;
        }

        /// \brief Transaction-bound typed logical capture session.
        /// \details The session owns a writable transaction. Successful local
        /// writes are buffered as logical changes and raw capture is
        /// suppressed for the transaction. Pending changes are copied to the
        /// caller only by \c commit(out), after the session has prepared the
        /// destination vector and before the native commit. If a session
        /// operation throws after physical mutation, outbox enqueue, or native
        /// commit processing begins, the session requests transaction rollback
        /// and becomes inactive. Preparation or encoding failures before
        /// transaction mutation leave it active. The adapter, table, and
        /// connection referenced by the adapter must outlive the session.
        class LogicalCaptureSession
                : private detail::CapturedLogicalTransaction {
        public:
            explicit LogicalCaptureSession(
                    const KeyTableLogicalAdapter& adapter)
                : detail::CapturedLogicalTransaction(
                      *adapter.m_table.connection()),
                  m_adapter(adapter) {
                const LogicalApplyResult marker_result =
                    validate_logical_adapter_marker(
                        m_txn.handle(),
                        adapter.m_table.connection()->env_handle(),
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

            bool insert(const KeyT& key) {
                ensure_active();
                const LogicalChange change = m_adapter.make_insert(key);
                const std::size_t previous_size = m_pending.size();
                m_pending.push_back(change);
                try {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    const bool inserted =
                        m_adapter.m_table.insert(key, m_txn.handle());
                    if (!inserted) {
                        m_pending.resize(previous_size);
                    }
                    return inserted;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            bool erase(const KeyT& key) {
                ensure_active();
                const LogicalChange change = m_adapter.make_delete(key);
                const std::size_t previous_size = m_pending.size();
                m_pending.push_back(change);
                try {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    const bool removed =
                        m_adapter.m_table.erase(key, m_txn.handle());
                    if (!removed) {
                        m_pending.resize(previous_size);
                    }
                    return removed;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            void clear() {
                ensure_active();
                const LogicalChange change = m_adapter.make_clear();
                m_pending.push_back(change);
                try {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    m_adapter.m_table.clear(m_txn.handle());
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Commits local mutations and returns their logical changes.
            /// \warning The returned changes are not published atomically with
            /// the table transaction. Use \c commit_to_outbox() when local
            /// durability and delivery enqueueing must share one transaction.
            void commit(std::vector<LogicalChange>& out) {
                ensure_active();
                commit_pending(out);
            }

            /// \brief Commits captured table mutations and their delivery atomically.
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
            void ensure_active() const {
                detail::CapturedLogicalTransaction::ensure_active(
                    "KeyTable logical capture session is not active");
            }

            const KeyTableLogicalAdapter& m_adapter;
        };

        std::unique_ptr<LogicalCaptureSession> begin_capture_session() const {
            return std::unique_ptr<LogicalCaptureSession>(
                new LogicalCaptureSession(*this));
        }

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
            if (!validation.ok) return validation;

            try {
                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_table.connection(), txn);
                if (change.opcode == opcode_value(KeyTableLogicalOpcode::Insert)) {
                    const KeyT key = decode_key_only(change.payload);
                    (void)m_table.insert(key, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == opcode_value(KeyTableLogicalOpcode::Delete)) {
                    const KeyT key = decode_key_only(change.payload);
                    (void)m_table.erase(key, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == opcode_value(KeyTableLogicalOpcode::Clear)) {
                    m_table.clear(txn);
                    return LogicalApplyResult::success();
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyTable logical adapter apply failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyTable logical adapter apply failed");
            }
            return LogicalApplyResult::failure(
                "KeyTable logical adapter opcode is unsupported");
        }

    private:
#include "KeyTableLogicalAdapter.ipp"

    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
