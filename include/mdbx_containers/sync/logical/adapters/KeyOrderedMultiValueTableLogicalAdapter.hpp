#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file logical/adapters/KeyOrderedMultiValueTableLogicalAdapter.hpp
/// \brief Ordered logical adapter for append-only multi-value tables.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
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
#include "KeyOrderedMultiValueTableLogicalAdapter.ipp"

    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
