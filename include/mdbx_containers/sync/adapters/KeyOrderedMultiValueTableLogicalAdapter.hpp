#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file KeyOrderedMultiValueTableLogicalAdapter.hpp
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

#include "../../KeyOrderedMultiValueTable.hpp"
#include "../ILogicalDeliveryOutbox.hpp"
#include "KeyValueTableLogicalAdapter.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Opcodes understood by \c KeyOrderedMultiValueTableLogicalAdapter.
    enum KeyOrderedMultiValueLogicalOpcode {
        KeyOrderedMultiValueLogicalAppendOne = 1
    };

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
    public:
        typedef KeyOrderedMultiValueTable<KeyT, ValueT, Options> table_type;

        static_assert(std::is_same<typename KeyCodec::value_type,
                                   KeyT>::value,
                      "KeyOrderedMultiValueTableLogicalAdapter key codec local type must match KeyT");
        static_assert(std::is_same<typename ValueCodec::value_type,
                                   ValueT>::value,
                      "KeyOrderedMultiValueTableLogicalAdapter value codec local type must match ValueT");

        KeyOrderedMultiValueTableLogicalAdapter(
                table_type& table,
                const std::string& schema_id,
                std::uint32_t schema_version = 1)
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
            if (m_schema_version == 0u) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTableLogicalAdapter schema version is zero");
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
            change.opcode = KeyOrderedMultiValueLogicalAppendOne;
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
        class LogicalCaptureSession {
        public:
            explicit LogicalCaptureSession(
                    const KeyOrderedMultiValueTableLogicalAdapter& adapter)
                : m_adapter(adapter),
                  m_txn(adapter.m_table.connection()->transaction(
                      TransactionMode::WRITABLE)),
                  m_active(true) {
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
                const std::size_t old_size = out.size();
                out.insert(out.end(), m_pending.begin(), m_pending.end());
                try {
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

            /// \brief Commits appends and an ordered delivery atomically.
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

            void rollback_and_deactivate() noexcept {
                try {
                    m_pending.clear();
                } catch (...) {
                }
                try {
                    m_txn.rollback();
                } catch (...) {
                }
                m_active = false;
            }

            void ensure_active() const {
                if (!m_active) {
                    throw std::logic_error(
                        "KeyOrderedMultiValue logical capture session is not active");
                }
            }

            const KeyOrderedMultiValueTableLogicalAdapter& m_adapter;
            Transaction m_txn;
            std::vector<LogicalChange> m_pending;
            bool m_active;
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
        struct PayloadCursor {
            const std::uint8_t* data;
            std::size_t size;
            std::size_t pos;
        };

        static void require(PayloadCursor& cursor, std::size_t size) {
            if (cursor.pos > cursor.size || size > cursor.size - cursor.pos) {
                throw std::runtime_error(
                    "KeyOrderedMultiValue logical payload underrun");
            }
        }

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::vector<std::uint8_t>& bytes) {
            if (bytes.size() > static_cast<std::size_t>(
                    (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error(
                    "KeyOrderedMultiValue logical payload blob is too large");
            }
            detail::append_u32_le(out,
                static_cast<std::uint32_t>(bytes.size()));
            out.insert(out.end(), bytes.begin(), bytes.end());
        }

        static std::vector<std::uint8_t> read_blob(PayloadCursor& cursor) {
            require(cursor, 4u);
            const std::uint32_t size =
                detail::read_u32_le(cursor.data + cursor.pos);
            cursor.pos += 4u;
            require(cursor, size);
            std::vector<std::uint8_t> out;
            if (size != 0u) {
                out.assign(cursor.data + cursor.pos,
                           cursor.data + cursor.pos + size);
            }
            cursor.pos += size;
            return out;
        }

        static PayloadCursor make_cursor(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = {
                payload.empty() ? nullptr : &payload[0],
                payload.size(),
                0u
            };
            return cursor;
        }

        static void ensure_end(const PayloadCursor& cursor) {
            if (cursor.pos != cursor.size) {
                throw std::runtime_error(
                    "KeyOrderedMultiValue logical payload has trailing bytes");
            }
        }

        static void encode_pair(const KeyT& key,
                                const ValueT& value,
                                std::vector<std::uint8_t>& out) {
            out.clear();
            append_blob(out, KeyCodec::encode(key));
            append_blob(out, ValueCodec::encode(value));
        }

        static std::pair<KeyT, ValueT> decode_pair(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const KeyT key = KeyCodec::decode(read_blob(cursor));
            const ValueT value = ValueCodec::decode(read_blob(cursor));
            ensure_end(cursor);
            return std::make_pair(key, value);
        }

        static LogicalApplyResult validate_payload(
                const LogicalChange& change) {
            if (change.opcode != KeyOrderedMultiValueLogicalAppendOne) {
                return LogicalApplyResult::failure(
                    "KeyOrderedMultiValue logical adapter opcode is unsupported");
            }
            try {
                (void)decode_pair(change.payload);
                return LogicalApplyResult::success();
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string(
                        "KeyOrderedMultiValue logical payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyOrderedMultiValue logical payload is invalid");
            }
        }

        table_type& m_table;
        std::string m_schema_id;
        std::uint32_t m_schema_version;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
