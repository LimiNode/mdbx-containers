#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_DESTRUCTIVE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_DESTRUCTIVE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file KeyOrderedMultiValueTableDestructiveLogicalAdapter.hpp
/// \brief Destructive ordered logical adapter with persistent element ids.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "../../KeyOrderedMultiValueTable.hpp"
#include "../ILogicalDeliveryOutbox.hpp"
#include "../LogicalTableAdapter.hpp"
#include "../LogicalSchemaValidation.hpp"
#include "../stores/MetaStore.hpp"
#include "KeyOrderedMultiValueDestructiveState.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Opcodes for schema-version-2 destructive ordered tables.
    enum KeyOrderedMultiValueDestructiveLogicalOpcode {
        KeyOrderedMultiValueDestructiveLogicalAppend = 1,
        KeyOrderedMultiValueDestructiveLogicalErase = 2
    };

    /// \brief Ordered logical adapter with stable identities and tombstones.
    /// \details Schema version 2 is intentionally distinct from the v1
    /// append-only adapter. Each live value has an immutable
    /// \c OrderedElementId, allowing one exact occurrence to be erased even
    /// when the same value occurs repeatedly for a key.
    template<class KeyT, class ValueT,
             class KeyCodec, class ValueCodec,
             class Options = DefaultTableOptions>
    class KeyOrderedMultiValueTableDestructiveLogicalAdapter
            : public ILogicalTableAdapter {
    public:
        typedef KeyOrderedMultiValueTable<KeyT, ValueT, Options> table_type;

        static_assert(std::is_same<typename KeyCodec::value_type,
                                   KeyT>::value,
                      "KeyOrderedMultiValue destructive logical adapter key codec local type must match KeyT");
        static_assert(std::is_same<typename ValueCodec::value_type,
                                   ValueT>::value,
                      "KeyOrderedMultiValue destructive logical adapter value codec local type must match ValueT");

        KeyOrderedMultiValueTableDestructiveLogicalAdapter(
                table_type& table,
                const std::string& schema_id,
                const std::string& state_dbi_name,
                const std::string& by_key_dbi_name,
                std::uint32_t schema_version = 2u)
            : m_table(table),
              m_schema_id(schema_id),
              m_schema_version(schema_version),
              m_state(table.connection()->env_handle(), state_dbi_name,
                      by_key_dbi_name) {
            if (m_schema_id.empty()) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValue destructive logical schema id is empty");
            }
            if (m_table.dbi_name().empty()) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValue destructive logical DBI name is empty");
            }
            if (m_schema_version != 2u) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValue destructive logical adapter supports only schema version 2");
            }
            const std::vector<std::string> names = affected_dbis();
            for (std::size_t i = 0u; i < names.size(); ++i) {
                for (std::size_t j = i + 1u; j < names.size(); ++j) {
                    if (names[i] == names[j]) {
                        throw std::invalid_argument(
                            "KeyOrderedMultiValue destructive logical DBI names must differ");
                    }
                }
            }
        }

        /// \brief Opens the primary table with marker-aware creation policy.
        /// \details A fresh schema preserves the caller's \c MDBX_CREATE flag.
        /// If its persistent schema marker already exists, this helper removes
        /// \c MDBX_CREATE before constructing the table accessor. A missing
        /// primary DBI is then reported as corruption instead of silently being
        /// recreated. Use this helper for every schema-v2 reopen; direct table
        /// construction is the ordinary non-schema-bound table API.
        static std::shared_ptr<table_type> open_primary_for_schema(
                const std::shared_ptr<Connection>& connection,
                const std::string& schema_id,
                const std::string& primary_dbi_name,
                MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE) {
            if (!connection || schema_id.empty() || primary_dbi_name.empty()) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValue destructive primary configuration is invalid");
            }

            bool marker_exists = false;
            {
                Transaction txn = connection->transaction(TransactionMode::READ_ONLY);
                MDBX_dbi schema_dbi = 0;
                const int open_rc = mdbx_dbi_open(
                    txn.handle(), "_mdbxc_sync_schema",
                    static_cast<MDBX_db_flags_t>(0), &schema_dbi);
                if (open_rc != MDBX_NOTFOUND) {
                    check_mdbx(open_rc,
                               "KeyOrderedMultiValue destructive schema registry open failed");
                    SchemaRegistryStore schemas(connection->env_handle());
                    LogicalSchemaRecord marker;
                    if (schemas.get(txn.handle(), schema_id, marker)) {
                        if (marker.kind != LogicalTableKind::KeyOrderedMultiValue ||
                            marker.schema_version != 2u ||
                            marker.dbi_name != primary_dbi_name) {
                            throw std::runtime_error(
                                "KeyOrderedMultiValue destructive schema marker does not match primary DBI");
                        }
                        marker_exists = true;
                    }
                }
                txn.rollback();
            }

            const MDBX_db_flags_t effective_flags = marker_exists
                ? static_cast<MDBX_db_flags_t>(flags & ~MDBX_CREATE)
                : flags;
            return std::shared_ptr<table_type>(
                new table_type(connection, primary_dbi_name, effective_flags));
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
            out.push_back(m_state.state_dbi_name());
            out.push_back(m_state.by_key_dbi_name());
            return out;
        }

        bool requires_ordered_delivery() const override {
            return true;
        }

        void initialize_storage(MDBX_txn* txn) const override {
            checked_txn_env(
                txn, m_table.connection()->env_handle(),
                "KeyOrderedMultiValue destructive storage initialization");
            // Fresh v2 setup cannot silently invent ids for an existing table.
            if (m_table.count(txn) != 0u) {
                throw std::runtime_error(
                    "KeyOrderedMultiValue destructive primary DBI is not empty");
            }
            m_state.initialize_empty(txn);
        }

        void verify_storage(MDBX_txn* txn) const override {
            checked_txn_env(
                txn, m_table.connection()->env_handle(),
                "KeyOrderedMultiValue destructive storage verification");
            m_table.count(txn);
            m_state.verify_existing(txn);
        }

        LogicalChange make_append(const OrderedElementId& id,
                                  const KeyT& key,
                                  const ValueT& value) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = KeyOrderedMultiValueDestructiveLogicalAppend;
            encode_append(id, key, value, change.payload);
            return change;
        }

        LogicalChange make_erase(const OrderedElementId& id) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = KeyOrderedMultiValueDestructiveLogicalErase;
            change.payload = encode_ordered_element_id_logical(id);
            return change;
        }

        /// \brief Transaction-bound capture for destructive ordered changes.
        /// \details Only \c commit_to_outbox() is exposed. This preserves the
        /// atomic boundary between the physical table, v2 element state, and
        /// the durable ordered-delivery envelope. An append followed by erase
        /// of the same new id coalesces to no frame operation while its
        /// allocated counter remains monotonic.
        class LogicalCaptureSession {
        public:
            explicit LogicalCaptureSession(
                    const KeyOrderedMultiValueTableDestructiveLogicalAdapter& adapter)
                : m_adapter(adapter),
                  m_txn(adapter.m_table.connection()->transaction(
                      TransactionMode::WRITABLE)),
                  m_active(true) {
                const LogicalApplyResult marker_result =
                    validate_logical_adapter_marker(
                        m_txn.handle(), adapter.m_table.connection()->env_handle(),
                        adapter);
                if (!marker_result.ok) {
                    throw std::runtime_error(marker_result.error);
                }
                const LogicalApplyResult origin_result =
                    validate_ordered_logical_adapter_origin(
                        m_txn.handle(), adapter.m_table.connection()->env_handle(),
                        adapter);
                if (!origin_result.ok) {
                    throw std::runtime_error(origin_result.error);
                }
                MetaStore meta(adapter.m_table.connection()->env_handle());
                meta.open(m_txn.handle());
                m_origin = meta.get_node_id(m_txn.handle());
                if (compare_node_id(m_origin, make_zero_node()) == 0) {
                    throw std::runtime_error(
                        "KeyOrderedMultiValue destructive capture origin is missing");
                }
            }

            ~LogicalCaptureSession() noexcept {
                rollback();
            }

            LogicalCaptureSession(const LogicalCaptureSession&) = delete;
            LogicalCaptureSession& operator=(const LogicalCaptureSession&) = delete;

            /// \brief Appends and returns its durable element identity.
            OrderedElementId append(const KeyT& key, const ValueT& value) {
                ensure_active();
                try {
                    const OrderedElementId id =
                        m_adapter.m_state.allocate_id(m_txn.handle(), m_origin);
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    m_adapter.append_live_element(m_txn.handle(), id, key, value);
                    m_pending.push_back(m_adapter.make_append(id, key, value));
                    return id;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Erases exactly one live value by its immutable identity.
            void erase(const OrderedElementId& id) {
                ensure_active();
                try {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    const std::size_t pending_index = find_pending_append(id);
                    if (pending_index != m_pending.size()) {
                        m_adapter.erase_live_element(m_txn.handle(), id, false);
                        m_pending.erase(m_pending.begin() +
                            static_cast<std::ptrdiff_t>(pending_index));
                    } else {
                        m_adapter.erase_live_element(m_txn.handle(), id, true);
                        m_pending.push_back(m_adapter.make_erase(id));
                    }
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Resolves and erases one current per-key append position.
            bool erase_at(const KeyT& key,
                          std::size_t index,
                          const BroadEraseBounds& bounds) {
                ensure_active();
                try {
                    OrderedElementCandidateSet candidates(bounds);
                    const bool found = resolve_live_elements(
                        key, candidates,
                        [index](std::size_t current,
                                const OrderedElementStateRecord&) {
                            return current == index;
                        });
                    if (!found) return false;
                    erase_selected(candidates.sorted_ids());
                    return true;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Resolves and erases all canonical value matches under a key.
            std::size_t erase_value(const KeyT& key,
                                    const ValueT& value,
                                    const BroadEraseBounds& bounds) {
                ensure_active();
                try {
                    const std::vector<std::uint8_t> value_bytes =
                        ValueCodec::encode(value);
                    OrderedElementCandidateSet candidates(bounds);
                    resolve_live_elements(
                        key, candidates,
                        [&value_bytes](std::size_t,
                                       const OrderedElementStateRecord& record) {
                            return record.value == value_bytes;
                        });
                    const std::vector<OrderedElementId> ids =
                        candidates.sorted_ids();
                    erase_selected(ids);
                    return ids.size();
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Resolves and erases all current values under a key.
            std::size_t erase_key(const KeyT& key,
                                  const BroadEraseBounds& bounds) {
                ensure_active();
                try {
                    OrderedElementCandidateSet candidates(bounds);
                    resolve_live_elements(
                        key, candidates,
                        [](std::size_t, const OrderedElementStateRecord&) {
                            return true;
                        });
                    const std::vector<OrderedElementId> ids =
                        candidates.sorted_ids();
                    erase_selected(ids);
                    return ids.size();
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Commits captured mutations and delivery atomically.
            LogicalDeliveryEnvelope commit_to_outbox(
                    ILogicalDeliveryOutbox& outbox,
                    const DbId& destination,
                    const CodecBounds* bounds = nullptr) {
                ensure_active();
                try {
                    LogicalChangeFrame frame;
                    frame.changes = m_pending;
                    const LogicalDeliveryEnvelope envelope =
                        outbox.enqueue_logical_delivery(
                            m_txn.handle(), destination, frame, bounds);
                    if (compare_node_id(envelope.origin_node_id, m_origin) != 0) {
                        throw std::runtime_error(
                            "Ordered destructive outbox origin does not match capture origin");
                    }
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
                if (m_active) rollback_and_deactivate();
            }

            std::size_t pending_size() const {
                return m_pending.size();
            }

        private:
            static bool contains_origin(const std::vector<NodeId>& origins,
                                        const NodeId& origin) {
                for (std::size_t i = 0u; i < origins.size(); ++i) {
                    if (compare_node_id(origins[i], origin) == 0) {
                        return true;
                    }
                }
                return false;
            }

            template<typename Selector>
            bool resolve_live_elements(
                    const KeyT& key,
                    OrderedElementCandidateSet& candidates,
                    Selector select) const {
                const std::vector<std::uint8_t> key_bytes = KeyCodec::encode(key);
                std::vector<OrderedElementId> index_ids =
                    m_adapter.m_state.live_ids_for_key(
                        m_txn.handle(), key_bytes, &candidates);
                std::vector<OrderedElementId> state_ids =
                    m_adapter.m_state.live_state_ids_for_key(
                        m_txn.handle(), key_bytes, &candidates);
                std::sort(index_ids.begin(), index_ids.end(), OrderedElementIdLess());
                std::sort(state_ids.begin(), state_ids.end(), OrderedElementIdLess());
                if (index_ids != state_ids) {
                    throw std::runtime_error(
                        "Ordered destructive key index and state records differ");
                }

                std::vector<ValueT> physical_values;
                m_adapter.m_table.db_collect_values(
                    key, physical_values, m_txn.handle(),
                    [&candidates]() {
                        candidates.inspect_record();
                    });
                if (physical_values.size() != index_ids.size()) {
                    throw std::runtime_error(
                        "Ordered destructive table and state counts differ");
                }

                std::vector<NodeId> verified_origins;
                bool selected = false;
                for (std::size_t i = 0u; i < index_ids.size(); ++i) {
                    if (!contains_origin(verified_origins, index_ids[i].origin)) {
                        m_adapter.m_state.verify_introduced_high_water(
                            m_txn.handle(), index_ids[i].origin, &candidates);
                        verified_origins.push_back(index_ids[i].origin);
                    }
                    candidates.inspect_record();
                    OrderedElementStateRecord record;
                    if (!m_adapter.m_state.get(
                            m_txn.handle(), index_ids[i], record) ||
                        !record.live || record.key != key_bytes ||
                        record.value != ValueCodec::encode(physical_values[i])) {
                        throw std::runtime_error(
                            "Ordered destructive table and state value order differs");
                    }
                    if (select(i, record)) {
                        candidates.select(index_ids[i]);
                        selected = true;
                    }
                }
                return selected;
            }

            void erase_selected(const std::vector<OrderedElementId>& ids) {
                for (std::size_t i = 0u; i < ids.size(); ++i) {
                    erase(ids[i]);
                }
            }

            std::size_t find_pending_append(const OrderedElementId& id) const {
                for (std::size_t i = 0u; i < m_pending.size(); ++i) {
                    if (m_pending[i].opcode ==
                            KeyOrderedMultiValueDestructiveLogicalAppend &&
                        decode_change(m_pending[i]).id == id) {
                        return i;
                    }
                }
                return m_pending.size();
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
                        "KeyOrderedMultiValue destructive capture session is not active");
                }
            }

            const KeyOrderedMultiValueTableDestructiveLogicalAdapter& m_adapter;
            Transaction m_txn;
            NodeId m_origin;
            std::vector<LogicalChange> m_pending;
            bool m_active;
        };

        std::unique_ptr<LogicalCaptureSession> begin_capture_session() const & {
            return std::unique_ptr<LogicalCaptureSession>(
                new LogicalCaptureSession(*this));
        }

        std::unique_ptr<LogicalCaptureSession> begin_capture_session() const && = delete;

        LogicalApplyResult preflight(MDBX_txn* txn,
                                     const LogicalChange& change) const override {
            try {
                const DecodedChange decoded = decode_change(change);
                return validate_state(txn, decoded);
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyOrderedMultiValue destructive payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyOrderedMultiValue destructive payload is invalid");
            }
        }

        LogicalApplyResult preflight_batch(
                MDBX_txn* txn,
                const LogicalChangeBatchView& changes) const override {
            std::map<OrderedElementId, DecodedChange, OrderedElementIdLess> seen;
            bool have_virtual_high_water = false;
            std::uint64_t virtual_high_water = 0u;
            try {
                for (std::size_t i = 0u; i < changes.size(); ++i) {
                    const DecodedChange decoded = decode_change(changes[i]);
                    const typename std::map<OrderedElementId, DecodedChange,
                        OrderedElementIdLess>::const_iterator prior =
                        seen.find(decoded.id);
                    if (prior != seen.end()) {
                        return LogicalApplyResult::failure(
                            "Duplicate OrderedElementId in destructive logical batch");
                    }
                    const LogicalApplyResult state_result =
                        validate_state(txn, decoded);
                    if (!state_result.ok) return state_result;
                    OrderedElementStateRecord existing;
                    const bool exists = m_state.get(txn, decoded.id, existing);
                    if (decoded.is_append && !exists) {
                        if (!have_virtual_high_water) {
                            virtual_high_water = m_state.highest_introduced(
                                txn, decoded.id.origin);
                            have_virtual_high_water = true;
                        }
                        if (decoded.id.sequence <= virtual_high_water) {
                            return LogicalApplyResult::failure(
                                "OrderedElementId is not increasing in destructive logical batch");
                        }
                        virtual_high_water = decoded.id.sequence;
                    }
                    seen.insert(std::make_pair(decoded.id, decoded));
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyOrderedMultiValue destructive payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyOrderedMultiValue destructive payload is invalid");
            }
            return LogicalApplyResult::success();
        }

        LogicalApplyResult apply(MDBX_txn* txn,
                                 const LogicalChange& change) override {
            try {
                const DecodedChange decoded = decode_change(change);
                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_table.connection(), txn);
                if (decoded.is_append) {
                    OrderedElementStateRecord existing;
                    if (m_state.get(txn, decoded.id, existing)) {
                        if (existing.live && existing.key == decoded.key_bytes &&
                            existing.value == decoded.value_bytes) {
                            return LogicalApplyResult::success();
                        }
                        throw std::runtime_error(
                            "OrderedElementId conflicts with persisted state");
                    }
                    append_live_element(txn, decoded.id, decoded.key, decoded.value);
                } else {
                    erase_live_element(txn, decoded.id, true);
                }
                return LogicalApplyResult::success();
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyOrderedMultiValue destructive apply failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyOrderedMultiValue destructive apply failed");
            }
        }

        const OrderedElementStateStore& state_store() const {
            return m_state;
        }

    private:
        struct PayloadCursor {
            const std::uint8_t* data;
            std::size_t size;
            std::size_t pos;
        };

        struct DecodedChange {
            OrderedElementId id;
            bool is_append;
            KeyT key;
            ValueT value;
            std::vector<std::uint8_t> key_bytes;
            std::vector<std::uint8_t> value_bytes;

            DecodedChange() : is_append(false) {}
        };

        static void require(const PayloadCursor& cursor, std::size_t count) {
            if (cursor.pos > cursor.size || count > cursor.size - cursor.pos) {
                throw std::runtime_error(
                    "KeyOrderedMultiValue destructive payload underrun");
            }
        }

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::vector<std::uint8_t>& bytes) {
            if (bytes.size() > static_cast<std::size_t>(
                    (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error(
                    "KeyOrderedMultiValue destructive payload blob is too large");
            }
            detail::append_u32_le(out, static_cast<std::uint32_t>(bytes.size()));
            out.insert(out.end(), bytes.begin(), bytes.end());
        }

        static std::vector<std::uint8_t> read_blob(PayloadCursor& cursor) {
            require(cursor, 4u);
            const std::uint32_t count =
                detail::read_u32_le(cursor.data + cursor.pos);
            cursor.pos += 4u;
            require(cursor, count);
            std::vector<std::uint8_t> out;
            if (count != 0u) {
                out.assign(cursor.data + cursor.pos,
                           cursor.data + cursor.pos + count);
            }
            cursor.pos += count;
            return out;
        }

        static PayloadCursor make_cursor(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = {
                payload.empty() ? nullptr : &payload[0], payload.size(), 0u
            };
            return cursor;
        }

        static std::vector<std::uint8_t> read_id(PayloadCursor& cursor) {
            const std::size_t size = NodeId().size() + 8u;
            require(cursor, size);
            std::vector<std::uint8_t> out(cursor.data + cursor.pos,
                                          cursor.data + cursor.pos + size);
            cursor.pos += size;
            return out;
        }

        static void require_end(const PayloadCursor& cursor) {
            if (cursor.pos != cursor.size) {
                throw std::runtime_error(
                    "KeyOrderedMultiValue destructive payload has trailing bytes");
            }
        }

        static KeyT decode_canonical_key(const std::vector<std::uint8_t>& bytes) {
            const KeyT key = KeyCodec::decode(bytes);
            if (KeyCodec::encode(key) != bytes) {
                throw std::runtime_error(
                    "KeyOrderedMultiValue destructive key is non-canonical");
            }
            return key;
        }

        static ValueT decode_canonical_value(
                const std::vector<std::uint8_t>& bytes) {
            const ValueT value = ValueCodec::decode(bytes);
            if (ValueCodec::encode(value) != bytes) {
                throw std::runtime_error(
                    "KeyOrderedMultiValue destructive value is non-canonical");
            }
            return value;
        }

        static void encode_append(const OrderedElementId& id,
                                  const KeyT& key,
                                  const ValueT& value,
                                  std::vector<std::uint8_t>& out) {
            out = encode_ordered_element_id_logical(id);
            append_blob(out, KeyCodec::encode(key));
            append_blob(out, ValueCodec::encode(value));
        }

        static DecodedChange decode_change(const LogicalChange& change) {
            DecodedChange out;
            if (change.opcode == KeyOrderedMultiValueDestructiveLogicalErase) {
                out.id = decode_ordered_element_id_logical(change.payload);
                return out;
            }
            if (change.opcode != KeyOrderedMultiValueDestructiveLogicalAppend) {
                throw std::runtime_error(
                    "KeyOrderedMultiValue destructive opcode is unsupported");
            }
            PayloadCursor cursor = make_cursor(change.payload);
            out.id = decode_ordered_element_id_logical(read_id(cursor));
            out.key_bytes = read_blob(cursor);
            out.value_bytes = read_blob(cursor);
            require_end(cursor);
            out.key = decode_canonical_key(out.key_bytes);
            out.value = decode_canonical_value(out.value_bytes);
            out.is_append = true;
            return out;
        }

        LogicalApplyResult validate_state(MDBX_txn* txn,
                                          const DecodedChange& decoded) const {
            const LogicalApplyResult marker_result =
                validate_logical_adapter_marker(
                    txn, m_table.connection()->env_handle(), *this);
            if (!marker_result.ok) return marker_result;
            if (decoded.is_append) {
                m_state.verify_introduced_high_water(txn, decoded.id.origin);
                SchemaRegistryStore schemas(m_table.connection()->env_handle());
                LogicalSchemaRecord marker;
                if (!schemas.get(txn, m_schema_id, marker) ||
                    compare_node_id(marker.ordered_delivery_origin_node_id,
                                    decoded.id.origin) != 0) {
                    return LogicalApplyResult::failure(
                        "Append OrderedElementId origin does not match schema marker");
                }
            }
            OrderedElementStateRecord record;
            const bool exists = m_state.get(txn, decoded.id, record);
            const std::uint64_t introduced =
                m_state.highest_introduced(txn, decoded.id.origin);
            if (exists && decoded.id.sequence > introduced) {
                return LogicalApplyResult::failure(
                    "Ordered element introduced high-water mark is corrupt");
            }
            if (decoded.is_append && exists) {
                if (!record.live) {
                    return LogicalApplyResult::failure(
                        "OrderedElementId is tombstoned");
                }
                if (record.key != decoded.key_bytes ||
                    record.value != decoded.value_bytes) {
                    return LogicalApplyResult::failure(
                        "OrderedElementId conflicts with persisted state");
                }
            }
            if (decoded.is_append && !exists &&
                decoded.id.sequence <= introduced) {
                return LogicalApplyResult::failure(
                    "OrderedElementId is not above the introduced high-water mark");
            }
            if (!decoded.is_append && (!exists || !record.live)) {
                return LogicalApplyResult::failure(
                    "OrderedElementId is not live");
            }
            if (decoded.is_append) {
                ensure_key_parity(txn, decoded.key, decoded.key_bytes);
            } else {
                ensure_key_parity(txn, decode_canonical_key(record.key),
                                  record.key);
            }
            return LogicalApplyResult::success();
        }

        void append_live_element(MDBX_txn* txn,
                                 const OrderedElementId& id,
                                 const KeyT& key,
                                 const ValueT& value) const {
            const std::vector<std::uint8_t> key_bytes = KeyCodec::encode(key);
            const std::vector<std::uint8_t> value_bytes = ValueCodec::encode(value);
            m_state.verify_introduced_high_water(txn, id.origin);
            m_table.append(key, value, txn);
            m_state.put_live(txn, id, key_bytes, value_bytes);
            ensure_key_parity(txn, key, key_bytes);
        }

        void erase_live_element(MDBX_txn* txn,
                                const OrderedElementId& id,
                                bool preserve_tombstone) const {
            OrderedElementStateRecord record;
            if (!m_state.get(txn, id, record) || !record.live) {
                throw std::runtime_error("Ordered element is not live");
            }
            const KeyT key = decode_canonical_key(record.key);
            const std::vector<OrderedElementId> ids =
                m_state.live_ids_for_key(txn, record.key);
            std::size_t index = ids.size();
            for (std::size_t i = 0u; i < ids.size(); ++i) {
                if (ids[i] == id) {
                    index = i;
                    break;
                }
            }
            if (index == ids.size() || !m_table.erase_at(key, index, txn)) {
                throw std::runtime_error("Ordered element physical value is missing");
            }
            if (preserve_tombstone) {
                m_state.tombstone(txn, id);
            } else {
                m_state.erase_live(txn, id);
            }
            ensure_key_parity(txn, key, record.key);
        }

        void ensure_key_parity(MDBX_txn* txn,
                               const KeyT& key,
                               const std::vector<std::uint8_t>& key_bytes) const {
            const std::vector<ValueT> values = m_table.find(key, txn);
            std::vector<OrderedElementId> ids =
                m_state.live_ids_for_key(txn, key_bytes);
            std::vector<OrderedElementId> state_ids =
                m_state.live_state_ids_for_key(txn, key_bytes);
            std::sort(ids.begin(), ids.end(), OrderedElementIdLess());
            std::sort(state_ids.begin(), state_ids.end(), OrderedElementIdLess());
            if (values.size() != ids.size() || ids != state_ids) {
                throw std::runtime_error(
                    "Ordered element state, index, and table counts differ");
            }
            for (std::size_t i = 0u; i < ids.size(); ++i) {
                OrderedElementStateRecord record;
                if (!m_state.get(txn, ids[i], record) || !record.live ||
                    record.key != key_bytes ||
                    record.value != ValueCodec::encode(values[i])) {
                    throw std::runtime_error(
                        "Ordered element state and table value order differs");
                }
            }
        }

        table_type& m_table;
        std::string m_schema_id;
        std::uint32_t m_schema_version;
        OrderedElementStateStore m_state;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_DESTRUCTIVE_LOGICAL_ADAPTER_HPP_INCLUDED
