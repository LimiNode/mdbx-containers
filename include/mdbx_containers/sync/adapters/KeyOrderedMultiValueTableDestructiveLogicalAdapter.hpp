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
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "../../KeyOrderedMultiValueTable.hpp"
#include "../LogicalTableAdapter.hpp"
#include "../LogicalSchemaValidation.hpp"
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
                    m_table.append(decoded.key, decoded.value, txn);
                    m_state.put_live(txn, decoded.id, decoded.key_bytes,
                                     decoded.value_bytes);
                    ensure_key_parity(txn, decoded.key, decoded.key_bytes);
                } else {
                    OrderedElementStateRecord record;
                    if (!m_state.get(txn, decoded.id, record) || !record.live) {
                        throw std::runtime_error("Ordered element is not live");
                    }
                    const KeyT key = decode_canonical_key(record.key);
                    const std::vector<OrderedElementId> ids =
                        m_state.live_ids_for_key(txn, record.key);
                    std::size_t index = ids.size();
                    for (std::size_t i = 0u; i < ids.size(); ++i) {
                        if (ids[i] == decoded.id) {
                            index = i;
                            break;
                        }
                    }
                    if (index == ids.size() || !m_table.erase_at(key, index, txn)) {
                        throw std::runtime_error(
                            "Ordered element physical value is missing");
                    }
                    m_state.tombstone(txn, decoded.id);
                    ensure_key_parity(txn, key, record.key);
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
            return LogicalApplyResult::success();
        }

        void ensure_key_parity(MDBX_txn* txn,
                               const KeyT& key,
                               const std::vector<std::uint8_t>& key_bytes) const {
            const std::vector<ValueT> values = m_table.find(key, txn);
            const std::vector<OrderedElementId> ids =
                m_state.live_ids_for_key(txn, key_bytes);
            std::vector<OrderedElementId> state_ids =
                m_state.live_state_ids_for_key(txn, key_bytes);
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
