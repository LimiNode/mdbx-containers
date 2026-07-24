#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file KeyValueTableLogicalAdapter.hpp
/// \brief Minimal logical adapter for \c KeyValueTable.

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../KeyValueTable.hpp"
#include "../LogicalTableAdapter.hpp"
#include "../common.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Opcodes understood by \c KeyValueTableLogicalAdapter.
    enum KeyValueTableLogicalOpcode {
        KeyValueLogicalUpsert = 1,
        KeyValueLogicalDelete = 2,
        KeyValueLogicalClear  = 3
    };

    /// \brief First concrete logical adapter for simple one-value-per-key tables.
    /// \details The payload format is adapter-owned and intentionally separate
    /// from the raw DBI wire codec. It is used only when a caller explicitly
    /// invokes \c LogicalTableRegistry::preflight_then_apply().
    template<class KeyT, class ValueT, class Options = DefaultTableOptions>
    class KeyValueTableLogicalAdapter : public ILogicalTableAdapter {
    public:
        typedef KeyValueTable<KeyT, ValueT, Options> table_type;

        KeyValueTableLogicalAdapter(table_type& table,
                                    const std::string& schema_id,
                                    const std::string& dbi_name,
                                    std::uint32_t schema_version = 1)
            : m_table(table),
              m_schema_id(schema_id),
              m_dbi_name(dbi_name),
              m_schema_version(schema_version) {
            if (m_schema_id.empty()) {
                throw std::invalid_argument(
                    "KeyValueTableLogicalAdapter schema id is empty");
            }
            if (m_dbi_name.empty()) {
                throw std::invalid_argument(
                    "KeyValueTableLogicalAdapter DBI name is empty");
            }
            if (m_schema_version == 0) {
                throw std::invalid_argument(
                    "KeyValueTableLogicalAdapter schema version is zero");
            }
        }

        LogicalSchemaRef schema_ref() const override {
            LogicalSchemaRef ref;
            ref.schema_id = m_schema_id;
            ref.kind = LogicalTableKind::KeyValue;
            ref.schema_version = m_schema_version;
            return ref;
        }

        std::vector<std::string> affected_dbis() const override {
            std::vector<std::string> out;
            out.push_back(m_dbi_name);
            return out;
        }

        LogicalChange make_upsert(const KeyT& key, const ValueT& value) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = KeyValueLogicalUpsert;
            encode_upsert(key, value, change.payload);
            return change;
        }

        LogicalChange make_delete(const KeyT& key) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = KeyValueLogicalDelete;
            encode_key_only(key, change.payload);
            return change;
        }

        LogicalChange make_clear() const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = KeyValueLogicalClear;
            return change;
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
                if (change.opcode == KeyValueLogicalUpsert) {
                    KeyT key;
                    ValueT value;
                    decode_upsert(change.payload, key, value);
                    m_table.insert_or_assign(key, value, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyValueLogicalDelete) {
                    KeyT key;
                    decode_key_only(change.payload, key);
                    (void)m_table.erase(key, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyValueLogicalClear) {
                    m_table.clear(txn);
                    return LogicalApplyResult::success();
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyValue logical adapter apply failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyValue logical adapter apply failed");
            }
            return LogicalApplyResult::failure(
                "KeyValue logical adapter opcode is unsupported");
        }

    private:
        struct PayloadCursor {
            const std::uint8_t* data;
            std::size_t size;
            std::size_t pos;
        };

        static void require(PayloadCursor& cursor, std::size_t size) {
            if (cursor.pos > cursor.size ||
                size > cursor.size - cursor.pos) {
                throw std::runtime_error(
                    "KeyValue logical payload underrun");
            }
        }

        static void append_blob(std::vector<std::uint8_t>& out,
                                const MDBX_val& value) {
            if (value.iov_len >
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max())) {
                throw std::length_error(
                    "KeyValue logical payload blob is too large");
            }
            detail::append_u32_le(out,
                static_cast<std::uint32_t>(value.iov_len));
            if (value.iov_len == 0) {
                return;
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(value.iov_base);
            out.insert(out.end(), bytes, bytes + value.iov_len);
        }

        static MDBX_val read_blob(PayloadCursor& cursor) {
            require(cursor, 4);
            const std::uint32_t size =
                detail::read_u32_le(cursor.data + cursor.pos);
            cursor.pos += 4;
            require(cursor, size);
            MDBX_val out = {
                size == 0 ? nullptr :
                    const_cast<std::uint8_t*>(cursor.data + cursor.pos),
                size
            };
            cursor.pos += size;
            return out;
        }

        static PayloadCursor make_cursor(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = {
                payload.empty() ? nullptr : &payload[0],
                payload.size(),
                0
            };
            return cursor;
        }

        static void ensure_end(const PayloadCursor& cursor) {
            if (cursor.pos != cursor.size) {
                throw std::runtime_error(
                    "KeyValue logical payload has trailing bytes");
            }
        }

        static void encode_key_only(const KeyT& key,
                                    std::vector<std::uint8_t>& out) {
            SerializeScratch key_scratch;
            out.clear();
            const MDBX_val encoded_key =
                serialize_key<Options::safe_integer_key>(key, key_scratch);
            append_blob(out, encoded_key);
        }

        static void encode_upsert(const KeyT& key,
                                  const ValueT& value,
                                  std::vector<std::uint8_t>& out) {
            SerializeScratch key_scratch;
            SerializeScratch value_scratch;
            out.clear();
            const MDBX_val encoded_key =
                serialize_key<Options::safe_integer_key>(key, key_scratch);
            const MDBX_val encoded_value =
                serialize_value(value, value_scratch);
            append_blob(out, encoded_key);
            append_blob(out, encoded_value);
        }

        static void decode_key_only(const std::vector<std::uint8_t>& payload,
                                    KeyT& key) {
            PayloadCursor cursor = make_cursor(payload);
            const MDBX_val encoded_key = read_blob(cursor);
            ensure_end(cursor);
            key = deserialize_key<KeyT>(encoded_key);
        }

        static void decode_upsert(const std::vector<std::uint8_t>& payload,
                                  KeyT& key,
                                  ValueT& value) {
            PayloadCursor cursor = make_cursor(payload);
            const MDBX_val encoded_key = read_blob(cursor);
            const MDBX_val encoded_value = read_blob(cursor);
            ensure_end(cursor);
            key = deserialize_key<KeyT>(encoded_key);
            value = deserialize_value<ValueT>(encoded_value);
        }

        LogicalApplyResult validate_payload(
                const LogicalChange& change) const {
            try {
                if (change.opcode == KeyValueLogicalUpsert) {
                    KeyT key;
                    ValueT value;
                    decode_upsert(change.payload, key, value);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyValueLogicalDelete) {
                    KeyT key;
                    decode_key_only(change.payload, key);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyValueLogicalClear) {
                    if (!change.payload.empty()) {
                        return LogicalApplyResult::failure(
                            "KeyValue clear payload must be empty");
                    }
                    return LogicalApplyResult::success();
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyValue logical payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyValue logical payload is invalid");
            }
            return LogicalApplyResult::failure(
                "KeyValue logical adapter opcode is unsupported");
        }

        table_type& m_table;
        std::string m_schema_id;
        std::string m_dbi_name;
        std::uint32_t m_schema_version;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
