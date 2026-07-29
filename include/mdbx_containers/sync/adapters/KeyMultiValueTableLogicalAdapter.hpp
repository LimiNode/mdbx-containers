#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file KeyMultiValueTableLogicalAdapter.hpp
/// \brief Logical adapter for unordered \c KeyMultiValueTable operations.

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "../../KeyMultiValueTable.hpp"
#include "KeyValueTableLogicalAdapter.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Opcodes understood by \c KeyMultiValueTableLogicalAdapter.
    enum KeyMultiValueLogicalOpcode {
        KeyMultiValueLogicalInsertOne       = 1,
        KeyMultiValueLogicalEraseKey        = 2,
        KeyMultiValueLogicalEraseAllValues  = 3,
        KeyMultiValueLogicalClear           = 4
    };

    /// \brief Logical adapter for an unordered \c KeyMultiValueTable.
    /// \details The adapter transports public key/value bytes, never the
    /// table's private duplicate sequence prefix. Replaying an insert assigns
    /// a local prefix at the receiving replica and therefore preserves
    /// multiplicity without treating physical duplicate bytes as an identity.
    template<class KeyT, class ValueT,
             class KeyCodec, class ValueCodec,
             class Options = DefaultTableOptions>
    class KeyMultiValueTableLogicalAdapter : public ILogicalTableAdapter {
    public:
        typedef KeyMultiValueTable<KeyT, ValueT, Options> table_type;

        static_assert(std::is_same<typename KeyCodec::value_type,
                                   KeyT>::value,
                      "KeyMultiValueTableLogicalAdapter key codec local type must match KeyT");
        static_assert(std::is_same<typename ValueCodec::value_type,
                                   ValueT>::value,
                      "KeyMultiValueTableLogicalAdapter value codec local type must match ValueT");

        KeyMultiValueTableLogicalAdapter(table_type& table,
                                         const std::string& schema_id,
                                         std::uint32_t schema_version = 1)
            : m_table(table),
              m_schema_id(schema_id),
              m_schema_version(schema_version) {
            if (m_schema_id.empty()) {
                throw std::invalid_argument(
                    "KeyMultiValueTableLogicalAdapter schema id is empty");
            }
            if (m_table.dbi_name().empty()) {
                throw std::invalid_argument(
                    "KeyMultiValueTableLogicalAdapter DBI name is empty");
            }
            if (m_schema_version == 0u) {
                throw std::invalid_argument(
                    "KeyMultiValueTableLogicalAdapter schema version is zero");
            }
        }

        LogicalSchemaRef schema_ref() const override {
            return LogicalSchemaRef(m_schema_id,
                                    LogicalTableKind::KeyMultiValue,
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

        LogicalChange make_insert_one(const KeyT& key,
                                      const ValueT& value) const {
            return make_pair_change(KeyMultiValueLogicalInsertOne, key, value);
        }

        LogicalChange make_erase_key(const KeyT& key) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = KeyMultiValueLogicalEraseKey;
            encode_key(key, change.payload);
            return change;
        }

        LogicalChange make_erase_all_values(const KeyT& key,
                                             const ValueT& value) const {
            return make_pair_change(KeyMultiValueLogicalEraseAllValues,
                                    key, value);
        }

        LogicalChange make_clear() const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = KeyMultiValueLogicalClear;
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
            if (!validation.ok) {
                return validation;
            }

            try {
                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_table.connection(), txn);
                if (change.opcode == KeyMultiValueLogicalInsertOne) {
                    const std::pair<KeyT, ValueT> pair =
                        decode_pair(change.payload);
                    m_table.insert(pair.first, pair.second, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyMultiValueLogicalEraseKey) {
                    (void)m_table.erase(decode_key(change.payload), txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyMultiValueLogicalEraseAllValues) {
                    const std::pair<KeyT, ValueT> pair =
                        decode_pair(change.payload);
                    (void)m_table.erase(pair.first, pair.second, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyMultiValueLogicalClear) {
                    m_table.clear(txn);
                    return LogicalApplyResult::success();
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyMultiValue logical adapter apply failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyMultiValue logical adapter apply failed");
            }
            return LogicalApplyResult::failure(
                "KeyMultiValue logical adapter opcode is unsupported");
        }

    private:
        struct PayloadCursor {
            const std::uint8_t* data;
            std::size_t size;
            std::size_t pos;
        };

        LogicalChange make_pair_change(std::uint32_t opcode,
                                        const KeyT& key,
                                        const ValueT& value) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode;
            encode_pair(key, value, change.payload);
            return change;
        }

        static void require(PayloadCursor& cursor, std::size_t size) {
            if (cursor.pos > cursor.size || size > cursor.size - cursor.pos) {
                throw std::runtime_error(
                    "KeyMultiValue logical payload underrun");
            }
        }

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::vector<std::uint8_t>& bytes) {
            if (bytes.size() >
                static_cast<std::size_t>(
                    (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error(
                    "KeyMultiValue logical payload blob is too large");
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
                    "KeyMultiValue logical payload has trailing bytes");
            }
        }

        static void encode_key(const KeyT& key,
                               std::vector<std::uint8_t>& out) {
            out.clear();
            append_blob(out, KeyCodec::encode(key));
        }

        static void encode_pair(const KeyT& key,
                                const ValueT& value,
                                std::vector<std::uint8_t>& out) {
            out.clear();
            append_blob(out, KeyCodec::encode(key));
            append_blob(out, ValueCodec::encode(value));
        }

        static KeyT decode_key(const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const KeyT key = KeyCodec::decode(read_blob(cursor));
            ensure_end(cursor);
            return key;
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
            try {
                if (change.opcode == KeyMultiValueLogicalInsertOne ||
                    change.opcode == KeyMultiValueLogicalEraseAllValues) {
                    (void)decode_pair(change.payload);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyMultiValueLogicalEraseKey) {
                    (void)decode_key(change.payload);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyMultiValueLogicalClear) {
                    if (!change.payload.empty()) {
                        return LogicalApplyResult::failure(
                            "KeyMultiValue logical clear payload must be empty");
                    }
                    return LogicalApplyResult::success();
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyMultiValue logical payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyMultiValue logical payload is invalid");
            }
            return LogicalApplyResult::failure(
                "KeyMultiValue logical adapter opcode is unsupported");
        }

        table_type& m_table;
        std::string m_schema_id;
        std::uint32_t m_schema_version;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
