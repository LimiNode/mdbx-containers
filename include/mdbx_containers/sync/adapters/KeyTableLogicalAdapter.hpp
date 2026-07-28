#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file KeyTableLogicalAdapter.hpp
/// \brief Minimal logical adapter for \c KeyTable.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "../../KeyTable.hpp"
#include "KeyValueTableLogicalAdapter.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Opcodes understood by \c KeyTableLogicalAdapter.
    enum KeyTableLogicalOpcode {
        KeyTableLogicalInsert = 1,
        KeyTableLogicalDelete = 2,
        KeyTableLogicalClear  = 3
    };

    /// \brief Logical adapter for typed \c KeyTable insert/delete/clear ops.
    /// \details The adapter reuses the explicit key codec tags used by
    /// \c KeyValueTableLogicalAdapter. It is an apply-side helper only; local
    /// capture sessions for key-only tables can be layered on top later.
    template<class KeyT,
             class KeyCodec,
             class Options = DefaultTableOptions>
    class KeyTableLogicalAdapter : public ILogicalTableAdapter {
    public:
        typedef KeyTable<KeyT, Options> table_type;

        static_assert(std::is_same<typename KeyCodec::value_type,
                                   KeyT>::value,
                      "KeyTableLogicalAdapter key codec local type must match KeyT");

        KeyTableLogicalAdapter(table_type& table,
                               const std::string& schema_id,
                               std::uint32_t schema_version = 1)
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
            if (m_schema_version == 0) {
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
            change.opcode = KeyTableLogicalInsert;
            encode_key_only(key, change.payload);
            return change;
        }

        LogicalChange make_delete(const KeyT& key) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = KeyTableLogicalDelete;
            encode_key_only(key, change.payload);
            return change;
        }

        LogicalChange make_clear() const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = KeyTableLogicalClear;
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
                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_table.connection(), txn);
                if (change.opcode == KeyTableLogicalInsert) {
                    const KeyT key = decode_key_only(change.payload);
                    (void)m_table.insert(key, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyTableLogicalDelete) {
                    const KeyT key = decode_key_only(change.payload);
                    (void)m_table.erase(key, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyTableLogicalClear) {
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
        struct PayloadCursor {
            const std::uint8_t* data;
            std::size_t size;
            std::size_t pos;
        };

        static void require(PayloadCursor& cursor, std::size_t size) {
            if (cursor.pos > cursor.size ||
                size > cursor.size - cursor.pos) {
                throw std::runtime_error(
                    "KeyTable logical payload underrun");
            }
        }

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::vector<std::uint8_t>& value) {
            if (value.size() >
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max())) {
                throw std::length_error(
                    "KeyTable logical payload blob is too large");
            }
            detail::append_u32_le(out,
                static_cast<std::uint32_t>(value.size()));
            out.insert(out.end(), value.begin(), value.end());
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
                    "KeyTable logical payload has trailing bytes");
            }
        }

        static void encode_key_only(const KeyT& key,
                                    std::vector<std::uint8_t>& out) {
            out.clear();
            const std::vector<std::uint8_t> encoded_key =
                KeyCodec::encode(key);
            append_blob(out, encoded_key);
        }

        static KeyT decode_key_only(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const std::vector<std::uint8_t> encoded_key = read_blob(cursor);
            ensure_end(cursor);
            return KeyCodec::decode(encoded_key);
        }

        LogicalApplyResult validate_payload(
                const LogicalChange& change) const {
            try {
                if (change.opcode == KeyTableLogicalInsert ||
                    change.opcode == KeyTableLogicalDelete) {
                    (void)decode_key_only(change.payload);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyTableLogicalClear) {
                    if (!change.payload.empty()) {
                        return LogicalApplyResult::failure(
                            "KeyTable clear payload must be empty");
                    }
                    return LogicalApplyResult::success();
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyTable logical payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyTable logical payload is invalid");
            }
            return LogicalApplyResult::failure(
                "KeyTable logical adapter opcode is unsupported");
        }

        table_type& m_table;
        std::string m_schema_id;
        std::uint32_t m_schema_version;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
