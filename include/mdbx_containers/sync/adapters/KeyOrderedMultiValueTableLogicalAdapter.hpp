#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file KeyOrderedMultiValueTableLogicalAdapter.hpp
/// \brief Ordered logical adapter for append-only multi-value tables.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "../../KeyOrderedMultiValueTable.hpp"
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
