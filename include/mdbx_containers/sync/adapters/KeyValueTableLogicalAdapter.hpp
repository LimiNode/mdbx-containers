#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file KeyValueTableLogicalAdapter.hpp
/// \brief Minimal logical adapter for \c KeyValueTable.

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
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

namespace detail {

    template<class T, class Enable = void>
    struct KeyValueLogicalCodec;

    template<class T>
    struct KeyValueLogicalCodec<
        T,
        typename std::enable_if<
            std::is_integral<T>::value && !std::is_same<T, bool>::value
        >::type> {
        static std::vector<std::uint8_t> encode(T value) {
            static_assert(sizeof(T) <= 8,
                          "KeyValue logical integral codec supports up to 64 bits");
            std::uint64_t raw = 0;
            if (std::numeric_limits<T>::is_signed) {
                const std::int64_t signed_value =
                    static_cast<std::int64_t>(value);
                raw = static_cast<std::uint64_t>(signed_value);
            } else {
                raw = static_cast<std::uint64_t>(value);
            }
            std::vector<std::uint8_t> out(8);
            for (int i = 7; i >= 0; --i) {
                out[static_cast<std::size_t>(i)] =
                    static_cast<std::uint8_t>(raw & 0xFFu);
                raw >>= 8;
            }
            return out;
        }

        static T decode(const std::vector<std::uint8_t>& bytes) {
            if (bytes.size() != 8u) {
                throw std::runtime_error(
                    "KeyValue logical integral payload must be 8 bytes");
            }
            std::uint64_t raw = 0;
            for (std::size_t i = 0; i < bytes.size(); ++i) {
                raw = (raw << 8) | bytes[i];
            }
            if (std::numeric_limits<T>::is_signed) {
                std::int64_t signed_value = 0;
                if (raw <= static_cast<std::uint64_t>(
                        std::numeric_limits<std::int64_t>::max())) {
                    signed_value = static_cast<std::int64_t>(raw);
                } else {
                    const std::uint64_t magnitude = (~raw) + 1u;
                    const std::uint64_t min_magnitude =
                        static_cast<std::uint64_t>(
                            std::numeric_limits<std::int64_t>::max()) + 1u;
                    if (magnitude == min_magnitude) {
                        signed_value =
                            (std::numeric_limits<std::int64_t>::min)();
                    } else {
                        signed_value =
                            -static_cast<std::int64_t>(magnitude);
                    }
                }
                if (signed_value <
                        static_cast<std::int64_t>(
                            (std::numeric_limits<T>::min)()) ||
                    signed_value >
                        static_cast<std::int64_t>(
                            (std::numeric_limits<T>::max)())) {
                    throw std::out_of_range(
                        "KeyValue logical integral payload is out of range");
                }
                return static_cast<T>(signed_value);
            }
            if (raw > static_cast<std::uint64_t>(
                    (std::numeric_limits<T>::max)())) {
                throw std::out_of_range(
                    "KeyValue logical unsigned payload is out of range");
            }
            return static_cast<T>(raw);
        }
    };

    template<>
    struct KeyValueLogicalCodec<bool, void> {
        static std::vector<std::uint8_t> encode(bool value) {
            std::vector<std::uint8_t> out(1);
            out[0] = value ? 1u : 0u;
            return out;
        }

        static bool decode(const std::vector<std::uint8_t>& bytes) {
            if (bytes.size() != 1u || bytes[0] > 1u) {
                throw std::runtime_error(
                    "KeyValue logical bool payload is invalid");
            }
            return bytes[0] != 0u;
        }
    };

    template<>
    struct KeyValueLogicalCodec<std::string, void> {
        static std::vector<std::uint8_t> encode(const std::string& value) {
            return std::vector<std::uint8_t>(value.begin(), value.end());
        }

        static std::string decode(const std::vector<std::uint8_t>& bytes) {
            return std::string(bytes.begin(), bytes.end());
        }
    };

} // namespace detail

    /// \brief First concrete logical adapter for simple one-value-per-key tables.
    /// \details The payload format is adapter-owned and intentionally separate
    /// from the raw DBI wire codec. The initial stable codec supports
    /// \c std::string, \c bool, and integral types up to 64 bits. Incoming
    /// logical apply suppresses local raw capture for the supplied transaction.
    /// It is used only when a caller explicitly invokes
    /// \c LogicalTableRegistry::preflight_then_apply().
    template<class KeyT, class ValueT, class Options = DefaultTableOptions>
    class KeyValueTableLogicalAdapter : public ILogicalTableAdapter {
    public:
        typedef KeyValueTable<KeyT, ValueT, Options> table_type;

        KeyValueTableLogicalAdapter(table_type& table,
                                    const std::string& schema_id,
                                    std::uint32_t schema_version = 1)
            : m_table(table),
              m_schema_id(schema_id),
              m_schema_version(schema_version) {
            if (m_schema_id.empty()) {
                throw std::invalid_argument(
                    "KeyValueTableLogicalAdapter schema id is empty");
            }
            if (m_table.dbi_name().empty()) {
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
            out.push_back(m_table.dbi_name());
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
                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_table.connection(), txn);
                if (change.opcode == KeyValueLogicalUpsert) {
                    const std::pair<KeyT, ValueT> decoded =
                        decode_upsert(change.payload);
                    m_table.insert_or_assign(
                        decoded.first, decoded.second, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyValueLogicalDelete) {
                    const KeyT key = decode_key_only(change.payload);
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

        static void append_blob(
                std::vector<std::uint8_t>& out,
                const std::vector<std::uint8_t>& value) {
            if (value.size() >
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max())) {
                throw std::length_error(
                    "KeyValue logical payload blob is too large");
            }
            detail::append_u32_le(out,
                static_cast<std::uint32_t>(value.size()));
            out.insert(out.end(), value.begin(), value.end());
        }

        static std::vector<std::uint8_t> read_blob(PayloadCursor& cursor) {
            require(cursor, 4);
            const std::uint32_t size =
                detail::read_u32_le(cursor.data + cursor.pos);
            cursor.pos += 4;
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
            out.clear();
            const std::vector<std::uint8_t> encoded_key =
                detail::KeyValueLogicalCodec<KeyT>::encode(key);
            append_blob(out, encoded_key);
        }

        static void encode_upsert(const KeyT& key,
                                  const ValueT& value,
                                  std::vector<std::uint8_t>& out) {
            out.clear();
            const std::vector<std::uint8_t> encoded_key =
                detail::KeyValueLogicalCodec<KeyT>::encode(key);
            const std::vector<std::uint8_t> encoded_value =
                detail::KeyValueLogicalCodec<ValueT>::encode(value);
            append_blob(out, encoded_key);
            append_blob(out, encoded_value);
        }

        static KeyT decode_key_only(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const std::vector<std::uint8_t> encoded_key = read_blob(cursor);
            ensure_end(cursor);
            return detail::KeyValueLogicalCodec<KeyT>::decode(encoded_key);
        }

        static std::pair<KeyT, ValueT> decode_upsert(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const std::vector<std::uint8_t> encoded_key = read_blob(cursor);
            const std::vector<std::uint8_t> encoded_value = read_blob(cursor);
            ensure_end(cursor);
            return std::make_pair(
                detail::KeyValueLogicalCodec<KeyT>::decode(encoded_key),
                detail::KeyValueLogicalCodec<ValueT>::decode(encoded_value));
        }

        LogicalApplyResult validate_payload(
                const LogicalChange& change) const {
            try {
                if (change.opcode == KeyValueLogicalUpsert) {
                    (void)decode_upsert(change.payload);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyValueLogicalDelete) {
                    (void)decode_key_only(change.payload);
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
        std::uint32_t m_schema_version;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
