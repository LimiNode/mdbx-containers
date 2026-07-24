#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_STORES_SCHEMA_REGISTRY_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_STORES_SCHEMA_REGISTRY_STORE_HPP_INCLUDED

/// \file SchemaRegistryStore.hpp
/// \brief Persistent logical sync schema registry.

#include <cstdint>
#include <cstring>
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>

#include "../../common/MdbxException.hpp"
#include "../../detail/utils.hpp"
#include "../common.hpp"
#include "../LogicalSchema.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Persistent description of one logical sync schema.
    struct LogicalSchemaRecord {
        std::string dbi_name;                 ///< Primary logical DBI name.
        LogicalTableKind kind = LogicalTableKind::Unknown; ///< Logical adapter kind.
        std::uint32_t schema_version = 0;     ///< Application schema version.
        std::uint32_t flags = 0;              ///< Reserved flags; must be zero for now.
        std::vector<std::string> dbi_names;   ///< All physical DBIs owned by the schema.
    };

    /// \brief Thin wrapper around \c _mdbxc_sync_schema.
    /// \details Key = logical schema id string bytes. Value =
    /// \c LogicalSchemaRecord with length-prefixed UTF-8 strings.
    class SchemaRegistryStore {
    public:
        explicit SchemaRegistryStore(
                MDBX_env* env,
                const std::string& dbi_name = "_mdbxc_sync_schema")
            : m_env(env), m_dbi_name(dbi_name), m_dbi(0), m_open(false) {}

        /// \brief Opens the registry DBI inside the supplied transaction.
        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "SchemaRegistryStore::open");
            open_checked(txn);
        }

        bool is_open() const { return m_open; }
        MDBX_dbi handle(MDBX_txn* txn) const {
            txn = checked_txn(txn, "SchemaRegistryStore::handle");
            open_checked(txn);
            return m_dbi;
        }

        void reset_open() { m_open = false; }

        void ensure_open() const {
            if (!m_open) {
                throw std::logic_error("SchemaRegistryStore is not open");
            }
        }

        /// \brief Stores a new schema record or verifies an existing one.
        /// \throws std::invalid_argument when \p schema_id or record fields
        /// are structurally invalid.
        /// \throws std::runtime_error when an existing record differs.
        void register_or_verify(MDBX_txn* txn,
                                const std::string& schema_id,
                                const LogicalSchemaRecord& record) {
            txn = checked_txn(txn, "SchemaRegistryStore::register_or_verify");
            validate_record(schema_id, record);
            open(txn);

            LogicalSchemaRecord existing;
            if (get(txn, schema_id, existing)) {
                if (!records_equal(existing, record)) {
                    throw std::runtime_error("Logical sync schema registry mismatch");
                }
                return;
            }

            std::vector<std::uint8_t> value;
            encode_record(schema_id, record, value);
            MDBX_val key = { const_cast<char*>(schema_id.data()), schema_id.size() };
            MDBX_val val = { value.empty() ? nullptr : &value[0], value.size() };
            check_mdbx(mdbx_put(txn, m_dbi, &key, &val, MDBX_NOOVERWRITE),
                       "SchemaRegistryStore put failed");
        }

        /// \brief Reads a schema record.
        /// \return true when present, false when absent.
        bool get(MDBX_txn* txn,
                 const std::string& schema_id,
                 LogicalSchemaRecord& out) const {
            txn = checked_txn(txn, "SchemaRegistryStore::get");
            open_const(txn);
            MDBX_val key = { const_cast<char*>(schema_id.data()), schema_id.size() };
            MDBX_val val;
            const int rc = mdbx_get(txn, m_dbi, &key, &val);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "SchemaRegistryStore get failed");
            LogicalSchemaRecord decoded;
            decode_record(val, schema_id, decoded);
            out = decoded;
            return true;
        }

        /// \brief Returns all registered schema ids in MDBX key order.
        std::vector<std::string> schema_ids(MDBX_txn* txn) const {
            txn = checked_txn(txn, "SchemaRegistryStore::schema_ids");
            open_const(txn);
            std::vector<std::string> out;
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "SchemaRegistryStore cursor open failed");
            try {
                MDBX_val key;
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    const char* data = static_cast<const char*>(key.iov_base);
                    const std::string schema_id(data, data + key.iov_len);
                    LogicalSchemaRecord decoded;
                    decode_record(value, schema_id, decoded);
                    out.push_back(schema_id);
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "SchemaRegistryStore cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            return out;
        }

    private:
        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        void open_const(MDBX_txn* txn) const {
            txn = checked_txn(txn, "SchemaRegistryStore::open");
            open_checked(txn);
        }

        void open_checked(MDBX_txn* txn) const {
            int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE, &m_dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0), &m_dbi);
            }
            check_mdbx(rc, "Failed to open SchemaRegistryStore DBI");
            m_open = true;
        }

        static void validate_record(const std::string& schema_id,
                                    const LogicalSchemaRecord& record) {
            if (schema_id.empty()) {
                throw std::invalid_argument("Logical schema id must not be empty");
            }
            if (record.dbi_name.empty()) {
                throw std::invalid_argument("Logical schema DBI name must not be empty");
            }
            for (std::size_t i = 0; i < record.dbi_names.size(); ++i) {
                if (record.dbi_names[i].empty()) {
                    throw std::invalid_argument(
                        "Logical schema owned DBI name must not be empty");
                }
            }
            if (!is_known_logical_table_kind(record.kind)) {
                throw std::invalid_argument("Logical schema kind must be known");
            }
            if (record.schema_version == 0) {
                throw std::invalid_argument("Logical schema version must be non-zero");
            }
            if (record.flags != 0) {
                throw std::invalid_argument("Logical schema flags are reserved");
            }
        }

        static bool records_equal(const LogicalSchemaRecord& a,
                                  const LogicalSchemaRecord& b) {
            const LogicalSchemaRecord ca = canonical_record(a);
            const LogicalSchemaRecord cb = canonical_record(b);
            return ca.dbi_name == cb.dbi_name &&
                   ca.kind == cb.kind &&
                   ca.schema_version == cb.schema_version &&
                   ca.flags == cb.flags &&
                   ca.dbi_names == cb.dbi_names;
        }

        static void append_string(std::vector<std::uint8_t>& out,
                                  const std::string& value) {
            if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
                throw std::length_error("Logical schema string is too large");
            }
            detail::append_u32_le(out, static_cast<std::uint32_t>(value.size()));
            out.insert(out.end(), value.begin(), value.end());
        }

        static std::string read_string(const std::uint8_t* data,
                                       std::size_t total,
                                       std::size_t& pos) {
            require(data, total, pos, 4);
            const std::uint32_t size = detail::read_u32_le(data + pos);
            pos += 4;
            require(data, total, pos, size);
            const char* begin = reinterpret_cast<const char*>(data + pos);
            pos += size;
            return std::string(begin, begin + size);
        }

        static void require(const std::uint8_t*,
                            std::size_t total,
                            std::size_t pos,
                            std::size_t size) {
            if (pos > total || size > total - pos) {
                throw std::runtime_error("SchemaRegistryStore decode underrun");
            }
        }

        static void encode_record(const std::string& schema_id,
                                  const LogicalSchemaRecord& record,
                                  std::vector<std::uint8_t>& out) {
            LogicalSchemaRecord canonical = canonical_record(record);
            if (canonical.dbi_names.size() >
                std::numeric_limits<std::uint32_t>::max()) {
                throw std::length_error("Logical schema DBI list is too large");
            }
            validate_record(schema_id, canonical);
            out.clear();
            append_string(out, schema_id);
            detail::append_u16_le(out, static_cast<std::uint16_t>(canonical.kind));
            detail::append_u32_le(out, canonical.schema_version);
            detail::append_u32_le(out, canonical.flags);
            append_string(out, canonical.dbi_name);
            detail::append_u32_le(out,
                static_cast<std::uint32_t>(canonical.dbi_names.size()));
            for (std::size_t i = 0; i < canonical.dbi_names.size(); ++i) {
                append_string(out, canonical.dbi_names[i]);
            }
        }

        static void decode_record(const MDBX_val& value,
                                  const std::string& schema_id,
                                  LogicalSchemaRecord& out) {
            const std::uint8_t* data =
                static_cast<const std::uint8_t*>(value.iov_base);
            std::size_t pos = 0;
            const std::size_t total = value.iov_len;
            LogicalSchemaRecord decoded;
            const std::string embedded_schema_id =
                read_string(data, total, pos);
            if (embedded_schema_id != schema_id) {
                throw std::runtime_error(
                    "SchemaRegistryStore schema id mismatch");
            }
            require(data, total, pos, 2);
            decoded.kind =
                static_cast<LogicalTableKind>(detail::read_u16_le(data + pos));
            pos += 2;
            require(data, total, pos, 4);
            decoded.schema_version = detail::read_u32_le(data + pos);
            pos += 4;
            require(data, total, pos, 4);
            decoded.flags = detail::read_u32_le(data + pos);
            pos += 4;
            decoded.dbi_name = read_string(data, total, pos);
            require(data, total, pos, 4);
            const std::uint32_t count = detail::read_u32_le(data + pos);
            pos += 4;
            if (count > (total - pos) / 4u) {
                throw std::runtime_error(
                    "SchemaRegistryStore DBI list count exceeds payload bounds");
            }
            decoded.dbi_names.reserve(count);
            for (std::uint32_t i = 0; i < count; ++i) {
                decoded.dbi_names.push_back(read_string(data, total, pos));
            }
            if (pos != total) {
                throw std::runtime_error("SchemaRegistryStore trailing bytes");
            }
            validate_record(schema_id, decoded);
            out = canonical_record(decoded);
        }

        static LogicalSchemaRecord canonical_record(
                const LogicalSchemaRecord& record) {
            LogicalSchemaRecord out = record;
            std::sort(out.dbi_names.begin(), out.dbi_names.end());
            if (std::unique(out.dbi_names.begin(), out.dbi_names.end()) !=
                out.dbi_names.end()) {
                throw std::invalid_argument(
                    "Logical schema owned DBI names must be unique");
            }
            return out;
        }

        MDBX_env*   m_env;
        std::string m_dbi_name;
        mutable MDBX_dbi m_dbi;
        mutable bool     m_open;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_STORES_SCHEMA_REGISTRY_STORE_HPP_INCLUDED
