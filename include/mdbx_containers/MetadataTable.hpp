#pragma once
#ifndef MDBX_CONTAINERS_HEADER_METADATA_TABLE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_METADATA_TABLE_HPP_INCLUDED

/// \file MetadataTable.hpp
/// \brief Typed key-value metadata stored in one MDBX DBI.

#include "common.hpp"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace mdbxc {

    /// \class MetadataTable
    /// \ingroup mdbxc_tables
    /// \brief Persistent typed metadata for a collection or component.
    /// \details
    /// Supports string, uint32, uint64, int64, double, and bool values. Each
    /// value stores a compact type tag before its serialized payload.
    ///
    /// Typed getters throw when a key is absent or has a different stored type.
    /// The get_*_or() family returns its fallback only for an absent key; it
    /// does not hide a type mismatch or malformed stored value. All operations
    /// accept either an optional raw transaction handle or a Transaction wrapper.
    class MetadataTable final : public BaseTable {
    public:
        /// \brief Constructs metadata storage using an existing connection.
        /// \param connection Existing connection.
        /// \param name Name of the metadata DBI.
        /// \param flags MDBX DBI flags.
        explicit MetadataTable(
            std::shared_ptr<Connection> connection,
            std::string name = "metadata",
            MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(std::move(connection), std::move(name), flags) {}

        /// \brief Constructs metadata storage using configuration settings.
        /// \param config Connection configuration settings.
        /// \param name Name of the metadata DBI.
        /// \param flags MDBX DBI flags.
        explicit MetadataTable(
            const Config& config,
            std::string name = "metadata",
            MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(Connection::create(config), std::move(name), flags) {}

        /// \brief Destroys the metadata table wrapper.
        ~MetadataTable() override = default;

        /// \brief Stores a string metadata value.
        /// \param key Metadata key.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        void set_string(const std::string& key, const std::string& value,
                        MDBX_txn* txn = nullptr) {
            set_value(key, ValueType::String, value, txn);
        }

        /// \brief Stores a string metadata value using an external transaction.
        /// \param key Metadata key.
        /// \param value Value to store.
        /// \param txn Active transaction wrapper.
        void set_string(const std::string& key, const std::string& value,
                        const Transaction& txn) {
            set_string(key, value, txn.handle());
        }

        /// \brief Stores an unsigned 32-bit metadata value.
        /// \param key Metadata key.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        void set_uint32(const std::string& key, std::uint32_t value,
                        MDBX_txn* txn = nullptr) {
            set_value(key, ValueType::Uint32, value, txn);
        }

        /// \brief Stores an unsigned 32-bit metadata value using an external transaction.
        /// \param key Metadata key.
        /// \param value Value to store.
        /// \param txn Active transaction wrapper.
        void set_uint32(const std::string& key, std::uint32_t value,
                        const Transaction& txn) {
            set_uint32(key, value, txn.handle());
        }

        /// \brief Stores an unsigned 64-bit metadata value.
        /// \param key Metadata key.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        void set_uint64(const std::string& key, std::uint64_t value,
                        MDBX_txn* txn = nullptr) {
            set_value(key, ValueType::Uint64, value, txn);
        }

        /// \brief Stores an unsigned 64-bit metadata value using an external transaction.
        /// \param key Metadata key.
        /// \param value Value to store.
        /// \param txn Active transaction wrapper.
        void set_uint64(const std::string& key, std::uint64_t value,
                        const Transaction& txn) {
            set_uint64(key, value, txn.handle());
        }

        /// \brief Stores a signed 64-bit metadata value.
        /// \param key Metadata key.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        void set_int64(const std::string& key, std::int64_t value,
                       MDBX_txn* txn = nullptr) {
            set_value(key, ValueType::Int64, value, txn);
        }

        /// \brief Stores a signed 64-bit metadata value using an external transaction.
        /// \param key Metadata key.
        /// \param value Value to store.
        /// \param txn Active transaction wrapper.
        void set_int64(const std::string& key, std::int64_t value,
                       const Transaction& txn) {
            set_int64(key, value, txn.handle());
        }

        /// \brief Stores a double metadata value.
        /// \param key Metadata key.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        void set_double(const std::string& key, double value,
                        MDBX_txn* txn = nullptr) {
            set_value(key, ValueType::Double, value, txn);
        }

        /// \brief Stores a double metadata value using an external transaction.
        /// \param key Metadata key.
        /// \param value Value to store.
        /// \param txn Active transaction wrapper.
        void set_double(const std::string& key, double value,
                        const Transaction& txn) {
            set_double(key, value, txn.handle());
        }

        /// \brief Stores a boolean metadata value.
        /// \param key Metadata key.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        void set_bool(const std::string& key, bool value,
                      MDBX_txn* txn = nullptr) {
            set_value(key, ValueType::Bool, value, txn);
        }

        /// \brief Stores a boolean metadata value using an external transaction.
        /// \param key Metadata key.
        /// \param value Value to store.
        /// \param txn Active transaction wrapper.
        void set_bool(const std::string& key, bool value,
                      const Transaction& txn) {
            set_bool(key, value, txn.handle());
        }

        /// \brief Returns a stored string metadata value.
        /// \param key Metadata key.
        /// \param txn Optional transaction handle.
        /// \return Stored value.
        /// \throws std::out_of_range if the key is absent.
        /// \throws std::invalid_argument if the stored value has another type.
        std::string get_string(const std::string& key,
                               MDBX_txn* txn = nullptr) const {
            return get_required<std::string>(key, ValueType::String, txn);
        }

        /// \brief Returns a stored string metadata value using an external transaction.
        /// \param key Metadata key.
        /// \param txn Active transaction wrapper.
        /// \return Stored value.
        /// \throws std::out_of_range if the key is absent.
        /// \throws std::invalid_argument if the stored value has another type.
        std::string get_string(const std::string& key,
                               const Transaction& txn) const {
            return get_string(key, txn.handle());
        }

        /// \brief Returns a stored unsigned 32-bit metadata value.
        /// \param key Metadata key.
        /// \param txn Optional transaction handle.
        /// \return Stored value.
        /// \throws std::out_of_range if the key is absent.
        /// \throws std::invalid_argument if the stored value has another type.
        std::uint32_t get_uint32(const std::string& key,
                                 MDBX_txn* txn = nullptr) const {
            return get_required<std::uint32_t>(key, ValueType::Uint32, txn);
        }

        /// \brief Returns a stored unsigned 32-bit metadata value using an external transaction.
        /// \param key Metadata key.
        /// \param txn Active transaction wrapper.
        /// \return Stored value.
        /// \throws std::out_of_range if the key is absent.
        /// \throws std::invalid_argument if the stored value has another type.
        std::uint32_t get_uint32(const std::string& key,
                                 const Transaction& txn) const {
            return get_uint32(key, txn.handle());
        }

        /// \brief Returns a stored unsigned 64-bit metadata value.
        /// \param key Metadata key.
        /// \param txn Optional transaction handle.
        /// \return Stored value.
        /// \throws std::out_of_range if the key is absent.
        /// \throws std::invalid_argument if the stored value has another type.
        std::uint64_t get_uint64(const std::string& key,
                                 MDBX_txn* txn = nullptr) const {
            return get_required<std::uint64_t>(key, ValueType::Uint64, txn);
        }

        /// \brief Returns a stored unsigned 64-bit metadata value using an external transaction.
        /// \param key Metadata key.
        /// \param txn Active transaction wrapper.
        /// \return Stored value.
        /// \throws std::out_of_range if the key is absent.
        /// \throws std::invalid_argument if the stored value has another type.
        std::uint64_t get_uint64(const std::string& key,
                                 const Transaction& txn) const {
            return get_uint64(key, txn.handle());
        }

        /// \brief Returns a stored signed 64-bit metadata value.
        /// \param key Metadata key.
        /// \param txn Optional transaction handle.
        /// \return Stored value.
        /// \throws std::out_of_range if the key is absent.
        /// \throws std::invalid_argument if the stored value has another type.
        std::int64_t get_int64(const std::string& key,
                               MDBX_txn* txn = nullptr) const {
            return get_required<std::int64_t>(key, ValueType::Int64, txn);
        }

        /// \brief Returns a stored signed 64-bit metadata value using an external transaction.
        /// \param key Metadata key.
        /// \param txn Active transaction wrapper.
        /// \return Stored value.
        /// \throws std::out_of_range if the key is absent.
        /// \throws std::invalid_argument if the stored value has another type.
        std::int64_t get_int64(const std::string& key,
                               const Transaction& txn) const {
            return get_int64(key, txn.handle());
        }

        /// \brief Returns a stored double metadata value.
        /// \param key Metadata key.
        /// \param txn Optional transaction handle.
        /// \return Stored value.
        /// \throws std::out_of_range if the key is absent.
        /// \throws std::invalid_argument if the stored value has another type.
        double get_double(const std::string& key,
                          MDBX_txn* txn = nullptr) const {
            return get_required<double>(key, ValueType::Double, txn);
        }

        /// \brief Returns a stored double metadata value using an external transaction.
        /// \param key Metadata key.
        /// \param txn Active transaction wrapper.
        /// \return Stored value.
        /// \throws std::out_of_range if the key is absent.
        /// \throws std::invalid_argument if the stored value has another type.
        double get_double(const std::string& key,
                          const Transaction& txn) const {
            return get_double(key, txn.handle());
        }

        /// \brief Returns a stored boolean metadata value.
        /// \param key Metadata key.
        /// \param txn Optional transaction handle.
        /// \return Stored value.
        /// \throws std::out_of_range if the key is absent.
        /// \throws std::invalid_argument if the stored value has another type.
        bool get_bool(const std::string& key,
                      MDBX_txn* txn = nullptr) const {
            return get_required<bool>(key, ValueType::Bool, txn);
        }

        /// \brief Returns a stored boolean metadata value using an external transaction.
        /// \param key Metadata key.
        /// \param txn Active transaction wrapper.
        /// \return Stored value.
        /// \throws std::out_of_range if the key is absent.
        /// \throws std::invalid_argument if the stored value has another type.
        bool get_bool(const std::string& key,
                      const Transaction& txn) const {
            return get_bool(key, txn.handle());
        }

        /// \brief Returns a string metadata value or a fallback when the key is absent.
        /// \param key Metadata key.
        /// \param fallback Value returned when the key is absent.
        /// \param txn Optional transaction handle.
        /// \return Stored value or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        std::string get_string_or(const std::string& key,
                                  std::string fallback,
                                  MDBX_txn* txn = nullptr) const {
            return get_or<std::string>(key, ValueType::String,
                                       std::move(fallback), txn);
        }

        /// \brief Returns a string metadata value or a fallback using an external transaction.
        /// \param key Metadata key.
        /// \param fallback Value returned when the key is absent.
        /// \param txn Active transaction wrapper.
        /// \return Stored value or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        std::string get_string_or(const std::string& key,
                                  std::string fallback,
                                  const Transaction& txn) const {
            return get_string_or(key, std::move(fallback), txn.handle());
        }

        /// \brief Returns an unsigned 32-bit metadata value or a fallback when absent.
        /// \param key Metadata key.
        /// \param fallback Value returned when the key is absent.
        /// \param txn Optional transaction handle.
        /// \return Stored value or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        std::uint32_t get_uint32_or(const std::string& key, std::uint32_t fallback,
                                    MDBX_txn* txn = nullptr) const {
            return get_or<std::uint32_t>(key, ValueType::Uint32, fallback, txn);
        }

        /// \brief Returns an unsigned 32-bit metadata value or a fallback using an external transaction.
        /// \param key Metadata key.
        /// \param fallback Value returned when the key is absent.
        /// \param txn Active transaction wrapper.
        /// \return Stored value or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        std::uint32_t get_uint32_or(const std::string& key, std::uint32_t fallback,
                                    const Transaction& txn) const {
            return get_uint32_or(key, fallback, txn.handle());
        }

        /// \brief Returns an unsigned 64-bit metadata value or a fallback when absent.
        /// \param key Metadata key.
        /// \param fallback Value returned when the key is absent.
        /// \param txn Optional transaction handle.
        /// \return Stored value or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        std::uint64_t get_uint64_or(const std::string& key, std::uint64_t fallback,
                                    MDBX_txn* txn = nullptr) const {
            return get_or<std::uint64_t>(key, ValueType::Uint64, fallback, txn);
        }

        /// \brief Returns an unsigned 64-bit metadata value or a fallback using an external transaction.
        /// \param key Metadata key.
        /// \param fallback Value returned when the key is absent.
        /// \param txn Active transaction wrapper.
        /// \return Stored value or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        std::uint64_t get_uint64_or(const std::string& key, std::uint64_t fallback,
                                    const Transaction& txn) const {
            return get_uint64_or(key, fallback, txn.handle());
        }

        /// \brief Returns a signed 64-bit metadata value or a fallback when absent.
        /// \param key Metadata key.
        /// \param fallback Value returned when the key is absent.
        /// \param txn Optional transaction handle.
        /// \return Stored value or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        std::int64_t get_int64_or(const std::string& key, std::int64_t fallback,
                                  MDBX_txn* txn = nullptr) const {
            return get_or<std::int64_t>(key, ValueType::Int64, fallback, txn);
        }

        /// \brief Returns a signed 64-bit metadata value or a fallback using an external transaction.
        /// \param key Metadata key.
        /// \param fallback Value returned when the key is absent.
        /// \param txn Active transaction wrapper.
        /// \return Stored value or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        std::int64_t get_int64_or(const std::string& key, std::int64_t fallback,
                                  const Transaction& txn) const {
            return get_int64_or(key, fallback, txn.handle());
        }

        /// \brief Returns a double metadata value or a fallback when the key is absent.
        /// \param key Metadata key.
        /// \param fallback Value returned when the key is absent.
        /// \param txn Optional transaction handle.
        /// \return Stored value or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        double get_double_or(const std::string& key, double fallback,
                             MDBX_txn* txn = nullptr) const {
            return get_or<double>(key, ValueType::Double, fallback, txn);
        }

        /// \brief Returns a double metadata value or a fallback using an external transaction.
        /// \param key Metadata key.
        /// \param fallback Value returned when the key is absent.
        /// \param txn Active transaction wrapper.
        /// \return Stored value or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        double get_double_or(const std::string& key, double fallback,
                             const Transaction& txn) const {
            return get_double_or(key, fallback, txn.handle());
        }

        /// \brief Returns a boolean metadata value or a fallback when the key is absent.
        /// \param key Metadata key.
        /// \param fallback Value returned when the key is absent.
        /// \param txn Optional transaction handle.
        /// \return Stored value or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        bool get_bool_or(const std::string& key, bool fallback,
                         MDBX_txn* txn = nullptr) const {
            return get_or<bool>(key, ValueType::Bool, fallback, txn);
        }

        /// \brief Returns a boolean metadata value or a fallback using an external transaction.
        /// \param key Metadata key.
        /// \param fallback Value returned when the key is absent.
        /// \param txn Active transaction wrapper.
        /// \return Stored value or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        bool get_bool_or(const std::string& key, bool fallback,
                         const Transaction& txn) const {
            return get_bool_or(key, fallback, txn.handle());
        }

        /// \brief Stores the collection schema version under the fixed key.
        /// \details The \c schema_version key is reserved by the schema-version
        /// helpers and must not be used for unrelated metadata.
        /// \param value Schema version to store.
        /// \param txn Optional transaction handle.
        void set_schema_version(std::uint32_t value, MDBX_txn* txn = nullptr) {
            set_uint32(schema_version_key(), value, txn);
        }

        /// \brief Stores the collection schema version using an external transaction.
        /// \param value Schema version to store.
        /// \param txn Active transaction wrapper.
        void set_schema_version(std::uint32_t value, const Transaction& txn) {
            set_schema_version(value, txn.handle());
        }

        /// \brief Returns the schema version or throws if it has not been set.
        /// \param txn Optional transaction handle.
        /// \return Stored schema version.
        /// \throws std::out_of_range if no schema version has been stored.
        /// \throws std::invalid_argument if the stored value has another type.
        std::uint32_t schema_version(MDBX_txn* txn = nullptr) const {
            return get_uint32(schema_version_key(), txn);
        }

        /// \brief Returns the schema version using an external transaction.
        /// \param txn Active transaction wrapper.
        /// \return Stored schema version.
        /// \throws std::out_of_range if no schema version has been stored.
        /// \throws std::invalid_argument if the stored value has another type.
        std::uint32_t schema_version(const Transaction& txn) const {
            return schema_version(txn.handle());
        }

        /// \brief Returns the schema version or a caller-supplied fallback.
        /// \param fallback Value returned when no schema version has been stored.
        /// \param txn Optional transaction handle.
        /// \return Stored schema version or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        std::uint32_t schema_version_or(std::uint32_t fallback,
                                        MDBX_txn* txn = nullptr) const {
            return get_uint32_or(schema_version_key(), fallback, txn);
        }

        /// \brief Returns the schema version or a fallback using an external transaction.
        /// \param fallback Value returned when no schema version has been stored.
        /// \param txn Active transaction wrapper.
        /// \return Stored schema version or the fallback.
        /// \throws std::invalid_argument if the stored value has another type.
        std::uint32_t schema_version_or(std::uint32_t fallback,
                                        const Transaction& txn) const {
            return schema_version_or(fallback, txn.handle());
        }

        /// \brief Removes a metadata key.
        /// \param key Metadata key.
        /// \param txn Optional transaction handle.
        /// \return True when a stored value was removed.
        bool erase(const std::string& key, MDBX_txn* txn = nullptr) {
            bool removed = false;
            with_transaction([this, &key, &removed](MDBX_txn* t) {
                removed = db_erase(key, t);
            }, TransactionMode::WRITABLE, txn);
            return removed;
        }

        /// \brief Removes a metadata key using an external transaction.
        /// \param key Metadata key.
        /// \param txn Active transaction wrapper.
        /// \return True when a stored value was removed.
        bool erase(const std::string& key, const Transaction& txn) {
            return erase(key, txn.handle());
        }

    private:
        enum class ValueType : std::uint8_t {
            String = 1u,
            Uint32 = 2u,
            Uint64 = 3u,
            Int64 = 4u,
            Double = 5u,
            Bool = 6u
        };

        static const char* schema_version_key() {
            return "schema_version";
        }

        template<class ValueT>
        void set_value(const std::string& key, ValueType type, const ValueT& value,
                       MDBX_txn* txn) {
            with_transaction([this, &key, type, &value](MDBX_txn* t) {
                db_set(key, type, value, t);
            }, TransactionMode::WRITABLE, txn);
        }

        template<class ValueT>
        ValueT get_required(const std::string& key, ValueType type,
                            MDBX_txn* txn) const {
            ValueT value;
            with_transaction([this, &key, type, &value](MDBX_txn* t) {
                if (!db_try_get(key, type, value, t)) {
                    throw std::out_of_range("MetadataTable: key not found: " + key);
                }
            }, TransactionMode::READ_ONLY, txn);
            return value;
        }

        template<class ValueT>
        ValueT get_or(const std::string& key, ValueType type, ValueT fallback,
                      MDBX_txn* txn) const {
            ValueT value;
            bool found = false;
            with_transaction([this, &key, type, &value, &found](MDBX_txn* t) {
                found = db_try_get(key, type, value, t);
            }, TransactionMode::READ_ONLY, txn);
            return found ? std::move(value) : std::move(fallback);
        }

        static MDBX_val make_key(const std::string& key, SerializeScratch& scratch) {
            return serialize_key<true>(key, scratch);
        }

        template<class ValueT>
        static std::vector<std::uint8_t> encode(ValueType type, const ValueT& value) {
            SerializeScratch scratch;
            const MDBX_val serialized = serialize_value(value, scratch);
            std::vector<std::uint8_t> result;
            result.reserve(1u + serialized.iov_len);
            result.push_back(static_cast<std::uint8_t>(type));
            if (serialized.iov_len != 0u) {
                const std::uint8_t* bytes =
                    static_cast<const std::uint8_t*>(serialized.iov_base);
                result.insert(result.end(), bytes, bytes + serialized.iov_len);
            }
            return result;
        }

        static std::vector<std::uint8_t> encode(ValueType type, bool value) {
            std::vector<std::uint8_t> result;
            result.reserve(2u);
            result.push_back(static_cast<std::uint8_t>(type));
            result.push_back(value ? 1u : 0u);
            return result;
        }

        template<class ValueT>
        static ValueT decode(ValueType expected, const MDBX_val& stored) {
            if (stored.iov_len == 0u || stored.iov_base == nullptr) {
                throw std::runtime_error("MetadataTable: malformed stored value");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(stored.iov_base);
            if (bytes[0] != static_cast<std::uint8_t>(expected)) {
                throw std::invalid_argument("MetadataTable: stored value type mismatch");
            }
            MDBX_val payload;
            payload.iov_base = const_cast<std::uint8_t*>(bytes + 1u);
            payload.iov_len = stored.iov_len - 1u;
            return deserialize_value<ValueT>(payload);
        }

        static bool decode_bool(ValueType expected, const MDBX_val& stored) {
            if (stored.iov_len != 2u || stored.iov_base == nullptr) {
                throw std::runtime_error("MetadataTable: malformed bool value");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(stored.iov_base);
            if (bytes[0] != static_cast<std::uint8_t>(expected)) {
                throw std::invalid_argument("MetadataTable: stored value type mismatch");
            }
            if (bytes[1] > 1u) {
                throw std::runtime_error("MetadataTable: malformed bool payload");
            }
            return bytes[1] == 1u;
        }

        template<class ValueT>
        void db_set(const std::string& key, ValueType type, const ValueT& value,
                    MDBX_txn* txn) {
            SerializeScratch key_scratch;
            MDBX_val db_key = make_key(key, key_scratch);
            std::vector<std::uint8_t> encoded = encode(type, value);
            MDBX_val db_value;
            db_value.iov_base = encoded.data();
            db_value.iov_len = encoded.size();
            check_mdbx(mdbx_put(txn, m_dbi, &db_key, &db_value, MDBX_UPSERT),
                       "Failed to write metadata value");
#       if MDBXC_SYNC_ENABLED
            record_op(txn, sync::ChangeOpType::Put,
                      capture_bytes(db_key), capture_bytes(db_value));
#       endif
        }

        template<class ValueT>
        bool db_try_get(const std::string& key, ValueType type, ValueT& value,
                        MDBX_txn* txn) const {
            SerializeScratch key_scratch;
            MDBX_val db_key = make_key(key, key_scratch);
            MDBX_val db_value;
            const int rc = mdbx_get(txn, m_dbi, &db_key, &db_value);
            if (rc == MDBX_NOTFOUND) {
                return false;
            }
            check_mdbx(rc, "Failed to read metadata value");
            value = decode<ValueT>(type, db_value);
            return true;
        }

        bool db_try_get(const std::string& key, ValueType type, bool& value,
                        MDBX_txn* txn) const {
            SerializeScratch key_scratch;
            MDBX_val db_key = make_key(key, key_scratch);
            MDBX_val db_value;
            const int rc = mdbx_get(txn, m_dbi, &db_key, &db_value);
            if (rc == MDBX_NOTFOUND) {
                return false;
            }
            check_mdbx(rc, "Failed to read metadata value");
            value = decode_bool(type, db_value);
            return true;
        }

        bool db_erase(const std::string& key, MDBX_txn* txn) {
            SerializeScratch key_scratch;
            MDBX_val db_key = make_key(key, key_scratch);
            const int rc = mdbx_del(txn, m_dbi, &db_key, nullptr);
            if (rc == MDBX_NOTFOUND) {
                return false;
            }
            check_mdbx(rc, "Failed to erase metadata value");
#       if MDBXC_SYNC_ENABLED
            record_op(txn, sync::ChangeOpType::Delete, capture_bytes(db_key), {});
#       endif
            return true;
        }
    };

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_METADATA_TABLE_HPP_INCLUDED
