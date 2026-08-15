#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_DBI_BINDING_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_DBI_BINDING_STORE_HPP_INCLUDED

/// \file logical/stores/LogicalDbiBindingStore.hpp
/// \brief Durable binding from a user DBI to a receiver-neutral logical dataset.

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>

#include "detail/NamedDbiLookup.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Durable logical dataset binding for one user DBI.
    struct LogicalDbiBinding {
        DbId destination;
        LogicalSchemaRef schema;
    };

    /// \brief Persistent marker for DBIs whose ordinary table API captures
    ///        receiver-neutral logical changes.
    /// \details The key is the user DBI name. The value records the destination
    /// database UUID and complete logical schema reference, so a reopen cannot
    /// silently attach a different codec/schema contract to the same DBI.
    class LogicalDbiBindingStore {
    public:
        explicit LogicalDbiBindingStore(
                MDBX_env* env,
                const std::string& dbi_name = "_mdbxc_logical_dbi_bindings")
            : m_env(env), m_dbi_name(dbi_name), m_dbi(0), m_open(false) {}

        void register_or_verify(MDBX_txn* txn,
                                const std::string& user_dbi_name,
                                const DbId& destination,
                                const LogicalSchemaRef& schema) {
            txn = checked_txn(txn, "LogicalDbiBindingStore::register_or_verify");
            validate_user_dbi_name(user_dbi_name);
            if (is_zero_sync_id(destination) ||
                !is_logical_schema_ref_complete(schema)) {
                throw std::invalid_argument(
                    "LogicalDbiBindingStore binding is incomplete");
            }
            open_for_write(txn);
            MDBX_val key = make_name_val(user_dbi_name);
            const std::vector<std::uint8_t> encoded = encode_binding(
                destination, schema);
            MDBX_val value = make_bytes_val(encoded);
            const int rc = mdbx_put(txn, m_dbi, &key, &value, MDBX_NOOVERWRITE);
            if (rc == MDBX_SUCCESS) return;
            if (rc != MDBX_KEYEXIST) {
                check_mdbx(rc, "LogicalDbiBindingStore register failed");
            }
            MDBX_val existing;
            check_mdbx(mdbx_get(txn, m_dbi, &key, &existing),
                       "LogicalDbiBindingStore read existing binding failed");
            LogicalDbiBinding existing_binding;
            decode_binding(existing, existing_binding);
            if (existing_binding.destination != destination ||
                existing_binding.schema.schema_id != schema.schema_id ||
                existing_binding.schema.kind != schema.kind ||
                existing_binding.schema.schema_version != schema.schema_version) {
                throw std::runtime_error("Logical DBI binding mismatch");
            }
        }

        bool contains(MDBX_txn* txn, const std::string& user_dbi_name) const {
            LogicalDbiBinding binding;
            return get(txn, user_dbi_name, binding);
        }

        bool get(MDBX_txn* txn,
                 const std::string& user_dbi_name,
                 LogicalDbiBinding& out) const {
            txn = checked_txn(txn, "LogicalDbiBindingStore::get");
            if (user_dbi_name.empty() || !open_existing(txn)) return false;
            MDBX_val key = make_name_val(user_dbi_name);
            MDBX_val value;
            const int rc = mdbx_get(txn, m_dbi, &key, &value);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "LogicalDbiBindingStore lookup failed");
            LogicalDbiBinding decoded;
            decode_binding(value, decoded);
            out = decoded;
            return true;
        }

        DbId destination(MDBX_txn* txn, const std::string& user_dbi_name) const {
            txn = checked_txn(txn, "LogicalDbiBindingStore::destination");
            if (!open_existing(txn)) {
                throw std::out_of_range("Logical DBI binding is not registered");
            }
            MDBX_val key = make_name_val(user_dbi_name);
            MDBX_val value;
            const int rc = mdbx_get(txn, m_dbi, &key, &value);
            if (rc == MDBX_NOTFOUND) {
                throw std::out_of_range("Logical DBI binding is not registered");
            }
            check_mdbx(rc, "LogicalDbiBindingStore lookup failed");
            LogicalDbiBinding binding;
            decode_binding(value, binding);
            return binding.destination;
        }

    private:
        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        static void validate_user_dbi_name(const std::string& name) {
            if (name.empty() || is_reserved_dbi_name(name)) {
                throw std::invalid_argument(
                    "LogicalDbiBindingStore requires a non-reserved user DBI name");
            }
        }

        static MDBX_val make_name_val(const std::string& name) {
            MDBX_val out = { name.empty() ? nullptr : const_cast<char*>(name.data()),
                             name.size() };
            return out;
        }

        static MDBX_val make_bytes_val(const std::vector<std::uint8_t>& bytes) {
            MDBX_val out = { bytes.empty() ? nullptr :
                              const_cast<std::uint8_t*>(&bytes[0]), bytes.size() };
            return out;
        }

        static void append_u32(std::vector<std::uint8_t>& out,
                               std::uint32_t value) {
            out.push_back(static_cast<std::uint8_t>(value));
            out.push_back(static_cast<std::uint8_t>(value >> 8u));
            out.push_back(static_cast<std::uint8_t>(value >> 16u));
            out.push_back(static_cast<std::uint8_t>(value >> 24u));
        }

        static std::uint32_t read_u32(const std::uint8_t* data,
                                      std::size_t size,
                                      std::size_t& pos) {
            if (size - pos < 4u) {
                throw std::runtime_error("Logical DBI binding is truncated");
            }
            const std::uint32_t value =
                static_cast<std::uint32_t>(data[pos]) |
                (static_cast<std::uint32_t>(data[pos + 1u]) << 8u) |
                (static_cast<std::uint32_t>(data[pos + 2u]) << 16u) |
                (static_cast<std::uint32_t>(data[pos + 3u]) << 24u);
            pos += 4u;
            return value;
        }

        static std::vector<std::uint8_t> encode_binding(
                const DbId& destination, const LogicalSchemaRef& schema) {
            if (schema.schema_id.size() >
                static_cast<std::size_t>((std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error("Logical DBI binding schema id is too large");
            }
            std::vector<std::uint8_t> out;
            out.reserve(1u + 4u + 4u + schema.schema_id.size() + destination.size());
            out.push_back(static_cast<std::uint8_t>(schema.kind));
            append_u32(out, schema.schema_version);
            append_u32(out, static_cast<std::uint32_t>(schema.schema_id.size()));
            out.insert(out.end(), schema.schema_id.begin(), schema.schema_id.end());
            out.insert(out.end(), destination.begin(), destination.end());
            return out;
        }

        static void decode_binding(const MDBX_val& value,
                                   LogicalDbiBinding& binding) {
            const std::uint8_t* data =
                static_cast<const std::uint8_t*>(value.iov_base);
            std::size_t pos = 0u;
            if (data == nullptr || value.iov_len < 1u) {
                throw std::runtime_error("Logical DBI binding is invalid");
            }
            binding.schema.kind = static_cast<LogicalTableKind>(data[pos++]);
            binding.schema.schema_version = read_u32(data, value.iov_len, pos);
            const std::uint32_t id_size = read_u32(data, value.iov_len, pos);
            if (id_size > value.iov_len - pos ||
                binding.destination.size() > value.iov_len - pos - id_size) {
                throw std::runtime_error("Logical DBI binding is truncated");
            }
            binding.schema.schema_id.assign(
                reinterpret_cast<const char*>(data + pos), id_size);
            pos += id_size;
            std::memcpy(binding.destination.data(), data + pos,
                        binding.destination.size());
            pos += binding.destination.size();
            if (pos != value.iov_len || is_zero_sync_id(binding.destination) ||
                !is_logical_schema_ref_complete(binding.schema)) {
                throw std::runtime_error("Logical DBI binding is invalid");
            }
        }

        void open_for_write(MDBX_txn* txn) {
            if (m_open) return;
            check_mdbx(mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE, &m_dbi),
                       "Failed to open LogicalDbiBindingStore DBI");
            m_open = true;
        }

        bool open_existing(MDBX_txn* txn) const {
            if (m_open) return true;
            if (!detail::named_dbi_exists(txn, m_dbi_name)) return false;
            MDBX_dbi opened = 0;
            check_mdbx(mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                     static_cast<MDBX_db_flags_t>(0), &opened),
                       "Failed to open LogicalDbiBindingStore DBI");
            m_dbi = opened;
            m_open = true;
            return true;
        }

        MDBX_env* m_env;
        std::string m_dbi_name;
        mutable MDBX_dbi m_dbi;
        mutable bool m_open;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_DBI_BINDING_STORE_HPP_INCLUDED
