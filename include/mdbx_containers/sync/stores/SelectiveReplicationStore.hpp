#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_STORES_SELECTIVE_REPLICATION_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_STORES_SELECTIVE_REPLICATION_STORE_HPP_INCLUDED

/// \file SelectiveReplicationStore.hpp
/// \brief Durable selective-replication descriptors and scope-local state.

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>
#include <mdbx_containers/detail/utils.hpp>

namespace mdbxc {
namespace sync {

    /// \brief Resolved owner of one DBI registered in a selective scope.
    struct SelectiveReplicationDbiBinding {
        std::string scope_id;
        NodeId designated_writer_origin{};
        std::uint32_t dbi_flags = 0u;
    };

    /// \brief Persists immutable selective-scope descriptors and DBI ownership.
    /// \details Uses `_mdbxc_selective_scopes` keyed by `ScopeId` and a
    /// separate `_mdbxc_selective_scope_dbis` index keyed by DBI name. The
    /// index lets connection-level write guards reject a foreign local writer
    /// without scanning every descriptor.
    class SelectiveReplicationStore {
    public:
        SelectiveReplicationStore(
                MDBX_env* env,
                const std::string& scopes_dbi_name =
                    "_mdbxc_selective_scopes",
                const std::string& dbis_dbi_name =
                    "_mdbxc_selective_scope_dbis")
            : m_env(env),
              m_scopes_dbi_name(scopes_dbi_name),
              m_dbis_dbi_name(dbis_dbi_name),
              m_scopes_dbi(0),
              m_dbis_dbi(0),
              m_open(false) {}

        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "SelectiveReplicationStore::open");
            if (m_open) return;
            int rc = mdbx_dbi_open(txn, m_scopes_dbi_name.c_str(), MDBX_CREATE,
                                   &m_scopes_dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, m_scopes_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0),
                                   &m_scopes_dbi);
            }
            check_mdbx(rc, "Failed to open selective scope descriptor DBI");
            rc = mdbx_dbi_open(txn, m_dbis_dbi_name.c_str(), MDBX_CREATE,
                               &m_dbis_dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, m_dbis_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0),
                                   &m_dbis_dbi);
            }
            check_mdbx(rc, "Failed to open selective scope DBI index");
            m_open = true;
        }

        /// \brief Opens persisted descriptor state without creating DBIs.
        /// \return \c true when both descriptor and DBI-index stores exist.
        /// \throws std::runtime_error when only one store exists.
        bool open_existing(MDBX_txn* txn) {
            txn = checked_txn(txn, "SelectiveReplicationStore::open_existing");
            if (m_open) return true;
            MDBX_dbi main_dbi = 0;
            check_mdbx(mdbx_dbi_open(txn, nullptr,
                                     static_cast<MDBX_db_flags_t>(0), &main_dbi),
                       "Failed to open main DBI for selective scope lookup");
            const bool has_scopes =
                named_dbi_exists(txn, main_dbi, m_scopes_dbi_name);
            const bool has_dbi_index =
                named_dbi_exists(txn, main_dbi, m_dbis_dbi_name);
            if (!has_scopes && !has_dbi_index) return false;
            if (!has_scopes || !has_dbi_index) {
                throw std::runtime_error(
                    "selective replication descriptor and DBI-index stores disagree");
            }

            int rc = mdbx_dbi_open(txn, m_scopes_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0), &m_scopes_dbi);
            check_mdbx(rc, "Failed to open selective scope descriptor DBI");
            rc = mdbx_dbi_open(txn, m_dbis_dbi_name.c_str(),
                               static_cast<MDBX_db_flags_t>(0), &m_dbis_dbi);
            check_mdbx(rc, "Failed to open selective scope DBI index");
            m_open = true;
            return true;
        }

        void reset_open() { m_open = false; }

        bool is_open() const { return m_open; }

        void register_or_verify(MDBX_txn* txn,
                                const SelectiveReplicationDescriptor& descriptor) {
            txn = checked_txn(txn, "SelectiveReplicationStore::register_or_verify");
            ensure_open();
            validate_descriptor(descriptor);

            SelectiveReplicationDescriptor existing;
            const bool has_existing = get(txn, descriptor.scope_id, existing);
            if (has_existing && !descriptors_equal(existing, descriptor)) {
                throw std::logic_error(
                    "selective replication scope descriptor is immutable");
            }

            for (std::size_t i = 0; i < descriptor.manifest.size(); ++i) {
                SelectiveReplicationDbiBinding binding;
                if (find_for_dbi(txn, descriptor.manifest[i].dbi_name(), binding) &&
                    !binding_matches_descriptor(binding, descriptor,
                                                descriptor.manifest[i])) {
                    throw std::logic_error(
                        "DBI already belongs to a different selective replication scope");
                }
            }

            if (!has_existing) {
                put_descriptor(txn, descriptor);
            }
            for (std::size_t i = 0; i < descriptor.manifest.size(); ++i) {
                put_binding(txn, descriptor, descriptor.manifest[i]);
            }
        }

        bool get(MDBX_txn* txn, const std::string& scope_id,
                 SelectiveReplicationDescriptor& out) const {
            txn = checked_txn(txn, "SelectiveReplicationStore::get");
            ensure_open();
            MDBX_val key = string_value(scope_id);
            MDBX_val value;
            const int rc = mdbx_get(txn, m_scopes_dbi, &key, &value);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Selective replication descriptor read failed");
            decode_descriptor(value, out);
            if (out.scope_id != scope_id) {
                throw std::runtime_error(
                    "selective replication descriptor scope identity mismatch");
            }
            return true;
        }

        bool find_for_dbi(MDBX_txn* txn, const std::string& dbi_name,
                          SelectiveReplicationDbiBinding& out) const {
            txn = checked_txn(txn, "SelectiveReplicationStore::find_for_dbi");
            ensure_open();
            MDBX_val key = string_value(dbi_name);
            MDBX_val value;
            const int rc = mdbx_get(txn, m_dbis_dbi, &key, &value);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Selective replication DBI binding read failed");
            decode_binding(value, out);
            return true;
        }

        /// \brief Looks up a DBI in the default durable scope index without
        /// creating or opening unrelated selective stores.
        /// \details This is the connection write-guard path. Binding bytes are
        /// decoded by the same canonical decoder as instance lookups.
        static bool find_existing_for_dbi(
                MDBX_env* env, MDBX_txn* txn, const std::string& dbi_name,
                SelectiveReplicationDbiBinding& out) {
            txn = checked_txn_env(
                txn, env,
                "SelectiveReplicationStore::find_existing_for_dbi");
            static const char scopes_dbi_name[] =
                "_mdbxc_selective_scopes";
            static const char dbis_dbi_name[] =
                "_mdbxc_selective_scope_dbis";

            MDBX_dbi main_dbi = 0;
            check_mdbx(mdbx_dbi_open(
                           txn, nullptr, static_cast<MDBX_db_flags_t>(0),
                           &main_dbi),
                       "Failed to open main DBI for selective scope lookup");
            MDBX_val registry_name = {
                const_cast<char*>(dbis_dbi_name),
                sizeof(dbis_dbi_name) - 1u
            };
            MDBX_val registry_value;
            const int registry_rc = mdbx_get(
                txn, main_dbi, &registry_name, &registry_value);
            if (registry_rc == MDBX_NOTFOUND) {
                MDBX_val descriptor_name = {
                    const_cast<char*>(scopes_dbi_name),
                    sizeof(scopes_dbi_name) - 1u
                };
                const int descriptor_rc = mdbx_get(
                    txn, main_dbi, &descriptor_name, &registry_value);
                if (descriptor_rc != MDBX_NOTFOUND) {
                    check_mdbx(
                        descriptor_rc,
                        "Failed to find selective scope descriptor store");
                    throw std::runtime_error(
                        "selective scope descriptors exist without DBI index");
                }
                return false;
            }
            check_mdbx(registry_rc,
                       "Failed to find selective scope DBI index");

            MDBX_dbi bindings_dbi = 0;
            const int open_rc = mdbx_dbi_open(
                txn, dbis_dbi_name, static_cast<MDBX_db_flags_t>(0),
                &bindings_dbi);
            if (open_rc == MDBX_NOTFOUND) {
                throw std::runtime_error(
                    "selective scope DBI index is missing after registry lookup");
            }
            check_mdbx(open_rc, "Failed to open selective scope DBI index");

            MDBX_val key = string_value(dbi_name);
            MDBX_val value;
            const int get_rc = mdbx_get(
                txn, bindings_dbi, &key, &value);
            if (get_rc == MDBX_NOTFOUND) return false;
            check_mdbx(get_rc,
                       "Selective replication DBI binding read failed");
            decode_binding(value, out);
            return true;
        }

    private:
        static const std::uint32_t format_version = 1u;

        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        void ensure_open() const {
            if (!m_open) {
                throw std::logic_error("SelectiveReplicationStore is not open");
            }
        }

        static bool named_dbi_exists(MDBX_txn* txn, MDBX_dbi main_dbi,
                                     const std::string& name) {
            MDBX_val key = string_value(name);
            MDBX_val value;
            const int rc = mdbx_get(txn, main_dbi, &key, &value);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to read selective scope store marker");
            return true;
        }

        static void validate_descriptor(
                const SelectiveReplicationDescriptor& descriptor) {
            if (descriptor.scope_id.empty()) {
                throw std::invalid_argument(
                    "selective replication scope_id must not be empty");
            }
            if (descriptor.scope_id.size() >
                selective_replication_max_scope_id_len) {
                throw std::length_error(
                    "selective replication scope_id exceeds the v1 wire limit");
            }
            if (is_zero_sync_id(descriptor.designated_writer_origin)) {
                throw std::invalid_argument(
                    "selective replication designated writer must not be zero");
            }
            if (descriptor.manifest.empty()) {
                throw std::invalid_argument(
                    "selective replication manifest must not be empty");
            }
            if (descriptor.manifest.size() >
                selective_replication_max_manifest_entries) {
                throw std::length_error(
                    "selective replication manifest exceeds the v1 wire limit");
            }
            for (std::size_t i = 0; i < descriptor.manifest.size(); ++i) {
                const std::string& dbi_name = descriptor.manifest[i].dbi_name();
                if (dbi_name.empty() ||
                    ::mdbxc::is_reserved_dbi_name(dbi_name)) {
                    throw std::invalid_argument(
                        "selective replication manifest contains invalid DBI name");
                }
                if (dbi_name.size() >
                    selective_replication_max_dbi_name_len) {
                    throw std::length_error(
                        "selective replication DBI name exceeds the v1 wire limit");
                }
                if (i != 0u &&
                    descriptor.manifest[i - 1u].dbi_name() >= dbi_name) {
                    throw std::invalid_argument(
                        "selective replication manifest DBI names must be sorted and unique");
                }
            }
        }

        static bool descriptors_equal(
                const SelectiveReplicationDescriptor& left,
                const SelectiveReplicationDescriptor& right) {
            if (left.scope_id != right.scope_id ||
                compare_node_id(left.designated_writer_origin,
                                right.designated_writer_origin) != 0 ||
                left.manifest.size() != right.manifest.size()) {
                return false;
            }
            for (std::size_t i = 0; i < left.manifest.size(); ++i) {
                if (left.manifest[i].dbi_name() != right.manifest[i].dbi_name() ||
                    left.manifest[i].dbi_flags() != right.manifest[i].dbi_flags()) {
                    return false;
                }
            }
            return true;
        }

        static bool binding_matches_descriptor(
                const SelectiveReplicationDbiBinding& binding,
                const SelectiveReplicationDescriptor& descriptor,
                const SelectiveReplicationDbi& dbi) {
            return binding.scope_id == descriptor.scope_id &&
                   compare_node_id(binding.designated_writer_origin,
                                   descriptor.designated_writer_origin) == 0 &&
                   binding.dbi_flags == dbi.dbi_flags();
        }

        static MDBX_val string_value(const std::string& value) {
            MDBX_val out = {
                value.empty() ? nullptr : const_cast<char*>(value.data()),
                value.size()
            };
            return out;
        }

        static void append_string(std::vector<std::uint8_t>& out,
                                  const std::string& value) {
            if (value.size() >
                static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
                throw std::length_error("selective replication string is too long");
            }
            detail::append_u32_le(out, static_cast<std::uint32_t>(value.size()));
            out.insert(out.end(), value.begin(), value.end());
        }

        static std::string read_string(const std::uint8_t*& cursor,
                                       const std::uint8_t* end) {
            if (static_cast<std::size_t>(end - cursor) < 4u) {
                throw std::runtime_error("truncated selective replication string length");
            }
            const std::uint32_t size = detail::read_u32_le(cursor);
            cursor += 4u;
            if (size > static_cast<std::uint32_t>(end - cursor)) {
                throw std::runtime_error("truncated selective replication string");
            }
            const std::string out(reinterpret_cast<const char*>(cursor), size);
            cursor += size;
            return out;
        }

        static void append_node_id(std::vector<std::uint8_t>& out,
                                   const NodeId& node_id) {
            out.insert(out.end(), node_id.begin(), node_id.end());
        }

        static NodeId read_node_id(const std::uint8_t*& cursor,
                                   const std::uint8_t* end) {
            if (static_cast<std::size_t>(end - cursor) < 16u) {
                throw std::runtime_error("truncated selective replication node id");
            }
            NodeId out{};
            std::memcpy(out.data(), cursor, out.size());
            cursor += out.size();
            return out;
        }

        void put_descriptor(MDBX_txn* txn,
                            const SelectiveReplicationDescriptor& descriptor) {
            std::vector<std::uint8_t> bytes;
            detail::append_u32_le(bytes, format_version);
            append_string(bytes, descriptor.scope_id);
            append_node_id(bytes, descriptor.designated_writer_origin);
            if (descriptor.manifest.size() >
                static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
                throw std::length_error("selective replication manifest is too large");
            }
            detail::append_u32_le(
                bytes, static_cast<std::uint32_t>(descriptor.manifest.size()));
            for (std::size_t i = 0; i < descriptor.manifest.size(); ++i) {
                append_string(bytes, descriptor.manifest[i].dbi_name());
                detail::append_u32_le(bytes, descriptor.manifest[i].dbi_flags());
            }
            MDBX_val key = string_value(descriptor.scope_id);
            MDBX_val value = {
                bytes.empty() ? nullptr : &bytes[0], bytes.size()
            };
            check_mdbx(mdbx_put(txn, m_scopes_dbi, &key, &value, MDBX_NOOVERWRITE),
                       "selective replication descriptor write failed");
        }

        void put_binding(MDBX_txn* txn,
                         const SelectiveReplicationDescriptor& descriptor,
                         const SelectiveReplicationDbi& dbi) {
            std::vector<std::uint8_t> bytes;
            detail::append_u32_le(bytes, format_version);
            append_string(bytes, descriptor.scope_id);
            append_node_id(bytes, descriptor.designated_writer_origin);
            detail::append_u32_le(bytes, dbi.dbi_flags());
            MDBX_val key = string_value(dbi.dbi_name());
            MDBX_val value = {
                bytes.empty() ? nullptr : &bytes[0], bytes.size()
            };
            check_mdbx(mdbx_put(txn, m_dbis_dbi, &key, &value, MDBX_UPSERT),
                       "selective replication DBI binding write failed");
        }

        static void decode_descriptor(const MDBX_val& value,
                                      SelectiveReplicationDescriptor& out) {
            if (value.iov_base == nullptr || value.iov_len < 4u) {
                throw std::runtime_error(
                    "unsupported selective replication descriptor format");
            }
            const std::uint8_t* cursor =
                static_cast<const std::uint8_t*>(value.iov_base);
            const std::uint8_t* const end = cursor + value.iov_len;
            if (detail::read_u32_le(cursor) != format_version) {
                throw std::runtime_error("unsupported selective replication descriptor format");
            }
            cursor += 4u;
            SelectiveReplicationDescriptor decoded;
            decoded.scope_id = read_string(cursor, end);
            decoded.designated_writer_origin = read_node_id(cursor, end);
            if (static_cast<std::size_t>(end - cursor) < 4u) {
                throw std::runtime_error("truncated selective replication manifest count");
            }
            const std::uint32_t count = detail::read_u32_le(cursor);
            cursor += 4u;
            if (count > static_cast<std::uint32_t>((end - cursor) / 8u)) {
                throw std::runtime_error(
                    "selective replication manifest count exceeds remaining bytes");
            }
            decoded.manifest.reserve(count);
            for (std::uint32_t i = 0; i < count; ++i) {
                SelectiveReplicationDbi dbi;
                dbi.m_dbi_name = read_string(cursor, end);
                if (static_cast<std::size_t>(end - cursor) < 4u) {
                    throw std::runtime_error("truncated selective replication DBI flags");
                }
                dbi.m_dbi_flags = detail::read_u32_le(cursor);
                cursor += 4u;
                decoded.manifest.push_back(dbi);
            }
            if (cursor != end) {
                throw std::runtime_error("trailing selective replication descriptor bytes");
            }
            validate_descriptor(decoded);
            out = decoded;
        }

        static void decode_binding(const MDBX_val& value,
                                   SelectiveReplicationDbiBinding& out) {
            if (value.iov_base == nullptr || value.iov_len < 4u) {
                throw std::runtime_error(
                    "unsupported selective replication DBI binding format");
            }
            const std::uint8_t* cursor =
                static_cast<const std::uint8_t*>(value.iov_base);
            const std::uint8_t* const end = cursor + value.iov_len;
            if (detail::read_u32_le(cursor) != format_version) {
                throw std::runtime_error("unsupported selective replication DBI binding format");
            }
            cursor += 4u;
            SelectiveReplicationDbiBinding decoded;
            decoded.scope_id = read_string(cursor, end);
            decoded.designated_writer_origin = read_node_id(cursor, end);
            if (static_cast<std::size_t>(end - cursor) != 4u) {
                throw std::runtime_error("invalid selective replication DBI binding size");
            }
            decoded.dbi_flags = detail::read_u32_le(cursor);
            out = decoded;
        }

        MDBX_env* m_env;
        std::string m_scopes_dbi_name;
        std::string m_dbis_dbi_name;
        MDBX_dbi m_scopes_dbi;
        MDBX_dbi m_dbis_dbi;
        bool m_open;
    };

    /// \brief Scope-local changelog keyed by `(ScopeId, scope-local seq)`.
    class ScopedChangeLogStore {
    public:
        ScopedChangeLogStore(
                MDBX_env* env,
                const std::string& dbi_name = "_mdbxc_selective_changelog")
            : m_env(env), m_dbi_name(dbi_name), m_dbi(0), m_open(false) {}

        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "ScopedChangeLogStore::open");
            if (m_open) return;
            int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE, &m_dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0), &m_dbi);
            }
            check_mdbx(rc, "Failed to open scoped changelog DBI");
            m_open = true;
        }

        /// \brief Opens retained scoped history without creating its DBI.
        bool open_existing(MDBX_txn* txn) {
            txn = checked_txn(txn, "ScopedChangeLogStore::open_existing");
            if (m_open) return true;
            const int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                         static_cast<MDBX_db_flags_t>(0),
                                         &m_dbi);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to open existing scoped changelog DBI");
            m_open = true;
            return true;
        }

        void reset_open() { m_open = false; }
        bool is_open() const { return m_open; }

        void append(MDBX_txn* txn, const std::string& scope_id,
                    std::uint64_t sequence,
                    const std::vector<std::uint8_t>& bytes) {
            txn = checked_txn(txn, "ScopedChangeLogStore::append");
            ensure_open();
            std::vector<std::uint8_t> key_bytes;
            encode_key(scope_id, sequence, key_bytes);
            MDBX_val key = { key_bytes.empty() ? nullptr : &key_bytes[0],
                             key_bytes.size() };
            MDBX_val value = { bytes.empty() ? nullptr :
                                    const_cast<std::uint8_t*>(&bytes[0]),
                               bytes.size() };
            check_mdbx(mdbx_put(txn, m_dbi, &key, &value, MDBX_NOOVERWRITE),
                       "scoped changelog append failed");
        }

        bool get(MDBX_txn* txn, const std::string& scope_id,
                 std::uint64_t sequence,
                 std::vector<std::uint8_t>& out) const {
            txn = checked_txn(txn, "ScopedChangeLogStore::get");
            ensure_open();
            std::vector<std::uint8_t> key_bytes;
            encode_key(scope_id, sequence, key_bytes);
            MDBX_val key = { key_bytes.empty() ? nullptr : &key_bytes[0],
                             key_bytes.size() };
            MDBX_val value;
            const int rc = mdbx_get(txn, m_dbi, &key, &value);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "scoped changelog read failed");
            if (value.iov_len == 0u) {
                out.clear();
            } else {
                const std::uint8_t* const bytes =
                    static_cast<const std::uint8_t*>(value.iov_base);
                out.assign(bytes, bytes + value.iov_len);
            }
            return true;
        }

    private:
        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        void ensure_open() const {
            if (!m_open) throw std::logic_error("ScopedChangeLogStore is not open");
        }

        static void encode_key(const std::string& scope_id,
                               std::uint64_t sequence,
                               std::vector<std::uint8_t>& out) {
            if (scope_id.empty()) {
                throw std::invalid_argument("scoped changelog scope_id must not be empty");
            }
            if (scope_id.size() >
                static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
                throw std::length_error("scoped changelog scope_id is too long");
            }
            out.clear();
            detail::append_u32_le(out, static_cast<std::uint32_t>(scope_id.size()));
            out.insert(out.end(), scope_id.begin(), scope_id.end());
            detail::append_u64_be(out, sequence);
        }

        MDBX_env* m_env;
        std::string m_dbi_name;
        MDBX_dbi m_dbi;
        bool m_open;
    };

    /// \brief Stores one scope-local sequence tail per `ScopeId`.
    class ScopedProgressStore {
    public:
        ScopedProgressStore(
                MDBX_env* env,
                const std::string& dbi_name = "_mdbxc_selective_progress")
            : m_env(env), m_dbi_name(dbi_name), m_dbi(0), m_open(false) {}

        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "ScopedProgressStore::open");
            if (m_open) return;
            int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE, &m_dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0), &m_dbi);
            }
            check_mdbx(rc, "Failed to open scoped progress DBI");
            m_open = true;
        }

        /// \brief Opens scoped progress without creating its optional DBI.
        bool open_existing(MDBX_txn* txn) {
            txn = checked_txn(txn, "ScopedProgressStore::open_existing");
            if (m_open) return true;
            const int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                         static_cast<MDBX_db_flags_t>(0),
                                         &m_dbi);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to open existing scoped progress DBI");
            m_open = true;
            return true;
        }

        void reset_open() { m_open = false; }
        bool is_open() const { return m_open; }

        std::uint64_t last_sequence(MDBX_txn* txn,
                                    const std::string& scope_id) const {
            txn = checked_txn(txn, "ScopedProgressStore::last_sequence");
            ensure_open();
            MDBX_val key = string_value(scope_id);
            MDBX_val value;
            const int rc = mdbx_get(txn, m_dbi, &key, &value);
            if (rc == MDBX_NOTFOUND) return 0u;
            check_mdbx(rc, "scoped progress read failed");
            if (value.iov_len != 8u) {
                throw std::runtime_error("scoped progress has invalid size");
            }
            return detail::read_u64_le(
                static_cast<const std::uint8_t*>(value.iov_base));
        }

        std::uint64_t increment_sequence(MDBX_txn* txn,
                                         const std::string& scope_id) {
            txn = checked_txn(txn, "ScopedProgressStore::increment_sequence");
            const std::uint64_t next = last_sequence(txn, scope_id) + 1u;
            set_last_sequence(txn, scope_id, next);
            return next;
        }

        void set_last_sequence(MDBX_txn* txn, const std::string& scope_id,
                               std::uint64_t sequence) {
            txn = checked_txn(txn, "ScopedProgressStore::set_last_sequence");
            ensure_open();
            std::uint8_t bytes[8];
            detail::write_u64_le(sequence, bytes);
            MDBX_val key = string_value(scope_id);
            MDBX_val value = { bytes, sizeof(bytes) };
            check_mdbx(mdbx_put(txn, m_dbi, &key, &value, MDBX_UPSERT),
                       "scoped progress write failed");
        }

    private:
        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        void ensure_open() const {
            if (!m_open) throw std::logic_error("ScopedProgressStore is not open");
        }

        static MDBX_val string_value(const std::string& value) {
            if (value.empty()) {
                throw std::invalid_argument("scoped progress scope_id must not be empty");
            }
            MDBX_val out = { const_cast<char*>(value.data()), value.size() };
            return out;
        }

        MDBX_env* m_env;
        std::string m_dbi_name;
        MDBX_dbi m_dbi;
        bool m_open;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_STORES_SELECTIVE_REPLICATION_STORE_HPP_INCLUDED
