#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_STORES_RECEIVER_REPLICATION_MODE_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_STORES_RECEIVER_REPLICATION_MODE_STORE_HPP_INCLUDED

/// \file ReceiverReplicationModeStore.hpp
/// \brief Durable exclusion between full-global and selective raw apply.

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>

namespace mdbxc {
namespace sync {

    /// \brief Durable raw-delivery mode selected by one receiving database.
    enum class ReceiverReplicationMode : std::uint8_t {
        FullGlobal = 1u,
        Selective = 2u
    };

    /// \brief One receiver-mode record and its admitted selective scopes.
    struct ReceiverReplicationModeState {
        ReceiverReplicationMode mode = ReceiverReplicationMode::FullGlobal;
        std::vector<std::string> scope_ids;
    };

    /// \brief Persists the mutually exclusive raw-delivery mode per DbId.
    /// \details Format v1 is `[u32 version][u8 mode][u32 scope_count]`
    /// followed by sorted, unique `[u32 size][ScopeId bytes]` values. A
    /// full-global record has no scopes. A selective record has at least one.
    class ReceiverReplicationModeStore {
    public:
        explicit ReceiverReplicationModeStore(
                MDBX_env* env,
                const std::string& dbi_name = "_mdbxc_receiver_mode")
            : m_env(env), m_dbi_name(dbi_name), m_dbi(0), m_open(false) {}

        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "ReceiverReplicationModeStore::open");
            if (m_open) return;
            int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE, &m_dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0), &m_dbi);
            }
            check_mdbx(rc, "Failed to open receiver replication mode DBI");
            m_open = true;
        }

        /// \brief Opens existing mode state without creating an optional DBI.
        bool open_existing(MDBX_txn* txn) {
            txn = checked_txn(
                txn, "ReceiverReplicationModeStore::open_existing");
            if (m_open) return true;
            const int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                         static_cast<MDBX_db_flags_t>(0),
                                         &m_dbi);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to open existing receiver mode DBI");
            m_open = true;
            return true;
        }

        bool get(MDBX_txn* txn, const DbId& db_id,
                 ReceiverReplicationModeState& out) const {
            txn = checked_txn(txn, "ReceiverReplicationModeStore::get");
            ensure_open();
            MDBX_val key = id_value(db_id);
            MDBX_val value;
            const int rc = mdbx_get(txn, m_dbi, &key, &value);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Receiver replication mode read failed");
            decode(value, out);
            return true;
        }

        /// \brief Claims full-global mode or verifies an existing claim.
        /// \return false when selective mode is already durable.
        bool claim_full_global(MDBX_txn* txn, const DbId& db_id) {
            txn = checked_txn(
                txn, "ReceiverReplicationModeStore::claim_full_global");
            ensure_open();
            ReceiverReplicationModeState state;
            if (get(txn, db_id, state)) {
                return state.mode == ReceiverReplicationMode::FullGlobal;
            }
            state.mode = ReceiverReplicationMode::FullGlobal;
            put(txn, db_id, state);
            return true;
        }

        /// \brief Claims selective mode and admits one immutable ScopeId.
        /// \return false when full-global mode is already durable.
        bool claim_selective(MDBX_txn* txn, const DbId& db_id,
                             const std::string& scope_id) {
            txn = checked_txn(
                txn, "ReceiverReplicationModeStore::claim_selective");
            ensure_open();
            validate_scope_id(scope_id);
            ReceiverReplicationModeState state;
            if (get(txn, db_id, state)) {
                if (state.mode != ReceiverReplicationMode::Selective) {
                    return false;
                }
                const std::vector<std::string>::iterator position =
                    std::lower_bound(state.scope_ids.begin(),
                                     state.scope_ids.end(), scope_id);
                if (position != state.scope_ids.end() &&
                    *position == scope_id) {
                    return true;
                }
                state.scope_ids.insert(position, scope_id);
            } else {
                state.mode = ReceiverReplicationMode::Selective;
                state.scope_ids.push_back(scope_id);
            }
            put(txn, db_id, state);
            return true;
        }

    private:
        static const std::uint32_t format_version = 1u;

        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        void ensure_open() const {
            if (!m_open) {
                throw std::logic_error(
                    "ReceiverReplicationModeStore is not open");
            }
        }

        static MDBX_val id_value(const DbId& db_id) {
            if (is_zero_sync_id(db_id)) {
                throw std::invalid_argument(
                    "receiver replication mode DbId must not be zero");
            }
            MDBX_val out = {
                const_cast<std::uint8_t*>(db_id.data()), db_id.size()
            };
            return out;
        }

        static void validate_scope_id(const std::string& scope_id) {
            if (scope_id.empty() ||
                scope_id.size() > selective_replication_max_scope_id_len) {
                throw std::invalid_argument(
                    "receiver replication mode has invalid ScopeId");
            }
        }

        static void append_u32(std::vector<std::uint8_t>& out,
                               std::uint32_t value) {
            detail::append_u32_le(out, value);
        }

        static void put_scope(std::vector<std::uint8_t>& out,
                              const std::string& scope_id) {
            validate_scope_id(scope_id);
            append_u32(out, static_cast<std::uint32_t>(scope_id.size()));
            out.insert(out.end(), scope_id.begin(), scope_id.end());
        }

        static std::uint32_t read_u32(const std::uint8_t*& cursor,
                                      const std::uint8_t* end,
                                      const char* context) {
            if (static_cast<std::size_t>(end - cursor) < 4u) {
                throw std::runtime_error(context);
            }
            const std::uint32_t value = detail::read_u32_le(cursor);
            cursor += 4u;
            return value;
        }

        static std::string read_scope(const std::uint8_t*& cursor,
                                      const std::uint8_t* end) {
            const std::uint32_t size = read_u32(
                cursor, end, "truncated receiver mode ScopeId length");
            if (size == 0u || size > selective_replication_max_scope_id_len ||
                static_cast<std::size_t>(end - cursor) < size) {
                throw std::runtime_error(
                    "invalid receiver replication mode ScopeId");
            }
            const std::string scope(
                reinterpret_cast<const char*>(cursor), size);
            cursor += size;
            return scope;
        }

        static std::vector<std::uint8_t> encode(
                const ReceiverReplicationModeState& state) {
            if (state.mode == ReceiverReplicationMode::FullGlobal &&
                !state.scope_ids.empty()) {
                throw std::logic_error(
                    "full-global receiver mode must not carry scopes");
            }
            if (state.mode == ReceiverReplicationMode::Selective &&
                state.scope_ids.empty()) {
                throw std::logic_error(
                    "selective receiver mode must carry a scope");
            }
            if (state.scope_ids.size() >
                selective_replication_max_manifest_entries) {
                throw std::length_error(
                    "receiver mode scope count exceeds the v1 bound");
            }
            std::vector<std::uint8_t> bytes;
            append_u32(bytes, format_version);
            bytes.push_back(static_cast<std::uint8_t>(state.mode));
            append_u32(bytes,
                       static_cast<std::uint32_t>(state.scope_ids.size()));
            for (std::size_t i = 0u; i < state.scope_ids.size(); ++i) {
                if (i != 0u && state.scope_ids[i - 1u] >= state.scope_ids[i]) {
                    throw std::logic_error(
                        "receiver mode ScopeIds must be sorted and unique");
                }
                put_scope(bytes, state.scope_ids[i]);
            }
            return bytes;
        }

        static void decode(const MDBX_val& value,
                           ReceiverReplicationModeState& out) {
            if (value.iov_base == nullptr || value.iov_len < 9u) {
                throw std::runtime_error(
                    "invalid receiver replication mode record");
            }
            const std::uint8_t* cursor =
                static_cast<const std::uint8_t*>(value.iov_base);
            const std::uint8_t* const end = cursor + value.iov_len;
            if (read_u32(cursor, end,
                         "truncated receiver mode version") !=
                format_version) {
                throw std::runtime_error(
                    "unsupported receiver replication mode format");
            }
            const std::uint8_t raw_mode = *cursor++;
            ReceiverReplicationModeState decoded;
            if (raw_mode == static_cast<std::uint8_t>(
                                ReceiverReplicationMode::FullGlobal)) {
                decoded.mode = ReceiverReplicationMode::FullGlobal;
            } else if (raw_mode == static_cast<std::uint8_t>(
                                       ReceiverReplicationMode::Selective)) {
                decoded.mode = ReceiverReplicationMode::Selective;
            } else {
                throw std::runtime_error(
                    "unknown receiver replication mode");
            }
            const std::uint32_t count = read_u32(
                cursor, end, "truncated receiver mode scope count");
            if (count > selective_replication_max_manifest_entries) {
                throw std::runtime_error(
                    "receiver mode scope count exceeds the v1 bound");
            }
            decoded.scope_ids.reserve(count);
            for (std::uint32_t i = 0u; i < count; ++i) {
                decoded.scope_ids.push_back(read_scope(cursor, end));
                if (i != 0u &&
                    decoded.scope_ids[i - 1u] >= decoded.scope_ids[i]) {
                    throw std::runtime_error(
                        "receiver mode ScopeIds are not canonical");
                }
            }
            if (cursor != end ||
                (decoded.mode == ReceiverReplicationMode::FullGlobal &&
                 !decoded.scope_ids.empty()) ||
                (decoded.mode == ReceiverReplicationMode::Selective &&
                 decoded.scope_ids.empty())) {
                throw std::runtime_error(
                    "invalid receiver replication mode payload");
            }
            out = decoded;
        }

        void put(MDBX_txn* txn, const DbId& db_id,
                 const ReceiverReplicationModeState& state) {
            const std::vector<std::uint8_t> bytes = encode(state);
            MDBX_val key = id_value(db_id);
            MDBX_val value = {
                bytes.empty() ? nullptr :
                    const_cast<std::uint8_t*>(bytes.data()),
                bytes.size()
            };
            check_mdbx(mdbx_put(txn, m_dbi, &key, &value, MDBX_UPSERT),
                       "Receiver replication mode write failed");
        }

        MDBX_env* m_env;
        std::string m_dbi_name;
        MDBX_dbi m_dbi;
        bool m_open;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_STORES_RECEIVER_REPLICATION_MODE_STORE_HPP_INCLUDED
