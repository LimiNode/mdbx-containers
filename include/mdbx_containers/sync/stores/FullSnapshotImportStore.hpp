#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_STORES_FULL_SNAPSHOT_IMPORT_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_STORES_FULL_SNAPSHOT_IMPORT_STORE_HPP_INCLUDED

/// \file FullSnapshotImportStore.hpp
/// \brief Durable staging pages for an incomplete complete-database snapshot.

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>

#include <mdbx_containers/sync/protocol/FullSnapshotProtocol.hpp>

namespace mdbxc {
namespace sync {

    /// \brief Stores accepted non-final complete-snapshot pages by chunk index.
    /// \details The DBI exists only while a persisted import is incomplete.
    /// Each value is a validated \c FullSnapshotCodec payload; the importer
    /// reconstructs its in-memory replacement plan from these values after a
    /// process restart. The final page removes this DBI in the same transaction
    /// that publishes user DBIs and the applied cursor.
    class FullSnapshotImportStore {
    public:
        explicit FullSnapshotImportStore(
                MDBX_env* env,
                const std::string& dbi_name = "_mdbxc_snapshot_import")
            : m_env(env), m_dbi_name(dbi_name), m_dbi(0u), m_open(false) {
        }

        void append(MDBX_txn* txn, const FullSnapshotChunk& chunk) {
            txn = checked_txn(txn, "FullSnapshotImportStore::append");
            if (!chunk.has_more) {
                throw std::invalid_argument(
                    "FullSnapshotImportStore stores only non-final chunks");
            }
            open(txn);
            const std::vector<std::uint8_t> key = make_key(chunk.chunk_index);
            const std::vector<std::uint8_t> encoded = FullSnapshotCodec::encode(chunk);
            MDBX_val raw_key = {
                key.empty() ? nullptr : const_cast<std::uint8_t*>(&key[0]),
                key.size()
            };
            MDBX_val raw_value = {
                encoded.empty() ? nullptr : const_cast<std::uint8_t*>(&encoded[0]),
                encoded.size()
            };
            check_mdbx(
                mdbx_put(txn, m_dbi, &raw_key, &raw_value, MDBX_NOOVERWRITE),
                "FullSnapshotImportStore append failed");
        }

        template<class Visitor>
        bool for_each(MDBX_txn* txn, Visitor visitor) const {
            txn = checked_txn(txn, "FullSnapshotImportStore::for_each");
            if (!try_open_existing(txn)) return false;
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "FullSnapshotImportStore cursor open failed");
            bool found = false;
            try {
                MDBX_val key;
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_FIRST);
                std::uint64_t expected_index = 0u;
                while (rc == MDBX_SUCCESS) {
                    if (key.iov_len != 8u) {
                        throw std::runtime_error(
                            "FullSnapshotImportStore chunk key is malformed");
                    }
                    const std::uint64_t index = detail::read_u64_be(
                        static_cast<const std::uint8_t*>(key.iov_base));
                    if (index != expected_index) {
                        throw std::runtime_error(
                            "FullSnapshotImportStore chunk indexes are not contiguous");
                    }
                    std::vector<std::uint8_t> encoded(value.iov_len);
                    if (value.iov_len != 0u) {
                        std::memcpy(&encoded[0], value.iov_base, value.iov_len);
                    }
                    const FullSnapshotChunk chunk = FullSnapshotCodec::decode(encoded);
                    if (chunk.chunk_index != index || !chunk.has_more) {
                        throw std::runtime_error(
                            "FullSnapshotImportStore contains an invalid staged chunk");
                    }
                    visitor(chunk);
                    found = true;
                    ++expected_index;
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "FullSnapshotImportStore cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            return found;
        }

        void discard(MDBX_txn* txn) const {
            txn = checked_txn(txn, "FullSnapshotImportStore::discard");
            if (!try_open_existing(txn)) return;
            check_mdbx(mdbx_drop(txn, m_dbi, true),
                       "FullSnapshotImportStore discard failed");
        }

    private:
        void open(MDBX_txn* txn) {
            if (m_open) return;
            check_mdbx(
                mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE, &m_dbi),
                "Failed to open FullSnapshotImportStore DBI");
            m_open = true;
        }

        bool try_open_existing(MDBX_txn* txn) const {
            if (m_open) return true;
            const int rc = mdbx_dbi_open(
                txn, m_dbi_name.c_str(), static_cast<MDBX_db_flags_t>(0), &m_dbi);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to open FullSnapshotImportStore DBI");
            m_open = true;
            return true;
        }

        static std::vector<std::uint8_t> make_key(std::uint64_t index) {
            std::vector<std::uint8_t> out;
            detail::append_u64_be(out, index);
            return out;
        }

        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        MDBX_env*           m_env;
        std::string         m_dbi_name;
        mutable MDBX_dbi    m_dbi;
        mutable bool        m_open;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_STORES_FULL_SNAPSHOT_IMPORT_STORE_HPP_INCLUDED
