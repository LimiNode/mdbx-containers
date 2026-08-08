#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_DELIVERY_ORDER_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_DELIVERY_ORDER_STORE_HPP_INCLUDED

/// \file logical/stores/LogicalDeliveryOrderStore.hpp
/// \brief Persistent contiguous receiver frontier for ordered delivery.

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>


namespace mdbxc {
namespace sync {

    /// \brief One persisted ordered-delivery frontier.
    struct LogicalDeliveryOrderEntry {
        NodeId origin_node_id{};
        std::uint64_t acknowledged_through = 0u;
    };

    /// \brief Persists the highest contiguous ordered delivery per origin.
    class LogicalDeliveryOrderStore {
    public:
        explicit LogicalDeliveryOrderStore(
                MDBX_env* env,
                const std::string& dbi_name = "_mdbxc_logical_delivery_order")
            : m_env(env),
              m_dbi_name(dbi_name),
              m_dbi(0) {}

        /// \brief Creates or opens the order-state DBI in \p txn.
        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "LogicalDeliveryOrderStore::open");
            open_for_write(txn);
        }

        /// \brief Returns the highest contiguous delivered sequence for origin.
        /// \return Zero when no ordered delivery from \p origin was committed.
        std::uint64_t last_applied(MDBX_txn* txn,
                                   const NodeId& origin) const {
            txn = checked_txn(txn, "LogicalDeliveryOrderStore::last_applied");
            validate_origin(origin);
            open_existing(txn);
            MDBX_val key = make_key(origin);
            MDBX_val value;
            const int rc = mdbx_get(txn, m_dbi, &key, &value);
            if (rc == MDBX_NOTFOUND) {
                return 0u;
            }
            check_mdbx(rc, "LogicalDeliveryOrderStore read failed");
            return decode_value(value);
        }

        /// \brief Advances one origin by exactly one contiguous sequence.
        /// \throws std::invalid_argument unless \p sequence is last + 1.
        void advance(MDBX_txn* txn,
                     const NodeId& origin,
                     std::uint64_t sequence) {
            txn = checked_txn(txn, "LogicalDeliveryOrderStore::advance");
            validate_origin(origin);
            if (sequence == 0u) {
                throw std::invalid_argument(
                    "LogicalDeliveryOrderStore sequence is zero");
            }
            open_for_write(txn);
            const std::uint64_t previous = last_applied(txn, origin);
            if (previous == (std::numeric_limits<std::uint64_t>::max)() ||
                sequence != previous + 1u) {
                throw std::invalid_argument(
                    "LogicalDeliveryOrderStore sequence is not contiguous");
            }
            MDBX_val key = make_key(origin);
            const std::vector<std::uint8_t> encoded = encode_value(sequence);
            MDBX_val value = make_val(encoded);
            check_mdbx(mdbx_put(txn, m_dbi, &key, &value, MDBX_UPSERT),
                       "LogicalDeliveryOrderStore write failed");
        }

        /// \brief Returns whether any valid ordered-delivery frontier exists.
        /// \details The whole DBI is decoded so malformed state is not treated
        /// as an empty receiver frontier.
        bool has_entries(MDBX_txn* txn) const {
            txn = checked_txn(txn,
                              "LogicalDeliveryOrderStore::has_entries");
            open_existing(txn);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalDeliveryOrderStore cursor open failed");
            bool found = false;
            try {
                MDBX_val key;
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    (void)decode_origin(key);
                    (void)decode_value(value);
                    found = true;
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc,
                               "LogicalDeliveryOrderStore cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            return found;
        }

        /// \brief Lists every persisted ordered-delivery frontier.
        /// \details All entries are decoded before returning. This is used by
        /// the logical recovery baseline, which transfers receiver ordering
        /// state as data rather than treating it as an implicit local cache.
        std::vector<LogicalDeliveryOrderEntry> list_entries(
                MDBX_txn* txn) const {
            txn = checked_txn(txn,
                              "LogicalDeliveryOrderStore::list_entries");
            open_existing(txn);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalDeliveryOrderStore cursor open failed");
            std::vector<LogicalDeliveryOrderEntry> out;
            try {
                MDBX_val key;
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    LogicalDeliveryOrderEntry entry;
                    entry.origin_node_id = decode_origin(key);
                    entry.acknowledged_through = decode_value(value);
                    out.push_back(entry);
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc,
                               "LogicalDeliveryOrderStore cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            return out;
        }

        /// \brief Restores one exact frontier into an otherwise empty store.
        /// \details Recovery must not silently merge a baseline with local
        /// receiver history. Repeating the same entry is idempotent; a
        /// different existing frontier is rejected.
        void restore_entry(MDBX_txn* txn,
                           const LogicalDeliveryOrderEntry& entry) {
            txn = checked_txn(txn,
                              "LogicalDeliveryOrderStore::restore_entry");
            validate_origin(entry.origin_node_id);
            if (entry.acknowledged_through == 0u) {
                throw std::invalid_argument(
                    "LogicalDeliveryOrderStore restored sequence is zero");
            }
            open_for_write(txn);
            const std::uint64_t existing = last_applied(
                txn, entry.origin_node_id);
            if (existing != 0u && existing != entry.acknowledged_through) {
                throw std::runtime_error(
                    "LogicalDeliveryOrderStore recovery frontier mismatch");
            }
            if (existing == entry.acknowledged_through) {
                return;
            }
            MDBX_val key = make_key(entry.origin_node_id);
            const std::vector<std::uint8_t> encoded =
                encode_value(entry.acknowledged_through);
            MDBX_val value = make_val(encoded);
            check_mdbx(mdbx_put(txn, m_dbi, &key, &value, MDBX_NOOVERWRITE),
                       "LogicalDeliveryOrderStore recovery write failed");
        }

    private:
        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        void open_existing(MDBX_txn* txn) const {
            check_mdbx(mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                     static_cast<MDBX_db_flags_t>(0), &m_dbi),
                       "Failed to open LogicalDeliveryOrderStore DBI");
        }

        void open_for_write(MDBX_txn* txn) const {
            int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE,
                                   &m_dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0), &m_dbi);
            }
            check_mdbx(rc, "Failed to open LogicalDeliveryOrderStore DBI");
        }

        static void validate_origin(const NodeId& origin) {
            if (is_zero_sync_id(origin)) {
                throw std::invalid_argument(
                    "LogicalDeliveryOrderStore origin is zero");
            }
        }

        static MDBX_val make_key(const NodeId& origin) {
            MDBX_val out = {
                const_cast<std::uint8_t*>(origin.data()),
                origin.size()
            };
            return out;
        }

        static NodeId decode_origin(const MDBX_val& key) {
            if (key.iov_len != NodeId().size() || key.iov_base == nullptr) {
                throw std::runtime_error(
                    "LogicalDeliveryOrderStore key has invalid size");
            }
            NodeId origin;
            std::memcpy(origin.data(), key.iov_base, origin.size());
            validate_origin(origin);
            return origin;
        }

        static MDBX_val make_val(const std::vector<std::uint8_t>& bytes) {
            MDBX_val out = {
                const_cast<std::uint8_t*>(
                    bytes.empty() ? nullptr : &bytes[0]),
                bytes.size()
            };
            return out;
        }

        static std::vector<std::uint8_t> encode_value(
                std::uint64_t sequence) {
            std::vector<std::uint8_t> out;
            out.reserve(2u + 8u);
            detail::append_u16_le(out, value_version());
            detail::append_u64_le(out, sequence);
            return out;
        }

        static std::uint64_t decode_value(const MDBX_val& value) {
            if (value.iov_len != 2u + 8u || value.iov_base == nullptr) {
                throw std::runtime_error(
                    "LogicalDeliveryOrderStore value has invalid size");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(value.iov_base);
            if (detail::read_u16_le(bytes) != value_version()) {
                throw std::runtime_error(
                    "Unsupported LogicalDeliveryOrderStore value version");
            }
            const std::uint64_t sequence = detail::read_u64_le(bytes + 2u);
            if (sequence == 0u) {
                throw std::runtime_error(
                    "LogicalDeliveryOrderStore value is zero");
            }
            return sequence;
        }

        static std::uint16_t value_version() { return 1u; }

        MDBX_env* m_env;
        std::string m_dbi_name;
        mutable MDBX_dbi m_dbi;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_DELIVERY_ORDER_STORE_HPP_INCLUDED
