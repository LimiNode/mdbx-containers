#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_DESTRUCTIVE_STATE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_DESTRUCTIVE_STATE_HPP_INCLUDED

/// \file KeyOrderedMultiValueDestructiveState.hpp
/// \brief Persistent state primitives for destructive ordered logical schemas.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>

#include "../../common/MdbxException.hpp"
#include "../../detail/utils.hpp"
#include "../common.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Immutable identity of one destructive ordered-table element.
    struct OrderedElementId {
        NodeId origin = make_zero_node();
        std::uint64_t sequence = 0u;

        bool operator==(const OrderedElementId& other) const {
            return compare_node_id(origin, other.origin) == 0 &&
                   sequence == other.sequence;
        }

        bool operator!=(const OrderedElementId& other) const {
            return !(*this == other);
        }
    };

    inline bool is_valid_ordered_element_id(const OrderedElementId& id) {
        return compare_node_id(id.origin, make_zero_node()) != 0 &&
               id.sequence != 0u;
    }

    /// \brief Encodes an id for logical payloads: node bytes plus LE sequence.
    inline std::vector<std::uint8_t> encode_ordered_element_id_logical(
            const OrderedElementId& id) {
        if (!is_valid_ordered_element_id(id)) {
            throw std::invalid_argument("Ordered element id is invalid");
        }
        std::vector<std::uint8_t> out;
        out.insert(out.end(), id.origin.begin(), id.origin.end());
        detail::append_u64_le(out, id.sequence);
        return out;
    }

    /// \brief Encodes an id for bytewise ordered DBI values: node plus BE seq.
    inline std::vector<std::uint8_t> encode_ordered_element_id_index(
            const OrderedElementId& id) {
        if (!is_valid_ordered_element_id(id)) {
            throw std::invalid_argument("Ordered element id is invalid");
        }
        std::vector<std::uint8_t> out;
        out.insert(out.end(), id.origin.begin(), id.origin.end());
        detail::append_u64_be(out, id.sequence);
        return out;
    }

    inline OrderedElementId decode_ordered_element_id_logical(
            const std::vector<std::uint8_t>& bytes) {
        const std::size_t expected = NodeId().size() + 8u;
        if (bytes.size() != expected) {
            throw std::runtime_error("Ordered logical element id has invalid size");
        }
        OrderedElementId out;
        out.origin = make_node_id(&bytes[0]);
        out.sequence = detail::read_u64_le(&bytes[0] + out.origin.size());
        if (!is_valid_ordered_element_id(out)) {
            throw std::runtime_error("Ordered logical element id is invalid");
        }
        return out;
    }

    inline OrderedElementId decode_ordered_element_id_index(
            const std::vector<std::uint8_t>& bytes) {
        const std::size_t expected = NodeId().size() + 8u;
        if (bytes.size() != expected) {
            throw std::runtime_error("Ordered element index id has invalid size");
        }
        OrderedElementId out;
        out.origin = make_node_id(&bytes[0]);
        out.sequence = detail::read_u64_be(&bytes[0] + out.origin.size());
        if (!is_valid_ordered_element_id(out)) {
            throw std::runtime_error("Ordered element index id is invalid");
        }
        return out;
    }

    /// \brief Persisted state for one destructive ordered-table element.
    struct OrderedElementStateRecord {
        bool live = false;
        std::vector<std::uint8_t> key;
        std::vector<std::uint8_t> value;
    };

    /// \brief Transactional store for destructive ordered element state.
    /// \details The state DBI stores origin counters and Live/Tombstone records.
    /// The DUPSORT index maps canonical logical key bytes to immutable ids.
    /// Every public operation participates in the caller-owned transaction.
    class OrderedElementStateStore {
    public:
        OrderedElementStateStore(MDBX_env* env,
                                 const std::string& state_dbi_name,
                                 const std::string& by_key_dbi_name)
            : m_env(env),
              m_state_dbi_name(state_dbi_name),
              m_by_key_dbi_name(by_key_dbi_name) {
            if (m_env == nullptr || m_state_dbi_name.empty() ||
                m_by_key_dbi_name.empty() ||
                m_state_dbi_name == m_by_key_dbi_name) {
                throw std::invalid_argument(
                    "Ordered element state DBI configuration is invalid");
            }
        }

        OrderedElementId allocate_id(MDBX_txn* txn, const NodeId& origin) const {
            txn = checked_txn(txn, "OrderedElementStateStore::allocate_id");
            if (compare_node_id(origin, make_zero_node()) == 0) {
                throw std::invalid_argument("Ordered element origin is zero");
            }
            const MDBX_dbi state = open_state(txn);
            const std::vector<std::uint8_t> key = make_counter_key(origin);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value;
            std::uint64_t previous = 0u;
            const int rc = mdbx_get(txn, state, &raw_key, &raw_value);
            if (rc == MDBX_SUCCESS) {
                if (raw_value.iov_len != 8u) {
                    throw std::runtime_error("Ordered element counter is corrupt");
                }
                previous = detail::read_u64_le(
                    static_cast<const std::uint8_t*>(raw_value.iov_base));
            } else if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Ordered element counter read failed");
            }
            if (previous == (std::numeric_limits<std::uint64_t>::max)()) {
                throw std::overflow_error("Ordered element counter overflow");
            }
            const std::uint64_t next = previous + 1u;
            std::vector<std::uint8_t> encoded;
            detail::append_u64_le(encoded, next);
            MDBX_val raw_next = make_val(encoded);
            check_mdbx(mdbx_put(txn, state, &raw_key, &raw_next, MDBX_UPSERT),
                       "Ordered element counter write failed");
            OrderedElementId out;
            out.origin = origin;
            out.sequence = next;
            return out;
        }

        void put_live(MDBX_txn* txn,
                      const OrderedElementId& id,
                      const std::vector<std::uint8_t>& key,
                      const std::vector<std::uint8_t>& value) const {
            txn = checked_txn(txn, "OrderedElementStateStore::put_live");
            require_id(id);
            OrderedElementStateRecord existing;
            if (get(txn, id, existing)) {
                throw std::runtime_error("Ordered element id already exists");
            }

            const MDBX_dbi state = open_state(txn);
            const MDBX_dbi by_key = open_by_key(txn);
            const std::vector<std::uint8_t> state_key = make_element_key(id);
            const std::vector<std::uint8_t> state_value = make_live_value(key, value);
            MDBX_val raw_state_key = make_val(state_key);
            MDBX_val raw_state_value = make_val(state_value);
            check_mdbx(mdbx_put(txn, state, &raw_state_key, &raw_state_value,
                                MDBX_NOOVERWRITE),
                       "Ordered element state write failed");

            const std::vector<std::uint8_t> index_value =
                encode_ordered_element_id_index(id);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_index_value = make_val(index_value);
            check_mdbx(mdbx_put(txn, by_key, &raw_key, &raw_index_value,
                                MDBX_NODUPDATA),
                       "Ordered element key index write failed");
        }

        void tombstone(MDBX_txn* txn, const OrderedElementId& id) const {
            txn = checked_txn(txn, "OrderedElementStateStore::tombstone");
            require_id(id);
            OrderedElementStateRecord record;
            if (!get(txn, id, record)) {
                throw std::runtime_error("Ordered element id is missing");
            }
            if (!record.live) {
                throw std::runtime_error("Ordered element id is already tombstoned");
            }
            const MDBX_dbi state = open_state(txn);
            const std::vector<std::uint8_t> state_key = make_element_key(id);
            const std::vector<std::uint8_t> tombstone_value(1u, tombstone_tag());
            MDBX_val raw_key = make_val(state_key);
            MDBX_val raw_value = make_val(tombstone_value);
            check_mdbx(mdbx_put(txn, state, &raw_key, &raw_value, MDBX_UPSERT),
                       "Ordered element tombstone write failed");

            const MDBX_dbi by_key = open_by_key(txn);
            const std::vector<std::uint8_t> index_value =
                encode_ordered_element_id_index(id);
            MDBX_val raw_index_key = make_val(record.key);
            MDBX_val raw_index_value = make_val(index_value);
            check_mdbx(mdbx_del(txn, by_key, &raw_index_key, &raw_index_value),
                       "Ordered element key index tombstone delete failed");
        }

        /// \brief Removes a newly allocated live element without a tombstone.
        /// \details Used when one local typed capture session coalesces its own
        /// append and erase. The origin counter remains advanced.
        void erase_live(MDBX_txn* txn, const OrderedElementId& id) const {
            txn = checked_txn(txn, "OrderedElementStateStore::erase_live");
            require_id(id);
            OrderedElementStateRecord record;
            if (!get(txn, id, record) || !record.live) {
                throw std::runtime_error("Ordered live element is missing");
            }
            const MDBX_dbi state = open_state(txn);
            const MDBX_dbi by_key = open_by_key(txn);
            const std::vector<std::uint8_t> state_key = make_element_key(id);
            const std::vector<std::uint8_t> index_value =
                encode_ordered_element_id_index(id);
            MDBX_val raw_state_key = make_val(state_key);
            MDBX_val raw_index_key = make_val(record.key);
            MDBX_val raw_index_value = make_val(index_value);
            check_mdbx(mdbx_del(txn, by_key, &raw_index_key, &raw_index_value),
                       "Ordered element key index erase failed");
            check_mdbx(mdbx_del(txn, state, &raw_state_key, nullptr),
                       "Ordered element state erase failed");
        }

        bool get(MDBX_txn* txn,
                 const OrderedElementId& id,
                 OrderedElementStateRecord& out) const {
            txn = checked_txn(txn, "OrderedElementStateStore::get");
            require_id(id);
            const MDBX_dbi state = open_state(txn);
            const std::vector<std::uint8_t> key = make_element_key(id);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value;
            const int rc = mdbx_get(txn, state, &raw_key, &raw_value);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Ordered element state read failed");
            out = decode_state_value(raw_value);
            return true;
        }

        std::vector<OrderedElementId> live_ids_for_key(
                MDBX_txn* txn,
                const std::vector<std::uint8_t>& key) const {
            txn = checked_txn(txn, "OrderedElementStateStore::live_ids_for_key");
            const MDBX_dbi by_key = open_by_key(txn);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, by_key, &cursor),
                       "Ordered element key index cursor open failed");
            std::vector<OrderedElementId> out;
            try {
                MDBX_val raw_key = make_val(key);
                MDBX_val raw_value;
                int rc = mdbx_cursor_get(cursor, &raw_key, &raw_value, MDBX_SET_KEY);
                while (rc == MDBX_SUCCESS) {
                    const OrderedElementId id = decode_index_value(raw_value);
                    OrderedElementStateRecord record;
                    if (!get(txn, id, record)) {
                        throw std::runtime_error(
                            "Ordered element key index references missing state");
                    }
                    if (!record.live || record.key != key) {
                        throw std::runtime_error(
                            "Ordered element key index state mismatch");
                    }
                    out.push_back(id);
                    rc = mdbx_cursor_get(cursor, &raw_key, &raw_value,
                                         MDBX_NEXT_DUP);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "Ordered element key index cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            return out;
        }

    private:
        static std::uint8_t counter_tag() { return 0x00u; }
        static std::uint8_t element_tag() { return 0x01u; }
        static std::uint8_t live_tag() { return 0x01u; }
        static std::uint8_t tombstone_tag() { return 0x02u; }

        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        MDBX_dbi open_state(MDBX_txn* txn) const {
            return open_named(txn, m_state_dbi_name,
                              static_cast<MDBX_db_flags_t>(0));
        }

        MDBX_dbi open_by_key(MDBX_txn* txn) const {
            return open_named(txn, m_by_key_dbi_name, MDBX_DUPSORT);
        }

        static MDBX_val make_val(const std::vector<std::uint8_t>& bytes) {
            MDBX_val out = {
                const_cast<std::uint8_t*>(bytes.empty() ? nullptr : &bytes[0]),
                bytes.size()
            };
            return out;
        }

        MDBX_dbi open_named(MDBX_txn* txn,
                            const std::string& name,
                            MDBX_db_flags_t flags) const {
            MDBX_dbi dbi = 0;
            int rc = mdbx_dbi_open(txn, name.c_str(), flags | MDBX_CREATE, &dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, name.c_str(), flags, &dbi);
            }
            check_mdbx(rc, "Ordered element state DBI open failed");
            return dbi;
        }

        static void require_id(const OrderedElementId& id) {
            if (!is_valid_ordered_element_id(id)) {
                throw std::invalid_argument("Ordered element id is invalid");
            }
        }

        static std::vector<std::uint8_t> make_counter_key(const NodeId& origin) {
            std::vector<std::uint8_t> out;
            out.push_back(counter_tag());
            out.insert(out.end(), origin.begin(), origin.end());
            return out;
        }

        static std::vector<std::uint8_t> make_element_key(
                const OrderedElementId& id) {
            std::vector<std::uint8_t> out;
            out.push_back(element_tag());
            const std::vector<std::uint8_t> encoded =
                encode_ordered_element_id_index(id);
            out.insert(out.end(), encoded.begin(), encoded.end());
            return out;
        }

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::vector<std::uint8_t>& bytes) {
            if (bytes.size() > static_cast<std::size_t>(
                    (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error("Ordered element state blob is too large");
            }
            detail::append_u32_le(out, static_cast<std::uint32_t>(bytes.size()));
            out.insert(out.end(), bytes.begin(), bytes.end());
        }

        static std::vector<std::uint8_t> make_live_value(
                const std::vector<std::uint8_t>& key,
                const std::vector<std::uint8_t>& value) {
            std::vector<std::uint8_t> out;
            out.push_back(live_tag());
            append_blob(out, key);
            append_blob(out, value);
            return out;
        }

        static void require(const std::uint8_t* data,
                            std::size_t size,
                            std::size_t pos,
                            std::size_t need) {
            if (data == nullptr || pos > size || need > size - pos) {
                throw std::runtime_error("Ordered element state decode underrun");
            }
        }

        static std::vector<std::uint8_t> read_blob(const std::uint8_t* data,
                                                   std::size_t size,
                                                   std::size_t& pos) {
            require(data, size, pos, 4u);
            const std::uint32_t length = detail::read_u32_le(data + pos);
            pos += 4u;
            require(data, size, pos, length);
            std::vector<std::uint8_t> out;
            if (length != 0u) {
                out.assign(data + pos, data + pos + length);
            }
            pos += length;
            return out;
        }

        static OrderedElementStateRecord decode_state_value(const MDBX_val& value) {
            const std::uint8_t* data =
                static_cast<const std::uint8_t*>(value.iov_base);
            const std::size_t size = value.iov_len;
            require(data, size, 0u, 1u);
            if (data[0] == tombstone_tag()) {
                if (size != 1u) {
                    throw std::runtime_error(
                        "Ordered element tombstone has trailing bytes");
                }
                return OrderedElementStateRecord();
            }
            if (data[0] != live_tag()) {
                throw std::runtime_error("Ordered element state tag is invalid");
            }
            std::size_t pos = 1u;
            OrderedElementStateRecord out;
            out.live = true;
            out.key = read_blob(data, size, pos);
            out.value = read_blob(data, size, pos);
            if (pos != size) {
                throw std::runtime_error("Ordered element state has trailing bytes");
            }
            return out;
        }

        static OrderedElementId decode_index_value(const MDBX_val& value) {
            const std::uint8_t* data =
                static_cast<const std::uint8_t*>(value.iov_base);
            std::vector<std::uint8_t> bytes;
            if (value.iov_len != NodeId().size() + 8u || data == nullptr) {
                throw std::runtime_error("Ordered element key index value is invalid");
            }
            bytes.assign(data, data + value.iov_len);
            return decode_ordered_element_id_index(bytes);
        }

        MDBX_env* m_env;
        std::string m_state_dbi_name;
        std::string m_by_key_dbi_name;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_DESTRUCTIVE_STATE_HPP_INCLUDED
