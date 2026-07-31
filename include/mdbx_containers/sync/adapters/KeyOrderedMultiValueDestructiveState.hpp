#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_DESTRUCTIVE_STATE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_DESTRUCTIVE_STATE_HPP_INCLUDED

/// \file KeyOrderedMultiValueDestructiveState.hpp
/// \brief Persistent state primitives for destructive ordered logical schemas.

#include <algorithm>
#include <cstddef>
#include <cstring>
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

    struct OrderedElementIdLess {
        bool operator()(const OrderedElementId& lhs,
                        const OrderedElementId& rhs) const {
            const int compared = compare_node_id(lhs.origin, rhs.origin);
            return compared < 0 ||
                   (compared == 0 && lhs.sequence < rhs.sequence);
        }
    };

    /// \brief Explicit bounds for destructive broad local erasure.
    /// \details Every persisted primary, state, or key-index record must be
    /// counted before it is decoded or otherwise inspected. Selected ids are
    /// counted before they are materialized in the candidate set.
    struct BroadEraseBounds {
        std::size_t max_selected_elements;
        std::size_t max_scanned_records;
    };

    /// \brief Bounded, deterministic candidate set for exact element erasure.
    /// \details Broad selectors resolve to immutable ids before mutating the
    /// table. This helper centralizes both limits and canonical id ordering so
    /// every selector has the same rollback boundary.
    class OrderedElementCandidateSet {
    public:
        explicit OrderedElementCandidateSet(const BroadEraseBounds& bounds)
            : m_bounds(bounds),
              m_scanned_records(0u) {}

        /// \brief Accounts for one record before decoding or inspecting it.
        void inspect_record() {
            if (m_scanned_records >= m_bounds.max_scanned_records) {
                throw std::length_error(
                    "Ordered destructive broad erase scan limit exceeded");
            }
            ++m_scanned_records;
        }

        /// \brief Adds one resolved immutable id before later mutation.
        void select(const OrderedElementId& id) {
            if (!is_valid_ordered_element_id(id)) {
                throw std::invalid_argument(
                    "Ordered destructive broad erase id is invalid");
            }
            if (m_ids.size() >= m_bounds.max_selected_elements) {
                throw std::length_error(
                    "Ordered destructive broad erase selection limit exceeded");
            }
            m_ids.push_back(id);
        }

        std::size_t scanned_records() const {
            return m_scanned_records;
        }

        std::size_t selected_size() const {
            return m_ids.size();
        }

        /// \brief Returns ids in canonical origin/sequence order.
        std::vector<OrderedElementId> sorted_ids() const {
            std::vector<OrderedElementId> out = m_ids;
            std::sort(out.begin(), out.end(), OrderedElementIdLess());
            for (std::size_t i = 1u; i < out.size(); ++i) {
                if (out[i - 1u] == out[i]) {
                    throw std::runtime_error(
                        "Ordered destructive broad erase resolved a duplicate id");
                }
            }
            return out;
        }

    private:
        BroadEraseBounds m_bounds;
        std::size_t m_scanned_records;
        std::vector<OrderedElementId> m_ids;
    };

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
    /// \details The state DBI stores origin allocation counters, introduced
    /// sequence high-water marks, and Live/Tombstone records.
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
            verify_introduced_high_water(txn, origin);
            const std::vector<std::uint8_t> key = make_counter_key(origin);
            const std::uint64_t allocated = read_sequence(
                txn, state, key, "Ordered element counter");
            const std::uint64_t introduced = highest_introduced(txn, origin);
            const std::uint64_t previous = allocated > introduced ?
                allocated : introduced;
            if (previous == (std::numeric_limits<std::uint64_t>::max)()) {
                throw std::overflow_error("Ordered element counter overflow");
            }
            const std::uint64_t next = previous + 1u;
            write_sequence(txn, state, key, next,
                           "Ordered element counter write failed");
            OrderedElementId out;
            out.origin = origin;
            out.sequence = next;
            return out;
        }

        const std::string& state_dbi_name() const {
            return m_state_dbi_name;
        }

        const std::string& by_key_dbi_name() const {
            return m_by_key_dbi_name;
        }

        /// \brief Creates or verifies v2 state DBIs during schema setup.
        /// \details Capture and apply open only existing DBIs afterwards.
        void initialize(MDBX_txn* txn) const {
            txn = checked_txn(txn, "OrderedElementStateStore::initialize");
            open_state_create(txn);
            open_by_key_create(txn);
        }

        /// \brief Creates auxiliary DBIs only for a fresh, empty schema.
        void initialize_empty(MDBX_txn* txn) const {
            txn = checked_txn(
                txn, "OrderedElementStateStore::initialize_empty");
            const MDBX_dbi state = open_state_create(txn);
            const MDBX_dbi by_key = open_by_key_create(txn);
            require_empty(txn, state, "Ordered element state DBI is not empty");
            require_empty(txn, by_key, "Ordered element key index DBI is not empty");
        }

        /// \brief Verifies already-marked auxiliary DBIs without creating them.
        void verify_existing(MDBX_txn* txn) const {
            txn = checked_txn(
                txn, "OrderedElementStateStore::verify_existing");
            open_state(txn);
            open_by_key(txn);
            verify_all_introduced_high_water(txn);
        }

        /// \brief Verifies the introduced high-water mark for one origin.
        /// \details Every persisted Live or Tombstone id must be at or below
        /// the durable high-water mark. This prevents a damaged marker from
        /// admitting a lower id that would disagree with DUPSORT id order.
        void verify_introduced_high_water(
                MDBX_txn* txn,
                const NodeId& origin,
                OrderedElementCandidateSet* candidates = nullptr) const {
            txn = checked_txn(
                txn, "OrderedElementStateStore::verify_introduced_high_water");
            if (compare_node_id(origin, make_zero_node()) == 0) {
                throw std::invalid_argument("Ordered element origin is zero");
            }
            const MDBX_dbi state = open_state(txn);
            const std::vector<std::uint8_t> prefix = make_element_prefix(origin);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, state, &cursor),
                       "Ordered element state high-water cursor open failed");
            std::uint64_t highest_persisted = 0u;
            try {
                MDBX_val raw_key = make_val(prefix);
                MDBX_val raw_value;
                int rc = mdbx_cursor_get(cursor, &raw_key, &raw_value,
                                         MDBX_SET_RANGE);
                while (rc == MDBX_SUCCESS && has_prefix(raw_key, prefix)) {
                    inspect_record(candidates);
                    const OrderedElementId id = decode_element_key(raw_key);
                    decode_state_value(raw_value);
                    if (id.sequence > highest_persisted) {
                        highest_persisted = id.sequence;
                    }
                    rc = mdbx_cursor_get(cursor, &raw_key, &raw_value,
                                         MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND && rc != MDBX_SUCCESS) {
                    check_mdbx(rc,
                               "Ordered element state high-water cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);

            if (highest_persisted == 0u) return;
            std::uint64_t introduced = 0u;
            if (!read_sequence_if_present(
                    txn, state, make_introduced_key(origin), introduced,
                    "Ordered element introduced high-water mark", candidates) ||
                introduced < highest_persisted) {
                throw std::runtime_error(
                    "Ordered element introduced high-water mark is corrupt");
            }
        }

        /// \brief Returns the largest sequence ever introduced for one origin.
        std::uint64_t highest_introduced(
                MDBX_txn* txn,
                const NodeId& origin) const {
            txn = checked_txn(
                txn, "OrderedElementStateStore::highest_introduced");
            if (compare_node_id(origin, make_zero_node()) == 0) {
                throw std::invalid_argument("Ordered element origin is zero");
            }
            const MDBX_dbi state = open_state(txn);
            return read_sequence(txn, state, make_introduced_key(origin),
                                 "Ordered element introduced high-water mark");
        }
        void put_live(MDBX_txn* txn,
                      const OrderedElementId& id,
                      const std::vector<std::uint8_t>& key,
                      const std::vector<std::uint8_t>& value) const {
            txn = checked_txn(txn, "OrderedElementStateStore::put_live");
            require_id(id);
            verify_introduced_high_water(txn, id.origin);
            OrderedElementStateRecord existing;
            if (get(txn, id, existing)) {
                throw std::runtime_error("Ordered element id already exists");
            }

            const MDBX_dbi state = open_state(txn);
            const MDBX_dbi by_key = open_by_key(txn);
            const std::uint64_t introduced = highest_introduced(txn, id.origin);
            if (id.sequence <= introduced) {
                throw std::runtime_error(
                    "Ordered element id is not above the introduced high-water mark");
            }
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
            write_sequence(txn, state, make_introduced_key(id.origin),
                           id.sequence,
                           "Ordered element introduced high-water write failed");
        }

        void tombstone(MDBX_txn* txn,
                       const OrderedElementId& id,
                       OrderedElementCandidateSet* candidates = nullptr) const {
            txn = checked_txn(txn, "OrderedElementStateStore::tombstone");
            require_id(id);
            OrderedElementStateRecord record;
            if (!get(txn, id, record, candidates)) {
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
        void erase_live(MDBX_txn* txn,
                        const OrderedElementId& id,
                        OrderedElementCandidateSet* candidates = nullptr) const {
            txn = checked_txn(txn, "OrderedElementStateStore::erase_live");
            require_id(id);
            OrderedElementStateRecord record;
            if (!get(txn, id, record, candidates) || !record.live) {
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
                 OrderedElementStateRecord& out,
                 OrderedElementCandidateSet* candidates = nullptr) const {
            txn = checked_txn(txn, "OrderedElementStateStore::get");
            require_id(id);
            const MDBX_dbi state = open_state(txn);
            const std::vector<std::uint8_t> key = make_element_key(id);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value;
            const int rc = mdbx_get(txn, state, &raw_key, &raw_value);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Ordered element state read failed");
            inspect_record(candidates);
            out = decode_state_value(raw_value);
            return true;
        }

        std::vector<OrderedElementId> live_ids_for_key(
                MDBX_txn* txn,
                const std::vector<std::uint8_t>& key,
                OrderedElementCandidateSet* candidates = nullptr) const {
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
                    inspect_record(candidates);
                    const OrderedElementId id = decode_index_value(raw_value);
                    OrderedElementStateRecord record;
                    if (!get(txn, id, record, candidates)) {
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

        /// \brief Reverse-scans Live records for \p key.
        /// \details Used to detect state entries missing from the DUPSORT index.
        std::vector<OrderedElementId> live_state_ids_for_key(
                MDBX_txn* txn,
                const std::vector<std::uint8_t>& key,
                OrderedElementCandidateSet* candidates = nullptr) const {
            txn = checked_txn(
                txn, "OrderedElementStateStore::live_state_ids_for_key");
            const MDBX_dbi state = open_state(txn);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, state, &cursor),
                       "Ordered element state cursor open failed");
            std::vector<OrderedElementId> out;
            try {
                MDBX_val raw_key;
                MDBX_val raw_value;
                int rc = mdbx_cursor_get(cursor, &raw_key, &raw_value,
                                         MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    inspect_record(candidates);
                    const std::uint8_t* bytes =
                        static_cast<const std::uint8_t*>(raw_key.iov_base);
                    if (bytes == nullptr || raw_key.iov_len == 0u) {
                        throw std::runtime_error(
                            "Ordered element state key is invalid");
                    }
                    if (bytes[0] == counter_tag() ||
                        bytes[0] == introduced_tag()) {
                        if (raw_key.iov_len != 1u + NodeId().size() ||
                            raw_value.iov_len != 8u) {
                            throw std::runtime_error(
                                "Ordered element counter record is corrupt");
                        }
                    } else if (bytes[0] == element_tag()) {
                        const OrderedElementId id = decode_element_key(raw_key);
                        const OrderedElementStateRecord record =
                            decode_state_value(raw_value);
                        if (record.live && record.key == key) {
                            out.push_back(id);
                        }
                    } else {
                        throw std::runtime_error(
                            "Ordered element state key tag is invalid");
                    }
                    rc = mdbx_cursor_get(cursor, &raw_key, &raw_value,
                                         MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "Ordered element state cursor read failed");
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
        static std::uint8_t introduced_tag() { return 0x02u; }
        static std::uint8_t live_tag() { return 0x01u; }
        static std::uint8_t tombstone_tag() { return 0x02u; }

        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        MDBX_dbi open_state(MDBX_txn* txn) const {
            return open_named_existing(txn, m_state_dbi_name,
                                       static_cast<MDBX_db_flags_t>(0));
        }

        static void inspect_record(OrderedElementCandidateSet* candidates) {
            if (candidates != nullptr) {
                candidates->inspect_record();
            }
        }

        MDBX_dbi open_by_key(MDBX_txn* txn) const {
            return open_named_existing(txn, m_by_key_dbi_name, MDBX_DUPSORT);
        }

        MDBX_dbi open_state_create(MDBX_txn* txn) const {
            return open_named_create(txn, m_state_dbi_name,
                                     static_cast<MDBX_db_flags_t>(0));
        }

        MDBX_dbi open_by_key_create(MDBX_txn* txn) const {
            return open_named_create(txn, m_by_key_dbi_name, MDBX_DUPSORT);
        }

        static bool has_prefix(const MDBX_val& value,
                               const std::vector<std::uint8_t>& prefix) {
            const std::uint8_t* data =
                static_cast<const std::uint8_t*>(value.iov_base);
            return data != nullptr && value.iov_len >= prefix.size() &&
                   std::memcmp(data, &prefix[0], prefix.size()) == 0;
        }

        static bool contains_origin(const std::vector<NodeId>& origins,
                                    const NodeId& origin) {
            for (std::size_t i = 0u; i < origins.size(); ++i) {
                if (compare_node_id(origins[i], origin) == 0) return true;
            }
            return false;
        }

        void verify_all_introduced_high_water(MDBX_txn* txn) const {
            const MDBX_dbi state = open_state(txn);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, state, &cursor),
                       "Ordered element state verification cursor open failed");
            std::vector<NodeId> origins;
            try {
                MDBX_val raw_key;
                MDBX_val raw_value;
                int rc = mdbx_cursor_get(cursor, &raw_key, &raw_value,
                                         MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    const std::uint8_t* bytes =
                        static_cast<const std::uint8_t*>(raw_key.iov_base);
                    if (bytes == nullptr || raw_key.iov_len == 0u) {
                        throw std::runtime_error(
                            "Ordered element state key is invalid");
                    }
                    if (bytes[0] == counter_tag() ||
                        bytes[0] == introduced_tag()) {
                        if (raw_key.iov_len != 1u + NodeId().size() ||
                            raw_value.iov_len != 8u ||
                            compare_node_id(make_node_id(bytes + 1u),
                                            make_zero_node()) == 0) {
                            throw std::runtime_error(
                                "Ordered element counter record is corrupt");
                        }
                    } else if (bytes[0] == element_tag()) {
                        const OrderedElementId id = decode_element_key(raw_key);
                        decode_state_value(raw_value);
                        if (!contains_origin(origins, id.origin)) {
                            origins.push_back(id.origin);
                        }
                    } else {
                        throw std::runtime_error(
                            "Ordered element state key tag is invalid");
                    }
                    rc = mdbx_cursor_get(cursor, &raw_key, &raw_value,
                                         MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "Ordered element state verification cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            for (std::size_t i = 0u; i < origins.size(); ++i) {
                verify_introduced_high_water(txn, origins[i]);
            }
        }

        static MDBX_val make_val(const std::vector<std::uint8_t>& bytes) {
            MDBX_val out = {
                const_cast<std::uint8_t*>(bytes.empty() ? nullptr : &bytes[0]),
                bytes.size()
            };
            return out;
        }

        static void require_empty(MDBX_txn* txn,
                                  MDBX_dbi dbi,
                                  const char* message) {
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, dbi, &cursor),
                       "Ordered element state empty check cursor open failed");
            MDBX_val key;
            MDBX_val value;
            const int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_FIRST);
            mdbx_cursor_close(cursor);
            if (rc == MDBX_SUCCESS) {
                throw std::runtime_error(message);
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Ordered element state empty check failed");
            }
        }

        static std::uint64_t read_sequence(
                MDBX_txn* txn,
                MDBX_dbi dbi,
                const std::vector<std::uint8_t>& key,
                const char* context) {
            std::uint64_t out = 0u;
            read_sequence_if_present(txn, dbi, key, out, context);
            return out;
        }

        static bool read_sequence_if_present(
                MDBX_txn* txn,
                MDBX_dbi dbi,
                const std::vector<std::uint8_t>& key,
                std::uint64_t& out,
                const char* context,
                OrderedElementCandidateSet* candidates = nullptr) {
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value;
            const int rc = mdbx_get(txn, dbi, &raw_key, &raw_value);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, context);
            inspect_record(candidates);
            if (raw_value.iov_len != 8u || raw_value.iov_base == nullptr) {
                throw std::runtime_error(std::string(context) + " is corrupt");
            }
            out = detail::read_u64_le(
                static_cast<const std::uint8_t*>(raw_value.iov_base));
            return true;
        }

        static void write_sequence(
                MDBX_txn* txn,
                MDBX_dbi dbi,
                const std::vector<std::uint8_t>& key,
                std::uint64_t value,
                const char* context) {
            std::vector<std::uint8_t> encoded;
            detail::append_u64_le(encoded, value);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value = make_val(encoded);
            check_mdbx(mdbx_put(txn, dbi, &raw_key, &raw_value, MDBX_UPSERT),
                       context);
        }

        MDBX_dbi open_named_existing(MDBX_txn* txn,
                                      const std::string& name,
                                      MDBX_db_flags_t expected_flags) const {
            MDBX_dbi dbi = 0;
            const int rc = mdbx_dbi_open(txn, name.c_str(),
                                         expected_flags, &dbi);
            check_mdbx(rc, "Ordered element state DBI open failed");
            validate_dbi_flags(txn, dbi, expected_flags);
            return dbi;
        }

        MDBX_dbi open_named_create(MDBX_txn* txn,
                                   const std::string& name,
                                   MDBX_db_flags_t expected_flags) const {
            MDBX_dbi dbi = 0;
            const int rc = mdbx_dbi_open(
                txn, name.c_str(), expected_flags | MDBX_CREATE, &dbi);
            check_mdbx(rc, "Ordered element state DBI setup failed");
            validate_dbi_flags(txn, dbi, expected_flags);
            return dbi;
        }

        static void validate_dbi_flags(MDBX_txn* txn,
                                       MDBX_dbi dbi,
                                       MDBX_db_flags_t expected_flags) {
            unsigned raw_flags = 0u;
            check_mdbx(mdbx_dbi_flags(txn, dbi, &raw_flags),
                       "Ordered element state DBI flags read failed");
            const unsigned persistent_mask =
                MDBX_REVERSEKEY | MDBX_DUPSORT | MDBX_INTEGERKEY |
                MDBX_INTEGERDUP | MDBX_REVERSEDUP | MDBX_DUPFIXED;
            if ((raw_flags & persistent_mask) !=
                (static_cast<unsigned>(expected_flags) & persistent_mask)) {
                throw std::runtime_error(
                    "Ordered element state DBI flags are incompatible");
            }
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

        static std::vector<std::uint8_t> make_introduced_key(
                const NodeId& origin) {
            std::vector<std::uint8_t> out;
            out.push_back(introduced_tag());
            out.insert(out.end(), origin.begin(), origin.end());
            return out;
        }

        static std::vector<std::uint8_t> make_element_key(
                const OrderedElementId& id) {
            std::vector<std::uint8_t> out = make_element_prefix(id.origin);
            const std::vector<std::uint8_t> encoded =
                encode_ordered_element_id_index(id);
            out.insert(out.end(), encoded.begin() + id.origin.size(), encoded.end());
            return out;
        }

        static std::vector<std::uint8_t> make_element_prefix(
                const NodeId& origin) {
            std::vector<std::uint8_t> out;
            out.push_back(element_tag());
            out.insert(out.end(), origin.begin(), origin.end());
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

        static OrderedElementId decode_element_key(const MDBX_val& key) {
            const std::uint8_t* data =
                static_cast<const std::uint8_t*>(key.iov_base);
            const std::size_t id_size = NodeId().size() + 8u;
            if (data == nullptr || key.iov_len != 1u + id_size ||
                data[0] != element_tag()) {
                throw std::runtime_error("Ordered element state key is invalid");
            }
            std::vector<std::uint8_t> bytes(data + 1u, data + key.iov_len);
            return decode_ordered_element_id_index(bytes);
        }

        MDBX_env* m_env;
        std::string m_state_dbi_name;
        std::string m_by_key_dbi_name;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_ORDERED_MULTI_VALUE_DESTRUCTIVE_STATE_HPP_INCLUDED
