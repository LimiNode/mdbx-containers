#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_OUTBOX_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_OUTBOX_STORE_HPP_INCLUDED

/// \file logical/stores/LogicalOutboxStore.hpp
/// \brief Persistent sender-side queue for ordered logical delivery.

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>

#include <mdbx_containers/common.hpp>
#include <mdbx_containers/sync/logical/LogicalDeliveryEnvelopeCodec.hpp>
#include <mdbx_containers/sync/common.hpp>

namespace mdbxc {
namespace sync {

    /// \brief Persists ordered logical delivery envelopes per destination.
    /// \details Each destination has an independent monotonic sequence. Entry
    /// keys use a big-endian sequence suffix so MDBX cursor scans return the
    /// pending stream in delivery order. This store owns sender-side queueing;
    /// it does not send frames or interpret receiver acknowledgements.
    class LogicalOutboxStore {
    public:
        explicit LogicalOutboxStore(
                MDBX_env* env,
                const std::string& dbi_name = "_mdbxc_logical_outbox")
            : m_env(env),
              m_dbi_name(dbi_name),
              m_dbi(0) {}

        /// \brief Creates or opens the outbox DBI in \p txn.
        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "LogicalOutboxStore::open");
            open_for_write(txn);
        }

        /// \brief Returns the DBI handle valid for \p txn.
        MDBX_dbi handle(MDBX_txn* txn) const {
            txn = checked_txn(txn, "LogicalOutboxStore::handle");
            open_existing(txn);
            return m_dbi;
        }

        /// \brief Appends one envelope to \p destination's ordered stream.
        /// \details The generated frame id is stable for the persisted
        /// sequence. A rollback also rolls back the sequence allocation.
        LogicalDeliveryEnvelope enqueue(
                MDBX_txn* txn,
                const DbId& destination,
                const NodeId& origin,
                const LogicalChangeFrame& frame,
                const CodecBounds* bounds = nullptr) {
            txn = checked_txn(txn, "LogicalOutboxStore::enqueue");
            validate_identity(destination, origin);
            open_for_write(txn);

            Metadata metadata = read_metadata(txn, destination);
            if (is_zero_sync_id(metadata.origin_node_id)) {
                metadata.origin_node_id = origin;
            } else if (compare_node_id(metadata.origin_node_id, origin) != 0) {
                throw std::invalid_argument(
                    "LogicalOutboxStore destination already belongs to a different origin");
            }
            if (metadata.next_sequence ==
                (std::numeric_limits<std::uint64_t>::max)()) {
                throw std::overflow_error(
                    "LogicalOutboxStore sequence exhausted");
            }

            LogicalDeliveryEnvelope envelope;
            envelope.destination_db_uuid = destination;
            envelope.origin_node_id = origin;
            envelope.origin_sequence = metadata.next_sequence;
            envelope.frame_id = make_frame_id(metadata.next_sequence);
            envelope.frame = frame;
            const std::vector<std::uint8_t> encoded =
                LogicalDeliveryEnvelopeCodec::encode(envelope, bounds);
            const std::vector<std::uint8_t> key =
                make_entry_key(destination, metadata.next_sequence);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value = make_val(encoded);
            check_mdbx(mdbx_put(txn, m_dbi, &raw_key, &raw_value,
                                MDBX_NOOVERWRITE),
                       "LogicalOutboxStore enqueue failed");

            ++metadata.next_sequence;
            write_metadata(txn, destination, metadata);
            return envelope;
        }

        /// \brief Returns envelopes still pending for \p destination.
        /// \param limit Maximum number of envelopes, or zero for all pending.
        std::vector<LogicalDeliveryEnvelope> list_pending(
                MDBX_txn* txn,
                const DbId& destination,
                std::size_t limit = 0,
                const CodecBounds* bounds = nullptr) const {
            txn = checked_txn(txn, "LogicalOutboxStore::list_pending");
            if (is_zero_sync_id(destination)) {
                throw std::invalid_argument(
                    "LogicalOutboxStore destination is zero");
            }
            open_existing(txn);
            const Metadata metadata = read_metadata(txn, destination);
            std::vector<LogicalDeliveryEnvelope> out;
            if (metadata.acknowledged_through + 1u >=
                metadata.next_sequence) {
                return out;
            }

            const std::vector<std::uint8_t> prefix =
                make_entry_prefix(destination);
            std::vector<std::uint8_t> first_key = prefix;
            detail::append_u64_be(first_key,
                                  metadata.acknowledged_through + 1u);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalOutboxStore pending cursor open failed");
            try {
                MDBX_val key = make_val(first_key);
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_SET_RANGE);
                while (rc == MDBX_SUCCESS && has_entry_prefix(key, prefix)) {
                    const std::uint64_t sequence = decode_entry_sequence(key);
                    if (sequence >= metadata.next_sequence) {
                        break;
                    }
                    LogicalDeliveryEnvelope envelope = decode_value(value, bounds);
                    if (compare_node_id(envelope.destination_db_uuid,
                                        destination) != 0 ||
                        compare_node_id(envelope.origin_node_id,
                                        metadata.origin_node_id) != 0 ||
                        envelope.origin_sequence != sequence) {
                        throw std::runtime_error(
                            "LogicalOutboxStore entry key/value mismatch");
                    }
                    out.push_back(envelope);
                    if (limit != 0u && out.size() >= limit) {
                        break;
                    }
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND) {
                    check_mdbx(rc,
                               "LogicalOutboxStore pending cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            return out;
        }

        /// \brief Returns the persisted cumulative acknowledgement frontier.
        std::uint64_t acknowledged_through(
                MDBX_txn* txn,
                const DbId& destination) const {
            txn = checked_txn(txn,
                              "LogicalOutboxStore::acknowledged_through");
            if (is_zero_sync_id(destination)) {
                throw std::invalid_argument(
                    "LogicalOutboxStore destination is zero");
            }
            open_existing(txn);
            return read_metadata(txn, destination).acknowledged_through;
        }

        /// \brief Returns the highest sequence durably allocated locally.
        /// \details This is the sender's persisted known-tail bound. A peer
        /// acknowledgement must never advance beyond it, including after the
        /// sender restarts and redelivers an earlier pending entry.
        std::uint64_t known_tail(MDBX_txn* txn,
                                 const DbId& destination) const {
            txn = checked_txn(txn, "LogicalOutboxStore::known_tail");
            if (is_zero_sync_id(destination)) {
                throw std::invalid_argument(
                    "LogicalOutboxStore destination is zero");
            }
            open_existing(txn);
            const Metadata metadata = read_metadata(txn, destination);
            return metadata.next_sequence - 1u;
        }

        /// \brief Advances one destination's cumulative acknowledgement.
        /// \return Number of deleted pending entries.
        std::size_t acknowledge_through(MDBX_txn* txn,
                                        const DbId& destination,
                                        std::uint64_t sequence) {
            txn = checked_txn(txn,
                              "LogicalOutboxStore::acknowledge_through");
            if (is_zero_sync_id(destination)) {
                throw std::invalid_argument(
                    "LogicalOutboxStore destination is zero");
            }
            open_for_write(txn);
            Metadata metadata = read_metadata(txn, destination);
            if (sequence < metadata.acknowledged_through) {
                throw std::invalid_argument(
                    "LogicalOutboxStore acknowledgement moved backwards");
            }
            if (sequence == metadata.acknowledged_through) {
                return 0u;
            }
            if (sequence >= metadata.next_sequence) {
                throw std::invalid_argument(
                    "LogicalOutboxStore acknowledgement exceeds enqueued sequence");
            }

            const std::vector<std::uint8_t> prefix =
                make_entry_prefix(destination);
            std::vector<std::uint8_t> first_key = prefix;
            detail::append_u64_be(first_key,
                                  metadata.acknowledged_through + 1u);
            validate_acknowledgement_prefix(
                txn, prefix, first_key, metadata.acknowledged_through + 1u,
                sequence);

            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalOutboxStore acknowledgement cursor open failed");
            std::size_t removed = 0u;
            try {
                MDBX_val key = make_val(first_key);
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_SET_RANGE);
                while (rc == MDBX_SUCCESS && has_entry_prefix(key, prefix)) {
                    const std::uint64_t current = decode_entry_sequence(key);
                    if (current > sequence) {
                        break;
                    }
                    check_mdbx(mdbx_cursor_del(cursor, MDBX_CURRENT),
                               "LogicalOutboxStore acknowledgement delete failed");
                    ++removed;
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND) {
                    check_mdbx(rc,
                               "LogicalOutboxStore acknowledgement cursor read failed");
                }
                if (removed != static_cast<std::size_t>(
                        sequence - metadata.acknowledged_through)) {
                    throw std::runtime_error(
                        "LogicalOutboxStore acknowledgement delete count is invalid");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            metadata.acknowledged_through = sequence;
            write_metadata(txn, destination, metadata);
            return removed;
        }

        /// \brief Returns whether any valid durable outbox state exists.
        /// \details Metadata is state even when all entries were acknowledged:
        /// it records an allocated logical-delivery stream. Every record is
        /// decoded and cross-checked before this method returns.
        bool has_persistent_state(MDBX_txn* txn,
                                  const CodecBounds* bounds = nullptr) const {
            txn = checked_txn(txn,
                              "LogicalOutboxStore::has_persistent_state");
            open_existing(txn);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalOutboxStore state cursor open failed");
            bool found = false;
            try {
                MDBX_val key;
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    const DbId destination = decode_destination(key);
                    if (is_metadata_key(key)) {
                        (void)decode_metadata(value);
                    } else {
                        const std::uint64_t sequence = decode_entry_sequence(key);
                        const Metadata metadata = read_metadata(txn, destination);
                        const LogicalDeliveryEnvelope envelope =
                            decode_value(value, bounds);
                        if (is_zero_sync_id(metadata.origin_node_id) ||
                            sequence <= metadata.acknowledged_through ||
                            sequence >= metadata.next_sequence ||
                            compare_node_id(envelope.destination_db_uuid,
                                            destination) != 0 ||
                            compare_node_id(envelope.origin_node_id,
                                            metadata.origin_node_id) != 0 ||
                            envelope.origin_sequence != sequence) {
                            throw std::runtime_error(
                                "LogicalOutboxStore entry key/value mismatch");
                        }
                    }
                    found = true;
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc,
                               "LogicalOutboxStore state cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            return found;
        }

    private:
        struct Metadata {
            Metadata()
                : next_sequence(1u),
                  acknowledged_through(0u),
                  origin_node_id() {}

            std::uint64_t next_sequence;
            std::uint64_t acknowledged_through;
            NodeId origin_node_id;
        };

        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        void open_existing(MDBX_txn* txn) const {
            check_mdbx(mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                     static_cast<MDBX_db_flags_t>(0), &m_dbi),
                       "Failed to open LogicalOutboxStore DBI");
        }

        void open_for_write(MDBX_txn* txn) const {
            int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE,
                                   &m_dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0), &m_dbi);
            }
            check_mdbx(rc, "Failed to open LogicalOutboxStore DBI");
        }

        static void validate_identity(const DbId& destination,
                                      const NodeId& origin) {
            if (is_zero_sync_id(destination)) {
                throw std::invalid_argument(
                    "LogicalOutboxStore destination is zero");
            }
            if (is_zero_sync_id(origin)) {
                throw std::invalid_argument(
                    "LogicalOutboxStore origin is zero");
            }
        }

        static std::string make_frame_id(std::uint64_t sequence) {
            return std::string("mdbxc-ordered-") + std::to_string(sequence);
        }

        static std::vector<std::uint8_t> make_metadata_key(
                const DbId& destination) {
            std::vector<std::uint8_t> out;
            out.reserve(2u + destination.size());
            out.push_back(key_version());
            out.push_back(metadata_key_kind());
            out.insert(out.end(), destination.begin(), destination.end());
            return out;
        }

        static std::vector<std::uint8_t> make_entry_prefix(
                const DbId& destination) {
            std::vector<std::uint8_t> out;
            out.reserve(2u + destination.size());
            out.push_back(key_version());
            out.push_back(entry_key_kind());
            out.insert(out.end(), destination.begin(), destination.end());
            return out;
        }

        static std::vector<std::uint8_t> make_entry_key(
                const DbId& destination,
                std::uint64_t sequence) {
            std::vector<std::uint8_t> out = make_entry_prefix(destination);
            detail::append_u64_be(out, sequence);
            return out;
        }

        static MDBX_val make_val(const std::vector<std::uint8_t>& bytes) {
            MDBX_val out = {
                const_cast<std::uint8_t*>(
                    bytes.empty() ? nullptr : &bytes[0]),
                bytes.size()
            };
            return out;
        }

        static bool has_entry_prefix(const MDBX_val& key,
                                     const std::vector<std::uint8_t>& prefix) {
            return key.iov_len == prefix.size() + 8u &&
                   key.iov_base != nullptr &&
                   std::memcmp(key.iov_base, &prefix[0], prefix.size()) == 0;
        }

        static bool is_metadata_key(const MDBX_val& key) {
            if (key.iov_len != entry_prefix_size() || key.iov_base == nullptr) {
                return false;
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(key.iov_base);
            return bytes[0] == key_version() &&
                   bytes[1] == metadata_key_kind();
        }

        static DbId decode_destination(const MDBX_val& key) {
            if (key.iov_len != entry_prefix_size() &&
                key.iov_len != entry_key_size()) {
                throw std::runtime_error(
                    "LogicalOutboxStore key has invalid size");
            }
            if (key.iov_base == nullptr) {
                throw std::runtime_error(
                    "LogicalOutboxStore key is null");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(key.iov_base);
            if (bytes[0] != key_version() ||
                (bytes[1] != metadata_key_kind() &&
                 bytes[1] != entry_key_kind())) {
                throw std::runtime_error(
                    "LogicalOutboxStore key has invalid prefix");
            }
            DbId destination;
            std::memcpy(destination.data(), bytes + 2u, destination.size());
            if (is_zero_sync_id(destination)) {
                throw std::runtime_error(
                    "LogicalOutboxStore destination is zero");
            }
            return destination;
        }

        static std::uint64_t decode_entry_sequence(const MDBX_val& key) {
            if (key.iov_len != entry_key_size() || key.iov_base == nullptr) {
                throw std::runtime_error(
                    "LogicalOutboxStore entry key has invalid size");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(key.iov_base);
            if (bytes[0] != key_version() ||
                bytes[1] != entry_key_kind()) {
                throw std::runtime_error(
                    "LogicalOutboxStore entry key has invalid prefix");
            }
            return detail::read_u64_be(bytes + entry_prefix_size());
        }

        void validate_acknowledgement_prefix(
                MDBX_txn* txn,
                const std::vector<std::uint8_t>& prefix,
                const std::vector<std::uint8_t>& first_key,
                std::uint64_t first_sequence,
                std::uint64_t last_sequence) const {
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalOutboxStore acknowledgement validation cursor open failed");
            try {
                std::uint64_t expected = first_sequence;
                MDBX_val key = make_val(first_key);
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_SET_RANGE);
                while (expected <= last_sequence) {
                    if (rc != MDBX_SUCCESS || !has_entry_prefix(key, prefix) ||
                        decode_entry_sequence(key) != expected) {
                        throw std::runtime_error(
                            "LogicalOutboxStore acknowledgement prefix is not contiguous");
                    }
                    if (expected == last_sequence) {
                        break;
                    }
                    ++expected;
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND) {
                    check_mdbx(rc,
                               "LogicalOutboxStore acknowledgement validation cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
        }

        Metadata read_metadata(MDBX_txn* txn,
                               const DbId& destination) const {
            const std::vector<std::uint8_t> key = make_metadata_key(destination);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value;
            const int rc = mdbx_get(txn, m_dbi, &raw_key, &raw_value);
            if (rc == MDBX_NOTFOUND) {
                return Metadata();
            }
            check_mdbx(rc, "LogicalOutboxStore metadata read failed");
            return decode_metadata(raw_value);
        }

        void write_metadata(MDBX_txn* txn,
                            const DbId& destination,
                            const Metadata& metadata) const {
            const std::vector<std::uint8_t> key = make_metadata_key(destination);
            const std::vector<std::uint8_t> value = encode_metadata(metadata);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value = make_val(value);
            check_mdbx(mdbx_put(txn, m_dbi, &raw_key, &raw_value,
                                MDBX_UPSERT),
                       "LogicalOutboxStore metadata write failed");
        }

        static std::vector<std::uint8_t> encode_metadata(
                const Metadata& metadata) {
            if (metadata.next_sequence == 0u ||
                metadata.acknowledged_through >= metadata.next_sequence ||
                is_zero_sync_id(metadata.origin_node_id)) {
                throw std::logic_error(
                    "LogicalOutboxStore metadata is invalid");
            }
            std::vector<std::uint8_t> out;
            out.reserve(2u + 8u + 8u + metadata.origin_node_id.size());
            detail::append_u16_le(out, metadata_value_version());
            detail::append_u64_le(out, metadata.next_sequence);
            detail::append_u64_le(out, metadata.acknowledged_through);
            out.insert(out.end(), metadata.origin_node_id.begin(),
                       metadata.origin_node_id.end());
            return out;
        }

        static Metadata decode_metadata(const MDBX_val& value) {
            if (value.iov_len != metadata_value_size() ||
                value.iov_base == nullptr) {
                throw std::runtime_error(
                    "LogicalOutboxStore metadata has invalid size");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(value.iov_base);
            if (detail::read_u16_le(bytes) != metadata_value_version()) {
                throw std::runtime_error(
                    "Unsupported LogicalOutboxStore metadata version");
            }
            Metadata out;
            out.next_sequence = detail::read_u64_le(bytes + 2u);
            out.acknowledged_through = detail::read_u64_le(bytes + 10u);
            std::memcpy(out.origin_node_id.data(), bytes + 18u,
                        out.origin_node_id.size());
            if (out.next_sequence == 0u ||
                out.acknowledged_through >= out.next_sequence ||
                is_zero_sync_id(out.origin_node_id)) {
                throw std::runtime_error(
                    "LogicalOutboxStore metadata is invalid");
            }
            return out;
        }

        static LogicalDeliveryEnvelope decode_value(
                const MDBX_val& value,
                const CodecBounds* bounds) {
            if (value.iov_len == 0u || value.iov_base == nullptr) {
                throw std::runtime_error(
                    "LogicalOutboxStore entry value is empty");
            }
            const std::uint8_t* begin =
                static_cast<const std::uint8_t*>(value.iov_base);
            const std::vector<std::uint8_t> encoded(
                begin, begin + value.iov_len);
            return LogicalDeliveryEnvelopeCodec::decode(encoded, bounds);
        }

        static std::uint8_t key_version() { return 1u; }
        static std::uint8_t metadata_key_kind() { return 0u; }
        static std::uint8_t entry_key_kind() { return 1u; }
        static std::uint16_t metadata_value_version() { return 2u; }
        static std::size_t entry_prefix_size() { return 2u + DbId().size(); }
        static std::size_t entry_key_size() { return entry_prefix_size() + 8u; }
        static std::size_t metadata_value_size() {
            return 2u + 8u + 8u + NodeId().size();
        }

        MDBX_env* m_env;
        std::string m_dbi_name;
        mutable MDBX_dbi m_dbi;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_OUTBOX_STORE_HPP_INCLUDED
