#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_JOURNAL_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_JOURNAL_STORE_HPP_INCLUDED

/// \file logical/stores/LogicalJournalStore.hpp
/// \brief Persistent receiver-neutral journal for logical changes.

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

    /// \brief Persists locally originated logical frames independently of peers.
    /// \details One journal stream is ordered by destination database and
    /// origin. Receiver-specific outbox entries are projected later without
    /// changing the durable journal record or its sequence.
    class LogicalJournalStore {
    public:
        explicit LogicalJournalStore(
                MDBX_env* env,
                const std::string& dbi_name = "_mdbxc_logical_journal")
            : m_env(env),
              m_dbi_name(dbi_name),
              m_dbi(0) {}

        void open(MDBX_txn* txn) {
            txn = checked_txn(txn, "LogicalJournalStore::open");
            open_for_write(txn);
        }

        /// \brief Returns whether the journal has its persistent v1 layout marker.
        /// \details This is a constant-size lookup intended for append-time
        /// migration guards. It deliberately does not validate journal entries;
        /// use has_persistent_state() for that deep integrity inspection.
        bool is_initialized(MDBX_txn* txn) const {
            txn = checked_txn(txn, "LogicalJournalStore::is_initialized");
            if (!try_open_existing(txn)) return false;
            return has_layout_marker(txn);
        }

        /// \brief Writes the persistent v1 layout marker if it is absent.
        /// \details Callers must perform any migration guard before this call.
        /// The marker and the first journal entry are committed atomically when
        /// both operations use the same transaction.
        void initialize(MDBX_txn* txn) {
            txn = checked_txn(txn, "LogicalJournalStore::initialize");
            open_for_write(txn);
            const std::vector<std::uint8_t> key = make_layout_marker_key();
            const std::vector<std::uint8_t> value = make_layout_marker_value();
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value = make_val(value);
            const int rc = mdbx_put(txn, m_dbi, &raw_key, &raw_value,
                                    MDBX_NOOVERWRITE);
            if (rc == MDBX_SUCCESS) return;
            if (rc == MDBX_KEYEXIST) {
                (void)has_layout_marker(txn);
                return;
            }
            check_mdbx(rc, "LogicalJournalStore layout marker write failed");
        }

        LogicalDeliveryEnvelope append(
                MDBX_txn* txn,
                const DbId& destination,
                const NodeId& origin,
                const LogicalChangeFrame& frame,
                const CodecBounds* bounds = nullptr) {
            txn = checked_txn(txn, "LogicalJournalStore::append");
            validate_identity(destination, origin);
            open_for_write(txn);
            if (!has_layout_marker(txn)) {
                throw std::logic_error(
                    "LogicalJournalStore layout is not initialized");
            }
            OriginMetadata metadata = read_origin_metadata(txn, destination,
                                                            origin);
            if (metadata.next_sequence ==
                (std::numeric_limits<std::uint64_t>::max)()) {
                throw std::overflow_error("LogicalJournalStore sequence exhausted");
            }

            LogicalDeliveryEnvelope envelope;
            envelope.destination_db_uuid = destination;
            envelope.origin_node_id = origin;
            envelope.origin_sequence = metadata.next_sequence;
            envelope.frame_id = make_frame_id(metadata.next_sequence);
            envelope.frame = frame;
            if (bounds != nullptr) {
                (void)LogicalDeliveryEnvelopeCodec::encode(envelope, bounds);
            }
            const std::vector<std::uint8_t> key = make_entry_key(
                destination, origin, envelope.origin_sequence);
            const std::vector<std::uint8_t> value =
                LogicalDeliveryEnvelopeCodec::encode(envelope);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value = make_val(value);
            check_mdbx(mdbx_put(txn, m_dbi, &raw_key, &raw_value,
                                MDBX_NOOVERWRITE),
                       "LogicalJournalStore append failed");
            ++metadata.next_sequence;
            write_origin_metadata(txn, destination, origin, metadata);
            return envelope;
        }

        template<typename Visitor>
        void for_each_after(MDBX_txn* txn,
                            const DbId& destination,
                            const NodeId& origin,
                            std::uint64_t sequence,
                            Visitor visitor,
                            const CodecBounds* bounds = nullptr,
                            std::size_t limit = 0u) const {
            txn = checked_txn(txn, "LogicalJournalStore::for_each_after");
            validate_identity(destination, origin);
            if (!try_open_existing(txn)) return;
            const OriginMetadata metadata = read_origin_metadata(txn,
                                                                  destination,
                                                                  origin);
            if (sequence >= metadata.next_sequence - 1u) return;

            const std::vector<std::uint8_t> prefix = make_entry_prefix(
                destination, origin);
            std::vector<std::uint8_t> first_key = prefix;
            detail::append_u64_be(first_key, sequence + 1u);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalJournalStore cursor open failed");
            std::size_t visited = 0u;
            try {
                MDBX_val key = make_val(first_key);
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_SET_RANGE);
                while (rc == MDBX_SUCCESS && has_entry_prefix(key, prefix)) {
                    const std::uint64_t current = decode_entry_sequence(key);
                    if (current >= metadata.next_sequence) break;
                    const LogicalDeliveryEnvelope envelope = decode_value(value,
                                                                          bounds);
                    if (compare_node_id(envelope.destination_db_uuid,
                                        destination) != 0 ||
                        compare_node_id(envelope.origin_node_id, origin) != 0 ||
                        envelope.origin_sequence != current) {
                        throw std::runtime_error(
                            "LogicalJournalStore entry key/value mismatch");
                    }
                    visitor(envelope);
                    ++visited;
                    if (limit != 0u && visited >= limit) break;
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "LogicalJournalStore cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
        }

        std::uint64_t known_tail(MDBX_txn* txn,
                                 const DbId& destination,
                                 const NodeId& origin) const {
            txn = checked_txn(txn, "LogicalJournalStore::known_tail");
            validate_identity(destination, origin);
            if (!try_open_existing(txn)) return 0u;
            return read_origin_metadata(txn, destination, origin).
                next_sequence - 1u;
        }

        /// \brief Verifies that \p envelope is the immutable stored journal entry.
        /// \throws std::invalid_argument if no byte-identical journal entry exists.
        void verify_envelope(MDBX_txn* txn,
                             const LogicalDeliveryEnvelope& envelope,
                             const CodecBounds* bounds = nullptr) const {
            txn = checked_txn(txn, "LogicalJournalStore::verify_envelope");
            validate_identity(envelope.destination_db_uuid,
                              envelope.origin_node_id);
            if (envelope.origin_sequence == 0u) {
                throw std::invalid_argument(
                    "LogicalJournalStore envelope sequence is zero");
            }
            if (bounds != nullptr) {
                (void)LogicalDeliveryEnvelopeCodec::encode(envelope, bounds);
            }
            if (!try_open_existing(txn)) {
                throw std::invalid_argument(
                    "LogicalJournalStore envelope is not a journal entry");
            }
            const std::vector<std::uint8_t> key = make_entry_key(
                envelope.destination_db_uuid, envelope.origin_node_id,
                envelope.origin_sequence);
            const std::vector<std::uint8_t> encoded =
                LogicalDeliveryEnvelopeCodec::encode(envelope);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value;
            const int rc = mdbx_get(txn, m_dbi, &raw_key, &raw_value);
            if (rc == MDBX_NOTFOUND) {
                throw std::invalid_argument(
                    "LogicalJournalStore envelope is not a journal entry");
            }
            check_mdbx(rc, "LogicalJournalStore envelope read failed");
            if (raw_value.iov_len != encoded.size() ||
                raw_value.iov_base == nullptr ||
                std::memcmp(raw_value.iov_base, &encoded[0],
                            encoded.size()) != 0) {
                throw std::invalid_argument(
                    "LogicalJournalStore envelope is not a journal entry");
            }
        }

        bool has_persistent_state(MDBX_txn* txn,
                                  const CodecBounds* bounds = nullptr) const {
            txn = checked_txn(txn, "LogicalJournalStore::has_persistent_state");
            if (!try_open_existing(txn)) return false;
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalJournalStore state cursor open failed");
            bool found = false;
            try {
                MDBX_val key;
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    if (is_layout_marker_key(key)) {
                        decode_layout_marker(value);
                    } else if (is_origin_metadata_key(key)) {
                        (void)decode_origin_metadata(value);
                    } else {
                        const Entry entry = decode_entry(key);
                        const LogicalDeliveryEnvelope envelope = decode_value(
                            value, bounds);
                        if (compare_node_id(envelope.destination_db_uuid,
                                            entry.destination) != 0 ||
                            compare_node_id(envelope.origin_node_id,
                                            entry.origin) != 0 ||
                            envelope.origin_sequence != entry.sequence) {
                            throw std::runtime_error(
                                "LogicalJournalStore entry key/value mismatch");
                        }
                    }
                    found = true;
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc, "LogicalJournalStore state cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            return found;
        }

    private:
        struct OriginMetadata {
            OriginMetadata() : next_sequence(1u) {}
            std::uint64_t next_sequence;
        };

        struct Entry {
            DbId destination;
            NodeId origin;
            std::uint64_t sequence;
        };

        MDBX_txn* checked_txn(MDBX_txn* txn, const char* context) const {
            return checked_txn_env(txn, m_env, context);
        }

        bool try_open_existing(MDBX_txn* txn) const {
            if (!detail::named_dbi_exists(txn, m_dbi_name)) return false;
            const int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                         static_cast<MDBX_db_flags_t>(0),
                                         &m_dbi);
            check_mdbx(rc, "Failed to open LogicalJournalStore DBI");
            return true;
        }

        void open_for_write(MDBX_txn* txn) const {
            int rc = mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE,
                                   &m_dbi);
            if (rc == MDBX_EACCESS) {
                rc = mdbx_dbi_open(txn, m_dbi_name.c_str(),
                                   static_cast<MDBX_db_flags_t>(0), &m_dbi);
            }
            check_mdbx(rc, "Failed to open LogicalJournalStore DBI");
        }

        static void validate_identity(const DbId& destination,
                                      const NodeId& origin) {
            if (is_zero_sync_id(destination) || is_zero_sync_id(origin)) {
                throw std::invalid_argument("LogicalJournalStore identity is zero");
            }
        }

        static std::string make_frame_id(std::uint64_t sequence) {
            return std::string("mdbxc-ordered-") + std::to_string(sequence);
        }

        static std::vector<std::uint8_t> make_origin_metadata_key(
                const DbId& destination, const NodeId& origin) {
            std::vector<std::uint8_t> out;
            out.reserve(2u + destination.size() + origin.size());
            out.push_back(key_version());
            out.push_back(origin_metadata_key_kind());
            out.insert(out.end(), destination.begin(), destination.end());
            out.insert(out.end(), origin.begin(), origin.end());
            return out;
        }

        static std::vector<std::uint8_t> make_layout_marker_key() {
            std::vector<std::uint8_t> out;
            out.reserve(layout_marker_key_size());
            out.push_back(key_version());
            out.push_back(layout_marker_key_kind());
            return out;
        }

        static std::vector<std::uint8_t> make_layout_marker_value() {
            std::vector<std::uint8_t> out;
            out.push_back(layout_value_version());
            return out;
        }

        static std::vector<std::uint8_t> make_entry_prefix(
                const DbId& destination, const NodeId& origin) {
            std::vector<std::uint8_t> out;
            out.reserve(2u + destination.size() + origin.size());
            out.push_back(key_version());
            out.push_back(entry_key_kind());
            out.insert(out.end(), destination.begin(), destination.end());
            out.insert(out.end(), origin.begin(), origin.end());
            return out;
        }

        static std::vector<std::uint8_t> make_entry_key(
                const DbId& destination, const NodeId& origin,
                std::uint64_t sequence) {
            std::vector<std::uint8_t> out = make_entry_prefix(destination, origin);
            detail::append_u64_be(out, sequence);
            return out;
        }

        static MDBX_val make_val(const std::vector<std::uint8_t>& bytes) {
            MDBX_val out = {
                const_cast<std::uint8_t*>(bytes.empty() ? nullptr : &bytes[0]),
                bytes.size()
            };
            return out;
        }

        static bool has_entry_prefix(const MDBX_val& key,
                                     const std::vector<std::uint8_t>& prefix) {
            return key.iov_len == prefix.size() + 8u && key.iov_base != nullptr &&
                   std::memcmp(key.iov_base, &prefix[0], prefix.size()) == 0;
        }

        static bool is_origin_metadata_key(const MDBX_val& key) {
            if (key.iov_len != metadata_key_size() || key.iov_base == nullptr) {
                return false;
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(key.iov_base);
            return bytes[0] == key_version() &&
                   bytes[1] == origin_metadata_key_kind();
        }

        static bool is_layout_marker_key(const MDBX_val& key) {
            if (key.iov_len != layout_marker_key_size() ||
                key.iov_base == nullptr) {
                return false;
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(key.iov_base);
            return bytes[0] == key_version() &&
                   bytes[1] == layout_marker_key_kind();
        }

        static Entry decode_entry(const MDBX_val& key) {
            if (key.iov_len != entry_key_size() || key.iov_base == nullptr) {
                throw std::runtime_error("LogicalJournalStore entry key is invalid");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(key.iov_base);
            if (bytes[0] != key_version() || bytes[1] != entry_key_kind()) {
                throw std::runtime_error("LogicalJournalStore entry key has invalid prefix");
            }
            Entry out;
            std::memcpy(out.destination.data(), bytes + 2u,
                        out.destination.size());
            std::memcpy(out.origin.data(), bytes + 2u + out.destination.size(),
                        out.origin.size());
            out.sequence = detail::read_u64_be(bytes + metadata_key_size());
            validate_identity(out.destination, out.origin);
            return out;
        }

        static std::uint64_t decode_entry_sequence(const MDBX_val& key) {
            return decode_entry(key).sequence;
        }

        OriginMetadata read_origin_metadata(MDBX_txn* txn,
                                            const DbId& destination,
                                            const NodeId& origin) const {
            const std::vector<std::uint8_t> key = make_origin_metadata_key(
                destination, origin);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value;
            const int rc = mdbx_get(txn, m_dbi, &raw_key, &raw_value);
            if (rc == MDBX_NOTFOUND) return OriginMetadata();
            check_mdbx(rc, "LogicalJournalStore origin metadata read failed");
            return decode_origin_metadata(raw_value);
        }

        void write_origin_metadata(MDBX_txn* txn,
                                   const DbId& destination,
                                   const NodeId& origin,
                                   const OriginMetadata& metadata) const {
            const std::vector<std::uint8_t> key = make_origin_metadata_key(
                destination, origin);
            const std::vector<std::uint8_t> value = encode_origin_metadata(metadata);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value = make_val(value);
            check_mdbx(mdbx_put(txn, m_dbi, &raw_key, &raw_value, MDBX_UPSERT),
                       "LogicalJournalStore origin metadata write failed");
        }

        static std::vector<std::uint8_t> encode_origin_metadata(
                const OriginMetadata& metadata) {
            if (metadata.next_sequence == 0u) {
                throw std::logic_error("LogicalJournalStore metadata is invalid");
            }
            std::vector<std::uint8_t> out;
            out.reserve(10u);
            detail::append_u16_le(out, metadata_value_version());
            detail::append_u64_le(out, metadata.next_sequence);
            return out;
        }

        static OriginMetadata decode_origin_metadata(const MDBX_val& value) {
            if (value.iov_len != metadata_value_size() || value.iov_base == nullptr) {
                throw std::runtime_error("LogicalJournalStore metadata is invalid");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(value.iov_base);
            if (detail::read_u16_le(bytes) != metadata_value_version()) {
                throw std::runtime_error("Unsupported LogicalJournalStore metadata version");
            }
            OriginMetadata out;
            out.next_sequence = detail::read_u64_le(bytes + 2u);
            if (out.next_sequence == 0u) {
                throw std::runtime_error("LogicalJournalStore metadata is invalid");
            }
            return out;
        }

        bool has_layout_marker(MDBX_txn* txn) const {
            const std::vector<std::uint8_t> key = make_layout_marker_key();
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value;
            const int rc = mdbx_get(txn, m_dbi, &raw_key, &raw_value);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "LogicalJournalStore layout marker read failed");
            decode_layout_marker(raw_value);
            return true;
        }

        static void decode_layout_marker(const MDBX_val& value) {
            if (value.iov_len != layout_marker_value_size() ||
                value.iov_base == nullptr) {
                throw std::runtime_error(
                    "LogicalJournalStore layout marker is invalid");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(value.iov_base);
            if (bytes[0] != layout_value_version()) {
                throw std::runtime_error(
                    "Unsupported LogicalJournalStore layout version");
            }
        }

        static LogicalDeliveryEnvelope decode_value(const MDBX_val& value,
                                                    const CodecBounds* bounds) {
            if (value.iov_base == nullptr && value.iov_len != 0u) {
                throw std::runtime_error("LogicalJournalStore value is null");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(value.iov_base);
            return LogicalDeliveryEnvelopeCodec::decode(
                std::vector<std::uint8_t>(bytes, bytes + value.iov_len), bounds);
        }

        static constexpr std::uint8_t key_version() { return 1u; }
        static constexpr std::uint8_t layout_marker_key_kind() { return 0u; }
        static constexpr std::uint8_t origin_metadata_key_kind() { return 1u; }
        static constexpr std::uint8_t entry_key_kind() { return 2u; }
        static constexpr std::uint8_t layout_value_version() { return 1u; }
        static constexpr std::uint16_t metadata_value_version() { return 1u; }
        static constexpr std::size_t layout_marker_key_size() { return 2u; }
        static constexpr std::size_t layout_marker_value_size() { return 1u; }
        static constexpr std::size_t metadata_key_size() {
            return 2u + DbId().size() + NodeId().size();
        }
        static constexpr std::size_t entry_key_size() {
            return metadata_key_size() + 8u;
        }
        static constexpr std::size_t metadata_value_size() { return 10u; }

        MDBX_env* m_env;
        std::string m_dbi_name;
        mutable MDBX_dbi m_dbi;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_JOURNAL_STORE_HPP_INCLUDED
