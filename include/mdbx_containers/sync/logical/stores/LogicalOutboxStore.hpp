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

#include "LogicalJournalStore.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Persists ordered logical events and receiver-local delivery state.
    /// \details Event sequence allocation is global for one destination
    /// database and origin. Receiver routes retain only their acknowledgement
    /// and known-tail state. Entry keys use a big-endian sequence suffix so
    /// MDBX cursor scans return the pending route in delivery order. This store
    /// owns sender-side queueing; it does not send frames or interpret receiver
    /// acknowledgements.
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

        /// \brief Appends one globally ordered event to one receiver route.
        /// \details The event is first persisted in the receiver-neutral
        /// journal, then projected onto \p receiver in the same transaction.
        /// A rollback also rolls back sequence allocation and route state.
        LogicalDeliveryEnvelope enqueue(
                MDBX_txn* txn,
                const DbId& destination,
                const NodeId& receiver,
                const NodeId& origin,
                const LogicalChangeFrame& frame,
                const CodecBounds* bounds = nullptr) {
            txn = checked_txn(txn, "LogicalOutboxStore::enqueue");
            validate_identity(destination, receiver, origin);
            open_for_write(txn);
            const RouteMetadata existing_route = read_route_metadata(
                txn, destination, receiver);
            if (!is_zero_sync_id(existing_route.origin_node_id) &&
                compare_node_id(existing_route.origin_node_id, origin) != 0) {
                throw std::invalid_argument(
                    "LogicalOutboxStore receiver already belongs to a different origin");
            }
            LogicalJournalStore journal(m_env);
            if (!journal.is_initialized(txn)) {
                if (has_persistent_state(txn)) {
                    throw std::logic_error(
                        "LogicalOutboxStore legacy state cannot allocate journal entries");
                }
                journal.initialize(txn);
            }
            const LogicalDeliveryEnvelope envelope = journal.append(
                txn, destination, origin, frame, bounds);
            return project(txn, receiver, envelope, bounds);
        }

        /// \brief Adds an existing journal envelope to one receiver route.
        /// \details Projection accepts only an immutable journal entry and
        /// preserves its assigned event identity. Repeating an already
        /// projected envelope is a no-op only when its durable bytes are
        /// identical. A newly created direct route may start at its first
        /// projected sequence; journal materialization starts at sequence one.
        LogicalDeliveryEnvelope project(
                MDBX_txn* txn,
                const NodeId& receiver,
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr) {
            txn = checked_txn(txn, "LogicalOutboxStore::project");
            validate_identity(envelope.destination_db_uuid, receiver,
                              envelope.origin_node_id);
            if (envelope.origin_sequence == 0u) {
                throw std::invalid_argument(
                    "LogicalOutboxStore projected sequence is zero");
            }
            if (bounds != nullptr) {
                (void)LogicalDeliveryEnvelopeCodec::encode(envelope, bounds);
            }
            LogicalJournalStore journal(m_env);
            journal.verify_envelope(txn, envelope, bounds);
            const std::vector<std::uint8_t> encoded =
                LogicalDeliveryEnvelopeCodec::encode(envelope);
            open_for_write(txn);

            RouteMetadata metadata = read_route_metadata(
                txn, envelope.destination_db_uuid, receiver);
            if (is_zero_sync_id(metadata.origin_node_id)) {
                metadata.origin_node_id = envelope.origin_node_id;
                metadata.acknowledged_through = envelope.origin_sequence - 1u;
                metadata.known_tail = envelope.origin_sequence - 1u;
            } else if (compare_node_id(metadata.origin_node_id,
                                       envelope.origin_node_id) != 0) {
                throw std::invalid_argument(
                    "LogicalOutboxStore receiver already belongs to a different origin");
            }

            if (envelope.origin_sequence <= metadata.acknowledged_through) {
                return envelope;
            }
            const std::vector<std::uint8_t> key = make_entry_key(
                envelope.destination_db_uuid, receiver,
                envelope.origin_sequence);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value;
            if (envelope.origin_sequence <= metadata.known_tail) {
                const int rc = mdbx_get(txn, m_dbi, &raw_key, &raw_value);
                if (rc == MDBX_NOTFOUND) {
                    throw std::runtime_error(
                        "LogicalOutboxStore projected entry is missing");
                }
                check_mdbx(rc, "LogicalOutboxStore projected entry read failed");
                if (raw_value.iov_len != encoded.size() ||
                    raw_value.iov_base == nullptr ||
                    std::memcmp(raw_value.iov_base, &encoded[0],
                                encoded.size()) != 0) {
                    throw std::runtime_error(
                        "LogicalOutboxStore projected entry conflicts");
                }
                return envelope;
            }
            if (envelope.origin_sequence != metadata.known_tail + 1u) {
                throw std::invalid_argument(
                    "LogicalOutboxStore projected sequence has a gap");
            }
            raw_value = make_val(encoded);
            check_mdbx(mdbx_put(txn, m_dbi, &raw_key, &raw_value,
                                MDBX_NOOVERWRITE),
                       "LogicalOutboxStore projection failed");
            metadata.known_tail = envelope.origin_sequence;
            write_route_metadata(txn, envelope.destination_db_uuid, receiver,
                                 metadata);
            return envelope;
        }

        /// \brief Visits envelopes still pending for one \p receiver.
        /// \details The visitor runs while the MDBX cursor is open and may
        /// throw to stop enumeration without retaining later envelopes.
        template <typename Visitor>
        void for_each_pending(
                MDBX_txn* txn,
                const DbId& destination,
                const NodeId& receiver,
                Visitor visitor,
                const CodecBounds* bounds = nullptr,
                std::size_t limit = 0u) const {
            txn = checked_txn(txn, "LogicalOutboxStore::for_each_pending");
            validate_route(destination, receiver);
            open_existing(txn);
            const RouteMetadata metadata = read_route_metadata(
                txn, destination, receiver);
            if (is_zero_sync_id(metadata.origin_node_id) ||
                metadata.acknowledged_through >= metadata.known_tail) {
                return;
            }

            const std::vector<std::uint8_t> prefix =
                make_entry_prefix(destination, receiver);
            std::vector<std::uint8_t> first_key = prefix;
            detail::append_u64_be(first_key,
                                  metadata.acknowledged_through + 1u);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalOutboxStore pending cursor open failed");
            std::size_t visited = 0u;
            try {
                MDBX_val key = make_val(first_key);
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_SET_RANGE);
                while (rc == MDBX_SUCCESS && has_entry_prefix(key, prefix)) {
                    const std::uint64_t sequence = decode_entry_sequence(key);
                    if (sequence > metadata.known_tail) {
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
                    visitor(envelope);
                    ++visited;
                    if (limit != 0u && visited >= limit) {
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
        }

        /// \brief Returns envelopes still pending for one \p receiver.
        /// \param limit Maximum number of envelopes, or zero for all pending.
        std::vector<LogicalDeliveryEnvelope> list_pending(
                MDBX_txn* txn,
                const DbId& destination,
                const NodeId& receiver,
                std::size_t limit = 0,
                const CodecBounds* bounds = nullptr) const {
            std::vector<LogicalDeliveryEnvelope> out;
            for_each_pending(txn, destination, receiver,
                [&out](const LogicalDeliveryEnvelope& envelope) {
                    out.push_back(envelope);
                }, bounds, limit);
            return out;
        }

        /// \brief Returns the persisted cumulative acknowledgement frontier.
        std::uint64_t acknowledged_through(
                MDBX_txn* txn,
                const DbId& destination,
                const NodeId& receiver) const {
            txn = checked_txn(txn,
                              "LogicalOutboxStore::acknowledged_through");
            validate_route(destination, receiver);
            open_existing(txn);
            return read_route_metadata(txn, destination, receiver).
                acknowledged_through;
        }

        /// \brief Returns the highest sequence durably allocated locally.
        /// \details This is the sender's persisted known-tail bound. A peer
        /// acknowledgement must never advance beyond it, including after the
        /// sender restarts and redelivers an earlier pending entry.
        std::uint64_t known_tail(MDBX_txn* txn,
                                 const DbId& destination,
                                 const NodeId& receiver) const {
            txn = checked_txn(txn, "LogicalOutboxStore::known_tail");
            validate_route(destination, receiver);
            open_existing(txn);
            return read_route_metadata(txn, destination, receiver).known_tail;
        }

        /// \brief Returns the origin assigned to one receiver route.
        /// \return Zero id when the route does not exist.
        NodeId route_origin(MDBX_txn* txn,
                            const DbId& destination,
                            const NodeId& receiver) const {
            txn = checked_txn(txn, "LogicalOutboxStore::route_origin");
            validate_route(destination, receiver);
            open_existing(txn);
            return read_route_metadata(txn, destination, receiver).
                origin_node_id;
        }

        /// \brief Returns whether any receiver route has pending work.
        /// \details This is used before a raw-only peer can provide a logical
        /// hello and therefore cannot identify one receiver route.
        bool has_pending_for_destination(
                MDBX_txn* txn,
                const DbId& destination,
                const CodecBounds* bounds = nullptr) const {
            txn = checked_txn(txn,
                              "LogicalOutboxStore::has_pending_for_destination");
            if (is_zero_sync_id(destination)) {
                throw std::invalid_argument(
                    "LogicalOutboxStore destination is zero");
            }
            open_existing(txn);
            MDBX_cursor* cursor = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &cursor),
                       "LogicalOutboxStore pending-state cursor open failed");
            try {
                MDBX_val key;
                MDBX_val value;
                int rc = mdbx_cursor_get(cursor, &key, &value, MDBX_FIRST);
                while (rc == MDBX_SUCCESS) {
                    if (!is_origin_metadata_key(key) &&
                        !is_route_metadata_key(key)) {
                        const Route route = decode_route(key);
                        if (compare_node_id(route.destination, destination) == 0) {
                            const std::uint64_t sequence =
                                decode_entry_sequence(key);
                            const RouteMetadata metadata = read_route_metadata(
                                txn, route.destination, route.receiver);
                            const LogicalDeliveryEnvelope envelope =
                                decode_value(value, bounds);
                            if (is_zero_sync_id(metadata.origin_node_id) ||
                                sequence <= metadata.acknowledged_through ||
                                sequence > metadata.known_tail ||
                                compare_node_id(envelope.destination_db_uuid,
                                                route.destination) != 0 ||
                                compare_node_id(envelope.origin_node_id,
                                                metadata.origin_node_id) != 0 ||
                                envelope.origin_sequence != sequence) {
                                throw std::runtime_error(
                                    "LogicalOutboxStore entry key/value mismatch");
                            }
                            mdbx_cursor_close(cursor);
                            return true;
                        }
                    }
                    rc = mdbx_cursor_get(cursor, &key, &value, MDBX_NEXT);
                }
                if (rc != MDBX_NOTFOUND) {
                    check_mdbx(rc,
                               "LogicalOutboxStore pending-state cursor read failed");
                }
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            return false;
        }

        /// \brief Advances one destination's cumulative acknowledgement.
        /// \return Number of deleted pending entries.
        std::size_t acknowledge_through(MDBX_txn* txn,
                                        const DbId& destination,
                                        const NodeId& receiver,
                                        std::uint64_t sequence) {
            txn = checked_txn(txn,
                              "LogicalOutboxStore::acknowledge_through");
            validate_route(destination, receiver);
            open_for_write(txn);
            RouteMetadata metadata = read_route_metadata(txn, destination,
                                                         receiver);
            if (is_zero_sync_id(metadata.origin_node_id)) {
                throw std::invalid_argument(
                    "LogicalOutboxStore acknowledgement route is unknown");
            }
            if (sequence < metadata.acknowledged_through) {
                throw std::invalid_argument(
                    "LogicalOutboxStore acknowledgement moved backwards");
            }
            if (sequence == metadata.acknowledged_through) {
                return 0u;
            }
            if (sequence > metadata.known_tail) {
                throw std::invalid_argument(
                    "LogicalOutboxStore acknowledgement exceeds enqueued sequence");
            }

            const std::vector<std::uint8_t> acknowledged_key =
                make_entry_key(destination, receiver, sequence);
            MDBX_val raw_acknowledged_key = make_val(acknowledged_key);
            MDBX_val raw_acknowledged_value;
            const int acknowledged_rc = mdbx_get(
                txn, m_dbi, &raw_acknowledged_key, &raw_acknowledged_value);
            if (acknowledged_rc == MDBX_NOTFOUND) {
                throw std::runtime_error(
                    "LogicalOutboxStore acknowledgement target is missing");
            }
            check_mdbx(acknowledged_rc,
                       "LogicalOutboxStore acknowledgement target read failed");
            const LogicalDeliveryEnvelope acknowledged = decode_value(
                raw_acknowledged_value, nullptr);
            if (compare_node_id(acknowledged.destination_db_uuid,
                                destination) != 0 ||
                compare_node_id(acknowledged.origin_node_id,
                                metadata.origin_node_id) != 0 ||
                acknowledged.origin_sequence != sequence) {
                throw std::runtime_error(
                    "LogicalOutboxStore acknowledgement target is invalid");
            }

            const std::vector<std::uint8_t> prefix =
                make_entry_prefix(destination, receiver);
            std::vector<std::uint8_t> first_key = prefix;
            detail::append_u64_be(first_key,
                                  metadata.acknowledged_through + 1u);
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
                    if (current <= metadata.acknowledged_through ||
                        current > metadata.known_tail) {
                        throw std::runtime_error(
                            "LogicalOutboxStore acknowledgement entry is invalid");
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
            } catch (...) {
                mdbx_cursor_close(cursor);
                throw;
            }
            mdbx_cursor_close(cursor);
            metadata.acknowledged_through = sequence;
            write_route_metadata(txn, destination, receiver, metadata);
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
                    if (is_origin_metadata_key(key)) {
                        (void)decode_origin_metadata(value);
                    } else {
                        const Route route = decode_route(key);
                        if (is_route_metadata_key(key)) {
                            (void)decode_metadata(value);
                            found = true;
                            rc = mdbx_cursor_get(cursor, &key, &value,
                                                 MDBX_NEXT);
                            continue;
                        }
                        const std::uint64_t sequence = decode_entry_sequence(key);
                        const RouteMetadata metadata = read_route_metadata(
                            txn, route.destination, route.receiver);
                        const LogicalDeliveryEnvelope envelope =
                            decode_value(value, bounds);
                        if (is_zero_sync_id(metadata.origin_node_id) ||
                            sequence <= metadata.acknowledged_through ||
                            sequence > metadata.known_tail ||
                            compare_node_id(envelope.destination_db_uuid,
                                            route.destination) != 0 ||
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
        struct OriginMetadata {
            OriginMetadata()
                : next_sequence(1u) {}

            std::uint64_t next_sequence;
        };

        struct RouteMetadata {
            RouteMetadata()
                : acknowledged_through(0u),
                  known_tail(0u),
                  origin_node_id() {}

            std::uint64_t acknowledged_through;
            std::uint64_t known_tail;
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

        struct Route {
            DbId destination;
            NodeId receiver;
        };

        static void validate_route(const DbId& destination,
                                   const NodeId& receiver) {
            if (is_zero_sync_id(destination)) {
                throw std::invalid_argument(
                    "LogicalOutboxStore destination is zero");
            }
            if (is_zero_sync_id(receiver)) {
                throw std::invalid_argument(
                    "LogicalOutboxStore receiver is zero");
            }
        }

        static void validate_identity(const DbId& destination,
                                      const NodeId& receiver,
                                      const NodeId& origin) {
            validate_route(destination, receiver);
            if (is_zero_sync_id(origin)) {
                throw std::invalid_argument(
                    "LogicalOutboxStore origin is zero");
            }
        }

        static std::string make_frame_id(std::uint64_t sequence) {
            return std::string("mdbxc-ordered-") + std::to_string(sequence);
        }

        static std::vector<std::uint8_t> make_origin_metadata_key(
                const DbId& destination,
                const NodeId& origin) {
            std::vector<std::uint8_t> out;
            out.reserve(2u + destination.size() + origin.size());
            out.push_back(key_version());
            out.push_back(origin_metadata_key_kind());
            out.insert(out.end(), destination.begin(), destination.end());
            out.insert(out.end(), origin.begin(), origin.end());
            return out;
        }

        static std::vector<std::uint8_t> make_route_metadata_key(
                const DbId& destination,
                const NodeId& receiver) {
            std::vector<std::uint8_t> out;
            out.reserve(2u + destination.size() + receiver.size());
            out.push_back(key_version());
            out.push_back(route_metadata_key_kind());
            out.insert(out.end(), destination.begin(), destination.end());
            out.insert(out.end(), receiver.begin(), receiver.end());
            return out;
        }

        static std::vector<std::uint8_t> make_entry_prefix(
                const DbId& destination,
                const NodeId& receiver) {
            std::vector<std::uint8_t> out;
            out.reserve(2u + destination.size() + receiver.size());
            out.push_back(key_version());
            out.push_back(entry_key_kind());
            out.insert(out.end(), destination.begin(), destination.end());
            out.insert(out.end(), receiver.begin(), receiver.end());
            return out;
        }

        static std::vector<std::uint8_t> make_entry_key(
                const DbId& destination,
                const NodeId& receiver,
                std::uint64_t sequence) {
            std::vector<std::uint8_t> out = make_entry_prefix(destination,
                                                                receiver);
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

        static bool is_origin_metadata_key(const MDBX_val& key) {
            if (key.iov_len != entry_prefix_size() || key.iov_base == nullptr) {
                return false;
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(key.iov_base);
            return bytes[0] == key_version() &&
                   bytes[1] == origin_metadata_key_kind();
        }

        static bool is_route_metadata_key(const MDBX_val& key) {
            if (key.iov_len != entry_prefix_size() || key.iov_base == nullptr) {
                return false;
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(key.iov_base);
            return bytes[0] == key_version() &&
                   bytes[1] == route_metadata_key_kind();
        }

        static Route decode_route(const MDBX_val& key) {
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
                (bytes[1] != route_metadata_key_kind() &&
                 bytes[1] != entry_key_kind())) {
                throw std::runtime_error(
                    "LogicalOutboxStore key has invalid prefix");
            }
            Route route;
            std::memcpy(route.destination.data(), bytes + 2u,
                        route.destination.size());
            std::memcpy(route.receiver.data(),
                        bytes + 2u + route.destination.size(),
                        route.receiver.size());
            if (is_zero_sync_id(route.destination) ||
                is_zero_sync_id(route.receiver)) {
                throw std::runtime_error("LogicalOutboxStore route is incomplete");
            }
            return route;
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

        OriginMetadata read_origin_metadata(MDBX_txn* txn,
                                            const DbId& destination,
                                            const NodeId& origin) const {
            const std::vector<std::uint8_t> key = make_origin_metadata_key(
                destination, origin);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value;
            const int rc = mdbx_get(txn, m_dbi, &raw_key, &raw_value);
            if (rc == MDBX_NOTFOUND) {
                return OriginMetadata();
            }
            check_mdbx(rc, "LogicalOutboxStore origin metadata read failed");
            return decode_origin_metadata(raw_value);
        }

        RouteMetadata read_route_metadata(MDBX_txn* txn,
                                          const DbId& destination,
                                          const NodeId& receiver) const {
            const std::vector<std::uint8_t> key = make_route_metadata_key(
                destination, receiver);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value;
            const int rc = mdbx_get(txn, m_dbi, &raw_key, &raw_value);
            if (rc == MDBX_NOTFOUND) {
                return RouteMetadata();
            }
            check_mdbx(rc, "LogicalOutboxStore route metadata read failed");
            return decode_metadata(raw_value);
        }

        void write_origin_metadata(MDBX_txn* txn,
                                   const DbId& destination,
                                   const NodeId& origin,
                                   const OriginMetadata& metadata) const {
            const std::vector<std::uint8_t> key = make_origin_metadata_key(
                destination, origin);
            const std::vector<std::uint8_t> value =
                encode_origin_metadata(metadata);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value = make_val(value);
            check_mdbx(mdbx_put(txn, m_dbi, &raw_key, &raw_value,
                                MDBX_UPSERT),
                       "LogicalOutboxStore origin metadata write failed");
        }

        void write_route_metadata(MDBX_txn* txn,
                                  const DbId& destination,
                                  const NodeId& receiver,
                                  const RouteMetadata& metadata) const {
            const std::vector<std::uint8_t> key = make_route_metadata_key(
                destination, receiver);
            const std::vector<std::uint8_t> value = encode_metadata(metadata);
            MDBX_val raw_key = make_val(key);
            MDBX_val raw_value = make_val(value);
            check_mdbx(mdbx_put(txn, m_dbi, &raw_key, &raw_value,
                                MDBX_UPSERT),
                       "LogicalOutboxStore route metadata write failed");
        }

        static std::vector<std::uint8_t> encode_origin_metadata(
                const OriginMetadata& metadata) {
            if (metadata.next_sequence == 0u) {
                throw std::logic_error(
                    "LogicalOutboxStore origin metadata is invalid");
            }
            std::vector<std::uint8_t> out;
            out.reserve(2u + 8u);
            detail::append_u16_le(out, origin_metadata_value_version());
            detail::append_u64_le(out, metadata.next_sequence);
            return out;
        }

        static OriginMetadata decode_origin_metadata(const MDBX_val& value) {
            if (value.iov_len != origin_metadata_value_size() ||
                value.iov_base == nullptr) {
                throw std::runtime_error(
                    "LogicalOutboxStore origin metadata has invalid size");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(value.iov_base);
            if (detail::read_u16_le(bytes) != origin_metadata_value_version()) {
                throw std::runtime_error(
                    "Unsupported LogicalOutboxStore origin metadata version");
            }
            OriginMetadata out;
            out.next_sequence = detail::read_u64_le(bytes + 2u);
            if (out.next_sequence == 0u) {
                throw std::runtime_error(
                    "LogicalOutboxStore origin metadata is invalid");
            }
            return out;
        }

        static std::vector<std::uint8_t> encode_metadata(
                const RouteMetadata& metadata) {
            if (metadata.known_tail < metadata.acknowledged_through ||
                is_zero_sync_id(metadata.origin_node_id)) {
                throw std::logic_error(
                    "LogicalOutboxStore route metadata is invalid");
            }
            std::vector<std::uint8_t> out;
            out.reserve(2u + 8u + 8u + metadata.origin_node_id.size());
            detail::append_u16_le(out, route_metadata_value_version());
            detail::append_u64_le(out, metadata.acknowledged_through);
            detail::append_u64_le(out, metadata.known_tail);
            out.insert(out.end(), metadata.origin_node_id.begin(),
                       metadata.origin_node_id.end());
            return out;
        }

        static RouteMetadata decode_metadata(const MDBX_val& value) {
            if (value.iov_len != route_metadata_value_size() ||
                value.iov_base == nullptr) {
                throw std::runtime_error(
                    "LogicalOutboxStore route metadata has invalid size");
            }
            const std::uint8_t* bytes =
                static_cast<const std::uint8_t*>(value.iov_base);
            if (detail::read_u16_le(bytes) != route_metadata_value_version()) {
                throw std::runtime_error(
                    "Unsupported LogicalOutboxStore route metadata version");
            }
            RouteMetadata out;
            out.acknowledged_through = detail::read_u64_le(bytes + 2u);
            out.known_tail = detail::read_u64_le(bytes + 10u);
            std::memcpy(out.origin_node_id.data(), bytes + 18u,
                        out.origin_node_id.size());
            if (out.known_tail < out.acknowledged_through ||
                is_zero_sync_id(out.origin_node_id)) {
                throw std::runtime_error(
                    "LogicalOutboxStore route metadata is invalid");
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

        static std::uint8_t key_version() { return 3u; }
        static std::uint8_t origin_metadata_key_kind() { return 0u; }
        static std::uint8_t route_metadata_key_kind() { return 1u; }
        static std::uint8_t entry_key_kind() { return 2u; }
        static std::uint16_t origin_metadata_value_version() { return 1u; }
        static std::uint16_t route_metadata_value_version() { return 1u; }
        static std::size_t entry_prefix_size() {
            return 2u + DbId().size() + NodeId().size();
        }
        static std::size_t entry_key_size() { return entry_prefix_size() + 8u; }
        static std::size_t origin_metadata_value_size() { return 2u + 8u; }
        static std::size_t route_metadata_value_size() {
            return 2u + 8u + 8u + NodeId().size();
        }

        MDBX_env* m_env;
        std::string m_dbi_name;
        mutable MDBX_dbi m_dbi;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_STORES_LOGICAL_OUTBOX_STORE_HPP_INCLUDED
