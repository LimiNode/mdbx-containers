#pragma once
#ifndef MDBX_CONTAINERS_HEADER_ID_ALLOCATOR_TABLE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_ID_ALLOCATOR_TABLE_HPP_INCLUDED

/// \file IdAllocatorTable.hpp
/// \brief Durable transaction-aware uint64_t identifier allocator.

#include "common.hpp"

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>

namespace mdbxc {

    /// \class IdAllocatorTable
    /// \ingroup mdbxc_tables
    /// \brief Allocates positive uint64_t identifiers from a dedicated MDBX DBI.
    /// \details
    /// Stores one internal counter value. Allocations and resets become durable
    /// only when their writable transaction commits; rollback restores the
    /// previous value. The allocator starts at zero and \c next() returns one
    /// for its first successful allocation.
    ///
    /// \warning This is a local allocator, not a distributed identity scheme.
    ///          Applications with multiple independent writers or replicas must
    ///          provide a separate identity and conflict-resolution contract.
    class IdAllocatorTable final : public BaseTable {
    public:
        /// \brief Constructs an allocator using an existing connection.
        /// \param connection Existing connection.
        /// \param name Name of the allocator DBI.
        /// \param flags Additional MDBX database flags.
        explicit IdAllocatorTable(
            std::shared_ptr<Connection> connection,
            std::string name = "id_allocator",
            MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(std::move(connection),
                        std::move(name),
                        flags | get_mdbx_flags<std::uint32_t>()) {}

        /// \brief Constructs an allocator using configuration settings.
        /// \param config Configuration settings.
        /// \param name Name of the allocator DBI.
        /// \param flags Additional MDBX database flags.
        explicit IdAllocatorTable(
            const Config& config,
            std::string name = "id_allocator",
            MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(Connection::create(config),
                        std::move(name),
                        flags | get_mdbx_flags<std::uint32_t>()) {}

        ~IdAllocatorTable() override = default;

        /// \brief Returns the last allocated identifier, or zero before allocation.
        /// \param txn Optional transaction handle.
        std::uint64_t current(MDBX_txn* txn = nullptr) const {
            std::uint64_t result = 0;
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_current(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Returns the last allocated identifier using an external transaction.
        std::uint64_t current(const Transaction& txn) const {
            return current(txn.handle());
        }

        /// \brief Allocates and returns the next positive identifier.
        /// \param txn Optional writable transaction handle.
        /// \throws std::overflow_error if the counter is already at uint64_t max.
        std::uint64_t next(MDBX_txn* txn = nullptr) {
            std::uint64_t result = 0;
            with_transaction([this, &result](MDBX_txn* t) {
                ensure_writable(t, "IdAllocatorTable::next");
                result = db_next(t);
            }, TransactionMode::WRITABLE, txn);
            return result;
        }

        /// \brief Allocates an identifier using an external writable transaction.
        std::uint64_t next(const Transaction& txn) {
            return next(txn.handle());
        }

        /// \brief Sets the last allocated identifier.
        /// \details Passing zero resets the allocator so the next successful
        ///          allocation returns one.
        /// \param value Value reported by subsequent \c current() calls.
        /// \param txn Optional writable transaction handle.
        void reset_to(std::uint64_t value, MDBX_txn* txn = nullptr) {
            with_transaction([this, value](MDBX_txn* t) {
                ensure_writable(t, "IdAllocatorTable::reset_to");
                db_set_current(value, t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Resets the allocator so the next successful allocation returns one.
        /// \param txn Optional writable transaction handle.
        void reset(MDBX_txn* txn = nullptr) {
            reset_to(0u, txn);
        }

        /// \brief Sets the last allocated identifier using an external writable transaction.
        void reset_to(std::uint64_t value, const Transaction& txn) {
            reset_to(value, txn.handle());
        }

        /// \brief Resets the allocator using an external writable transaction.
        void reset(const Transaction& txn) {
            reset(txn.handle());
        }

    private:
        static MDBX_val make_counter_key(SerializeScratch& scratch) {
            const std::uint32_t key = 0u;
            return serialize_key<true>(key, scratch);
        }

        static void ensure_writable(MDBX_txn* txn, const char* operation) {
            const MDBX_txn_flags_t flags = mdbx_txn_flags(txn);
            if ((static_cast<int>(flags) & static_cast<int>(MDBX_TXN_RDONLY)) != 0) {
                throw std::invalid_argument(std::string(operation) +
                                            " requires a writable transaction");
            }
        }

        std::uint64_t db_current(MDBX_txn* txn) const {
            SerializeScratch scratch;
            MDBX_val key = make_counter_key(scratch);
            MDBX_val value;
            const int rc = mdbx_get(txn, m_dbi, &key, &value);
            if (rc == MDBX_NOTFOUND) {
                return 0u;
            }
            check_mdbx(rc, "Failed to read ID allocator counter");
            return deserialize_value<std::uint64_t>(value);
        }

        std::uint64_t db_next(MDBX_txn* txn) {
            const std::uint64_t previous = db_current(txn);
            if (previous == (std::numeric_limits<std::uint64_t>::max)()) {
                throw std::overflow_error("IdAllocatorTable::next: id overflow");
            }
            const std::uint64_t result = previous + 1u;
            db_set_current(result, txn);
            return result;
        }

        void db_set_current(std::uint64_t value, MDBX_txn* txn) {
            SerializeScratch key_scratch;
            SerializeScratch value_scratch;
            MDBX_val key = make_counter_key(key_scratch);
            MDBX_val db_value = serialize_value(value, value_scratch);
            check_mdbx(mdbx_put(txn, m_dbi, &key, &db_value, MDBX_UPSERT),
                       "Failed to write ID allocator counter");
#       if MDBXC_SYNC_ENABLED
            record_op(txn, sync::ChangeOpType::Put,
                      capture_bytes(key), capture_bytes(db_value));
#       endif
        }
    };

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_ID_ALLOCATOR_TABLE_HPP_INCLUDED
