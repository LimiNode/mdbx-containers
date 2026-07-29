#pragma once
#ifndef MDBX_CONTAINERS_HEADER_TABLE_SEQUENCE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_TABLE_SEQUENCE_HPP_INCLUDED

/// \file TableSequence.hpp
/// \brief Transactional MDBX sequence allocator for an existing table DBI.

#include "common.hpp"

#include <cstdint>
#include <limits>
#include <stdexcept>

namespace mdbxc {

    /// \class TableSequence
    /// \ingroup mdbxc_tables
    /// \brief Allocates positive, per-table uint64_t identifiers through MDBX.
    /// \details
    /// Binds to the DBI of an existing table wrapper and delegates allocation to
    /// \c mdbx_dbi_sequence(). Allocations become durable only when the
    /// caller-owned writable transaction commits, and are discarded when it
    /// rolls back. The sequence is independent from any keys stored in the
    /// table; it does not inspect records and does not emit a separate sync
    /// change operation for sequence metadata.
    ///
    /// \warning This class does not create global identifiers across databases
    /// or replicas. It allocates one local sequence for one MDBX DBI.
    class TableSequence {
    public:
        /// \brief Binds the allocator to an existing table's DBI.
        /// \param table Existing table wrapper. Its connection lifecycle must
        /// remain active while this allocator is used.
        explicit TableSequence(const BaseTable& table)
            : m_connection(table.connection())
            , m_dbi(table.handle()) {}

        /// \brief Returns the current sequence value in a caller-owned transaction.
        /// \param txn Read-only or writable transaction for the bound connection.
        /// \return The most recently allocated value, or zero before allocation.
        std::uint64_t current(MDBX_txn* txn) const {
            MDBX_txn* checked = checked_transaction(txn, "TableSequence::current");
            std::uint64_t result = 0;
            check_mdbx(mdbx_dbi_sequence(checked, m_dbi, &result, 0u),
                       "Failed to read table sequence");
            return result;
        }

        /// \brief Returns the current sequence value in a managed transaction.
        std::uint64_t current(const Transaction& txn) const {
            return current(txn.handle());
        }

        /// \brief Allocates one positive identifier in a writable transaction.
        /// \return The allocated identifier.
        std::uint64_t next(MDBX_txn* txn) const {
            return reserve(1u, txn);
        }

        /// \brief Allocates one positive identifier in a managed writable transaction.
        std::uint64_t next(const Transaction& txn) const {
            return next(txn.handle());
        }

        /// \brief Reserves a contiguous range of positive identifiers.
        /// \param count Number of identifiers to reserve; must be non-zero.
        /// \param txn Writable transaction for the bound connection.
        /// \return The first identifier in the reserved range.
        /// \throws std::overflow_error if the requested range exceeds uint64_t.
        std::uint64_t reserve(std::uint64_t count, MDBX_txn* txn) const {
            if (count == 0u) {
                throw std::invalid_argument(
                    "TableSequence::reserve count must be non-zero");
            }

            MDBX_txn* checked = checked_transaction(txn, "TableSequence::reserve");
            const MDBX_txn_flags_t flags = mdbx_txn_flags(checked);
            if ((static_cast<int>(flags) & static_cast<int>(MDBX_TXN_RDONLY)) != 0) {
                throw std::invalid_argument(
                    "TableSequence::reserve requires a writable transaction");
            }

            std::uint64_t previous = 0;
            const int rc = mdbx_dbi_sequence(checked, m_dbi, &previous, count);
            if (rc == MDBX_RESULT_TRUE) {
                throw std::overflow_error(
                    "TableSequence::reserve would overflow uint64_t");
            }
            check_mdbx(rc, "Failed to reserve table sequence");

            if (previous == (std::numeric_limits<std::uint64_t>::max)()) {
                throw std::overflow_error(
                    "TableSequence::reserve would overflow uint64_t");
            }
            return previous + 1u;
        }

        /// \brief Reserves a contiguous range in a managed writable transaction.
        std::uint64_t reserve(std::uint64_t count, const Transaction& txn) const {
            return reserve(count, txn.handle());
        }

    private:
        MDBX_txn* checked_transaction(MDBX_txn* txn, const char* context) const {
            MDBX_txn* checked = checked_txn_env(
                txn, m_connection->env_handle(), context);
#       if MDBXC_SYNC_ENABLED
            m_connection->ensure_sync_capture_txn_supported(checked, context);
#       endif
            return checked;
        }

        std::shared_ptr<Connection> m_connection;
        MDBX_dbi m_dbi;
    };

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_TABLE_SEQUENCE_HPP_INCLUDED
