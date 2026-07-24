#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_I_SYNC_CAPTURE_SINK_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_I_SYNC_CAPTURE_SINK_HPP_INCLUDED

/// \file ISyncCaptureSink.hpp
/// \brief Bridge between \c Connection / \c BaseTable writes and the sync
/// subsystem's per-transaction change recorder.
/// \details
/// \c Connection holds a non-owning pointer to an \c ISyncCaptureSink set via
/// \c Connection::attach_sync_capture(). \c BaseTable::record_op() forwards
/// every successful write through this sink. \c Transaction::commit() calls
/// \c flush_in_txn() on the same write transaction so the captured batch is
/// written to \c _mdbxc_changelog atomically with the user-visible change.

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "sync_module.hpp"

#if MDBXC_SYNC_ENABLED
#include <mdbx.h>

#include "ChangeOp.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Interface between mdbxc-core write paths and the sync recorder.
    /// \details The implementation owns thread-local pending state, decides
    /// when to assemble a batch, and writes it to \c _mdbxc_changelog inside
    /// the same write transaction that performed the user-visible change.
    class ISyncCaptureSink {
    public:
        virtual ~ISyncCaptureSink() = default;

        /// \brief Called by \c BaseTable after a successful user-table write.
        /// \param txn The active MDBX write transaction that performed the
        ///        change.
        /// \param change Full raw-domain change operation.
        /// \details Default implementation forwards to the legacy raw-field
        /// overload only when \p change contains no enriched metadata. New
        /// sinks should override this overload when they need access to the
        /// complete \c ChangeOp shape.
        virtual void record_change_op(MDBX_txn* txn, const ChangeOp& change) {
            if (change.op_flags != OP_NONE ||
                !change.identity_key.empty() ||
                !change.revision_key.empty()) {
                throw std::logic_error(
                    "ISyncCaptureSink legacy record_change_op cannot accept enriched ChangeOp");
            }
            record_change(txn, change.dbi_name, change.op_type,
                          change.dbi_flags, change.storage_key, change.value);
        }

        /// \brief Legacy raw-field capture entry point.
        /// \details Existing sinks may continue overriding this overload.
        /// New code should prefer \c record_change_op(MDBX_txn*, const ChangeOp&).
        /// \param txn The active MDBX write transaction that performed the
        ///        change. Implementations may stage the op in thread-local
        ///        memory and defer the on-disk write to \c flush_in_txn.
        /// \param dbi_name Name of the user table (the DBI name as passed
        ///        to the table constructor).
        /// \param op_type Kind of write (put/delete/clear).
        /// \param dbi_flags MDBX DBI flags reported by \c mdbx_dbi_flags()
        ///        for the user table, used by apply to open compatible DBIs.
        /// \param storage_key Serialized MDBX key bytes of the touched record.
        /// \param value Serialized MDBX value bytes for \c Put; empty for
        ///        \c Delete / \c ClearTable.
        virtual void record_change(MDBX_txn* txn,
                                   const std::string& dbi_name,
                                   ChangeOpType op_type,
                                   std::uint32_t dbi_flags,
                                   const std::vector<std::uint8_t>& storage_key,
                                   const std::vector<std::uint8_t>& value) = 0;

        /// \brief Called by \c Transaction::commit() before the actual commit.
        /// \param txn The about-to-commit write transaction.
        /// \details Implementations must write any pending captured changes to
        /// \c _mdbxc_changelog within \p txn so that user-visible writes and
        /// their changelog entry land or fail atomically together.
        virtual void flush_in_txn(MDBX_txn* txn) = 0;

        /// \brief Discards any pending ops recorded for a transaction that
        /// is about to be aborted or rolled back.
        /// \param txn The about-to-be-aborted write transaction.
        /// \details Default implementation is a no-op; overloads drop the
        /// pending ops so the next transaction on the same thread (or the
        /// next MDBX_txn* address if the allocator reuses it) starts clean.
        virtual void discard_txn(MDBX_txn* txn) noexcept {
            (void)txn;
        }
    };

    /// \brief Adapter base for sinks that want to implement only the full
    ///        \c ChangeOp entry point.
    /// \details The legacy raw-field overload is still the abstract compatibility
    /// contract of \c ISyncCaptureSink. Derive from this helper when new code
    /// wants every raw-shaped operation converted into a \c ChangeOp object.
    class FullChangeSyncCaptureSink : public ISyncCaptureSink {
    public:
        void record_change(MDBX_txn* txn,
                           const std::string& dbi_name,
                           ChangeOpType op_type,
                           std::uint32_t dbi_flags,
                           const std::vector<std::uint8_t>& storage_key,
                           const std::vector<std::uint8_t>& value) override {
            ChangeOp op;
            op.op_type = op_type;
            op.dbi_flags = dbi_flags;
            op.dbi_name = dbi_name;
            op.storage_key = storage_key;
            op.value = value;
            record_change_op(txn, op);
        }

        void record_change_op(MDBX_txn* txn,
                              const ChangeOp& change) override = 0;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_I_SYNC_CAPTURE_SINK_HPP_INCLUDED
#endif // MDBXC_SYNC_ENABLED guard
