#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_CAPTURE_ILOGICALDBICAPTURE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_CAPTURE_ILOGICALDBICAPTURE_HPP_INCLUDED

/// \file capture/ILogicalDbiCapture.hpp
/// \brief Type-erased transaction-bound logical capture for one bound DBI.

#if MDBXC_SYNC_ENABLED

#include <cstdint>
#include <string>

#include <mdbx.h>

namespace mdbxc {
namespace sync {

    /// \brief Type-erased capture contract owned by \c Connection for a
    ///        logical table binding.
    /// \details The concrete adapter owns codec-specific conversion from the
    /// public table values represented by the opaque pointers into a logical
    /// change. \c Connection owns the registration and invokes flush/discard
    /// at the same transaction lifecycle points as raw capture.
    class ILogicalDbiCapture {
    public:
        virtual ~ILogicalDbiCapture() = default;

        virtual const std::string& dbi_name() const = 0;
        virtual LogicalSchemaRef schema_ref() const = 0;
        virtual const DbId& destination() const = 0;

        virtual void record_insert(MDBX_txn* txn,
                                   const void* key,
                                   const void* value) = 0;
        virtual void record_erase_key(MDBX_txn* txn, const void* key) = 0;
        virtual void record_erase_all_values(MDBX_txn* txn,
                                              const void* key,
                                              const void* value) = 0;
        virtual void record_erase_one_value(MDBX_txn* txn,
                                             const void* key,
                                             const void* value) = 0;
        virtual void record_clear(MDBX_txn* txn) = 0;

        virtual void flush_in_txn(MDBX_txn* txn) = 0;
        virtual void discard_txn(MDBX_txn* txn) noexcept = 0;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBXC_SYNC_ENABLED
#endif // MDBX_CONTAINERS_HEADER_SYNC_CAPTURE_ILOGICALDBICAPTURE_HPP_INCLUDED
