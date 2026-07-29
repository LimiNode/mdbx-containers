#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ILOGICAL_DELIVERY_OUTBOX_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ILOGICAL_DELIVERY_OUTBOX_HPP_INCLUDED

/// \file ILogicalDeliveryOutbox.hpp
/// \brief Transaction-bound publisher for ordered logical delivery frames.

#include <mdbx.h>

#include "LogicalDeliveryEnvelope.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Persists logical deliveries in a caller-owned write transaction.
    /// \details Implementations must not commit or roll back \p txn. This lets
    /// a logical capture session commit table mutations and its delivery
    /// envelope atomically.
    class ILogicalDeliveryOutbox {
    public:
        virtual ~ILogicalDeliveryOutbox() {}

        virtual LogicalDeliveryEnvelope enqueue_logical_delivery(
                MDBX_txn* txn,
                const DbId& destination,
                const LogicalChangeFrame& frame,
                const CodecBounds* bounds = nullptr) = 0;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ILOGICAL_DELIVERY_OUTBOX_HPP_INCLUDED
