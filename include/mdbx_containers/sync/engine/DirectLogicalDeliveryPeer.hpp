#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ENGINE_DIRECT_LOGICAL_DELIVERY_PEER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ENGINE_DIRECT_LOGICAL_DELIVERY_PEER_HPP_INCLUDED

/// \file engine/DirectLogicalDeliveryPeer.hpp
/// \brief In-process ordered logical delivery peer for tests and local tools.

#include "SyncEngine.hpp"

namespace mdbxc {
namespace sync {

    class DirectLogicalDeliveryPeer : public ILogicalDeliveryPeer {
    public:
        explicit DirectLogicalDeliveryPeer(SyncEngine& remote)
            : m_remote(remote) {}

        LogicalDeliveryHello logical_delivery_hello() override {
            return m_remote.logical_delivery_hello();
        }

        LogicalDeliveryAcknowledgement deliver_ordered_logical_delivery(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr) override {
            return m_remote.apply_ordered_logical_delivery_envelope(
                envelope, bounds);
        }

        LogicalDeliveryAcknowledgement deliver_ordered_logical_request(
                const LogicalDeliveryRequest& request,
                const CodecBounds* bounds = nullptr) override {
            return m_remote.apply_ordered_logical_delivery_envelope(
                request.envelope, &request.sender_capabilities, bounds);
        }

    private:
        SyncEngine& m_remote;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ENGINE_DIRECT_LOGICAL_DELIVERY_PEER_HPP_INCLUDED
