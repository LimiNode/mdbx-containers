#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_RECOVERY_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_RECOVERY_HPP_INCLUDED

/// \file logical/logical_recovery.hpp
/// \brief Explicit baseline DTOs for logical-aware fresh-replica recovery.

#include <cstdint>
#include <string>
#include <vector>

#include "../protocol.hpp"
#include "../protocol/FullSnapshotProtocol.hpp"
#include "stores/LogicalDeliveryOrderStore.hpp"
#include "stores/LogicalDeliveryStore.hpp"
#include "stores/SchemaRegistryStore.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Immutable logical metadata captured with one physical snapshot.
    /// \details The physical user-DBI snapshot remains represented by
    /// \c FullSnapshotChunk pages. This DTO supplies the state that raw
    /// snapshots intentionally exclude: schema markers, replay identity,
    /// ordered receiver frontiers, and the source outbox tail for the recovered
    /// destination. It is committed with the final physical replacement page.
    struct LogicalRecoveryBaseline {
        NodeId source_node_id{};
        DbId source_db_uuid{};
        std::string snapshot_id;
        std::vector<LogicalSchemaRegistryEntry> schemas;
        std::vector<LogicalDeliveryMarkerInfo> delivery_markers;
        std::vector<LogicalDeliveryWatermarkInfo> delivery_watermarks;
        std::vector<LogicalDeliveryOrderEntry> delivery_order;
        /// \brief Source envelopes still awaiting this recovered destination.
        /// They become receiver replay markers, not receiver outbox entries.
        std::vector<LogicalDeliveryEnvelope> source_outbox_pending;
        std::uint64_t source_outbox_known_tail = 0u;
    };

    /// \brief One paged logical-aware recovery request.
    /// \details This is intentionally separate from raw \c PullRequest so a
    /// raw peer cannot accidentally bypass the CompleteUserDatabase logical
    /// state guard.
    struct LogicalRecoveryRequest {
        NodeId requester{};
        std::string snapshot_id;
        std::string continuation;
        std::uint64_t max_bytes = 4ULL * 1024ULL * 1024ULL;
        std::uint64_t max_single_batch_bytes = 4ULL * 1024ULL * 1024ULL;
    };

    /// \brief One logical-aware recovery page or structured failure.
    struct LogicalRecoveryResponse {
        bool ok = true;
        bool has_more = false;
        FullSnapshotChunk snapshot_chunk;
        /// \brief Present exactly on the final successful page.
        bool has_baseline = false;
        LogicalRecoveryBaseline baseline;
        std::string error;
        SyncResponseErrorCode error_code = SyncResponseErrorCode::None;
        bool error_retryable = false;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_RECOVERY_HPP_INCLUDED
