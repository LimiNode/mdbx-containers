#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_DELIVERY_ENVELOPE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_DELIVERY_ENVELOPE_HPP_INCLUDED

/// \file logical/LogicalDeliveryEnvelope.hpp
/// \brief Retry-safe delivery metadata for logical change frames.

#include <cstdint>
#include <string>

#include <mdbx_containers/sync/protocol/CodecBounds.hpp>
#include "LogicalChange.hpp"
#include <mdbx_containers/sync/common.hpp>

namespace mdbxc {
namespace sync {

    /// \brief Logical frame plus delivery identity.
    /// \details This envelope is the replay-safe layer above
    /// \c LogicalChangeFrame. The frame remains an adapter payload container;
    /// the envelope supplies destination routing and stable origin identity
    /// that receivers can persist atomically with logical mutations.
    struct LogicalDeliveryEnvelope {
        DbId destination_db_uuid{};       ///< Expected receiver database id.
        NodeId origin_node_id{};          ///< Stable sender node id.
        std::uint64_t origin_sequence = 0;///< Sender-side sequence component.
        std::string frame_id;             ///< Stable sender frame id.
        LogicalChangeFrame frame;         ///< Logical payload.
    };

    /// \brief Result of an external per-origin delivery ordering check.
    enum class LogicalDeliveryOrderStatus {
        InOrder, ///< Observed sequence equals expected next sequence.
        SequenceBehindWatermark, ///< Observed sequence is below the watermark.
        SequenceGap ///< Observed sequence is above expected next sequence.
    };

    /// \brief External ordering check outcome for one delivery envelope.
    /// \details This helper is intentionally stateless. It does not persist a
    /// watermark, buffer gaps, or affect engine apply semantics.
    struct LogicalDeliveryOrderResult {
        LogicalDeliveryOrderStatus status =
            LogicalDeliveryOrderStatus::SequenceGap;
        std::uint64_t expected_sequence = 0;
        std::uint64_t observed_sequence = 0;

        bool can_apply_without_buffering() const {
            return status == LogicalDeliveryOrderStatus::InOrder;
        }
    };

    /// \brief Classifies \p envelope against a caller-owned next sequence.
    inline LogicalDeliveryOrderResult check_logical_delivery_order(
            const LogicalDeliveryEnvelope& envelope,
            std::uint64_t expected_next_sequence) {
        LogicalDeliveryOrderResult out;
        out.expected_sequence = expected_next_sequence;
        out.observed_sequence = envelope.origin_sequence;
        if (envelope.origin_sequence == expected_next_sequence) {
            out.status = LogicalDeliveryOrderStatus::InOrder;
        } else if (envelope.origin_sequence < expected_next_sequence) {
            out.status =
                LogicalDeliveryOrderStatus::SequenceBehindWatermark;
        } else {
            out.status = LogicalDeliveryOrderStatus::SequenceGap;
        }
        return out;
    }

    /// \brief Returns true when \p id is all zeros.
    inline bool is_zero_sync_id(const NodeId& id) {
        const NodeId zero{};
        return compare_node_id(id, zero) == 0;
    }

    /// \brief Returns true when \p envelope has required delivery identity.
    inline bool is_logical_delivery_envelope_complete(
            const LogicalDeliveryEnvelope& envelope) {
        return !is_zero_sync_id(envelope.destination_db_uuid) &&
               !is_zero_sync_id(envelope.origin_node_id) &&
               envelope.origin_sequence != 0 &&
               !envelope.frame_id.empty();
    }

    /// \brief Returns effective delivery envelope codec bounds.
    inline const CodecBounds& logical_delivery_effective_bounds(
            const CodecBounds* bounds) {
        static const CodecBounds defaults;
        return bounds != nullptr ? *bounds : defaults;
    }

    /// \brief Throws when delivery identity violates structural bounds.
    inline void validate_logical_delivery_envelope(
            const LogicalDeliveryEnvelope& envelope,
            const CodecBounds* bounds = nullptr) {
        const CodecBounds& effective =
            logical_delivery_effective_bounds(bounds);
        if (!is_logical_delivery_envelope_complete(envelope)) {
            throw std::runtime_error(
                "Logical delivery envelope identity is incomplete");
        }
        if (envelope.frame_id.size() >
            effective.max_logical_delivery_frame_id_len) {
            throw std::length_error(
                "logical delivery frame id exceeds "
                "max_logical_delivery_frame_id_len");
        }
    }

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_DELIVERY_ENVELOPE_HPP_INCLUDED
