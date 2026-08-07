#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CHANGEBATCH_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CHANGEBATCH_HPP_INCLUDED

/// \file protocol/ChangeBatch.hpp
/// \brief A single atomic group of replicated \c ChangeOp operations.

#include <cstdint>
#include <vector>

#include "ChangeOp.hpp"
#include "codec_flags.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Versioned group of operations emitted by one origin node.
    /// \invariant All operations in a batch are recorded and applied under a
    /// single MDBX write transaction.
    struct ChangeBatch {
        std::uint32_t         version = 1;      ///< Schema version; always 1 in v0.1.
        std::uint32_t         batch_flags = BATCH_NONE; ///< Per-batch feature flags (BATCH_NONE, BATCH_HAS_MORE, ...).
        NodeId                origin_node_id{}; ///< Identifier of the node that produced this batch.
        std::uint64_t         seq = 0;          ///< Monotonic per-node sequence number assigned by MetaStore::increment_local_seq.
        std::uint64_t         time_unix_ns = 0; ///< Physical timestamp metadata only.
        std::vector<ChangeOp> ops;              ///< List of operations in this batch; order is preserved on apply.
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CHANGEBATCH_HPP_INCLUDED
