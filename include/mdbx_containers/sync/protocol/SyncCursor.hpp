#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_SYNCCURSOR_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_SYNCCURSOR_HPP_INCLUDED

/// \file protocol/SyncCursor.hpp
/// \brief Vector-clock style replication cursor.

#include <cstdint>
#include <map>


namespace mdbxc {
namespace sync {

    /// \brief Per-origin sequence cursor used by sync peers.
    /// \details The cursor maps an origin \c NodeId to the highest contiguous
    /// \c seq number already applied locally. Incremental sync asks the remote
    /// for batches strictly above these numbers.
    struct SyncCursor {
        std::map<NodeId, std::uint64_t> last_seq_by_origin;

        /// \brief Returns the last contiguous applied \c seq for \p origin.
        std::uint64_t last_seq_for(const NodeId& origin) const {
            const std::map<NodeId, std::uint64_t>::const_iterator it =
                last_seq_by_origin.find(origin);
            return it == last_seq_by_origin.end() ? 0ULL : it->second;
        }

        /// \brief Returns \c true when there is no record for \p origin.
        bool is_empty_for(const NodeId& origin) const {
            return last_seq_by_origin.find(origin) == last_seq_by_origin.end();
        }
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_SYNCCURSOR_HPP_INCLUDED
