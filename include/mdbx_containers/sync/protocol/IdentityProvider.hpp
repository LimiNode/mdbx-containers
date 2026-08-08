#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_IDENTITYPROVIDER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_IDENTITYPROVIDER_HPP_INCLUDED

/// \file protocol/IdentityProvider.hpp
/// \brief Application-level identity and revision keys for replication.

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "ChangeOp.hpp"
#include "codec_flags.hpp"

namespace mdbxc {
namespace sync {

    /// \brief View over a replicated record exposed to identity providers.
    struct RecordView {
        std::string dbi_name;                     ///< Per-batch feature flags (BATCH_NONE, BATCH_HAS_MORE, ...).
        ChangeOpType op_type = ChangeOpType::Put; ///< Identifier of the node that produced this batch.
        std::vector<std::uint8_t> storage_key;    ///< Monotonic per-node sequence number assigned by MetaStore::increment_local_seq.
        std::vector<std::uint8_t> value;          ///< List of operations in this batch; order is preserved on apply.
    };

    /// \brief Optional identity and revision for a replicated record.
    /// \details \c has_identity = false disables identity indexing for this
    /// record; conflict and dedup detection then fall back to \c storage_key.
    struct RecordIdentity {
        bool has_identity = false;
        std::vector<std::uint8_t> identity_key;
        bool has_revision = false;
        std::vector<std::uint8_t> revision_key;
    };

    /// \brief Application-supplied callback that derives identity and
    /// revision keys from a \c RecordView.
    using IdentityProvider = std::function<RecordIdentity(const RecordView&)>;

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_IDENTITYPROVIDER_HPP_INCLUDED
