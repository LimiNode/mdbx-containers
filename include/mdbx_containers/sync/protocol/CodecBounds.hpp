#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CODECBOUNDS_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CODECBOUNDS_HPP_INCLUDED

/// \file protocol/CodecBounds.hpp
/// \brief Hard-coded codec bound defaults.

#include <cstdint>

namespace mdbxc {
namespace sync {

    /// \brief Default structural limits enforced by the codec decoder.
    /// \details \c ChangeBatchCodec enforces batch and operation bounds.
    /// \c TransportMessageCodec also uses the transport-specific bounds below
    /// for request/response envelopes.
    struct CodecBounds {
        std::uint32_t max_ops_per_batch        = 10000;               ///< Max operations in one ChangeBatch.
        std::uint32_t max_dbi_name_len         = 256;                 ///< Max DBI name bytes in one ChangeOp.
        std::uint32_t max_storage_key_len      = 16u * 1024u;         ///< Max physical key bytes in one ChangeOp.
        std::uint32_t max_value_len            = 4u * 1024u * 1024u;  ///< Max value bytes in one ChangeOp.
        std::uint32_t max_identity_key_len     = 16u * 1024u;         ///< Max logical identity key bytes in one ChangeOp.
        std::uint32_t max_revision_key_len     = 16u * 1024u;         ///< Max revision key bytes in one ChangeOp.
        std::uint32_t max_batch_total_bytes    = 64u * 1024u * 1024u; ///< Max encoded ChangeBatch bytes.
        std::uint32_t max_cursor_origins       = 10000;               ///< Max origins in one transport cursor.
        std::uint32_t max_batches_per_message  = 10000;               ///< Max batches in one pull/push transport message.
        std::uint32_t max_error_len            = 16u * 1024u;         ///< Max transport error string bytes.
        std::uint32_t max_logical_schema_id_len = 16u * 1024u;        ///< Max logical frame schema id bytes.
        std::uint32_t max_logical_delivery_frame_id_len =
            16u * 1024u; ///< Max logical delivery frame id bytes.
        std::uint32_t max_transport_message_bytes =
            128u * 1024u * 1024u; ///< Max encoded transport message bytes.
        std::uint32_t max_snapshot_id_len = 256u; ///< Max snapshot identity bytes.
        std::uint32_t max_snapshot_manifest_entries = 10000u; ///< Max DBIs in one snapshot manifest.
        std::uint32_t max_snapshot_chunk_bytes =
            128u * 1024u * 1024u; ///< Max encoded full-snapshot chunk bytes.
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CODECBOUNDS_HPP_INCLUDED
