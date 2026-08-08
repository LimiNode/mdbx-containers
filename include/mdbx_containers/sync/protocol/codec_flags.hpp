#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CODEC_FLAGS_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CODEC_FLAGS_HPP_INCLUDED

/// \file protocol/codec_flags.hpp
/// \brief Codec flag bits for \c ChangeBatch and \c ChangeOp.

#include <cstdint>

namespace mdbxc {
namespace sync {

    /// \brief Bit flags for the \c ChangeBatch level.
    /// \note All unknown bits trigger a decoder error.
    enum ChangeBatchFlags : std::uint32_t {
        BATCH_NONE              = 0,
        BATCH_COMPRESSED_ZSTD   = 1u << 0, ///< Reserved, unsupported in v0.1.
        BATCH_HAS_MORE          = 1u << 1, ///< More chunks follow (full export).
    };

    /// \brief Bit flags for the \c ChangeOp level.
    /// \note All unknown bits trigger a decoder error.
    enum ChangeOpFlags : std::uint32_t {
        OP_NONE             = 0,
        OP_HAS_IDENTITY_KEY = 1u << 0, ///< identity_key is present and differs from storage_key.
        OP_HAS_REVISION_KEY = 1u << 1, ///< revision_key is present.
        OP_TOMBSTONE        = 1u << 2, ///< Delete-marker (value is absent).
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CODEC_FLAGS_HPP_INCLUDED
