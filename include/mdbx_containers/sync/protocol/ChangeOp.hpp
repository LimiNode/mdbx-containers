#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CHANGEOP_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CHANGEOP_HPP_INCLUDED

/// \file protocol/ChangeOp.hpp
/// \brief A single replicated operation: typed MDBX DBI write.

#include <cstdint>
#include <string>
#include <vector>

#include "codec_flags.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Low-level operation kind carried over the wire.
    enum class ChangeOpType : std::uint8_t {
        Put         = 0, ///< mdbx_put (upsert).
        Delete      = 1, ///< mdbx_del + identity_index tombstone.
        ClearTable  = 2, ///< Drop the entire DBI contents.
    };

    /// \brief Single raw DBI operation captured by the change recorder.
    /// \details \c storage_key is the MDBX key serialized by the table.
    /// \c dbi_flags carries the persistent MDBX DBI flags needed to open the
    /// destination DBI compatibly during apply. \c identity_key is the
    /// application-level identity (empty when equal to storage_key).
    /// \c revision_key is an optional application-level version.
    struct ChangeOp {
        ChangeOpType           op_type   = ChangeOpType::Put; ///< Operation kind: Put/Delete/ClearTable.
        std::uint32_t          op_flags  = OP_NONE;           ///< Per-op feature flags.
        std::uint32_t          dbi_flags = 0;                 ///< Raw MDBX DBI flags for \c dbi_name.
        std::string            dbi_name;                      ///< User table name (MDBX DBI name).
        std::vector<std::uint8_t> storage_key;                ///< Serialized MDBX key bytes.
        std::vector<std::uint8_t> value;                      ///< Serialized value bytes for Put.
        std::vector<std::uint8_t> identity_key;               ///< Optional logical identity key.
        std::vector<std::uint8_t> revision_key;               ///< Optional application revision/version key.
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_PROTOCOL_CHANGEOP_HPP_INCLUDED
