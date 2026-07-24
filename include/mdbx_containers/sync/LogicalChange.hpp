#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_CHANGE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_CHANGE_HPP_INCLUDED

/// \file LogicalChange.hpp
/// \brief Public model for future logical table sync operations.

#include <cstdint>
#include <string>
#include <vector>

#include "LogicalSchema.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Distinguishes current raw DBI operations from future logical ops.
    enum class ChangeDomain : std::uint8_t {
        RawDbi       = 0, ///< Existing \c ChangeOp raw DBI write domain.
        LogicalTable = 1, ///< Future adapter-driven logical table domain.
    };

    /// \brief Stable reference to a registered logical schema.
    /// \details \c schema_id must match a record in \c SchemaRegistryStore
    /// before a receiver can apply \c LogicalChange payloads for it.
    struct LogicalSchemaRef {
        std::string schema_id; ///< Application-defined schema id.
        LogicalTableKind kind; ///< Expected adapter kind.
        std::uint32_t schema_version; ///< Expected application schema version.

        LogicalSchemaRef()
            : kind(LogicalTableKind::Unknown),
              schema_version(0) {}

        LogicalSchemaRef(const std::string& id,
                         LogicalTableKind table_kind,
                         std::uint32_t version)
            : schema_id(id),
              kind(table_kind),
              schema_version(version) {}
    };

    /// \brief Opaque logical table operation reserved for future adapters.
    /// \details The sync core treats \c payload as adapter-owned bytes. It
    /// must not guess table semantics from \c opcode.
    struct LogicalChange {
        LogicalSchemaRef schema;          ///< Target logical schema.
        std::uint32_t opcode;             ///< Adapter-local operation code.
        std::uint32_t flags;              ///< Reserved flags; must be zero for now.
        std::vector<std::uint8_t> payload; ///< Adapter-owned operation bytes.

        LogicalChange()
            : opcode(0),
              flags(0) {}

        LogicalChange(const LogicalSchemaRef& schema_ref,
                      std::uint32_t operation_code,
                      std::uint32_t operation_flags,
                      const std::vector<std::uint8_t>& operation_payload)
            : schema(schema_ref),
              opcode(operation_code),
              flags(operation_flags),
              payload(operation_payload) {}
    };

    /// \brief Returns true when \p ref names a concrete logical schema.
    inline bool is_logical_schema_ref_complete(const LogicalSchemaRef& ref) {
        return !ref.schema_id.empty() &&
               is_known_logical_table_kind(ref.kind) &&
               ref.schema_version != 0;
    }

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_CHANGE_HPP_INCLUDED
