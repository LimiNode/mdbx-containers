#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_SCHEMA_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_SCHEMA_HPP_INCLUDED

/// \file LogicalSchema.hpp
/// \brief Lightweight logical sync schema model shared by stores and adapters.

#include <cstdint>

namespace mdbxc {
namespace sync {

    /// \brief Logical table kinds reserved for future non-raw sync adapters.
    enum class LogicalTableKind : std::uint16_t {
        Unknown              = 0, ///< Invalid placeholder.
        KeyMultiValue        = 1, ///< Unordered key-to-multiple-values table.
        KeyOrderedMultiValue = 2, ///< Key-to-multiple-values table with presentation order.
        AnyValue             = 3, ///< Dynamically typed value table.
        HashedKeyValue       = 4, ///< Hashed key-value store.
    };

    /// \brief Returns true when \p kind is a supported logical table kind.
    inline bool is_known_logical_table_kind(LogicalTableKind kind) {
        switch (kind) {
            case LogicalTableKind::KeyMultiValue:
            case LogicalTableKind::KeyOrderedMultiValue:
            case LogicalTableKind::AnyValue:
            case LogicalTableKind::HashedKeyValue:
                return true;
            case LogicalTableKind::Unknown:
                return false;
        }
        return false;
    }

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_SCHEMA_HPP_INCLUDED
