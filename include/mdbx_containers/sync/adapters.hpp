#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_HPP_INCLUDED

/// \file sync/adapters.hpp
/// \brief Umbrella for logical adapters bound to MDBX Containers table types.
/// \details
/// Supplies table and logical-sync prerequisites before the adapter leaf
/// headers. Include this header or \c sync/logical.hpp instead of including a
/// header under \c sync/logical/adapters directly.

#include <mdbx_containers/sync/sync_module.hpp>

#if MDBXC_SYNC_ENABLED
#include <mdbx_containers/common.hpp>
#include <mdbx_containers/common/MdbxException.hpp>
#include <mdbx_containers/detail/utils.hpp>
#include <mdbx_containers/KeyValueTable.hpp>
#include <mdbx_containers/KeyTable.hpp>
#include <mdbx_containers/KeyMultiValueTable.hpp>
#include <mdbx_containers/KeyOrderedMultiValueTable.hpp>
#include <mdbx_containers/vector/VectorStore.hpp>
#include <mdbx_containers/sync/common.hpp>
#include <mdbx_containers/sync/logical/ILogicalDeliveryOutbox.hpp>
#include <mdbx_containers/sync/logical/LogicalTableAdapter.hpp>
#include <mdbx_containers/sync/logical/logical_schema_validation.hpp>
#include <mdbx_containers/sync/stores/MetaStore.hpp>
#include <mdbx_containers/sync/logical/adapters/KeyValueTableLogicalAdapter.hpp>
#include <mdbx_containers/sync/logical/adapters/KeyTableLogicalAdapter.hpp>
#include <mdbx_containers/sync/logical/adapters/KeyMultiValueTableLogicalAdapter.hpp>
#include <mdbx_containers/sync/logical/adapters/KeyOrderedMultiValueTableLogicalAdapter.hpp>
#include <mdbx_containers/sync/logical/adapters/KeyOrderedMultiValueDestructiveState.hpp>
#include <mdbx_containers/sync/logical/adapters/KeyOrderedMultiValueTableDestructiveLogicalAdapter.hpp>
#include <mdbx_containers/sync/logical/adapters/VectorStoreLogicalAdapter.hpp>
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_HPP_INCLUDED
