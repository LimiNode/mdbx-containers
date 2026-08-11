#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_HPP_INCLUDED

/// \file sync/adapters.hpp
/// \brief Umbrella for logical adapters bound to MDBX Containers table types.
/// \details
/// Supplies table and logical-sync prerequisites before the adapter leaf
/// headers. Include this header instead of including a header under
/// \c sync/logical/adapters directly.

#include "config.hpp"

#if MDBXC_SYNC_ENABLED
#include <mdbx_containers/tables.hpp>
#include <mdbx_containers/vector.hpp>
#include "capture.hpp"
#include "storage.hpp"
#include "logical.hpp"
#include "logical/adapters/detail/blob_payload.hpp"
#include "logical/adapters/detail/captured_logical_transaction.hpp"
#include "logical/adapters/KeyValueTableLogicalAdapter.hpp"
#include "logical/adapters/VersionedKeyValueTable.hpp"
#include "logical/adapters/KeyTableLogicalAdapter.hpp"
#include "logical/adapters/KeyMultiValueTableLogicalAdapter.hpp"
#include "logical/adapters/KeyOrderedMultiValueTableLogicalAdapter.hpp"
#include "logical/adapters/KeyOrderedMultiValueDestructiveState.hpp"
#include "logical/adapters/KeyOrderedMultiValueTableDestructiveLogicalAdapter.hpp"
#include "logical/adapters/VectorStoreLogicalAdapter.hpp"
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_HPP_INCLUDED
