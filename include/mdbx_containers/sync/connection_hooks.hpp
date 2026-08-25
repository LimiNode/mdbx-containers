#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_CONNECTION_HOOKS_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_CONNECTION_HOOKS_HPP_INCLUDED

/// \file sync/connection_hooks.hpp
/// \brief Prerequisites for the logical capture hook used by \c Connection.
/// \details Internal integration seam included by \c common.hpp before
/// \c Connection. Public users include \c mdbx_containers/common.hpp or a
/// higher-level aggregate instead of this header.

#include "config.hpp"

#if MDBXC_SYNC_ENABLED
#include <mdbx_containers/detail/utils.hpp>
#include "common.hpp"
#include "logical/LogicalChange.hpp"
#include "capture/ILogicalDbiCapture.hpp"
#include "logical/stores/LogicalDbiBindingStore.hpp"
#include "stores/MetaStore.hpp"
#include "stores/SelectiveReplicationStore.hpp"
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_CONNECTION_HOOKS_HPP_INCLUDED
