#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_HPP_INCLUDED

/// \file sync.hpp
/// \brief Aggregate header for the optional sync subsystem.
/// \details
/// Pulls in all foundation types and codec when \c MDBXC_SYNC_ENABLED is
/// non-zero. Otherwise the include is a no-op so applications that do not
/// need replication pay zero compile-time or runtime cost.

#include "sync/config.hpp"

#if MDBXC_SYNC_ENABLED
#include "sync/core.hpp"
#include "sync/protocol.hpp"
#include "sync/storage.hpp"
#include "sync/capture.hpp"
#include "sync/logical.hpp"
#include "sync/adapters.hpp"
#include "sync/engine.hpp"
#include "sync/transport.hpp"
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_HPP_INCLUDED
