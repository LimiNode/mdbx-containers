#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_HPP_INCLUDED

/// \file sync.hpp
/// \brief Aggregate header for the optional sync subsystem.
/// \details
/// Pulls in all foundation types and codec when \c MDBXC_SYNC_ENABLED is
/// non-zero. Otherwise the include is a no-op so applications that do not
/// need replication pay zero compile-time or runtime cost.

#include "sync/sync_module.hpp"

#if MDBXC_SYNC_ENABLED
#include "sync/core.hpp"
#include "sync/protocol.hpp"
#include "sync/transport/TransportMessageCodec.hpp"
#include "sync/logical.hpp"
#include "sync/engine.hpp"
#include "sync/transport/ISyncPeer.hpp"
#include "sync/transport/HttpTransport.hpp"
#include "sync/transport/WebSocketTransport.hpp"
#include "sync/transport/TransportMiddleware.hpp"
#include "sync/stores/MetaStore.hpp"
#include "sync/stores/OriginIndexStore.hpp"
#include "sync/stores/ChangeLogStore.hpp"
#include "sync/stores/AppliedStore.hpp"
#include "sync/stores/IdentityIndexStore.hpp"
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_HPP_INCLUDED
