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
#include "common.hpp"
#include "sync/common.hpp"
#include "sync/codec_flags.hpp"
#include "sync/LogicalSchema.hpp"
#include "sync/ChangeOp.hpp"
#include "sync/LogicalChange.hpp"
#include "sync/CodecBounds.hpp"
#include "sync/ChangeBatch.hpp"
#include "sync/ChangeBatchCodec.hpp"
#include "sync/ChangeAccumulator.hpp"
#include "sync/SyncCaptureScope.hpp"
#include "sync/cancellation.hpp"
#include "sync/ConflictPolicy.hpp"
#include "sync/ISyncCaptureSink.hpp"
#include "sync/SyncApplyObserver.hpp"
#include "sync/IdentityProvider.hpp"
#include "sync/ISyncPeer.hpp"
#include "sync/SyncCursor.hpp"
#include "sync/protocol.hpp"
#include "sync/TransportMessageCodec.hpp"
#include "sync/SyncEngine.hpp"
#include "sync/SyncWorker.hpp"
#include "sync/SyncWorkerGuard.hpp"
#include "sync/SyncNodeSession.hpp"
#include "sync/DirectSyncPeer.hpp"
#include "sync/HttpTransport.hpp"
#include "sync/WebSocketTransport.hpp"
#include "sync/TransportMiddleware.hpp"
#include "sync/stores/MetaStore.hpp"
#include "sync/stores/OriginIndexStore.hpp"
#include "sync/stores/ChangeLogStore.hpp"
#include "sync/stores/AppliedStore.hpp"
#include "sync/stores/IdentityIndexStore.hpp"
#include "sync/stores/SchemaRegistryStore.hpp"
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_HPP_INCLUDED
