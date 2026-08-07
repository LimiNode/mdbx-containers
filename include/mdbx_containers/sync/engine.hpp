#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ENGINE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ENGINE_HPP_INCLUDED

/// \file sync/engine.hpp
/// \brief Umbrella for sync engine, worker, and direct peer APIs.

#include "config.hpp"

#if MDBXC_SYNC_ENABLED
#include "core.hpp"
#include "protocol.hpp"
#include "storage.hpp"
#include "capture.hpp"
#include "logical.hpp"
#include "engine/ISyncPeer.hpp"
#include "engine/SyncEngine.hpp"
#include "engine/SyncWorker.hpp"
#include "engine/SyncWorkerGuard.hpp"
#include "engine/SyncNodeSession.hpp"
#include "engine/DirectSyncPeer.hpp"
#include "engine/DirectLogicalDeliveryPeer.hpp"
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_ENGINE_HPP_INCLUDED
