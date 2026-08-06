#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ENGINE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ENGINE_HPP_INCLUDED

/// \file sync/engine.hpp
/// \brief Umbrella for sync engine, worker, and direct peer APIs.

#include <mdbx_containers/sync/sync_module.hpp>

#if MDBXC_SYNC_ENABLED
#include <mdbx_containers/sync/engine/SyncEngine.hpp>
#include <mdbx_containers/sync/engine/SyncWorker.hpp>
#include <mdbx_containers/sync/engine/SyncWorkerGuard.hpp>
#include <mdbx_containers/sync/engine/SyncNodeSession.hpp>
#include <mdbx_containers/sync/engine/DirectSyncPeer.hpp>
#include <mdbx_containers/sync/logical/DirectLogicalDeliveryPeer.hpp>
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_ENGINE_HPP_INCLUDED
