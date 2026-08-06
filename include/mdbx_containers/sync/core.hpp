#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_CORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_CORE_HPP_INCLUDED

/// \file sync/core.hpp
/// \brief Umbrella for sync-independent core models and capture hooks.

#include <mdbx_containers/sync/sync_module.hpp>

#if MDBXC_SYNC_ENABLED
#include <mdbx_containers/common.hpp>
#include <mdbx_containers/sync/common.hpp>
#include <mdbx_containers/sync/protocol/codec_flags.hpp>
#include <mdbx_containers/sync/core/cancellation.hpp>
#include <mdbx_containers/sync/protocol/CodecBounds.hpp>
#include <mdbx_containers/sync/engine/ConflictPolicy.hpp>
#include <mdbx_containers/sync/protocol/IdentityProvider.hpp>
#include <mdbx_containers/sync/protocol/SyncCursor.hpp>
#include <mdbx_containers/sync/protocol/ChangeBatch.hpp>
#include <mdbx_containers/sync/protocol/ChangeBatchCodec.hpp>
#include <mdbx_containers/sync/core/SyncApplyObserver.hpp>
#include <mdbx_containers/sync/capture/ISyncCaptureSink.hpp>
#include <mdbx_containers/sync/capture/SyncCaptureScope.hpp>
#include <mdbx_containers/sync/capture/ChangeAccumulator.hpp>
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_CORE_HPP_INCLUDED
