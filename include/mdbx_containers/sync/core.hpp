#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_CORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_CORE_HPP_INCLUDED

/// \file sync/core.hpp
/// \brief Umbrella for sync foundation models and connection-facing observers.

#include "config.hpp"

#if MDBXC_SYNC_ENABLED
#include "common.hpp"
#include "core/cancellation.hpp"
#include "core/ConflictPolicy.hpp"
#include "core/SelectiveReplication.hpp"
#include "core/SyncApplyObserver.hpp"
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_CORE_HPP_INCLUDED
