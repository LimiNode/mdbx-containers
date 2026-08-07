#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_CAPTURE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_CAPTURE_HPP_INCLUDED

/// \file sync/capture.hpp
/// \brief Umbrella for raw write capture and changelog emission.

#include "config.hpp"

#if MDBXC_SYNC_ENABLED
#include <mdbx_containers/common.hpp>
#include "protocol.hpp"
#include "storage.hpp"
#include "capture/ISyncCaptureSink.hpp"
#include "capture/SyncCaptureScope.hpp"
#include "capture/ChangeAccumulator.hpp"
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_CAPTURE_HPP_INCLUDED
