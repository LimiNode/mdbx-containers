#pragma once
#ifndef MDBX_CONTAINERS_HEADER_COMMON_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_COMMON_HPP_INCLUDED

/// \file common.hpp
/// \brief Publicly usable low-level components of the MDBX Containers library.
/// 
/// Includes building blocks such as Connection, Transaction, Config, and exceptions.

// --- Standard includes (self-contained header) ---
#include <string>
#include <vector>
#include <deque>
#include <list>
#include <set>
#include <array>
#include <algorithm>
#include <bitset>
#include <unordered_set>
#include <unordered_map>
#include <type_traits>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <cassert>
#include <cstdlib>
#include <mdbx.h>

#if __cplusplus >= 201703L
#	include <optional>
#	include <cstddef> // std::byte
#endif

#ifndef MDBX_CONTAINERS_SEPARATE_COMPILATION
#define MDBX_CONTAINERS_HEADER_ONLY
#endif

#ifndef XXH_INLINE_ALL
#   define XXH_INLINE_ALL
#endif
#ifndef XXH_NAMESPACE
#   define XXH_NAMESPACE MDBXC_XXH_
#endif

#include "detail/xxhash.h"
#include "common/hashing.hpp"
#include "common/MdbxException.hpp"
#include "common/Config.hpp"
#include "common/TransactionTracker.hpp"
#include "detail/utils.hpp"
#include "common/Transaction.hpp"
#include "detail/path_utils.hpp"
#if MDBXC_SYNC_ENABLED
#include "sync/protocol/ChangeOp.hpp"
#include "sync/capture/ISyncCaptureSink.hpp"
#include "sync/core/SyncApplyObserver.hpp"
#endif
#include "common/Connection.hpp"
#include "detail/BaseTable.hpp"

#endif // MDBX_CONTAINERS_HEADER_COMMON_HPP_INCLUDED
