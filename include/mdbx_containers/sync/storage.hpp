#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_STORAGE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_STORAGE_HPP_INCLUDED

/// \file sync/storage.hpp
/// \brief Umbrella for durable raw-sync stores.

#include "config.hpp"

#if MDBXC_SYNC_ENABLED
#include <mdbx_containers/common.hpp>
#include "core.hpp"
#include "stores/AppliedStore.hpp"
#include "stores/MetaStore.hpp"
#include "stores/OriginIndexStore.hpp"
#include "stores/ChangeLogStore.hpp"
#include "stores/IdentityIndexStore.hpp"
#include "stores/VersionedDbiStore.hpp"
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_STORAGE_HPP_INCLUDED
