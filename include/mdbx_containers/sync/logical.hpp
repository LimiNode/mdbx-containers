#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_HPP_INCLUDED

/// \file sync/logical.hpp
/// \brief Umbrella for logical schema, delivery, and adapter APIs.

#include <mdbx_containers/sync/sync_module.hpp>

#if MDBXC_SYNC_ENABLED
#include <mdbx_containers/sync/logical/LogicalSchema.hpp>
#include <mdbx_containers/sync/logical/LogicalChange.hpp>
#include <mdbx_containers/sync/logical/LogicalChangeFrameCodec.hpp>
#include <mdbx_containers/sync/logical/LogicalDeliveryEnvelope.hpp>
#include <mdbx_containers/sync/logical/LogicalDeliveryEnvelopeCodec.hpp>
#include <mdbx_containers/sync/logical/LogicalDeliveryProtocol.hpp>
#include <mdbx_containers/sync/logical/ILogicalDeliveryOutbox.hpp>
#include <mdbx_containers/sync/logical/ILogicalDeliveryPeer.hpp>
#include <mdbx_containers/sync/logical/LogicalTableAdapter.hpp>
#include <mdbx_containers/sync/logical/logical_schema_validation.hpp>
#include <mdbx_containers/sync/adapters.hpp>
#include <mdbx_containers/sync/logical/stores/LogicalDeliveryStore.hpp>
#include <mdbx_containers/sync/logical/stores/LogicalDeliveryOrderStore.hpp>
#include <mdbx_containers/sync/logical/stores/LogicalOutboxStore.hpp>
#include <mdbx_containers/sync/logical/stores/SchemaRegistryStore.hpp>
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_HPP_INCLUDED
