#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_HPP_INCLUDED

/// \file sync/logical.hpp
/// \brief Umbrella for logical schema, delivery, and durable logical state.

#include "config.hpp"

#if MDBXC_SYNC_ENABLED
#include "protocol.hpp"
#include "storage.hpp"
#include "logical/LogicalSchema.hpp"
#include "logical/LogicalChange.hpp"
#include "logical/LogicalChangeFrameCodec.hpp"
#include "logical/LogicalDeliveryEnvelope.hpp"
#include "logical/LogicalDeliveryEnvelopeCodec.hpp"
#include "logical/LogicalDeliveryProtocol.hpp"
#include "logical/ILogicalDeliveryOutbox.hpp"
#include "logical/ILogicalDeliveryPeer.hpp"
#include "logical/LogicalTableAdapter.hpp"
#include "logical/stores/SchemaRegistryStore.hpp"
#include "logical/stores/LogicalDbiBindingStore.hpp"
#include "logical/stores/LogicalDeliveryStore.hpp"
#include "logical/stores/LogicalDeliveryOrderStore.hpp"
#include "logical/stores/LogicalJournalStore.hpp"
#include "logical/stores/LogicalOutboxStore.hpp"
#include "logical/logical_schema_validation.hpp"
#include "logical/logical_recovery.hpp"
#include "logical/LogicalRecoveryProtocol.hpp"
#include "logical/ILogicalRecoveryPeer.hpp"
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_HPP_INCLUDED
