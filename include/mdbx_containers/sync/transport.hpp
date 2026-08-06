#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_HPP_INCLUDED

/// \file sync/transport.hpp
/// \brief Aggregate header for framework-neutral sync transport adapters.
/// \details
/// This header pulls in the sync core plus the HTTP/WebSocket transport seams
/// and adapter-local middleware. It does not include concrete socket backend
/// integrations such as Simple-Web-Server. Concrete backend headers include
/// the specific framework-neutral headers they need instead of this aggregate.

#include <mdbx_containers/sync.hpp>

#if MDBXC_SYNC_ENABLED
#include <mdbx_containers/sync/transport/HttpTransport.hpp>
#include <mdbx_containers/sync/transport/WebSocketTransport.hpp>
#include <mdbx_containers/sync/transport/TransportMiddleware.hpp>
#endif

#endif // MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_HPP_INCLUDED
