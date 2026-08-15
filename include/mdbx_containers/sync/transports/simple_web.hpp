#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_TRANSPORTS_SIMPLE_WEB_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_TRANSPORTS_SIMPLE_WEB_HPP_INCLUDED

/// \file sync/transports/simple_web.hpp
/// \brief Aggregate header for optional Simple-Web sync transport bindings.
/// \details
/// Include this header only in translation units that intentionally use both
/// the HTTP and WebSocket Simple-Web bindings. To avoid pulling WebSocket
/// handshake dependencies into an HTTP-only target, include the backend-specific
/// header from \c sync/transports/simple_web/ instead.

#include "simple_web/HttpTransport.hpp"
#include "simple_web/WebSocketTransport.hpp"

#endif // MDBX_CONTAINERS_HEADER_SYNC_TRANSPORTS_SIMPLE_WEB_HPP_INCLUDED
