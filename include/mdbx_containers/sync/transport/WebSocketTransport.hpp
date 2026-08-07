#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_WEBSOCKETTRANSPORT_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_WEBSOCKETTRANSPORT_HPP_INCLUDED

/// \file transport/WebSocketTransport.hpp
/// \brief Framework-neutral WebSocket-shaped adapter for sync transport DTOs.
/// \details
/// This header does not open sockets and does not depend on any WebSocket
/// library. It defines a synchronous request/response message seam over
/// complete binary WebSocket messages encoded by \c TransportMessageCodec.
/// Concrete bindings own connection setup, authentication headers, ping/pong,
/// fragmentation/reassembly, backpressure, retries, and socket cancellation.

#include <cstdint>
#include <stdexcept>
#include <vector>

#include "TransportMessageCodec.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Retry classification for adapter-level WebSocket close codes.
    enum class WebSocketSyncCloseRetryClass : std::uint8_t {
        Success,
        Retryable,
        Permanent
    };

    /// \brief Classifies WebSocket close codes for sync retry policy.
    /// \details Codes 1005 and 1006 are local observations rather than close
    /// frame status values, but adapters commonly surface them to callers.
    inline WebSocketSyncCloseRetryClass
    classify_websocket_sync_close_code(unsigned close_code) {
        switch (close_code) {
            case 1000:
                return WebSocketSyncCloseRetryClass::Success;
            case 1001:
            case 1005:
            case 1006:
            case 1011:
            case 1012:
            case 1013:
            case 1014:
                return WebSocketSyncCloseRetryClass::Retryable;
            default:
                return WebSocketSyncCloseRetryClass::Permanent;
        }
    }

    /// \brief Returns true when a WebSocket close code is retryable.
    inline bool websocket_sync_close_code_is_retryable(
            unsigned close_code) {
        return classify_websocket_sync_close_code(close_code) ==
               WebSocketSyncCloseRetryClass::Retryable;
    }

    /// \brief Builds retry advice from a WebSocket close code.
    inline SyncTransportRetryHint websocket_sync_retry_hint(
            unsigned close_code) {
        SyncTransportRetryHint hint;
        const WebSocketSyncCloseRetryClass classification =
            classify_websocket_sync_close_code(close_code);
        if (classification == WebSocketSyncCloseRetryClass::Success) {
            return hint;
        }
        hint.available = true;
        hint.retryable =
            classification == WebSocketSyncCloseRetryClass::Retryable;
        return hint;
    }

    /// \brief Client-side bridge implemented by a concrete WebSocket library.
    /// \details \p binary_message must contain exactly one
    /// \c TransportMessageCodec request. Implementations return exactly one
    /// encoded response message or throw on transport failure.
    class IWebSocketSyncChannel {
    public:
        virtual ~IWebSocketSyncChannel() {}

        /// \brief Sends one binary sync message and waits for its response.
        /// \param binary_message Encoded pull or push request.
        /// \param cancel_token Local call-control token; it is not serialized.
        /// \return Encoded pull or push response.
        /// \note The v0.1 wire DTOs have no request id. Implementations must
        /// serialize concurrent exchanges on one connection unless the
        /// concrete WebSocket binding adds its own correlation layer.
        virtual std::vector<std::uint8_t> exchange_binary(
                const std::vector<std::uint8_t>& binary_message,
                const CancellationToken& cancel_token) = 0;

        /// \brief Best-effort cancellation hook for an in-flight exchange.
        virtual void request_cancel() {}

        /// \brief Returns retry advice for the most recent exchange failure.
        /// \details Channels that surface WebSocket close codes or
        /// framework-specific retry metadata may override this method. The
        /// default returns an unavailable hint.
        /// \return Retry hint for the last observed exchange failure.
        virtual SyncTransportRetryHint last_retry_hint() const {
            return SyncTransportRetryHint();
        }
    };

    /// \brief Server-side dispatcher from binary WebSocket messages to
    /// \c SyncEngine.
    class WebSocketSyncServer {
    public:
        explicit WebSocketSyncServer(SyncEngine& engine,
                                     const CodecBounds& bounds = CodecBounds())
            : m_engine(engine), m_bounds(bounds) {}

        /// \brief Handles one complete binary WebSocket message.
        /// \details Request messages produce response messages. Response
        /// messages are rejected because this server is not a forwarding relay.
        /// Malformed messages throw; concrete WebSocket bindings should map
        /// those failures to their close/error policy.
        std::vector<std::uint8_t> handle_binary_message(
                const std::vector<std::uint8_t>& binary_message) const {
            const TransportMessageType type =
                TransportMessageCodec::peek_message_type(
                    binary_message, &m_bounds);
            switch (type) {
                case TransportMessageType::PullRequest:
                    return handle_pull(binary_message);
                case TransportMessageType::PushRequest:
                    return handle_push(binary_message);
                case TransportMessageType::PullResponse:
                case TransportMessageType::PushResponse:
                    throw std::runtime_error(
                        "WebSocket sync server received response message");
            }
            throw std::runtime_error("Unexpected WebSocket sync message type");
        }

    private:
        std::vector<std::uint8_t> handle_pull(
                const std::vector<std::uint8_t>& binary_message) const {
            const PullRequest request =
                TransportMessageCodec::decode_pull_request(
                    binary_message, &m_bounds);
            const PullResponse response = m_engine.handle_pull(request);
            return TransportMessageCodec::encode_pull_response(
                response, &m_bounds);
        }

        std::vector<std::uint8_t> handle_push(
                const std::vector<std::uint8_t>& binary_message) const {
            const PushRequest request =
                TransportMessageCodec::decode_push_request(
                    binary_message, &m_bounds);
            const PushResponse response = m_engine.handle_push(request);
            return TransportMessageCodec::encode_push_response(
                response, &m_bounds);
        }

        SyncEngine& m_engine;
        CodecBounds m_bounds;
    };

    /// \brief \c ISyncPeer implementation over an abstract WebSocket channel.
    class WebSocketSyncPeer : public ISyncPeer {
    public:
        explicit WebSocketSyncPeer(
                IWebSocketSyncChannel& channel,
                const CodecBounds& bounds = CodecBounds())
            : m_channel(channel), m_bounds(bounds) {}

        PullResponse pull(const PullRequest& request) override {
            const std::vector<std::uint8_t> request_message =
                TransportMessageCodec::encode_pull_request(
                    request, &m_bounds);
            const std::vector<std::uint8_t> response_message =
                m_channel.exchange_binary(request_message,
                                          request.cancel_token);
            return TransportMessageCodec::decode_pull_response(
                response_message, &m_bounds);
        }

        PushResponse push(const PushRequest& request) override {
            const std::vector<std::uint8_t> request_message =
                TransportMessageCodec::encode_push_request(
                    request, &m_bounds);
            const std::vector<std::uint8_t> response_message =
                m_channel.exchange_binary(request_message,
                                          request.cancel_token);
            return TransportMessageCodec::decode_push_response(
                response_message, &m_bounds);
        }

        void request_cancel() override {
            m_channel.request_cancel();
        }

        SyncTransportRetryHint last_retry_hint() const override {
            return m_channel.last_retry_hint();
        }

    private:
        IWebSocketSyncChannel& m_channel;
        CodecBounds m_bounds;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_WEBSOCKETTRANSPORT_HPP_INCLUDED
