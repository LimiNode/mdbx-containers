#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_TRANSPORTS_SIMPLE_WEB_WEB_SOCKET_TRANSPORT_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_TRANSPORTS_SIMPLE_WEB_WEB_SOCKET_TRANSPORT_HPP_INCLUDED

/// \file sync/transports/simple_web/WebSocketTransport.hpp
/// \brief Optional Simple-WebSocket-Server binding for WebSocket sync transport.
/// \details
/// This header is not included by mdbx_containers/sync.hpp. Include it only in
/// translation units that intentionally use eidheim/Simple-WebSocket-Server and
/// standalone Asio.

#if !defined(MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT) || \
        !MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT
#error "Simple-WebSocket transport requires MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT=1"
#endif

#ifndef ASIO_STANDALONE
#define ASIO_STANDALONE 1
#endif
#ifndef USE_STANDALONE_ASIO
#define USE_STANDALONE_ASIO 1
#endif

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include <client_ws.hpp>
#include <server_ws.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <mdbx_containers/sync/transport.hpp>

#if !MDBXC_SYNC_ENABLED
#error "mdbx_containers/sync/transports/simple_web/WebSocketTransport.hpp requires MDBXC_SYNC_ENABLED=1"
#endif

namespace mdbxc {
namespace sync {
namespace simple_web {

    namespace detail {
        inline std::string ws_endpoint(const std::string& host,
                                       std::uint16_t port,
                                       const std::string& path) {
            return host + ":" + std::to_string(static_cast<unsigned>(port)) +
                   path;
        }

        inline std::vector<std::uint8_t> ws_string_to_bytes(
                const std::string& text) {
            return std::vector<std::uint8_t>(text.begin(), text.end());
        }

        inline std::string ws_bytes_to_string(
                const std::vector<std::uint8_t>& bytes) {
            if (bytes.empty()) {
                return std::string();
            }
            return std::string(reinterpret_cast<const char*>(&bytes[0]),
                               bytes.size());
        }

        inline const char* ws_close_retry_label(unsigned close_code) {
            return websocket_sync_close_code_is_retryable(close_code)
                ? "retryable"
                : "permanent";
        }
    } // namespace detail

    /// \brief Default binary frame opcode used by Simple-WebSocket-Server.
    inline unsigned char websocket_binary_frame_opcode() {
        return 130;
    }

    /// \brief Default sync WebSocket path.
    inline const char* websocket_sync_path() {
        return "/mdbxc/sync/v1/ws";
    }

    /// \brief Configuration for \c WebSocketSyncChannel.
    struct WebSocketSyncChannelConfig {
        /// \brief Remote WebSocket server host name or address.
        std::string host = "127.0.0.1";
        /// \brief Remote WebSocket server port.
        std::uint16_t port = 0;
        /// \brief WebSocket endpoint path.
        std::string path = websocket_sync_path();
        /// \brief Optional bearer token sent during the WebSocket handshake.
        std::string bearer_token;
        /// \brief Simple-WebSocket-Server opcode for binary frames.
        unsigned char binary_frame_opcode = 130;
        /// \brief Maximum accepted binary request/response size.
        CodecBounds bounds;
        /// \brief Whole exchange deadline. Zero disables the deadline.
        std::chrono::milliseconds exchange_timeout =
            std::chrono::milliseconds::zero();
    };

    class WebSocketExchangeState {
    public:
        void set_value(const std::vector<std::uint8_t>& bytes) {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_completed) {
                m_completed = true;
                m_promise.set_value(bytes);
            }
        }

        void set_exception(const std::string& message) {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_completed) {
                m_completed = true;
                m_promise.set_exception(std::make_exception_ptr(
                    std::runtime_error(message)));
            }
        }

        std::future<std::vector<std::uint8_t> > future() {
            return m_promise.get_future();
        }

    private:
        std::mutex m_mutex;
        bool m_completed = false;
        std::promise<std::vector<std::uint8_t> > m_promise;
    };

    class WebSocketDeadlineSignal {
    public:
        bool wait_until_finished(std::chrono::milliseconds timeout) {
            std::unique_lock<std::mutex> lock(m_mutex);
            if (!m_finished) {
                m_changed.wait_for(lock, timeout, [this]() {
                    return m_finished;
                });
            }
            return m_finished;
        }

        void finish() {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_finished = true;
            }
            m_changed.notify_all();
        }

    private:
        std::mutex m_mutex;
        std::condition_variable m_changed;
        bool m_finished = false;
    };

    /// \brief Simple-WebSocket-Server channel binding for
    /// \c WebSocketSyncPeer.
    /// \note One instance is intended for one active \c exchange_binary() call
    /// at a time. This matches \c SyncWorker, which performs transport
    /// operations sequentially. Use separate channel instances for concurrent
    /// callers.
    class WebSocketSyncChannel : public IWebSocketSyncChannel {
    public:
        explicit WebSocketSyncChannel(
                const WebSocketSyncChannelConfig& config)
            : m_config(config),
              m_cancel_generation(0),
              m_active_client(nullptr),
              m_cancel_count(0) {}

        WebSocketSyncChannel(const std::string& host,
                             std::uint16_t port,
                             const std::string& bearer_token = std::string())
            : m_cancel_generation(0),
              m_active_client(nullptr),
              m_cancel_count(0) {
            m_config.host = host;
            m_config.port = port;
            m_config.bearer_token = bearer_token;
        }

        std::vector<std::uint8_t> exchange_binary(
                const std::vector<std::uint8_t>& binary_message,
                const CancellationToken& cancel_token) override {
            if (binary_message.size() >
                m_config.bounds.max_transport_message_bytes) {
                throw std::length_error(
                    "WebSocket sync request exceeds max_transport_message_bytes");
            }
            if (cancel_token.is_cancellation_requested()) {
                throw std::runtime_error(
                    "cancelled before WebSocket exchange");
            }
            if (m_config.exchange_timeout.count() < 0) {
                throw std::invalid_argument(
                    "WebSocket exchange_timeout must not be negative");
            }

            const std::uint64_t call_generation =
                m_cancel_generation.load(std::memory_order_acquire);
            Client client(detail::ws_endpoint(
                m_config.host, m_config.port, m_config.path));
            if (!m_config.bearer_token.empty()) {
                client.config.header.emplace(
                    "Authorization",
                    std::string("Bearer ") + m_config.bearer_token);
            }
            ActiveClientGuard active(*this, client);

            std::shared_ptr<WebSocketExchangeState> state(
                new WebSocketExchangeState());
            std::future<std::vector<std::uint8_t> > result =
                state->future();
            const std::string outbound =
                detail::ws_bytes_to_string(binary_message);
            const unsigned char opcode = m_config.binary_frame_opcode;
            const CodecBounds bounds = m_config.bounds;
            const std::chrono::milliseconds exchange_timeout =
                m_config.exchange_timeout;
            std::shared_ptr<WebSocketDeadlineSignal> deadline_signal(
                new WebSocketDeadlineSignal());

            client.on_open =
                [state, outbound, opcode](
                    std::shared_ptr<Client::Connection> connection) {
                    try {
                        connection->send(outbound, nullptr, opcode);
                    } catch (const std::exception& e) {
                        state->set_exception(e.what());
                        connection->send_close(1011);
                    }
                };

            client.on_message =
                [state, bounds](
                    std::shared_ptr<Client::Connection> connection,
                    std::shared_ptr<Client::InMessage> in_message) {
                    const std::string inbound = in_message->string();
                    if (inbound.size() >
                        bounds.max_transport_message_bytes) {
                        state->set_exception(
                            "WebSocket sync response exceeds max_transport_message_bytes");
                        connection->send_close(1009);
                        return;
                    }
                    state->set_value(detail::ws_string_to_bytes(inbound));
                    connection->send_close(1000);
                };

            client.on_close =
                [state](
                    std::shared_ptr<Client::Connection> connection,
                    int status,
                    const std::string& reason) {
                    (void)connection;
                    if (status == 1000) {
                        state->set_exception(
                            "WebSocket closed before sync response");
                    } else {
                        state->set_exception(
                            "WebSocket closed with status " +
                            std::to_string(status) + " (" +
                            detail::ws_close_retry_label(
                                static_cast<unsigned>(status)) +
                            "): " + reason);
                    }
                };

            client.on_error =
                [state](
                    std::shared_ptr<Client::Connection> connection,
                    const SimpleWeb::error_code& ec) {
                    (void)connection;
                    state->set_exception(ec.message());
                };

            if (cancel_token.is_cancellation_requested() ||
                m_cancel_generation.load(std::memory_order_acquire) !=
                    call_generation) {
                throw std::runtime_error("cancelled before WebSocket start");
            }

            std::thread deadline_thread;
            if (exchange_timeout.count() > 0) {
                deadline_thread = std::thread(
                    [state, deadline_signal, exchange_timeout, &client]() {
                        if (!deadline_signal->wait_until_finished(
                                exchange_timeout)) {
                            state->set_exception(
                                "WebSocket exchange deadline exceeded");
                            client.stop();
                        }
                    });
            }
            try {
                client.start();
            } catch (...) {
                deadline_signal->finish();
                if (deadline_thread.joinable()) {
                    deadline_thread.join();
                }
                throw;
            }
            deadline_signal->finish();
            if (deadline_thread.joinable()) {
                deadline_thread.join();
            }
            if (cancel_token.is_cancellation_requested() ||
                m_cancel_generation.load(std::memory_order_acquire) !=
                    call_generation) {
                throw std::runtime_error("cancelled during WebSocket exchange");
            }
            return result.get();
        }

        void request_cancel() override {
            m_cancel_generation.fetch_add(1, std::memory_order_acq_rel);
            m_cancel_count.fetch_add(1, std::memory_order_acq_rel);

            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_active_client != nullptr) {
                m_active_client->stop();
            }
        }

        std::size_t cancel_count() const {
            return m_cancel_count.load(std::memory_order_acquire);
        }

    private:
        typedef SimpleWeb::SocketClient<SimpleWeb::WS> Client;

        class ActiveClientGuard {
        public:
            ActiveClientGuard(WebSocketSyncChannel& owner, Client& client)
                : m_owner(owner), m_client(&client) {
                m_owner.set_active_client(m_client);
            }

            ~ActiveClientGuard() {
                m_owner.clear_active_client(m_client);
            }

            ActiveClientGuard(const ActiveClientGuard&) = delete;
            ActiveClientGuard& operator=(const ActiveClientGuard&) = delete;

        private:
            WebSocketSyncChannel& m_owner;
            Client* m_client;
        };

        void set_active_client(Client* client) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_active_client = client;
        }

        void clear_active_client(Client* client) {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_active_client == client) {
                m_active_client = nullptr;
            }
        }

        WebSocketSyncChannelConfig m_config;
        std::atomic<std::uint64_t> m_cancel_generation;
        mutable std::mutex m_mutex;
        Client* m_active_client;
        std::atomic<std::size_t> m_cancel_count;
    };

    /// \brief Configuration for \c WebSocketSyncListener.
    struct WebSocketSyncListenerConfig {
        /// \brief Local bind host name or address.
        std::string host = "127.0.0.1";
        /// \brief Local bind port. Use 0 to let the OS assign a free port.
        std::uint16_t port = 0;
        /// \brief Simple-WebSocket-Server worker thread count.
        std::size_t thread_pool_size = 1;
        /// \brief Regex endpoint accepting sync WebSocket connections.
        std::string endpoint_regex = "^/mdbxc/sync/v1/ws/?$";
        /// \brief Optional bearer token required during the handshake.
        std::string bearer_token;
        /// \brief Whether the binding should pass an authenticated node.
        bool has_authenticated_node = false;
        /// \brief Authenticated session node passed to middleware.
        NodeId authenticated_node{};
        /// \brief Explicit DB allow-list for this listener/session.
        SyncDbAccess db_access;
        /// \brief Simple-WebSocket-Server opcode for binary frames.
        unsigned char binary_frame_opcode = 130;
        /// \brief Optional external lock around middleware server handling.
        /// \details
        /// Use this when the same MDBX-backed \c SyncEngine is also touched
        /// by application code while the WebSocket listener is running.
        std::mutex* handler_mutex = nullptr;
        /// \brief Maximum accepted binary message size before dispatch/decode.
        CodecBounds bounds;
    };

    /// \brief Simple-WebSocket-Server listener binding for
    /// \c WebSocketSyncServerMiddleware.
    class WebSocketSyncListener {
    public:
        WebSocketSyncListener(WebSocketSyncServerMiddleware& server,
                              const WebSocketSyncListenerConfig& config)
            : m_server(server),
              m_config(config),
              m_running(false) {
            m_ws.config.address = config.host;
            m_ws.config.port = config.port;
            m_ws.config.thread_pool_size = config.thread_pool_size;

            Server::Endpoint& endpoint =
                m_ws.endpoint[config.endpoint_regex];

            endpoint.on_handshake =
                [this](
                    std::shared_ptr<Server::Connection> connection,
                    SimpleWeb::CaseInsensitiveMultimap& response_header) {
                    (void)response_header;
                    if (m_config.bearer_token.empty()) {
                        return SimpleWeb::StatusCode::
                            information_switching_protocols;
                    }
                    const SimpleWeb::CaseInsensitiveMultimap::const_iterator
                        it = connection->header.find("Authorization");
                    if (it == connection->header.end() ||
                        it->second !=
                            std::string("Bearer ") +
                                m_config.bearer_token) {
                        return SimpleWeb::StatusCode::
                            client_error_unauthorized;
                    }
                    return SimpleWeb::StatusCode::
                        information_switching_protocols;
                };

            endpoint.on_message =
                [this](
                    std::shared_ptr<Server::Connection> connection,
                    std::shared_ptr<Server::InMessage> in_message) {
                    try {
                        WebSocketSyncRequestContext context;
                        context.has_authenticated_node =
                            m_config.has_authenticated_node;
                        context.authenticated_node =
                            m_config.authenticated_node;
                        context.db_access = m_config.db_access;
                        const std::string message = in_message->string();
                        if (message.size() >
                            m_config.bounds.max_transport_message_bytes) {
                            connection->send_close(
                                1009,
                                "WebSocket sync request exceeds max_transport_message_bytes");
                            return;
                        }
                        context.binary_message =
                            detail::ws_string_to_bytes(message);
                        std::vector<std::uint8_t> response;
                        if (m_config.handler_mutex != nullptr) {
                            std::lock_guard<std::mutex> lock(
                                *m_config.handler_mutex);
                            response =
                                m_server.handle_binary_message(context);
                        } else {
                            response =
                                m_server.handle_binary_message(context);
                        }
                        connection->send(
                            detail::ws_bytes_to_string(response),
                            nullptr,
                            m_config.binary_frame_opcode);
                    } catch (const WebSocketSyncRejected& e) {
                        connection->send_close(
                            static_cast<int>(e.close_code()), e.what());
                    } catch (const std::exception& e) {
                        connection->send_close(1011, e.what());
                    }
                };

            endpoint.on_error =
                [](
                    std::shared_ptr<Server::Connection> connection,
                    const SimpleWeb::error_code& ec) {
                    (void)connection;
                    (void)ec;
                };
        }

        ~WebSocketSyncListener() {
            stop();
        }

        void start() {
            if (m_running) {
                return;
            }

            std::shared_ptr<std::promise<unsigned short> > started(
                new std::promise<unsigned short>());
            std::future<unsigned short> started_future =
                started->get_future();

            m_thread = std::thread(
                [this, started]() {
                    bool started_callback_called = false;
                    try {
                        m_ws.start(
                            [started, &started_callback_called](
                                unsigned short assigned_port) {
                                started_callback_called = true;
                                started->set_value(assigned_port);
                            });
                    } catch (...) {
                        if (!started_callback_called) {
                            try {
                                started->set_exception(
                                    std::current_exception());
                            } catch (...) {}
                        }
                    }
                });

            try {
                m_config.port = started_future.get();
            } catch (...) {
                if (m_thread.joinable()) {
                    m_thread.join();
                }
                throw;
            }
            m_running = true;
        }

        void stop() {
            if (!m_running) {
                return;
            }
            m_ws.stop();
            if (m_thread.joinable()) {
                m_thread.join();
            }
            m_running = false;
        }

        std::uint16_t port() const {
            return m_config.port;
        }

    private:
        typedef SimpleWeb::SocketServer<SimpleWeb::WS> Server;

        WebSocketSyncServerMiddleware& m_server;
        WebSocketSyncListenerConfig m_config;
        Server m_ws;
        std::thread m_thread;
        bool m_running;
    };

} // namespace simple_web
} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_TRANSPORTS_SIMPLE_WEB_WEB_SOCKET_TRANSPORT_HPP_INCLUDED
