#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_TRANSPORTS_SIMPLE_WEB_HTTP_TRANSPORT_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_TRANSPORTS_SIMPLE_WEB_HTTP_TRANSPORT_HPP_INCLUDED

/// \file sync/transports/simple_web/HttpTransport.hpp
/// \brief Optional Simple-Web-Server binding for HTTP sync transport.
/// \details
/// This header is not included by mdbx_containers/sync.hpp. Include it only in
/// translation units that intentionally use eidheim/Simple-Web-Server and
/// standalone Asio.

#if !defined(MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT) || \
        !MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT
#error "Simple-Web HTTP transport requires MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT=1"
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

#include <client_http.hpp>
#include <server_http.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <mdbx_containers/sync/transport.hpp>

#if !MDBXC_SYNC_ENABLED
#error "mdbx_containers/sync/transports/simple_web/HttpTransport.hpp requires MDBXC_SYNC_ENABLED=1"
#endif

namespace mdbxc {
namespace sync {
namespace simple_web {

    namespace detail {
        inline std::string endpoint(const std::string& host,
                                    std::uint16_t port) {
            return host + ":" + std::to_string(static_cast<unsigned>(port));
        }

        inline std::vector<std::uint8_t> string_to_bytes(
                const std::string& text) {
            return std::vector<std::uint8_t>(text.begin(), text.end());
        }

        inline std::string bytes_to_string(
                const std::vector<std::uint8_t>& bytes) {
            if (bytes.empty()) {
                return std::string();
            }
            return std::string(reinterpret_cast<const char*>(&bytes[0]),
                               bytes.size());
        }

        inline std::string header_value(
                const SimpleWeb::CaseInsensitiveMultimap& headers,
                const std::string& name) {
            const SimpleWeb::CaseInsensitiveMultimap::const_iterator it =
                headers.find(name);
            return it == headers.end() ? std::string() : it->second;
        }

        inline bool decimal_size_exceeds_limit(
                const std::string& value,
                std::uint64_t limit) {
            std::size_t pos = 0;
            while (pos < value.size() &&
                   (value[pos] == ' ' || value[pos] == '\t')) {
                ++pos;
            }
            if (pos == value.size()) {
                return false;
            }

            std::uint64_t parsed = 0;
            bool saw_digit = false;
            for (; pos < value.size(); ++pos) {
                const char ch = value[pos];
                if (ch >= '0' && ch <= '9') {
                    saw_digit = true;
                    const std::uint64_t digit =
                        static_cast<std::uint64_t>(ch - '0');
                    if (parsed >
                        (std::numeric_limits<std::uint64_t>::max() -
                         digit) / 10u) {
                        return true;
                    }
                    parsed = parsed * 10u + digit;
                    if (parsed > limit) {
                        return true;
                    }
                    continue;
                }

                while (pos < value.size() &&
                       (value[pos] == ' ' || value[pos] == '\t')) {
                    ++pos;
                }
                return false;
            }
            return saw_digit && parsed > limit;
        }

        inline bool content_length_exceeds_limit(
                const SimpleWeb::CaseInsensitiveMultimap& headers,
                const CodecBounds& bounds) {
            const std::string value = header_value(headers, "Content-Length");
            if (value.empty()) {
                return false;
            }
            return decimal_size_exceeds_limit(
                value, bounds.max_transport_message_bytes);
        }

        inline SimpleWeb::StatusCode to_simple_status(unsigned status) {
            return SimpleWeb::status_code(
                std::to_string(static_cast<unsigned>(status)));
        }
    } // namespace detail

    /// \brief Configuration for \c HttpSyncClient.
    struct HttpSyncClientConfig {
        /// \brief Remote HTTP server host name or address.
        std::string host = "127.0.0.1";
        /// \brief Remote HTTP server port.
        std::uint16_t port = 0;
        /// \brief Simple-Web-Server connect and request timeout.
        std::chrono::seconds timeout = std::chrono::seconds(2);
        /// \brief Optional bearer token added as an Authorization header.
        std::string bearer_token;
        /// \brief Extra headers sent with every sync POST request.
        std::vector<HttpSyncHeader> headers;
        /// \brief Maximum accepted response body size before decode.
        CodecBounds bounds;
    };

    /// \brief Simple-Web-Server HTTP client binding for \c HttpSyncPeer.
    /// \note One instance is intended for one active \c post() call at a time.
    /// This matches \c SyncWorker, which performs transport operations
    /// sequentially. Use separate client instances for concurrent callers.
    class HttpSyncClient : public IHttpSyncClient {
    public:
        explicit HttpSyncClient(const HttpSyncClientConfig& config)
            : m_config(config),
              m_cancel_generation(0),
              m_active_client(nullptr),
              m_cancel_count(0) {}

        HttpSyncClient(const std::string& host,
                       std::uint16_t port,
                       std::chrono::seconds timeout,
                       const std::string& bearer_token = std::string())
            : m_cancel_generation(0),
              m_active_client(nullptr),
              m_cancel_count(0) {
            m_config.host = host;
            m_config.port = port;
            m_config.timeout = timeout;
            m_config.bearer_token = bearer_token;
        }

        HttpSyncResponse post(
                const std::string& target,
                const std::string& content_type,
                const std::vector<std::uint8_t>& body,
                const CancellationToken& cancel_token) override {
            if (cancel_token.is_cancellation_requested()) {
                return make_cancelled_response(
                    "cancelled before HTTP request");
            }

            const std::uint64_t call_generation =
                m_cancel_generation.load(std::memory_order_acquire);
            Client client(detail::endpoint(m_config.host, m_config.port));
            client.config.timeout =
                static_cast<long>(m_config.timeout.count());
            client.config.timeout_connect =
                static_cast<long>(m_config.timeout.count());
            ActiveClientGuard active(*this, client);

            if (cancel_token.is_cancellation_requested() ||
                m_cancel_generation.load(std::memory_order_acquire) !=
                    call_generation) {
                return make_cancelled_response(
                    "cancelled before HTTP request");
            }

            try {
                SimpleWeb::CaseInsensitiveMultimap headers;
                headers.emplace("Content-Type", content_type);
                if (!m_config.bearer_token.empty()) {
                    headers.emplace(
                        "Authorization",
                        std::string("Bearer ") + m_config.bearer_token);
                }
                for (std::size_t i = 0; i < m_config.headers.size(); ++i) {
                    headers.emplace(m_config.headers[i].name,
                                    m_config.headers[i].value);
                }

                const std::shared_ptr<Client::Response> received =
                    client.request("POST",
                                   target,
                                   detail::bytes_to_string(body),
                                   headers);

                HttpSyncResponse out;
                out.status_code =
                    static_cast<unsigned>(
                        std::atoi(received->status_code.c_str()));
                out.content_type =
                    detail::header_value(received->header, "Content-Type");
                for (SimpleWeb::CaseInsensitiveMultimap::const_iterator it =
                         received->header.begin();
                     it != received->header.end();
                     ++it) {
                    http_add_header(out.headers, it->first, it->second);
                }
                if (detail::content_length_exceeds_limit(
                        received->header, m_config.bounds)) {
                    return make_payload_too_large_response(
                        "HTTP sync response exceeds max_transport_message_bytes");
                }
                const std::string content = received->content.string();
                if (content.size() >
                    m_config.bounds.max_transport_message_bytes) {
                    return make_payload_too_large_response(
                        "HTTP sync response exceeds max_transport_message_bytes");
                }
                out.body = detail::string_to_bytes(content);
                return out;
            } catch (const SimpleWeb::system_error& e) {
                if (m_cancel_generation.load(std::memory_order_acquire) !=
                        call_generation ||
                    cancel_token.is_cancellation_requested()) {
                    return make_cancelled_response(e.what());
                }
                throw;
            }
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
        typedef SimpleWeb::Client<SimpleWeb::HTTP> Client;

        class ActiveClientGuard {
        public:
            ActiveClientGuard(HttpSyncClient& owner, Client& client)
                : m_owner(owner), m_client(&client) {
                m_owner.set_active_client(m_client);
            }

            ~ActiveClientGuard() {
                m_owner.clear_active_client(m_client);
            }

            ActiveClientGuard(const ActiveClientGuard&) = delete;
            ActiveClientGuard& operator=(const ActiveClientGuard&) = delete;

        private:
            HttpSyncClient& m_owner;
            Client* m_client;
        };

        static HttpSyncResponse make_cancelled_response(
                const std::string& diagnostic) {
            HttpSyncResponse out;
            out.status_code = 503;
            out.content_type = "text/plain; charset=utf-8";
            out.error = diagnostic.empty() ? "cancelled" : diagnostic;
            out.body = detail::string_to_bytes(out.error);
            return out;
        }

        static HttpSyncResponse make_payload_too_large_response(
                const std::string& diagnostic) {
            HttpSyncResponse out;
            out.status_code = 413;
            out.content_type = "text/plain; charset=utf-8";
            out.error = diagnostic;
            out.body = detail::string_to_bytes(out.error);
            return out;
        }

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

        HttpSyncClientConfig m_config;
        std::atomic<std::uint64_t> m_cancel_generation;
        mutable std::mutex m_mutex;
        Client* m_active_client;
        std::atomic<std::size_t> m_cancel_count;
    };

    /// \brief Configuration for \c HttpSyncListener.
    struct HttpSyncListenerConfig {
        /// \brief Local bind host name or address.
        std::string host = "127.0.0.1";
        /// \brief Local bind port. Use 0 to let the OS assign a free port.
        std::uint16_t port = 0;
        /// \brief Simple-Web-Server worker thread count.
        std::size_t thread_pool_size = 1;
        /// \brief Regex route accepting /pull and /push sync requests.
        std::string sync_route_regex = "^/mdbxc/sync/v1/(pull|push)$";
        /// \brief Install a default POST handler returning 404.
        bool install_default_post_handler = true;
        /// \brief Optional external lock around \c HttpSyncServer::handle().
        /// \details
        /// Use this when the same MDBX-backed \c SyncEngine is also touched
        /// by application code while the HTTP listener is running.
        std::mutex* handler_mutex = nullptr;
        /// \brief Maximum accepted request body size before dispatch/decode.
        CodecBounds bounds;
    };

    /// \brief Simple-Web-Server listener binding for \c HttpSyncServer.
    class HttpSyncListener {
    public:
        HttpSyncListener(HttpSyncServer& handler,
                         const HttpSyncListenerConfig& config,
                         ISyncTransportPolicy* policy = nullptr)
            : m_handler(handler),
              m_policy(policy),
              m_config(config),
              m_running(false) {
            m_server.config.address = config.host;
            m_server.config.port = config.port;
            m_server.config.thread_pool_size = config.thread_pool_size;

            m_server.resource[config.sync_route_regex]["POST"] =
                [this](std::shared_ptr<Server::Response> response,
                       std::shared_ptr<Server::Request> request) {
                    handle_post(response, request);
                };

            if (config.install_default_post_handler) {
                m_server.default_resource["POST"] =
                    [](std::shared_ptr<Server::Response> response,
                       std::shared_ptr<Server::Request> request) {
                        (void)request;
                        HttpSyncResponse out;
                        out.status_code = 404;
                        out.content_type = "text/plain; charset=utf-8";
                        out.error = "unknown sync route";
                        out.body = detail::string_to_bytes(out.error);
                        write_response(response, out);
                    };
            }
        }

        ~HttpSyncListener() {
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
                        m_server.start(
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
            m_server.stop();
            if (m_thread.joinable()) {
                m_thread.join();
            }
            m_running = false;
        }

        std::uint16_t port() const {
            return m_config.port;
        }

    private:
        typedef SimpleWeb::Server<SimpleWeb::HTTP> Server;

        static void write_response(
                const std::shared_ptr<Server::Response>& response,
                const HttpSyncResponse& out) {
            SimpleWeb::CaseInsensitiveMultimap headers;
            headers.emplace("Content-Type",
                            out.content_type.empty()
                                ? "text/plain; charset=utf-8"
                                : out.content_type);
            for (std::size_t i = 0; i < out.headers.size(); ++i) {
                headers.emplace(out.headers[i].name, out.headers[i].value);
            }
            response->write(detail::to_simple_status(out.status_code),
                            detail::bytes_to_string(out.body),
                            headers);
        }

        HttpSyncRequest make_request_metadata(
                const std::shared_ptr<Server::Request>& request) const {
            HttpSyncRequest in;
            in.method = request->method;
            in.target = request->path;
            in.content_type =
                detail::header_value(request->header, "Content-Type");
            for (SimpleWeb::CaseInsensitiveMultimap::const_iterator it =
                     request->header.begin();
                 it != request->header.end();
                 ++it) {
                http_add_header(in.headers, it->first, it->second);
            }
            try {
                in.remote_address =
                    request->remote_endpoint().address().to_string();
            } catch (...) {
                in.remote_address.clear();
            }
            return in;
        }

        bool load_request_body(
                const std::shared_ptr<Server::Request>& request,
                HttpSyncRequest& in) const {
            if (detail::content_length_exceeds_limit(
                    request->header, m_config.bounds)) {
                return false;
            }
            const std::string body = request->content.string();
            if (body.size() >
                m_config.bounds.max_transport_message_bytes) {
                return false;
            }
            in.body = detail::string_to_bytes(body);
            return true;
        }

        static HttpSyncResponse make_rejected_response(
                const HttpSyncRequest& in,
                const SyncTransportDecision& decision) {
            HttpSyncResponse out;
            out.status_code =
                decision.status_code == 0 ? 403 : decision.status_code;
            out.content_type = "text/plain; charset=utf-8";
            out.headers = decision.response_headers;
            http_copy_sync_correlation_headers(in.headers, out.headers);
            out.error = decision.error.empty() ? "rejected" : decision.error;
            out.body = detail::string_to_bytes(out.error);
            return out;
        }

        static HttpSyncResponse make_payload_too_large_response(
                const HttpSyncRequest& in) {
            HttpSyncResponse out;
            out.status_code = 413;
            out.content_type = "text/plain; charset=utf-8";
            http_copy_sync_correlation_headers(in.headers, out.headers);
            out.error =
                "HTTP sync request exceeds max_transport_message_bytes";
            out.body = detail::string_to_bytes(out.error);
            return out;
        }

        void handle_post(
                const std::shared_ptr<Server::Response>& response,
                const std::shared_ptr<Server::Request>& request) {
            HttpSyncRequest in = make_request_metadata(request);
            if (!load_request_body(request, in)) {
                write_response(response, make_payload_too_large_response(in));
                return;
            }
            if (m_policy != nullptr) {
                const SyncTransportDecision decision =
                    m_policy->check_http_request(in);
                if (!decision.allowed) {
                    write_response(
                        response, make_rejected_response(in, decision));
                    return;
                }
            }

            HttpSyncResponse out;
            if (m_config.handler_mutex != nullptr) {
                std::lock_guard<std::mutex> lock(*m_config.handler_mutex);
                out = m_handler.handle(in);
            } else {
                out = m_handler.handle(in);
            }
            write_response(response, out);
        }

        HttpSyncServer& m_handler;
        ISyncTransportPolicy* m_policy;
        HttpSyncListenerConfig m_config;
        Server m_server;
        std::thread m_thread;
        bool m_running;
    };

} // namespace simple_web
} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_TRANSPORTS_SIMPLE_WEB_HTTP_TRANSPORT_HPP_INCLUDED
