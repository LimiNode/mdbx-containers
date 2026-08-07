#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_TRANSPORTS_KURLYK_HTTP_TRANSPORT_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_TRANSPORTS_KURLYK_HTTP_TRANSPORT_HPP_INCLUDED

/// \file sync/transports/kurlyk/HttpTransport.hpp
/// \brief Optional Kurlyk/libcurl client binding for HTTP sync transport.
/// \details
/// This header is not included by mdbx_containers/sync.hpp. Include it only in
/// translation units that intentionally use NewYaroslav/kurlyk as the concrete
/// HTTP client backend.

#if !defined(MDBXC_HAS_KURLYK_HTTP_TRANSPORT) || \
        !MDBXC_HAS_KURLYK_HTTP_TRANSPORT
#error "Kurlyk HTTP transport requires MDBXC_HAS_KURLYK_HTTP_TRANSPORT=1"
#endif

#ifndef KURLYK_HTTP_SUPPORT
#define KURLYK_HTTP_SUPPORT 1
#endif
#ifndef KURLYK_WEBSOCKET_SUPPORT
#define KURLYK_WEBSOCKET_SUPPORT 0
#endif
#ifndef KURLYK_AUTH_SUPPORT
#define KURLYK_AUTH_SUPPORT 0
#endif
#ifndef KURLYK_OAUTH_SUPPORT
#define KURLYK_OAUTH_SUPPORT 0
#endif

#include <kurlyk.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <future>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx_containers/sync/transport.hpp>

#if !MDBXC_SYNC_ENABLED
#error "mdbx_containers/sync/transports/kurlyk/HttpTransport.hpp requires MDBXC_SYNC_ENABLED=1"
#endif

namespace mdbxc {
namespace sync {
namespace kurlyk {

    namespace detail {
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

        inline void add_headers(::kurlyk::Headers& destination,
                                const std::vector<HttpSyncHeader>& source) {
            for (std::size_t i = 0; i < source.size(); ++i) {
                destination.emplace(source[i].name, source[i].value);
            }
        }
    } // namespace detail

    /// \brief Configuration for \c HttpSyncClient.
    struct HttpSyncClientConfig {
        /// \brief Base URL, for example \c http://127.0.0.1:18080.
        std::string base_url = "http://127.0.0.1:18080";
        /// \brief Poll interval used while waiting for Kurlyk's future.
        std::chrono::milliseconds wait_poll_interval =
            std::chrono::milliseconds(10);
        /// \brief Optional bearer token added as an Authorization header.
        std::string bearer_token;
        /// \brief Extra headers sent with every sync POST request.
        std::vector<HttpSyncHeader> headers;
        /// \brief Maximum accepted response body size before decode.
        CodecBounds bounds;
    };

    /// \brief Kurlyk/libcurl HTTP client binding for \c HttpSyncPeer.
    /// \note One instance is intended for one active \c post() call at a time.
    /// Use separate client instances for concurrent callers.
    class HttpSyncClient : public IHttpSyncClient {
    public:
        explicit HttpSyncClient(const HttpSyncClientConfig& config)
            : m_config(config),
              m_client(config.base_url),
              m_cancel_count(0) {
            validate_config(m_config);
        }

        explicit HttpSyncClient(const std::string& base_url,
                                const std::string& bearer_token =
                                    std::string())
            : m_client(base_url),
              m_cancel_count(0) {
            m_config.base_url = base_url;
            m_config.bearer_token = bearer_token;
            validate_config(m_config);
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

            ::kurlyk::Headers headers;
            headers.emplace("Content-Type", content_type);
            if (!m_config.bearer_token.empty()) {
                headers.emplace("Authorization",
                                std::string("Bearer ") +
                                m_config.bearer_token);
            }
            detail::add_headers(headers, m_config.headers);

            std::future< ::kurlyk::HttpResponsePtr > future =
                m_client.post(target,
                              ::kurlyk::QueryParams(),
                              headers,
                              detail::bytes_to_string(body));

            while (future.wait_for(m_config.wait_poll_interval) !=
                   std::future_status::ready) {
                if (cancel_token.is_cancellation_requested()) {
                    cancel_requests();
                    return make_cancelled_response(
                        "cancelled during HTTP request");
                }
            }

            try {
                ::kurlyk::HttpResponsePtr response = future.get();
                if (!response) {
                    return make_transport_error_response(
                        "Kurlyk HTTP client returned empty response");
                }
                return convert_response(*response, m_config.bounds);
            } catch (const std::exception& e) {
                if (cancel_token.is_cancellation_requested()) {
                    return make_cancelled_response(e.what());
                }
                throw;
            }
        }

        void request_cancel() override {
            m_cancel_count.fetch_add(1, std::memory_order_acq_rel);
            cancel_requests();
        }

        std::size_t cancel_count() const {
            return m_cancel_count.load(std::memory_order_acquire);
        }

    private:
        static HttpSyncResponse make_cancelled_response(
                const std::string& diagnostic) {
            HttpSyncResponse out;
            out.status_code = 503;
            out.content_type = "text/plain; charset=utf-8";
            out.error = diagnostic.empty() ? "cancelled" : diagnostic;
            out.body = detail::string_to_bytes(out.error);
            return out;
        }

        static HttpSyncResponse make_transport_error_response(
                const std::string& diagnostic) {
            HttpSyncResponse out;
            out.status_code = 0;
            out.content_type = "text/plain; charset=utf-8";
            out.error = diagnostic;
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

        static HttpSyncResponse convert_response(
                const ::kurlyk::HttpResponse& response,
                const CodecBounds& bounds) {
            HttpSyncResponse out;
            out.status_code = response.status_code < 0
                ? 0u
                : static_cast<unsigned>(response.status_code);
            for (::kurlyk::Headers::const_iterator it =
                     response.headers.begin();
                 it != response.headers.end();
                 ++it) {
                http_add_header(out.headers, it->first, it->second);
            }
            out.content_type =
                http_header_value(out.headers, "Content-Type");
            if (response.content.size() >
                bounds.max_transport_message_bytes) {
                return make_payload_too_large_response(
                    "Kurlyk HTTP sync response exceeds max_transport_message_bytes");
            }
            out.body = detail::string_to_bytes(response.content);
            if (!response.error_message.empty()) {
                out.error = response.error_message;
            } else if (response.error_code) {
                out.error = response.error_code.message();
            }
            return out;
        }

        static void validate_config(const HttpSyncClientConfig& config) {
            if (config.base_url.empty()) {
                throw std::invalid_argument(
                    "Kurlyk HTTP sync base_url must not be empty");
            }
            if (config.wait_poll_interval.count() <= 0) {
                throw std::invalid_argument(
                    "Kurlyk HTTP sync wait_poll_interval must be positive");
            }
        }

        void cancel_requests() {
            std::lock_guard<std::mutex> lock(m_cancel_mutex);
            m_client.cancel_requests();
        }

        HttpSyncClientConfig m_config;
        ::kurlyk::HttpClient m_client;
        std::atomic<std::size_t> m_cancel_count;
        mutable std::mutex m_cancel_mutex;
    };

} // namespace kurlyk
} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_TRANSPORTS_KURLYK_HTTP_TRANSPORT_HPP_INCLUDED
