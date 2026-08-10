#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_HTTPTRANSPORT_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_HTTPTRANSPORT_HPP_INCLUDED

/// \file transport/HttpTransport.hpp
/// \brief Framework-neutral HTTP-shaped adapter for sync transport DTOs.
/// \details
/// This header does not open sockets and does not depend on any HTTP library.
/// It defines the stable route/content-type/body contract that a concrete HTTP
/// client or server binding can adapt to its framework. Authorization, rate
/// limits, allow/deny lists, TLS, and routing middleware belong around this
/// adapter layer.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "TransportMessageCodec.hpp"
#include "../logical/LogicalRecoveryProtocol.hpp"

namespace mdbxc {
namespace sync {

    /// \brief One HTTP header name/value pair preserved by adapter seams.
    struct HttpSyncHeader {
        std::string name;
        std::string value;
    };

    inline char http_ascii_lower(char value) {
        return value >= 'A' && value <= 'Z'
            ? static_cast<char>(value - 'A' + 'a')
            : value;
    }

    /// \brief Case-insensitive ASCII comparison for HTTP header names.
    inline bool http_header_name_equals(const std::string& lhs,
                                        const std::string& rhs) {
        if (lhs.size() != rhs.size()) {
            return false;
        }
        for (std::size_t i = 0; i < lhs.size(); ++i) {
            if (http_ascii_lower(lhs[i]) != http_ascii_lower(rhs[i])) {
                return false;
            }
        }
        return true;
    }

    /// \brief Returns the first matching HTTP header value or an empty string.
    inline std::string http_header_value(
            const std::vector<HttpSyncHeader>& headers,
            const std::string& name) {
        for (std::size_t i = 0; i < headers.size(); ++i) {
            if (http_header_name_equals(headers[i].name, name)) {
                return headers[i].value;
            }
        }
        return std::string();
    }

    /// \brief Returns true when a matching HTTP header name is present.
    inline bool http_has_header(const std::vector<HttpSyncHeader>& headers,
                                const std::string& name) {
        for (std::size_t i = 0; i < headers.size(); ++i) {
            if (http_header_name_equals(headers[i].name, name)) {
                return true;
            }
        }
        return false;
    }

    /// \brief Appends one response/request header.
    inline void http_add_header(std::vector<HttpSyncHeader>& headers,
                                const std::string& name,
                                const std::string& value) {
        HttpSyncHeader header;
        header.name = name;
        header.value = value;
        headers.push_back(header);
    }

    /// \brief Conventional HTTP header names used by sync adapters.
    class HttpSyncHeaders {
    public:
        static const char* request_id() {
            return "X-MDBXC-Sync-Request-Id";
        }

        static const char* trace_id() {
            return "X-MDBXC-Sync-Trace-Id";
        }
    };

    inline void http_copy_header_if_present(
            const std::vector<HttpSyncHeader>& source,
            std::vector<HttpSyncHeader>& destination,
            const std::string& name) {
        const std::string value = http_header_value(source, name);
        if (!value.empty() && !http_has_header(destination, name)) {
            http_add_header(destination, name, value);
        }
    }

    /// \brief Copies request/trace correlation headers when present.
    /// \details Header values are adapter-local metadata. They are not part of
    /// \c TransportMessageCodec and are intentionally optional.
    inline void http_copy_sync_correlation_headers(
            const std::vector<HttpSyncHeader>& source,
            std::vector<HttpSyncHeader>& destination) {
        http_copy_header_if_present(
            source, destination, HttpSyncHeaders::request_id());
        http_copy_header_if_present(
            source, destination, HttpSyncHeaders::trace_id());
    }

    /// \brief Minimal request shape consumed by \c HttpSyncServer.
    struct HttpSyncRequest {
        std::string method;
        std::string target;
        std::string content_type;
        std::vector<HttpSyncHeader> headers;
        std::string remote_address;
        std::vector<std::uint8_t> body;
    };

    /// \brief Minimal response shape produced by \c HttpSyncServer.
    /// \details Successful sync responses use the binary sync content type and
    /// a \c TransportMessageCodec body. Non-200 responses use \c text/plain
    /// and carry the diagnostic text in \c body; \c error mirrors the text for
    /// in-process adapters that do not serialize the response object.
    struct HttpSyncResponse {
        unsigned status_code = 200;
        std::string content_type;
        std::vector<HttpSyncHeader> headers;
        std::vector<std::uint8_t> body;
        std::string error;
    };

    /// \brief Retry classification for adapter-level HTTP failures.
    enum class HttpSyncRetryClass : std::uint8_t {
        Success,
        Retryable,
        Permanent
    };

    /// \brief Classifies HTTP status codes for sync transport retry policy.
    /// \details This helper classifies transport-level response status only.
    /// Sync DTO responses with HTTP 200 still carry their own \c ok/error
    /// fields and must be handled by the caller.
    inline HttpSyncRetryClass classify_http_sync_status(unsigned status_code) {
        if (status_code >= 200 && status_code < 300) {
            return HttpSyncRetryClass::Success;
        }
        switch (status_code) {
            case 408:
            case 425:
            case 429:
            case 500:
            case 502:
            case 503:
            case 504:
                return HttpSyncRetryClass::Retryable;
            default:
                return HttpSyncRetryClass::Permanent;
        }
    }

    /// \brief Returns true when an HTTP sync status is retryable.
    inline bool http_sync_status_is_retryable(unsigned status_code) {
        return classify_http_sync_status(status_code) ==
               HttpSyncRetryClass::Retryable;
    }

    inline bool parse_http_retry_after_delta_seconds(
            const std::string& value,
            std::uint64_t& seconds) {
        if (value.empty()) {
            return false;
        }

        std::uint64_t out = 0;
        for (std::size_t i = 0; i < value.size(); ++i) {
            const char ch = value[i];
            if (ch < '0' || ch > '9') {
                return false;
            }
            const std::uint64_t digit =
                static_cast<std::uint64_t>(ch - '0');
            if (out >
                    (std::numeric_limits<std::uint64_t>::max() - digit) /
                        10u) {
                return false;
            }
            out = out * 10u + digit;
        }

        seconds = out;
        return true;
    }

    /// \brief Builds retry advice from an HTTP sync response.
    /// \details Successful statuses return an unavailable hint because they
    /// are not transport failures. Permanent and retryable failures return an
    /// available hint. Retryable statuses preserve an optional relative
    /// \c Retry-After value.
    inline SyncTransportRetryHint http_sync_retry_hint(
            const HttpSyncResponse& response) {
        SyncTransportRetryHint hint;
        const HttpSyncRetryClass classification =
            classify_http_sync_status(response.status_code);
        if (classification == HttpSyncRetryClass::Success) {
            return hint;
        }
        hint.available = true;
        hint.retryable = classification == HttpSyncRetryClass::Retryable;
        if (!hint.retryable) {
            return hint;
        }

        const std::string retry_after =
            http_header_value(response.headers, "Retry-After");
        std::uint64_t seconds = 0;
        if (parse_http_retry_after_delta_seconds(retry_after, seconds)) {
            hint.has_retry_after = true;
            hint.retry_after_seconds = seconds;
        }
        return hint;
    }

    /// \brief Client-side bridge implemented by a concrete HTTP library.
    class IHttpSyncClient {
    public:
        virtual ~IHttpSyncClient() {}

        /// \brief Sends one binary sync request body to \p target.
        /// \details Implementations own connection pooling, timeouts, TLS,
        /// authorization headers, rate-limit retries, and socket cancellation.
        virtual HttpSyncResponse post(
                const std::string& target,
                const std::string& content_type,
                const std::vector<std::uint8_t>& body,
                const CancellationToken& cancel_token) = 0;

        /// \brief Best-effort cancellation hook for an in-flight \c post().
        virtual void request_cancel() {}
    };

    /// \brief Shared constants for HTTP sync adapters.
    class HttpSyncRoutes {
    public:
        static const char* method_post() {
            return "POST";
        }

        static const char* content_type() {
            return "application/vnd.mdbx-containers.sync.v1+octet-stream";
        }

        static const char* pull_target() {
            return "/mdbxc/sync/v1/pull";
        }

        static const char* push_target() {
            return "/mdbxc/sync/v1/push";
        }

        static const char* logical_hello_target() {
            return "/mdbxc/sync/v1/logical/hello";
        }

        static const char* logical_delivery_target() {
            return "/mdbxc/sync/v1/logical/delivery";
        }

        static const char* logical_recovery_target() {
            return "/mdbxc/sync/v1/logical/recovery";
        }
    };

    /// \brief Server-side dispatcher from HTTP-shaped requests to SyncEngine.
    class HttpSyncServer {
    public:
        explicit HttpSyncServer(SyncEngine& engine,
                                const CodecBounds& bounds = CodecBounds())
            : m_engine(engine), m_bounds(bounds) {}

        /// \brief Handles one already-parsed HTTP request.
        /// \details Returns HTTP-style status codes for transport/framing
        /// failures. Sync-level failures such as db_id mismatch are encoded
        /// inside the binary PullResponse/PushResponse with status 200.
        HttpSyncResponse handle(const HttpSyncRequest& request) const {
            HttpSyncResponse response;
            if (request.method != HttpSyncRoutes::method_post()) {
                response = make_error(405, "method not allowed");
            } else if (request.content_type != HttpSyncRoutes::content_type()) {
                response = make_error(415, "unsupported content type");
            } else if (request.target == HttpSyncRoutes::pull_target()) {
                response = handle_pull(request.body);
            } else if (request.target == HttpSyncRoutes::push_target()) {
                response = handle_push(request.body);
            } else if (request.target ==
                       HttpSyncRoutes::logical_hello_target()) {
                response = handle_logical_hello(request.body);
            } else if (request.target ==
                       HttpSyncRoutes::logical_delivery_target()) {
                response = handle_logical_delivery(request.body);
            } else if (request.target ==
                       HttpSyncRoutes::logical_recovery_target()) {
                response = handle_logical_recovery(request.body);
            } else {
                response = make_error(404, "unknown sync route");
            }
            http_copy_sync_correlation_headers(
                request.headers, response.headers);
            return response;
        }

    private:
        HttpSyncResponse handle_pull(
                const std::vector<std::uint8_t>& body) const {
            PullRequest decoded;
            try {
                decoded = TransportMessageCodec::decode_pull_request(
                    body, &m_bounds);
            } catch (const std::length_error& e) {
                return make_error(413, e.what());
            } catch (const std::exception& e) {
                return make_error(400, e.what());
            }

            try {
                const PullResponse response = m_engine.handle_pull(decoded);
                return make_binary(
                    TransportMessageCodec::encode_pull_response(
                        response, &m_bounds));
            } catch (const std::exception& e) {
                return make_error(500, e.what());
            }
        }

        HttpSyncResponse handle_push(
                const std::vector<std::uint8_t>& body) const {
            PushRequest decoded;
            try {
                decoded = TransportMessageCodec::decode_push_request(
                    body, &m_bounds);
            } catch (const std::length_error& e) {
                return make_error(413, e.what());
            } catch (const std::exception& e) {
                return make_error(400, e.what());
            }

            try {
                const PushResponse response = m_engine.handle_push(decoded);
                return make_binary(
                    TransportMessageCodec::encode_push_response(
                        response, &m_bounds));
            } catch (const std::exception& e) {
                return make_error(500, e.what());
            }
        }

        HttpSyncResponse handle_logical_hello(
                const std::vector<std::uint8_t>& body) const {
            try {
                LogicalDeliveryProtocolCodec::decode_hello_request(
                    body, &m_bounds);
                return make_binary(
                    LogicalDeliveryProtocolCodec::encode_hello(
                        m_engine.logical_delivery_hello(), &m_bounds));
            } catch (const std::length_error& e) {
                return make_error(413, e.what());
            } catch (const std::exception& e) {
                return make_error(400, e.what());
            }
        }

        HttpSyncResponse handle_logical_delivery(
                const std::vector<std::uint8_t>& body) const {
            try {
                const LogicalDeliveryProtocolCodec::MessageType type =
                    LogicalDeliveryProtocolCodec::peek_message_type(
                        body, &m_bounds);
                LogicalDeliveryAcknowledgement acknowledgement;
                if (type == LogicalDeliveryProtocolCodec::MessageType::Delivery) {
                    return make_error(
                        400, "receiver-bound logical delivery request required");
                } else if (type ==
                           LogicalDeliveryProtocolCodec::MessageType::DeliveryRequest) {
                    const LogicalDeliveryRequest request =
                        LogicalDeliveryProtocolCodec::decode_delivery_request(
                            body, &m_bounds);
                    acknowledgement =
                        m_engine.apply_ordered_logical_delivery_request(
                            request, &m_bounds);
                } else {
                    return make_error(
                        400, "unexpected logical delivery message type");
                }
                return make_binary(
                    LogicalDeliveryProtocolCodec::encode_acknowledgement(
                        acknowledgement, &m_bounds));
            } catch (const std::length_error& e) {
                return make_error(413, e.what());
            } catch (const std::exception& e) {
                return make_error(400, e.what());
            }
        }

        HttpSyncResponse handle_logical_recovery(
                const std::vector<std::uint8_t>& body) const {
            try {
                const LogicalRecoveryRequest request =
                    LogicalRecoveryProtocolCodec::decode_request(body, &m_bounds);
                return make_binary(LogicalRecoveryProtocolCodec::encode_response(
                    m_engine.handle_logical_recovery(request), &m_bounds));
            } catch (const std::length_error& e) {
                return make_error(413, e.what());
            } catch (const std::exception& e) {
                return make_error(400, e.what());
            }
        }

        static HttpSyncResponse make_binary(
                const std::vector<std::uint8_t>& body) {
            HttpSyncResponse response;
            response.status_code = 200;
            response.content_type = HttpSyncRoutes::content_type();
            response.body = body;
            return response;
        }

        static HttpSyncResponse make_error(unsigned status,
                                           const std::string& error) {
            HttpSyncResponse response;
            response.status_code = status;
            response.content_type = "text/plain; charset=utf-8";
            response.error = error;
            response.body.assign(error.begin(), error.end());
            return response;
        }

        SyncEngine& m_engine;
        CodecBounds m_bounds;
    };

    /// \brief \c ISyncPeer implementation over an abstract HTTP client.
    class HttpSyncPeer : public ISyncPeer {
    public:
        explicit HttpSyncPeer(IHttpSyncClient& client,
                              const CodecBounds& bounds = CodecBounds())
            : m_client(client), m_bounds(bounds) {}

        PullResponse pull(const PullRequest& request) override {
            clear_last_retry_hint();
            const std::vector<std::uint8_t> body =
                TransportMessageCodec::encode_pull_request(
                    request, &m_bounds);
            const HttpSyncResponse response = m_client.post(
                HttpSyncRoutes::pull_target(),
                HttpSyncRoutes::content_type(),
                body,
                request.cancel_token);
            require_ok_response(response, "pull");
            return TransportMessageCodec::decode_pull_response(
                response.body, &m_bounds);
        }

        PushResponse push(const PushRequest& request) override {
            clear_last_retry_hint();
            const std::vector<std::uint8_t> body =
                TransportMessageCodec::encode_push_request(
                    request, &m_bounds);
            const HttpSyncResponse response = m_client.post(
                HttpSyncRoutes::push_target(),
                HttpSyncRoutes::content_type(),
                body,
                request.cancel_token);
            require_ok_response(response, "push");
            return TransportMessageCodec::decode_push_response(
                response.body, &m_bounds);
        }

        bool supports_logical_delivery() const override {
            return true;
        }

        bool supports_logical_recovery() const override {
            return true;
        }

        LogicalRecoveryResponse logical_recovery(
                const LogicalRecoveryRequest& request) override {
            return logical_recovery_with_cancel(request, nullptr);
        }

        LogicalRecoveryResponse logical_recovery_with_cancel(
                const LogicalRecoveryRequest& request,
                const CancellationToken* cancel_token = nullptr) override {
            clear_last_retry_hint();
            const CancellationToken token = cancel_token != nullptr
                ? *cancel_token
                : CancellationToken();
            const HttpSyncResponse response = m_client.post(
                HttpSyncRoutes::logical_recovery_target(),
                HttpSyncRoutes::content_type(),
                LogicalRecoveryProtocolCodec::encode_request(request, &m_bounds),
                token);
            require_ok_response(response, "logical recovery");
            return LogicalRecoveryProtocolCodec::decode_response(
                response.body, &m_bounds);
        }

        LogicalDeliveryHello logical_delivery_hello() override {
            return logical_delivery_hello_with_cancel(nullptr);
        }

        LogicalDeliveryHello logical_delivery_hello_with_cancel(
                const CancellationToken* cancel_token = nullptr) override {
            clear_last_retry_hint();
            const CancellationToken token = cancel_token != nullptr
                ? *cancel_token
                : CancellationToken();
            const HttpSyncResponse response = m_client.post(
                HttpSyncRoutes::logical_hello_target(),
                HttpSyncRoutes::content_type(),
                LogicalDeliveryProtocolCodec::encode_hello_request(&m_bounds),
                token);
            require_ok_response(response, "logical hello");
            return LogicalDeliveryProtocolCodec::decode_hello(
                response.body, &m_bounds);
        }

        LogicalDeliveryAcknowledgement deliver_ordered_logical_delivery(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr) override {
            return deliver_ordered_logical_delivery_with_cancel(
                envelope, bounds, nullptr);
        }

        LogicalDeliveryAcknowledgement
        deliver_ordered_logical_delivery_with_cancel(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr,
                const CancellationToken* cancel_token = nullptr) override {
            return deliver_logical_message(
                LogicalDeliveryProtocolCodec::encode_delivery(envelope,
                                                              effective_logical_bounds(bounds)),
                cancel_token);
        }

        LogicalDeliveryAcknowledgement deliver_ordered_logical_request(
                const LogicalDeliveryRequest& request,
                const CodecBounds* bounds = nullptr) override {
            return deliver_ordered_logical_request_with_cancel(
                request, bounds, nullptr);
        }

        LogicalDeliveryAcknowledgement
        deliver_ordered_logical_request_with_cancel(
                const LogicalDeliveryRequest& request,
                const CodecBounds* bounds = nullptr,
                const CancellationToken* cancel_token = nullptr) override {
            return deliver_logical_message(
                LogicalDeliveryProtocolCodec::encode_delivery_request(
                    request, effective_logical_bounds(bounds)),
                cancel_token);
        }

        void request_cancel() override {
            m_client.request_cancel();
        }

        SyncTransportRetryHint last_retry_hint() const override {
            std::lock_guard<std::mutex> lock(m_retry_mutex);
            return m_last_retry_hint;
        }

    private:
        const CodecBounds* effective_logical_bounds(
                const CodecBounds* bounds) const {
            return bounds != nullptr ? bounds : &m_bounds;
        }

        LogicalDeliveryAcknowledgement deliver_logical_message(
                const std::vector<std::uint8_t>& body,
                const CancellationToken* cancel_token) {
            clear_last_retry_hint();
            const CancellationToken token = cancel_token != nullptr
                ? *cancel_token
                : CancellationToken();
            const HttpSyncResponse response = m_client.post(
                HttpSyncRoutes::logical_delivery_target(),
                HttpSyncRoutes::content_type(), body, token);
            require_ok_response(response, "logical delivery");
            return LogicalDeliveryProtocolCodec::decode_acknowledgement(
                response.body, &m_bounds);
        }

        void require_ok_response(const HttpSyncResponse& response,
                                 const char* operation) const {
            if (response.status_code != 200) {
                set_last_retry_hint(http_sync_retry_hint(response));
                const std::string diagnostic = response_diagnostic(response);
                throw std::runtime_error(
                    std::string("HTTP sync ") + operation +
                    " failed with status " +
                    std::to_string(response.status_code) + ": " +
                    diagnostic);
            }
            if (response.content_type != HttpSyncRoutes::content_type()) {
                throw std::runtime_error(
                    std::string("HTTP sync ") + operation +
                    " returned unexpected content type");
            }
        }

        void clear_last_retry_hint() const {
            set_last_retry_hint(SyncTransportRetryHint());
        }

        void set_last_retry_hint(
                const SyncTransportRetryHint& hint) const {
            std::lock_guard<std::mutex> lock(m_retry_mutex);
            m_last_retry_hint = hint;
        }

        static std::string response_diagnostic(
                const HttpSyncResponse& response) {
            if (!response.error.empty()) {
                return response.error;
            }
            if (response.body.empty()) {
                return std::string();
            }
            return std::string(
                reinterpret_cast<const char*>(&response.body[0]),
                response.body.size());
        }

        IHttpSyncClient& m_client;
        CodecBounds m_bounds;
        mutable std::mutex m_retry_mutex;
        mutable SyncTransportRetryHint m_last_retry_hint;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_HTTPTRANSPORT_HPP_INCLUDED
