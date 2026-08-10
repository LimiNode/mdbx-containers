#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_TRANSPORTMIDDLEWARE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_TRANSPORTMIDDLEWARE_HPP_INCLUDED

/// \file transport/TransportMiddleware.hpp
/// \brief Policy and metrics wrappers for sync transport adapters.
/// \details
/// These helpers sit around \c ISyncPeer or \c IHttpSyncClient. They do not
/// add credentials to sync DTOs and do not own sockets; concrete transports can
/// use them to enforce allow lists, fixed request budgets, and metrics hooks.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "HttpTransport.hpp"
#include "WebSocketTransport.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Transport operation observed by middleware.
    enum class SyncTransportOperation : std::uint8_t {
        Pull,
        Push,
        HttpPost,
        WebSocketMessage
    };

    /// \brief Result returned by a transport policy.
    struct SyncTransportDecision {
        bool allowed = true;
        unsigned status_code = 0;
        std::string error;
        std::vector<HttpSyncHeader> response_headers;

        static SyncTransportDecision allow() {
            return SyncTransportDecision();
        }

        static SyncTransportDecision reject(const std::string& message,
                                            unsigned status = 403) {
            SyncTransportDecision out;
            out.allowed = false;
            out.status_code = status;
            out.error = message;
            return out;
        }

        void add_response_header(const std::string& name,
                                 const std::string& value) {
            http_add_header(response_headers, name, value);
        }
    };

    /// \brief Explicit DB access rule used by transport identity policies.
    /// \details \c any() permits every \c db_id, \c none() permits none, and
    /// \c only() starts a restricted allow-list. This avoids giving an empty
    /// container a hidden meaning in session contexts.
    struct SyncDbAccess {
        bool allow_all_dbs = false;
        std::set<DbId> db_ids;

        static SyncDbAccess any() {
            SyncDbAccess out;
            out.allow_all_dbs = true;
            return out;
        }

        static SyncDbAccess none() {
            return SyncDbAccess();
        }

        static SyncDbAccess only(const DbId& db_id) {
            SyncDbAccess out;
            out.allow_db_id(db_id);
            return out;
        }

        void set_allow_any() {
            allow_all_dbs = true;
            db_ids.clear();
        }

        void set_deny_all() {
            allow_all_dbs = false;
            db_ids.clear();
        }

        void allow_db_id(const DbId& db_id) {
            allow_all_dbs = false;
            db_ids.insert(db_id);
        }

        bool allows_db_id(const DbId& db_id) const {
            return allow_all_dbs ||
                   db_ids.find(db_id) != db_ids.end();
        }
    };

    /// \brief Adapter-local context for one WebSocket sync message.
    /// \details Concrete WebSocket bindings authenticate the session during
    /// handshake or with framework-specific metadata, then pass the resulting
    /// node identity here before dispatching the binary sync message. The
    /// identity and DB access rule are not serialized in the sync DTO. The
    /// default \c db_access denies every DB until the binding explicitly fills
    /// it with \c SyncDbAccess::any() or a restricted allow-list.
    struct WebSocketSyncRequestContext {
        bool has_authenticated_node = false;
        NodeId authenticated_node{};
        SyncDbAccess db_access;
        std::vector<std::uint8_t> binary_message;
        std::string request_id;
        std::string trace_id;
    };

    /// \brief Adapter-local trace identifiers for logging and metrics.
    /// \details These fields are never serialized into sync DTOs. HTTP
    /// adapters usually get them from headers, while WebSocket bindings may
    /// copy them from handshake metadata or framework session context.
    struct SyncTransportTraceContext {
        std::string request_id;
        std::string trace_id;

        bool empty() const {
            return request_id.empty() && trace_id.empty();
        }
    };

    inline SyncTransportTraceContext http_sync_trace_context(
            const std::vector<HttpSyncHeader>& headers) {
        SyncTransportTraceContext out;
        out.request_id =
            http_header_value(headers, HttpSyncHeaders::request_id());
        out.trace_id =
            http_header_value(headers, HttpSyncHeaders::trace_id());
        return out;
    }

    inline SyncTransportTraceContext http_sync_trace_context(
            const HttpSyncRequest& request) {
        return http_sync_trace_context(request.headers);
    }

    inline SyncTransportTraceContext websocket_sync_trace_context(
            const WebSocketSyncRequestContext& request) {
        SyncTransportTraceContext out;
        out.request_id = request.request_id;
        out.trace_id = request.trace_id;
        return out;
    }

    /// \brief Policy hook for sync transport requests.
    /// \details Default methods allow every request. Implementations may
    /// inspect DTO metadata, route names, or message sizes before forwarding.
    class ISyncTransportPolicy {
    public:
        virtual ~ISyncTransportPolicy() {}

        virtual SyncTransportDecision check_pull(
                const PullRequest& request) {
            (void)request;
            return SyncTransportDecision::allow();
        }

        virtual SyncTransportDecision check_push(
                const PushRequest& request) {
            (void)request;
            return SyncTransportDecision::allow();
        }

        virtual SyncTransportDecision check_http_post(
                const std::string& target,
                const std::string& content_type,
                const std::vector<std::uint8_t>& body) {
            (void)target;
            (void)content_type;
            (void)body;
            return SyncTransportDecision::allow();
        }

        virtual SyncTransportDecision check_http_request(
                const HttpSyncRequest& request) {
            return check_http_post(request.target,
                                   request.content_type,
                                   request.body);
        }

        virtual SyncTransportDecision check_websocket_message(
                const WebSocketSyncRequestContext& request) {
            (void)request;
            return SyncTransportDecision::allow();
        }
    };

    /// \brief Extracts a bearer token from an HTTP sync request.
    inline std::string http_bearer_token(const HttpSyncRequest& request) {
        static const char scheme[] = "bearer";
        const std::string value =
            http_header_value(request.headers, "Authorization");
        if (value.size() <= sizeof(scheme)) {
            return std::string();
        }
        for (std::size_t i = 0; i < sizeof(scheme) - 1u; ++i) {
            if (http_ascii_lower(value[i]) != scheme[i]) {
                return std::string();
            }
        }
        if (value[sizeof(scheme) - 1u] != ' ') {
            return std::string();
        }
        return value.substr(sizeof(scheme));
    }

    /// \brief Allows only configured node ids and database ids.
    /// \details Node allow-list defaults to "allow any". DB access defaults to
    /// "allow any" for compatibility with simple peer-level policies.
    class NodeDbAllowListPolicy : public ISyncTransportPolicy {
    public:
        NodeDbAllowListPolicy()
            : m_db_access(SyncDbAccess::any()) {}

        void allow_node_id(const NodeId& node_id) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_allowed_nodes.insert(node_id);
        }

        void allow_db_id(const DbId& db_id) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_db_access.allow_db_id(db_id);
        }

        void allow_any_db_id() {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_db_access.set_allow_any();
        }

        void deny_all_db_ids() {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_db_access.set_deny_all();
        }

        void clear() {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_allowed_nodes.clear();
            m_db_access = SyncDbAccess::any();
        }

        SyncTransportDecision check_pull(
                const PullRequest& request) override {
            return check_node_and_db(request.requester,
                                     request.db_id,
                                     "sync requester is not allowed");
        }

        SyncTransportDecision check_push(
                const PushRequest& request) override {
            return check_node_and_db(request.sender,
                                     request.db_id,
                                     "sync sender is not allowed");
        }

    private:
        SyncTransportDecision check_node_and_db(
                const NodeId& node_id,
                const DbId& db_id,
                const char* node_error) const {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_db_access.allows_db_id(db_id)) {
                return SyncTransportDecision::reject(
                    "sync db_id is not allowed", 403);
            }
            if (!m_allowed_nodes.empty() &&
                m_allowed_nodes.find(node_id) == m_allowed_nodes.end()) {
                return SyncTransportDecision::reject(node_error, 403);
            }
            return SyncTransportDecision::allow();
        }

        mutable std::mutex m_mutex;
        std::set<NodeId> m_allowed_nodes;
        SyncDbAccess m_db_access;
    };

    /// \brief Allows only configured HTTP sync targets.
    /// \details Empty allow-list means every target is allowed.
    class HttpRouteAllowListPolicy : public ISyncTransportPolicy {
    public:
        void allow_target(const std::string& target) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_allowed_targets.insert(target);
        }

        void clear() {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_allowed_targets.clear();
        }

        SyncTransportDecision check_http_post(
                const std::string& target,
                const std::string& content_type,
                const std::vector<std::uint8_t>& body) override {
            (void)content_type;
            (void)body;
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_allowed_targets.empty() &&
                m_allowed_targets.find(target) == m_allowed_targets.end()) {
                return SyncTransportDecision::reject(
                    "sync HTTP route is not allowed", 403);
            }
            return SyncTransportDecision::allow();
        }

    private:
        mutable std::mutex m_mutex;
        std::set<std::string> m_allowed_targets;
    };

    /// \brief Allows only configured bearer tokens on HTTP sync requests.
    /// \details Empty allow-list means every token is allowed, but a token must
    /// still be present.
    class HttpBearerTokenPolicy : public ISyncTransportPolicy {
    public:
        void allow_token(const std::string& token) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_allowed_tokens.insert(token);
        }

        void clear() {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_allowed_tokens.clear();
        }

        SyncTransportDecision check_http_request(
                const HttpSyncRequest& request) override {
            const std::string token = http_bearer_token(request);
            if (token.empty()) {
                SyncTransportDecision decision =
                    SyncTransportDecision::reject(
                        "sync bearer token is missing", 401);
                decision.add_response_header(
                    "WWW-Authenticate",
                    "Bearer realm=\"mdbx-containers-sync\"");
                return decision;
            }

            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_allowed_tokens.empty() &&
                m_allowed_tokens.find(token) == m_allowed_tokens.end()) {
                SyncTransportDecision decision =
                    SyncTransportDecision::reject(
                        "sync bearer token is not allowed", 401);
                decision.add_response_header(
                    "WWW-Authenticate",
                    "Bearer realm=\"mdbx-containers-sync\"");
                return decision;
            }
            return SyncTransportDecision::allow();
        }

    private:
        mutable std::mutex m_mutex;
        std::set<std::string> m_allowed_tokens;
    };

    /// \brief Binds HTTP bearer tokens to sync \c NodeId values.
    /// \details This policy enforces the production-facing identity contract:
    /// the authenticated bearer principal must match
    /// \c PullRequest::requester for pull, \c PushRequest::sender for push,
    /// and \c LogicalRecoveryRequest::requester for logical recovery.
    /// Optional per-token DB access rules validate \c db_id before dispatch.
    /// Token bindings default to \c SyncDbAccess::any(); calling
    /// \c allow_db_id_for_token() switches that token to a restricted list.
    /// The policy decodes request DTOs only after the bearer token is
    /// accepted; adapter-local headers and remote addresses are not serialized.
    class HttpBearerNodeIdentityPolicy : public ISyncTransportPolicy {
    public:
        explicit HttpBearerNodeIdentityPolicy(
                const CodecBounds& bounds = CodecBounds())
            : m_bounds(bounds) {}

        void allow_token_for_node(const std::string& token,
                                  const NodeId& node_id) {
            std::lock_guard<std::mutex> lock(m_mutex);
            Binding& binding = m_bindings[token];
            binding.node_id = node_id;
        }

        void allow_db_id_for_token(const std::string& token,
                                   const DbId& db_id) {
            std::lock_guard<std::mutex> lock(m_mutex);
            std::map<std::string, Binding>::iterator it =
                m_bindings.find(token);
            if (it == m_bindings.end()) {
                throw std::invalid_argument(
                    "sync bearer token identity is not registered");
            }
            it->second.db_access.allow_db_id(db_id);
        }

        void allow_any_db_id_for_token(const std::string& token) {
            std::lock_guard<std::mutex> lock(m_mutex);
            std::map<std::string, Binding>::iterator it =
                m_bindings.find(token);
            if (it == m_bindings.end()) {
                throw std::invalid_argument(
                    "sync bearer token identity is not registered");
            }
            it->second.db_access.set_allow_any();
        }

        void deny_all_db_ids_for_token(const std::string& token) {
            std::lock_guard<std::mutex> lock(m_mutex);
            std::map<std::string, Binding>::iterator it =
                m_bindings.find(token);
            if (it == m_bindings.end()) {
                throw std::invalid_argument(
                    "sync bearer token identity is not registered");
            }
            it->second.db_access.set_deny_all();
        }

        void clear() {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_bindings.clear();
        }

        SyncTransportDecision check_http_request(
                const HttpSyncRequest& request) override {
            const std::string token = http_bearer_token(request);
            if (token.empty()) {
                return unauthorized("sync bearer token is missing");
            }

            Binding binding;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                std::map<std::string, Binding>::const_iterator it =
                    m_bindings.find(token);
                if (it == m_bindings.end()) {
                    return unauthorized(
                        "sync bearer token identity is not allowed");
                }
                binding = it->second;
            }

            if (request.content_type != HttpSyncRoutes::content_type()) {
                return SyncTransportDecision::allow();
            }
            if (request.target == HttpSyncRoutes::pull_target()) {
                return check_pull_identity(request.body, binding);
            }
            if (request.target == HttpSyncRoutes::push_target()) {
                return check_push_identity(request.body, binding);
            }
            if (request.target == HttpSyncRoutes::logical_recovery_target()) {
                return check_logical_recovery_identity(request.body, binding);
            }
            return SyncTransportDecision::allow();
        }

    private:
        struct Binding {
            Binding()
                : db_access(SyncDbAccess::any()) {}

            NodeId node_id{};
            SyncDbAccess db_access;
        };

        static SyncTransportDecision unauthorized(
                const std::string& message) {
            SyncTransportDecision decision =
                SyncTransportDecision::reject(message, 401);
            decision.add_response_header(
                "WWW-Authenticate",
                "Bearer realm=\"mdbx-containers-sync\"");
            return decision;
        }

        SyncTransportDecision check_pull_identity(
                const std::vector<std::uint8_t>& body,
                const Binding& binding) const {
            try {
                const PullRequest request =
                    TransportMessageCodec::decode_pull_request(
                        body, &m_bounds);
                if (request.requester != binding.node_id) {
                    return SyncTransportDecision::reject(
                        "sync requester does not match authenticated node",
                        403);
                }
                return check_db(binding, request.db_id);
            } catch (const std::length_error& e) {
                return SyncTransportDecision::reject(e.what(), 413);
            } catch (const std::exception& e) {
                return SyncTransportDecision::reject(e.what(), 400);
            }
        }

        SyncTransportDecision check_push_identity(
                const std::vector<std::uint8_t>& body,
                const Binding& binding) const {
            try {
                const PushRequest request =
                    TransportMessageCodec::decode_push_request(
                        body, &m_bounds);
                if (request.sender != binding.node_id) {
                    return SyncTransportDecision::reject(
                        "sync sender does not match authenticated node",
                        403);
                }
                return check_db(binding, request.db_id);
            } catch (const std::length_error& e) {
                return SyncTransportDecision::reject(e.what(), 413);
            } catch (const std::exception& e) {
                return SyncTransportDecision::reject(e.what(), 400);
            }
        }

        SyncTransportDecision check_logical_recovery_identity(
                const std::vector<std::uint8_t>& body,
                const Binding& binding) const {
            try {
                const LogicalRecoveryRequest request =
                    LogicalRecoveryProtocolCodec::decode_request(body, &m_bounds);
                if (request.requester != binding.node_id) {
                    return SyncTransportDecision::reject(
                        "logical recovery requester does not match authenticated node",
                        403);
                }
                return check_db(binding, request.db_id);
            } catch (const std::length_error& e) {
                return SyncTransportDecision::reject(e.what(), 413);
            } catch (const std::exception& e) {
                return SyncTransportDecision::reject(e.what(), 400);
            }
        }

        static SyncTransportDecision check_db(const Binding& binding,
                                              const DbId& db_id) {
            if (!binding.db_access.allows_db_id(db_id)) {
                return SyncTransportDecision::reject(
                    "sync db_id is not allowed for authenticated node", 403);
            }
            return SyncTransportDecision::allow();
        }

        mutable std::mutex m_mutex;
        CodecBounds m_bounds;
        std::map<std::string, Binding> m_bindings;
    };

    /// \brief Binds a WebSocket session identity to sync \c NodeId values.
    /// \details Concrete WebSocket bindings should authenticate the session
    /// before constructing \c WebSocketSyncRequestContext. This policy then
    /// decodes the binary DTO and requires \c PullRequest::requester or
    /// \c PushRequest::sender to match the authenticated session node.
    /// Optional per-session DB access rules validate \c db_id before dispatch.
    /// \c WebSocketSyncRequestContext::db_access defaults to deny all, so
    /// bindings must explicitly set \c SyncDbAccess::any() or allow specific
    /// DB ids. For WebSocket bindings, \c SyncTransportDecision::status_code
    /// may be interpreted as a WebSocket close code by the concrete adapter.
    class WebSocketAuthenticatedNodeIdentityPolicy
        : public ISyncTransportPolicy {
    public:
        explicit WebSocketAuthenticatedNodeIdentityPolicy(
                const CodecBounds& bounds = CodecBounds())
            : m_bounds(bounds) {}

        SyncTransportDecision check_websocket_message(
                const WebSocketSyncRequestContext& request) override {
            if (!request.has_authenticated_node) {
                return SyncTransportDecision::reject(
                    "sync WebSocket authenticated node is missing", 1008);
            }

            try {
                if (is_logical_recovery_message(request.binary_message)) {
                    return check_logical_recovery_identity(request);
                }
                const TransportMessageType type =
                    TransportMessageCodec::peek_message_type(
                        request.binary_message, &m_bounds);
                switch (type) {
                    case TransportMessageType::PullRequest:
                        return check_pull_identity(request);
                    case TransportMessageType::PushRequest:
                        return check_push_identity(request);
                    case TransportMessageType::PullResponse:
                    case TransportMessageType::PushResponse:
                        return SyncTransportDecision::reject(
                            "sync WebSocket server received response message",
                            1008);
                }
            } catch (const std::length_error& e) {
                return SyncTransportDecision::reject(e.what(), 1009);
            } catch (const std::exception& e) {
                return SyncTransportDecision::reject(e.what(), 1007);
            }
            return SyncTransportDecision::reject(
                "Unexpected WebSocket sync message type", 1008);
        }

    private:
        static bool is_logical_recovery_message(
                const std::vector<std::uint8_t>& message) {
            return message.size() >= LogicalRecoveryProtocolCodec::magic_size() &&
                std::memcmp(&message[0], LogicalRecoveryProtocolCodec::magic(),
                            LogicalRecoveryProtocolCodec::magic_size()) == 0;
        }

        SyncTransportDecision check_pull_identity(
                const WebSocketSyncRequestContext& context) const {
            const PullRequest request =
                TransportMessageCodec::decode_pull_request(
                    context.binary_message, &m_bounds);
            if (request.requester != context.authenticated_node) {
                return SyncTransportDecision::reject(
                    "sync requester does not match authenticated WebSocket node",
                    1008);
            }
            return check_db(context.db_access, request.db_id);
        }

        SyncTransportDecision check_push_identity(
                const WebSocketSyncRequestContext& context) const {
            const PushRequest request =
                TransportMessageCodec::decode_push_request(
                    context.binary_message, &m_bounds);
            if (request.sender != context.authenticated_node) {
                return SyncTransportDecision::reject(
                    "sync sender does not match authenticated WebSocket node",
                    1008);
            }
            return check_db(context.db_access, request.db_id);
        }

        SyncTransportDecision check_logical_recovery_identity(
                const WebSocketSyncRequestContext& context) const {
            const LogicalRecoveryProtocolCodec::MessageType type =
                LogicalRecoveryProtocolCodec::peek_message_type(
                    context.binary_message, &m_bounds);
            if (type != LogicalRecoveryProtocolCodec::MessageType::Request) {
                return SyncTransportDecision::reject(
                    "sync WebSocket server received logical recovery response message",
                    1008);
            }
            const LogicalRecoveryRequest request =
                LogicalRecoveryProtocolCodec::decode_request(
                    context.binary_message, &m_bounds);
            if (request.requester != context.authenticated_node) {
                return SyncTransportDecision::reject(
                    "logical recovery requester does not match authenticated WebSocket node",
                    1008);
            }
            return check_db(context.db_access, request.db_id);
        }

        static SyncTransportDecision check_db(
                const SyncDbAccess& db_access,
                const DbId& db_id) {
            if (!db_access.allows_db_id(db_id)) {
                return SyncTransportDecision::reject(
                    "sync db_id is not allowed for authenticated WebSocket node",
                    1008);
            }
            return SyncTransportDecision::allow();
        }

        CodecBounds m_bounds;
    };

    /// \brief WebSocket policy rejection with a concrete close code.
    /// \details Server-side WebSocket bindings should catch this exception and
    /// send \c close_code() as the WebSocket close frame status. Other
    /// exceptions from the binding or server path usually map to an internal
    /// server error close code such as 1011.
    class WebSocketSyncRejected : public std::runtime_error {
    public:
        WebSocketSyncRejected(unsigned close_code,
                              const std::string& message)
            : std::runtime_error(message),
              m_close_code(close_code) {}

        unsigned close_code() const {
            return m_close_code;
        }

    private:
        unsigned m_close_code;
    };

    /// \brief Allows only configured remote addresses on HTTP sync requests.
    /// \details Empty allow-list means every remote address is allowed.
    class HttpRemoteAddressAllowListPolicy : public ISyncTransportPolicy {
    public:
        void allow_remote_address(const std::string& remote_address) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_allowed_remote_addresses.insert(remote_address);
        }

        void clear() {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_allowed_remote_addresses.clear();
        }

        SyncTransportDecision check_http_request(
                const HttpSyncRequest& request) override {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_allowed_remote_addresses.empty() &&
                m_allowed_remote_addresses.find(request.remote_address) ==
                    m_allowed_remote_addresses.end()) {
                return SyncTransportDecision::reject(
                    "sync remote address is not allowed", 403);
            }
            return SyncTransportDecision::allow();
        }

    private:
        mutable std::mutex m_mutex;
        std::set<std::string> m_allowed_remote_addresses;
    };

    /// \brief Fixed-window HTTP request limiter keyed by token or remote address.
    /// \details Uses the bearer token when present, otherwise \c remote_address,
    /// otherwise the literal "anonymous". Rejections include a \c Retry-After
    /// header with the remaining window time in seconds. \p max_buckets caps
    /// tracked client identities when non-zero; expired buckets are evicted
    /// before a new identity is rejected.
    class FixedWindowHttpRateLimitPolicy : public ISyncTransportPolicy {
    public:
        FixedWindowHttpRateLimitPolicy(std::uint64_t max_requests,
                                       std::chrono::seconds window,
                                       std::size_t max_buckets = 0)
            : m_max_requests(max_requests),
              m_window(window),
              m_max_buckets(max_buckets) {
            if (window.count() <= 0) {
                throw std::invalid_argument(
                    "HTTP rate-limit window must be positive");
            }
        }

        void clear() {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_buckets.clear();
        }

        std::size_t bucket_count() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_buckets.size();
        }

        SyncTransportDecision check_http_request(
                const HttpSyncRequest& request) override {
            const std::chrono::steady_clock::time_point now =
                std::chrono::steady_clock::now();
            const std::string identity = client_identity(request);

            std::lock_guard<std::mutex> lock(m_mutex);
            std::map<std::string, Bucket>::iterator it =
                m_buckets.find(identity);
            if (it == m_buckets.end()) {
                evict_expired_buckets(now);
                if (m_max_buckets != 0 &&
                    m_buckets.size() >= m_max_buckets) {
                    SyncTransportDecision decision =
                        SyncTransportDecision::reject(
                            "sync HTTP rate-limit bucket cap exceeded", 429);
                    decision.add_response_header(
                        "Retry-After",
                        retry_after_seconds(now, earliest_reset_at()));
                    return decision;
                }
                it = m_buckets.insert(
                    std::make_pair(identity, Bucket())).first;
            }

            Bucket& bucket = it->second;
            if (bucket.reset_at == std::chrono::steady_clock::time_point() ||
                now >= bucket.reset_at) {
                bucket.remaining = m_max_requests;
                bucket.reset_at = now + m_window;
            }

            if (bucket.remaining == 0) {
                SyncTransportDecision decision =
                    SyncTransportDecision::reject(
                        "sync HTTP rate limit exceeded", 429);
                decision.add_response_header(
                    "Retry-After",
                    retry_after_seconds(now, bucket.reset_at));
                return decision;
            }
            --bucket.remaining;
            return SyncTransportDecision::allow();
        }

    private:
        struct Bucket {
            Bucket() : remaining(0), reset_at() {}

            std::uint64_t remaining;
            std::chrono::steady_clock::time_point reset_at;
        };

        static std::string client_identity(const HttpSyncRequest& request) {
            const std::string token = http_bearer_token(request);
            if (!token.empty()) {
                return std::string("token:") + token;
            }
            if (!request.remote_address.empty()) {
                return std::string("remote:") + request.remote_address;
            }
            return "anonymous";
        }

        static std::string retry_after_seconds(
                std::chrono::steady_clock::time_point now,
                std::chrono::steady_clock::time_point reset_at) {
            if (reset_at <= now) {
                return "0";
            }
            const std::chrono::seconds seconds =
                std::chrono::duration_cast<std::chrono::seconds>(
                    reset_at - now);
            const std::uint64_t count =
                seconds.count() <= 0 ? 1u
                                     : static_cast<std::uint64_t>(
                                           seconds.count());
            return std::to_string(count);
        }

        void evict_expired_buckets(
                std::chrono::steady_clock::time_point now) {
            for (std::map<std::string, Bucket>::iterator it =
                     m_buckets.begin();
                 it != m_buckets.end();) {
                if (it->second.reset_at !=
                        std::chrono::steady_clock::time_point() &&
                    now >= it->second.reset_at) {
                    m_buckets.erase(it++);
                } else {
                    ++it;
                }
            }
        }

        std::chrono::steady_clock::time_point earliest_reset_at() const {
            std::chrono::steady_clock::time_point earliest;
            for (std::map<std::string, Bucket>::const_iterator it =
                     m_buckets.begin();
                 it != m_buckets.end(); ++it) {
                if (earliest == std::chrono::steady_clock::time_point() ||
                    it->second.reset_at < earliest) {
                    earliest = it->second.reset_at;
                }
            }
            return earliest;
        }

        mutable std::mutex m_mutex;
        std::uint64_t m_max_requests;
        std::chrono::seconds m_window;
        std::size_t m_max_buckets;
        std::map<std::string, Bucket> m_buckets;
    };

    /// \brief Rejects HTTP bodies and WebSocket messages above a byte limit.
    /// \details This is a pre-decode adapter guard. \c CodecBounds still
    /// validates the binary DTO payload during encode/decode.
    class TransportMessageSizePolicy : public ISyncTransportPolicy {
    public:
        explicit TransportMessageSizePolicy(
                std::size_t max_transport_message_bytes)
            : m_max_transport_message_bytes(max_transport_message_bytes) {
            if (max_transport_message_bytes == 0) {
                throw std::invalid_argument(
                    "sync transport message size limit must be positive");
            }
        }

        SyncTransportDecision check_http_post(
                const std::string& target,
                const std::string& content_type,
                const std::vector<std::uint8_t>& body) override {
            (void)target;
            (void)content_type;
            if (body.size() > m_max_transport_message_bytes) {
                return SyncTransportDecision::reject(
                    "sync HTTP body is too large", 413);
            }
            return SyncTransportDecision::allow();
        }

        SyncTransportDecision check_websocket_message(
                const WebSocketSyncRequestContext& request) override {
            if (request.binary_message.size() >
                m_max_transport_message_bytes) {
                return SyncTransportDecision::reject(
                    "sync WebSocket message is too large", 1009);
            }
            return SyncTransportDecision::allow();
        }

    private:
        std::size_t m_max_transport_message_bytes;
    };

    /// \brief Fixed request-budget policy for deterministic rate-limit tests.
    /// \details Each accepted operation consumes one budget unit. Use
    /// \c unlimited_budget() for dimensions that should not be limited.
    class FixedBudgetSyncTransportPolicy : public ISyncTransportPolicy {
    public:
        static std::uint64_t unlimited_budget() {
            return std::numeric_limits<std::uint64_t>::max();
        }

        FixedBudgetSyncTransportPolicy(
                std::uint64_t pull_budget,
                std::uint64_t push_budget,
                std::uint64_t http_post_budget = unlimited_budget())
            : m_pull_remaining(pull_budget),
              m_push_remaining(push_budget),
              m_http_post_remaining(http_post_budget) {}

        void reset(std::uint64_t pull_budget,
                   std::uint64_t push_budget,
                   std::uint64_t http_post_budget = unlimited_budget()) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_pull_remaining = pull_budget;
            m_push_remaining = push_budget;
            m_http_post_remaining = http_post_budget;
        }

        SyncTransportDecision check_pull(
                const PullRequest& request) override {
            (void)request;
            return consume(m_pull_remaining, "sync pull rate limit exceeded");
        }

        SyncTransportDecision check_push(
                const PushRequest& request) override {
            (void)request;
            return consume(m_push_remaining, "sync push rate limit exceeded");
        }

        SyncTransportDecision check_http_post(
                const std::string& target,
                const std::string& content_type,
                const std::vector<std::uint8_t>& body) override {
            (void)target;
            (void)content_type;
            (void)body;
            return consume(m_http_post_remaining,
                           "sync HTTP post rate limit exceeded");
        }

    private:
        SyncTransportDecision consume(std::uint64_t& remaining,
                                      const char* error) {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (remaining == unlimited_budget()) {
                return SyncTransportDecision::allow();
            }
            if (remaining == 0) {
                return SyncTransportDecision::reject(error, 429);
            }
            --remaining;
            return SyncTransportDecision::allow();
        }

        mutable std::mutex m_mutex;
        std::uint64_t m_pull_remaining;
        std::uint64_t m_push_remaining;
        std::uint64_t m_http_post_remaining;
    };

    /// \brief Runs several policies in insertion order.
    /// \details Added policies are non-owning references and must outlive the
    /// composite. Build the policy list before publishing the composite to
    /// worker threads; calling \c add() concurrently with \c check_*() is not
    /// supported.
    class CompositeSyncTransportPolicy : public ISyncTransportPolicy {
    public:
        void add(ISyncTransportPolicy& policy) {
            m_policies.push_back(&policy);
        }

        SyncTransportDecision check_pull(
                const PullRequest& request) override {
            for (std::size_t i = 0; i < m_policies.size(); ++i) {
                const SyncTransportDecision decision =
                    m_policies[i]->check_pull(request);
                if (!decision.allowed) {
                    return decision;
                }
            }
            return SyncTransportDecision::allow();
        }

        SyncTransportDecision check_push(
                const PushRequest& request) override {
            for (std::size_t i = 0; i < m_policies.size(); ++i) {
                const SyncTransportDecision decision =
                    m_policies[i]->check_push(request);
                if (!decision.allowed) {
                    return decision;
                }
            }
            return SyncTransportDecision::allow();
        }

        SyncTransportDecision check_http_post(
                const std::string& target,
                const std::string& content_type,
                const std::vector<std::uint8_t>& body) override {
            for (std::size_t i = 0; i < m_policies.size(); ++i) {
                const SyncTransportDecision decision =
                    m_policies[i]->check_http_post(target, content_type, body);
                if (!decision.allowed) {
                    return decision;
                }
            }
            return SyncTransportDecision::allow();
        }

        SyncTransportDecision check_http_request(
                const HttpSyncRequest& request) override {
            for (std::size_t i = 0; i < m_policies.size(); ++i) {
                const SyncTransportDecision decision =
                    m_policies[i]->check_http_request(request);
                if (!decision.allowed) {
                    return decision;
                }
            }
            return SyncTransportDecision::allow();
        }

        SyncTransportDecision check_websocket_message(
                const WebSocketSyncRequestContext& request) override {
            for (std::size_t i = 0; i < m_policies.size(); ++i) {
                const SyncTransportDecision decision =
                    m_policies[i]->check_websocket_message(request);
                if (!decision.allowed) {
                    return decision;
                }
            }
            return SyncTransportDecision::allow();
        }

    private:
        std::vector<ISyncTransportPolicy*> m_policies;
    };

    /// \brief Snapshot of counters recorded by \c SyncTransportMetricsObserver.
    /// \details Counters represent middleware hook invocations. If the same
    /// observer is installed at several stacked middleware layers, one logical
    /// user action may increment more than one counter.
    struct SyncTransportMetricsSnapshot {
        std::uint64_t pull_calls = 0;
        std::uint64_t push_calls = 0;
        std::uint64_t http_post_calls = 0;
        std::uint64_t rejected_calls = 0;
        std::uint64_t failed_calls = 0;
        std::uint64_t request_cancel_calls = 0;
        std::uint64_t http_request_calls = 0;
        std::uint64_t websocket_message_calls = 0;
        std::uint64_t pulled_batches = 0;
        std::uint64_t pushed_batches = 0;
    };

    /// \brief Best-effort observer for transport middleware events.
    /// \details Middleware wrappers swallow observer exceptions so logging or
    /// metrics hooks cannot change transport behavior.
    class ISyncTransportObserver {
    public:
        virtual ~ISyncTransportObserver() {}

        virtual void on_sync_transport_rejected(
                SyncTransportOperation operation,
                const std::string& error) {
            (void)operation;
            (void)error;
        }

        virtual void on_sync_transport_pull_result(
                const PullRequest& request,
                const PullResponse& response) {
            (void)request;
            (void)response;
        }

        virtual void on_sync_transport_push_result(
                const PushRequest& request,
                const PushResponse& response) {
            (void)request;
            (void)response;
        }

        virtual void on_sync_transport_http_request(
                const HttpSyncRequest& request) {
            (void)request;
        }

        virtual void on_sync_transport_http_post_result(
                const std::string& target,
                const HttpSyncResponse& response) {
            (void)target;
            (void)response;
        }

        virtual void on_sync_transport_websocket_message(
                const WebSocketSyncRequestContext& request) {
            (void)request;
        }

        virtual void on_sync_transport_exception(
                SyncTransportOperation operation,
                const std::string& error) {
            (void)operation;
            (void)error;
        }

        virtual void on_sync_transport_cancel_requested() {}
    };

    /// \brief Thread-safe counter observer for basic transport metrics.
    /// \details Counts middleware hook invocations, not necessarily distinct
    /// logical user operations. Use separate observers when peer-level and
    /// HTTP-level layers should be reported independently.
    class SyncTransportMetricsObserver : public ISyncTransportObserver {
    public:
        SyncTransportMetricsSnapshot snapshot() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_snapshot;
        }

        void reset() {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_snapshot = SyncTransportMetricsSnapshot();
        }

        void on_sync_transport_rejected(
                SyncTransportOperation operation,
                const std::string& error) override {
            (void)operation;
            (void)error;
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_snapshot.rejected_calls;
        }

        void on_sync_transport_pull_result(
                const PullRequest& request,
                const PullResponse& response) override {
            (void)request;
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_snapshot.pull_calls;
            if (!response.ok) {
                ++m_snapshot.failed_calls;
            }
            m_snapshot.pulled_batches += response.batches.size();
        }

        void on_sync_transport_push_result(
                const PushRequest& request,
                const PushResponse& response) override {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_snapshot.push_calls;
            if (!response.ok) {
                ++m_snapshot.failed_calls;
            }
            m_snapshot.pushed_batches += request.batches.size();
        }

        void on_sync_transport_http_request(
                const HttpSyncRequest& request) override {
            (void)request;
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_snapshot.http_request_calls;
        }

        void on_sync_transport_http_post_result(
                const std::string& target,
                const HttpSyncResponse& response) override {
            (void)target;
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_snapshot.http_post_calls;
            if (response.status_code < 200 || response.status_code >= 300) {
                ++m_snapshot.failed_calls;
            }
        }

        void on_sync_transport_websocket_message(
                const WebSocketSyncRequestContext& request) override {
            (void)request;
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_snapshot.websocket_message_calls;
        }

        void on_sync_transport_exception(
                SyncTransportOperation operation,
                const std::string& error) override {
            (void)operation;
            (void)error;
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_snapshot.failed_calls;
        }

        void on_sync_transport_cancel_requested() override {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_snapshot.request_cancel_calls;
        }

    private:
        mutable std::mutex m_mutex;
        SyncTransportMetricsSnapshot m_snapshot;
    };

    namespace detail {

        inline void notify_transport_rejected(
                ISyncTransportObserver* observer,
                SyncTransportOperation operation,
                const std::string& error) {
            if (observer == nullptr) {
                return;
            }
            try {
                observer->on_sync_transport_rejected(operation, error);
            } catch (...) {}
        }

        inline void notify_transport_exception(
                ISyncTransportObserver* observer,
                SyncTransportOperation operation,
                const std::string& error) {
            if (observer == nullptr) {
                return;
            }
            try {
                observer->on_sync_transport_exception(operation, error);
            } catch (...) {}
        }

        inline void notify_transport_cancel_requested(
                ISyncTransportObserver* observer) {
            if (observer == nullptr) {
                return;
            }
            try {
                observer->on_sync_transport_cancel_requested();
            } catch (...) {}
        }

        inline void notify_transport_http_request(
                ISyncTransportObserver* observer,
                const HttpSyncRequest& request) {
            if (observer == nullptr) {
                return;
            }
            try {
                observer->on_sync_transport_http_request(request);
            } catch (...) {}
        }

        inline void notify_transport_websocket_message(
                ISyncTransportObserver* observer,
                const WebSocketSyncRequestContext& request) {
            if (observer == nullptr) {
                return;
            }
            try {
                observer->on_sync_transport_websocket_message(request);
            } catch (...) {}
        }

        inline HttpSyncResponse make_http_error_response(
                const SyncTransportDecision& decision,
                const char* fallback) {
            HttpSyncResponse response;
            response.status_code =
                decision.status_code == 0 ? 403 : decision.status_code;
            response.content_type = "text/plain; charset=utf-8";
            response.headers = decision.response_headers;
            response.error = decision.error.empty()
                ? std::string(fallback)
                : decision.error;
            response.body.assign(response.error.begin(), response.error.end());
            return response;
        }

    } // namespace detail

    /// \brief \c ISyncPeer wrapper that applies policy and observer hooks.
    /// \details The wrapped peer must outlive the middleware. The optional
    /// policy and observer are non-owning pointers and must also outlive the
    /// middleware when provided.
    class SyncPeerMiddleware : public ISyncPeer {
    public:
        explicit SyncPeerMiddleware(
                ISyncPeer& next,
                ISyncTransportPolicy* policy = nullptr,
                ISyncTransportObserver* observer = nullptr)
            : m_next(next),
              m_policy(policy),
              m_observer(observer) {}

        PullResponse pull(const PullRequest& request) override {
            if (m_policy != nullptr) {
                const SyncTransportDecision decision =
                    m_policy->check_pull(request);
                if (!decision.allowed) {
                    detail::notify_transport_rejected(
                        m_observer, SyncTransportOperation::Pull,
                        decision.error);
                    PullResponse response;
                    response.ok = false;
                    response.error = reject_message(decision, "pull rejected");
                    return response;
                }
            }

            try {
                const PullResponse response = m_next.pull(request);
                notify_pull_result(request, response);
                return response;
            } catch (const std::exception& e) {
                detail::notify_transport_exception(
                    m_observer, SyncTransportOperation::Pull, e.what());
                throw;
            } catch (...) {
                detail::notify_transport_exception(
                    m_observer, SyncTransportOperation::Pull,
                    "unknown pull exception");
                throw;
            }
        }

        PushResponse push(const PushRequest& request) override {
            if (m_policy != nullptr) {
                const SyncTransportDecision decision =
                    m_policy->check_push(request);
                if (!decision.allowed) {
                    detail::notify_transport_rejected(
                        m_observer, SyncTransportOperation::Push,
                        decision.error);
                    PushResponse response;
                    response.ok = false;
                    response.error = reject_message(decision, "push rejected");
                    return response;
                }
            }

            try {
                const PushResponse response = m_next.push(request);
                notify_push_result(request, response);
                return response;
            } catch (const std::exception& e) {
                detail::notify_transport_exception(
                    m_observer, SyncTransportOperation::Push, e.what());
                throw;
            } catch (...) {
                detail::notify_transport_exception(
                    m_observer, SyncTransportOperation::Push,
                    "unknown push exception");
                throw;
            }
        }

        void request_cancel() override {
            detail::notify_transport_cancel_requested(m_observer);
            m_next.request_cancel();
        }

        bool supports_logical_delivery() const override {
            return m_next.supports_logical_delivery();
        }

        LogicalDeliveryHello logical_delivery_hello() override {
            return m_next.logical_delivery_hello();
        }

        LogicalDeliveryHello logical_delivery_hello_with_cancel(
                const CancellationToken* cancel_token = nullptr) override {
            return m_next.logical_delivery_hello_with_cancel(cancel_token);
        }

        LogicalDeliveryAcknowledgement deliver_ordered_logical_delivery(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr) override {
            return m_next.deliver_ordered_logical_delivery(envelope, bounds);
        }

        LogicalDeliveryAcknowledgement
        deliver_ordered_logical_delivery_with_cancel(
                const LogicalDeliveryEnvelope& envelope,
                const CodecBounds* bounds = nullptr,
                const CancellationToken* cancel_token = nullptr) override {
            return m_next.deliver_ordered_logical_delivery_with_cancel(
                envelope, bounds, cancel_token);
        }

        LogicalDeliveryAcknowledgement deliver_ordered_logical_request(
                const LogicalDeliveryRequest& request,
                const CodecBounds* bounds = nullptr) override {
            return m_next.deliver_ordered_logical_request(request, bounds);
        }

        LogicalDeliveryAcknowledgement
        deliver_ordered_logical_request_with_cancel(
                const LogicalDeliveryRequest& request,
                const CodecBounds* bounds = nullptr,
                const CancellationToken* cancel_token = nullptr) override {
            return m_next.deliver_ordered_logical_request_with_cancel(
                request, bounds, cancel_token);
        }

        bool supports_logical_recovery() const override {
            return m_next.supports_logical_recovery();
        }

        LogicalRecoveryResponse logical_recovery(
                const LogicalRecoveryRequest& request) override {
            return m_next.logical_recovery(request);
        }

        LogicalRecoveryResponse logical_recovery_with_cancel(
                const LogicalRecoveryRequest& request,
                const CancellationToken* cancel_token = nullptr) override {
            return m_next.logical_recovery_with_cancel(request, cancel_token);
        }

    private:
        static std::string reject_message(
                const SyncTransportDecision& decision,
                const char* fallback) {
            return decision.error.empty() ? std::string(fallback)
                                          : decision.error;
        }

        void notify_pull_result(const PullRequest& request,
                                const PullResponse& response) const {
            if (m_observer == nullptr) {
                return;
            }
            try {
                m_observer->on_sync_transport_pull_result(request, response);
            } catch (...) {}
        }

        void notify_push_result(const PushRequest& request,
                                const PushResponse& response) const {
            if (m_observer == nullptr) {
                return;
            }
            try {
                m_observer->on_sync_transport_push_result(request, response);
            } catch (...) {}
        }

        ISyncPeer& m_next;
        ISyncTransportPolicy* m_policy;
        ISyncTransportObserver* m_observer;
    };

    /// \brief \c HttpSyncServer wrapper that applies request-context policy.
    /// \details The wrapped server must outlive the middleware. The optional
    /// policy and observer are non-owning pointers and must also outlive the
    /// middleware when provided.
    class HttpSyncServerMiddleware {
    public:
        explicit HttpSyncServerMiddleware(
                HttpSyncServer& next,
                ISyncTransportPolicy* policy = nullptr,
                ISyncTransportObserver* observer = nullptr)
            : m_next(next),
              m_policy(policy),
              m_observer(observer) {}

        HttpSyncResponse handle(const HttpSyncRequest& request) {
            detail::notify_transport_http_request(m_observer, request);
            if (m_policy != nullptr) {
                const SyncTransportDecision decision =
                    m_policy->check_http_request(request);
                if (!decision.allowed) {
                    detail::notify_transport_rejected(
                        m_observer, SyncTransportOperation::HttpPost,
                        decision.error);
                    HttpSyncResponse response =
                        detail::make_http_error_response(
                        decision, "HTTP sync request rejected");
                    http_copy_sync_correlation_headers(
                        request.headers, response.headers);
                    return response;
                }
            }

            try {
                const HttpSyncResponse response = m_next.handle(request);
                notify_http_post_result(request.target, response);
                return response;
            } catch (const std::exception& e) {
                detail::notify_transport_exception(
                    m_observer, SyncTransportOperation::HttpPost, e.what());
                throw;
            } catch (...) {
                detail::notify_transport_exception(
                    m_observer, SyncTransportOperation::HttpPost,
                    "unknown HTTP server exception");
                throw;
            }
        }

    private:
        void notify_http_post_result(const std::string& target,
                                     const HttpSyncResponse& response) const {
            if (m_observer == nullptr) {
                return;
            }
            try {
                m_observer->on_sync_transport_http_post_result(target, response);
            } catch (...) {}
        }

        HttpSyncServer& m_next;
        ISyncTransportPolicy* m_policy;
        ISyncTransportObserver* m_observer;
    };

    /// \brief \c WebSocketSyncServer wrapper that applies session policy.
    /// \details The wrapped server must outlive the middleware. Concrete
    /// WebSocket bindings provide an adapter-local request context containing
    /// the authenticated session node and one complete binary sync message.
    class WebSocketSyncServerMiddleware {
    public:
        explicit WebSocketSyncServerMiddleware(
                WebSocketSyncServer& next,
                ISyncTransportPolicy* policy = nullptr,
                ISyncTransportObserver* observer = nullptr)
            : m_next(next),
              m_policy(policy),
              m_observer(observer) {}

        std::vector<std::uint8_t> handle_binary_message(
                const WebSocketSyncRequestContext& request) {
            detail::notify_transport_websocket_message(m_observer, request);
            if (m_policy != nullptr) {
                const SyncTransportDecision decision =
                    m_policy->check_websocket_message(request);
                if (!decision.allowed) {
                    detail::notify_transport_rejected(
                        m_observer, SyncTransportOperation::WebSocketMessage,
                        decision.error);
                    throw WebSocketSyncRejected(
                        decision.status_code == 0 ? 1008
                                                  : decision.status_code,
                        reject_message(
                            decision, "WebSocket sync request rejected"));
                }
            }

            try {
                return m_next.handle_binary_message(request.binary_message);
            } catch (const WebSocketSyncRejected&) {
                throw;
            } catch (const std::exception& e) {
                detail::notify_transport_exception(
                    m_observer, SyncTransportOperation::WebSocketMessage,
                    e.what());
                throw;
            } catch (...) {
                detail::notify_transport_exception(
                    m_observer, SyncTransportOperation::WebSocketMessage,
                    "unknown WebSocket server exception");
                throw;
            }
        }

    private:
        static std::string reject_message(
                const SyncTransportDecision& decision,
                const char* fallback) {
            return decision.error.empty() ? std::string(fallback)
                                          : decision.error;
        }

        WebSocketSyncServer& m_next;
        ISyncTransportPolicy* m_policy;
        ISyncTransportObserver* m_observer;
    };

    /// \brief \c IHttpSyncClient wrapper that applies route-level policy.
    /// \details The wrapped client must outlive the middleware. The optional
    /// policy and observer are non-owning pointers and must also outlive the
    /// middleware when provided.
    class HttpSyncClientMiddleware : public IHttpSyncClient {
    public:
        explicit HttpSyncClientMiddleware(
                IHttpSyncClient& next,
                ISyncTransportPolicy* policy = nullptr,
                ISyncTransportObserver* observer = nullptr)
            : m_next(next),
              m_policy(policy),
              m_observer(observer) {}

        HttpSyncResponse post(
                const std::string& target,
                const std::string& content_type,
                const std::vector<std::uint8_t>& body,
                const CancellationToken& cancel_token) override {
            HttpSyncRequest request;
            request.method = HttpSyncRoutes::method_post();
            request.target = target;
            request.content_type = content_type;
            request.body = body;

            if (m_policy != nullptr) {
                const SyncTransportDecision decision =
                    m_policy->check_http_request(request);
                if (!decision.allowed) {
                    detail::notify_transport_rejected(
                        m_observer, SyncTransportOperation::HttpPost,
                        decision.error);
                    return detail::make_http_error_response(
                        decision, "HTTP sync request rejected");
                }
            }

            try {
                const HttpSyncResponse response =
                    m_next.post(target, content_type, body, cancel_token);
                notify_http_post_result(target, response);
                return response;
            } catch (const std::exception& e) {
                detail::notify_transport_exception(
                    m_observer, SyncTransportOperation::HttpPost, e.what());
                throw;
            } catch (...) {
                detail::notify_transport_exception(
                    m_observer, SyncTransportOperation::HttpPost,
                    "unknown HTTP post exception");
                throw;
            }
        }

        void request_cancel() override {
            detail::notify_transport_cancel_requested(m_observer);
            m_next.request_cancel();
        }

    private:
        void notify_http_post_result(const std::string& target,
                                     const HttpSyncResponse& response) const {
            if (m_observer == nullptr) {
                return;
            }
            try {
                m_observer->on_sync_transport_http_post_result(target, response);
            } catch (...) {}
        }

        IHttpSyncClient& m_next;
        ISyncTransportPolicy* m_policy;
        ISyncTransportObserver* m_observer;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_TRANSPORT_TRANSPORTMIDDLEWARE_HPP_INCLUDED
