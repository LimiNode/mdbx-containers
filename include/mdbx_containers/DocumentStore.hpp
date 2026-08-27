#pragma once
#ifndef MDBX_CONTAINERS_HEADER_DOCUMENT_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_DOCUMENT_STORE_HPP_INCLUDED

/// \file DocumentStore.hpp
/// \brief Source-document records with generated IDs and unique source URIs.

#include "common.hpp"
#include "IdAllocatorTable.hpp"
#include "KeyValueTable.hpp"
#include "detail/CompositeStoreTransaction.hpp"
#include "rag/Records.hpp"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace mdbxc {

    /// \brief Persistent source-document store for RAG-style provenance.
    /// \details Owns three DBIs: records, ID allocator, and source URI index.
    class DocumentStore {
    public:
        explicit DocumentStore(std::shared_ptr<Connection> connection,
                               std::string name = "documents")
            : m_connection(require_connection(std::move(connection)))
            , m_ids(m_connection, make_name(name, "document_ids"))
            , m_records(m_connection, make_name(name, "document_records"))
            , m_source_ids(m_connection, make_name(name, "document_source_uris")) {}

        explicit DocumentStore(const Config& config, std::string name = "documents")
            : DocumentStore(Connection::create(config), std::move(name)) {}

        /// \brief Adds a document and returns its generated positive ID.
        /// \throws std::invalid_argument if id is preassigned or source_uri is empty.
        /// \throws std::invalid_argument if source_uri already belongs to another document.
        std::uint64_t add(Document document, MDBX_txn* txn = nullptr) {
            validate_new_document(document);
            ensure_local_only();
            std::uint64_t result = 0u;
            with_write_transaction([this, &document, &result](MDBX_txn* t) {
                result = m_ids.next(t);
                document.id = result;
                if (!m_source_ids.insert(document.source_uri, result, t)) {
                    throw std::invalid_argument("DocumentStore source_uri already exists");
                }
                m_records.insert_or_assign(result, detail::encode_document(document), t);
            }, txn);
            return result;
        }

        std::uint64_t add(Document document, const Transaction& txn) {
            return add(std::move(document), txn.handle());
        }

        Document get(std::uint64_t id, MDBX_txn* txn = nullptr) const {
            std::pair<bool, Document> found = find_compat(id, txn);
            if (!found.first) {
                throw std::out_of_range("DocumentStore: document not found");
            }
            return std::move(found.second);
        }

        Document get(std::uint64_t id, const Transaction& txn) const {
            return get(id, txn.handle());
        }

        std::pair<bool, Document> find_compat(std::uint64_t id,
                                              MDBX_txn* txn = nullptr) const {
            std::pair<bool, Document> result(false, Document());
            with_read_transaction([this, id, &result](MDBX_txn* t) {
                std::pair<bool, std::vector<std::uint8_t> > stored =
                    m_records.find_compat(id, t);
                if (stored.first) {
                    result.first = true;
                    result.second = detail::decode_document(stored.second);
                    if (result.second.id != id) {
                        throw std::runtime_error("DocumentStore record ID mismatch");
                    }
                }
            }, txn);
            return result;
        }

        std::pair<bool, Document> find_compat(std::uint64_t id,
                                              const Transaction& txn) const {
            return find_compat(id, txn.handle());
        }

        Document get_by_source_uri(const std::string& source_uri,
                                   MDBX_txn* txn = nullptr) const {
            std::pair<bool, Document> found = find_by_source_uri_compat(source_uri, txn);
            if (!found.first) {
                throw std::out_of_range("DocumentStore: source URI not found");
            }
            return std::move(found.second);
        }

        Document get_by_source_uri(const std::string& source_uri,
                                   const Transaction& txn) const {
            return get_by_source_uri(source_uri, txn.handle());
        }

        std::pair<bool, Document> find_by_source_uri_compat(
            const std::string& source_uri, MDBX_txn* txn = nullptr) const {
            std::pair<bool, Document> result(false, Document());
            with_read_transaction([this, &source_uri, &result](MDBX_txn* t) {
                std::pair<bool, std::uint64_t> id = m_source_ids.find_compat(source_uri, t);
                if (!id.first) {
                    return;
                }
                std::pair<bool, Document> document = find_compat(id.second, t);
                if (!document.first) {
                    throw std::runtime_error("DocumentStore source URI index is stale");
                }
                if (document.second.source_uri != source_uri) {
                    throw std::runtime_error("DocumentStore source URI index is inconsistent");
                }
                result = std::move(document);
            }, txn);
            return result;
        }

        std::pair<bool, Document> find_by_source_uri_compat(
            const std::string& source_uri, const Transaction& txn) const {
            return find_by_source_uri_compat(source_uri, txn.handle());
        }

        bool erase(std::uint64_t id, MDBX_txn* txn = nullptr) {
            ensure_local_only();
            bool removed = false;
            with_write_transaction([this, id, &removed](MDBX_txn* t) {
                std::pair<bool, Document> existing = find_compat(id, t);
                if (!existing.first) {
                    return;
                }
                const std::pair<bool, std::uint64_t> indexed_id =
                    m_source_ids.find_compat(existing.second.source_uri, t);
                if (!indexed_id.first || indexed_id.second != id) {
                    throw std::runtime_error("DocumentStore source URI index is inconsistent");
                }
                if (!m_source_ids.erase(existing.second.source_uri, t)) {
                    throw std::runtime_error("DocumentStore source URI index disappeared");
                }
                removed = m_records.erase(id, t);
            }, txn);
            return removed;
        }

        bool erase(std::uint64_t id, const Transaction& txn) {
            return erase(id, txn.handle());
        }

        void clear(MDBX_txn* txn = nullptr) {
            ensure_local_only();
            with_write_transaction([this](MDBX_txn* t) {
                m_ids.reset(t);
                m_records.clear(t);
                m_source_ids.clear(t);
            }, txn);
        }

        void clear(const Transaction& txn) {
            clear(txn.handle());
        }

        std::size_t count(MDBX_txn* txn = nullptr) const {
            std::size_t result = 0u;
            with_read_transaction([this, &result](MDBX_txn* t) {
                result = m_records.count(t);
            }, txn);
            return result;
        }

        std::size_t count(const Transaction& txn) const {
            return count(txn.handle());
        }

    private:
        static std::shared_ptr<Connection> require_connection(
            std::shared_ptr<Connection> connection) {
            if (!connection) {
                throw std::invalid_argument("DocumentStore connection cannot be null");
            }
            return connection;
        }

        static std::string make_name(const std::string& name, const char* suffix) {
            if (name.empty()) {
                throw std::invalid_argument("DocumentStore name cannot be empty");
            }
            return name + "_" + suffix;
        }

        static void validate_new_document(const Document& document) {
            if (document.id != 0u) {
                throw std::invalid_argument("DocumentStore::add requires id == 0");
            }
            if (document.source_uri.empty()) {
                throw std::invalid_argument("DocumentStore::add requires source_uri");
            }
        }

        void ensure_local_only() const {
#       if MDBXC_SYNC_ENABLED
            if (m_connection->sync_capture() != nullptr) {
                throw std::logic_error(
                    "DocumentStore mutations are unsupported while sync capture is attached");
            }
#       endif
        }

        template<class Fn>
        void with_write_transaction(Fn fn, MDBX_txn* txn) {
            detail::CompositeStoreTransaction::run(
                *m_connection, fn, TransactionMode::WRITABLE, txn);
        }

        template<class Fn>
        void with_read_transaction(Fn fn, MDBX_txn* txn) const {
            detail::CompositeStoreTransaction::run(
                *m_connection, fn, TransactionMode::READ_ONLY, txn);
        }

        std::shared_ptr<Connection> m_connection;
        IdAllocatorTable m_ids;
        KeyValueTable<std::uint64_t, std::vector<std::uint8_t> > m_records;
        KeyValueTable<std::string, std::uint64_t> m_source_ids;
    };
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_DOCUMENT_STORE_HPP_INCLUDED
