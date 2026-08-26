#pragma once
#ifndef MDBX_CONTAINERS_HEADER_CHUNK_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_CHUNK_STORE_HPP_INCLUDED

/// \file ChunkStore.hpp
/// \brief Source chunks with a document-to-chunk secondary index.

#include "common.hpp"
#include "IdAllocatorTable.hpp"
#include "KeyMultiValueTable.hpp"
#include "KeyValueTable.hpp"
#include "rag/Records.hpp"

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace mdbxc {

    /// \brief Persistent RAG chunks indexed by owning document ID.
    /// \details Owns three DBIs: records, ID allocator, and document index.
    class ChunkStore {
    public:
        explicit ChunkStore(std::shared_ptr<Connection> connection,
                            std::string name = "chunks")
            : m_connection(require_connection(std::move(connection)))
            , m_ids(m_connection, make_name(name, "ids"))
            , m_records(m_connection, make_name(name, "records"))
            , m_document_chunks(m_connection, make_name(name, "document_ids")) {}

        explicit ChunkStore(const Config& config, std::string name = "chunks")
            : ChunkStore(Connection::create(config), std::move(name)) {}

        /// \brief Adds a chunk and returns its generated positive ID.
        std::uint64_t add(Chunk chunk, MDBX_txn* txn = nullptr) {
            validate_new_chunk(chunk);
            std::uint64_t result = 0u;
            with_write_transaction([this, &chunk, &result](MDBX_txn* t) {
                ensure_chunk_index_available(chunk.document_id, chunk.chunk_index, t);
                result = m_ids.next(t);
                chunk.id = result;
                m_records.insert_or_assign(result, detail::encode_chunk(chunk), t);
                m_document_chunks.insert(chunk.document_id, result, t);
            }, txn);
            return result;
        }

        std::uint64_t add(Chunk chunk, const Transaction& txn) {
            return add(std::move(chunk), txn.handle());
        }

        Chunk get(std::uint64_t id, MDBX_txn* txn = nullptr) const {
            std::pair<bool, Chunk> found = find_compat(id, txn);
            if (!found.first) {
                throw std::out_of_range("ChunkStore: chunk not found");
            }
            return std::move(found.second);
        }

        Chunk get(std::uint64_t id, const Transaction& txn) const {
            return get(id, txn.handle());
        }

        std::pair<bool, Chunk> find_compat(std::uint64_t id,
                                           MDBX_txn* txn = nullptr) const {
            std::pair<bool, Chunk> result(false, Chunk());
            with_read_transaction([this, id, &result](MDBX_txn* t) {
                std::pair<bool, std::vector<std::uint8_t> > stored =
                    m_records.find_compat(id, t);
                if (stored.first) {
                    result.first = true;
                    result.second = detail::decode_chunk(stored.second);
                    if (result.second.id != id) {
                        throw std::runtime_error("ChunkStore record ID mismatch");
                    }
                }
            }, txn);
            return result;
        }

        std::pair<bool, Chunk> find_compat(std::uint64_t id,
                                           const Transaction& txn) const {
            return find_compat(id, txn.handle());
        }

        /// \brief Returns one document's chunks ordered by chunk_index then ID.
        std::vector<Chunk> by_document(std::uint64_t document_id,
                                       MDBX_txn* txn = nullptr) const {
            std::vector<Chunk> chunks;
            with_read_transaction([this, document_id, &chunks](MDBX_txn* t) {
                const std::vector<std::uint64_t> ids =
                    m_document_chunks.find(document_id, t);
                chunks.reserve(ids.size());
                for (std::size_t i = 0u; i < ids.size(); ++i) {
                    std::pair<bool, Chunk> found = find_compat(ids[i], t);
                    if (!found.first || found.second.document_id != document_id) {
                        throw std::runtime_error("ChunkStore document index is stale");
                    }
                    chunks.push_back(std::move(found.second));
                }
                std::sort(chunks.begin(), chunks.end(),
                          [](const Chunk& left, const Chunk& right) {
                              if (left.chunk_index != right.chunk_index) {
                                  return left.chunk_index < right.chunk_index;
                              }
                              return left.id < right.id;
                          });
            }, txn);
            return chunks;
        }

        std::vector<Chunk> by_document(std::uint64_t document_id,
                                       const Transaction& txn) const {
            return by_document(document_id, txn.handle());
        }

        bool erase(std::uint64_t id, MDBX_txn* txn = nullptr) {
            bool removed = false;
            with_write_transaction([this, id, &removed](MDBX_txn* t) {
                std::pair<bool, Chunk> existing = find_compat(id, t);
                if (!existing.first) {
                    return;
                }
                const std::size_t index_removed = m_document_chunks.erase(
                    existing.second.document_id, id, t);
                if (index_removed != 1u) {
                    throw std::runtime_error("ChunkStore document index is inconsistent");
                }
                removed = m_records.erase(id, t);
            }, txn);
            return removed;
        }

        bool erase(std::uint64_t id, const Transaction& txn) {
            return erase(id, txn.handle());
        }

        /// \brief Erases every chunk belonging to a document.
        /// \return Number of chunk records removed.
        std::size_t erase_document(std::uint64_t document_id,
                                   MDBX_txn* txn = nullptr) {
            std::size_t removed = 0u;
            with_write_transaction([this, document_id, &removed](MDBX_txn* t) {
                const std::vector<std::uint64_t> ids =
                    m_document_chunks.find(document_id, t);
                for (std::size_t i = 0u; i < ids.size(); ++i) {
                    if (!m_records.erase(ids[i], t)) {
                        throw std::runtime_error("ChunkStore document index is stale");
                    }
                    ++removed;
                }
                if (!ids.empty() && !m_document_chunks.erase(document_id, t)) {
                    throw std::runtime_error("ChunkStore failed to erase document index");
                }
            }, txn);
            return removed;
        }

        std::size_t erase_document(std::uint64_t document_id,
                                   const Transaction& txn) {
            return erase_document(document_id, txn.handle());
        }

        void clear(MDBX_txn* txn = nullptr) {
            with_write_transaction([this](MDBX_txn* t) {
                m_ids.reset(t);
                m_records.clear(t);
                m_document_chunks.clear(t);
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
                throw std::invalid_argument("ChunkStore connection cannot be null");
            }
            return connection;
        }

        static std::string make_name(const std::string& name, const char* suffix) {
            if (name.empty()) {
                throw std::invalid_argument("ChunkStore name cannot be empty");
            }
            return name + "_" + suffix;
        }

        static void validate_new_chunk(const Chunk& chunk) {
            if (chunk.id != 0u || chunk.document_id == 0u) {
                throw std::invalid_argument("ChunkStore::add requires id == 0 and document_id");
            }
            if (chunk.char_start > chunk.char_end) {
                throw std::invalid_argument("ChunkStore::add has invalid character range");
            }
        }

        void ensure_chunk_index_available(std::uint64_t document_id,
                                          std::uint32_t chunk_index,
                                          MDBX_txn* txn) const {
            const std::vector<std::uint64_t> ids = m_document_chunks.find(document_id, txn);
            for (std::size_t i = 0u; i < ids.size(); ++i) {
                std::pair<bool, Chunk> existing = find_compat(ids[i], txn);
                if (!existing.first || existing.second.document_id != document_id) {
                    throw std::runtime_error("ChunkStore document index is stale");
                }
                if (existing.second.chunk_index == chunk_index) {
                    throw std::invalid_argument("ChunkStore chunk_index already exists for document");
                }
            }
        }

        template<class Fn>
        void with_write_transaction(Fn fn, MDBX_txn* txn) {
            if (txn != nullptr) {
                fn(txn);
                return;
            }
            Transaction managed = m_connection->transaction(TransactionMode::WRITABLE);
            try {
                fn(managed.handle());
                managed.commit();
            } catch (...) {
                try { managed.rollback(); } catch (...) {}
                throw;
            }
        }

        template<class Fn>
        void with_read_transaction(Fn fn, MDBX_txn* txn) const {
            if (txn != nullptr) {
                fn(txn);
                return;
            }
            Transaction managed = m_connection->transaction(TransactionMode::READ_ONLY);
            try {
                fn(managed.handle());
                managed.commit();
            } catch (...) {
                try { managed.rollback(); } catch (...) {}
                throw;
            }
        }

        std::shared_ptr<Connection> m_connection;
        IdAllocatorTable m_ids;
        KeyValueTable<std::uint64_t, std::vector<std::uint8_t> > m_records;
        KeyMultiValueTable<std::uint64_t, std::uint64_t> m_document_chunks;
    };
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_CHUNK_STORE_HPP_INCLUDED
