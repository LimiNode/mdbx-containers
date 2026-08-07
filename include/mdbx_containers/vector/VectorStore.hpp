#pragma once
#ifndef MDBX_CONTAINERS_HEADER_VECTOR_VECTOR_STORE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_VECTOR_VECTOR_STORE_HPP_INCLUDED

#include "FlatVectorIndex.hpp"
#include "VectorRecord.hpp"
#include "SearchResult.hpp"
#include <string>
#include <memory>
#include <mutex>

namespace mdbxc {

namespace sync {
    class VectorStoreLogicalAdapter;
}

    /// \brief Persistent local vector store with an exact in-memory search index.
    ///
    /// The store persists embeddings, text, and metadata in MDBX tables, then
    /// rebuilds a \ref FlatVectorIndex when opened.
    ///
    /// \warning Search is exact \c O(N*dim), all embeddings are loaded into RAM,
    /// and mutable index synchronization is caller-managed.
    /// \warning Lazy sync-apply refresh is protected by the connection
    /// apply/read barrier. One \c VectorStore instance serializes its public
    /// operations with an instance mutex. In C++17 builds, different
    /// cache-backed readers can still share the connection read side; C++11
    /// builds conservatively serialize the connection barrier.
    ///
    /// \note Non-empty collections have a single active dimension established by
    /// the first successfully added embedding.
    class VectorStore {
    public:
        /// \brief Opens a vector store using a new MDBX connection.
        /// \param config MDBX environment configuration.
        /// \param collection Logical collection name. Allowed characters are
        /// ASCII letters, digits, \c _ and \c -.
        /// \param metric Metric used by the in-memory index.
        /// \throws std::invalid_argument if \c collection is empty or contains
        /// unsupported characters.
        VectorStore(const Config& config,
                    std::string collection = "default",
                    VectorMetric metric = VectorMetric::COSINE);

        /// \brief Opens a vector store using an existing connection.
        /// \param connection Shared MDBX connection.
        /// \param collection Logical collection name. Allowed characters are
        /// ASCII letters, digits, \c _ and \c -.
        /// \param metric Metric used by the in-memory index.
        /// \throws std::invalid_argument if \c connection is null, or
        /// \c collection is empty or contains unsupported characters.
        VectorStore(std::shared_ptr<Connection> connection,
                    std::string collection = "default",
                    VectorMetric metric = VectorMetric::COSINE);

        VectorStore(const VectorStore&) = delete;
        VectorStore& operator=(const VectorStore&) = delete;
        VectorStore(VectorStore&&) = delete;
        VectorStore& operator=(VectorStore&&) = delete;

        /// \brief Adds a record and updates the RAM index after commit.
        /// \param embedding Dense embedding.
        /// \param text Text payload.
        /// \param metadata_json Metadata encoded as JSON text.
        /// \return Stable generated id.
        /// \throws std::invalid_argument if \c embedding is invalid.
        /// \throws MdbxException if a database error occurs.
        uint64_t add(const Embedding& embedding,
                     const std::string& text,
                     const std::string& metadata_json = "{}");

        /// \brief Searches the RAM index and loads payloads for matches.
        /// \param query Query embedding.
        /// \param top_k Maximum number of matches.
        /// \return Search results ordered by descending score.
        /// \throws std::invalid_argument if \c query is invalid.
        /// \throws std::runtime_error if persisted payload rows are missing.
        std::vector<SearchResult> search(const Embedding& query,
                                         std::size_t top_k) const;

        /// \brief Removes a record from persistent tables and the RAM index.
        /// \param id Record id.
        /// \return \c true if any persisted row existed.
        bool erase(uint64_t id);

        /// \brief Clears all persistent tables and the RAM index.
        void clear();

        /// \brief Rebuilds the RAM index from persisted embeddings.
        void rebuild_index();

        /// \brief Returns the number of persisted embeddings.
        std::size_t count() const;

        /// \brief Returns the validated collection name.
        const std::string& collection() const noexcept;

    private:
        friend class sync::VectorStoreLogicalAdapter;

        std::string m_collection;
        VectorMetric m_metric;
        std::shared_ptr<Connection> m_connection;
        SequenceTable<uint64_t> m_ids;
        mutable KeyValueTable<uint64_t, Embedding> m_embeddings;
        KeyValueTable<uint64_t, std::string> m_texts;
        KeyValueTable<uint64_t, std::string> m_metadata;
        mutable FlatVectorIndex m_index;
        mutable std::uint64_t m_sync_apply_generation_seen = 0;
        mutable std::mutex m_store_mutex;

        static std::shared_ptr<Connection> require_connection(std::shared_ptr<Connection> connection);
        static std::string validate_collection_name(const std::string& name);
        static std::string make_table_name(const std::string& collection, const std::string& suffix);
        void ensure_index_fresh() const;
        void ensure_index_fresh_locked() const;
        bool is_index_fresh() const;
        std::vector<SearchResult> search_locked(const Embedding& query,
                                                std::size_t top_k) const;
        void rebuild_index_impl_locked() const;
        std::uint64_t current_sync_apply_generation() const;
    };

} // namespace mdbxc

#include "VectorStore.ipp"

#endif // MDBX_CONTAINERS_HEADER_VECTOR_VECTOR_STORE_HPP_INCLUDED
