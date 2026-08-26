#pragma once
#ifndef MDBX_CONTAINERS_HEADER_VECTOR_FLAT_VECTOR_INDEX_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_VECTOR_FLAT_VECTOR_INDEX_HPP_INCLUDED

#include "Embedding.hpp"
#include "VectorMetric.hpp"
#include <vector>
#include <cstdint>
#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace mdbxc {

    /// \brief Lightweight id/score pair returned by \ref FlatVectorIndex.
    struct VectorMatch {
        uint64_t id = 0; ///< Matched vector id.
        float score = 0.0f; ///< Metric score; larger values rank first.
    };

    /// \brief In-memory exact vector index.
    ///
    /// \warning Search is exact \c O(N*dim). All vectors are held in RAM.
    /// The class does not synchronize concurrent mutation and search.
    class FlatVectorIndex {
    public:
        /// \brief Creates an empty index for the given metric.
        /// \param metric Scoring metric used for all vectors.
        explicit FlatVectorIndex(VectorMetric metric = VectorMetric::COSINE);

        /// \brief Removes all vectors and resets the index dimension.
        void clear();

        /// \brief Adds a vector id to the index.
        /// \param id Caller-owned stable id.
        /// \param embedding Dense embedding to index.
        /// \throws std::invalid_argument if the embedding is invalid or has a mismatched dimension.
        void add(uint64_t id, const Embedding& embedding);

        /// \brief Removes a vector id from the index.
        /// \param id Id to remove.
        /// \return \c true if the id was present.
        bool erase(uint64_t id);

        /// \brief Searches the index and returns the best matches.
        /// \param query Query embedding.
        /// \param top_k Maximum number of matches to return.
        /// \return Matches ordered by descending score.
        /// \throws std::invalid_argument if the query is invalid or has a mismatched dimension.
        std::vector<VectorMatch> search(const Embedding& query, std::size_t top_k) const;

        /// \brief Returns the number of indexed vectors.
        std::size_t size() const noexcept;

        /// \brief Returns the active index dimension, or zero when empty.
        uint32_t dim() const noexcept;

    private:
        VectorMetric m_metric;
        uint32_t m_dim = 0;
        std::vector<uint64_t> m_ids;
        std::vector<float> m_vectors;

        void check_dim(const Embedding& embedding);
        void reserve_for_add();
        static std::size_t geometric_capacity(std::size_t current,
                                              std::size_t required,
                                              std::size_t maximum);
        float compute_score(const float* query_vec, const float* candidate_vec) const;
    };

} // namespace mdbxc

#include "FlatVectorIndex.ipp"

#endif // MDBX_CONTAINERS_HEADER_VECTOR_FLAT_VECTOR_INDEX_HPP_INCLUDED
