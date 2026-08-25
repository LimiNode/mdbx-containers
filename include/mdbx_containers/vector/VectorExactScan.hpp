#pragma once
#ifndef MDBX_CONTAINERS_HEADER_VECTOR_VECTOR_EXACT_SCAN_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_VECTOR_VECTOR_EXACT_SCAN_HPP_INCLUDED

/// \file VectorExactScan.hpp
/// \brief Immutable scalar exact-search snapshot for \ref VectorCollection.

#include "VectorCollection.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace mdbxc {

    /// \brief Stable record id and scalar metric score from \ref VectorExactScan.
    struct VectorExactMatch {
        std::string record_id; ///< Caller-owned opaque record id.
        float score = 0.0f; ///< Metric score; larger values rank first.
    };

    /// \brief Immutable in-memory exact-search snapshot of a vector collection.
    /// \details Copies collection records into contiguous storage. Changes made
    /// to the collection after construction are not visible until \ref rebuild
    /// is called. Cosine snapshots normalize candidates internally; zero-norm
    /// vectors retain an all-zero representation.
    /// \thread_safety Not thread-safe. Synchronize rebuild and search externally.
    class VectorExactScan {
    public:
        /// \brief Materializes an exact-search snapshot from a collection.
        /// \param collection Persistent vector collection to read.
        /// \throws std::invalid_argument if a materialized record is incompatible.
        explicit VectorExactScan(const VectorCollection& collection);

        /// \brief Replaces the current snapshot with collection records.
        /// \param collection Persistent vector collection to read.
        /// \throws std::invalid_argument if a materialized record is incompatible.
        void rebuild(const VectorCollection& collection);

        /// \brief Searches the immutable snapshot with a scalar exact scan.
        /// \param query Query embedding whose dimension matches the collection.
        /// \param top_k Maximum number of matches to return.
        /// \return Matches ordered by descending score, then ascending binary id.
        /// NaN scores are ordered after all numeric scores and then by binary id.
        /// \throws std::invalid_argument if \p query is invalid or has a mismatched dimension.
        /// \complexity O(N*dimension + N*log(top_k)).
        std::vector<VectorExactMatch> search(const Embedding& query,
                                              std::size_t top_k) const;

        /// \brief Returns the number of records captured in the snapshot.
        /// \return Number of snapshot records.
        std::size_t size() const noexcept;

        /// \brief Returns whether the snapshot has no captured records.
        /// \return \c true when no records are captured.
        bool empty() const noexcept;

        /// \brief Returns the fixed vector dimension captured by the snapshot.
        /// \return Descriptor dimension used for search validation.
        std::uint32_t dimension() const noexcept;

        /// \brief Returns the scoring metric captured by the snapshot.
        /// \return Descriptor metric used by \ref search.
        VectorMetric metric() const noexcept;

    private:
        VectorMetric m_metric = VectorMetric::COSINE;
        std::uint32_t m_dimension = 0u;
        std::vector<std::string> m_record_ids;
        std::vector<float> m_values;

        float compute_score(const float* query, const float* candidate) const;
    };

} // namespace mdbxc

#include "VectorExactScan.ipp"

#endif // MDBX_CONTAINERS_HEADER_VECTOR_VECTOR_EXACT_SCAN_HPP_INCLUDED
