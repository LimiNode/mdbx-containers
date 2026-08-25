#include <cstring>
#include <cmath>
#include <algorithm>
#include <stdexcept>

#include "detail/vector_math.hpp"

namespace mdbxc {

    inline FlatVectorIndex::FlatVectorIndex(VectorMetric metric)
        : m_metric(metric) {}

    inline void FlatVectorIndex::clear() {
        m_ids.clear();
        m_vectors.clear();
        m_dim = 0;
    }

    inline void FlatVectorIndex::check_dim(const Embedding& embedding) {
        embedding.validate();
        if (m_dim == 0) {
            m_dim = embedding.dim;
        } else if (embedding.dim != m_dim) {
            throw std::invalid_argument("Embedding dimension does not match index dimension");
        }
    }

    inline std::size_t FlatVectorIndex::geometric_capacity(
            std::size_t current,
            std::size_t required,
            std::size_t maximum) {
        if (required > maximum) {
            throw std::length_error("FlatVectorIndex capacity exceeded");
        }
        std::size_t capacity = current == 0u ? 1u : current;
        while (capacity < required) {
            if (capacity > maximum - capacity) {
                capacity = maximum;
                break;
            }
            capacity += capacity;
        }
        return capacity;
    }

    inline void FlatVectorIndex::reserve_for_add() {
        const std::size_t dimension = static_cast<std::size_t>(m_dim);
        const std::size_t max_records = (std::min)(
            m_ids.max_size(), m_vectors.max_size() / dimension);
        if (m_ids.size() == max_records) {
            throw std::length_error("FlatVectorIndex capacity exceeded");
        }
        const std::size_t required_records = m_ids.size() + 1u;
        const std::size_t current_records = (std::min)(
            m_ids.capacity(), m_vectors.capacity() / dimension);
        const std::size_t target_records = geometric_capacity(
            current_records, required_records, max_records);

        if (m_ids.capacity() < target_records) {
            m_ids.reserve(target_records);
        }
        const std::size_t target_values = target_records * dimension;
        if (m_vectors.capacity() < target_values) {
            m_vectors.reserve(target_values);
        }
    }

    inline void FlatVectorIndex::add(uint64_t id, const Embedding& embedding) {
        check_dim(embedding);
        std::vector<float> stored(embedding.values);
        if (m_metric == VectorMetric::COSINE) {
            detail::normalize_vector_for_cosine(stored.data(), stored.size());
        }

        // Reserve both arrays before mutating either one. Once capacity is
        // available, uint64_t/float insertion cannot allocate or throw.
        reserve_for_add();
        m_ids.push_back(id);
        m_vectors.insert(m_vectors.end(), stored.begin(), stored.end());
    }

    inline bool FlatVectorIndex::erase(uint64_t id) {
        for (std::size_t i = 0; i < m_ids.size(); ++i) {
            if (m_ids[i] == id) {
                std::size_t last = m_ids.size() - 1;
                if (i != last) {
                    m_ids[i] = m_ids[last];
                    std::memcpy(&m_vectors[i * m_dim], &m_vectors[last * m_dim], m_dim * sizeof(float));
                }
                m_ids.pop_back();
                m_vectors.resize(m_vectors.size() - m_dim);
                if (m_ids.empty()) {
                    m_dim = 0;
                }
                return true;
            }
        }
        return false;
    }

    inline float FlatVectorIndex::compute_score(const float* query_vec,
                                                  const float* candidate_vec) const {
        if (m_metric == VectorMetric::COSINE || m_metric == VectorMetric::DOT) {
            float dot = 0.0f;
            for (std::size_t i = 0; i < m_dim; ++i) {
                dot += query_vec[i] * candidate_vec[i];
            }
            return dot;
        }
        // L2: score = -squared_distance (higher is better)
        float sq_dist = 0.0f;
        for (std::size_t i = 0; i < m_dim; ++i) {
            float diff = query_vec[i] - candidate_vec[i];
            sq_dist += diff * diff;
        }
        return -sq_dist;
    }

    inline std::vector<VectorMatch> FlatVectorIndex::search(const Embedding& query,
                                                              std::size_t top_k) const {
        query.validate();
        if (top_k == 0) {
            return std::vector<VectorMatch>();
        }
        if (m_dim == 0 || m_ids.empty()) {
            return std::vector<VectorMatch>();
        }
        if (query.dim != m_dim) {
            throw std::invalid_argument("Query dimension does not match index dimension");
        }

        // Prepare query vector (normalize for COSINE)
        std::vector<float> query_vec(query.values);
        if (m_metric == VectorMetric::COSINE) {
            detail::normalize_vector_for_cosine(
                query_vec.data(), query_vec.size());
        }

        std::size_t n = m_ids.size();
        std::vector<VectorMatch> matches;
        matches.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            VectorMatch m;
            m.id = m_ids[i];
            m.score = compute_score(query_vec.data(), &m_vectors[i * m_dim]);
            matches.push_back(m);
        }

        if (top_k > n) {
            top_k = n;
        }
        std::partial_sort(matches.begin(), matches.begin() + top_k, matches.end(),
            [](const VectorMatch& a, const VectorMatch& b) {
                return a.score > b.score;
            });
        matches.resize(top_k);
        return matches;
    }

    inline std::size_t FlatVectorIndex::size() const noexcept {
        return m_ids.size();
    }

    inline uint32_t FlatVectorIndex::dim() const noexcept {
        return m_dim;
    }

} // namespace mdbxc
