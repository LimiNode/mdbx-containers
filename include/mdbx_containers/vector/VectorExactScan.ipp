#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

namespace mdbxc {

    inline VectorExactScan::VectorExactScan(const VectorCollection& collection) {
        rebuild(collection);
    }

    inline void VectorExactScan::normalize(float* values, std::size_t dimension) {
        float squared_norm = 0.0f;
        for (std::size_t index = 0u; index < dimension; ++index) {
            squared_norm += values[index] * values[index];
        }
        const float norm = std::sqrt(squared_norm);
        if (norm == 0.0f) {
            std::fill(values, values + dimension, 0.0f);
            return;
        }
        for (std::size_t index = 0u; index < dimension; ++index) {
            values[index] /= norm;
        }
    }

    inline void VectorExactScan::rebuild(const VectorCollection& collection) {
        const VectorCollectionDescriptor& descriptor = collection.descriptor();
        std::vector<std::pair<std::string, Embedding> > records;
        collection.m_records->load(records);

        std::vector<std::string> record_ids;
        std::vector<float> values;
        if (records.size() >
                (std::numeric_limits<std::size_t>::max)() /
                    static_cast<std::size_t>(descriptor.dimension)) {
            throw std::length_error("Vector exact scan snapshot is too large");
        }
        record_ids.reserve(records.size());
        values.reserve(records.size() * static_cast<std::size_t>(descriptor.dimension));
        for (std::size_t record_index = 0u; record_index < records.size(); ++record_index) {
            const std::string& record_id = records[record_index].first;
            const Embedding& embedding = records[record_index].second;
            if (record_id.empty()) {
                throw std::invalid_argument("Vector exact scan record id must not be empty");
            }
            embedding.validate();
            if (embedding.dim != descriptor.dimension) {
                throw std::invalid_argument(
                    "Vector exact scan record dimension does not match collection descriptor");
            }
            record_ids.push_back(record_id);
            const std::size_t value_offset = values.size();
            values.insert(values.end(), embedding.values.begin(), embedding.values.end());
            if (descriptor.metric == VectorMetric::COSINE) {
                normalize(values.data() + value_offset, descriptor.dimension);
            }
        }

        m_metric = descriptor.metric;
        m_dimension = descriptor.dimension;
        m_record_ids.swap(record_ids);
        m_values.swap(values);
    }

    inline float VectorExactScan::compute_score(const float* query,
                                                 const float* candidate) const {
        if (m_metric == VectorMetric::COSINE || m_metric == VectorMetric::DOT) {
            float dot_product = 0.0f;
            for (std::size_t index = 0u; index < m_dimension; ++index) {
                dot_product += query[index] * candidate[index];
            }
            return dot_product;
        }

        float squared_distance = 0.0f;
        for (std::size_t index = 0u; index < m_dimension; ++index) {
            const float difference = query[index] - candidate[index];
            squared_distance += difference * difference;
        }
        return -squared_distance;
    }

    inline std::vector<VectorExactMatch> VectorExactScan::search(
            const Embedding& query,
            std::size_t top_k) const {
        query.validate();
        if (query.dim != m_dimension) {
            throw std::invalid_argument(
                "Vector exact scan query dimension does not match collection descriptor");
        }
        if (top_k == 0u || m_record_ids.empty()) {
            return std::vector<VectorExactMatch>();
        }

        std::vector<float> prepared_query(query.values);
        if (m_metric == VectorMetric::COSINE) {
            normalize(prepared_query.data(), prepared_query.size());
        }

        std::vector<VectorExactMatch> matches;
        matches.reserve(m_record_ids.size());
        for (std::size_t record_index = 0u;
             record_index < m_record_ids.size();
             ++record_index) {
            VectorExactMatch match;
            match.record_id = m_record_ids[record_index];
            match.score = compute_score(
                prepared_query.data(),
                m_values.data() + record_index * static_cast<std::size_t>(m_dimension));
            matches.push_back(std::move(match));
        }

        if (top_k > matches.size()) {
            top_k = matches.size();
        }
        std::partial_sort(
            matches.begin(), matches.begin() + top_k, matches.end(),
            [](const VectorExactMatch& left, const VectorExactMatch& right) {
                const bool left_is_nan = std::isnan(left.score);
                const bool right_is_nan = std::isnan(right.score);
                if (left_is_nan != right_is_nan) {
                    return !left_is_nan;
                }
                if (left_is_nan) {
                    return left.record_id < right.record_id;
                }
                if (left.score != right.score) {
                    return left.score > right.score;
                }
                return left.record_id < right.record_id;
            });
        matches.resize(top_k);
        return matches;
    }

    inline std::size_t VectorExactScan::size() const noexcept {
        return m_record_ids.size();
    }

    inline bool VectorExactScan::empty() const noexcept {
        return m_record_ids.empty();
    }

    inline std::uint32_t VectorExactScan::dimension() const noexcept {
        return m_dimension;
    }

    inline VectorMetric VectorExactScan::metric() const noexcept {
        return m_metric;
    }

} // namespace mdbxc
