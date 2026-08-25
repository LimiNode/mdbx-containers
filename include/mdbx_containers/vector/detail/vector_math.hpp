#pragma once
#ifndef MDBX_CONTAINERS_HEADER_VECTOR_DETAIL_VECTOR_MATH_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_VECTOR_DETAIL_VECTOR_MATH_HPP_INCLUDED

/// \file vector_math.hpp
/// \brief Shared scalar vector operations for in-memory search implementations.

#include <algorithm>
#include <cmath>
#include <cstddef>

namespace mdbxc {
namespace detail {

    /// \brief Normalizes a dense float vector using double norm accumulation.
    inline void normalize_vector_for_cosine(float* values,
                                            std::size_t dimension) {
        if (dimension == 0u) return;
        double squared_norm = 0.0;
        for (std::size_t index = 0u; index < dimension; ++index) {
            const double component = static_cast<double>(values[index]);
            squared_norm += component * component;
        }
        const double norm = std::sqrt(squared_norm);
        if (norm == 0.0) {
            std::fill(values, values + dimension, 0.0f);
            return;
        }
        for (std::size_t index = 0u; index < dimension; ++index) {
            values[index] = static_cast<float>(
                static_cast<double>(values[index]) / norm);
        }
    }

} // namespace detail
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_VECTOR_DETAIL_VECTOR_MATH_HPP_INCLUDED
