#pragma once
#ifndef MDBX_CONTAINERS_HEADER_VECTOR_EMBEDDING_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_VECTOR_EMBEDDING_HPP_INCLUDED

#include <vector>
#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace mdbxc {

    /// \brief Dense float vector persisted by the vector store.
    ///
    /// Serialized format is little-endian \c uint32_t dimension, little-endian
    /// reserved \c uint32_t set to zero, followed by \c dim raw \c float values.
    struct Embedding {
        uint32_t dim = 0; ///< Number of float components.
        std::vector<float> values; ///< Dense vector values.

        /// \brief Returns whether the value array is empty.
        bool empty() const noexcept {
            return values.empty();
        }

        /// \brief Validates basic embedding invariants.
        /// \throws std::invalid_argument if \c dim is zero or does not match \c values.size().
        void validate() const {
            if (dim == 0) {
                throw std::invalid_argument("Embedding dimension is zero");
            }
            if (values.size() != static_cast<std::size_t>(dim)) {
                throw std::invalid_argument("Embedding dimension does not match values size");
            }
        }

        /// \brief Serializes the embedding for MDBX storage.
        /// \return Binary representation suitable for table values.
        /// \throws std::invalid_argument if the embedding invariants are invalid.
        std::vector<uint8_t> to_bytes() const {
            validate();
            const std::size_t value_bytes = values.size() * sizeof(float);
            std::vector<uint8_t> result(8u + value_bytes);
            result[0] = static_cast<uint8_t>(dim & 0xFF);
            result[1] = static_cast<uint8_t>((dim >> 8) & 0xFF);
            result[2] = static_cast<uint8_t>((dim >> 16) & 0xFF);
            result[3] = static_cast<uint8_t>((dim >> 24) & 0xFF);
            result[4] = result[5] = result[6] = result[7] = 0;
            std::memcpy(result.data() + 8, values.data(), value_bytes);
            return result;
        }

        /// \brief Deserializes an embedding from MDBX storage bytes.
        /// \param data Pointer to serialized bytes.
        /// \param len Serialized byte length.
        /// \return Restored embedding.
        /// \throws std::runtime_error if the binary format is invalid.
        static Embedding from_bytes(const void* data, std::size_t len) {
            if (data == nullptr) {
                throw std::runtime_error("Embedding binary format data is null");
            }
            if (len < 8) {
                throw std::runtime_error("Embedding binary format too short");
            }
            const uint8_t* p = static_cast<const uint8_t*>(data);
            uint32_t dim = static_cast<uint32_t>(p[0])
                         | (static_cast<uint32_t>(p[1]) << 8)
                         | (static_cast<uint32_t>(p[2]) << 16)
                         | (static_cast<uint32_t>(p[3]) << 24);
            uint32_t reserved = static_cast<uint32_t>(p[4])
                              | (static_cast<uint32_t>(p[5]) << 8)
                              | (static_cast<uint32_t>(p[6]) << 16)
                              | (static_cast<uint32_t>(p[7]) << 24);
            if (reserved != 0) {
                throw std::runtime_error("Embedding binary format reserved field is non-zero");
            }
            std::size_t expected_payload = static_cast<std::size_t>(dim) * sizeof(float);
            if (len != 8 + expected_payload) {
                throw std::runtime_error("Embedding binary format size mismatch");
            }
            if (dim == 0) {
                throw std::runtime_error("Embedding dimension is zero in binary format");
            }
            Embedding e;
            e.dim = dim;
            e.values.resize(dim);
            std::memcpy(e.values.data(), p + 8, expected_payload);
            return e;
        }
    };

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_VECTOR_EMBEDDING_HPP_INCLUDED
