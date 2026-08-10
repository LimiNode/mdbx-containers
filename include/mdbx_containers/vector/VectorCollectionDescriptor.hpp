#pragma once
#ifndef MDBX_CONTAINERS_HEADER_VECTOR_VECTOR_COLLECTION_DESCRIPTOR_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_VECTOR_VECTOR_COLLECTION_DESCRIPTOR_HPP_INCLUDED

/// \file VectorCollectionDescriptor.hpp
/// \brief Persistent compatibility descriptor for \ref VectorCollection.

#include "VectorMetric.hpp"

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace mdbxc {

    /// \brief Normalization provenance persisted with a vector collection.
    /// \details Describes the pipeline that produced stored vectors. It does
    /// not cause \ref VectorCollection to transform or normalize values.
    enum class VectorNormalization : std::uint32_t {
        None = 0,
        UnitL2 = 1
    };

    /// \brief Versioned persistent compatibility contract for one vector collection.
    /// \details All fields are immutable after the collection is created. Opening
    /// the same collection with a different descriptor fails before the records
    /// table is opened or mutated.
    struct VectorCollectionDescriptor {
        std::string collection_id;
        std::uint32_t descriptor_version = 1u;
        std::uint32_t dimension = 0u;
        VectorMetric metric = VectorMetric::COSINE;
        VectorNormalization normalization = VectorNormalization::None;
        std::string vector_codec_id;
        std::uint32_t vector_codec_version = 0u;
        std::string signature_encoder_id;
        std::uint32_t signature_encoder_version = 0u;
        std::uint32_t block_layout_version = 0u;

        /// \brief Validates descriptor fields before persistence or use.
        /// \throws std::invalid_argument if a field is unsupported or incomplete.
        void validate() const {
            validate_collection_id(collection_id);
            if (descriptor_version != 1u) {
                throw std::invalid_argument("Unsupported vector collection descriptor version");
            }
            if (dimension == 0u) {
                throw std::invalid_argument("Vector collection dimension must be non-zero");
            }
            if (!is_known_metric(metric)) {
                throw std::invalid_argument("Vector collection metric is invalid");
            }
            if (!is_known_normalization(normalization)) {
                throw std::invalid_argument("Vector collection normalization is invalid");
            }
            if (vector_codec_id != "raw-f32" || vector_codec_version != 1u) {
                throw std::invalid_argument(
                    "Vector collection requires the raw-f32 vector codec version 1");
            }
            if (signature_encoder_id != "none" ||
                signature_encoder_version != 1u) {
                throw std::invalid_argument(
                    "Vector collection requires the none signature encoder version 1");
            }
            if (block_layout_version != 1u) {
                throw std::invalid_argument(
                    "Vector collection requires block layout version 1");
            }
        }

        /// \brief Serializes the descriptor as a versioned little-endian record.
        /// \return Binary representation suitable for persistent storage.
        /// \throws std::invalid_argument if the descriptor is invalid.
        std::vector<std::uint8_t> to_bytes() const {
            validate();
            std::vector<std::uint8_t> out;
            append_u32(out, descriptor_magic());
            append_u32(out, descriptor_version);
            append_u32(out, dimension);
            append_u32(out, static_cast<std::uint32_t>(metric));
            append_u32(out, static_cast<std::uint32_t>(normalization));
            append_string(out, collection_id);
            append_string(out, vector_codec_id);
            append_u32(out, vector_codec_version);
            append_string(out, signature_encoder_id);
            append_u32(out, signature_encoder_version);
            append_u32(out, block_layout_version);
            return out;
        }

        /// \brief Restores a descriptor from \ref to_bytes output.
        /// \param data Serialized bytes.
        /// \param len Number of serialized bytes.
        /// \return Decoded descriptor.
        /// \throws std::runtime_error if the record is malformed or unsupported.
        static VectorCollectionDescriptor from_bytes(const void* data,
                                                     std::size_t len) {
            if (data == nullptr) {
                throw std::runtime_error("Vector collection descriptor data is null");
            }
            const std::uint8_t* bytes = static_cast<const std::uint8_t*>(data);
            std::size_t pos = 0u;
            if (read_u32(bytes, len, pos) != descriptor_magic()) {
                throw std::runtime_error("Vector collection descriptor magic mismatch");
            }

            VectorCollectionDescriptor descriptor;
            descriptor.descriptor_version = read_u32(bytes, len, pos);
            descriptor.dimension = read_u32(bytes, len, pos);
            descriptor.metric = static_cast<VectorMetric>(read_u32(bytes, len, pos));
            descriptor.normalization =
                static_cast<VectorNormalization>(read_u32(bytes, len, pos));
            descriptor.collection_id = read_string(bytes, len, pos);
            descriptor.vector_codec_id = read_string(bytes, len, pos);
            descriptor.vector_codec_version = read_u32(bytes, len, pos);
            descriptor.signature_encoder_id = read_string(bytes, len, pos);
            descriptor.signature_encoder_version = read_u32(bytes, len, pos);
            descriptor.block_layout_version = read_u32(bytes, len, pos);
            if (pos != len) {
                throw std::runtime_error("Vector collection descriptor trailing bytes");
            }
            try {
                descriptor.validate();
            } catch (const std::invalid_argument&) {
                throw std::runtime_error("Vector collection descriptor is invalid");
            }
            return descriptor;
        }

        /// \brief Compares every persistent compatibility field.
        /// \param other Descriptor to compare.
        /// \return \c true when both descriptors are identical.
        bool operator==(const VectorCollectionDescriptor& other) const {
            return collection_id == other.collection_id &&
                   descriptor_version == other.descriptor_version &&
                   dimension == other.dimension &&
                   metric == other.metric &&
                   normalization == other.normalization &&
                   vector_codec_id == other.vector_codec_id &&
                   vector_codec_version == other.vector_codec_version &&
                   signature_encoder_id == other.signature_encoder_id &&
                   signature_encoder_version == other.signature_encoder_version &&
                   block_layout_version == other.block_layout_version;
        }

        /// \brief Compares every persistent compatibility field.
        /// \param other Descriptor to compare.
        /// \return \c true when at least one field differs.
        bool operator!=(const VectorCollectionDescriptor& other) const {
            return !(*this == other);
        }

    private:
        static std::uint32_t descriptor_magic() {
            return 0x4C4F4356u;
        }

        static bool is_known_metric(VectorMetric value) {
            return value == VectorMetric::COSINE || value == VectorMetric::DOT ||
                   value == VectorMetric::L2;
        }

        static bool is_known_normalization(VectorNormalization value) {
            return value == VectorNormalization::None ||
                   value == VectorNormalization::UnitL2;
        }

        static void validate_collection_id(const std::string& value) {
            if (value.empty()) {
                throw std::invalid_argument("Vector collection id must not be empty");
            }
            for (std::size_t i = 0; i < value.size(); ++i) {
                const char c = value[i];
                if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                    (c >= '0' && c <= '9') || c == '_' || c == '-') {
                    continue;
                }
                throw std::invalid_argument(
                    "Vector collection id contains unsupported character");
            }
        }

        static void append_string(std::vector<std::uint8_t>& out,
                                  const std::string& value) {
            if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
                throw std::length_error("Vector collection descriptor string is too large");
            }
            append_u32(out, static_cast<std::uint32_t>(value.size()));
            out.insert(out.end(), value.begin(), value.end());
        }

        static void require(std::size_t total,
                            std::size_t pos,
                            std::size_t size) {
            if (pos > total || size > total - pos) {
                throw std::runtime_error("Vector collection descriptor decode underrun");
            }
        }

        static std::uint32_t read_u32(const std::uint8_t* data,
                                      std::size_t total,
                                      std::size_t& pos) {
            require(total, pos, 4u);
            const std::uint32_t value =
                static_cast<std::uint32_t>(data[pos]) |
                (static_cast<std::uint32_t>(data[pos + 1u]) << 8u) |
                (static_cast<std::uint32_t>(data[pos + 2u]) << 16u) |
                (static_cast<std::uint32_t>(data[pos + 3u]) << 24u);
            pos += 4u;
            return value;
        }

        static void append_u32(std::vector<std::uint8_t>& out,
                               std::uint32_t value) {
            out.push_back(static_cast<std::uint8_t>(value & 0xFFu));
            out.push_back(static_cast<std::uint8_t>((value >> 8u) & 0xFFu));
            out.push_back(static_cast<std::uint8_t>((value >> 16u) & 0xFFu));
            out.push_back(static_cast<std::uint8_t>((value >> 24u) & 0xFFu));
        }

        static std::string read_string(const std::uint8_t* data,
                                       std::size_t total,
                                       std::size_t& pos) {
            const std::uint32_t size = read_u32(data, total, pos);
            require(total, pos, size);
            const char* begin = reinterpret_cast<const char*>(data + pos);
            pos += size;
            return std::string(begin, begin + size);
        }
    };

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_VECTOR_VECTOR_COLLECTION_DESCRIPTOR_HPP_INCLUDED
