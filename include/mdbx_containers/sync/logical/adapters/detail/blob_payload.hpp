#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_DETAIL_BLOB_PAYLOAD_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_DETAIL_BLOB_PAYLOAD_HPP_INCLUDED

/// \file logical/adapters/detail/blob_payload.hpp
/// \brief Internal u32-length-prefixed logical payload primitives.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace mdbxc {
namespace sync {
namespace detail {

    /// \brief Reads one adapter-owned logical payload without copying its frame.
    class BlobPayloadCursor {
    public:
        explicit BlobPayloadCursor(const std::vector<std::uint8_t>& payload)
            : m_data(payload.empty() ? nullptr : &payload[0]),
              m_size(payload.size()),
              m_pos(0u) {}

        std::uint8_t read_u8(const char* label) {
            require(1u, label);
            return m_data[m_pos++];
        }

        std::uint32_t read_u32_le(const char* label) {
            require(4u, label);
            const std::uint32_t value = detail::read_u32_le(m_data + m_pos);
            m_pos += 4u;
            return value;
        }

        std::uint64_t read_u64_le(const char* label) {
            require(8u, label);
            const std::uint64_t value = detail::read_u64_le(m_data + m_pos);
            m_pos += 8u;
            return value;
        }

        std::vector<std::uint8_t> read_blob(const char* label) {
            const std::uint32_t size = read_u32_le(label);
            return read_bytes(size, label);
        }

        std::vector<std::uint8_t> read_bytes(
                std::size_t size,
                const char* label) {
            require(size, label);
            std::vector<std::uint8_t> out;
            if (size != 0u) {
                out.assign(m_data + m_pos, m_data + m_pos + size);
            }
            m_pos += size;
            return out;
        }

        void require(std::size_t size, const char* label) const {
            if (m_pos > m_size || size > m_size - m_pos) {
                throw std::runtime_error(
                    std::string(label) + " payload underrun");
            }
        }

        void ensure_end(const char* label) const {
            if (m_pos != m_size) {
                throw std::runtime_error(
                    std::string(label) + " payload has trailing bytes");
            }
        }

    private:
        const std::uint8_t* m_data;
        std::size_t m_size;
        std::size_t m_pos;
    };

    template<class BytesT>
    inline void append_blob_payload(
            std::vector<std::uint8_t>& out,
            const BytesT& bytes,
            const char* label) {
        if (bytes.size() > static_cast<std::size_t>(
                (std::numeric_limits<std::uint32_t>::max)())) {
            throw std::length_error(
                std::string(label) + " payload blob is too large");
        }
        detail::append_u32_le(out, static_cast<std::uint32_t>(bytes.size()));
        out.insert(out.end(), bytes.begin(), bytes.end());
    }

} // namespace detail
} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_DETAIL_BLOB_PAYLOAD_HPP_INCLUDED
