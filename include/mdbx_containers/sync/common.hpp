#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_COMMON_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_COMMON_HPP_INCLUDED

/// \file common.hpp
/// \brief Shared sync types: node identifiers and byte-order helpers.

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>
#include <array>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <memory>

#include <mdbx.h>

namespace mdbxc {
namespace sync {

    /// \brief 16-byte stable identifier (UUID-like).
    using NodeId = std::array<std::uint8_t, 16>;

    /// \brief 16-byte database identifier.
    using DbId = std::array<std::uint8_t, 16>;

    /// \brief Creates a \c NodeId filled with zeros.
    inline NodeId make_zero_node() {
        NodeId out{};
        return out;
    }

    /// \brief Creates a \c NodeId from raw bytes.
    /// \param bytes Source buffer with at least 16 bytes.
    inline NodeId make_node_id(const std::uint8_t* bytes) {
        NodeId out{};
        std::memcpy(out.data(), bytes, 16);
        return out;
    }

    /// \brief Lexicographic compare of two \c NodeId values.
    /// \return Negative if \p a < \p b, zero if equal, positive otherwise.
    inline int compare_node_id(const NodeId& a, const NodeId& b) {
        for (std::size_t i = 0; i < a.size(); ++i) {
            if (a[i] != b[i]) {
                return a[i] < b[i] ? -1 : 1;
            }
        }
        return 0;
    }

    /// \brief Stable lexical hash of a string (FNV-1a 64).
    /// \param s Input string.
    /// \return 64-bit hash.
    inline std::uint64_t fnv1a_64(const std::string& s) {
        std::uint64_t h = 0xcbf29ce484222325ULL;
        for (std::size_t i = 0; i < s.size(); ++i) {
            h ^= static_cast<std::uint64_t>(static_cast<std::uint8_t>(s[i]));
            h *= 0x100000001b3ULL;
        }
        return h;
    }

    namespace detail {

        inline void write_u16_le(std::uint16_t value, std::uint8_t* out) {
            out[0] = static_cast<std::uint8_t>(value & 0xff);
            out[1] = static_cast<std::uint8_t>((value >> 8) & 0xff);
        }

        inline void write_u32_le(std::uint32_t value, std::uint8_t* out) {
            out[0] = static_cast<std::uint8_t>(value & 0xff);
            out[1] = static_cast<std::uint8_t>((value >> 8) & 0xff);
            out[2] = static_cast<std::uint8_t>((value >> 16) & 0xff);
            out[3] = static_cast<std::uint8_t>((value >> 24) & 0xff);
        }

        inline void write_u64_le(std::uint64_t value, std::uint8_t* out) {
            for (int i = 0; i < 8; ++i) {
                out[i] = static_cast<std::uint8_t>((value >> (i * 8)) & 0xff);
            }
        }

        inline void write_u64_be(std::uint64_t value, std::uint8_t* out) {
            for (int i = 0; i < 8; ++i) {
                out[i] =
                    static_cast<std::uint8_t>((value >> ((7 - i) * 8)) & 0xff);
            }
        }

        inline std::uint16_t read_u16_le(const std::uint8_t* bytes) {
            return static_cast<std::uint16_t>(
                static_cast<std::uint16_t>(bytes[0]) |
                (static_cast<std::uint16_t>(bytes[1]) << 8));
        }

        inline std::uint32_t read_u32_le(const std::uint8_t* bytes) {
            return static_cast<std::uint32_t>(bytes[0]) |
                   (static_cast<std::uint32_t>(bytes[1]) << 8) |
                   (static_cast<std::uint32_t>(bytes[2]) << 16) |
                   (static_cast<std::uint32_t>(bytes[3]) << 24);
        }

        inline std::uint64_t read_u64_le(const std::uint8_t* bytes) {
            std::uint64_t out = 0;
            for (int i = 0; i < 8; ++i) {
                out |= static_cast<std::uint64_t>(bytes[i]) << (i * 8);
            }
            return out;
        }

        inline std::uint64_t read_u64_be(const std::uint8_t* bytes) {
            std::uint64_t out = 0;
            for (int i = 0; i < 8; ++i) {
                out = (out << 8) | static_cast<std::uint64_t>(bytes[i]);
            }
            return out;
        }

        inline void append_u16_le(std::vector<std::uint8_t>& out,
                                  std::uint16_t value) {
            std::uint8_t bytes[2];
            write_u16_le(value, bytes);
            out.insert(out.end(), bytes, bytes + sizeof(bytes));
        }

        inline void append_u32_le(std::vector<std::uint8_t>& out,
                                  std::uint32_t value) {
            std::uint8_t bytes[4];
            write_u32_le(value, bytes);
            out.insert(out.end(), bytes, bytes + sizeof(bytes));
        }

        inline void append_u64_le(std::vector<std::uint8_t>& out,
                                  std::uint64_t value) {
            std::uint8_t bytes[8];
            write_u64_le(value, bytes);
            out.insert(out.end(), bytes, bytes + sizeof(bytes));
        }

        /// \brief Appends an unsigned integer in big-endian byte order.
        /// \details This is for bytewise MDBX keys whose lexical order must
        /// match their numeric order. Protocol payloads remain little-endian.
        inline void append_u64_be(std::vector<std::uint8_t>& out,
                                  std::uint64_t value) {
            std::uint8_t bytes[8];
            write_u64_be(value, bytes);
            out.insert(out.end(), bytes, bytes + sizeof(bytes));
        }

    } // namespace detail

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_COMMON_HPP_INCLUDED
