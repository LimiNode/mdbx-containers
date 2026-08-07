#pragma once
#ifndef MDBX_CONTAINERS_HEADER_COMMON_HASHING_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_COMMON_HASHING_HPP_INCLUDED

/// \file common/hashing.hpp
/// \brief Public byte-view and hasher utilities for hashed containers.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>
#include <vector>

#if __cplusplus >= 201703L
#   include <cstddef>
#endif

namespace mdbxc {

    /// \struct ByteView
    /// \ingroup mdbxc_utils
    /// \brief Non-owning C++11-compatible view over contiguous bytes.
    struct ByteView {
        const void* data;
        std::size_t size;

        /// \brief Constructs an empty view.
        ByteView() noexcept : data(nullptr), size(0) {}

        /// \brief Constructs a view over \p data with \p size bytes.
        /// \param data Pointer to the first byte.
        /// \param size Number of bytes in the view.
        ByteView(const void* data, std::size_t size) noexcept : data(data), size(size) {}
    };

    /// \brief Creates a byte view over a string.
    /// \param key String key.
    /// \return View over the string bytes.
    inline ByteView make_byte_view(const std::string& key) noexcept {
        return ByteView(key.empty() ? nullptr : static_cast<const void*>(key.data()), key.size());
    }

    namespace detail {

        template<class T>
        struct is_hashed_byte_element {
            static const bool value =
                std::is_same<T, char>::value ||
                std::is_same<T, unsigned char>::value ||
                std::is_same<T, uint8_t>::value;
        };

#       if __cplusplus >= 201703L
        template<>
        struct is_hashed_byte_element<std::byte> {
            static const bool value = true;
        };
#       endif

    } // namespace detail

    /// \brief Creates a byte view over a supported byte vector.
    /// \tparam ByteT Byte-like element type.
    /// \tparam Alloc Allocator type.
    /// \param key Byte vector key.
    /// \return View over the vector bytes.
    template<class ByteT, class Alloc>
    typename std::enable_if<detail::is_hashed_byte_element<ByteT>::value, ByteView>::type
    make_byte_view(const std::vector<ByteT, Alloc>& key) noexcept {
        static_assert(sizeof(ByteT) == 1, "Hashed byte-vector key elements must be one byte wide");
        return ByteView(key.empty() ? nullptr : static_cast<const void*>(key.data()), key.size());
    }

    /// \brief Trait indicating keys supported by HashedKeyValueStore.
    /// \tparam T Candidate key type.
    template<class T>
    struct is_hashed_key_type {
        static const bool value = std::is_same<T, std::string>::value;
    };

    /// \brief Trait specialization for supported byte vectors.
    /// \tparam ByteT Byte-like element type.
    /// \tparam Alloc Allocator type.
    template<class ByteT, class Alloc>
    struct is_hashed_key_type<std::vector<ByteT, Alloc> > {
        static const bool value = detail::is_hashed_byte_element<ByteT>::value;
    };

    /// \struct XXH3Hasher
    /// \ingroup mdbxc_utils
    /// \brief Default non-cryptographic XXH3 64-bit hasher.
    ///
    /// \warning This hasher is a lookup accelerator, not a security boundary.
    ///          For externally controlled keys prefer a stable keyed hasher such
    ///          as \ref SipHashHasher.
    struct XXH3Hasher {
        /// \brief Hashes contiguous key bytes.
        /// \param key Bytes to hash.
        /// \return 64-bit XXH3 digest.
        std::uint64_t operator()(ByteView key) const noexcept {
            return static_cast<std::uint64_t>(XXH3_64bits(key.data, key.size));
        }
    };

    /// \class SipHashHasher
    /// \ingroup mdbxc_utils
    /// \brief Stable keyed SipHash-2-4 hasher for untrusted keys.
    ///
    /// The same key material must be reused for an existing
    /// HashedKeyValueStore; changing it changes the lookup hash domain.
    class SipHashHasher {
    public:
        /// \brief Constructs a keyed SipHash hasher.
        /// \param k0 First 64 bits of key material.
        /// \param k1 Second 64 bits of key material.
        explicit SipHashHasher(std::uint64_t k0, std::uint64_t k1) noexcept
            : m_k0(k0), m_k1(k1) {}

        /// \brief Hashes contiguous key bytes.
        /// \param key Bytes to hash.
        /// \return 64-bit SipHash-2-4 digest.
        std::uint64_t operator()(ByteView key) const noexcept {
            static const unsigned char empty = 0;
            const unsigned char* data = key.data
                ? static_cast<const unsigned char*>(key.data)
                : &empty;
            const std::size_t len = key.size;

            std::uint64_t v0 = UINT64_C(0x736f6d6570736575) ^ m_k0;
            std::uint64_t v1 = UINT64_C(0x646f72616e646f6d) ^ m_k1;
            std::uint64_t v2 = UINT64_C(0x6c7967656e657261) ^ m_k0;
            std::uint64_t v3 = UINT64_C(0x7465646279746573) ^ m_k1;

            const std::size_t full_blocks = len / 8;
            for (std::size_t i = 0; i < full_blocks; ++i) {
                std::uint64_t m = load64_le(data + i * 8);
                v3 ^= m;
                sip_round(v0, v1, v2, v3);
                sip_round(v0, v1, v2, v3);
                v0 ^= m;
            }

            std::uint64_t last = static_cast<std::uint64_t>(len) << 56;
            const unsigned char* tail = data + full_blocks * 8;
            switch (len & 7u) {
                case 7: last |= static_cast<std::uint64_t>(tail[6]) << 48;
                case 6: last |= static_cast<std::uint64_t>(tail[5]) << 40;
                case 5: last |= static_cast<std::uint64_t>(tail[4]) << 32;
                case 4: last |= static_cast<std::uint64_t>(tail[3]) << 24;
                case 3: last |= static_cast<std::uint64_t>(tail[2]) << 16;
                case 2: last |= static_cast<std::uint64_t>(tail[1]) << 8;
                case 1: last |= static_cast<std::uint64_t>(tail[0]);
                case 0: break;
            }

            v3 ^= last;
            sip_round(v0, v1, v2, v3);
            sip_round(v0, v1, v2, v3);
            v0 ^= last;
            v2 ^= UINT64_C(0xff);
            sip_round(v0, v1, v2, v3);
            sip_round(v0, v1, v2, v3);
            sip_round(v0, v1, v2, v3);
            sip_round(v0, v1, v2, v3);
            return v0 ^ v1 ^ v2 ^ v3;
        }

    private:
        std::uint64_t m_k0;
        std::uint64_t m_k1;

        static std::uint64_t rotl(std::uint64_t value, int bits) noexcept {
            return (value << bits) | (value >> (64 - bits));
        }

        static std::uint64_t load64_le(const unsigned char* data) noexcept {
            std::uint64_t value = 0;
            for (int i = 0; i < 8; ++i) {
                value |= static_cast<std::uint64_t>(data[i]) << (8 * i);
            }
            return value;
        }

        static void sip_round(std::uint64_t& v0,
                              std::uint64_t& v1,
                              std::uint64_t& v2,
                              std::uint64_t& v3) noexcept {
            v0 += v1;
            v1 = rotl(v1, 13);
            v1 ^= v0;
            v0 = rotl(v0, 32);
            v2 += v3;
            v3 = rotl(v3, 16);
            v3 ^= v2;
            v0 += v3;
            v3 = rotl(v3, 21);
            v3 ^= v0;
            v2 += v1;
            v1 = rotl(v1, 17);
            v1 ^= v2;
            v2 = rotl(v2, 32);
        }
    };

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_COMMON_HASHING_HPP_INCLUDED
