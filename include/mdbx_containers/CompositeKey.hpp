#pragma once
#ifndef MDBX_CONTAINERS_HEADER_COMPOSITE_KEY_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_COMPOSITE_KEY_HPP_INCLUDED

/// \file CompositeKey.hpp
/// \brief Order-preserving typed composite keys for MDBX tables.

#include "common.hpp"

#include <climits>
#include <tuple>

namespace mdbxc {
namespace detail {

    template<typename T>
    struct composite_key_dependent_false : std::false_type {};

    inline void composite_key_require_bytes(const std::uint8_t* current,
                                            const std::uint8_t* end,
                                            std::size_t count) {
        if (current == nullptr ||
            static_cast<std::size_t>(end - current) < count) {
            throw std::runtime_error(
                "CompositeKey::from_bytes: truncated component");
        }
    }

    template<typename UnsignedT>
    inline void composite_key_append_big_endian(UnsignedT value,
                                                 std::vector<std::uint8_t>& out) {
        static_assert(CHAR_BIT == 8, "CompositeKey requires 8-bit bytes");
        for (std::size_t i = sizeof(UnsignedT); i > 0u; --i) {
            const std::size_t shift = (i - 1u) * CHAR_BIT;
            out.push_back(static_cast<std::uint8_t>(value >> shift));
        }
    }

    template<typename UnsignedT>
    inline UnsignedT composite_key_read_big_endian(
            const std::uint8_t*& current, const std::uint8_t* end) {
        composite_key_require_bytes(current, end, sizeof(UnsignedT));
        UnsignedT value = 0;
        for (std::size_t i = 0; i < sizeof(UnsignedT); ++i) {
            value = static_cast<UnsignedT>(
                (value << CHAR_BIT) | static_cast<UnsignedT>(*current));
            ++current;
        }
        return value;
    }

    inline void composite_key_append_terminated_bytes(
            const std::uint8_t* data, std::size_t size,
            std::vector<std::uint8_t>& out) {
        for (std::size_t i = 0; i < size; ++i) {
            if (data[i] == 0u) {
                out.push_back(0u);
                out.push_back(0xffu);
            } else {
                out.push_back(data[i]);
            }
        }
        out.push_back(0u);
        out.push_back(0u);
    }

    inline std::vector<std::uint8_t> composite_key_read_terminated_bytes(
            const std::uint8_t*& current, const std::uint8_t* end) {
        std::vector<std::uint8_t> out;
        while (current != end) {
            const std::uint8_t byte = *current;
            ++current;
            if (byte != 0u) {
                out.push_back(byte);
                continue;
            }
            if (current == end) {
                throw std::runtime_error(
                    "CompositeKey::from_bytes: truncated byte component");
            }
            const std::uint8_t escaped = *current;
            ++current;
            if (escaped == 0u) {
                return out;
            }
            if (escaped == 0xffu) {
                out.push_back(0u);
                continue;
            }
            throw std::runtime_error(
                "CompositeKey::from_bytes: invalid byte component escape");
        }
        throw std::runtime_error(
            "CompositeKey::from_bytes: missing byte component terminator");
    }

    template<typename T, typename Enable = void>
    struct CompositeKeyPartCodec {
        static void append(const T&, std::vector<std::uint8_t>&) {
            static_assert(composite_key_dependent_false<T>::value,
                          "CompositeKey supports integral, float, double, bool, "
                          "string, and uint8_t vector components only");
        }

        static T read(const std::uint8_t*&, const std::uint8_t*) {
            static_assert(composite_key_dependent_false<T>::value,
                          "CompositeKey supports integral, float, double, bool, "
                          "string, and uint8_t vector components only");
            return T();
        }
    };

    template<typename T>
    struct CompositeKeyPartCodec<T, typename std::enable_if<
            std::is_integral<T>::value && !std::is_same<T, bool>::value>::type> {
        typedef typename std::make_unsigned<T>::type UnsignedT;

        static void append(T value, std::vector<std::uint8_t>& out) {
            static_assert(sizeof(T) <= sizeof(std::uint64_t),
                          "CompositeKey integral components must be at most 64 bits");
            UnsignedT sortable = static_cast<UnsignedT>(value);
            if (std::is_signed<T>::value) {
                sortable = static_cast<UnsignedT>(
                    sortable ^ (UnsignedT(1) << (sizeof(T) * CHAR_BIT - 1u)));
            }
            composite_key_append_big_endian(sortable, out);
        }

        static T read(const std::uint8_t*& current, const std::uint8_t* end) {
            static_assert(sizeof(T) <= sizeof(std::uint64_t),
                          "CompositeKey integral components must be at most 64 bits");
            UnsignedT raw = composite_key_read_big_endian<UnsignedT>(current, end);
            if (std::is_signed<T>::value) {
                raw = static_cast<UnsignedT>(
                    raw ^ (UnsignedT(1) << (sizeof(T) * CHAR_BIT - 1u)));
                T value;
                std::memcpy(&value, &raw, sizeof(value));
                return value;
            }
            return static_cast<T>(raw);
        }
    };

    template<>
    struct CompositeKeyPartCodec<bool, void> {
        static void append(bool value, std::vector<std::uint8_t>& out) {
            out.push_back(value ? 1u : 0u);
        }

        static bool read(const std::uint8_t*& current, const std::uint8_t* end) {
            composite_key_require_bytes(current, end, 1u);
            const std::uint8_t value = *current;
            ++current;
            if (value > 1u) {
                throw std::runtime_error(
                    "CompositeKey::from_bytes: invalid bool component");
            }
            return value != 0u;
        }
    };

    template<>
    struct CompositeKeyPartCodec<float, void> {
        static void append(float value, std::vector<std::uint8_t>& out) {
            composite_key_append_big_endian(sortable_key_from_float(value), out);
        }

        static float read(const std::uint8_t*& current, const std::uint8_t* end) {
            const std::uint32_t raw =
                composite_key_read_big_endian<std::uint32_t>(current, end);
            const float value = float_from_sortable_key(raw);
            try {
                if (sortable_key_from_float(value) != raw) {
                    throw std::invalid_argument("non-canonical float");
                }
            } catch (const std::invalid_argument&) {
                throw std::runtime_error(
                    "CompositeKey::from_bytes: invalid or non-canonical float component");
            }
            return value;
        }
    };

    template<>
    struct CompositeKeyPartCodec<double, void> {
        static void append(double value, std::vector<std::uint8_t>& out) {
            composite_key_append_big_endian(sortable_key_from_double(value), out);
        }

        static double read(const std::uint8_t*& current, const std::uint8_t* end) {
            const std::uint64_t raw =
                composite_key_read_big_endian<std::uint64_t>(current, end);
            const double value = double_from_sortable_key(raw);
            try {
                if (sortable_key_from_double(value) != raw) {
                    throw std::invalid_argument("non-canonical double");
                }
            } catch (const std::invalid_argument&) {
                throw std::runtime_error(
                    "CompositeKey::from_bytes: invalid or non-canonical double component");
            }
            return value;
        }
    };

    template<>
    struct CompositeKeyPartCodec<std::string, void> {
        static void append(const std::string& value,
                           std::vector<std::uint8_t>& out) {
            composite_key_append_terminated_bytes(
                reinterpret_cast<const std::uint8_t*>(value.data()), value.size(), out);
        }

        static std::string read(const std::uint8_t*& current,
                                const std::uint8_t* end) {
            const std::vector<std::uint8_t> bytes =
                composite_key_read_terminated_bytes(current, end);
            return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        }
    };

    template<>
    struct CompositeKeyPartCodec<std::vector<std::uint8_t>, void> {
        static void append(const std::vector<std::uint8_t>& value,
                           std::vector<std::uint8_t>& out) {
            composite_key_append_terminated_bytes(
                value.empty() ? nullptr : value.data(), value.size(), out);
        }

        static std::vector<std::uint8_t> read(const std::uint8_t*& current,
                                               const std::uint8_t* end) {
            return composite_key_read_terminated_bytes(current, end);
        }
    };

    template<std::size_t Index, std::size_t Count>
    struct CompositeKeyCodec {
        template<typename TupleT>
        static void append(const TupleT& parts, std::vector<std::uint8_t>& out) {
            typedef typename std::tuple_element<Index, TupleT>::type PartT;
            CompositeKeyPartCodec<PartT>::append(std::get<Index>(parts), out);
            CompositeKeyCodec<Index + 1u, Count>::append(parts, out);
        }

        template<typename TupleT>
        static void read(TupleT& parts, const std::uint8_t*& current,
                         const std::uint8_t* end) {
            typedef typename std::tuple_element<Index, TupleT>::type PartT;
            std::get<Index>(parts) = CompositeKeyPartCodec<PartT>::read(current, end);
            CompositeKeyCodec<Index + 1u, Count>::read(parts, current, end);
        }
    };

    template<std::size_t Count>
    struct CompositeKeyCodec<Count, Count> {
        template<typename TupleT>
        static void append(const TupleT&, std::vector<std::uint8_t>&) {}

        template<typename TupleT>
        static void read(TupleT&, const std::uint8_t*&, const std::uint8_t*) {}
    };

} // namespace detail

/// \brief Typed composite key with a canonical bytewise-sortable encoding.
/// \tparam Parts Two to five supported component types.
/// \details Integral components use big-endian sortable encodings; strings and
///          byte vectors use a prefix-safe escaped terminator. The resulting
///          bytes preserve lexicographic component ordering under MDBX's normal
///          bytewise key comparator.
template<typename... Parts>
struct CompositeKey {
    static_assert(sizeof...(Parts) >= 2u && sizeof...(Parts) <= 5u,
                  "CompositeKey requires from two to five components");

    typedef std::tuple<Parts...> parts_type;
    parts_type parts;

    CompositeKey() : parts() {}

    explicit CompositeKey(const Parts&... values) : parts(values...) {}

    /// \brief Serializes this key into canonical bytewise-sortable bytes.
    /// \return Serialized key bytes.
    std::vector<std::uint8_t> to_bytes() const {
        std::vector<std::uint8_t> out;
        detail::CompositeKeyCodec<0u, sizeof...(Parts)>::append(parts, out);
        return out;
    }

    /// \brief Restores a composite key from `to_bytes()` output.
    /// \param data Serialized bytes.
    /// \param size Number of serialized bytes.
    /// \return Decoded composite key.
    /// \throws std::runtime_error When bytes are truncated, malformed, or trailing.
    static CompositeKey from_bytes(const void* data, std::size_t size) {
        if (data == nullptr) {
            throw std::runtime_error("CompositeKey::from_bytes: null input");
        }
        const std::uint8_t* current = static_cast<const std::uint8_t*>(data);
        const std::uint8_t* const end = current + size;
        CompositeKey out;
        detail::CompositeKeyCodec<0u, sizeof...(Parts)>::read(
            out.parts, current, end);
        if (current != end) {
            throw std::runtime_error("CompositeKey::from_bytes: trailing bytes");
        }
        return out;
    }

    bool operator==(const CompositeKey& other) const { return parts == other.parts; }
    bool operator!=(const CompositeKey& other) const { return !(*this == other); }
    bool operator<(const CompositeKey& other) const {
        return to_bytes() < other.to_bytes();
    }
};

/// \brief Constructs a composite key with decayed component types.
/// \tparam Parts Component argument types.
/// \param parts Component values in ordering priority.
/// \return Composite key containing the supplied components.
template<typename... Parts>
CompositeKey<typename std::decay<Parts>::type...>
make_composite_key(Parts&&... parts) {
    typedef CompositeKey<typename std::decay<Parts>::type...> KeyT;
    return KeyT(std::forward<Parts>(parts)...);
}

/// \brief Serializes components as one canonical composite key.
/// \tparam Parts Component argument types.
/// \param parts Component values in ordering priority.
/// \return Canonical composite key bytes.
template<typename... Parts>
std::vector<std::uint8_t> composite_key_to_bytes(Parts&&... parts) {
    return make_composite_key(std::forward<Parts>(parts)...).to_bytes();
}

/// \brief Decodes canonical composite key bytes.
/// \tparam Parts Target component types.
/// \param data Serialized bytes.
/// \param size Number of serialized bytes.
/// \return Decoded composite key.
template<typename... Parts>
CompositeKey<Parts...> composite_key_from_bytes(const void* data,
                                                std::size_t size) {
    return CompositeKey<Parts...>::from_bytes(data, size);
}

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_COMPOSITE_KEY_HPP_INCLUDED
