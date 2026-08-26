#include "test_assert.hpp"
#include <bitset>
#include <cmath>
#include <iostream>
#include <limits>
#include <mdbx_containers/common.hpp>

namespace {

MDBX_val empty_value() {
    MDBX_val val;
    val.iov_base = nullptr;
    val.iov_len = 0;
    return val;
}

template<typename T>
void assert_deserializes_empty() {
    T out = mdbxc::deserialize_value<T>(empty_value());
    MDBXC_TEST_ASSERT(out.empty());
}

template<typename T>
void assert_rejects_oversized_string_length() {
    const std::uint32_t len = static_cast<std::uint32_t>(~std::uint32_t(0));
    std::uint8_t raw[sizeof(len)];
    std::memcpy(raw, &len, sizeof(len));

    MDBX_val val;
    val.iov_base = raw;
    val.iov_len = sizeof(raw);

    bool thrown = false;
    try {
        (void)mdbxc::deserialize_value<T>(val);
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    MDBXC_TEST_ASSERT(thrown);
}

template<typename T>
T deserialize_unaligned_uint32_values() {
    std::vector<std::uint8_t> raw(1 + 3 * sizeof(std::uint32_t));
    std::uint32_t expected[] = {1u, 2u, 3u};
    std::memcpy(raw.data() + 1, expected, sizeof(expected));

    MDBX_val val;
    val.iov_base = raw.data() + 1;
    val.iov_len = sizeof(expected);

    return mdbxc::deserialize_value<T>(val);
}

template<typename T>
void assert_ordered_uint32_values(const T& out) {
    MDBXC_TEST_ASSERT(out.size() == 3);
    typename T::const_iterator it = out.begin();
    MDBXC_TEST_ASSERT(*it == 1u);
    ++it;
    MDBXC_TEST_ASSERT(*it == 2u);
    ++it;
    MDBXC_TEST_ASSERT(*it == 3u);
}

void assert_unordered_uint32_values(const std::unordered_set<std::uint32_t>& out) {
    MDBXC_TEST_ASSERT(out.size() == 3);
    MDBXC_TEST_ASSERT(out.find(1u) != out.end());
    MDBXC_TEST_ASSERT(out.find(2u) != out.end());
    MDBXC_TEST_ASSERT(out.find(3u) != out.end());
}

void assert_serialize_scratch_clear_retains_capacity() {
    mdbxc::SerializeScratch scratch;
    scratch.bytes.reserve(64u);
    scratch.bytes.push_back(1u);
    const std::size_t capacity = scratch.bytes.capacity();

    scratch.clear();

    MDBXC_TEST_ASSERT(scratch.bytes.empty());
    MDBXC_TEST_ASSERT(scratch.bytes.capacity() == capacity);
}

template<typename T>
void assert_integer_key_flag() {
    MDBXC_TEST_ASSERT(
        (mdbxc::get_mdbx_flags<T>() & MDBX_INTEGERKEY) !=
        static_cast<MDBX_db_flags_t>(0));
}

template<typename T>
void assert_not_integer_key_flag() {
    MDBXC_TEST_ASSERT(
        (mdbxc::get_mdbx_flags<T>() & MDBX_INTEGERKEY) ==
        static_cast<MDBX_db_flags_t>(0));
}

template<typename T>
void assert_key_roundtrip(T value) {
    mdbxc::SerializeScratch scratch;
    const MDBX_val serialized = mdbxc::serialize_key(value, scratch);
    const T restored = mdbxc::deserialize_key<T>(serialized);
    MDBXC_TEST_ASSERT(restored == value);
}

template<typename T>
void assert_key_storage_size(T value, std::size_t expected_size) {
    MDBXC_TEST_ASSERT(mdbxc::get_key_size(value) == expected_size);
    mdbxc::SerializeScratch scratch;
    const MDBX_val serialized = mdbxc::serialize_key(value, scratch);
    MDBXC_TEST_ASSERT(serialized.iov_len == expected_size);
}

template<typename LeftT, typename RightT>
void assert_same_serialized_key(LeftT lhs, RightT rhs) {
    mdbxc::SerializeScratch lhs_scratch;
    mdbxc::SerializeScratch rhs_scratch;
    const MDBX_val lhs_value = mdbxc::serialize_key(lhs, lhs_scratch);
    const MDBX_val rhs_value = mdbxc::serialize_key(rhs, rhs_scratch);
    MDBXC_TEST_ASSERT(lhs_value.iov_len == rhs_value.iov_len);
    MDBXC_TEST_ASSERT(
        std::memcmp(lhs_value.iov_base, rhs_value.iov_base, lhs_value.iov_len) == 0);
}

template<typename T>
void assert_rejects_nan_key() {
    mdbxc::SerializeScratch scratch;
    bool thrown = false;
    try {
        (void)mdbxc::serialize_key(
            std::numeric_limits<T>::quiet_NaN(), scratch);
    } catch (const std::invalid_argument&) {
        thrown = true;
    }
    MDBXC_TEST_ASSERT(thrown);
}

float float_from_raw_bits(std::uint32_t bits) {
    float out;
    std::memcpy(&out, &bits, sizeof(out));
    return out;
}

double double_from_raw_bits(std::uint64_t bits) {
    double out;
    std::memcpy(&out, &bits, sizeof(out));
    return out;
}

void assert_rejects_float_nan_bits(std::uint32_t bits) {
    mdbxc::SerializeScratch scratch;
    bool thrown = false;
    try {
        (void)mdbxc::serialize_key(float_from_raw_bits(bits), scratch);
    } catch (const std::invalid_argument&) {
        thrown = true;
    }
    MDBXC_TEST_ASSERT(thrown);
}

void assert_rejects_double_nan_bits(std::uint64_t bits) {
    mdbxc::SerializeScratch scratch;
    bool thrown = false;
    try {
        (void)mdbxc::serialize_key(double_from_raw_bits(bits), scratch);
    } catch (const std::invalid_argument&) {
        thrown = true;
    }
    MDBXC_TEST_ASSERT(thrown);
}

#if defined(__SIZEOF_INT128__)
int compare_serialized_keys(const MDBX_val& lhs, const MDBX_val& rhs) {
    const std::size_t common =
        lhs.iov_len < rhs.iov_len ? lhs.iov_len : rhs.iov_len;
    const int cmp = std::memcmp(lhs.iov_base, rhs.iov_base, common);
    if (cmp != 0) {
        return cmp;
    }
    if (lhs.iov_len == rhs.iov_len) {
        return 0;
    }
    return lhs.iov_len < rhs.iov_len ? -1 : 1;
}

template<typename T>
void assert_serialized_key_less(T lhs, T rhs) {
    mdbxc::SerializeScratch lhs_scratch;
    mdbxc::SerializeScratch rhs_scratch;
    const MDBX_val lhs_value = mdbxc::serialize_key(lhs, lhs_scratch);
    const MDBX_val rhs_value = mdbxc::serialize_key(rhs, rhs_scratch);
    MDBXC_TEST_ASSERT(compare_serialized_keys(lhs_value, rhs_value) < 0);
}
#endif

} // namespace

int main() {
    assert_serialize_scratch_clear_retains_capacity();
    assert_deserializes_empty<std::string>();
    assert_deserializes_empty<std::vector<std::uint8_t>>();
    assert_deserializes_empty<std::deque<std::uint8_t>>();
    assert_deserializes_empty<std::list<std::uint8_t>>();
    assert_deserializes_empty<std::vector<std::uint32_t>>();
    assert_deserializes_empty<std::deque<std::uint32_t>>();
    assert_deserializes_empty<std::list<std::uint32_t>>();
    assert_deserializes_empty<std::set<std::uint32_t>>();
    assert_deserializes_empty<std::unordered_set<std::uint32_t>>();
    assert_deserializes_empty<std::vector<std::string>>();
    assert_deserializes_empty<std::deque<std::string>>();
    assert_deserializes_empty<std::list<std::string>>();
    assert_deserializes_empty<std::set<std::string>>();
    assert_deserializes_empty<std::unordered_set<std::string>>();

    assert_ordered_uint32_values(deserialize_unaligned_uint32_values<std::vector<std::uint32_t>>());
    assert_ordered_uint32_values(deserialize_unaligned_uint32_values<std::deque<std::uint32_t>>());
    assert_ordered_uint32_values(deserialize_unaligned_uint32_values<std::list<std::uint32_t>>());
    assert_ordered_uint32_values(deserialize_unaligned_uint32_values<std::set<std::uint32_t>>());
    assert_unordered_uint32_values(deserialize_unaligned_uint32_values<std::unordered_set<std::uint32_t>>());

    assert_rejects_oversized_string_length<std::vector<std::string>>();
    assert_rejects_oversized_string_length<std::deque<std::string>>();
    assert_rejects_oversized_string_length<std::list<std::string>>();
    assert_rejects_oversized_string_length<std::set<std::string>>();
    assert_rejects_oversized_string_length<std::unordered_set<std::string>>();

    assert_integer_key_flag<short>();
    assert_integer_key_flag<unsigned short>();
    assert_integer_key_flag<long>();
    assert_integer_key_flag<unsigned long>();
    assert_integer_key_flag<long long>();
    assert_integer_key_flag<unsigned long long>();
    assert_integer_key_flag<wchar_t>();
    assert_integer_key_flag<char16_t>();
    assert_integer_key_flag<char32_t>();
    assert_integer_key_flag<bool>();

    assert_key_roundtrip<short>(static_cast<short>(-123));
    assert_key_roundtrip<unsigned short>(static_cast<unsigned short>(123));
    assert_key_roundtrip<long>(static_cast<long>(-123456));
    assert_key_roundtrip<unsigned long>(static_cast<unsigned long>(123456));
    assert_key_roundtrip<long long>(static_cast<long long>(-1234567890123LL));
    assert_key_roundtrip<unsigned long long>(
        static_cast<unsigned long long>(1234567890123ULL));
    assert_key_roundtrip<wchar_t>(static_cast<wchar_t>(42));
    assert_key_roundtrip<char16_t>(static_cast<char16_t>(42));
    assert_key_roundtrip<char32_t>(static_cast<char32_t>(42));
    assert_key_roundtrip<bool>(true);
    assert_key_roundtrip<char>(
        static_cast<char>(static_cast<unsigned char>(0xFFu)));

    assert_key_storage_size<int>(42, sizeof(std::uint32_t));
    assert_key_storage_size<short>(static_cast<short>(-1), sizeof(std::uint32_t));
    assert_key_storage_size<unsigned short>(
        static_cast<unsigned short>(1), sizeof(std::uint32_t));
    assert_key_storage_size<long>(static_cast<long>(-1), sizeof(std::uint64_t));
    assert_key_storage_size<unsigned long>(
        static_cast<unsigned long>(1), sizeof(std::uint64_t));
    assert_key_storage_size<long long>(
        static_cast<long long>(-1), sizeof(std::uint64_t));
    assert_key_storage_size<unsigned long long>(
        static_cast<unsigned long long>(1), sizeof(std::uint64_t));
    assert_key_storage_size<wchar_t>(
        static_cast<wchar_t>(42), sizeof(std::uint32_t));
    assert_key_storage_size<char16_t>(
        static_cast<char16_t>(42), sizeof(std::uint32_t));
    assert_key_storage_size<char32_t>(
        static_cast<char32_t>(42), sizeof(std::uint32_t));
    assert_key_storage_size<bool>(true, sizeof(std::uint32_t));
    assert_key_storage_size<std::bitset<1> >(std::bitset<1>(1u), 1u);
    assert_key_storage_size<std::bitset<9> >(std::bitset<9>(0x101u), 2u);

    assert_same_serialized_key<long, std::int64_t>(
        static_cast<long>(42), static_cast<std::int64_t>(42));
    assert_same_serialized_key<unsigned long, std::uint64_t>(
        static_cast<unsigned long>(42), static_cast<std::uint64_t>(42));
    assert_same_serialized_key<char, std::uint32_t>(
        static_cast<char>(42), static_cast<std::uint32_t>(42));
    assert_same_serialized_key<wchar_t, std::uint32_t>(
        static_cast<wchar_t>(42), static_cast<std::uint32_t>(42));
    assert_same_serialized_key<char16_t, std::uint32_t>(
        static_cast<char16_t>(42), static_cast<std::uint32_t>(42));
    assert_same_serialized_key<char32_t, std::uint32_t>(
        static_cast<char32_t>(42), static_cast<std::uint32_t>(42));
    assert_same_serialized_key<char, std::uint32_t>(
        static_cast<char>(static_cast<unsigned char>(0xFFu)),
        static_cast<std::uint32_t>(0xFFu));

    assert_same_serialized_key<float>(-0.0f, 0.0f);
    assert_same_serialized_key<double>(-0.0, 0.0);
    assert_key_roundtrip<float>(0.0f);
    assert_key_roundtrip<double>(0.0);
    assert_rejects_nan_key<float>();
    assert_rejects_nan_key<double>();
    assert_rejects_float_nan_bits(0x7fc00000u);
    assert_rejects_float_nan_bits(0x7f800001u);
    assert_rejects_float_nan_bits(0xffc00000u);
    assert_rejects_float_nan_bits(0xff800001u);
    assert_rejects_double_nan_bits(0x7ff8000000000000ull);
    assert_rejects_double_nan_bits(0x7ff0000000000001ull);
    assert_rejects_double_nan_bits(0xfff8000000000000ull);
    assert_rejects_double_nan_bits(0xfff0000000000001ull);

#if defined(__SIZEOF_INT128__)
    assert_not_integer_key_flag<__int128>();
    assert_not_integer_key_flag<unsigned __int128>();
    assert_key_storage_size<__int128>(
        static_cast<__int128>(1), sizeof(__int128));
    assert_key_storage_size<unsigned __int128>(
        static_cast<unsigned __int128>(1), sizeof(unsigned __int128));
    assert_key_roundtrip<__int128>(static_cast<__int128>(-123456789));
    assert_key_roundtrip<unsigned __int128>(
        static_cast<unsigned __int128>(123456789u));
    assert_serialized_key_less<__int128>(
        static_cast<__int128>(-2),
        static_cast<__int128>(-1));
    assert_serialized_key_less<__int128>(
        static_cast<__int128>(-1),
        static_cast<__int128>(0));
    assert_serialized_key_less<__int128>(
        static_cast<__int128>(0),
        static_cast<__int128>(1));
    assert_serialized_key_less<unsigned __int128>(
        static_cast<unsigned __int128>(1),
        static_cast<unsigned __int128>(2));
#endif

    return 0;
}
