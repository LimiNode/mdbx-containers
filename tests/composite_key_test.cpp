#include "test_assert.hpp"

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include <mdbx_containers/tables.hpp>

namespace {

struct TrivialCustomId {
    std::uint64_t value;

    std::vector<std::uint8_t> to_bytes() const {
        std::vector<std::uint8_t> bytes(sizeof(value));
        std::memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    }

    static TrivialCustomId from_bytes(const void* data, std::size_t size) {
        if (size != sizeof(std::uint64_t)) {
            throw std::runtime_error("TrivialCustomId: size mismatch");
        }
        TrivialCustomId out;
        std::memcpy(&out.value, data, sizeof(out.value));
        return out;
    }
};

struct TrivialFromOnlyId {
    std::uint64_t value;

    static TrivialFromOnlyId from_bytes(const void* data, std::size_t size) {
        if (size != sizeof(std::uint64_t)) {
            throw std::runtime_error("TrivialFromOnlyId: size mismatch");
        }
        TrivialFromOnlyId out;
        std::memcpy(&out.value, data, sizeof(out.value));
        return out;
    }
};

int compare_bytes(const std::vector<std::uint8_t>& lhs,
                  const std::vector<std::uint8_t>& rhs) {
    const std::size_t common = lhs.size() < rhs.size() ? lhs.size() : rhs.size();
    const int comparison = common == 0u ? 0 :
        std::memcmp(lhs.data(), rhs.data(), common);
    if (comparison != 0) {
        return comparison;
    }
    if (lhs.size() == rhs.size()) {
        return 0;
    }
    return lhs.size() < rhs.size() ? -1 : 1;
}

template<typename KeyT>
void assert_key_bytes_less(const KeyT& lhs, const KeyT& rhs) {
    MDBXC_TEST_ASSERT(lhs < rhs);
    MDBXC_TEST_ASSERT(compare_bytes(lhs.to_bytes(), rhs.to_bytes()) < 0);
}

template<typename Fn>
void assert_throws_runtime_error(Fn fn) {
    bool thrown = false;
    try {
        fn();
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    MDBXC_TEST_ASSERT(thrown);
}

template<typename T>
void assert_integral_golden_bytes(
        T value, const std::vector<std::uint8_t>& expected) {
    const mdbxc::CompositeKey<T, bool> key(value, false);
    MDBXC_TEST_ASSERT(key.to_bytes() == expected);
    const mdbxc::CompositeKey<T, bool> restored =
        mdbxc::CompositeKey<T, bool>::from_bytes(
            expected.data(), expected.size());
    MDBXC_TEST_ASSERT(restored == key);
}

void assert_trivial_custom_key_compatibility() {
    static_assert(std::is_trivially_copyable<TrivialCustomId>::value,
                  "Regression type must be trivially copyable");
    static_assert(std::is_trivially_copyable<TrivialFromOnlyId>::value,
                  "Regression type must be trivially copyable");

    const TrivialCustomId id = {42u};
    mdbxc::SerializeScratch scratch;
    const MDBX_val serialized = mdbxc::serialize_key(id, scratch);
    MDBXC_TEST_ASSERT(serialized.iov_len == sizeof(id));

    const TrivialCustomId restored =
        mdbxc::deserialize_key<TrivialCustomId>(serialized);
    MDBXC_TEST_ASSERT(restored.value == id.value);

    const TrivialFromOnlyId from_only_id = {7u};
    const MDBX_val from_only_serialized =
        mdbxc::SerializeScratch::view(&from_only_id, sizeof(from_only_id));
    const TrivialFromOnlyId from_only_restored =
        mdbxc::deserialize_key<TrivialFromOnlyId>(from_only_serialized);
    MDBXC_TEST_ASSERT(from_only_restored.value == from_only_id.value);
}

} // namespace

int main() {
    typedef mdbxc::CompositeKey<std::int64_t, std::string, std::uint32_t> KeyT;
    const std::string binary_text("north\0star", 10u);
    const KeyT original(-7, binary_text, 42u);
    const std::vector<std::uint8_t> bytes = original.to_bytes();

    MDBXC_TEST_ASSERT(KeyT::from_bytes(bytes.data(), bytes.size()) == original);
    const KeyT decoded =
        mdbxc::composite_key_from_bytes<std::int64_t, std::string, std::uint32_t>(
            bytes.data(), bytes.size());
    MDBXC_TEST_ASSERT(decoded == original);
    MDBXC_TEST_ASSERT(
        mdbxc::composite_key_to_bytes(std::int64_t(-7), binary_text, std::uint32_t(42)) ==
        bytes);

    assert_key_bytes_less(KeyT(-8, "z", 1u), KeyT(-7, "", 0u));
    assert_key_bytes_less(
        KeyT(std::numeric_limits<std::int64_t>::min(), "", 0u),
        KeyT(std::numeric_limits<std::int64_t>::max(), "", 0u));
    assert_key_bytes_less(KeyT(-7, "alpha", 99u), KeyT(-7, "beta", 0u));
    assert_key_bytes_less(KeyT(-7, std::string("a", 1u), 1u),
                          KeyT(-7, std::string("a\0", 2u), 0u));
    assert_key_bytes_less(KeyT(-7, std::string("a\0", 2u), 1u),
                          KeyT(-7, std::string("a\1", 2u), 0u));
    assert_key_bytes_less(KeyT(-7, "same", 1u), KeyT(-7, "same", 2u));

    typedef mdbxc::CompositeKey<float, std::vector<std::uint8_t> > FloatKeyT;
    const std::vector<std::uint8_t> binary_bytes = {0u, 0xffu, 1u};
    const FloatKeyT float_key(-1.5f, binary_bytes);
    const std::vector<std::uint8_t> float_key_bytes = float_key.to_bytes();
    MDBXC_TEST_ASSERT(
        FloatKeyT::from_bytes(float_key_bytes.data(), float_key_bytes.size()) ==
        float_key);
    assert_key_bytes_less(FloatKeyT(-1.0f, binary_bytes),
                          FloatKeyT(0.0f, binary_bytes));

    typedef mdbxc::CompositeKey<std::uint8_t, std::uint16_t, std::uint32_t,
                                std::uint64_t, bool> FivePartKeyT;
    const FivePartKeyT five_part_key(1u, 2u, 3u, 4u, true);
    const std::vector<std::uint8_t> five_part_bytes = five_part_key.to_bytes();
    MDBXC_TEST_ASSERT(
        FivePartKeyT::from_bytes(five_part_bytes.data(), five_part_bytes.size()) ==
        five_part_key);

    assert_integral_golden_bytes<short>(
        static_cast<short>(-1),
        std::vector<std::uint8_t>{
            0x7fu, 0xffu, 0xffu, 0xffu, 0u, 0u, 0u, 0u
        });
    assert_integral_golden_bytes<long>(
        static_cast<long>(-1),
        std::vector<std::uint8_t>{
            0x7fu, 0xffu, 0xffu, 0xffu, 0xffu, 0xffu, 0xffu, 0xffu,
            0u, 0u, 0u, 0u
        });
    assert_integral_golden_bytes<unsigned long>(
        static_cast<unsigned long>(42u),
        std::vector<std::uint8_t>{
            0u, 0u, 0u, 0u, 0u, 0u, 0u, 42u, 0u, 0u, 0u, 0u
        });
    assert_integral_golden_bytes<char>(
        static_cast<char>(static_cast<unsigned char>(0xffu)),
        std::vector<std::uint8_t>{
            0u, 0u, 0u, 0xffu, 0u, 0u, 0u, 0u
        });
    assert_integral_golden_bytes<wchar_t>(
        static_cast<wchar_t>(42),
        std::vector<std::uint8_t>{
            0u, 0u, 0u, 42u, 0u, 0u, 0u, 0u
        });
    assert_integral_golden_bytes<std::int32_t>(
        static_cast<std::int32_t>(-1),
        std::vector<std::uint8_t>{
            0x7fu, 0xffu, 0xffu, 0xffu, 0u, 0u, 0u, 0u
        });
    assert_integral_golden_bytes<std::int64_t>(
        static_cast<std::int64_t>(-1),
        std::vector<std::uint8_t>{
            0x7fu, 0xffu, 0xffu, 0xffu, 0xffu, 0xffu, 0xffu, 0xffu,
            0u, 0u, 0u, 0u
        });
    assert_trivial_custom_key_compatibility();

    std::vector<std::uint8_t> truncated(bytes.begin(), bytes.end() - 1u);
    assert_throws_runtime_error([&truncated]() {
        (void)KeyT::from_bytes(truncated.data(), truncated.size());
    });
    const std::uint8_t invalid_escape[] = {0u, 1u};
    assert_throws_runtime_error([&invalid_escape]() {
        (void)mdbxc::CompositeKey<std::string, std::string>::from_bytes(
            invalid_escape, sizeof(invalid_escape));
    });
    const std::uint8_t invalid_float[] = {
        0u, 0u, 0u, 0u, 0u, 0u, 0u, 0u
    };
    assert_throws_runtime_error([&invalid_float]() {
        (void)mdbxc::CompositeKey<float, std::uint32_t>::from_bytes(
            invalid_float, sizeof(invalid_float));
    });
    const std::uint8_t noncanonical_negative_zero[] = {
        0x7fu, 0xffu, 0xffu, 0xffu, 0u, 0u, 0u, 0u
    };
    assert_throws_runtime_error([&noncanonical_negative_zero]() {
        (void)mdbxc::CompositeKey<float, std::uint32_t>::from_bytes(
            noncanonical_negative_zero, sizeof(noncanonical_negative_zero));
    });

    mdbxc::Config cfg;
    cfg.pathname = "data/composite_key_test.mdbx";
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    cfg.relative_to_exe = false;
    const std::shared_ptr<mdbxc::Connection> connection = mdbxc::Connection::create(cfg);

    mdbxc::KeyValueTable<KeyT, std::string> table(connection, "composite_keys");
    table.clear();
    table.insert_or_assign(KeyT(-1, "beta", 2u), "second");
    table.insert_or_assign(KeyT(-1, "alpha", 1u), "first");
    table.insert_or_assign(KeyT(0, "alpha", 1u), "third");

    const std::vector<std::pair<KeyT, std::string> > range = table.range<std::vector>(
        KeyT(-1, "", 0u), KeyT(0, "", 0u));
    MDBXC_TEST_ASSERT(range.size() == 2u);
    MDBXC_TEST_ASSERT(range[0].first == KeyT(-1, "alpha", 1u));
    MDBXC_TEST_ASSERT(range[1].first == KeyT(-1, "beta", 2u));

    return 0;
}
