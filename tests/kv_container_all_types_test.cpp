#include <iostream>
#include <sstream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include "test_assert.hpp"
#include <chrono>
#include <vector>
#include <deque>
#include <list>
#include <set>
#include <unordered_set>
#include <cstring>
#include <bitset>
#include <utility>
#include <stdexcept>
#include <limits>
#include <map>
#include <memory>
#include <cmath>

#include <mdbx_containers/KeyValueTable.hpp>

#if __cplusplus >= 201703L
#define ASSERT_FOUND(table, key, expected)                                                \
    do {                                                                                  \
        auto res = (table).find(key);                                                     \
        if (!res.has_value() || !(res.value() == (expected))) {                           \
            throw std::runtime_error("ASSERT_FOUND failed");                             \
        }                                                                                 \
    } while (0)
#else
#define ASSERT_FOUND(table, key, expected)                                                 \
    do {                                                                                  \
        auto res = (table).find_compat(key);                                              \
        if (!res.first || !(res.second == (expected))) {                                  \
            throw std::runtime_error("ASSERT_FOUND failed");                             \
        }                                                                                 \
    } while (0)
#endif

// ---- synchronized ostream (flushes once on scope exit) ----
class SyncOStream {
public:
    explicit SyncOStream(std::ostream& os) : os_(os) {}
    
    SyncOStream(const SyncOStream&) = delete;
    SyncOStream& operator=(const SyncOStream&) = delete;
    
    SyncOStream(SyncOStream&& other) noexcept
        : os_(other.os_), buffer_(std::move(other.buffer_)) {}
        
    SyncOStream& operator=(SyncOStream&&) = delete;

    template <typename T>
    SyncOStream& operator<<(const T& value) {
        buffer_ << value;
        return *this;
    }
    // manipulators (std::endl, etc.)
    SyncOStream& operator<<(std::ostream& (*manip)(std::ostream&)) {
        buffer_ << manip;
        return *this;
    }
    ~SyncOStream() {
        std::lock_guard<std::mutex> lock(get_mutex());
        os_ << buffer_.str();
        os_.flush();
    }
private:
    std::ostream& os_;
    std::ostringstream buffer_;
    static std::mutex& get_mutex() {
        static std::mutex m;
        return m;
    }
};

inline SyncOStream sync_cout() { return SyncOStream(std::cout); }
inline SyncOStream sync_cerr() { return SyncOStream(std::cerr); }

// ---- thread runner that captures exceptions ----
template <class F>
std::thread make_thread_catching(F&& f, std::exception_ptr& out_eptr) {
    typedef typename std::decay<F>::type Fn;
    std::shared_ptr<Fn> fn(new Fn(std::forward<F>(f)));
    return std::thread([fn, &out_eptr]{
        try { (*fn)(); }
        catch (...) { out_eptr = std::current_exception(); }
    });
}

// ---- safe join (no throw) ----
inline void safe_join(std::thread& t) noexcept {
    if (t.joinable()) t.join();
}

template<typename KeyT>
void assert_signed_key_range_order(
        const std::shared_ptr<mdbxc::Connection>& conn,
        const std::string& db_name) {
    mdbxc::KeyValueTable<KeyT, std::string> kv(conn, db_name);
    kv.clear();

    kv.insert_or_assign(static_cast<KeyT>(-2), "minus-two");
    kv.insert_or_assign(static_cast<KeyT>(-1), "minus-one");
    kv.insert_or_assign(static_cast<KeyT>(0), "zero");
    kv.insert_or_assign(static_cast<KeyT>(1), "one");

    std::vector<std::pair<KeyT, std::string> > expected;
    expected.push_back(std::make_pair(static_cast<KeyT>(-2), std::string("minus-two")));
    expected.push_back(std::make_pair(static_cast<KeyT>(-1), std::string("minus-one")));
    expected.push_back(std::make_pair(static_cast<KeyT>(0), std::string("zero")));
    expected.push_back(std::make_pair(static_cast<KeyT>(1), std::string("one")));

    std::vector<std::pair<KeyT, std::string> > loaded;
    kv.load(loaded);
    MDBXC_TEST_ASSERT(loaded == expected);
    MDBXC_TEST_ASSERT(kv.template range<std::vector>(
        static_cast<KeyT>(-2), static_cast<KeyT>(1)) == expected);
    MDBXC_TEST_ASSERT(kv.range_values(
        static_cast<KeyT>(-2), static_cast<KeyT>(1)) ==
        (std::vector<std::string>{"minus-two", "minus-one", "zero", "one"}));

    std::vector<std::pair<KeyT, std::string> > reverse_expected;
    reverse_expected.push_back(std::make_pair(static_cast<KeyT>(1), std::string("one")));
    reverse_expected.push_back(std::make_pair(static_cast<KeyT>(0), std::string("zero")));
    reverse_expected.push_back(std::make_pair(static_cast<KeyT>(-1), std::string("minus-one")));
    reverse_expected.push_back(std::make_pair(static_cast<KeyT>(-2), std::string("minus-two")));

    MDBXC_TEST_ASSERT(kv.range_reverse(
        static_cast<KeyT>(-2), static_cast<KeyT>(1)) == reverse_expected);
    MDBXC_TEST_ASSERT(kv.count_range(
        static_cast<KeyT>(-2), static_cast<KeyT>(1)) == 4);

#if __cplusplus >= 201703L
    auto first = kv.first();
    MDBXC_TEST_ASSERT(first.has_value() && first->first == static_cast<KeyT>(-2));
    auto last = kv.last();
    MDBXC_TEST_ASSERT(last.has_value() && last->first == static_cast<KeyT>(1));
    auto upper = kv.upper_bound(static_cast<KeyT>(-1));
    MDBXC_TEST_ASSERT(upper.has_value() && upper->first == static_cast<KeyT>(0));
#else
    std::pair<bool, std::pair<KeyT, std::string> > first = kv.first_compat();
    MDBXC_TEST_ASSERT(first.first && first.second.first == static_cast<KeyT>(-2));
    std::pair<bool, std::pair<KeyT, std::string> > last = kv.last_compat();
    MDBXC_TEST_ASSERT(last.first && last.second.first == static_cast<KeyT>(1));
    std::pair<bool, std::pair<KeyT, std::string> > upper =
        kv.upper_bound_compat(static_cast<KeyT>(-1));
    MDBXC_TEST_ASSERT(upper.first && upper.second.first == static_cast<KeyT>(0));
#endif
}

template<typename KeyT>
void assert_unsigned_key_table(
        const std::shared_ptr<mdbxc::Connection>& conn,
        const std::string& db_name) {
    mdbxc::KeyValueTable<KeyT, std::string> kv(conn, db_name);
    kv.clear();

    const KeyT one = static_cast<KeyT>(1);
    const KeyT two = static_cast<KeyT>(2);
    kv.insert_or_assign(two, "two");
    kv.insert_or_assign(one, "one");
    ASSERT_FOUND(kv, one, std::string("one"));
    ASSERT_FOUND(kv, two, std::string("two"));

    std::vector<std::pair<KeyT, std::string> > expected;
    expected.push_back(std::make_pair(one, std::string("one")));
    expected.push_back(std::make_pair(two, std::string("two")));
    MDBXC_TEST_ASSERT(kv.template range<std::vector>(one, two) == expected);
}

// ---- sample serializable structs ----
struct SimpleStruct {
    int   x{};
    float y{};

    SimpleStruct() = default;
    SimpleStruct(int x_, float y_) : x(x_), y(y_) {}

    std::vector<uint8_t> to_bytes() const {
        std::vector<uint8_t> bytes(sizeof(int) + sizeof(float));
        std::memcpy(bytes.data(), &x, sizeof(int));
        std::memcpy(bytes.data() + sizeof(int), &y, sizeof(float));
        return bytes;
    }
    static SimpleStruct from_bytes(const void* data, size_t size) {
        if (size != (sizeof(int) + sizeof(float)))
            throw std::runtime_error("Invalid data size for SimpleStruct");
        const uint8_t* ptr = static_cast<const uint8_t*>(data);
        SimpleStruct s{};
        std::memcpy(&s.x, ptr, sizeof(int));
        std::memcpy(&s.y, ptr + sizeof(int), sizeof(float));
        return s;
    }
    bool operator==(const SimpleStruct& other) const {
        return x == other.x && y == other.y;
    }
};

struct ConcurrentStruct {
    int value{};

    ConcurrentStruct() = default;
    explicit ConcurrentStruct(int v) : value(v) {}

    std::vector<uint8_t> to_bytes() const {
        std::vector<uint8_t> bytes(sizeof(int));
        std::memcpy(bytes.data(), &value, sizeof(int));
        return bytes;
    }
    static ConcurrentStruct from_bytes(const void* data, size_t size) {
        if (size != sizeof(int)) throw std::runtime_error("Invalid data size");
        ConcurrentStruct s{};
        std::memcpy(&s.value, data, sizeof(int));
        return s;
    }
    bool operator==(const ConcurrentStruct& other) const { return value == other.value; }
    bool operator!=(const ConcurrentStruct& other) const { return !(*this == other); }
};

int main() {
    mdbxc::Config cfg;
    cfg.pathname       = "data/kv_container_all_types_v2";
    cfg.max_dbs        = 64;
    cfg.no_subdir      = false;
    cfg.relative_to_exe= false;

    auto conn = mdbxc::Connection::create(cfg);

    // --- basic cases ---
    std::cout << "[case] int8 -> int8\n";
    {
        mdbxc::KeyValueTable<int8_t, int8_t> kv(conn, "i8_i8");
        kv.insert_or_assign(1, 100);
        ASSERT_FOUND(kv, 1, 100);
    }

    std::cout << "[case] int8 -> int64\n";
    {
        mdbxc::KeyValueTable<int8_t, int64_t> kv(conn, "i8_i64");
        kv.insert_or_assign(2, 1234567890123456LL);
        ASSERT_FOUND(kv, 2, 1234567890123456LL);
    }

    std::cout << "[case] int32 -> string\n";
    {
        mdbxc::KeyValueTable<int32_t, std::string> kv(conn, "i32_str");
        kv.insert_or_assign(3, "hello");
        ASSERT_FOUND(kv, 3, std::string("hello"));
    }

    std::cout << "[case] safe/fast int32 key options compatibility\n";
    {
        mdbxc::KeyValueTable<int32_t, std::string> safe_kv(conn, "i32_options");
        mdbxc::KeyValueTable<int32_t, std::string, mdbxc::FastIntegerKeyOptions> fast_kv(conn, "i32_options");
        safe_kv.insert_or_assign(31, "safe");
        ASSERT_FOUND(fast_kv, 31, std::string("safe"));
        fast_kv.insert_or_assign(32, "fast");
        ASSERT_FOUND(safe_kv, 32, std::string("fast"));
    }

    std::cout << "[case] safe/fast uint64 key options compatibility\n";
    {
        mdbxc::KeyValueTable<uint64_t, std::string> safe_kv(conn, "u64_options");
        mdbxc::KeyValueTable<uint64_t, std::string, mdbxc::FastIntegerKeyOptions> fast_kv(conn, "u64_options");
        safe_kv.insert_or_assign(6401u, "safe");
        ASSERT_FOUND(fast_kv, 6401u, std::string("safe"));
        fast_kv.insert_or_assign(6402u, "fast");
        ASSERT_FOUND(safe_kv, 6402u, std::string("fast"));
    }

    std::cout << "[case] string -> string\n";
    {
        mdbxc::KeyValueTable<std::string, std::string> kv(conn, "str_str");
        kv.insert_or_assign("key", "value");
        ASSERT_FOUND(kv, std::string("key"), std::string("value"));
    }

    std::cout << "[case] pair insert_or_assign overloads\n";
    {
        mdbxc::KeyValueTable<int, std::string> kv(conn, "pair_insert_or_assign");

        std::pair<int, std::string> first = std::make_pair(1, std::string("one"));
        kv.insert_or_assign(first);
        ASSERT_FOUND(kv, 1, std::string("one"));

        std::pair<int, std::string> replacement = std::make_pair(1, std::string("uno"));
        kv.insert_or_assign(replacement);
        ASSERT_FOUND(kv, 1, std::string("uno"));

        mdbxc::Transaction txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        std::pair<int, std::string> second = std::make_pair(2, std::string("two"));
        kv.insert_or_assign(second, txn);
        txn.commit();
        ASSERT_FOUND(kv, 2, std::string("two"));
    }

    std::cout << "[case] key range values\n";
    {
        mdbxc::KeyValueTable<int, std::string> kv(conn, "range_values");
        kv.clear();

        kv.insert_or_assign(1, "one");
        kv.insert_or_assign(2, "two");
        kv.insert_or_assign(4, "four");
        kv.insert_or_assign(5, "five");

        std::vector<std::pair<int, std::string> > middle_pairs;
        middle_pairs.push_back(std::make_pair(2, std::string("two")));
        middle_pairs.push_back(std::make_pair(4, std::string("four")));
        MDBXC_TEST_ASSERT((kv.range(2, 4) == std::map<int, std::string>(middle_pairs.begin(), middle_pairs.end())));
        MDBXC_TEST_ASSERT(kv.range<std::vector>(2, 4) == middle_pairs);
        MDBXC_TEST_ASSERT(kv.range_values(2, 4) == (std::vector<std::string>{"two", "four"}));
        MDBXC_TEST_ASSERT(kv.range_values<std::set>(2, 4) == (std::set<std::string>{"four", "two"}));

        std::vector<std::pair<int, std::string> > first_pair;
        first_pair.push_back(std::make_pair(1, std::string("one")));
        MDBXC_TEST_ASSERT((kv.range(0, 1) == std::map<int, std::string>(first_pair.begin(), first_pair.end())));
        MDBXC_TEST_ASSERT(kv.range_values(0, 1) == (std::vector<std::string>{"one"}));
        MDBXC_TEST_ASSERT(kv.range(3, 3).empty());
        MDBXC_TEST_ASSERT(kv.range(5, 2).empty());

        mdbxc::Transaction read_txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        std::vector<std::pair<int, std::string> > txn_pairs;
        txn_pairs.push_back(std::make_pair(1, std::string("one")));
        txn_pairs.push_back(std::make_pair(2, std::string("two")));
        MDBXC_TEST_ASSERT((kv.range(1, 2, read_txn) == std::map<int, std::string>(txn_pairs.begin(), txn_pairs.end())));
        MDBXC_TEST_ASSERT(kv.range<std::vector>(1, 2, read_txn) == txn_pairs);
        MDBXC_TEST_ASSERT(kv.range_values(1, 2, read_txn) == (std::vector<std::string>{"one", "two"}));
        read_txn.commit();
    }

    std::cout << "[case] numeric key range boundaries\n";
    {
        mdbxc::KeyValueTable<int, std::string> kv(conn, "range_int_boundaries");
        kv.clear();

        kv.insert_or_assign(-2, "minus-two");
        kv.insert_or_assign(-1, "minus-one");
        kv.insert_or_assign(0, "zero");
        kv.insert_or_assign(1, "one");

        std::vector<std::pair<int, std::string> > negative_pairs;
        negative_pairs.push_back(std::make_pair(-2, std::string("minus-two")));
        negative_pairs.push_back(std::make_pair(-1, std::string("minus-one")));
        MDBXC_TEST_ASSERT((kv.range(-2, -1) == std::map<int, std::string>(negative_pairs.begin(), negative_pairs.end())));
        MDBXC_TEST_ASSERT(kv.range<std::vector>(-2, -1) == negative_pairs);
        MDBXC_TEST_ASSERT(kv.range_values(-2, -1) ==
               (std::vector<std::string>{"minus-two", "minus-one"}));

        std::vector<std::pair<int, std::string> > nonnegative_pairs;
        nonnegative_pairs.push_back(std::make_pair(0, std::string("zero")));
        nonnegative_pairs.push_back(std::make_pair(1, std::string("one")));
        MDBXC_TEST_ASSERT((kv.range(0, 1) == std::map<int, std::string>(nonnegative_pairs.begin(), nonnegative_pairs.end())));
        MDBXC_TEST_ASSERT(kv.range_values(0, 1) == (std::vector<std::string>{"zero", "one"}));

        std::vector<std::pair<int, std::string> > signed_pairs;
        signed_pairs.push_back(std::make_pair(-2, std::string("minus-two")));
        signed_pairs.push_back(std::make_pair(-1, std::string("minus-one")));
        signed_pairs.push_back(std::make_pair(0, std::string("zero")));
        signed_pairs.push_back(std::make_pair(1, std::string("one")));
        std::vector<std::pair<int, std::string> > loaded_pairs;
        kv.load(loaded_pairs);
        MDBXC_TEST_ASSERT(loaded_pairs == signed_pairs);
        MDBXC_TEST_ASSERT(kv.range<std::vector>(-2, 1) == signed_pairs);
        MDBXC_TEST_ASSERT(kv.range_values(-2, 1) ==
               (std::vector<std::string>{"minus-two", "minus-one", "zero", "one"}));

        std::vector<std::pair<int, std::string> > reverse_limited;
        reverse_limited.push_back(std::make_pair(1, std::string("one")));
        reverse_limited.push_back(std::make_pair(0, std::string("zero")));
        reverse_limited.push_back(std::make_pair(-1, std::string("minus-one")));
        MDBXC_TEST_ASSERT(kv.range_reverse(-2, 1, 3) == reverse_limited);

        kv.clear();
        kv.insert_or_assign(-2, "minus-two");
        kv.insert_or_assign(0, "zero");
#if __cplusplus >= 201703L
        auto lower = kv.lower_bound(-1);
        MDBXC_TEST_ASSERT(lower.has_value() && lower->first == 0);
        auto upper = kv.upper_bound(-2);
        MDBXC_TEST_ASSERT(upper.has_value() && upper->first == 0);
        auto first = kv.first();
        MDBXC_TEST_ASSERT(first.has_value() && first->first == -2);
        auto last = kv.last();
        MDBXC_TEST_ASSERT(last.has_value() && last->first == 0);
#else
        std::pair<bool, std::pair<int, std::string> > lower = kv.lower_bound_compat(-1);
        MDBXC_TEST_ASSERT(lower.first && lower.second.first == 0);
        std::pair<bool, std::pair<int, std::string> > upper = kv.upper_bound_compat(-2);
        MDBXC_TEST_ASSERT(upper.first && upper.second.first == 0);
        std::pair<bool, std::pair<int, std::string> > first = kv.first_compat();
        MDBXC_TEST_ASSERT(first.first && first.second.first == -2);
        std::pair<bool, std::pair<int, std::string> > last = kv.last_compat();
        MDBXC_TEST_ASSERT(last.first && last.second.first == 0);
#endif
    }

    {
        mdbxc::KeyValueTable<uint64_t, std::string> kv(conn, "range_u64_boundaries");
        kv.clear();

        const uint64_t max_key = std::numeric_limits<uint64_t>::max();
        kv.insert_or_assign(0u, "zero");
        kv.insert_or_assign(1u, "one");
        kv.insert_or_assign(max_key, "max");

        std::vector<std::pair<uint64_t, std::string> > all_pairs;
        all_pairs.push_back(std::make_pair(uint64_t(0), std::string("zero")));
        all_pairs.push_back(std::make_pair(uint64_t(1), std::string("one")));
        all_pairs.push_back(std::make_pair(max_key, std::string("max")));
        MDBXC_TEST_ASSERT((kv.range(0u, max_key) ==
                std::map<uint64_t, std::string>(all_pairs.begin(), all_pairs.end())));
        MDBXC_TEST_ASSERT(kv.range<std::vector>(0u, max_key) == all_pairs);
        MDBXC_TEST_ASSERT(kv.range_values(0u, max_key) ==
               (std::vector<std::string>{"zero", "one", "max"}));
    }

    std::cout << "[case] additional integral key table support\n";
    {
        assert_signed_key_range_order<short>(conn, "range_short_boundaries");
        assert_signed_key_range_order<long>(conn, "range_long_boundaries");
        assert_signed_key_range_order<long long>(conn, "range_llong_boundaries");
        assert_unsigned_key_table<unsigned short>(conn, "range_ushort_boundaries");
        assert_unsigned_key_table<unsigned long>(conn, "range_ulong_boundaries");
        assert_unsigned_key_table<wchar_t>(conn, "range_wchar_boundaries");
        assert_unsigned_key_table<char16_t>(conn, "range_char16_boundaries");
        assert_unsigned_key_table<char32_t>(conn, "range_char32_boundaries");
    }

#if defined(__SIZEOF_INT128__)
    std::cout << "[case] wide integral bytewise key support\n";
    {
        assert_signed_key_range_order<__int128>(conn, "range_i128_boundaries");
        assert_unsigned_key_table<unsigned __int128>(conn, "range_u128_boundaries");
    }
#endif

    {
        mdbxc::KeyValueTable<double, std::string> kv(conn, "range_double_boundaries");
        kv.clear();

        kv.insert_or_assign(-1.0, "minus-one");
        kv.insert_or_assign(0.0, "zero");
        kv.insert_or_assign(1.0, "one");

        std::vector<std::pair<double, std::string> > all_pairs;
        all_pairs.push_back(std::make_pair(-1.0, std::string("minus-one")));
        all_pairs.push_back(std::make_pair(0.0, std::string("zero")));
        all_pairs.push_back(std::make_pair(1.0, std::string("one")));
        MDBXC_TEST_ASSERT((kv.range(-1.0, 1.0) ==
                std::map<double, std::string>(all_pairs.begin(), all_pairs.end())));
        MDBXC_TEST_ASSERT(kv.range<std::vector>(-1.0, 1.0) == all_pairs);
        MDBXC_TEST_ASSERT(kv.range_values(-1.0, 1.0) ==
               (std::vector<std::string>{"minus-one", "zero", "one"}));
    }

    std::cout << "[case] floating point key edge cases\n";
    {
        mdbxc::KeyValueTable<float, std::string> floats(conn, "float_key_edges");
        floats.clear();
        floats.insert_or_assign(-0.0f, "negative-zero");
        floats.insert_or_assign(0.0f, "positive-zero");
        MDBXC_TEST_ASSERT(floats.count() == 1);
        ASSERT_FOUND(floats, -0.0f, std::string("positive-zero"));
        ASSERT_FOUND(floats, 0.0f, std::string("positive-zero"));

        bool float_nan_rejected = false;
        try {
            floats.insert_or_assign(
                std::numeric_limits<float>::quiet_NaN(), "nan");
        } catch (const std::invalid_argument&) {
            float_nan_rejected = true;
        }
        MDBXC_TEST_ASSERT(float_nan_rejected);

        mdbxc::KeyValueTable<double, std::string> doubles(conn, "double_key_edges");
        doubles.clear();
        doubles.insert_or_assign(-0.0, "negative-zero");
        doubles.insert_or_assign(0.0, "positive-zero");
        MDBXC_TEST_ASSERT(doubles.count() == 1);
        ASSERT_FOUND(doubles, -0.0, std::string("positive-zero"));
        ASSERT_FOUND(doubles, 0.0, std::string("positive-zero"));

        bool double_nan_rejected = false;
        try {
            doubles.insert_or_assign(
                std::numeric_limits<double>::quiet_NaN(), "nan");
        } catch (const std::invalid_argument&) {
            double_nan_rejected = true;
        }
        MDBXC_TEST_ASSERT(double_nan_rejected);
    }

    std::cout << "[case] string -> POD(SimpleStruct)\n";
    {
        mdbxc::KeyValueTable<std::string, SimpleStruct> kv(conn, "str_struct");
        SimpleStruct s{42, 3.14f};
        kv.insert_or_assign("obj", s);
        ASSERT_FOUND(kv, std::string("obj"), s);
    }

    std::cout << "[case] string -> set<int>\n";
    {
        mdbxc::KeyValueTable<std::string, std::set<int>> kv(conn, "str_set_int");
        std::set<int> s{1, 2, 3};
        kv.insert_or_assign("digits", s);
        ASSERT_FOUND(kv, std::string("digits"), s);
    }

    std::cout << "[case] string -> unordered_set<int>\n";
    {
        mdbxc::KeyValueTable<std::string, std::unordered_set<int>> kv(conn, "str_unordered_set_int");
        std::unordered_set<int> s{1, 2, 3};
        kv.insert_or_assign("digits", s);
        ASSERT_FOUND(kv, std::string("digits"), s);
    }

    std::cout << "[case] string -> deque<int>\n";
    {
        mdbxc::KeyValueTable<std::string, std::deque<int>> kv(conn, "str_deque_int");
        std::deque<int> values{1, 2, 3};
        kv.insert_or_assign("digits", values);
        ASSERT_FOUND(kv, std::string("digits"), values);
    }

    std::cout << "[case] string -> list<int>\n";
    {
        mdbxc::KeyValueTable<std::string, std::list<int>> kv(conn, "str_list_int");
        std::list<int> values{1, 2, 3};
        kv.insert_or_assign("digits", values);
        ASSERT_FOUND(kv, std::string("digits"), values);
    }

    std::cout << "[case] int64 -> vector<uint8_t>\n";
    {
        mdbxc::KeyValueTable<int64_t, std::vector<uint8_t>> kv(conn, "i64_vec");
        std::vector<uint8_t> data{1, 2, 3, 4};
        kv.insert_or_assign(9, data);
        ASSERT_FOUND(kv, 9, data);
    }

    std::cout << "[case] string -> vector<SimpleStruct>\n";
    {
        mdbxc::KeyValueTable<std::string, std::vector<SimpleStruct>> kv(conn, "str_vec_struct");
        std::vector<SimpleStruct> vec{{1, 1.0f}, {2, 2.0f}};
        kv.insert_or_assign("many", vec);
        ASSERT_FOUND(kv, std::string("many"), vec);
    }

    std::cout << "[case] string -> list<string>\n";
    {
        mdbxc::KeyValueTable<std::string, std::list<std::string>> kv(conn, "str_list_str");
        std::list<std::string> lst{"a", "b", "c"};
        kv.insert_or_assign("letters", lst);
        ASSERT_FOUND(kv, std::string("letters"), lst);
    }

    std::cout << "[case] string -> deque<string>\n";
    {
        mdbxc::KeyValueTable<std::string, std::deque<std::string>> kv(conn, "str_deque_str");
        std::deque<std::string> values{"a", "b", "c"};
        kv.insert_or_assign("letters", values);
        ASSERT_FOUND(kv, std::string("letters"), values);
    }

    std::cout << "[case] string -> vector<string>\n";
    {
        mdbxc::KeyValueTable<std::string, std::vector<std::string>> kv(conn, "str_vector_str");
        std::vector<std::string> lst{"a", "b", "c"};
        kv.insert_or_assign("letters", lst);
        ASSERT_FOUND(kv, std::string("letters"), lst);
    }

    std::cout << "[case] string -> set<string>\n";
    {
        mdbxc::KeyValueTable<std::string, std::set<std::string>> kv(conn, "str_set_str");
        std::set<std::string> s{"a", "b", "c"};
        kv.insert_or_assign("letters", s);
        ASSERT_FOUND(kv, std::string("letters"), s);
    }

    std::cout << "[case] string -> unordered_set<string>\n";
    {
        mdbxc::KeyValueTable<std::string, std::unordered_set<std::string>> kv(conn, "str_unordered_set_str");
        std::unordered_set<std::string> s{"a", "b", "c"};
        kv.insert_or_assign("letters", s);
        ASSERT_FOUND(kv, std::string("letters"), s);
    }

    std::cout << "[case] string -> self-serializable struct\n";
    {
        struct Serializable {
            int a = 0;
            std::string b;

            Serializable() {}
            Serializable(int a_, const std::string& b_) : a(a_), b(b_) {}

            std::vector<uint8_t> to_bytes() const {
                std::vector<uint8_t> result(sizeof(int) + b.size());
                std::memcpy(result.data(), &a, sizeof(int));
                std::memcpy(result.data() + sizeof(int), b.data(), b.size());
                return result;
            }
            static Serializable from_bytes(const void* data, size_t size) {
                if (size < sizeof(int))
                    throw std::runtime_error("Invalid data size for Serializable");
                const uint8_t* ptr = static_cast<const uint8_t*>(data);
                Serializable s;
                std::memcpy(&s.a, ptr, sizeof(int));
                s.b = std::string(reinterpret_cast<const char*>(ptr + sizeof(int)), size - sizeof(int));
                return s;
            }
            bool operator==(const Serializable& other) const {
                return a == other.a && b == other.b;
            }
        };

        mdbxc::KeyValueTable<std::string, Serializable> kv(conn, "str_serializable");
        Serializable s{7, "seven"};
        kv.insert_or_assign("ser", s);
        ASSERT_FOUND(kv, std::string("ser"), s);
    }

    // --- NEW: bitset key example ---
    std::cout << "[case] std::bitset<32> -> int\n";
    {
        using Bits = std::bitset<32>;
        mdbxc::KeyValueTable<Bits, int> kv(conn, "bitset32_int");
        Bits key(std::string("10101010101010101010101010101010")); // 32-bit pattern
        kv.insert_or_assign(key, 31415);
        ASSERT_FOUND(kv, key, 31415);
    }

    // --- concurrency smoke test ---
    std::cout << "[concurrency] start\n";
    {
        mdbxc::KeyValueTable<int, ConcurrentStruct> kv(conn, "concurrent_test");

        std::mutex mtx;
        std::condition_variable cv;
        std::size_t epoch = 0;
        std::size_t last_seen = 0;
        std::atomic<bool> failed(false);
        std::atomic<bool> done(false);
        ConcurrentStruct written;
        std::exception_ptr w_ex, r_ex;

        auto writer = make_thread_catching([&kv, &failed, &mtx, &written, &epoch, &cv, &done] {
            try {
                for (int i = 0; i < 1000; ++i) {
                    if (failed) break;
                    ConcurrentStruct w(i);
                    {
                        std::lock_guard<std::mutex> lock(mtx);
                        kv.insert_or_assign(1, w);
                        written = w;
                        ++epoch;
                    }
                    cv.notify_one();
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                done.store(true, std::memory_order_release);
                cv.notify_all();
                sync_cout() << "[writer] done\n";
            } catch (const std::exception& ex) {
                sync_cerr() << "[error] writer: " << ex.what() << "\n";
                failed = true;
                done.store(true, std::memory_order_release);
                cv.notify_all();
            } catch (...) {
                sync_cerr() << "[error] writer: unknown\n";
                failed = true;
                done.store(true, std::memory_order_release);
                cv.notify_all();
            }
        }, w_ex);

        auto reader = make_thread_catching([&mtx, &cv, &epoch, &last_seen, &done, &failed, &written, &kv] {
            try {
                for (;;) {
                    std::unique_lock<std::mutex> lock(mtx);
                    bool ok = cv.wait_for(lock, std::chrono::seconds(2), [&epoch, &last_seen, &done] {
                        return epoch > last_seen || done.load(std::memory_order_acquire);
                    });
                    if (!ok) {
                        sync_cerr() << "[error] reader timeout (epoch=" << epoch
                                    << ", last_seen=" << last_seen << ")\n";
                        failed = true;
                        break;
                    }
                    if (done.load(std::memory_order_acquire) && epoch == last_seen) {
                        break;
                    }
                    last_seen = epoch;
                    ConcurrentStruct expected = written;
                    lock.unlock();

#                   if __cplusplus >= 201703L
                    auto val = kv.find(1);
                    if (!val || *val != expected) {
                        sync_cerr() << "[error] mismatch: got "
                                    << (val ? val->value : -1)
                                    << ", expected " << expected.value << "\n";
                        failed = true;
                        break;
                    }
#                   else
                    auto val = kv.find_compat(1);
                    if (!val.first || val.second != expected) {
                        sync_cerr() << "[error] mismatch: got "
                                    << val.second.value
                                    << ", expected " << expected.value << "\n";
                        failed = true;
                        break;
                    }
#                   endif
                }
                sync_cout() << "[reader] done\n";
            } catch (const std::exception& ex) {
                sync_cerr() << "[error] reader: " << ex.what() << "\n";
                failed = true;
            } catch (...) {
                sync_cerr() << "[error] reader: unknown\n";
                failed = true;
            }
        }, r_ex);

        safe_join(writer);
        safe_join(reader);

        if (w_ex) {
            try { std::rethrow_exception(w_ex); }
            catch (const std::exception& e) { sync_cerr() << "[error] writer rethrow: " << e.what() << "\n"; }
            catch (...) { sync_cerr() << "[error] writer rethrow: unknown\n"; }
        }
        if (r_ex) {
            try { std::rethrow_exception(r_ex); }
            catch (const std::exception& e) { sync_cerr() << "[error] reader rethrow: " << e.what() << "\n"; }
            catch (...) { sync_cerr() << "[error] reader rethrow: unknown\n"; }
        }

        if (w_ex || r_ex) {
            sync_cerr() << "[concurrency] failed: thread exception\n";
            return 1;
        }
        if (failed) {
            std::cerr << "[concurrency] failed\n";
            return 1;
        }

        std::cout << "[concurrency] ok\n";
    }

    // --- Range API extension tests ---
    std::cout << "[case] range api extensions\n";
    {
        mdbxc::KeyValueTable<int, std::string> kv(conn, "kv_range_api");
        kv.clear();
        kv.insert_or_assign(1, "one");
        kv.insert_or_assign(2, "two");
        kv.insert_or_assign(3, "three");
        kv.insert_or_assign(4, "four");
        kv.insert_or_assign(5, "five");

        // for_each_range
        std::vector<std::pair<int, std::string>> collected;
        bool completed = kv.for_each_range(1, 5, [&collected](const int& k, const std::string& v) -> bool {
            collected.push_back(std::make_pair(k, v));
            return true;
        });
        MDBXC_TEST_ASSERT(completed);
        MDBXC_TEST_ASSERT(collected.size() == 5);

        // filter_range
        std::vector<std::pair<int, std::string>> evens = kv.filter_range(1, 5, [](const int& k, const std::string&) -> bool {
            return k % 2 == 0;
        });
        MDBXC_TEST_ASSERT(evens.size() == 2);
        MDBXC_TEST_ASSERT(evens[0].first == 2);
        MDBXC_TEST_ASSERT(evens[1].first == 4);

        // reverse range
        std::vector<std::pair<int, std::string>> rev = kv.range_reverse(1, 5);
        MDBXC_TEST_ASSERT(rev.size() == 5);
        MDBXC_TEST_ASSERT(rev[0].first == 5);
        MDBXC_TEST_ASSERT(rev[4].first == 1);

        // reverse range limit
        std::vector<std::pair<int, std::string>> rev_limit = kv.range_reverse(1, 5, 2);
        MDBXC_TEST_ASSERT(rev_limit.size() == 2);
        MDBXC_TEST_ASSERT(rev_limit[0].first == 5);

        // contains_range / count_range / erase_range
        MDBXC_TEST_ASSERT(kv.contains_range(2, 4));
        MDBXC_TEST_ASSERT(kv.count_range(2, 4) == 3);
        std::size_t erased = kv.erase_range(2, 4);
        MDBXC_TEST_ASSERT(erased == 3);
        MDBXC_TEST_ASSERT(kv.count() == 2);

        // update existing
        bool updated = kv.update(1, [](std::string& v) {
            v = "ONE";
        });
        MDBXC_TEST_ASSERT(updated);
        ASSERT_FOUND(kv, 1, std::string("ONE"));

        // update missing
        bool missing_updated = kv.update(99, [](std::string& v) {
            v = "X";
        });
        MDBXC_TEST_ASSERT(!missing_updated);

        // update rollback when mutator throws
        bool update_threw = false;
        try {
            kv.update(1, [](std::string& v) {
                v = "BAD";
                throw std::runtime_error("update failure");
            });
        } catch (const std::runtime_error&) {
            update_threw = true;
        }
        MDBXC_TEST_ASSERT(update_threw);
        ASSERT_FOUND(kv, 1, std::string("ONE"));

        // find_many
        kv.insert_or_assign(2, "two");
        kv.insert_or_assign(3, "three");
        std::map<int, std::string> many = kv.find_many(std::vector<int>{1, 2, 99});
        MDBXC_TEST_ASSERT(many.size() == 2);
        MDBXC_TEST_ASSERT(many[1] == "ONE");
        MDBXC_TEST_ASSERT(many[2] == "two");

        // find_many_vector preserves order, skips missing
        std::vector<std::pair<int, std::string>> many_vec = kv.find_many_vector(std::vector<int>{99, 3, 1});
        MDBXC_TEST_ASSERT(many_vec.size() == 2);
        MDBXC_TEST_ASSERT(many_vec[0].first == 3);
        MDBXC_TEST_ASSERT(many_vec[1].first == 1);

        // bounds compat (C++11 only)
#if __cplusplus < 201703L
        std::pair<bool, std::pair<int, std::string>> lb = kv.lower_bound_compat(2);
        MDBXC_TEST_ASSERT(lb.first && lb.second.first == 2);
        std::pair<bool, std::pair<int, std::string>> lb_named = kv.lower_bound(2);
        if (!lb_named.first || lb_named.second.first != 2) {
            throw std::runtime_error("lower_bound failed for KeyValueTable C++11");
        }
        std::pair<bool, std::pair<int, std::string>> f = kv.first_compat();
        MDBXC_TEST_ASSERT(f.first && f.second.first == 1);
        std::pair<bool, std::pair<int, std::string>> l = kv.last_compat();
        if (!l.first || l.second.first != 5) {
            throw std::runtime_error("last_compat failed for KeyValueTable");
        }
        std::pair<bool, int> min_key = kv.min_key_compat();
        if (!min_key.first || min_key.second != 1) {
            throw std::runtime_error("min_key_compat failed for KeyValueTable");
        }
        std::pair<bool, int> max_key = kv.max_key_compat();
        if (!max_key.first || max_key.second != 5) {
            throw std::runtime_error("max_key_compat failed for KeyValueTable");
        }
        std::pair<bool, int> named_max_key = kv.max_key();
        if (!named_max_key.first || named_max_key.second != 5) {
            throw std::runtime_error("max_key failed for KeyValueTable C++11");
        }
#endif
    }

#if __cplusplus >= 201703L
    {
        mdbxc::KeyValueTable<int, std::string> kv(conn, "kv_range_api_opt");
        kv.clear();
        kv.insert_or_assign(1, "a");
        kv.insert_or_assign(3, "c");
        kv.insert_or_assign(2, "b");

        auto lb = kv.lower_bound(1);
        if (!lb.has_value() || lb->first != 1) {
            throw std::runtime_error("lower_bound failed for KeyValueTable");
        }
        auto ub = kv.upper_bound(1);
        if (!ub.has_value() || ub->first != 2) {
            throw std::runtime_error("upper_bound failed for KeyValueTable");
        }
        auto fr = kv.first();
        if (!fr.has_value() || fr->first != 1) {
            throw std::runtime_error("first failed for KeyValueTable");
        }
        auto la = kv.last();
        if (!la.has_value() || la->first != 3) {
            throw std::runtime_error("last failed for KeyValueTable");
        }
        auto min_key = kv.min_key();
        if (!min_key.has_value() || min_key.value() != 1) {
            throw std::runtime_error("min_key failed for KeyValueTable");
        }
        auto max_key = kv.max_key();
        if (!max_key.has_value() || max_key.value() != 3) {
            throw std::runtime_error("max_key failed for KeyValueTable");
        }

        mdbxc::Transaction read_txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        auto txn_bound = kv.lower_bound(2, read_txn);
        if (!txn_bound.has_value() || txn_bound->first != 2) {
            throw std::runtime_error("transaction lower_bound failed for KeyValueTable");
        }
        read_txn.commit();
    }
#endif

    {
        mdbxc::KeyValueTable<std::string, int> kv(conn, "kv_range_api_string_bounds");
        kv.clear();
        kv.insert_or_assign("alpha", 1);
        kv.insert_or_assign("beta", 2);
#if __cplusplus >= 201703L
        auto ub = kv.upper_bound(std::string("alpha"));
        if (!ub.has_value() || ub->first != "beta") {
            throw std::runtime_error("string upper_bound failed for KeyValueTable");
        }
#else
        std::pair<bool, std::pair<std::string, int>> ub = kv.upper_bound_compat(std::string("alpha"));
        if (!ub.first || ub.second.first != "beta") {
            throw std::runtime_error("string upper_bound_compat failed for KeyValueTable");
        }
#endif
    }

    std::cout << "[result] all tests passed\n";
    return 0;
}
