#include "test_assert.hpp"
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx_containers/KeyTable.hpp>

int main() {
    mdbxc::Config cfg;
    cfg.pathname = "data/key_table_test.mdbx";
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    cfg.relative_to_exe = false;

    auto conn = mdbxc::Connection::create(cfg);

    {
        mdbxc::KeyTable<std::string> table(conn, "keys");
        table.clear();

        MDBXC_TEST_ASSERT(table.empty());
        MDBXC_TEST_ASSERT(table.insert("alpha"));
        MDBXC_TEST_ASSERT(!table.insert("alpha"));
        MDBXC_TEST_ASSERT(table.insert("beta"));
        MDBXC_TEST_ASSERT(table.contains("alpha"));
        MDBXC_TEST_ASSERT(!table.contains("gamma"));
        MDBXC_TEST_ASSERT(table.count() == 2);

        std::set<std::string> as_set;
        table.load(as_set);
        MDBXC_TEST_ASSERT(as_set == (std::set<std::string>{"alpha", "beta"}));

        std::vector<std::string> as_vector = table.retrieve_all<std::vector>();
        MDBXC_TEST_ASSERT(as_vector.size() == 2);

        MDBXC_TEST_ASSERT(table.range("alpha", "beta") ==
               (std::set<std::string>{"alpha", "beta"}));
        MDBXC_TEST_ASSERT(table.range<std::vector>("alpha", "beta") ==
               (std::vector<std::string>{"alpha", "beta"}));
        MDBXC_TEST_ASSERT(table.range("aardvark", "alpha") ==
               (std::set<std::string>{"alpha"}));
        MDBXC_TEST_ASSERT(table.range("gamma", "omega").empty());
        MDBXC_TEST_ASSERT(table.range("zeta", "alpha").empty());

        MDBXC_TEST_ASSERT(table.erase("alpha"));
        MDBXC_TEST_ASSERT(!table.erase("alpha"));
        MDBXC_TEST_ASSERT(!table.contains("alpha"));
        MDBXC_TEST_ASSERT(table.count() == 1);

        std::set<std::string> replacement{"delta", "epsilon"};
        table.reconcile(replacement);
        MDBXC_TEST_ASSERT(table.count() == 2);
        MDBXC_TEST_ASSERT(table.contains("delta"));
        MDBXC_TEST_ASSERT(!table.contains("beta"));

        std::set<std::string> assigned{"zeta"};
        table = assigned;
        MDBXC_TEST_ASSERT(table.count() == 1);
        MDBXC_TEST_ASSERT(table.contains("zeta"));
    }

    {
        mdbxc::KeyTable<int> table(conn, "manual_keys");
        table.clear();

        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBXC_TEST_ASSERT(table.insert(1, txn));
        MDBXC_TEST_ASSERT(table.insert(2, txn));
        txn.commit();

        auto read_txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        MDBXC_TEST_ASSERT(table.contains(1, read_txn));
        MDBXC_TEST_ASSERT(table.count(read_txn) == 2);
        MDBXC_TEST_ASSERT(table.range(1, 2, read_txn) == (std::set<int>{1, 2}));
        MDBXC_TEST_ASSERT(table.range<std::vector>(1, 2, read_txn) == (std::vector<int>{1, 2}));
        read_txn.commit();
    }

    {
        mdbxc::KeyTable<int> safe_table(conn, "key_options");
        mdbxc::KeyTable<int, mdbxc::FastIntegerKeyOptions> fast_table(conn, "key_options");
        safe_table.clear();
        MDBXC_TEST_ASSERT(safe_table.insert(11));
        MDBXC_TEST_ASSERT(fast_table.contains(11));
        MDBXC_TEST_ASSERT(fast_table.insert(12));
        MDBXC_TEST_ASSERT(safe_table.contains(12));
        MDBXC_TEST_ASSERT(safe_table.range(10, 12) == (std::set<int>{11, 12}));
        MDBXC_TEST_ASSERT(fast_table.range(12, 12) == (std::set<int>{12}));
    }

    // --- Range API extension tests ---
    {
        mdbxc::KeyTable<int> table(conn, "range_api_keys");
        table.clear();
        table.insert(1);
        table.insert(2);
        table.insert(3);
        table.insert(4);
        table.insert(5);

        // for_each_range full scan
        std::vector<int> collected;
        bool completed = table.for_each_range(1, 5, [&collected](const int& key) -> bool {
            collected.push_back(key);
            return true;
        });
        MDBXC_TEST_ASSERT(completed);
        MDBXC_TEST_ASSERT(collected == (std::vector<int>{1, 2, 3, 4, 5}));

        // for_each_range early stop
        collected.clear();
        completed = table.for_each_range(1, 5, [&collected](const int& key) -> bool {
            collected.push_back(key);
            return collected.size() < 3;
        });
        MDBXC_TEST_ASSERT(!completed);
        MDBXC_TEST_ASSERT(collected.size() == 3);

        // filter_range
        std::vector<int> evens = table.filter_range(1, 5, [](const int& key) -> bool {
            return key % 2 == 0;
        });
        MDBXC_TEST_ASSERT(evens == (std::vector<int>{2, 4}));

        // reverse range
        std::vector<int> rev = table.range_reverse(1, 5);
        MDBXC_TEST_ASSERT(rev == (std::vector<int>{5, 4, 3, 2, 1}));

        // reverse range limit
        std::vector<int> rev_limit = table.range_reverse(1, 5, 2);
        MDBXC_TEST_ASSERT(rev_limit == (std::vector<int>{5, 4}));

        // reverse range limit 0
        MDBXC_TEST_ASSERT(table.range_reverse(1, 5, static_cast<std::size_t>(0)).empty());

        // contains_range / count_range / erase_range
        MDBXC_TEST_ASSERT(table.contains_range(2, 4));
        MDBXC_TEST_ASSERT(!table.contains_range(10, 20));
        MDBXC_TEST_ASSERT(table.count_range(2, 4) == 3);
        MDBXC_TEST_ASSERT(table.count_range(10, 20) == 0);

        std::size_t erased = table.erase_range(2, 4);
        MDBXC_TEST_ASSERT(erased == 3);
        MDBXC_TEST_ASSERT(table.count() == 2);
        MDBXC_TEST_ASSERT(table.contains(1));
        MDBXC_TEST_ASSERT(table.contains(5));
        MDBXC_TEST_ASSERT(!table.contains(3));

        // from > to returns empty / 0 / false
        MDBXC_TEST_ASSERT(table.range_reverse(5, 1).empty());
        MDBXC_TEST_ASSERT(table.count_range(5, 1) == 0);
        MDBXC_TEST_ASSERT(!table.contains_range(5, 1));
    }

#if __cplusplus < 201703L
    // --- Bounds compat (C++11) ---
    {
        mdbxc::KeyTable<int> table(conn, "range_api_compat_keys");
        table.clear();
        table.insert(10);
        table.insert(20);

        std::pair<bool, int> lb = table.lower_bound_compat(10);
        MDBXC_TEST_ASSERT(lb.first && lb.second == 10);
        std::pair<bool, int> ub = table.upper_bound_compat(10);
        MDBXC_TEST_ASSERT(ub.first && ub.second == 20);
        std::pair<bool, int> named_ub = table.upper_bound(10);
        if (!named_ub.first || named_ub.second != 20) {
            throw std::runtime_error("upper_bound failed for KeyTable C++11");
        }
        std::pair<bool, int> f = table.first_compat();
        MDBXC_TEST_ASSERT(f.first && f.second == 10);
        std::pair<bool, int> l = table.last_compat();
        MDBXC_TEST_ASSERT(l.first && l.second == 20);
        std::pair<bool, int> mn = table.min_key_compat();
        MDBXC_TEST_ASSERT(mn.first && mn.second == 10);
        std::pair<bool, int> mx = table.max_key_compat();
        MDBXC_TEST_ASSERT(mx.first && mx.second == 20);
        std::pair<bool, int> named_mx = table.max_key();
        if (!named_mx.first || named_mx.second != 20) {
            throw std::runtime_error("max_key failed for KeyTable C++11");
        }

        mdbxc::KeyTable<int> empty_table(conn, "range_api_compat_empty");
        empty_table.clear();
        MDBXC_TEST_ASSERT(!empty_table.lower_bound_compat(1).first);
        MDBXC_TEST_ASSERT(!empty_table.first_compat().first);
    }

    {
        mdbxc::KeyTable<std::string> table(conn, "range_api_compat_string_bounds");
        table.clear();
        table.insert("alpha");
        table.insert("beta");

        std::pair<bool, std::string> upper = table.upper_bound_compat(std::string("alpha"));
        if (!upper.first || upper.second != "beta") {
            throw std::runtime_error("string upper_bound_compat failed for KeyTable");
        }
    }
#endif

#if __cplusplus >= 201703L
    // --- Bounds optional (C++17) ---
    {
        mdbxc::KeyTable<int> table(conn, "range_api_opt_keys");
        table.clear();
        table.insert(1);
        table.insert(2);
        table.insert(3);

        MDBXC_TEST_ASSERT(table.lower_bound(2) == std::optional<int>(2));
        auto upper = table.upper_bound(2);
        if (!upper.has_value() || upper.value() != 3) {
            throw std::runtime_error("upper_bound failed for KeyTable");
        }
        MDBXC_TEST_ASSERT(table.first() == std::optional<int>(1));
        MDBXC_TEST_ASSERT(table.last() == std::optional<int>(3));
        MDBXC_TEST_ASSERT(table.min_key() == std::optional<int>(1));
        MDBXC_TEST_ASSERT(table.max_key() == std::optional<int>(3));

        mdbxc::Transaction read_txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        auto txn_lower = table.lower_bound(2, read_txn);
        if (!txn_lower.has_value() || txn_lower.value() != 2) {
            throw std::runtime_error("transaction lower_bound failed for KeyTable");
        }
        read_txn.commit();
    }

    {
        mdbxc::KeyTable<std::string> table(conn, "range_api_string_bounds");
        table.clear();
        table.insert("alpha");
        table.insert("beta");

        auto upper = table.upper_bound(std::string("alpha"));
        if (!upper.has_value() || upper.value() != "beta") {
            throw std::runtime_error("string upper_bound failed for KeyTable");
        }
    }
#endif

    {
        mdbxc::KeyTable<int> table(conn, "signed_integer_key_order");
        table.clear();
        table.insert(1);
        table.insert(-2);
        table.insert(0);
        table.insert(-1);

        MDBXC_TEST_ASSERT(table.retrieve_all<std::vector>() ==
                          (std::vector<int>{-2, -1, 0, 1}));
        MDBXC_TEST_ASSERT(table.operator()<std::vector>() ==
                          (std::vector<int>{-2, -1, 0, 1}));
        MDBXC_TEST_ASSERT(table.range<std::vector>(-2, 1) ==
                          (std::vector<int>{-2, -1, 0, 1}));
        MDBXC_TEST_ASSERT(table.range_reverse(-2, 1, 3) ==
                          (std::vector<int>{1, 0, -1}));

        table.clear();
        table.insert(-2);
        table.insert(0);

#if __cplusplus >= 201703L
        auto lower = table.lower_bound(-1);
        MDBXC_TEST_ASSERT(lower.has_value() && lower.value() == 0);
        auto upper = table.upper_bound(-2);
        MDBXC_TEST_ASSERT(upper.has_value() && upper.value() == 0);
        auto first = table.first();
        MDBXC_TEST_ASSERT(first.has_value() && first.value() == -2);
        auto last = table.last();
        MDBXC_TEST_ASSERT(last.has_value() && last.value() == 0);
#else
        std::pair<bool, int> lower = table.lower_bound_compat(-1);
        MDBXC_TEST_ASSERT(lower.first && lower.second == 0);
        std::pair<bool, int> upper = table.upper_bound_compat(-2);
        MDBXC_TEST_ASSERT(upper.first && upper.second == 0);
        std::pair<bool, int> first = table.first_compat();
        MDBXC_TEST_ASSERT(first.first && first.second == -2);
        std::pair<bool, int> last = table.last_compat();
        MDBXC_TEST_ASSERT(last.first && last.second == 0);
#endif
    }

    std::cout << "KeyTable test passed.\n";
    return 0;
}
