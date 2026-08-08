#include "test_assert.hpp"

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx_containers/KeyOrderedMultiValueTable.hpp>

namespace {

template<class T>
void assert_vector_equal(const std::vector<T>& actual, const std::vector<T>& expected) {
    MDBXC_TEST_ASSERT(actual.size() == expected.size());
    for (std::size_t i = 0; i < actual.size(); ++i) {
        MDBXC_TEST_ASSERT(actual[i] == expected[i]);
    }
}

template<class Fn>
void assert_throws_length_error(Fn fn) {
    bool thrown = false;
    try {
        fn();
    } catch (const std::length_error&) {
        thrown = true;
    }
    MDBXC_TEST_ASSERT(thrown);
}

template<class Fn>
void assert_throws_invalid_argument(Fn fn) {
    bool thrown = false;
    try {
        fn();
    } catch (const std::invalid_argument&) {
        thrown = true;
    }
    MDBXC_TEST_ASSERT(thrown);
}

void create_raw_dbi(const std::shared_ptr<mdbxc::Connection>& conn,
                    const std::string& name,
                    MDBX_db_flags_t flags) {
    auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
    MDBX_dbi dbi = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(txn.handle(), name.c_str(), flags, &dbi),
                      "Failed to create raw DBI for ordered multi-value test");
    txn.commit();
}

void create_raw_integer_dbi_with_u32_key(const std::shared_ptr<mdbxc::Connection>& conn,
                                         const std::string& name) {
    auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
    MDBX_dbi dbi = 0;
    mdbxc::check_mdbx(
        mdbx_dbi_open(txn.handle(),
                      name.c_str(),
                      static_cast<MDBX_db_flags_t>(MDBX_CREATE |
                                                   MDBX_DUPSORT |
                                                   MDBX_INTEGERKEY),
                      &dbi),
        "Failed to create raw INTEGERKEY DBI for ordered multi-value test");

    std::uint32_t key = 1u;
    char raw_value = 'x';
    MDBX_val db_key;
    db_key.iov_base = &key;
    db_key.iov_len = sizeof(key);
    MDBX_val db_value;
    db_value.iov_base = &raw_value;
    db_value.iov_len = sizeof(raw_value);
    mdbxc::check_mdbx(mdbx_put(txn.handle(), dbi, &db_key, &db_value, MDBX_UPSERT),
                      "Failed to seed raw INTEGERKEY DBI for ordered multi-value test");
    txn.commit();
}

} // namespace

int main() {
    mdbxc::Config cfg;
    cfg.pathname = "data/key_ordered_multi_value_table_v2_test.mdbx";
    cfg.max_dbs = 32;
    cfg.no_subdir = true;
    cfg.relative_to_exe = false;

    auto conn = mdbxc::Connection::create(cfg);

    {
        mdbxc::KeyOrderedMultiValueTable<int, std::string> table(conn, "ordered_multi_values");
        table.clear();

        MDBXC_TEST_ASSERT(table.empty());
        table.append(7, "created");
        table.append(7, "created");
        table.append(7, "sent");
        table.append(8, "queued");

        MDBXC_TEST_ASSERT(table.count() == 4u);
        MDBXC_TEST_ASSERT(table.count(7) == 3u);
        MDBXC_TEST_ASSERT(table.count(7, std::string("created")) == 2u);
        MDBXC_TEST_ASSERT(table.count(7, std::string("missing")) == 0u);
        MDBXC_TEST_ASSERT(table.contains(7));
        MDBXC_TEST_ASSERT(table.contains(7, std::string("sent")));
        MDBXC_TEST_ASSERT(!table.contains(9));

        std::vector<std::string> key_values;
        key_values.push_back("created");
        key_values.push_back("created");
        key_values.push_back("sent");
        assert_vector_equal(table.find(7), key_values);

        std::vector<std::pair<int, std::string> > expected_pairs;
        expected_pairs.push_back(std::make_pair(7, std::string("created")));
        expected_pairs.push_back(std::make_pair(7, std::string("created")));
        expected_pairs.push_back(std::make_pair(7, std::string("sent")));
        expected_pairs.push_back(std::make_pair(8, std::string("queued")));
        assert_vector_equal(table.retrieve_all_vector(), expected_pairs);
        assert_vector_equal(table.range_vector(7, 8), expected_pairs);

        MDBXC_TEST_ASSERT(table.erase_at(7, 1u));
        std::vector<std::string> after_erase_at;
        after_erase_at.push_back("created");
        after_erase_at.push_back("sent");
        assert_vector_equal(table.find(7), after_erase_at);

        MDBXC_TEST_ASSERT(!table.erase_at(7, 9u));
        MDBXC_TEST_ASSERT(table.erase(7, std::string("created")) == 1u);
        std::vector<std::string> sent_only;
        sent_only.push_back("sent");
        assert_vector_equal(table.find(7), sent_only);

        MDBXC_TEST_ASSERT(table.erase(7));
        MDBXC_TEST_ASSERT(!table.contains(7));
        MDBXC_TEST_ASSERT(table.count() == 1u);
    }

    {
        mdbxc::KeyOrderedMultiValueTable<int, std::string> table(conn, "ordered_lifecycle");
        table.clear();
        table.append(1, "a");
        table.append(1, "b");
        table.append(1, "c");
        MDBXC_TEST_ASSERT(table.erase_at(1, 2u));
        table.append(1, "d");

        std::vector<std::string> after_tail_delete;
        after_tail_delete.push_back("a");
        after_tail_delete.push_back("b");
        after_tail_delete.push_back("d");
        assert_vector_equal(table.find(1), after_tail_delete);

        MDBXC_TEST_ASSERT(table.erase(1));
        table.append(1, "after-key-erase");
        std::vector<std::string> after_key_erase;
        after_key_erase.push_back("after-key-erase");
        assert_vector_equal(table.find(1), after_key_erase);

        table.clear();
        table.append(1, "after-clear");
        std::vector<std::string> after_clear;
        after_clear.push_back("after-clear");
        assert_vector_equal(table.find(1), after_clear);
    }

    {
        mdbxc::KeyOrderedMultiValueTable<int, std::string> table(conn, "ordered_signed_keys");
        table.clear();
        table.append(1, "one");
        table.append(-2, "minus-two-a");
        table.append(0, "zero");
        table.append(-2, "minus-two-b");
        table.append(-1, "minus-one");

        std::vector<std::pair<int, std::string> > expected;
        expected.push_back(std::make_pair(-2, std::string("minus-two-a")));
        expected.push_back(std::make_pair(-2, std::string("minus-two-b")));
        expected.push_back(std::make_pair(-1, std::string("minus-one")));
        expected.push_back(std::make_pair(0, std::string("zero")));
        expected.push_back(std::make_pair(1, std::string("one")));

        assert_vector_equal(table.retrieve_all_vector(), expected);
        assert_vector_equal(table.range_vector(-2, 1), expected);

        std::vector<std::string> minus_two;
        minus_two.push_back("minus-two-a");
        minus_two.push_back("minus-two-b");
        assert_vector_equal(table.find(-2), minus_two);
    }

    {
        mdbxc::KeyOrderedMultiValueTable<int, std::string> table(conn, "ordered_replace");
        table.clear();

        std::vector<mdbxc::KeyOrderedMultiValueTable<int, std::string>::value_type> input;
        input.push_back(std::make_pair(3, std::string("a")));
        input.push_back(std::make_pair(3, std::string("b")));
        input.push_back(std::make_pair(3, std::string("a")));
        table.append(input);

        std::vector<std::string> first;
        first.push_back("a");
        first.push_back("b");
        first.push_back("a");
        assert_vector_equal(table.find(3), first);

        std::vector<mdbxc::KeyOrderedMultiValueTable<int, std::string>::value_type> replacement;
        replacement.push_back(std::make_pair(4, std::string("x")));
        replacement.push_back(std::make_pair(4, std::string("y")));
        table = replacement;

        MDBXC_TEST_ASSERT(!table.contains(3));
        std::vector<std::string> second;
        second.push_back("x");
        second.push_back("y");
        assert_vector_equal(table.find(4), second);

        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        table.append(4, "z", txn);
        txn.commit();

        second.push_back("z");
        assert_vector_equal(table.find(4), second);
    }

    {
        mdbxc::KeyOrderedMultiValueTable<int, std::string> table(conn, "ordered_rollback");
        table.clear();
        table.append(1, "committed");

        {
            auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
            table.append(1, "rolled-back", txn);
            txn.rollback();
        }

        table.append(1, "after-rollback");
        std::vector<std::string> expected;
        expected.push_back("committed");
        expected.push_back("after-rollback");
        assert_vector_equal(table.find(1), expected);
    }

    {
        mdbxc::Config reopen_cfg;
        reopen_cfg.pathname = "data/key_ordered_multi_value_reopen_v2_test.mdbx";
        reopen_cfg.max_dbs = 4;
        reopen_cfg.no_subdir = true;
        reopen_cfg.relative_to_exe = false;

        {
            auto reopen_conn = mdbxc::Connection::create(reopen_cfg);
            mdbxc::KeyOrderedMultiValueTable<int, std::string> table(
                reopen_conn, "ordered_reopen");
            table.clear();
            table.append(1, "first");
            table.append(1, "second");
        }
        {
            auto reopen_conn = mdbxc::Connection::create(reopen_cfg);
            mdbxc::KeyOrderedMultiValueTable<int, std::string> reopened(
                reopen_conn, "ordered_reopen");
            reopened.append(1, "third");
            std::vector<std::string> expected;
            expected.push_back("first");
            expected.push_back("second");
            expected.push_back("third");
            assert_vector_equal(reopened.find(1), expected);
        }
    }

    {
        assert_throws_invalid_argument([conn]() {
            mdbxc::KeyOrderedMultiValueTable<int, std::string> table(
                conn, "ordered_bad_reversedup",
                static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_REVERSEDUP));
            (void)table;
        });
        assert_throws_invalid_argument([conn]() {
            mdbxc::KeyOrderedMultiValueTable<int, std::string> table(
                conn, "ordered_bad_integerdup",
                static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_INTEGERDUP));
            (void)table;
        });
        assert_throws_invalid_argument([conn]() {
            mdbxc::KeyOrderedMultiValueTable<int, std::string> table(
                conn, "ordered_bad_dupfixed",
                static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_DUPFIXED));
            (void)table;
        });

        create_raw_dbi(conn,
                       "ordered_accede_reversedup",
                       static_cast<MDBX_db_flags_t>(MDBX_CREATE |
                                                    MDBX_DUPSORT |
                                                    MDBX_REVERSEDUP));
        assert_throws_invalid_argument([conn]() {
            mdbxc::KeyOrderedMultiValueTable<int, std::string> table(
                conn, "ordered_accede_reversedup",
                static_cast<MDBX_db_flags_t>(MDBX_DB_ACCEDE));
            (void)table;
        });

        create_raw_dbi(conn,
                       "ordered_accede_non_dupsort",
                       static_cast<MDBX_db_flags_t>(MDBX_CREATE));
        assert_throws_invalid_argument([conn]() {
            mdbxc::KeyOrderedMultiValueTable<std::string, std::string> table(
                conn, "ordered_accede_non_dupsort",
                static_cast<MDBX_db_flags_t>(MDBX_DB_ACCEDE));
            (void)table;
        });

        create_raw_dbi(conn,
                       "ordered_accede_missing_integerkey",
                       static_cast<MDBX_db_flags_t>(MDBX_CREATE |
                                                    MDBX_DUPSORT));
        assert_throws_invalid_argument([conn]() {
            mdbxc::KeyOrderedMultiValueTable<int, std::string> table(
                conn, "ordered_accede_missing_integerkey",
                static_cast<MDBX_db_flags_t>(MDBX_DB_ACCEDE));
            (void)table;
        });

        create_raw_dbi(conn,
                       "ordered_accede_unexpected_integerkey",
                       static_cast<MDBX_db_flags_t>(MDBX_CREATE |
                                                    MDBX_DUPSORT |
                                                    MDBX_INTEGERKEY));
        assert_throws_invalid_argument([conn]() {
            mdbxc::KeyOrderedMultiValueTable<std::string, std::string> table(
                conn, "ordered_accede_unexpected_integerkey",
                static_cast<MDBX_db_flags_t>(MDBX_DB_ACCEDE));
            (void)table;
        });

        assert_throws_invalid_argument([conn]() {
            mdbxc::KeyOrderedMultiValueTable<std::string, std::string> table(
                conn, "ordered_requested_bad_integerkey",
                static_cast<MDBX_db_flags_t>(MDBX_CREATE | MDBX_INTEGERKEY));
            (void)table;
        });

        create_raw_dbi(conn,
                       "ordered_accede_integerkey_ok",
                       static_cast<MDBX_db_flags_t>(MDBX_CREATE |
                                                    MDBX_DUPSORT |
                                                    MDBX_INTEGERKEY));
        mdbxc::KeyOrderedMultiValueTable<int, std::string> compatible(
            conn,
            "ordered_accede_integerkey_ok",
            static_cast<MDBX_db_flags_t>(MDBX_DB_ACCEDE));
        compatible.append(-1, "minus-one");
        compatible.append(0, "zero");
        compatible.append(256, "two-fifty-six");

        create_raw_integer_dbi_with_u32_key(conn, "ordered_accede_wrong_integerkey_width");
        assert_throws_invalid_argument([conn]() {
            mdbxc::KeyOrderedMultiValueTable<long long, std::string> table(
                conn, "ordered_accede_wrong_integerkey_width",
                static_cast<MDBX_db_flags_t>(MDBX_DB_ACCEDE));
            (void)table;
        });
    }

    {
        mdbxc::Config limit_cfg;
        limit_cfg.pathname = "data/key_ordered_multi_value_oversized_v2_test.mdbx";
        limit_cfg.max_dbs = 4;
        limit_cfg.max_dupsort_value_size = 128;
        limit_cfg.no_subdir = true;
        limit_cfg.relative_to_exe = false;

        auto limit_conn = mdbxc::Connection::create(limit_cfg);
        mdbxc::KeyOrderedMultiValueTable<int, std::string> table(limit_conn, "ordered_oversized");
        table.clear();
        std::string large(1024, 'x');
        assert_throws_length_error([&table, &large]() {
            table.append(1, large);
        });
    }

    std::cout << "KeyOrderedMultiValueTable test passed.\n";
    return 0;
}
