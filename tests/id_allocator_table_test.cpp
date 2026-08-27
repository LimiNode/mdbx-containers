#include "test_assert.hpp"

#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>

#include <mdbx_containers/IdAllocatorTable.hpp>

namespace {

    mdbxc::Config make_config(const std::string& pathname) {
        mdbxc::Config cfg;
        cfg.pathname = pathname;
        cfg.max_dbs = 8;
        cfg.no_subdir = true;
        cfg.relative_to_exe = false;
        return cfg;
    }

} // namespace

int main() {
    const mdbxc::Config cfg = make_config("data/id_allocator_table_test.mdbx");
    std::shared_ptr<mdbxc::Connection> conn = mdbxc::Connection::create(cfg);
    mdbxc::IdAllocatorTable ids(conn, "ids");

    // --- 1. automatic_transactions_and_reset ---
    {
        ids.reset();
        MDBXC_TEST_ASSERT(ids.current() == 0u);
        MDBXC_TEST_ASSERT(ids.next() == 1u);
        MDBXC_TEST_ASSERT(ids.next() == 2u);
        MDBXC_TEST_ASSERT(ids.current() == 2u);

        ids.reset_to(41u);
        MDBXC_TEST_ASSERT(ids.current() == 41u);
        MDBXC_TEST_ASSERT(ids.next() == 42u);
        ids.reset(0);
        MDBXC_TEST_ASSERT(ids.current() == 0u);
        ids.reset();
        MDBXC_TEST_ASSERT(ids.next() == 1u);
    }

    // --- 2. caller_transaction_and_restart_persistence ---
    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBXC_TEST_ASSERT(ids.next(txn) == 2u);
        MDBXC_TEST_ASSERT(ids.next(txn) == 3u);
        MDBXC_TEST_ASSERT(ids.current(txn) == 3u);
        txn.commit();
    }
    conn->disconnect();
    conn->connect();
    {
        mdbxc::IdAllocatorTable reopened(conn, "ids");
        MDBXC_TEST_ASSERT(reopened.current() == 3u);
        MDBXC_TEST_ASSERT(reopened.next() == 4u);
    }

    // --- 3. rollback_discards_allocations_and_resets ---
    {
        ids.reset();
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBXC_TEST_ASSERT(ids.next(txn) == 1u);
        ids.reset_to(99u, txn);
        MDBXC_TEST_ASSERT(ids.current(txn) == 99u);
        txn.rollback();
        MDBXC_TEST_ASSERT(ids.current() == 0u);
        MDBXC_TEST_ASSERT(ids.next() == 1u);
    }

    // --- 4. transaction_reset_overloads ---
    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        ids.reset_to(12u, txn);
        MDBXC_TEST_ASSERT(ids.current(txn) == 12u);
        ids.reset(txn);
        MDBXC_TEST_ASSERT(ids.current(txn) == 0u);
        ids.reset(txn.handle());
        MDBXC_TEST_ASSERT(ids.current(txn) == 0u);
        txn.rollback();
    }

    // --- 5. read_only_and_overflow_rejection_do_not_mutate ---
    {
        bool read_only_threw = false;
        auto read_txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        try {
            (void)ids.next(read_txn);
        } catch (const std::invalid_argument&) {
            read_only_threw = true;
        }
        MDBXC_TEST_ASSERT(read_only_threw);
        read_txn.commit();

        ids.reset_to((std::numeric_limits<std::uint64_t>::max)());
        bool overflow_threw = false;
        try {
            (void)ids.next();
        } catch (const std::overflow_error&) {
            overflow_threw = true;
        }
        MDBXC_TEST_ASSERT(overflow_threw);
        MDBXC_TEST_ASSERT(ids.current() ==
                          (std::numeric_limits<std::uint64_t>::max)());
        ids.reset();
    }

    // --- 6. foreign_transaction_is_rejected_before_dbi_use ---
    {
        std::shared_ptr<mdbxc::Connection> foreign_connection =
            mdbxc::Connection::create(
                make_config("data/id_allocator_foreign_test.mdbx"));
        auto foreign_txn = foreign_connection->transaction(
            mdbxc::TransactionMode::WRITABLE);
        bool foreign_threw = false;
        try {
            (void)ids.next(foreign_txn.handle());
        } catch (const std::invalid_argument&) {
            foreign_threw = true;
        }
        MDBXC_TEST_ASSERT(foreign_threw);
        foreign_txn.rollback();
    }

    // --- 7. configuration_constructor ---
    {
        mdbxc::IdAllocatorTable configured(
            make_config("data/id_allocator_config_test.mdbx"), "configured_ids");
        MDBXC_TEST_ASSERT(configured.current() == 0u);
        MDBXC_TEST_ASSERT(configured.next() == 1u);
    }

    std::cout << "IdAllocatorTable test passed.\n";
    return 0;
}
