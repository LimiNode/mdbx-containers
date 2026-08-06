#include "test_assert.hpp"

#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>

#include <mdbx_containers/KeyValueTable.hpp>
#include <mdbx_containers/TableSequence.hpp>

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
    const mdbxc::Config cfg = make_config("data/table_sequence_test.mdbx");
    std::shared_ptr<mdbxc::Connection> conn = mdbxc::Connection::create(cfg);
    mdbxc::KeyValueTable<std::uint64_t, std::string> table(conn, "sequence_source");
    mdbxc::TableSequence sequence(table);

    // --- 1. allocation_and_restart_persistence ---
    {
        table.clear();
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBXC_TEST_ASSERT(sequence.current(txn) == 0u);
        MDBXC_TEST_ASSERT(sequence.next(txn) == 1u);
        MDBXC_TEST_ASSERT(sequence.reserve(3u, txn) == 2u);
        MDBXC_TEST_ASSERT(sequence.current(txn) == 4u);
        txn.commit();
    }
    conn->disconnect();
    conn->connect();
    {
        mdbxc::KeyValueTable<std::uint64_t, std::string> reopened(conn, "sequence_source");
        mdbxc::TableSequence reopened_sequence(reopened);
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBXC_TEST_ASSERT(reopened_sequence.next(txn) == 5u);
        txn.commit();
    }

    // --- 2. rollback_discards_allocation ---
    {
        mdbxc::KeyValueTable<std::uint64_t, std::string> rollback_table(
            conn, "sequence_rollback");
        rollback_table.clear();
        mdbxc::TableSequence rollback_sequence(rollback_table);
        {
            auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
            MDBXC_TEST_ASSERT(rollback_sequence.reserve(2u, txn) == 1u);
            txn.rollback();
        }
        {
            auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
            MDBXC_TEST_ASSERT(rollback_sequence.next(txn) == 1u);
            txn.commit();
        }
    }

    // --- 3. validation_and_read_only_rejection ---
    {
        bool zero_count_threw = false;
        try {
            auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
            (void)sequence.reserve(0u, txn);
        } catch (const std::invalid_argument&) {
            zero_count_threw = true;
        }
        MDBXC_TEST_ASSERT(zero_count_threw);

        bool read_only_threw = false;
        auto read_txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        try {
            (void)sequence.next(read_txn);
        } catch (const std::invalid_argument&) {
            read_only_threw = true;
        }
        MDBXC_TEST_ASSERT(read_only_threw);
        read_txn.commit();
    }

    // --- 4. overflow_has_no_partial_update ---
    {
        mdbxc::KeyValueTable<std::uint64_t, std::string> overflow_table(
            conn, "sequence_overflow");
        overflow_table.clear();
        mdbxc::TableSequence overflow_sequence(overflow_table);
        {
            auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
            MDBXC_TEST_ASSERT(
                overflow_sequence.reserve((std::numeric_limits<std::uint64_t>::max)(), txn) ==
                1u);
            txn.commit();
        }
        {
            bool overflow_threw = false;
            auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
            try {
                (void)overflow_sequence.next(txn);
            } catch (const std::overflow_error&) {
                overflow_threw = true;
            }
            MDBXC_TEST_ASSERT(overflow_threw);
            MDBXC_TEST_ASSERT(
                overflow_sequence.current(txn) ==
                (std::numeric_limits<std::uint64_t>::max)());
            txn.commit();
        }
    }

    // --- 5. foreign_transaction_is_rejected_before_dbi_use ---
    {
        std::shared_ptr<mdbxc::Connection> foreign_connection =
            mdbxc::Connection::create(
                make_config("data/table_sequence_foreign_test.mdbx"));
        auto foreign_txn = foreign_connection->transaction(
            mdbxc::TransactionMode::WRITABLE);
        bool foreign_threw = false;
        try {
            (void)sequence.next(foreign_txn.handle());
        } catch (const std::invalid_argument&) {
            foreign_threw = true;
        }
        MDBXC_TEST_ASSERT(foreign_threw);
        foreign_txn.rollback();
    }

    std::cout << "TableSequence test passed.\n";
    return 0;
}
