#include "test_assert.hpp"
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <mdbx_containers/KeyValueTable.hpp>
#include <mdbx_containers/ValueTable.hpp>

namespace {

mdbxc::Transaction make_moved_writable_transaction(
    const std::shared_ptr<mdbxc::Connection>& conn) {
    auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
    return std::move(txn);
}

} // namespace

int main() {
    mdbxc::Config cfg;
    cfg.pathname = "data/transaction_test.mdbx";
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    cfg.relative_to_exe = false;

    auto conn = mdbxc::Connection::create(cfg);
    mdbxc::KeyValueTable<int, std::string> names(conn, "txn_names");
    mdbxc::ValueTable<int> version(conn, "txn_version");

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);

        bool nested_raii_blocked = false;
        try {
            auto nested = conn->transaction(mdbxc::TransactionMode::WRITABLE);
            (void)nested;
        } catch (const std::logic_error&) {
            nested_raii_blocked = true;
        }
        MDBXC_TEST_ASSERT(nested_raii_blocked);

        bool manual_begin_blocked = false;
        try {
            conn->begin(mdbxc::TransactionMode::WRITABLE);
        } catch (const std::logic_error&) {
            manual_begin_blocked = true;
        }
        MDBXC_TEST_ASSERT(manual_begin_blocked);

        bool table_ctor_blocked = false;
        try {
            mdbxc::KeyValueTable<int, std::string> nested_table(conn, "txn_nested_ctor");
            (void)nested_table;
        } catch (const std::logic_error&) {
            table_ctor_blocked = true;
        }
        MDBXC_TEST_ASSERT(table_ctor_blocked);

        txn.rollback();
    }

    {
        auto txn = make_moved_writable_transaction(conn);
        names.clear(txn);
        version.clear(txn);

        names.insert_or_assign(1, "one", txn);
        version.set(1, txn);
        txn.commit();

        MDBXC_TEST_ASSERT(names.at(1) == "one");
        MDBXC_TEST_ASSERT(version.get() == 1);
    }

    {
        auto read_txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        MDBXC_TEST_ASSERT(names.at(1, read_txn) == "one");
        MDBXC_TEST_ASSERT(version.get(read_txn) == 1);
        read_txn.commit();

        names.insert_or_assign(2, "two");
        MDBXC_TEST_ASSERT(names.at(2) == "two");
    }

    {
        const int key = 100;
        {
            auto old_txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
            old_txn.commit();

            names.begin(mdbxc::TransactionMode::WRITABLE);
        }

        names.insert_or_assign(key, "rolled back");
        names.rollback();
        MDBXC_TEST_ASSERT(!names.contains(key));
    }

    {
        const int key = 101;
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        txn.commit();

        txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        names.insert_or_assign(key, "rolled back");
        txn.rollback();
        MDBXC_TEST_ASSERT(!names.contains(key));
    }

    {
        mdbxc::Config read_only_cfg;
        read_only_cfg.pathname = "data/read_only_table_test.mdbx";
        read_only_cfg.max_dbs = 4;
        read_only_cfg.no_subdir = true;
        read_only_cfg.relative_to_exe = false;

        {
            auto write_conn = mdbxc::Connection::create(read_only_cfg);
            mdbxc::KeyValueTable<int, std::string> write_table(write_conn, "readonly_names");
            write_table.clear();
            write_table.insert_or_assign(1, "one");
        }

        read_only_cfg.read_only = true;
        auto read_conn = mdbxc::Connection::create(read_only_cfg);
        mdbxc::KeyValueTable<int, std::string> read_table(read_conn, "readonly_names");
        MDBXC_TEST_ASSERT(read_table.at(1) == "one");
        MDBXC_TEST_ASSERT(read_table.contains(1));

        bool write_failed = false;
        try {
            read_table.insert_or_assign(2, "two");
        } catch (const mdbxc::MdbxException&) {
            write_failed = true;
        }
        MDBXC_TEST_ASSERT(write_failed);
        MDBXC_TEST_ASSERT(!read_table.contains(2));
    }

    {
        mdbxc::Config cleanup_cfg;
        cleanup_cfg.pathname = "data/transaction_cleanup_test.mdbx";
        cleanup_cfg.max_dbs = 2;
        cleanup_cfg.no_subdir = true;
        cleanup_cfg.relative_to_exe = false;

        auto cleanup_conn = mdbxc::Connection::create(cleanup_cfg);
        cleanup_conn->begin(mdbxc::TransactionMode::WRITABLE);
        std::shared_ptr<mdbxc::Transaction> held_txn = cleanup_conn->current_txn();
        MDBXC_TEST_ASSERT(held_txn);
        MDBXC_TEST_ASSERT(held_txn->handle());

        bool disconnect_failed = false;
        try {
            cleanup_conn->disconnect();
        } catch (const mdbxc::MdbxException& ex) {
            disconnect_failed = ex.error_code() == MDBX_BUSY;
        }
        MDBXC_TEST_ASSERT(disconnect_failed);
        MDBXC_TEST_ASSERT(cleanup_conn->is_connected());
        MDBXC_TEST_ASSERT(held_txn->handle());

        cleanup_conn->rollback();
        MDBXC_TEST_ASSERT(!held_txn->handle());
        cleanup_conn->disconnect();
        MDBXC_TEST_ASSERT(!cleanup_conn->is_connected());

        cleanup_conn->connect();
        MDBXC_TEST_ASSERT(cleanup_conn->is_connected());
        cleanup_conn->disconnect();
    }

    {
        mdbxc::Config reset_read_cfg;
        reset_read_cfg.pathname = "data/transaction_reset_read_test.mdbx";
        reset_read_cfg.max_dbs = 2;
        reset_read_cfg.no_subdir = true;
        reset_read_cfg.relative_to_exe = false;

        auto reset_read_conn = mdbxc::Connection::create(reset_read_cfg);
        mdbxc::KeyValueTable<int, std::string> reset_read_table(reset_read_conn, "reset_read_names");
        reset_read_table.insert_or_assign(1, "one");

        {
            auto read_txn = reset_read_conn->transaction(mdbxc::TransactionMode::READ_ONLY);
            MDBXC_TEST_ASSERT(reset_read_table.at(1, read_txn) == "one");
            read_txn.commit();
            MDBXC_TEST_ASSERT(read_txn.handle());

            bool disconnect_failed = false;
            try {
                reset_read_conn->disconnect();
            } catch (const mdbxc::MdbxException& ex) {
                disconnect_failed = ex.error_code() == MDBX_BUSY;
            }
            MDBXC_TEST_ASSERT(disconnect_failed);

            bool shutdown_failed = false;
            try {
                reset_read_conn->shutdown_for(std::chrono::milliseconds(1));
            } catch (const std::logic_error&) {
                shutdown_failed = true;
            }
            MDBXC_TEST_ASSERT(shutdown_failed);
        }

        reset_read_conn->disconnect();
        MDBXC_TEST_ASSERT(!reset_read_conn->is_connected());
    }

    {
        mdbxc::Config shutdown_cfg;
        shutdown_cfg.pathname = "data/transaction_shutdown_test.mdbx";
        shutdown_cfg.max_dbs = 2;
        shutdown_cfg.no_subdir = true;
        shutdown_cfg.relative_to_exe = false;

        auto shutdown_conn = mdbxc::Connection::create(shutdown_cfg);

        std::mutex sync_mutex;
        std::condition_variable sync_cv;
        bool worker_started = false;
        bool worker_finish = false;
        bool worker_done = false;

        std::thread worker(
            [shutdown_conn, &sync_mutex, &sync_cv,
             &worker_started, &worker_finish, &worker_done]() {
                shutdown_conn->begin(mdbxc::TransactionMode::WRITABLE);
                {
                    std::lock_guard<std::mutex> lock(sync_mutex);
                    worker_started = true;
                }
                sync_cv.notify_one();

                {
                    std::unique_lock<std::mutex> lock(sync_mutex);
                    sync_cv.wait(lock, [&worker_finish]() {
                        return worker_finish;
                    });
                }

                shutdown_conn->rollback();
                {
                    std::lock_guard<std::mutex> lock(sync_mutex);
                    worker_done = true;
                }
                sync_cv.notify_one();
            });

        {
            std::unique_lock<std::mutex> lock(sync_mutex);
            sync_cv.wait(lock, [&worker_started]() {
                return worker_started;
            });
        }

        bool closed = shutdown_conn->shutdown_for(std::chrono::milliseconds(50));
        MDBXC_TEST_ASSERT(!closed);
        MDBXC_TEST_ASSERT(shutdown_conn->is_connected());

        bool new_txn_blocked = false;
        try {
            auto blocked_txn = shutdown_conn->transaction(mdbxc::TransactionMode::READ_ONLY);
            (void)blocked_txn;
        } catch (const std::logic_error&) {
            new_txn_blocked = true;
        }
        MDBXC_TEST_ASSERT(new_txn_blocked);

        {
            std::lock_guard<std::mutex> lock(sync_mutex);
            worker_finish = true;
        }
        sync_cv.notify_one();

        {
            std::unique_lock<std::mutex> lock(sync_mutex);
            sync_cv.wait(lock, [&worker_done]() {
                return worker_done;
            });
        }
        worker.join();

        closed = shutdown_conn->shutdown_for(std::chrono::seconds(2));
        MDBXC_TEST_ASSERT(closed);
        MDBXC_TEST_ASSERT(!shutdown_conn->is_connected());

        shutdown_conn->connect();
        MDBXC_TEST_ASSERT(shutdown_conn->is_connected());
        shutdown_conn->shutdown();
        MDBXC_TEST_ASSERT(!shutdown_conn->is_connected());
    }

    std::cout << "Transaction test passed.\n";
    return 0;
}
