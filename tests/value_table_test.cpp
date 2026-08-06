#include "test_assert.hpp"
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx_containers/ValueTable.hpp>

/// \brief Custom serializable state used by ValueTable tests.
struct StoredState {
    int id;
    double score;

    std::vector<uint8_t> to_bytes() const {
        std::vector<uint8_t> bytes(sizeof(id) + sizeof(score));
        std::memcpy(bytes.data(), &id, sizeof(id));
        std::memcpy(bytes.data() + sizeof(id), &score, sizeof(score));
        return bytes;
    }

    static StoredState from_bytes(const void* data, size_t size) {
        if (size != sizeof(int) + sizeof(double)) {
            throw std::runtime_error("Invalid data size for StoredState");
        }
        StoredState out;
        const uint8_t* ptr = static_cast<const uint8_t*>(data);
        std::memcpy(&out.id, ptr, sizeof(out.id));
        std::memcpy(&out.score, ptr + sizeof(out.id), sizeof(out.score));
        return out;
    }

    bool operator==(const StoredState& other) const {
        return id == other.id && score == other.score;
    }
};

int main() {
    mdbxc::Config cfg;
    cfg.pathname = "data/value_table_test.mdbx";
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    cfg.relative_to_exe = false;

    auto conn = mdbxc::Connection::create(cfg);

    {
        mdbxc::ValueTable<int> table(conn, "integer_state");
        table.clear();

        MDBXC_TEST_ASSERT(table.empty());
        MDBXC_TEST_ASSERT(!table.has_value());
        MDBXC_TEST_ASSERT(table.count() == 0);
        MDBXC_TEST_ASSERT(table.get_or(7) == 7);

        int out = 0;
        MDBXC_TEST_ASSERT(!table.try_get(out));
        MDBXC_TEST_ASSERT(!table.erase());

        bool threw = false;
        try {
            (void)table.get();
        } catch (const std::out_of_range&) {
            threw = true;
        }
        MDBXC_TEST_ASSERT(threw);

        MDBXC_TEST_ASSERT(table.insert(10));
        MDBXC_TEST_ASSERT(!table.insert(20));
        MDBXC_TEST_ASSERT(table.has_value());
        MDBXC_TEST_ASSERT(!table.empty());
        MDBXC_TEST_ASSERT(table.count() == 1);
        MDBXC_TEST_ASSERT(table.get() == 10);
        MDBXC_TEST_ASSERT(table.try_get(out) && out == 10);

        table.set(30);
        MDBXC_TEST_ASSERT(table.get() == 30);

        bool updated = table.update([](int& value) {
            value += 1;
        });
        MDBXC_TEST_ASSERT(updated);
        MDBXC_TEST_ASSERT(table.get() == 31);

#if __cplusplus >= 201703L
        auto found = table.find();
        MDBXC_TEST_ASSERT(found && *found == 31);
#else
        auto found = table.find();
        MDBXC_TEST_ASSERT(found.first && found.second == 31);
#endif

        std::pair<bool, int> compat = table.find_compat();
        MDBXC_TEST_ASSERT(compat.first && compat.second == 31);

        MDBXC_TEST_ASSERT(table.erase());
        MDBXC_TEST_ASSERT(!table.erase());
        MDBXC_TEST_ASSERT(!table.update([](int& value) {
            value += 1;
        }));
        MDBXC_TEST_ASSERT(table.count() == 0);
    }

    {
        mdbxc::ValueTable<StoredState> table(conn, "custom_state");
        table.clear();

        StoredState expected;
        expected.id = 42;
        expected.score = 3.5;
        table.set(expected);

        StoredState loaded = table.get();
        MDBXC_TEST_ASSERT(loaded == expected);

        MDBXC_TEST_ASSERT(table.update([](StoredState& value) {
            value.id += 1;
            value.score += 0.5;
        }));

        StoredState changed = table.get();
        MDBXC_TEST_ASSERT(changed.id == 43);
        MDBXC_TEST_ASSERT(changed.score == 4.0);

        table.clear();
        MDBXC_TEST_ASSERT(!table.has_value());
    }

    {
        mdbxc::ValueTable<int> table(conn, "manual_value_txn");
        table.clear();

        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBXC_TEST_ASSERT(table.insert(5, txn));
        txn.commit();

        auto read_txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        MDBXC_TEST_ASSERT(table.has_value(read_txn));
        MDBXC_TEST_ASSERT(table.count(read_txn) == 1);
        MDBXC_TEST_ASSERT(table.get(read_txn) == 5);
        read_txn.commit();

        auto update_txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBXC_TEST_ASSERT(table.update([](int& value) {
            value *= 2;
        }, update_txn));
        update_txn.commit();

        MDBXC_TEST_ASSERT(table.get() == 10);
    }

    {
        mdbxc::Config standalone_cfg;
        standalone_cfg.pathname = "data/value_table_config_test.mdbx";
        standalone_cfg.max_dbs = 2;
        standalone_cfg.no_subdir = true;
        standalone_cfg.relative_to_exe = false;

        mdbxc::ValueTable<std::string> table(standalone_cfg, "config_value");
        table.clear();
        table.set("ready");
        MDBXC_TEST_ASSERT(table.get() == "ready");
    }

    std::cout << "ValueTable test passed.\n";
    return 0;
}
