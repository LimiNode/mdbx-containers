#include <mdbx_containers/sync.hpp>

#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void cleanup(const std::string& path) {
    std::remove(path.c_str());
}

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId out{};
    for (std::size_t i = 0u; i < out.size(); ++i) {
        out[i] = static_cast<std::uint8_t>(seed + i);
    }
    return out;
}

void test_ordered_element_state_persists_live_and_tombstone_records() {
    const std::string path = "test_ordered_element_state.mdbx";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0x41u);
    const std::vector<std::uint8_t> key(1u, 0xABu);
    const std::vector<std::uint8_t> value(2u, 0xCDu);

    mdbxc::sync::OrderedElementStateStore store(
        connection->env_handle(), "ordered_state", "ordered_by_key");
    mdbxc::sync::OrderedElementId first;
    mdbxc::sync::OrderedElementId second;
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        store.initialize(transaction.handle());
        first = store.allocate_id(transaction.handle(), origin);
        second = store.allocate_id(transaction.handle(), origin);
        if (first.sequence != 1u || second.sequence != 2u ||
            first.origin != origin || second.origin != origin) {
            throw std::runtime_error("ordered element counter allocation mismatch");
        }
        store.put_live(transaction.handle(), first, key, value);
        store.put_live(transaction.handle(), second, key, value);
        store.tombstone(transaction.handle(), first);
        transaction.commit();
    }

    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::READ_ONLY);
        mdbxc::sync::OrderedElementStateRecord record;
        if (!store.get(transaction.handle(), first, record) || record.live ||
            !record.key.empty() || !record.value.empty()) {
            throw std::runtime_error("ordered element tombstone did not persist");
        }
        const std::vector<mdbxc::sync::OrderedElementId> live =
            store.live_ids_for_key(transaction.handle(), key);
        if (live.size() != 1u || live[0] != second) {
            throw std::runtime_error("ordered element live index is incorrect");
        }
        transaction.rollback();
    }

    const std::vector<std::uint8_t> logical =
        mdbxc::sync::encode_ordered_element_id_logical(second);
    const std::vector<std::uint8_t> index =
        mdbxc::sync::encode_ordered_element_id_index(second);
    if (mdbxc::sync::decode_ordered_element_id_logical(logical) != second ||
        mdbxc::sync::decode_ordered_element_id_index(index) != second) {
        throw std::runtime_error("ordered element id codec round trip mismatch");
    }

    connection->disconnect();
    cleanup(path);
}

void test_ordered_element_state_requires_initialized_compatible_dbis() {
    const std::string path = "test_ordered_element_state_setup.mdbx";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    mdbxc::sync::OrderedElementStateStore missing(
        connection->env_handle(), "missing_state", "missing_by_key");
    mdbxc::sync::OrderedElementStateRecord record;
    mdbxc::sync::OrderedElementId id;
    id.origin = make_node(0x51u);
    id.sequence = 1u;

    bool rejected_missing = false;
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::READ_ONLY);
        try {
            missing.get(transaction.handle(), id, record);
        } catch (const std::exception&) {
            rejected_missing = true;
        }
        transaction.rollback();
    }
    if (!rejected_missing) {
        throw std::runtime_error("missing ordered state DBI was treated as empty");
    }

    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBX_dbi dbi = 0;
        const int rc = mdbx_dbi_open(
            transaction.handle(), "missing_state",
            static_cast<MDBX_db_flags_t>(0), &dbi);
        if (rc != MDBX_NOTFOUND) {
            throw std::runtime_error(
                "missing ordered state DBI was created by a normal read");
        }
        transaction.rollback();
    }

    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBX_dbi state = 0;
        MDBX_dbi by_key = 0;
        mdbxc::check_mdbx(mdbx_dbi_open(
            transaction.handle(), "wrong_state", MDBX_CREATE | MDBX_DUPSORT,
            &state), "wrong ordered state setup failed");
        mdbxc::check_mdbx(mdbx_dbi_open(
            transaction.handle(), "wrong_by_key", MDBX_CREATE, &by_key),
            "wrong ordered index setup failed");
        transaction.commit();
    }

    mdbxc::sync::OrderedElementStateStore incompatible(
        connection->env_handle(), "wrong_state", "wrong_by_key");
    bool rejected_flags = false;
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        try {
            incompatible.initialize(transaction.handle());
        } catch (const std::exception&) {
            rejected_flags = true;
        }
        transaction.rollback();
    }
    if (!rejected_flags) {
        throw std::runtime_error("incompatible ordered state DBI flags accepted");
    }

    connection->disconnect();
    cleanup(path);
}

void test_ordered_element_state_survives_reopen() {
    const std::string path = "test_ordered_element_state_reopen.mdbx";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const mdbxc::sync::NodeId origin = make_node(0x61u);
    const std::vector<std::uint8_t> key(1u, 0xBAu);
    const std::vector<std::uint8_t> value(1u, 0xDCu);
    mdbxc::sync::OrderedElementId first;
    mdbxc::sync::OrderedElementId second;

    {
        const std::shared_ptr<mdbxc::Connection> connection =
            mdbxc::Connection::create(config);
        mdbxc::sync::OrderedElementStateStore store(
            connection->env_handle(), "reopen_state", "reopen_by_key");
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        store.initialize_empty(transaction.handle());
        first = store.allocate_id(transaction.handle(), origin);
        second = store.allocate_id(transaction.handle(), origin);
        store.put_live(transaction.handle(), first, key, value);
        store.put_live(transaction.handle(), second, key, value);
        store.tombstone(transaction.handle(), first);
        transaction.commit();
        connection->disconnect();
    }

    {
        const std::shared_ptr<mdbxc::Connection> connection =
            mdbxc::Connection::create(config);
        mdbxc::sync::OrderedElementStateStore store(
            connection->env_handle(), "reopen_state", "reopen_by_key");
        {
            mdbxc::Transaction transaction =
                connection->transaction(mdbxc::TransactionMode::READ_ONLY);
            store.verify_existing(transaction.handle());
            mdbxc::sync::OrderedElementStateRecord record;
            if (!store.get(transaction.handle(), first, record) || record.live ||
                !store.get(transaction.handle(), second, record) || !record.live) {
                throw std::runtime_error(
                    "ordered element state did not survive environment reopen");
            }
            transaction.rollback();
        }
        {
            mdbxc::Transaction transaction =
                connection->transaction(mdbxc::TransactionMode::WRITABLE);
            const mdbxc::sync::OrderedElementId third =
                store.allocate_id(transaction.handle(), origin);
            if (third.sequence != 3u) {
                throw std::runtime_error(
                    "ordered element counter did not survive environment reopen");
            }
            transaction.commit();
        }
        connection->disconnect();
    }

    cleanup(path);
}

} // namespace

int main() {
    test_ordered_element_state_persists_live_and_tombstone_records();
    test_ordered_element_state_requires_initialized_compatible_dbis();
    test_ordered_element_state_survives_reopen();
    return 0;
}
