#include <mdbx_containers/sync.hpp>

#include <cstdio>
#include <cstdint>
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

MDBX_val make_raw_val(const std::vector<std::uint8_t>& bytes) {
    MDBX_val value = {
        const_cast<std::uint8_t*>(bytes.empty() ? nullptr : &bytes[0]),
        bytes.size()
    };
    return value;
}

std::vector<std::uint8_t> make_introduced_key(
        const mdbxc::sync::NodeId& origin) {
    std::vector<std::uint8_t> key(1u, 0x02u);
    key.insert(key.end(), origin.begin(), origin.end());
    return key;
}

std::vector<std::uint8_t> make_element_key(
        const mdbxc::sync::OrderedElementId& id) {
    std::vector<std::uint8_t> key(1u, 0x01u);
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::encode_ordered_element_id_index(id);
    key.insert(key.end(), encoded.begin(), encoded.end());
    return key;
}

void write_raw_state_record(
        const std::shared_ptr<mdbxc::Connection>& connection,
        const std::string& state_name,
        const std::vector<std::uint8_t>& key,
        const std::vector<std::uint8_t>& value) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::WRITABLE);
    MDBX_dbi state = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(
        transaction.handle(), state_name.c_str(),
        static_cast<MDBX_db_flags_t>(0), &state),
        "test state DBI open failed");
    MDBX_val raw_key = make_raw_val(key);
    MDBX_val raw_value = make_raw_val(value);
    mdbxc::check_mdbx(mdbx_put(transaction.handle(), state, &raw_key,
                                &raw_value, MDBX_UPSERT),
                      "test state record write failed");
    transaction.commit();
}

void write_raw_introduced_high_water(
        const std::shared_ptr<mdbxc::Connection>& connection,
        const std::string& state_name,
        const mdbxc::sync::NodeId& origin,
        std::uint64_t sequence) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::WRITABLE);
    MDBX_dbi state = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(
        transaction.handle(), state_name.c_str(),
        static_cast<MDBX_db_flags_t>(0), &state),
        "test state DBI open failed");
    const std::vector<std::uint8_t> key = make_introduced_key(origin);
    std::vector<std::uint8_t> value;
    mdbxc::sync::detail::append_u64_le(value, sequence);
    MDBX_val raw_key = make_raw_val(key);
    MDBX_val raw_value = make_raw_val(value);
    mdbxc::check_mdbx(mdbx_put(transaction.handle(), state, &raw_key,
                                &raw_value, MDBX_UPSERT),
                      "test introduced high-water write failed");
    transaction.commit();
}

void delete_raw_introduced_high_water(
        const std::shared_ptr<mdbxc::Connection>& connection,
        const std::string& state_name,
        const mdbxc::sync::NodeId& origin) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::WRITABLE);
    MDBX_dbi state = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(
        transaction.handle(), state_name.c_str(),
        static_cast<MDBX_db_flags_t>(0), &state),
        "test state DBI open failed");
    const std::vector<std::uint8_t> key = make_introduced_key(origin);
    MDBX_val raw_key = make_raw_val(key);
    mdbxc::check_mdbx(mdbx_del(transaction.handle(), state, &raw_key, nullptr),
                      "test introduced high-water delete failed");
    transaction.commit();
}

void require_high_water_rejected(
        mdbxc::sync::OrderedElementStateStore& store,
        const std::shared_ptr<mdbxc::Connection>& connection,
        const mdbxc::sync::NodeId& origin) {
    bool verification_rejected = false;
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::READ_ONLY);
        try {
            store.verify_existing(transaction.handle());
        } catch (const std::exception&) {
            verification_rejected = true;
        }
        transaction.rollback();
    }
    if (!verification_rejected) {
        throw std::runtime_error("corrupt introduced high-water passed verification");
    }

    bool allocation_rejected = false;
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        try {
            store.allocate_id(transaction.handle(), origin);
        } catch (const std::exception&) {
            allocation_rejected = true;
        }
        transaction.rollback();
    }
    if (!allocation_rejected) {
        throw std::runtime_error("corrupt introduced high-water allowed allocation");
    }
}

void require_origin_scan_rejected(
        mdbxc::sync::OrderedElementStateStore& store,
        const std::shared_ptr<mdbxc::Connection>& connection,
        const mdbxc::sync::NodeId& origin) {
    bool rejected = false;
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::READ_ONLY);
    try {
        store.verify_introduced_high_water(transaction.handle(), origin);
    } catch (const std::exception&) {
        rejected = true;
    }
    transaction.rollback();
    if (!rejected) {
        throw std::runtime_error("corrupt ordered element origin scan passed");
    }
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

void test_ordered_element_state_rejects_corrupt_introduced_high_water() {
    const std::string path = "test_ordered_element_state_high_water.mdbx";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0x71u);
    const std::vector<std::uint8_t> key(1u, 0xAAu);
    const std::vector<std::uint8_t> value(1u, 0xBBu);
    const std::string state_name = "high_water_state";
    mdbxc::sync::OrderedElementStateStore store(
        connection->env_handle(), state_name, "high_water_by_key");
    mdbxc::sync::OrderedElementId id;
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        store.initialize_empty(transaction.handle());
        id = store.allocate_id(transaction.handle(), origin);
        store.put_live(transaction.handle(), id, key, value);
        transaction.commit();
    }

    delete_raw_introduced_high_water(connection, state_name, origin);
    require_high_water_rejected(store, connection, origin);

    write_raw_introduced_high_water(connection, state_name, origin, id.sequence);
    write_raw_introduced_high_water(connection, state_name, origin, id.sequence - 1u);
    require_high_water_rejected(store, connection, origin);

    write_raw_introduced_high_water(connection, state_name, origin, id.sequence);
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        store.tombstone(transaction.handle(), id);
        transaction.commit();
    }
    write_raw_introduced_high_water(connection, state_name, origin, id.sequence - 1u);
    require_high_water_rejected(store, connection, origin);

    connection->disconnect();
    cleanup(path);
}

void test_ordered_element_state_rejects_second_origin_high_water_corruption() {
    const std::string path = "test_ordered_element_state_second_origin_high_water.mdbx";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId first_origin = make_node(0x72u);
    const mdbxc::sync::NodeId second_origin = make_node(0x82u);
    const std::vector<std::uint8_t> key(1u, 0xAAu);
    const std::vector<std::uint8_t> value(1u, 0xBBu);
    const std::string state_name = "second_origin_high_water_state";
    mdbxc::sync::OrderedElementStateStore store(
        connection->env_handle(), state_name, "second_origin_high_water_by_key");
    mdbxc::sync::OrderedElementId second_id;
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        store.initialize_empty(transaction.handle());
        const mdbxc::sync::OrderedElementId first_id =
            store.allocate_id(transaction.handle(), first_origin);
        second_id = store.allocate_id(transaction.handle(), second_origin);
        store.put_live(transaction.handle(), first_id, key, value);
        store.put_live(transaction.handle(), second_id, key, value);
        transaction.commit();
    }

    delete_raw_introduced_high_water(connection, state_name, second_origin);
    require_high_water_rejected(store, connection, second_origin);

    write_raw_introduced_high_water(
        connection, state_name, second_origin, second_id.sequence);
    write_raw_introduced_high_water(
        connection, state_name, second_origin, second_id.sequence - 1u);
    require_high_water_rejected(store, connection, second_origin);

    connection->disconnect();
    cleanup(path);
}

void test_ordered_element_state_rejects_malformed_records_in_origin_scan() {
    const std::string path = "test_ordered_element_state_malformed_origin_scan.mdbx";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId malformed_key_origin = make_node(0x91u);
    const mdbxc::sync::NodeId malformed_value_origin = make_node(0xA1u);
    const std::string state_name = "malformed_origin_scan_state";
    mdbxc::sync::OrderedElementStateStore store(
        connection->env_handle(), state_name, "malformed_origin_scan_by_key");
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        store.initialize_empty(transaction.handle());
        transaction.commit();
    }

    std::vector<std::uint8_t> malformed_key(1u, 0x01u);
    malformed_key.insert(malformed_key.end(), malformed_key_origin.begin(),
                         malformed_key_origin.end());
    write_raw_state_record(
        connection, state_name, malformed_key, std::vector<std::uint8_t>(1u, 0x02u));
    require_origin_scan_rejected(store, connection, malformed_key_origin);

    mdbxc::sync::OrderedElementId malformed_value_id;
    malformed_value_id.origin = malformed_value_origin;
    malformed_value_id.sequence = 1u;
    write_raw_state_record(connection, state_name,
                           make_element_key(malformed_value_id),
                           std::vector<std::uint8_t>(1u, 0x7Fu));
    require_origin_scan_rejected(store, connection, malformed_value_origin);

    connection->disconnect();
    cleanup(path);
}

} // namespace

int main() {
    test_ordered_element_state_persists_live_and_tombstone_records();
    test_ordered_element_state_requires_initialized_compatible_dbis();
    test_ordered_element_state_survives_reopen();
    test_ordered_element_state_rejects_corrupt_introduced_high_water();
    test_ordered_element_state_rejects_second_origin_high_water_corruption();
    test_ordered_element_state_rejects_malformed_records_in_origin_scan();
    return 0;
}
