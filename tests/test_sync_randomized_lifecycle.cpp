/// \file test_sync_randomized_lifecycle.cpp
/// \brief Fixed-seed sync lifecycle test for repeated writes, rollbacks,
///        periodic replication, and state equality.

#include <mdbx_containers.hpp>
#include <mdbx_containers/sync.hpp>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>

namespace {

typedef std::map<int, std::string> Model;

void cleanup(const std::string& path) {
    std::remove(path.c_str());
    std::remove((path + "-lck").c_str());
}

std::shared_ptr<mdbxc::Connection> open_env(const std::string& path) {
    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 24;
    config.no_subdir = true;
    return mdbxc::Connection::create(config);
}

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId node{};
    for (int i = 0; i < 16; ++i) {
        node[i] = static_cast<std::uint8_t>(seed + i);
    }
    return node;
}

void require_table_matches_model(const std::shared_ptr<mdbxc::Connection>& conn,
                                 mdbxc::KeyValueTable<int, std::string>& table,
                                 const Model& expected,
                                 const char* label) {
    Model actual;
    auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
    table.load(actual, txn.handle());
    txn.commit();
    if (actual != expected) {
        throw std::runtime_error(std::string(label) + " does not match reference model");
    }
}

void sync_primary_to_replica(mdbxc::sync::SyncEngine& primary_engine,
                             mdbxc::sync::SyncEngine& replica_engine,
                             const std::shared_ptr<mdbxc::Connection>& replica_conn,
                             const mdbxc::sync::NodeId& replica_node,
                             const mdbxc::sync::NodeId& db_id) {
    mdbxc::sync::DirectSyncPeer peer(&primary_engine);
    mdbxc::sync::PullRequest request;
    request.requester = replica_node;
    request.db_id = db_id;
    request.have = replica_engine.applied_cursor();

    bool has_more = false;
    do {
        const mdbxc::sync::SyncCursor before = request.have;
        const mdbxc::sync::PullResponse response = peer.pull(request);
        if (!response.ok) {
            throw std::runtime_error("pull failed: " + response.error);
        }
        if (!response.batches.empty()) {
            auto txn = replica_conn->transaction(mdbxc::TransactionMode::WRITABLE);
            for (std::vector<mdbxc::sync::ChangeBatch>::const_iterator it = response.batches.begin();
                 it != response.batches.end(); ++it) {
                const mdbxc::sync::ApplyResult result =
                    replica_engine.apply_batch(txn.handle(), *it);
                if (result == mdbxc::sync::ApplyResult::Conflict) {
                    throw std::runtime_error("replica apply returned Conflict");
                }
            }
            txn.commit();
        } else if (response.has_more) {
            throw std::runtime_error("pull reported has_more without batches");
        }

        has_more = response.has_more;
        request.have = replica_engine.applied_cursor();
        if (has_more &&
            request.have.last_seq_by_origin == before.last_seq_by_origin) {
            throw std::runtime_error("pull pagination made no cursor progress");
        }
    } while (has_more);
}

void apply_random_operation(const std::shared_ptr<mdbxc::Connection>& conn,
                            mdbxc::KeyValueTable<int, std::string>& table,
                            Model& model,
                            std::mt19937& rng) {
    const int op = static_cast<int>(rng() % 5);
    const int key = static_cast<int>(rng() % 50);
    const std::string value = "v" + std::to_string(rng() % 1000);

    auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
    switch (op) {
    case 0:
        table.insert_or_assign(key, value, txn.handle());
        model[key] = value;
        txn.commit();
        return;
    case 1:
        table.erase(key, txn.handle());
        model.erase(key);
        txn.commit();
        return;
    case 2:
        table.clear(txn.handle());
        model.clear();
        txn.commit();
        return;
    case 3:
        table.insert_or_assign(key, value, txn.handle());
        txn.rollback();
        return;
    default:
        for (int i = 0; i < 3; ++i) {
            const int multi_key = static_cast<int>(rng() % 50);
            const std::string multi_value = "m" + std::to_string(rng() % 1000);
            table.insert_or_assign(multi_key, multi_value, txn.handle());
            model[multi_key] = multi_value;
        }
        txn.commit();
        return;
    }
}

void run_randomized_lifecycle_repro() {
    const std::uint32_t seed = 123456;
    const std::size_t iterations = 200;
    const std::size_t sync_every = 25;

    const std::string primary_path = "test_randomized_lifecycle_primary.mdbx";
    const std::string replica_path = "test_randomized_lifecycle_replica.mdbx";
    cleanup(primary_path);
    cleanup(replica_path);

    std::shared_ptr<mdbxc::Connection> primary_conn = open_env(primary_path);
    std::shared_ptr<mdbxc::Connection> replica_conn = open_env(replica_path);

    const mdbxc::sync::NodeId primary_node = make_node(0xA0);
    const mdbxc::sync::NodeId replica_node = make_node(0xB0);
    const mdbxc::sync::NodeId db_id = make_node(0xD0);

    mdbxc::sync::SyncEngine primary_engine(primary_conn);
    mdbxc::sync::SyncEngine replica_engine(replica_conn);
    primary_engine.initialize_local_identity(primary_node, db_id);
    replica_engine.initialize_local_identity(replica_node, db_id);

    std::unique_ptr<mdbxc::KeyValueTable<int, std::string> > primary_kv;
    std::unique_ptr<mdbxc::KeyValueTable<int, std::string> > replica_kv;
    try {
        primary_kv.reset(new mdbxc::KeyValueTable<int, std::string>(primary_conn, "kv"));
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("primary table open failed: ") + e.what());
    }
    try {
        replica_kv.reset(new mdbxc::KeyValueTable<int, std::string>(replica_conn, "kv"));
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("replica table open failed: ") + e.what());
    }
    Model reference;
    std::mt19937 rng(seed);

    mdbxc::sync::ThreadLocalChangeAccumulator sink(primary_conn);
    primary_conn->attach_sync_capture(&sink);

    for (std::size_t i = 0; i < iterations; ++i) {
        apply_random_operation(primary_conn, *primary_kv, reference, rng);
        if ((i + 1) % sync_every == 0) {
            require_table_matches_model(primary_conn, *primary_kv, reference, "primary");
            sync_primary_to_replica(primary_engine, replica_engine, replica_conn,
                                    replica_node, db_id);
            require_table_matches_model(replica_conn, *replica_kv, reference, "replica");
        }
    }

    primary_conn->detach_sync_capture();
    sync_primary_to_replica(primary_engine, replica_engine, replica_conn,
                            replica_node, db_id);
    require_table_matches_model(primary_conn, *primary_kv, reference, "primary final");
    require_table_matches_model(replica_conn, *replica_kv, reference, "replica final");

    primary_kv.reset();
    replica_kv.reset();

    primary_conn->disconnect();
    replica_conn->disconnect();

    std::shared_ptr<mdbxc::Connection> restarted_primary = open_env(primary_path);
    std::shared_ptr<mdbxc::Connection> restarted_replica = open_env(replica_path);
    std::unique_ptr<mdbxc::KeyValueTable<int, std::string> > restarted_primary_kv;
    std::unique_ptr<mdbxc::KeyValueTable<int, std::string> > restarted_replica_kv;
    try {
        restarted_primary_kv.reset(
            new mdbxc::KeyValueTable<int, std::string>(restarted_primary, "kv"));
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("restarted primary table open failed: ") +
                                 e.what());
    }
    try {
        restarted_replica_kv.reset(
            new mdbxc::KeyValueTable<int, std::string>(restarted_replica, "kv"));
    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("restarted replica table open failed: ") +
                                 e.what());
    }
    require_table_matches_model(restarted_primary, *restarted_primary_kv,
                                reference, "restarted primary");
    require_table_matches_model(restarted_replica, *restarted_replica_kv,
                                reference, "restarted replica");
    restarted_primary_kv.reset();
    restarted_replica_kv.reset();
    restarted_primary->disconnect();
    restarted_replica->disconnect();

    cleanup(primary_path);
    cleanup(replica_path);
}

} // namespace

int main() {
    try {
        run_randomized_lifecycle_repro();
    } catch (const std::exception& e) {
        std::printf("FAIL test_randomized_lifecycle: %s\n", e.what());
        return 1;
    } catch (...) {
        std::printf("FAIL test_randomized_lifecycle: non-std exception\n");
        return 1;
    }

    std::printf("PASS test_randomized_lifecycle\n");
    return 0;
}
