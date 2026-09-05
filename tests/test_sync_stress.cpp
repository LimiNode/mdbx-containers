/// \file test_sync_stress.cpp
/// \brief Longer fixed-seed sync stress test with multi-origin pagination and restarts.

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
#include <vector>

namespace {

typedef std::map<int, std::string> Model;

struct StressConfig {
    std::size_t operations;
    std::size_t sync_every;
    std::size_t restart_every;
    std::uint64_t max_batches_per_pull;
    std::uint64_t max_bytes_per_pull;
    std::size_t remote_origin_count;
};

struct RemoteOriginState {
    mdbxc::sync::NodeId node_id;
    std::uint64_t seq;
    int key_base;
};

struct ModelMutation {
    bool erase;
    int key;
    std::string value;
};

StressConfig default_stress_config() {
    StressConfig config;
    config.operations = 20000;
    config.sync_every = 200;
    config.restart_every = 5000;
    config.max_batches_per_pull = 17;
    config.max_bytes_per_pull = 512;
    config.remote_origin_count = 3;
    return config;
}

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

std::vector<RemoteOriginState> make_remote_origins(std::size_t count) {
    std::vector<RemoteOriginState> origins;
    origins.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        RemoteOriginState origin;
        origin.node_id = make_node(static_cast<std::uint8_t>(0x10 + i * 0x10));
        origin.seq = 0;
        origin.key_base = 10000 + static_cast<int>(i) * 10000;
        origins.push_back(origin);
    }
    return origins;
}

void assign_bytes(std::vector<std::uint8_t>& out, const MDBX_val& val) {
    if (val.iov_len == 0) {
        out.clear();
        return;
    }
    const std::uint8_t* begin = static_cast<const std::uint8_t*>(val.iov_base);
    out.assign(begin, begin + val.iov_len);
}

void append_put_op(mdbxc::sync::ChangeBatch& batch,
                   int key,
                   const std::string& value) {
    mdbxc::SerializeScratch key_scratch;
    mdbxc::SerializeScratch value_scratch;
    const MDBX_val db_key = mdbxc::serialize_key<true>(key, key_scratch);
    const MDBX_val db_value = mdbxc::serialize_value(value, value_scratch);

    mdbxc::sync::ChangeOp op;
    op.op_type = mdbxc::sync::ChangeOpType::Put;
    op.dbi_flags = static_cast<std::uint32_t>(MDBX_INTEGERKEY);
    op.dbi_name = "kv";
    assign_bytes(op.storage_key, db_key);
    assign_bytes(op.value, db_value);
    batch.ops.push_back(op);
}

void append_delete_op(mdbxc::sync::ChangeBatch& batch, int key) {
    mdbxc::SerializeScratch key_scratch;
    const MDBX_val db_key = mdbxc::serialize_key<true>(key, key_scratch);

    mdbxc::sync::ChangeOp op;
    op.op_type = mdbxc::sync::ChangeOpType::Delete;
    op.dbi_flags = static_cast<std::uint32_t>(MDBX_INTEGERKEY);
    op.dbi_name = "kv";
    assign_bytes(op.storage_key, db_key);
    batch.ops.push_back(op);
}

void append_remote_random_op(mdbxc::sync::ChangeBatch& batch,
                             std::vector<ModelMutation>& mutations,
                             const RemoteOriginState& origin,
                             std::mt19937& rng) {
    const int key = origin.key_base + static_cast<int>(rng() % 512);
    if ((rng() % 4) == 0) {
        append_delete_op(batch, key);
        ModelMutation mutation;
        mutation.erase = true;
        mutation.key = key;
        mutations.push_back(mutation);
        return;
    }

    const std::string value =
        "r" + std::to_string(origin.key_base) + "_" + std::to_string(rng() % 100000);
    append_put_op(batch, key, value);
    ModelMutation mutation;
    mutation.erase = false;
    mutation.key = key;
    mutation.value = value;
    mutations.push_back(mutation);
}

mdbxc::sync::ChangeBatch make_remote_batch(RemoteOriginState& origin,
                                           std::vector<ModelMutation>& mutations,
                                           std::mt19937& rng) {
    ++origin.seq;

    mdbxc::sync::ChangeBatch batch;
    batch.origin_node_id = origin.node_id;
    batch.seq = origin.seq;
    batch.time_unix_ns = origin.seq;

    const int op = static_cast<int>(rng() % 6);
    if (op < 5) {
        append_remote_random_op(batch, mutations, origin, rng);
    } else {
        for (int i = 0; i < 4; ++i) {
            append_remote_random_op(batch, mutations, origin, rng);
        }
    }
    return batch;
}

void apply_mutations(Model& model, const std::vector<ModelMutation>& mutations) {
    for (std::vector<ModelMutation>::const_iterator it = mutations.begin();
         it != mutations.end(); ++it) {
        if (it->erase) {
            model.erase(it->key);
        } else {
            model[it->key] = it->value;
        }
    }
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

class StressHarness {
public:
    StressHarness(std::uint32_t seed,
                  const mdbxc::sync::NodeId& primary_node,
                  const mdbxc::sync::NodeId& replica_node,
                  const mdbxc::sync::NodeId& db_id)
        : m_primary_path("test_sync_stress_primary_" + std::to_string(seed) + ".mdbx"),
          m_replica_path("test_sync_stress_replica_" + std::to_string(seed) + ".mdbx"),
          m_primary_node(primary_node),
          m_replica_node(replica_node),
          m_db_id(db_id),
          m_capture_attached(false) {}

    ~StressHarness() {
        try {
            close();
        } catch (...) {}
        cleanup(m_primary_path);
        cleanup(m_replica_path);
    }

    void reset_files() {
        cleanup(m_primary_path);
        cleanup(m_replica_path);
    }

    void open() {
        m_primary_conn = open_env(m_primary_path);
        m_replica_conn = open_env(m_replica_path);

        m_primary_engine.reset(new mdbxc::sync::SyncEngine(m_primary_conn));
        m_replica_engine.reset(new mdbxc::sync::SyncEngine(m_replica_conn));
        m_primary_engine->initialize_local_identity(m_primary_node, m_db_id);
        m_replica_engine->initialize_local_identity(m_replica_node, m_db_id);

        m_primary_kv.reset(
            new mdbxc::KeyValueTable<int, std::string>(m_primary_conn, "kv"));
        m_replica_kv.reset(
            new mdbxc::KeyValueTable<int, std::string>(m_replica_conn, "kv"));

        m_sink.reset(new mdbxc::sync::ThreadLocalChangeAccumulator(m_primary_conn));
        m_primary_conn->attach_sync_capture(m_sink.get());
        m_capture_attached = true;
    }

    void close() {
        if (m_capture_attached && m_primary_conn) {
            m_primary_conn->detach_sync_capture();
            m_capture_attached = false;
        }
        m_sink.reset();
        m_primary_kv.reset();
        m_replica_kv.reset();
        m_primary_engine.reset();
        m_replica_engine.reset();
        if (m_primary_conn) {
            m_primary_conn->disconnect();
            m_primary_conn.reset();
        }
        if (m_replica_conn) {
            m_replica_conn->disconnect();
            m_replica_conn.reset();
        }
    }

    void restart() {
        close();
        open();
    }

    void require_primary_matches(const Model& expected, const char* label) {
        require_table_matches_model(m_primary_conn, *m_primary_kv, expected, label);
    }

    void require_replica_matches(const Model& expected, const char* label) {
        require_table_matches_model(m_replica_conn, *m_replica_kv, expected, label);
    }

    void require_origin_index_valid(const char* label) const {
        auto txn = m_primary_conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        mdbxc::sync::ChangeLogStore log(m_primary_conn->env_handle());
        log.open(txn.handle());
        if (!log.origin_index_matches_changelog(txn.handle())) {
            throw std::runtime_error(std::string(label) + " origin index mismatch");
        }
        txn.commit();
    }

    void apply_remote_batch(const mdbxc::sync::ChangeBatch& batch) {
        auto txn = m_primary_conn->transaction(mdbxc::TransactionMode::WRITABLE);
        const mdbxc::sync::ApplyResult result =
            m_primary_engine->apply_batch(txn.handle(), batch);
        if (result != mdbxc::sync::ApplyResult::Applied) {
            throw std::runtime_error("stress primary remote apply did not apply");
        }

        mdbxc::sync::ChangeLogStore log(m_primary_conn->env_handle());
        log.open(txn.handle());
        const std::vector<std::uint8_t> encoded =
            mdbxc::sync::ChangeBatchCodec::encode(batch);
        log.append(txn.handle(), batch.origin_node_id, batch.seq, encoded);
        txn.commit();
    }

    void sync_to_replica(std::uint64_t max_batches, std::uint64_t max_bytes) {
        mdbxc::sync::DirectSyncPeer peer(m_primary_engine.get());
        mdbxc::sync::PullRequest request;
        request.requester = m_replica_node;
        request.db_id = m_db_id;
        request.have = m_replica_engine->applied_cursor();
        request.max_batches = max_batches;
        request.max_bytes = max_bytes;

        bool has_more = false;
        do {
            const mdbxc::sync::SyncCursor before = request.have;
            const mdbxc::sync::PullResponse response = peer.pull(request);
            if (!response.ok) {
                throw std::runtime_error("stress pull failed: " + response.error);
            }
            if (!response.batches.empty()) {
                mdbxc::sync::PushRequest push;
                push.sender = m_primary_node;
                push.db_id = m_db_id;
                push.batches = response.batches;
                const mdbxc::sync::PushResponse applied =
                    m_replica_engine->handle_push(push);
                if (!applied.ok) {
                    throw std::runtime_error("stress replica push failed: " + applied.error);
                }
                request.have = applied.receiver_have;
            } else if (response.has_more) {
                throw std::runtime_error("stress pull reported has_more without batches");
            } else {
                request.have = m_replica_engine->applied_cursor();
            }

            has_more = response.has_more;
            if (has_more &&
                request.have.last_seq_by_origin == before.last_seq_by_origin) {
                throw std::runtime_error("stress pull pagination made no cursor progress");
            }
        } while (has_more);
    }

    mdbxc::sync::SyncCursor replica_cursor() const {
        return m_replica_engine->applied_cursor();
    }

    std::shared_ptr<mdbxc::Connection> primary_conn() const {
        return m_primary_conn;
    }

    mdbxc::KeyValueTable<int, std::string>& primary_table() {
        return *m_primary_kv;
    }

private:
    std::string m_primary_path;
    std::string m_replica_path;
    mdbxc::sync::NodeId m_primary_node;
    mdbxc::sync::NodeId m_replica_node;
    mdbxc::sync::NodeId m_db_id;
    std::shared_ptr<mdbxc::Connection> m_primary_conn;
    std::shared_ptr<mdbxc::Connection> m_replica_conn;
    std::unique_ptr<mdbxc::sync::SyncEngine> m_primary_engine;
    std::unique_ptr<mdbxc::sync::SyncEngine> m_replica_engine;
    std::unique_ptr<mdbxc::KeyValueTable<int, std::string> > m_primary_kv;
    std::unique_ptr<mdbxc::KeyValueTable<int, std::string> > m_replica_kv;
    std::unique_ptr<mdbxc::sync::ThreadLocalChangeAccumulator> m_sink;
    bool m_capture_attached;
};

void require_remote_cursors(const StressHarness& harness,
                            const std::vector<RemoteOriginState>& origins,
                            const char* label) {
    const mdbxc::sync::SyncCursor cursor = harness.replica_cursor();
    for (std::vector<RemoteOriginState>::const_iterator it = origins.begin();
         it != origins.end(); ++it) {
        if (cursor.last_seq_for(it->node_id) != it->seq) {
            throw std::runtime_error(std::string(label) + " remote cursor mismatch");
        }
    }
}

void apply_local_random_operation(StressHarness& harness,
                                  Model& model,
                                  std::mt19937& rng) {
    const int op = static_cast<int>(rng() % 8);
    const int key = static_cast<int>(rng() % 512);
    const std::string value = "v" + std::to_string(rng() % 100000);

    auto txn = harness.primary_conn()->transaction(mdbxc::TransactionMode::WRITABLE);
    switch (op) {
    case 0:
    case 1:
    case 2:
        harness.primary_table().insert_or_assign(key, value, txn.handle());
        model[key] = value;
        txn.commit();
        return;
    case 3:
    case 4:
        harness.primary_table().erase(key, txn.handle());
        model.erase(key);
        txn.commit();
        return;
    case 5:
        harness.primary_table().insert_or_assign(key, value, txn.handle());
        txn.rollback();
        return;
    default:
        for (int i = 0; i < 4; ++i) {
            const int multi_key = static_cast<int>(rng() % 512);
            const std::string multi_value = "m" + std::to_string(rng() % 100000);
            harness.primary_table().insert_or_assign(multi_key, multi_value, txn.handle());
            model[multi_key] = multi_value;
        }
        txn.commit();
        return;
    }
}

void apply_remote_random_operation(StressHarness& harness,
                                   Model& model,
                                   std::vector<RemoteOriginState>& origins,
                                   std::mt19937& rng) {
    const std::size_t index = static_cast<std::size_t>(rng() % origins.size());
    std::vector<ModelMutation> mutations;
    const mdbxc::sync::ChangeBatch batch =
        make_remote_batch(origins[index], mutations, rng);
    harness.apply_remote_batch(batch);
    apply_mutations(model, mutations);
}

void run_seed(std::uint32_t seed) {
    const StressConfig config = default_stress_config();

    StressHarness harness(seed, make_node(0xA0), make_node(0xB0), make_node(0xD0));
    harness.reset_files();
    harness.open();

    Model reference;
    std::vector<RemoteOriginState> remote_origins =
        make_remote_origins(config.remote_origin_count);
    std::mt19937 rng(seed);
    for (std::size_t i = 0; i < config.operations; ++i) {
        if ((rng() % 3) == 0) {
            apply_remote_random_operation(harness, reference, remote_origins, rng);
        } else {
            apply_local_random_operation(harness, reference, rng);
        }

        if ((i + 1) % config.restart_every == 0) {
            harness.restart();
            harness.require_primary_matches(reference, "stress primary after restart");
            harness.require_origin_index_valid("stress primary after restart");
            harness.sync_to_replica(config.max_batches_per_pull, config.max_bytes_per_pull);
            harness.require_replica_matches(reference, "stress replica after restart sync");
            require_remote_cursors(harness, remote_origins,
                                   "stress replica after restart sync");
        } else if ((i + 1) % config.sync_every == 0) {
            harness.require_primary_matches(reference, "stress primary");
            harness.sync_to_replica(config.max_batches_per_pull, config.max_bytes_per_pull);
            harness.require_replica_matches(reference, "stress replica");
            require_remote_cursors(harness, remote_origins, "stress replica");
        }
    }

    harness.require_primary_matches(reference, "stress primary final");
    harness.require_origin_index_valid("stress primary final");
    harness.sync_to_replica(config.max_batches_per_pull, config.max_bytes_per_pull);
    harness.require_replica_matches(reference, "stress replica final");
    require_remote_cursors(harness, remote_origins, "stress replica final");
    harness.close();
    harness.reset_files();
}

void run_stress() {
    const std::uint32_t seeds[] = { 123456u, 987654u, 424242u };
    for (std::size_t i = 0; i < sizeof(seeds) / sizeof(seeds[0]); ++i) {
        run_seed(seeds[i]);
    }
}

} // namespace

int main() {
    try {
        run_stress();
    } catch (const std::exception& e) {
        std::printf("FAIL test_sync_stress: %s\n", e.what());
        return 1;
    } catch (...) {
        std::printf("FAIL test_sync_stress: non-std exception\n");
        return 1;
    }

    std::printf("PASS test_sync_stress\n");
    return 0;
}
