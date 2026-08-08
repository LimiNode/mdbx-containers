#include <mdbx_containers.hpp>
#include <mdbx_containers/sync.hpp>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

void cleanup(const std::string& p) {
    std::remove(p.c_str());
}

template<class KVT>
typename KVT::value_type::second_type kv_or_throw(const std::shared_ptr<mdbxc::Connection>& conn,
        KVT& kv, const typename KVT::value_type::first_type& key, const char* what) {
    typename KVT::value_type::second_type out{};
    auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
    if (!kv.try_get(key, out, txn.handle())) {
        throw std::runtime_error(std::string("missing: ") + what);
    }
    return out;
}

template<class KVT>
bool kv_has(const std::shared_ptr<mdbxc::Connection>& conn,
        KVT& kv, const typename KVT::value_type::first_type& key) {
    typename KVT::value_type::second_type out{};
    auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
    return kv.try_get(key, out, txn.handle());
}

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId n{};
    for (int i = 0; i < 16; ++i) {
        n[i] = static_cast<std::uint8_t>(seed + i);
    }
    return n;
}

std::shared_ptr<mdbxc::Connection> open_env(const std::string& path) {
    using namespace mdbxc;
    Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    return Connection::create(cfg);
}

mdbxc::sync::FullSnapshotExportOptions complete_snapshot_test_options() {
    mdbxc::sync::FullSnapshotExportOptions options;
    options.replacement_scope =
        mdbxc::sync::FullSnapshotScope::CompleteUserDatabase;
    options.max_materialized_operations = 16u;
    options.max_materialized_bytes = 4096u;
    options.max_active_sessions = 1u;
    return options;
}

void require_logical_snapshot_rejected(
        const mdbxc::sync::PullResponse& response,
        const char* context) {
    if (response.ok || response.is_full_snapshot ||
        !response.snapshot_chunk.snapshot_id.empty() ||
        response.error_code !=
            mdbxc::sync::SyncResponseErrorCode::SnapshotLogicalStateUnsupported ||
        response.error_retryable) {
        throw std::runtime_error(
            std::string("complete snapshot accepted ") + context);
    }
}

mdbxc::sync::PullResponse request_complete_snapshot(
        mdbxc::sync::SyncEngine& engine,
        const mdbxc::sync::DbId& db_id,
        const mdbxc::sync::NodeId& requester) {
    mdbxc::sync::PullRequest request;
    request.requester = requester;
    request.db_id = db_id;
    request.request_full_snapshot = true;
    request.max_bytes = 8192u;
    request.max_single_batch_bytes = 8192u;
    return engine.handle_pull(request);
}

class CountingApplyObserver : public mdbxc::sync::ISyncApplyObserver {
public:
    CountingApplyObserver()
        : calls(0), generation(0), applied_batches(0), applied_ops(0) {}

    void on_sync_apply_committed(
        const mdbxc::sync::SyncApplyEvent& event) override {
        ++calls;
        generation = event.generation;
        applied_batches = event.applied_batches;
        applied_ops = event.applied_ops;
        affected_dbi_names = event.affected_dbi_names;
    }

    std::size_t calls;
    std::uint64_t generation;
    std::size_t applied_batches;
    std::size_t applied_ops;
    std::vector<std::string> affected_dbi_names;
};

class ThrowingApplyObserver : public mdbxc::sync::ISyncApplyObserver {
public:
    void on_sync_apply_committed(
        const mdbxc::sync::SyncApplyEvent&) override {
        throw std::runtime_error("observer failure");
    }
};

class ReentrantVectorApplyObserver : public mdbxc::sync::ISyncApplyObserver {
public:
    explicit ReentrantVectorApplyObserver(mdbxc::VectorStore* store)
        : m_store(store), calls(0), last_count(0) {}

    void on_sync_apply_committed(
        const mdbxc::sync::SyncApplyEvent&) override {
        ++calls;
        last_count = m_store->count();
    }

    mdbxc::VectorStore* m_store;
    std::size_t calls;
    std::size_t last_count;
};

class BlockingApplyObserver : public mdbxc::sync::ISyncApplyObserver {
public:
    BlockingApplyObserver() : m_entered(false), m_release(false), calls(0) {}

    void on_sync_apply_committed(
        const mdbxc::sync::SyncApplyEvent&) override {
        std::unique_lock<std::mutex> lock(m_mutex);
        ++calls;
        m_entered = true;
        m_cv.notify_all();
        while (!m_release) {
            m_cv.wait(lock);
        }
    }

    bool wait_until_entered(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_cv.wait_for(lock, timeout,
            [this]() { return m_entered; });
    }

    void release_callback() {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_release = true;
        m_cv.notify_all();
    }

    std::size_t call_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return calls;
    }

private:
    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
    bool m_entered;
    bool m_release;
    std::size_t calls;
};

template<class Fn>
void expect_invalid_argument(const char* name, Fn fn) {
    bool rejected = false;
    try {
        fn();
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    if (!rejected) {
        throw std::runtime_error(
            std::string(name) + " did not throw std::invalid_argument");
    }
}

void seed_node_id(std::shared_ptr<mdbxc::Connection> conn,
                  const mdbxc::sync::NodeId& node_id) {
    using namespace mdbxc;
    sync::MetaStore meta(conn->env_handle());
    auto txn = conn->transaction(TransactionMode::WRITABLE);
    meta.open(txn.handle());
    meta.set_node_id(txn.handle(), node_id);
    txn.commit();
}

void assign_bytes(std::vector<std::uint8_t>& out, const MDBX_val& val) {
    out.clear();
    if (val.iov_len == 0) {
        return;
    }
    const std::uint8_t* begin = static_cast<const std::uint8_t*>(val.iov_base);
    out.assign(begin, begin + val.iov_len);
}

mdbxc::sync::ChangeBatch make_raw_batch(const mdbxc::sync::NodeId& origin,
                                        std::uint64_t seq,
                                        const std::string& dbi_name,
                                        std::uint8_t key_seed) {
    mdbxc::sync::ChangeBatch batch;
    batch.origin_node_id = origin;
    batch.seq = seq;
    mdbxc::sync::ChangeOp op;
    op.op_type = mdbxc::sync::ChangeOpType::Put;
    op.dbi_name = dbi_name;
    op.storage_key = { key_seed, static_cast<std::uint8_t>(seq & 0xff) };
    op.value = { static_cast<std::uint8_t>(0x80u | key_seed),
                 static_cast<std::uint8_t>(seq & 0xff) };
    batch.ops.push_back(op);
    return batch;
}

mdbxc::sync::ChangeBatch make_raw_batch_with_value_size(
        const mdbxc::sync::NodeId& origin,
        std::uint64_t seq,
        const std::string& dbi_name,
        std::uint8_t key_seed,
        std::size_t value_size) {
    mdbxc::sync::ChangeBatch batch =
        make_raw_batch(origin, seq, dbi_name, key_seed);
    batch.ops[0].value.assign(value_size, key_seed);
    return batch;
}

std::vector<std::uint8_t> make_changelog_key(const mdbxc::sync::NodeId& origin,
                                             std::uint64_t seq) {
    std::vector<std::uint8_t> out(24);
    std::memcpy(out.data(), origin.data(), 16);
    for (int i = 0; i < 8; ++i) {
        out[16 + i] =
            static_cast<std::uint8_t>((seq >> ((7 - i) * 8)) & 0xff);
    }
    return out;
}

void put_raw_changelog(MDBX_txn* txn,
                       MDBX_dbi dbi,
                       const mdbxc::sync::NodeId& origin,
                       std::uint64_t seq,
                       const std::vector<std::uint8_t>& bytes) {
    std::vector<std::uint8_t> key = make_changelog_key(origin, seq);
    MDBX_val k = { key.empty() ? nullptr : &key[0], key.size() };
    MDBX_val v = { bytes.empty() ? nullptr : const_cast<std::uint8_t*>(&bytes[0]),
                   bytes.size() };
    mdbxc::check_mdbx(mdbx_put(txn, dbi, &k, &v, MDBX_NOOVERWRITE),
                      "raw changelog put failed");
}

void append_raw_batch(mdbxc::sync::ChangeLogStore& log,
                      MDBX_txn* txn,
                      const mdbxc::sync::NodeId& origin,
                      std::uint64_t seq,
                      const std::string& dbi_name,
                      std::uint8_t key_seed) {
    const mdbxc::sync::ChangeBatch batch = make_raw_batch(origin, seq, dbi_name, key_seed);
    const std::vector<std::uint8_t> bytes = mdbxc::sync::ChangeBatchCodec::encode(batch);
    log.append(txn, origin, seq, bytes);
}

void append_raw_batch(mdbxc::sync::ChangeLogStore& log,
                      MDBX_txn* txn,
                      const mdbxc::sync::ChangeBatch& batch) {
    const std::vector<std::uint8_t> bytes =
        mdbxc::sync::ChangeBatchCodec::encode(batch);
    log.append(txn, batch.origin_node_id, batch.seq, bytes);
}

void append_raw_bytes(mdbxc::sync::ChangeLogStore& log,
                      MDBX_txn* txn,
                      const mdbxc::sync::NodeId& origin,
                      std::uint64_t seq,
                      const std::vector<std::uint8_t>& bytes) {
    log.append(txn, origin, seq, bytes);
}

const char* op_type_name(mdbxc::sync::ChangeOpType type) {
    switch (type) {
        case mdbxc::sync::ChangeOpType::Put:
            return "Put";
        case mdbxc::sync::ChangeOpType::Delete:
            return "Delete";
        case mdbxc::sync::ChangeOpType::ClearTable:
            return "ClearTable";
    }
    return "unknown";
}

void assign_int_key(std::vector<std::uint8_t>& out, int key) {
    mdbxc::SerializeScratch scratch;
    const MDBX_val serialized = mdbxc::serialize_key<true>(key, scratch);
    assign_bytes(out, serialized);
}

void assign_int_value(std::vector<std::uint8_t>& out, int value) {
    mdbxc::SerializeScratch scratch;
    const MDBX_val serialized = mdbxc::serialize_value(value, scratch);
    assign_bytes(out, serialized);
}

void test_engine_round_trip_kv() {
    using namespace mdbxc;
    const std::string primary_path = "test_engine_primary.mdbx";
    const std::string replica_path = "test_engine_replica.mdbx";
    cleanup(primary_path);
    cleanup(replica_path);

    auto primary_conn   = open_env(primary_path);
    auto replica_conn   = open_env(replica_path);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_uuid      = make_node(0xD0);

    sync::SyncEngine primary_engine(primary_conn);
    sync::SyncEngine replica_engine(replica_conn);
    primary_engine.initialize_local_identity(primary_node, db_uuid);
    replica_engine.initialize_local_identity(replica_node, db_uuid);

    sync::ThreadLocalChangeAccumulator primary_sink(primary_conn);
    primary_conn->attach_sync_capture(&primary_sink);

    {
        KeyValueTable<int, int> kv(primary_conn, "kv");
        kv.insert_or_assign(1, 100);
        kv.insert_or_assign(2, 200);
        kv.insert_or_assign(3, 300);
    }

    primary_conn->detach_sync_capture();

    sync::DirectSyncPeer peer(&primary_engine);
    sync::PullRequest req;
    req.requester = replica_node;
    req.db_id     = db_uuid;
    const sync::PullResponse resp = peer.pull(req);

    if (resp.batches.size() != 3u) {
        throw std::runtime_error("expected 3 batches, got " +
                                 std::to_string(resp.batches.size()));
    }

    {
        auto txn = replica_conn->transaction(TransactionMode::WRITABLE);
        for (const sync::ChangeBatch& batch : resp.batches) {
            const sync::ApplyResult r = replica_engine.apply_batch(txn.handle(), batch);
            if (r != sync::ApplyResult::Applied) {
                throw std::runtime_error("apply_batch did not return Applied");
            }
        }
        txn.commit();
    }

    KeyValueTable<int, int> replica_kv(replica_conn, "kv");
    if (kv_or_throw(replica_conn, replica_kv, 1, "replica kv[1]") != 100) throw std::runtime_error("replica kv[1] != 100");
    if (kv_or_throw(replica_conn, replica_kv, 2, "replica kv[2]") != 200) throw std::runtime_error("replica kv[2] != 200");
    if (kv_or_throw(replica_conn, replica_kv, 3, "replica kv[3]") != 300) throw std::runtime_error("replica kv[3] != 300");

    replica_conn->disconnect();
    cleanup(primary_path);
    cleanup(replica_path);
}

void test_public_tables_reject_reserved_dbi_names() {
    using namespace mdbxc;
    const std::string p = "test_public_reserved_dbi_names.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    bool rejected = false;
    try {
        KeyValueTable<int, int> kv(conn, "_mdbxc_user_table");
        (void)kv;
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    if (!rejected) {
        throw std::runtime_error("public table accepted reserved DBI name");
    }

    sync::MetaStore meta(conn->env_handle());
    auto txn = conn->transaction(TransactionMode::WRITABLE);
    meta.open(txn.handle());
    meta.set_schema_version(txn.handle(), sync::meta_schema_version());
    txn.commit();

    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_reserved_dbi_changes_and_rolls_back_page() {
    using namespace mdbxc;
    const std::string p = "test_engine_reserved_dbi_changes.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    const sync::NodeId local_node = make_node(0x10);
    const sync::NodeId db_id = make_node(0x11);
    const sync::NodeId origin = make_node(0x20);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(local_node, db_id);
    KeyValueTable<int, int> safe(conn, "safe");

    const sync::ChangeOpType op_types[] = {
        sync::ChangeOpType::Put,
        sync::ChangeOpType::Delete,
        sync::ChangeOpType::ClearTable
    };

    for (std::size_t i = 0; i < sizeof(op_types) / sizeof(op_types[0]); ++i) {
        sync::ChangeBatch batch;
        batch.origin_node_id = origin;
        batch.seq = 1;

        sync::ChangeOp safe_op;
        safe_op.op_type = sync::ChangeOpType::Put;
        safe_op.dbi_name = "safe";
        safe_op.dbi_flags = static_cast<std::uint32_t>(MDBX_INTEGERKEY);
        assign_int_key(safe_op.storage_key, static_cast<int>(100 + i));
        assign_int_value(safe_op.value, static_cast<int>(200 + i));
        batch.ops.push_back(safe_op);

        sync::ChangeOp reserved_op;
        reserved_op.op_type = op_types[i];
        reserved_op.dbi_name = "_mdbxc_meta";
        assign_int_key(reserved_op.storage_key, 7);
        if (reserved_op.op_type == sync::ChangeOpType::Put) {
            assign_int_value(reserved_op.value, 8);
        }
        batch.ops.push_back(reserved_op);

        sync::PushRequest request;
        request.sender = origin;
        request.db_id = db_id;
        request.batches.push_back(batch);

        const sync::PushResponse response = engine.handle_push(request);
        if (response.ok) {
            throw std::runtime_error(
                std::string("reserved DBI ") +
                op_type_name(op_types[i]) + " push unexpectedly succeeded");
        }
        if (response.error.find("reserved_dbi_name") == std::string::npos ||
            response.error.find("_mdbxc_meta") == std::string::npos) {
            throw std::runtime_error(
                std::string("reserved DBI ") +
                op_type_name(op_types[i]) + " returned wrong error: " +
                response.error);
        }
        if (response.error_code != sync::SyncResponseErrorCode::ApplyConflict ||
            response.error_retryable ||
            response.receiver_have.last_seq_for(origin) != 0u) {
            throw std::runtime_error(
                std::string("reserved DBI ") +
                op_type_name(op_types[i]) + " returned wrong structured error");
        }
        if (engine.applied_cursor().last_seq_for(origin) != 0u) {
            throw std::runtime_error(
                std::string("reserved DBI ") +
                op_type_name(op_types[i]) + " advanced applied cursor");
        }
        if (kv_has(conn, safe, static_cast<int>(100 + i))) {
            throw std::runtime_error(
                std::string("reserved DBI ") +
                op_type_name(op_types[i]) + " did not roll back page");
        }
        if (engine.db_uuid() != db_id) {
            throw std::runtime_error(
                std::string("reserved DBI ") +
                op_type_name(op_types[i]) + " changed metadata");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_reserved_dbi_rolls_back_multi_batch_push() {
    using namespace mdbxc;
    const std::string p = "test_engine_reserved_dbi_multi_batch.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    const sync::NodeId local_node = make_node(0x12);
    const sync::NodeId db_id = make_node(0x13);
    const sync::NodeId origin = make_node(0x22);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(local_node, db_id);
    KeyValueTable<int, int> safe(conn, "safe");

    sync::ChangeBatch valid_batch;
    valid_batch.origin_node_id = origin;
    valid_batch.seq = 1;

    sync::ChangeOp safe_op;
    safe_op.op_type = sync::ChangeOpType::Put;
    safe_op.dbi_name = "safe";
    safe_op.dbi_flags = static_cast<std::uint32_t>(MDBX_INTEGERKEY);
    assign_int_key(safe_op.storage_key, 501);
    assign_int_value(safe_op.value, 601);
    valid_batch.ops.push_back(safe_op);

    sync::ChangeBatch reserved_batch;
    reserved_batch.origin_node_id = origin;
    reserved_batch.seq = 2;

    sync::ChangeOp reserved_op;
    reserved_op.op_type = sync::ChangeOpType::ClearTable;
    reserved_op.dbi_name = "_mdbxc_meta";
    reserved_batch.ops.push_back(reserved_op);

    sync::PushRequest request;
    request.sender = origin;
    request.db_id = db_id;
    request.batches.push_back(valid_batch);
    request.batches.push_back(reserved_batch);

    const sync::PushResponse response = engine.handle_push(request);
    if (response.ok) {
        throw std::runtime_error("reserved DBI multi-batch push unexpectedly succeeded");
    }
    if (response.error.find("reserved_dbi_name") == std::string::npos ||
        response.error.find("_mdbxc_meta") == std::string::npos) {
        throw std::runtime_error("reserved DBI multi-batch push returned wrong error: " +
                                 response.error);
    }
    if (response.error_code != sync::SyncResponseErrorCode::ApplyConflict ||
        response.error_retryable ||
        response.receiver_have.last_seq_for(origin) != 0u) {
        throw std::runtime_error(
            "reserved DBI multi-batch push returned wrong structured error");
    }
    if (engine.applied_cursor().last_seq_for(origin) != 0u) {
        throw std::runtime_error("reserved DBI multi-batch push advanced applied cursor");
    }
    if (kv_has(conn, safe, 501)) {
        throw std::runtime_error("reserved DBI multi-batch push committed earlier batch");
    }
    if (engine.db_uuid() != db_id) {
        throw std::runtime_error("reserved DBI multi-batch push changed metadata");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_skips_self_origin() {
    using namespace mdbxc;
    const std::string p = "test_engine_self_origin.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    seed_node_id(conn, make_node(0x10));

    sync::SyncEngine engine(conn);
    sync::MetaStore meta(conn->env_handle());
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        meta.open(txn.handle());
        meta.set_node_id(txn.handle(), make_node(0x10));
        txn.commit();
    }

    sync::ChangeBatch batch;
    batch.origin_node_id = make_node(0x10);
    batch.seq = 1;
    sync::ChangeOp op;
    op.op_type = sync::ChangeOpType::Put;
    op.dbi_name = "t";
    op.storage_key = { 0x01 };
    op.value = { 0xAA };
    batch.ops.push_back(op);

    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        const sync::ApplyResult r = engine.apply_batch(txn.handle(), batch);
        if (r != sync::ApplyResult::Skipped) {
            throw std::runtime_error("self-origin batch should be Skipped");
        }
        txn.commit();
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_idempotent_replay() {
    using namespace mdbxc;
    const std::string p = "test_engine_replay.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    seed_node_id(conn, make_node(0x10));

    sync::SyncEngine engine(conn);

    sync::ChangeBatch batch;
    batch.origin_node_id = make_node(0x20);
    batch.seq = 1;
    sync::ChangeOp op;
    op.op_type = sync::ChangeOpType::Put;
    op.dbi_name = "t";
    op.storage_key = { 0x42 };
    op.value = { 0x11, 0x22 };
    batch.ops.push_back(op);

    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        if (engine.apply_batch(txn.handle(), batch) != sync::ApplyResult::Applied) {
            throw std::runtime_error("first apply should be Applied");
        }
        if (engine.apply_batch(txn.handle(), batch) != sync::ApplyResult::Skipped) {
            throw std::runtime_error("second apply should be Skipped");
        }
        txn.commit();
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_applies_legacy_zero_flags_to_integer_dbi() {
    using namespace mdbxc;
    const std::string p = "test_engine_legacy_zero_flags.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xD0));
    KeyValueTable<int, int> kv(conn, "kv");

    const int key = 42;
    const int value = 77;
    SerializeScratch key_scratch;
    SerializeScratch value_scratch;
    const MDBX_val db_key = serialize_key<true>(key, key_scratch);
    const MDBX_val db_value = serialize_value(value, value_scratch);

    sync::ChangeBatch batch;
    batch.origin_node_id = make_node(0x20);
    batch.seq = 1;
    sync::ChangeOp op;
    op.op_type = sync::ChangeOpType::Put;
    op.dbi_flags = 0;
    op.dbi_name = "kv";
    assign_bytes(op.storage_key, db_key);
    assign_bytes(op.value, db_value);
    batch.ops.push_back(op);

    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        if (engine.apply_batch(txn.handle(), batch) != sync::ApplyResult::Applied) {
            throw std::runtime_error("legacy zero-flags batch should apply");
        }
        txn.commit();
    }

    if (kv_or_throw(conn, kv, key, "legacy zero-flags kv") != value) {
        throw std::runtime_error("legacy zero-flags kv value mismatch");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_conflicting_dbi_flags_returns_conflict() {
    using namespace mdbxc;
    const std::string p = "test_engine_conflicting_dbi_flags.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xD0));

    sync::ChangeBatch batch;
    batch.origin_node_id = make_node(0x20);
    batch.seq = 1;

    sync::ChangeOp first;
    first.op_type = sync::ChangeOpType::Put;
    first.dbi_name = "kv";
    first.dbi_flags = static_cast<std::uint32_t>(MDBX_INTEGERKEY);
    first.storage_key = { 0x01 };
    first.value = { 0x11 };
    batch.ops.push_back(first);

    sync::ChangeOp second = first;
    second.dbi_flags = static_cast<std::uint32_t>(MDBX_REVERSEKEY);
    second.storage_key = { 0x02 };
    second.value = { 0x22 };
    batch.ops.push_back(second);

    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        const sync::ApplyOutcome outcome = engine.apply_batch_ex(txn.handle(), batch);
        if (outcome.result != sync::ApplyResult::Conflict) {
            throw std::runtime_error("conflicting dbi_flags batch should return Conflict");
        }
        if (outcome.conflict_reason != sync::ApplyConflictReason::InconsistentBatchDbiFlags) {
            throw std::runtime_error("conflicting dbi_flags batch returned wrong reason");
        }
        if (outcome.dbi_name != "kv") {
            throw std::runtime_error("conflicting dbi_flags batch returned wrong DBI name");
        }
        txn.commit();
    }

    if (engine.applied_cursor().last_seq_for(make_node(0x20)) != 0u) {
        throw std::runtime_error("conflicting dbi_flags batch advanced cursor");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_existing_dbi_flag_mismatch_returns_conflict() {
    using namespace mdbxc;
    const std::string p = "test_engine_existing_dbi_flag_mismatch.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    seed_node_id(conn, make_node(0x10));
    sync::SyncEngine engine(conn);
    KeyValueTable<int, int> kv(conn, "kv");

    const sync::NodeId origin = make_node(0x20);

    SerializeScratch key_scratch;
    SerializeScratch value_scratch;
    const MDBX_val good_key = serialize_key<true>(1, key_scratch);
    const MDBX_val good_value = serialize_value(100, value_scratch);

    sync::ChangeBatch good;
    good.origin_node_id = origin;
    good.seq = 1;
    sync::ChangeOp good_op;
    good_op.op_type = sync::ChangeOpType::Put;
    good_op.dbi_name = "kv";
    good_op.dbi_flags = static_cast<std::uint32_t>(MDBX_INTEGERKEY);
    assign_bytes(good_op.storage_key, good_key);
    assign_bytes(good_op.value, good_value);
    good.ops.push_back(good_op);

    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        if (engine.apply_batch(txn.handle(), good) != sync::ApplyResult::Applied) {
            throw std::runtime_error("matching DBI flags batch should apply");
        }
        txn.commit();
    }

    key_scratch.clear();
    value_scratch.clear();
    const MDBX_val bad_key = serialize_key<true>(2, key_scratch);
    const MDBX_val bad_value = serialize_value(200, value_scratch);

    sync::ChangeBatch bad;
    bad.origin_node_id = origin;
    bad.seq = 2;
    sync::ChangeOp bad_op;
    bad_op.op_type = sync::ChangeOpType::Put;
    bad_op.dbi_name = "kv";
    bad_op.dbi_flags = static_cast<std::uint32_t>(MDBX_REVERSEKEY);
    assign_bytes(bad_op.storage_key, bad_key);
    assign_bytes(bad_op.value, bad_value);
    bad.ops.push_back(bad_op);

    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        const sync::ApplyOutcome outcome = engine.apply_batch_ex(txn.handle(), bad);
        if (outcome.result != sync::ApplyResult::Conflict) {
            throw std::runtime_error("existing DBI flag mismatch should return Conflict");
        }
        if (outcome.conflict_reason != sync::ApplyConflictReason::ExistingDbiFlagsMismatch) {
            throw std::runtime_error("existing DBI flag mismatch returned wrong reason");
        }
        if (outcome.dbi_name != "kv") {
            throw std::runtime_error("existing DBI flag mismatch returned wrong DBI name");
        }
        if (outcome.incoming_dbi_flags != static_cast<std::uint32_t>(MDBX_REVERSEKEY)) {
            throw std::runtime_error("existing DBI flag mismatch returned wrong flags");
        }
        if (!outcome.actual_dbi_flags_available ||
            outcome.actual_dbi_flags != static_cast<std::uint32_t>(MDBX_INTEGERKEY)) {
            throw std::runtime_error("existing DBI flag mismatch returned wrong actual flags");
        }
        txn.commit();
    }

    if (engine.applied_cursor().last_seq_for(origin) != 1u) {
        throw std::runtime_error("existing DBI flag mismatch advanced cursor");
    }
    if (kv_has(conn, kv, 2)) {
        throw std::runtime_error("existing DBI flag mismatch applied data");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_existing_dbi_flag_mismatch_reports_first_batch_dbi() {
    using namespace mdbxc;
    const std::string p = "test_engine_existing_dbi_flag_mismatch_first.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    seed_node_id(conn, make_node(0x10));
    sync::SyncEngine engine(conn);
    KeyValueTable<int, int> beta(conn, "beta");
    KeyValueTable<int, int> alpha(conn, "alpha");
    (void)beta;
    (void)alpha;

    SerializeScratch key_scratch;
    SerializeScratch value_scratch;
    const MDBX_val key = serialize_key<true>(1, key_scratch);
    const MDBX_val value = serialize_value(100, value_scratch);

    sync::ChangeBatch batch;
    batch.origin_node_id = make_node(0x20);
    batch.seq = 1;

    sync::ChangeOp beta_op;
    beta_op.op_type = sync::ChangeOpType::Put;
    beta_op.dbi_name = "beta";
    beta_op.dbi_flags = static_cast<std::uint32_t>(MDBX_REVERSEKEY);
    assign_bytes(beta_op.storage_key, key);
    assign_bytes(beta_op.value, value);
    batch.ops.push_back(beta_op);

    sync::ChangeOp alpha_op = beta_op;
    alpha_op.dbi_name = "alpha";
    batch.ops.push_back(alpha_op);

    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        const sync::ApplyOutcome outcome = engine.apply_batch_ex(txn.handle(), batch);
        if (outcome.result != sync::ApplyResult::Conflict) {
            throw std::runtime_error("multi-DBI mismatch should return Conflict");
        }
        if (outcome.conflict_reason != sync::ApplyConflictReason::ExistingDbiFlagsMismatch) {
            throw std::runtime_error("multi-DBI mismatch returned wrong reason");
        }
        if (outcome.dbi_name != "beta") {
            throw std::runtime_error("multi-DBI mismatch did not report first batch DBI");
        }
        if (!outcome.actual_dbi_flags_available ||
            outcome.actual_dbi_flags != static_cast<std::uint32_t>(MDBX_INTEGERKEY)) {
            throw std::runtime_error("multi-DBI mismatch returned wrong actual flags");
        }
        txn.commit();
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_push_dbi_conflicts_are_not_retryable() {
    using namespace mdbxc;
    const std::string p = "test_engine_push_dbi_conflicts_retryable.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xD0));
    const sync::NodeId origin = make_node(0x20);

    {
        sync::ChangeBatch batch;
        batch.origin_node_id = origin;
        batch.seq = 1;

        sync::ChangeOp first;
        first.op_type = sync::ChangeOpType::Put;
        first.dbi_name = "inconsistent";
        first.dbi_flags = static_cast<std::uint32_t>(MDBX_INTEGERKEY);
        assign_int_key(first.storage_key, 1);
        assign_int_value(first.value, 10);
        batch.ops.push_back(first);

        sync::ChangeOp second = first;
        second.dbi_flags = static_cast<std::uint32_t>(MDBX_REVERSEKEY);
        assign_int_key(second.storage_key, 2);
        assign_int_value(second.value, 20);
        batch.ops.push_back(second);

        sync::PushRequest request;
        request.sender = origin;
        request.db_id = make_node(0xD0);
        request.batches.push_back(batch);
        const sync::PushResponse response = engine.handle_push(request);
        if (response.ok ||
            response.error_code != sync::SyncResponseErrorCode::ApplyConflict ||
            response.error_retryable ||
            response.receiver_have.last_seq_for(origin) != 0u) {
            throw std::runtime_error(
                "inconsistent DBI flags returned wrong structured response");
        }
    }

    {
        KeyValueTable<int, int> existing(conn, "existing");
        (void)existing;

        sync::ChangeBatch batch;
        batch.origin_node_id = origin;
        batch.seq = 1;

        sync::ChangeOp op;
        op.op_type = sync::ChangeOpType::Put;
        op.dbi_name = "existing";
        op.dbi_flags = static_cast<std::uint32_t>(MDBX_REVERSEKEY);
        assign_int_key(op.storage_key, 1);
        assign_int_value(op.value, 10);
        batch.ops.push_back(op);

        sync::PushRequest request;
        request.sender = origin;
        request.db_id = make_node(0xD0);
        request.batches.push_back(batch);
        const sync::PushResponse response = engine.handle_push(request);
        if (response.ok ||
            response.error_code != sync::SyncResponseErrorCode::ApplyConflict ||
            response.error_retryable ||
            response.receiver_have.last_seq_for(origin) != 0u) {
            throw std::runtime_error(
                "existing DBI flags mismatch returned wrong structured response");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_gap_returns_conflict() {
    using namespace mdbxc;
    const std::string p = "test_engine_gap.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    seed_node_id(conn, make_node(0x10));
    sync::SyncEngine engine(conn);

    auto make_batch = [](std::uint64_t seq) {
        sync::ChangeBatch b;
        b.origin_node_id = make_node(0x20);
        b.seq = seq;
        sync::ChangeOp op;
        op.op_type = sync::ChangeOpType::Put;
        op.dbi_name = "t";
        op.storage_key = { static_cast<std::uint8_t>(seq) };
        op.value = { 0xFF };
        b.ops.push_back(op);
        return b;
    };

    auto txn = conn->transaction(TransactionMode::WRITABLE);
    if (engine.apply_batch(txn.handle(), make_batch(1)) != sync::ApplyResult::Applied) {
        throw std::runtime_error("seq=1 should apply");
    }
    const sync::ApplyOutcome gap = engine.apply_batch_ex(txn.handle(), make_batch(3));
    if (gap.result != sync::ApplyResult::Conflict) {
        throw std::runtime_error("seq=3 should be Conflict (gap after seq=1)");
    }
    if (gap.conflict_reason != sync::ApplyConflictReason::SequenceGap ||
        gap.last_applied_seq != 1u ||
        gap.batch_seq != 3u) {
        throw std::runtime_error("seq=3 returned wrong conflict details");
    }
    if (engine.apply_batch(txn.handle(), make_batch(2)) != sync::ApplyResult::Applied) {
        throw std::runtime_error("seq=2 should apply after seq=1");
    }
    txn.commit();

    conn->disconnect();
    cleanup(p);
}

void test_engine_applied_cursor() {
    using namespace mdbxc;
    const std::string p = "test_engine_cursor.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    seed_node_id(conn, make_node(0x10));
    sync::SyncEngine engine(conn);

    auto make_batch = [](std::uint64_t seq) {
        sync::ChangeBatch b;
        b.origin_node_id = make_node(0x20);
        b.seq = seq;
        sync::ChangeOp op;
        op.op_type = sync::ChangeOpType::Put;
        op.dbi_name = "t";
        op.storage_key = { static_cast<std::uint8_t>(seq) };
        op.value = { 0xFF };
        b.ops.push_back(op);
        return b;
    };

    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        engine.apply_batch(txn.handle(), make_batch(1));
        engine.apply_batch(txn.handle(), make_batch(2));
        txn.commit();
    }

    const sync::SyncCursor cur = engine.applied_cursor();
    const std::uint64_t last =
        cur.last_seq_for(make_node(0x20));
    if (last != 2u) {
        throw std::runtime_error("cursor should report last=2, got " +
                                 std::to_string(last));
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_handle_push_to_remote() {
    using namespace mdbxc;
    const std::string origin_path = "test_engine_push_origin.mdbx";
    const std::string remote_path = "test_engine_push_remote.mdbx";
    cleanup(origin_path);
    cleanup(remote_path);

    auto origin_conn = open_env(origin_path);
    auto remote_conn = open_env(remote_path);

    sync::SyncEngine origin_engine(origin_conn);
    sync::SyncEngine remote_engine(remote_conn);
    origin_engine.initialize_local_identity(make_node(0xA0), make_node(0xD0));
    remote_engine.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    sync::ThreadLocalChangeAccumulator origin_sink(origin_conn);
    origin_conn->attach_sync_capture(&origin_sink);
    {
        KeyValueTable<int, int> kv(origin_conn, "kv");
        kv.insert_or_assign(7, 0x77);
    }
    origin_conn->detach_sync_capture();

    sync::DirectSyncPeer peer(&remote_engine);
    sync::PushRequest req;
    req.sender = make_node(0xA0);
    req.db_id  = make_node(0xD0);
    CountingApplyObserver observer;
    ThrowingApplyObserver throwing_observer;
    VectorStore remote_vectors(remote_conn, "observer_reentrant");
    ReentrantVectorApplyObserver reentrant_observer(&remote_vectors);
    const std::uint64_t observer_token =
        remote_conn->add_sync_apply_observer(&observer);
    const std::uint64_t throwing_token =
        remote_conn->add_sync_apply_observer(&throwing_observer);
    const std::uint64_t reentrant_token =
        remote_conn->add_sync_apply_observer(&reentrant_observer);
    if (remote_conn->sync_apply_generation() != 0u) {
        throw std::runtime_error("fresh replica should have sync apply generation 0");
    }

    {
        auto txn = origin_conn->transaction(TransactionMode::WRITABLE);
        sync::MetaStore meta(origin_conn->env_handle());
        meta.open(txn.handle());
        sync::ChangeLogStore log(origin_conn->env_handle());
        log.open(txn.handle());
        const std::uint64_t last_seq = meta.get_local_seq(txn.handle());
        std::vector<std::uint8_t> buf;
        if (!log.get(txn.handle(), make_node(0xA0), last_seq, buf)) {
            throw std::runtime_error("origin changelog has no batch for last seq");
        }
        const sync::ChangeBatch b = sync::ChangeBatchCodec::decode_exact(buf);
        req.batches.push_back(b);
        txn.commit();
    }

    const sync::PushResponse resp = peer.push(req);
    if (!resp.ok) {
        throw std::runtime_error("push should succeed: " + resp.error);
    }
    if (resp.receiver_have.last_seq_for(make_node(0xA0)) != 1u) {
        throw std::runtime_error("receiver cursor should reflect applied seq");
    }
    if (remote_conn->sync_apply_generation() != 1u) {
        throw std::runtime_error("remote apply should increment generation");
    }
    if (observer.calls != 1u || observer.generation != 1u ||
        observer.applied_batches != 1u || observer.applied_ops != 1u) {
        throw std::runtime_error("remote apply observer did not receive event");
    }
    if (observer.affected_dbi_names.size() != 1u ||
        observer.affected_dbi_names[0] != "kv") {
        throw std::runtime_error("remote apply observer DBI names incorrect");
    }
    if (reentrant_observer.calls != 1u ||
        reentrant_observer.last_count != 0u) {
        throw std::runtime_error(
            "remote apply observer could not use cache-backed table API");
    }

    KeyValueTable<int, int> remote_kv(remote_conn, "kv");
    if (kv_or_throw(remote_conn, remote_kv, 7, "remote kv[7]") != 0x77) {
        throw std::runtime_error("remote kv[7] != 0x77 after push");
    }

    const sync::PushResponse replay = peer.push(req);
    if (!replay.ok) {
        throw std::runtime_error("idempotent replay should succeed: " +
                                 replay.error);
    }
    if (remote_conn->sync_apply_generation() != 1u) {
        throw std::runtime_error(
            "skipped idempotent replay should not increment generation");
    }
    if (observer.calls != 1u) {
        throw std::runtime_error("idempotent replay should not notify observer");
    }
    if (!remote_conn->remove_sync_apply_observer(observer_token) ||
        !remote_conn->remove_sync_apply_observer(throwing_token) ||
        !remote_conn->remove_sync_apply_observer(reentrant_token)) {
        throw std::runtime_error("failed to remove sync apply observer");
    }
    if (remote_conn->remove_sync_apply_observer(observer_token)) {
        throw std::runtime_error("observer token was removed twice");
    }

    origin_conn->disconnect();
    remote_conn->disconnect();
    cleanup(origin_path);
    cleanup(remote_path);
}

void test_engine_push_gap_rolls_back() {
    using namespace mdbxc;
    const std::string p = "test_engine_push_gap.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xD0));
    CountingApplyObserver observer;
    conn->add_sync_apply_observer(&observer);

    auto make_batch = [](std::uint64_t seq) {
        sync::ChangeBatch b;
        b.origin_node_id = make_node(0x20);
        b.seq = seq;
        sync::ChangeOp op;
        op.op_type = sync::ChangeOpType::Put;
        op.dbi_name = "t";
        op.storage_key = { static_cast<std::uint8_t>(seq) };
        op.value = { 0xFF };
        b.ops.push_back(op);
        return b;
    };

    // Direct apply: seq=1 first
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        if (engine.apply_batch(txn.handle(), make_batch(1)) != sync::ApplyResult::Applied) {
            throw std::runtime_error("seq=1 should apply");
        }
        txn.commit();
    }

    // Push [seq=3] — should be Conflict, ok=false, no commit
    {
        sync::DirectSyncPeer peer(&engine);
        sync::PushRequest req;
        req.sender = make_node(0x20);
        req.db_id  = make_node(0xD0);
        req.batches.push_back(make_batch(3));

        const sync::PushResponse resp = peer.push(req);
        if (resp.ok) {
            throw std::runtime_error("push with gap should return ok=false");
        }
        if (resp.error.find("sequence_gap") == std::string::npos) {
            throw std::runtime_error("push with gap should return sequence_gap error");
        }
        if (resp.error_code != sync::SyncResponseErrorCode::ApplyConflict ||
            !resp.error_retryable ||
            resp.receiver_have.last_seq_for(make_node(0x20)) != 1u) {
            throw std::runtime_error("push with gap error code incorrect");
        }
        if (observer.calls != 0u) {
            throw std::runtime_error("failed push should not notify apply observer");
        }
    }

    // After rejected push, table 't' should still be empty (rollback worked)
    {
        KeyValueTable<std::uint8_t, std::uint8_t> t(conn, "t");
        if (kv_has(conn, t, static_cast<std::uint8_t>(3))) {
            throw std::runtime_error("seq=3 must not be persisted on rejected push");
        }
    }

    // Receiver cursor should remain at seq=1
    {
        const sync::SyncCursor cur = engine.applied_cursor();
        if (cur.last_seq_for(make_node(0x20)) != 1u) {
            throw std::runtime_error("cursor should still be at seq=1 after rejected push");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_sync_apply_observer_remove_waits_for_in_flight_callback() {
    using namespace mdbxc;
    const std::string p = "test_sync_apply_observer_remove_waits.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xD0));

    BlockingApplyObserver observer;
    const std::uint64_t token = conn->add_sync_apply_observer(&observer);

    sync::PushRequest req;
    req.sender = make_node(0x20);
    req.db_id = make_node(0xD0);
    req.batches.push_back(make_raw_batch(make_node(0x20), 1, "t", 0x31));

    sync::PushResponse response;
    std::string push_error;
    std::thread push_thread(
        [&engine, &req, &response, &push_error]() {
            try {
                response = engine.handle_push(req);
            } catch (const std::exception& e) {
                push_error = e.what();
            } catch (...) {
                push_error = "unknown push error";
            }
        });

    if (!observer.wait_until_entered(std::chrono::milliseconds(1000))) {
        observer.release_callback();
        push_thread.join();
        throw std::runtime_error(
            "sync apply observer callback did not start");
    }

    bool removed = false;
    bool remove_returned = false;
    std::mutex remove_mutex;
    std::thread remove_thread(
        [conn, token, &removed, &remove_returned, &remove_mutex]() {
            const bool ok = conn->remove_sync_apply_observer(token);
            std::lock_guard<std::mutex> lock(remove_mutex);
            removed = ok;
            remove_returned = true;
        });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    bool returned_while_callback_was_blocked = false;
    {
        std::lock_guard<std::mutex> lock(remove_mutex);
        returned_while_callback_was_blocked = remove_returned;
    }

    observer.release_callback();
    push_thread.join();
    remove_thread.join();

    if (returned_while_callback_was_blocked) {
        throw std::runtime_error(
            "remove_sync_apply_observer returned with callback in flight");
    }
    if (!removed) {
        throw std::runtime_error("remove_sync_apply_observer returned false");
    }
    if (!push_error.empty()) {
        throw std::runtime_error(push_error);
    }
    if (!response.ok) {
        throw std::runtime_error("push should succeed: " + response.error);
    }
    if (observer.call_count() != 1u) {
        throw std::runtime_error("observer should be called once");
    }

    sync::PushRequest second_req;
    second_req.sender = make_node(0x20);
    second_req.db_id = make_node(0xD0);
    second_req.batches.push_back(make_raw_batch(make_node(0x20), 2, "t", 0x32));
    const sync::PushResponse second_response = engine.handle_push(second_req);
    if (!second_response.ok) {
        throw std::runtime_error("second push should succeed: " +
                                 second_response.error);
    }
    if (observer.call_count() != 1u) {
        throw std::runtime_error(
            "removed observer should not receive later callbacks");
    }

    conn->disconnect();
    cleanup(p);
}

void test_sync_apply_observer_reports_unique_dbi_names() {
    using namespace mdbxc;
    const std::string p = "test_sync_apply_observer_dbi_names.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xD0));

    CountingApplyObserver observer;
    const std::uint64_t token = conn->add_sync_apply_observer(&observer);

    sync::ChangeBatch batch;
    batch.origin_node_id = make_node(0x20);
    batch.seq = 1;
    for (int i = 0; i < 3; ++i) {
        sync::ChangeOp op;
        op.op_type = sync::ChangeOpType::Put;
        op.dbi_name = i == 1 ? "second_table" : "first_table";
        op.storage_key.push_back(static_cast<std::uint8_t>(0x40 + i));
        op.value.push_back(static_cast<std::uint8_t>(0x50 + i));
        batch.ops.push_back(op);
    }

    sync::PushRequest req;
    req.sender = make_node(0x20);
    req.db_id = make_node(0xD0);
    req.batches.push_back(batch);

    const sync::PushResponse resp = engine.handle_push(req);
    if (!resp.ok) {
        throw std::runtime_error("push should succeed: " + resp.error);
    }
    if (observer.calls != 1u || observer.applied_batches != 1u ||
        observer.applied_ops != 3u) {
        throw std::runtime_error("observer apply counts incorrect");
    }
    if (observer.affected_dbi_names.size() != 2u ||
        observer.affected_dbi_names[0] != "first_table" ||
        observer.affected_dbi_names[1] != "second_table") {
        throw std::runtime_error("observer DBI names are not unique/stable");
    }

    if (!conn->remove_sync_apply_observer(token)) {
        throw std::runtime_error("failed to remove observer");
    }

    conn->disconnect();
    cleanup(p);
}

void test_sync_apply_observer_reports_dbi_names_across_batches() {
    using namespace mdbxc;
    const std::string p = "test_sync_apply_observer_dbi_multi_batch.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xD0));

    CountingApplyObserver observer;
    const std::uint64_t token = conn->add_sync_apply_observer(&observer);

    const sync::NodeId origin = make_node(0x20);
    sync::PushRequest req;
    req.sender = origin;
    req.db_id = make_node(0xD0);
    req.batches.push_back(make_raw_batch(origin, 1, "first_table", 0x41));
    req.batches.push_back(make_raw_batch(origin, 2, "second_table", 0x42));
    req.batches.push_back(make_raw_batch(origin, 3, "first_table", 0x43));

    const sync::PushResponse resp = engine.handle_push(req);
    if (!resp.ok) {
        throw std::runtime_error("multi-batch push should succeed: " +
                                 resp.error);
    }
    if (observer.calls != 1u || observer.applied_batches != 3u ||
        observer.applied_ops != 3u) {
        throw std::runtime_error(
            "multi-batch observer apply counts incorrect");
    }
    if (observer.affected_dbi_names.size() != 2u ||
        observer.affected_dbi_names[0] != "first_table" ||
        observer.affected_dbi_names[1] != "second_table") {
        throw std::runtime_error(
            "multi-batch observer DBI names are not first-seen unique");
    }

    if (!conn->remove_sync_apply_observer(token)) {
        throw std::runtime_error("failed to remove observer");
    }

    conn->disconnect();
    cleanup(p);
}

void test_sync_apply_observer_ignores_skipped_batch_dbi_names() {
    using namespace mdbxc;
    const std::string p = "test_sync_apply_observer_dbi_skipped.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xD0));

    const sync::NodeId origin = make_node(0x20);
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        if (engine.apply_batch(txn.handle(),
                               make_raw_batch(origin, 1, "skipped_table",
                                              0x51)) !=
            sync::ApplyResult::Applied) {
            throw std::runtime_error("pre-applied batch should apply");
        }
        txn.commit();
    }

    CountingApplyObserver observer;
    const std::uint64_t token = conn->add_sync_apply_observer(&observer);

    sync::PushRequest req;
    req.sender = origin;
    req.db_id = make_node(0xD0);
    req.batches.push_back(make_raw_batch(origin, 1, "skipped_table", 0x51));
    req.batches.push_back(make_raw_batch(origin, 2, "applied_table", 0x52));

    const sync::PushResponse resp = engine.handle_push(req);
    if (!resp.ok) {
        throw std::runtime_error("skip+apply push should succeed: " +
                                 resp.error);
    }
    if (observer.calls != 1u || observer.applied_batches != 1u ||
        observer.applied_ops != 1u) {
        throw std::runtime_error("skip+apply observer counts incorrect");
    }
    if (observer.affected_dbi_names.size() != 1u ||
        observer.affected_dbi_names[0] != "applied_table") {
        throw std::runtime_error(
            "skipped batch DBI names leaked into observer event");
    }

    if (!conn->remove_sync_apply_observer(token)) {
        throw std::runtime_error("failed to remove observer");
    }

    conn->disconnect();
    cleanup(p);
}

void test_sync_apply_observer_reports_clear_and_delete_dbi_names() {
    using namespace mdbxc;
    const std::string p = "test_sync_apply_observer_dbi_clear_delete.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xD0));

    CountingApplyObserver observer;
    const std::uint64_t token = conn->add_sync_apply_observer(&observer);

    const sync::NodeId origin = make_node(0x20);
    sync::ChangeBatch batch;
    batch.origin_node_id = origin;
    batch.seq = 1;

    sync::ChangeOp put;
    put.op_type = sync::ChangeOpType::Put;
    put.dbi_name = "mixed_table";
    put.storage_key = { 0x61 };
    put.value = { 0x71 };
    batch.ops.push_back(put);

    sync::ChangeOp clear;
    clear.op_type = sync::ChangeOpType::ClearTable;
    clear.dbi_name = "cleared_table";
    batch.ops.push_back(clear);

    sync::ChangeOp del;
    del.op_type = sync::ChangeOpType::Delete;
    del.dbi_name = "mixed_table";
    del.storage_key = { 0x61 };
    batch.ops.push_back(del);

    sync::PushRequest req;
    req.sender = origin;
    req.db_id = make_node(0xD0);
    req.batches.push_back(batch);

    const sync::PushResponse resp = engine.handle_push(req);
    if (!resp.ok) {
        throw std::runtime_error("mixed op push should succeed: " +
                                 resp.error);
    }
    if (observer.calls != 1u || observer.applied_batches != 1u ||
        observer.applied_ops != 3u) {
        throw std::runtime_error("mixed op observer counts incorrect");
    }
    if (observer.affected_dbi_names.size() != 2u ||
        observer.affected_dbi_names[0] != "mixed_table" ||
        observer.affected_dbi_names[1] != "cleared_table") {
        throw std::runtime_error(
            "ClearTable/Delete DBI names were not reported correctly");
    }

    if (!conn->remove_sync_apply_observer(token)) {
        throw std::runtime_error("failed to remove observer");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_push_multi_batch_gap_reports_persistent_cursor() {
    using namespace mdbxc;
    const std::string p = "test_engine_push_multi_batch_gap_cursor.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xD0));
    CountingApplyObserver observer;
    const std::uint64_t token = conn->add_sync_apply_observer(&observer);

    auto make_batch = [](std::uint64_t seq) {
        sync::ChangeBatch b;
        b.origin_node_id = make_node(0x20);
        b.seq = seq;
        sync::ChangeOp op;
        op.op_type = sync::ChangeOpType::Put;
        op.dbi_name = "t";
        op.storage_key = { static_cast<std::uint8_t>(seq) };
        op.value = { 0xFF };
        b.ops.push_back(op);
        return b;
    };

    sync::DirectSyncPeer peer(&engine);
    sync::PushRequest req;
    req.sender = make_node(0x20);
    req.db_id = make_node(0xD0);
    req.batches.push_back(make_batch(1));
    req.batches.push_back(make_batch(3));

    const sync::PushResponse resp = peer.push(req);
    if (resp.ok ||
        resp.error_code != sync::SyncResponseErrorCode::ApplyConflict ||
        !resp.error_retryable ||
        resp.receiver_have.last_seq_for(make_node(0x20)) != 0u) {
        throw std::runtime_error(
            "multi-batch gap did not report persistent retryable cursor");
    }
    if (engine.applied_cursor().last_seq_for(make_node(0x20)) != 0u) {
        throw std::runtime_error("multi-batch gap advanced persistent cursor");
    }
    if (observer.calls != 0u || observer.applied_batches != 0u ||
        !observer.affected_dbi_names.empty()) {
        throw std::runtime_error(
            "rolled-back multi-batch gap notified apply observer");
    }
    {
        KeyValueTable<std::uint8_t, std::uint8_t> t(conn, "t");
        if (kv_has(conn, t, static_cast<std::uint8_t>(1)) ||
            kv_has(conn, t, static_cast<std::uint8_t>(3))) {
            throw std::runtime_error("multi-batch gap committed partial data");
        }
    }

    if (!conn->remove_sync_apply_observer(token)) {
        throw std::runtime_error("failed to remove observer");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_handle_pull_wrong_db_id() {
    using namespace mdbxc;
    const std::string p = "test_engine_pull_wrong_db.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xA0), make_node(0xD0));

    sync::DirectSyncPeer peer(&engine);
    sync::PullRequest req;
    req.requester = make_node(0xB0);
    req.db_id     = make_node(0xFF);  // wrong db_id

    const sync::PullResponse resp = peer.pull(req);
    if (resp.ok) {
        throw std::runtime_error("pull with wrong db_id should return ok=false");
    }
    if (!resp.batches.empty()) {
        throw std::runtime_error("pull with wrong db_id should not return batches");
    }
    if (resp.error_code != sync::SyncResponseErrorCode::DbIdMismatch ||
        resp.error_retryable) {
        throw std::runtime_error("pull wrong db_id error code incorrect");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_unconfigured_full_snapshot_request() {
    using namespace mdbxc;
    const std::string p = "test_engine_full_snapshot_request.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xA0), make_node(0xD0));

    sync::PullRequest req;
    req.requester = make_node(0xB0);
    req.db_id = make_node(0xD0);
    req.request_full_snapshot = true;

    const sync::PullResponse resp = engine.handle_pull(req);
    if (resp.ok) {
        throw std::runtime_error("full snapshot request should be rejected");
    }
    if (!resp.batches.empty()) {
        throw std::runtime_error("full snapshot rejection returned batches");
    }
    if (resp.error.find("not configured") == std::string::npos) {
        throw std::runtime_error("full snapshot rejection error is not explicit");
    }
    if (resp.error_code != sync::SyncResponseErrorCode::SnapshotNotConfigured ||
        resp.error_retryable) {
        throw std::runtime_error("full snapshot rejection code incorrect");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_exports_stable_full_snapshot_pages() {
    using namespace mdbxc;
    const std::string p = "test_engine_full_snapshot_export.mdbx";
    cleanup(p);

    std::shared_ptr<Connection> conn = open_env(p);
    sync::FullSnapshotExportOptions options;
    sync::FullSnapshotManifestEntry entry;
    entry.dbi_name = "documents";
    entry.dbi_flags = 0u;
    options.manifest.push_back(entry);
    options.max_materialized_operations = 16u;
    options.max_materialized_bytes = 4096u;
    options.max_active_sessions = 1u;
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    const sync::NodeId source_node = make_node(0xA2);
    const sync::NodeId db_id = make_node(0xD2);
    engine.initialize_local_identity(source_node, db_id);

    KeyValueTable<std::string, std::string> documents(conn, "documents");
    documents.insert_or_assign("one", "1");
    documents.insert_or_assign("two", "2");
    documents.insert_or_assign("three", "3");

    sync::PullRequest request;
    request.requester = make_node(0xB2);
    request.db_id = db_id;
    request.request_full_snapshot = true;
    request.max_bytes = 1u;
    request.max_single_batch_bytes = 8192u;

    sync::PullResponse response = engine.handle_pull(request);
    if (!response.ok || !response.is_full_snapshot ||
        !response.batches.empty() || !response.has_more) {
        throw std::runtime_error("first full snapshot page is invalid");
    }
    if (response.snapshot_chunk.chunk_index != 0u ||
        response.snapshot_chunk.snapshot_id.empty() ||
        response.snapshot_chunk.continuation.empty() ||
        response.snapshot_chunk.manifest.size() != 1u ||
        response.snapshot_chunk.manifest[0].dbi_name != "documents") {
        throw std::runtime_error("first full snapshot session metadata is invalid");
    }

    const sync::PullResponse busy = engine.handle_pull(request);
    if (busy.ok ||
        busy.error_code != sync::SyncResponseErrorCode::SnapshotSessionBusy ||
        !busy.error_retryable) {
        throw std::runtime_error("full snapshot session capacity is not bounded");
    }

    sync::PullRequest foreign_request = request;
    foreign_request.requester = make_node(0xC2);
    foreign_request.full_snapshot_id = response.snapshot_chunk.snapshot_id;
    foreign_request.full_snapshot_continuation =
        response.snapshot_chunk.continuation;
    const sync::PullResponse foreign = engine.handle_pull(foreign_request);
    if (foreign.ok ||
        foreign.error_code != sync::SyncResponseErrorCode::SnapshotSessionInvalid) {
        throw std::runtime_error("full snapshot session accepted a foreign requester");
    }

    documents.insert_or_assign("late", "not-in-snapshot");

    std::size_t clear_count = 0u;
    std::size_t put_count = 0u;
    for (;;) {
        const std::vector<sync::ChangeOp>& ops = response.snapshot_chunk.batch.ops;
        for (std::size_t i = 0u; i < ops.size(); ++i) {
            if (ops[i].op_type == sync::ChangeOpType::ClearTable) {
                ++clear_count;
            } else if (ops[i].op_type == sync::ChangeOpType::Put) {
                ++put_count;
                const std::string key(ops[i].storage_key.begin(),
                                      ops[i].storage_key.end());
                if (key == "late") {
                    throw std::runtime_error(
                        "full snapshot included a post-session source write");
                }
            } else {
                throw std::runtime_error("full snapshot contains a non-physical op");
            }
        }
        if (!response.has_more) {
            break;
        }
        request.full_snapshot_id = response.snapshot_chunk.snapshot_id;
        request.full_snapshot_continuation =
            response.snapshot_chunk.continuation;
        response = engine.handle_pull(request);
        if (!response.ok || !response.is_full_snapshot ||
            response.snapshot_chunk.snapshot_id != request.full_snapshot_id) {
            throw std::runtime_error("full snapshot continuation was rejected");
        }
    }
    if (clear_count != 1u || put_count != 3u) {
        throw std::runtime_error("full snapshot materialization has wrong content");
    }

    request.full_snapshot_continuation = "foreign-token";
    const sync::PullResponse invalid = engine.handle_pull(request);
    if (invalid.ok ||
        invalid.error_code != sync::SyncResponseErrorCode::SnapshotSessionInvalid) {
        throw std::runtime_error("invalid full snapshot continuation was accepted");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_full_snapshot_tail_includes_applied_origins() {
    using namespace mdbxc;
    const std::string p = "test_engine_full_snapshot_applied_tail.mdbx";
    cleanup(p);

    const sync::NodeId source_node = make_node(0xA7);
    const sync::NodeId remote_origin = make_node(0xB7);
    const sync::DbId db_id = make_node(0xD7);
    std::shared_ptr<Connection> conn = open_env(p);
    sync::FullSnapshotExportOptions options;
    options.replacement_scope = sync::FullSnapshotScope::CompleteUserDatabase;
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);

    sync::PushRequest pushed;
    pushed.sender = remote_origin;
    pushed.db_id = db_id;
    pushed.batches.push_back(make_raw_batch(remote_origin, 1u,
                                            "documents", 0x51u));
    if (!engine.handle_push(pushed).ok) {
        throw std::runtime_error(
            "snapshot source did not apply remote origin batch");
    }

    sync::PullRequest request;
    request.requester = make_node(0xC7);
    request.db_id = db_id;
    request.request_full_snapshot = true;
    request.max_bytes = 8192u;
    request.max_single_batch_bytes = 8192u;
    const sync::PullResponse response = engine.handle_pull(request);
    if (!response.ok || !response.is_full_snapshot || response.has_more ||
        response.snapshot_chunk.source_tail.last_seq_for(remote_origin) != 1u) {
        throw std::runtime_error(
            "snapshot source tail omitted an applied remote origin");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_exports_complete_full_snapshot_inventory() {
    using namespace mdbxc;
    const std::string p = "test_engine_complete_full_snapshot_inventory.mdbx";
    cleanup(p);

    const sync::NodeId source_node = make_node(0xA8);
    const sync::DbId db_id = make_node(0xD8);
    std::shared_ptr<Connection> conn = open_env(p);
    sync::FullSnapshotExportOptions options;
    options.replacement_scope = sync::FullSnapshotScope::CompleteUserDatabase;
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    KeyValueTable<std::string, std::string> audit(conn, "audit");
    documents.insert_or_assign("document", "value");
    audit.insert_or_assign("audit", "value");

    sync::PullRequest request;
    request.requester = make_node(0xB8);
    request.db_id = db_id;
    request.request_full_snapshot = true;
    request.max_bytes = 8192u;
    request.max_single_batch_bytes = 8192u;
    const sync::PullResponse response = engine.handle_pull(request);
    if (!response.ok || !response.is_full_snapshot || response.has_more ||
        response.snapshot_chunk.replacement_scope !=
            sync::FullSnapshotScope::CompleteUserDatabase ||
        response.snapshot_chunk.manifest.size() != 2u ||
        response.snapshot_chunk.manifest[0].dbi_name != "audit" ||
        response.snapshot_chunk.manifest[1].dbi_name != "documents") {
        throw std::runtime_error(
            "complete full snapshot did not inventory all user DBIs");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_complete_snapshot_with_logical_state() {
    using namespace mdbxc;
    typedef sync::KeyValueLogicalStringCodec<std::string> StringCodec;
    typedef sync::KeyValueTableLogicalAdapter<
        std::string, std::string, StringCodec, StringCodec> LogicalAdapter;

    const std::string p = "test_engine_complete_snapshot_logical_state.mdbx";
    const std::string schema_id = "app.snapshot.logical_key_value.v1";
    cleanup(p);

    const sync::NodeId source_node = make_node(0xA9);
    const sync::NodeId logical_origin = make_node(0xB9);
    const sync::DbId db_id = make_node(0xD9);
    sync::FullSnapshotExportOptions options;
    options.replacement_scope = sync::FullSnapshotScope::CompleteUserDatabase;
    options.max_materialized_operations = 16u;
    options.max_materialized_bytes = 4096u;
    options.max_active_sessions = 1u;

    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    LogicalAdapter adapter(documents, schema_id);
    sync::LogicalSchemaRecord record;
    record.dbi_name = "documents";
    record.kind = sync::LogicalTableKind::KeyValue;
    record.schema_version = 1u;
    record.dbi_names.push_back("documents");
    engine.register_logical_schema(schema_id, record);
    engine.register_logical_adapter(adapter);

    sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = db_id;
    envelope.origin_node_id = logical_origin;
    envelope.origin_sequence = 1u;
    envelope.frame_id = "snapshot-logical-state";
    envelope.frame.changes.push_back(
        adapter.make_upsert("logical", "value"));
    const sync::LogicalDeliveryAcknowledgement acknowledgement =
        engine.apply_ordered_logical_delivery_envelope(envelope);
    if (!acknowledgement.ok ||
        kv_or_throw(conn, documents, std::string("logical"),
                    "logical snapshot source value") != "value") {
        throw std::runtime_error(
            "failed to prepare logical snapshot source state");
    }

    sync::PullRequest request;
    request.requester = make_node(0xC9);
    request.db_id = db_id;
    request.request_full_snapshot = true;
    request.max_bytes = 8192u;
    request.max_single_batch_bytes = 8192u;
    const sync::PullResponse first = engine.handle_pull(request);
    const sync::PullResponse second = engine.handle_pull(request);
    if (first.ok || second.ok || first.is_full_snapshot ||
        !first.snapshot_chunk.snapshot_id.empty() ||
        first.error_code !=
            sync::SyncResponseErrorCode::SnapshotLogicalStateUnsupported ||
        second.error_code !=
            sync::SyncResponseErrorCode::SnapshotLogicalStateUnsupported ||
        first.error_retryable || second.error_retryable) {
        throw std::runtime_error(
            "complete snapshot accepted registered logical state");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_complete_snapshot_with_empty_ordered_frame() {
    using namespace mdbxc;
    const std::string p =
        "test_engine_complete_snapshot_empty_ordered_frame.mdbx";
    cleanup(p);

    const sync::NodeId source_node = make_node(0xAA);
    const sync::NodeId logical_origin = make_node(0xBA);
    const sync::DbId db_id = make_node(0xDA);
    sync::FullSnapshotExportOptions options;
    options.replacement_scope = sync::FullSnapshotScope::CompleteUserDatabase;
    options.max_materialized_operations = 16u;
    options.max_materialized_bytes = 4096u;
    options.max_active_sessions = 1u;

    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    documents.insert_or_assign("document", "raw-value");

    sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = db_id;
    envelope.origin_node_id = logical_origin;
    envelope.origin_sequence = 1u;
    envelope.frame_id = "snapshot-empty-ordered-frame";
    const sync::LogicalDeliveryAcknowledgement acknowledgement =
        engine.apply_ordered_logical_delivery_envelope(envelope);
    if (!acknowledgement.ok ||
        acknowledgement.acknowledged_through != 1u) {
        throw std::runtime_error(
            "failed to prepare empty ordered logical snapshot state");
    }
    {
        auto txn = conn->transaction(TransactionMode::READ_ONLY);
        sync::SchemaRegistryStore schemas(conn->env_handle());
        if (schemas.has_entries(txn.handle())) {
            throw std::runtime_error(
                "empty ordered logical frame unexpectedly registered a schema");
        }
    }

    sync::PullRequest request;
    request.requester = make_node(0xCA);
    request.db_id = db_id;
    request.request_full_snapshot = true;
    request.max_bytes = 8192u;
    request.max_single_batch_bytes = 8192u;
    const sync::PullResponse first = engine.handle_pull(request);
    const sync::PullResponse second = engine.handle_pull(request);
    if (first.ok || second.ok || first.is_full_snapshot ||
        !first.snapshot_chunk.snapshot_id.empty() ||
        !second.snapshot_chunk.snapshot_id.empty() ||
        first.error_code !=
            sync::SyncResponseErrorCode::SnapshotLogicalStateUnsupported ||
        second.error_code !=
            sync::SyncResponseErrorCode::SnapshotLogicalStateUnsupported ||
        first.error_retryable || second.error_retryable ||
        kv_or_throw(conn, documents, std::string("document"),
                    "empty ordered snapshot source value") != "raw-value") {
        throw std::runtime_error(
            "complete snapshot accepted empty ordered logical state");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_complete_snapshot_with_logical_outbox_state() {
    using namespace mdbxc;
    const std::string p =
        "test_engine_complete_snapshot_logical_outbox_state.mdbx";
    cleanup(p);

    const sync::NodeId source_node = make_node(0xAB);
    const sync::DbId db_id = make_node(0xDB);
    const sync::DbId destination = make_node(0xEB);
    sync::FullSnapshotExportOptions options;
    options.replacement_scope = sync::FullSnapshotScope::CompleteUserDatabase;
    options.max_materialized_operations = 16u;
    options.max_materialized_bytes = 4096u;
    options.max_active_sessions = 1u;

    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    documents.insert_or_assign("document", "raw-value");
    engine.enqueue_logical_delivery(destination, destination, sync::LogicalChangeFrame());
    {
        auto txn = conn->transaction(TransactionMode::READ_ONLY);
        sync::SchemaRegistryStore schemas(conn->env_handle());
        if (schemas.has_entries(txn.handle())) {
            throw std::runtime_error(
                "logical outbox unexpectedly registered a schema");
        }
    }

    sync::PullRequest request;
    request.requester = make_node(0xCB);
    request.db_id = db_id;
    request.request_full_snapshot = true;
    request.max_bytes = 8192u;
    request.max_single_batch_bytes = 8192u;
    const sync::PullResponse response = engine.handle_pull(request);
    if (response.ok || response.is_full_snapshot ||
        !response.snapshot_chunk.snapshot_id.empty() ||
        response.error_code !=
            sync::SyncResponseErrorCode::SnapshotLogicalStateUnsupported ||
        response.error_retryable ||
        kv_or_throw(conn, documents, std::string("document"),
                    "logical outbox snapshot source value") != "raw-value") {
        throw std::runtime_error(
            "complete snapshot accepted logical outbox state");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_complete_snapshot_with_frontier_only() {
    using namespace mdbxc;
    const std::string p = "test_engine_complete_snapshot_frontier_only.mdbx";
    cleanup(p);
    const sync::NodeId source_node = make_node(0xAE);
    const sync::NodeId frontier_origin = make_node(0xBE);
    const sync::DbId db_id = make_node(0xDE);
    const sync::FullSnapshotExportOptions options =
        complete_snapshot_test_options();
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    documents.insert_or_assign("document", "raw-value");
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        sync::LogicalDeliveryOrderStore order(conn->env_handle());
        order.advance(txn.handle(), frontier_origin, 1u);
        txn.commit();
    }
    const sync::PullResponse response = request_complete_snapshot(
        engine, db_id, make_node(0xCE));
    require_logical_snapshot_rejected(response, "frontier-only state");
    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_complete_snapshot_with_watermark_only() {
    using namespace mdbxc;
    const std::string p = "test_engine_complete_snapshot_watermark_only.mdbx";
    cleanup(p);
    const sync::NodeId source_node = make_node(0xAF);
    const sync::NodeId watermark_origin = make_node(0xBF);
    const sync::DbId db_id = make_node(0xDF);
    const sync::FullSnapshotExportOptions options =
        complete_snapshot_test_options();
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    documents.insert_or_assign("document", "raw-value");
    if (engine.prune_logical_delivery_markers(watermark_origin, 1u) != 0u) {
        throw std::runtime_error("watermark-only setup removed a marker");
    }
    const sync::PullResponse response = request_complete_snapshot(
        engine, db_id, make_node(0xCF));
    require_logical_snapshot_rejected(response, "watermark-only state");
    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_complete_snapshot_with_malformed_frontier() {
    using namespace mdbxc;
    const std::string p = "test_engine_complete_snapshot_bad_frontier.mdbx";
    cleanup(p);
    const sync::NodeId source_node = make_node(0xB0);
    const sync::NodeId frontier_origin = make_node(0xC0);
    const sync::DbId db_id = make_node(0xE0);
    const sync::FullSnapshotExportOptions options =
        complete_snapshot_test_options();
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    documents.insert_or_assign("document", "raw-value");
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        sync::LogicalDeliveryOrderStore order(conn->env_handle());
        order.open(txn.handle());
        MDBX_dbi dbi = 0;
        check_mdbx(mdbx_dbi_open(
                       txn.handle(), "_mdbxc_logical_delivery_order",
                       static_cast<MDBX_db_flags_t>(0), &dbi),
                   "failed to reopen frontier fixture DBI");
        std::vector<std::uint8_t> bad_value(1u, 0xFFu);
        MDBX_val key = {
            const_cast<std::uint8_t*>(frontier_origin.data()),
            frontier_origin.size()
        };
        MDBX_val value = {
            bad_value.empty() ? nullptr : &bad_value[0], bad_value.size()
        };
        check_mdbx(mdbx_put(txn.handle(), dbi, &key, &value, MDBX_UPSERT),
                   "failed to create malformed frontier fixture");
        txn.commit();
    }
    const sync::PullResponse response = request_complete_snapshot(
        engine, db_id, make_node(0xD0));
    require_logical_snapshot_rejected(response, "malformed frontier");
    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_complete_snapshot_with_malformed_outbox_metadata() {
    using namespace mdbxc;
    const std::string p = "test_engine_complete_snapshot_bad_outbox.mdbx";
    cleanup(p);
    const sync::NodeId source_node = make_node(0xB1);
    const sync::DbId destination = make_node(0xE1);
    const sync::DbId db_id = make_node(0xE2);
    const sync::FullSnapshotExportOptions options =
        complete_snapshot_test_options();
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    documents.insert_or_assign("document", "raw-value");
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        sync::LogicalOutboxStore outbox(conn->env_handle());
        outbox.open(txn.handle());
        const MDBX_dbi dbi = outbox.handle(txn.handle());
        std::vector<std::uint8_t> key;
        key.push_back(1u);
        key.push_back(0u);
        key.insert(key.end(), destination.begin(), destination.end());
        std::vector<std::uint8_t> bad_value(1u, 0xFFu);
        MDBX_val raw_key = {
            key.empty() ? nullptr : &key[0], key.size()
        };
        MDBX_val raw_value = {
            bad_value.empty() ? nullptr : &bad_value[0], bad_value.size()
        };
        check_mdbx(mdbx_put(txn.handle(), dbi, &raw_key, &raw_value,
                            MDBX_UPSERT),
                   "failed to create malformed outbox fixture");
        txn.commit();
    }
    const sync::PullResponse response = request_complete_snapshot(
        engine, db_id, make_node(0xD1));
    require_logical_snapshot_rejected(response, "malformed outbox metadata");
    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_complete_snapshot_with_malformed_watermark() {
    using namespace mdbxc;
    const std::string p = "test_engine_complete_snapshot_bad_watermark.mdbx";
    cleanup(p);
    const sync::NodeId source_node = make_node(0xB2);
    const sync::NodeId watermark_origin = make_node(0xC2);
    const sync::DbId db_id = make_node(0xE3);
    const sync::FullSnapshotExportOptions options =
        complete_snapshot_test_options();
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    documents.insert_or_assign("document", "raw-value");
    if (engine.prune_logical_delivery_markers(watermark_origin, 1u) != 0u) {
        throw std::runtime_error("malformed watermark setup removed a marker");
    }
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        MDBX_dbi dbi = 0;
        check_mdbx(mdbx_dbi_open(
                       txn.handle(), "_mdbxc_logical_delivery_watermarks",
                       static_cast<MDBX_db_flags_t>(0), &dbi),
                   "failed to reopen watermark fixture DBI");
        std::vector<std::uint8_t> bad_value(1u, 0xFFu);
        MDBX_val key = {
            const_cast<std::uint8_t*>(watermark_origin.data()),
            watermark_origin.size()
        };
        MDBX_val value = {
            bad_value.empty() ? nullptr : &bad_value[0], bad_value.size()
        };
        check_mdbx(mdbx_put(txn.handle(), dbi, &key, &value, MDBX_UPSERT),
                   "failed to create malformed watermark fixture");
        txn.commit();
    }
    const sync::PullResponse response = request_complete_snapshot(
        engine, db_id, make_node(0xD2));
    require_logical_snapshot_rejected(response, "malformed watermark");
    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_complete_snapshot_with_malformed_replay_marker() {
    using namespace mdbxc;
    const std::string p = "test_engine_complete_snapshot_bad_replay_marker.mdbx";
    cleanup(p);
    const sync::NodeId source_node = make_node(0xB3);
    const sync::DbId db_id = make_node(0xE4);
    const sync::FullSnapshotExportOptions options =
        complete_snapshot_test_options();
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    documents.insert_or_assign("document", "raw-value");
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        sync::LogicalDeliveryStore delivery(conn->env_handle());
        delivery.open(txn.handle());
        const MDBX_dbi dbi = delivery.handle(txn.handle());
        std::vector<std::uint8_t> bad_key(1u, 0x01u);
        std::vector<std::uint8_t> bad_value(1u, 0xFFu);
        MDBX_val key = {
            bad_key.empty() ? nullptr : &bad_key[0], bad_key.size()
        };
        MDBX_val value = {
            bad_value.empty() ? nullptr : &bad_value[0], bad_value.size()
        };
        check_mdbx(mdbx_put(txn.handle(), dbi, &key, &value, MDBX_UPSERT),
                   "failed to create malformed replay marker fixture");
        txn.commit();
    }
    const sync::PullResponse response = request_complete_snapshot(
        engine, db_id, make_node(0xD3));
    require_logical_snapshot_rejected(response, "malformed replay marker");
    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_complete_snapshot_with_malformed_schema_record() {
    using namespace mdbxc;
    const std::string p = "test_engine_complete_snapshot_bad_schema_record.mdbx";
    cleanup(p);
    const sync::NodeId source_node = make_node(0xB4);
    const sync::DbId db_id = make_node(0xE5);
    const sync::FullSnapshotExportOptions options =
        complete_snapshot_test_options();
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    documents.insert_or_assign("document", "raw-value");
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        sync::SchemaRegistryStore schemas(conn->env_handle());
        schemas.open(txn.handle());
        const MDBX_dbi dbi = schemas.handle(txn.handle());
        const std::string schema_id = "malformed.schema";
        std::vector<std::uint8_t> bad_value(1u, 0xFFu);
        MDBX_val key = {
            const_cast<char*>(schema_id.data()), schema_id.size()
        };
        MDBX_val value = {
            bad_value.empty() ? nullptr : &bad_value[0], bad_value.size()
        };
        check_mdbx(mdbx_put(txn.handle(), dbi, &key, &value, MDBX_UPSERT),
                   "failed to create malformed schema fixture");
        txn.commit();
    }
    const sync::PullResponse response = request_complete_snapshot(
        engine, db_id, make_node(0xD4));
    require_logical_snapshot_rejected(response, "malformed schema record");
    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_complete_snapshot_with_malformed_outbox_envelope() {
    using namespace mdbxc;
    const std::string p = "test_engine_complete_snapshot_bad_outbox_envelope.mdbx";
    cleanup(p);
    const sync::NodeId source_node = make_node(0xB5);
    const sync::DbId destination = make_node(0xE6);
    const sync::DbId db_id = make_node(0xE7);
    const sync::FullSnapshotExportOptions options =
        complete_snapshot_test_options();
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn, sync::ConflictPolicy::Reject, options);
    engine.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    documents.insert_or_assign("document", "raw-value");
    engine.enqueue_logical_delivery(destination, destination, sync::LogicalChangeFrame());
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        sync::LogicalOutboxStore outbox(conn->env_handle());
        outbox.open(txn.handle());
        const MDBX_dbi dbi = outbox.handle(txn.handle());
        std::vector<std::uint8_t> key;
        key.push_back(1u);
        key.push_back(1u);
        key.insert(key.end(), destination.begin(), destination.end());
        for (int shift = 7; shift >= 0; --shift) {
            key.push_back(static_cast<std::uint8_t>(
                (static_cast<std::uint64_t>(1u) >> (shift * 8)) & 0xffu));
        }
        std::vector<std::uint8_t> bad_value(1u, 0xFFu);
        MDBX_val raw_key = {
            key.empty() ? nullptr : &key[0], key.size()
        };
        MDBX_val raw_value = {
            bad_value.empty() ? nullptr : &bad_value[0], bad_value.size()
        };
        check_mdbx(mdbx_put(txn.handle(), dbi, &raw_key, &raw_value,
                            MDBX_UPSERT),
                   "failed to create malformed outbox envelope fixture");
        txn.commit();
    }
    const sync::PullResponse response = request_complete_snapshot(
        engine, db_id, make_node(0xD5));
    require_logical_snapshot_rejected(response, "malformed outbox envelope");
    conn->disconnect();
    cleanup(p);
}

mdbxc::sync::FullSnapshotChunk make_import_chunk(
        const mdbxc::sync::NodeId& source_node,
        const mdbxc::sync::DbId& db_id,
        const std::string& snapshot_id,
        std::uint64_t chunk_index,
        bool has_more,
        mdbxc::sync::FullSnapshotScope scope =
            mdbxc::sync::FullSnapshotScope::ManifestOnly) {
    mdbxc::sync::FullSnapshotChunk chunk;
    chunk.source_node_id = source_node;
    chunk.source_db_uuid = db_id;
    chunk.snapshot_id = snapshot_id;
    chunk.chunk_index = chunk_index;
    chunk.has_more = has_more;
    chunk.replacement_scope = scope;
    chunk.continuation = has_more ? "next" : std::string();
    mdbxc::sync::FullSnapshotManifestEntry entry;
    entry.dbi_name = "documents";
    entry.dbi_flags = 0u;
    chunk.manifest.push_back(entry);
    chunk.batch.origin_node_id = source_node;
    chunk.batch.version = mdbxc::sync::ChangeBatchCodec::batch_version();
    chunk.batch.seq = 0u;
    chunk.batch.batch_flags = has_more
        ? static_cast<std::uint32_t>(mdbxc::sync::BATCH_HAS_MORE)
        : static_cast<std::uint32_t>(mdbxc::sync::BATCH_NONE);
    return chunk;
}

void append_import_put(mdbxc::sync::FullSnapshotChunk& chunk,
                       const std::string& key,
                       const std::string& value) {
    mdbxc::sync::ChangeOp op;
    op.op_type = mdbxc::sync::ChangeOpType::Put;
    op.dbi_name = "documents";
    op.storage_key.assign(key.begin(), key.end());
    op.value.assign(value.begin(), value.end());
    chunk.batch.ops.push_back(op);
}

void append_import_clear(mdbxc::sync::FullSnapshotChunk& chunk) {
    mdbxc::sync::ChangeOp op;
    op.op_type = mdbxc::sync::ChangeOpType::ClearTable;
    op.dbi_name = "documents";
    chunk.batch.ops.push_back(op);
}

void test_engine_imports_full_snapshot_and_bootstraps_cursor() {
    using namespace mdbxc;
    const std::string source_path = "test_engine_full_snapshot_source.mdbx";
    const std::string replica_path = "test_engine_full_snapshot_replica.mdbx";
    cleanup(source_path);
    cleanup(replica_path);

    const sync::NodeId source_node = make_node(0xA3);
    const sync::NodeId replica_node = make_node(0xB3);
    const sync::DbId db_id = make_node(0xD3);
    sync::FullSnapshotExportOptions options;
    options.replacement_scope = sync::FullSnapshotScope::CompleteUserDatabase;
    options.max_materialized_operations = 16u;
    options.max_materialized_bytes = 4096u;

    std::shared_ptr<Connection> source_conn = open_env(source_path);
    sync::SyncEngine source(source_conn, sync::ConflictPolicy::Reject, options);
    source.initialize_local_identity(source_node, db_id);
    KeyValueTable<std::string, std::string> source_documents(
        source_conn, "documents");
    source_documents.insert_or_assign("one", "1");
    source_documents.insert_or_assign("two", "2");
    KeyValueTable<std::string, std::string> source_audit(
        source_conn, "audit");
    source_audit.insert_or_assign("event", "snapshot");
    {
        auto txn = source_conn->transaction(TransactionMode::WRITABLE);
        sync::ChangeLogStore changelog(source_conn->env_handle());
        changelog.open(txn.handle());
        append_raw_batch(changelog, txn.handle(), source_node, 1u,
                         "documents", 0x11u);
        txn.commit();
    }

    std::shared_ptr<Connection> replica_conn = open_env(replica_path);
    sync::SyncEngine replica(replica_conn);
    replica.initialize_local_identity(replica_node, db_id);

    sync::PullRequest request;
    request.requester = replica_node;
    request.db_id = db_id;
    request.request_full_snapshot = true;
    request.max_bytes = 1u;
    request.max_single_batch_bytes = 8192u;

    sync::PullResponse response = source.handle_pull(request);
    if (!response.ok || !response.is_full_snapshot || !response.has_more) {
        throw std::runtime_error("full snapshot source did not paginate");
    }
    const sync::FullSnapshotImportResult first =
        replica.apply_full_snapshot_chunk(response.snapshot_chunk);
    if (first.completed || first.next_chunk_index != 1u) {
        throw std::runtime_error("first full snapshot page was not staged");
    }
    {
        auto txn = replica_conn->transaction(TransactionMode::READ_ONLY);
        MDBX_dbi dbi = 0;
        const int rc = mdbx_dbi_open(
            txn.handle(), "documents", static_cast<MDBX_db_flags_t>(0), &dbi);
        if (rc != MDBX_NOTFOUND) {
            throw std::runtime_error(
                "full snapshot staged a destination DBI before final page");
        }
    }

    sync::FullSnapshotImportResult result = first;
    while (response.has_more) {
        request.full_snapshot_id = response.snapshot_chunk.snapshot_id;
        request.full_snapshot_continuation = response.snapshot_chunk.continuation;
        response = source.handle_pull(request);
        if (!response.ok || !response.is_full_snapshot) {
            throw std::runtime_error("full snapshot continuation failed");
        }
        result = replica.apply_full_snapshot_chunk(response.snapshot_chunk);
    }
    if (!result.completed) {
        throw std::runtime_error("final full snapshot page did not commit");
    }

    KeyValueTable<std::string, std::string> replica_documents(
        replica_conn, "documents");
    KeyValueTable<std::string, std::string> replica_audit(
        replica_conn, "audit");
    if (kv_or_throw(replica_conn, replica_documents, std::string("one"),
                    "imported one") != "1" ||
        kv_or_throw(replica_conn, replica_documents, std::string("two"),
                    "imported two") != "2" ||
        kv_or_throw(replica_conn, replica_audit, std::string("event"),
                    "imported audit") != "snapshot") {
        throw std::runtime_error("full snapshot replica content is wrong");
    }
    if (replica.applied_cursor().last_seq_for(source_node) != 1u) {
        throw std::runtime_error(
            "full snapshot did not bootstrap the source applied cursor");
    }

    source_conn->disconnect();
    replica_conn->disconnect();
    cleanup(source_path);
    cleanup(replica_path);
}

void test_engine_manifest_only_snapshot_does_not_bootstrap_cursor() {
    using namespace mdbxc;
    const std::string p = "test_engine_manifest_only_no_cursor.mdbx";
    cleanup(p);

    const sync::NodeId source_node = make_node(0xA9);
    const sync::NodeId replica_node = make_node(0xB9);
    const sync::DbId db_id = make_node(0xD9);
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);

    sync::FullSnapshotChunk chunk = make_import_chunk(
        source_node, db_id, "manifest-only", 0u, false);
    chunk.source_tail.last_seq_by_origin[source_node] = 2u;
    append_import_clear(chunk);
    append_import_put(chunk, "document", "value");
    if (!engine.apply_full_snapshot_chunk(chunk).completed) {
        throw std::runtime_error("ManifestOnly snapshot did not complete");
    }
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    if (kv_or_throw(conn, documents, std::string("document"),
                    "ManifestOnly document") != "value" ||
        engine.applied_cursor().last_seq_for(source_node) != 0u) {
        throw std::runtime_error(
            "ManifestOnly snapshot bootstrapped global applied progress");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_complete_snapshot_rejects_destination_identity_in_tail() {
    using namespace mdbxc;
    const std::string p = "test_engine_complete_snapshot_reused_node.mdbx";
    cleanup(p);

    const sync::NodeId source_node = make_node(0xAA);
    const sync::NodeId replica_node = make_node(0xBA);
    const sync::DbId db_id = make_node(0xDA);
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);

    sync::FullSnapshotChunk chunk = make_import_chunk(
        source_node, db_id, "reused-node", 0u, false,
        sync::FullSnapshotScope::CompleteUserDatabase);
    chunk.source_tail.last_seq_by_origin[replica_node] = 5u;
    append_import_clear(chunk);
    append_import_put(chunk, "document", "must-not-appear");
    bool rejected = false;
    try {
        (void)engine.apply_full_snapshot_chunk(chunk);
    } catch (const std::logic_error&) {
        rejected = true;
    }
    if (!rejected) {
        throw std::runtime_error(
            "complete snapshot accepted destination identity from source tail");
    }
    {
        auto txn = conn->transaction(TransactionMode::READ_ONLY);
        MDBX_dbi dbi = 0;
        const int rc = mdbx_dbi_open(
            txn.handle(), "documents", static_cast<MDBX_db_flags_t>(0), &dbi);
        if (rc != MDBX_NOTFOUND) {
            throw std::runtime_error(
                "reused-node snapshot mutated a destination DBI");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_full_snapshot_import_fails_closed_before_final_page() {
    using namespace mdbxc;
    const std::string p = "test_engine_full_snapshot_import_failure.mdbx";
    cleanup(p);

    const sync::NodeId source_node = make_node(0xA4);
    const sync::NodeId replica_node = make_node(0xB4);
    const sync::DbId db_id = make_node(0xD4);
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);

    sync::FullSnapshotChunk first = make_import_chunk(
        source_node, db_id, "snapshot-a", 0u, true);
    append_import_clear(first);
    const sync::FullSnapshotImportResult staged =
        engine.apply_full_snapshot_chunk(first);
    if (staged.completed) {
        throw std::runtime_error("non-final snapshot page committed");
    }

    sync::FullSnapshotChunk mismatched = make_import_chunk(
        source_node, db_id, "snapshot-b", 1u, false);
    append_import_put(mismatched, "new", "value");
    bool rejected = false;
    try {
        (void)engine.apply_full_snapshot_chunk(mismatched);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    if (!rejected) {
        throw std::runtime_error("mismatched snapshot continuation was accepted");
    }
    {
        auto txn = conn->transaction(TransactionMode::READ_ONLY);
        MDBX_dbi dbi = 0;
        const int rc = mdbx_dbi_open(
            txn.handle(), "documents", static_cast<MDBX_db_flags_t>(0), &dbi);
        if (rc != MDBX_NOTFOUND) {
            throw std::runtime_error(
                "failed snapshot import mutated a destination DBI");
        }
    }

    sync::FullSnapshotChunk restart = make_import_chunk(
        source_node, db_id, "snapshot-c", 0u, false);
    append_import_clear(restart);
    append_import_put(restart, "new", "value");
    if (!engine.apply_full_snapshot_chunk(restart).completed) {
        throw std::runtime_error("snapshot restart after failure did not commit");
    }
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    if (kv_or_throw(conn, documents, std::string("new"),
                    "restarted snapshot") != "value") {
        throw std::runtime_error("restarted snapshot content is wrong");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_full_snapshot_rejects_nonfresh_destination() {
    using namespace mdbxc;
    const std::string p = "test_engine_full_snapshot_nonfresh.mdbx";
    cleanup(p);

    const sync::NodeId source_node = make_node(0xA5);
    const sync::NodeId replica_node = make_node(0xB5);
    const sync::DbId db_id = make_node(0xD5);
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);
    KeyValueTable<std::string, std::string> documents(conn, "documents");
    documents.insert_or_assign("old", "preserved");

    sync::FullSnapshotChunk chunk = make_import_chunk(
        source_node, db_id, "snapshot-nonfresh", 0u, false);
    append_import_clear(chunk);
    append_import_put(chunk, "new", "must-not-appear");
    bool rejected = false;
    try {
        (void)engine.apply_full_snapshot_chunk(chunk);
    } catch (const std::logic_error&) {
        rejected = true;
    }
    if (!rejected ||
        kv_or_throw(conn, documents, std::string("old"),
                    "preserved destination") != "preserved" ||
        kv_has(conn, documents, std::string("new"))) {
        throw std::runtime_error(
            "nonfresh destination was changed by full snapshot import");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_full_snapshot_import_bounds_fail_closed() {
    using namespace mdbxc;
    const std::string p = "test_engine_full_snapshot_import_bounds.mdbx";
    cleanup(p);

    const sync::NodeId source_node = make_node(0xA6);
    const sync::NodeId replica_node = make_node(0xB6);
    const sync::DbId db_id = make_node(0xD6);
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);
    sync::FullSnapshotImportOptions limits;
    limits.max_staged_operations = 1u;
    limits.max_staged_bytes = 4096u;
    engine.set_full_snapshot_import_options(limits);

    sync::FullSnapshotChunk chunk = make_import_chunk(
        source_node, db_id, "snapshot-bounds", 0u, false);
    append_import_clear(chunk);
    append_import_put(chunk, "new", "value");
    bool rejected = false;
    try {
        (void)engine.apply_full_snapshot_chunk(chunk);
    } catch (const std::length_error&) {
        rejected = true;
    }
    if (!rejected) {
        throw std::runtime_error("full snapshot import staging bound was ignored");
    }
    {
        auto txn = conn->transaction(TransactionMode::READ_ONLY);
        MDBX_dbi dbi = 0;
        const int rc = mdbx_dbi_open(
            txn.handle(), "documents", static_cast<MDBX_db_flags_t>(0), &dbi);
        if (rc != MDBX_NOTFOUND) {
            throw std::runtime_error(
                "bounded full snapshot import created a destination DBI");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_full_snapshot_import_rejects_invalid_replacement_plan() {
    using namespace mdbxc;
    const std::string p = "test_engine_full_snapshot_invalid_plan.mdbx";
    cleanup(p);

    const sync::NodeId source_node = make_node(0xA8);
    const sync::NodeId replica_node = make_node(0xB8);
    const sync::DbId db_id = make_node(0xD8);
    std::shared_ptr<Connection> conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(replica_node, db_id);

    sync::FullSnapshotChunk put_before_clear = make_import_chunk(
        source_node, db_id, "snapshot-put-before-clear", 0u, false);
    append_import_put(put_before_clear, "key", "value");
    bool rejected = false;
    try {
        (void)engine.apply_full_snapshot_chunk(put_before_clear);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    if (!rejected) {
        throw std::runtime_error("snapshot accepted Put before ClearTable");
    }

    sync::FullSnapshotChunk first_clear = make_import_chunk(
        source_node, db_id, "snapshot-duplicate-clear", 0u, true);
    append_import_clear(first_clear);
    (void)engine.apply_full_snapshot_chunk(first_clear);
    sync::FullSnapshotChunk duplicate_clear = make_import_chunk(
        source_node, db_id, "snapshot-duplicate-clear", 1u, false);
    append_import_clear(duplicate_clear);
    rejected = false;
    try {
        (void)engine.apply_full_snapshot_chunk(duplicate_clear);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    if (!rejected) {
        throw std::runtime_error("snapshot accepted duplicate ClearTable");
    }

    sync::FullSnapshotChunk missing_clear = make_import_chunk(
        source_node, db_id, "snapshot-missing-clear", 0u, false);
    sync::FullSnapshotManifestEntry secondary;
    secondary.dbi_name = "secondary";
    missing_clear.manifest.push_back(secondary);
    append_import_clear(missing_clear);
    rejected = false;
    try {
        (void)engine.apply_full_snapshot_chunk(missing_clear);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    if (!rejected) {
        throw std::runtime_error("snapshot omitted a manifest ClearTable");
    }

    sync::FullSnapshotChunk self_origin = make_import_chunk(
        replica_node, db_id, "snapshot-self-origin", 0u, false);
    append_import_clear(self_origin);
    bool rejected_self_origin = false;
    try {
        (void)engine.apply_full_snapshot_chunk(self_origin);
    } catch (const std::logic_error&) {
        rejected_self_origin = true;
    }
    if (!rejected_self_origin) {
        throw std::runtime_error("snapshot accepted matching source and destination node");
    }

    {
        auto txn = conn->transaction(TransactionMode::READ_ONLY);
        MDBX_dbi dbi = 0;
        const int rc = mdbx_dbi_open(
            txn.handle(), "documents", static_cast<MDBX_db_flags_t>(0), &dbi);
        if (rc != MDBX_NOTFOUND) {
            throw std::runtime_error(
                "invalid snapshot replacement plan created destination DBI");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_recovers_logical_baseline_atomically() {
    using namespace mdbxc;
    typedef sync::KeyValueLogicalStringCodec<std::string> StringCodec;
    typedef sync::KeyValueTableLogicalAdapter<
        std::string, std::string, StringCodec, StringCodec> LogicalAdapter;

    const std::string source_path = "test_engine_logical_recovery_source.mdbx";
    const std::string replica_path = "test_engine_logical_recovery_replica.mdbx";
    const std::string rejected_path = "test_engine_logical_recovery_rejected.mdbx";
    const std::string schema_id = "app.logical_recovery.key_value.v1";
    cleanup(source_path);
    cleanup(replica_path);
    cleanup(rejected_path);

    const sync::NodeId source_node = make_node(0x91);
    const sync::NodeId replica_node = make_node(0xA1);
    const sync::NodeId rejected_node = make_node(0xB1);
    const sync::DbId db_id = make_node(0xD1);
    sync::FullSnapshotExportOptions options;
    options.replacement_scope = sync::FullSnapshotScope::CompleteUserDatabase;
    options.max_materialized_operations = 32u;
    options.max_materialized_bytes = 8192u;

    std::shared_ptr<Connection> source_conn = open_env(source_path);
    std::shared_ptr<Connection> replica_conn = open_env(replica_path);
    std::shared_ptr<Connection> rejected_conn = open_env(rejected_path);
    sync::SyncEngine source(source_conn, sync::ConflictPolicy::Reject, options);
    sync::SyncEngine replica(replica_conn);
    sync::SyncEngine rejected(rejected_conn);
    source.initialize_local_identity(source_node, db_id);
    replica.initialize_local_identity(replica_node, db_id);
    rejected.initialize_local_identity(rejected_node, db_id);

    KeyValueTable<std::string, std::string> source_documents(
        source_conn, "documents");
    KeyValueTable<std::string, std::string> replica_documents(
        replica_conn, "documents");
    KeyValueTable<std::string, std::string> rejected_documents(
        rejected_conn, "documents");
    LogicalAdapter source_adapter(source_documents, schema_id);
    LogicalAdapter replica_adapter(replica_documents, schema_id);
    LogicalAdapter rejected_adapter(rejected_documents, schema_id);
    sync::LogicalSchemaRecord record;
    record.dbi_name = "documents";
    record.kind = sync::LogicalTableKind::KeyValue;
    record.schema_version = 1u;
    record.dbi_names.push_back("documents");
    source.initialize_logical_adapter_schema(source_adapter, record);
    source.register_logical_adapter(source_adapter);
    replica.register_logical_adapter(replica_adapter);
    rejected.register_logical_adapter(rejected_adapter);

    {
        std::unique_ptr<LogicalAdapter::LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        session->insert_or_assign("first", "one");
        const sync::LogicalDeliveryEnvelope envelope =
            session->commit_to_outbox(source, db_id, replica_node);
        if (envelope.origin_sequence != 1u) {
            throw std::runtime_error("logical recovery source did not enqueue sequence one");
        }
    }

    sync::PullRequest raw_request;
    raw_request.requester = replica_node;
    raw_request.db_id = db_id;
    raw_request.request_full_snapshot = true;
    raw_request.max_bytes = 8192u;
    raw_request.max_single_batch_bytes = 8192u;
    const sync::PullResponse raw_rejected = source.handle_pull(raw_request);
    if (raw_rejected.ok || raw_rejected.error_code !=
            sync::SyncResponseErrorCode::SnapshotLogicalStateUnsupported) {
        throw std::runtime_error("logical recovery relaxed the raw snapshot guard");
    }

    sync::LogicalRecoveryRequest request;
    request.requester = replica_node;
    request.max_bytes = 8192u;
    request.max_single_batch_bytes = 8192u;
    const sync::LogicalRecoveryResponse response =
        source.handle_logical_recovery(request);
    if (!response.ok || response.has_more || !response.has_baseline) {
        throw std::runtime_error("logical recovery source did not return final baseline");
    }

    sync::FullSnapshotExportOptions exact_budget = options;
    exact_budget.max_materialized_operations = 4u;
    source.set_full_snapshot_export_options(exact_budget);
    const sync::LogicalRecoveryResponse exact_budget_response =
        source.handle_logical_recovery(request);
    if (!exact_budget_response.ok || !exact_budget_response.has_baseline) {
        throw std::runtime_error(
            "logical recovery exact combined materialization budget was rejected");
    }
    sync::FullSnapshotExportOptions insufficient_budget = exact_budget;
    insufficient_budget.max_materialized_operations = 3u;
    source.set_full_snapshot_export_options(insufficient_budget);
    const sync::LogicalRecoveryResponse insufficient_budget_response =
        source.handle_logical_recovery(request);
    if (insufficient_budget_response.ok ||
        insufficient_budget_response.error_code !=
            sync::SyncResponseErrorCode::BatchTooLarge) {
        throw std::runtime_error(
            "logical recovery accepted an insufficient combined materialization budget");
    }
    source.set_full_snapshot_export_options(options);

    sync::LogicalRecoveryBaseline malformed = response.baseline;
    malformed.source_db_uuid = make_node(0xE1);
    bool rejected_baseline = false;
    try {
        (void)rejected.apply_logical_recovery_chunk(
            response.snapshot_chunk, &malformed);
    } catch (const std::invalid_argument&) {
        rejected_baseline = true;
    }
    if (!rejected_baseline || kv_has(rejected_conn, rejected_documents,
                                     std::string("first"))) {
        throw std::runtime_error(
            "malformed logical recovery baseline mutated destination state");
    }

    sync::LogicalRecoveryBaseline truncated = response.baseline;
    truncated.source_outbox_known_tail = 2u;
    bool rejected_truncated_suffix = false;
    try {
        (void)rejected.apply_logical_recovery_chunk(
            response.snapshot_chunk, &truncated);
    } catch (const std::invalid_argument&) {
        rejected_truncated_suffix = true;
    }
    if (!rejected_truncated_suffix || kv_has(rejected_conn, rejected_documents,
                                              std::string("first"))) {
        throw std::runtime_error(
            "truncated logical recovery suffix mutated destination state");
    }

    sync::FullSnapshotImportOptions import_limits;
    import_limits.max_staged_operations = 2u;
    import_limits.max_staged_bytes = 8192u;
    rejected.set_full_snapshot_import_options(import_limits);
    bool rejected_import_budget = false;
    try {
        (void)rejected.apply_logical_recovery_chunk(
            response.snapshot_chunk, &response.baseline);
    } catch (const std::length_error&) {
        rejected_import_budget = true;
    }
    if (!rejected_import_budget || kv_has(rejected_conn, rejected_documents,
                                          std::string("first"))) {
        throw std::runtime_error(
            "logical recovery baseline exceeded import budget after mutation");
    }

    if (!replica.apply_logical_recovery_chunk(
            response.snapshot_chunk, &response.baseline).completed ||
        kv_or_throw(replica_conn, replica_documents, std::string("first"),
                    "logical recovery first value") != "one") {
        throw std::runtime_error("logical recovery did not import physical state");
    }
    {
        auto txn = replica_conn->transaction(TransactionMode::READ_ONLY);
        sync::SchemaRegistryStore schemas(replica_conn->env_handle());
        sync::LogicalDeliveryOrderStore order(replica_conn->env_handle());
        if (!schemas.has_entries(txn.handle()) ||
            order.last_applied(txn.handle(), source_node) != 1u) {
            throw std::runtime_error(
                "logical recovery did not restore schema or outbox frontier");
        }
    }

    {
        std::unique_ptr<LogicalAdapter::LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        session->insert_or_assign("second", "two");
        (void)session->commit_to_outbox(source, db_id, replica_node);
    }
    sync::DirectSyncPeer peer(&replica);
    const sync::LogicalDeliveryDispatchResult dispatched =
        source.deliver_pending_logical_deliveries(peer, db_id, replica_node);
    if (!dispatched.ok || !source.pending_logical_deliveries(
            db_id, replica_node).empty() ||
        kv_or_throw(replica_conn, replica_documents, std::string("second"),
                    "logical recovery continued delivery") != "two") {
        throw std::runtime_error(
            "logical recovery did not continue ordered delivery after baseline");
    }

    source_conn->disconnect();
    replica_conn->disconnect();
    rejected_conn->disconnect();
    cleanup(source_path);
    cleanup(replica_path);
    cleanup(rejected_path);
}

void test_engine_cancels_direct_logical_recovery_materialization() {
    using namespace mdbxc;
    const std::string source_path =
        "test_engine_cancel_direct_logical_recovery_source.mdbx";
    cleanup(source_path);

    const sync::NodeId source_node = make_node(0x92);
    const sync::NodeId replica_node = make_node(0xA2);
    const sync::DbId db_id = make_node(0xD2);
    sync::FullSnapshotExportOptions options;
    options.replacement_scope = sync::FullSnapshotScope::CompleteUserDatabase;
    options.max_materialized_operations = 15000u;
    options.max_materialized_bytes = 128ULL * 1024ULL * 1024ULL;

    std::shared_ptr<Connection> source_conn = open_env(source_path);
    sync::SyncEngine source(source_conn, sync::ConflictPolicy::Reject, options);
    source.initialize_local_identity(source_node, db_id);
    KeyValueTable<int, std::string> documents(source_conn, "documents");
    const std::string payload(4096u, 'x');
    {
        auto txn = source_conn->transaction(TransactionMode::WRITABLE);
        for (int i = 0; i < 10000; ++i) {
            documents.insert_or_assign(i, payload, txn.handle());
        }
        txn.commit();
    }

    sync::DirectSyncPeer peer(&source);
    sync::LogicalRecoveryRequest request;
    request.requester = replica_node;
    request.max_bytes = 8192u;
    request.max_single_batch_bytes = 8192u;
    sync::CancellationSource cancellation;
    const sync::CancellationToken cancellation_token = cancellation.token();
    sync::LogicalRecoveryResponse response;
    std::thread recovery([&peer, &request, &cancellation_token, &response]() {
        response = peer.logical_recovery_with_cancel(
            request, &cancellation_token);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    cancellation.request_cancel();
    recovery.join();
    if (response.ok || !response.error_retryable ||
        response.error.find("cancelled") == std::string::npos) {
        throw std::runtime_error(
            "direct logical recovery did not stop materialization after cancellation");
    }

    source_conn->disconnect();
    cleanup(source_path);
}

void test_engine_changelog_page_rejects_full_snapshot_request() {
    using namespace mdbxc;
    const std::string p = "test_engine_changelog_full_snapshot_request.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xA1), make_node(0xD1));

    sync::PullRequest req;
    req.requester = make_node(0xB1);
    req.db_id = make_node(0xD1);
    req.request_full_snapshot = true;

    {
        auto txn = conn->transaction(TransactionMode::READ_ONLY);
        const sync::PullResponse resp =
            engine.pull_changelog_page(txn.handle(), 0, req);
        if (resp.ok) {
            throw std::runtime_error(
                "changelog page full snapshot request should be rejected");
        }
        if (!resp.batches.empty()) {
            throw std::runtime_error(
                "changelog page full snapshot rejection returned batches");
        }
        if (resp.error.find("request_full_snapshot") == std::string::npos) {
            throw std::runtime_error(
                "changelog page full snapshot rejection error is not explicit");
        }
        if (resp.error_code !=
                sync::SyncResponseErrorCode::UnsupportedFullSnapshot ||
            resp.error_retryable) {
            throw std::runtime_error(
                "changelog page full snapshot rejection code incorrect");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_pull_reports_snapshot_required_after_prune() {
    using namespace mdbxc;
    const std::string p = "test_engine_pull_snapshot_required_after_prune.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    const sync::NodeId local = make_node(0xA2);
    const sync::NodeId origin = make_node(0xB2);
    const sync::DbId db_id = make_node(0xD2);
    engine.initialize_local_identity(local, db_id);

    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        sync::ChangeLogStore log(conn->env_handle());
        log.open(txn.handle());
        append_raw_batch(log, txn.handle(), origin, 1, "t", 0x01);
        append_raw_batch(log, txn.handle(), origin, 2, "t", 0x02);
        append_raw_batch(log, txn.handle(), origin, 3, "t", 0x03);
        const std::size_t removed = log.prune_up_to(txn.handle(), origin, 2);
        if (removed != 2u) {
            throw std::runtime_error("test prune did not remove first two batches");
        }
        txn.commit();
    }

    sync::PullRequest req;
    req.requester = make_node(0xC2);
    req.db_id = db_id;

    const sync::PullResponse resp = engine.handle_pull(req);
    if (resp.ok) {
        throw std::runtime_error("pruned changelog pull should fail");
    }
    if (!resp.batches.empty() || resp.has_more) {
        throw std::runtime_error("snapshot-required pull returned batches");
    }
    if (resp.error_code != sync::SyncResponseErrorCode::SnapshotRequired ||
        resp.error_retryable) {
        throw std::runtime_error("snapshot-required pull code incorrect");
    }
    if (!resp.remote_tail_known ||
        resp.remote_tail.last_seq_for(origin) != 3u) {
        throw std::runtime_error("snapshot-required pull lost remote tail");
    }
    if (resp.error.find("earliest_retained_seq=3") == std::string::npos) {
        throw std::runtime_error("snapshot-required pull error lacks earliest seq");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_pull_max_bytes_is_soft_page_budget() {
    using namespace mdbxc;
    const std::string p = "test_engine_pull_soft_max_bytes.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    const sync::NodeId local = make_node(0xA3);
    const sync::NodeId origin = make_node(0xB3);
    const sync::DbId db_id = make_node(0xD3);
    engine.initialize_local_identity(local, db_id);

    const sync::ChangeBatch large =
        make_raw_batch_with_value_size(origin, 1, "kv", 0xA1, 2048u);
    const sync::ChangeBatch small =
        make_raw_batch_with_value_size(origin, 2, "kv", 0xA2, 8u);
    const std::size_t large_bytes =
        sync::ChangeBatchCodec::encode(large).size();

    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        sync::ChangeLogStore log(conn->env_handle());
        log.open(txn.handle());
        append_raw_batch(log, txn.handle(), large);
        append_raw_batch(log, txn.handle(), small);
        txn.commit();
    }

    sync::PullRequest req;
    req.requester = make_node(0xC3);
    req.db_id = db_id;
    req.max_bytes = 1;
    req.max_single_batch_bytes =
        static_cast<std::uint64_t>(large_bytes + 1024u);

    const sync::PullResponse resp = engine.handle_pull(req);
    if (!resp.ok) {
        throw std::runtime_error("soft max_bytes pull failed: " + resp.error);
    }
    if (resp.batches.size() != 1u || resp.batches[0].seq != 1u) {
        throw std::runtime_error(
            "soft max_bytes did not return the first oversized page batch");
    }
    if (!resp.has_more) {
        throw std::runtime_error(
            "soft max_bytes should report more retained batches");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_pull_rejects_oversized_single_batch() {
    using namespace mdbxc;
    const std::string p = "test_engine_pull_single_batch_limit.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    const sync::NodeId local = make_node(0xA4);
    const sync::NodeId origin = make_node(0xB4);
    const sync::DbId db_id = make_node(0xD4);
    engine.initialize_local_identity(local, db_id);

    const sync::ChangeBatch large =
        make_raw_batch_with_value_size(origin, 1, "kv", 0xA4, 2048u);
    const std::size_t large_bytes =
        sync::ChangeBatchCodec::encode(large).size();

    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        sync::ChangeLogStore log(conn->env_handle());
        log.open(txn.handle());
        append_raw_batch(log, txn.handle(), large);
        txn.commit();
    }

    sync::PullRequest req;
    req.requester = make_node(0xC4);
    req.db_id = db_id;
    req.max_single_batch_bytes =
        static_cast<std::uint64_t>(large_bytes - 1u);

    const sync::PullResponse resp = engine.handle_pull(req);
    if (resp.ok) {
        throw std::runtime_error(
            "oversized single batch pull should have failed");
    }
    if (!resp.batches.empty() || resp.has_more) {
        throw std::runtime_error(
            "oversized single batch rejection returned page data");
    }
    if (resp.error_code != sync::SyncResponseErrorCode::BatchTooLarge ||
        resp.error_retryable) {
        throw std::runtime_error(
            "oversized single batch rejection code incorrect");
    }
    if (resp.error.find("max_single_batch_bytes") == std::string::npos) {
        throw std::runtime_error(
            "oversized single batch rejection error lacks limit name");
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_handle_push_wrong_db_id() {
    using namespace mdbxc;
    const std::string p = "test_engine_push_wrong_db.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0xA0), make_node(0xD0));

    sync::ChangeBatch b;
    b.origin_node_id = make_node(0x20);
    b.seq = 1;
    sync::ChangeOp op;
    op.op_type = sync::ChangeOpType::Put;
    op.dbi_name = "t";
    op.storage_key = { 0x01 };
    op.value = { 0xAA };
    b.ops.push_back(op);

    sync::DirectSyncPeer peer(&engine);
    sync::PushRequest req;
    req.sender = make_node(0x20);
    req.db_id  = make_node(0xFF);  // wrong
    req.batches.push_back(b);

    const sync::PushResponse resp = peer.push(req);
    if (resp.ok) {
        throw std::runtime_error("push with wrong db_id should return ok=false");
    }
    if (resp.error_code != sync::SyncResponseErrorCode::DbIdMismatch ||
        resp.error_retryable) {
        throw std::runtime_error("push wrong db_id error code incorrect");
    }

    {
        KeyValueTable<std::uint8_t, std::uint8_t> t(conn, "t");
        if (kv_has(conn, t, static_cast<std::uint8_t>(1))) {
            throw std::runtime_error("table must be empty on wrong db_id push");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_engine_rejects_last_writer_wins_policy() {
    using namespace mdbxc;
    const std::string p = "test_engine_lww_policy.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    expect_invalid_argument("SyncEngine LastWriterWins policy",
                            [&conn]() {
                                sync::SyncEngine engine(
                                    conn,
                                    sync::ConflictPolicy::LastWriterWins);
                                (void)engine;
                            });

    conn->disconnect();
    cleanup(p);
}

void test_engine_handle_pull_pagination_has_more() {
    using namespace mdbxc;
    const std::string primary_path = "test_engine_pull_pag.mdbx";
    const std::string replica_path = "test_engine_pull_pag_replica.mdbx";
    cleanup(primary_path); cleanup(replica_path);

    auto primary_conn = open_env(primary_path);
    auto replica_conn = open_env(replica_path);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_uuid      = make_node(0xD0);

    sync::SyncEngine primary_engine(primary_conn);
    sync::SyncEngine replica_engine(replica_conn);
    primary_engine.initialize_local_identity(primary_node, db_uuid);
    replica_engine.initialize_local_identity(replica_node, db_uuid);

    sync::ThreadLocalChangeAccumulator primary_sink(primary_conn);
    primary_conn->attach_sync_capture(&primary_sink);
    {
        KeyValueTable<int, int> kv(primary_conn, "kv");
        for (int i = 1; i <= 5; ++i) {
            kv.insert_or_assign(i, i * 100);
        }
    }
    primary_conn->detach_sync_capture();

    sync::DirectSyncPeer peer(&primary_engine);
    sync::PullRequest req;
    req.requester  = replica_node;
    req.db_id      = db_uuid;
    req.max_batches = 2;  // force pagination
    const sync::PullResponse resp = peer.pull(req);

    if (resp.batches.size() != 2u) {
        throw std::runtime_error("expected 2 batches, got " +
                                 std::to_string(resp.batches.size()));
    }
    if (!resp.has_more) {
        throw std::runtime_error("has_more should be true when limit truncates pull");
    }

    // Apply first page
    {
        auto txn = replica_conn->transaction(TransactionMode::WRITABLE);
        for (const sync::ChangeBatch& b : resp.batches) {
            replica_engine.apply_batch(txn.handle(), b);
        }
        txn.commit();
    }

    // Pull page 2 with cursor from page 1
    sync::PullRequest req2;
    req2.requester = replica_node;
    req2.db_id     = db_uuid;
    req2.have      = replica_engine.applied_cursor();
    req2.max_batches = 100;  // no limit this time
    const sync::PullResponse resp2 = peer.pull(req2);
    if (resp2.batches.size() != 3u) {
        throw std::runtime_error("expected 3 remaining batches, got " +
                                 std::to_string(resp2.batches.size()));
    }
    if (resp2.has_more) {
        throw std::runtime_error("has_more should be false after draining changelog");
    }

    {
        auto txn = replica_conn->transaction(TransactionMode::WRITABLE);
        for (const sync::ChangeBatch& b : resp2.batches) {
            replica_engine.apply_batch(txn.handle(), b);
        }
        txn.commit();
    }

    KeyValueTable<int, int> replica_kv(replica_conn, "kv");
    for (int i = 1; i <= 5; ++i) {
        if (kv_or_throw(replica_conn, replica_kv, i, "missing on replica") != i * 100) {
            throw std::runtime_error("wrong value on replica after paginated pull");
        }
    }

    primary_conn->disconnect();
    replica_conn->disconnect();
    cleanup(primary_path); cleanup(replica_path);
}

void test_engine_handle_pull_multi_origin_pagination() {
    using namespace mdbxc;
    const std::string primary_path = "test_engine_pull_multi_origin.mdbx";
    const std::string replica_path = "test_engine_pull_multi_origin_replica.mdbx";
    cleanup(primary_path); cleanup(replica_path);

    auto primary_conn = open_env(primary_path);
    auto replica_conn = open_env(replica_path);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_uuid = make_node(0xD0);
    const sync::NodeId origin_a = make_node(0x20);
    const sync::NodeId origin_b = make_node(0x40);

    sync::SyncEngine primary_engine(primary_conn);
    sync::SyncEngine replica_engine(replica_conn);
    primary_engine.initialize_local_identity(primary_node, db_uuid);
    replica_engine.initialize_local_identity(replica_node, db_uuid);

    {
        auto txn = primary_conn->transaction(TransactionMode::WRITABLE);
        sync::ChangeLogStore log(primary_conn->env_handle());
        log.open(txn.handle());
        append_raw_batch(log, txn.handle(), origin_a, 1, "kv", 0xA1);
        append_raw_batch(log, txn.handle(), origin_a, 2, "kv", 0xA2);
        append_raw_batch(log, txn.handle(), origin_b, 1, "kv", 0xB1);
        append_raw_batch(log, txn.handle(), origin_b, 2, "kv", 0xB2);
        txn.commit();
    }

    sync::DirectSyncPeer peer(&primary_engine);
    sync::PullRequest req;
    req.requester = replica_node;
    req.db_id = db_uuid;
    req.max_batches = 2;
    const sync::PullResponse first = peer.pull(req);
    if (first.batches.size() != 2u || !first.has_more) {
        throw std::runtime_error("first multi-origin page should contain 2 batches and has_more");
    }
    if (first.batches[0].origin_node_id != origin_a ||
        first.batches[1].origin_node_id != origin_a) {
        throw std::runtime_error("first multi-origin page should stop inside origin A");
    }

    {
        auto txn = replica_conn->transaction(TransactionMode::WRITABLE);
        for (const sync::ChangeBatch& batch : first.batches) {
            if (replica_engine.apply_batch(txn.handle(), batch) != sync::ApplyResult::Applied) {
                throw std::runtime_error("first multi-origin page apply failed");
            }
        }
        txn.commit();
    }

    sync::PullRequest req2;
    req2.requester = replica_node;
    req2.db_id = db_uuid;
    req2.have = replica_engine.applied_cursor();
    req2.max_batches = 2;
    const sync::PullResponse second = peer.pull(req2);
    if (second.batches.size() != 2u) {
        throw std::runtime_error("second multi-origin page should contain origin B batches");
    }
    if (second.has_more) {
        throw std::runtime_error("second multi-origin page should drain changelog");
    }
    if (second.batches[0].origin_node_id != origin_b ||
        second.batches[1].origin_node_id != origin_b) {
        throw std::runtime_error("second multi-origin page should include origin B");
    }

    {
        auto txn = replica_conn->transaction(TransactionMode::WRITABLE);
        for (const sync::ChangeBatch& batch : second.batches) {
            if (replica_engine.apply_batch(txn.handle(), batch) != sync::ApplyResult::Applied) {
                throw std::runtime_error("second multi-origin page apply failed");
            }
        }
        txn.commit();
    }

    const sync::SyncCursor cursor = replica_engine.applied_cursor();
    if (cursor.last_seq_for(origin_a) != 2u || cursor.last_seq_for(origin_b) != 2u) {
        throw std::runtime_error("multi-origin pagination did not apply both origins");
    }

    primary_conn->disconnect();
    replica_conn->disconnect();
    cleanup(primary_path); cleanup(replica_path);
}

void test_engine_handle_pull_legacy_changelog_without_origin_index() {
    using namespace mdbxc;
    const std::string primary_path = "test_engine_pull_legacy_origins.mdbx";
    cleanup(primary_path);

    auto primary_conn = open_env(primary_path);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_uuid = make_node(0xD0);
    const sync::NodeId origin = make_node(0x20);

    sync::SyncEngine primary_engine(primary_conn);
    primary_engine.initialize_local_identity(primary_node, db_uuid);

    {
        auto txn = primary_conn->transaction(TransactionMode::WRITABLE);
        MDBX_dbi raw = 0;
        check_mdbx(mdbx_dbi_open(txn.handle(), "_mdbxc_changelog",
                                 MDBX_CREATE, &raw),
                   "open raw changelog failed");
        const sync::ChangeBatch batch = make_raw_batch(origin, 1, "kv", 0xA1);
        const std::vector<std::uint8_t> bytes = sync::ChangeBatchCodec::encode(batch);
        put_raw_changelog(txn.handle(), raw, origin, 1, bytes);
        txn.commit();
    }

    {
        auto txn = primary_conn->transaction(TransactionMode::READ_ONLY);
        sync::OriginIndexStore origins(primary_conn->env_handle());
        if (origins.open_existing(txn.handle())) {
            throw std::runtime_error("legacy setup should not create origin index");
        }
    }

    sync::DirectSyncPeer peer(&primary_engine);
    sync::PullRequest req;
    req.requester = replica_node;
    req.db_id = db_uuid;
    const sync::PullResponse response = peer.pull(req);
    if (!response.ok) {
        throw std::runtime_error("legacy origin-index fallback pull failed: " +
                                 response.error);
    }
    if (response.batches.size() != 1u ||
        response.batches[0].origin_node_id != origin ||
        response.batches[0].seq != 1u) {
        throw std::runtime_error("legacy origin-index fallback returned wrong batch");
    }

    primary_conn->disconnect();
    cleanup(primary_path);
}

void test_engine_handle_pull_skips_old_batches_without_decoding() {
    using namespace mdbxc;
    const std::string primary_path = "test_engine_pull_skip_old_decode.mdbx";
    cleanup(primary_path);

    auto primary_conn = open_env(primary_path);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_uuid = make_node(0xD0);
    const sync::NodeId origin_a = make_node(0x20);
    const sync::NodeId origin_b = make_node(0x40);

    sync::SyncEngine primary_engine(primary_conn);
    primary_engine.initialize_local_identity(primary_node, db_uuid);

    {
        auto txn = primary_conn->transaction(TransactionMode::WRITABLE);
        sync::ChangeLogStore log(primary_conn->env_handle());
        log.open(txn.handle());
        append_raw_bytes(log, txn.handle(), origin_a, 1, std::vector<std::uint8_t>{ 0x01, 0x02 });
        append_raw_batch(log, txn.handle(), origin_b, 1, "kv", 0xB1);
        txn.commit();
    }

    sync::DirectSyncPeer peer(&primary_engine);
    sync::PullRequest req;
    req.requester = replica_node;
    req.db_id = db_uuid;
    req.have.last_seq_by_origin[origin_a] = 1;

    const sync::PullResponse response = peer.pull(req);
    if (!response.ok) {
        throw std::runtime_error("skip-old pull should succeed");
    }
    if (response.batches.size() != 1u) {
        throw std::runtime_error("skip-old pull should return only origin B batch");
    }
    if (response.batches[0].origin_node_id != origin_b ||
        response.batches[0].seq != 1u) {
        throw std::runtime_error("skip-old pull returned the wrong batch");
    }

    primary_conn->disconnect();
    cleanup(primary_path);
}

void test_engine_handle_pull_lifecycle() {
    using namespace mdbxc;
    const std::string p = "test_engine_pull_lifecycle.mdbx";
    cleanup(p);

    auto conn = open_env(p);
    sync::SyncEngine local_engine(conn);
    local_engine.initialize_local_identity(make_node(0x10), make_node(0x10));

    sync::DirectSyncPeer peer(&local_engine);
    sync::PullRequest req;
    req.requester = make_node(0x20);
    req.db_id     = make_node(0x10);
    // Many handle_pull() calls in sequence — the read txn guard must abort
    // (release handle + reader slot) every time, otherwise long-lived
    // servers would slowly exhaust the reader table (MDBX_READERS limit).
    // This test will catch a regression back to mdbx_txn_reset().
    for (int i = 0; i < 256; ++i) {
        const sync::PullResponse resp = peer.pull(req);
        if (!resp.ok) {
            throw std::runtime_error("pull lifecycle: ok=false at i=" +
                                     std::to_string(i));
        }
    }

    conn->disconnect();
    cleanup(p);
}

} // namespace

int main() {
    struct Case { const char* name; void (*fn)(); };
    const Case cases[] = {
        { "test_engine_round_trip_kv",          &test_engine_round_trip_kv },
        { "test_public_tables_reject_reserved_dbi_names",
          &test_public_tables_reject_reserved_dbi_names },
        { "test_engine_rejects_reserved_dbi_changes",
          &test_engine_rejects_reserved_dbi_changes_and_rolls_back_page },
        { "test_engine_reserved_dbi_rolls_back_multi_batch_push",
          &test_engine_reserved_dbi_rolls_back_multi_batch_push },
        { "test_engine_skips_self_origin",      &test_engine_skips_self_origin },
        { "test_engine_idempotent_replay",      &test_engine_idempotent_replay },
        { "test_engine_legacy_zero_flags",      &test_engine_applies_legacy_zero_flags_to_integer_dbi },
        { "test_engine_conflicting_dbi_flags",  &test_engine_conflicting_dbi_flags_returns_conflict },
        { "test_engine_existing_dbi_flag_mismatch",&test_engine_existing_dbi_flag_mismatch_returns_conflict },
        { "test_engine_existing_dbi_flag_mismatch_first",&test_engine_existing_dbi_flag_mismatch_reports_first_batch_dbi },
        { "test_engine_push_dbi_conflicts_not_retryable",&test_engine_push_dbi_conflicts_are_not_retryable },
        { "test_engine_gap_returns_conflict",   &test_engine_gap_returns_conflict },
        { "test_engine_applied_cursor",         &test_engine_applied_cursor },
        { "test_engine_handle_push_to_remote",  &test_engine_handle_push_to_remote },
        { "test_engine_push_gap_rolls_back",    &test_engine_push_gap_rolls_back },
        { "test_sync_apply_observer_remove_waits",
          &test_sync_apply_observer_remove_waits_for_in_flight_callback },
        { "test_sync_apply_observer_reports_unique_dbi_names",
          &test_sync_apply_observer_reports_unique_dbi_names },
        { "test_sync_apply_observer_reports_dbi_names_across_batches",
          &test_sync_apply_observer_reports_dbi_names_across_batches },
        { "test_sync_apply_observer_ignores_skipped_batch_dbi_names",
          &test_sync_apply_observer_ignores_skipped_batch_dbi_names },
        { "test_sync_apply_observer_reports_clear_delete_dbi_names",
          &test_sync_apply_observer_reports_clear_and_delete_dbi_names },
        { "test_engine_push_multi_batch_gap_cursor",&test_engine_push_multi_batch_gap_reports_persistent_cursor },
        { "test_engine_handle_pull_wrong_db_id",&test_engine_handle_pull_wrong_db_id },
        { "test_engine_rejects_unconfigured_full_snapshot_request",
          &test_engine_rejects_unconfigured_full_snapshot_request },
        { "test_engine_exports_stable_full_snapshot_pages",
          &test_engine_exports_stable_full_snapshot_pages },
        { "test_engine_full_snapshot_tail_includes_applied_origins",
          &test_engine_full_snapshot_tail_includes_applied_origins },
        { "test_engine_exports_complete_full_snapshot_inventory",
          &test_engine_exports_complete_full_snapshot_inventory },
        { "test_engine_rejects_complete_snapshot_with_logical_state",
          &test_engine_rejects_complete_snapshot_with_logical_state },
        { "test_engine_rejects_complete_snapshot_with_empty_ordered_frame",
          &test_engine_rejects_complete_snapshot_with_empty_ordered_frame },
        { "test_engine_rejects_complete_snapshot_with_logical_outbox_state",
          &test_engine_rejects_complete_snapshot_with_logical_outbox_state },
        { "test_engine_rejects_complete_snapshot_with_frontier_only",
          &test_engine_rejects_complete_snapshot_with_frontier_only },
        { "test_engine_rejects_complete_snapshot_with_watermark_only",
          &test_engine_rejects_complete_snapshot_with_watermark_only },
        { "test_engine_rejects_complete_snapshot_with_malformed_frontier",
          &test_engine_rejects_complete_snapshot_with_malformed_frontier },
        { "test_engine_rejects_complete_snapshot_with_malformed_outbox_metadata",
          &test_engine_rejects_complete_snapshot_with_malformed_outbox_metadata },
        { "test_engine_rejects_complete_snapshot_with_malformed_watermark",
          &test_engine_rejects_complete_snapshot_with_malformed_watermark },
        { "test_engine_rejects_complete_snapshot_with_malformed_replay_marker",
          &test_engine_rejects_complete_snapshot_with_malformed_replay_marker },
        { "test_engine_rejects_complete_snapshot_with_malformed_schema_record",
          &test_engine_rejects_complete_snapshot_with_malformed_schema_record },
        { "test_engine_rejects_complete_snapshot_with_malformed_outbox_envelope",
          &test_engine_rejects_complete_snapshot_with_malformed_outbox_envelope },
        { "test_engine_imports_full_snapshot_and_bootstraps_cursor",
          &test_engine_imports_full_snapshot_and_bootstraps_cursor },
        { "test_engine_manifest_only_snapshot_does_not_bootstrap_cursor",
          &test_engine_manifest_only_snapshot_does_not_bootstrap_cursor },
        { "test_engine_complete_snapshot_rejects_destination_identity_in_tail",
          &test_engine_complete_snapshot_rejects_destination_identity_in_tail },
        { "test_engine_full_snapshot_import_fails_closed_before_final_page",
          &test_engine_full_snapshot_import_fails_closed_before_final_page },
        { "test_engine_full_snapshot_rejects_nonfresh_destination",
          &test_engine_full_snapshot_rejects_nonfresh_destination },
        { "test_engine_full_snapshot_import_bounds_fail_closed",
          &test_engine_full_snapshot_import_bounds_fail_closed },
        { "test_engine_full_snapshot_import_rejects_invalid_replacement_plan",
          &test_engine_full_snapshot_import_rejects_invalid_replacement_plan },
        { "test_engine_recovers_logical_baseline_atomically",
          &test_engine_recovers_logical_baseline_atomically },
        { "test_engine_cancels_direct_logical_recovery_materialization",
          &test_engine_cancels_direct_logical_recovery_materialization },
        { "test_engine_changelog_page_rejects_full_snapshot_request",
          &test_engine_changelog_page_rejects_full_snapshot_request },
        { "test_engine_pull_reports_snapshot_required_after_prune",
          &test_engine_pull_reports_snapshot_required_after_prune },
        { "test_engine_pull_max_bytes_is_soft_page_budget",
          &test_engine_pull_max_bytes_is_soft_page_budget },
        { "test_engine_pull_rejects_oversized_single_batch",
          &test_engine_pull_rejects_oversized_single_batch },
        { "test_engine_handle_push_wrong_db_id",&test_engine_handle_push_wrong_db_id },
        { "test_engine_rejects_last_writer_wins_policy",
          &test_engine_rejects_last_writer_wins_policy },
        { "test_engine_handle_pull_pagination", &test_engine_handle_pull_pagination_has_more },
        { "test_engine_handle_pull_multi_origin",&test_engine_handle_pull_multi_origin_pagination },
        { "test_engine_handle_pull_legacy_origin_index",&test_engine_handle_pull_legacy_changelog_without_origin_index },
        { "test_engine_handle_pull_skip_old_decode",&test_engine_handle_pull_skips_old_batches_without_decoding },
        { "test_engine_handle_pull_lifecycle", &test_engine_handle_pull_lifecycle },
    };

    int rc = 0;
    for (std::size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        try {
            cases[i].fn();
            std::printf("PASS %s\n", cases[i].name);
        } catch (const std::exception& e) {
            std::printf("FAIL %s: %s\n", cases[i].name, e.what());
            rc = static_cast<int>(i + 1);
        } catch (...) {
            std::printf("FAIL %s: non-std exception\n", cases[i].name);
            rc = static_cast<int>(i + 1);
        }
    }
    return rc;
}
