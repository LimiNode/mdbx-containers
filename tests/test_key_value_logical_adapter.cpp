#include <mdbx_containers/sync.hpp>

#include "test_assert.hpp"

#include <cstdio>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

typedef mdbxc::sync::KeyValueLogicalInt32Codec<int> IntKeyCodec;
typedef mdbxc::sync::KeyValueLogicalStringCodec<std::string> StringValueCodec;
typedef mdbxc::sync::KeyValueTableLogicalAdapter<
    int, std::string, IntKeyCodec, StringValueCodec> IntStringAdapter;
typedef mdbxc::sync::KeyValueTableLogicalAdapter<
    long long,
    std::string,
    mdbxc::sync::KeyValueLogicalInt32Codec<long long>,
    StringValueCodec> LongLongInt32StringAdapter;

static_assert(mdbxc::sync::detail::KeyValueLogicalIntegerLocalSupported<
                  std::int32_t>::value,
              "int32_t local type must be supported by integer codec");
static_assert(mdbxc::sync::detail::KeyValueLogicalIntegerLocalSupported<
                  long>::value,
              "long can be used only with an explicit integer codec tag");
static_assert(!mdbxc::sync::detail::KeyValueLogicalIntegerLocalSupported<
                  char>::value,
              "plain char must not be supported by integer codec");
static_assert(!mdbxc::sync::detail::KeyValueLogicalIntegerLocalSupported<
                  wchar_t>::value,
              "wchar_t must not be supported by integer codec");
static_assert(!mdbxc::sync::detail::KeyValueLogicalIntegerLocalSupported<
                  char16_t>::value,
              "char16_t must not be supported by integer codec");
static_assert(!mdbxc::sync::detail::KeyValueLogicalIntegerLocalSupported<
                  char32_t>::value,
              "char32_t must not be supported by integer codec");

void cleanup(const std::string& p) {
    std::remove(p.c_str());
}

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId n{};
    for (int i = 0; i < 16; ++i) {
        n[i] = static_cast<std::uint8_t>(seed + i);
    }
    return n;
}

mdbxc::sync::LogicalSchemaRecord make_record(const std::string& dbi_name,
                                             std::uint32_t version = 1) {
    mdbxc::sync::LogicalSchemaRecord record;
    record.dbi_name = dbi_name;
    record.kind = mdbxc::sync::LogicalTableKind::KeyValue;
    record.schema_version = version;
    record.dbi_names.push_back(dbi_name);
    return record;
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

class RawWritingLogicalAdapter : public mdbxc::sync::ILogicalTableAdapter {
public:
    RawWritingLogicalAdapter(
            mdbxc::KeyValueTable<int, std::string>& table,
            const std::string& schema_id)
        : m_table(table),
          m_schema_id(schema_id) {}

    mdbxc::sync::LogicalSchemaRef schema_ref() const override {
        mdbxc::sync::LogicalSchemaRef ref;
        ref.schema_id = m_schema_id;
        ref.kind = mdbxc::sync::LogicalTableKind::KeyValue;
        ref.schema_version = 1;
        return ref;
    }

    std::vector<std::string> affected_dbis() const override {
        std::vector<std::string> out;
        out.push_back(m_table.dbi_name());
        return out;
    }

    mdbxc::sync::LogicalApplyResult preflight(
            MDBX_txn* txn,
            const mdbxc::sync::LogicalChange& change) const override {
        (void)txn;
        if (change.schema.schema_id != m_schema_id) {
            return mdbxc::sync::LogicalApplyResult::failure(
                "unexpected schema id");
        }
        return mdbxc::sync::LogicalApplyResult::success();
    }

    mdbxc::sync::LogicalApplyResult apply(
            MDBX_txn* txn,
            const mdbxc::sync::LogicalChange& change) override {
        (void)change;
        m_table.insert_or_assign(30, "thirty", txn);
        return mdbxc::sync::LogicalApplyResult::success();
    }

private:
    mdbxc::KeyValueTable<int, std::string>& m_table;
    std::string m_schema_id;
};

class MultiDbiLogicalAdapter : public mdbxc::sync::ILogicalTableAdapter {
public:
    MultiDbiLogicalAdapter(
            mdbxc::KeyValueTable<int, std::string>& table,
            const std::string& secondary_dbi_name,
            const std::string& schema_id)
        : m_table(table),
          m_secondary_dbi_name(secondary_dbi_name),
          m_schema_id(schema_id) {}

    mdbxc::sync::LogicalSchemaRef schema_ref() const override {
        mdbxc::sync::LogicalSchemaRef ref;
        ref.schema_id = m_schema_id;
        ref.kind = mdbxc::sync::LogicalTableKind::KeyValue;
        ref.schema_version = 1;
        return ref;
    }

    std::vector<std::string> affected_dbis() const override {
        std::vector<std::string> out;
        out.push_back(m_table.dbi_name());
        out.push_back(m_secondary_dbi_name);
        return out;
    }

    mdbxc::sync::LogicalApplyResult preflight(
            MDBX_txn* txn,
            const mdbxc::sync::LogicalChange& change) const override {
        (void)txn;
        (void)change;
        return mdbxc::sync::LogicalApplyResult::success();
    }

    mdbxc::sync::LogicalApplyResult apply(
            MDBX_txn* txn,
            const mdbxc::sync::LogicalChange& change) override {
        (void)change;
        m_table.insert_or_assign(31, "thirty-one", txn);
        return mdbxc::sync::LogicalApplyResult::success();
    }

private:
    mdbxc::KeyValueTable<int, std::string>& m_table;
    std::string m_secondary_dbi_name;
    std::string m_schema_id;
};

void test_key_value_logical_adapter_applies_basic_ops() {
    const std::string path = "test_key_value_logical_adapter_basic.mdbx";
    const std::string dbi_name = "logical_key_value_items";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn = mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xA0));
    engine.register_logical_schema("app.logical_kv.v1",
                                   make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv.v1");
    MDBXC_TEST_ASSERT(adapter.affected_dbis().size() == 1u);
    MDBXC_TEST_ASSERT(adapter.affected_dbis()[0] == dbi_name);
    mdbxc::sync::LogicalTableRegistry registry;
    registry.register_adapter(&adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(adapter.make_upsert(1, "one"));
    changes.push_back(adapter.make_upsert(2, "two"));
    changes.push_back(adapter.make_upsert(3, ""));
    changes.push_back(adapter.make_delete(1));

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::WRITABLE);
        const mdbxc::sync::LogicalApplyResult result =
            registry.preflight_then_apply(txn.handle(), changes);
        MDBXC_TEST_ASSERT(result.ok);
        txn.commit();
    }

    MDBXC_TEST_ASSERT(!table.contains(1));
    const std::pair<bool, std::string> found = table.find_compat(2);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == "two");
    const std::pair<bool, std::string> empty_found = table.find_compat(3);
    MDBXC_TEST_ASSERT(empty_found.first);
    MDBXC_TEST_ASSERT(empty_found.second.empty());
    MDBXC_TEST_ASSERT(table.count() == 2u);

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::WRITABLE);
        std::vector<mdbxc::sync::LogicalChange> clear_changes;
        clear_changes.push_back(adapter.make_clear());
        const mdbxc::sync::LogicalApplyResult result =
            registry.preflight_then_apply(txn.handle(), clear_changes);
        MDBXC_TEST_ASSERT(result.ok);
        txn.commit();
    }
    MDBXC_TEST_ASSERT(table.count() == 0u);

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_adapter_applies_through_sync_engine() {
    const std::string path = "test_key_value_logical_adapter_engine.mdbx";
    const std::string dbi_name = "logical_key_value_engine";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x16), make_node(0xA6));
    engine.register_logical_schema("app.logical_kv_engine.v1",
                                   make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_engine.v1");
    engine.register_logical_adapter(adapter);
    MDBXC_TEST_ASSERT(engine.logical_adapter_count() == 1u);

    CountingApplyObserver observer;
    const std::uint64_t token =
        conn->add_sync_apply_observer(&observer);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(adapter.make_upsert(11, "eleven"));
    changes.push_back(adapter.make_upsert(12, "twelve"));
    changes.push_back(adapter.make_delete(12));

    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(result.ok);

    const std::pair<bool, std::string> found = table.find_compat(11);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == "eleven");
    MDBXC_TEST_ASSERT(!table.contains(12));
    MDBXC_TEST_ASSERT(observer.calls == 1u);
    MDBXC_TEST_ASSERT(observer.generation == conn->sync_apply_generation());
    MDBXC_TEST_ASSERT(observer.applied_batches == 1u);
    MDBXC_TEST_ASSERT(observer.applied_ops == changes.size());
    MDBXC_TEST_ASSERT(observer.affected_dbi_names.size() == 1u);
    MDBXC_TEST_ASSERT(observer.affected_dbi_names[0] == dbi_name);

    MDBXC_TEST_ASSERT(conn->remove_sync_apply_observer(token));
    MDBXC_TEST_ASSERT(engine.unregister_logical_adapter(
        "app.logical_kv_engine.v1"));
    MDBXC_TEST_ASSERT(engine.logical_adapter_count() == 0u);

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_adapter_engine_rejects_unknown_schema() {
    const std::string path = "test_key_value_logical_adapter_unknown.mdbx";
    const std::string dbi_name = "logical_key_value_unknown";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x17), make_node(0xA7));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_unknown.v1");

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(adapter.make_upsert(5, "five"));

    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find("No logical adapter") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(!table.contains(5));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_engine_rejects_missing_schema_marker() {
    const std::string path = "test_key_value_logical_adapter_missing_marker.mdbx";
    const std::string dbi_name = "logical_key_value_missing_marker";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x1A), make_node(0xAA));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_missing_marker.v1");
    engine.register_logical_adapter(adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(adapter.make_upsert(6, "six"));
    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find("schema marker") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(!table.contains(6));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_engine_rejects_stale_schema_marker() {
    const std::string path = "test_key_value_logical_adapter_stale_marker.mdbx";
    const std::string dbi_name = "logical_key_value_stale_marker";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x1B), make_node(0xAB));
    engine.register_logical_schema("app.logical_kv_stale_marker.v1",
                                   make_record(dbi_name, 1));
    engine.migrate_logical_schema("app.logical_kv_stale_marker.v1",
                                  make_record(dbi_name, 1),
                                  make_record(dbi_name, 2));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_stale_marker.v1", 1);
    engine.register_logical_adapter(adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(adapter.make_upsert(7, "seven"));
    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find("schema marker") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(!table.contains(7));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_engine_rejects_marker_dbi_mismatch() {
    const std::string path = "test_key_value_logical_adapter_dbi_marker.mdbx";
    const std::string dbi_name = "logical_key_value_dbi_marker";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x1C), make_node(0xAC));
    engine.register_logical_schema("app.logical_kv_dbi_marker.v1",
                                   make_record(dbi_name + "_other"));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_dbi_marker.v1");
    engine.register_logical_adapter(adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(adapter.make_upsert(8, "eight"));
    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find("DBI set") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(!table.contains(8));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_engine_rejects_multi_dbi_adapter_until_primary_contract() {
    const std::string path = "test_key_value_logical_adapter_multi_dbi.mdbx";
    const std::string dbi_name = "logical_key_value_multi_dbi";
    const std::string secondary_dbi_name = dbi_name + "_index";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x1E), make_node(0xAE));
    mdbxc::sync::LogicalSchemaRecord record = make_record(dbi_name);
    record.dbi_names.push_back(secondary_dbi_name);
    engine.register_logical_schema("app.logical_kv_multi_dbi.v1", record);

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    MultiDbiLogicalAdapter adapter(
        table, secondary_dbi_name, "app.logical_kv_multi_dbi.v1");
    engine.register_logical_adapter(adapter);

    mdbxc::sync::LogicalChange change;
    change.schema = adapter.schema_ref();
    change.opcode = mdbxc::sync::KeyValueLogicalUpsert;
    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(change);
    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find("multi-DBI") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(!table.contains(31));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_engine_suppresses_generic_raw_capture() {
    const std::string path = "test_key_value_logical_adapter_engine_capture.mdbx";
    const std::string dbi_name = "logical_key_value_engine_capture";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId node = make_node(0x1D);
    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(node, make_node(0xAD));
    engine.register_logical_schema("app.logical_kv_engine_capture.v1",
                                   make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    RawWritingLogicalAdapter adapter(
        table, "app.logical_kv_engine_capture.v1");
    engine.register_logical_adapter(adapter);

    mdbxc::sync::ThreadLocalChangeAccumulator capture(conn);
    conn->attach_sync_capture(&capture);

    std::vector<mdbxc::sync::LogicalChange> changes;
    mdbxc::sync::LogicalChange change;
    change.schema = adapter.schema_ref();
    change.opcode = mdbxc::sync::KeyValueLogicalUpsert;
    changes.push_back(change);
    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    conn->detach_sync_capture();
    MDBXC_TEST_ASSERT(result.ok);

    const std::pair<bool, std::string> found = table.find_compat(30);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == "thirty");

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        mdbxc::sync::ChangeLogStore changelog(conn->env_handle());
        changelog.open(txn.handle());
        MDBXC_TEST_ASSERT(!changelog.contains(txn.handle(), node, 1));
    }

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_capture_session_commits_typed_local_writes() {
    const std::string source_path =
        "test_key_value_logical_adapter_session_source.mdbx";
    const std::string replica_path =
        "test_key_value_logical_adapter_session_replica.mdbx";
    const std::string dbi_name = "logical_key_value_session";
    cleanup(source_path);
    cleanup(replica_path);

    mdbxc::Config source_cfg;
    source_cfg.pathname = source_path;
    source_cfg.max_dbs = 16;
    source_cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> source =
        mdbxc::Connection::create(source_cfg);

    mdbxc::sync::SyncEngine source_engine(source);
    const mdbxc::sync::NodeId source_node = make_node(0x1E);
    source_engine.initialize_local_identity(source_node, make_node(0xAE));
    source_engine.register_logical_schema("app.logical_kv_session.v1",
                                          make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> source_table(source, dbi_name);
    IntStringAdapter source_adapter(
        source_table, "app.logical_kv_session.v1");
    mdbxc::sync::ThreadLocalChangeAccumulator raw_capture(source);
    source->attach_sync_capture(&raw_capture);

    std::vector<mdbxc::sync::LogicalChange> changes;
    {
        std::unique_ptr<IntStringAdapter::LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        session->insert_or_assign(1, "one");
        session->insert_or_assign(2, "two");
        const bool removed = session->erase(1);
        MDBXC_TEST_ASSERT(removed);
        const bool missing = session->erase(9);
        MDBXC_TEST_ASSERT(!missing);
        MDBXC_TEST_ASSERT(session->pending_size() == 3u);
        session->commit(changes);
    }
    source->detach_sync_capture();

    MDBXC_TEST_ASSERT(changes.size() == 3u);
    MDBXC_TEST_ASSERT(changes[0].opcode ==
                      mdbxc::sync::KeyValueLogicalUpsert);
    MDBXC_TEST_ASSERT(changes[1].opcode ==
                      mdbxc::sync::KeyValueLogicalUpsert);
    MDBXC_TEST_ASSERT(changes[2].opcode ==
                      mdbxc::sync::KeyValueLogicalDelete);
    MDBXC_TEST_ASSERT(!source_table.contains(1));
    const std::pair<bool, std::string> source_found =
        source_table.find_compat(2);
    MDBXC_TEST_ASSERT(source_found.first);
    MDBXC_TEST_ASSERT(source_found.second == "two");

    {
        mdbxc::Transaction txn =
            source->transaction(mdbxc::TransactionMode::READ_ONLY);
        mdbxc::sync::ChangeLogStore changelog(source->env_handle());
        changelog.open(txn.handle());
        MDBXC_TEST_ASSERT(!changelog.contains(txn.handle(), source_node, 1));
    }

    mdbxc::Config replica_cfg;
    replica_cfg.pathname = replica_path;
    replica_cfg.max_dbs = 16;
    replica_cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> replica =
        mdbxc::Connection::create(replica_cfg);

    mdbxc::sync::SyncEngine replica_engine(replica);
    replica_engine.initialize_local_identity(make_node(0x1F), make_node(0xAE));
    replica_engine.register_logical_schema("app.logical_kv_session.v1",
                                           make_record(dbi_name));
    mdbxc::KeyValueTable<int, std::string> replica_table(replica, dbi_name);
    IntStringAdapter replica_adapter(
        replica_table, "app.logical_kv_session.v1");
    replica_engine.register_logical_adapter(replica_adapter);

    const mdbxc::sync::LogicalApplyResult applied =
        replica_engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(applied.ok);
    MDBXC_TEST_ASSERT(!replica_table.contains(1));
    const std::pair<bool, std::string> replica_found =
        replica_table.find_compat(2);
    MDBXC_TEST_ASSERT(replica_found.first);
    MDBXC_TEST_ASSERT(replica_found.second == "two");

    source->disconnect();
    replica->disconnect();
    cleanup(source_path);
    cleanup(replica_path);
}

void test_key_value_logical_capture_session_discards_rollback() {
    const std::string path = "test_key_value_logical_adapter_session_rollback.mdbx";
    const std::string dbi_name = "logical_key_value_session_rollback";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x20), make_node(0xB0));
    engine.register_logical_schema("app.logical_kv_session_rollback.v1",
                                   make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_session_rollback.v1");

    std::vector<mdbxc::sync::LogicalChange> changes;
    {
        std::unique_ptr<IntStringAdapter::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        session->insert_or_assign(3, "three");
        MDBXC_TEST_ASSERT(session->pending_size() == 1u);
        session->rollback();
    }

    MDBXC_TEST_ASSERT(changes.empty());
    MDBXC_TEST_ASSERT(!table.contains(3));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_capture_session_fails_before_mutation() {
    const std::string path = "test_key_value_logical_adapter_session_range.mdbx";
    const std::string dbi_name = "logical_key_value_session_range";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::KeyValueTable<long long, std::string> table(conn, dbi_name);
    LongLongInt32StringAdapter adapter(
        table, "app.logical_kv_session_range.v1");

    const long long too_large =
        static_cast<long long>((std::numeric_limits<std::int32_t>::max)()) + 1;
    bool threw = false;
    {
        std::unique_ptr<LongLongInt32StringAdapter::LogicalCaptureSession>
            session = adapter.begin_capture_session();
        try {
            session->insert_or_assign(too_large, "too-large");
        } catch (const std::out_of_range&) {
            threw = true;
        }
        MDBXC_TEST_ASSERT(threw);
        MDBXC_TEST_ASSERT(session->pending_size() == 0u);
        session->rollback();
    }

    MDBXC_TEST_ASSERT(!table.contains(too_large));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_adapter_rejects_malformed_payload() {
    const std::string path = "test_key_value_logical_adapter_malformed.mdbx";
    const std::string dbi_name = "logical_key_value_malformed";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn = mdbxc::Connection::create(cfg);

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_malformed.v1");
    mdbxc::sync::LogicalTableRegistry registry;
    registry.register_adapter(&adapter);

    mdbxc::sync::LogicalChange malformed = adapter.make_upsert(7, "seven");
    malformed.payload.pop_back();

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::WRITABLE);
        std::vector<mdbxc::sync::LogicalChange> changes;
        changes.push_back(malformed);
        const mdbxc::sync::LogicalApplyResult result =
            registry.preflight_then_apply(txn.handle(), changes);
        MDBXC_TEST_ASSERT(!result.ok);
        MDBXC_TEST_ASSERT(result.error.find("payload") != std::string::npos);
        txn.rollback();
    }

    MDBXC_TEST_ASSERT(table.count() == 0u);

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_adapter_uses_stable_payload() {
    const std::string path = "test_key_value_logical_adapter_payload.mdbx";
    const std::string dbi_name = "logical_key_value_payload";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn = mdbxc::Connection::create(cfg);

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_payload.v1");

    const mdbxc::sync::LogicalChange change =
        adapter.make_upsert(-1, "one");
    const std::uint8_t expected_raw[] = {
        4, 0, 0, 0,
        0xFF, 0xFF, 0xFF, 0xFF,
        3, 0, 0, 0,
        static_cast<std::uint8_t>('o'),
        static_cast<std::uint8_t>('n'),
        static_cast<std::uint8_t>('e')
    };
    const std::vector<std::uint8_t> expected(
        expected_raw,
        expected_raw + sizeof(expected_raw) / sizeof(expected_raw[0]));
    MDBXC_TEST_ASSERT(change.payload == expected);

    mdbxc::KeyValueTable<std::int32_t, std::uint64_t> fixed_table(
        conn, "logical_key_value_payload_fixed");
    typedef mdbxc::sync::KeyValueLogicalInt32Codec<std::int32_t>
        FixedInt32Codec;
    typedef mdbxc::sync::KeyValueLogicalUInt64Codec<std::uint64_t>
        FixedUInt64Codec;
    typedef mdbxc::sync::KeyValueTableLogicalAdapter<
        std::int32_t, std::uint64_t,
        FixedInt32Codec, FixedUInt64Codec> FixedAdapter;
    FixedAdapter fixed_adapter(
        fixed_table, "app.logical_kv_payload_fixed.v1");
    const mdbxc::sync::LogicalChange fixed_change =
        fixed_adapter.make_upsert(
            (std::numeric_limits<std::int32_t>::min)(),
            (std::numeric_limits<std::uint64_t>::max)());
    const std::uint8_t expected_fixed_raw[] = {
        4, 0, 0, 0,
        0x00, 0x00, 0x00, 0x80,
        8, 0, 0, 0,
        0xFF, 0xFF, 0xFF, 0xFF,
        0xFF, 0xFF, 0xFF, 0xFF
    };
    const std::vector<std::uint8_t> expected_fixed(
        expected_fixed_raw,
        expected_fixed_raw + sizeof(expected_fixed_raw) /
            sizeof(expected_fixed_raw[0]));
    MDBXC_TEST_ASSERT(fixed_change.payload == expected_fixed);

    mdbxc::KeyValueTable<long, std::string> long_table(
        conn, "logical_key_value_payload_long");
    typedef mdbxc::sync::KeyValueLogicalInt64Codec<long> LongInt64Codec;
    typedef mdbxc::sync::KeyValueTableLogicalAdapter<
        long, std::string, LongInt64Codec, StringValueCodec> LongAdapter;
    LongAdapter long_adapter(
        long_table, "app.logical_kv_payload_long.v1");
    const mdbxc::sync::LogicalChange long_change =
        long_adapter.make_upsert(42L, "forty-two");
    const std::uint8_t expected_long_raw[] = {
        8, 0, 0, 0,
        42, 0, 0, 0, 0, 0, 0, 0,
        9, 0, 0, 0,
        static_cast<std::uint8_t>('f'),
        static_cast<std::uint8_t>('o'),
        static_cast<std::uint8_t>('r'),
        static_cast<std::uint8_t>('t'),
        static_cast<std::uint8_t>('y'),
        static_cast<std::uint8_t>('-'),
        static_cast<std::uint8_t>('t'),
        static_cast<std::uint8_t>('w'),
        static_cast<std::uint8_t>('o')
    };
    const std::vector<std::uint8_t> expected_long(
        expected_long_raw,
        expected_long_raw + sizeof(expected_long_raw) /
            sizeof(expected_long_raw[0]));
    MDBXC_TEST_ASSERT(long_change.payload == expected_long);

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_adapter_decodes_literal_little_endian_payload() {
    const std::string path = "test_key_value_logical_adapter_literal.mdbx";
    const std::string dbi_name = "logical_key_value_literal";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn = mdbxc::Connection::create(cfg);

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_literal.v1");
    mdbxc::sync::LogicalTableRegistry registry;
    registry.register_adapter(&adapter);

    mdbxc::sync::LogicalChange literal;
    literal.schema = adapter.schema_ref();
    literal.opcode = mdbxc::sync::KeyValueLogicalUpsert;
    const std::uint8_t literal_payload[] = {
        4, 0, 0, 0,
        1, 0, 0, 0,
        3, 0, 0, 0,
        static_cast<std::uint8_t>('u'),
        static_cast<std::uint8_t>('n'),
        static_cast<std::uint8_t>('o')
    };
    literal.payload.assign(
        literal_payload,
        literal_payload + sizeof(literal_payload) /
            sizeof(literal_payload[0]));

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::WRITABLE);
        std::vector<mdbxc::sync::LogicalChange> changes;
        changes.push_back(literal);
        const mdbxc::sync::LogicalApplyResult result =
            registry.preflight_then_apply(txn.handle(), changes);
        MDBXC_TEST_ASSERT(result.ok);
        txn.commit();
    }

    const std::pair<bool, std::string> found = table.find_compat(1);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == "uno");

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_apply_does_not_recapture_incoming_change() {
    const std::string path = "test_key_value_logical_adapter_capture.mdbx";
    const std::string dbi_name = "logical_key_value_capture";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn = mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId node = make_node(0x44);
    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(node, make_node(0x55));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_capture.v1");
    mdbxc::sync::LogicalTableRegistry registry;
    registry.register_adapter(&adapter);

    mdbxc::sync::ThreadLocalChangeAccumulator capture(conn);
    conn->attach_sync_capture(&capture);

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::WRITABLE);
        std::vector<mdbxc::sync::LogicalChange> changes;
        changes.push_back(adapter.make_upsert(10, "ten"));
        const mdbxc::sync::LogicalApplyResult result =
            registry.preflight_then_apply(txn.handle(), changes);
        MDBXC_TEST_ASSERT(result.ok);
        txn.commit();
    }

    table.insert_or_assign(11, "eleven");
    conn->detach_sync_capture();

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        mdbxc::sync::ChangeLogStore changelog(conn->env_handle());
        changelog.open(txn.handle());
        std::vector<std::uint8_t> raw;
        MDBXC_TEST_ASSERT(changelog.get(txn.handle(), node, 1, raw));
        MDBXC_TEST_ASSERT(!changelog.contains(txn.handle(), node, 2));
        const mdbxc::sync::ChangeBatch batch =
            mdbxc::sync::ChangeBatchCodec::decode(raw);
        MDBXC_TEST_ASSERT(batch.ops.size() == 1u);
        MDBXC_TEST_ASSERT(batch.ops[0].dbi_name == dbi_name);
        const std::string captured_value(
            batch.ops[0].value.begin(), batch.ops[0].value.end());
        MDBXC_TEST_ASSERT(captured_value == "eleven");
    }

    const std::pair<bool, std::string> logical_found = table.find_compat(10);
    MDBXC_TEST_ASSERT(logical_found.first);
    MDBXC_TEST_ASSERT(logical_found.second == "ten");

    conn->disconnect();
    cleanup(path);
}

} // namespace

int main() {
    test_key_value_logical_adapter_applies_basic_ops();
    test_key_value_logical_adapter_applies_through_sync_engine();
    test_key_value_logical_adapter_engine_rejects_unknown_schema();
    test_key_value_logical_engine_rejects_missing_schema_marker();
    test_key_value_logical_engine_rejects_stale_schema_marker();
    test_key_value_logical_engine_rejects_marker_dbi_mismatch();
    test_key_value_logical_engine_rejects_multi_dbi_adapter_until_primary_contract();
    test_key_value_logical_engine_suppresses_generic_raw_capture();
    test_key_value_logical_capture_session_commits_typed_local_writes();
    test_key_value_logical_capture_session_discards_rollback();
    test_key_value_logical_capture_session_fails_before_mutation();
    test_key_value_logical_adapter_rejects_malformed_payload();
    test_key_value_logical_adapter_uses_stable_payload();
    test_key_value_logical_adapter_decodes_literal_little_endian_payload();
    test_key_value_logical_apply_does_not_recapture_incoming_change();
    return 0;
}
