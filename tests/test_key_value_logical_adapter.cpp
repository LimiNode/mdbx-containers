#include <mdbx_containers/sync.hpp>

#include "test_assert.hpp"

#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

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

mdbxc::sync::LogicalSchemaRecord make_record(const std::string& dbi_name) {
    mdbxc::sync::LogicalSchemaRecord record;
    record.dbi_name = dbi_name;
    record.kind = mdbxc::sync::LogicalTableKind::KeyValue;
    record.schema_version = 1;
    record.dbi_names.push_back(dbi_name);
    return record;
}

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
    mdbxc::sync::KeyValueTableLogicalAdapter<int, std::string> adapter(
        table, "app.logical_kv.v1", dbi_name);
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
    mdbxc::sync::KeyValueTableLogicalAdapter<int, std::string> adapter(
        table, "app.logical_kv_malformed.v1", dbi_name);
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

} // namespace

int main() {
    test_key_value_logical_adapter_applies_basic_ops();
    test_key_value_logical_adapter_rejects_malformed_payload();
    return 0;
}
