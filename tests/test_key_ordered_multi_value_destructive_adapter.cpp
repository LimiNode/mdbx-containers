#include <mdbx_containers/sync.hpp>

#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

typedef mdbxc::sync::KeyValueLogicalInt32Codec<int> IntKeyCodec;
typedef mdbxc::sync::KeyValueLogicalStringCodec<std::string> StringValueCodec;
typedef mdbxc::KeyOrderedMultiValueTable<int, std::string> table_type;
typedef mdbxc::sync::KeyOrderedMultiValueTableDestructiveLogicalAdapter<
    int, std::string, IntKeyCodec, StringValueCodec> adapter_type;

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

mdbxc::sync::LogicalSchemaRecord make_v2_record(
        const std::string& primary,
        const std::string& state,
        const std::string& by_key,
        const mdbxc::sync::NodeId& origin) {
    mdbxc::sync::LogicalSchemaRecord record;
    record.dbi_name = primary;
    record.kind = mdbxc::sync::LogicalTableKind::KeyOrderedMultiValue;
    record.schema_version = 2u;
    record.dbi_names.push_back(primary);
    record.dbi_names.push_back(state);
    record.dbi_names.push_back(by_key);
    record.ordered_delivery_origin_node_id = origin;
    return record;
}

void apply_changes(mdbxc::sync::LogicalTableRegistry& registry,
                   const std::shared_ptr<mdbxc::Connection>& connection,
                   const std::vector<mdbxc::sync::LogicalChange>& changes,
                   bool expect_success) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::WRITABLE);
    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(transaction.handle(), changes, true);
    if (result.ok != expect_success) {
        throw std::runtime_error("unexpected destructive adapter apply result: " +
                                 result.error);
    }
    if (result.ok) {
        transaction.commit();
    } else {
        transaction.rollback();
    }
}

void test_destructive_append_erase_and_batch_preflight() {
    const std::string path = "test_key_ordered_multi_value_destructive_adapter.mdbx";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const std::string primary = "ordered_values";
    const std::string state = "ordered_values_state";
    const std::string by_key = "ordered_values_by_key";
    const std::string schema = "app.ordered_values.v2";
    const mdbxc::sync::NodeId origin = make_node(0x21u);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(make_node(0x11u), make_node(0x12u));
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    mdbxc::sync::LogicalTableRegistry registry;
    registry.register_adapter(&adapter);

    mdbxc::sync::OrderedElementId first;
    mdbxc::sync::OrderedElementId second;
    mdbxc::sync::OrderedElementId third;
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        first = adapter.state_store().allocate_id(transaction.handle(), origin);
        second = adapter.state_store().allocate_id(transaction.handle(), origin);
        third = adapter.state_store().allocate_id(transaction.handle(), origin);
        transaction.commit();
    }

    std::vector<mdbxc::sync::LogicalChange> appends;
    appends.push_back(adapter.make_append(first, 7, "same"));
    appends.push_back(adapter.make_append(second, 7, "same"));
    apply_changes(registry, connection, appends, true);

    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::READ_ONLY);
        const std::vector<std::string> values = table.find(7, transaction.handle());
        if (values.size() != 2u || values[0] != "same" || values[1] != "same") {
            throw std::runtime_error("destructive append order is incorrect");
        }
        transaction.rollback();
    }

    std::vector<mdbxc::sync::LogicalChange> erase;
    erase.push_back(adapter.make_erase(first));
    apply_changes(registry, connection, erase, true);

    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::READ_ONLY);
        const std::vector<std::string> values = table.find(7, transaction.handle());
        if (values.size() != 1u || values[0] != "same") {
            throw std::runtime_error("destructive erase removed the wrong value");
        }
        transaction.rollback();
    }

    std::vector<mdbxc::sync::LogicalChange> duplicate;
    duplicate.push_back(adapter.make_append(third, 7, "again"));
    duplicate.push_back(adapter.make_erase(third));
    apply_changes(registry, connection, duplicate, false);
    if (table.find(7).size() != 1u) {
        throw std::runtime_error("failed batch changed ordered table");
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_preflight_rejects_foreign_append_id() {
    const std::string path = "test_key_ordered_multi_value_destructive_origin.mdbx";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const std::string primary = "origin_values";
    const std::string state = "origin_state";
    const std::string by_key = "origin_by_key";
    const std::string schema = "app.origin.v2";
    const mdbxc::sync::NodeId origin = make_node(0x31u);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(make_node(0x13u), make_node(0x14u));
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    mdbxc::sync::LogicalTableRegistry registry;
    registry.register_adapter(&adapter);

    mdbxc::sync::OrderedElementId foreign;
    foreign.origin = make_node(0x41u);
    foreign.sequence = 1u;
    std::vector<mdbxc::sync::LogicalChange> rejected;
    rejected.push_back(adapter.make_append(foreign, 3, "foreign"));
    apply_changes(registry, connection, rejected, false);
    if (!table.find(3).empty()) {
        throw std::runtime_error("foreign append changed the ordered table");
    }

    mdbxc::sync::OrderedElementId valid;
    valid.origin = origin;
    valid.sequence = 1u;
    std::vector<mdbxc::sync::LogicalChange> accepted;
    accepted.push_back(adapter.make_append(valid, 3, "local"));
    apply_changes(registry, connection, accepted, true);
    if (table.find(3).size() != 1u || table.find(3)[0] != "local") {
        throw std::runtime_error("valid authoritative append was rejected");
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_capture_coalesces_and_commits_to_outbox() {
    const std::string path = "test_key_ordered_multi_value_destructive_capture.mdbx";
    const std::string primary = "ordered_capture_values";
    const std::string state = "ordered_capture_state";
    const std::string by_key = "ordered_capture_by_key";
    const std::string schema = "app.ordered_capture.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0x61u);
    const mdbxc::sync::DbId local_db = make_node(0xA1u);
    const mdbxc::sync::DbId destination = make_node(0xB1u);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(origin, local_db);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        const mdbxc::sync::OrderedElementId id = session->append(9, "transient");
        session->erase(id);
        if (session->pending_size() != 0u) {
            throw std::runtime_error("append/erase was not coalesced");
        }
        const mdbxc::sync::LogicalDeliveryEnvelope envelope =
            session->commit_to_outbox(engine, destination);
        if (envelope.origin_sequence != 1u ||
            !envelope.frame.changes.empty()) {
            throw std::runtime_error("empty destructive delivery is incorrect");
        }
    }
    if (!table.find(9).empty()) {
        throw std::runtime_error("coalesced local element remained physical");
    }

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        const mdbxc::sync::OrderedElementId id = session->append(9, "durable");
        if (id.sequence != 2u || session->pending_size() != 1u) {
            throw std::runtime_error("ordered capture id allocation is incorrect");
        }
        const mdbxc::sync::LogicalDeliveryEnvelope envelope =
            session->commit_to_outbox(engine, destination);
        if (envelope.origin_sequence != 2u ||
            envelope.frame.changes.size() != 1u) {
            throw std::runtime_error("destructive outbox delivery is incorrect");
        }
    }
    if (table.find(9).size() != 1u || table.find(9)[0] != "durable") {
        throw std::runtime_error("durable local ordered value is missing");
    }

    connection->disconnect();
    cleanup(path);
}

} // namespace

int main() {
    test_destructive_append_erase_and_batch_preflight();
    test_destructive_preflight_rejects_foreign_append_id();
    test_destructive_capture_coalesces_and_commits_to_outbox();
    return 0;
}
