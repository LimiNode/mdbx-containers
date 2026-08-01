#include <mdbx_containers/sync.hpp>

#include <cstdio>
#include <map>
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

class CommitFailureCaptureSink : public mdbxc::sync::ISyncCaptureSink {
public:
    CommitFailureCaptureSink()
        : flush_calls(0u), discard_calls(0u) {}

    void record_change(
            MDBX_txn*,
            const std::string&,
            mdbxc::sync::ChangeOpType,
            std::uint32_t,
            const std::vector<std::uint8_t>&,
            const std::vector<std::uint8_t>&) override {}

    void flush_in_txn(MDBX_txn* txn) override {
        ++flush_calls;
        pending[txn] = 1u;
    }

    void discard_txn(MDBX_txn* txn) noexcept override {
        ++discard_calls;
        pending.erase(txn);
    }

    std::map<MDBX_txn*, std::size_t> pending;
    std::size_t flush_calls;
    std::size_t discard_calls;
};

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

MDBX_val make_raw_val(const std::vector<std::uint8_t>& bytes) {
    MDBX_val value = {
        const_cast<std::uint8_t*>(bytes.empty() ? nullptr : &bytes[0]),
        bytes.size()
    };
    return value;
}

std::vector<std::uint8_t> make_state_key(
        const mdbxc::sync::OrderedElementId& id) {
    std::vector<std::uint8_t> key(1u, 0x01u);
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::encode_ordered_element_id_index(id);
    key.insert(key.end(), encoded.begin(), encoded.end());
    return key;
}

std::vector<std::uint8_t> make_introduced_key(
        const mdbxc::sync::NodeId& origin) {
    std::vector<std::uint8_t> key(1u, 0x02u);
    key.insert(key.end(), origin.begin(), origin.end());
    return key;
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

void delete_raw_state_record(
        const std::shared_ptr<mdbxc::Connection>& connection,
        const std::string& state_name,
        const mdbxc::sync::OrderedElementId& id) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::WRITABLE);
    MDBX_dbi state = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(
        transaction.handle(), state_name.c_str(),
        static_cast<MDBX_db_flags_t>(0), &state),
        "test state DBI open failed");
    const std::vector<std::uint8_t> key = make_state_key(id);
    MDBX_val raw_key = make_raw_val(key);
    mdbxc::check_mdbx(mdbx_del(transaction.handle(), state, &raw_key, nullptr),
                      "test state record delete failed");
    transaction.commit();
}

void insert_raw_index_record(
        const std::shared_ptr<mdbxc::Connection>& connection,
        const std::string& by_key_name,
        const std::vector<std::uint8_t>& key,
        const mdbxc::sync::OrderedElementId& id) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::WRITABLE);
    MDBX_dbi by_key = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(
        transaction.handle(), by_key_name.c_str(), MDBX_DUPSORT, &by_key),
        "test state index DBI open failed");
    const std::vector<std::uint8_t> value =
        mdbxc::sync::encode_ordered_element_id_index(id);
    MDBX_val raw_key = make_raw_val(key);
    MDBX_val raw_value = make_raw_val(value);
    mdbxc::check_mdbx(mdbx_put(transaction.handle(), by_key,
                                &raw_key, &raw_value, MDBX_NODUPDATA),
                      "test state index write failed");
    transaction.commit();
}

void insert_raw_index_value(
        const std::shared_ptr<mdbxc::Connection>& connection,
        const std::string& by_key_name,
        const std::vector<std::uint8_t>& key,
        const std::vector<std::uint8_t>& value) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::WRITABLE);
    MDBX_dbi by_key = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(
        transaction.handle(), by_key_name.c_str(), MDBX_DUPSORT, &by_key),
        "test state index DBI open failed");
    MDBX_val raw_key = make_raw_val(key);
    MDBX_val raw_value = make_raw_val(value);
    mdbxc::check_mdbx(mdbx_put(transaction.handle(), by_key,
                                &raw_key, &raw_value, MDBX_NODUPDATA),
                      "test raw state index write failed");
    transaction.commit();
}

void delete_raw_index_record(
        const std::shared_ptr<mdbxc::Connection>& connection,
        const std::string& by_key_name,
        const std::vector<std::uint8_t>& key,
        const mdbxc::sync::OrderedElementId& id) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::WRITABLE);
    MDBX_dbi by_key = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(
        transaction.handle(), by_key_name.c_str(), MDBX_DUPSORT, &by_key),
        "test state index DBI open failed");
    const std::vector<std::uint8_t> value =
        mdbxc::sync::encode_ordered_element_id_index(id);
    MDBX_val raw_key = make_raw_val(key);
    MDBX_val raw_value = make_raw_val(value);
    mdbxc::check_mdbx(mdbx_del(transaction.handle(), by_key,
                                &raw_key, &raw_value),
                      "test state index delete failed");
    transaction.commit();
}

void delete_raw_index_value(
        const std::shared_ptr<mdbxc::Connection>& connection,
        const std::string& by_key_name,
        const std::vector<std::uint8_t>& key,
        const std::vector<std::uint8_t>& value) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::WRITABLE);
    MDBX_dbi by_key = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(
        transaction.handle(), by_key_name.c_str(), MDBX_DUPSORT, &by_key),
        "test state index DBI open failed");
    MDBX_val raw_key = make_raw_val(key);
    MDBX_val raw_value = make_raw_val(value);
    mdbxc::check_mdbx(mdbx_del(transaction.handle(), by_key,
                                &raw_key, &raw_value),
                      "test raw state index delete failed");
    transaction.commit();
}

void drop_named_dbi(const std::shared_ptr<mdbxc::Connection>& connection,
                    const std::string& name,
                    MDBX_db_flags_t flags) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::WRITABLE);
    MDBX_dbi dbi = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(
        transaction.handle(), name.c_str(), flags, &dbi),
        "test named DBI open failed");
    mdbxc::check_mdbx(mdbx_drop(transaction.handle(), dbi, true),
                      "test named DBI drop failed");
    transaction.commit();
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

void test_destructive_capture_rolls_back_injected_native_commit_failure() {
    const std::string path =
        "test_key_ordered_multi_value_destructive_native_commit_failure.mdbx";
    const std::string primary = "ordered_commit_failure_values";
    const std::string state = "ordered_commit_failure_state";
    const std::string by_key = "ordered_commit_failure_by_key";
    const std::string schema = "app.ordered_commit_failure.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0x62u);
    const mdbxc::sync::DbId local_db = make_node(0xA2u);
    const mdbxc::sync::DbId destination = make_node(0xB2u);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(origin, local_db);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    CommitFailureCaptureSink capture_sink;
    {
        mdbxc::sync::SyncCaptureScope capture_scope(connection, capture_sink);
        std::unique_ptr<adapter_type::LogicalCaptureSession> failed_session =
            adapter.begin_capture_session();
        const mdbxc::sync::OrderedElementId failed_id =
            failed_session->append(10, "discarded");
        if (failed_id.sequence != 1u || failed_session->pending_size() != 1u) {
            throw std::runtime_error("commit-failure capture setup is incorrect");
        }

        mdbxc::detail::fail_next_transaction_commit_for_test(MDBX_MAP_FULL);
        bool commit_failed = false;
        try {
            failed_session->commit_to_outbox(engine, destination);
        } catch (const mdbxc::MdbxException&) {
            commit_failed = true;
        }
        if (!commit_failed || failed_session->pending_size() != 0u ||
            capture_sink.flush_calls != 1u ||
            capture_sink.discard_calls != 1u ||
            !capture_sink.pending.empty()) {
            throw std::runtime_error(
                "native commit failure did not deactivate destructive capture");
        }

        bool reuse_rejected = false;
        try {
            failed_session->append(10, "must-not-append");
        } catch (const std::logic_error&) {
            reuse_rejected = true;
        }
        if (!reuse_rejected || !table.find(10).empty() ||
            !engine.pending_logical_deliveries(destination).empty()) {
            throw std::runtime_error(
                "native commit failure leaked destructive capture state");
        }
    }

    std::unique_ptr<adapter_type::LogicalCaptureSession> clean_session =
        adapter.begin_capture_session();
    const mdbxc::sync::OrderedElementId clean_id =
        clean_session->append(10, "committed");
    const mdbxc::sync::LogicalDeliveryEnvelope envelope =
        clean_session->commit_to_outbox(engine, destination);
    if (clean_id.sequence != 1u || envelope.origin_sequence != 1u ||
        table.find(10).size() != 1u || table.find(10)[0] != "committed" ||
        engine.pending_logical_deliveries(destination).size() != 1u) {
        throw std::runtime_error(
            "native commit failure did not roll back table, state, and outbox");
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_capture_resolves_bounded_broad_erasure() {
    const std::string path =
        "test_key_ordered_multi_value_destructive_broad_erasure.mdbx";
    const std::string primary = "ordered_broad_values";
    const std::string state = "ordered_broad_state";
    const std::string by_key = "ordered_broad_by_key";
    const std::string schema = "app.ordered_broad.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0x63u);
    const mdbxc::sync::DbId local_db = make_node(0xA3u);
    const mdbxc::sync::DbId destination = make_node(0xB3u);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(origin, local_db);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        session->append(1, "first");
        session->append(1, "same");
        session->append(1, "same");
        session->append(2, "other");
        session->commit_to_outbox(engine, destination);
    }

    const mdbxc::sync::BroadEraseBounds bounds = { 4u, 64u };
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        if (!session->erase_at(1, 1u, bounds) ||
            session->erase_value(1, "same", bounds) != 1u ||
            session->erase_key(2, bounds) != 1u) {
            throw std::runtime_error("bounded broad erasure selection is incorrect");
        }
        const mdbxc::sync::LogicalDeliveryEnvelope envelope =
            session->commit_to_outbox(engine, destination);
        if (envelope.frame.changes.size() != 3u) {
            throw std::runtime_error("broad erasure did not emit exact erase changes");
        }
        for (std::size_t i = 0u; i < envelope.frame.changes.size(); ++i) {
            if (envelope.frame.changes[i].opcode !=
                mdbxc::sync::KeyOrderedMultiValueDestructiveLogicalErase) {
                throw std::runtime_error("broad erasure emitted a non-exact opcode");
            }
        }
    }
    if (table.find(1).size() != 1u || table.find(1)[0] != "first" ||
        !table.find(2).empty()) {
        throw std::runtime_error("bounded broad erasure changed the wrong values");
    }

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        const mdbxc::sync::BroadEraseBounds no_selection = { 0u, 64u };
        bool rejected = false;
        try {
            session->erase_key(1, no_selection);
        } catch (const std::length_error&) {
            rejected = true;
        }
        const std::vector<std::string> values = table.find(1);
        if (!rejected || values.size() != 1u || values[0] != "first") {
            throw std::runtime_error(
                "broad selection-limit failure did not roll back the session");
        }
    }

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        const mdbxc::sync::BroadEraseBounds no_scan = { 4u, 0u };
        bool rejected = false;
        try {
            session->erase_at(99, 0u, no_scan);
        } catch (const std::length_error&) {
            rejected = true;
        }
        const std::vector<std::string> values = table.find(1);
        if (!rejected || values.size() != 1u || values[0] != "first") {
            throw std::runtime_error(
                "broad scan-limit failure did not roll back the session");
        }
    }

    if (table.find(1).size() != 1u || table.find(1)[0] != "first") {
        throw std::runtime_error("failed broad erasure changed durable table state");
    }

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        session->append(1, "same");
        session->append(1, "middle");
        session->append(1, "same");
        session->append(1, "last");
        session->commit_to_outbox(engine, destination);
    }
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        if (session->erase_value(1, "same", bounds) != 2u) {
            throw std::runtime_error(
                "broad erasure did not select every repeated value");
        }
        session->commit_to_outbox(engine, destination);
    }
    {
        const std::vector<std::string> values = table.find(1);
        if (values.size() != 3u || values[0] != "first" ||
            values[1] != "middle" || values[2] != "last") {
            throw std::runtime_error(
                "broad erasure shifted surviving duplicate positions");
        }
    }

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        session->append(3, "transient");
        if (session->erase_value(3, "transient", bounds) != 1u ||
            session->pending_size() != 0u) {
            throw std::runtime_error(
                "broad erasure did not coalesce a pending append");
        }
        const mdbxc::sync::LogicalDeliveryEnvelope envelope =
            session->commit_to_outbox(engine, destination);
        if (!envelope.frame.changes.empty() || !table.find(3).empty()) {
            throw std::runtime_error(
                "coalesced broad erasure leaked a physical or logical record");
        }
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_capture_bounds_post_selection_mutation_scans() {
    const std::string path =
        "test_key_ordered_multi_value_destructive_mutation_bounds.mdbx";
    const std::string primary = "ordered_mutation_bound_values";
    const std::string state = "ordered_mutation_bound_state";
    const std::string by_key = "ordered_mutation_bound_by_key";
    const std::string schema = "app.ordered_mutation_bound.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0x65u);
    const mdbxc::sync::DbId local_db = make_node(0xA5u);
    const mdbxc::sync::DbId destination = make_node(0xB5u);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(origin, local_db);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        session->append(1, "first");
        session->append(1, "second");
        session->commit_to_outbox(engine, destination);
    }

    std::size_t selection_limit = 0u;
    bool selection_found = false;
    for (std::size_t limit = 0u; limit < 256u; ++limit) {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        try {
            if (session->erase_at(1, 99u, { 2u, limit })) {
                throw std::runtime_error("missing ordered index was selected");
            }
            selection_limit = limit;
            selection_found = true;
            break;
        } catch (const std::length_error&) {
        }
    }
    if (!selection_found) {
        throw std::runtime_error("could not establish ordered selection bound");
    }

    std::unique_ptr<adapter_type::LogicalCaptureSession> session =
        adapter.begin_capture_session();
    bool rejected = false;
    try {
        session->erase_at(1, 0u, { 2u, selection_limit });
    } catch (const std::length_error&) {
        rejected = true;
    }
    if (!rejected || table.find(1).size() != 2u ||
        table.find(1)[0] != "first" || table.find(1)[1] != "second") {
        throw std::runtime_error(
            "post-selection scan limit did not roll back ordered erasure");
    }
    bool reuse_rejected = false;
    try {
        session->erase_at(1, 0u, { 2u, 256u });
    } catch (const std::logic_error&) {
        reuse_rejected = true;
    }
    if (!reuse_rejected) {
        throw std::runtime_error(
            "post-selection scan-limit failure left capture active");
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_capture_clears_bounded_tombstone_heavy_table() {
    const std::string path =
        "test_key_ordered_multi_value_destructive_broad_clear.mdbx";
    const std::string primary = "ordered_clear_values";
    const std::string state = "ordered_clear_state";
    const std::string by_key = "ordered_clear_by_key";
    const std::string schema = "app.ordered_clear.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0x64u);
    const mdbxc::sync::DbId local_db = make_node(0xA4u);
    const mdbxc::sync::DbId destination = make_node(0xB4u);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(origin, local_db);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));

    mdbxc::sync::OrderedElementId first;
    mdbxc::sync::OrderedElementId second;
    mdbxc::sync::OrderedElementId third;
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        first = session->append(1, "first");
        second = session->append(1, "second");
        third = session->append(2, "third");
        session->append(2, "fourth");
        session->append(3, "fifth");
        session->commit_to_outbox(engine, destination);
    }
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        session->erase(first);
        session->erase(second);
        session->erase(third);
        session->commit_to_outbox(engine, destination);
    }
    if (table.count() != 2u || table.find(2).size() != 1u ||
        table.find(3).size() != 1u) {
        throw std::runtime_error("ordered clear tombstone fixture is invalid");
    }

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        bool rejected = false;
        try {
            session->clear({ 1u, 256u });
        } catch (const std::length_error&) {
            rejected = true;
        }
        if (!rejected || table.count() != 2u) {
            throw std::runtime_error(
                "ordered clear selected beyond its candidate limit");
        }
    }

    const mdbxc::sync::BroadEraseBounds bounds = { 8u, 256u };
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        if (session->clear(bounds) != 2u) {
            throw std::runtime_error("ordered broad clear selected the wrong ids");
        }
        const mdbxc::sync::LogicalDeliveryEnvelope envelope =
            session->commit_to_outbox(engine, destination);
        if (envelope.frame.changes.size() != 2u) {
            throw std::runtime_error("ordered broad clear emitted the wrong changes");
        }
        for (std::size_t i = 0u; i < envelope.frame.changes.size(); ++i) {
            if (envelope.frame.changes[i].opcode !=
                mdbxc::sync::KeyOrderedMultiValueDestructiveLogicalErase) {
                throw std::runtime_error("ordered broad clear emitted a non-erase");
            }
        }
    }
    if (table.count() != 0u) {
        throw std::runtime_error("ordered broad clear left a live physical value");
    }

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        session->append(4, "survives");
        session->commit_to_outbox(engine, destination);
    }
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        const mdbxc::sync::BroadEraseBounds no_selection = { 0u, 256u };
        bool rejected = false;
        try {
            session->clear(no_selection);
        } catch (const std::length_error&) {
            rejected = true;
        }
        const std::vector<std::string> values = table.find(4);
        if (!rejected || values.size() != 1u || values[0] != "survives") {
            throw std::runtime_error(
                "ordered broad clear selection failure did not roll back");
        }
    }

    table.append(5, "untracked");
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        bool rejected = false;
        try {
            session->clear(bounds);
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        if (!rejected || table.find(4).size() != 1u ||
            table.find(5).size() != 1u) {
            throw std::runtime_error(
                "ordered broad clear accepted untracked physical data");
        }
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_capture_clear_rejects_complete_schema_corruption() {
    const std::string path =
        "test_key_ordered_multi_value_destructive_clear_corruption.mdbx";
    const std::string primary = "ordered_clear_corrupt_values";
    const std::string state = "ordered_clear_corrupt_state";
    const std::string by_key = "ordered_clear_corrupt_by_key";
    const std::string schema = "app.ordered_clear_corrupt.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0x66u);
    const mdbxc::sync::DbId local_db = make_node(0xA6u);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(origin, local_db);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    const mdbxc::sync::BroadEraseBounds bounds = { 8u, 1024u };
    const std::vector<std::uint8_t> key = IntKeyCodec::encode(7);
    mdbxc::sync::OrderedElementId orphan;
    orphan.origin = origin;
    orphan.sequence = 1u;

    insert_raw_index_record(connection, by_key, key, orphan);
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        bool rejected = false;
        try {
            session->clear(bounds);
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        if (!rejected || !table.empty()) {
            throw std::runtime_error("ordered clear accepted orphan key index id");
        }
    }
    delete_raw_index_record(connection, by_key, key, orphan);

    const std::vector<std::uint8_t> malformed_index_value(1u, 0x01u);
    insert_raw_index_value(
        connection, by_key, key, malformed_index_value);
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        bool rejected = false;
        try {
            session->clear(bounds);
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        if (!rejected || !table.empty()) {
            throw std::runtime_error("ordered clear accepted malformed index id");
        }
    }
    delete_raw_index_value(connection, by_key, key, malformed_index_value);

    mdbxc::sync::OrderedElementId tombstone_id;
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        tombstone_id = session->append(8, "tombstone");
        session->commit_to_outbox(engine, make_node(0xB6u));
    }
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        session->erase(tombstone_id);
        session->commit_to_outbox(engine, make_node(0xB6u));
    }
    insert_raw_index_record(
        connection, by_key, IntKeyCodec::encode(8), tombstone_id);
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        bool rejected = false;
        try {
            session->clear(bounds);
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        if (!rejected || !table.empty()) {
            throw std::runtime_error("ordered clear accepted tombstone index id");
        }
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_capture_clear_checks_tombstone_only_origin_high_water() {
    const std::string path =
        "test_key_ordered_multi_value_destructive_clear_high_water.mdbx";
    const std::string primary = "ordered_clear_high_water_values";
    const std::string state = "ordered_clear_high_water_state";
    const std::string by_key = "ordered_clear_high_water_by_key";
    const std::string schema = "app.ordered_clear_high_water.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0x67u);
    const mdbxc::sync::NodeId tombstone_origin = make_node(0x77u);
    const mdbxc::sync::DbId local_db = make_node(0xA7u);
    const mdbxc::sync::DbId destination = make_node(0xB7u);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(origin, local_db);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        session->append(1, "live");
        session->commit_to_outbox(engine, destination);
    }

    mdbxc::sync::OrderedElementId tombstone_id;
    tombstone_id.origin = tombstone_origin;
    tombstone_id.sequence = 1u;
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        mdbxc::Connection::SyncCaptureSuppressionScope suppress_capture(
            *connection, transaction.handle());
        table.append(2, "tombstone", transaction);
        adapter.state_store().put_live(
            transaction.handle(), tombstone_id,
            IntKeyCodec::encode(2), StringValueCodec::encode("tombstone"));
        if (!table.erase_at(2, 0u, transaction)) {
            throw std::runtime_error("tombstone-only origin fixture is invalid");
        }
        adapter.state_store().tombstone(transaction.handle(), tombstone_id);
        transaction.commit();
    }
    delete_raw_introduced_high_water(connection, state, tombstone_origin);

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        bool rejected = false;
        try {
            session->clear({ 8u, 1024u });
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        if (!rejected || table.find(1).size() != 1u ||
            table.find(1)[0] != "live") {
            throw std::runtime_error(
                "ordered clear missed tombstone-only origin high-water corruption");
        }
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_capture_trusted_clear_avoids_repeated_state_scans() {
    const std::string path =
        "test_key_ordered_multi_value_destructive_trusted_clear.mdbx";
    const std::string primary = "ordered_trusted_clear_values";
    const std::string state = "ordered_trusted_clear_state";
    const std::string by_key = "ordered_trusted_clear_by_key";
    const std::string schema = "app.ordered_trusted_clear.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0x68u);
    const mdbxc::sync::DbId local_db = make_node(0xA8u);
    const mdbxc::sync::DbId destination = make_node(0xB8u);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(origin, local_db);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));

    std::vector<mdbxc::sync::OrderedElementId> ids;
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        for (std::size_t i = 0u; i < 64u; ++i) {
            ids.push_back(session->append(1, "value"));
        }
        session->commit_to_outbox(engine, destination);
    }
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        for (std::size_t i = 0u; i + 1u < ids.size(); ++i) {
            session->erase(ids[i]);
        }
        session->commit_to_outbox(engine, destination);
    }

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        const mdbxc::sync::BroadEraseBounds bounds = { 1u, 160u };
        if (session->clear(bounds) != 1u) {
            throw std::runtime_error("trusted ordered clear selected the wrong id");
        }
        session->commit_to_outbox(engine, destination);
    }
    if (!table.empty()) {
        throw std::runtime_error(
            "trusted ordered clear did not remove the final live value");
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_capture_uses_transaction_bound_key_index_proof() {
    const std::string path =
        "test_key_ordered_multi_value_destructive_key_index_proof.mdbx";
    const std::string primary = "ordered_proof_values";
    const std::string state = "ordered_proof_state";
    const std::string by_key = "ordered_proof_by_key";
    const std::string schema = "app.ordered_proof.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0x69u);
    const mdbxc::sync::DbId local_db = make_node(0xA9u);
    const mdbxc::sync::DbId destination = make_node(0xB9u);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(origin, local_db);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        session->append(1, "first");
        session->append(1, "second");
        session->append(1, "third");
        session->commit_to_outbox(engine, destination);
    }

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        bool default_rejected = false;
        try {
            (void)session->erase_value(1, "second", { 3u, 24u });
        } catch (const std::length_error&) {
            default_rejected = true;
        }
        if (!default_rejected || table.find(1).size() != 3u) {
            throw std::runtime_error(
                "default ordered selector did not enforce reverse-scan budget");
        }
    }

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        const mdbxc::sync::OrderedElementKeyIndexProof proof =
            session->validate_key_index(1, { 3u, 32u });
        if (session->erase_value_trusted(1, "second", proof, { 3u, 24u }) != 1u) {
            throw std::runtime_error(
                "trusted ordered selector removed the wrong number of values");
        }
        session->commit_to_outbox(engine, destination);
    }
    const std::vector<std::string> remaining = table.find(1);
    if (remaining.size() != 2u || remaining[0] != "first" ||
        remaining[1] != "third") {
        throw std::runtime_error(
            "trusted ordered selector changed the wrong physical values");
    }

    {
        std::unique_ptr<adapter_type::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        const mdbxc::sync::OrderedElementKeyIndexProof proof =
            session->validate_key_index(1, { 3u, 32u });
        session->append(1, "new");
        bool stale_rejected = false;
        try {
            (void)session->erase_key_trusted(1, proof, { 3u, 32u });
        } catch (const std::logic_error&) {
            stale_rejected = true;
        }
        if (!stale_rejected) {
            throw std::runtime_error(
                "ordered key index proof survived a capture mutation");
        }
    }
    if (table.find(1).size() != 2u) {
        throw std::runtime_error(
            "stale ordered key index proof changed the table");
    }

    connection->disconnect();
    cleanup(path);
}

void test_ordered_delivery_rolls_back_malformed_v2_change_and_deduplicates() {
    const std::string path = "test_key_ordered_multi_value_destructive_replay.mdbx";
    const std::string primary = "ordered_replay_values";
    const std::string state = "ordered_replay_state";
    const std::string by_key = "ordered_replay_by_key";
    const std::string schema = "app.ordered_replay.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId receiver_node = make_node(0x71u);
    const mdbxc::sync::NodeId origin = make_node(0x81u);
    const mdbxc::sync::DbId receiver_db = make_node(0xC1u);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(receiver_node, receiver_db);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    engine.register_logical_adapter(adapter);

    mdbxc::sync::OrderedElementId id;
    id.origin = origin;
    id.sequence = 1u;
    const mdbxc::sync::LogicalChange valid_change =
        adapter.make_append(id, 17, "seventeen");
    mdbxc::sync::LogicalDeliveryEnvelope malformed;
    malformed.destination_db_uuid = receiver_db;
    malformed.origin_node_id = origin;
    malformed.origin_sequence = 1u;
    malformed.frame_id = "ordered-v2-frame-1";
    malformed.frame.changes.push_back(valid_change);
    malformed.frame.changes[0].payload.resize(3u);

    const mdbxc::sync::LogicalDeliveryAcknowledgement failed =
        engine.apply_ordered_logical_delivery_envelope(malformed);
    if (failed.ok || failed.acknowledged_through != 0u || !table.find(17).empty()) {
        throw std::runtime_error("malformed v2 delivery changed durable state");
    }

    mdbxc::sync::LogicalDeliveryEnvelope valid = malformed;
    valid.frame.changes[0] = valid_change;
    const mdbxc::sync::LogicalDeliveryAcknowledgement applied =
        engine.apply_ordered_logical_delivery_envelope(valid);
    if (!applied.ok || applied.acknowledged_through != 1u ||
        table.find(17).size() != 1u || table.find(17)[0] != "seventeen") {
        throw std::runtime_error("valid v2 delivery did not apply after rollback");
    }

    const mdbxc::sync::LogicalDeliveryAcknowledgement replay =
        engine.apply_ordered_logical_delivery_envelope(valid);
    if (!replay.ok || replay.acknowledged_through != 1u ||
        table.find(17).size() != 1u) {
        throw std::runtime_error("exact v2 replay was not a no-op");
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_preflight_rejects_untracked_physical_value() {
    const std::string path = "test_key_ordered_multi_value_destructive_parity.mdbx";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const std::string primary = "ordered_parity_values";
    const std::string state = "ordered_parity_state";
    const std::string by_key = "ordered_parity_by_key";
    const std::string schema = "app.ordered_parity.v2";
    const mdbxc::sync::NodeId origin = make_node(0x91u);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(make_node(0x92u), make_node(0x93u));
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    mdbxc::sync::LogicalTableRegistry registry;
    registry.register_adapter(&adapter);
    table.append(5, "raw");

    mdbxc::sync::OrderedElementId id;
    id.origin = origin;
    id.sequence = 1u;
    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(adapter.make_append(id, 5, "logical"));
    apply_changes(registry, connection, changes, false);
    const std::vector<std::string> values = table.find(5);
    if (values.size() != 1u || values[0] != "raw") {
        throw std::runtime_error("parity preflight changed raw physical data");
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_preflight_rejects_state_index_corruption() {
    const std::string path = "test_key_ordered_multi_value_destructive_state_parity.mdbx";
    const std::string primary = "state_parity_values";
    const std::string state = "state_parity_state";
    const std::string by_key = "state_parity_by_key";
    const std::string schema = "app.state_parity.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0xA1u);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(make_node(0xA2u), make_node(0xA3u));
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    mdbxc::sync::LogicalTableRegistry registry;
    registry.register_adapter(&adapter);

    const std::vector<std::uint8_t> key1 = IntKeyCodec::encode(1);
    const std::vector<std::uint8_t> key2 = IntKeyCodec::encode(2);
    const std::vector<std::uint8_t> key3 = IntKeyCodec::encode(3);
    const std::vector<std::uint8_t> key4 = IntKeyCodec::encode(4);
    const std::vector<std::uint8_t> key5 = IntKeyCodec::encode(5);
    const std::vector<std::uint8_t> key6 = IntKeyCodec::encode(6);
    const std::vector<std::uint8_t> value = StringValueCodec::encode("state");
    mdbxc::sync::OrderedElementId ids[5];
    for (std::size_t i = 0u; i < 5u; ++i) {
        ids[i].origin = origin;
        ids[i].sequence = i + 1u;
    }
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        adapter.state_store().put_live(transaction.handle(), ids[0], key1, value);
        adapter.state_store().put_live(transaction.handle(), ids[1], key2, value);
        adapter.state_store().put_live(transaction.handle(), ids[2], key3, value);
        adapter.state_store().put_live(transaction.handle(), ids[3], key5, value);
        adapter.state_store().put_live(transaction.handle(), ids[4], key6, value);
        adapter.state_store().tombstone(transaction.handle(), ids[2]);
        transaction.commit();
    }
    delete_raw_index_record(connection, by_key, key1, ids[0]);
    delete_raw_state_record(connection, state, ids[1]);
    insert_raw_index_record(connection, by_key, key3, ids[2]);
    insert_raw_index_record(connection, by_key, key4, ids[3]);

    const int keys[5] = { 1, 2, 3, 4, 6 };
    for (std::size_t i = 0u; i < 5u; ++i) {
        mdbxc::sync::OrderedElementId candidate;
        candidate.origin = origin;
        candidate.sequence = 10u + i;
        std::vector<mdbxc::sync::LogicalChange> changes;
        changes.push_back(adapter.make_append(candidate, keys[i], "candidate"));
        apply_changes(registry, connection, changes, false);
    }
    if (!table.empty()) {
        throw std::runtime_error("state/index corruption changed the table");
    }

    connection->disconnect();
    cleanup(path);
}

void test_ordered_delivery_rejects_missing_auxiliary_dbis() {
    const int cases[3] = { 0, 1, 2 };
    for (std::size_t i = 0u; i < 3u; ++i) {
        const bool drop_state = cases[i] == 0 || cases[i] == 2;
        const bool drop_index = cases[i] == 1 || cases[i] == 2;
        const std::string path =
            std::string("test_key_ordered_multi_value_destructive_missing_") +
            (cases[i] == 0 ? "state.mdbx" :
             (cases[i] == 1 ? "index.mdbx" : "both.mdbx"));
        const std::string primary = "missing_aux_values";
        const std::string state = "missing_aux_state";
        const std::string by_key = "missing_aux_by_key";
        const std::string schema = "app.missing_aux.v2";
        cleanup(path);

        mdbxc::Config config;
        config.pathname = path;
        config.max_dbs = 16;
        config.no_subdir = true;
        const std::shared_ptr<mdbxc::Connection> connection =
            mdbxc::Connection::create(config);
        const mdbxc::sync::NodeId receiver = make_node(0xB1u);
        const mdbxc::sync::NodeId origin = make_node(0xC1u);
        const mdbxc::sync::DbId db_id = make_node(0xD1u);
        table_type table(connection, primary);
        adapter_type adapter(table, schema, state, by_key);
        mdbxc::sync::SyncEngine engine(connection);
        engine.initialize_local_identity(receiver, db_id);
        const mdbxc::sync::LogicalSchemaRecord record =
            make_v2_record(primary, state, by_key, origin);
        engine.initialize_logical_adapter_schema(adapter, record);
        engine.register_logical_adapter(adapter);
        if (drop_state) {
            drop_named_dbi(connection, state,
                           static_cast<MDBX_db_flags_t>(0));
        }
        if (drop_index) {
            drop_named_dbi(connection, by_key, MDBX_DUPSORT);
        }

        mdbxc::sync::OrderedElementId id;
        id.origin = origin;
        id.sequence = 1u;
        mdbxc::sync::LogicalDeliveryEnvelope envelope;
        envelope.destination_db_uuid = db_id;
        envelope.origin_node_id = origin;
        envelope.origin_sequence = 1u;
        envelope.frame_id = cases[i] == 0 ? "missing-state" :
                            (cases[i] == 1 ? "missing-index" : "missing-both");
        envelope.frame.changes.push_back(adapter.make_append(id, 21, "value"));
        const mdbxc::sync::LogicalDeliveryAcknowledgement failed =
            engine.apply_ordered_logical_delivery_envelope(envelope);
        if (failed.ok || !table.empty()) {
            throw std::runtime_error(
                "missing destructive auxiliary DBI was treated as empty");
        }

        bool setup_rejected = false;
        try {
            engine.initialize_logical_adapter_schema(adapter, record);
        } catch (const std::exception&) {
            setup_rejected = true;
        }
        if (!setup_rejected) {
            throw std::runtime_error(
                "existing marker recreated a missing auxiliary DBI");
        }

        const std::string& missing_name = drop_state ? state : by_key;
        const MDBX_db_flags_t missing_flags =
            drop_state ? static_cast<MDBX_db_flags_t>(0) : MDBX_DUPSORT;
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBX_dbi missing_dbi = 0;
        const int rc = mdbx_dbi_open(transaction.handle(), missing_name.c_str(),
                                     missing_flags, &missing_dbi);
        transaction.rollback();
        if (rc != MDBX_NOTFOUND) {
            throw std::runtime_error(
                "failed setup recreated a missing destructive auxiliary DBI");
        }

        connection->disconnect();
        cleanup(path);
    }
}

void test_destructive_schema_setup_requires_empty_primary() {
    const std::string path = "test_key_ordered_multi_value_destructive_setup.mdbx";
    const std::string primary = "setup_values";
    const std::string state = "setup_state";
    const std::string by_key = "setup_by_key";
    const std::string schema = "app.setup.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0xD1u);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(make_node(0xD2u), make_node(0xD3u));
    table.append(1, "untracked");

    bool rejected = false;
    try {
        engine.initialize_logical_adapter_schema(
            adapter, make_v2_record(primary, state, by_key, origin));
    } catch (const std::exception&) {
        rejected = true;
    }
    if (!rejected || table.find(1).size() != 1u) {
        throw std::runtime_error(
            "destructive schema setup accepted a non-empty primary DBI");
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_schema_reopen_rejects_missing_primary() {
    const std::string path = "test_key_ordered_multi_value_destructive_missing_primary.mdbx";
    const std::string primary = "missing_primary_values";
    const std::string state = "missing_primary_state";
    const std::string by_key = "missing_primary_by_key";
    const std::string schema = "app.missing_primary.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const mdbxc::sync::NodeId origin = make_node(0xD3u);
    {
        const std::shared_ptr<mdbxc::Connection> connection =
            mdbxc::Connection::create(config);
        const std::shared_ptr<table_type> table =
            adapter_type::open_primary_for_schema(connection, schema, primary);
        adapter_type adapter(*table, schema, state, by_key);
        mdbxc::sync::SyncEngine engine(connection);
        engine.initialize_local_identity(make_node(0xD4u), make_node(0xD5u));
        engine.initialize_logical_adapter_schema(
            adapter, make_v2_record(primary, state, by_key, origin));
        drop_named_dbi(connection, primary, static_cast<MDBX_db_flags_t>(0));
        connection->disconnect();
    }

    {
        const std::shared_ptr<mdbxc::Connection> connection =
            mdbxc::Connection::create(config);
        bool rejected = false;
        try {
            adapter_type::open_primary_for_schema(connection, schema, primary);
        } catch (const std::exception&) {
            rejected = true;
        }
        if (!rejected) {
            throw std::runtime_error(
                "destructive schema reopen recreated a missing primary DBI");
        }

        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBX_dbi dbi = 0;
        const int rc = mdbx_dbi_open(transaction.handle(), primary.c_str(),
                                     static_cast<MDBX_db_flags_t>(0), &dbi);
        transaction.rollback();
        if (rc != MDBX_NOTFOUND) {
            throw std::runtime_error(
                "failed destructive schema reopen recreated primary DBI");
        }
        connection->disconnect();
    }
    cleanup(path);
}

void test_destructive_preflight_rejects_corrupt_introduced_high_water() {
    const std::string path = "test_key_ordered_multi_value_destructive_high_water.mdbx";
    const std::string primary = "high_water_values";
    const std::string state = "high_water_state";
    const std::string by_key = "high_water_by_key";
    const std::string schema = "app.high_water.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0xD6u);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(make_node(0xD7u), make_node(0xD8u));
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    mdbxc::sync::LogicalTableRegistry registry;
    registry.register_adapter(&adapter);

    mdbxc::sync::OrderedElementId id2;
    id2.origin = origin;
    id2.sequence = 2u;
    mdbxc::sync::OrderedElementId id1;
    id1.origin = origin;
    id1.sequence = 1u;
    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(adapter.make_append(id2, 9, "same"));
    apply_changes(registry, connection, changes, true);

    delete_raw_introduced_high_water(connection, state, origin);
    changes.clear();
    changes.push_back(adapter.make_append(id1, 9, "same"));
    apply_changes(registry, connection, changes, false);
    const std::vector<std::string> values = table.find(9);
    if (values.size() != 1u || values[0] != "same") {
        throw std::runtime_error(
            "corrupt introduced high-water changed the physical ordered table");
    }

    connection->disconnect();
    cleanup(path);
}

void test_destructive_ordering_and_duplicate_append_contract() {
    const std::string path = "test_key_ordered_multi_value_destructive_ordering.mdbx";
    const std::string primary = "ordering_values";
    const std::string state = "ordering_state";
    const std::string by_key = "ordering_by_key";
    const std::string schema = "app.ordering.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0xD4u);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(make_node(0xD5u), make_node(0xD6u));
    engine.initialize_logical_adapter_schema(
        adapter, make_v2_record(primary, state, by_key, origin));
    mdbxc::sync::LogicalTableRegistry registry;
    registry.register_adapter(&adapter);

    mdbxc::sync::OrderedElementId id2;
    id2.origin = origin;
    id2.sequence = 2u;
    mdbxc::sync::OrderedElementId id1;
    id1.origin = origin;
    id1.sequence = 1u;
    mdbxc::sync::OrderedElementId id5;
    id5.origin = origin;
    id5.sequence = 5u;
    mdbxc::sync::OrderedElementId id8;
    id8.origin = origin;
    id8.sequence = 8u;
    mdbxc::sync::OrderedElementId id7;
    id7.origin = origin;
    id7.sequence = 7u;
    mdbxc::sync::OrderedElementId id9;
    id9.origin = origin;
    id9.sequence = 9u;
    mdbxc::sync::OrderedElementId id10;
    id10.origin = origin;
    id10.sequence = 10u;
    mdbxc::sync::OrderedElementId id11;
    id11.origin = origin;
    id11.sequence = 11u;

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(adapter.make_append(id2, 7, "same"));
    apply_changes(registry, connection, changes, true);
    changes.clear();
    changes.push_back(adapter.make_append(id1, 7, "same"));
    apply_changes(registry, connection, changes, false);
    if (table.find(7).size() != 1u || table.find(7)[0] != "same") {
        throw std::runtime_error(
            "out-of-order ordered element changed the physical table");
    }

    changes.clear();
    changes.push_back(adapter.make_append(id5, 7, "five"));
    apply_changes(registry, connection, changes, true);
    changes.clear();
    changes.push_back(adapter.make_append(id8, 7, "eight"));
    apply_changes(registry, connection, changes, true);
    changes.clear();
    changes.push_back(adapter.make_append(id8, 7, "eight"));
    apply_changes(registry, connection, changes, true);
    if (table.find(7).size() != 3u) {
        throw std::runtime_error("idempotent ordered append duplicated a row");
    }

    changes.clear();
    changes.push_back(adapter.make_append(id8, 7, "conflict"));
    apply_changes(registry, connection, changes, false);
    changes.clear();
    changes.push_back(adapter.make_erase(id5));
    apply_changes(registry, connection, changes, true);
    changes.clear();
    changes.push_back(adapter.make_append(id5, 7, "five"));
    apply_changes(registry, connection, changes, false);
    changes.clear();
    changes.push_back(adapter.make_append(id7, 7, "seven"));
    apply_changes(registry, connection, changes, false);

    changes.clear();
    changes.push_back(adapter.make_append(id9, 7, "nine"));
    changes.push_back(adapter.make_append(id9, 7, "nine"));
    apply_changes(registry, connection, changes, false);
    if (table.find(7).size() != 2u) {
        throw std::runtime_error("duplicate append in one batch changed a row");
    }

    changes.clear();
    changes.push_back(adapter.make_append(id11, 7, "eleven"));
    changes.push_back(adapter.make_append(id10, 7, "ten"));
    apply_changes(registry, connection, changes, false);
    if (table.find(7).size() != 2u) {
        throw std::runtime_error(
            "out-of-order destructive batch changed the physical table");
    }

    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        const mdbxc::sync::OrderedElementId allocated =
            adapter.state_store().allocate_id(transaction.handle(), origin);
        if (allocated.sequence != 9u) {
            throw std::runtime_error(
                "local allocator did not advance past remote introduced ids");
        }
        transaction.commit();
    }

    connection->disconnect();
    cleanup(path);
}

void test_ordered_delivery_survives_environment_reopen() {
    const std::string path = "test_key_ordered_multi_value_destructive_delivery_reopen.mdbx";
    const std::string primary = "delivery_reopen_values";
    const std::string state = "delivery_reopen_state";
    const std::string by_key = "delivery_reopen_by_key";
    const std::string schema = "app.delivery_reopen.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const mdbxc::sync::NodeId receiver = make_node(0xE1u);
    const mdbxc::sync::NodeId origin = make_node(0xE2u);
    const mdbxc::sync::DbId db_id = make_node(0xE3u);
    mdbxc::sync::OrderedElementId id;
    id.origin = origin;
    id.sequence = 1u;
    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = db_id;
    envelope.origin_node_id = origin;
    envelope.origin_sequence = 1u;
    envelope.frame_id = "ordered-v2-reopen";

    {
        const std::shared_ptr<mdbxc::Connection> connection =
            mdbxc::Connection::create(config);
        table_type table(connection, primary);
        adapter_type adapter(table, schema, state, by_key);
        mdbxc::sync::SyncEngine engine(connection);
        engine.initialize_local_identity(receiver, db_id);
        engine.initialize_logical_adapter_schema(
            adapter, make_v2_record(primary, state, by_key, origin));
        engine.register_logical_adapter(adapter);
        envelope.frame.changes.push_back(adapter.make_append(id, 42, "persisted"));
        const mdbxc::sync::LogicalDeliveryAcknowledgement applied =
            engine.apply_ordered_logical_delivery_envelope(envelope);
        if (!applied.ok || applied.acknowledged_through != 1u) {
            throw std::runtime_error("ordered delivery did not commit before reopen");
        }
        connection->disconnect();
    }

    {
        const std::shared_ptr<mdbxc::Connection> connection =
            mdbxc::Connection::create(config);
        const std::shared_ptr<table_type> table =
            adapter_type::open_primary_for_schema(connection, schema, primary);
        adapter_type adapter(*table, schema, state, by_key);
        mdbxc::sync::SyncEngine engine(connection);
        engine.initialize_local_identity(receiver, db_id);
        engine.initialize_logical_adapter_schema(
            adapter, make_v2_record(primary, state, by_key, origin));
        engine.register_logical_adapter(adapter);
        const mdbxc::sync::LogicalDeliveryAcknowledgement replay =
            engine.apply_ordered_logical_delivery_envelope(envelope);
        if (!replay.ok || replay.acknowledged_through != 1u ||
            table->find(42).size() != 1u) {
            throw std::runtime_error(
                "ordered delivery replay was not durable across environment reopen");
        }
        {
            mdbxc::Transaction transaction =
                connection->transaction(mdbxc::TransactionMode::READ_ONLY);
            mdbxc::sync::LogicalDeliveryOrderStore order(connection->env_handle());
            if (order.last_applied(transaction.handle(), origin) != 1u) {
                throw std::runtime_error(
                    "ordered delivery frontier did not survive environment reopen");
            }
            transaction.rollback();
        }
        connection->disconnect();
    }

    cleanup(path);
}

void test_ordered_delivery_rejects_incompatible_auxiliary_dbi() {
    const std::string path = "test_key_ordered_multi_value_destructive_bad_index.mdbx";
    const std::string primary = "bad_index_values";
    const std::string state = "bad_index_state";
    const std::string by_key = "bad_index_by_key";
    const std::string schema = "app.bad_index.v2";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId receiver = make_node(0xB4u);
    const mdbxc::sync::NodeId origin = make_node(0xC4u);
    const mdbxc::sync::DbId db_id = make_node(0xD4u);
    table_type table(connection, primary);
    adapter_type adapter(table, schema, state, by_key);
    mdbxc::sync::SyncEngine engine(connection);
    engine.initialize_local_identity(receiver, db_id);
    const mdbxc::sync::LogicalSchemaRecord record =
        make_v2_record(primary, state, by_key, origin);
    engine.initialize_logical_adapter_schema(adapter, record);
    engine.register_logical_adapter(adapter);
    drop_named_dbi(connection, by_key, MDBX_DUPSORT);
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBX_dbi bad_index = 0;
        mdbxc::check_mdbx(mdbx_dbi_open(
            transaction.handle(), by_key.c_str(), MDBX_CREATE, &bad_index),
            "test incompatible index DBI creation failed");
        transaction.commit();
    }

    mdbxc::sync::OrderedElementId id;
    id.origin = origin;
    id.sequence = 1u;
    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = db_id;
    envelope.origin_node_id = origin;
    envelope.origin_sequence = 1u;
    envelope.frame_id = "bad-index";
    envelope.frame.changes.push_back(adapter.make_append(id, 22, "value"));
    const mdbxc::sync::LogicalDeliveryAcknowledgement failed =
        engine.apply_ordered_logical_delivery_envelope(envelope);
    if (failed.ok || !table.empty()) {
        throw std::runtime_error(
            "incompatible destructive auxiliary DBI was accepted");
    }

    bool setup_rejected = false;
    try {
        engine.initialize_logical_adapter_schema(adapter, record);
    } catch (const std::exception&) {
        setup_rejected = true;
    }
    if (!setup_rejected) {
        throw std::runtime_error("incompatible auxiliary DBI passed setup");
    }

    connection->disconnect();
    cleanup(path);
}

} // namespace

int main() {
    test_destructive_append_erase_and_batch_preflight();
    test_destructive_preflight_rejects_foreign_append_id();
    test_destructive_capture_coalesces_and_commits_to_outbox();
    test_destructive_capture_rolls_back_injected_native_commit_failure();
    test_destructive_capture_resolves_bounded_broad_erasure();
    test_destructive_capture_bounds_post_selection_mutation_scans();
    test_destructive_capture_clears_bounded_tombstone_heavy_table();
    test_destructive_capture_clear_rejects_complete_schema_corruption();
    test_destructive_capture_clear_checks_tombstone_only_origin_high_water();
    test_destructive_capture_trusted_clear_avoids_repeated_state_scans();
    test_destructive_capture_uses_transaction_bound_key_index_proof();
    test_ordered_delivery_rolls_back_malformed_v2_change_and_deduplicates();
    test_destructive_preflight_rejects_untracked_physical_value();
    test_destructive_preflight_rejects_state_index_corruption();
    test_ordered_delivery_rejects_missing_auxiliary_dbis();
    test_ordered_delivery_rejects_incompatible_auxiliary_dbi();
    test_destructive_schema_setup_requires_empty_primary();
    test_destructive_schema_reopen_rejects_missing_primary();
    test_destructive_preflight_rejects_corrupt_introduced_high_water();
    test_destructive_ordering_and_duplicate_append_contract();
    test_ordered_delivery_survives_environment_reopen();
    return 0;
}
