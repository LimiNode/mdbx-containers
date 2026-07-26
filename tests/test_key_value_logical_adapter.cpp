#include <mdbx_containers/sync.hpp>

#include "test_assert.hpp"

#include <cstdio>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

typedef mdbxc::sync::KeyValueLogicalInt32Codec<int> IntKeyCodec;
typedef mdbxc::sync::KeyValueLogicalStringCodec<std::string> StringValueCodec;
typedef mdbxc::sync::KeyValueTableLogicalAdapter<
    int, std::string, IntKeyCodec, StringValueCodec> IntStringAdapter;

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
    test_key_value_logical_adapter_rejects_malformed_payload();
    test_key_value_logical_adapter_uses_stable_payload();
    test_key_value_logical_adapter_decodes_literal_little_endian_payload();
    test_key_value_logical_apply_does_not_recapture_incoming_change();
    return 0;
}
