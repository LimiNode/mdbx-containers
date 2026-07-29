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
typedef mdbxc::sync::KeyTableLogicalAdapter<int, IntKeyCodec>
    IntKeyTableAdapter;

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

enum BadLogicalDeliveryMarkerKeyCase {
    BadLogicalDeliveryMarkerKeySize,
    BadLogicalDeliveryMarkerKeyVersion,
    BadLogicalDeliveryMarkerKeyOrigin,
    BadLogicalDeliveryMarkerKeySequence,
    BadLogicalDeliveryMarkerKeyDigest
};

void copy_first_logical_delivery_marker(
        MDBX_txn* txn,
        mdbxc::sync::LogicalDeliveryStore& store,
        std::vector<std::uint8_t>& key,
        std::vector<std::uint8_t>& value) {
    MDBX_cursor* cursor = nullptr;
    MDBXC_TEST_ASSERT(
        mdbx_cursor_open(txn, store.handle(txn), &cursor) == MDBX_SUCCESS);
    MDBX_val raw_key;
    MDBX_val raw_value;
    const int rc = mdbx_cursor_get(cursor, &raw_key, &raw_value, MDBX_FIRST);
    MDBXC_TEST_ASSERT(rc == MDBX_SUCCESS);
    const std::uint8_t* key_begin =
        static_cast<const std::uint8_t*>(raw_key.iov_base);
    const std::uint8_t* value_begin =
        static_cast<const std::uint8_t*>(raw_value.iov_base);
    key.assign(key_begin, key_begin + raw_key.iov_len);
    value.assign(value_begin, value_begin + raw_value.iov_len);
    mdbx_cursor_close(cursor);
}

void put_raw_logical_delivery_marker(
        MDBX_txn* txn,
        mdbxc::sync::LogicalDeliveryStore& store,
        const std::vector<std::uint8_t>& key,
        const std::vector<std::uint8_t>& value) {
    MDBX_val raw_key = {
        const_cast<std::uint8_t*>(key.empty() ? nullptr : &key[0]),
        key.size()
    };
    MDBX_val raw_value = {
        const_cast<std::uint8_t*>(value.empty() ? nullptr : &value[0]),
        value.size()
    };
    MDBXC_TEST_ASSERT(
        mdbx_put(txn, store.handle(txn), &raw_key, &raw_value,
                 MDBX_NOOVERWRITE) == MDBX_SUCCESS);
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

mdbxc::sync::LogicalSchemaRecord make_key_table_record(
        const std::string& dbi_name,
        std::uint32_t version = 1) {
    mdbxc::sync::LogicalSchemaRecord record;
    record.dbi_name = dbi_name;
    record.kind = mdbxc::sync::LogicalTableKind::KeyTable;
    record.schema_version = version;
    record.dbi_names.push_back(dbi_name);
    return record;
}

mdbxc::sync::LogicalChangeFrame make_outbox_test_frame(
        const std::string& schema_id,
        std::uint8_t payload_byte) {
    mdbxc::sync::LogicalSchemaRef ref;
    ref.schema_id = schema_id;
    ref.kind = mdbxc::sync::LogicalTableKind::KeyValue;
    ref.schema_version = 1u;
    std::vector<std::uint8_t> payload(1u, payload_byte);
    mdbxc::sync::LogicalChangeFrame frame;
    frame.changes.push_back(mdbxc::sync::LogicalChange(
        ref, 1u, 0u, payload));
    return frame;
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

    std::string primary_dbi() const override {
        return m_table.dbi_name();
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

    std::string primary_dbi() const override {
        return m_table.dbi_name();
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

class LegacySingleDbiLogicalAdapter
        : public mdbxc::sync::ILogicalTableAdapter {
public:
    LegacySingleDbiLogicalAdapter(
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
        (void)change;
        return mdbxc::sync::LogicalApplyResult::success();
    }

    mdbxc::sync::LogicalApplyResult apply(
            MDBX_txn* txn,
            const mdbxc::sync::LogicalChange& change) override {
        (void)change;
        m_table.insert_or_assign(32, "thirty-two", txn);
        return mdbxc::sync::LogicalApplyResult::success();
    }

private:
    mdbxc::KeyValueTable<int, std::string>& m_table;
    std::string m_schema_id;
};

class LegacyMultiDbiLogicalAdapter : public LegacySingleDbiLogicalAdapter {
public:
    LegacyMultiDbiLogicalAdapter(
            mdbxc::KeyValueTable<int, std::string>& table,
            const std::string& secondary_dbi_name,
            const std::string& schema_id)
        : LegacySingleDbiLogicalAdapter(table, schema_id),
          m_secondary_dbi_name(secondary_dbi_name),
          m_table(table) {}

    std::vector<std::string> affected_dbis() const override {
        std::vector<std::string> out;
        out.push_back(m_table.dbi_name());
        out.push_back(m_secondary_dbi_name);
        return out;
    }

private:
    std::string m_secondary_dbi_name;
    mdbxc::KeyValueTable<int, std::string>& m_table;
};

class ConfigurablePrimaryLogicalAdapter
        : public mdbxc::sync::ILogicalTableAdapter {
public:
    ConfigurablePrimaryLogicalAdapter(
            mdbxc::KeyValueTable<int, std::string>& table,
            const std::string& schema_id,
            const std::string& primary_dbi,
            const std::vector<std::string>& affected_dbis)
        : m_table(table),
          m_schema_id(schema_id),
          m_primary_dbi(primary_dbi),
          m_affected_dbis(affected_dbis) {}

    mdbxc::sync::LogicalSchemaRef schema_ref() const override {
        mdbxc::sync::LogicalSchemaRef ref;
        ref.schema_id = m_schema_id;
        ref.kind = mdbxc::sync::LogicalTableKind::KeyValue;
        ref.schema_version = 1;
        return ref;
    }

    std::vector<std::string> affected_dbis() const override {
        return m_affected_dbis;
    }

    std::string primary_dbi() const override {
        return m_primary_dbi;
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
        m_table.insert_or_assign(33, "thirty-three", txn);
        return mdbxc::sync::LogicalApplyResult::success();
    }

private:
    mdbxc::KeyValueTable<int, std::string>& m_table;
    std::string m_schema_id;
    std::string m_primary_dbi;
    std::vector<std::string> m_affected_dbis;
};

class ThrowingPreflightLogicalAdapter
        : public mdbxc::sync::ILogicalTableAdapter {
public:
    ThrowingPreflightLogicalAdapter(
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

    std::string primary_dbi() const override {
        return m_table.dbi_name();
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
        (void)change;
        throw std::runtime_error("forced logical preflight failure");
    }

    mdbxc::sync::LogicalApplyResult apply(
            MDBX_txn* txn,
            const mdbxc::sync::LogicalChange& change) override {
        (void)change;
        m_table.insert_or_assign(88, "should-not-apply", txn);
        return mdbxc::sync::LogicalApplyResult::success();
    }

private:
    mdbxc::KeyValueTable<int, std::string>& m_table;
    std::string m_schema_id;
};

class CountingDeliveryLogicalAdapter
        : public mdbxc::sync::ILogicalTableAdapter {
public:
    CountingDeliveryLogicalAdapter(
            mdbxc::KeyValueTable<int, std::string>& table,
            const std::string& schema_id,
            int& calls,
            bool* preflight_should_fail = nullptr,
            bool* affected_should_fail = nullptr,
            int* affected_calls = nullptr)
        : m_table(table),
          m_schema_id(schema_id),
          m_calls(calls),
          m_preflight_should_fail(preflight_should_fail),
          m_affected_should_fail(affected_should_fail),
          m_affected_calls(affected_calls) {}

    mdbxc::sync::LogicalSchemaRef schema_ref() const override {
        mdbxc::sync::LogicalSchemaRef ref;
        ref.schema_id = m_schema_id;
        ref.kind = mdbxc::sync::LogicalTableKind::KeyValue;
        ref.schema_version = 1;
        return ref;
    }

    std::string primary_dbi() const override {
        return m_table.dbi_name();
    }

    std::vector<std::string> affected_dbis() const override {
        if (m_affected_calls != nullptr) {
            ++(*m_affected_calls);
        }
        if (m_affected_should_fail != nullptr &&
                *m_affected_should_fail) {
            throw std::runtime_error("forced logical delivery affected_dbis failure");
        }
        std::vector<std::string> out;
        out.push_back(m_table.dbi_name());
        return out;
    }

    mdbxc::sync::LogicalApplyResult preflight(
            MDBX_txn* txn,
            const mdbxc::sync::LogicalChange& change) const override {
        (void)txn;
        (void)change;
        if (m_preflight_should_fail != nullptr &&
                *m_preflight_should_fail) {
            return mdbxc::sync::LogicalApplyResult::failure(
                "forced logical delivery preflight failure");
        }
        return mdbxc::sync::LogicalApplyResult::success();
    }

    mdbxc::sync::LogicalApplyResult apply(
            MDBX_txn* txn,
            const mdbxc::sync::LogicalChange& change) override {
        (void)change;
        ++m_calls;
        m_table.insert_or_assign(90, std::to_string(m_calls), txn);
        return mdbxc::sync::LogicalApplyResult::success();
    }

private:
    mdbxc::KeyValueTable<int, std::string>& m_table;
    std::string m_schema_id;
    int& m_calls;
    bool* m_preflight_should_fail;
    bool* m_affected_should_fail;
    int* m_affected_calls;
};

class CountingOnlyDeliveryLogicalAdapter
        : public mdbxc::sync::ILogicalTableAdapter {
public:
    CountingOnlyDeliveryLogicalAdapter(const std::string& dbi_name,
                                       const std::string& schema_id,
                                       int& calls)
        : m_dbi_name(dbi_name),
          m_schema_id(schema_id),
          m_calls(calls) {}

    mdbxc::sync::LogicalSchemaRef schema_ref() const override {
        mdbxc::sync::LogicalSchemaRef ref;
        ref.schema_id = m_schema_id;
        ref.kind = mdbxc::sync::LogicalTableKind::KeyValue;
        ref.schema_version = 1;
        return ref;
    }

    std::string primary_dbi() const override {
        return m_dbi_name;
    }

    std::vector<std::string> affected_dbis() const override {
        std::vector<std::string> out;
        out.push_back(m_dbi_name);
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
        (void)txn;
        (void)change;
        ++m_calls;
        return mdbxc::sync::LogicalApplyResult::success();
    }

private:
    std::string m_dbi_name;
    std::string m_schema_id;
    int& m_calls;
};

class ThrowingFlushSink : public mdbxc::sync::ISyncCaptureSink {
public:
    void record_change(MDBX_txn* txn,
                       const std::string& dbi_name,
                       mdbxc::sync::ChangeOpType op_type,
                       std::uint32_t dbi_flags,
                       const std::vector<std::uint8_t>& storage_key,
                       const std::vector<std::uint8_t>& value) override {
        (void)txn;
        (void)dbi_name;
        (void)op_type;
        (void)dbi_flags;
        (void)storage_key;
        (void)value;
    }

    void flush_in_txn(MDBX_txn* txn) override {
        (void)txn;
        throw std::runtime_error("forced logical capture commit failure");
    }
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
    MDBXC_TEST_ASSERT(adapter.primary_dbi() == dbi_name);
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

void test_key_table_logical_adapter_applies_through_sync_engine() {
    const std::string path = "test_key_table_logical_adapter_engine.mdbx";
    const std::string dbi_name = "logical_key_table_engine";
    const std::string schema_id = "app.logical_key_table_engine.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x48), make_node(0xD1));
    engine.register_logical_schema(
        schema_id, make_key_table_record(dbi_name));

    mdbxc::KeyTable<int> table(conn, dbi_name);
    IntKeyTableAdapter adapter(table, schema_id);
    engine.register_logical_adapter(adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(adapter.make_insert(11));
    changes.push_back(adapter.make_insert(12));
    changes.push_back(adapter.make_delete(11));

    mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(result.ok);
    MDBXC_TEST_ASSERT(!table.contains(11));
    MDBXC_TEST_ASSERT(table.contains(12));
    MDBXC_TEST_ASSERT(table.count() == 1u);

    changes.clear();
    changes.push_back(adapter.make_clear());
    result = engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(result.ok);
    MDBXC_TEST_ASSERT(table.count() == 0u);

    conn->disconnect();
    cleanup(path);
}

void test_key_table_logical_adapter_rejects_malformed_payload() {
    const std::string path = "test_key_table_logical_adapter_malformed.mdbx";
    const std::string dbi_name = "logical_key_table_malformed";
    const std::string schema_id = "app.logical_key_table_malformed.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x49), make_node(0xD2));
    engine.register_logical_schema(
        schema_id, make_key_table_record(dbi_name));

    mdbxc::KeyTable<int> table(conn, dbi_name);
    IntKeyTableAdapter adapter(table, schema_id);
    engine.register_logical_adapter(adapter);

    mdbxc::sync::LogicalChange malformed = adapter.make_insert(5);
    malformed.payload.push_back(0xffu);
    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(malformed);

    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find("payload") != std::string::npos);
    MDBXC_TEST_ASSERT(!table.contains(5));

    conn->disconnect();
    cleanup(path);
}

void test_key_table_logical_capture_session_commits_typed_local_writes() {
    const std::string path =
        "test_key_table_logical_adapter_session.mdbx";
    const std::string dbi_name = "logical_key_table_session";
    const std::string schema_id = "app.logical_key_table_session.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId node = make_node(0x4A);
    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(node, make_node(0xD3));
    engine.register_logical_schema(
        schema_id, make_key_table_record(dbi_name));

    mdbxc::KeyTable<int> table(conn, dbi_name);
    IntKeyTableAdapter adapter(table, schema_id);
    mdbxc::sync::ThreadLocalChangeAccumulator raw_capture(conn);
    conn->attach_sync_capture(&raw_capture);

    std::vector<mdbxc::sync::LogicalChange> changes;
    {
        std::unique_ptr<IntKeyTableAdapter::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        MDBXC_TEST_ASSERT(session->insert(1));
        MDBXC_TEST_ASSERT(!session->insert(1));
        MDBXC_TEST_ASSERT(session->insert(2));
        MDBXC_TEST_ASSERT(session->erase(1));
        MDBXC_TEST_ASSERT(!session->erase(9));
        MDBXC_TEST_ASSERT(session->pending_size() == 3u);
        session->commit(changes);
    }
    conn->detach_sync_capture();

    MDBXC_TEST_ASSERT(changes.size() == 3u);
    MDBXC_TEST_ASSERT(changes[0].opcode ==
                      mdbxc::sync::KeyTableLogicalInsert);
    MDBXC_TEST_ASSERT(changes[1].opcode ==
                      mdbxc::sync::KeyTableLogicalInsert);
    MDBXC_TEST_ASSERT(changes[2].opcode ==
                      mdbxc::sync::KeyTableLogicalDelete);
    MDBXC_TEST_ASSERT(!table.contains(1));
    MDBXC_TEST_ASSERT(table.contains(2));

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

void test_key_table_logical_frame_replicates_capture_session() {
    const std::string source_path =
        "test_key_table_logical_frame_source.mdbx";
    const std::string replica_path =
        "test_key_table_logical_frame_replica.mdbx";
    const std::string dbi_name = "logical_key_table_frame";
    const std::string schema_id = "app.logical_key_table_frame.v1";
    cleanup(source_path);
    cleanup(replica_path);

    mdbxc::Config source_cfg;
    source_cfg.pathname = source_path;
    source_cfg.max_dbs = 16;
    source_cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> source =
        mdbxc::Connection::create(source_cfg);

    mdbxc::Config replica_cfg;
    replica_cfg.pathname = replica_path;
    replica_cfg.max_dbs = 16;
    replica_cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> replica =
        mdbxc::Connection::create(replica_cfg);

    mdbxc::sync::SyncEngine source_engine(source);
    source_engine.initialize_local_identity(make_node(0x4B),
                                            make_node(0xD4));
    source_engine.register_logical_schema(
        schema_id, make_key_table_record(dbi_name));

    mdbxc::sync::SyncEngine replica_engine(replica);
    replica_engine.initialize_local_identity(make_node(0x4C),
                                             make_node(0xD4));
    replica_engine.register_logical_schema(
        schema_id, make_key_table_record(dbi_name));

    mdbxc::KeyTable<int> source_table(source, dbi_name);
    mdbxc::KeyTable<int> replica_table(replica, dbi_name);
    IntKeyTableAdapter source_adapter(source_table, schema_id);
    IntKeyTableAdapter replica_adapter(replica_table, schema_id);
    replica_engine.register_logical_adapter(replica_adapter);

    std::vector<mdbxc::sync::LogicalChange> captured;
    {
        std::unique_ptr<IntKeyTableAdapter::LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        MDBXC_TEST_ASSERT(session->insert(41));
        MDBXC_TEST_ASSERT(session->insert(42));
        MDBXC_TEST_ASSERT(session->erase(42));
        session->commit(captured);
    }

    MDBXC_TEST_ASSERT(captured.size() == 3u);
    MDBXC_TEST_ASSERT(source_table.contains(41));
    MDBXC_TEST_ASSERT(!source_table.contains(42));

    mdbxc::sync::LogicalChangeFrame frame;
    frame.changes = captured;
    const std::vector<std::uint8_t> wire =
        mdbxc::sync::LogicalChangeFrameCodec::encode(frame);
    const mdbxc::sync::LogicalApplyResult result =
        replica_engine.apply_logical_frame_bytes(wire);
    MDBXC_TEST_ASSERT(result.ok);
    MDBXC_TEST_ASSERT(replica_table.contains(41));
    MDBXC_TEST_ASSERT(!replica_table.contains(42));

    source->disconnect();
    replica->disconnect();
    cleanup(source_path);
    cleanup(replica_path);
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

void test_key_value_logical_engine_accepts_multi_dbi_adapter_with_primary_contract() {
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
    MDBXC_TEST_ASSERT(result.ok);
    const std::pair<bool, std::string> found = table.find_compat(31);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == "thirty-one");

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_engine_rejects_multi_dbi_primary_mismatch() {
    const std::string path =
        "test_key_value_logical_adapter_multi_dbi_primary.mdbx";
    const std::string dbi_name = "logical_key_value_multi_dbi_primary";
    const std::string secondary_dbi_name = dbi_name + "_index";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x1F), make_node(0xAF));
    mdbxc::sync::LogicalSchemaRecord record = make_record(dbi_name);
    record.dbi_name = secondary_dbi_name;
    record.dbi_names.push_back(secondary_dbi_name);
    engine.register_logical_schema(
        "app.logical_kv_multi_dbi_primary.v1", record);

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    MultiDbiLogicalAdapter adapter(
        table, secondary_dbi_name,
        "app.logical_kv_multi_dbi_primary.v1");
    engine.register_logical_adapter(adapter);

    mdbxc::sync::LogicalChange change;
    change.schema = adapter.schema_ref();
    change.opcode = mdbxc::sync::KeyValueLogicalUpsert;
    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(change);
    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find("primary DBI") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(!table.contains(31));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_engine_accepts_legacy_single_dbi_default_primary() {
    const std::string path =
        "test_key_value_logical_adapter_legacy_single.mdbx";
    const std::string dbi_name = "logical_key_value_legacy_single";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x20), make_node(0xB0));
    engine.register_logical_schema("app.logical_kv_legacy_single.v1",
                                   make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    LegacySingleDbiLogicalAdapter adapter(
        table, "app.logical_kv_legacy_single.v1");
    engine.register_logical_adapter(adapter);
    MDBXC_TEST_ASSERT(adapter.primary_dbi() == dbi_name);

    mdbxc::sync::LogicalChange change;
    change.schema = adapter.schema_ref();
    change.opcode = mdbxc::sync::KeyValueLogicalUpsert;
    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(change);
    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(result.ok);
    const std::pair<bool, std::string> found = table.find_compat(32);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == "thirty-two");

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_engine_rejects_legacy_multi_dbi_without_primary() {
    const std::string path =
        "test_key_value_logical_adapter_legacy_multi.mdbx";
    const std::string dbi_name = "logical_key_value_legacy_multi";
    const std::string secondary_dbi_name = dbi_name + "_index";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x21), make_node(0xB1));
    mdbxc::sync::LogicalSchemaRecord record = make_record(dbi_name);
    record.dbi_names.push_back(secondary_dbi_name);
    engine.register_logical_schema("app.logical_kv_legacy_multi.v1",
                                   record);

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    LegacyMultiDbiLogicalAdapter adapter(
        table, secondary_dbi_name, "app.logical_kv_legacy_multi.v1");
    engine.register_logical_adapter(adapter);
    MDBXC_TEST_ASSERT(adapter.primary_dbi().empty());

    mdbxc::sync::LogicalChange change;
    change.schema = adapter.schema_ref();
    change.opcode = mdbxc::sync::KeyValueLogicalUpsert;
    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(change);
    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find("primary DBI") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(!table.contains(32));

    conn->disconnect();
    cleanup(path);
}

void run_primary_contract_rejection(
        const std::string& suffix,
        const std::string& primary_dbi,
        const std::vector<std::string>& affected_dbis,
        const mdbxc::sync::LogicalSchemaRecord& record,
        const std::string& expected_error) {
    const std::string path =
        "test_key_value_logical_adapter_primary_" + suffix + ".mdbx";
    const std::string dbi_name =
        "logical_key_value_primary_" + suffix;
    const std::string schema_id =
        "app.logical_kv_primary_" + suffix + ".v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x22), make_node(0xB2));
    engine.register_logical_schema(schema_id, record);

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    ConfigurablePrimaryLogicalAdapter adapter(
        table, schema_id, primary_dbi, affected_dbis);
    engine.register_logical_adapter(adapter);

    mdbxc::sync::LogicalChange change;
    change.schema = adapter.schema_ref();
    change.opcode = mdbxc::sync::KeyValueLogicalUpsert;
    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(change);
    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_changes(changes);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find(expected_error) !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(!table.contains(33));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_engine_rejects_explicit_empty_primary() {
    const std::string suffix = "empty_primary";
    const std::string dbi_name =
        "logical_key_value_primary_" + suffix;
    std::vector<std::string> affected_dbis;
    affected_dbis.push_back(dbi_name);
    run_primary_contract_rejection(
        suffix, std::string(), affected_dbis, make_record(dbi_name),
        "primary DBI is empty");
}

void test_key_value_logical_engine_rejects_primary_missing_from_affected() {
    const std::string suffix = "missing_primary";
    const std::string dbi_name =
        "logical_key_value_primary_" + suffix;
    const std::string secondary_dbi_name = dbi_name + "_index";
    std::vector<std::string> affected_dbis;
    affected_dbis.push_back(dbi_name);
    affected_dbis.push_back(secondary_dbi_name);
    mdbxc::sync::LogicalSchemaRecord record = make_record(dbi_name);
    record.dbi_names.push_back(secondary_dbi_name);
    run_primary_contract_rejection(
        suffix, dbi_name + "_missing", affected_dbis, record,
        "not listed");
}

void test_key_value_logical_engine_rejects_duplicate_affected_dbis() {
    const std::string suffix = "duplicate_affected";
    const std::string dbi_name =
        "logical_key_value_primary_" + suffix;
    std::vector<std::string> affected_dbis;
    affected_dbis.push_back(dbi_name);
    affected_dbis.push_back(dbi_name);
    run_primary_contract_rejection(
        suffix, dbi_name, affected_dbis, make_record(dbi_name),
        "DBI set");
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

void test_key_value_logical_frame_replicates_capture_session() {
    const std::string source_path =
        "test_key_value_logical_frame_source.mdbx";
    const std::string replica_path =
        "test_key_value_logical_frame_replica.mdbx";
    const std::string dbi_name = "logical_key_value_frame";
    const std::string schema_id = "app.logical_kv_frame.v1";
    cleanup(source_path);
    cleanup(replica_path);

    mdbxc::Config source_cfg;
    source_cfg.pathname = source_path;
    source_cfg.max_dbs = 16;
    source_cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> source =
        mdbxc::Connection::create(source_cfg);

    mdbxc::Config replica_cfg;
    replica_cfg.pathname = replica_path;
    replica_cfg.max_dbs = 16;
    replica_cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> replica =
        mdbxc::Connection::create(replica_cfg);

    mdbxc::sync::SyncEngine source_engine(source);
    source_engine.initialize_local_identity(make_node(0x32),
                                            make_node(0xC2));
    source_engine.register_logical_schema(schema_id, make_record(dbi_name));

    mdbxc::sync::SyncEngine replica_engine(replica);
    replica_engine.initialize_local_identity(make_node(0x33),
                                             make_node(0xC2));
    replica_engine.register_logical_schema(schema_id, make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> source_table(source, dbi_name);
    mdbxc::KeyValueTable<int, std::string> replica_table(replica, dbi_name);
    IntStringAdapter source_adapter(source_table, schema_id);
    IntStringAdapter replica_adapter(replica_table, schema_id);
    replica_engine.register_logical_adapter(replica_adapter);

    std::vector<mdbxc::sync::LogicalChange> captured;
    {
        std::unique_ptr<IntStringAdapter::LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        session->insert_or_assign(41, "forty-one");
        session->insert_or_assign(42, "forty-two");
        MDBXC_TEST_ASSERT(session->erase(42));
        session->commit(captured);
    }

    MDBXC_TEST_ASSERT(captured.size() == 3u);
    MDBXC_TEST_ASSERT(source_table.contains(41));
    MDBXC_TEST_ASSERT(!source_table.contains(42));

    mdbxc::sync::LogicalChangeFrame frame;
    frame.changes = captured;
    const std::vector<std::uint8_t> wire =
        mdbxc::sync::LogicalChangeFrameCodec::encode(frame);
    const mdbxc::sync::LogicalApplyResult result =
        replica_engine.apply_logical_frame_bytes(wire);
    MDBXC_TEST_ASSERT(result.ok);

    const std::pair<bool, std::string> found =
        replica_table.find_compat(41);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == "forty-one");
    MDBXC_TEST_ASSERT(!replica_table.contains(42));

    source->disconnect();
    replica->disconnect();
    cleanup(source_path);
    cleanup(replica_path);
}

void test_key_value_logical_frame_rejects_malformed_bytes_before_apply() {
    const std::string path =
        "test_key_value_logical_frame_malformed.mdbx";
    const std::string dbi_name = "logical_key_value_frame_malformed";
    const std::string schema_id = "app.logical_kv_frame_malformed.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x34), make_node(0xC4));
    engine.register_logical_schema(schema_id, make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, schema_id);
    engine.register_logical_adapter(adapter);

    std::vector<std::uint8_t> malformed;
    malformed.push_back(0);
    malformed.push_back(1);
    malformed.push_back(2);
    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_frame_bytes(malformed);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find("decode failed") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(table.count() == 0u);

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_frame_reports_apply_stage_exception() {
    const std::string path =
        "test_key_value_logical_frame_apply_exception.mdbx";
    const std::string dbi_name = "logical_key_value_frame_apply_exception";
    const std::string schema_id =
        "app.logical_kv_frame_apply_exception.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x35), make_node(0xC5));
    engine.register_logical_schema(schema_id, make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    ThrowingPreflightLogicalAdapter adapter(table, schema_id);
    engine.register_logical_adapter(adapter);

    mdbxc::sync::LogicalChangeFrame frame;
    std::vector<std::uint8_t> payload;
    frame.changes.push_back(mdbxc::sync::LogicalChange(
        adapter.schema_ref(), 1u, 0u, payload));
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalChangeFrameCodec::encode(frame);

    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_frame_bytes(encoded);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find("apply failed") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(result.error.find("decode failed") ==
                      std::string::npos);
    MDBXC_TEST_ASSERT(!table.contains(88));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_delivery_envelope_deduplicates_after_reopen() {
    const std::string path =
        "test_key_value_logical_delivery_dedup.mdbx";
    const std::string dbi_name = "logical_key_value_delivery_dedup";
    const std::string schema_id = "app.logical_kv_delivery_dedup.v1";
    cleanup(path);

    const mdbxc::sync::NodeId local_node = make_node(0x36);
    const mdbxc::sync::NodeId origin_node = make_node(0x37);
    const mdbxc::sync::NodeId db_uuid = make_node(0xC6);

    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = db_uuid;
    envelope.origin_node_id = origin_node;
    envelope.origin_sequence = 1;
    envelope.frame_id = "frame-0001";
    {
        mdbxc::sync::LogicalSchemaRef ref;
        ref.schema_id = schema_id;
        ref.kind = mdbxc::sync::LogicalTableKind::KeyValue;
        ref.schema_version = 1;
        std::vector<std::uint8_t> payload;
        envelope.frame.changes.push_back(
            mdbxc::sync::LogicalChange(ref, 1u, 0u, payload));
    }
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalDeliveryEnvelopeCodec::encode(envelope);

    {
        mdbxc::Config cfg;
        cfg.pathname = path;
        cfg.max_dbs = 16;
        cfg.no_subdir = true;
        std::shared_ptr<mdbxc::Connection> conn =
            mdbxc::Connection::create(cfg);

        mdbxc::sync::SyncEngine engine(conn);
        engine.initialize_local_identity(local_node, db_uuid);
        engine.register_logical_schema(schema_id, make_record(dbi_name));

        mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
        int apply_calls = 0;
        CountingDeliveryLogicalAdapter adapter(
            table, schema_id, apply_calls);
        engine.register_logical_adapter(adapter);

        const mdbxc::sync::LogicalApplyResult first =
            engine.apply_logical_delivery_envelope_bytes(encoded);
        MDBXC_TEST_ASSERT(first.ok);
        MDBXC_TEST_ASSERT(apply_calls == 1);
        const std::pair<bool, std::string> found = table.find_compat(90);
        MDBXC_TEST_ASSERT(found.first);
        MDBXC_TEST_ASSERT(found.second == "1");

        conn->disconnect();
    }

    {
        mdbxc::Config cfg;
        cfg.pathname = path;
        cfg.max_dbs = 16;
        cfg.no_subdir = true;
        std::shared_ptr<mdbxc::Connection> conn =
            mdbxc::Connection::create(cfg);

        mdbxc::sync::SyncEngine engine(conn);
        engine.initialize_local_identity(local_node, db_uuid);
        engine.register_logical_schema(schema_id, make_record(dbi_name));

        mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
        int apply_calls = 0;
        bool affected_should_fail = true;
        int affected_calls = 0;
        CountingDeliveryLogicalAdapter adapter(
            table, schema_id, apply_calls, nullptr,
            &affected_should_fail, &affected_calls);
        engine.register_logical_adapter(adapter);

        const mdbxc::sync::LogicalApplyResult duplicate =
            engine.apply_logical_delivery_envelope_bytes(encoded);
        MDBXC_TEST_ASSERT(duplicate.ok);
        MDBXC_TEST_ASSERT(apply_calls == 0);
        MDBXC_TEST_ASSERT(affected_calls == 0);
        const std::pair<bool, std::string> found = table.find_compat(90);
        MDBXC_TEST_ASSERT(found.first);
        MDBXC_TEST_ASSERT(found.second == "1");

        conn->disconnect();
    }

    cleanup(path);
}

void test_key_value_logical_delivery_envelope_rejects_wrong_destination() {
    const std::string path =
        "test_key_value_logical_delivery_wrong_destination.mdbx";
    const std::string dbi_name = "logical_key_value_delivery_wrong_dest";
    const std::string schema_id = "app.logical_kv_delivery_wrong_dest.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId db_uuid = make_node(0xC7);
    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x38), db_uuid);
    engine.register_logical_schema(schema_id, make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    int apply_calls = 0;
    bool affected_should_fail = true;
    int affected_calls = 0;
    CountingDeliveryLogicalAdapter adapter(
        table, schema_id, apply_calls, nullptr,
        &affected_should_fail, &affected_calls);
    engine.register_logical_adapter(adapter);

    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = make_node(0xC8);
    envelope.origin_node_id = make_node(0x39);
    envelope.origin_sequence = 1;
    envelope.frame_id = "wrong-destination";
    envelope.frame.changes.push_back(mdbxc::sync::LogicalChange(
        adapter.schema_ref(), 1u, 0u, std::vector<std::uint8_t>()));

    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_delivery_envelope(envelope);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find("destination") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(apply_calls == 0);
    MDBXC_TEST_ASSERT(affected_calls == 0);
    MDBXC_TEST_ASSERT(!table.contains(90));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_delivery_envelope_rejects_identity_collision() {
    const std::string path =
        "test_key_value_logical_delivery_identity_collision.mdbx";
    const std::string dbi_name = "logical_key_value_delivery_collision";
    const std::string schema_id = "app.logical_kv_delivery_collision.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId db_uuid = make_node(0xC9);
    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x3A), db_uuid);
    engine.register_logical_schema(schema_id, make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    int apply_calls = 0;
    bool affected_should_fail = false;
    int affected_calls = 0;
    CountingDeliveryLogicalAdapter adapter(
        table, schema_id, apply_calls, nullptr,
        &affected_should_fail, &affected_calls);
    engine.register_logical_adapter(adapter);

    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = db_uuid;
    envelope.origin_node_id = make_node(0x3B);
    envelope.origin_sequence = 7;
    envelope.frame_id = "identity-collision";
    envelope.frame.changes.push_back(mdbxc::sync::LogicalChange(
        adapter.schema_ref(), 1u, 0u, std::vector<std::uint8_t>()));

    const mdbxc::sync::LogicalApplyResult first =
        engine.apply_logical_delivery_envelope_bytes(
            mdbxc::sync::LogicalDeliveryEnvelopeCodec::encode(envelope));
    MDBXC_TEST_ASSERT(first.ok);
    MDBXC_TEST_ASSERT(apply_calls == 1);
    std::pair<bool, std::string> found = table.find_compat(90);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == "1");

    mdbxc::sync::LogicalDeliveryEnvelope collision = envelope;
    std::vector<std::uint8_t> payload;
    payload.push_back(0x42u);
    collision.frame.changes[0] = mdbxc::sync::LogicalChange(
        adapter.schema_ref(), 1u, 0u, payload);
    affected_should_fail = true;

    const mdbxc::sync::LogicalApplyResult second =
        engine.apply_logical_delivery_envelope_bytes(
            mdbxc::sync::LogicalDeliveryEnvelopeCodec::encode(collision));
    MDBXC_TEST_ASSERT(!second.ok);
    MDBXC_TEST_ASSERT(second.error.find("identity conflict") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(apply_calls == 1);
    found = table.find_compat(90);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == "1");

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_delivery_marker_rolls_back_after_apply_failure() {
    const std::string path =
        "test_key_value_logical_delivery_marker_rollback.mdbx";
    const std::string dbi_name = "logical_key_value_delivery_rollback";
    const std::string schema_id = "app.logical_kv_delivery_rollback.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId db_uuid = make_node(0xCA);
    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x3C), db_uuid);
    engine.register_logical_schema(schema_id, make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    int apply_calls = 0;
    bool fail_preflight = true;
    CountingDeliveryLogicalAdapter adapter(
        table, schema_id, apply_calls, &fail_preflight);
    engine.register_logical_adapter(adapter);

    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = db_uuid;
    envelope.origin_node_id = make_node(0x3D);
    envelope.origin_sequence = 8;
    envelope.frame_id = "marker-rollback";
    envelope.frame.changes.push_back(mdbxc::sync::LogicalChange(
        adapter.schema_ref(), 1u, 0u, std::vector<std::uint8_t>()));
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalDeliveryEnvelopeCodec::encode(envelope);

    const mdbxc::sync::LogicalApplyResult failed =
        engine.apply_logical_delivery_envelope_bytes(encoded);
    MDBXC_TEST_ASSERT(!failed.ok);
    MDBXC_TEST_ASSERT(failed.error.find(
                          "forced logical delivery preflight failure") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(apply_calls == 0);
    MDBXC_TEST_ASSERT(!table.contains(90));

    fail_preflight = false;
    const mdbxc::sync::LogicalApplyResult retried =
        engine.apply_logical_delivery_envelope_bytes(encoded);
    MDBXC_TEST_ASSERT(retried.ok);
    MDBXC_TEST_ASSERT(apply_calls == 1);
    const std::pair<bool, std::string> found = table.find_compat(90);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == "1");

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_delivery_envelope_skips_self_origin() {
    const std::string path =
        "test_key_value_logical_delivery_self_origin.mdbx";
    const std::string dbi_name = "logical_key_value_delivery_self_origin";
    const std::string schema_id = "app.logical_kv_delivery_self_origin.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId local_node = make_node(0x3E);
    const mdbxc::sync::NodeId db_uuid = make_node(0xCB);
    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(local_node, db_uuid);
    engine.register_logical_schema(schema_id, make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    int apply_calls = 0;
    bool affected_should_fail = true;
    int affected_calls = 0;
    CountingDeliveryLogicalAdapter adapter(
        table, schema_id, apply_calls, nullptr,
        &affected_should_fail, &affected_calls);
    engine.register_logical_adapter(adapter);

    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = db_uuid;
    envelope.origin_node_id = local_node;
    envelope.origin_sequence = 9;
    envelope.frame_id = "self-origin";
    envelope.frame.changes.push_back(mdbxc::sync::LogicalChange(
        adapter.schema_ref(), 1u, 0u, std::vector<std::uint8_t>()));

    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_delivery_envelope_bytes(
            mdbxc::sync::LogicalDeliveryEnvelopeCodec::encode(envelope));
    MDBXC_TEST_ASSERT(result.ok);
    MDBXC_TEST_ASSERT(apply_calls == 0);
    MDBXC_TEST_ASSERT(affected_calls == 0);
    MDBXC_TEST_ASSERT(!table.contains(90));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_delivery_prunes_markers_behind_watermark() {
    const std::string path =
        "test_key_value_logical_delivery_prune.mdbx";
    const std::string dbi_name = "logical_key_value_delivery_prune";
    const std::string schema_id = "app.logical_kv_delivery_prune.v1";
    cleanup(path);

    const mdbxc::sync::NodeId local_node = make_node(0x4D);
    const mdbxc::sync::NodeId origin_node = make_node(0x4E);
    const mdbxc::sync::NodeId other_origin_node = make_node(0x4F);
    const mdbxc::sync::NodeId db_uuid = make_node(0xD5);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;

    mdbxc::sync::LogicalDeliveryEnvelope first;
    first.destination_db_uuid = db_uuid;
    first.origin_node_id = origin_node;
    first.origin_sequence = 1;
    first.frame_id = "delivery-prune-1";

    mdbxc::sync::LogicalDeliveryEnvelope second;
    second.destination_db_uuid = db_uuid;
    second.origin_node_id = origin_node;
    second.origin_sequence = 2;
    second.frame_id = "delivery-prune-2";

    mdbxc::sync::LogicalDeliveryEnvelope third;
    third.destination_db_uuid = db_uuid;
    third.origin_node_id = origin_node;
    third.origin_sequence = 3;
    third.frame_id = "delivery-prune-3";

    mdbxc::sync::LogicalDeliveryEnvelope other_origin;
    other_origin.destination_db_uuid = db_uuid;
    other_origin.origin_node_id = other_origin_node;
    other_origin.origin_sequence = 1;
    other_origin.frame_id = "delivery-prune-other-origin";

    {
        std::shared_ptr<mdbxc::Connection> conn =
            mdbxc::Connection::create(cfg);
        mdbxc::sync::SyncEngine engine(conn);
        engine.initialize_local_identity(local_node, db_uuid);
        engine.register_logical_schema(schema_id, make_record(dbi_name));

        mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
        IntStringAdapter adapter(table, schema_id);
        engine.register_logical_adapter(adapter);

        first.frame.changes.push_back(adapter.make_upsert(90, "one"));
        second.frame.changes.push_back(adapter.make_upsert(90, "two"));
        third.frame.changes.push_back(adapter.make_upsert(90, "three"));
        other_origin.frame.changes.push_back(
            adapter.make_upsert(91, "other-origin"));

        MDBXC_TEST_ASSERT(
            engine.apply_logical_delivery_envelope(first).ok);
        MDBXC_TEST_ASSERT(
            engine.apply_logical_delivery_envelope(second).ok);
        MDBXC_TEST_ASSERT(
            engine.apply_logical_delivery_envelope(other_origin).ok);
        MDBXC_TEST_ASSERT(table.find_compat(90).second == "two");

        MDBXC_TEST_ASSERT(
            engine.prune_logical_delivery_markers(origin_node, 1) == 1u);
        MDBXC_TEST_ASSERT(
            engine.prune_logical_delivery_markers(origin_node, 1) == 0u);

        MDBXC_TEST_ASSERT(
            engine.apply_logical_delivery_envelope(first).ok);
        MDBXC_TEST_ASSERT(table.find_compat(90).second == "two");
        MDBXC_TEST_ASSERT(
            engine.apply_logical_delivery_envelope(third).ok);
        MDBXC_TEST_ASSERT(table.find_compat(90).second == "three");
        MDBXC_TEST_ASSERT(table.find_compat(91).second == "other-origin");

        {
            mdbxc::Transaction txn =
                conn->transaction(mdbxc::TransactionMode::READ_ONLY);
            mdbxc::sync::LogicalDeliveryStore delivery(conn->env_handle());
            delivery.open(txn.handle());
            MDBXC_TEST_ASSERT(
                delivery.watermark(txn.handle(), origin_node) == 1u);
            MDBXC_TEST_ASSERT(delivery.count(txn.handle()) == 3u);
            MDBXC_TEST_ASSERT(!delivery.contains(txn.handle(), first));
            MDBXC_TEST_ASSERT(delivery.contains(txn.handle(), second));
        }

        MDBXC_TEST_ASSERT(
            engine.prune_logical_delivery_markers(origin_node, 3) == 2u);
        bool decreasing_threw = false;
        try {
            (void)engine.prune_logical_delivery_markers(origin_node, 2);
        } catch (const std::invalid_argument&) {
            decreasing_threw = true;
        }
        MDBXC_TEST_ASSERT(decreasing_threw);

        conn->disconnect();
    }

    {
        std::shared_ptr<mdbxc::Connection> conn =
            mdbxc::Connection::create(cfg);
        mdbxc::sync::SyncEngine engine(conn);
        engine.initialize_local_identity(local_node, db_uuid);
        engine.register_logical_schema(schema_id, make_record(dbi_name));

        mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
        IntStringAdapter adapter(table, schema_id);
        engine.register_logical_adapter(adapter);

        MDBXC_TEST_ASSERT(
            engine.apply_logical_delivery_envelope(first).ok);
        MDBXC_TEST_ASSERT(table.find_compat(90).second == "three");

        conn->disconnect();
    }

    cleanup(path);
}

void test_logical_delivery_store_reads_legacy_layout_without_watermarks() {
    const std::string path =
        "test_logical_delivery_legacy_without_watermarks.mdbx";
    const std::string dbi_name = "logical_delivery_legacy_table";
    const std::string schema_id = "app.logical_delivery_legacy.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId local_node = make_node(0x50);
    const mdbxc::sync::NodeId origin_node = make_node(0x51);
    const mdbxc::sync::NodeId db_uuid = make_node(0xD6);
    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(local_node, db_uuid);
    engine.register_logical_schema(schema_id, make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, schema_id);
    engine.register_logical_adapter(adapter);

    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = db_uuid;
    envelope.origin_node_id = origin_node;
    envelope.origin_sequence = 1;
    envelope.frame_id = "legacy-layout-marker";
    envelope.frame.changes.push_back(adapter.make_upsert(92, "legacy"));
    MDBXC_TEST_ASSERT(engine.apply_logical_delivery_envelope(envelope).ok);

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBX_dbi watermark_dbi = 0;
        const int open_rc = mdbx_dbi_open(
            txn.handle(), "_mdbxc_logical_delivery_watermarks",
            static_cast<MDBX_db_flags_t>(0), &watermark_dbi);
        if (open_rc == MDBX_SUCCESS) {
            mdbxc::check_mdbx(mdbx_drop(txn.handle(), watermark_dbi, 1),
                              "test legacy watermark DBI drop failed");
        } else {
            MDBXC_TEST_ASSERT(open_rc == MDBX_NOTFOUND);
        }
        txn.commit();
    }

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        mdbxc::sync::LogicalDeliveryStore delivery(conn->env_handle());
        MDBXC_TEST_ASSERT(delivery.count(txn.handle()) == 1u);
        MDBXC_TEST_ASSERT(delivery.list_markers(txn.handle()).size() == 1u);
        MDBXC_TEST_ASSERT(
            delivery.watermark(txn.handle(), origin_node) == 0u);
        MDBXC_TEST_ASSERT(delivery.contains(txn.handle(), envelope));
    }

    conn->disconnect();
    cleanup(path);
}

void test_logical_delivery_store_reopens_watermark_after_aborted_create() {
    const std::string path =
        "test_logical_delivery_watermark_abort_reopen.mdbx";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId origin = make_node(0x52);
    mdbxc::sync::LogicalDeliveryStore store(conn->env_handle());

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBXC_TEST_ASSERT(store.prune_up_to(txn.handle(), origin, 7u) == 0u);
        txn.rollback();
    }

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        MDBXC_TEST_ASSERT(store.watermark(txn.handle(), origin) == 0u);
    }

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBXC_TEST_ASSERT(store.prune_up_to(txn.handle(), origin, 7u) == 0u);
        txn.commit();
    }

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        MDBXC_TEST_ASSERT(store.watermark(txn.handle(), origin) == 7u);
    }

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_delivery_envelope_accepts_long_frame_id() {
    const std::string path =
        "test_key_value_logical_delivery_long_frame_id.mdbx";
    const std::string dbi_name = "logical_key_value_delivery_long_frame";
    const std::string schema_id = "app.logical_kv_delivery_long_frame.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId db_uuid = make_node(0xCC);
    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x3F), db_uuid);
    engine.register_logical_schema(schema_id, make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    int apply_calls = 0;
    CountingDeliveryLogicalAdapter adapter(table, schema_id, apply_calls);
    engine.register_logical_adapter(adapter);

    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = db_uuid;
    envelope.origin_node_id = make_node(0x40);
    envelope.origin_sequence = 10;
    envelope.frame_id = std::string(3000u, 'f');
    envelope.frame.changes.push_back(mdbxc::sync::LogicalChange(
        adapter.schema_ref(), 1u, 0u, std::vector<std::uint8_t>()));

    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_delivery_envelope_bytes(
            mdbxc::sync::LogicalDeliveryEnvelopeCodec::encode(envelope));
    MDBXC_TEST_ASSERT(result.ok);
    MDBXC_TEST_ASSERT(apply_calls == 1);
    const std::pair<bool, std::string> found = table.find_compat(90);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == "1");

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_delivery_envelope_keeps_custom_bounds() {
    const std::string path =
        "test_key_value_logical_delivery_custom_bounds.mdbx";
    const std::string dbi_name = "logical_key_value_delivery_bounds";
    const std::string schema_id = "app.logical_kv_delivery_bounds.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId db_uuid = make_node(0xCD);
    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x41), db_uuid);
    engine.register_logical_schema(schema_id, make_record(dbi_name));

    int apply_calls = 0;
    CountingOnlyDeliveryLogicalAdapter adapter(
        dbi_name, schema_id, apply_calls);
    engine.register_logical_adapter(adapter);

    mdbxc::sync::CodecBounds bounds;
    bounds.max_ops_per_batch = 10001u;

    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = db_uuid;
    envelope.origin_node_id = make_node(0x42);
    envelope.origin_sequence = 11;
    envelope.frame_id = "custom-bounds";
    const mdbxc::sync::LogicalSchemaRef schema = adapter.schema_ref();
    envelope.frame.changes.reserve(bounds.max_ops_per_batch);
    for (std::uint32_t i = 0; i < bounds.max_ops_per_batch; ++i) {
        (void)i;
        envelope.frame.changes.push_back(mdbxc::sync::LogicalChange(
            schema, 1u, 0u, std::vector<std::uint8_t>()));
    }

    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::LogicalDeliveryEnvelopeCodec::encode(
            envelope, &bounds);
    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_delivery_envelope_bytes(encoded, &bounds);
    MDBXC_TEST_ASSERT(result.ok);
    MDBXC_TEST_ASSERT(apply_calls ==
                      static_cast<int>(bounds.max_ops_per_batch));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_delivery_object_api_rejects_frame_id_bound() {
    const std::string path =
        "test_key_value_logical_delivery_object_bound.mdbx";
    const std::string dbi_name = "logical_key_value_delivery_object_bound";
    const std::string schema_id = "app.logical_kv_delivery_object_bound.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId db_uuid = make_node(0xCE);
    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x43), db_uuid);
    engine.register_logical_schema(schema_id, make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    int apply_calls = 0;
    bool affected_should_fail = true;
    int affected_calls = 0;
    CountingDeliveryLogicalAdapter adapter(
        table, schema_id, apply_calls, nullptr,
        &affected_should_fail, &affected_calls);
    engine.register_logical_adapter(adapter);

    mdbxc::sync::CodecBounds bounds;
    bounds.max_logical_delivery_frame_id_len = 8u;

    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = db_uuid;
    envelope.origin_node_id = make_node(0x44);
    envelope.origin_sequence = 12;
    envelope.frame_id = "123456789";
    envelope.frame.changes.push_back(mdbxc::sync::LogicalChange(
        adapter.schema_ref(), 1u, 0u, std::vector<std::uint8_t>()));

    const mdbxc::sync::LogicalApplyResult result =
        engine.apply_logical_delivery_envelope(envelope, &bounds);
    MDBXC_TEST_ASSERT(!result.ok);
    MDBXC_TEST_ASSERT(result.error.find(
                          "max_logical_delivery_frame_id_len") !=
                      std::string::npos);
    MDBXC_TEST_ASSERT(apply_calls == 0);
    MDBXC_TEST_ASSERT(affected_calls == 0);
    MDBXC_TEST_ASSERT(!table.contains(90));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_delivery_store_rejects_frame_id_bound() {
    const std::string path =
        "test_key_value_logical_delivery_store_bound.mdbx";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::CodecBounds bounds;
    bounds.max_logical_delivery_frame_id_len = 8u;

    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = make_node(0xCF);
    envelope.origin_node_id = make_node(0x45);
    envelope.origin_sequence = 13;
    envelope.frame_id = "123456789";
    mdbxc::sync::LogicalSchemaRef ref;
    ref.schema_id = "app.logical_kv_delivery_store_bound.v1";
    ref.kind = mdbxc::sync::LogicalTableKind::KeyValue;
    ref.schema_version = 1;
    envelope.frame.changes.push_back(mdbxc::sync::LogicalChange(
        ref, 1u, 0u, std::vector<std::uint8_t>()));

    bool threw = false;
    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::WRITABLE);
        mdbxc::sync::LogicalDeliveryStore store(conn->env_handle());
        store.open(txn.handle());
        try {
            store.try_mark_applied(txn.handle(), envelope, &bounds);
        } catch (const std::length_error& e) {
            threw = std::string(e.what()).find(
                        "max_logical_delivery_frame_id_len") !=
                    std::string::npos;
        }
        txn.rollback();
    }
    MDBXC_TEST_ASSERT(threw);

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_delivery_store_lists_markers() {
    const std::string path =
        "test_key_value_logical_delivery_store_list.mdbx";
    const std::string dbi_name = "logical_key_value_delivery_store_list";
    const std::string schema_id = "app.logical_kv_delivery_store_list.v1";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    const mdbxc::sync::NodeId db_uuid = make_node(0xD0);
    const mdbxc::sync::NodeId origin = make_node(0x46);
    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x47), db_uuid);
    engine.register_logical_schema(schema_id, make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    int apply_calls = 0;
    CountingDeliveryLogicalAdapter adapter(table, schema_id, apply_calls);
    engine.register_logical_adapter(adapter);

    for (int i = 0; i < 2; ++i) {
        mdbxc::sync::LogicalDeliveryEnvelope envelope;
        envelope.destination_db_uuid = db_uuid;
        envelope.origin_node_id = origin;
        envelope.origin_sequence = static_cast<std::uint64_t>(30 + i);
        envelope.frame_id = i == 0 ? "inspect-a" : "inspect-b";
        envelope.frame.changes.push_back(mdbxc::sync::LogicalChange(
            adapter.schema_ref(), 1u, 0u, std::vector<std::uint8_t>()));
        const mdbxc::sync::LogicalApplyResult result =
            engine.apply_logical_delivery_envelope(envelope);
        MDBXC_TEST_ASSERT(result.ok);
    }
    MDBXC_TEST_ASSERT(apply_calls == 2);

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        mdbxc::sync::LogicalDeliveryStore store(conn->env_handle());
        store.open(txn.handle());
        MDBXC_TEST_ASSERT(store.count(txn.handle()) == 2u);
        const std::vector<mdbxc::sync::LogicalDeliveryMarkerInfo> one =
            store.list_markers(txn.handle(), 1u);
        MDBXC_TEST_ASSERT(one.size() == 1u);

        const std::vector<mdbxc::sync::LogicalDeliveryMarkerInfo> all =
            store.list_markers(txn.handle());
        MDBXC_TEST_ASSERT(all.size() == 2u);
        bool saw_a = false;
        bool saw_b = false;
        for (std::size_t i = 0; i < all.size(); ++i) {
            MDBXC_TEST_ASSERT(
                mdbxc::sync::compare_node_id(
                    all[i].destination_db_uuid, db_uuid) == 0);
            MDBXC_TEST_ASSERT(
                mdbxc::sync::compare_node_id(
                    all[i].origin_node_id, origin) == 0);
            MDBXC_TEST_ASSERT(
                all[i].frame_codec_version ==
                mdbxc::sync::LogicalChangeFrameCodec::codec_version());
            MDBXC_TEST_ASSERT(all[i].frame_bytes_size > 0u);
            if (all[i].frame_id == "inspect-a") {
                saw_a = all[i].origin_sequence == 30u;
            } else if (all[i].frame_id == "inspect-b") {
                saw_b = all[i].origin_sequence == 31u;
            }
        }
        MDBXC_TEST_ASSERT(saw_a);
        MDBXC_TEST_ASSERT(saw_b);
    }

    conn->disconnect();
    cleanup(path);
}

void run_logical_delivery_store_bad_marker_key_case(
        BadLogicalDeliveryMarkerKeyCase marker_case,
        const std::string& suffix) {
    const std::string path =
        "test_key_value_logical_delivery_bad_marker_" + suffix + ".mdbx";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::LogicalSchemaRef ref;
    ref.schema_id = "app.logical_kv_delivery_bad_marker.v1";
    ref.kind = mdbxc::sync::LogicalTableKind::KeyValue;
    ref.schema_version = 1;

    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.destination_db_uuid = make_node(0xD3);
    envelope.origin_node_id = make_node(0x4A);
    envelope.origin_sequence = 40;
    envelope.frame_id = "bad-marker-key";
    envelope.frame.changes.push_back(mdbxc::sync::LogicalChange(
        ref, 1u, 0u, std::vector<std::uint8_t>()));

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::WRITABLE);
        mdbxc::sync::LogicalDeliveryStore store(conn->env_handle());
        store.open(txn.handle());
        MDBXC_TEST_ASSERT(
            store.try_mark_applied(txn.handle(), envelope));

        std::vector<std::uint8_t> key;
        std::vector<std::uint8_t> value;
        copy_first_logical_delivery_marker(
            txn.handle(), store, key, value);
        MDBXC_TEST_ASSERT(key.size() > 40u);

        switch (marker_case) {
            case BadLogicalDeliveryMarkerKeySize:
                key.pop_back();
                break;
            case BadLogicalDeliveryMarkerKeyVersion:
                key[0] ^= 0x80u;
                break;
            case BadLogicalDeliveryMarkerKeyOrigin:
                key[2] ^= 0x80u;
                break;
            case BadLogicalDeliveryMarkerKeySequence:
                key[18] ^= 0x01u;
                break;
            case BadLogicalDeliveryMarkerKeyDigest:
                key[key.size() - 1u] ^= 0x01u;
                break;
        }
        put_raw_logical_delivery_marker(
            txn.handle(), store, key, value);
        txn.commit();
    }

    {
        mdbxc::Transaction txn =
            conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        mdbxc::sync::LogicalDeliveryStore store(conn->env_handle());
        store.open(txn.handle());
        bool count_threw = false;
        try {
            (void)store.count(txn.handle());
        } catch (const std::runtime_error&) {
            count_threw = true;
        }
        MDBXC_TEST_ASSERT(count_threw);

        bool list_threw = false;
        try {
            (void)store.list_markers(txn.handle());
        } catch (const std::runtime_error&) {
            list_threw = true;
        }
        MDBXC_TEST_ASSERT(list_threw);
    }

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_delivery_store_rejects_bad_marker_keys() {
    run_logical_delivery_store_bad_marker_key_case(
        BadLogicalDeliveryMarkerKeySize, "size");
    run_logical_delivery_store_bad_marker_key_case(
        BadLogicalDeliveryMarkerKeyVersion, "version");
    run_logical_delivery_store_bad_marker_key_case(
        BadLogicalDeliveryMarkerKeyOrigin, "origin");
    run_logical_delivery_store_bad_marker_key_case(
        BadLogicalDeliveryMarkerKeySequence, "sequence");
    run_logical_delivery_store_bad_marker_key_case(
        BadLogicalDeliveryMarkerKeyDigest, "digest");
}

void test_logical_delivery_order_status_helper() {
    mdbxc::sync::LogicalDeliveryEnvelope envelope;
    envelope.origin_sequence = 5;
    envelope.frame_id = "frame-a";

    mdbxc::sync::LogicalDeliveryOrderResult result =
        mdbxc::sync::check_logical_delivery_order(envelope, 5);
    MDBXC_TEST_ASSERT(
        result.status == mdbxc::sync::LogicalDeliveryOrderStatus::InOrder);
    MDBXC_TEST_ASSERT(result.expected_sequence == 5u);
    MDBXC_TEST_ASSERT(result.observed_sequence == 5u);
    MDBXC_TEST_ASSERT(result.can_apply_without_buffering());

    result = mdbxc::sync::check_logical_delivery_order(envelope, 6);
    MDBXC_TEST_ASSERT(
        result.status ==
        mdbxc::sync::LogicalDeliveryOrderStatus::SequenceBehindWatermark);
    MDBXC_TEST_ASSERT(!result.can_apply_without_buffering());

    envelope.frame_id = "frame-b";
    result = mdbxc::sync::check_logical_delivery_order(envelope, 6);
    MDBXC_TEST_ASSERT(
        result.status ==
        mdbxc::sync::LogicalDeliveryOrderStatus::SequenceBehindWatermark);
    MDBXC_TEST_ASSERT(!result.can_apply_without_buffering());

    result = mdbxc::sync::check_logical_delivery_order(envelope, 4);
    MDBXC_TEST_ASSERT(
        result.status == mdbxc::sync::LogicalDeliveryOrderStatus::SequenceGap);
    MDBXC_TEST_ASSERT(!result.can_apply_without_buffering());
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

void test_key_value_logical_capture_session_requires_schema_marker() {
    const std::string path = "test_key_value_logical_adapter_session_marker.mdbx";
    const std::string dbi_name = "logical_key_value_session_marker";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x21), make_node(0xB1));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_session_marker.v1");

    bool caught = false;
    try {
        std::unique_ptr<IntStringAdapter::LogicalCaptureSession> session =
            adapter.begin_capture_session();
    } catch (const std::runtime_error&) {
        caught = true;
    }
    MDBXC_TEST_ASSERT(caught);
    MDBXC_TEST_ASSERT(!table.contains(4));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_capture_session_rejects_stale_marker() {
    const std::string path = "test_key_value_logical_adapter_session_stale.mdbx";
    const std::string dbi_name = "logical_key_value_session_stale";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x22), make_node(0xB2));
    engine.register_logical_schema("app.logical_kv_session_stale.v1",
                                   make_record(dbi_name, 1));
    engine.migrate_logical_schema("app.logical_kv_session_stale.v1",
                                  make_record(dbi_name, 1),
                                  make_record(dbi_name, 2));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter stale_adapter(
        table, "app.logical_kv_session_stale.v1", 1);

    bool caught = false;
    try {
        std::unique_ptr<IntStringAdapter::LogicalCaptureSession> session =
            stale_adapter.begin_capture_session();
    } catch (const std::runtime_error&) {
        caught = true;
    }
    MDBXC_TEST_ASSERT(caught);
    MDBXC_TEST_ASSERT(!table.contains(5));

    IntStringAdapter current_adapter(
        table, "app.logical_kv_session_stale.v1", 2);
    std::vector<mdbxc::sync::LogicalChange> changes;
    {
        std::unique_ptr<IntStringAdapter::LogicalCaptureSession> session =
            current_adapter.begin_capture_session();
        session->insert_or_assign(5, "five");
        session->commit(changes);
    }
    MDBXC_TEST_ASSERT(changes.size() == 1u);
    const std::pair<bool, std::string> found = table.find_compat(5);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == "five");

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_capture_session_rejects_dbi_marker_mismatch() {
    const std::string path = "test_key_value_logical_adapter_session_dbi.mdbx";
    const std::string dbi_name = "logical_key_value_session_dbi";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x23), make_node(0xB3));
    engine.register_logical_schema("app.logical_kv_session_dbi.v1",
                                   make_record(dbi_name + "_other"));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_session_dbi.v1");

    bool caught = false;
    try {
        std::unique_ptr<IntStringAdapter::LogicalCaptureSession> session =
            adapter.begin_capture_session();
    } catch (const std::runtime_error&) {
        caught = true;
    }
    MDBXC_TEST_ASSERT(caught);
    MDBXC_TEST_ASSERT(!table.contains(6));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_capture_session_erases_output_on_commit_failure() {
    const std::string path = "test_key_value_logical_adapter_session_commit_fail.mdbx";
    const std::string dbi_name = "logical_key_value_session_commit_fail";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x24), make_node(0xB4));
    engine.register_logical_schema("app.logical_kv_session_commit_fail.v1",
                                   make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    IntStringAdapter adapter(table, "app.logical_kv_session_commit_fail.v1");
    ThrowingFlushSink sink;
    conn->attach_sync_capture(&sink);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(adapter.make_delete(99));
    bool caught = false;
    {
        std::unique_ptr<IntStringAdapter::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        session->insert_or_assign(7, "seven");
        try {
            session->commit(changes);
        } catch (const std::runtime_error&) {
            caught = true;
        }
    }
    conn->detach_sync_capture();

    MDBXC_TEST_ASSERT(caught);
    MDBXC_TEST_ASSERT(changes.size() == 1u);
    MDBXC_TEST_ASSERT(!table.contains(7));

    conn->disconnect();
    cleanup(path);
}

void test_key_value_logical_capture_session_captures_clear() {
    const std::string path = "test_key_value_logical_adapter_session_clear.mdbx";
    const std::string dbi_name = "logical_key_value_session_clear";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn =
        mdbxc::Connection::create(cfg);

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x25), make_node(0xB5));
    engine.register_logical_schema("app.logical_kv_session_clear.v1",
                                   make_record(dbi_name));

    mdbxc::KeyValueTable<int, std::string> table(conn, dbi_name);
    table.insert_or_assign(1, "one");
    table.insert_or_assign(2, "two");
    IntStringAdapter adapter(table, "app.logical_kv_session_clear.v1");

    std::vector<mdbxc::sync::LogicalChange> changes;
    {
        std::unique_ptr<IntStringAdapter::LogicalCaptureSession> session =
            adapter.begin_capture_session();
        session->clear();
        session->commit(changes);
    }

    MDBXC_TEST_ASSERT(changes.size() == 1u);
    MDBXC_TEST_ASSERT(changes[0].opcode ==
                      mdbxc::sync::KeyValueLogicalClear);
    MDBXC_TEST_ASSERT(table.count() == 0u);

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

    mdbxc::sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x26), make_node(0xB6));
    engine.register_logical_schema("app.logical_kv_session_range.v1",
                                   make_record(dbi_name));

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

void test_logical_outbox_persists_ordered_destination_streams() {
    const std::string path = "test_logical_outbox_ordered.mdbx";
    cleanup(path);

    const mdbxc::sync::NodeId local_node = make_node(0x91);
    const mdbxc::sync::DbId local_db = make_node(0xA1);
    const mdbxc::sync::DbId destination_a = make_node(0xB1);
    const mdbxc::sync::DbId destination_b = make_node(0xC1);

    {
        mdbxc::Config cfg;
        cfg.pathname = path;
        cfg.max_dbs = 16;
        cfg.no_subdir = true;
        std::shared_ptr<mdbxc::Connection> conn = mdbxc::Connection::create(cfg);
        mdbxc::sync::SyncEngine engine(conn);
        engine.initialize_local_identity(local_node, local_db);

        {
            mdbxc::Transaction txn =
                conn->transaction(mdbxc::TransactionMode::WRITABLE);
            mdbxc::sync::LogicalOutboxStore outbox(conn->env_handle());
            const mdbxc::sync::LogicalDeliveryEnvelope rolled_back =
                outbox.enqueue(txn.handle(), destination_a, local_node,
                               make_outbox_test_frame("app.outbox.rollback", 1u));
            MDBXC_TEST_ASSERT(rolled_back.origin_sequence == 1u);
            txn.rollback();
        }

        const mdbxc::sync::LogicalDeliveryEnvelope first =
            engine.enqueue_logical_delivery(
                destination_a, make_outbox_test_frame("app.outbox.a", 2u));
        const mdbxc::sync::LogicalDeliveryEnvelope second =
            engine.enqueue_logical_delivery(
                destination_a, make_outbox_test_frame("app.outbox.a", 3u));
        const mdbxc::sync::LogicalDeliveryEnvelope other_destination =
            engine.enqueue_logical_delivery(
                destination_b, make_outbox_test_frame("app.outbox.b", 4u));
        MDBXC_TEST_ASSERT(first.origin_sequence == 1u);
        MDBXC_TEST_ASSERT(second.origin_sequence == 2u);
        MDBXC_TEST_ASSERT(other_destination.origin_sequence == 1u);
        MDBXC_TEST_ASSERT(first.origin_node_id == local_node);
        MDBXC_TEST_ASSERT(first.frame_id == "mdbxc-ordered-1");

        const std::vector<mdbxc::sync::LogicalDeliveryEnvelope> pending_a =
            engine.pending_logical_deliveries(destination_a);
        const std::vector<mdbxc::sync::LogicalDeliveryEnvelope> pending_b =
            engine.pending_logical_deliveries(destination_b);
        MDBXC_TEST_ASSERT(pending_a.size() == 2u);
        MDBXC_TEST_ASSERT(pending_a[0].origin_sequence == 1u);
        MDBXC_TEST_ASSERT(pending_a[1].origin_sequence == 2u);
        MDBXC_TEST_ASSERT(pending_b.size() == 1u);
        MDBXC_TEST_ASSERT(pending_b[0].origin_sequence == 1u);

        MDBXC_TEST_ASSERT(
            engine.acknowledge_logical_deliveries(destination_a, 1u) == 1u);
        MDBXC_TEST_ASSERT(
            engine.logical_delivery_acknowledged_through(destination_a) == 1u);
        MDBXC_TEST_ASSERT(
            engine.acknowledge_logical_deliveries(destination_a, 1u) == 0u);
        const std::vector<mdbxc::sync::LogicalDeliveryEnvelope> after_ack =
            engine.pending_logical_deliveries(destination_a);
        MDBXC_TEST_ASSERT(after_ack.size() == 1u);
        MDBXC_TEST_ASSERT(after_ack[0].origin_sequence == 2u);

        conn->disconnect();
    }

    {
        mdbxc::Config cfg;
        cfg.pathname = path;
        cfg.max_dbs = 16;
        cfg.no_subdir = true;
        std::shared_ptr<mdbxc::Connection> conn = mdbxc::Connection::create(cfg);
        mdbxc::sync::SyncEngine engine(conn);
        engine.initialize_local_identity(local_node, local_db);

        const std::vector<mdbxc::sync::LogicalDeliveryEnvelope> pending =
            engine.pending_logical_deliveries(destination_a);
        MDBXC_TEST_ASSERT(pending.size() == 1u);
        MDBXC_TEST_ASSERT(pending[0].origin_sequence == 2u);
        MDBXC_TEST_ASSERT(
            engine.logical_delivery_acknowledged_through(destination_a) == 1u);
        const mdbxc::sync::LogicalDeliveryEnvelope next =
            engine.enqueue_logical_delivery(
                destination_a, make_outbox_test_frame("app.outbox.a", 5u));
        MDBXC_TEST_ASSERT(next.origin_sequence == 3u);
        MDBXC_TEST_ASSERT(
            engine.acknowledge_logical_deliveries(destination_a, 3u) == 2u);
        MDBXC_TEST_ASSERT(
            engine.pending_logical_deliveries(destination_a).empty());
        MDBXC_TEST_ASSERT(
            engine.logical_delivery_acknowledged_through(destination_a) == 3u);

        conn->disconnect();
    }

    cleanup(path);
}

} // namespace

int main() {
    test_key_value_logical_adapter_applies_basic_ops();
    test_key_value_logical_adapter_applies_through_sync_engine();
    test_key_table_logical_adapter_applies_through_sync_engine();
    test_key_table_logical_adapter_rejects_malformed_payload();
    test_key_table_logical_capture_session_commits_typed_local_writes();
    test_key_table_logical_frame_replicates_capture_session();
    test_key_value_logical_adapter_engine_rejects_unknown_schema();
    test_key_value_logical_engine_rejects_missing_schema_marker();
    test_key_value_logical_engine_rejects_stale_schema_marker();
    test_key_value_logical_engine_rejects_marker_dbi_mismatch();
    test_key_value_logical_engine_accepts_multi_dbi_adapter_with_primary_contract();
    test_key_value_logical_engine_rejects_multi_dbi_primary_mismatch();
    test_key_value_logical_engine_accepts_legacy_single_dbi_default_primary();
    test_key_value_logical_engine_rejects_legacy_multi_dbi_without_primary();
    test_key_value_logical_engine_rejects_explicit_empty_primary();
    test_key_value_logical_engine_rejects_primary_missing_from_affected();
    test_key_value_logical_engine_rejects_duplicate_affected_dbis();
    test_key_value_logical_engine_suppresses_generic_raw_capture();
    test_key_value_logical_capture_session_commits_typed_local_writes();
    test_key_value_logical_frame_replicates_capture_session();
    test_key_value_logical_frame_rejects_malformed_bytes_before_apply();
    test_key_value_logical_frame_reports_apply_stage_exception();
    test_key_value_logical_delivery_envelope_deduplicates_after_reopen();
    test_key_value_logical_delivery_envelope_rejects_wrong_destination();
    test_key_value_logical_delivery_envelope_rejects_identity_collision();
    test_key_value_logical_delivery_marker_rolls_back_after_apply_failure();
    test_key_value_logical_delivery_envelope_skips_self_origin();
    test_key_value_logical_delivery_prunes_markers_behind_watermark();
    test_logical_delivery_store_reads_legacy_layout_without_watermarks();
    test_logical_delivery_store_reopens_watermark_after_aborted_create();
    test_key_value_logical_delivery_envelope_accepts_long_frame_id();
    test_key_value_logical_delivery_envelope_keeps_custom_bounds();
    test_key_value_logical_delivery_object_api_rejects_frame_id_bound();
    test_key_value_logical_delivery_store_rejects_frame_id_bound();
    test_key_value_logical_delivery_store_lists_markers();
    test_key_value_logical_delivery_store_rejects_bad_marker_keys();
    test_logical_delivery_order_status_helper();
    test_key_value_logical_capture_session_discards_rollback();
    test_key_value_logical_capture_session_requires_schema_marker();
    test_key_value_logical_capture_session_rejects_stale_marker();
    test_key_value_logical_capture_session_rejects_dbi_marker_mismatch();
    test_key_value_logical_capture_session_erases_output_on_commit_failure();
    test_key_value_logical_capture_session_captures_clear();
    test_key_value_logical_capture_session_fails_before_mutation();
    test_key_value_logical_adapter_rejects_malformed_payload();
    test_key_value_logical_adapter_uses_stable_payload();
    test_key_value_logical_adapter_decodes_literal_little_endian_payload();
    test_key_value_logical_apply_does_not_recapture_incoming_change();
    test_logical_outbox_persists_ordered_destination_streams();
    return 0;
}
