#include <mdbx_containers.hpp>
#include <mdbx_containers/sync.hpp>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

const char* g_executable_path = nullptr;

void cleanup(const std::string& p) {
    std::remove(p.c_str());
}

class StubSink : public mdbxc::sync::ISyncCaptureSink {
public:
    mutable std::mutex m_mutex;
    std::vector<mdbxc::sync::ChangeOp> m_recorded;
    std::vector<mdbxc::sync::ChangeBatch> m_flushed;

    void record_change(MDBX_txn* txn,
                       const std::string& dbi_name,
                       mdbxc::sync::ChangeOpType op_type,
                       std::uint32_t dbi_flags,
                       const std::vector<std::uint8_t>& storage_key,
                       const std::vector<std::uint8_t>& value) override {
        (void)txn;
        std::lock_guard<std::mutex> lk(m_mutex);
        mdbxc::sync::ChangeOp op;
        op.op_type = op_type;
        op.dbi_flags = dbi_flags;
        op.dbi_name = dbi_name;
        op.storage_key = storage_key;
        op.value = value;
        m_recorded.push_back(std::move(op));
    }

    void flush_in_txn(MDBX_txn* txn) override {
        (void)txn;
        std::lock_guard<std::mutex> lk(m_mutex);
        m_flushed.emplace_back();
    }

    void clear_recorded() {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_recorded.clear();
        m_flushed.clear();
    }
};

class FullChangeSink : public mdbxc::sync::FullChangeSyncCaptureSink {
public:
    std::vector<mdbxc::sync::ChangeOp> m_recorded;

    void record_change_op(MDBX_txn* txn,
                          const mdbxc::sync::ChangeOp& change) override {
        (void)txn;
        m_recorded.push_back(change);
    }

    void flush_in_txn(MDBX_txn* txn) override {
        (void)txn;
    }
};

class ThrowingChangeSink : public mdbxc::sync::FullChangeSyncCaptureSink {
public:
    int flush_calls = 0;

    void record_change_op(MDBX_txn* txn,
                          const mdbxc::sync::ChangeOp& change) override {
        (void)txn;
        (void)change;
        throw std::runtime_error("capture record failure");
    }

    void flush_in_txn(MDBX_txn* txn) override {
        (void)txn;
        ++flush_calls;
    }
};

class ThrowOnceFlushSink : public mdbxc::sync::FullChangeSyncCaptureSink {
public:
    int record_calls = 0;
    int flush_calls = 0;
    bool consume_before_throw = false;
    std::vector<mdbxc::sync::ChangeOp> pending;

    void record_change_op(MDBX_txn* txn,
                          const mdbxc::sync::ChangeOp& change) override {
        (void)txn;
        ++record_calls;
        pending.push_back(change);
    }

    void flush_in_txn(MDBX_txn* txn) override {
        (void)txn;
        ++flush_calls;
        if (flush_calls == 1) {
            if (consume_before_throw) {
                pending.clear();
            }
            throw std::runtime_error("capture flush failure");
        }
        pending.clear();
    }
};

class FlushOnlySink : public mdbxc::sync::ISyncCaptureSink {
public:
    void flush_in_txn(MDBX_txn* txn) override {
        (void)txn;
    }
};

void require_no_capture(const StubSink& sink,
                        const char* table_name,
                        const char* operation_name) {
    std::lock_guard<std::mutex> lk(sink.m_mutex);
    if (!sink.m_recorded.empty()) {
        throw std::runtime_error(
            std::string(table_name) + "::" + operation_name +
            " must not emit sync ChangeOp in v0.1 without enabled wire-format support; got " +
            std::to_string(sink.m_recorded.size())
        );
    }
}

void require_one_clear_capture(const StubSink& sink,
                               const char* dbi_name,
                               const char* operation_name) {
    std::lock_guard<std::mutex> lk(sink.m_mutex);
    if (sink.m_recorded.size() != 1u) {
        throw std::runtime_error(
            std::string(operation_name) + " expected one ClearTable op, got " +
            std::to_string(sink.m_recorded.size()));
    }
    const mdbxc::sync::ChangeOp& op = sink.m_recorded[0];
    if (op.dbi_name != dbi_name ||
        op.op_type != mdbxc::sync::ChangeOpType::ClearTable ||
        !op.storage_key.empty() ||
        !op.value.empty()) {
        throw std::runtime_error(
            std::string(operation_name) + " ClearTable capture incorrect");
    }
}

mdbxc::Embedding make_embedding(const std::vector<float>& values) {
    mdbxc::Embedding e;
    e.dim = static_cast<std::uint32_t>(values.size());
    e.values = values;
    return e;
}

std::vector<std::uint8_t> storage_key_bytes(int key) {
    mdbxc::SerializeScratch scratch;
    const MDBX_val value = mdbxc::serialize_key(key, scratch);
    const std::uint8_t* data =
        static_cast<const std::uint8_t*>(value.iov_base);
    return std::vector<std::uint8_t>(data, data + value.iov_len);
}

void test_no_sink_no_capture() {
    using namespace mdbxc;
    const std::string p = "test_capture_no_sink.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    KeyValueTable<int, int> kv(conn, "t");
    kv.insert_or_assign(1, 100);

    cleanup(p);
}

void test_capture_writes_via_sink() {
    using namespace mdbxc;
    const std::string p = "test_capture_with_sink.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    conn->attach_sync_capture(&sink);

    KeyValueTable<int, int> kv(conn, "t");
    kv.insert_or_assign(1, 100);
    kv.insert_or_assign(2, 200);
    kv.insert_or_assign(3, 300);
    kv.erase(2);

    conn->detach_sync_capture();
    cleanup(p);

    if (sink.m_recorded.size() != 4u) {
        throw std::runtime_error("expected 4 recorded ops, got " +
                                 std::to_string(sink.m_recorded.size()));
    }
    if (sink.m_flushed.size() < 1u) {
        throw std::runtime_error("expected at least one flush_in_txn call");
    }
    if (sink.m_recorded[0].op_type != sync::ChangeOpType::Put ||
        sink.m_recorded[1].op_type != sync::ChangeOpType::Put ||
        sink.m_recorded[2].op_type != sync::ChangeOpType::Put ||
        sink.m_recorded[3].op_type != sync::ChangeOpType::Delete) {
        throw std::runtime_error("op_type sequence incorrect");
    }
    if (sink.m_recorded[0].dbi_name != "t") {
        throw std::runtime_error("dbi_name not propagated");
    }
    if ((sink.m_recorded[0].dbi_flags & MDBX_INTEGERKEY) == 0) {
        throw std::runtime_error("dbi_flags did not propagate MDBX_INTEGERKEY");
    }
}

void test_capture_scope_restores_previous_sink() {
    using namespace mdbxc;
    const std::string p = "test_capture_scope.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink outer_sink;
    StubSink inner_sink;
    conn->attach_sync_capture(&outer_sink);
    if (conn->sync_capture() != &outer_sink) {
        throw std::runtime_error("outer sink was not attached");
    }

    KeyValueTable<int, int> kv(conn, "t");
    {
        sync::SyncCaptureScope scope(conn, inner_sink);
        if (!scope.active() || conn->sync_capture() != &inner_sink) {
            throw std::runtime_error("capture scope did not attach inner sink");
        }
        kv.insert_or_assign(1, 100);
    }

    if (conn->sync_capture() != &outer_sink) {
        throw std::runtime_error("capture scope did not restore outer sink");
    }
    kv.insert_or_assign(2, 200);

    conn->detach_sync_capture();
    kv.insert_or_assign(3, 300);
    conn->disconnect();
    cleanup(p);

    if (inner_sink.m_recorded.size() != 1u ||
        inner_sink.m_recorded[0].op_type != sync::ChangeOpType::Put) {
        throw std::runtime_error("inner sink did not capture scoped write");
    }
    if (outer_sink.m_recorded.size() != 1u ||
        outer_sink.m_recorded[0].op_type != sync::ChangeOpType::Put) {
        throw std::runtime_error("outer sink did not capture restored write");
    }
}

void test_capture_scope_nested_scopes_restore_in_order() {
    using namespace mdbxc;
    const std::string p = "test_capture_scope_nested.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink outer_sink;
    StubSink inner_sink;
    KeyValueTable<int, int> kv(conn, "t");

    {
        sync::SyncCaptureScope outer(conn, outer_sink);
        if (conn->sync_capture() != &outer_sink) {
            throw std::runtime_error("outer scope did not attach outer sink");
        }
        kv.insert_or_assign(1, 100);

        {
            sync::SyncCaptureScope inner(conn, inner_sink);
            if (conn->sync_capture() != &inner_sink) {
                throw std::runtime_error("inner scope did not attach inner sink");
            }
            kv.insert_or_assign(2, 200);
        }

        if (conn->sync_capture() != &outer_sink) {
            throw std::runtime_error("inner scope did not restore outer sink");
        }
        kv.insert_or_assign(3, 300);
    }

    if (conn->sync_capture() != nullptr) {
        throw std::runtime_error("outer scope did not restore null sink");
    }
    kv.insert_or_assign(4, 400);
    conn->disconnect();
    cleanup(p);

    if (outer_sink.m_recorded.size() != 2u) {
        throw std::runtime_error("outer sink captured wrong nested count");
    }
    if (inner_sink.m_recorded.size() != 1u) {
        throw std::runtime_error("inner sink captured wrong nested count");
    }
}

void test_capture_scope_rejects_out_of_order_detach() {
    using namespace mdbxc;
    const std::string p = "test_capture_scope_out_of_order_detach.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink outer_sink;
    StubSink inner_sink;
    KeyValueTable<int, int> kv(conn, "t");

    {
        sync::SyncCaptureScope outer(conn, outer_sink);
        {
            sync::SyncCaptureScope inner(conn, inner_sink);
            bool rejected = false;
            try {
                outer.detach();
            } catch (const std::logic_error&) {
                rejected = true;
            }
            if (!rejected) {
                throw std::runtime_error("out-of-order detach was not rejected");
            }
            if (!outer.active() || conn->sync_capture() != &inner_sink) {
                throw std::runtime_error("out-of-order detach changed active inner sink");
            }
            kv.insert_or_assign(1, 100);
        }
        if (conn->sync_capture() != &outer_sink) {
            throw std::runtime_error("inner scope did not restore outer after rejected detach");
        }
        kv.insert_or_assign(2, 200);
    }

    if (conn->sync_capture() != nullptr) {
        throw std::runtime_error("outer scope did not restore null after rejected detach");
    }
    kv.insert_or_assign(3, 300);
    conn->disconnect();
    cleanup(p);

    if (inner_sink.m_recorded.size() != 1u) {
        throw std::runtime_error("inner sink captured wrong out-of-order count");
    }
    if (outer_sink.m_recorded.size() != 1u) {
        throw std::runtime_error("outer sink captured wrong out-of-order count");
    }
}

void test_capture_scope_rejects_same_sink_out_of_order_detach() {
    using namespace mdbxc;
    const std::string p = "test_capture_scope_same_sink_out_of_order_detach.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    KeyValueTable<int, int> kv(conn, "t");

    sync::SyncCaptureScope outer(conn, sink);
    {
        sync::SyncCaptureScope inner(conn, sink);
        bool rejected = false;
        try {
            outer.detach();
        } catch (const std::logic_error&) {
            rejected = true;
        }
        if (!rejected) {
            throw std::runtime_error("same-sink out-of-order detach was not rejected");
        }
        if (!outer.active() || conn->sync_capture() != &sink) {
            throw std::runtime_error("same-sink rejected detach changed attachment");
        }
        kv.insert_or_assign(1, 100);
    }

    if (conn->sync_capture() != &sink) {
        throw std::runtime_error("same-sink inner scope did not restore outer token");
    }
    kv.insert_or_assign(2, 200);

    outer.detach();
    if (outer.active() || conn->sync_capture() != nullptr) {
        throw std::runtime_error("same-sink outer scope did not restore null sink");
    }
    kv.insert_or_assign(3, 300);
    conn->disconnect();
    cleanup(p);

    if (sink.m_recorded.size() != 2u) {
        throw std::runtime_error("same-sink nested scopes captured wrong count");
    }
}

void run_capture_scope_out_of_order_destruction_child() {
    using namespace mdbxc;
    const std::string p = "test_capture_scope_out_of_order_destruction_child.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink outer_sink;
    StubSink inner_sink;

    sync::SyncCaptureScope* outer =
        new sync::SyncCaptureScope(conn, outer_sink);
    sync::SyncCaptureScope* inner =
        new sync::SyncCaptureScope(conn, inner_sink);

    delete outer;

    delete inner;
    conn->disconnect();
    cleanup(p);
}

void test_capture_uses_full_change_op_overload() {
    using namespace mdbxc;
    const std::string p = "test_capture_full_change_op.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    FullChangeSink sink;
    conn->attach_sync_capture(&sink);

    KeyValueTable<int, int> kv(conn, "full_capture");
    kv.insert_or_assign(7, 70);

    if (sink.m_recorded.size() != 1u) {
        throw std::runtime_error("full ChangeOp sink did not receive capture");
    }
    const sync::ChangeOp& op = sink.m_recorded[0];
    if (op.dbi_name != "full_capture" ||
        op.op_type != sync::ChangeOpType::Put ||
        op.storage_key.empty() ||
        op.value.empty()) {
        throw std::runtime_error("full ChangeOp capture fields mismatch");
    }

    conn->disconnect();
    cleanup(p);
}

void test_legacy_sink_rejects_enriched_change_op() {
    StubSink sink;
    mdbxc::sync::ISyncCaptureSink* base = &sink;

    mdbxc::sync::ChangeOp op;
    op.dbi_name = "legacy_capture";
    op.op_type = mdbxc::sync::ChangeOpType::Put;
    op.dbi_flags = MDBX_CREATE;
    op.storage_key.push_back(0x01u);
    op.value.push_back(0x02u);
    op.op_flags = mdbxc::sync::OP_HAS_IDENTITY_KEY;
    op.identity_key.push_back(0x03u);

    bool rejected = false;
    try {
        base->record_change_op(nullptr, op);
    } catch (const std::logic_error&) {
        rejected = true;
    }

    if (!rejected) {
        throw std::runtime_error(
            "legacy capture sink accepted enriched ChangeOp");
    }

    std::lock_guard<std::mutex> lk(sink.m_mutex);
    if (!sink.m_recorded.empty()) {
        throw std::runtime_error(
            "legacy capture sink recorded truncated enriched ChangeOp");
    }
}

void test_capture_scope_terminates_on_out_of_order_destruction() {
    const std::string p = "test_capture_scope_out_of_order_destruction_child.mdbx";
    cleanup(p);

    if (g_executable_path == nullptr) {
        throw std::runtime_error("test executable path is unavailable");
    }

    std::string command = "\"";
    command += g_executable_path;
    command += "\" --sync-capture-out-of-order-destruction-child";
#ifdef _WIN32
    command += " >NUL 2>NUL";
#else
    command += " >/dev/null 2>/dev/null";
#endif

    const int result = std::system(command.c_str());
    cleanup(p);
    if (result == 0) {
        throw std::runtime_error(
            "out-of-order SyncCaptureScope destruction did not terminate");
    }
}

void test_capture_scope_detach_restores_null_once() {
    using namespace mdbxc;
    const std::string p = "test_capture_scope_detach.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    KeyValueTable<int, int> kv(conn, "t");

    sync::SyncCaptureScope scope(conn, sink);
    if (!scope.active() || conn->sync_capture() != &sink) {
        throw std::runtime_error("scope did not attach sink before detach");
    }
    kv.insert_or_assign(1, 100);

    scope.detach();
    if (scope.active() || conn->sync_capture() != nullptr) {
        throw std::runtime_error("scope did not restore null sink on detach");
    }
    scope.detach();
    if (conn->sync_capture() != nullptr) {
        throw std::runtime_error("double detach changed null sink");
    }

    kv.insert_or_assign(2, 200);
    conn->disconnect();
    cleanup(p);

    if (sink.m_recorded.size() != 1u) {
        throw std::runtime_error("detached scope captured after detach");
    }
}

void test_capture_scope_rejects_null_arguments() {
    using namespace mdbxc;
    const std::string p = "test_capture_scope_null_args.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    bool null_connection_rejected = false;
    bool null_sink_rejected = false;

    try {
        std::shared_ptr<Connection> null_connection;
        sync::SyncCaptureScope scope(null_connection, sink);
        (void)scope;
    } catch (const std::invalid_argument&) {
        null_connection_rejected = true;
    }

    try {
        sync::SyncCaptureScope scope(conn,
                                     static_cast<sync::ISyncCaptureSink*>(nullptr));
        (void)scope;
    } catch (const std::invalid_argument&) {
        null_sink_rejected = true;
    }

    conn->disconnect();
    cleanup(p);

    if (!null_connection_rejected) {
        throw std::runtime_error("null connection was not rejected");
    }
    if (!null_sink_rejected) {
        throw std::runtime_error("null sink was not rejected");
    }
}

void test_sequence_set_writes_via_sink() {
    using namespace mdbxc;
    const std::string p = "test_capture_sequence_set.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    conn->attach_sync_capture(&sink);

    SequenceTable<std::string> events(conn, "events");
    events.insert_or_assign(7, "seven");
    events.insert_or_assign(8, "");

    conn->detach_sync_capture();
    conn->disconnect();
    cleanup(p);

    if (sink.m_recorded.size() != 2u) {
        throw std::runtime_error("expected two sequence set ops, got " +
                                 std::to_string(sink.m_recorded.size()));
    }
    for (std::size_t i = 0; i < sink.m_recorded.size(); ++i) {
        const sync::ChangeOp& op = sink.m_recorded[i];
        if (op.op_type != sync::ChangeOpType::Put) {
            throw std::runtime_error("sequence set must capture Put");
        }
        if (op.dbi_name != "events") {
            throw std::runtime_error("sequence set dbi_name not propagated");
        }
        if (op.storage_key.empty()) {
            throw std::runtime_error("sequence set key not captured");
        }
        if ((op.dbi_flags & MDBX_INTEGERKEY) == 0) {
            throw std::runtime_error("sequence set dbi_flags missing MDBX_INTEGERKEY");
        }
    }
    if (sink.m_recorded[0].value.empty()) {
        throw std::runtime_error("non-empty sequence set value not captured");
    }
    if (!sink.m_recorded[1].value.empty()) {
        throw std::runtime_error("empty sequence set value must remain empty");
    }
}

void test_value_table_writes_storage_key_via_sink() {
    using namespace mdbxc;
    const std::string p = "test_capture_value_table.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    conn->attach_sync_capture(&sink);

    ValueTable<int> state(conn, "state");
    if (!state.insert(41)) {
        throw std::runtime_error("value insert unexpectedly returned false");
    }
    state.set(42);
    if (!state.erase()) {
        throw std::runtime_error("value erase unexpectedly returned false");
    }

    conn->detach_sync_capture();
    conn->disconnect();
    cleanup(p);

    if (sink.m_recorded.size() != 3u) {
        throw std::runtime_error("expected three value ops, got " +
                                 std::to_string(sink.m_recorded.size()));
    }
    const sync::ChangeOpType expected[] = {
        sync::ChangeOpType::Put,
        sync::ChangeOpType::Put,
        sync::ChangeOpType::Delete
    };
    for (std::size_t i = 0; i < sink.m_recorded.size(); ++i) {
        const sync::ChangeOp& op = sink.m_recorded[i];
        if (op.dbi_name != "state") {
            throw std::runtime_error("value table dbi_name not propagated");
        }
        if (op.op_type != expected[i]) {
            throw std::runtime_error("value table op_type sequence incorrect");
        }
        if (op.storage_key.empty()) {
            throw std::runtime_error("value table storage_key must be physical key bytes");
        }
        if ((op.dbi_flags & MDBX_INTEGERKEY) == 0) {
            throw std::runtime_error("value table dbi_flags missing MDBX_INTEGERKEY");
        }
        if (op.op_type == sync::ChangeOpType::Put && op.value.empty()) {
            throw std::runtime_error("value table Put missing value bytes");
        }
    }
}

void test_key_value_bulk_writes_via_sink() {
    using namespace mdbxc;
    const std::string p = "test_capture_kv_bulk.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    conn->attach_sync_capture(&sink);

    KeyValueTable<int, int> kv(conn, "bulk");
    std::map<int, int> initial;
    initial[1] = 10;
    initial[2] = 20;
    kv.append(initial);

    std::vector<std::pair<int, int> > extra;
    extra.push_back(std::make_pair(3, 30));
    kv.append(extra);

    std::map<int, int> replacement;
    replacement[2] = 200;
    replacement[3] = 300;
    replacement[4] = 400;
    kv.reconcile(replacement);

    std::vector<std::pair<int, int> > final_state;
    final_state.push_back(std::make_pair(4, 440));
    kv.reconcile(final_state);

    conn->detach_sync_capture();
    conn->disconnect();
    cleanup(p);

    if (sink.m_recorded.size() != 10u) {
        throw std::runtime_error("expected ten bulk ops, got " +
                                 std::to_string(sink.m_recorded.size()));
    }
    std::size_t put_count = 0;
    std::size_t delete_count = 0;
    for (const sync::ChangeOp& op : sink.m_recorded) {
        if (op.dbi_name != "bulk") {
            throw std::runtime_error("bulk dbi_name not propagated");
        }
        if (op.storage_key.empty()) {
            throw std::runtime_error("bulk op missing storage_key");
        }
        if (op.op_type == sync::ChangeOpType::Put) {
            ++put_count;
            if (op.value.empty()) {
                throw std::runtime_error("bulk Put missing value bytes");
            }
        } else if (op.op_type == sync::ChangeOpType::Delete) {
            ++delete_count;
        } else {
            throw std::runtime_error("bulk op_type unexpected");
        }
    }
    if (put_count != 7u || delete_count != 3u) {
        throw std::runtime_error("bulk Put/Delete counts incorrect");
    }
}

void test_range_erase_writes_via_sink() {
    using namespace mdbxc;
    const std::string p = "test_capture_range_erase.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    conn->attach_sync_capture(&sink);

    KeyTable<int> keys(conn, "keys");
    keys.insert(1);
    keys.insert(2);
    keys.insert(3);
    keys.insert(4);
    sink.clear_recorded();
    if (keys.erase_range(2, 3) != 2u) {
        throw std::runtime_error("key range erase count incorrect");
    }
    if (sink.m_recorded.size() != 2u) {
        throw std::runtime_error("expected two key range Delete ops");
    }
    for (const sync::ChangeOp& op : sink.m_recorded) {
        if (op.dbi_name != "keys" ||
            op.op_type != sync::ChangeOpType::Delete ||
            op.storage_key.empty()) {
            throw std::runtime_error("key range Delete capture incorrect");
        }
    }

    KeyValueTable<int, int> kv(conn, "range_kv");
    kv.insert_or_assign(1, 10);
    kv.insert_or_assign(2, 20);
    kv.insert_or_assign(3, 30);
    kv.insert_or_assign(4, 40);
    sink.clear_recorded();
    if (kv.erase_range(2, 3) != 2u) {
        throw std::runtime_error("kv range erase count incorrect");
    }
    if (sink.m_recorded.size() != 2u) {
        throw std::runtime_error("expected two kv range Delete ops");
    }
    for (const sync::ChangeOp& op : sink.m_recorded) {
        if (op.dbi_name != "range_kv" ||
            op.op_type != sync::ChangeOpType::Delete ||
            op.storage_key.empty()) {
            throw std::runtime_error("kv range Delete capture incorrect");
        }
    }

    conn->detach_sync_capture();
    conn->disconnect();
    cleanup(p);
}

void test_clear_writes_via_sink() {
    using namespace mdbxc;
    const std::string p = "test_capture_clear.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    conn->attach_sync_capture(&sink);

    {
        KeyValueTable<int, int> kv(conn, "clear_kv");
        kv.insert_or_assign(1, 10);
        kv.insert_or_assign(2, 20);
        sink.clear_recorded();
        kv.clear();
        require_one_clear_capture(sink, "clear_kv", "KeyValueTable::clear");
    }

    {
        KeyTable<int> keys(conn, "clear_keys");
        keys.insert(1);
        keys.insert(2);
        sink.clear_recorded();
        keys.clear();
        require_one_clear_capture(sink, "clear_keys", "KeyTable::clear");
    }

    {
        ValueTable<int> value(conn, "clear_value");
        value.set(42);
        sink.clear_recorded();
        value.clear();
        require_one_clear_capture(sink, "clear_value", "ValueTable::clear");
    }

    {
        SequenceTable<std::string> seq(conn, "clear_seq");
        seq.append("one");
        seq.append("two");
        sink.clear_recorded();
        seq.clear();
        require_one_clear_capture(sink, "clear_seq", "SequenceTable::clear");
    }

    {
        VectorStore store(conn, "clear_vector");
        store.add(make_embedding({ 1.0f, 0.0f }), "one", "{}");
        sink.clear_recorded();
        store.clear();
        const char* expected_dbi[] = {
            "vectors_clear_vector_ids",
            "vectors_clear_vector_embeddings",
            "vectors_clear_vector_texts",
            "vectors_clear_vector_metadata"
        };
        {
            std::lock_guard<std::mutex> lk(sink.m_mutex);
            if (sink.m_recorded.size() != 4u) {
                throw std::runtime_error("VectorStore::clear expected four ClearTable ops");
            }
            for (std::size_t i = 0; i < sink.m_recorded.size(); ++i) {
                const sync::ChangeOp& op = sink.m_recorded[i];
                if (op.dbi_name != expected_dbi[i] ||
                    op.op_type != sync::ChangeOpType::ClearTable ||
                    !op.storage_key.empty() ||
                    !op.value.empty()) {
                    throw std::runtime_error("VectorStore::clear capture incorrect");
                }
            }
        }
    }

    conn->detach_sync_capture();
    conn->disconnect();
    cleanup(p);
}

void test_vector_store_writes_via_sink() {
    using namespace mdbxc;
    const std::string p = "test_capture_vector_store.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    conn->attach_sync_capture(&sink);

    {
        VectorStore store(conn, "sync_vector");
        const uint64_t id = store.add(make_embedding({ 1.0f, 0.0f }),
                                      "document", "{}");
        if (id != 0u) {
            throw std::runtime_error("first vector id should be zero");
        }
    }

    conn->detach_sync_capture();
    conn->disconnect();
    cleanup(p);

    if (sink.m_recorded.size() != 4u) {
        throw std::runtime_error("expected four vector store ops, got " +
                                 std::to_string(sink.m_recorded.size()));
    }
    const char* expected_dbi[] = {
        "vectors_sync_vector_ids",
        "vectors_sync_vector_embeddings",
        "vectors_sync_vector_texts",
        "vectors_sync_vector_metadata"
    };
    for (std::size_t i = 0; i < sink.m_recorded.size(); ++i) {
        const sync::ChangeOp& op = sink.m_recorded[i];
        if (op.dbi_name != expected_dbi[i]) {
            throw std::runtime_error("vector store dbi order/name incorrect");
        }
        if (op.op_type != sync::ChangeOpType::Put ||
            op.storage_key.empty() ||
            op.value.empty()) {
            throw std::runtime_error("vector store Put capture incorrect");
        }
        if ((op.dbi_flags & MDBX_INTEGERKEY) == 0) {
            throw std::runtime_error("vector store dbi_flags missing MDBX_INTEGERKEY");
        }
    }
}

void test_any_value_table_does_not_capture_in_v01() {
    using namespace mdbxc;
    const std::string p = "test_capture_any_value_deferred.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    {
        sync::SyncCaptureScope capture(conn, sink);
        AnyValueTable<int> any_values(conn, "any_values");
        any_values.set(1, std::string("one"));
        require_no_capture(sink, "AnyValueTable", "set");
        any_values.insert(2, std::string("two"));
        require_no_capture(sink, "AnyValueTable", "insert");
        any_values.erase(1);
        require_no_capture(sink, "AnyValueTable", "erase");
    }

    conn->disconnect();
    cleanup(p);
}

void test_key_multi_value_table_does_not_capture_in_v01() {
    using namespace mdbxc;
    typedef KeyMultiValueTable<int, std::string> MultiTable;
    typedef KeyOrderedMultiValueTable<int, std::string> OrderedMultiTable;
    const std::string p = "test_capture_key_multi_value_deferred.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    {
        sync::SyncCaptureScope capture(conn, sink);
        MultiTable multi_values(conn, "multi_values");
        multi_values.insert(1, "one");
        require_no_capture(sink, "KeyMultiValueTable", "insert");
        multi_values.insert(1, "uno");
        require_no_capture(sink, "KeyMultiValueTable", "insert repeated key");

        std::vector<MultiTable::value_type> appended;
        appended.push_back(MultiTable::value_type(2, "two"));
        appended.push_back(MultiTable::value_type(2, "two"));
        multi_values.append(appended);
        require_no_capture(sink, "KeyMultiValueTable", "append");

        std::vector<MultiTable::value_type> reconciled;
        reconciled.push_back(MultiTable::value_type(1, "one"));
        reconciled.push_back(MultiTable::value_type(3, "three"));
        multi_values.reconcile(reconciled);
        require_no_capture(sink, "KeyMultiValueTable", "reconcile");

        multi_values.erase(1, "one");
        require_no_capture(sink, "KeyMultiValueTable", "erase value");
        multi_values.erase(3);
        require_no_capture(sink, "KeyMultiValueTable", "erase key");

        multi_values.insert(4, "four");
        require_no_capture(sink, "KeyMultiValueTable", "prepare erase_range");
        multi_values.insert(5, "five");
        require_no_capture(sink, "KeyMultiValueTable", "prepare erase_range");
        if (multi_values.erase_range(4, 5) == 0u) {
            throw std::runtime_error("KeyMultiValueTable::erase_range did not remove prepared rows");
        }
        require_no_capture(sink, "KeyMultiValueTable", "erase_range");

        multi_values.insert(6, "six");
        require_no_capture(sink, "KeyMultiValueTable", "prepare clear");
        if (multi_values.empty()) {
            throw std::runtime_error("KeyMultiValueTable clear precondition is empty");
        }
        multi_values.clear();
        require_no_capture(sink, "KeyMultiValueTable", "clear");

        OrderedMultiTable ordered_multi_values(conn, "ordered_multi_values");
        ordered_multi_values.append(1, "one");
        require_no_capture(sink, "KeyOrderedMultiValueTable", "append");
        ordered_multi_values.append(1, "uno");
        require_no_capture(sink, "KeyOrderedMultiValueTable", "append repeated key");
        if (!ordered_multi_values.erase_at(1, 0u)) {
            throw std::runtime_error("KeyOrderedMultiValueTable::erase_at did not remove prepared row");
        }
        require_no_capture(sink, "KeyOrderedMultiValueTable", "erase_at");
        ordered_multi_values.clear();
        require_no_capture(sink, "KeyOrderedMultiValueTable", "clear");
    }

    conn->disconnect();
    cleanup(p);
}

void test_hashed_key_value_store_does_not_capture_in_v01() {
    using namespace mdbxc;
    typedef HashedKeyValueStore<std::string, std::string> HashedStore;
    const std::string p = "test_capture_hashed_key_value_deferred.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    {
        sync::SyncCaptureScope capture(conn, sink);
        HashedStore hashed(conn, "hashed_values");
        hashed.insert_or_assign("one", "uno");
        require_no_capture(sink, "HashedKeyValueStore", "insert_or_assign");
        hashed.insert("two", "dos");
        require_no_capture(sink, "HashedKeyValueStore", "insert");

        std::vector<HashedStore::value_type> appended;
        appended.push_back(HashedStore::value_type("three", "tres"));
        hashed.append(appended);
        require_no_capture(sink, "HashedKeyValueStore", "append");

        std::vector<HashedStore::value_type> reconciled;
        reconciled.push_back(HashedStore::value_type("one", "eins"));
        hashed.reconcile(reconciled);
        require_no_capture(sink, "HashedKeyValueStore", "reconcile");

        hashed.erase("one");
        require_no_capture(sink, "HashedKeyValueStore", "erase");

        hashed.insert_or_assign("clear-key", "clear-value");
        require_no_capture(sink, "HashedKeyValueStore", "prepare clear");
        if (!hashed.contains("clear-key")) {
            throw std::runtime_error("HashedKeyValueStore clear precondition is missing");
        }
        hashed.clear();
        require_no_capture(sink, "HashedKeyValueStore", "clear");
    }

    conn->disconnect();
    cleanup(p);
}

void test_capture_flush_on_commit() {
    using namespace mdbxc;
    const std::string p = "test_capture_flush.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    StubSink sink;
    conn->attach_sync_capture(&sink);

    {
        KeyTable<int> t(conn, "t2");
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        t.insert(7);
        txn.commit();
    }

    conn->detach_sync_capture();
    cleanup(p);

    if (sink.m_flushed.empty()) {
        throw std::runtime_error("flush_in_txn was not called on Transaction::commit");
    }
}

void test_capture_record_failure_blocks_explicit_commit() {
    using namespace mdbxc;
    const std::string p = "test_capture_record_failure.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    KeyValueTable<int, int> table(conn, "data");
    ThrowingChangeSink sink;
    conn->attach_sync_capture(&sink);

    auto txn = conn->transaction(TransactionMode::WRITABLE);
    bool record_threw = false;
    try {
        table.insert_or_assign(1, 100, txn.handle());
    } catch (const std::runtime_error&) {
        record_threw = true;
    }
    if (!record_threw) {
        throw std::runtime_error("capture record failure did not throw");
    }

    bool commit_rejected = false;
    try {
        txn.commit();
    } catch (const std::logic_error&) {
        commit_rejected = true;
    }
    if (!commit_rejected) {
        throw std::runtime_error(
            "transaction committed after capture record failure");
    }
    if (sink.flush_calls != 0) {
        throw std::runtime_error(
            "capture flush ran after record failure marker");
    }

    txn.rollback();
    conn->detach_sync_capture();

    {
        auto clean_txn = conn->transaction(TransactionMode::WRITABLE);
        table.insert_or_assign(2, 200, clean_txn.handle());
        clean_txn.commit();
    }

    {
        auto ro = conn->transaction(TransactionMode::READ_ONLY);
        int value = 0;
        if (table.try_get(1, value, ro.handle())) {
            throw std::runtime_error(
                "failed capture transaction committed user value");
        }
        if (!table.try_get(2, value, ro.handle()) || value != 200) {
            throw std::runtime_error(
                "capture failure marker was not cleared after rollback");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_capture_flush_failure_blocks_commit_retry() {
    using namespace mdbxc;
    const std::string p = "test_capture_flush_failure.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    KeyValueTable<int, int> table(conn, "data");
    ThrowOnceFlushSink sink;
    conn->attach_sync_capture(&sink);

    auto txn = conn->transaction(TransactionMode::WRITABLE);
    table.insert_or_assign(1, 100, txn.handle());

    bool flush_threw = false;
    try {
        txn.commit();
    } catch (const std::runtime_error&) {
        flush_threw = true;
    }
    if (!flush_threw || sink.flush_calls != 1) {
        throw std::runtime_error("first commit did not fail in capture flush");
    }

    bool retry_rejected = false;
    try {
        txn.commit();
    } catch (const std::logic_error&) {
        retry_rejected = true;
    }
    if (!retry_rejected || sink.flush_calls != 1) {
        throw std::runtime_error(
            "commit retry after capture flush failure was not rejected early");
    }

    txn.rollback();
    conn->detach_sync_capture();

    {
        auto ro = conn->transaction(TransactionMode::READ_ONLY);
        int value = 0;
        if (table.try_get(1, value, ro.handle())) {
            throw std::runtime_error(
                "failed capture flush transaction committed user value");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_partial_capture_flush_failure_blocks_commit_retry() {
    using namespace mdbxc;
    const std::string p = "test_capture_partial_flush_failure.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    KeyValueTable<int, int> table(conn, "data");
    ThrowOnceFlushSink sink;
    sink.consume_before_throw = true;
    conn->attach_sync_capture(&sink);

    auto txn = conn->transaction(TransactionMode::WRITABLE);
    table.insert_or_assign(1, 100, txn.handle());

    bool flush_threw = false;
    try {
        txn.commit();
    } catch (const std::runtime_error&) {
        flush_threw = true;
    }
    if (!flush_threw || !sink.pending.empty()) {
        throw std::runtime_error(
            "partial capture flush failure precondition mismatch");
    }

    bool retry_rejected = false;
    try {
        txn.commit();
    } catch (const std::logic_error&) {
        retry_rejected = true;
    }
    if (!retry_rejected || sink.flush_calls != 1) {
        throw std::runtime_error(
            "partial capture flush failure allowed commit retry");
    }

    txn.rollback();
    conn->detach_sync_capture();

    {
        auto ro = conn->transaction(TransactionMode::READ_ONLY);
        int value = 0;
        if (table.try_get(1, value, ro.handle())) {
            throw std::runtime_error(
                "partial capture flush failure committed user value");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_raw_external_transaction_rejected_while_capture_attached() {
    using namespace mdbxc;
    const std::string p = "test_capture_raw_txn_rejected.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    KeyValueTable<int, int> table(conn, "data");
    StubSink sink;
    conn->attach_sync_capture(&sink);

    MDBX_txn* raw = nullptr;
    check_mdbx(
        mdbx_txn_begin(conn->env_handle(),
                       nullptr,
                       MDBX_TXN_READWRITE,
                       &raw),
        "failed to begin raw transaction");

    bool rejected = false;
    try {
        table.insert_or_assign(1, 100, raw);
    } catch (const std::logic_error&) {
        rejected = true;
    }
    if (!rejected) {
        mdbx_txn_abort(raw);
        throw std::runtime_error(
            "raw unmanaged transaction accepted with sync capture attached");
    }
    check_mdbx(mdbx_txn_commit(raw), "failed to commit empty raw transaction");
    conn->detach_sync_capture();

    {
        auto ro = conn->transaction(TransactionMode::READ_ONLY);
        int value = 0;
        if (table.try_get(1, value, ro.handle())) {
            throw std::runtime_error(
                "raw rejected transaction still wrote user value");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_raw_read_only_transaction_allowed_while_capture_attached() {
    using namespace mdbxc;
    const std::string p = "test_capture_raw_read_only_txn_allowed.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    KeyValueTable<int, int> table(conn, "data");
    table.insert_or_assign(1, 100);
    table.insert_or_assign(2, 200);
    table.insert_or_assign(3, 300);

    StubSink sink;
    conn->attach_sync_capture(&sink);

    MDBX_txn* raw = nullptr;
    check_mdbx(
        mdbx_txn_begin(conn->env_handle(),
                       nullptr,
                       MDBX_TXN_RDONLY,
                       &raw),
        "failed to begin raw read-only transaction");

    try {
        int value = 0;
        if (!table.try_get(1, value, raw) || value != 100) {
            throw std::runtime_error(
                "raw read-only transaction failed point lookup");
        }
        if (!table.contains(2, raw)) {
            throw std::runtime_error(
                "raw read-only transaction failed contains");
        }
        if (!table.contains_range(1, 3, raw)) {
            throw std::runtime_error(
                "raw read-only transaction failed contains_range");
        }
        if (table.count(raw) != 3u) {
            throw std::runtime_error(
                "raw read-only transaction failed count");
        }
        const std::map<int, int> pairs =
            table.range(1, 3, raw);
        if (pairs.size() != 3u) {
            throw std::runtime_error(
                "raw read-only transaction failed range scan");
        }
    } catch (...) {
        mdbx_txn_abort(raw);
        throw;
    }
    mdbx_txn_abort(raw);

    conn->detach_sync_capture();
    conn->disconnect();
    cleanup(p);
}

void test_flush_only_capture_sink_is_abstract() {
    static_assert(std::is_abstract<FlushOnlySink>::value,
                  "flush-only capture sink must be abstract");
}

void test_changelog_capture_roundtrip() {
    using namespace mdbxc;
    const std::string p = "test_capture_changelog.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    /// SyncEngine normally writes node_id before capture is enabled. We
    /// simulate that by writing the node id through MetaStore directly.
    const sync::NodeId node_id = [] {
        sync::NodeId n{};
        for (int i = 0; i < 16; ++i) {
            n[i] = static_cast<std::uint8_t>(0xC0 + i);
        }
        return n;
    }();

    {
        sync::MetaStore meta(conn->env_handle());
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        meta.open(txn.handle());
        meta.set_node_id(txn.handle(), node_id);
        txn.commit();
    }

    sync::ThreadLocalChangeAccumulator sink(conn);
    conn->attach_sync_capture(&sink);

    {
        KeyValueTable<int, int> kv(conn, "data");
        kv.insert_or_assign(1, 100);
        kv.insert_or_assign(2, 200);
    }

    conn->detach_sync_capture();

    sync::ChangeLogStore cl(conn->env_handle());
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        cl.open(txn.handle());
        txn.commit();
    }
    {
        auto txn = conn->transaction(TransactionMode::READ_ONLY);
        if (!cl.contains(txn.handle(), node_id, 1)) {
            throw std::runtime_error("changelog missing seq 1");
        }
        if (!cl.contains(txn.handle(), node_id, 2)) {
            throw std::runtime_error("changelog missing seq 2");
        }
        if (cl.contains(txn.handle(), node_id, 3)) {
            throw std::runtime_error("changelog unexpectedly has seq 3");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_changelog_capture_auto_transaction_loop() {
    using namespace mdbxc;
    const std::string p = "test_capture_auto_transaction_loop.mdbx";
    const std::size_t write_count = 64u;
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    sync::NodeId node_id{};
    for (int i = 0; i < 16; ++i) {
        node_id[i] = static_cast<std::uint8_t>(0xD0 + i);
    }

    {
        sync::MetaStore meta(conn->env_handle());
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        meta.open(txn.handle());
        meta.set_node_id(txn.handle(), node_id);
        txn.commit();
    }

    sync::ThreadLocalChangeAccumulator sink(conn);
    conn->attach_sync_capture(&sink);

    KeyValueTable<int, int> kv(conn, "data");
    for (std::size_t i = 0; i < write_count; ++i) {
        const int key = static_cast<int>(i + 1u);
        kv.insert_or_assign(key, key * 10);
    }

    conn->detach_sync_capture();

    sync::ChangeLogStore changelog(conn->env_handle());
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        changelog.open(txn.handle());
        txn.commit();
    }

    {
        auto txn = conn->transaction(TransactionMode::READ_ONLY);
        for (std::size_t i = 0; i < write_count; ++i) {
            const std::uint64_t seq = static_cast<std::uint64_t>(i + 1u);
            std::vector<std::uint8_t> bytes;
            if (!changelog.get(txn.handle(), node_id, seq, bytes)) {
                throw std::runtime_error(
                    "auto-transaction loop changelog is missing seq " +
                    std::to_string(seq));
            }

            const sync::ChangeBatch batch =
                sync::ChangeBatchCodec::decode_exact(bytes);
            const int key = static_cast<int>(i + 1u);
            if (batch.origin_node_id != node_id || batch.seq != seq ||
                batch.ops.size() != 1u ||
                batch.ops[0].dbi_name != "data" ||
                batch.ops[0].op_type != sync::ChangeOpType::Put ||
                batch.ops[0].storage_key.empty() || batch.ops[0].value.empty()) {
                throw std::runtime_error(
                    "auto-transaction loop changelog batch is malformed at seq " +
                    std::to_string(seq));
            }

            const MDBX_val captured_value = SerializeScratch::view(
                batch.ops[0].value.data(), batch.ops[0].value.size());
            if (deserialize_value<int>(captured_value) != key * 10) {
                throw std::runtime_error(
                    "auto-transaction loop changelog value mismatch at seq " +
                    std::to_string(seq));
            }
        }

        if (changelog.contains(txn.handle(), node_id,
                               static_cast<std::uint64_t>(write_count + 1u))) {
            throw std::runtime_error(
                "auto-transaction loop changelog unexpectedly has an extra batch");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_changelog_capture_preserves_enriched_change_op() {
    using namespace mdbxc;
    const std::string p = "test_capture_enriched_changelog.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    sync::NodeId node_id{};
    for (int i = 0; i < 16; ++i) {
        node_id[i] = static_cast<std::uint8_t>(0xB0 + i);
    }

    {
        sync::MetaStore meta(conn->env_handle());
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        meta.open(txn.handle());
        meta.set_node_id(txn.handle(), node_id);
        txn.commit();
    }

    sync::ThreadLocalChangeAccumulator sink(conn);
    conn->attach_sync_capture(&sink);

    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        sync::ChangeOp op;
        op.op_type = sync::ChangeOpType::Put;
        op.op_flags =
            sync::OP_HAS_IDENTITY_KEY | sync::OP_HAS_REVISION_KEY;
        op.dbi_flags = MDBX_CREATE;
        op.dbi_name = "logical_data";
        op.storage_key.push_back(0x01u);
        op.value.push_back(0x02u);
        op.identity_key.push_back(0x03u);
        op.revision_key.push_back(0x04u);
        sink.record_change_op(txn.handle(), op);
        txn.commit();
    }

    conn->detach_sync_capture();

    sync::ChangeLogStore cl(conn->env_handle());
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        cl.open(txn.handle());
        txn.commit();
    }

    std::vector<std::uint8_t> bytes;
    {
        auto txn = conn->transaction(TransactionMode::READ_ONLY);
        if (!cl.get(txn.handle(), node_id, 1, bytes)) {
            throw std::runtime_error("enriched changelog batch missing");
        }
    }

    const sync::ChangeBatch batch = sync::ChangeBatchCodec::decode(bytes);
    if (batch.ops.size() != 1u) {
        throw std::runtime_error("enriched changelog batch op count mismatch");
    }

    const sync::ChangeOp& decoded = batch.ops[0];
    if (decoded.op_flags !=
            (sync::OP_HAS_IDENTITY_KEY | sync::OP_HAS_REVISION_KEY) ||
        decoded.identity_key.size() != 1u ||
        decoded.identity_key[0] != 0x03u ||
        decoded.revision_key.size() != 1u ||
        decoded.revision_key[0] != 0x04u) {
        throw std::runtime_error(
            "enriched ChangeOp metadata was not preserved");
    }

    conn->disconnect();
    cleanup(p);
}

void test_zero_node_id_rejected() {
    using namespace mdbxc;
    const std::string p = "test_capture_zero_node_id.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    sync::ThreadLocalChangeAccumulator sink(conn);
    conn->attach_sync_capture(&sink);

    bool caught = false;
    try {
        KeyValueTable<int, int> kv(conn, "t");
        kv.insert_or_assign(1, 100);
    } catch (const std::runtime_error&) {
        caught = true;
    }

    conn->detach_sync_capture();
    conn->disconnect();
    cleanup(p);

    if (!caught) {
        throw std::runtime_error("capture must reject uninitialised node_id");
    }
}
void test_explicit_rollback_does_not_flush() {
    using namespace mdbxc;
    const std::string p = "test_capture_rollback.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    /// Pre-init node_id.
    {
        sync::MetaStore meta(conn->env_handle());
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        meta.open(txn.handle());
        sync::NodeId n{};
        n[0] = 0xA1;
        meta.set_node_id(txn.handle(), n);
        txn.commit();
    }

    sync::ThreadLocalChangeAccumulator sink(conn);
    conn->attach_sync_capture(&sink);

    KeyValueTable<int, int> kv(conn, "t");
    {
        kv.insert_or_assign(1, 100);
    }
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        kv.insert_or_assign(2, 200);
        txn.rollback();
    }
    {
        kv.insert_or_assign(3, 300);
    }

    conn->detach_sync_capture();

    sync::ChangeLogStore cl(conn->env_handle());
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        cl.open(txn.handle());
        txn.commit();
    }

    std::vector<std::uint8_t> batch1, batch2;
    {
        auto txn = conn->transaction(TransactionMode::READ_ONLY);
        sync::NodeId n{};
        n[0] = 0xA1;
        if (!cl.get(txn.handle(), n, 1, batch1)) {
            throw std::runtime_error("changelog missing seq 1");
        }
        if (!cl.get(txn.handle(), n, 2, batch2)) {
            throw std::runtime_error("changelog missing seq 2");
        }
    }

    auto batch_has_key = [](const std::vector<std::uint8_t>& bytes, const std::vector<std::uint8_t>& key) {
        const sync::ChangeBatch b = sync::ChangeBatchCodec::decode_exact(bytes);
        for (const sync::ChangeOp& op : b.ops) {
            if (op.dbi_name == "t" && op.storage_key == key) return true;
        }
        return false;
    };

    if (!batch_has_key(batch1, storage_key_bytes(1))) throw std::runtime_error("seq 1 should contain key=1");
    if (batch_has_key(batch1, storage_key_bytes(2))) throw std::runtime_error("seq 1 should not contain rolled-back key=2");
    if (!batch_has_key(batch2, storage_key_bytes(3))) throw std::runtime_error("seq 2 should contain key=3");
    if (batch_has_key(batch2, storage_key_bytes(2))) throw std::runtime_error("seq 2 leaked rolled-back key=2");

    conn->disconnect();
    cleanup(p);
}


void test_aborted_transaction_does_not_flush() {
    using namespace mdbxc;
    const std::string p = "test_capture_aborted.mdbx";
    cleanup(p);

    Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = Connection::create(cfg);

    /// Pre-init node_id so capture does not throw on the first attempt.
    {
        sync::MetaStore meta(conn->env_handle());
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        meta.open(txn.handle());
        sync::NodeId n{};
        n[0] = 0xA1;
        meta.set_node_id(txn.handle(), n);
        txn.commit();
    }

    sync::ThreadLocalChangeAccumulator sink(conn);
    conn->attach_sync_capture(&sink);

    /// One KeyValueTable instance shared between the two writes; the DBI
    /// handle stays valid across both transactions because MDBX caches DBIs
    /// by name in the environment.
    KeyValueTable<int, int> kv(conn, "t");
    {
        kv.insert_or_assign(1, 100);
    }
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        kv.insert_or_assign(2, 200);
        /// txn aborts on destruction without commit.
    }
    {
        kv.insert_or_assign(3, 300);
    }

    conn->detach_sync_capture();

    /// Decode every committed ChangeBatch and ensure the aborted op (key=2)
    /// does not appear. Also confirm that seq 1 carries op=1 and seq 2
    /// carries op=3 (the post-abort commit).
    sync::ChangeLogStore cl(conn->env_handle());
    {
        auto txn = conn->transaction(TransactionMode::WRITABLE);
        cl.open(txn.handle());
        txn.commit();
    }

    std::vector<std::uint8_t> batch1, batch2;
    {
        auto txn = conn->transaction(TransactionMode::READ_ONLY);
        sync::NodeId n{};
        n[0] = 0xA1;
        if (!cl.get(txn.handle(), n, 1, batch1)) {
            throw std::runtime_error("changelog missing seq 1 from first commit");
        }
        if (!cl.get(txn.handle(), n, 2, batch2)) {
            throw std::runtime_error("changelog missing seq 2 from post-abort commit");
        }
    }

    auto batch_has_key = [](const std::vector<std::uint8_t>& bytes, const std::vector<std::uint8_t>& key) {
        const sync::ChangeBatch b = sync::ChangeBatchCodec::decode_exact(bytes);
        for (const sync::ChangeOp& op : b.ops) {
            if (op.dbi_name == "t" && op.storage_key == key) {
                return true;
            }
        }
        return false;
    };

    if (!batch_has_key(batch1, storage_key_bytes(1))) {
        throw std::runtime_error("seq 1 should contain op with key=1");
    }
    if (batch_has_key(batch1, storage_key_bytes(2))) {
        throw std::runtime_error("seq 1 should not contain aborted op with key=2");
    }
    if (!batch_has_key(batch2, storage_key_bytes(3))) {
        throw std::runtime_error("seq 2 should contain op with key=3 (post-abort commit)");
    }
    if (batch_has_key(batch2, storage_key_bytes(2))) {
        throw std::runtime_error("seq 2 leaked aborted op with key=2");
    }

    conn->disconnect();
    cleanup(p);
}

} // namespace

int main(int argc, char** argv) {
    if (argc == 2 &&
        std::string(argv[1]) ==
            "--sync-capture-out-of-order-destruction-child") {
        run_capture_scope_out_of_order_destruction_child();
        return 0;
    }

    g_executable_path = argc > 0 ? argv[0] : nullptr;

    struct Case {
        const char* name;
        void (*fn)();
    };

    const Case cases[] = {
        { "test_no_sink_no_capture", &test_no_sink_no_capture },
        { "test_capture_writes_via_sink", &test_capture_writes_via_sink },
        { "test_capture_uses_full_change_op_overload",
          &test_capture_uses_full_change_op_overload },
        { "test_legacy_sink_rejects_enriched_change_op",
          &test_legacy_sink_rejects_enriched_change_op },
        { "test_capture_scope_restores_previous_sink",
          &test_capture_scope_restores_previous_sink },
        { "test_capture_scope_nested_scopes_restore_in_order",
          &test_capture_scope_nested_scopes_restore_in_order },
        { "test_capture_scope_rejects_out_of_order_detach",
          &test_capture_scope_rejects_out_of_order_detach },
        { "test_capture_scope_rejects_same_sink_out_of_order_detach",
          &test_capture_scope_rejects_same_sink_out_of_order_detach },
        { "test_capture_scope_terminates_on_out_of_order_destruction",
          &test_capture_scope_terminates_on_out_of_order_destruction },
        { "test_capture_scope_detach_restores_null_once",
          &test_capture_scope_detach_restores_null_once },
        { "test_capture_scope_rejects_null_arguments",
          &test_capture_scope_rejects_null_arguments },
        { "test_sequence_set_writes_via_sink", &test_sequence_set_writes_via_sink },
        { "test_value_table_writes_storage_key_via_sink",
          &test_value_table_writes_storage_key_via_sink },
        { "test_key_value_bulk_writes_via_sink",
          &test_key_value_bulk_writes_via_sink },
        { "test_range_erase_writes_via_sink", &test_range_erase_writes_via_sink },
        { "test_clear_writes_via_sink", &test_clear_writes_via_sink },
        { "test_vector_store_writes_via_sink",
          &test_vector_store_writes_via_sink },
        { "test_capture_flush_on_commit", &test_capture_flush_on_commit },
        { "test_capture_record_failure_blocks_explicit_commit",
          &test_capture_record_failure_blocks_explicit_commit },
        { "test_capture_flush_failure_blocks_commit_retry",
          &test_capture_flush_failure_blocks_commit_retry },
        { "test_partial_capture_flush_failure_blocks_commit_retry",
          &test_partial_capture_flush_failure_blocks_commit_retry },
        { "test_raw_external_transaction_rejected_while_capture_attached",
          &test_raw_external_transaction_rejected_while_capture_attached },
        { "test_raw_read_only_transaction_allowed_while_capture_attached",
          &test_raw_read_only_transaction_allowed_while_capture_attached },
        { "test_flush_only_capture_sink_is_abstract",
          &test_flush_only_capture_sink_is_abstract },
        { "test_any_value_table_does_not_capture_in_v01",
          &test_any_value_table_does_not_capture_in_v01 },
        { "test_key_multi_value_table_does_not_capture_in_v01",
          &test_key_multi_value_table_does_not_capture_in_v01 },
        { "test_hashed_key_value_store_does_not_capture_in_v01",
          &test_hashed_key_value_store_does_not_capture_in_v01 },
        { "test_changelog_capture_roundtrip", &test_changelog_capture_roundtrip },
        { "test_changelog_capture_auto_transaction_loop",
          &test_changelog_capture_auto_transaction_loop },
        { "test_changelog_capture_preserves_enriched_change_op",
          &test_changelog_capture_preserves_enriched_change_op },
        { "test_zero_node_id_rejected", &test_zero_node_id_rejected },
        { "test_aborted_transaction_does_not_flush",
          &test_aborted_transaction_does_not_flush },
        { "test_explicit_rollback_does_not_flush",
          &test_explicit_rollback_does_not_flush }
    };

    for (std::size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        try {
            cases[i].fn();
            std::printf("PASS %s\n", cases[i].name);
        } catch (const std::exception& e) {
            std::printf("FAIL %s: %s\n", cases[i].name, e.what());
            return static_cast<int>(i + 1u);
        } catch (...) {
            std::printf("FAIL %s: non-std exception\n", cases[i].name);
            return static_cast<int>(i + 1u);
        }
    }

    return 0;
}
