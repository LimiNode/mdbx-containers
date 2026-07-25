#include <mdbx_containers/sync.hpp>

#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void cleanup(const std::string& p) {
    std::remove(p.c_str());
}

class RecordingAdapter : public mdbxc::sync::ILogicalTableAdapter {
public:
    explicit RecordingAdapter(const std::string& schema_id)
        : m_schema_id(schema_id),
          m_kind(mdbxc::sync::LogicalTableKind::KeyMultiValue),
          m_schema_version(1) {}

    RecordingAdapter(const std::string& schema_id,
                     mdbxc::sync::LogicalTableKind kind,
                     std::uint32_t schema_version)
        : m_schema_id(schema_id),
          m_kind(kind),
          m_schema_version(schema_version) {}

    mdbxc::sync::LogicalSchemaRef schema_ref() const override {
        mdbxc::sync::LogicalSchemaRef ref;
        ref.schema_id = m_schema_id;
        ref.kind = m_kind;
        ref.schema_version = m_schema_version;
        return ref;
    }

    std::vector<std::string> affected_dbis() const override {
        std::vector<std::string> out;
        out.push_back("items");
        return out;
    }

    mdbxc::sync::LogicalApplyResult preflight(
            MDBX_txn* txn,
            const mdbxc::sync::LogicalChange& change) const override {
        (void)txn;
        ++m_preflight_calls;
        m_events.push_back(std::string("P") + std::to_string(change.opcode));
        if (change.opcode == 99u) {
            return mdbxc::sync::LogicalApplyResult::failure("blocked");
        }
        return mdbxc::sync::LogicalApplyResult::success();
    }

    mdbxc::sync::LogicalApplyResult apply(
            MDBX_txn* txn,
            const mdbxc::sync::LogicalChange& change) override {
        (void)txn;
        ++m_apply_calls;
        m_events.push_back(std::string("A") + std::to_string(change.opcode));
        m_applied_opcodes.push_back(change.opcode);
        if (change.opcode == m_throw_apply_opcode) {
            throw std::runtime_error("apply threw");
        }
        if (change.opcode == m_fail_apply_opcode) {
            return mdbxc::sync::LogicalApplyResult::failure("apply blocked");
        }
        return mdbxc::sync::LogicalApplyResult::success();
    }

    mutable std::size_t m_preflight_calls = 0;
    std::size_t m_apply_calls = 0;
    std::uint32_t m_fail_apply_opcode = 0;
    std::uint32_t m_throw_apply_opcode = 0;
    mutable std::vector<std::string> m_events;
    std::vector<std::uint32_t> m_applied_opcodes;

private:
    std::string m_schema_id;
    mdbxc::sync::LogicalTableKind m_kind;
    std::uint32_t m_schema_version;
};

class MdbxMutatingAdapter : public RecordingAdapter {
public:
    MdbxMutatingAdapter(const std::string& schema_id,
                        const std::string& dbi_name,
                        bool fail_apply)
        : RecordingAdapter(schema_id),
          m_dbi_name(dbi_name),
          m_fail_apply(fail_apply) {}

    std::vector<std::string> affected_dbis() const override {
        std::vector<std::string> out;
        out.push_back(m_dbi_name);
        return out;
    }

    mdbxc::sync::LogicalApplyResult apply(
            MDBX_txn* txn,
            const mdbxc::sync::LogicalChange& change) override {
        if (m_fail_apply) {
            return mdbxc::sync::LogicalApplyResult::failure("apply blocked");
        }

        MDBX_dbi dbi = 0;
        mdbxc::check_mdbx(
            mdbx_dbi_open(txn, m_dbi_name.c_str(), MDBX_CREATE, &dbi),
            "logical adapter test DBI open failed");
        const char key_bytes[] = "logical-key";
        const char value_bytes[] = "partial-value";
        MDBX_val key = {
            const_cast<char*>(key_bytes),
            std::strlen(key_bytes)
        };
        MDBX_val value = {
            const_cast<char*>(value_bytes),
            std::strlen(value_bytes)
        };
        mdbxc::check_mdbx(mdbx_put(txn, dbi, &key, &value, MDBX_UPSERT),
                          "logical adapter test put failed");
        return RecordingAdapter::apply(txn, change);
    }

private:
    std::string m_dbi_name;
    bool m_fail_apply;
};

class IncompleteAdapter : public RecordingAdapter {
public:
    IncompleteAdapter() : RecordingAdapter("") {}

    mdbxc::sync::LogicalSchemaRef schema_ref() const override {
        return mdbxc::sync::LogicalSchemaRef();
    }
};

mdbxc::sync::LogicalChange make_change(const std::string& schema_id,
                                       std::uint32_t opcode) {
    mdbxc::sync::LogicalChange change;
    change.schema.schema_id = schema_id;
    change.schema.kind = mdbxc::sync::LogicalTableKind::KeyMultiValue;
    change.schema.schema_version = 1;
    change.opcode = opcode;
    return change;
}

void test_registry_rejects_invalid_adapters() {
    mdbxc::sync::LogicalTableRegistry registry;
    bool caught_null = false;
    try {
        registry.register_adapter(nullptr);
    } catch (const std::invalid_argument&) {
        caught_null = true;
    }
    if (!caught_null) {
        throw std::runtime_error("null logical adapter was accepted");
    }

    IncompleteAdapter incomplete;
    bool caught_incomplete = false;
    try {
        registry.register_adapter(&incomplete);
    } catch (const std::invalid_argument&) {
        caught_incomplete = true;
    }
    if (!caught_incomplete) {
        throw std::runtime_error("incomplete logical adapter was accepted");
    }
}

void test_registry_rejects_duplicate_schema_id() {
    mdbxc::sync::LogicalTableRegistry registry;
    RecordingAdapter first("schema.items.v1");
    RecordingAdapter second("schema.items.v1");
    registry.register_adapter(&first);

    bool caught = false;
    try {
        registry.register_adapter(&second);
    } catch (const std::invalid_argument&) {
        caught = true;
    }
    if (!caught) {
        throw std::runtime_error("duplicate logical adapter was accepted");
    }
}

void test_preflight_then_apply_order() {
    mdbxc::sync::LogicalTableRegistry registry;
    RecordingAdapter adapter("schema.items.v1");
    registry.register_adapter(&adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(make_change("schema.items.v1", 1));
    changes.push_back(make_change("schema.items.v1", 2));

    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(nullptr, changes);
    if (!result.ok ||
        adapter.m_preflight_calls != 2u ||
        adapter.m_apply_calls != 2u ||
        adapter.m_events.size() != 4u ||
        adapter.m_events[0] != "P1" ||
        adapter.m_events[1] != "P2" ||
        adapter.m_events[2] != "A1" ||
        adapter.m_events[3] != "A2" ||
        adapter.m_applied_opcodes.size() != 2u ||
        adapter.m_applied_opcodes[0] != 1u ||
        adapter.m_applied_opcodes[1] != 2u) {
        throw std::runtime_error("logical preflight/apply order mismatch");
    }
}

void test_preflight_failure_blocks_apply() {
    mdbxc::sync::LogicalTableRegistry registry;
    RecordingAdapter adapter("schema.items.v1");
    registry.register_adapter(&adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(make_change("schema.items.v1", 99));
    changes.push_back(make_change("schema.items.v1", 2));

    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(nullptr, changes);
    if (result.ok ||
        adapter.m_preflight_calls != 1u ||
        adapter.m_apply_calls != 0u) {
        throw std::runtime_error("logical preflight failure applied changes");
    }
}

void test_schema_mismatch_blocks_adapter_calls() {
    mdbxc::sync::LogicalTableRegistry registry;
    RecordingAdapter adapter("schema.items.v1");
    registry.register_adapter(&adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    mdbxc::sync::LogicalChange wrong_kind =
        make_change("schema.items.v1", 1);
    wrong_kind.schema.kind = mdbxc::sync::LogicalTableKind::AnyValue;
    changes.push_back(wrong_kind);

    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(nullptr, changes);
    if (result.ok ||
        adapter.m_preflight_calls != 0u ||
        adapter.m_apply_calls != 0u) {
        throw std::runtime_error(
            "schema kind mismatch reached logical adapter");
    }
}

void test_schema_version_mismatch_blocks_adapter_calls() {
    mdbxc::sync::LogicalTableRegistry registry;
    RecordingAdapter adapter("schema.items.v1");
    registry.register_adapter(&adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    mdbxc::sync::LogicalChange wrong_version =
        make_change("schema.items.v1", 1);
    wrong_version.schema.schema_version = 2;
    changes.push_back(wrong_version);

    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(nullptr, changes);
    if (result.ok ||
        adapter.m_preflight_calls != 0u ||
        adapter.m_apply_calls != 0u) {
        throw std::runtime_error(
            "schema version mismatch reached logical adapter");
    }
}

void test_reserved_flags_block_adapter_calls() {
    mdbxc::sync::LogicalTableRegistry registry;
    RecordingAdapter adapter("schema.items.v1");
    registry.register_adapter(&adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    mdbxc::sync::LogicalChange change =
        make_change("schema.items.v1", 1);
    change.flags = 1u;
    changes.push_back(change);

    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(nullptr, changes);
    if (result.ok ||
        adapter.m_preflight_calls != 0u ||
        adapter.m_apply_calls != 0u) {
        throw std::runtime_error(
            "reserved logical flags reached logical adapter");
    }
}

void test_late_missing_adapter_blocks_all_preflight() {
    mdbxc::sync::LogicalTableRegistry registry;
    RecordingAdapter adapter("schema.items.v1");
    registry.register_adapter(&adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(make_change("schema.items.v1", 1));
    changes.push_back(make_change("schema.missing.v1", 2));

    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(nullptr, changes);
    if (result.ok ||
        adapter.m_preflight_calls != 0u ||
        adapter.m_apply_calls != 0u) {
        throw std::runtime_error(
            "late missing adapter did not block preflight phase");
    }
}

void test_late_schema_mismatch_blocks_all_preflight() {
    mdbxc::sync::LogicalTableRegistry registry;
    RecordingAdapter adapter("schema.items.v1");
    registry.register_adapter(&adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(make_change("schema.items.v1", 1));
    mdbxc::sync::LogicalChange wrong_kind =
        make_change("schema.items.v1", 2);
    wrong_kind.schema.kind = mdbxc::sync::LogicalTableKind::AnyValue;
    changes.push_back(wrong_kind);

    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(nullptr, changes);
    if (result.ok ||
        adapter.m_preflight_calls != 0u ||
        adapter.m_apply_calls != 0u) {
        throw std::runtime_error(
            "late schema kind mismatch did not block preflight phase");
    }
}

void test_late_schema_version_mismatch_blocks_all_preflight() {
    mdbxc::sync::LogicalTableRegistry registry;
    RecordingAdapter adapter("schema.items.v1");
    registry.register_adapter(&adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(make_change("schema.items.v1", 1));
    mdbxc::sync::LogicalChange wrong_version =
        make_change("schema.items.v1", 2);
    wrong_version.schema.schema_version = 2;
    changes.push_back(wrong_version);

    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(nullptr, changes);
    if (result.ok ||
        adapter.m_preflight_calls != 0u ||
        adapter.m_apply_calls != 0u) {
        throw std::runtime_error(
            "late schema version mismatch did not block preflight phase");
    }
}

void test_late_reserved_flags_block_all_preflight() {
    mdbxc::sync::LogicalTableRegistry registry;
    RecordingAdapter adapter("schema.items.v1");
    registry.register_adapter(&adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(make_change("schema.items.v1", 1));
    mdbxc::sync::LogicalChange flagged =
        make_change("schema.items.v1", 2);
    flagged.flags = 1u;
    changes.push_back(flagged);

    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(nullptr, changes);
    if (result.ok ||
        adapter.m_preflight_calls != 0u ||
        adapter.m_apply_calls != 0u) {
        throw std::runtime_error(
            "late reserved flags did not block preflight phase");
    }
}

void test_apply_failure_returns_failure_after_prior_apply() {
    mdbxc::sync::LogicalTableRegistry registry;
    RecordingAdapter adapter("schema.items.v1");
    adapter.m_fail_apply_opcode = 2;
    registry.register_adapter(&adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(make_change("schema.items.v1", 1));
    changes.push_back(make_change("schema.items.v1", 2));

    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(nullptr, changes);
    if (result.ok ||
        adapter.m_events.size() != 4u ||
        adapter.m_events[0] != "P1" ||
        adapter.m_events[1] != "P2" ||
        adapter.m_events[2] != "A1" ||
        adapter.m_events[3] != "A2" ||
        adapter.m_apply_calls != 2u) {
        throw std::runtime_error(
            "apply failure sequence contract mismatch");
    }
}

void test_apply_exception_returns_failure_after_prior_apply() {
    mdbxc::sync::LogicalTableRegistry registry;
    RecordingAdapter adapter("schema.items.v1");
    adapter.m_throw_apply_opcode = 2;
    registry.register_adapter(&adapter);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(make_change("schema.items.v1", 1));
    changes.push_back(make_change("schema.items.v1", 2));

    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(nullptr, changes);
    if (result.ok ||
        result.error.find("apply threw") == std::string::npos ||
        adapter.m_events.size() != 4u ||
        adapter.m_events[0] != "P1" ||
        adapter.m_events[1] != "P2" ||
        adapter.m_events[2] != "A1" ||
        adapter.m_events[3] != "A2" ||
        adapter.m_apply_calls != 2u) {
        throw std::runtime_error(
            "apply exception sequence contract mismatch");
    }
}

void test_missing_adapter_fails_without_apply() {
    mdbxc::sync::LogicalTableRegistry registry;
    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(make_change("schema.missing.v1", 1));

    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(nullptr, changes);
    if (result.ok) {
        throw std::runtime_error("missing logical adapter succeeded");
    }
}

void test_mdbx_mutation_rolls_back_when_later_apply_fails() {
    const std::string path = "test_logical_adapter_mdbx_rollback.mdbx";
    cleanup(path);

    mdbxc::Config cfg;
    cfg.pathname = path;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    std::shared_ptr<mdbxc::Connection> conn = mdbxc::Connection::create(cfg);

    mdbxc::sync::LogicalTableRegistry registry;
    MdbxMutatingAdapter writer("schema.writer.v1",
                               "logical_adapter_rollback_items",
                               false);
    MdbxMutatingAdapter failing("schema.failing.v1",
                                "logical_adapter_rollback_items",
                                true);
    registry.register_adapter(&writer);
    registry.register_adapter(&failing);

    std::vector<mdbxc::sync::LogicalChange> changes;
    changes.push_back(make_change("schema.writer.v1", 1));
    changes.push_back(make_change("schema.failing.v1", 2));

    mdbxc::Transaction txn =
        conn->transaction(mdbxc::TransactionMode::WRITABLE);
    const mdbxc::sync::LogicalApplyResult result =
        registry.preflight_then_apply(txn.handle(), changes);
    if (result.ok) {
        throw std::runtime_error("failing logical apply succeeded");
    }

    MDBX_dbi write_dbi = 0;
    mdbxc::check_mdbx(
        mdbx_dbi_open(txn.handle(), "logical_adapter_rollback_items",
                      static_cast<MDBX_db_flags_t>(0), &write_dbi),
        "logical adapter rollback write DBI open failed");
    const char key_bytes[] = "logical-key";
    MDBX_val key = {
        const_cast<char*>(key_bytes),
        std::strlen(key_bytes)
    };
    MDBX_val value;
    mdbxc::check_mdbx(mdbx_get(txn.handle(), write_dbi, &key, &value),
                      "logical adapter rollback write read failed");
    const char expected_value[] = "partial-value";
    if (value.iov_len != std::strlen(expected_value) ||
        std::memcmp(value.iov_base, expected_value,
                    std::strlen(expected_value)) != 0) {
        throw std::runtime_error(
            "logical adapter mutation was not visible before rollback");
    }
    txn.rollback();

    {
        MDBX_txn* raw_read = nullptr;
        mdbxc::check_mdbx(mdbx_txn_begin(conn->env_handle(), nullptr,
                                         MDBX_TXN_RDONLY, &raw_read),
                          "logical adapter rollback read txn begin failed");
        struct ReadGuard {
            MDBX_txn* txn;
            ~ReadGuard() { if (txn != nullptr) mdbx_txn_abort(txn); }
        } read_guard = { raw_read };

        MDBX_dbi dbi = 0;
        const int open_rc = mdbx_dbi_open(
            raw_read, "logical_adapter_rollback_items",
            static_cast<MDBX_db_flags_t>(0), &dbi);
        if (open_rc != MDBX_NOTFOUND) {
            mdbxc::check_mdbx(open_rc,
                              "logical adapter rollback DBI open failed");
            const int get_rc = mdbx_get(raw_read, dbi, &key, &value);
            if (get_rc != MDBX_NOTFOUND) {
                mdbxc::check_mdbx(get_rc,
                                  "logical adapter rollback read failed");
                throw std::runtime_error(
                    "logical adapter partial MDBX mutation survived rollback");
            }
        }
    }

    conn->disconnect();
    cleanup(path);
}

} // namespace

int main() {
    test_registry_rejects_invalid_adapters();
    test_registry_rejects_duplicate_schema_id();
    test_preflight_then_apply_order();
    test_preflight_failure_blocks_apply();
    test_schema_mismatch_blocks_adapter_calls();
    test_schema_version_mismatch_blocks_adapter_calls();
    test_reserved_flags_block_adapter_calls();
    test_late_missing_adapter_blocks_all_preflight();
    test_late_schema_mismatch_blocks_all_preflight();
    test_late_schema_version_mismatch_blocks_all_preflight();
    test_late_reserved_flags_block_all_preflight();
    test_apply_failure_returns_failure_after_prior_apply();
    test_apply_exception_returns_failure_after_prior_apply();
    test_missing_adapter_fails_without_apply();
    test_mdbx_mutation_rolls_back_when_later_apply_fails();
    return 0;
}
