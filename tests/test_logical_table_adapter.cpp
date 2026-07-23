#include <mdbx_containers/sync.hpp>

#include <stdexcept>
#include <string>
#include <vector>

namespace {

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
    return 0;
}
