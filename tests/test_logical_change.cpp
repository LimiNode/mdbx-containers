#include <mdbx_containers/sync.hpp>

#include <cstdint>
#include <stdexcept>
#include <vector>

void test_logical_schema_ref_defaults() {
    mdbxc::sync::LogicalSchemaRef ref;
    if (mdbxc::sync::is_logical_schema_ref_complete(ref)) {
        throw std::runtime_error("default LogicalSchemaRef must be incomplete");
    }
}

void test_logical_schema_ref_complete() {
    mdbxc::sync::LogicalSchemaRef ref;
    ref.schema_id = "app.timeline.v1";
    ref.kind = mdbxc::sync::LogicalTableKind::KeyOrderedMultiValue;
    ref.schema_version = 1;

    if (!mdbxc::sync::is_logical_schema_ref_complete(ref)) {
        throw std::runtime_error("filled LogicalSchemaRef must be complete");
    }
}

void test_logical_schema_ref_positional_construction() {
    const mdbxc::sync::LogicalSchemaRef ref(
        "app.timeline.v1",
        mdbxc::sync::LogicalTableKind::KeyOrderedMultiValue,
        1);

    if (ref.schema_id != "app.timeline.v1" ||
        ref.kind != mdbxc::sync::LogicalTableKind::KeyOrderedMultiValue ||
        ref.schema_version != 1u ||
        !mdbxc::sync::is_logical_schema_ref_complete(ref)) {
        throw std::runtime_error(
            "LogicalSchemaRef positional construction mismatch");
    }
}

void test_logical_schema_ref_rejects_unknown_kind() {
    mdbxc::sync::LogicalSchemaRef ref;
    ref.schema_id = "app.unknown.v1";
    ref.kind = static_cast<mdbxc::sync::LogicalTableKind>(0xffffu);
    ref.schema_version = 1;

    if (mdbxc::sync::is_logical_schema_ref_complete(ref)) {
        throw std::runtime_error("unknown LogicalTableKind must be incomplete");
    }
}

void test_logical_change_payload_is_opaque() {
    mdbxc::sync::LogicalChange change;
    change.schema.schema_id = "app.timeline.v1";
    change.schema.kind = mdbxc::sync::LogicalTableKind::KeyOrderedMultiValue;
    change.schema.schema_version = 1;
    change.opcode = 7;
    change.payload.push_back(0xA1);
    change.payload.push_back(0xB2);

    if (change.opcode != 7 ||
        change.payload.size() != 2u ||
        change.payload[0] != 0xA1 ||
        change.payload[1] != 0xB2) {
        throw std::runtime_error("LogicalChange payload mismatch");
    }
}

void test_logical_change_positional_construction() {
    const mdbxc::sync::LogicalSchemaRef ref(
        "app.timeline.v1",
        mdbxc::sync::LogicalTableKind::KeyOrderedMultiValue,
        1);
    std::vector<std::uint8_t> payload;
    payload.push_back(0xA1);
    payload.push_back(0xB2);

    const mdbxc::sync::LogicalChange change(ref, 7, 0, payload);

    if (change.schema.schema_id != "app.timeline.v1" ||
        change.schema.kind !=
            mdbxc::sync::LogicalTableKind::KeyOrderedMultiValue ||
        change.schema.schema_version != 1u ||
        change.opcode != 7u ||
        change.flags != 0u ||
        change.payload != payload) {
        throw std::runtime_error(
            "LogicalChange positional construction mismatch");
    }
}

int main() {
    test_logical_schema_ref_defaults();
    test_logical_schema_ref_complete();
    test_logical_schema_ref_positional_construction();
    test_logical_schema_ref_rejects_unknown_kind();
    test_logical_change_payload_is_opaque();
    test_logical_change_positional_construction();
    return 0;
}
