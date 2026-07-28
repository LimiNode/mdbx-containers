/**
 * \ingroup mdbxc_examples
 * \brief Explicit KeyValueTable logical frame replication.
 *
 * This example demonstrates the current opt-in logical sync path:
 *   - source writes through KeyValueTableLogicalAdapter::LogicalCaptureSession;
 *   - captured logical changes are encoded with LogicalChangeFrameCodec;
 *   - replica decodes and applies the frame with SyncEngine.
 *
 * The normal pull/push transport DTOs remain raw-DBI only. Applications must
 * opt into this logical frame path explicitly until transport capability
 * negotiation is added.
 *
 * LogicalChangeFrame is only the payload container used here for a local
 * handoff. It does not provide destination routing, delivery ordering, or
 * replay protection for retrying transports; those belong to a separate
 * delivery envelope or caller-owned protocol.
 *
 * Expected output:
 *   [source] captured 3 logical change(s)
 *   [wire] logical frame bytes=...
 *   [replica] product 101=keyboard product 102 visible=no
 *   OK: sync_23_key_value_logical_frame
 */

#include "sync_example_utils.hpp"

#include <cstdio>
#include <exception>
#include <memory>
#include <string>
#include <vector>

namespace {

typedef mdbxc::sync::KeyValueLogicalInt32Codec<int> IntKeyCodec;
typedef mdbxc::sync::KeyValueLogicalStringCodec<std::string> StringValueCodec;
typedef mdbxc::sync::KeyValueTableLogicalAdapter<
    int, std::string, IntKeyCodec, StringValueCodec> ProductAdapter;

} // namespace

int main() {
    const std::string source_path = "sync_23_source.mdbx";
    const std::string replica_path = "sync_23_replica.mdbx";
    const std::string dbi_name = "products";
    const std::string schema_id = "app.products.kv.v1";

    sync_example::cleanup(source_path);
    sync_example::cleanup(replica_path);

    std::shared_ptr<mdbxc::Connection> source_conn;
    std::shared_ptr<mdbxc::Connection> replica_conn;

    try {
        const mdbxc::sync::NodeId source_node =
            sync_example::make_node(0x41);
        const mdbxc::sync::NodeId replica_node =
            sync_example::make_node(0x42);
        const mdbxc::sync::NodeId db_id =
            sync_example::make_node(0xD3);

        source_conn = sync_example::open(source_path);
        replica_conn = sync_example::open(replica_path);

        mdbxc::sync::SyncEngine source_engine(source_conn);
        mdbxc::sync::SyncEngine replica_engine(replica_conn);
        source_engine.initialize_local_identity(source_node, db_id);
        replica_engine.initialize_local_identity(replica_node, db_id);

        mdbxc::sync::LogicalSchemaRecord record;
        record.dbi_name = dbi_name;
        record.kind = mdbxc::sync::LogicalTableKind::KeyValue;
        record.schema_version = 1;
        record.dbi_names.push_back(dbi_name);
        source_engine.register_logical_schema(schema_id, record);
        replica_engine.register_logical_schema(schema_id, record);

        mdbxc::KeyValueTable<int, std::string> source_products(
            source_conn, dbi_name);
        mdbxc::KeyValueTable<int, std::string> replica_products(
            replica_conn, dbi_name);
        ProductAdapter source_adapter(source_products, schema_id);
        ProductAdapter replica_adapter(replica_products, schema_id);
        replica_engine.register_logical_adapter(replica_adapter);

        std::vector<mdbxc::sync::LogicalChange> captured;
        {
            std::unique_ptr<ProductAdapter::LogicalCaptureSession> session =
                source_adapter.begin_capture_session();
            session->insert_or_assign(101, "keyboard");
            session->insert_or_assign(102, "mouse");
            (void)session->erase(102);
            session->commit(captured);
        }

        std::printf("[source] captured %zu logical change(s)\n",
                    captured.size());

        mdbxc::sync::LogicalChangeFrame frame;
        frame.changes = captured;
        const std::vector<std::uint8_t> wire =
            mdbxc::sync::LogicalChangeFrameCodec::encode(frame);
        std::printf("[wire] logical frame bytes=%zu\n", wire.size());

        const mdbxc::sync::LogicalApplyResult applied =
            replica_engine.apply_logical_frame_bytes(wire);
        sync_example::require(applied.ok, applied.error);

        const std::string product = sync_example::kv_or_throw(
            replica_conn, replica_products, 101, "replica product 101");
        const bool second_visible =
            sync_example::kv_has(replica_conn, replica_products, 102);
        sync_example::require(product == "keyboard",
                              "replica product 101 mismatch");
        sync_example::require(!second_visible,
                              "deleted product 102 replicated as visible");

        std::printf("[replica] product 101=%s product 102 visible=%s\n",
                    product.c_str(), second_visible ? "yes" : "no");

        sync_example::disconnect_and_cleanup(source_conn, source_path);
        sync_example::disconnect_and_cleanup(replica_conn, replica_path);
        std::puts("OK: sync_23_key_value_logical_frame");
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr,
                     "sync_23_key_value_logical_frame failed: %s\n",
                     e.what());
    }

    sync_example::disconnect_and_cleanup(source_conn, source_path);
    sync_example::disconnect_and_cleanup(replica_conn, replica_path);
    return 1;
}
