#include "test_assert.hpp"

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx_containers/sync/engine/SyncEngine.hpp>
#include <mdbx_containers/sync/adapters.hpp>
#include <mdbx_containers/vector.hpp>

static mdbxc::Embedding make_vector(float x, float y) {
    mdbxc::Embedding embedding;
    embedding.dim = 2u;
    embedding.values.push_back(x);
    embedding.values.push_back(y);
    return embedding;
}

static mdbxc::Config make_config(const std::string& path) {
    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 32;
    config.no_subdir = true;
    config.relative_to_exe = false;
    return config;
}

static mdbxc::sync::NodeId make_id(std::uint8_t first) {
    mdbxc::sync::NodeId id = mdbxc::sync::make_zero_node();
    id[0] = first;
    return id;
}

static void initialize_adapter(
        mdbxc::sync::SyncEngine& engine,
        mdbxc::sync::VectorStoreLogicalAdapter& adapter,
        const mdbxc::sync::NodeId& node,
        const mdbxc::sync::NodeId& db_uuid) {
    engine.initialize_local_identity(node, db_uuid);
    engine.initialize_logical_adapter_schema(
        adapter, adapter.schema_record());
    engine.register_logical_adapter(adapter);
}

int main() {
    const mdbxc::sync::NodeId db_uuid = make_id(42u);
    auto source_connection = mdbxc::Connection::create(
        make_config("data/vector_store_logical_source.mdbx"));
    auto replica_connection = mdbxc::Connection::create(
        make_config("data/vector_store_logical_replica.mdbx"));
    mdbxc::VectorStore source(source_connection, "docs");
    mdbxc::VectorStore replica(replica_connection, "docs");
    source.clear();
    replica.clear();

    mdbxc::sync::SyncEngine source_engine(source_connection);
    mdbxc::sync::SyncEngine replica_engine(replica_connection);
    mdbxc::sync::VectorStoreLogicalAdapter source_adapter(source);
    mdbxc::sync::VectorStoreLogicalAdapter replica_adapter(replica);
    initialize_adapter(source_engine, source_adapter, make_id(1u), db_uuid);
    initialize_adapter(replica_engine, replica_adapter, make_id(2u), db_uuid);

    std::vector<mdbxc::sync::LogicalChange> changes;
    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        const std::uint64_t first = session->add(
            make_vector(1.0f, 0.0f), "first", "{\"rank\":1}");
        const std::uint64_t second = session->add(
            make_vector(0.0f, 1.0f), "second", "{\"rank\":2}");
        MDBXC_TEST_ASSERT(second == first + 1u);
        session->commit(changes);
    }
    MDBXC_TEST_ASSERT(changes.size() == 2u);
    MDBXC_TEST_ASSERT(source.count() == 2u);
    MDBXC_TEST_ASSERT(replica_engine.apply_logical_changes(changes).ok);
    MDBXC_TEST_ASSERT(replica.count() == 2u);
    std::vector<mdbxc::SearchResult> replica_results =
        replica.search(make_vector(1.0f, 0.0f), 1u);
    MDBXC_TEST_ASSERT(replica_results.size() == 1u);
    MDBXC_TEST_ASSERT(replica_results[0].text == "first");
    MDBXC_TEST_ASSERT(replica_results[0].metadata_json == "{\"rank\":1}");

    mdbxc::sync::LogicalChange malformed =
        source_adapter.make_add(100u, make_vector(1.0f, 1.0f), "bad", "{}");
    malformed.payload.push_back(0xffu);
    std::vector<mdbxc::sync::LogicalChange> malformed_frame(1u, malformed);
    MDBXC_TEST_ASSERT(!replica_engine.apply_logical_changes(malformed_frame).ok);
    MDBXC_TEST_ASSERT(replica.count() == 2u);

    mdbxc::sync::LogicalChange duplicate =
        source_adapter.make_add(1u, make_vector(1.0f, 1.0f),
                                "replacement", "{}");
    std::vector<mdbxc::sync::LogicalChange> duplicate_frame(1u, duplicate);
    MDBXC_TEST_ASSERT(!replica_engine.apply_logical_changes(duplicate_frame).ok);
    MDBXC_TEST_ASSERT(replica.count() == 2u);
    replica_results = replica.search(make_vector(1.0f, 0.0f), 1u);
    MDBXC_TEST_ASSERT(replica_results.size() == 1u);
    MDBXC_TEST_ASSERT(replica_results[0].text == "first");

    mdbxc::sync::LogicalChange wrong_schema =
        source_adapter.make_erase(2u);
    wrong_schema.schema.schema_version = 2u;
    std::vector<mdbxc::sync::LogicalChange> wrong_schema_frame(1u,
                                                                wrong_schema);
    MDBXC_TEST_ASSERT(!replica_engine.apply_logical_changes(
        wrong_schema_frame).ok);
    MDBXC_TEST_ASSERT(replica.count() == 2u);

    std::vector<mdbxc::sync::LogicalChange> erase_changes;
    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        MDBXC_TEST_ASSERT(session->erase(1u));
        session->commit(erase_changes);
    }
    MDBXC_TEST_ASSERT(erase_changes.size() == 1u);
    MDBXC_TEST_ASSERT(replica_engine.apply_logical_changes(erase_changes).ok);
    MDBXC_TEST_ASSERT(replica.count() == 1u);

    std::vector<mdbxc::sync::LogicalChange> clear_changes;
    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        session->clear();
        session->commit(clear_changes);
    }
    MDBXC_TEST_ASSERT(clear_changes.size() == 1u);
    MDBXC_TEST_ASSERT(replica_engine.apply_logical_changes(clear_changes).ok);
    MDBXC_TEST_ASSERT(source.count() == 0u);
    MDBXC_TEST_ASSERT(replica.count() == 0u);

    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        session->add(make_vector(2.0f, 0.0f), "rollback", "{}");
        mdbxc::Embedding wrong_dimension;
        wrong_dimension.dim = 3u;
        wrong_dimension.values.push_back(3.0f);
        wrong_dimension.values.push_back(0.0f);
        wrong_dimension.values.push_back(0.0f);
        bool dimension_failed = false;
        try {
            session->add(wrong_dimension, "wrong-dimension", "{}");
        } catch (const std::invalid_argument&) {
            dimension_failed = true;
        }
        MDBXC_TEST_ASSERT(dimension_failed);
        MDBXC_TEST_ASSERT(session->pending_size() == 1u);
        session->rollback();
    }
    MDBXC_TEST_ASSERT(source.count() == 0u);

    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        session->add(make_vector(1.0f, 0.0f), "after rollback", "{}");
        std::vector<mdbxc::sync::LogicalChange> after_rollback;
        session->commit(after_rollback);
        MDBXC_TEST_ASSERT(after_rollback.size() == 1u);
    }
    MDBXC_TEST_ASSERT(source.count() == 1u);

    return 0;
}
