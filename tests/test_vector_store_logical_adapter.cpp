#include "test_assert.hpp"

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx_containers/sync.hpp>
#include <mdbx_containers/vector.hpp>

static mdbxc::Embedding make_vector(float x, float y) {
    mdbxc::Embedding embedding;
    embedding.dim = 2u;
    embedding.values.push_back(x);
    embedding.values.push_back(y);
    return embedding;
}

static mdbxc::Embedding make_three_dimensional_vector() {
    mdbxc::Embedding embedding;
    embedding.dim = 3u;
    embedding.values.push_back(1.0f);
    embedding.values.push_back(0.0f);
    embedding.values.push_back(0.0f);
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

static void drop_named_dbi(const std::shared_ptr<mdbxc::Connection>& connection,
                           const std::string& name) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::WRITABLE);
    MDBX_dbi dbi = 0;
    mdbxc::check_mdbx(mdbx_dbi_open(
        transaction.handle(), name.c_str(), static_cast<MDBX_db_flags_t>(0), &dbi),
                      "VectorStore test DBI open failed");
    mdbxc::check_mdbx(mdbx_drop(transaction.handle(), dbi, true),
                      "VectorStore test DBI drop failed");
    transaction.commit();
}

static bool named_dbi_exists(const std::shared_ptr<mdbxc::Connection>& connection,
                             const std::string& name) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::READ_ONLY);
    MDBX_dbi dbi = 0;
    const int rc = mdbx_dbi_open(
        transaction.handle(), name.c_str(), static_cast<MDBX_db_flags_t>(0), &dbi);
    transaction.rollback();
    if (rc == MDBX_NOTFOUND) return false;
    mdbxc::check_mdbx(rc, "VectorStore test DBI existence check failed");
    return true;
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

    bool unsupported_schema_version_threw = false;
    try {
        mdbxc::sync::VectorStoreLogicalAdapter unsupported_adapter(
            source, "schema-v2-rejected", 2u);
    } catch (const std::invalid_argument&) {
        unsupported_schema_version_threw = true;
    }
    MDBXC_TEST_ASSERT(unsupported_schema_version_threw);

    mdbxc::sync::SyncEngine source_engine(source_connection);
    mdbxc::sync::SyncEngine replica_engine(replica_connection);
    mdbxc::sync::VectorStoreLogicalAdapter source_adapter(source);
    mdbxc::sync::VectorStoreLogicalAdapter replica_adapter(replica);
    initialize_adapter(source_engine, source_adapter, make_id(1u), db_uuid);
    initialize_adapter(replica_engine, replica_adapter, make_id(2u), db_uuid);

    std::vector<mdbxc::sync::LogicalChange> changes;
    std::uint64_t first = 0u;
    std::uint64_t second = 0u;
    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        first = session->add(
            make_vector(1.0f, 0.0f), "first", "{\"rank\":1}");
        second = session->add(
            make_vector(0.0f, 1.0f), "second", "{\"rank\":2}");
        MDBXC_TEST_ASSERT(second == first + 1u);
        session->commit(changes);
    }
    MDBXC_TEST_ASSERT(changes.size() == 2u);
    MDBXC_TEST_ASSERT(source.count() == 2u);
    mdbxc::sync::LogicalChangeFrame frame;
    frame.changes = changes;
    const std::vector<std::uint8_t> encoded_frame =
        mdbxc::sync::LogicalChangeFrameCodec::encode(frame);
    const mdbxc::sync::LogicalChangeFrame decoded_frame =
        mdbxc::sync::LogicalChangeFrameCodec::decode(encoded_frame);
    MDBXC_TEST_ASSERT(decoded_frame.changes.size() == changes.size());
    MDBXC_TEST_ASSERT(decoded_frame.changes[0].schema.kind ==
                      mdbxc::sync::LogicalTableKind::VectorStore);
    MDBXC_TEST_ASSERT(replica_engine.apply_logical_changes(changes).ok);
    MDBXC_TEST_ASSERT(replica.count() == 2u);
    std::vector<mdbxc::SearchResult> replica_results =
        replica.search(make_vector(1.0f, 0.0f), 1u);
    MDBXC_TEST_ASSERT(replica_results.size() == 1u);
    MDBXC_TEST_ASSERT(replica_results[0].text == "first");
    MDBXC_TEST_ASSERT(replica_results[0].metadata_json == "{\"rank\":1}");

    const std::uint64_t replica_local_id = replica.add(
        make_vector(1.0f, 1.0f), "replica-local", "{}");
    MDBXC_TEST_ASSERT(replica_local_id == second + 1u);
    MDBXC_TEST_ASSERT(replica.erase(replica_local_id));

    mdbxc::sync::LogicalChange malformed =
        source_adapter.make_add(100u, make_vector(1.0f, 1.0f), "bad", "{}");
    malformed.payload.push_back(0xffu);
    std::vector<mdbxc::sync::LogicalChange> malformed_frame(1u, malformed);
    MDBXC_TEST_ASSERT(!replica_engine.apply_logical_changes(malformed_frame).ok);
    MDBXC_TEST_ASSERT(replica.count() == 2u);

    const mdbxc::sync::LogicalChange wrong_dimension_add =
        source_adapter.make_add(
            100u, make_three_dimensional_vector(), "wrong-dimension", "{}");
    std::vector<mdbxc::sync::LogicalChange> wrong_dimension_frame(
        1u, wrong_dimension_add);
    MDBXC_TEST_ASSERT(!replica_engine.apply_logical_changes(
        wrong_dimension_frame).ok);
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
    MDBXC_TEST_ASSERT(replica_engine.apply_logical_changes(erase_changes).ok);
    MDBXC_TEST_ASSERT(replica.count() == 1u);

    const std::uint64_t source_local_id = source.add(
        make_vector(1.0f, 1.0f), "source-local", "{}");
    MDBXC_TEST_ASSERT(source_local_id > second);
    MDBXC_TEST_ASSERT(source.erase(source_local_id));

    std::vector<mdbxc::sync::LogicalChange> clear_changes;
    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        session->clear();
        session->add(make_three_dimensional_vector(), "after clear", "{}");
        session->commit(clear_changes);
    }
    MDBXC_TEST_ASSERT(clear_changes.size() == 2u);
    MDBXC_TEST_ASSERT(replica_engine.apply_logical_changes(clear_changes).ok);
    MDBXC_TEST_ASSERT(source.count() == 1u);
    MDBXC_TEST_ASSERT(replica.count() == 1u);
    replica_results = replica.search(make_three_dimensional_vector(), 1u);
    MDBXC_TEST_ASSERT(replica_results.size() == 1u);
    MDBXC_TEST_ASSERT(replica_results[0].text == "after clear");

    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        session->add(make_three_dimensional_vector(), "rollback", "{}");
        const mdbxc::Embedding wrong_dimension = make_vector(2.0f, 0.0f);
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
    MDBXC_TEST_ASSERT(source.count() == 1u);

    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session =
            source_adapter.begin_capture_session();
        session->add(make_three_dimensional_vector(), "after rollback", "{}");
        std::vector<mdbxc::sync::LogicalChange> after_rollback;
        session->commit(after_rollback);
        MDBXC_TEST_ASSERT(after_rollback.size() == 1u);
    }
    MDBXC_TEST_ASSERT(source.count() == 2u);

    auto erase_source_connection = mdbxc::Connection::create(
        make_config("data/vector_store_logical_erase_source.mdbx"));
    auto erase_replica_connection = mdbxc::Connection::create(
        make_config("data/vector_store_logical_erase_replica.mdbx"));
    mdbxc::VectorStore erase_source(erase_source_connection, "docs");
    mdbxc::VectorStore erase_replica(erase_replica_connection, "docs");
    erase_source.clear();
    erase_replica.clear();
    mdbxc::sync::SyncEngine erase_source_engine(erase_source_connection);
    mdbxc::sync::SyncEngine erase_replica_engine(erase_replica_connection);
    mdbxc::sync::VectorStoreLogicalAdapter erase_source_adapter(erase_source);
    mdbxc::sync::VectorStoreLogicalAdapter erase_replica_adapter(erase_replica);
    initialize_adapter(erase_source_engine, erase_source_adapter, make_id(3u), db_uuid);
    initialize_adapter(erase_replica_engine, erase_replica_adapter, make_id(4u), db_uuid);

    std::vector<mdbxc::sync::LogicalChange> erase_dimension_changes;
    std::uint64_t erase_dimension_id = 0u;
    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session =
            erase_source_adapter.begin_capture_session();
        erase_dimension_id = session->add(make_vector(1.0f, 0.0f), "two-d", "{}");
        session->commit(erase_dimension_changes);
    }
    MDBXC_TEST_ASSERT(erase_replica_engine.apply_logical_changes(
        erase_dimension_changes).ok);

    erase_dimension_changes.clear();
    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session =
            erase_source_adapter.begin_capture_session();
        MDBXC_TEST_ASSERT(session->erase(erase_dimension_id));
        session->add(make_three_dimensional_vector(), "three-d", "{}");
        session->commit(erase_dimension_changes);
    }
    MDBXC_TEST_ASSERT(erase_dimension_changes.size() == 2u);
    MDBXC_TEST_ASSERT(erase_replica_engine.apply_logical_changes(
        erase_dimension_changes).ok);
    MDBXC_TEST_ASSERT(erase_source.count() == 1u);
    MDBXC_TEST_ASSERT(erase_replica.count() == 1u);

    const std::string schema_open_path =
        "data/vector_store_logical_schema_open.mdbx";
    const std::string schema_open_id = "vector-store-schema-open";
    {
        auto schema_connection = mdbxc::Connection::create(make_config(schema_open_path));
        mdbxc::VectorStore schema_store(schema_connection, "schema_open");
        schema_store.clear();
        mdbxc::sync::SyncEngine schema_engine(schema_connection);
        mdbxc::sync::VectorStoreLogicalAdapter schema_adapter(
            schema_store, schema_open_id);
        initialize_adapter(schema_engine, schema_adapter, make_id(5u), db_uuid);
    }
    auto schema_connection = mdbxc::Connection::create(make_config(schema_open_path));
    {
        mdbxc::Transaction transaction =
            schema_connection->transaction(mdbxc::TransactionMode::READ_ONLY);
        mdbxc::sync::SchemaRegistryStore schemas(schema_connection->env_handle());
        mdbxc::sync::LogicalSchemaRecord marker;
        MDBXC_TEST_ASSERT(schemas.get(transaction.handle(), schema_open_id, marker));
        MDBXC_TEST_ASSERT(marker.kind == mdbxc::sync::LogicalTableKind::VectorStore);
        MDBXC_TEST_ASSERT(marker.schema_version == 1u);
        MDBXC_TEST_ASSERT(marker.flags == 0u);
        MDBXC_TEST_ASSERT(marker.dbi_name == "vectors_schema_open_ids");
        MDBXC_TEST_ASSERT(marker.dbi_names.size() == 4u);
        MDBXC_TEST_ASSERT(marker.dbi_names[0] == "vectors_schema_open_embeddings");
        MDBXC_TEST_ASSERT(marker.dbi_names[1] == "vectors_schema_open_ids");
        MDBXC_TEST_ASSERT(marker.dbi_names[2] == "vectors_schema_open_metadata");
        MDBXC_TEST_ASSERT(marker.dbi_names[3] == "vectors_schema_open_texts");
        transaction.rollback();
    }
    {
        std::unique_ptr<mdbxc::VectorStore> reopened =
            mdbxc::sync::VectorStoreLogicalAdapter::open_store_for_schema(
                schema_connection, schema_open_id, "schema_open");
        MDBXC_TEST_ASSERT(reopened->count() == 0u);
    }
    const std::string missing_embedding_dbi =
        "vectors_schema_open_embeddings";
    drop_named_dbi(schema_connection, missing_embedding_dbi);
    bool schema_open_failed = false;
    try {
        std::unique_ptr<mdbxc::VectorStore> reopened =
            mdbxc::sync::VectorStoreLogicalAdapter::open_store_for_schema(
                schema_connection, schema_open_id, "schema_open");
    } catch (const std::exception&) {
        schema_open_failed = true;
    }
    MDBXC_TEST_ASSERT(schema_open_failed);
    MDBXC_TEST_ASSERT(!named_dbi_exists(schema_connection, missing_embedding_dbi));
    mdbxc::VectorStore direct_store(schema_connection, "schema_open");
    MDBXC_TEST_ASSERT(named_dbi_exists(schema_connection, missing_embedding_dbi));

    auto corrupt_connection = mdbxc::Connection::create(
        make_config("data/vector_store_logical_corrupt_clear.mdbx"));
    mdbxc::VectorStore corrupt_store(corrupt_connection, "corrupt_clear");
    corrupt_store.clear();
    mdbxc::sync::SyncEngine corrupt_engine(corrupt_connection);
    mdbxc::sync::VectorStoreLogicalAdapter corrupt_adapter(corrupt_store);
    initialize_adapter(corrupt_engine, corrupt_adapter, make_id(6u), db_uuid);
    std::uint64_t corrupt_id = 0u;
    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session = corrupt_adapter.begin_capture_session();
        corrupt_id = session->add(make_vector(1.0f, 0.0f), "corrupt", "{}");
        std::vector<mdbxc::sync::LogicalChange> corrupt_seed;
        session->commit(corrupt_seed);
    }
    mdbxc::KeyValueTable<std::uint64_t, std::string> corrupt_texts(
        corrupt_connection, "vectors_corrupt_clear_texts");
    MDBXC_TEST_ASSERT(corrupt_texts.erase(corrupt_id));
    std::vector<mdbxc::sync::LogicalChange> corrupt_clear(
        1u, corrupt_adapter.make_clear());
    MDBXC_TEST_ASSERT(!corrupt_engine.apply_logical_changes(corrupt_clear).ok);
    MDBXC_TEST_ASSERT(corrupt_store.count() == 1u);
    {
        std::unique_ptr<mdbxc::sync::VectorStoreLogicalAdapter::
            LogicalCaptureSession> session = corrupt_adapter.begin_capture_session();
        bool local_clear_failed = false;
        try {
            session->clear();
        } catch (const std::runtime_error&) {
            local_clear_failed = true;
        }
        MDBXC_TEST_ASSERT(local_clear_failed);
        MDBXC_TEST_ASSERT(session->pending_size() == 0u);
        session->rollback();
    }
    MDBXC_TEST_ASSERT(corrupt_store.count() == 1u);

    return 0;
}
