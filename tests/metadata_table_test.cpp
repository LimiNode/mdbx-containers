#include "test_assert.hpp"

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx_containers/KeyValueTable.hpp>
#include <mdbx_containers/MetadataTable.hpp>

namespace {

    mdbxc::Config make_config(const std::string& pathname) {
        mdbxc::Config cfg;
        cfg.pathname = pathname;
        cfg.max_dbs = 8;
        cfg.no_subdir = true;
        cfg.relative_to_exe = false;
        return cfg;
    }

} // namespace

int main() {
    const mdbxc::Config cfg = make_config("data/metadata_table_test.mdbx");
    std::shared_ptr<mdbxc::Connection> conn = mdbxc::Connection::create(cfg);
    mdbxc::MetadataTable metadata(conn, "collection_metadata");

    // --- 1. typed_values_and_safe_defaults ---
    {
        metadata.set_string("embedding_model", "text-embedding-3-small");
        metadata.set_uint32("dim", 1536u);
        metadata.set_uint64("document_count", 42u);
        metadata.set_int64("created_at_ms", -1000);
        metadata.set_double("score_threshold", 0.75);
        metadata.set_bool("normalized", true);

        MDBXC_TEST_ASSERT(
            metadata.get_string("embedding_model") == "text-embedding-3-small");
        MDBXC_TEST_ASSERT(metadata.get_uint32("dim") == 1536u);
        MDBXC_TEST_ASSERT(metadata.get_uint64("document_count") == 42u);
        MDBXC_TEST_ASSERT(metadata.get_int64("created_at_ms") == -1000);
        MDBXC_TEST_ASSERT(metadata.get_double("score_threshold") == 0.75);
        MDBXC_TEST_ASSERT(metadata.get_bool("normalized"));
        MDBXC_TEST_ASSERT(metadata.get_uint32_or("missing", 7u) == 7u);
        MDBXC_TEST_ASSERT(metadata.get_string_or("missing_text", "fallback") ==
                          "fallback");
    }

    // --- 2. schema_version_and_type_mismatch_fail_closed ---
    {
        metadata.set_schema_version(2u);
        MDBXC_TEST_ASSERT(metadata.schema_version() == 2u);
        MDBXC_TEST_ASSERT(metadata.schema_version_or(1u) == 2u);

        bool mismatch_threw = false;
        try {
            (void)metadata.get_string("dim");
        } catch (const std::invalid_argument&) {
            mismatch_threw = true;
        }
        MDBXC_TEST_ASSERT(mismatch_threw);

        bool missing_threw = false;
        try {
            (void)metadata.get_bool("missing_bool");
        } catch (const std::out_of_range&) {
            missing_threw = true;
        }
        MDBXC_TEST_ASSERT(missing_threw);
    }

    // --- 3. external_transaction_and_rollback ---
    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        metadata.set_string("state", "pending", txn);
        metadata.set_uint64("document_count", 99u, txn);
        MDBXC_TEST_ASSERT(metadata.get_string("state", txn) == "pending");
        MDBXC_TEST_ASSERT(metadata.get_uint64("document_count", txn) == 99u);
        txn.rollback();

        MDBXC_TEST_ASSERT(metadata.get_string_or("state", "absent") == "absent");
        MDBXC_TEST_ASSERT(metadata.get_uint64("document_count") == 42u);
    }

    // --- 4. malformed_bool_payload_fails_closed ---
    {
        mdbxc::KeyValueTable<std::string, std::vector<std::uint8_t>> raw_metadata(
            conn, "collection_metadata");
        raw_metadata.insert_or_assign(
            "malformed_bool", std::vector<std::uint8_t>{6u, 2u});

        bool malformed_threw = false;
        try {
            (void)metadata.get_bool("malformed_bool");
        } catch (const std::runtime_error&) {
            malformed_threw = true;
        }
        MDBXC_TEST_ASSERT(malformed_threw);
    }

    // --- 5. erase_and_restart_persistence ---
    {
        MDBXC_TEST_ASSERT(metadata.erase("normalized"));
        MDBXC_TEST_ASSERT(!metadata.erase("normalized"));
        conn->disconnect();
        conn->connect();

        mdbxc::MetadataTable reopened(conn, "collection_metadata");
        MDBXC_TEST_ASSERT(reopened.get_uint32("dim") == 1536u);
        MDBXC_TEST_ASSERT(reopened.schema_version() == 2u);
        MDBXC_TEST_ASSERT(!reopened.get_bool_or("normalized", false));
    }

    // --- 6. foreign_transaction_is_rejected_before_dbi_use ---
    {
        std::shared_ptr<mdbxc::Connection> foreign_connection =
            mdbxc::Connection::create(
                make_config("data/metadata_table_foreign_test.mdbx"));
        auto foreign_txn = foreign_connection->transaction(
            mdbxc::TransactionMode::WRITABLE);
        bool foreign_threw = false;
        try {
            metadata.set_uint32("foreign", 1u, foreign_txn.handle());
        } catch (const std::invalid_argument&) {
            foreign_threw = true;
        }
        MDBXC_TEST_ASSERT(foreign_threw);
        foreign_txn.rollback();
    }

    std::cout << "MetadataTable test passed.\n";
    return 0;
}
