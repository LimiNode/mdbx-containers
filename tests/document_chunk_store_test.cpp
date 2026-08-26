#include "test_assert.hpp"

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx_containers/ChunkStore.hpp>
#include <mdbx_containers/DocumentStore.hpp>

namespace {
    mdbxc::Config make_config(const std::string& pathname) {
        mdbxc::Config cfg;
        cfg.pathname = pathname;
        cfg.max_dbs = 16;
        cfg.no_subdir = true;
        cfg.relative_to_exe = false;
        return cfg;
    }

    mdbxc::Document make_document(const std::string& uri, const std::string& title) {
        mdbxc::Document value;
        value.source_uri = uri;
        value.title = title;
        value.source_type = "markdown";
        value.created_at_ms = 100;
        value.indexed_at_ms = 200;
        value.metadata_json = "{\"lang\":\"en\"}";
        return value;
    }

    mdbxc::Chunk make_chunk(std::uint64_t document_id, std::uint32_t index,
                            std::uint32_t start, std::uint32_t end,
                            const std::string& text) {
        mdbxc::Chunk value;
        value.document_id = document_id;
        value.chunk_index = index;
        value.char_start = start;
        value.char_end = end;
        value.text = text;
        value.metadata_json = "{}";
        return value;
    }
}

int main() {
    const mdbxc::Config cfg = make_config("data/document_chunk_store_test.mdbx");
    const std::shared_ptr<mdbxc::Connection> conn = mdbxc::Connection::create(cfg);
    mdbxc::DocumentStore documents(conn, "documents");
    mdbxc::ChunkStore chunks(conn, "chunks");

    // --- 1. document_ids_and_source_uri_uniqueness ---
    const std::uint64_t first_id = documents.add(make_document("file:///guide.md", "Guide"));
    const std::uint64_t second_id = documents.add(make_document("file:///faq.md", "FAQ"));
    MDBXC_TEST_ASSERT(first_id == 1u);
    MDBXC_TEST_ASSERT(second_id == 2u);
    MDBXC_TEST_ASSERT(documents.get(first_id).title == "Guide");
    MDBXC_TEST_ASSERT(documents.get_by_source_uri("file:///faq.md").id == second_id);
    bool duplicate_threw = false;
    try {
        (void)documents.add(make_document("file:///guide.md", "Duplicate"));
    } catch (const std::invalid_argument&) {
        duplicate_threw = true;
    }
    MDBXC_TEST_ASSERT(duplicate_threw);
    MDBXC_TEST_ASSERT(documents.count() == 2u);

    // --- 2. chunk_order_provenance_and_duplicate_index_rejection ---
    const std::uint64_t later_chunk = chunks.add(make_chunk(first_id, 2u, 20u, 30u, "later"));
    const std::uint64_t first_chunk = chunks.add(make_chunk(first_id, 0u, 0u, 10u, "first"));
    std::vector<mdbxc::Chunk> ordered = chunks.by_document(first_id);
    MDBXC_TEST_ASSERT(ordered.size() == 2u);
    MDBXC_TEST_ASSERT(ordered[0].id == first_chunk);
    MDBXC_TEST_ASSERT(ordered[0].chunk_index == 0u);
    MDBXC_TEST_ASSERT(ordered[0].char_start == 0u && ordered[0].char_end == 10u);
    MDBXC_TEST_ASSERT(ordered[1].id == later_chunk);
    MDBXC_TEST_ASSERT(ordered[1].text == "later");
    bool duplicate_index_threw = false;
    try {
        (void)chunks.add(make_chunk(first_id, 2u, 31u, 40u, "duplicate"));
    } catch (const std::invalid_argument&) {
        duplicate_index_threw = true;
    }
    MDBXC_TEST_ASSERT(duplicate_index_threw);
    MDBXC_TEST_ASSERT(chunks.count() == 2u);

    // --- 3. external_transaction_is_atomic_across_stores ---
    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        const std::uint64_t transient_id =
            documents.add(make_document("file:///transient.md", "Transient"), txn);
        (void)chunks.add(make_chunk(transient_id, 0u, 0u, 9u, "transient"), txn);
        txn.rollback();
    }
    MDBXC_TEST_ASSERT(!documents.find_by_source_uri_compat("file:///transient.md").first);
    MDBXC_TEST_ASSERT(chunks.count() == 2u);

    // --- 4. document_and_chunk_deletion_can_share_one_transaction ---
    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBXC_TEST_ASSERT(chunks.erase_document(first_id, txn) == 2u);
        MDBXC_TEST_ASSERT(documents.erase(first_id, txn));
        txn.commit();
    }
    MDBXC_TEST_ASSERT(!documents.find_compat(first_id).first);
    MDBXC_TEST_ASSERT(chunks.by_document(first_id).empty());
    MDBXC_TEST_ASSERT(documents.count() == 1u);

    // --- 5. restart_persistence ---
    conn->disconnect();
    conn->connect();
    {
        mdbxc::DocumentStore reopened_documents(conn, "documents");
        mdbxc::ChunkStore reopened_chunks(conn, "chunks");
        const mdbxc::Document reopened = reopened_documents.get(second_id);
        MDBXC_TEST_ASSERT(reopened.source_type == "markdown");
        MDBXC_TEST_ASSERT(reopened.metadata_json == "{\"lang\":\"en\"}");
        MDBXC_TEST_ASSERT(reopened_chunks.count() == 0u);
    }

    std::cout << "Document and Chunk store test passed.\n";
    return 0;
}
