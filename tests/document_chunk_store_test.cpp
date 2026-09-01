#include "test_assert.hpp"

#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx_containers/ChunkStore.hpp>
#include <mdbx_containers/KeyMultiValueTable.hpp>
#include <mdbx_containers/DocumentStore.hpp>
#include <mdbx_containers/KeyValueTable.hpp>
#include <mdbx_containers/sync.hpp>

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

    class CountingCapture final : public mdbxc::sync::ISyncCaptureSink {
    public:
        void record_change(MDBX_txn*,
                           const std::string&,
                           mdbxc::sync::ChangeOpType,
                           std::uint32_t,
                           const std::vector<std::uint8_t>&,
                           const std::vector<std::uint8_t>&) override {
            ++record_count;
        }

        void flush_in_txn(MDBX_txn*) override {
            ++flush_count;
        }

        std::size_t record_count = 0u;
        std::size_t flush_count = 0u;
    };
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

    // --- 4. corrupt_document_index_rejects_deletion_before_mutation ---
    {
        const std::uint64_t foreign_chunk =
            chunks.add(make_chunk(second_id, 0u, 0u, 8u, "foreign"));
        mdbxc::KeyMultiValueTable<std::uint64_t, std::uint64_t> document_index(
            conn, "chunks_chunk_document_index");
        document_index.insert(first_id, foreign_chunk);

        bool by_document_threw = false;
        try {
            (void)chunks.by_document(first_id);
        } catch (const std::runtime_error&) {
            by_document_threw = true;
        }
        MDBXC_TEST_ASSERT(by_document_threw);

        bool erase_threw = false;
        try {
            (void)chunks.erase_document(first_id);
        } catch (const std::runtime_error&) {
            erase_threw = true;
        }
        MDBXC_TEST_ASSERT(erase_threw);
        MDBXC_TEST_ASSERT(chunks.count() == 3u);
        MDBXC_TEST_ASSERT(chunks.get(foreign_chunk).document_id == second_id);
        MDBXC_TEST_ASSERT(document_index.find(first_id).size() == 3u);
        MDBXC_TEST_ASSERT(document_index.erase(first_id, foreign_chunk) == 1u);

        document_index.insert(first_id, first_chunk);
        bool duplicate_erase_threw = false;
        {
            auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
            try {
                (void)chunks.erase_document(first_id, txn);
            } catch (const std::runtime_error&) {
                duplicate_erase_threw = true;
            }
            txn.commit();
        }
        MDBXC_TEST_ASSERT(duplicate_erase_threw);
        MDBXC_TEST_ASSERT(chunks.count() == 3u);
        MDBXC_TEST_ASSERT(document_index.erase(first_id, first_chunk) == 2u);
        document_index.insert(first_id, first_chunk);
    }

    // --- 5. document_and_chunk_deletion_can_share_one_transaction ---
    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBXC_TEST_ASSERT(chunks.erase_document(first_id, txn) == 2u);
        MDBXC_TEST_ASSERT(documents.erase(first_id, txn));
        txn.commit();
    }
    MDBXC_TEST_ASSERT(!documents.find_compat(first_id).first);
    MDBXC_TEST_ASSERT(chunks.by_document(first_id).empty());
    MDBXC_TEST_ASSERT(documents.count() == 1u);

    // --- 6. shared_prefix_uses_independent_dbi_names ---
    {
        mdbxc::DocumentStore shared_documents(conn, "rag");
        mdbxc::ChunkStore shared_chunks(conn, "rag");
        const std::uint64_t document_id =
            shared_documents.add(make_document("file:///rag.md", "RAG"));
        const std::uint64_t chunk_id =
            shared_chunks.add(make_chunk(document_id, 0u, 0u, 3u, "rag"));
        MDBXC_TEST_ASSERT(document_id == 1u);
        MDBXC_TEST_ASSERT(chunk_id == 1u);
        MDBXC_TEST_ASSERT(shared_documents.count() == 1u);
        MDBXC_TEST_ASSERT(shared_chunks.count() == 1u);
    }

    // --- 7. document_timestamp_codec_preserves_int64_boundaries ---
    {
        const std::int64_t timestamps[] = {
            -1,
            (std::numeric_limits<std::int64_t>::min)(),
            0,
            (std::numeric_limits<std::int64_t>::max)()
        };
        for (std::size_t i = 0u; i != sizeof(timestamps) / sizeof(timestamps[0]); ++i) {
            mdbxc::Document document = make_document("file:///timestamp.md", "Timestamp");
            document.id = static_cast<std::uint64_t>(i + 1u);
            document.created_at_ms = timestamps[i];
            document.indexed_at_ms = timestamps[i];
            const mdbxc::Document decoded = mdbxc::detail::decode_document(
                mdbxc::detail::encode_document(document));
            MDBXC_TEST_ASSERT(decoded.created_at_ms == timestamps[i]);
            MDBXC_TEST_ASSERT(decoded.indexed_at_ms == timestamps[i]);
        }
    }

    // --- 8. manual_connection_transaction_is_shared_by_both_stores ---
    std::uint64_t manual_document_id = 0u;
    std::uint64_t manual_chunk_id = 0u;
    conn->begin(mdbxc::TransactionMode::WRITABLE);
    manual_document_id =
        documents.add(make_document("file:///manual.md", "Manual"));
    manual_chunk_id =
        chunks.add(make_chunk(manual_document_id, 0u, 0u, 6u, "manual"));
    conn->commit();
    MDBXC_TEST_ASSERT(documents.get(manual_document_id).title == "Manual");
    MDBXC_TEST_ASSERT(chunks.get(manual_chunk_id).document_id == manual_document_id);

    conn->begin(mdbxc::TransactionMode::WRITABLE);
    const std::uint64_t rolled_back_document_id =
        documents.add(make_document("file:///manual-rollback.md", "Rollback"));
    const std::uint64_t rolled_back_chunk_id =
        chunks.add(make_chunk(rolled_back_document_id, 0u, 0u, 8u, "rollback"));
    conn->rollback();
    MDBXC_TEST_ASSERT(!documents.find_compat(rolled_back_document_id).first);
    MDBXC_TEST_ASSERT(!chunks.find_compat(rolled_back_chunk_id).first);

    // --- 9. source_uri_index_mismatch_fails_closed_before_erase ---
    {
        mdbxc::KeyValueTable<std::string, std::uint64_t> source_index(
            conn, "documents_document_source_uris");
        source_index.insert_or_assign("file:///faq.md", manual_document_id);

        bool lookup_threw = false;
        try {
            (void)documents.find_by_source_uri_compat("file:///faq.md");
        } catch (const std::runtime_error&) {
            lookup_threw = true;
        }
        MDBXC_TEST_ASSERT(lookup_threw);

        bool erase_threw = false;
        try {
            (void)documents.erase(second_id);
        } catch (const std::runtime_error&) {
            erase_threw = true;
        }
        MDBXC_TEST_ASSERT(erase_threw);
        MDBXC_TEST_ASSERT(documents.get(second_id).title == "FAQ");
        source_index.insert_or_assign("file:///faq.md", second_id);
    }

    // --- 10. sync_capture_rejects_mutations_before_any_capture_or_write ---
    {
        const std::size_t documents_before = documents.count();
        const std::size_t chunks_before = chunks.count();
        CountingCapture capture;
        conn->attach_sync_capture(&capture);

        bool document_threw = false;
        try {
            (void)documents.add(make_document("file:///captured.md", "Captured"));
        } catch (const std::logic_error&) {
            document_threw = true;
        }
        bool chunk_threw = false;
        try {
            (void)chunks.add(make_chunk(second_id, 3u, 0u, 8u, "captured"));
        } catch (const std::logic_error&) {
            chunk_threw = true;
        }
        conn->detach_sync_capture();

        MDBXC_TEST_ASSERT(document_threw);
        MDBXC_TEST_ASSERT(chunk_threw);
        MDBXC_TEST_ASSERT(documents.count() == documents_before);
        MDBXC_TEST_ASSERT(chunks.count() == chunks_before);
        MDBXC_TEST_ASSERT(capture.record_count == 0u);
        MDBXC_TEST_ASSERT(capture.flush_count == 0u);
    }

    // --- 11. restart_persistence ---
    conn->disconnect();
    conn->connect();
    {
        mdbxc::DocumentStore reopened_documents(conn, "documents");
        mdbxc::ChunkStore reopened_chunks(conn, "chunks");
        const mdbxc::Document reopened = reopened_documents.get(second_id);
        MDBXC_TEST_ASSERT(reopened.source_type == "markdown");
        MDBXC_TEST_ASSERT(reopened.metadata_json == "{\"lang\":\"en\"}");
        MDBXC_TEST_ASSERT(reopened_chunks.count() == 2u);
        MDBXC_TEST_ASSERT(reopened_chunks.get(manual_chunk_id).document_id ==
                          manual_document_id);
    }

    std::cout << "Document and Chunk store test passed.\n";
    return 0;
}
