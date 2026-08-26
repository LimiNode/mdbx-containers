#include "test_assert.hpp"
#include <exception>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <mdbx_containers/vector.hpp>

static mdbxc::Embedding make_embedding(const std::vector<float>& vals) {
    mdbxc::Embedding e;
    e.dim = static_cast<uint32_t>(vals.size());
    e.values = vals;
    return e;
}

static void assert_flat_cosine_self_similarity(float magnitude) {
    mdbxc::FlatVectorIndex index(mdbxc::VectorMetric::COSINE);
    const mdbxc::Embedding embedding = make_embedding({magnitude, 0.0f});
    index.add(1u, embedding);
    const std::vector<mdbxc::VectorMatch> matches = index.search(embedding, 1u);
    MDBXC_TEST_ASSERT(matches.size() == 1u);
    MDBXC_TEST_ASSERT(std::fabs(matches[0].score - 1.0f) < 0.0001f);
}

int main() {
    // --- 1. Add and search ---
    {
        mdbxc::Config cfg;
        cfg.pathname = "data/vector_store_test_1.mdbx";
        cfg.max_dbs = 10;
        cfg.no_subdir = true;
        cfg.relative_to_exe = false;

        mdbxc::VectorStore store(cfg, "test1");
        store.clear();

        mdbxc::Embedding e1 = make_embedding({1.0f, 0.0f, 0.0f});
        mdbxc::Embedding e2 = make_embedding({0.0f, 1.0f, 0.0f});
        store.add(e1, "A", "{\"source\":\"unit\"}");
        store.add(e2, "B");

        mdbxc::Embedding query = make_embedding({1.0f, 0.0f, 0.0f});
        std::vector<mdbxc::SearchResult> results = store.search(query, 1);
        MDBXC_TEST_ASSERT(results.size() == 1);
        MDBXC_TEST_ASSERT(results[0].text == "A");
        MDBXC_TEST_ASSERT(results[0].metadata_json == "{\"source\":\"unit\"}");
    }

    // --- 2. Top-k order ---
    {
        mdbxc::Config cfg;
        cfg.pathname = "data/vector_store_test_2.mdbx";
        cfg.max_dbs = 10;
        cfg.no_subdir = true;
        cfg.relative_to_exe = false;

        mdbxc::VectorStore store(cfg, "test2");
        store.clear();

        mdbxc::Embedding e1 = make_embedding({1.0f, 0.0f});
        mdbxc::Embedding e2 = make_embedding({0.8f, 0.2f});
        mdbxc::Embedding e3 = make_embedding({0.0f, 1.0f});
        uint64_t id1 = store.add(e1, "first");
        store.add(e2, "second");
        store.add(e3, "third");

        mdbxc::Embedding query = make_embedding({1.0f, 0.0f});
        std::vector<mdbxc::SearchResult> results = store.search(query, 2);
        MDBXC_TEST_ASSERT(results.size() == 2);
        MDBXC_TEST_ASSERT(results[0].id == id1);
    }

    // --- 3. Persistence rebuild ---
    {
        mdbxc::Config cfg;
        cfg.pathname = "data/vector_store_test_3.mdbx";
        cfg.max_dbs = 10;
        cfg.no_subdir = true;
        cfg.relative_to_exe = false;

        {
            mdbxc::VectorStore store(cfg, "persist");
            store.clear();
            mdbxc::Embedding e1 = make_embedding({1.0f, 0.0f});
            store.add(e1, "persisted");
        }

        {
            mdbxc::VectorStore store(cfg, "persist");
            mdbxc::Embedding query = make_embedding({1.0f, 0.0f});
            std::vector<mdbxc::SearchResult> results = store.search(query, 1);
            MDBXC_TEST_ASSERT(results.size() == 1);
            MDBXC_TEST_ASSERT(results[0].text == "persisted");
        }
    }

    // --- 4. Erase ---
    {
        mdbxc::Config cfg;
        cfg.pathname = "data/vector_store_test_4.mdbx";
        cfg.max_dbs = 10;
        cfg.no_subdir = true;
        cfg.relative_to_exe = false;

        mdbxc::VectorStore store(cfg, "erase_test");
        store.clear();

        mdbxc::Embedding e1 = make_embedding({1.0f, 0.0f});
        mdbxc::Embedding e2 = make_embedding({0.0f, 1.0f});
        store.add(e1, "keep");
        uint64_t id2 = store.add(e2, "remove");

        MDBXC_TEST_ASSERT(store.erase(id2));

        mdbxc::Embedding query = make_embedding({0.0f, 1.0f});
        std::vector<mdbxc::SearchResult> results = store.search(query, 10);
        for (std::size_t i = 0; i < results.size(); ++i) {
            MDBXC_TEST_ASSERT(results[i].id != id2);
        }
    }

    // --- 5. Dimension mismatch ---
    {
        mdbxc::Config cfg;
        cfg.pathname = "data/vector_store_test_5.mdbx";
        cfg.max_dbs = 10;
        cfg.no_subdir = true;
        cfg.relative_to_exe = false;

        mdbxc::VectorStore store(cfg, "dim_test");
        store.clear();

        mdbxc::Embedding e3 = make_embedding({1.0f, 0.0f, 0.0f});
        store.add(e3, "dim3");

        mdbxc::Embedding query2 = make_embedding({1.0f, 0.0f});
        bool threw = false;
        try {
            store.search(query2, 1);
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        MDBXC_TEST_ASSERT(threw);
    }

    // --- 6. Empty embedding ---
    {
        mdbxc::Config cfg;
        cfg.pathname = "data/vector_store_test_6.mdbx";
        cfg.max_dbs = 10;
        cfg.no_subdir = true;
        cfg.relative_to_exe = false;

        mdbxc::VectorStore store(cfg, "empty_test");
        store.clear();

        mdbxc::Embedding empty_e;
        bool threw = false;
        try {
            store.add(empty_e, "bad");
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        MDBXC_TEST_ASSERT(threw);
    }

    // --- 7. Collections isolation ---
    {
        mdbxc::Config cfg;
        cfg.pathname = "data/vector_store_test_7.mdbx";
        cfg.max_dbs = 20;
        cfg.no_subdir = true;
        cfg.relative_to_exe = false;

        auto conn = mdbxc::Connection::create(cfg);

        mdbxc::VectorStore storeA(conn, "docs");
        mdbxc::VectorStore storeB(conn, "code_raw");
        storeA.clear();
        storeB.clear();
        MDBXC_TEST_ASSERT(storeB.collection() == "code_raw");

        mdbxc::Embedding e1 = make_embedding({1.0f, 0.0f});
        storeA.add(e1, "document");

        mdbxc::Embedding query = make_embedding({1.0f, 0.0f});
        std::vector<mdbxc::SearchResult> resultsB = storeB.search(query, 1);
        MDBXC_TEST_ASSERT(resultsB.empty());

        bool rejected_slash = false;
        try {
            mdbxc::VectorStore invalid(conn, "code/raw");
            (void)invalid;
        } catch (const std::invalid_argument&) {
            rejected_slash = true;
        }
        MDBXC_TEST_ASSERT(rejected_slash);

        bool rejected_space = false;
        try {
            mdbxc::VectorStore invalid(conn, "code raw");
            (void)invalid;
        } catch (const std::invalid_argument&) {
            rejected_space = true;
        }
        MDBXC_TEST_ASSERT(rejected_space);
    }

    // --- 8. FlatVectorIndex metrics and top_k=0 ---
    {
        mdbxc::FlatVectorIndex dot_index(mdbxc::VectorMetric::DOT);
        dot_index.add(10, make_embedding({2.0f, 0.0f}));
        dot_index.add(11, make_embedding({0.0f, 1.0f}));
        std::vector<mdbxc::VectorMatch> dot_results =
            dot_index.search(make_embedding({1.0f, 0.0f}), 1);
        MDBXC_TEST_ASSERT(dot_results.size() == 1);
        MDBXC_TEST_ASSERT(dot_results[0].id == 10);
        MDBXC_TEST_ASSERT(dot_results[0].score == 2.0f);
        MDBXC_TEST_ASSERT(dot_index.search(make_embedding({1.0f, 0.0f}), 0).empty());

        mdbxc::FlatVectorIndex l2_index(mdbxc::VectorMetric::L2);
        l2_index.add(20, make_embedding({1.0f, 0.0f}));
        l2_index.add(21, make_embedding({3.0f, 0.0f}));
        std::vector<mdbxc::VectorMatch> l2_results =
            l2_index.search(make_embedding({2.5f, 0.0f}), 2);
        MDBXC_TEST_ASSERT(l2_results.size() == 2);
        MDBXC_TEST_ASSERT(l2_results[0].id == 21);
        MDBXC_TEST_ASSERT(l2_index.erase(20));
        MDBXC_TEST_ASSERT(l2_index.erase(21));
        MDBXC_TEST_ASSERT(l2_index.dim() == 0);

        assert_flat_cosine_self_similarity(
            (std::numeric_limits<float>::max)());
        assert_flat_cosine_self_similarity(
            (std::numeric_limits<float>::min)());
    }

    // --- 9. Failed add does not persist ---
    {
        mdbxc::Config cfg;
        cfg.pathname = "data/vector_store_test_9.mdbx";
        cfg.max_dbs = 10;
        cfg.no_subdir = true;
        cfg.relative_to_exe = false;

        {
            mdbxc::VectorStore store(cfg, "failed_add");
            store.clear();
            store.add(make_embedding({1.0f, 0.0f, 0.0f}), "dim3");
            bool threw = false;
            try {
                store.add(make_embedding({1.0f, 0.0f}), "dim2");
            } catch (const std::invalid_argument&) {
                threw = true;
            }
            MDBXC_TEST_ASSERT(threw);
            MDBXC_TEST_ASSERT(store.count() == 1);
        }
        {
            mdbxc::VectorStore store(cfg, "failed_add");
            mdbxc::Embedding query = make_embedding({1.0f, 0.0f, 0.0f});
            auto results = store.search(query, 1);
            MDBXC_TEST_ASSERT(results.size() == 1);
            MDBXC_TEST_ASSERT(results[0].text == "dim3");
        }
    }

    // --- 10. Embedding serialization validation ---
    {
        mdbxc::Embedding embedding = make_embedding({1.0f, 2.0f});
        std::vector<uint8_t> bytes = embedding.to_bytes();
        mdbxc::Embedding restored = mdbxc::Embedding::from_bytes(bytes.data(), bytes.size());
        MDBXC_TEST_ASSERT(restored.dim == 2);
        MDBXC_TEST_ASSERT(restored.values == embedding.values);

        bool threw = false;
        try {
            mdbxc::Embedding invalid;
            invalid.dim = 3;
            invalid.values.push_back(1.0f);
            invalid.to_bytes();
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        MDBXC_TEST_ASSERT(threw);

        threw = false;
        bytes[4] = 1;
        try {
            mdbxc::Embedding::from_bytes(bytes.data(), bytes.size());
        } catch (const std::runtime_error&) {
            threw = true;
        }
        MDBXC_TEST_ASSERT(threw);
    }

    // --- 11. One VectorStore instance serializes concurrent searches ---
    {
        mdbxc::Config cfg;
        cfg.pathname = "data/vector_store_test_11.mdbx";
        cfg.max_dbs = 10;
        cfg.no_subdir = true;
        cfg.relative_to_exe = false;

        mdbxc::VectorStore store(cfg, "concurrent_search");
        store.clear();
        store.add(make_embedding({1.0f, 0.0f}), "left");
        store.add(make_embedding({0.0f, 1.0f}), "right");

        const mdbxc::Embedding query = make_embedding({1.0f, 0.0f});
        std::exception_ptr error1;
        std::exception_ptr error2;
        std::thread first([&store, &query, &error1]() {
            try {
                for (int i = 0; i < 200; ++i) {
                    const std::vector<mdbxc::SearchResult> results =
                        store.search(query, 2);
                    if (results.size() != 2u) {
                        throw std::runtime_error(
                            "concurrent VectorStore search returned wrong size");
                    }
                }
            } catch (...) {
                error1 = std::current_exception();
            }
        });
        std::thread second([&store, &query, &error2]() {
            try {
                for (int i = 0; i < 200; ++i) {
                    const std::vector<mdbxc::SearchResult> results =
                        store.search(query, 2);
                    if (results.size() != 2u) {
                        throw std::runtime_error(
                            "concurrent VectorStore search returned wrong size");
                    }
                }
            } catch (...) {
                error2 = std::current_exception();
            }
        });
        first.join();
        second.join();
        if (error1) {
            std::rethrow_exception(error1);
        }
        if (error2) {
            std::rethrow_exception(error2);
        }
    }

    std::cout << "VectorStore test passed.\n";
    return 0;
}
