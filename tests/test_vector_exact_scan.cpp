#include "test_assert.hpp"

#include <cmath>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx_containers/vector.hpp>

namespace {

mdbxc::Config make_config(const std::string& path) {
    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 32;
    config.no_subdir = true;
    config.relative_to_exe = false;
    return config;
}

mdbxc::Embedding make_embedding(float first, float second) {
    mdbxc::Embedding embedding;
    embedding.dim = 2u;
    embedding.values.push_back(first);
    embedding.values.push_back(second);
    return embedding;
}

mdbxc::VectorCollectionDescriptor make_descriptor(
        const std::string& collection_id,
        mdbxc::VectorMetric metric) {
    mdbxc::VectorCollectionDescriptor descriptor;
    descriptor.collection_id = collection_id;
    descriptor.dimension = 2u;
    descriptor.metric = metric;
    descriptor.normalization = mdbxc::VectorNormalization::None;
    descriptor.vector_codec_id = "raw-f32";
    descriptor.vector_codec_version = 1u;
    descriptor.signature_encoder_id = "none";
    descriptor.signature_encoder_version = 1u;
    descriptor.block_layout_version = 1u;
    return descriptor;
}

bool approximately_equal(float left, float right) {
    return std::fabs(left - right) < 0.0001f;
}

bool search_rejects_dimension(const mdbxc::VectorExactScan& scan) {
    mdbxc::Embedding wrong_dimension;
    wrong_dimension.dim = 3u;
    wrong_dimension.values.push_back(1.0f);
    wrong_dimension.values.push_back(0.0f);
    wrong_dimension.values.push_back(0.0f);
    try {
        scan.search(wrong_dimension, 1u);
    } catch (const std::invalid_argument&) {
        return true;
    }
    return false;
}

void verify_cosine_self_similarity(
        const std::shared_ptr<mdbxc::Connection>& connection,
        const std::string& collection_id,
        float magnitude) {
    mdbxc::VectorCollection collection(
        connection, make_descriptor(collection_id, mdbxc::VectorMetric::COSINE));
    const mdbxc::Embedding embedding = make_embedding(magnitude, 0.0f);
    collection.insert_or_assign("self", embedding);
    const mdbxc::VectorExactScan scan(collection);
    const std::vector<mdbxc::VectorExactMatch> matches = scan.search(embedding, 1u);
    MDBXC_TEST_ASSERT(matches.size() == 1u);
    MDBXC_TEST_ASSERT(matches[0].record_id == "self");
    MDBXC_TEST_ASSERT(approximately_equal(matches[0].score, 1.0f));
}

void verify_metric(mdbxc::VectorMetric metric,
                   const std::string& collection_id,
                   const std::shared_ptr<mdbxc::Connection>& connection) {
    mdbxc::VectorCollection collection(
        connection, make_descriptor(collection_id, metric));
    collection.insert_or_assign("best", make_embedding(2.0f, 0.0f));
    collection.insert_or_assign("next", make_embedding(1.0f, 0.0f));
    collection.insert_or_assign("other", make_embedding(0.0f, 1.0f));

    const mdbxc::VectorExactScan scan(collection);
    const std::vector<mdbxc::VectorExactMatch> matches =
        scan.search(make_embedding(1.0f, 0.0f), 3u);
    MDBXC_TEST_ASSERT(matches.size() == 3u);
    if (metric == mdbxc::VectorMetric::COSINE) {
        MDBXC_TEST_ASSERT(matches[0].record_id == "best");
        MDBXC_TEST_ASSERT(matches[1].record_id == "next");
        MDBXC_TEST_ASSERT(matches[2].record_id == "other");
        MDBXC_TEST_ASSERT(approximately_equal(matches[0].score, 1.0f));
        MDBXC_TEST_ASSERT(approximately_equal(matches[1].score, 1.0f));
        MDBXC_TEST_ASSERT(approximately_equal(matches[2].score, 0.0f));
    } else if (metric == mdbxc::VectorMetric::DOT) {
        MDBXC_TEST_ASSERT(matches[0].record_id == "best");
        MDBXC_TEST_ASSERT(matches[1].record_id == "next");
        MDBXC_TEST_ASSERT(matches[2].record_id == "other");
        MDBXC_TEST_ASSERT(approximately_equal(matches[0].score, 2.0f));
        MDBXC_TEST_ASSERT(approximately_equal(matches[1].score, 1.0f));
        MDBXC_TEST_ASSERT(approximately_equal(matches[2].score, 0.0f));
    } else {
        MDBXC_TEST_ASSERT(matches[0].record_id == "next");
        MDBXC_TEST_ASSERT(matches[1].record_id == "best");
        MDBXC_TEST_ASSERT(matches[2].record_id == "other");
        MDBXC_TEST_ASSERT(approximately_equal(matches[0].score, 0.0f));
        MDBXC_TEST_ASSERT(approximately_equal(matches[1].score, -1.0f));
        MDBXC_TEST_ASSERT(approximately_equal(matches[2].score, -2.0f));
    }
    MDBXC_TEST_ASSERT(search_rejects_dimension(scan));
}

} // namespace

int main() {
    const mdbxc::Config config = make_config("vector_exact_scan_test.mdbx");
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);

    verify_metric(mdbxc::VectorMetric::COSINE, "exact-cosine", connection);
    verify_metric(mdbxc::VectorMetric::DOT, "exact-dot", connection);
    verify_metric(mdbxc::VectorMetric::L2, "exact-l2", connection);
    verify_cosine_self_similarity(
        connection, "exact-cosine-max", (std::numeric_limits<float>::max)());
    verify_cosine_self_similarity(
        connection, "exact-cosine-min", (std::numeric_limits<float>::min)());

    {
        mdbxc::VectorCollection empty_collection(
            connection, make_descriptor("exact-empty", mdbxc::VectorMetric::DOT));
        const mdbxc::VectorExactScan empty_scan(empty_collection);
        MDBXC_TEST_ASSERT(empty_scan.empty());
        MDBXC_TEST_ASSERT(empty_scan.size() == 0u);
        MDBXC_TEST_ASSERT(empty_scan.dimension() == 2u);
        MDBXC_TEST_ASSERT(empty_scan.search(make_embedding(1.0f, 0.0f), 1u).empty());
    }

    {
        mdbxc::VectorCollection tie_collection(
            connection, make_descriptor("exact-ties", mdbxc::VectorMetric::DOT));
        tie_collection.insert_or_assign("record-c", make_embedding(1.0f, 0.0f));
        tie_collection.insert_or_assign("record-a", make_embedding(1.0f, 0.0f));
        tie_collection.insert_or_assign("record-b", make_embedding(1.0f, 0.0f));
        const mdbxc::VectorExactScan tie_scan(tie_collection);
        const std::vector<mdbxc::VectorExactMatch> matches =
            tie_scan.search(make_embedding(1.0f, 0.0f), 3u);
        MDBXC_TEST_ASSERT(matches[0].record_id == "record-a");
        MDBXC_TEST_ASSERT(matches[1].record_id == "record-b");
        MDBXC_TEST_ASSERT(matches[2].record_id == "record-c");
        MDBXC_TEST_ASSERT(tie_scan.search(make_embedding(1.0f, 0.0f), 0u).empty());
        MDBXC_TEST_ASSERT(tie_scan.search(make_embedding(1.0f, 0.0f), 100u).size() == 3u);
    }

    {
        mdbxc::VectorCollection binary_id_collection(
            connection, make_descriptor("exact-binary-id", mdbxc::VectorMetric::DOT));
        const std::string first_id("record\0a", 8u);
        const std::string second_id("record\0b", 8u);
        binary_id_collection.insert_or_assign(second_id, make_embedding(1.0f, 0.0f));
        binary_id_collection.insert_or_assign(first_id, make_embedding(1.0f, 0.0f));
        const mdbxc::VectorExactScan binary_id_scan(binary_id_collection);
        const std::vector<mdbxc::VectorExactMatch> matches =
            binary_id_scan.search(make_embedding(1.0f, 0.0f), 2u);
        MDBXC_TEST_ASSERT(matches[0].record_id == first_id);
        MDBXC_TEST_ASSERT(matches[1].record_id == second_id);
    }

    {
        mdbxc::VectorCollection nan_collection(
            connection, make_descriptor("exact-nan-order", mdbxc::VectorMetric::DOT));
        const float nan_value = (std::numeric_limits<float>::quiet_NaN)();
        nan_collection.insert_or_assign("nan-b", make_embedding(nan_value, 0.0f));
        nan_collection.insert_or_assign("finite", make_embedding(1.0f, 0.0f));
        nan_collection.insert_or_assign("nan-a", make_embedding(nan_value, 0.0f));
        const mdbxc::VectorExactScan nan_scan(nan_collection);
        const std::vector<mdbxc::VectorExactMatch> matches =
            nan_scan.search(make_embedding(1.0f, 0.0f), 3u);
        MDBXC_TEST_ASSERT(matches[0].record_id == "finite");
        MDBXC_TEST_ASSERT(matches[1].record_id == "nan-a");
        MDBXC_TEST_ASSERT(matches[2].record_id == "nan-b");
        MDBXC_TEST_ASSERT(std::isnan(matches[1].score));
        MDBXC_TEST_ASSERT(std::isnan(matches[2].score));
    }

    {
        mdbxc::VectorCollection collection(
            connection, make_descriptor("exact-snapshot", mdbxc::VectorMetric::DOT));
        collection.insert_or_assign("old", make_embedding(1.0f, 0.0f));
        mdbxc::VectorExactScan scan(collection);
        collection.erase("old");
        collection.insert_or_assign("new", make_embedding(3.0f, 0.0f));

        std::vector<mdbxc::VectorExactMatch> matches =
            scan.search(make_embedding(1.0f, 0.0f), 1u);
        MDBXC_TEST_ASSERT(matches.size() == 1u);
        MDBXC_TEST_ASSERT(matches[0].record_id == "old");

        scan.rebuild(collection);
        matches = scan.search(make_embedding(1.0f, 0.0f), 1u);
        MDBXC_TEST_ASSERT(matches.size() == 1u);
        MDBXC_TEST_ASSERT(matches[0].record_id == "new");
    }

    connection->disconnect();
    return 0;
}
