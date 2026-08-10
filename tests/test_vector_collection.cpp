#include "test_assert.hpp"

#include <cstdint>
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

mdbxc::Embedding make_embedding(float first, float second, float third) {
    mdbxc::Embedding embedding;
    embedding.dim = 3u;
    embedding.values.push_back(first);
    embedding.values.push_back(second);
    embedding.values.push_back(third);
    return embedding;
}

mdbxc::VectorCollectionDescriptor make_descriptor(const std::string& collection_id) {
    mdbxc::VectorCollectionDescriptor descriptor;
    descriptor.collection_id = collection_id;
    descriptor.dimension = 3u;
    descriptor.metric = mdbxc::VectorMetric::COSINE;
    descriptor.normalization = mdbxc::VectorNormalization::None;
    descriptor.vector_codec_id = "raw-f32";
    descriptor.vector_codec_version = 1u;
    descriptor.signature_encoder_id = "none";
    descriptor.signature_encoder_version = 1u;
    descriptor.block_layout_version = 1u;
    return descriptor;
}

bool throws_invalid_argument_for_empty_id(
        mdbxc::VectorCollection& collection,
        const mdbxc::Embedding& embedding) {
    try {
        collection.insert_or_assign(std::string(), embedding);
    } catch (const std::invalid_argument&) {
        return true;
    }
    return false;
}

bool rejects_invalid_descriptor(mdbxc::VectorCollectionDescriptor descriptor) {
    try {
        descriptor.validate();
    } catch (const std::invalid_argument&) {
        return true;
    }
    return false;
}

} // namespace

int main() {
    const mdbxc::Config config = make_config("vector_collection_test.mdbx");
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::VectorCollectionDescriptor descriptor =
        make_descriptor("knowledge-v1");

    {
        const std::vector<std::uint8_t> bytes = descriptor.to_bytes();
        const mdbxc::VectorCollectionDescriptor restored =
            mdbxc::VectorCollectionDescriptor::from_bytes(bytes.data(), bytes.size());
        MDBXC_TEST_ASSERT(restored == descriptor);

        std::vector<std::uint8_t> trailing = bytes;
        trailing.push_back(0u);
        bool rejected = false;
        try {
            mdbxc::VectorCollectionDescriptor::from_bytes(
                trailing.data(), trailing.size());
        } catch (const std::runtime_error&) {
            rejected = true;
        }
        MDBXC_TEST_ASSERT(rejected);
    }

    {
        mdbxc::VectorCollectionDescriptor unsupported_codec = descriptor;
        unsupported_codec.vector_codec_id = "pq-8bit";
        MDBXC_TEST_ASSERT(rejects_invalid_descriptor(unsupported_codec));

        mdbxc::VectorCollectionDescriptor unsupported_signature = descriptor;
        unsupported_signature.signature_encoder_id = "mih-256";
        MDBXC_TEST_ASSERT(rejects_invalid_descriptor(unsupported_signature));

        mdbxc::VectorCollectionDescriptor unsupported_layout = descriptor;
        unsupported_layout.block_layout_version = 2u;
        MDBXC_TEST_ASSERT(rejects_invalid_descriptor(unsupported_layout));
    }

    {
        mdbxc::VectorCollection collection(connection, descriptor);
        const mdbxc::Embedding first = make_embedding(1.0f, 0.0f, 0.0f);
        const mdbxc::Embedding replacement = make_embedding(0.0f, 1.0f, 0.0f);
        const std::string binary_record_id("record\0binary", 13u);
        collection.insert_or_assign("record-a", first);
        collection.insert_or_assign("record-a", replacement);
        collection.insert_or_assign(binary_record_id, first);

        mdbxc::Embedding restored;
        MDBXC_TEST_ASSERT(collection.try_get("record-a", restored));
        MDBXC_TEST_ASSERT(restored.values == replacement.values);
        MDBXC_TEST_ASSERT(collection.try_get(binary_record_id, restored));
        MDBXC_TEST_ASSERT(collection.count() == 2u);
        MDBXC_TEST_ASSERT(throws_invalid_argument_for_empty_id(collection, first));
        MDBXC_TEST_ASSERT(!collection.erase("missing-record"));
        MDBXC_TEST_ASSERT(collection.erase("record-a"));
        MDBXC_TEST_ASSERT(collection.erase(binary_record_id));
        MDBXC_TEST_ASSERT(collection.empty());
    }

    {
        mdbxc::VectorCollection collection(connection, descriptor);
        const mdbxc::Embedding embedding = make_embedding(1.0f, 0.0f, 0.0f);
        collection.insert_or_assign("record-a", embedding);

        mdbxc::Embedding wrong_dimension;
        wrong_dimension.dim = 2u;
        wrong_dimension.values.push_back(1.0f);
        wrong_dimension.values.push_back(0.0f);
        bool rejected = false;
        try {
            collection.insert_or_assign("record-a", wrong_dimension);
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        MDBXC_TEST_ASSERT(rejected);

        mdbxc::Embedding restored;
        MDBXC_TEST_ASSERT(collection.try_get("record-a", restored));
        MDBXC_TEST_ASSERT(restored.values == embedding.values);

        mdbxc::VectorCollectionDescriptor mismatched = descriptor;
        mismatched.dimension = 4u;
        rejected = false;
        try {
            mdbxc::VectorCollection invalid(connection, mismatched);
            (void)invalid;
        } catch (const std::invalid_argument&) {
            rejected = true;
        }
        MDBXC_TEST_ASSERT(rejected);

        MDBXC_TEST_ASSERT(collection.try_get("record-a", restored));
        MDBXC_TEST_ASSERT(restored.values == embedding.values);
    }

    {
        mdbxc::VectorStore legacy(connection, "knowledge-v1");
        legacy.clear();
        legacy.add(make_embedding(0.0f, 0.0f, 1.0f), "legacy");

        mdbxc::VectorCollection collection(connection, descriptor);
        mdbxc::Embedding restored;
        MDBXC_TEST_ASSERT(collection.try_get("record-a", restored));
        MDBXC_TEST_ASSERT(legacy.count() == 1u);
    }

    connection->disconnect();
    return 0;
}
