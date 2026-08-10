/// \file vector_exact_scan_benchmark.cpp
/// \brief Manual CSV baseline for \ref mdbxc::VectorExactScan.

#include <mdbx_containers/vector.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

struct Scenario {
    const char* name;
    std::size_t records;
    std::uint32_t dimension;
    std::size_t queries;
};

struct CleanupGuard {
    explicit CleanupGuard(const std::string& path_value) : path(path_value) {}

    ~CleanupGuard() {
        std::remove(path.c_str());
        std::remove((path + "-lck").c_str());
    }

    std::string path;
};

std::uint32_t next_random(std::uint32_t& state) {
    state ^= state << 13u;
    state ^= state >> 17u;
    state ^= state << 5u;
    return state;
}

float next_float(std::uint32_t& state) {
    const std::uint32_t value = next_random(state) & 0xFFFFu;
    return static_cast<float>(value) / 32767.5f - 1.0f;
}

double elapsed_ms(const std::chrono::steady_clock::time_point& started) {
    return std::chrono::duration_cast<std::chrono::duration<double, std::milli> >(
        std::chrono::steady_clock::now() - started).count();
}

const char* metric_name(mdbxc::VectorMetric metric) {
    if (metric == mdbxc::VectorMetric::COSINE) return "cosine";
    if (metric == mdbxc::VectorMetric::DOT) return "dot";
    return "l2";
}

mdbxc::VectorCollectionDescriptor make_descriptor(
        const std::string& collection_id,
        std::uint32_t dimension,
        mdbxc::VectorMetric metric) {
    mdbxc::VectorCollectionDescriptor descriptor;
    descriptor.collection_id = collection_id;
    descriptor.dimension = dimension;
    descriptor.metric = metric;
    descriptor.normalization = mdbxc::VectorNormalization::None;
    descriptor.vector_codec_id = "raw-f32";
    descriptor.vector_codec_version = 1u;
    descriptor.signature_encoder_id = "none";
    descriptor.signature_encoder_version = 1u;
    descriptor.block_layout_version = 1u;
    return descriptor;
}

mdbxc::Embedding make_embedding(std::uint32_t dimension, std::uint32_t& random_state) {
    mdbxc::Embedding embedding;
    embedding.dim = dimension;
    embedding.values.resize(dimension);
    for (std::size_t index = 0u; index < embedding.values.size(); ++index) {
        embedding.values[index] = next_float(random_state);
    }
    return embedding;
}

void seed_collection(mdbxc::VectorCollection& collection,
                     const Scenario& scenario,
                     std::uint32_t& random_state) {
    for (std::size_t record_index = 0u; record_index < scenario.records; ++record_index) {
        collection.insert_or_assign(
            std::string("record-") + std::to_string(record_index),
            make_embedding(scenario.dimension, random_state));
    }
}

void run_metric(const Scenario& scenario,
                const std::shared_ptr<mdbxc::Connection>& connection,
                mdbxc::VectorMetric metric,
                std::uint32_t& random_state) {
    const std::string collection_id =
        std::string("benchmark-") + scenario.name + '-' + metric_name(metric);
    mdbxc::VectorCollection collection(
        connection, make_descriptor(collection_id, scenario.dimension, metric));

    const std::chrono::steady_clock::time_point seed_started =
        std::chrono::steady_clock::now();
    seed_collection(collection, scenario, random_state);
    const double seed_ms = elapsed_ms(seed_started);

    const std::chrono::steady_clock::time_point rebuild_started =
        std::chrono::steady_clock::now();
    const mdbxc::VectorExactScan scan(collection);
    const double rebuild_ms = elapsed_ms(rebuild_started);

    const std::chrono::steady_clock::time_point search_started =
        std::chrono::steady_clock::now();
    std::size_t total_matches = 0u;
    for (std::size_t query_index = 0u; query_index < scenario.queries; ++query_index) {
        total_matches += scan.search(
            make_embedding(scenario.dimension, random_state), 10u).size();
    }
    const double search_ms = elapsed_ms(search_started);
    const double queries_per_second = search_ms == 0.0 ? 0.0 :
        static_cast<double>(scenario.queries) * 1000.0 / search_ms;

    std::cout << scenario.name << ',' << metric_name(metric) << ','
              << scenario.records << ',' << scenario.dimension << ','
              << scenario.queries << ',' << seed_ms << ',' << rebuild_ms << ','
              << search_ms << ',' << queries_per_second << ',' << total_matches << '\n';
}

Scenario select_scenario(int argc, char** argv) {
    const Scenario quick = {"quick", 2000u, 128u, 64u};
    const Scenario realistic = {"realistic", 50000u, 384u, 512u};
    if (argc == 1) return quick;
    if (argc == 2 && std::string(argv[1]) == "--preset") {
        throw std::invalid_argument("missing preset name");
    }
    if (argc == 2 && std::string(argv[1]) == "--list-presets") {
        std::cout << "quick\nrealistic\n";
        std::exit(0);
    }
    if (argc == 3 && std::string(argv[1]) == "--preset") {
        const std::string preset(argv[2]);
        if (preset == quick.name) return quick;
        if (preset == realistic.name) return realistic;
        throw std::invalid_argument("unknown preset: " + preset);
    }
    throw std::invalid_argument(
        "usage: vector_exact_scan_benchmark [--list-presets | --preset quick|realistic]");
}

void run(const Scenario& scenario) {
    const std::string path = "benchmark_vector_exact_scan.mdbx";
    CleanupGuard cleanup(path);
    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 32;
    config.no_subdir = true;
    config.relative_to_exe = false;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);

    std::uint32_t random_state = 0x51A7E5EDu;
    std::cout << "scenario,metric,records,dimension,queries,seed_ms,rebuild_ms,"
                 "search_ms,queries_per_second,total_matches\n";
    run_metric(scenario, connection, mdbxc::VectorMetric::COSINE, random_state);
    run_metric(scenario, connection, mdbxc::VectorMetric::DOT, random_state);
    run_metric(scenario, connection, mdbxc::VectorMetric::L2, random_state);
    connection->disconnect();
}

} // namespace

int main(int argc, char** argv) {
    try {
        run(select_scenario(argc, argv));
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL vector_exact_scan_benchmark: " << error.what() << '\n';
    } catch (...) {
        std::cerr << "FAIL vector_exact_scan_benchmark: non-std exception\n";
    }
    return 1;
}
