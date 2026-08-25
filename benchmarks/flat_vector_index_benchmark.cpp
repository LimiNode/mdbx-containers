/// \file flat_vector_index_benchmark.cpp
/// \brief Manual append baseline for \ref mdbxc::FlatVectorIndex.

#include <mdbx_containers/vector.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

namespace {

struct Scenario {
    const char* name;
    std::size_t records;
    std::uint32_t dimension;
    std::size_t iterations;
};

double elapsed_ms(const std::chrono::steady_clock::time_point& started) {
    return std::chrono::duration_cast<std::chrono::duration<double, std::milli> >(
        std::chrono::steady_clock::now() - started).count();
}

mdbxc::Embedding make_embedding(std::uint32_t dimension) {
    mdbxc::Embedding embedding;
    embedding.dim = dimension;
    embedding.values.resize(dimension);
    for (std::size_t index = 0u; index < embedding.values.size(); ++index) {
        embedding.values[index] = static_cast<float>((index % 17u) + 1u) / 17.0f;
    }
    return embedding;
}

Scenario select_scenario(int argc, char** argv) {
    const Scenario quick = {"quick", 4000u, 128u, 3u};
    const Scenario realistic = {"realistic", 50000u, 384u, 3u};
    if (argc == 1) return quick;
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
        "usage: flat_vector_index_benchmark "
        "[--list-presets | --preset quick|realistic]");
}

void run(const Scenario& scenario) {
    const mdbxc::Embedding embedding = make_embedding(scenario.dimension);
    double best_append_ms = 0.0;
    std::size_t checksum = 0u;
    for (std::size_t iteration = 0u;
         iteration < scenario.iterations;
         ++iteration) {
        mdbxc::FlatVectorIndex index(mdbxc::VectorMetric::DOT);
        const std::chrono::steady_clock::time_point started =
            std::chrono::steady_clock::now();
        for (std::size_t record = 0u; record < scenario.records; ++record) {
            index.add(static_cast<std::uint64_t>(record), embedding);
        }
        const double append_ms = elapsed_ms(started);
        if (iteration == 0u || append_ms < best_append_ms) {
            best_append_ms = append_ms;
        }
        checksum += index.size();
        checksum += index.dim();
    }

    const double vectors_per_second = best_append_ms == 0.0 ? 0.0 :
        static_cast<double>(scenario.records) * 1000.0 / best_append_ms;
    std::cout << "scenario,records,dimension,iterations,best_append_ms,"
                 "vectors_per_second,checksum\n";
    std::cout << scenario.name << ',' << scenario.records << ','
              << scenario.dimension << ',' << scenario.iterations << ','
              << best_append_ms << ',' << vectors_per_second << ','
              << checksum << '\n';
}

} // namespace

int main(int argc, char** argv) {
    try {
        run(select_scenario(argc, argv));
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL flat_vector_index_benchmark: "
                  << error.what() << '\n';
    } catch (...) {
        std::cerr << "FAIL flat_vector_index_benchmark: non-std exception\n";
    }
    return 1;
}
