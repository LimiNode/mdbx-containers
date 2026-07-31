/// \file ordered_destructive_state_benchmark.cpp
/// \brief Manual benchmark for destructive ordered-state scan paths.

#include <mdbx_containers/sync.hpp>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

struct Scenario {
    std::uint64_t origins;
    std::uint64_t elements_per_origin;
    std::uint64_t key_count;
    std::uint64_t iterations;
    std::uint64_t tombstones_per_origin;
};

struct CleanupGuard {
    explicit CleanupGuard(const std::string& path_value) : path(path_value) {}

    ~CleanupGuard() {
        std::remove(path.c_str());
        std::remove((path + "-lck").c_str());
    }

    std::string path;
};

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId out{};
    for (std::size_t i = 0u; i < out.size(); ++i) {
        out[i] = static_cast<std::uint8_t>(seed + i);
    }
    return out;
}

std::uint64_t parse_u64(const char* text, const char* name) {
    if (text == nullptr || text[0] == '\0') {
        throw std::invalid_argument(std::string("missing ") + name);
    }
    char* end = nullptr;
    errno = 0;
    const unsigned long long value = std::strtoull(text, &end, 10);
    const unsigned long long max_value =
        static_cast<unsigned long long>((std::numeric_limits<std::uint64_t>::max)());
    if (errno == ERANGE || end == text || *end != '\0' || value > max_value) {
        throw std::invalid_argument(std::string("invalid ") + name + ": " + text);
    }
    return static_cast<std::uint64_t>(value);
}

Scenario parse_scenario(int argc, char** argv) {
    Scenario scenario;
    scenario.origins = 4u;
    scenario.elements_per_origin = 64u;
    scenario.key_count = 8u;
    scenario.iterations = 50u;
    scenario.tombstones_per_origin = 0u;
    if (argc == 1) return scenario;
    if (argc != 5 && argc != 6) {
        throw std::invalid_argument(
            "usage: ordered_destructive_state_benchmark "
            "[origins elements_per_origin key_count iterations "
            "[tombstones_per_origin]]");
    }
    scenario.origins = parse_u64(argv[1], "origins");
    scenario.elements_per_origin = parse_u64(argv[2], "elements_per_origin");
    scenario.key_count = parse_u64(argv[3], "key_count");
    scenario.iterations = parse_u64(argv[4], "iterations");
    if (argc == 6) {
        scenario.tombstones_per_origin =
            parse_u64(argv[5], "tombstones_per_origin");
    }
    return scenario;
}

void validate_scenario(const Scenario& scenario) {
    if (scenario.origins == 0u || scenario.elements_per_origin == 0u ||
        scenario.key_count == 0u || scenario.iterations == 0u) {
        throw std::invalid_argument("benchmark arguments must be positive");
    }
    if (scenario.origins > 200u || scenario.elements_per_origin > 4096u ||
        scenario.key_count > 1024u || scenario.iterations > 100000u) {
        throw std::invalid_argument("benchmark arguments exceed manual safety bounds");
    }
    if (scenario.tombstones_per_origin > scenario.elements_per_origin) {
        throw std::invalid_argument(
            "tombstones_per_origin exceeds elements_per_origin");
    }
}

std::vector<std::uint8_t> make_key(std::uint64_t index) {
    std::vector<std::uint8_t> key;
    mdbxc::sync::detail::append_u64_le(key, index);
    return key;
}

std::vector<std::uint8_t> make_value(std::uint64_t origin_index,
                                     std::uint64_t element_index) {
    std::vector<std::uint8_t> value;
    mdbxc::sync::detail::append_u64_le(value, origin_index);
    mdbxc::sync::detail::append_u64_le(value, element_index);
    return value;
}

double elapsed_ms(const std::chrono::steady_clock::time_point& started) {
    const std::chrono::steady_clock::duration elapsed =
        std::chrono::steady_clock::now() - started;
    return std::chrono::duration_cast<std::chrono::duration<double, std::milli> >(
        elapsed).count();
}

std::uint64_t expected_target_ids(const Scenario& scenario) {
    std::uint64_t per_origin = 0u;
    for (std::uint64_t element_index = scenario.tombstones_per_origin;
         element_index < scenario.elements_per_origin;
         ++element_index) {
        if (element_index % scenario.key_count == 0u) {
            ++per_origin;
        }
    }
    return per_origin * scenario.origins;
}

void seed_state(const std::shared_ptr<mdbxc::Connection>& connection,
                const Scenario& scenario,
                const mdbxc::sync::OrderedElementStateStore& store,
                std::vector<mdbxc::sync::NodeId>& origins) {
    mdbxc::Transaction transaction =
        connection->transaction(mdbxc::TransactionMode::WRITABLE);
    store.initialize_empty(transaction.handle());
    for (std::uint64_t origin_index = 0u;
         origin_index < scenario.origins;
         ++origin_index) {
        const mdbxc::sync::NodeId origin =
            make_node(static_cast<std::uint8_t>(origin_index + 1u));
        origins.push_back(origin);
        std::vector<mdbxc::sync::OrderedElementId> ids;
        for (std::uint64_t element_index = 0u;
             element_index < scenario.elements_per_origin;
             ++element_index) {
            const mdbxc::sync::OrderedElementId id =
                store.allocate_id(transaction.handle(), origin);
            store.put_live(transaction.handle(), id,
                           make_key(element_index % scenario.key_count),
                           make_value(origin_index, element_index));
            ids.push_back(id);
        }
        for (std::uint64_t tombstone_index = 0u;
             tombstone_index < scenario.tombstones_per_origin;
             ++tombstone_index) {
            store.tombstone(
                transaction.handle(), ids[static_cast<std::size_t>(tombstone_index)]);
        }
    }
    transaction.commit();
}

std::uint64_t measure_live_state_reverse_scan(
        const std::shared_ptr<mdbxc::Connection>& connection,
        const Scenario& scenario,
        const mdbxc::sync::OrderedElementStateStore& store,
        const std::vector<std::uint8_t>& key,
        double& out_ms) {
    const std::uint64_t expected = expected_target_ids(scenario);
    std::uint64_t observed = 0u;
    const std::chrono::steady_clock::time_point started =
        std::chrono::steady_clock::now();
    for (std::uint64_t iteration = 0u; iteration < scenario.iterations; ++iteration) {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::READ_ONLY);
        const std::vector<mdbxc::sync::OrderedElementId> ids =
            store.live_state_ids_for_key(transaction.handle(), key);
        transaction.rollback();
        if (ids.size() != expected) {
            throw std::runtime_error(
                "live state reverse scan returned an unexpected id count");
        }
        observed += static_cast<std::uint64_t>(ids.size());
    }
    out_ms = elapsed_ms(started);
    return observed;
}

void measure_origin_scans(const std::shared_ptr<mdbxc::Connection>& connection,
                          const Scenario& scenario,
                          const mdbxc::sync::OrderedElementStateStore& store,
                          const std::vector<mdbxc::sync::NodeId>& origins,
                          double& out_ms) {
    const std::chrono::steady_clock::time_point started =
        std::chrono::steady_clock::now();
    for (std::uint64_t iteration = 0u; iteration < scenario.iterations; ++iteration) {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::READ_ONLY);
        for (std::size_t i = 0u; i < origins.size(); ++i) {
            store.verify_introduced_high_water(transaction.handle(), origins[i]);
        }
        transaction.rollback();
    }
    out_ms = elapsed_ms(started);
}

void run(const Scenario& scenario) {
    const std::string path = "benchmark_ordered_destructive_state.mdbx";
    CleanupGuard cleanup(path);
    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 8;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    mdbxc::sync::OrderedElementStateStore store(
        connection->env_handle(), "benchmark_ordered_state",
        "benchmark_ordered_state_by_key");
    std::vector<mdbxc::sync::NodeId> origins;
    seed_state(connection, scenario, store, origins);

    double reverse_ms = 0.0;
    const std::uint64_t matched_live_ids = measure_live_state_reverse_scan(
        connection, scenario, store, make_key(0u), reverse_ms);
    double origin_ms = 0.0;
    measure_origin_scans(connection, scenario, store, origins, origin_ms);

    const std::uint64_t element_records =
        scenario.origins * scenario.elements_per_origin;
    const std::uint64_t tombstone_records =
        scenario.origins * scenario.tombstones_per_origin;
    const std::uint64_t live_element_records =
        element_records - tombstone_records;
    const std::uint64_t state_records =
        scenario.origins * (scenario.elements_per_origin + 2u);
    const std::uint64_t by_key_records = live_element_records;
    std::cout
        << "scan,origins,elements_per_origin,key_count,iterations,element_records,"
        << "live_element_records,tombstone_records,state_records,by_key_records,"
        << "matched_live_ids,elapsed_ms\n"
        << "live_state_ids_for_key," << scenario.origins << ','
        << scenario.elements_per_origin << ',' << scenario.key_count << ','
        << scenario.iterations << ',' << element_records << ','
        << live_element_records << ',' << tombstone_records << ','
        << state_records << ',' << by_key_records << ','
        << matched_live_ids << ',' << reverse_ms << '\n'
        << "verify_introduced_high_water," << scenario.origins << ','
        << scenario.elements_per_origin << ',' << scenario.key_count << ','
        << scenario.iterations << ',' << element_records << ','
        << live_element_records << ',' << tombstone_records << ','
        << state_records << ',' << by_key_records << ','
        << 0u << ',' << origin_ms << '\n';
    connection->disconnect();
}

} // namespace

int main(int argc, char** argv) {
    try {
        const Scenario scenario = parse_scenario(argc, argv);
        validate_scenario(scenario);
        run(scenario);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL ordered_destructive_state_benchmark: " << e.what() << '\n';
    } catch (...) {
        std::cerr << "FAIL ordered_destructive_state_benchmark: non-std exception\n";
    }
    return 1;
}
