#include <mdbx_containers/sync.hpp>

#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void cleanup(const std::string& path) {
    std::remove(path.c_str());
}

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId out{};
    for (std::size_t i = 0u; i < out.size(); ++i) {
        out[i] = static_cast<std::uint8_t>(seed + i);
    }
    return out;
}

void test_ordered_element_state_persists_live_and_tombstone_records() {
    const std::string path = "test_ordered_element_state.mdbx";
    cleanup(path);

    mdbxc::Config config;
    config.pathname = path;
    config.max_dbs = 16;
    config.no_subdir = true;
    const std::shared_ptr<mdbxc::Connection> connection =
        mdbxc::Connection::create(config);
    const mdbxc::sync::NodeId origin = make_node(0x41u);
    const std::vector<std::uint8_t> key(1u, 0xABu);
    const std::vector<std::uint8_t> value(2u, 0xCDu);

    mdbxc::sync::OrderedElementStateStore store(
        connection->env_handle(), "ordered_state", "ordered_by_key");
    mdbxc::sync::OrderedElementId first;
    mdbxc::sync::OrderedElementId second;
    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::WRITABLE);
        first = store.allocate_id(transaction.handle(), origin);
        second = store.allocate_id(transaction.handle(), origin);
        if (first.sequence != 1u || second.sequence != 2u ||
            first.origin != origin || second.origin != origin) {
            throw std::runtime_error("ordered element counter allocation mismatch");
        }
        store.put_live(transaction.handle(), first, key, value);
        store.put_live(transaction.handle(), second, key, value);
        store.tombstone(transaction.handle(), first);
        transaction.commit();
    }

    {
        mdbxc::Transaction transaction =
            connection->transaction(mdbxc::TransactionMode::READ_ONLY);
        mdbxc::sync::OrderedElementStateRecord record;
        if (!store.get(transaction.handle(), first, record) || record.live ||
            !record.key.empty() || !record.value.empty()) {
            throw std::runtime_error("ordered element tombstone did not persist");
        }
        const std::vector<mdbxc::sync::OrderedElementId> live =
            store.live_ids_for_key(transaction.handle(), key);
        if (live.size() != 1u || live[0] != second) {
            throw std::runtime_error("ordered element live index is incorrect");
        }
        transaction.rollback();
    }

    const std::vector<std::uint8_t> logical =
        mdbxc::sync::encode_ordered_element_id_logical(second);
    const std::vector<std::uint8_t> index =
        mdbxc::sync::encode_ordered_element_id_index(second);
    if (mdbxc::sync::decode_ordered_element_id_logical(logical) != second ||
        mdbxc::sync::decode_ordered_element_id_index(index) != second) {
        throw std::runtime_error("ordered element id codec round trip mismatch");
    }

    connection->disconnect();
    cleanup(path);
}

} // namespace

int main() {
    test_ordered_element_state_persists_live_and_tombstone_records();
    return 0;
}
