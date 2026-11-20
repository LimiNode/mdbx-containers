#include <mdbx_containers/KeyValueTable.hpp>

#include "test_assert.hpp"

#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

int main() {
    mdbxc::Config config;
    config.pathname = "data/key_value_bulk_retrieval";
    config.relative_to_exe = false;
    auto connection = mdbxc::Connection::create(config);
    mdbxc::KeyValueTable<std::string, std::string> table(connection, "bulk");
    MDBXC_TEST_ASSERT(table.retrieve_all().empty());

    table.insert_or_assign("first", "one");
    table.insert_or_assign("second", "two");
    const std::map<std::string, std::string> expected = {
        {"first", "one"}, {"second", "two"}
    };
    MDBXC_TEST_ASSERT(table.retrieve_all() == expected);
    MDBXC_TEST_ASSERT(table.retrieve_all<std::map>() == expected);
    const auto unordered = table.retrieve_all<std::unordered_map>();
    MDBXC_TEST_ASSERT(unordered.size() == expected.size());
    MDBXC_TEST_ASSERT(unordered.at("first") == "one" && unordered.at("second") == "two");

    auto transaction = connection->transaction(mdbxc::TransactionMode::READ_ONLY);
    MDBXC_TEST_ASSERT(table.retrieve_all(transaction) == expected);
    MDBXC_TEST_ASSERT(table.retrieve_all<std::map>(transaction.handle()) == expected);
    MDBXC_TEST_ASSERT(table.retrieve_all<std::unordered_map>(transaction) == unordered);

    std::map<std::string, std::string> accumulated = {{"existing", "kept"}};
    table.load<std::map>(accumulated, transaction.handle());
    MDBXC_TEST_ASSERT(accumulated.size() == 3U && accumulated.at("existing") == "kept");
    MDBXC_TEST_ASSERT(accumulated.at("first") == "one" && accumulated.at("second") == "two");

    std::vector<std::pair<std::string, std::string> > pairs;
    table.load(pairs, transaction);
    MDBXC_TEST_ASSERT(pairs ==
        (std::vector<std::pair<std::string, std::string> >(expected.begin(), expected.end())));
    transaction.commit();
}
