#include "test_assert.hpp"
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx_containers/HashedKeyValueStore.hpp>

#if __cplusplus >= 201703L
#   include <cstddef>
#endif

namespace {

struct ConstantHasher {
    std::uint64_t operator()(mdbxc::ByteView) const noexcept {
        return 42;
    }
};

template<class Store, class Key, class Value>
void assert_found(const Store& store, const Key& key, const Value& expected) {
#if __cplusplus >= 201703L
    std::optional<Value> found = store.find(key);
    MDBXC_TEST_ASSERT(found.has_value());
    MDBXC_TEST_ASSERT(*found == expected);
#else
    std::pair<bool, Value> found = store.find_compat(key);
    MDBXC_TEST_ASSERT(found.first);
    MDBXC_TEST_ASSERT(found.second == expected);
#endif
}

template<class Fn>
void assert_throws_length_error(Fn fn) {
    bool thrown = false;
    try {
        fn();
    } catch (const std::length_error&) {
        thrown = true;
    }
    MDBXC_TEST_ASSERT(thrown);
}

template<class Fn>
void assert_throws_any(Fn fn) {
    bool thrown = false;
    try {
        fn();
    } catch (const std::exception&) {
        thrown = true;
    }
    MDBXC_TEST_ASSERT(thrown);
}

} // namespace

int main() {
    mdbxc::Config cfg;
    cfg.pathname = "data/hashed_key_value_store_test.mdbx";
    cfg.max_dbs = 32;
    cfg.no_subdir = true;
    cfg.relative_to_exe = false;

    auto conn = mdbxc::Connection::create(cfg);

    {
        mdbxc::HashedKeyValueStore<std::string, int> store(conn, "hashed_strings");
        store.clear();

        MDBXC_TEST_ASSERT(store.empty());
        MDBXC_TEST_ASSERT(store.insert("alpha", 1));
        MDBXC_TEST_ASSERT(!store.insert("alpha", 11));
        store.insert_or_assign("beta", 2);
        assert_found<decltype(store), std::string, int>(store, "alpha", 1);
        MDBXC_TEST_ASSERT(store.at("beta") == 2);

        int out = 0;
        MDBXC_TEST_ASSERT(store.try_get("alpha", out));
        MDBXC_TEST_ASSERT(out == 1);
        MDBXC_TEST_ASSERT(!store.try_get("missing", out));
        MDBXC_TEST_ASSERT(store.contains("alpha"));
        MDBXC_TEST_ASSERT(!store.contains("missing"));

        store["gamma"] = 3;
        MDBXC_TEST_ASSERT(static_cast<int>(store["gamma"]) == 3);
        MDBXC_TEST_ASSERT(static_cast<int>(store["defaulted"]) == 0);
        MDBXC_TEST_ASSERT(store.count() == 4);

        std::map<std::string, int> as_map;
        store.load(as_map);
        MDBXC_TEST_ASSERT(as_map["alpha"] == 1);
        MDBXC_TEST_ASSERT(as_map["beta"] == 2);
        MDBXC_TEST_ASSERT(as_map["gamma"] == 3);

        std::vector<std::pair<std::string, int> > as_vector = store.retrieve_all<std::vector>();
        MDBXC_TEST_ASSERT(as_vector.size() == 4);

        MDBXC_TEST_ASSERT(store.erase("alpha"));
        MDBXC_TEST_ASSERT(!store.erase("alpha"));
        MDBXC_TEST_ASSERT(!store.contains("alpha"));
        MDBXC_TEST_ASSERT(store.count() == 3);
    }

    {
        mdbxc::HashedKeyValueStore<std::string, int> store(conn, "hashed_bulk");
        store.clear();

        std::map<std::string, int> source;
        source["one"] = 1;
        source["two"] = 2;
        store.reconcile(source);
        MDBXC_TEST_ASSERT(store.count() == 2);

        std::vector<std::pair<std::string, int> > additions;
        additions.push_back(std::make_pair(std::string("two"), 20));
        additions.push_back(std::make_pair(std::string("three"), 3));
        store.append(additions);
        MDBXC_TEST_ASSERT(store.at("two") == 20);
        MDBXC_TEST_ASSERT(store.at("three") == 3);

        std::vector<std::pair<std::string, int> > replacement;
        replacement.push_back(std::make_pair(std::string("two"), 200));
        replacement.push_back(std::make_pair(std::string("two"), 201));
        replacement.push_back(std::make_pair(std::string("four"), 4));
        store = replacement;
        MDBXC_TEST_ASSERT(store.count() == 2);
        MDBXC_TEST_ASSERT(store.at("two") == 201);
        MDBXC_TEST_ASSERT(store.at("four") == 4);
        MDBXC_TEST_ASSERT(!store.contains("one"));
        MDBXC_TEST_ASSERT(!store.contains("three"));
    }

    {
        mdbxc::HashedKeyValueStore<std::string, std::string, ConstantHasher> store(
            conn,
            "hashed_collisions",
            ConstantHasher()
        );
        store.clear();

        MDBXC_TEST_ASSERT(store.insert("alpha", "one"));
        MDBXC_TEST_ASSERT(store.insert("beta", "two"));
        MDBXC_TEST_ASSERT(store.insert("gamma", "three"));
        store.insert_or_assign("beta", "TWO");

        assert_found<decltype(store), std::string, std::string>(store, "alpha", "one");
        assert_found<decltype(store), std::string, std::string>(store, "beta", "TWO");
        assert_found<decltype(store), std::string, std::string>(store, "gamma", "three");
        MDBXC_TEST_ASSERT(store.erase("alpha"));
        MDBXC_TEST_ASSERT(!store.contains("alpha"));
        MDBXC_TEST_ASSERT(store.contains("beta"));
        MDBXC_TEST_ASSERT(store.count() == 2);
    }

    {
        typedef std::vector<uint8_t> Bytes;
        mdbxc::HashedKeyValueStore<Bytes, std::string> store(conn, "hashed_bytes");
        store.clear();

        Bytes first;
        first.push_back(0);
        first.push_back(1);
        first.push_back(2);

        Bytes second;
        second.push_back(0);
        second.push_back(1);
        second.push_back(3);

        MDBXC_TEST_ASSERT(store.insert(first, "first"));
        store.insert_or_assign(second, "second");
        assert_found<decltype(store), Bytes, std::string>(store, first, "first");
        assert_found<decltype(store), Bytes, std::string>(store, second, "second");

        std::map<Bytes, std::string> as_map = store.retrieve_all();
        MDBXC_TEST_ASSERT(as_map[first] == "first");
        MDBXC_TEST_ASSERT(as_map[second] == "second");
    }

    {
        typedef std::vector<char> CharBytes;
        mdbxc::HashedKeyValueStore<CharBytes, int> store(conn, "hashed_char_bytes");
        store.clear();

        CharBytes key;
        key.push_back('a');
        key.push_back('\0');
        key.push_back('b');

        store.insert_or_assign(key, 7);
        assert_found<decltype(store), CharBytes, int>(store, key, 7);
    }

#if __cplusplus >= 201703L
    {
        typedef std::vector<std::byte> StdBytes;
        mdbxc::HashedKeyValueStore<StdBytes, std::string> store(conn, "hashed_std_bytes");
        store.clear();

        StdBytes key;
        key.push_back(std::byte{0x01});
        key.push_back(std::byte{0x02});
        key.push_back(std::byte{0xff});

        store.insert_or_assign(key, "byte-key");
        assert_found<decltype(store), StdBytes, std::string>(store, key, "byte-key");

        std::map<StdBytes, std::string> as_map = store.retrieve_all();
        MDBXC_TEST_ASSERT(as_map[key] == "byte-key");
    }
#endif

    {
        mdbxc::SipHashHasher sip(UINT64_C(0x0706050403020100), UINT64_C(0x0f0e0d0c0b0a0908));
        mdbxc::HashedKeyValueStore<std::string, int, mdbxc::SipHashHasher> store(conn, "hashed_sip", sip);
        store.clear();
        store.insert_or_assign("token", 123);

        mdbxc::HashedKeyValueStore<std::string, int, mdbxc::SipHashHasher> reopened(
            conn,
            "hashed_sip",
            mdbxc::SipHashHasher(UINT64_C(0x0706050403020100), UINT64_C(0x0f0e0d0c0b0a0908))
        );
        MDBXC_TEST_ASSERT(reopened.contains("token"));
        MDBXC_TEST_ASSERT(reopened.at("token") == 123);

        mdbxc::HashedKeyValueStore<std::string, int, mdbxc::SipHashHasher> wrong_key(
            conn,
            "hashed_sip",
            mdbxc::SipHashHasher(1, 2)
        );
        MDBXC_TEST_ASSERT(wrong_key.count() == 1);
        MDBXC_TEST_ASSERT(!wrong_key.contains("token"));
    }

    {
        mdbxc::HashedKeyValueStore<std::string, std::string> store(conn, "hashed_txn");
        store.clear();

        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBXC_TEST_ASSERT(store.insert("one", "1", txn));
        store.insert_or_assign("two", "2", txn);
        txn.commit();

        auto read_txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        MDBXC_TEST_ASSERT(store.count(read_txn) == 2);
        MDBXC_TEST_ASSERT(store.at("one", read_txn) == "1");
        MDBXC_TEST_ASSERT(store.contains("two", read_txn));
        read_txn.commit();
    }

    {
        mdbxc::HashedKeyValueStore<std::string, std::string> store(conn, "hashed_large_value");
        store.clear();

        std::string large(64 * 1024, 'x');
        large[0] = 'A';
        large[large.size() - 1] = 'Z';
        store.insert_or_assign("large", large);
        MDBXC_TEST_ASSERT(store.at("large") == large);
    }

    {
        mdbxc::Config read_only_cfg;
        read_only_cfg.pathname = "data/hashed_large_read_only_test.mdbx";
        read_only_cfg.max_dbs = 4;
        read_only_cfg.no_subdir = true;
        read_only_cfg.relative_to_exe = false;

        {
            auto write_conn = mdbxc::Connection::create(read_only_cfg);
            mdbxc::HashedKeyValueStore<std::string, std::string> write_store(
                write_conn,
                "hashed_large_read_only"
            );
            write_store.clear();
            write_store.insert_or_assign("alpha", "one");
        }

        read_only_cfg.read_only = true;
        auto read_conn = mdbxc::Connection::create(read_only_cfg);
        mdbxc::HashedKeyValueStore<std::string, std::string> read_store(
            read_conn,
            "hashed_large_read_only"
        );
        MDBXC_TEST_ASSERT(read_store.at("alpha") == "one");
        MDBXC_TEST_ASSERT(read_store.contains("alpha"));

        bool write_failed = false;
        try {
            read_store.insert_or_assign("beta", "two");
        } catch (const mdbxc::MdbxException&) {
            write_failed = true;
        }
        MDBXC_TEST_ASSERT(write_failed);
        MDBXC_TEST_ASSERT(!read_store.contains("beta"));
    }

    {
        typedef mdbxc::HashedKeyValueStore<
            std::string,
            int,
            mdbxc::XXH3Hasher,
            mdbxc::HashedStoreLayout::SmallValues> SmallStore;
        SmallStore store(conn, "hashed_small_values");
        store.clear();

        MDBXC_TEST_ASSERT(store.empty());
        MDBXC_TEST_ASSERT(store.insert("a", 1));
        MDBXC_TEST_ASSERT(!store.insert("a", 11));
        store.insert_or_assign("b", 2);
        store.insert_or_assign("b", 22);
        assert_found<SmallStore, std::string, int>(store, "a", 1);
        assert_found<SmallStore, std::string, int>(store, "b", 22);
        MDBXC_TEST_ASSERT(store.contains("a"));
        MDBXC_TEST_ASSERT(store.count() == 2);

        std::map<std::string, int> as_map;
        store.load(as_map);
        MDBXC_TEST_ASSERT(as_map["a"] == 1);
        MDBXC_TEST_ASSERT(as_map["b"] == 22);

        std::vector<std::pair<std::string, int> > replacement;
        replacement.push_back(std::make_pair(std::string("b"), 200));
        replacement.push_back(std::make_pair(std::string("b"), 201));
        replacement.push_back(std::make_pair(std::string("c"), 3));
        store.reconcile(replacement);
        MDBXC_TEST_ASSERT(store.count() == 2);
        MDBXC_TEST_ASSERT(store.at("b") == 201);
        MDBXC_TEST_ASSERT(store.at("c") == 3);
        MDBXC_TEST_ASSERT(!store.contains("a"));
    }

    {
        typedef mdbxc::HashedKeyValueStore<
            std::string,
            std::string,
            ConstantHasher,
            mdbxc::HashedStoreLayout::SmallValues> SmallCollisionStore;
        SmallCollisionStore store(conn, "hashed_small_collisions", ConstantHasher());
        store.clear();

        MDBXC_TEST_ASSERT(store.insert("alpha", "one"));
        MDBXC_TEST_ASSERT(store.insert("beta", "two"));
        MDBXC_TEST_ASSERT(store.insert("gamma", "three"));
        store.insert_or_assign("beta", "TWO");

        assert_found<SmallCollisionStore, std::string, std::string>(store, "alpha", "one");
        assert_found<SmallCollisionStore, std::string, std::string>(store, "beta", "TWO");
        assert_found<SmallCollisionStore, std::string, std::string>(store, "gamma", "three");
        MDBXC_TEST_ASSERT(store.erase("beta"));
        MDBXC_TEST_ASSERT(!store.contains("beta"));
        MDBXC_TEST_ASSERT(store.contains("alpha"));
        MDBXC_TEST_ASSERT(store.contains("gamma"));
        MDBXC_TEST_ASSERT(store.count() == 2);
    }

    {
        mdbxc::Config small_cfg;
        small_cfg.pathname = "data/hashed_small_oversized_test.mdbx";
        small_cfg.max_dbs = 4;
        small_cfg.max_dupsort_value_size = 128;
        small_cfg.no_subdir = true;
        small_cfg.relative_to_exe = false;

        auto small_conn = mdbxc::Connection::create(small_cfg);
        typedef mdbxc::HashedKeyValueStore<
            std::string,
            std::string,
            mdbxc::XXH3Hasher,
            mdbxc::HashedStoreLayout::SmallValues> SmallStringStore;
        SmallStringStore store(small_conn, "hashed_small_oversized");
        store.clear();
        std::string large(1024, 'x');
        assert_throws_length_error([&store, &large]() {
            store.insert_or_assign("large", large);
        });
    }

    {
        mdbxc::Config small_cfg;
        small_cfg.pathname = "data/hashed_small_maxdbs_test.mdbx";
        small_cfg.max_dbs = 1;
        small_cfg.no_subdir = true;
        small_cfg.relative_to_exe = false;

        auto small_conn = mdbxc::Connection::create(small_cfg);
        typedef mdbxc::HashedKeyValueStore<
            std::string,
            int,
            mdbxc::XXH3Hasher,
            mdbxc::HashedStoreLayout::SmallValues> SmallStore;
        SmallStore store(small_conn, "one_dbi_small");
        store.clear();
        store.insert_or_assign("ok", 7);
        MDBXC_TEST_ASSERT(store.at("ok") == 7);
    }

    {
        mdbxc::Config large_cfg;
        large_cfg.pathname = "data/hashed_large_maxdbs_test.mdbx";
        large_cfg.max_dbs = 1;
        large_cfg.no_subdir = true;
        large_cfg.relative_to_exe = false;

        auto large_conn = mdbxc::Connection::create(large_cfg);
        assert_throws_any([&large_conn]() {
            mdbxc::HashedKeyValueStore<std::string, int> store(large_conn, "two_dbi_large");
            (void)store;
        });
    }

    std::cout << "HashedKeyValueStore test passed.\n";
    return 0;
}
