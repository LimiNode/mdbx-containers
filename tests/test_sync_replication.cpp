/// \file test_sync_replication.cpp
/// \brief Multi-table replication scenarios for the sync engine.

#include <mdbx_containers.hpp>
#include <mdbx_containers/sync.hpp>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <map>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

void cleanup(const std::string& p) { std::remove(p.c_str()); }

template<class KVT>
typename KVT::value_type::second_type kv_or_throw(
        const std::shared_ptr<mdbxc::Connection>& conn,
        KVT& kv, const typename KVT::value_type::first_type& key,
        const char* what) {
    typename KVT::value_type::second_type out{};
    auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
    if (!kv.try_get(key, out, txn.handle())) {
        throw std::runtime_error(std::string("missing: ") + what);
    }
    return out;
}

template<class KVT>
bool kv_has(const std::shared_ptr<mdbxc::Connection>& conn,
        KVT& kv, const typename KVT::value_type::first_type& key) {
    typename KVT::value_type::second_type out{};
    auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
    return kv.try_get(key, out, txn.handle());
}

template<class KeyTableT, class KeyT>
bool key_has(const std::shared_ptr<mdbxc::Connection>& conn,
        KeyTableT& keys, const KeyT& key) {
    auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
    return keys.contains(key, txn.handle());
}

template<class ValueT>
ValueT seq_or_throw(const std::shared_ptr<mdbxc::Connection>& conn,
                    mdbxc::SequenceTable<ValueT>& seq,
                    std::uint64_t id,
                    const char* what) {
    ValueT out{};
    auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
    if (!seq.try_get(id, out, txn.handle())) {
        throw std::runtime_error(std::string("missing: ") + what);
    }
    return out;
}

template<class ValueT>
bool seq_has(const std::shared_ptr<mdbxc::Connection>& conn,
             mdbxc::SequenceTable<ValueT>& seq,
             std::uint64_t id) {
    ValueT out{};
    auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
    return seq.try_get(id, out, txn.handle());
}

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId n{};
    for (int i = 0; i < 16; ++i) n[i] = static_cast<std::uint8_t>(seed + i);
    return n;
}

mdbxc::Embedding make_embedding(float x, float y) {
    mdbxc::Embedding e;
    e.dim = 2;
    e.values.push_back(x);
    e.values.push_back(y);
    return e;
}

mdbxc::Config cfg(const std::string& path) {
    mdbxc::Config c;
    c.pathname = path;
    c.max_dbs = 24;
    c.no_subdir = true;
    return c;
}

std::shared_ptr<mdbxc::Connection> open(const std::string& path) {
    return mdbxc::Connection::create(cfg(path));
}

std::size_t pull_all_to_replica(mdbxc::sync::SyncEngine& primary_engine,
                                mdbxc::sync::SyncEngine& replica_engine,
                                const mdbxc::sync::NodeId& sender_node,
                                const mdbxc::sync::NodeId& replica_node,
                                const mdbxc::sync::NodeId& db_id,
                                std::uint64_t max_batches = 1000) {
    mdbxc::sync::DirectSyncPeer peer(&primary_engine);
    mdbxc::sync::PullRequest request;
    request.requester = replica_node;
    request.db_id = db_id;
    request.have = replica_engine.applied_cursor();
    request.max_batches = max_batches;

    std::size_t applied = 0;
    bool has_more = false;
    do {
        const mdbxc::sync::SyncCursor before = request.have;
        const mdbxc::sync::PullResponse response = peer.pull(request);
        if (!response.ok) {
            throw std::runtime_error("pull failed: " + response.error);
        }
        if (!response.batches.empty()) {
            mdbxc::sync::PushRequest push;
            push.sender = sender_node;
            push.db_id = db_id;
            push.batches = response.batches;
            const mdbxc::sync::PushResponse pushed = replica_engine.handle_push(push);
            if (!pushed.ok) {
                throw std::runtime_error("push apply failed: " + pushed.error);
            }
            applied += response.batches.size();
        } else if (response.has_more) {
            throw std::runtime_error("pull reported has_more without batches");
        }

        has_more = response.has_more;
        request.have = replica_engine.applied_cursor();
        if (has_more &&
            request.have.last_seq_by_origin == before.last_seq_by_origin) {
            throw std::runtime_error("pull pagination made no cursor progress");
        }
    } while (has_more);

    return applied;
}

/// \brief Populates three different table types on \p conn while capture is on.
void write_three_tables(std::shared_ptr<mdbxc::Connection> conn) {
    using namespace mdbxc;
    KeyValueTable<int, std::string> kv_int(conn, "kv_int");
    kv_int.insert_or_assign(1, "one");
    kv_int.insert_or_assign(2, "two");

    KeyValueTable<std::string, std::string> kv_str(conn, "kv_str");
    kv_str.insert_or_assign("a", "alpha");
    kv_str.insert_or_assign("b", "beta");

    SequenceTable<std::string> events(conn, "events");
    events.append("e0");
    events.append("e1");
}

void verify_three_tables(std::shared_ptr<mdbxc::Connection> conn) {
    using namespace mdbxc;
    KeyValueTable<int, std::string> kv_int(conn, "kv_int");
    if (kv_or_throw(conn, kv_int, 1, "kv_int[1]") != "one")   throw std::runtime_error("kv_int[1]");
    if (kv_or_throw(conn, kv_int, 2, "kv_int[2]") != "two")   throw std::runtime_error("kv_int[2]");

    KeyValueTable<std::string, std::string> kv_str(conn, "kv_str");
    if (kv_or_throw(conn, kv_str, "a", "kv_str[a]") != "alpha") throw std::runtime_error("kv_str[a]");
    if (kv_or_throw(conn, kv_str, "b", "kv_str[b]") != "beta")  throw std::runtime_error("kv_str[b]");

    SequenceTable<std::string> events(conn, "events");
    if (events.count() != 2u) throw std::runtime_error("events.count");
}

void test_replication_pull_three_tables() {
    using namespace mdbxc;
    const std::string p = "test_rep_pull_three.mdbx";
    const std::string r = "test_rep_pull_three_replica.mdbx";
    cleanup(p); cleanup(r);

    auto primary = open(p);
    auto replica = open(r);
    sync::SyncEngine pe(primary), re(replica);
    pe.initialize_local_identity(make_node(0xA0), make_node(0xD0));
    re.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    sync::ThreadLocalChangeAccumulator sink(primary);
    primary->attach_sync_capture(&sink);
    write_three_tables(primary);
    primary->detach_sync_capture();

    sync::DirectSyncPeer peer(&pe);
    sync::PullRequest req;
    req.requester = make_node(0xB0);
    req.db_id     = make_node(0xD0);
    const sync::PullResponse resp = peer.pull(req);
    if (resp.batches.size() != 6u) {
        throw std::runtime_error("expected 6 batches, got " +
                                 std::to_string(resp.batches.size()));
    }

    {
        auto txn = replica->transaction(TransactionMode::WRITABLE);
        for (const sync::ChangeBatch& b : resp.batches) {
            if (re.apply_batch(txn.handle(), b) != sync::ApplyResult::Applied) {
                throw std::runtime_error("apply not Applied");
            }
        }
        txn.commit();
    }
    verify_three_tables(replica);

    primary->disconnect(); replica->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_push_three_tables() {
    using namespace mdbxc;
    const std::string p = "test_rep_push_three.mdbx";
    const std::string r = "test_rep_push_three_remote.mdbx";
    cleanup(p); cleanup(r);

    auto primary = open(p);
    auto remote  = open(r);
    sync::SyncEngine pe(primary), re(remote);
    pe.initialize_local_identity(make_node(0xA0), make_node(0xD0));
    re.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    sync::ThreadLocalChangeAccumulator sink(primary);
    primary->attach_sync_capture(&sink);
    write_three_tables(primary);
    primary->detach_sync_capture();

    sync::PushRequest req;
    req.sender = make_node(0xA0);
    req.db_id  = make_node(0xD0);
    {
        auto txn = primary->transaction(TransactionMode::WRITABLE);
        sync::MetaStore meta(primary->env_handle());
        meta.open(txn.handle());
        sync::ChangeLogStore log(primary->env_handle());
        log.open(txn.handle());
        const std::uint64_t last = meta.get_local_seq(txn.handle());
        std::vector<std::uint8_t> buf;
        for (std::uint64_t s = 1; s <= last; ++s) {
            if (!log.get(txn.handle(), make_node(0xA0), s, buf)) continue;
            req.batches.push_back(sync::ChangeBatchCodec::decode_exact(buf));
        }
        txn.commit();
    }

    sync::DirectSyncPeer peer(&re);
    const sync::PushResponse resp = peer.push(req);
    if (!resp.ok) throw std::runtime_error("push not ok: " + resp.error);
    verify_three_tables(remote);

    primary->disconnect(); remote->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_mixed_ops() {
    using namespace mdbxc;
    const std::string p = "test_rep_mixed.mdbx";
    const std::string r = "test_rep_mixed_replica.mdbx";
    cleanup(p); cleanup(r);

    auto primary = open(p);
    auto replica = open(r);
    sync::SyncEngine pe(primary), re(replica);
    pe.initialize_local_identity(make_node(0xA0), make_node(0xD0));
    re.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    sync::ThreadLocalChangeAccumulator sink(primary);
    primary->attach_sync_capture(&sink);
    {
        KeyValueTable<int, std::string> kv(primary, "kv_int");
        kv.insert_or_assign(1, "one");
        kv.insert_or_assign(2, "two");
        kv.insert_or_assign(3, "three");
        kv.erase(2);
    }
    primary->detach_sync_capture();

    sync::DirectSyncPeer peer(&pe);
    sync::PullRequest req; req.requester = make_node(0xB0); req.db_id = make_node(0xD0);
    const sync::PullResponse resp = peer.pull(req);

    auto txn = replica->transaction(TransactionMode::WRITABLE);
    for (const sync::ChangeBatch& b : resp.batches) {
        re.apply_batch(txn.handle(), b);
    }
    txn.commit();

    {
        KeyValueTable<int, std::string> kv(replica, "kv_int");
        if (kv_or_throw(replica, kv, 1, "kv[1]") != "one")   throw std::runtime_error("kv[1]");
        if (kv_has(replica, kv, 2))                            throw std::runtime_error("kv[2] still present");
        if (kv_or_throw(replica, kv, 3, "kv[3]") != "three") throw std::runtime_error("kv[3]");
    }

    primary->disconnect(); replica->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_value_table_singleton_key() {
    using namespace mdbxc;
    const std::string p = "test_rep_value_table.mdbx";
    const std::string r = "test_rep_value_table_replica.mdbx";
    cleanup(p); cleanup(r);

    auto primary = open(p);
    auto replica = open(r);
    sync::SyncEngine pe(primary), re(replica);
    pe.initialize_local_identity(make_node(0xA0), make_node(0xD0));
    re.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    sync::ThreadLocalChangeAccumulator sink(primary);
    sync::DirectSyncPeer peer(&pe);

    primary->attach_sync_capture(&sink);
    {
        ValueTable<int> state(primary, "state");
        state.set(42);
    }
    primary->detach_sync_capture();

    {
        sync::PullRequest req;
        req.requester = make_node(0xB0);
        req.db_id = make_node(0xD0);
        const sync::PullResponse resp = peer.pull(req);
        if (resp.batches.size() != 1u) {
            throw std::runtime_error("value table set expected one batch");
        }
        auto txn = replica->transaction(TransactionMode::WRITABLE);
        for (const sync::ChangeBatch& b : resp.batches) {
            if (re.apply_batch(txn.handle(), b) != sync::ApplyResult::Applied) {
                throw std::runtime_error("value table set apply failed");
            }
        }
        txn.commit();
    }

    {
        ValueTable<int> state(replica, "state");
        const std::pair<bool, int> found = state.find_compat();
        if (!found.first || found.second != 42) {
            throw std::runtime_error("value table set did not replicate");
        }
    }

    primary->attach_sync_capture(&sink);
    {
        ValueTable<int> state(primary, "state");
        if (!state.erase()) {
            throw std::runtime_error("value table erase unexpectedly returned false");
        }
    }
    primary->detach_sync_capture();

    {
        sync::PullRequest req;
        req.requester = make_node(0xB0);
        req.db_id = make_node(0xD0);
        const sync::SyncCursor cur = re.applied_cursor();
        for (const auto& kv : cur.last_seq_by_origin) {
            req.have.last_seq_by_origin[kv.first] = kv.second;
        }
        const sync::PullResponse resp = peer.pull(req);
        if (resp.batches.size() != 1u) {
            throw std::runtime_error("value table erase expected one batch");
        }
        auto txn = replica->transaction(TransactionMode::WRITABLE);
        for (const sync::ChangeBatch& b : resp.batches) {
            if (re.apply_batch(txn.handle(), b) != sync::ApplyResult::Applied) {
                throw std::runtime_error("value table erase apply failed");
            }
        }
        txn.commit();
    }

    {
        ValueTable<int> state(replica, "state");
        if (state.has_value()) {
            throw std::runtime_error("value table erase did not replicate");
        }
    }

    primary->disconnect(); replica->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_metadata_table_roundtrip() {
    using namespace mdbxc;
    const std::string p = "test_rep_metadata_table.mdbx";
    const std::string r = "test_rep_metadata_table_replica.mdbx";
    cleanup(p); cleanup(r);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_id = make_node(0xD0);
    auto primary = open(p);
    auto replica = open(r);
    sync::SyncEngine pe(primary), re(replica);
    pe.initialize_local_identity(primary_node, db_id);
    re.initialize_local_identity(replica_node, db_id);

    MetadataTable primary_metadata(primary, "metadata");
    sync::ThreadLocalChangeAccumulator sink(primary);
    primary->attach_sync_capture(&sink);
    {
        auto txn = primary->transaction(TransactionMode::WRITABLE);
        primary_metadata.set_string("name", "primary", txn);
        primary_metadata.set_uint32("dimension", 1536u, txn);
        primary_metadata.set_uint64("generation", 9001u, txn);
        primary_metadata.set_int64("offset", -17, txn);
        primary_metadata.set_double("threshold", 0.25, txn);
        primary_metadata.set_bool("enabled", true, txn);
        primary_metadata.set_schema_version(3u, txn);
        primary_metadata.set_string("transient", "remove", txn);
        txn.commit();
    }
    if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id) != 1u) {
        throw std::runtime_error("metadata transaction expected one sync batch");
    }

    {
        MetadataTable metadata(replica, "metadata");
        if (metadata.get_string("name") != "primary" ||
            metadata.get_uint32("dimension") != 1536u ||
            metadata.get_uint64("generation") != 9001u ||
            metadata.get_int64("offset") != -17 ||
            metadata.get_double("threshold") != 0.25 ||
            !metadata.get_bool("enabled") ||
            metadata.schema_version() != 3u ||
            metadata.get_string("transient") != "remove") {
            throw std::runtime_error("metadata typed values did not replicate");
        }
        bool mismatch_threw = false;
        try {
            (void)metadata.get_string("dimension");
        } catch (const std::invalid_argument&) {
            mismatch_threw = true;
        }
        if (!mismatch_threw) {
            throw std::runtime_error("replicated metadata type mismatch did not fail");
        }
    }

    primary_metadata.set_bool("enabled", false);
    if (!primary_metadata.erase("transient")) {
        throw std::runtime_error("metadata erase unexpectedly returned false");
    }
    if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id) != 2u) {
        throw std::runtime_error("metadata updates expected two automatic batches");
    }

    {
        MetadataTable metadata(replica, "metadata");
        if (metadata.get_bool("enabled")) {
            throw std::runtime_error("metadata bool update did not replicate");
        }
        if (metadata.get_string_or("transient", "fallback") != "fallback") {
            throw std::runtime_error("metadata erase did not replicate");
        }
    }

    primary->detach_sync_capture();
    primary->disconnect(); replica->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_sequence_table_roundtrip() {
    using namespace mdbxc;
    const std::string p = "test_rep_sequence_roundtrip.mdbx";
    const std::string r = "test_rep_sequence_roundtrip_replica.mdbx";
    cleanup(p); cleanup(r);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_id = make_node(0xD0);

    std::shared_ptr<Connection> primary = open(p);
    std::shared_ptr<Connection> replica = open(r);

    {
        sync::SyncEngine pe(primary), re(replica);
        pe.initialize_local_identity(primary_node, db_id);
        re.initialize_local_identity(replica_node, db_id);

        sync::ThreadLocalChangeAccumulator sink(primary);
        primary->attach_sync_capture(&sink);
        {
            SequenceTable<std::string> events(primary, "seq_events");
            events.insert_or_assign(7, "seven");
            events.insert_or_assign(8, "");
            const std::uint64_t id = events.append("nine");
            if (id != 9u) {
                throw std::runtime_error("sequence append id after sparse set");
            }
        }
        if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) != 3u) {
            throw std::runtime_error("sequence initial sync pulled wrong batch count");
        }
        {
            SequenceTable<std::string> events(replica, "seq_events");
            if (events.count() != 3u) {
                throw std::runtime_error("sequence initial count did not replicate");
            }
            if (seq_or_throw(replica, events, 7, "seq_events[7]") != "seven") {
                throw std::runtime_error("sequence set did not replicate");
            }
            if (seq_or_throw(replica, events, 8, "seq_events[8]") != "") {
                throw std::runtime_error("sequence empty value did not replicate");
            }
            if (seq_or_throw(replica, events, 9, "seq_events[9]") != "nine") {
                throw std::runtime_error("sequence append did not replicate");
            }
        }

        {
            SequenceTable<std::string> events(primary, "seq_events");
            events.insert_or_assign(7, "SEVEN");
            if (!events.erase(8)) {
                throw std::runtime_error("sequence erase setup failed");
            }
        }
        if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) != 2u) {
            throw std::runtime_error("sequence update sync pulled wrong batch count");
        }
        primary->detach_sync_capture();
        {
            SequenceTable<std::string> events(replica, "seq_events");
            if (events.count() != 2u) {
                throw std::runtime_error("sequence update count did not replicate");
            }
            if (seq_or_throw(replica, events, 7, "seq_events[7] update") != "SEVEN") {
                throw std::runtime_error("sequence update did not replicate");
            }
            if (seq_has(replica, events, 8)) {
                throw std::runtime_error("sequence erase did not replicate");
            }
            if (seq_or_throw(replica, events, 9, "seq_events[9] preserved") != "nine") {
                throw std::runtime_error("sequence update lost preserved value");
            }
        }
    }

    primary->disconnect();
    replica->disconnect();
    primary.reset();
    replica.reset();

    primary = open(p);
    replica = open(r);
    {
        sync::SyncEngine pe(primary), re(replica);
        pe.initialize_local_identity(primary_node, db_id);
        re.initialize_local_identity(replica_node, db_id);
        {
            SequenceTable<std::string> events(replica, "seq_events");
            if (seq_has(replica, events, 8)) {
                throw std::runtime_error("sequence restart resurrected erased value");
            }
            if (seq_or_throw(replica, events, 7, "seq_events[7] restart") != "SEVEN") {
                throw std::runtime_error("sequence restart lost updated value");
            }
        }

        sync::ThreadLocalChangeAccumulator sink(primary);
        primary->attach_sync_capture(&sink);
        {
            SequenceTable<std::string> events(primary, "seq_events");
            events.insert_or_assign(9, "NINE");
            events.insert_or_assign(10, "ten");
        }
        if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) != 2u) {
            throw std::runtime_error("sequence restart sync pulled wrong batch count");
        }
        primary->detach_sync_capture();
        {
            SequenceTable<std::string> events(replica, "seq_events");
            if (events.count() != 3u) {
                throw std::runtime_error("sequence restart count did not replicate");
            }
            if (seq_or_throw(replica, events, 9, "seq_events[9] restart") != "NINE") {
                throw std::runtime_error("sequence restart update did not replicate");
            }
            if (seq_or_throw(replica, events, 10, "seq_events[10] restart") != "ten") {
                throw std::runtime_error("sequence restart insert did not replicate");
            }
        }
    }

    primary->disconnect();
    replica->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_key_value_bulk_roundtrip() {
    using namespace mdbxc;
    const std::string p = "test_rep_kv_bulk_roundtrip.mdbx";
    const std::string r = "test_rep_kv_bulk_roundtrip_replica.mdbx";
    cleanup(p); cleanup(r);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_id = make_node(0xD0);

    std::shared_ptr<Connection> primary = open(p);
    std::shared_ptr<Connection> replica = open(r);

    {
        sync::SyncEngine pe(primary), re(replica);
        pe.initialize_local_identity(primary_node, db_id);
        re.initialize_local_identity(replica_node, db_id);

        sync::ThreadLocalChangeAccumulator sink(primary);
        primary->attach_sync_capture(&sink);

        {
            KeyValueTable<int, std::string> kv(primary, "bulk_kv");
            std::map<int, std::string> initial;
            initial[1] = "one";
            initial[2] = "two";
            initial[3] = "three";
            kv.append(initial);
        }
        if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) == 0u) {
            throw std::runtime_error("bulk append produced no replicated batches");
        }
        {
            KeyValueTable<int, std::string> kv(replica, "bulk_kv");
            if (kv_or_throw(replica, kv, 1, "bulk_kv[1]") != "one") {
                throw std::runtime_error("bulk append kv[1]");
            }
            if (kv_or_throw(replica, kv, 3, "bulk_kv[3]") != "three") {
                throw std::runtime_error("bulk append kv[3]");
            }
        }

        {
            KeyValueTable<int, std::string> kv(primary, "bulk_kv");
            std::vector<std::pair<int, std::string> > extra;
            extra.push_back(std::make_pair(2, "two updated"));
            extra.push_back(std::make_pair(4, "four"));
            kv.append(extra);
        }
        if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) == 0u) {
            throw std::runtime_error("vector append produced no replicated batches");
        }
        {
            KeyValueTable<int, std::string> kv(replica, "bulk_kv");
            if (kv_or_throw(replica, kv, 2, "bulk_kv[2]") != "two updated") {
                throw std::runtime_error("vector append did not update kv[2]");
            }
            if (kv_or_throw(replica, kv, 4, "bulk_kv[4]") != "four") {
                throw std::runtime_error("vector append did not add kv[4]");
            }
        }

        {
            KeyValueTable<int, std::string> kv(primary, "bulk_kv");
            std::map<int, std::string> replacement;
            replacement[2] = "two reconciled";
            replacement[4] = "four";
            replacement[5] = "five";
            kv.reconcile(replacement);
        }
        if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) == 0u) {
            throw std::runtime_error("reconcile produced no replicated batches");
        }
        {
            KeyValueTable<int, std::string> kv(replica, "bulk_kv");
            if (kv_has(replica, kv, 1)) {
                throw std::runtime_error("reconcile did not delete stale kv[1]");
            }
            if (kv_has(replica, kv, 3)) {
                throw std::runtime_error("reconcile did not delete stale kv[3]");
            }
            if (kv_or_throw(replica, kv, 2, "bulk_kv[2]") != "two reconciled") {
                throw std::runtime_error("reconcile did not update kv[2]");
            }
            if (kv_or_throw(replica, kv, 5, "bulk_kv[5]") != "five") {
                throw std::runtime_error("reconcile did not add kv[5]");
            }
        }

        {
            KeyValueTable<int, std::string> kv(primary, "bulk_kv");
            if (kv.erase_range(2, 4) != 2u) {
                throw std::runtime_error("kv erase_range removed wrong count");
            }
        }
        if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) == 0u) {
            throw std::runtime_error("kv erase_range produced no replicated batches");
        }
        {
            KeyValueTable<int, std::string> kv(replica, "bulk_kv");
            if (kv_has(replica, kv, 2) || kv_has(replica, kv, 4)) {
                throw std::runtime_error("kv erase_range deletes did not replicate");
            }
            if (kv_or_throw(replica, kv, 5, "bulk_kv[5]") != "five") {
                throw std::runtime_error("kv erase_range removed kv[5]");
            }
        }

        primary->detach_sync_capture();
    }

    primary->disconnect();
    replica->disconnect();
    primary.reset();
    replica.reset();

    primary = open(p);
    replica = open(r);
    {
        sync::SyncEngine pe(primary), re(replica);
        pe.initialize_local_identity(primary_node, db_id);
        re.initialize_local_identity(replica_node, db_id);

        {
            KeyValueTable<int, std::string> kv(replica, "bulk_kv");
            if (kv_has(replica, kv, 2) || kv_has(replica, kv, 4)) {
                throw std::runtime_error("restarted replica resurrected erased keys");
            }
            if (kv_or_throw(replica, kv, 5, "bulk_kv[5] after restart") != "five") {
                throw std::runtime_error("restarted replica lost kv[5]");
            }
        }

        sync::ThreadLocalChangeAccumulator sink(primary);
        primary->attach_sync_capture(&sink);
        {
            KeyValueTable<int, std::string> kv(primary, "bulk_kv");
            std::vector<std::pair<int, std::string> > after_restart;
            after_restart.push_back(std::make_pair(5, "five restarted"));
            after_restart.push_back(std::make_pair(6, "six"));
            kv.append(after_restart);
        }
        if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) == 0u) {
            throw std::runtime_error("restart append produced no replicated batches");
        }
        primary->detach_sync_capture();

        {
            KeyValueTable<int, std::string> kv(replica, "bulk_kv");
            if (kv_or_throw(replica, kv, 5, "bulk_kv[5] after restart sync") !=
                    "five restarted") {
                throw std::runtime_error("restart append did not update kv[5]");
            }
            if (kv_or_throw(replica, kv, 6, "bulk_kv[6] after restart sync") != "six") {
                throw std::runtime_error("restart append did not add kv[6]");
            }
            if (kv_has(replica, kv, 2) || kv_has(replica, kv, 4)) {
                throw std::runtime_error("restart incremental replay restored erased keys");
            }
        }
    }

    primary->disconnect();
    replica->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_key_table_range_delete_roundtrip() {
    using namespace mdbxc;
    const std::string p = "test_rep_key_range_roundtrip.mdbx";
    const std::string r = "test_rep_key_range_roundtrip_replica.mdbx";
    cleanup(p); cleanup(r);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_id = make_node(0xD0);

    auto primary = open(p);
    auto replica = open(r);
    sync::SyncEngine pe(primary), re(replica);
    pe.initialize_local_identity(primary_node, db_id);
    re.initialize_local_identity(replica_node, db_id);

    sync::ThreadLocalChangeAccumulator sink(primary);
    primary->attach_sync_capture(&sink);
    {
        KeyTable<int> keys(primary, "keys");
        std::vector<int> initial;
        initial.push_back(1);
        initial.push_back(2);
        initial.push_back(3);
        initial.push_back(4);
        initial.push_back(5);
        keys.append(initial);
    }
    if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) == 0u) {
        throw std::runtime_error("key append produced no replicated batches");
    }

    {
        KeyTable<int> keys(replica, "keys");
        if (!key_has(replica, keys, 1) || !key_has(replica, keys, 5) ||
            keys.count() != 5u) {
            throw std::runtime_error("key append did not replicate");
        }
    }

    {
        KeyTable<int> keys(primary, "keys");
        if (keys.erase_range(2, 4) != 3u) {
            throw std::runtime_error("key erase_range removed wrong count");
        }
    }
    if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) == 0u) {
        throw std::runtime_error("key erase_range produced no replicated batches");
    }
    primary->detach_sync_capture();

    {
        KeyTable<int> keys(replica, "keys");
        if (!key_has(replica, keys, 1) || !key_has(replica, keys, 5)) {
            throw std::runtime_error("key erase_range removed boundary survivors");
        }
        if (key_has(replica, keys, 2) || key_has(replica, keys, 3) ||
            key_has(replica, keys, 4)) {
            throw std::runtime_error("key erase_range deletes did not replicate");
        }
        if (keys.count() != 2u) {
            throw std::runtime_error("key erase_range left wrong count");
        }
    }

    primary->disconnect(); replica->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_vector_store_roundtrip() {
    using namespace mdbxc;
    const std::string p = "test_rep_vector_roundtrip.mdbx";
    const std::string r = "test_rep_vector_roundtrip_replica.mdbx";
    cleanup(p); cleanup(r);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_id = make_node(0xD0);
    const Embedding alpha_query = make_embedding(1.0f, 0.0f);
    const Embedding beta_query = make_embedding(0.0f, 1.0f);
    const Embedding gamma_query = make_embedding(0.6f, 0.8f);

    std::shared_ptr<Connection> primary = open(p);
    std::shared_ptr<Connection> replica = open(r);
    std::uint64_t alpha_id = 0;
    std::uint64_t beta_id = 0;

    {
        sync::SyncEngine pe(primary), re(replica);
        pe.initialize_local_identity(primary_node, db_id);
        re.initialize_local_identity(replica_node, db_id);

        sync::ThreadLocalChangeAccumulator sink(primary);
        primary->attach_sync_capture(&sink);
        {
            VectorStore store(primary, "docs");
            alpha_id = store.add(alpha_query, "alpha", "{\"kind\":\"doc\"}");
            beta_id = store.add(beta_query, "beta", "{\"kind\":\"code\"}");
            if (alpha_id != 0u || beta_id != 1u) {
                throw std::runtime_error("vector ids were not assigned sequentially");
            }
        }
        if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) != 2u) {
            throw std::runtime_error("vector add sync pulled wrong batch count");
        }
        {
            VectorStore store(replica, "docs");
            if (store.count() != 2u) {
                throw std::runtime_error("vector add count did not replicate");
            }
            const std::vector<SearchResult> results = store.search(alpha_query, 1);
            if (results.size() != 1u || results[0].id != alpha_id ||
                results[0].text != "alpha" ||
                results[0].metadata_json != "{\"kind\":\"doc\"}") {
                throw std::runtime_error("vector add search did not replicate");
            }
        }

        {
            VectorStore store(primary, "docs");
            if (!store.erase(beta_id)) {
                throw std::runtime_error("vector erase setup failed");
            }
        }
        if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) != 1u) {
            throw std::runtime_error("vector erase sync pulled wrong batch count");
        }
        primary->detach_sync_capture();
        {
            VectorStore store(replica, "docs");
            if (store.count() != 1u) {
                throw std::runtime_error("vector erase count did not replicate");
            }
            const std::vector<SearchResult> results = store.search(beta_query, 10);
            for (std::size_t i = 0; i < results.size(); ++i) {
                if (results[i].id == beta_id) {
                    throw std::runtime_error("vector erase did not remove beta id");
                }
            }
        }
    }

    primary->disconnect();
    replica->disconnect();
    primary.reset();
    replica.reset();

    primary = open(p);
    replica = open(r);
    {
        sync::SyncEngine pe(primary), re(replica);
        pe.initialize_local_identity(primary_node, db_id);
        re.initialize_local_identity(replica_node, db_id);
        {
            VectorStore store(replica, "docs");
            if (store.count() != 1u) {
                throw std::runtime_error("vector restart count mismatch");
            }
            const std::vector<SearchResult> results = store.search(alpha_query, 1);
            if (results.size() != 1u || results[0].id != alpha_id ||
                results[0].text != "alpha") {
                throw std::runtime_error("vector restart rebuild lost alpha");
            }
        }

        sync::ThreadLocalChangeAccumulator sink(primary);
        primary->attach_sync_capture(&sink);
        std::uint64_t gamma_id = 0;
        {
            VectorStore store(primary, "docs");
            gamma_id = store.add(gamma_query, "gamma", "{\"kind\":\"note\"}");
            if (gamma_id != 2u) {
                throw std::runtime_error("vector restart add used unexpected id");
            }
        }
        if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1) != 1u) {
            throw std::runtime_error("vector restart sync pulled wrong batch count");
        }
        primary->detach_sync_capture();
        {
            VectorStore store(replica, "docs");
            if (store.count() != 2u) {
                throw std::runtime_error("vector restart add count did not replicate");
            }
            const std::vector<SearchResult> results = store.search(gamma_query, 1);
            if (results.size() != 1u || results[0].id != gamma_id ||
                results[0].text != "gamma" ||
                results[0].metadata_json != "{\"kind\":\"note\"}") {
                throw std::runtime_error("vector restart add search did not replicate");
            }
        }
    }

    primary->disconnect();
    replica->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_clear_table_roundtrip() {
    using namespace mdbxc;
    const std::string p = "test_rep_clear_roundtrip.mdbx";
    const std::string r = "test_rep_clear_roundtrip_replica.mdbx";
    cleanup(p); cleanup(r);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_id = make_node(0xD0);

    std::shared_ptr<Connection> primary = open(p);
    std::shared_ptr<Connection> replica = open(r);

    sync::SyncEngine pe(primary), re(replica);
    pe.initialize_local_identity(primary_node, db_id);
    re.initialize_local_identity(replica_node, db_id);

    sync::ThreadLocalChangeAccumulator sink(primary);
    primary->attach_sync_capture(&sink);

    {
        KeyValueTable<int, std::string> kv(primary, "clear_kv");
        kv.insert_or_assign(1, "one");
        kv.insert_or_assign(2, "two");

        KeyTable<int> keys(primary, "clear_keys");
        keys.insert(1);
        keys.insert(2);

        ValueTable<int> value(primary, "clear_value");
        value.set(42);

        SequenceTable<std::string> seq(primary, "clear_seq");
        seq.append("one");
        seq.append("two");

        VectorStore vectors(primary, "clear_vectors");
        vectors.add(make_embedding(1.0f, 0.0f), "one", "{}");
        vectors.add(make_embedding(0.0f, 1.0f), "two", "{}");
    }
    if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 2) == 0u) {
        throw std::runtime_error("initial clear roundtrip sync pulled no batches");
    }

    {
        KeyValueTable<int, std::string> kv(replica, "clear_kv");
        KeyTable<int> keys(replica, "clear_keys");
        ValueTable<int> value(replica, "clear_value");
        SequenceTable<std::string> seq(replica, "clear_seq");
        if (kv.count() != 2u || keys.count() != 2u || !value.has_value() ||
            seq.count() != 2u) {
            throw std::runtime_error("initial clear roundtrip state did not replicate");
        }
    }

    VectorStore live_vectors(replica, "clear_vectors");
    if (live_vectors.count() != 2u) {
        throw std::runtime_error("initial vector clear roundtrip state did not replicate");
    }
    if (live_vectors.search(make_embedding(1.0f, 0.0f), 10).empty()) {
        throw std::runtime_error("initial vector clear roundtrip search is empty");
    }

    {
        KeyValueTable<int, std::string> kv(primary, "clear_kv");
        kv.clear();

        KeyTable<int> keys(primary, "clear_keys");
        keys.clear();

        ValueTable<int> value(primary, "clear_value");
        value.clear();

        SequenceTable<std::string> seq(primary, "clear_seq");
        seq.clear();

        VectorStore vectors(primary, "clear_vectors");
        vectors.clear();
    }
    if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 2) == 0u) {
        throw std::runtime_error("clear roundtrip sync pulled no batches");
    }
    primary->detach_sync_capture();

    {
        KeyValueTable<int, std::string> kv(replica, "clear_kv");
        KeyTable<int> keys(replica, "clear_keys");
        ValueTable<int> value(replica, "clear_value");
        SequenceTable<std::string> seq(replica, "clear_seq");
        VectorStore vectors(replica, "clear_vectors");
        if (kv.count() != 0u ||
            keys.count() != 0u ||
            value.has_value() ||
            seq.count() != 0u ||
            vectors.count() != 0u) {
            throw std::runtime_error("ClearTable batch did not clear replica state");
        }
    }
    if (live_vectors.count() != 0u ||
        !live_vectors.search(make_embedding(1.0f, 0.0f), 10).empty()) {
        throw std::runtime_error(
            "VectorStore did not auto-refresh after remote ClearTable apply");
    }

    primary->disconnect();
    replica->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_wrapper_clear_roundtrips_independently() {
    using namespace mdbxc;
    const std::string p = "test_rep_wrapper_clear_roundtrip.mdbx";
    const std::string r = "test_rep_wrapper_clear_roundtrip_replica.mdbx";
    cleanup(p); cleanup(r);

    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_id = make_node(0xD0);

    std::shared_ptr<Connection> primary = open(p);
    std::shared_ptr<Connection> replica = open(r);

    sync::SyncEngine pe(primary), re(replica);
    pe.initialize_local_identity(primary_node, db_id);
    re.initialize_local_identity(replica_node, db_id);

    sync::ThreadLocalChangeAccumulator sink(primary);
    primary->attach_sync_capture(&sink);

    {
        KeyValueTable<int, std::string> kv(primary, "clear_each_kv");
        kv.insert_or_assign(1, "one");

        KeyTable<int> keys(primary, "clear_each_keys");
        keys.insert(2);

        ValueTable<int> value(primary, "clear_each_value");
        value.set(3);

        SequenceTable<std::string> seq(primary, "clear_each_seq");
        seq.append("four");
    }
    if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id) == 0u) {
        throw std::runtime_error("initial wrapper clear sync pulled no batches");
    }

    {
        KeyValueTable<int, std::string> kv(primary, "clear_each_kv");
        kv.clear();
    }
    if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id) != 1u) {
        throw std::runtime_error("KeyValueTable clear did not replicate one batch");
    }
    {
        KeyValueTable<int, std::string> kv(replica, "clear_each_kv");
        KeyTable<int> keys(replica, "clear_each_keys");
        ValueTable<int> value(replica, "clear_each_value");
        SequenceTable<std::string> seq(replica, "clear_each_seq");
        if (kv.count() != 0u || !key_has(replica, keys, 2) ||
            !value.has_value() || seq.count() != 1u) {
            throw std::runtime_error(
                "KeyValueTable clear affected wrong replica state");
        }
    }

    {
        KeyTable<int> keys(primary, "clear_each_keys");
        keys.clear();
    }
    if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id) != 1u) {
        throw std::runtime_error("KeyTable clear did not replicate one batch");
    }
    {
        KeyTable<int> keys(replica, "clear_each_keys");
        ValueTable<int> value(replica, "clear_each_value");
        SequenceTable<std::string> seq(replica, "clear_each_seq");
        if (keys.count() != 0u || !value.has_value() || seq.count() != 1u) {
            throw std::runtime_error(
                "KeyTable clear affected wrong replica state");
        }
    }

    {
        ValueTable<int> value(primary, "clear_each_value");
        value.clear();
    }
    if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id) != 1u) {
        throw std::runtime_error("ValueTable clear did not replicate one batch");
    }
    {
        ValueTable<int> value(replica, "clear_each_value");
        SequenceTable<std::string> seq(replica, "clear_each_seq");
        if (value.has_value() || seq.count() != 1u) {
            throw std::runtime_error(
                "ValueTable clear affected wrong replica state");
        }
    }

    {
        SequenceTable<std::string> seq(primary, "clear_each_seq");
        seq.clear();
    }
    if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id) != 1u) {
        throw std::runtime_error("SequenceTable clear did not replicate one batch");
    }
    {
        SequenceTable<std::string> seq(replica, "clear_each_seq");
        if (seq.count() != 0u) {
            throw std::runtime_error("SequenceTable clear did not replicate");
        }
    }

    primary->detach_sync_capture();
    primary->disconnect();
    replica->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_incremental_pull() {
    using namespace mdbxc;
    const std::string p = "test_rep_incr.mdbx";
    const std::string r = "test_rep_incr_replica.mdbx";
    cleanup(p); cleanup(r);

    auto primary = open(p);
    auto replica = open(r);
    sync::SyncEngine pe(primary), re(replica);
    pe.initialize_local_identity(make_node(0xA0), make_node(0xD0));
    re.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    sync::ThreadLocalChangeAccumulator sink(primary);
    sync::DirectSyncPeer peer(&pe);

    // Round 1: 2 batches on primary
    primary->attach_sync_capture(&sink);
    {
        KeyValueTable<int, int> kv(primary, "kv");
        kv.insert_or_assign(1, 100);
        kv.insert_or_assign(2, 200);
    }
    primary->detach_sync_capture();

    {
        sync::PullRequest req; req.requester = make_node(0xB0); req.db_id = make_node(0xD0);
        const sync::PullResponse resp = peer.pull(req);
        if (resp.batches.size() != 2u) {
            throw std::runtime_error("round1 expected 2 batches");
        }
        auto txn = replica->transaction(TransactionMode::WRITABLE);
        for (const sync::ChangeBatch& b : resp.batches) re.apply_batch(txn.handle(), b);
        txn.commit();
    }

    // Round 2: 2 more batches on primary, replica's cursor now points at seq=2.
    primary->attach_sync_capture(&sink);
    {
        KeyValueTable<int, int> kv(primary, "kv");
        kv.insert_or_assign(3, 300);
        kv.insert_or_assign(4, 400);
    }
    primary->detach_sync_capture();

    {
        sync::PullRequest req;
        req.requester = make_node(0xB0);
        req.db_id     = make_node(0xD0);
        const sync::SyncCursor cur = re.applied_cursor();
        for (const auto& kv : cur.last_seq_by_origin) {
            req.have.last_seq_by_origin[kv.first] = kv.second;
        }
        const sync::PullResponse resp = peer.pull(req);
        if (resp.batches.size() != 2u) {
            throw std::runtime_error("round2 expected 2 batches, got " +
                                     std::to_string(resp.batches.size()));
        }
        auto txn = replica->transaction(TransactionMode::WRITABLE);
        for (const sync::ChangeBatch& b : resp.batches) re.apply_batch(txn.handle(), b);
        txn.commit();
    }

    {
        KeyValueTable<int, int> kv(replica, "kv");
        if (kv_or_throw(replica, kv, 1, "kv[1]") != 100) throw std::runtime_error("kv[1]");
        if (kv_or_throw(replica, kv, 4, "kv[4]") != 400) throw std::runtime_error("kv[4]");
    }

    primary->disconnect(); replica->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_make_push_request_helper() {
    using namespace mdbxc;
    const std::string p = "test_rep_make_push_helper.mdbx";
    const std::string r = "test_rep_make_push_helper_replica.mdbx";
    cleanup(p); cleanup(r);

    auto primary = open(p);
    auto replica = open(r);
    sync::SyncEngine pe(primary), re(replica);
    pe.initialize_local_identity(make_node(0xA0), make_node(0xD0));
    re.initialize_local_identity(make_node(0xB0), make_node(0xD0));

    sync::ThreadLocalChangeAccumulator sink(primary);
    primary->attach_sync_capture(&sink);
    {
        KeyValueTable<int, std::string> kv(primary, "kv");
        kv.insert_or_assign(1, "a");
        kv.insert_or_assign(2, "b");
        kv.insert_or_assign(3, "c");
    }
    primary->detach_sync_capture();

    // Empty range -> empty request.
    {
        const sync::PushRequest empty = pe.make_push_request(
            /*from_seq=*/100, /*to_seq=*/0);
        if (!empty.batches.empty()) {
            throw std::runtime_error("empty range produced non-empty push");
        }
    }

    // Full window (from 1 to local tail).
    {
        const sync::PushRequest full = pe.make_push_request(
            /*from_seq=*/1, /*to_seq=*/0);
        if (full.batches.size() != 3u) {
            throw std::runtime_error("full window wrong size: " +
                                     std::to_string(full.batches.size()));
        }
        if (full.sender != make_node(0xA0) || full.db_id != make_node(0xD0)) {
            throw std::runtime_error("push request header fields wrong");
        }
        // End-to-end: helper -> DirectSyncPeer -> replica engine -> kv.
        sync::DirectSyncPeer peer(&re);
        const sync::PushResponse resp = peer.push(full);
        if (!resp.ok) { throw std::runtime_error("push via helper failed: " + resp.error); }
        KeyValueTable<int, std::string> kv_re(replica, "kv");
        if (kv_or_throw(replica, kv_re, 1, "kv[1] after helper push") != "a") throw std::runtime_error("kv[1] != a");
        if (kv_or_throw(replica, kv_re, 2, "kv[2] after helper push") != "b") throw std::runtime_error("kv[2] != b");
        if (kv_or_throw(replica, kv_re, 3, "kv[3] after helper push") != "c") throw std::runtime_error("kv[3] != c");
    }

    // Partial window (from 2 to 3).
    {
        const sync::PushRequest mid = pe.make_push_request(
            /*from_seq=*/2, /*to_seq=*/3);
        if (mid.batches.size() != 2u) {
            throw std::runtime_error("partial window wrong size: " +
                                     std::to_string(mid.batches.size()));
        }
    }

    // Reversed range (to_seq < from_seq) -> empty.
    {
        const sync::PushRequest rev = pe.make_push_request(
            /*from_seq=*/5, /*to_seq=*/2);
        if (!rev.batches.empty()) {
            throw std::runtime_error("reversed range produced non-empty push");
        }
    }

    // Gap in changelog -> exception, not silent skip.
    // Delete seq 2 directly via MDBX, then ask for [1, 3].
    {
        auto erase_txn = primary->transaction(mdbxc::TransactionMode::WRITABLE);
        sync::ChangeLogStore log(primary->env_handle());
        log.open(erase_txn.handle());
        const sync::NodeId local = make_node(0xA0);
        std::vector<std::uint8_t> key_buf(24);
        std::memcpy(key_buf.data(), local.data(), 16);
        const std::uint64_t seq = 2;
        for (int i = 0; i < 8; ++i) {
            key_buf[16 + i] = static_cast<std::uint8_t>((seq >> ((7 - i) * 8)) & 0xff);
        }
        MDBX_val k{ key_buf.data(), key_buf.size() };
        const int del_rc = mdbx_del(erase_txn.handle(), log.handle(), &k, nullptr);
        if (del_rc != MDBX_SUCCESS && del_rc != MDBX_NOTFOUND) {
            throw std::runtime_error("test setup: mdbx_del failed");
        }
        erase_txn.commit();

        bool threw = false;
        try {
            (void)pe.make_push_request(/*from_seq=*/1, /*to_seq=*/3);
        } catch (const std::runtime_error&) {
            threw = true;
        }
        if (!threw) {
            throw std::runtime_error("gap in changelog did not throw");
        }
    }

    primary->disconnect(); replica->disconnect();
    cleanup(p); cleanup(r);
}

void test_replication_idempotent_apply() {
    using namespace mdbxc;
    const std::string r = "test_rep_idempotent.mdbx";
    cleanup(r);

    auto conn = open(r);
    sync::SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x10), make_node(0xD0));

    sync::ChangeBatch b;
    b.origin_node_id = make_node(0x20);
    b.seq = 1;
    sync::ChangeOp op;
    op.op_type = sync::ChangeOpType::Put;
    op.dbi_name = "t";
    op.storage_key = { 0x42 };
    op.value = { 0xAB };
    b.ops.push_back(op);

    auto txn = conn->transaction(TransactionMode::WRITABLE);
    if (engine.apply_batch(txn.handle(), b) != sync::ApplyResult::Applied) {
        throw std::runtime_error("first apply != Applied");
    }
    if (engine.apply_batch(txn.handle(), b) != sync::ApplyResult::Skipped) {
        throw std::runtime_error("second apply != Skipped");
    }
    if (engine.apply_batch(txn.handle(), b) != sync::ApplyResult::Skipped) {
        throw std::runtime_error("third apply != Skipped");
    }
    txn.commit();

    conn->disconnect();
    cleanup(r);
}

void test_vector_store_search_during_remote_apply() {
    using namespace mdbxc;
    const std::string p = "test_rep_vector_barrier_primary.mdbx";
    const std::string r = "test_rep_vector_barrier_replica.mdbx";
    cleanup(p); cleanup(r);

    auto primary = open(p);
    auto replica = open(r);
    const sync::NodeId primary_node = make_node(0xA0);
    const sync::NodeId replica_node = make_node(0xB0);
    const sync::NodeId db_id = make_node(0xD0);
    sync::SyncEngine pe(primary), re(replica);
    pe.initialize_local_identity(primary_node, db_id);
    re.initialize_local_identity(replica_node, db_id);

    sync::ThreadLocalChangeAccumulator sink(primary);
    primary->attach_sync_capture(&sink);
    const std::size_t record_count = 128;
    const std::string metadata(2048, 'm');
    {
        VectorStore vectors(primary, "barrier_vectors");
        vectors.clear();
        for (std::size_t i = 0; i < record_count; ++i) {
            const float x = static_cast<float>(i + 1u);
            vectors.add(make_embedding(x, 1.0f), "row-" + std::to_string(i),
                        metadata);
        }
    }
    if (pull_all_to_replica(pe, re, primary_node, replica_node, db_id, 1000) == 0u) {
        throw std::runtime_error("initial vector barrier sync pulled no batches");
    }

    {
        VectorStore vectors(primary, "barrier_vectors");
        vectors.clear();
    }
    sync::DirectSyncPeer peer(&pe);
    sync::PullRequest request;
    request.requester = replica_node;
    request.db_id = db_id;
    request.have = re.applied_cursor();
    request.max_batches = 1000;
    const sync::PullResponse response = peer.pull(request);
    if (!response.ok || response.batches.empty()) {
        throw std::runtime_error("vector barrier clear pull failed");
    }
    sync::PushRequest push;
    push.sender = primary_node;
    push.db_id = db_id;
    push.batches = response.batches;

    VectorStore live_vectors(replica, "barrier_vectors");
    const Embedding query = make_embedding(1.0f, 1.0f);
    std::atomic<bool> search_started(false);
    std::atomic<bool> stop_search(false);
    std::exception_ptr search_error;
    std::thread search_thread(
        [&live_vectors, &query, &search_started, &stop_search,
         &search_error, record_count]() {
            search_started.store(true);
            try {
                while (!stop_search.load()) {
                    const std::vector<SearchResult> results =
                        live_vectors.search(query, record_count);
                    if (!results.empty() && results.size() != record_count) {
                        throw std::runtime_error(
                            "VectorStore concurrent search saw partial result");
                    }
                    std::this_thread::yield();
                }
            } catch (...) {
                search_error = std::current_exception();
                stop_search.store(true);
            }
        });

    while (!search_started.load()) {
        std::this_thread::yield();
    }

    sync::PushResponse pushed;
    try {
        pushed = re.handle_push(push);
    } catch (...) {
        stop_search.store(true);
        search_thread.join();
        throw;
    }
    stop_search.store(true);
    search_thread.join();
    if (search_error) {
        std::rethrow_exception(search_error);
    }
    if (!pushed.ok) {
        throw std::runtime_error("vector barrier push failed: " + pushed.error);
    }
    if (!live_vectors.search(query, record_count).empty()) {
        throw std::runtime_error("VectorStore barrier search did not refresh after apply");
    }

    primary->detach_sync_capture();
    primary->disconnect(); replica->disconnect();
    cleanup(p); cleanup(r);
}

} // namespace

int main() {
    struct Case { const char* name; void (*fn)(); };
    const Case cases[] = {
        { "test_replication_pull_three_tables",  &test_replication_pull_three_tables },
        { "test_replication_push_three_tables",  &test_replication_push_three_tables },
        { "test_replication_mixed_ops",          &test_replication_mixed_ops },
        { "test_replication_value_table_singleton_key", &test_replication_value_table_singleton_key },
        { "test_replication_metadata_table_roundtrip",
          &test_replication_metadata_table_roundtrip },
        { "test_replication_sequence_table_roundtrip",
          &test_replication_sequence_table_roundtrip },
        { "test_replication_key_value_bulk_roundtrip", &test_replication_key_value_bulk_roundtrip },
        { "test_replication_key_table_range_delete_roundtrip",
          &test_replication_key_table_range_delete_roundtrip },
        { "test_replication_vector_store_roundtrip",
          &test_replication_vector_store_roundtrip },
        { "test_replication_clear_table_roundtrip",
          &test_replication_clear_table_roundtrip },
        { "test_replication_wrapper_clear_roundtrips_independently",
          &test_replication_wrapper_clear_roundtrips_independently },
        { "test_replication_incremental_pull",   &test_replication_incremental_pull },
        { "test_replication_idempotent_apply",   &test_replication_idempotent_apply },
        { "test_replication_make_push_request", &test_replication_make_push_request_helper },
        { "test_vector_store_search_during_remote_apply",
          &test_vector_store_search_during_remote_apply },
    };
    int rc = 0;
    for (std::size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        try {
            cases[i].fn();
            std::printf("PASS %s\n", cases[i].name);
        } catch (const std::exception& e) {
            std::printf("FAIL %s: %s\n", cases[i].name, e.what());
            rc = static_cast<int>(i + 1);
        } catch (...) {
            std::printf("FAIL %s: non-std exception\n", cases[i].name);
            rc = static_cast<int>(i + 1);
        }
    }
    return rc;
}
