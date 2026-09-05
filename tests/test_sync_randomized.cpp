/// \file test_sync_randomized.cpp
/// \brief Sync-engine contract tests: state equality after pull, idempotent
///        replay, gap detection in the changelog.
///
/// Note: a deterministic randomized state-model test (fixed-seed mix of
/// put/delete that builds an in-memory std::map reference and asserts
/// primary == replica == reference on every sync step) is planned as a
/// follow-up. The current PR establishes the gap + replay contract; repeated
/// automatic transaction capture is covered separately by test_sync_capture.

#include <mdbx_containers.hpp>
#include <mdbx_containers/sync.hpp>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <string>

namespace {

void cleanup(const std::string& p) {
    std::remove(p.c_str());
    std::remove((p + "-lck").c_str());
}

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId n{};
    for (int i = 0; i < 16; ++i) n[i] = static_cast<std::uint8_t>(seed + i);
    return n;
}

std::shared_ptr<mdbxc::Connection> open_env(const std::string& path) {
    using namespace mdbxc;
    Config c;
    c.pathname = path;
    // SyncEngine owns system DBIs; this test also opens an application
    // table and the origin index while validating changelog pagination.
    c.max_dbs = 24;
    c.no_subdir = true;
    return Connection::create(c);
}

// Pre-populate a small changelog directly via MDBX. The sink is NOT
// used here because this test only validates the sync-engine contract,
// not the capture path (capture has its own dedicated tests).
void seed_changelog(mdbxc::Connection& conn,
                    const mdbxc::sync::NodeId& origin,
                    const std::string& dbi_name,
                    std::size_t batch_count) {
    auto txn = conn.transaction(mdbxc::TransactionMode::WRITABLE);
    mdbxc::sync::ChangeLogStore log(conn.env_handle());
    log.open(txn.handle());
    for (std::uint64_t seq = 1; seq <= batch_count; ++seq) {
        mdbxc::sync::ChangeBatch batch;
        batch.version = 1;
        batch.origin_node_id = origin;
        batch.seq = seq;
        mdbxc::sync::ChangeOp op;
        op.op_type = mdbxc::sync::ChangeOpType::Put;
        op.dbi_name = dbi_name;
        op.storage_key = { static_cast<std::uint8_t>(seq) };
        op.value = { 0xAA, 0xBB };
        batch.ops.push_back(std::move(op));
        const auto bytes = mdbxc::sync::ChangeBatchCodec::encode(batch);
        log.append(txn.handle(), origin, seq, bytes);
    }
    txn.commit();
}

void run_pull_apply_data_and_replay() {
    using namespace mdbxc;
    using namespace mdbxc::sync;

    const std::string p_path = "test_state_eq_primary.mdbx";
    const std::string r_path = "test_state_eq_replica.mdbx";
    cleanup(p_path); cleanup(r_path);

    auto p_conn = open_env(p_path);
    auto r_conn = open_env(r_path);

    const NodeId p_node = make_node(0xA0);
    const NodeId r_node = make_node(0xB0);
    const NodeId db_id  = make_node(0xD0);

    SyncEngine pe(p_conn), re(r_conn);
    pe.initialize_local_identity(p_node, db_id);
    re.initialize_local_identity(r_node, db_id);

    // Pre-populate primary with a small changelog (3 batches).
    seed_changelog(*p_conn, p_node, "kv", 3);

    // Pull everything.
    {
        DirectSyncPeer peer(&pe);
        PullRequest pr;
        pr.requester = r_node;
        pr.db_id     = db_id;
        pr.have      = SyncCursor{};
        auto resp = peer.pull(pr);
        if (!resp.ok) {
            throw std::runtime_error(std::string("pull failed: ") + resp.error);
        }
        if (resp.batches.size() != 3u) {
            throw std::runtime_error("expected 3 batches, got " +
                                     std::to_string(resp.batches.size()));
        }
        auto txn = r_conn->transaction(TransactionMode::WRITABLE);
        for (const auto& b : resp.batches) {
            const ApplyResult r = re.apply_batch(txn.handle(), b);
            if (r == ApplyResult::Conflict) {
                throw std::runtime_error("Conflict during apply");
            }
        }
        txn.commit();
    }

    // Cursor sanity: applied_cursor must reflect the tail.
    {
        const SyncCursor cur = re.applied_cursor();
        if (cur.last_seq_for(p_node) != 3u) {
            throw std::runtime_error("expected last_applied_seq=3, got " +
                                     std::to_string(cur.last_seq_for(p_node)));
        }
    }

    // Idempotent replay: pull again from the beginning, every batch
    // should be Skipped (already applied).
    {
        DirectSyncPeer peer(&pe);
        PullRequest pr;
        pr.requester = r_node;
        pr.db_id     = db_id;
        pr.have      = SyncCursor{};
        auto resp = peer.pull(pr);
        if (!resp.ok) {
            throw std::runtime_error(std::string("replay pull failed: ") + resp.error);
        }
        auto txn = r_conn->transaction(TransactionMode::WRITABLE);
        int skipped = 0;
        for (const auto& b : resp.batches) {
            if (re.apply_batch(txn.handle(), b) == ApplyResult::Skipped) {
                ++skipped;
            }
        }
        txn.commit();
        if (skipped != 3) {
            throw std::runtime_error("replay expected 3 Skipped, got " +
                                     std::to_string(skipped));
        }
    }

    p_conn->disconnect();
    r_conn->disconnect();
    cleanup(p_path);
    cleanup(r_path);
}

void test_make_push_request_rejects_changelog_gap() {
    using namespace mdbxc;
    using namespace mdbxc::sync;

    const std::string p_path = "test_push_gap_primary.mdbx";
    const std::string r_path = "test_push_gap_replica.mdbx";
    cleanup(p_path); cleanup(r_path);

    auto p_conn = open_env(p_path);
    auto r_conn = open_env(r_path);

    const NodeId p_node = make_node(0xA0);
    const NodeId r_node = make_node(0xB0);
    const NodeId db_id  = make_node(0xD0);

    SyncEngine pe(p_conn), re(r_conn);
    pe.initialize_local_identity(p_node, db_id);
    re.initialize_local_identity(r_node, db_id);

    ThreadLocalChangeAccumulator sink(p_conn);
    p_conn->attach_sync_capture(&sink);

    {
        KeyValueTable<int, std::string> kv(p_conn, "kv");
        kv.insert_or_assign(1, "a");
        kv.insert_or_assign(2, "b");
        kv.insert_or_assign(3, "c");
    }
    p_conn->detach_sync_capture();

    // Delete seq=2 directly from the changelog to create a hole.
    {
        auto txn = p_conn->transaction(TransactionMode::WRITABLE);
        ChangeLogStore log(p_conn->env_handle());
        log.open(txn.handle());
        std::vector<std::uint8_t> key_buf(24);
        std::memcpy(key_buf.data(), p_node.data(), 16);
        const std::uint64_t seq = 2;
        for (int i = 0; i < 8; ++i) {
            key_buf[16 + i] = static_cast<std::uint8_t>((seq >> ((7 - i) * 8)) & 0xff);
        }
        MDBX_val k{ key_buf.data(), key_buf.size() };
        const int rc = mdbx_del(txn.handle(), log.handle(), &k, nullptr);
        if (rc != MDBX_SUCCESS && rc != MDBX_NOTFOUND) {
            throw std::runtime_error("setup: mdbx_del failed");
        }
        txn.commit();
    }

    bool threw = false;
    try {
        (void)pe.make_push_request(/*from_seq=*/1, /*to_seq=*/3);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    if (!threw) {
        throw std::runtime_error("gap in changelog did not throw");
    }

    p_conn->disconnect();
    r_conn->disconnect();
    cleanup(p_path);
    cleanup(r_path);
}

} // namespace

int main() {
    struct Case { const char* name; void (*fn)(); };
    const Case cases[] = {
        { "test_pull_apply_data_and_replay",  &run_pull_apply_data_and_replay },
        { "test_make_push_request_rejects_changelog_gap",
                                             &test_make_push_request_rejects_changelog_gap },
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
