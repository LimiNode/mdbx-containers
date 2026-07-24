#include <mdbx_containers.hpp>
#include <mdbx_containers/sync.hpp>
#include <algorithm>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void cleanup(const std::string& p) {
    std::remove(p.c_str());
}

mdbxc::sync::NodeId make_node(std::uint8_t seed) {
    mdbxc::sync::NodeId n{};
    for (int i = 0; i < 16; ++i) {
        n[i] = static_cast<std::uint8_t>(seed + i);
    }
    return n;
}

std::vector<std::uint8_t> make_changelog_key(const mdbxc::sync::NodeId& origin,
                                             std::uint64_t seq) {
    std::vector<std::uint8_t> out(24);
    std::memcpy(out.data(), origin.data(), 16);
    for (int i = 0; i < 8; ++i) {
        out[16 + i] =
            static_cast<std::uint8_t>((seq >> ((7 - i) * 8)) & 0xff);
    }
    return out;
}

void put_raw_changelog(MDBX_txn* txn,
                       MDBX_dbi dbi,
                       const mdbxc::sync::NodeId& origin,
                       std::uint64_t seq,
                       const std::vector<std::uint8_t>& bytes) {
    std::vector<std::uint8_t> key = make_changelog_key(origin, seq);
    MDBX_val k = { key.empty() ? nullptr : &key[0], key.size() };
    MDBX_val v = { bytes.empty() ? nullptr : const_cast<std::uint8_t*>(&bytes[0]),
                   bytes.size() };
    mdbxc::check_mdbx(mdbx_put(txn, dbi, &k, &v, MDBX_NOOVERWRITE),
                      "raw changelog put failed");
}

void append_schema_string(std::vector<std::uint8_t>& out,
                          const std::string& value) {
    mdbxc::sync::detail::append_u32_le(out,
        static_cast<std::uint32_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}

std::vector<std::uint8_t> make_schema_record_bytes(
        const std::string& embedded_schema_id,
        std::uint16_t kind,
        std::uint32_t schema_version,
        std::uint32_t flags,
        const std::string& dbi_name,
        const std::vector<std::string>& dbi_names) {
    std::vector<std::uint8_t> out;
    append_schema_string(out, embedded_schema_id);
    mdbxc::sync::detail::append_u16_le(out, kind);
    mdbxc::sync::detail::append_u32_le(out, schema_version);
    mdbxc::sync::detail::append_u32_le(out, flags);
    append_schema_string(out, dbi_name);
    mdbxc::sync::detail::append_u32_le(out,
        static_cast<std::uint32_t>(dbi_names.size()));
    for (std::size_t i = 0; i < dbi_names.size(); ++i) {
        append_schema_string(out, dbi_names[i]);
    }
    return out;
}

std::vector<std::uint8_t> make_schema_record_bytes_with_count(
        const std::string& embedded_schema_id,
        std::uint16_t kind,
        std::uint32_t schema_version,
        std::uint32_t flags,
        const std::string& dbi_name,
        std::uint32_t dbi_names_count) {
    std::vector<std::uint8_t> out;
    append_schema_string(out, embedded_schema_id);
    mdbxc::sync::detail::append_u16_le(out, kind);
    mdbxc::sync::detail::append_u32_le(out, schema_version);
    mdbxc::sync::detail::append_u32_le(out, flags);
    append_schema_string(out, dbi_name);
    mdbxc::sync::detail::append_u32_le(out, dbi_names_count);
    return out;
}

void put_raw_schema_record(MDBX_txn* txn,
                           MDBX_dbi dbi,
                           const std::string& schema_id,
                           const std::vector<std::uint8_t>& bytes) {
    MDBX_val key = {
        const_cast<char*>(schema_id.data()),
        schema_id.size()
    };
    MDBX_val value = {
        bytes.empty() ? nullptr : const_cast<std::uint8_t*>(&bytes[0]),
        bytes.size()
    };
    mdbxc::check_mdbx(mdbx_put(txn, dbi, &key, &value, MDBX_UPSERT),
                      "raw schema registry put failed");
}

void test_meta_store() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_meta.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    MetaStore store(conn->env_handle());
    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        store.open(txn.handle());
        if (!store.is_open()) throw std::runtime_error("MetaStore not open");

        const NodeId db_uuid = make_node(0xA0);
        const NodeId node_id = make_node(0xB0);
        store.set_db_uuid(txn.handle(), db_uuid);
        store.set_node_id(txn.handle(), node_id);
        store.set_schema_version(txn.handle(), meta_schema_version());
        store.set_created_at_ms(txn.handle(), 1700000000000ULL);
        const std::uint64_t next = store.increment_local_seq(txn.handle());
        if (next != 1) throw std::runtime_error("local_seq start should be 1");
        store.increment_local_seq(txn.handle());
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        const NodeId db_uuid = store.get_db_uuid(txn.handle());
        const NodeId node_id = store.get_node_id(txn.handle());
        if (db_uuid != make_node(0xA0)) throw std::runtime_error("db_uuid mismatch");
        if (node_id != make_node(0xB0)) throw std::runtime_error("node_id mismatch");
        if (store.get_schema_version(txn.handle()) != meta_schema_version()) {
            throw std::runtime_error("schema_version mismatch");
        }
        if (store.get_local_seq(txn.handle()) != 2) {
            throw std::runtime_error("local_seq should be 2 after two increments");
        }
        if (store.get_created_at_ms(txn.handle()) != 1700000000000ULL) {
            throw std::runtime_error("created_at_ms mismatch");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_origin_index_store() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_origins.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    OriginIndexStore store(conn->env_handle());
    const NodeId origin_a = make_node(0x10);
    const NodeId origin_b = make_node(0x20);

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        store.open(txn.handle());
        if (!store.empty(txn.handle())) {
            throw std::runtime_error("new origin index should be empty");
        }
        store.note_origin(txn.handle(), origin_a, 7);
        store.note_origin(txn.handle(), origin_b, 3);
        store.note_origin(txn.handle(), origin_a, 2);
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        OriginIndexStore ro(conn->env_handle());
        if (!ro.open_existing(txn.handle())) {
            throw std::runtime_error("origin index should exist");
        }
        std::uint64_t seq = 0;
        if (!ro.last_seq(txn.handle(), origin_a, seq) || seq != 7u) {
            throw std::runtime_error("origin_a last seq mismatch");
        }
        if (!ro.last_seq(txn.handle(), origin_b, seq) || seq != 3u) {
            throw std::runtime_error("origin_b last seq mismatch");
        }
        const std::vector<NodeId> origins = ro.origins(txn.handle());
        if (origins.size() != 2u ||
            origins[0] != origin_a ||
            origins[1] != origin_b) {
            throw std::runtime_error("origin index order mismatch");
        }
        const std::vector<OriginIndexStore::OriginTail> tails =
            ro.origin_tails(txn.handle());
        if (tails.size() != 2u ||
            tails[0].origin != origin_a ||
            tails[0].last_seq != 7u ||
            tails[1].origin != origin_b ||
            tails[1].last_seq != 3u) {
            throw std::runtime_error("origin index tail mismatch");
        }
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        OriginIndexStore writable(conn->env_handle());
        writable.open(txn.handle());
        const std::size_t removed = writable.clear(txn.handle());
        if (removed != 2u || !writable.empty(txn.handle())) {
            throw std::runtime_error("origin index clear mismatch");
        }
        txn.commit();
    }

    conn->disconnect();
    cleanup(p);
}

void test_changelog_store() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_changelog.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    ChangeLogStore store(conn->env_handle());
    const NodeId origin = make_node(0x10);

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        store.open(txn.handle());

        ChangeBatch batch;
        batch.origin_node_id = origin;
        batch.seq = 7;
        const std::vector<std::uint8_t> bytes = ChangeBatchCodec::encode(batch);
        store.append(txn.handle(), origin, 7, bytes);

        if (!store.contains(txn.handle(), origin, 7)) {
            throw std::runtime_error("contains should be true after append");
        }
        if (store.contains(txn.handle(), origin, 8)) {
            throw std::runtime_error("contains should be false for missing seq");
        }
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        std::vector<std::uint8_t> out;
        if (!store.get(txn.handle(), origin, 7, out)) {
            throw std::runtime_error("get should return true");
        }
        const ChangeBatch decoded = ChangeBatchCodec::decode_exact(out);
        if (decoded.seq != 7) throw std::runtime_error("decoded seq mismatch");
        if (decoded.origin_node_id != origin) {
            throw std::runtime_error("decoded origin mismatch");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_changelog_updates_origin_index() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_changelog_origins.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    ChangeLogStore log(conn->env_handle());
    const NodeId origin = make_node(0x30);

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        log.open(txn.handle());
        log.append(txn.handle(), origin, 1, { 0x01 });
        log.append(txn.handle(), origin, 3, { 0x03 });
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        OriginIndexStore origins(conn->env_handle());
        if (!origins.open_existing(txn.handle())) {
            throw std::runtime_error("origin index should exist after append");
        }
        std::uint64_t seq = 0;
        if (!origins.last_seq(txn.handle(), origin, seq) || seq != 3u) {
            throw std::runtime_error("changelog did not update origin tail");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_changelog_backfills_legacy_origins_on_append() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_changelog_origin_backfill.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    const NodeId origin_a = make_node(0x10);
    const NodeId origin_b = make_node(0x40);

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBX_dbi raw = 0;
        mdbxc::check_mdbx(mdbx_dbi_open(txn.handle(), "_mdbxc_changelog",
                                        MDBX_CREATE, &raw),
                          "open raw changelog failed");
        put_raw_changelog(txn.handle(), raw, origin_a, 1, { 0xA1 });
        put_raw_changelog(txn.handle(), raw, origin_a, 2, { 0xA2 });

        ChangeLogStore log(conn->env_handle());
        log.open(txn.handle());
        log.append(txn.handle(), origin_b, 1, { 0xB1 });
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        OriginIndexStore origins(conn->env_handle());
        if (!origins.open_existing(txn.handle())) {
            throw std::runtime_error("origin index should exist after backfill");
        }
        std::uint64_t seq = 0;
        if (!origins.last_seq(txn.handle(), origin_a, seq) || seq != 2u) {
            throw std::runtime_error("legacy origin was not backfilled");
        }
        if (!origins.last_seq(txn.handle(), origin_b, seq) || seq != 1u) {
            throw std::runtime_error("new origin was not indexed");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_changelog_rebuilds_partial_origin_index() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_changelog_origin_rebuild.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    const NodeId origin_a = make_node(0x10);
    const NodeId origin_b = make_node(0x40);
    const NodeId stale_origin = make_node(0x70);

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        MDBX_dbi raw = 0;
        mdbxc::check_mdbx(mdbx_dbi_open(txn.handle(), "_mdbxc_changelog",
                                        MDBX_CREATE, &raw),
                          "open raw changelog failed");
        put_raw_changelog(txn.handle(), raw, origin_a, 1, { 0xA1 });
        put_raw_changelog(txn.handle(), raw, origin_a, 2, { 0xA2 });
        put_raw_changelog(txn.handle(), raw, origin_b, 1, { 0xB1 });

        OriginIndexStore origins(conn->env_handle());
        origins.open(txn.handle());
        origins.note_origin(txn.handle(), origin_a, 2);
        origins.note_origin(txn.handle(), stale_origin, 9);
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        ChangeLogStore log(conn->env_handle());
        log.open(txn.handle());
        if (log.origin_index_matches_changelog(txn.handle())) {
            throw std::runtime_error("partial origin index should not validate");
        }
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        ChangeLogStore log(conn->env_handle());
        log.open(txn.handle());
        const std::size_t origins_written = log.rebuild_origin_index(txn.handle());
        if (origins_written != 2u) {
            throw std::runtime_error("origin rebuild wrote wrong count");
        }
        if (!log.origin_index_matches_changelog(txn.handle())) {
            throw std::runtime_error("rebuilt origin index should validate");
        }
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        OriginIndexStore origins(conn->env_handle());
        if (!origins.open_existing(txn.handle())) {
            throw std::runtime_error("rebuilt origin index should exist");
        }
        std::uint64_t seq = 0;
        if (!origins.last_seq(txn.handle(), origin_a, seq) || seq != 2u) {
            throw std::runtime_error("rebuilt origin_a tail mismatch");
        }
        if (!origins.last_seq(txn.handle(), origin_b, seq) || seq != 1u) {
            throw std::runtime_error("rebuilt origin_b tail mismatch");
        }
        if (origins.last_seq(txn.handle(), stale_origin, seq)) {
            throw std::runtime_error("rebuilt index kept stale origin");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_applied_store() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_applied.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    AppliedStore store(conn->env_handle());
    const NodeId origin_a = make_node(0xC0);
    const NodeId origin_b = make_node(0xD0);

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        store.open(txn.handle());
        store.set_last_applied_seq(txn.handle(), origin_a, 42);
        store.set_last_applied_seq(txn.handle(), origin_b, 100);
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        if (store.last_applied_seq(txn.handle(), origin_a) != 42) {
            throw std::runtime_error("origin_a seq mismatch");
        }
        if (store.last_applied_seq(txn.handle(), origin_b) != 100) {
            throw std::runtime_error("origin_b seq mismatch");
        }
        const NodeId unknown{};
        if (store.last_applied_seq(txn.handle(), unknown) != 0) {
            throw std::runtime_error("unknown origin must be 0");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_identity_index_store() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_identity.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    IdentityIndexStore store(conn->env_handle());
    const std::string dbi = "trades";
    const std::vector<std::uint8_t> id_key = { 'E','U','R','U','S','D' };
    const std::vector<std::uint8_t> sk = { 0x01, 0x02, 0x03 };

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        store.open(txn.handle());
        IdentityIndexValue v;
        v.storage_key = sk;
        v.origin_node_id = make_node(0xE0);
        v.seq = 9;
        v.revision_key = { 0xAA };
        store.put(txn.handle(), dbi, id_key, v);
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        IdentityIndexValue out;
        if (!store.get(txn.handle(), dbi, id_key, out)) {
            throw std::runtime_error("identity get should succeed");
        }
        if (out.storage_key != sk) throw std::runtime_error("storage_key mismatch");
        if (out.seq != 9) throw std::runtime_error("seq mismatch");
        if (out.revision_key.size() != 1 || out.revision_key[0] != 0xAA) {
            throw std::runtime_error("revision_key mismatch");
        }
        if (out.flags != 0) throw std::runtime_error("flags should be 0");
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        IdentityIndexValue marker;
        marker.origin_node_id = make_node(0xE0);
        marker.seq = 10;
        store.tombstone(txn.handle(), dbi, id_key, marker);
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        IdentityIndexValue out;
        if (!store.get(txn.handle(), dbi, id_key, out)) {
            throw std::runtime_error("tombstoned record should still be readable");
        }
        if ((out.flags & IDENTITY_TOMBSTONE) == 0) {
            throw std::runtime_error("tombstone flag must be set");
        }
        if (out.seq != 10) throw std::runtime_error("tombstone seq mismatch");
    }

    conn->disconnect();
    cleanup(p);
}

void test_changelog_prune_up_to_boundary() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_prune.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    ChangeLogStore store(conn->env_handle());
    const NodeId origin = make_node(0xA1);

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        store.open(txn.handle());
        for (std::uint64_t s : { 1ULL, 2ULL, 255ULL, 256ULL, 257ULL, 1000ULL }) {
            store.append(txn.handle(), origin, s, { 0x01, static_cast<std::uint8_t>(s & 0xff) });
        }

        const std::size_t removed = store.prune_up_to(txn.handle(), origin, 256);
        if (removed != 4) {
            throw std::runtime_error("expected 4 removed, got " + std::to_string(removed));
        }

        for (std::uint64_t s : { 1ULL, 2ULL, 255ULL, 256ULL }) {
            if (store.contains(txn.handle(), origin, s)) {
                throw std::runtime_error("seq " + std::to_string(s) + " should be pruned");
            }
        }
        for (std::uint64_t s : { 257ULL, 1000ULL }) {
            if (!store.contains(txn.handle(), origin, s)) {
                throw std::runtime_error("seq " + std::to_string(s) + " should remain");
            }
        }
        txn.commit();
    }

    conn->disconnect();
    cleanup(p);
}

void test_changelog_prune_does_not_touch_other_origin() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_prune_other.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    ChangeLogStore store(conn->env_handle());
    const NodeId origin_a = make_node(0xA2);
    const NodeId origin_b = make_node(0xB2);

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        store.open(txn.handle());
        store.append(txn.handle(), origin_a, 1, { 0x01 });
        store.append(txn.handle(), origin_b, 1, { 0x02 });
        store.append(txn.handle(), origin_a, 2, { 0x03 });

        const std::size_t removed_a = store.prune_up_to(txn.handle(), origin_a, 1);
        if (removed_a != 1) {
            throw std::runtime_error("expected 1 from origin_a, got " + std::to_string(removed_a));
        }
        if (!store.contains(txn.handle(), origin_b, 1)) {
            throw std::runtime_error("origin_b seq 1 was pruned unexpectedly");
        }
        if (!store.contains(txn.handle(), origin_a, 2)) {
            throw std::runtime_error("origin_a seq 2 must remain");
        }
        txn.commit();
    }

    conn->disconnect();
    cleanup(p);
}

void test_identity_key_collision() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_collision.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    IdentityIndexStore store(conn->env_handle());

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        store.open(txn.handle());

        IdentityIndexValue v;
        v.storage_key = { 0xAA };
        v.origin_node_id = make_node(0xC3);
        v.seq = 1;
        store.put(txn.handle(), "ab",  { 'c' }, v);
        store.put(txn.handle(), "a",   { 'b', 'c' }, v);

        IdentityIndexValue got1, got2;
        if (!store.get(txn.handle(), "ab", { 'c' }, got1)) {
            throw std::runtime_error("ab/c not found");
        }
        if (!store.get(txn.handle(), "a", { 'b', 'c' }, got2)) {
            throw std::runtime_error("a/bc not found");
        }

        IdentityIndexValue canon;
        canon.storage_key = { 0xAA };
        canon.origin_node_id = make_node(0xC3);
        canon.seq = 1;
        if (got1.storage_key != canon.storage_key ||
            got2.storage_key != canon.storage_key) {
            throw std::runtime_error("collision: two distinct keys collapsed to one record");
        }
        txn.commit();
    }

    conn->disconnect();
    cleanup(p);
}

void test_schema_registry_store() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_schema_registry.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    LogicalSchemaRecord ordered;
    ordered.dbi_name = "events";
    ordered.kind = LogicalTableKind::KeyOrderedMultiValue;
    ordered.schema_version = 1;
    ordered.dbi_names.push_back("events");

    LogicalSchemaRecord vector_like;
    vector_like.dbi_name = "vectors";
    vector_like.kind = LogicalTableKind::HashedKeyValue;
    vector_like.schema_version = 3;
    vector_like.dbi_names.push_back("vectors.ids");
    vector_like.dbi_names.push_back("vectors.payload");

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        SchemaRegistryStore store(conn->env_handle());
        store.register_or_verify(txn.handle(), "app.events.v1", ordered);
        store.register_or_verify(txn.handle(), "app.vectors.v3", vector_like);
        store.register_or_verify(txn.handle(), "app.events.v1", ordered);
        LogicalSchemaRecord permuted = vector_like;
        std::reverse(permuted.dbi_names.begin(), permuted.dbi_names.end());
        store.register_or_verify(txn.handle(), "app.vectors.v3", permuted);
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        SchemaRegistryStore store(conn->env_handle());

        LogicalSchemaRecord out;
        if (!store.get(txn.handle(), "app.events.v1", out)) {
            throw std::runtime_error("schema registry record missing");
        }
        if (out.dbi_name != ordered.dbi_name ||
            out.kind != ordered.kind ||
            out.schema_version != ordered.schema_version ||
            out.dbi_names != ordered.dbi_names) {
            throw std::runtime_error("schema registry record mismatch");
        }

        const std::vector<std::string> ids = store.schema_ids(txn.handle());
        if (ids.size() != 2u ||
            ids[0] != "app.events.v1" ||
            ids[1] != "app.vectors.v3") {
            throw std::runtime_error("schema registry ids mismatch");
        }
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        SchemaRegistryStore store(conn->env_handle());
        LogicalSchemaRecord duplicate = ordered;
        duplicate.dbi_names.push_back("events");
        bool caught = false;
        try {
            store.register_or_verify(txn.handle(), "app.duplicate.v1", duplicate);
        } catch (const std::invalid_argument&) {
            caught = true;
        }
        if (!caught) {
            throw std::runtime_error("duplicate owned DBI was accepted");
        }
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        SchemaRegistryStore store(conn->env_handle());
        LogicalSchemaRecord mismatch = ordered;
        mismatch.kind = LogicalTableKind::KeyMultiValue;
        bool caught = false;
        try {
            store.register_or_verify(txn.handle(), "app.events.v1", mismatch);
        } catch (const std::runtime_error&) {
            caught = true;
        }
        if (!caught) {
            throw std::runtime_error("schema registry mismatch was accepted");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_schema_registry_open_after_aborted_create() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_schema_abort_reuse.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    LogicalSchemaRecord record;
    record.dbi_name = "events";
    record.kind = LogicalTableKind::KeyMultiValue;
    record.schema_version = 1;
    record.dbi_names.push_back("events");

    SchemaRegistryStore store(conn->env_handle());
    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        store.open(txn.handle());
        store.register_or_verify(txn.handle(), "app.events.v1", record);
        txn.rollback();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        store.register_or_verify(txn.handle(), "app.events.v1", record);
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        LogicalSchemaRecord out;
        if (!store.get(txn.handle(), "app.events.v1", out) ||
            out.dbi_name != "events") {
            throw std::runtime_error(
                "schema registry did not recover after aborted create");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void test_schema_registry_created_by_sync_engine_init() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_schema_engine_init.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 16;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    SyncEngine engine(conn);
    engine.initialize_local_identity(make_node(0x11), make_node(0x22));

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        SchemaRegistryStore store(conn->env_handle());
        store.open(txn.handle());
        if (!store.schema_ids(txn.handle()).empty()) {
            throw std::runtime_error("new schema registry should be empty");
        }
    }

    conn->disconnect();
    cleanup(p);
}

void expect_schema_get_failure(const std::string& label,
                               mdbxc::sync::SchemaRegistryStore& store,
                               MDBX_txn* txn,
                               const std::string& schema_id) {
    mdbxc::sync::LogicalSchemaRecord out;
    out.dbi_name = "sentinel";
    out.kind = mdbxc::sync::LogicalTableKind::KeyMultiValue;
    out.schema_version = 99;
    bool caught = false;
    try {
        (void)store.get(txn, schema_id, out);
    } catch (const std::runtime_error&) {
        caught = true;
    } catch (const std::invalid_argument&) {
        caught = true;
    }
    if (!caught) {
        throw std::runtime_error(label + " was accepted");
    }
    if (out.dbi_name != "sentinel" ||
        out.kind != mdbxc::sync::LogicalTableKind::KeyMultiValue ||
        out.schema_version != 99u) {
        throw std::runtime_error(label + " rewrote output on failure");
    }
}

void test_schema_registry_rejects_malformed_records() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_schema_malformed.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
        SchemaRegistryStore store(conn->env_handle());
        const MDBX_dbi raw = store.handle(txn.handle());
        put_raw_schema_record(
            txn.handle(), raw, "bad.count",
            make_schema_record_bytes_with_count(
                "bad.count",
                static_cast<std::uint16_t>(LogicalTableKind::KeyMultiValue),
                1, 0, "events", 0xffffffffu));
        put_raw_schema_record(
            txn.handle(), raw, "bad.kind",
            make_schema_record_bytes(
                "bad.kind", 0xffffu, 1, 0, "events",
                std::vector<std::string>()));
        put_raw_schema_record(
            txn.handle(), raw, "bad.version",
            make_schema_record_bytes(
                "bad.version",
                static_cast<std::uint16_t>(LogicalTableKind::KeyMultiValue),
                0, 0, "events", std::vector<std::string>()));
        put_raw_schema_record(
            txn.handle(), raw, "bad.flags",
            make_schema_record_bytes(
                "bad.flags",
                static_cast<std::uint16_t>(LogicalTableKind::KeyMultiValue),
                1, 1, "events", std::vector<std::string>()));
        put_raw_schema_record(
            txn.handle(), raw, "bad.dbi",
            make_schema_record_bytes(
                "bad.dbi",
                static_cast<std::uint16_t>(LogicalTableKind::KeyMultiValue),
                1, 0, "", std::vector<std::string>()));
        std::vector<std::string> empty_owned;
        empty_owned.push_back("");
        put_raw_schema_record(
            txn.handle(), raw, "bad.owned",
            make_schema_record_bytes(
                "bad.owned",
                static_cast<std::uint16_t>(LogicalTableKind::KeyMultiValue),
                1, 0, "events", empty_owned));
        put_raw_schema_record(
            txn.handle(), raw, "bad.schema_id",
            make_schema_record_bytes(
                "other.schema",
                static_cast<std::uint16_t>(LogicalTableKind::KeyMultiValue),
                1, 0, "events", std::vector<std::string>()));
        txn.commit();
    }

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::READ_ONLY);
        SchemaRegistryStore store(conn->env_handle());
        expect_schema_get_failure("bad count", store, txn.handle(), "bad.count");
        expect_schema_get_failure("bad kind", store, txn.handle(), "bad.kind");
        expect_schema_get_failure("bad version", store, txn.handle(), "bad.version");
        expect_schema_get_failure("bad flags", store, txn.handle(), "bad.flags");
        expect_schema_get_failure("bad dbi", store, txn.handle(), "bad.dbi");
        expect_schema_get_failure("bad owned dbi", store, txn.handle(), "bad.owned");
        expect_schema_get_failure(
            "bad schema id", store, txn.handle(), "bad.schema_id");

        bool schema_ids_failed = false;
        try {
            (void)store.schema_ids(txn.handle());
        } catch (const std::runtime_error&) {
            schema_ids_failed = true;
        } catch (const std::invalid_argument&) {
            schema_ids_failed = true;
        }
        if (!schema_ids_failed) {
            throw std::runtime_error(
                "schema_ids accepted malformed registry records");
        }
    }

    conn->disconnect();
    cleanup(p);
}

template<class Store, class Op>
void expect_open_required(const std::string& label, Store& store, Op op) {
    bool caught = false;
    try {
        op();
    } catch (const std::logic_error&) {
        caught = true;
    } catch (const mdbxc::MdbxException&) {
        caught = true;
    }
    if (!caught) {
        throw std::runtime_error(label + ": expected logic_error or MdbxException before open()");
    }
}

void test_stores_require_open() {
    using namespace mdbxc::sync;
    const std::string p = "test_sync_stores_open_required.mdbx";
    cleanup(p);

    mdbxc::Config cfg;
    cfg.pathname = p;
    cfg.max_dbs = 8;
    cfg.no_subdir = true;
    auto conn = mdbxc::Connection::create(cfg);

    {
        auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);

        MetaStore meta(conn->env_handle());
        expect_open_required("MetaStore::get_db_uuid", meta,
                            [&] { (void)meta.get_db_uuid(txn.handle()); });
        expect_open_required("MetaStore::set_db_uuid", meta,
                            [&] { meta.set_db_uuid(txn.handle(), make_node(0xA0)); });
        expect_open_required("MetaStore::get_local_seq", meta,
                            [&] { (void)meta.get_local_seq(txn.handle()); });
        expect_open_required("MetaStore::increment_local_seq", meta,
                            [&] { (void)meta.increment_local_seq(txn.handle()); });

        AppliedStore applied(conn->env_handle());
        expect_open_required("AppliedStore::last_applied_seq", applied,
                            [&] { (void)applied.last_applied_seq(txn.handle(), make_node(0xB0)); });
        expect_open_required("AppliedStore::set_last_applied_seq", applied,
                            [&] { applied.set_last_applied_seq(txn.handle(), make_node(0xB0), 1); });

        ChangeLogStore change(conn->env_handle());
        expect_open_required("ChangeLogStore::append", change,
                            [&] { change.append(txn.handle(), make_node(0xC0), 1, { 0x01 }); });
        expect_open_required("ChangeLogStore::contains", change,
                            [&] { (void)change.contains(txn.handle(), make_node(0xC0), 1); });
        expect_open_required("ChangeLogStore::erase", change,
                            [&] { (void)change.erase(txn.handle(), make_node(0xC0), 1); });
        expect_open_required("ChangeLogStore::prune_up_to", change,
                            [&] { (void)change.prune_up_to(txn.handle(), make_node(0xC0), 1); });
        expect_open_required("ChangeLogStore::origin_index_matches_changelog", change,
                            [&change, &txn] { (void)change.origin_index_matches_changelog(txn.handle()); });
        expect_open_required("ChangeLogStore::rebuild_origin_index", change,
                            [&change, &txn] { (void)change.rebuild_origin_index(txn.handle()); });

        OriginIndexStore origins(conn->env_handle());
        std::uint64_t seq = 0;
        expect_open_required("OriginIndexStore::empty", origins,
                            [&origins, &txn] { (void)origins.empty(txn.handle()); });
        expect_open_required("OriginIndexStore::clear", origins,
                            [&origins, &txn] { (void)origins.clear(txn.handle()); });
        expect_open_required("OriginIndexStore::note_origin", origins,
                            [&origins, &txn] { origins.note_origin(txn.handle(), make_node(0xD0), 1); });
        expect_open_required("OriginIndexStore::last_seq", origins,
                            [&origins, &txn, &seq] { (void)origins.last_seq(txn.handle(), make_node(0xD0), seq); });
        expect_open_required("OriginIndexStore::origins", origins,
                            [&origins, &txn] { (void)origins.origins(txn.handle()); });

        IdentityIndexStore identity(conn->env_handle());
        IdentityIndexValue iv;
        expect_open_required("IdentityIndexStore::put", identity,
                            [&] { identity.put(txn.handle(), "t", { 0x01 }, iv); });
        expect_open_required("IdentityIndexStore::get", identity,
                            [&] { (void)identity.get(txn.handle(), "t", { 0x01 }, iv); });
        expect_open_required("IdentityIndexStore::tombstone", identity,
                            [&] { identity.tombstone(txn.handle(), "t", { 0x01 }, iv); });
        expect_open_required("IdentityIndexStore::erase", identity,
                            [&] { (void)identity.erase(txn.handle(), "t", { 0x01 }); });
    }

    conn->disconnect();
    cleanup(p);
}

} // namespace

int main() {
    test_meta_store();
    test_origin_index_store();
    test_changelog_store();
    test_changelog_updates_origin_index();
    test_changelog_backfills_legacy_origins_on_append();
    test_changelog_rebuilds_partial_origin_index();
    test_applied_store();
    test_identity_index_store();
    test_changelog_prune_up_to_boundary();
    test_changelog_prune_does_not_touch_other_origin();
    test_identity_key_collision();
    test_schema_registry_store();
    test_schema_registry_open_after_aborted_create();
    test_schema_registry_created_by_sync_engine_init();
    test_schema_registry_rejects_malformed_records();
    test_stores_require_open();
    return 0;
}
