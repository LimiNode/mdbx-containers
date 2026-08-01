#include <mdbx_containers/sync.hpp>

#include "test_assert.hpp"

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

mdbxc::sync::NodeId make_id(std::uint8_t seed) {
    mdbxc::sync::NodeId out{};
    for (std::size_t i = 0u; i < out.size(); ++i) {
        out[i] = static_cast<std::uint8_t>(seed + i);
    }
    return out;
}

mdbxc::sync::FullSnapshotChunk make_chunk() {
    mdbxc::sync::FullSnapshotChunk out;
    out.source_node_id = make_id(0x10u);
    out.source_db_uuid = make_id(0x20u);
    out.snapshot_id = "snapshot-1";
    out.chunk_index = 0u;
    out.has_more = true;

    mdbxc::sync::FullSnapshotManifestEntry manifest_entry;
    manifest_entry.dbi_name = "documents";
    manifest_entry.dbi_flags = 0u;
    out.manifest.push_back(manifest_entry);

    out.batch.origin_node_id = out.source_node_id;
    out.batch.seq = 0u;
    out.batch.batch_flags = mdbxc::sync::BATCH_HAS_MORE;
    mdbxc::sync::ChangeOp op;
    op.op_type = mdbxc::sync::ChangeOpType::Put;
    op.dbi_name = "documents";
    op.storage_key.push_back(0x01u);
    op.value.push_back(0x02u);
    out.batch.ops.push_back(op);
    return out;
}

template <typename Fn>
bool throws(Fn fn) {
    try {
        fn();
    } catch (const std::exception&) {
        return true;
    }
    return false;
}

void test_full_snapshot_round_trip() {
    const mdbxc::sync::FullSnapshotChunk source = make_chunk();
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::FullSnapshotCodec::encode(source);
    const mdbxc::sync::FullSnapshotChunk decoded =
        mdbxc::sync::FullSnapshotCodec::decode(encoded);

    MDBXC_TEST_ASSERT(decoded.source_node_id == source.source_node_id);
    MDBXC_TEST_ASSERT(decoded.source_db_uuid == source.source_db_uuid);
    MDBXC_TEST_ASSERT(decoded.snapshot_id == source.snapshot_id);
    MDBXC_TEST_ASSERT(decoded.chunk_index == 0u);
    MDBXC_TEST_ASSERT(decoded.has_more);
    MDBXC_TEST_ASSERT(decoded.manifest.size() == 1u);
    MDBXC_TEST_ASSERT(decoded.manifest[0].dbi_name == "documents");
    MDBXC_TEST_ASSERT(decoded.batch.seq == 0u);
    MDBXC_TEST_ASSERT(decoded.batch.origin_node_id == source.source_node_id);
    MDBXC_TEST_ASSERT(decoded.batch.ops.size() == 1u);
}

void test_full_snapshot_rejects_reserved_and_unlisted_dbis() {
    mdbxc::sync::FullSnapshotChunk reserved = make_chunk();
    reserved.manifest[0].dbi_name = "_mdbxc_meta";
    reserved.batch.ops[0].dbi_name = "_mdbxc_meta";
    MDBXC_TEST_ASSERT(throws([&reserved]() {
        mdbxc::sync::FullSnapshotCodec::encode(reserved);
    }));

    mdbxc::sync::FullSnapshotChunk unlisted = make_chunk();
    unlisted.batch.ops[0].dbi_name = "other";
    MDBXC_TEST_ASSERT(throws([&unlisted]() {
        mdbxc::sync::FullSnapshotCodec::encode(unlisted);
    }));

    mdbxc::sync::FullSnapshotChunk mismatched_flags = make_chunk();
    mismatched_flags.batch.ops[0].dbi_flags = 1u;
    MDBXC_TEST_ASSERT(throws([&mismatched_flags]() {
        mdbxc::sync::FullSnapshotCodec::encode(mismatched_flags);
    }));
}

void test_full_snapshot_rejects_wrong_sequence_and_trailing_bytes() {
    mdbxc::sync::FullSnapshotChunk wrong_sequence = make_chunk();
    wrong_sequence.batch.seq = 1u;
    MDBXC_TEST_ASSERT(throws([&wrong_sequence]() {
        mdbxc::sync::FullSnapshotCodec::encode(wrong_sequence);
    }));

    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::FullSnapshotCodec::encode(make_chunk());
    std::vector<std::uint8_t> trailing = encoded;
    trailing.push_back(0xFFu);
    MDBXC_TEST_ASSERT(throws([&trailing]() {
        mdbxc::sync::FullSnapshotCodec::decode(trailing);
    }));
}

void test_full_snapshot_respects_bounds() {
    mdbxc::sync::CodecBounds bounds;
    bounds.max_snapshot_id_len = 4u;
    MDBXC_TEST_ASSERT(throws([&bounds]() {
        mdbxc::sync::FullSnapshotCodec::encode(make_chunk(), &bounds);
    }));
}

} // namespace

int main() {
    test_full_snapshot_round_trip();
    test_full_snapshot_rejects_reserved_and_unlisted_dbis();
    test_full_snapshot_rejects_wrong_sequence_and_trailing_bytes();
    test_full_snapshot_respects_bounds();
    return 0;
}
