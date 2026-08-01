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

void write_u32_le(std::vector<std::uint8_t>& bytes,
                  std::size_t offset,
                  std::uint32_t value) {
    MDBXC_TEST_ASSERT(offset + 4u <= bytes.size());
    for (std::size_t i = 0u; i < 4u; ++i) {
        bytes[offset + i] = static_cast<std::uint8_t>(value >> (8u * i));
    }
}

std::size_t manifest_count_offset(
        const mdbxc::sync::FullSnapshotChunk& chunk) {
    return mdbxc::sync::FullSnapshotCodec::magic_size() + 2u +
        chunk.source_node_id.size() + chunk.source_db_uuid.size() + 4u +
        chunk.snapshot_id.size() + 8u + 1u;
}

std::size_t nested_batch_offset(
        const mdbxc::sync::FullSnapshotChunk& chunk) {
    std::size_t offset = manifest_count_offset(chunk) + 4u;
    for (std::size_t i = 0u; i < chunk.manifest.size(); ++i) {
        offset += 4u + chunk.manifest[i].dbi_name.size() + 4u;
    }
    return offset + 4u;
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

    mdbxc::sync::FullSnapshotChunk mismatched_continuation = make_chunk();
    mismatched_continuation.has_more = false;
    MDBXC_TEST_ASSERT(throws([&mismatched_continuation]() {
        mdbxc::sync::FullSnapshotCodec::encode(mismatched_continuation);
    }));
}

void test_full_snapshot_respects_bounds() {
    mdbxc::sync::CodecBounds bounds;
    bounds.max_snapshot_id_len = 4u;
    MDBXC_TEST_ASSERT(throws([&bounds]() {
        mdbxc::sync::FullSnapshotCodec::encode(make_chunk(), &bounds);
    }));
}

void test_full_snapshot_default_bounds_reject_hostile_counts() {
    const mdbxc::sync::FullSnapshotChunk source = make_chunk();
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::FullSnapshotCodec::encode(source);

    std::vector<std::uint8_t> oversized_manifest = encoded;
    write_u32_le(oversized_manifest, manifest_count_offset(source), 10001u);
    MDBXC_TEST_ASSERT(throws([&oversized_manifest]() {
        mdbxc::sync::FullSnapshotCodec::decode(oversized_manifest);
    }));

    const std::size_t nested_offset = nested_batch_offset(source);
    std::vector<std::uint8_t> oversized_ops = encoded;
    const std::size_t ops_count_offset = nested_offset +
        mdbxc::sync::ChangeBatchCodec::magic_size() + 4u + 4u +
        source.source_node_id.size() + 8u + 8u;
    write_u32_le(oversized_ops, ops_count_offset, 10001u);
    MDBXC_TEST_ASSERT(throws([&oversized_ops]() {
        mdbxc::sync::FullSnapshotCodec::decode(oversized_ops);
    }));

    std::vector<std::uint8_t> oversized_nested = encoded;
    write_u32_le(oversized_nested, nested_offset - 4u, 0xFFFFFFFFu);
    MDBXC_TEST_ASSERT(throws([&oversized_nested]() {
        mdbxc::sync::FullSnapshotCodec::decode(oversized_nested);
    }));

    std::vector<std::uint8_t> invalid_continuation = encoded;
    invalid_continuation[manifest_count_offset(source) - 1u] = 2u;
    MDBXC_TEST_ASSERT(throws([&invalid_continuation]() {
        mdbxc::sync::FullSnapshotCodec::decode(invalid_continuation);
    }));
}

void test_full_snapshot_rejects_non_replacement_operations() {
    mdbxc::sync::FullSnapshotChunk deleted = make_chunk();
    deleted.batch.ops[0].op_type = mdbxc::sync::ChangeOpType::Delete;
    MDBXC_TEST_ASSERT(throws([&deleted]() {
        mdbxc::sync::FullSnapshotCodec::encode(deleted);
    }));

    mdbxc::sync::FullSnapshotChunk tombstone_put = make_chunk();
    tombstone_put.batch.ops[0].op_flags = mdbxc::sync::OP_TOMBSTONE;
    tombstone_put.batch.ops[0].value.clear();
    MDBXC_TEST_ASSERT(throws([&tombstone_put]() {
        mdbxc::sync::FullSnapshotCodec::encode(tombstone_put);
    }));

    mdbxc::sync::FullSnapshotChunk malformed_clear = make_chunk();
    malformed_clear.batch.ops[0].op_type =
        mdbxc::sync::ChangeOpType::ClearTable;
    malformed_clear.batch.ops[0].value.clear();
    MDBXC_TEST_ASSERT(throws([&malformed_clear]() {
        mdbxc::sync::FullSnapshotCodec::encode(malformed_clear);
    }));

    mdbxc::sync::FullSnapshotChunk clear = make_chunk();
    clear.batch.ops[0].op_type = mdbxc::sync::ChangeOpType::ClearTable;
    clear.batch.ops[0].storage_key.clear();
    clear.batch.ops[0].value.clear();
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::FullSnapshotCodec::encode(clear);
    const mdbxc::sync::FullSnapshotChunk decoded =
        mdbxc::sync::FullSnapshotCodec::decode(encoded);
    MDBXC_TEST_ASSERT(decoded.batch.ops[0].op_type ==
        mdbxc::sync::ChangeOpType::ClearTable);
}

void test_full_snapshot_manifest_boundary_and_malformed_input() {
    mdbxc::sync::CodecBounds exact_bounds;
    exact_bounds.max_snapshot_manifest_entries = 1u;
    const std::vector<std::uint8_t> encoded =
        mdbxc::sync::FullSnapshotCodec::encode(make_chunk(), &exact_bounds);
    const mdbxc::sync::FullSnapshotChunk decoded =
        mdbxc::sync::FullSnapshotCodec::decode(encoded, &exact_bounds);
    MDBXC_TEST_ASSERT(decoded.manifest.size() == 1u);

    mdbxc::sync::CodecBounds zero_bounds;
    zero_bounds.max_snapshot_manifest_entries = 0u;
    MDBXC_TEST_ASSERT(throws([&zero_bounds]() {
        mdbxc::sync::FullSnapshotCodec::encode(make_chunk(), &zero_bounds);
    }));

    std::vector<std::uint8_t> truncated = encoded;
    truncated.resize(truncated.size() - 1u);
    MDBXC_TEST_ASSERT(throws([&truncated]() {
        mdbxc::sync::FullSnapshotCodec::decode(truncated);
    }));

    std::vector<std::uint8_t> unknown_version = encoded;
    unknown_version[8u] = 2u;
    unknown_version[9u] = 0u;
    MDBXC_TEST_ASSERT(throws([&unknown_version]() {
        mdbxc::sync::FullSnapshotCodec::decode(unknown_version);
    }));

    mdbxc::sync::CodecBounds exact_size;
    exact_size.max_snapshot_chunk_bytes =
        static_cast<std::uint32_t>(encoded.size());
    MDBXC_TEST_ASSERT(
        mdbxc::sync::FullSnapshotCodec::decode(encoded, &exact_size).
            batch.ops.size() == 1u);
    std::vector<std::uint8_t> oversized_chunk = encoded;
    oversized_chunk.push_back(0u);
    MDBXC_TEST_ASSERT(throws([&oversized_chunk, &exact_size]() {
        mdbxc::sync::FullSnapshotCodec::decode(oversized_chunk, &exact_size);
    }));
}

void test_full_snapshot_rejects_incomplete_identity_and_manifest() {
    mdbxc::sync::FullSnapshotChunk zero_source = make_chunk();
    zero_source.source_node_id = mdbxc::sync::make_zero_node();
    zero_source.batch.origin_node_id = zero_source.source_node_id;
    MDBXC_TEST_ASSERT(throws([&zero_source]() {
        mdbxc::sync::FullSnapshotCodec::encode(zero_source);
    }));

    mdbxc::sync::FullSnapshotChunk empty_snapshot_id = make_chunk();
    empty_snapshot_id.snapshot_id.clear();
    MDBXC_TEST_ASSERT(throws([&empty_snapshot_id]() {
        mdbxc::sync::FullSnapshotCodec::encode(empty_snapshot_id);
    }));

    mdbxc::sync::FullSnapshotChunk duplicate_manifest = make_chunk();
    duplicate_manifest.manifest.push_back(duplicate_manifest.manifest[0]);
    MDBXC_TEST_ASSERT(throws([&duplicate_manifest]() {
        mdbxc::sync::FullSnapshotCodec::encode(duplicate_manifest);
    }));

    mdbxc::sync::FullSnapshotChunk oversized_key = make_chunk();
    oversized_key.batch.ops[0].storage_key.resize(16u * 1024u + 1u, 0u);
    MDBXC_TEST_ASSERT(throws([&oversized_key]() {
        mdbxc::sync::FullSnapshotCodec::encode(oversized_key);
    }));
}

} // namespace

int main() {
    test_full_snapshot_round_trip();
    test_full_snapshot_rejects_reserved_and_unlisted_dbis();
    test_full_snapshot_rejects_wrong_sequence_and_trailing_bytes();
    test_full_snapshot_respects_bounds();
    test_full_snapshot_default_bounds_reject_hostile_counts();
    test_full_snapshot_rejects_non_replacement_operations();
    test_full_snapshot_manifest_boundary_and_malformed_input();
    test_full_snapshot_rejects_incomplete_identity_and_manifest();
    return 0;
}
