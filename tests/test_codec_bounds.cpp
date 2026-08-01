#include <mdbx_containers/sync.hpp>
#include <stdexcept>
#include <string>

namespace {

template<class Fn>
void expect_throw(const std::string& label, Fn&& fn) {
    bool caught = false;
    try { fn(); } catch (...) { caught = true; }
    if (!caught) {
        throw std::runtime_error(label + ": expected throw, nothing thrown");
    }
}

void test_oversize_dbi_name() {
    using namespace mdbxc::sync;
    CodecBounds bounds;
    bounds.max_dbi_name_len = 8;
    ChangeBatch batch;
    ChangeOp op;
    op.op_type = ChangeOpType::Put;
    op.dbi_name = std::string(64, 'x');
    op.storage_key = { 0x01 };
    op.value = { 0x02 };
    batch.ops.push_back(op);
    expect_throw("dbi_name too long", [] {
        CodecBounds b;
        b.max_dbi_name_len = 8;
        ChangeBatch batch;
        ChangeOp op;
        op.op_type = ChangeOpType::Put;
        op.dbi_name = std::string(64, 'x');
        op.storage_key = { 0x01 };
        op.value = { 0x02 };
        batch.ops.push_back(op);
        (void)ChangeBatchCodec::encode(batch, &b);
    });
    (void)bounds;
}

void test_oversize_storage_key() {
    expect_throw("storage_key too long", [] {
        using namespace mdbxc::sync;
        CodecBounds b;
        b.max_storage_key_len = 4;
        ChangeBatch batch;
        ChangeOp op;
        op.op_type = ChangeOpType::Put;
        op.dbi_name = "t";
        op.storage_key.assign(64, 0x01);
        op.value = { 0x02 };
        batch.ops.push_back(op);
        (void)ChangeBatchCodec::encode(batch, &b);
    });
}

void test_oversize_value() {
    expect_throw("value too long", [] {
        using namespace mdbxc::sync;
        CodecBounds b;
        b.max_value_len = 4;
        ChangeBatch batch;
        ChangeOp op;
        op.op_type = ChangeOpType::Put;
        op.dbi_name = "t";
        op.storage_key = { 0x01 };
        op.value.assign(64, 0x02);
        batch.ops.push_back(op);
        (void)ChangeBatchCodec::encode(batch, &b);
    });
}

void test_too_many_ops() {
    expect_throw("ops count too large", [] {
        using namespace mdbxc::sync;
        CodecBounds b;
        b.max_ops_per_batch = 2;
        ChangeBatch batch;
        for (int i = 0; i < 8; ++i) {
            ChangeOp op;
            op.op_type = ChangeOpType::Put;
            op.dbi_name = "t";
            op.storage_key = { static_cast<std::uint8_t>(i) };
            op.value = { 0x00 };
            batch.ops.push_back(op);
        }
        (void)ChangeBatchCodec::encode(batch, &b);
    });
}

void test_batch_total_size_boundary() {
    using namespace mdbxc::sync;
    ChangeBatch batch;
    ChangeOp op;
    op.op_type = ChangeOpType::Put;
    op.dbi_name = "t";
    op.storage_key = { 0x01 };
    op.value = { 0x02 };
    batch.ops.push_back(op);

    const std::vector<std::uint8_t> unbounded =
        ChangeBatchCodec::encode(batch);
    CodecBounds exact;
    exact.max_batch_total_bytes =
        static_cast<std::uint32_t>(unbounded.size());
    const std::vector<std::uint8_t> encoded =
        ChangeBatchCodec::encode(batch, &exact);
    if (encoded != unbounded ||
        ChangeBatchCodec::decode_exact(encoded, &exact).ops.size() != 1u) {
        throw std::runtime_error("exact batch total size bound was rejected");
    }

    CodecBounds too_small = exact;
    --too_small.max_batch_total_bytes;
    bool rejected = false;
    try {
        (void)ChangeBatchCodec::encode(batch, &too_small);
    } catch (const std::length_error&) {
        rejected = true;
    }
    if (!rejected) {
        throw std::runtime_error(
            "encoded batch exceeded max_batch_total_bytes");
    }
}

} // namespace

int main() {
    test_oversize_dbi_name();
    test_oversize_storage_key();
    test_oversize_value();
    test_too_many_ops();
    test_batch_total_size_boundary();
    return 0;
}
