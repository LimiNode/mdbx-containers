#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file KeyValueTableLogicalAdapter.hpp
/// \brief Minimal logical adapter for \c KeyValueTable.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "../../KeyValueTable.hpp"
#include "../ILogicalDeliveryOutbox.hpp"
#include "../LogicalTableAdapter.hpp"
#include "../LogicalSchemaValidation.hpp"
#include "../common.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Opcodes understood by \c KeyValueTableLogicalAdapter.
    enum KeyValueTableLogicalOpcode {
        KeyValueLogicalUpsert = 1,
        KeyValueLogicalDelete = 2,
        KeyValueLogicalClear  = 3
    };

namespace detail {

    template<class T>
    struct KeyValueLogicalPlainCharType {
        typedef typename std::remove_cv<T>::type value_type;
        static const bool value =
            std::is_same<value_type, char>::value ||
            std::is_same<value_type, wchar_t>::value ||
            std::is_same<value_type, char16_t>::value ||
            std::is_same<value_type, char32_t>::value;
    };

    template<class T>
    struct KeyValueLogicalIntegerLocalSupported {
        typedef typename std::remove_cv<T>::type value_type;
        static const bool value =
            std::is_integral<value_type>::value &&
            !std::is_same<value_type, bool>::value &&
            !KeyValueLogicalPlainCharType<value_type>::value &&
            sizeof(value_type) <= 8;
    };

    template<class LocalT, class WireT>
    struct KeyValueLogicalIntegerCodecBase {
        typedef LocalT value_type;
        typedef WireT wire_type;

        static_assert(KeyValueLogicalIntegerLocalSupported<LocalT>::value,
                      "KeyValue logical integer codec requires a non-character integral local type up to 64 bits");
        static_assert(std::is_integral<WireT>::value &&
                      !std::is_same<WireT, bool>::value &&
                      sizeof(WireT) <= 8,
                      "KeyValue logical integer wire type must be an integer up to 64 bits");

        static std::vector<std::uint8_t> encode(LocalT value) {
            const std::uint64_t raw = encode_raw(value);
            std::vector<std::uint8_t> out;
            ::mdbxc::sync::detail::append_u64_le(out, raw);
            out.resize(sizeof(WireT));
            return out;
        }

        static LocalT decode(const std::vector<std::uint8_t>& bytes) {
            if (bytes.size() != sizeof(WireT)) {
                throw std::runtime_error(
                    "KeyValue logical integer payload has unexpected width");
            }
            std::uint8_t storage[8] = {};
            for (std::size_t i = 0; i < bytes.size(); ++i) {
                storage[i] = bytes[i];
            }
            const std::uint64_t raw =
                ::mdbxc::sync::detail::read_u64_le(storage);
            return decode_raw(raw);
        }

    private:
        static std::uint64_t bit_mask() {
            return sizeof(WireT) == 8u
                ? (std::numeric_limits<std::uint64_t>::max)()
                : ((static_cast<std::uint64_t>(1) <<
                    (sizeof(WireT) * 8u)) - 1u);
        }

        static std::int64_t wire_min() {
            if (!std::numeric_limits<WireT>::is_signed) {
                return 0;
            }
            if (sizeof(WireT) == 8u) {
                return (std::numeric_limits<std::int64_t>::min)();
            }
            return -static_cast<std::int64_t>(
                static_cast<std::uint64_t>(1) <<
                (sizeof(WireT) * 8u - 1u));
        }

        static std::uint64_t wire_unsigned_max() {
            return bit_mask();
        }

        static std::int64_t wire_signed_max() {
            if (sizeof(WireT) == 8u) {
                return (std::numeric_limits<std::int64_t>::max)();
            }
            return static_cast<std::int64_t>(
                (static_cast<std::uint64_t>(1) <<
                 (sizeof(WireT) * 8u - 1u)) - 1u);
        }

        static std::uint64_t signed_raw(std::int64_t value) {
            if (value >= 0) {
                return static_cast<std::uint64_t>(value) & bit_mask();
            }
            std::uint64_t magnitude = 0;
            if (value == (std::numeric_limits<std::int64_t>::min)()) {
                magnitude = static_cast<std::uint64_t>(1) << 63u;
            } else {
                magnitude = static_cast<std::uint64_t>(-value);
            }
            return ((~magnitude) + 1u) & bit_mask();
        }

        static std::uint64_t encode_raw(LocalT value) {
            if (std::numeric_limits<WireT>::is_signed) {
                std::int64_t signed_value = 0;
                if (std::numeric_limits<LocalT>::is_signed) {
                    signed_value = static_cast<std::int64_t>(value);
                } else {
                    const std::uint64_t unsigned_value =
                        static_cast<std::uint64_t>(value);
                    if (unsigned_value >
                        static_cast<std::uint64_t>(
                            (std::numeric_limits<std::int64_t>::max)())) {
                        throw std::out_of_range(
                            "KeyValue logical integer value exceeds signed wire range");
                    }
                    signed_value = static_cast<std::int64_t>(unsigned_value);
                }
                if (signed_value < wire_min() ||
                    signed_value > wire_signed_max()) {
                    throw std::out_of_range(
                        "KeyValue logical integer value is out of wire range");
                }
                return signed_raw(signed_value);
            }
            if (std::numeric_limits<LocalT>::is_signed && value < 0) {
                throw std::out_of_range(
                    "KeyValue logical integer value is negative for unsigned wire");
            }
            const std::uint64_t unsigned_value =
                static_cast<std::uint64_t>(value);
            if (unsigned_value > wire_unsigned_max()) {
                throw std::out_of_range(
                    "KeyValue logical integer value is out of wire range");
            }
            return unsigned_value;
        }

        static std::int64_t decode_signed_raw(std::uint64_t raw) {
            raw &= bit_mask();
            const std::size_t bits = sizeof(WireT) * 8u;
            const std::uint64_t sign_bit =
                static_cast<std::uint64_t>(1) << (bits - 1u);
            if ((raw & sign_bit) == 0u) {
                return static_cast<std::int64_t>(raw);
            }
            const std::uint64_t magnitude = ((~raw) & bit_mask()) + 1u;
            if (magnitude ==
                (static_cast<std::uint64_t>(1) << 63u)) {
                return (std::numeric_limits<std::int64_t>::min)();
            }
            return -static_cast<std::int64_t>(magnitude);
        }

        static LocalT decode_raw(std::uint64_t raw) {
            if (std::numeric_limits<WireT>::is_signed) {
                const std::int64_t signed_value = decode_signed_raw(raw);
                if (std::numeric_limits<LocalT>::is_signed) {
                    if (signed_value <
                            static_cast<std::int64_t>(
                                (std::numeric_limits<LocalT>::min)()) ||
                        signed_value >
                            static_cast<std::int64_t>(
                                (std::numeric_limits<LocalT>::max)())) {
                        throw std::out_of_range(
                            "KeyValue logical integer payload is out of local range");
                    }
                    return static_cast<LocalT>(signed_value);
                }
                if (signed_value < 0) {
                    throw std::out_of_range(
                        "KeyValue logical integer payload is negative for unsigned local type");
                }
                const std::uint64_t unsigned_value =
                    static_cast<std::uint64_t>(signed_value);
                if (unsigned_value >
                    static_cast<std::uint64_t>(
                        (std::numeric_limits<LocalT>::max)())) {
                    throw std::out_of_range(
                        "KeyValue logical integer payload is out of local range");
                }
                return static_cast<LocalT>(unsigned_value);
            }

            if (std::numeric_limits<LocalT>::is_signed) {
                if (raw >
                    static_cast<std::uint64_t>(
                        (std::numeric_limits<LocalT>::max)())) {
                    throw std::out_of_range(
                        "KeyValue logical integer payload is out of local range");
                }
                return static_cast<LocalT>(raw);
            }
            if (raw >
                static_cast<std::uint64_t>(
                    (std::numeric_limits<LocalT>::max)())) {
                throw std::out_of_range(
                    "KeyValue logical integer payload is out of local range");
            }
            return static_cast<LocalT>(raw);
        }
    };

} // namespace detail

    template<class LocalT>
    struct KeyValueLogicalInt8Codec
        : detail::KeyValueLogicalIntegerCodecBase<LocalT, std::int8_t> {};

    template<class LocalT>
    struct KeyValueLogicalUInt8Codec
        : detail::KeyValueLogicalIntegerCodecBase<LocalT, std::uint8_t> {};

    template<class LocalT>
    struct KeyValueLogicalInt16Codec
        : detail::KeyValueLogicalIntegerCodecBase<LocalT, std::int16_t> {};

    template<class LocalT>
    struct KeyValueLogicalUInt16Codec
        : detail::KeyValueLogicalIntegerCodecBase<LocalT, std::uint16_t> {};

    template<class LocalT>
    struct KeyValueLogicalInt32Codec
        : detail::KeyValueLogicalIntegerCodecBase<LocalT, std::int32_t> {};

    template<class LocalT>
    struct KeyValueLogicalUInt32Codec
        : detail::KeyValueLogicalIntegerCodecBase<LocalT, std::uint32_t> {};

    template<class LocalT>
    struct KeyValueLogicalInt64Codec
        : detail::KeyValueLogicalIntegerCodecBase<LocalT, std::int64_t> {};

    template<class LocalT>
    struct KeyValueLogicalUInt64Codec
        : detail::KeyValueLogicalIntegerCodecBase<LocalT, std::uint64_t> {};

    template<class LocalT>
    struct KeyValueLogicalBoolCodec {
        typedef LocalT value_type;
        static_assert(std::is_same<
                          typename std::remove_cv<LocalT>::type,
                          bool>::value,
                      "KeyValue logical bool codec requires bool local type");

        static std::vector<std::uint8_t> encode(bool value) {
            std::vector<std::uint8_t> out(1);
            out[0] = value ? 1u : 0u;
            return out;
        }

        static bool decode(const std::vector<std::uint8_t>& bytes) {
            if (bytes.size() != 1u || bytes[0] > 1u) {
                throw std::runtime_error(
                    "KeyValue logical bool payload is invalid");
            }
            return bytes[0] != 0u;
        }
    };

    template<class LocalT>
    struct KeyValueLogicalStringCodec {
        typedef LocalT value_type;
        static_assert(std::is_same<
                          typename std::remove_cv<LocalT>::type,
                          std::string>::value,
                      "KeyValue logical string codec requires std::string local type");

        static std::vector<std::uint8_t> encode(const std::string& value) {
            return std::vector<std::uint8_t>(value.begin(), value.end());
        }

        static std::string decode(const std::vector<std::uint8_t>& bytes) {
            return std::string(bytes.begin(), bytes.end());
        }
    };

    /// \brief First concrete logical adapter for simple one-value-per-key tables.
    /// \details The payload format is adapter-owned and intentionally separate
    /// from the raw DBI wire codec. The caller supplies explicit key/value
    /// codec tags, so local C++ storage types such as \c long can be mapped to
    /// a named logical wire type such as \c KeyValueLogicalInt64Codec<long>.
    /// Codec tags are part of the logical schema contract; changing them
    /// requires a new schema id, or an explicit schema-marker migration.
    /// Incoming logical apply suppresses local raw capture for the supplied
    /// transaction. It is used only when a caller explicitly invokes
    /// \c LogicalTableRegistry::preflight_then_apply().
    template<class KeyT, class ValueT,
             class KeyCodec, class ValueCodec,
             class Options = DefaultTableOptions>
    class KeyValueTableLogicalAdapter : public ILogicalTableAdapter {
    public:
        typedef KeyValueTable<KeyT, ValueT, Options> table_type;

        static_assert(std::is_same<typename KeyCodec::value_type,
                                   KeyT>::value,
                      "KeyValueTableLogicalAdapter key codec local type must match KeyT");
        static_assert(std::is_same<typename ValueCodec::value_type,
                                   ValueT>::value,
                      "KeyValueTableLogicalAdapter value codec local type must match ValueT");

        KeyValueTableLogicalAdapter(table_type& table,
                                    const std::string& schema_id,
                                    std::uint32_t schema_version = 1)
            : m_table(table),
              m_schema_id(schema_id),
              m_schema_version(schema_version) {
            if (m_schema_id.empty()) {
                throw std::invalid_argument(
                    "KeyValueTableLogicalAdapter schema id is empty");
            }
            if (m_table.dbi_name().empty()) {
                throw std::invalid_argument(
                    "KeyValueTableLogicalAdapter DBI name is empty");
            }
            if (m_schema_version == 0) {
                throw std::invalid_argument(
                    "KeyValueTableLogicalAdapter schema version is zero");
            }
        }

        LogicalSchemaRef schema_ref() const override {
            LogicalSchemaRef ref;
            ref.schema_id = m_schema_id;
            ref.kind = LogicalTableKind::KeyValue;
            ref.schema_version = m_schema_version;
            return ref;
        }

        std::string primary_dbi() const override {
            return m_table.dbi_name();
        }

        std::vector<std::string> affected_dbis() const override {
            std::vector<std::string> out;
            out.push_back(m_table.dbi_name());
            return out;
        }

        LogicalChange make_upsert(const KeyT& key, const ValueT& value) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = KeyValueLogicalUpsert;
            encode_upsert(key, value, change.payload);
            return change;
        }

        LogicalChange make_delete(const KeyT& key) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = KeyValueLogicalDelete;
            encode_key_only(key, change.payload);
            return change;
        }

        LogicalChange make_clear() const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = KeyValueLogicalClear;
            return change;
        }

        /// \brief Transaction-bound typed logical capture session.
        /// \details The session owns a writable transaction. Local writes are
        /// buffered as logical changes and raw capture is suppressed for the
        /// transaction. Pending changes are copied to the caller only by
        /// \c commit(out), after the session has prepared the destination
        /// vector and before the native commit. If commit fails, the appended
        /// tail is erased and the destructor rolls back the transaction.
        /// The adapter, table, and connection referenced by the adapter must
        /// outlive the session.
        class LogicalCaptureSession {
        public:
            explicit LogicalCaptureSession(
                    const KeyValueTableLogicalAdapter& adapter)
                : m_adapter(adapter),
                  m_txn(adapter.m_table.connection()->transaction(
                      TransactionMode::WRITABLE)),
                  m_active(true) {
                const LogicalApplyResult marker_result =
                    validate_logical_adapter_marker(
                        m_txn.handle(),
                        adapter.m_table.connection()->env_handle(),
                        adapter);
                if (!marker_result.ok) {
                    throw std::runtime_error(marker_result.error);
                }
            }

            ~LogicalCaptureSession() noexcept {
                rollback();
            }

            LogicalCaptureSession(const LogicalCaptureSession&) = delete;
            LogicalCaptureSession& operator=(
                    const LogicalCaptureSession&) = delete;

            void insert_or_assign(const KeyT& key, const ValueT& value) {
                ensure_active();
                LogicalChange change = m_adapter.make_upsert(key, value);
                const std::size_t previous_size = m_pending.size();
                m_pending.push_back(change);
                try {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    m_adapter.m_table.insert_or_assign(
                        key, value, m_txn.handle());
                } catch (...) {
                    m_pending.resize(previous_size);
                    throw;
                }
            }

            bool erase(const KeyT& key) {
                ensure_active();
                LogicalChange change = m_adapter.make_delete(key);
                const std::size_t previous_size = m_pending.size();
                m_pending.push_back(change);
                try {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    const bool removed =
                        m_adapter.m_table.erase(key, m_txn.handle());
                    if (!removed) {
                        m_pending.resize(previous_size);
                    }
                    return removed;
                } catch (...) {
                    m_pending.resize(previous_size);
                    throw;
                }
            }

            void clear() {
                ensure_active();
                LogicalChange change = m_adapter.make_clear();
                const std::size_t previous_size = m_pending.size();
                m_pending.push_back(change);
                try {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    m_adapter.m_table.clear(m_txn.handle());
                } catch (...) {
                    m_pending.resize(previous_size);
                    throw;
                }
            }

            /// \brief Commits local mutations and returns their logical changes.
            /// \warning The returned changes are not published atomically with
            /// the table transaction. Use \c commit_to_outbox() when local
            /// durability and delivery enqueueing must share one transaction.
            void commit(std::vector<LogicalChange>& out) {
                ensure_active();
                const std::size_t old_size = out.size();
                out.insert(out.end(), m_pending.begin(), m_pending.end());
                try {
                    m_txn.commit();
                } catch (...) {
                    out.erase(out.begin() +
                              static_cast<std::ptrdiff_t>(old_size),
                              out.end());
                    throw;
                }
                m_pending.clear();
                m_active = false;
            }

            /// \brief Commits captured table mutations and their delivery atomically.
            LogicalDeliveryEnvelope commit_to_outbox(
                    ILogicalDeliveryOutbox& outbox,
                    const DbId& destination,
                    const CodecBounds* bounds = nullptr) {
                ensure_active();
                LogicalChangeFrame frame;
                frame.changes = m_pending;
                const LogicalDeliveryEnvelope envelope =
                    outbox.enqueue_logical_delivery(
                        m_txn.handle(), destination, frame, bounds);
                m_txn.commit();
                m_pending.clear();
                m_active = false;
                return envelope;
            }

            void rollback() noexcept {
                if (!m_active) {
                    return;
                }
                try {
                    m_pending.clear();
                    m_txn.rollback();
                } catch (...) {
                }
                m_active = false;
            }

            std::size_t pending_size() const {
                return m_pending.size();
            }

        private:
            void ensure_active() const {
                if (!m_active) {
                    throw std::logic_error(
                        "KeyValue logical capture session is not active");
                }
            }

            const KeyValueTableLogicalAdapter& m_adapter;
            Transaction m_txn;
            std::vector<LogicalChange> m_pending;
            bool m_active;
        };

        std::unique_ptr<LogicalCaptureSession> begin_capture_session() const {
            return std::unique_ptr<LogicalCaptureSession>(
                new LogicalCaptureSession(*this));
        }

        LogicalApplyResult preflight(
                MDBX_txn* txn,
                const LogicalChange& change) const override {
            (void)txn;
            return validate_payload(change);
        }

        LogicalApplyResult apply(
                MDBX_txn* txn,
                const LogicalChange& change) override {
            const LogicalApplyResult validation = validate_payload(change);
            if (!validation.ok) return validation;

            try {
                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_table.connection(), txn);
                if (change.opcode == KeyValueLogicalUpsert) {
                    const std::pair<KeyT, ValueT> decoded =
                        decode_upsert(change.payload);
                    m_table.insert_or_assign(
                        decoded.first, decoded.second, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyValueLogicalDelete) {
                    const KeyT key = decode_key_only(change.payload);
                    (void)m_table.erase(key, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyValueLogicalClear) {
                    m_table.clear(txn);
                    return LogicalApplyResult::success();
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyValue logical adapter apply failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyValue logical adapter apply failed");
            }
            return LogicalApplyResult::failure(
                "KeyValue logical adapter opcode is unsupported");
        }

    private:
        struct PayloadCursor {
            const std::uint8_t* data;
            std::size_t size;
            std::size_t pos;
        };

        static void require(PayloadCursor& cursor, std::size_t size) {
            if (cursor.pos > cursor.size ||
                size > cursor.size - cursor.pos) {
                throw std::runtime_error(
                    "KeyValue logical payload underrun");
            }
        }

        static void append_blob(
                std::vector<std::uint8_t>& out,
                const std::vector<std::uint8_t>& value) {
            if (value.size() >
                static_cast<std::size_t>(
                    std::numeric_limits<std::uint32_t>::max())) {
                throw std::length_error(
                    "KeyValue logical payload blob is too large");
            }
            detail::append_u32_le(out,
                static_cast<std::uint32_t>(value.size()));
            out.insert(out.end(), value.begin(), value.end());
        }

        static std::vector<std::uint8_t> read_blob(PayloadCursor& cursor) {
            require(cursor, 4);
            const std::uint32_t size =
                detail::read_u32_le(cursor.data + cursor.pos);
            cursor.pos += 4;
            require(cursor, size);
            std::vector<std::uint8_t> out;
            if (size != 0u) {
                out.assign(cursor.data + cursor.pos,
                           cursor.data + cursor.pos + size);
            }
            cursor.pos += size;
            return out;
        }

        static PayloadCursor make_cursor(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = {
                payload.empty() ? nullptr : &payload[0],
                payload.size(),
                0
            };
            return cursor;
        }

        static void ensure_end(const PayloadCursor& cursor) {
            if (cursor.pos != cursor.size) {
                throw std::runtime_error(
                    "KeyValue logical payload has trailing bytes");
            }
        }

        static void encode_key_only(const KeyT& key,
                                    std::vector<std::uint8_t>& out) {
            out.clear();
            const std::vector<std::uint8_t> encoded_key =
                KeyCodec::encode(key);
            append_blob(out, encoded_key);
        }

        MDBX_txn* checked_adapter_txn(MDBX_txn* txn,
                                      const char* context) const {
            return checked_txn_env(txn, m_table.connection()->env_handle(),
                                   context);
        }

        static void encode_upsert(const KeyT& key,
                                  const ValueT& value,
                                  std::vector<std::uint8_t>& out) {
            out.clear();
            const std::vector<std::uint8_t> encoded_key =
                KeyCodec::encode(key);
            const std::vector<std::uint8_t> encoded_value =
                ValueCodec::encode(value);
            append_blob(out, encoded_key);
            append_blob(out, encoded_value);
        }

        static KeyT decode_key_only(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const std::vector<std::uint8_t> encoded_key = read_blob(cursor);
            ensure_end(cursor);
            return KeyCodec::decode(encoded_key);
        }

        static std::pair<KeyT, ValueT> decode_upsert(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const std::vector<std::uint8_t> encoded_key = read_blob(cursor);
            const std::vector<std::uint8_t> encoded_value = read_blob(cursor);
            ensure_end(cursor);
            return std::make_pair(
                KeyCodec::decode(encoded_key),
                ValueCodec::decode(encoded_value));
        }

        LogicalApplyResult validate_payload(
                const LogicalChange& change) const {
            try {
                if (change.opcode == KeyValueLogicalUpsert) {
                    (void)decode_upsert(change.payload);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyValueLogicalDelete) {
                    (void)decode_key_only(change.payload);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == KeyValueLogicalClear) {
                    if (!change.payload.empty()) {
                        return LogicalApplyResult::failure(
                            "KeyValue clear payload must be empty");
                    }
                    return LogicalApplyResult::success();
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyValue logical payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyValue logical payload is invalid");
            }
            return LogicalApplyResult::failure(
                "KeyValue logical adapter opcode is unsupported");
        }

        table_type& m_table;
        std::string m_schema_id;
        std::uint32_t m_schema_version;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ADAPTERS_KEY_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
