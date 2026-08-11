#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_VERSIONED_KEY_VALUE_TABLE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_VERSIONED_KEY_VALUE_TABLE_HPP_INCLUDED

/// \file logical/adapters/VersionedKeyValueTable.hpp
/// \brief Opt-in source-version writes for a \c KeyValueTable.

#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace mdbxc {
namespace sync {

    /// \brief Outcome of an attempted source-version write.
    enum class VersionedWriteResult {
        Applied, ///< Incoming source version won and was committed.
        Ignored  ///< A greater version or equal-version origin already won.
    };

    /// \brief Writes versioned raw changes for one \c KeyValueTable.
    /// \details A source version is compared lexicographically, then by the
    /// durable local \c NodeId. The table value stays unchanged when a newer
    /// version has already been accepted. Callers must use a canonical,
    /// non-empty byte encoding whose lexicographic order is the desired source
    /// order. Reusing one source version for different state from the same
    /// node throws \c std::invalid_argument.
    template<class KeyT, class ValueT, class Options = DefaultTableOptions>
    class VersionedKeyValueTable {
    public:
        /// \brief Binds versioned writes to one captured key-value table.
        /// \param table User table receiving versioned point mutations.
        /// \param capture Attached accumulator for the same connection.
        VersionedKeyValueTable(
                KeyValueTable<KeyT, ValueT, Options>& table,
                ThreadLocalChangeAccumulator& capture)
            : m_table(table), m_capture(capture) {
            register_table();
        }

        /// \brief Upserts a value when its source version wins.
        /// \param key User key to update.
        /// \param value User value stored outside sync version metadata.
        /// \param source_version Canonical non-empty source-version bytes.
        /// \return Whether the write was committed or superseded locally.
        /// \throws std::invalid_argument When the version is empty or reused
        ///         by this node for different state.
        /// \throws std::logic_error When sync identity or capture is absent.
        VersionedWriteResult insert_or_assign(
                const KeyT& key,
                const ValueT& value,
                const std::vector<std::uint8_t>& source_version) {
            if (source_version.empty()) {
                throw std::invalid_argument("VersionedKeyValueTable source_version is empty");
            }
            ensure_capture_attached();
            std::shared_ptr<Connection> connection = m_table.connection();
            auto txn = connection->transaction(TransactionMode::WRITABLE);
            const std::vector<std::uint8_t> storage_key = encode_key(key);
            const NodeId local_node = require_local_node(txn.handle());
            IdentityIndexStore index(connection->env_handle());
            index.open(txn.handle());
            if (!wins(index, txn.handle(), m_table.dbi_name(), storage_key,
                      source_version, local_node)) {
                validate_repeated_write(index, txn.handle(), storage_key,
                                        source_version, local_node,
                                        ChangeOpType::Put, encode_value(value));
                txn.commit();
                return VersionedWriteResult::Ignored;
            }
            {
                Connection::SyncCaptureSuppressionScope suppress(*connection, txn.handle());
                m_table.insert_or_assign(key, value, txn.handle());
            }
            record(txn.handle(), ChangeOpType::Put, storage_key,
                   encode_value(value), source_version);
            put_marker(index, txn.handle(), storage_key, local_node,
                       source_version, false);
            txn.commit();
            return VersionedWriteResult::Applied;
        }

        /// \brief Deletes a key when its source version wins.
        /// \param key User key to delete.
        /// \param source_version Canonical non-empty source-version bytes.
        /// \return Whether the delete was committed or superseded locally.
        /// \throws std::invalid_argument When the version is empty or reused
        ///         by this node for different state.
        /// \throws std::logic_error When sync identity or capture is absent.
        VersionedWriteResult erase(
                const KeyT& key,
                const std::vector<std::uint8_t>& source_version) {
            if (source_version.empty()) {
                throw std::invalid_argument("VersionedKeyValueTable source_version is empty");
            }
            ensure_capture_attached();
            std::shared_ptr<Connection> connection = m_table.connection();
            auto txn = connection->transaction(TransactionMode::WRITABLE);
            const std::vector<std::uint8_t> storage_key = encode_key(key);
            const NodeId local_node = require_local_node(txn.handle());
            IdentityIndexStore index(connection->env_handle());
            index.open(txn.handle());
            if (!wins(index, txn.handle(), m_table.dbi_name(), storage_key,
                      source_version, local_node)) {
                validate_repeated_write(index, txn.handle(), storage_key,
                                        source_version, local_node,
                                        ChangeOpType::Delete,
                                        std::vector<std::uint8_t>());
                txn.commit();
                return VersionedWriteResult::Ignored;
            }
            {
                Connection::SyncCaptureSuppressionScope suppress(*connection, txn.handle());
                (void)m_table.erase(key, txn.handle());
            }
            record(txn.handle(), ChangeOpType::Delete, storage_key,
                   std::vector<std::uint8_t>(), source_version);
            put_marker(index, txn.handle(), storage_key, local_node,
                       source_version, true);
            txn.commit();
            return VersionedWriteResult::Applied;
        }

    private:
        void register_table() {
            std::shared_ptr<Connection> connection = m_table.connection();
            auto txn = connection->transaction(TransactionMode::WRITABLE);
            VersionedDbiStore registry(connection->env_handle());
            if (!registry.contains(txn.handle(), m_table.dbi_name())) {
                if (!m_table.empty(txn.handle())) {
                    throw std::invalid_argument(
                        "VersionedKeyValueTable can register only an empty table");
                }
                (void)registry.register_dbi(txn.handle(), m_table.dbi_name());
            }
            txn.commit();
        }

        std::vector<std::uint8_t> encode_key(const KeyT& key) const {
            SerializeScratch scratch;
            const MDBX_val value = serialize_key<Options::safe_integer_key>(key, scratch);
            return bytes(value);
        }

        std::vector<std::uint8_t> encode_value(const ValueT& value) const {
            SerializeScratch scratch;
            const MDBX_val encoded = serialize_value(value, scratch);
            return bytes(encoded);
        }

        static std::vector<std::uint8_t> bytes(const MDBX_val& value) {
            if (value.iov_len == 0u) return std::vector<std::uint8_t>();
            const std::uint8_t* begin = static_cast<const std::uint8_t*>(value.iov_base);
            return std::vector<std::uint8_t>(begin, begin + value.iov_len);
        }

        NodeId require_local_node(MDBX_txn* txn) const {
            MetaStore meta(m_table.connection()->env_handle());
            meta.open(txn);
            const NodeId node = meta.get_node_id(txn);
            if (compare_node_id(node, make_zero_node()) == 0) {
                throw std::logic_error("VersionedKeyValueTable requires initialized SyncEngine identity");
            }
            return node;
        }

        static bool wins(IdentityIndexStore& index, MDBX_txn* txn,
                         const std::string& dbi_name,
                         const std::vector<std::uint8_t>& key,
                         const std::vector<std::uint8_t>& revision,
                         const NodeId& origin) {
            IdentityIndexValue current;
            if (!index.get(txn, dbi_name, key, current)) return true;
            if (revision != current.revision_key) return revision > current.revision_key;
            return compare_node_id(origin, current.origin_node_id) > 0;
        }

        void put_marker(IdentityIndexStore& index, MDBX_txn* txn,
                        const std::vector<std::uint8_t>& key,
                        const NodeId& origin,
                        const std::vector<std::uint8_t>& revision,
                        bool tombstone) const {
            IdentityIndexValue marker;
            marker.storage_key = key;
            marker.origin_node_id = origin;
            marker.revision_key = revision;
            if (tombstone) {
                index.tombstone(txn, m_table.dbi_name(), key, marker);
            } else {
                index.put(txn, m_table.dbi_name(), key, marker);
            }
        }

        void record(MDBX_txn* txn, ChangeOpType type,
                    const std::vector<std::uint8_t>& key,
                    const std::vector<std::uint8_t>& value,
                    const std::vector<std::uint8_t>& revision) const {
            ChangeOp op;
            op.op_type = type;
            op.dbi_flags = table_dbi_flags(txn);
            op.dbi_name = m_table.dbi_name();
            op.storage_key = key;
            op.value = value;
            op.op_flags = OP_HAS_REVISION_KEY;
            op.revision_key = revision;
            m_capture.record_change_op(txn, op);
        }

        void validate_repeated_write(
                IdentityIndexStore& index,
                MDBX_txn* txn,
                const std::vector<std::uint8_t>& key,
                const std::vector<std::uint8_t>& revision,
                const NodeId& origin,
                ChangeOpType type,
                const std::vector<std::uint8_t>& value) const {
            IdentityIndexValue current;
            if (!index.get(txn, m_table.dbi_name(), key, current) ||
                current.revision_key != revision ||
                compare_node_id(origin, current.origin_node_id) != 0) {
                return;
            }
            const bool is_tombstone =
                (current.flags & static_cast<std::uint32_t>(IDENTITY_TOMBSTONE)) != 0;
            if ((type == ChangeOpType::Delete && is_tombstone) ||
                (type == ChangeOpType::Put && !is_tombstone &&
                 stored_value_equals(txn, key, value))) {
                return;
            }
            throw std::invalid_argument(
                "VersionedKeyValueTable source_version was reused for different state");
        }

        bool stored_value_equals(
                MDBX_txn* txn,
                const std::vector<std::uint8_t>& key,
                const std::vector<std::uint8_t>& value) const {
            MDBX_dbi dbi = open_table_dbi(txn);
            MDBX_val db_key = { key.empty() ? nullptr :
                                const_cast<std::uint8_t*>(key.data()), key.size() };
            MDBX_val stored;
            const int rc = mdbx_get(txn, dbi, &db_key, &stored);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "VersionedKeyValueTable failed to read stored value");
            if (stored.iov_len != value.size()) return false;
            if (stored.iov_len == 0u) return true;
            return std::memcmp(stored.iov_base, value.data(), stored.iov_len) == 0;
        }

        std::uint32_t table_dbi_flags(MDBX_txn* txn) const {
            const MDBX_dbi dbi = open_table_dbi(txn);
            unsigned flags = 0u;
            check_mdbx(mdbx_dbi_flags(txn, dbi, &flags),
                       "VersionedKeyValueTable failed to read table flags");
            return static_cast<std::uint32_t>(flags);
        }

        MDBX_dbi open_table_dbi(MDBX_txn* txn) const {
            MDBX_dbi dbi = 0;
            check_mdbx(mdbx_dbi_open(txn, m_table.dbi_name().c_str(),
                                     MDBX_DB_ACCEDE, &dbi),
                       "VersionedKeyValueTable failed to open table DBI");
            return dbi;
        }

        void ensure_capture_attached() const {
            if (m_table.connection()->sync_capture() != &m_capture) {
                throw std::logic_error("VersionedKeyValueTable capture is not attached to the table connection");
            }
        }

        KeyValueTable<KeyT, ValueT, Options>& m_table;
        ThreadLocalChangeAccumulator&          m_capture;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_VERSIONED_KEY_VALUE_TABLE_HPP_INCLUDED
