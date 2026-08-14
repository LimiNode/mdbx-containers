#pragma once
#ifndef MDBX_CONTAINERS_HEADER_KEY_ORDERED_MULTI_VALUE_TABLE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_KEY_ORDERED_MULTI_VALUE_TABLE_HPP_INCLUDED

/// \file KeyOrderedMultiValueTable.hpp
/// \brief Ordered multi-value table storing an append sequence per key.
/// \details
/// Stores every appended key-value pair as a separate MDBX DUPSORT duplicate.
/// Duplicate values are prefixed with an internal per-key order number. Reads
/// for one key therefore preserve the order of currently stored values,
/// including repeated identical values.

#include "common.hpp"
#include <limits>
#include <vector>

namespace mdbxc {

    namespace sync {
        template<class KeyT, class ValueT, class KeyCodec, class ValueCodec,
                 class Options>
        class KeyOrderedMultiValueTableDestructiveLogicalAdapter;
    }

    /// \class KeyOrderedMultiValueTable
    /// \ingroup mdbxc_tables
    /// \brief Multi-value table where value order is part of the contract.
    /// \tparam KeyT Type of the keys.
    /// \tparam ValueT Type of the values.
    /// \tparam Options Compile-time table policy. Does not change the database
    ///         storage format.
    /// \details
    /// The MDBX key is the serialized user key. The MDBX duplicate value is
    /// \c big-endian-order || serialized-value. The order prefix is hidden from
    /// public reads and is local to each key. It is an internal presentation
    /// order for current records, not a stable public record identifier. The
    /// numeric prefix can be reused after deleting the last value for a key,
    /// erasing the key, or clearing the table.
    ///
    /// Use this table when repeated values and their current append order are
    /// significant. Use \ref KeyMultiValueTable when only pair multiplicity is
    /// significant and reconciliation by unordered multiset semantics is desired.
    ///
    /// \note Sync v0.1 emits no raw \c ChangeOp for this wrapper. The opt-in
    ///       schema-v1 logical adapter captures append history only. The
    ///       separate schema-v2 destructive logical adapter captures exact
    ///       element erasures, including bounded local selector expansion,
    ///       through one authoritative ordered-delivery origin.
    ///
    /// \note \c MDBX_DB_ACCEDE is accepted only for DBIs created with a
    ///       compatible \c KeyOrderedMultiValueTable storage contract: the same
    ///       key comparator choice, ascending bytewise duplicate ordering, and
    ///       duplicate values encoded with this table's internal order prefix.
    template<class KeyT, class ValueT, class Options = DefaultTableOptions>
    class KeyOrderedMultiValueTable final : public BaseTable {
        template<class, class, class, class, class>
        friend class sync::KeyOrderedMultiValueTableDestructiveLogicalAdapter;

    public:
        typedef std::pair<KeyT, ValueT> value_type;

        /// \brief Constructs table using existing connection.
        /// \param connection Existing connection.
        /// \param name Name of the table within the MDBX environment.
        /// \param flags Additional MDBX database flags.
        KeyOrderedMultiValueTable(std::shared_ptr<Connection> connection,
                                  std::string name = "ordered_multi_value_store",
                                  MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(std::move(connection),
                        std::move(name),
                        make_open_flags(flags)) {
            validate_storage_flags();
        }

        /// \brief Constructs table using configuration.
        /// \param config Configuration settings.
        /// \param name Name of the table within the MDBX environment.
        /// \param flags Additional MDBX database flags.
        explicit KeyOrderedMultiValueTable(const Config& config,
                                           std::string name = "ordered_multi_value_store",
                                           MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(Connection::create(config),
                        std::move(name),
                        make_open_flags(flags)) {
            validate_storage_flags();
        }

        /// \brief Destructor.
        ~KeyOrderedMultiValueTable() override = default;

        /// \brief Replaces table content with pairs from a vector.
        /// \param container Source pairs in the desired stored order.
        /// \return Reference to this table.
        KeyOrderedMultiValueTable& operator=(const std::vector<value_type>& container) {
            replace_with(container);
            return *this;
        }

        /// \brief Loads all key-value pairs into a vector.
        /// \param container Output vector.
        /// \param txn Optional transaction handle.
        void load(std::vector<value_type>& container, MDBX_txn* txn = nullptr) const {
            with_transaction([this, &container](MDBX_txn* t) {
                db_load(container, t);
            }, TransactionMode::READ_ONLY, txn);
        }

        /// \brief Loads all key-value pairs into a vector.
        /// \param container Output vector.
        /// \param txn Active transaction wrapper.
        void load(std::vector<value_type>& container, const Transaction& txn) const {
            load(container, txn.handle());
        }

        /// \brief Retrieves all key-value pairs into a vector.
        /// \param txn Optional transaction handle.
        /// \return Vector containing all pairs in key order and current per-key append order.
        std::vector<value_type> retrieve_all_vector(MDBX_txn* txn = nullptr) const {
            std::vector<value_type> container;
            load(container, txn);
            return container;
        }

        /// \brief Retrieves all key-value pairs into a vector.
        /// \param txn Active transaction wrapper.
        /// \return Vector containing all pairs in key order and current per-key append order.
        std::vector<value_type> retrieve_all_vector(const Transaction& txn) const {
            return retrieve_all_vector(txn.handle());
        }

        /// \brief Convenience wrapper around \ref retrieve_all_vector().
        std::vector<value_type> operator()() const {
            return retrieve_all_vector();
        }

        /// \brief Retrieves key-value pairs within an inclusive key range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Optional transaction handle.
        /// \return Vector containing matching pairs in key order and current per-key append order.
        std::vector<value_type> range_vector(const KeyT& from_key, const KeyT& to_key,
                                             MDBX_txn* txn = nullptr) const {
            std::vector<value_type> pairs;
            with_transaction([this, &from_key, &to_key, &pairs](MDBX_txn* t) {
                db_range(from_key, to_key, pairs, t);
            }, TransactionMode::READ_ONLY, txn);
            return pairs;
        }

        /// \brief Retrieves key-value pairs within an inclusive key range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Active transaction wrapper.
        /// \return Vector containing matching pairs in key order and current per-key append order.
        std::vector<value_type> range_vector(const KeyT& from_key, const KeyT& to_key,
                                             const Transaction& txn) const {
            return range_vector(from_key, to_key, txn.handle());
        }

        /// \brief Appends one value under a key.
        /// \param key Key to append to.
        /// \param value Value to append.
        /// \param txn Optional transaction handle.
        void append(const KeyT& key, const ValueT& value, MDBX_txn* txn = nullptr) {
            with_transaction([this, &key, &value](MDBX_txn* t) {
                db_append_one(key, value, t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Appends one value under a key.
        /// \param key Key to append to.
        /// \param value Value to append.
        /// \param txn Active transaction wrapper.
        void append(const KeyT& key, const ValueT& value, const Transaction& txn) {
            append(key, value, txn.handle());
        }

        /// \brief Alias for \ref append().
        void insert(const KeyT& key, const ValueT& value, MDBX_txn* txn = nullptr) {
            append(key, value, txn);
        }

        /// \brief Alias for \ref append().
        void insert(const KeyT& key, const ValueT& value, const Transaction& txn) {
            append(key, value, txn.handle());
        }

        /// \brief Appends pairs from a vector in vector iteration order.
        /// \param container Source pairs.
        /// \param txn Optional transaction handle.
        void append(const std::vector<value_type>& container, MDBX_txn* txn = nullptr) {
            with_transaction([this, &container](MDBX_txn* t) {
                db_append(container, t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Appends pairs from a vector in vector iteration order.
        /// \param container Source pairs.
        /// \param txn Active transaction wrapper.
        void append(const std::vector<value_type>& container, const Transaction& txn) {
            append(container, txn.handle());
        }

        /// \brief Replaces all table content with pairs from a vector.
        /// \param container Source pairs in the desired stored order.
        /// \param txn Optional transaction handle.
        void replace_with(const std::vector<value_type>& container, MDBX_txn* txn = nullptr) {
            with_transaction([this, &container](MDBX_txn* t) {
#           if MDBXC_SYNC_ENABLED
                ensure_logical_operation_supported(t, "replace_with()");
#           endif
                db_clear(t);
                db_append(container, t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Replaces all table content with pairs from a vector.
        /// \param container Source pairs in the desired stored order.
        /// \param txn Active transaction wrapper.
        void replace_with(const std::vector<value_type>& container, const Transaction& txn) {
            replace_with(container, txn.handle());
        }

        /// \brief Finds all values for a key.
        /// \param key Key to search for.
        /// \param txn Optional transaction handle.
        /// \return Values stored for the key in current append order.
        std::vector<ValueT> find(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::vector<ValueT> values;
            with_transaction([this, &key, &values](MDBX_txn* t) {
                db_find(key, values, t);
            }, TransactionMode::READ_ONLY, txn);
            return values;
        }

        /// \brief Finds all values for a key.
        /// \param key Key to search for.
        /// \param txn Active transaction wrapper.
        /// \return Values stored for the key in current append order.
        std::vector<ValueT> find(const KeyT& key, const Transaction& txn) const {
            return find(key, txn.handle());
        }

        /// \brief Checks whether a key exists.
        bool contains(const KeyT& key, MDBX_txn* txn = nullptr) const {
            bool result = false;
            with_transaction([this, &key, &result](MDBX_txn* t) {
                result = db_contains_key(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Checks whether a key exists.
        bool contains(const KeyT& key, const Transaction& txn) const {
            return contains(key, txn.handle());
        }

        /// \brief Checks whether a key-value pair exists.
        bool contains(const KeyT& key, const ValueT& value, MDBX_txn* txn = nullptr) const {
            return count(key, value, txn) > 0;
        }

        /// \brief Checks whether a key-value pair exists.
        bool contains(const KeyT& key, const ValueT& value, const Transaction& txn) const {
            return contains(key, value, txn.handle());
        }

        /// \brief Counts all stored pairs.
        std::size_t count(MDBX_txn* txn = nullptr) const {
            std::size_t result = 0;
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_count(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Counts all stored pairs.
        std::size_t count(const Transaction& txn) const {
            return count(txn.handle());
        }

        /// \brief Counts values stored for a key.
        std::size_t count(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::size_t result = 0;
            with_transaction([this, &key, &result](MDBX_txn* t) {
                result = db_count_key(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Counts values stored for a key.
        std::size_t count(const KeyT& key, const Transaction& txn) const {
            return count(key, txn.handle());
        }

        /// \brief Counts exact value matches under a key.
        std::size_t count(const KeyT& key, const ValueT& value, MDBX_txn* txn = nullptr) const {
            std::size_t result = 0;
            with_transaction([this, &key, &value, &result](MDBX_txn* t) {
                result = db_count_pair(key, value, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Counts exact value matches under a key.
        std::size_t count(const KeyT& key, const ValueT& value, const Transaction& txn) const {
            return count(key, value, txn.handle());
        }

        /// \brief Checks whether the table has no pairs.
        bool empty(MDBX_txn* txn = nullptr) const {
            return count(txn) == 0;
        }

        /// \brief Checks whether the table has no pairs.
        bool empty(const Transaction& txn) const {
            return empty(txn.handle());
        }

        /// \brief Erases all values for a key.
        /// \return \c true if the key existed.
        bool erase(const KeyT& key, MDBX_txn* txn = nullptr) {
            bool result = false;
            with_transaction([this, &key, &result](MDBX_txn* t) {
#           if MDBXC_SYNC_ENABLED
                ensure_logical_operation_supported(t, "erase(key)");
#           endif
                result = db_erase_key(key, t);
            }, TransactionMode::WRITABLE, txn);
            return result;
        }

        /// \brief Erases all values for a key.
        /// \return \c true if the key existed.
        bool erase(const KeyT& key, const Transaction& txn) {
            return erase(key, txn.handle());
        }

        /// \brief Erases all exact value matches under a key.
        /// \return Number of removed values.
        std::size_t erase(const KeyT& key, const ValueT& value, MDBX_txn* txn = nullptr) {
            std::size_t removed = 0;
            with_transaction([this, &key, &value, &removed](MDBX_txn* t) {
#           if MDBXC_SYNC_ENABLED
                ensure_logical_operation_supported(t, "erase(key, value)");
#           endif
                removed = db_erase_pair(key, value, t);
            }, TransactionMode::WRITABLE, txn);
            return removed;
        }

        /// \brief Erases all exact value matches under a key.
        /// \return Number of removed values.
        std::size_t erase(const KeyT& key, const ValueT& value, const Transaction& txn) {
            return erase(key, value, txn.handle());
        }

        /// \brief Erases the value at a zero-based position within one key.
        /// \param key Key whose ordered value is removed.
        /// \param index Zero-based append-order index under \p key.
        /// \param txn Optional transaction handle.
        /// \return \c true if the indexed value existed and was removed.
        bool erase_at(const KeyT& key, std::size_t index, MDBX_txn* txn = nullptr) {
            bool removed = false;
            with_transaction([this, &key, &index, &removed](MDBX_txn* t) {
#           if MDBXC_SYNC_ENABLED
                ensure_logical_operation_supported(t, "erase_at()");
#           endif
                removed = db_erase_at(key, index, t);
            }, TransactionMode::WRITABLE, txn);
            return removed;
        }

        /// \brief Erases the value at a zero-based position within one key.
        bool erase_at(const KeyT& key, std::size_t index, const Transaction& txn) {
            return erase_at(key, index, txn.handle());
        }

        /// \brief Removes all pairs.
        void clear(MDBX_txn* txn = nullptr) {
            with_transaction([this](MDBX_txn* t) {
#           if MDBXC_SYNC_ENABLED
                ensure_logical_operation_supported(t, "clear()");
#           endif
                db_clear(t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Removes all pairs.
        void clear(const Transaction& txn) {
            clear(txn.handle());
        }

    private:
        static const std::size_t order_size = sizeof(std::uint64_t);

        static MDBX_db_flags_t make_open_flags(MDBX_db_flags_t flags) {
            validate_requested_flags(flags);
            const unsigned requested = static_cast<unsigned>(flags);
            if ((requested & MDBX_DB_ACCEDE) != 0 &&
                (requested & MDBX_CREATE) == 0) {
                return flags;
            }
            return static_cast<MDBX_db_flags_t>(
                flags | MDBX_DUPSORT | get_mdbx_flags<KeyT>());
        }

        static bool expects_integer_key() {
            const unsigned expected =
                static_cast<unsigned>(get_mdbx_flags<KeyT>());
            return (expected & static_cast<unsigned>(MDBX_INTEGERKEY)) != 0u;
        }

        template<typename T>
        static typename std::enable_if<is_key_integral<T>::value, std::size_t>::type
        expected_integer_key_size_for() {
            return mdbx_integer_key_storage_size<T>::value;
        }

        template<typename T>
        static typename std::enable_if<std::is_same<T, float>::value, std::size_t>::type
        expected_integer_key_size_for() {
            return sizeof(std::uint32_t);
        }

        template<typename T>
        static typename std::enable_if<std::is_same<T, double>::value, std::size_t>::type
        expected_integer_key_size_for() {
            return sizeof(std::uint64_t);
        }

        template<typename T>
        static typename std::enable_if<
            !is_key_integral<T>::value &&
            !std::is_same<T, float>::value &&
            !std::is_same<T, double>::value,
            std::size_t>::type
        expected_integer_key_size_for() {
            return 0u;
        }

        static std::size_t expected_integer_key_size() {
            return expected_integer_key_size_for<KeyT>();
        }

        static void validate_requested_flags(MDBX_db_flags_t flags) {
            const unsigned requested = static_cast<unsigned>(flags);
            if (requested & MDBX_REVERSEDUP) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTable requires bytewise ascending duplicate ordering");
            }
            if (requested & MDBX_INTEGERDUP) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTable duplicate values are not MDBX_INTEGERDUP records");
            }
            if (requested & MDBX_DUPFIXED) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTable duplicate values are variable-sized");
            }
            if ((requested & static_cast<unsigned>(MDBX_INTEGERKEY)) != 0u &&
                !expects_integer_key()) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTable MDBX_INTEGERKEY is incompatible with KeyT");
            }
        }

        void validate_storage_flags() const {
            const std::uint32_t flags = m_dbi_flags;
            if ((flags & MDBX_DUPSORT) == 0) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTable requires an MDBX_DUPSORT DBI");
            }
            if ((flags & MDBX_REVERSEDUP) != 0) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTable cannot use MDBX_REVERSEDUP");
            }
            if ((flags & MDBX_INTEGERDUP) != 0) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTable cannot use MDBX_INTEGERDUP");
            }
            if ((flags & MDBX_DUPFIXED) != 0) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTable cannot use MDBX_DUPFIXED");
            }

            const bool actual_integer_key =
                (flags & static_cast<std::uint32_t>(MDBX_INTEGERKEY)) != 0u;
            if (actual_integer_key != expects_integer_key()) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTable DBI key comparator is incompatible with KeyT");
            }
            if (actual_integer_key) {
                validate_existing_integer_key_width();
            }
        }

        void validate_existing_integer_key_width() const {
            const std::size_t expected_size = expected_integer_key_size();
            if (expected_size == 0u) {
                return;
            }

            auto txn = m_connection->transaction(TransactionMode::READ_ONLY);
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn.handle(), m_dbi, cursor.out()),
                       "Failed to open cursor for ordered key comparator validation");

            MDBX_val key;
            MDBX_val value;
            const int rc = mdbx_cursor_get(cursor.get(), &key, &value, MDBX_FIRST);
            if (rc == MDBX_NOTFOUND) {
                return;
            }
            check_mdbx(rc, "Failed to read first key for ordered key comparator validation");
            if (key.iov_len != expected_size) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValueTable existing INTEGERKEY DBI key width is incompatible with KeyT");
            }
        }

        template<typename F>
        void with_transaction(F&& action, TransactionMode mode, MDBX_txn* txn = nullptr) const {
            if (txn) {
                action(checked_external_txn(txn));
                return;
            }
            txn = thread_txn();
            if (txn) {
                action(txn);
                return;
            }

            auto txn_guard = m_connection->transaction(mode);
            try {
                action(txn_guard.handle());
                txn_guard.commit();
            } catch (...) {
                try { txn_guard.rollback(); } catch (...) {}
                throw;
            }
        }

        static void encode_order(std::uint64_t order, std::uint8_t* out) {
            for (std::size_t i = 0; i < order_size; ++i) {
                out[i] = static_cast<std::uint8_t>((order >> ((order_size - 1 - i) * 8)) & 0xffu);
            }
        }

        static std::uint64_t decode_order(const MDBX_val& stored) {
            if (stored.iov_len < order_size) {
                throw std::runtime_error("Corrupted ordered multi-value record");
            }
            const std::uint8_t* bytes = static_cast<const std::uint8_t*>(stored.iov_base);
            std::uint64_t order = 0;
            for (std::size_t i = 0; i < order_size; ++i) {
                order = (order << 8) | static_cast<std::uint64_t>(bytes[i]);
            }
            return order;
        }

        static MDBX_val strip_order(const MDBX_val& stored) {
            if (stored.iov_len < order_size) {
                throw std::runtime_error("Corrupted ordered multi-value record");
            }
            MDBX_val raw;
            raw.iov_base = static_cast<std::uint8_t*>(stored.iov_base) + order_size;
            raw.iov_len = stored.iov_len - order_size;
            return raw;
        }

        MDBX_val make_stored_value(std::uint64_t order,
                                   const ValueT& value,
                                   SerializeScratch& sc_value) const {
            SerializeScratch raw_scratch;
            MDBX_val raw_value = serialize_value(value, raw_scratch);
            sc_value.bytes.resize(order_size + raw_value.iov_len);
            encode_order(order, sc_value.bytes.data());
            if (raw_value.iov_len) {
                std::memcpy(sc_value.bytes.data() + order_size, raw_value.iov_base, raw_value.iov_len);
            }
            return sc_value.view_bytes();
        }

        MDBX_val make_comparable_value(const ValueT& value, SerializeScratch& sc_value) const {
            SerializeScratch raw_scratch;
            MDBX_val raw_value = serialize_value(value, raw_scratch);
            sc_value.bytes.resize(raw_value.iov_len);
            if (raw_value.iov_len) {
                std::memcpy(sc_value.bytes.data(), raw_value.iov_base, raw_value.iov_len);
            }
            return sc_value.view_bytes();
        }

        bool stored_value_matches(const MDBX_val& stored, const MDBX_val& raw_value) const {
            MDBX_val stored_raw = strip_order(stored);
            if (stored_raw.iov_len != raw_value.iov_len) {
                return false;
            }
            if (stored_raw.iov_len == 0) {
                return true;
            }
            return std::memcmp(stored_raw.iov_base, raw_value.iov_base, stored_raw.iov_len) == 0;
        }

        std::uint64_t next_order(const KeyT& key, MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");

            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_KEY);
            if (rc == MDBX_NOTFOUND) {
                return 0;
            }
            check_mdbx(rc, "Failed to seek key");
            rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_LAST_DUP);
            check_mdbx(rc, "Failed to seek last ordered value");
            const std::uint64_t last = decode_order(db_val);
            if (last == std::numeric_limits<std::uint64_t>::max()) {
                throw std::overflow_error("Per-key ordered multi-value sequence exhausted");
            }
            return last + 1;
        }

        void db_load(std::vector<value_type>& container, MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");

            MDBX_val db_key, db_val;
            int rc = MDBX_SUCCESS;
            while ((rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT)) == MDBX_SUCCESS) {
                KeyT key = deserialize_key<KeyT>(db_key);
                ValueT value = deserialize_value<ValueT>(strip_order(db_val));
                container.push_back(value_type(std::move(key), std::move(value)));
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to read ordered multi-value table");
            }
        }

        void db_range(const KeyT& from_key, const KeyT& to_key,
                      std::vector<value_type>& pairs, MDBX_txn* txn) const {
            SerializeScratch sc_from_key;
            SerializeScratch sc_to_key;
            MDBX_val db_from_key = serialize_key<Options::safe_integer_key>(from_key, sc_from_key);
            MDBX_val db_to_key = serialize_key<Options::safe_integer_key>(to_key, sc_to_key);

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            if (mdbx_cmp(txn, m_dbi, &db_from_key, &db_to_key) > 0) {
                return;
            }

            MDBX_val db_key = db_from_key;
            MDBX_val db_val;
            bool stopped_by_upper_bound = false;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            while (rc == MDBX_SUCCESS) {
                if (mdbx_cmp(txn, m_dbi, &db_key, &db_to_key) > 0) {
                    stopped_by_upper_bound = true;
                    break;
                }
                KeyT key = deserialize_key<KeyT>(db_key);
                ValueT value = deserialize_value<ValueT>(strip_order(db_val));
                pairs.push_back(value_type(std::move(key), std::move(value)));
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
            }
            if (!stopped_by_upper_bound && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to read ordered multi-value key range");
            }
        }

        void db_append_one(const KeyT& key, const ValueT& value, MDBX_txn* txn) {
            SerializeScratch sc_key;
            SerializeScratch sc_value;
            const std::uint64_t order = next_order(key, txn);
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val = make_stored_value(order, value, sc_value);
            check_dupsort_value_size(db_val);
            check_mdbx(mdbx_put(txn, m_dbi, &db_key, &db_val, MDBX_UPSERT),
                       "Failed to append ordered multi-value record");
#       if MDBXC_SYNC_ENABLED
            record_logical_insert(txn, &key, &value);
#       endif
        }

        void db_append(const std::vector<value_type>& container, MDBX_txn* txn) {
            for (typename std::vector<value_type>::const_iterator it = container.begin();
                 it != container.end(); ++it) {
                db_append_one(it->first, it->second, txn);
            }
        }

        void db_find(const KeyT& key, std::vector<ValueT>& values, MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");

            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_KEY);
            if (rc == MDBX_NOTFOUND) {
                return;
            }
            check_mdbx(rc, "Failed to seek key");
            while (rc == MDBX_SUCCESS) {
                values.push_back(deserialize_value<ValueT>(strip_order(db_val)));
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT_DUP);
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to read ordered duplicate values");
            }
        }

        template<typename BeforeInspect>
        void db_collect_values(const KeyT& key,
                               std::vector<ValueT>& values,
                               MDBX_txn* txn,
                               BeforeInspect before_inspect) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()),
                       "Failed to open MDBX cursor");

            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_KEY);
            if (rc == MDBX_NOTFOUND) {
                return;
            }
            check_mdbx(rc, "Failed to seek key");
            while (rc == MDBX_SUCCESS) {
                before_inspect();
                values.push_back(deserialize_value<ValueT>(strip_order(db_val)));
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT_DUP);
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to read ordered duplicate values");
            }
        }

        template<typename BeforeInspect>
        void db_collect_entries(std::vector<value_type>& entries,
                                MDBX_txn* txn,
                                BeforeInspect before_inspect) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()),
                       "Failed to open MDBX cursor");

            MDBX_val db_key;
            MDBX_val db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_FIRST);
            while (rc == MDBX_SUCCESS) {
                before_inspect();
                entries.push_back(value_type(
                    deserialize_key<KeyT>(db_key),
                    deserialize_value<ValueT>(strip_order(db_val))));
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to read ordered multi-value records");
            }
        }

        bool db_contains_key(const KeyT& key, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_SUCCESS) return true;
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to check ordered multi-value key presence");
            return false;
        }

        std::size_t db_count(MDBX_txn* txn) const {
            MDBX_stat stat;
            check_mdbx(mdbx_dbi_stat(txn, m_dbi, &stat, sizeof(stat)),
                       "Failed to query ordered multi-value table statistics");
            return stat.ms_entries;
        }

        std::size_t db_count_key(const KeyT& key, MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");

            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_KEY);
            if (rc == MDBX_NOTFOUND) {
                return 0;
            }
            check_mdbx(rc, "Failed to seek key");
            size_t found = 0;
            check_mdbx(mdbx_cursor_count(cursor.get(), &found), "Failed to count ordered duplicate values");
            return found;
        }

        std::size_t db_count_pair(const KeyT& key, const ValueT& value, MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");

            SerializeScratch sc_key;
            SerializeScratch sc_value;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val raw_value = make_comparable_value(value, sc_value);
            MDBX_val db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_KEY);
            if (rc == MDBX_NOTFOUND) {
                return 0;
            }
            check_mdbx(rc, "Failed to seek key");
            std::size_t found = 0;
            while (rc == MDBX_SUCCESS) {
                if (stored_value_matches(db_val, raw_value)) {
                    ++found;
                }
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT_DUP);
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to scan ordered duplicate values");
            }
            return found;
        }

        bool db_erase_key(const KeyT& key, MDBX_txn* txn) {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            int rc = mdbx_del(txn, m_dbi, &db_key, nullptr);
            if (rc == MDBX_SUCCESS) return true;
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to erase ordered multi-value key");
            return false;
        }

        std::size_t db_erase_pair(const KeyT& key, const ValueT& value, MDBX_txn* txn) {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");

            SerializeScratch sc_key;
            SerializeScratch sc_value;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val raw_value = make_comparable_value(value, sc_value);
            MDBX_val db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_KEY);
            if (rc == MDBX_NOTFOUND) {
                return 0;
            }
            check_mdbx(rc, "Failed to seek key");
            std::size_t removed = 0;
            while (rc == MDBX_SUCCESS) {
                if (stored_value_matches(db_val, raw_value)) {
                    check_mdbx(mdbx_cursor_del(cursor.get(), MDBX_CURRENT),
                               "Failed to erase ordered duplicate value");
                    ++removed;
                }
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT_DUP);
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to scan ordered duplicate values");
            }
            return removed;
        }

        bool db_erase_at(const KeyT& key, std::size_t index, MDBX_txn* txn) {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");

            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_KEY);
            if (rc == MDBX_NOTFOUND) {
                return false;
            }
            check_mdbx(rc, "Failed to seek key");
            std::size_t current = 0;
            while (rc == MDBX_SUCCESS) {
                if (current == index) {
                    check_mdbx(mdbx_cursor_del(cursor.get(), MDBX_CURRENT),
                               "Failed to erase ordered duplicate by index");
                    return true;
                }
                ++current;
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT_DUP);
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to scan ordered duplicate values");
            }
            return false;
        }

        template<typename BeforeInspect>
        bool db_erase_at(const KeyT& key,
                         std::size_t index,
                         MDBX_txn* txn,
                         BeforeInspect before_inspect) {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()),
                       "Failed to open MDBX cursor");

            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_KEY);
            if (rc == MDBX_NOTFOUND) {
                return false;
            }
            check_mdbx(rc, "Failed to seek key");
            std::size_t current = 0u;
            while (rc == MDBX_SUCCESS) {
                before_inspect();
                if (current == index) {
                    check_mdbx(mdbx_cursor_del(cursor.get(), MDBX_CURRENT),
                               "Failed to erase ordered duplicate by index");
                    return true;
                }
                ++current;
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT_DUP);
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to scan ordered duplicate values");
            }
            return false;
        }

        void db_clear(MDBX_txn* txn) {
            check_mdbx(mdbx_drop(txn, m_dbi, 0), "Failed to clear ordered multi-value table");
        }
    };

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_KEY_ORDERED_MULTI_VALUE_TABLE_HPP_INCLUDED
