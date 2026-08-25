#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SEQUENCE_TABLE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SEQUENCE_TABLE_HPP_INCLUDED

/// \file SequenceTable.hpp
/// \brief Persistent appendable table with stable uint64_t indices.
/// \details
/// Provides a sparse sequence of values identified by stable uint64_t indices.
/// Indices do not shift when records are erased; append() uses max(existing)+1.

#include "common.hpp"
#include <vector>
#include <utility>
#include <limits>

namespace mdbxc {

    /// \class SequenceTable
    /// \ingroup mdbxc_tables
    /// \brief Persistent appendable table with stable uint64_t indices.
    /// \tparam ValueT Type of the stored values.
    /// \details
    /// Indices are stable record identifiers, not dense vector positions.
    /// Erasing a record does not shift following indices. insert_or_assign()
    /// and set() may create holes. append() uses max existing index + 1.
    /// \warning append() is not a global atomic sequence allocator.
    ///          Concurrent writers appending without external synchronization
    ///          may race for the same next index.
    template<class ValueT>
    class SequenceTable final : public BaseTable {
    public:
        /// \brief Constructs table using existing connection.
        /// \param connection Existing connection.
        /// \param name Name of the table within the MDBX environment.
        /// \param flags Additional MDBX database flags.
        explicit SequenceTable(std::shared_ptr<Connection> connection,
                               std::string name = "sequence_table",
                               MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(std::move(connection),
                        std::move(name),
                        flags | get_mdbx_flags<uint64_t>()) {}

        /// \brief Constructs table using configuration.
        /// \param config Configuration settings.
        /// \param name Name of the table within the MDBX environment.
        /// \param flags Additional MDBX database flags.
        explicit SequenceTable(const Config& config,
                               std::string name = "sequence_table",
                               MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(Connection::create(config),
                        std::move(name),
                        flags | get_mdbx_flags<uint64_t>()) {}

        /// \brief Destructor.
        ~SequenceTable() override = default;

        /// \brief Appends a value at the next available index.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        /// \return Index assigned to the value.
        /// \throws std::overflow_error if the index space is exhausted.
        /// \warning append() computes next id from current max key; concurrent
        ///          unsynchronized writers may race for the same next index.
        uint64_t append(const ValueT& value, MDBX_txn* txn = nullptr) {
            uint64_t result = 0;
            with_transaction([this, &value, &result](MDBX_txn* t) {
                result = db_append(value, t);
            }, TransactionMode::WRITABLE, txn);
            return result;
        }

        /// \brief Appends a value at the next available index using an external transaction.
        /// \param value Value to store.
        /// \param txn Active transaction wrapper.
        /// \return Index assigned to the value.
        uint64_t append(const ValueT& value, const Transaction& txn) {
            return append(value, txn.handle());
        }

        /// \brief Appends multiple values from a container.
        /// \tparam ContainerT Container type with const_iterator.
        /// \param values Source container of values.
        /// \param txn Optional transaction handle.
        /// \return Vector of assigned indices.
        /// \warning Same concurrency warning as append().
        template<class ContainerT>
        std::vector<uint64_t> append_many(const ContainerT& values, MDBX_txn* txn = nullptr) {
            std::vector<uint64_t> result;
            with_transaction([this, &values, &result](MDBX_txn* t) {
                for (typename ContainerT::const_iterator it = values.begin();
                     it != values.end(); ++it) {
                    result.push_back(db_append(*it, t));
                }
            }, TransactionMode::WRITABLE, txn);
            return result;
        }

        /// \brief Appends multiple values from a container using an external transaction.
        /// \tparam ContainerT Container type with const_iterator.
        /// \param values Source container of values.
        /// \param txn Active transaction wrapper.
        /// \return Vector of assigned indices.
        template<class ContainerT>
        std::vector<uint64_t> append_many(const ContainerT& values, const Transaction& txn) {
            return append_many(values, txn.handle());
        }

        /// \brief Retrieves the value at the given index or throws.
        /// \param id Index to look up.
        /// \param txn Optional transaction handle.
        /// \return Stored value.
        /// \throws std::out_of_range if the index is absent.
        ValueT at(uint64_t id, MDBX_txn* txn = nullptr) const {
            ValueT value;
            with_transaction([this, id, &value](MDBX_txn* t) {
                if (!db_get(id, value, t)) {
                    throw std::out_of_range("SequenceTable::at: id not found");
                }
            }, TransactionMode::READ_ONLY, txn);
            return value;
        }

        /// \brief Retrieves the value at the given index using an external transaction.
        /// \param id Index to look up.
        /// \param txn Active transaction wrapper.
        /// \return Stored value.
        /// \throws std::out_of_range if the index is absent.
        ValueT at(uint64_t id, const Transaction& txn) const {
            return at(id, txn.handle());
        }

        /// \brief Tries to retrieve the value at the given index.
        /// \param id Index to look up.
        /// \param out Output value.
        /// \param txn Optional transaction handle.
        /// \return \c true if the value exists.
        bool try_get(uint64_t id, ValueT& out, MDBX_txn* txn = nullptr) const {
            bool res = false;
            with_transaction([this, id, &out, &res](MDBX_txn* t) {
                res = db_get(id, out, t);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }

        /// \brief Tries to retrieve the value at the given index using an external transaction.
        /// \param id Index to look up.
        /// \param out Output value.
        /// \param txn Active transaction wrapper.
        /// \return \c true if the value exists.
        bool try_get(uint64_t id, ValueT& out, const Transaction& txn) const {
            return try_get(id, out, txn.handle());
        }

#       if __cplusplus >= 201703L
        /// \brief Finds the value at the given index.
        /// \param id Index to look up.
        /// \param txn Optional transaction handle.
        /// \return Optional containing the value when present.
        std::optional<ValueT> find(uint64_t id, MDBX_txn* txn = nullptr) const {
            std::optional<ValueT> result;
            with_transaction([this, id, &result](MDBX_txn* t) {
                ValueT tmp;
                if (db_get(id, tmp, t)) {
                    result = std::move(tmp);
                }
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds the value at the given index using an external transaction.
        /// \param id Index to look up.
        /// \param txn Active transaction wrapper.
        /// \return Optional containing the value when present.
        std::optional<ValueT> find(uint64_t id, const Transaction& txn) const {
            return find(id, txn.handle());
        }
#       endif

        /// \brief Finds the value at the given index in a C++11-compatible form.
        /// \param id Index to look up.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and value.
        std::pair<bool, ValueT> find_compat(uint64_t id, MDBX_txn* txn = nullptr) const {
            std::pair<bool, ValueT> result(false, ValueT());
            with_transaction([this, id, &result](MDBX_txn* t) {
                if (db_get(id, result.second, t)) {
                    result.first = true;
                }
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds the value at the given index in a C++11-compatible form.
        /// \param id Index to look up.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and value.
        std::pair<bool, ValueT> find_compat(uint64_t id, const Transaction& txn) const {
            return find_compat(id, txn.handle());
        }

        /// \brief Checks whether an index exists.
        /// \param id Index to check.
        /// \param txn Optional transaction handle.
        /// \return \c true if the index exists.
        bool contains(uint64_t id, MDBX_txn* txn = nullptr) const {
            bool res = false;
            with_transaction([this, id, &res](MDBX_txn* t) {
                res = db_contains(id, t);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }

        /// \brief Checks whether an index exists using an external transaction.
        /// \param id Index to check.
        /// \param txn Active transaction wrapper.
        /// \return \c true if the index exists.
        bool contains(uint64_t id, const Transaction& txn) const {
            return contains(id, txn.handle());
        }

        /// \brief Inserts or replaces a value at the given index.
        /// \param id Index to write.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        void insert_or_assign(uint64_t id, const ValueT& value, MDBX_txn* txn = nullptr) {
            with_transaction([this, id, &value](MDBX_txn* t) {
                db_set(id, value, t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Inserts or replaces a value at the given index using an external transaction.
        /// \param id Index to write.
        /// \param value Value to store.
        /// \param txn Active transaction wrapper.
        void insert_or_assign(uint64_t id, const ValueT& value, const Transaction& txn) {
            insert_or_assign(id, value, txn.handle());
        }

        /// \brief Sets a value at the given index (alias for insert_or_assign).
        /// \param id Index to write.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        void set(uint64_t id, const ValueT& value, MDBX_txn* txn = nullptr) {
            insert_or_assign(id, value, txn);
        }

        /// \brief Sets a value at the given index using an external transaction.
        /// \param id Index to write.
        /// \param value Value to store.
        /// \param txn Active transaction wrapper.
        void set(uint64_t id, const ValueT& value, const Transaction& txn) {
            insert_or_assign(id, value, txn);
        }

        /// \brief Erases the record at the given index.
        /// \param id Index to erase.
        /// \param txn Optional transaction handle.
        /// \return \c true if the record was removed.
        bool erase(uint64_t id, MDBX_txn* txn = nullptr) {
            bool res = false;
            with_transaction([this, id, &res](MDBX_txn* t) {
                res = db_erase(id, t);
            }, TransactionMode::WRITABLE, txn);
            return res;
        }

        /// \brief Erases the record at the given index using an external transaction.
        /// \param id Index to erase.
        /// \param txn Active transaction wrapper.
        /// \return \c true if the record was removed.
        bool erase(uint64_t id, const Transaction& txn) {
            return erase(id, txn.handle());
        }

        /// \brief Removes all records from the table.
        /// \param txn Optional transaction handle.
        void clear(MDBX_txn* txn = nullptr) {
            with_transaction([this](MDBX_txn* t) {
                db_clear(t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Removes all records from the table using an external transaction.
        /// \param txn Active transaction wrapper.
        void clear(const Transaction& txn) {
            clear(txn.handle());
        }

        /// \brief Counts stored records.
        /// \param txn Optional transaction handle.
        /// \return Number of records in the table.
        std::size_t count(MDBX_txn* txn = nullptr) const {
            std::size_t res = 0;
            with_transaction([this, &res](MDBX_txn* t) {
                res = db_count(t);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }

        /// \brief Counts stored records using an external transaction.
        /// \param txn Active transaction wrapper.
        /// \return Number of records in the table.
        std::size_t count(const Transaction& txn) const {
            return count(txn.handle());
        }

        /// \brief Checks whether the table has no records.
        /// \param txn Optional transaction handle.
        /// \return \c true if the table is empty.
        bool empty(MDBX_txn* txn = nullptr) const {
            return count(txn) == 0;
        }

        /// \brief Checks whether the table has no records using an external transaction.
        /// \param txn Active transaction wrapper.
        /// \return \c true if the table is empty.
        bool empty(const Transaction& txn) const {
            return empty(txn.handle());
        }

#       if __cplusplus >= 201703L
        /// \brief Returns the first (smallest) index in the table.
        /// \param txn Optional transaction handle.
        /// \return Optional containing the first index, or empty if the table is empty.
        std::optional<uint64_t> first_index(MDBX_txn* txn = nullptr) const {
            std::optional<uint64_t> result;
            with_transaction([this, &result](MDBX_txn* t) {
                std::pair<bool, uint64_t> r = db_first_index(t);
                if (r.first) result = r.second;
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Returns the first (smallest) index using an external transaction.
        /// \param txn Active transaction wrapper.
        /// \return Optional containing the first index, or empty if the table is empty.
        std::optional<uint64_t> first_index(const Transaction& txn) const {
            return first_index(txn.handle());
        }

        /// \brief Returns the last (largest) index in the table.
        /// \param txn Optional transaction handle.
        /// \return Optional containing the last index, or empty if the table is empty.
        std::optional<uint64_t> last_index(MDBX_txn* txn = nullptr) const {
            std::optional<uint64_t> result;
            with_transaction([this, &result](MDBX_txn* t) {
                std::pair<bool, uint64_t> r = db_last_index(t);
                if (r.first) result = r.second;
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Returns the last (largest) index using an external transaction.
        /// \param txn Active transaction wrapper.
        /// \return Optional containing the last index, or empty if the table is empty.
        std::optional<uint64_t> last_index(const Transaction& txn) const {
            return last_index(txn.handle());
        }
#       endif

        /// \brief Returns the first (smallest) index in a C++11-compatible form.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and index.
        std::pair<bool, uint64_t> first_index_compat(MDBX_txn* txn = nullptr) const {
            std::pair<bool, uint64_t> result(false, 0);
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_first_index(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Returns the first (smallest) index using an external transaction.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and index.
        std::pair<bool, uint64_t> first_index_compat(const Transaction& txn) const {
            return first_index_compat(txn.handle());
        }

        /// \brief Returns the last (largest) index in a C++11-compatible form.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and index.
        std::pair<bool, uint64_t> last_index_compat(MDBX_txn* txn = nullptr) const {
            std::pair<bool, uint64_t> result(false, 0);
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_last_index(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Returns the last (largest) index using an external transaction.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and index.
        std::pair<bool, uint64_t> last_index_compat(const Transaction& txn) const {
            return last_index_compat(txn.handle());
        }

        /// \brief Loads all values into a vector.
        /// \param values Output vector.
        /// \param txn Optional transaction handle.
        /// \note Values are returned in ascending index order.
        void load(std::vector<ValueT>& values, MDBX_txn* txn = nullptr) const {
            with_transaction([this, &values](MDBX_txn* t) {
                db_load(values, t);
            }, TransactionMode::READ_ONLY, txn);
        }

        /// \brief Loads all values into a vector using an external transaction.
        /// \param values Output vector.
        /// \param txn Active transaction wrapper.
        void load(std::vector<ValueT>& values, const Transaction& txn) const {
            load(values, txn.handle());
        }

        /// \brief Retrieves all values into a fresh vector.
        /// \param txn Optional transaction handle.
        /// \return Vector of values in ascending index order.
        std::vector<ValueT> retrieve_all(MDBX_txn* txn = nullptr) const {
            std::vector<ValueT> values;
            load(values, txn);
            return values;
        }

        /// \brief Retrieves all values using an external transaction.
        /// \param txn Active transaction wrapper.
        /// \return Vector of values in ascending index order.
        std::vector<ValueT> retrieve_all(const Transaction& txn) const {
            return retrieve_all(txn.handle());
        }

        /// \brief Loads all index-value pairs into a vector.
        /// \param entries Output vector of (index, value) pairs.
        /// \param txn Optional transaction handle.
        /// \note Pairs are returned in ascending index order.
        void load_entries(std::vector<std::pair<uint64_t, ValueT>>& entries,
                          MDBX_txn* txn = nullptr) const {
            with_transaction([this, &entries](MDBX_txn* t) {
                db_load_entries(entries, t);
            }, TransactionMode::READ_ONLY, txn);
        }

        /// \brief Loads all index-value pairs using an external transaction.
        /// \param entries Output vector of (index, value) pairs.
        /// \param txn Active transaction wrapper.
        void load_entries(std::vector<std::pair<uint64_t, ValueT>>& entries,
                          const Transaction& txn) const {
            load_entries(entries, txn.handle());
        }

        /// \brief Retrieves all index-value pairs into a fresh vector.
        /// \param txn Optional transaction handle.
        /// \return Vector of (index, value) pairs in ascending index order.
        std::vector<std::pair<uint64_t, ValueT>> retrieve_entries(MDBX_txn* txn = nullptr) const {
            std::vector<std::pair<uint64_t, ValueT>> entries;
            load_entries(entries, txn);
            return entries;
        }

        /// \brief Retrieves all index-value pairs using an external transaction.
        /// \param txn Active transaction wrapper.
        /// \return Vector of (index, value) pairs in ascending index order.
        std::vector<std::pair<uint64_t, ValueT>> retrieve_entries(const Transaction& txn) const {
            return retrieve_entries(txn.handle());
        }

        /// \brief Retrieves index-value pairs within an inclusive index range.
        /// \param from Start of the range (inclusive).
        /// \param to End of the range (inclusive).
        /// \param txn Optional transaction handle.
        /// \return Vector of (index, value) pairs in ascending index order.
        std::vector<std::pair<uint64_t, ValueT>> range(uint64_t from, uint64_t to,
                                                        MDBX_txn* txn = nullptr) const {
            std::vector<std::pair<uint64_t, ValueT>> result;
            with_transaction([this, from, to, &result](MDBX_txn* t) {
                result = db_range(from, to, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves index-value pairs within an inclusive index range using an external transaction.
        /// \param from Start of the range (inclusive).
        /// \param to End of the range (inclusive).
        /// \param txn Active transaction wrapper.
        /// \return Vector of (index, value) pairs in ascending index order.
        std::vector<std::pair<uint64_t, ValueT>> range(uint64_t from, uint64_t to,
                                                        const Transaction& txn) const {
            return range(from, to, txn.handle());
        }

    private:
        static uint64_t read_index_key(const MDBX_val& db_key) {
            if (!db_key.iov_base || db_key.iov_len != sizeof(uint64_t)) {
                throw std::runtime_error("SequenceTable: invalid index key");
            }
            uint64_t id = 0;
            std::memcpy(&id, db_key.iov_base, sizeof(id));
            return id;
        }

        static MDBX_val make_key(uint64_t id, SerializeScratch& sc) {
            return serialize_key<true>(id, sc);
        }

        uint64_t db_append(const ValueT& value, MDBX_txn* txn) {
            uint64_t next_id = 0;
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &raw),
                       "Failed to open cursor for append");
            CursorGuard cursor(raw);

            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_LAST);
            if (rc == MDBX_SUCCESS) {
                uint64_t last_id = read_index_key(db_key);
                if (last_id == std::numeric_limits<uint64_t>::max()) {
                    throw std::overflow_error("SequenceTable::append: id overflow");
                }
                next_id = last_id + 1;
            } else if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to seek last key in SequenceTable");
            }
            cursor.close();

            SerializeScratch sc_key;
            SerializeScratch sc_value;
            db_key = make_key(next_id, sc_key);
            db_val = serialize_value(value, sc_value);
            rc = mdbx_put(txn, m_dbi, &db_key, &db_val, MDBX_NOOVERWRITE);
            if (rc == MDBX_KEYEXIST) {
                throw std::runtime_error(
                    "SequenceTable::append: computed next index already exists; "
                    "concurrent append requires external synchronization or retry"
                );
            }
            check_mdbx(rc, "Failed to append value");
            {
#               if MDBXC_SYNC_ENABLED
                record_op(txn, sync::ChangeOpType::Put,
                          capture_bytes(db_key), capture_bytes(db_val));
#               endif
            }
            return next_id;
        }

        void db_set(uint64_t id, const ValueT& value, MDBX_txn* txn) {
            SerializeScratch sc_key;
            SerializeScratch sc_value;
            MDBX_val db_key = make_key(id, sc_key);
            MDBX_val db_val = serialize_value(value, sc_value);
            check_mdbx(mdbx_put(txn, m_dbi, &db_key, &db_val, MDBX_UPSERT),
                       "Failed to set value");
#           if MDBXC_SYNC_ENABLED
            record_op(txn, sync::ChangeOpType::Put,
                      capture_bytes(db_key), capture_bytes(db_val));
#           endif
        }

        bool db_get(uint64_t id, ValueT& out, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = make_key(id, sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to get value");
            out = deserialize_value<ValueT>(db_val);
            return true;
        }

        bool db_contains(uint64_t id, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = make_key(id, sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_SUCCESS) return true;
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to check value presence");
            return false;
        }

        bool db_erase(uint64_t id, MDBX_txn* txn) {
            SerializeScratch sc_key;
            MDBX_val db_key = make_key(id, sc_key);
            int rc = mdbx_del(txn, m_dbi, &db_key, nullptr);
            if (rc == MDBX_SUCCESS) {
#               if MDBXC_SYNC_ENABLED
                record_op(txn, sync::ChangeOpType::Delete,
                          capture_bytes(db_key), {});
#               endif
                return true;
            }
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to erase value");
            return false;
        }

        void db_clear(MDBX_txn* txn) {
            check_mdbx(mdbx_drop(txn, m_dbi, 0), "Failed to clear SequenceTable");
#           if MDBXC_SYNC_ENABLED
            record_op(txn, sync::ChangeOpType::ClearTable, {}, {});
#           endif
        }

        std::size_t db_count(MDBX_txn* txn) const {
            MDBX_stat stat;
            check_mdbx(mdbx_dbi_stat(txn, m_dbi, &stat, sizeof(stat)),
                       "Failed to query statistics");
            return stat.ms_entries;
        }

        std::pair<bool, uint64_t> db_first_index(MDBX_txn* txn) const {
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &raw),
                       "Failed to open cursor for first_index");
            CursorGuard cursor(raw);

            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_FIRST);
            if (rc == MDBX_SUCCESS) {
                return std::make_pair(true, read_index_key(db_key));
            } else if (rc == MDBX_NOTFOUND) {
                return std::make_pair(false, uint64_t(0));
            }
            check_mdbx(rc, "Failed to seek first key");
            return std::make_pair(false, uint64_t(0));
        }

        std::pair<bool, uint64_t> db_last_index(MDBX_txn* txn) const {
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &raw),
                       "Failed to open cursor for last_index");
            CursorGuard cursor(raw);

            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_LAST);
            if (rc == MDBX_SUCCESS) {
                return std::make_pair(true, read_index_key(db_key));
            } else if (rc == MDBX_NOTFOUND) {
                return std::make_pair(false, uint64_t(0));
            }
            check_mdbx(rc, "Failed to seek last key");
            return std::make_pair(false, uint64_t(0));
        }

        void db_load(std::vector<ValueT>& values, MDBX_txn* txn) const {
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &raw),
                       "Failed to open cursor for load");
            CursorGuard cursor(raw);

            MDBX_val db_key, db_val;
            int rc = MDBX_SUCCESS;
            while ((rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT))
                   == MDBX_SUCCESS) {
                values.push_back(deserialize_value<ValueT>(db_val));
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to iterate sequence table");
            }
        }

        void db_load_entries(std::vector<std::pair<uint64_t, ValueT>>& entries,
                             MDBX_txn* txn) const {
            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &raw),
                       "Failed to open cursor for load_entries");
            CursorGuard cursor(raw);

            MDBX_val db_key, db_val;
            int rc = MDBX_SUCCESS;
            while ((rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT))
                   == MDBX_SUCCESS) {
                uint64_t id = read_index_key(db_key);
                entries.push_back(std::make_pair(id, deserialize_value<ValueT>(db_val)));
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to iterate sequence table");
            }
        }

        std::vector<std::pair<uint64_t, ValueT>> db_range(uint64_t from, uint64_t to,
                                                           MDBX_txn* txn) const {
            std::vector<std::pair<uint64_t, ValueT>> result;
            if (from > to) return result;

            MDBX_cursor* raw = nullptr;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, &raw),
                       "Failed to open cursor for range");
            CursorGuard cursor(raw);

            SerializeScratch sc_key;
            MDBX_val db_key = make_key(from, sc_key);
            MDBX_val db_val;
            bool stopped_by_upper_bound = false;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            while (rc == MDBX_SUCCESS) {
                uint64_t id = read_index_key(db_key);
                if (id > to) {
                    stopped_by_upper_bound = true;
                    break;
                }
                result.push_back(std::make_pair(id, deserialize_value<ValueT>(db_val)));
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
            }
            if (!stopped_by_upper_bound && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to iterate range");
            }
            return result;
        }
    };

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SEQUENCE_TABLE_HPP_INCLUDED
