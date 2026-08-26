#pragma once
#ifndef MDBX_CONTAINERS_HEADER_VALUE_TABLE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_VALUE_TABLE_HPP_INCLUDED

/// \file ValueTable.hpp
/// \brief Singleton-value table persisted in MDBX.
/// \details
/// Provides a strongly typed persistent variable: one named MDBX table stores
/// at most one user value.

#include "common.hpp"
#include <utility>

namespace mdbxc {

    /// \class ValueTable
    /// \ingroup mdbxc_tables
    /// \brief Persistent table that stores at most one value.
    /// \tparam ValueT Type of the stored value.
    /// \details
    /// ValueTable is useful for metadata, module state, snapshots, and
    /// single-object configuration records. All ValueTable operations use one
    /// fixed internal key, so the public API exposes value semantics rather
    /// than key-value semantics.
    ///
    /// \note The singleton invariant is guaranteed for operations performed
    ///       through ValueTable. Opening the same MDBX DBI through another table
    ///       type can intentionally create extra physical rows; \c clear()
    ///       removes all rows in the DBI.
    template<class ValueT>
    class ValueTable final : public BaseTable {
    public:
        /// \brief Constructs table using existing connection.
        /// \param connection Existing connection.
        /// \param name Name of the table within the MDBX environment.
        /// \param flags Additional MDBX database flags.
        ValueTable(std::shared_ptr<Connection> connection,
                   std::string name = "value_store",
                   MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(std::move(connection),
                        std::move(name),
                        flags | get_mdbx_flags<std::uint32_t>()) {}

        /// \brief Constructs table using configuration.
        /// \param config Configuration settings.
        /// \param name Name of the table within the MDBX environment.
        /// \param flags Additional MDBX database flags.
        explicit ValueTable(const Config& config,
                            std::string name = "value_store",
                            MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(Connection::create(config),
                        std::move(name),
                        flags | get_mdbx_flags<std::uint32_t>()) {}

        /// \brief Destructor.
        ~ValueTable() override = default;

        /// \brief Sets the singleton value, replacing any existing value.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        void set(const ValueT& value, MDBX_txn* txn = nullptr) {
            with_transaction([this, &value](MDBX_txn* t) {
                db_set(value, t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Sets the singleton value using an external transaction.
        /// \param value Value to store.
        /// \param txn Active transaction wrapper.
        void set(const ValueT& value, const Transaction& txn) {
            set(value, txn.handle());
        }

        /// \brief Inserts the singleton value if absent.
        /// \param value Value to store.
        /// \param txn Optional transaction handle.
        /// \return \c true if inserted, \c false if the value already exists.
        bool insert(const ValueT& value, MDBX_txn* txn = nullptr) {
            bool res = false;
            with_transaction([this, &value, &res](MDBX_txn* t) {
                res = db_insert(value, t);
            }, TransactionMode::WRITABLE, txn);
            return res;
        }

        /// \brief Inserts the singleton value if absent using an external transaction.
        /// \param value Value to store.
        /// \param txn Active transaction wrapper.
        /// \return \c true if inserted, \c false if the value already exists.
        bool insert(const ValueT& value, const Transaction& txn) {
            return insert(value, txn.handle());
        }

        /// \brief Retrieves the singleton value or throws if absent.
        /// \param txn Optional transaction handle.
        /// \return Stored value.
        /// \throws std::out_of_range if the value is absent.
        ValueT get(MDBX_txn* txn = nullptr) const {
            ValueT value;
            with_transaction([this, &value](MDBX_txn* t) {
                if (!db_get(value, t)) {
                    throw std::out_of_range("Value not found");
                }
            }, TransactionMode::READ_ONLY, txn);
            return value;
        }

        /// \brief Retrieves the singleton value using an external transaction.
        /// \param txn Active transaction wrapper.
        /// \return Stored value.
        /// \throws std::out_of_range if the value is absent.
        ValueT get(const Transaction& txn) const {
            return get(txn.handle());
        }

        /// \brief Tries to retrieve the singleton value.
        /// \param out Output value.
        /// \param txn Optional transaction handle.
        /// \return \c true if the value exists.
        bool try_get(ValueT& out, MDBX_txn* txn = nullptr) const {
            bool res = false;
            with_transaction([this, &out, &res](MDBX_txn* t) {
                res = db_get(out, t);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }

        /// \brief Tries to retrieve the singleton value using an external transaction.
        /// \param out Output value.
        /// \param txn Active transaction wrapper.
        /// \return \c true if the value exists.
        bool try_get(ValueT& out, const Transaction& txn) const {
            return try_get(out, txn.handle());
        }

#       if __cplusplus >= 201703L

        /// \brief Finds the singleton value.
        /// \param txn Optional transaction handle.
        /// \return Optional containing the value when present.
        std::optional<ValueT> find(MDBX_txn* txn = nullptr) const {
            std::optional<ValueT> result;
            with_transaction([this, &result](MDBX_txn* t) {
                ValueT tmp;
                if (db_get(tmp, t)) {
                    result = std::move(tmp);
                }
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds the singleton value using an external transaction.
        /// \param txn Active transaction wrapper.
        /// \return Optional containing the value when present.
        std::optional<ValueT> find(const Transaction& txn) const {
            return find(txn.handle());
        }

#       else

        /// \brief Finds the singleton value.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and value.
        std::pair<bool, ValueT> find(MDBX_txn* txn = nullptr) const {
            return find_compat(txn);
        }

        /// \brief Finds the singleton value using an external transaction.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and value.
        std::pair<bool, ValueT> find(const Transaction& txn) const {
            return find(txn.handle());
        }

#       endif

        /// \brief Finds the singleton value in a C++11-compatible form.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and value.
        std::pair<bool, ValueT> find_compat(MDBX_txn* txn = nullptr) const {
            std::pair<bool, ValueT> result(false, ValueT());
            with_transaction([this, &result](MDBX_txn* t) {
                if (db_get(result.second, t)) {
                    result.first = true;
                }
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds the singleton value in a C++11-compatible form.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and value.
        std::pair<bool, ValueT> find_compat(const Transaction& txn) const {
            return find_compat(txn.handle());
        }

        /// \brief Returns the stored value or the supplied default.
        /// \param default_value Value returned when the singleton is absent.
        /// \param txn Optional transaction handle.
        /// \return Stored value or \p default_value.
        ValueT get_or(ValueT default_value, MDBX_txn* txn = nullptr) const {
            std::pair<bool, ValueT> found = find_compat(txn);
            if (found.first) {
                return std::move(found.second);
            }
            return default_value;
        }

        /// \brief Returns the stored value or the supplied default.
        /// \param default_value Value returned when the singleton is absent.
        /// \param txn Active transaction wrapper.
        /// \return Stored value or \p default_value.
        ValueT get_or(ValueT default_value, const Transaction& txn) const {
            return get_or(std::move(default_value), txn.handle());
        }

        /// \brief Updates the stored value in place.
        /// \tparam Fn Functor accepting \c ValueT&.
        /// \param fn Function applied to the stored value.
        /// \param txn Optional transaction handle.
        /// \return \c true if the value existed and was updated.
        template<class Fn>
        bool update(Fn&& fn, MDBX_txn* txn = nullptr) {
            bool res = false;
            with_transaction([this, &fn, &res](MDBX_txn* t) {
                ValueT tmp;
                if (!db_get(tmp, t)) {
                    return;
                }
                fn(tmp);
                db_set(tmp, t);
                res = true;
            }, TransactionMode::WRITABLE, txn);
            return res;
        }

        /// \brief Updates the stored value in place using an external transaction.
        /// \tparam Fn Functor accepting \c ValueT&.
        /// \param fn Function applied to the stored value.
        /// \param txn Active transaction wrapper.
        /// \return \c true if the value existed and was updated.
        template<class Fn>
        bool update(Fn&& fn, const Transaction& txn) {
            return update(std::forward<Fn>(fn), txn.handle());
        }

        /// \brief Checks whether the singleton value exists.
        /// \param txn Optional transaction handle.
        /// \return \c true if the value exists.
        bool has_value(MDBX_txn* txn = nullptr) const {
            bool res = false;
            with_transaction([this, &res](MDBX_txn* t) {
                res = db_has_value(t);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }

        /// \brief Checks whether the singleton value exists.
        /// \param txn Active transaction wrapper.
        /// \return \c true if the value exists.
        bool has_value(const Transaction& txn) const {
            return has_value(txn.handle());
        }

        /// \brief Counts the logical singleton value.
        /// \param txn Optional transaction handle.
        /// \return \c 1 when the singleton exists, otherwise \c 0.
        std::size_t count(MDBX_txn* txn = nullptr) const {
            return has_value(txn) ? 1u : 0u;
        }

        /// \brief Counts the logical singleton value.
        /// \param txn Active transaction wrapper.
        /// \return \c 1 when the singleton exists, otherwise \c 0.
        std::size_t count(const Transaction& txn) const {
            return count(txn.handle());
        }

        /// \brief Checks whether the singleton value is absent.
        /// \param txn Optional transaction handle.
        /// \return \c true if the singleton is absent.
        bool empty(MDBX_txn* txn = nullptr) const {
            return !has_value(txn);
        }

        /// \brief Checks whether the singleton value is absent.
        /// \param txn Active transaction wrapper.
        /// \return \c true if the singleton is absent.
        bool empty(const Transaction& txn) const {
            return empty(txn.handle());
        }

        /// \brief Erases the singleton value.
        /// \param txn Optional transaction handle.
        /// \return \c true if the value was removed.
        bool erase(MDBX_txn* txn = nullptr) {
            bool res = false;
            with_transaction([this, &res](MDBX_txn* t) {
                res = db_erase(t);
            }, TransactionMode::WRITABLE, txn);
            return res;
        }

        /// \brief Erases the singleton value.
        /// \param txn Active transaction wrapper.
        /// \return \c true if the value was removed.
        bool erase(const Transaction& txn) {
            return erase(txn.handle());
        }

        /// \brief Removes all physical rows from the DBI.
        /// \param txn Optional transaction handle.
        void clear(MDBX_txn* txn = nullptr) {
            with_transaction([this](MDBX_txn* t) {
                db_clear(t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Removes all physical rows from the DBI.
        /// \param txn Active transaction wrapper.
        void clear(const Transaction& txn) {
            clear(txn.handle());
        }

    private:
        static std::uint32_t singleton_key() {
            return 0u;
        }

        static MDBX_val make_key(SerializeScratch& sc) {
            const std::uint32_t key = singleton_key();
            return serialize_key<true>(key, sc);
        }

        void db_set(const ValueT& value, MDBX_txn* txn) {
            SerializeScratch sc_key;
            SerializeScratch sc_value;
            MDBX_val db_key = make_key(sc_key);
            MDBX_val db_val = serialize_value(value, sc_value);
            check_mdbx(mdbx_put(txn, m_dbi, &db_key, &db_val, MDBX_UPSERT),
                       "Failed to set value");
#           if MDBXC_SYNC_ENABLED
            record_op(txn, sync::ChangeOpType::Put,
                      capture_bytes(db_key), capture_bytes(db_val));
#           endif
        }

        bool db_insert(const ValueT& value, MDBX_txn* txn) {
            SerializeScratch sc_key;
            SerializeScratch sc_value;
            MDBX_val db_key = make_key(sc_key);
            MDBX_val db_val = serialize_value(value, sc_value);
            int rc = mdbx_put(txn, m_dbi, &db_key, &db_val, MDBX_NOOVERWRITE);
            if (rc == MDBX_SUCCESS) {
#               if MDBXC_SYNC_ENABLED
                record_op(txn, sync::ChangeOpType::Put,
                          capture_bytes(db_key), capture_bytes(db_val));
#               endif
                return true;
            }
            if (rc == MDBX_KEYEXIST) return false;
            check_mdbx(rc, "Failed to insert value");
            return false;
        }

        bool db_get(ValueT& out, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = make_key(sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to retrieve value");
            out = deserialize_value<ValueT>(db_val);
            return true;
        }

        bool db_has_value(MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = make_key(sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_SUCCESS) return true;
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to check value presence");
            return false;
        }

        bool db_erase(MDBX_txn* txn) {
            SerializeScratch sc_key;
            MDBX_val db_key = make_key(sc_key);
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
            check_mdbx(mdbx_drop(txn, m_dbi, 0), "Failed to clear value table");
#           if MDBXC_SYNC_ENABLED
            record_op(txn, sync::ChangeOpType::ClearTable, {}, {});
#           endif
        }
    };

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_VALUE_TABLE_HPP_INCLUDED
