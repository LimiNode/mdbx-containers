#pragma once
#ifndef MDBX_CONTAINERS_HEADER_KEY_TABLE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_KEY_TABLE_HPP_INCLUDED

/// \file KeyTable.hpp
/// \brief Set-like table storing keys without user values.
/// \details
/// Persists unique serialized keys with empty MDBX values, providing a compact
/// \c std::set -like workflow over an MDBX table.

#include "common.hpp"
#include <vector>

namespace mdbxc {

    /// \class KeyTable
    /// \ingroup mdbxc_tables
    /// \brief Ordered key-only table persisted in MDBX.
    /// \tparam KeyT Type of the keys.
    /// \tparam Options Compile-time table policy. Does not change the database
    ///         storage format.
    /// \details
    /// Provides persistent \c std::set -like semantics: every key is stored at
    /// most once, duplicate inserts are ignored, and reads return keys ordered by
    /// the underlying MDBX key comparator. Stored records use the serialized key
    /// as the MDBX key and an empty MDBX value.
    ///
    /// \note \c append() inserts missing keys and leaves existing keys
    ///       unchanged. \c reconcile() currently clears the table and appends the
    ///       source keys, so it should be treated as replacement rather than an
    ///       incremental set diff.
    template<class KeyT, class Options = DefaultTableOptions>
    class KeyTable final : public BaseTable {
    public:
        /// \brief Constructs table using existing connection.
        /// \param connection Existing connection.
        /// \param name Name of the table within the MDBX environment.
        /// \param flags Additional MDBX database flags.
        KeyTable(std::shared_ptr<Connection> connection,
                 std::string name = "key_store",
                 MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(std::move(connection),
                        std::move(name),
                        flags | get_mdbx_flags<KeyT>()) {}

        /// \brief Constructs table using configuration.
        /// \param config Configuration settings.
        /// \param name Name of the table within the MDBX environment.
        /// \param flags Additional MDBX database flags.
        explicit KeyTable(const Config& config,
                          std::string name = "key_store",
                          MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(Connection::create(config),
                        std::move(name),
                        flags | get_mdbx_flags<KeyT>()) {}

        /// \brief Destructor.
        ~KeyTable() override = default;

        /// \brief Replaces table content with keys from a container.
        /// \tparam ContainerT Container type storing keys.
        /// \param container Source keys.
        /// \return Reference to this table.
        /// \note Equivalent to \c reconcile(container): current table content is
        ///       replaced by the source key set.
        template<template<class...> class ContainerT>
        KeyTable& operator=(const ContainerT<KeyT>& container) {
            reconcile(container);
            return *this;
        }

        /// \brief Loads all keys into a container.
        /// \tparam ContainerT Container type storing keys.
        /// \return Filled container.
        /// \note The default \c std::set preserves unique-key table semantics.
        ///       Other containers receive keys according to their insertion
        ///       rules.
        template<template<class...> class ContainerT = std::set>
        ContainerT<KeyT> operator()() const {
            return retrieve_all<ContainerT>();
        }

        /// \brief Loads keys from the database into a container.
        /// \tparam ContainerT Container type storing keys.
        /// \param container Output container.
        /// \param txn Optional transaction handle.
        /// \note Appends/inserts into \p container; it does not clear existing
        ///       container contents first.
        template<template<class...> class ContainerT>
        void load(ContainerT<KeyT>& container, MDBX_txn* txn = nullptr) const {
            with_transaction([this, &container](MDBX_txn* t) {
                db_load(container, t);
            }, TransactionMode::READ_ONLY, txn);
        }

        /// \brief Loads keys from the database into a container.
        /// \tparam ContainerT Container type storing keys.
        /// \param container Output container.
        /// \param txn Active transaction wrapper.
        template<template<class...> class ContainerT>
        void load(ContainerT<KeyT>& container, const Transaction& txn) const {
            load(container, txn.handle());
        }

        /// \brief Retrieves all keys into the requested container.
        /// \tparam ContainerT Container type storing keys.
        /// \param txn Optional transaction handle.
        /// \return Filled container.
        /// \note Returns a fresh container populated from the table.
        template<template<class...> class ContainerT = std::set>
        ContainerT<KeyT> retrieve_all(MDBX_txn* txn = nullptr) const {
            ContainerT<KeyT> container;
            load(container, txn);
            return container;
        }

        /// \brief Retrieves all keys into the requested container.
        /// \tparam ContainerT Container type storing keys.
        /// \param txn Active transaction wrapper.
        /// \return Filled container.
        template<template<class...> class ContainerT = std::set>
        ContainerT<KeyT> retrieve_all(const Transaction& txn) const {
            return retrieve_all<ContainerT>(txn.handle());
        }

        /// \brief Retrieves keys within an inclusive key range.
        /// \tparam ContainerT Container type storing keys.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Optional transaction handle.
        /// \return Container with keys from the requested range.
        /// \throws MdbxException if a database error occurs.
        /// \note The default \c std::set preserves key-only table semantics.
        ///       Use \c range<std::vector>() to preserve MDBX iteration order.
        /// \complexity O(log n + m), where m is the number of returned keys.
        template<template<class...> class ContainerT = std::set>
        ContainerT<KeyT> range(const KeyT& from_key, const KeyT& to_key,
                               MDBX_txn* txn = nullptr) const {
            ContainerT<KeyT> out;
            with_transaction([this, &from_key, &to_key, &out](MDBX_txn* t) {
                db_range(from_key, to_key, out, t);
            }, TransactionMode::READ_ONLY, txn);
            return out;
        }

        /// \brief Retrieves keys within an inclusive key range.
        /// \tparam ContainerT Container type storing keys.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Active transaction wrapper.
        /// \return Container with keys from the requested range.
        /// \throws MdbxException if a database error occurs.
        /// \complexity O(log n + m), where m is the number of returned keys.
        template<template<class...> class ContainerT = std::set>
        ContainerT<KeyT> range(const KeyT& from_key, const KeyT& to_key,
                               const Transaction& txn) const {
            return range<ContainerT>(from_key, to_key, txn.handle());
        }

        // --- Streaming and range helpers ---

        /// \brief Calls a callback for every key in an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param callback Invoked as \c callback(const KeyT&). Return \c true to continue.
        /// \param txn Optional transaction handle.
        /// \return \c true if every key was visited, \c false if the callback stopped early.
        /// \throws MdbxException if a database error occurs.
        template<typename CallbackT>
        bool for_each_range(const KeyT& from_key, const KeyT& to_key,
                            CallbackT callback, MDBX_txn* txn = nullptr) const {
            bool completed = false;
            with_transaction([this, &from_key, &to_key, &callback, &completed](MDBX_txn* t) {
                completed = db_for_each_range(from_key, to_key, callback, t);
            }, TransactionMode::READ_ONLY, txn);
            return completed;
        }

        /// \brief Calls a callback for every key in an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param callback Invoked as \c callback(const KeyT&). Return \c true to continue.
        /// \param txn Active transaction wrapper.
        /// \return \c true if every key was visited, \c false if the callback stopped early.
        /// \throws MdbxException if a database error occurs.
        template<typename CallbackT>
        bool for_each_range(const KeyT& from_key, const KeyT& to_key,
                            CallbackT callback, const Transaction& txn) const {
            return for_each_range(from_key, to_key, callback, txn.handle());
        }

        /// \brief Collects keys matching a predicate within an inclusive range.
        /// \tparam ContainerT Container type storing keys (default \c std::vector).
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param pred Predicate invoked as \c pred(const KeyT&). Return \c true to collect.
        /// \param txn Optional transaction handle.
        /// \return Container with matching keys in MDBX order.
        /// \throws MdbxException if a database error occurs.
        template<template<class...> class ContainerT = std::vector, typename PredicateT>
        ContainerT<KeyT> filter_range(const KeyT& from_key, const KeyT& to_key,
                                      PredicateT pred, MDBX_txn* txn = nullptr) const {
            ContainerT<KeyT> out;
            for_each_range(from_key, to_key, [&out, &pred](const KeyT& key) -> bool {
                if (pred(key)) {
                    out.insert(out.end(), key);
                }
                return true;
            }, txn);
            return out;
        }

        /// \brief Collects keys matching a predicate within an inclusive range.
        /// \tparam ContainerT Container type storing keys (default \c std::vector).
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param pred Predicate invoked as \c pred(const KeyT&). Return \c true to collect.
        /// \param txn Active transaction wrapper.
        /// \return Container with matching keys in MDBX order.
        /// \throws MdbxException if a database error occurs.
        template<template<class...> class ContainerT = std::vector, typename PredicateT>
        ContainerT<KeyT> filter_range(const KeyT& from_key, const KeyT& to_key,
                                      PredicateT pred, const Transaction& txn) const {
            return filter_range<ContainerT>(from_key, to_key, pred, txn.handle());
        }

        // --- Bounds / edges ---

#       if __cplusplus >= 201703L

        /// \brief Finds the first key not less than the given key.
        /// \param key Key to bound.
        /// \param txn Optional transaction handle.
        /// \return \c std::optional<KeyT> with the lower-bound key, or empty if none.
        std::optional<KeyT> lower_bound(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::optional<KeyT> result;
            with_transaction([this, &key, &result](MDBX_txn* t) {
                result = db_lower_bound(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds the first key not less than the given key.
        /// \param key Key to bound.
        /// \param txn Active transaction wrapper.
        /// \return \c std::optional<KeyT> with the lower-bound key, or empty if none.
        std::optional<KeyT> lower_bound(const KeyT& key, const Transaction& txn) const {
            return lower_bound(key, txn.handle());
        }

        /// \brief Finds the first key strictly greater than the given key.
        /// \param key Key to bound.
        /// \param txn Optional transaction handle.
        /// \return \c std::optional<KeyT> with the upper-bound key, or empty if none.
        std::optional<KeyT> upper_bound(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::optional<KeyT> result;
            with_transaction([this, &key, &result](MDBX_txn* t) {
                result = db_upper_bound(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds the first key strictly greater than the given key.
        /// \param key Key to bound.
        /// \param txn Active transaction wrapper.
        /// \return \c std::optional<KeyT> with the upper-bound key, or empty if none.
        std::optional<KeyT> upper_bound(const KeyT& key, const Transaction& txn) const {
            return upper_bound(key, txn.handle());
        }

        /// \brief Retrieves the smallest key.
        /// \param txn Optional transaction handle.
        /// \return \c std::optional<KeyT> with the minimum key, or empty if the table is empty.
        std::optional<KeyT> first(MDBX_txn* txn = nullptr) const {
            std::optional<KeyT> result;
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_first(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves the smallest key.
        /// \param txn Active transaction wrapper.
        /// \return \c std::optional<KeyT> with the minimum key, or empty if the table is empty.
        std::optional<KeyT> first(const Transaction& txn) const {
            return first(txn.handle());
        }

        /// \brief Retrieves the largest key.
        /// \param txn Optional transaction handle.
        /// \return \c std::optional<KeyT> with the maximum key, or empty if the table is empty.
        std::optional<KeyT> last(MDBX_txn* txn = nullptr) const {
            std::optional<KeyT> result;
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_last(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves the largest key.
        /// \param txn Active transaction wrapper.
        /// \return \c std::optional<KeyT> with the maximum key, or empty if the table is empty.
        std::optional<KeyT> last(const Transaction& txn) const {
            return last(txn.handle());
        }

        /// \brief Retrieves the smallest key.
        /// \param txn Optional transaction handle.
        /// \return \c std::optional<KeyT> with the minimum key, or empty if the table is empty.
        std::optional<KeyT> min_key(MDBX_txn* txn = nullptr) const { return first(txn); }
        /// \brief Retrieves the smallest key.
        /// \param txn Active transaction wrapper.
        /// \return \c std::optional<KeyT> with the minimum key, or empty if the table is empty.
        std::optional<KeyT> min_key(const Transaction& txn) const { return first(txn); }
        /// \brief Retrieves the largest key.
        /// \param txn Optional transaction handle.
        /// \return \c std::optional<KeyT> with the maximum key, or empty if the table is empty.
        std::optional<KeyT> max_key(MDBX_txn* txn = nullptr) const { return last(txn); }
        /// \brief Retrieves the largest key.
        /// \param txn Active transaction wrapper.
        /// \return \c std::optional<KeyT> with the maximum key, or empty if the table is empty.
        std::optional<KeyT> max_key(const Transaction& txn) const { return last(txn); }

#       else

        /// \brief Finds the first key not less than the given key.
        /// \param key Key to bound.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> lower_bound(const KeyT& key, MDBX_txn* txn = nullptr) const {
            return lower_bound_compat(key, txn);
        }

        /// \brief Finds the first key not less than the given key.
        /// \param key Key to bound.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> lower_bound(const KeyT& key, const Transaction& txn) const {
            return lower_bound(key, txn.handle());
        }

        /// \brief Finds the first key not less than the given key (C++11-compatible).
        /// \param key Key to bound.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> lower_bound_compat(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::pair<bool, KeyT> result(false, KeyT());
            with_transaction([this, &key, &result](MDBX_txn* t) {
                result = db_lower_bound_compat(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds the first key not less than the given key (C++11-compatible).
        /// \param key Key to bound.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> lower_bound_compat(const KeyT& key, const Transaction& txn) const {
            return lower_bound_compat(key, txn.handle());
        }

        /// \brief Finds the first key strictly greater than the given key.
        /// \param key Key to bound.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> upper_bound(const KeyT& key, MDBX_txn* txn = nullptr) const {
            return upper_bound_compat(key, txn);
        }

        /// \brief Finds the first key strictly greater than the given key.
        /// \param key Key to bound.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> upper_bound(const KeyT& key, const Transaction& txn) const {
            return upper_bound(key, txn.handle());
        }

        /// \brief Finds the first key strictly greater than the given key (C++11-compatible).
        /// \param key Key to bound.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> upper_bound_compat(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::pair<bool, KeyT> result(false, KeyT());
            with_transaction([this, &key, &result](MDBX_txn* t) {
                result = db_upper_bound_compat(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds the first key strictly greater than the given key (C++11-compatible).
        /// \param key Key to bound.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> upper_bound_compat(const KeyT& key, const Transaction& txn) const {
            return upper_bound_compat(key, txn.handle());
        }

        /// \brief Retrieves the smallest key.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> first(MDBX_txn* txn = nullptr) const {
            return first_compat(txn);
        }

        /// \brief Retrieves the smallest key.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> first(const Transaction& txn) const {
            return first(txn.handle());
        }

        /// \brief Retrieves the smallest key (C++11-compatible).
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> first_compat(MDBX_txn* txn = nullptr) const {
            std::pair<bool, KeyT> result(false, KeyT());
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_first_compat(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves the smallest key (C++11-compatible).
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> first_compat(const Transaction& txn) const {
            return first_compat(txn.handle());
        }

        /// \brief Retrieves the largest key.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> last(MDBX_txn* txn = nullptr) const {
            return last_compat(txn);
        }

        /// \brief Retrieves the largest key.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> last(const Transaction& txn) const {
            return last(txn.handle());
        }

        /// \brief Retrieves the largest key (C++11-compatible).
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> last_compat(MDBX_txn* txn = nullptr) const {
            std::pair<bool, KeyT> result(false, KeyT());
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_last_compat(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves the largest key (C++11-compatible).
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> last_compat(const Transaction& txn) const {
            return last_compat(txn.handle());
        }

        /// \brief Retrieves the smallest key.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> min_key(MDBX_txn* txn = nullptr) const {
            return min_key_compat(txn);
        }

        /// \brief Retrieves the smallest key.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> min_key(const Transaction& txn) const {
            return min_key(txn.handle());
        }

        /// \brief Retrieves the smallest key (C++11-compatible).
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> min_key_compat(MDBX_txn* txn = nullptr) const { return first_compat(txn); }
        /// \brief Retrieves the smallest key (C++11-compatible).
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> min_key_compat(const Transaction& txn) const { return first_compat(txn); }
        /// \brief Retrieves the largest key.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> max_key(MDBX_txn* txn = nullptr) const {
            return max_key_compat(txn);
        }

        /// \brief Retrieves the largest key.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> max_key(const Transaction& txn) const {
            return max_key(txn.handle());
        }

        /// \brief Retrieves the largest key (C++11-compatible).
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> max_key_compat(MDBX_txn* txn = nullptr) const { return last_compat(txn); }
        /// \brief Retrieves the largest key (C++11-compatible).
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key.
        std::pair<bool, KeyT> max_key_compat(const Transaction& txn) const { return last_compat(txn); }
#       endif

        // --- Reverse scan ---

        /// \brief Retrieves keys within an inclusive range in reverse MDBX order.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Optional transaction handle.
        /// \return Vector with keys from the requested range in descending order.
        /// \throws MdbxException if a database error occurs.
        std::vector<KeyT> range_reverse(const KeyT& from_key, const KeyT& to_key,
                                        MDBX_txn* txn = nullptr) const {
            std::vector<KeyT> out;
            with_transaction([this, &from_key, &to_key, &out](MDBX_txn* t) {
                db_range_reverse(from_key, to_key, out, t);
            }, TransactionMode::READ_ONLY, txn);
            return out;
        }

        /// \brief Retrieves keys within an inclusive range in reverse MDBX order.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Active transaction wrapper.
        /// \return Vector with keys from the requested range in descending order.
        /// \throws MdbxException if a database error occurs.
        std::vector<KeyT> range_reverse(const KeyT& from_key, const KeyT& to_key,
                                        const Transaction& txn) const {
            return range_reverse(from_key, to_key, txn.handle());
        }

        /// \brief Retrieves up to \p limit keys within an inclusive range in reverse MDBX order.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param limit Maximum number of keys to return.
        /// \param txn Optional transaction handle.
        /// \return Vector with keys from the requested range in descending order.
        /// \throws MdbxException if a database error occurs.
        /// \note \c limit == 0 returns an empty vector.
        std::vector<KeyT> range_reverse(const KeyT& from_key, const KeyT& to_key,
                                        std::size_t limit, MDBX_txn* txn = nullptr) const {
            std::vector<KeyT> out;
            with_transaction([this, &from_key, &to_key, &limit, &out](MDBX_txn* t) {
                db_range_reverse(from_key, to_key, limit, out, t);
            }, TransactionMode::READ_ONLY, txn);
            return out;
        }

        /// \brief Retrieves up to \p limit keys within an inclusive range in reverse MDBX order.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param limit Maximum number of keys to return.
        /// \param txn Active transaction wrapper.
        /// \return Vector with keys from the requested range in descending order.
        /// \throws MdbxException if a database error occurs.
        std::vector<KeyT> range_reverse(const KeyT& from_key, const KeyT& to_key,
                                        std::size_t limit, const Transaction& txn) const {
            return range_reverse(from_key, to_key, limit, txn.handle());
        }

        // --- Range metadata and removal ---

        /// \brief Checks whether the table contains any key within an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Optional transaction handle.
        /// \return \c true if at least one key exists in the range.
        /// \throws MdbxException if a database error occurs.
        bool contains_range(const KeyT& from_key, const KeyT& to_key,
                            MDBX_txn* txn = nullptr) const {
            bool result = false;
            with_transaction([this, &from_key, &to_key, &result](MDBX_txn* t) {
                result = db_contains_range(from_key, to_key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Checks whether the table contains any key within an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Active transaction wrapper.
        /// \return \c true if at least one key exists in the range.
        /// \throws MdbxException if a database error occurs.
        bool contains_range(const KeyT& from_key, const KeyT& to_key,
                            const Transaction& txn) const {
            return contains_range(from_key, to_key, txn.handle());
        }

        /// \brief Counts keys within an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Optional transaction handle.
        /// \return Number of keys in the requested range.
        /// \throws MdbxException if a database error occurs.
        std::size_t count_range(const KeyT& from_key, const KeyT& to_key,
                                MDBX_txn* txn = nullptr) const {
            std::size_t result = 0;
            with_transaction([this, &from_key, &to_key, &result](MDBX_txn* t) {
                result = db_count_range(from_key, to_key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Counts keys within an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Active transaction wrapper.
        /// \return Number of keys in the requested range.
        /// \throws MdbxException if a database error occurs.
        std::size_t count_range(const KeyT& from_key, const KeyT& to_key,
                                const Transaction& txn) const {
            return count_range(from_key, to_key, txn.handle());
        }

        /// \brief Removes all keys within an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Optional transaction handle.
        /// \return Number of deleted records.
        /// \throws MdbxException if a database error occurs.
        std::size_t erase_range(const KeyT& from_key, const KeyT& to_key,
                                MDBX_txn* txn = nullptr) {
            std::size_t result = 0;
            with_transaction([this, &from_key, &to_key, &result](MDBX_txn* t) {
                result = db_erase_range(from_key, to_key, t);
            }, TransactionMode::WRITABLE, txn);
            return result;
        }

        /// \brief Removes all keys within an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Active transaction wrapper.
        /// \return Number of deleted records.
        /// \throws MdbxException if a database error occurs.
        std::size_t erase_range(const KeyT& from_key, const KeyT& to_key,
                                const Transaction& txn) {
            return erase_range(from_key, to_key, txn.handle());
        }

        // --- Existing bulk / point API ---

        /// \brief Appends keys to the table.
        /// \tparam ContainerT Container type storing keys.
        /// \param container Source keys.
        /// \param txn Optional transaction handle.
        /// \note Duplicate source keys and keys already present in the table are
        ///       ignored by set semantics.
        template<template<class...> class ContainerT>
        void append(const ContainerT<KeyT>& container, MDBX_txn* txn = nullptr) {
            with_transaction([this, &container](MDBX_txn* t) {
                db_append(container, t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Appends keys to the table.
        /// \tparam ContainerT Container type storing keys.
        /// \param container Source keys.
        /// \param txn Active transaction wrapper.
        template<template<class...> class ContainerT>
        void append(const ContainerT<KeyT>& container, const Transaction& txn) {
            append(container, txn.handle());
        }

        /// \brief Replaces table content with keys from a container.
        /// \tparam ContainerT Container type storing keys.
        /// \param container Source keys.
        /// \param txn Optional transaction handle.
        /// \note Clears existing table content before appending \p container.
        ///       This operation does not preserve existing records incrementally.
        template<template<class...> class ContainerT>
        void reconcile(const ContainerT<KeyT>& container, MDBX_txn* txn = nullptr) {
            with_transaction([this, &container](MDBX_txn* t) {
                db_clear(t);
                db_append(container, t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Replaces table content with keys from a container.
        /// \tparam ContainerT Container type storing keys.
        /// \param container Source keys.
        /// \param txn Active transaction wrapper.
        template<template<class...> class ContainerT>
        void reconcile(const ContainerT<KeyT>& container, const Transaction& txn) {
            reconcile(container, txn.handle());
        }

        /// \brief Inserts a key if it is absent.
        /// \param key Key to insert.
        /// \param txn Optional transaction handle.
        /// \return \c true if inserted, \c false if the key already exists.
        /// \note Uses MDBX no-overwrite semantics; existing keys are never
        ///       replaced.
        bool insert(const KeyT& key, MDBX_txn* txn = nullptr) {
            bool res = false;
            with_transaction([this, &key, &res](MDBX_txn* t) {
                res = db_insert(key, t);
            }, TransactionMode::WRITABLE, txn);
            return res;
        }

        /// \brief Inserts a key if it is absent.
        /// \param key Key to insert.
        /// \param txn Active transaction wrapper.
        /// \return \c true if inserted, \c false if the key already exists.
        bool insert(const KeyT& key, const Transaction& txn) {
            return insert(key, txn.handle());
        }

        /// \brief Checks whether a key exists.
        /// \param key Key to look up.
        /// \param txn Optional transaction handle.
        /// \return \c true if the key exists.
        bool contains(const KeyT& key, MDBX_txn* txn = nullptr) const {
            bool res = false;
            with_transaction([this, &key, &res](MDBX_txn* t) {
                res = db_contains(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }

        /// \brief Checks whether a key exists.
        /// \param key Key to look up.
        /// \param txn Active transaction wrapper.
        /// \return \c true if the key exists.
        bool contains(const KeyT& key, const Transaction& txn) const {
            return contains(key, txn.handle());
        }

        /// \brief Counts stored keys.
        /// \param txn Optional transaction handle.
        /// \return Number of keys in the table.
        std::size_t count(MDBX_txn* txn = nullptr) const {
            std::size_t res = 0;
            with_transaction([this, &res](MDBX_txn* t) {
                res = db_count(t);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }

        /// \brief Counts stored keys.
        /// \param txn Active transaction wrapper.
        /// \return Number of keys in the table.
        std::size_t count(const Transaction& txn) const {
            return count(txn.handle());
        }

        /// \brief Checks whether the table has no keys.
        /// \param txn Optional transaction handle.
        /// \return \c true if the table is empty.
        bool empty(MDBX_txn* txn = nullptr) const {
            return count(txn) == 0;
        }

        /// \brief Checks whether the table has no keys.
        /// \param txn Active transaction wrapper.
        /// \return \c true if the table is empty.
        bool empty(const Transaction& txn) const {
            return empty(txn.handle());
        }

        /// \brief Erases a key.
        /// \param key Key to remove.
        /// \param txn Optional transaction handle.
        /// \return \c true if the key was removed.
        bool erase(const KeyT& key, MDBX_txn* txn = nullptr) {
            bool res = false;
            with_transaction([this, &key, &res](MDBX_txn* t) {
                res = db_erase(key, t);
            }, TransactionMode::WRITABLE, txn);
            return res;
        }

        /// \brief Erases a key.
        /// \param key Key to remove.
        /// \param txn Active transaction wrapper.
        /// \return \c true if the key was removed.
        bool erase(const KeyT& key, const Transaction& txn) {
            return erase(key, txn.handle());
        }

        /// \brief Removes all keys.
        /// \param txn Optional transaction handle.
        void clear(MDBX_txn* txn = nullptr) {
            with_transaction([this](MDBX_txn* t) {
                db_clear(t);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Removes all keys.
        /// \param txn Active transaction wrapper.
        void clear(const Transaction& txn) {
            clear(txn.handle());
        }

    private:
        static MDBX_val empty_value() {
            MDBX_val value;
            value.iov_base = nullptr;
            value.iov_len = 0;
            return value;
        }

        template<template<class...> class ContainerT>
        void db_load(ContainerT<KeyT>& container, MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");

            MDBX_val db_key, db_val;
            int rc = MDBX_SUCCESS;
            while ((rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT)) == MDBX_SUCCESS) {
                container.insert(container.end(), deserialize_key<KeyT>(db_key));
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to read key table");
            }
        }

        template<class ContainerT>
        void db_range(const KeyT& from_key, const KeyT& to_key,
                      ContainerT& out, MDBX_txn* txn) const {
            SerializeScratch sc_from_key;
            SerializeScratch sc_to_key;
            MDBX_val db_from_key = serialize_key<Options::safe_integer_key>(from_key, sc_from_key);
            MDBX_val db_to_key = serialize_key<Options::safe_integer_key>(to_key, sc_to_key);

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            if (mdbx_cmp(txn, m_dbi, &db_from_key, &db_to_key) > 0) return;

            MDBX_val db_key = db_from_key;
            MDBX_val db_val;
            bool stopped_by_upper_bound = false;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            while (rc == MDBX_SUCCESS) {
                if (mdbx_cmp(txn, m_dbi, &db_key, &db_to_key) > 0) {
                    stopped_by_upper_bound = true;
                    break;
                }
                out.insert(out.end(), deserialize_key<KeyT>(db_key));
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
            }
            if (!stopped_by_upper_bound && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to read key range");
            }
        }

        // --- Streaming / range helpers ---

        template<typename CallbackT>
        bool db_for_each_range(const KeyT& from_key, const KeyT& to_key,
                               CallbackT& callback, MDBX_txn* txn) const {
            SerializeScratch sc_from_key;
            SerializeScratch sc_to_key;
            MDBX_val db_from_key = serialize_key<Options::safe_integer_key>(from_key, sc_from_key);
            MDBX_val db_to_key = serialize_key<Options::safe_integer_key>(to_key, sc_to_key);

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            if (mdbx_cmp(txn, m_dbi, &db_from_key, &db_to_key) > 0) {
                return true;
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
                if (!callback(deserialize_key<KeyT>(db_key))) {
                    return false;
                }
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
            }
            if (!stopped_by_upper_bound && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to iterate key range");
            }
            return true;
        }

        // --- Bounds ---

#       if __cplusplus >= 201703L

        std::optional<KeyT> db_lower_bound(const KeyT& key, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_SUCCESS) {
                return deserialize_key<KeyT>(db_key);
            }

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            if (rc == MDBX_SUCCESS) {
                return deserialize_key<KeyT>(db_key);
            }
            if (rc == MDBX_NOTFOUND) {
                return std::nullopt;
            }
            check_mdbx(rc, "Failed to seek lower bound");
            return std::nullopt;
        }

        std::optional<KeyT> db_upper_bound(const KeyT& key, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_key_exact = db_key;
            MDBX_val db_val;

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            if (rc == MDBX_NOTFOUND) {
                return std::nullopt;
            }
            check_mdbx(rc, "Failed to seek upper bound");

            // If exact match found, move to next distinct key
            if (mdbx_cmp(txn, m_dbi, &db_key, &db_key_exact) == 0) {
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
                if (rc == MDBX_NOTFOUND) {
                    return std::nullopt;
                }
                check_mdbx(rc, "Failed to seek next key for upper bound");
            }
            return deserialize_key<KeyT>(db_key);
        }

        std::optional<KeyT> db_first(MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_FIRST);
            if (rc == MDBX_NOTFOUND) {
                return std::nullopt;
            }
            check_mdbx(rc, "Failed to seek first key");
            return deserialize_key<KeyT>(db_key);
        }

        std::optional<KeyT> db_last(MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_LAST);
            if (rc == MDBX_NOTFOUND) {
                return std::nullopt;
            }
            check_mdbx(rc, "Failed to seek last key");
            return deserialize_key<KeyT>(db_key);
        }

#       else

        std::pair<bool, KeyT> db_lower_bound_compat(const KeyT& key, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_SUCCESS) {
                return std::make_pair(true, deserialize_key<KeyT>(db_key));
            }

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            if (rc == MDBX_SUCCESS) {
                return std::make_pair(true, deserialize_key<KeyT>(db_key));
            }
            if (rc == MDBX_NOTFOUND) {
                return std::make_pair(false, KeyT());
            }
            check_mdbx(rc, "Failed to seek lower bound");
            return std::make_pair(false, KeyT());
        }

        std::pair<bool, KeyT> db_upper_bound_compat(const KeyT& key, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_key_exact = db_key;
            MDBX_val db_val;

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            if (rc == MDBX_NOTFOUND) {
                return std::make_pair(false, KeyT());
            }
            check_mdbx(rc, "Failed to seek upper bound");

            if (mdbx_cmp(txn, m_dbi, &db_key, &db_key_exact) == 0) {
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
                if (rc == MDBX_NOTFOUND) {
                    return std::make_pair(false, KeyT());
                }
                check_mdbx(rc, "Failed to seek next key for upper bound");
            }
            return std::make_pair(true, deserialize_key<KeyT>(db_key));
        }

        std::pair<bool, KeyT> db_first_compat(MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_FIRST);
            if (rc == MDBX_NOTFOUND) {
                return std::make_pair(false, KeyT());
            }
            check_mdbx(rc, "Failed to seek first key");
            return std::make_pair(true, deserialize_key<KeyT>(db_key));
        }

        std::pair<bool, KeyT> db_last_compat(MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_LAST);
            if (rc == MDBX_NOTFOUND) {
                return std::make_pair(false, KeyT());
            }
            check_mdbx(rc, "Failed to seek last key");
            return std::make_pair(true, deserialize_key<KeyT>(db_key));
        }

#       endif

        // --- Reverse scan ---

        void db_range_reverse(const KeyT& from_key, const KeyT& to_key,
                              std::vector<KeyT>& out, MDBX_txn* txn) const {
            SerializeScratch sc_from_key;
            SerializeScratch sc_to_key;
            MDBX_val db_from_key = serialize_key<Options::safe_integer_key>(from_key, sc_from_key);
            MDBX_val db_to_key = serialize_key<Options::safe_integer_key>(to_key, sc_to_key);

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            if (mdbx_cmp(txn, m_dbi, &db_from_key, &db_to_key) > 0) return;

            MDBX_val db_key = db_to_key;
            MDBX_val db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            if (rc == MDBX_NOTFOUND) {
                // All keys are less than to_key; seek to last and walk backward
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_LAST);
            } else if (rc == MDBX_SUCCESS) {
                // If the found key is greater than to_key, step back once
                if (mdbx_cmp(txn, m_dbi, &db_key, &db_to_key) > 0) {
                    rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_PREV);
                }
            }
            if (rc == MDBX_NOTFOUND) return;
            check_mdbx(rc, "Failed to seek reverse range start");

            while (rc == MDBX_SUCCESS) {
                if (mdbx_cmp(txn, m_dbi, &db_key, &db_from_key) < 0) {
                    break;
                }
                out.push_back(deserialize_key<KeyT>(db_key));
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_PREV);
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to iterate reverse key range");
            }
        }

        void db_range_reverse(const KeyT& from_key, const KeyT& to_key,
                              std::size_t limit, std::vector<KeyT>& out, MDBX_txn* txn) const {
            if (limit == 0) return;
            SerializeScratch sc_from_key;
            SerializeScratch sc_to_key;
            MDBX_val db_from_key = serialize_key<Options::safe_integer_key>(from_key, sc_from_key);
            MDBX_val db_to_key = serialize_key<Options::safe_integer_key>(to_key, sc_to_key);

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            if (mdbx_cmp(txn, m_dbi, &db_from_key, &db_to_key) > 0) return;

            MDBX_val db_key = db_to_key;
            MDBX_val db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            if (rc == MDBX_NOTFOUND) {
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_LAST);
            } else if (rc == MDBX_SUCCESS) {
                if (mdbx_cmp(txn, m_dbi, &db_key, &db_to_key) > 0) {
                    rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_PREV);
                }
            }
            if (rc == MDBX_NOTFOUND) return;
            check_mdbx(rc, "Failed to seek reverse range start");

            bool stopped_by_limit = false;
            bool stopped_by_lower_bound = false;
            while (rc == MDBX_SUCCESS) {
                if (mdbx_cmp(txn, m_dbi, &db_key, &db_from_key) < 0) {
                    stopped_by_lower_bound = true;
                    break;
                }
                out.push_back(deserialize_key<KeyT>(db_key));
                if (out.size() >= limit) {
                    stopped_by_limit = true;
                    break;
                }
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_PREV);
            }
            if (!stopped_by_limit && !stopped_by_lower_bound && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to iterate reverse key range");
            }
        }

        // --- Range metadata and removal ---

        bool db_contains_range(const KeyT& from_key, const KeyT& to_key, MDBX_txn* txn) const {
            SerializeScratch sc_from_key;
            SerializeScratch sc_to_key;
            MDBX_val db_from_key = serialize_key<Options::safe_integer_key>(from_key, sc_from_key);
            MDBX_val db_to_key = serialize_key<Options::safe_integer_key>(to_key, sc_to_key);

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            if (mdbx_cmp(txn, m_dbi, &db_from_key, &db_to_key) > 0) return false;

            MDBX_val db_key = db_from_key;
            MDBX_val db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to seek range for contains");
            return mdbx_cmp(txn, m_dbi, &db_key, &db_to_key) <= 0;
        }

        std::size_t db_count_range(const KeyT& from_key, const KeyT& to_key, MDBX_txn* txn) const {
            SerializeScratch sc_from_key;
            SerializeScratch sc_to_key;
            MDBX_val db_from_key = serialize_key<Options::safe_integer_key>(from_key, sc_from_key);
            MDBX_val db_to_key = serialize_key<Options::safe_integer_key>(to_key, sc_to_key);

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            if (mdbx_cmp(txn, m_dbi, &db_from_key, &db_to_key) > 0) return 0;

            MDBX_val db_key = db_from_key;
            MDBX_val db_val;
            std::size_t count = 0;
            bool stopped_by_upper_bound = false;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            while (rc == MDBX_SUCCESS) {
                if (mdbx_cmp(txn, m_dbi, &db_key, &db_to_key) > 0) {
                    stopped_by_upper_bound = true;
                    break;
                }
                ++count;
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
            }
            if (!stopped_by_upper_bound && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to count key range");
            }
            return count;
        }

        std::size_t db_erase_range(const KeyT& from_key, const KeyT& to_key, MDBX_txn* txn) const {
            SerializeScratch sc_from_key;
            SerializeScratch sc_to_key;
            MDBX_val db_from_key = serialize_key<Options::safe_integer_key>(from_key, sc_from_key);
            MDBX_val db_to_key = serialize_key<Options::safe_integer_key>(to_key, sc_to_key);

            std::size_t removed = 0;
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            if (mdbx_cmp(txn, m_dbi, &db_from_key, &db_to_key) > 0) return 0;

            MDBX_val db_key = db_from_key;
            MDBX_val db_val;
            bool stopped_by_upper_bound = false;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            while (rc == MDBX_SUCCESS) {
                if (mdbx_cmp(txn, m_dbi, &db_key, &db_to_key) > 0) {
                    stopped_by_upper_bound = true;
                    break;
                }
#               if MDBXC_SYNC_ENABLED
                const std::vector<std::uint8_t> kbytes = capture_bytes(db_key);
#               endif
                check_mdbx(mdbx_cursor_del(cursor.get(), MDBX_CURRENT), "Failed to erase key in range");
#               if MDBXC_SYNC_ENABLED
                record_op(txn, sync::ChangeOpType::Delete, kbytes, {});
#               endif
                ++removed;
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
            }
            if (!stopped_by_upper_bound && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to erase key range");
            }
            return removed;
        }

        // --- Existing private helpers ---

        template<template<class...> class ContainerT>
        void db_append(const ContainerT<KeyT>& container, MDBX_txn* txn) {
            for (typename ContainerT<KeyT>::const_iterator it = container.begin();
                 it != container.end(); ++it) {
                db_insert(*it, txn);
            }
        }

        bool db_insert(const KeyT& key, MDBX_txn* txn) {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val = empty_value();
            int rc = mdbx_put(txn, m_dbi, &db_key, &db_val, MDBX_NOOVERWRITE);
            if (rc == MDBX_SUCCESS) {
#               if MDBXC_SYNC_ENABLED
                record_op(txn, sync::ChangeOpType::Put,
                          capture_bytes(db_key), {});
#               endif
                return true;
            }
            if (rc == MDBX_KEYEXIST) return false;
            check_mdbx(rc, "Failed to insert key");
            return false;
        }

        bool db_contains(const KeyT& key, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_SUCCESS) return true;
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to check key presence");
            return false;
        }

        std::size_t db_count(MDBX_txn* txn) const {
            MDBX_stat stat;
            check_mdbx(mdbx_dbi_stat(txn, m_dbi, &stat, sizeof(stat)), "Failed to query database statistics");
            return stat.ms_entries;
        }

        bool db_erase(const KeyT& key, MDBX_txn* txn) {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            int rc = mdbx_del(txn, m_dbi, &db_key, nullptr);
            if (rc == MDBX_SUCCESS) {
#               if MDBXC_SYNC_ENABLED
                record_op(txn, sync::ChangeOpType::Delete,
                          capture_bytes(db_key), {});
#               endif
                return true;
            }
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to erase key");
            return false;
        }

        void db_clear(MDBX_txn* txn) {
            check_mdbx(mdbx_drop(txn, m_dbi, 0), "Failed to clear table");
#           if MDBXC_SYNC_ENABLED
            record_op(txn, sync::ChangeOpType::ClearTable, {}, {});
#           endif
        }
    };

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_KEY_TABLE_HPP_INCLUDED
