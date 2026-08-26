#pragma once
#ifndef MDBX_CONTAINERS_HEADER_KEY_VALUE_TABLE_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_KEY_VALUE_TABLE_HPP_INCLUDED

/// \file KeyValueTable.hpp
/// \brief Map-like table storing one value per key in an MDBX database.
/// \details
/// Provides persistent \c std::map -like storage with explicit transaction
/// overloads and bulk synchronization helpers.

#include "common.hpp"
#include "detail/result_containers.hpp"
#include <map>
#include <unordered_set>

namespace mdbxc {

    /// \class KeyValueTable
    /// \ingroup mdbxc_tables
    /// \brief Map-like table persisted in MDBX.
    /// \tparam KeyT Type of the keys.
    /// \tparam ValueT Type of the values.
    /// \tparam Options Compile-time table policy. Does not change the database
    ///         storage format.
    /// \details
    /// Provides one stored value per key, similar to \c std::map. \c insert()
    /// succeeds only when the key is absent, while \c insert_or_assign() and
    /// \c append() upsert values for source keys. Lookup helpers expose
    /// \c std::optional in C++17 and \c std::pair<bool,ValueT> compatibility
    /// forms for C++11.
    ///
    /// Bulk loading writes database content into caller-provided containers
    /// using that container's own insertion rules. Bulk reconciliation upserts
    /// every source key and removes keys that are no longer present in the
    /// source; for vector input with duplicate keys, the last written value wins.
    ///
    /// \warning Reading through \c operator[] inserts and persists a
    ///          default-constructed value when the key is missing, matching the
    ///          mutating behavior of \c std::map::operator[].
    template<class KeyT, class ValueT, class Options = DefaultTableOptions>
    class KeyValueTable final : public BaseTable {
    public:
        typedef std::pair<KeyT, ValueT> value_type;

        /// \brief Default constructor.
        /// \param connection Existing \ref Connection instance.
        /// \param name Name of the table within the MDBX environment.
        /// \param flags Additional MDBX database flags.
        KeyValueTable(std::shared_ptr<Connection> connection,
                      std::string name = "kv_store",
                      MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(std::move(connection),
                        std::move(name),
                        flags | get_mdbx_flags<KeyT>()) {}

        /// \brief Constructor with configuration.
        /// \param config Configuration settings for the database.
        /// \param name Name of the table within the MDBX environment.
        /// \param flags Additional MDBX database flags.
        explicit KeyValueTable(const Config& config,
                               std::string name = "kv_store",
                               MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE)
            : BaseTable(Connection::create(config),
                        std::move(name),
                        flags | get_mdbx_flags<KeyT>()) {}

        /// \brief Destructor.
        ~KeyValueTable() override final = default;

        // --- Operators ---

        /// \brief Assigns a container (e.g., std::map or std::unordered_map) to the database.
        /// \param container The container with key-value pairs.
        /// \return Reference to this KeyValueTable.
        /// \throws MdbxException if a database error occurs.
        /// \note Equivalent to \c reconcile(container): source keys are upserted
        ///       and stale database keys are removed.
        template<template <class...> class ContainerT>
        KeyValueTable& operator=(const ContainerT<KeyT, ValueT>& container) {
            with_transaction([this, &container](MDBX_txn* txn) {
                db_reconcile(container, txn);
            }, TransactionMode::WRITABLE);
            return *this;
        }
        
        /// \brief Assigns a vector of key-value pairs to the database.
        /// \param container The vector with key-value pairs.
        /// \return Reference to this KeyValueTable.
        /// \throws MdbxException if a database error occurs.
        /// \note Equivalent to \c reconcile(container). If the vector contains
        ///       duplicate keys, later elements overwrite earlier ones.
        KeyValueTable& operator=(const std::vector<std::pair<KeyT, ValueT>>& container) {
            with_transaction([this, &container](MDBX_txn* txn) {
                db_reconcile(container, txn);
            }, TransactionMode::WRITABLE);
            return *this;
        }

        /// \brief Loads all key-value pairs from the database into a container.
        /// \tparam ContainerT Container type (e.g., std::map, std::unordered_map, std::vector).
        /// \return Filled container.
        /// \throws MdbxException if a database error occurs.
        /// \note The returned container is fresh; database records are inserted
        ///       according to the requested container's insertion rules.
        template<template<class...> class ContainerT = std::map>
#       if __cplusplus >= 201703L
        auto operator()() {
            using ReturnT = std::conditional_t<
                std::is_same_v<ContainerT<KeyT, ValueT>, std::vector<std::pair<KeyT, ValueT>>>,
                std::vector<std::pair<KeyT, ValueT>>,
                ContainerT<KeyT, ValueT>
            >;

            ReturnT container;
            with_transaction([this, &container](MDBX_txn* txn) {
                db_load(container, txn);
            }, TransactionMode::READ_ONLY);
            return container;
        }
#       else

        typename std::conditional<
            std::is_same<ContainerT<KeyT, ValueT>, std::vector<std::pair<KeyT, ValueT> > >::value,
            std::vector<std::pair<KeyT, ValueT> >,
            ContainerT<KeyT, ValueT>
        >::type operator()() {
            typedef typename std::conditional<
                std::is_same<ContainerT<KeyT, ValueT>, std::vector<std::pair<KeyT, ValueT> > >::value,
                std::vector<std::pair<KeyT, ValueT> >,
                ContainerT<KeyT, ValueT>
            >::type ReturnT;

            ReturnT container;
            with_transaction([this, &container](MDBX_txn* txn) {
                db_load(container, txn);
            }, TransactionMode::READ_ONLY);
            return container;
        }
#       endif

        /// \brief Helper proxy for convenient assignment via operator[].
        class AssignmentProxy {
        public:
            /// \brief Constructs the proxy for a specific key.
            /// \param db Reference to the owning table.
            /// \param key Key associated with this proxy.
            AssignmentProxy(KeyValueTable& db, KeyT key)
                : m_db(db), m_key(std::move(key)) {}

            /// \brief Assigns a value to the stored key.
            /// \param value Value to store.
            /// \return Reference to this proxy.
            AssignmentProxy& operator=(const ValueT& value) {
                m_db.insert_or_assign(m_key, value);
                return *this;
            }

            /// \brief Implicit conversion to the value type.
            /// If the key does not exist, a default-constructed value is inserted.
            /// \warning Reading a missing key through this conversion mutates
            ///          the table by persisting a default-constructed value.
            operator ValueT() const {
#               if __cplusplus >= 201703L
                auto val = m_db.find(m_key);
                if (val) return *val;
#               else
                std::pair<bool, ValueT> val = m_db.find(m_key);
                if (val.first) return val.second;
#               endif
                ValueT def{}; // default construct if missing
                m_db.insert_or_assign(m_key, def); // persist the default value
                return def;
            }

        private:
            KeyValueTable& m_db; ///< Reference to the owning table.
            KeyT m_key;          ///< Key associated with this proxy.
        };
        
        /// \brief Provides convenient access to insert or read a value by key.
        /// \param key Key to access.
        /// \return Proxy object used for assignment or implicit read.
        /// \warning Reading a missing key through the returned proxy inserts a
        ///          default-constructed value.
        AssignmentProxy operator[](const KeyT& key) {
            return AssignmentProxy(*this, key);
        }

        // --- Existing methods ---

        /// \brief Loads data from the database into the container.
        /// \tparam ContainerT Container type (e.g., std::map or std::unordered_map).
        /// \param container Container to be synchronized with database content.
        /// \param txn Active MDBX transaction.
        /// \throws MdbxException if a database error occurs.
        /// \note Adds records to \p container; it does not clear existing
        ///       container contents first.
        template<template <class...> class ContainerT>
        void load(ContainerT<KeyT, ValueT>& container, MDBX_txn* txn = nullptr) {
            with_transaction([this, &container](MDBX_txn* txn) {
                db_load(container, txn);
            }, TransactionMode::READ_ONLY, txn);
        }
        
        /// \brief Loads data from the database into the container using provided transaction.
        /// \tparam ContainerT Container type (e.g., std::map or std::unordered_map).
        /// \param container Container to be synchronized with database content.
        /// \param txn Transaction wrapper used for the operation.
        /// \throws MdbxException if a database error occurs.
        template<template <class...> class ContainerT>
        void load(ContainerT<KeyT, ValueT>& container, const Transaction& txn) {
            load(container, txn.handle());
        }
        
        /// \brief Loads all key-value pairs into a std::vector of pairs.
        /// \param container Container to be synchronized with database content.
        /// \param txn Transaction wrapper used for the operation.
        /// \throws MdbxException if a database error occurs.
        /// \note Adds records to \p container; it does not clear existing
        ///       container contents first.
        void load(std::vector<std::pair<KeyT, ValueT>>& container, MDBX_txn* txn = nullptr) {
            with_transaction([this, &container](MDBX_txn* txn) {
                db_load(container, txn);
            }, TransactionMode::READ_ONLY, txn);
        }
        
        /// \brief Loads all key-value pairs into a std::vector of pairs.
        /// \param container Container to be synchronized with database content.
        /// \param txn Transaction wrapper used for the operation.
        /// \throws MdbxException if a database error occurs.
        void load(std::vector<std::pair<KeyT, ValueT>>& container, const Transaction& txn) {
            load(container, txn.handle());
        }

        /// \brief Retrieves all key-value pairs into the specified container type.
        /// \tparam ContainerT Container type (e.g., std::map, std::unordered_map, std::vector).
        /// \param txn Optional transaction handle.
        /// \return Filled container.
        /// \throws MdbxException if a database error occurs.
        /// \note Returns a fresh container populated from the table.
        template<template<class...> class ContainerT = std::map>
#       if __cplusplus >= 201703L
        auto retrieve_all(MDBX_txn* txn = nullptr) {
            using ReturnT = std::conditional_t<
                std::is_same_v<ContainerT<KeyT, ValueT>, std::vector<std::pair<KeyT, ValueT>>>,
                std::vector<std::pair<KeyT, ValueT>>,
                ContainerT<KeyT, ValueT>
            >;

            ReturnT container;
            with_transaction([this, &container](MDBX_txn* txn) {
                db_load(container, txn);
            }, TransactionMode::READ_ONLY, txn);
            return container;
        }

        /// \brief Retrieves all key-value pairs into the specified container type.
        /// \tparam ContainerT Container type (e.g., std::map, std::unordered_map, std::vector).
        /// \param txn Transaction wrapper used for the retrieval.
        /// \return Filled container.
        /// \throws MdbxException if a database error occurs.
        template<template<class...> class ContainerT = std::map>
        auto retrieve_all(const Transaction& txn) {
            return retrieve_all<ContainerT>(txn.handle());
        }
#       else
        typename std::conditional<
            std::is_same<ContainerT<KeyT, ValueT>, std::vector<std::pair<KeyT, ValueT> > >::value,
            std::vector<std::pair<KeyT, ValueT> >,
            ContainerT<KeyT, ValueT>
        >::type retrieve_all(MDBX_txn* txn = nullptr) {
            typedef typename std::conditional<
                std::is_same<ContainerT<KeyT, ValueT>, std::vector<std::pair<KeyT, ValueT> > >::value,
                std::vector<std::pair<KeyT, ValueT> >,
                ContainerT<KeyT, ValueT>
            >::type ReturnT;

            ReturnT container;
            with_transaction([this, &container](MDBX_txn* txn) {
                db_load(container, txn);
            }, TransactionMode::READ_ONLY, txn);
            return container;
        }

        /// \brief Retrieves all key-value pairs into the specified container type.
        /// \tparam ContainerT Container type (e.g., std::map, std::unordered_map, std::vector).
        /// \param txn Transaction wrapper used for the retrieval.
        /// \return Filled container.
        /// \throws MdbxException if a database error occurs.
        template<template<class...> class ContainerT = std::map>
        typename std::conditional<
            std::is_same<ContainerT<KeyT, ValueT>, std::vector<std::pair<KeyT, ValueT> > >::value,
            std::vector<std::pair<KeyT, ValueT> >,
            ContainerT<KeyT, ValueT>
        >::type retrieve_all(const Transaction& txn) {
            return retrieve_all<ContainerT>(txn.handle());
        }
#       endif

        /// \brief Retrieves key-value pairs within an inclusive key range.
        /// \tparam ContainerT Container type storing key-value pairs.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Optional transaction handle.
        /// \return Container with key-value pairs from the requested range.
        /// \throws MdbxException if a database error occurs.
        /// \note The default \c std::map matches unique-key table semantics.
        ///       Use \c range<std::vector>() to preserve MDBX iteration order.
        /// \complexity O(log n + m), where m is the number of returned pairs.
        template<template<class...> class ContainerT = std::map>
        typename detail::key_value_result_container<ContainerT, KeyT, ValueT>::type
        range(const KeyT& from_key, const KeyT& to_key,
              MDBX_txn* txn = nullptr) const {
            typedef typename detail::key_value_result_container<ContainerT, KeyT, ValueT>::type ReturnT;
            ReturnT pairs;
            with_transaction([this, &from_key, &to_key, &pairs](MDBX_txn* txn) {
                db_range(from_key, to_key, pairs, txn);
            }, TransactionMode::READ_ONLY, txn);
            return pairs;
        }

        /// \brief Retrieves key-value pairs within an inclusive key range.
        /// \tparam ContainerT Container type storing key-value pairs.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Active transaction wrapper.
        /// \return Container with key-value pairs from the requested range.
        /// \throws MdbxException if a database error occurs.
        /// \complexity O(log n + m), where m is the number of returned pairs.
        template<template<class...> class ContainerT = std::map>
        typename detail::key_value_result_container<ContainerT, KeyT, ValueT>::type
        range(const KeyT& from_key, const KeyT& to_key,
              const Transaction& txn) const {
            return range<ContainerT>(from_key, to_key, txn.handle());
        }

        /// \brief Retrieves values whose keys are within an inclusive key range.
        /// \tparam ContainerT Container type storing values.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Optional transaction handle.
        /// \return Container with values from the requested range.
        /// \throws MdbxException if a database error occurs.
        /// \note The default \c std::vector preserves MDBX iteration order and
        ///       duplicate values. Associative containers apply their own rules.
        /// \complexity O(log n + m), where m is the number of returned values.
        template<template<class...> class ContainerT = std::vector>
        ContainerT<ValueT> range_values(const KeyT& from_key, const KeyT& to_key,
                                        MDBX_txn* txn = nullptr) const {
            ContainerT<ValueT> values;
            with_transaction([this, &from_key, &to_key, &values](MDBX_txn* txn) {
                db_range_values(from_key, to_key, values, txn);
            }, TransactionMode::READ_ONLY, txn);
            return values;
        }

        /// \brief Retrieves values whose keys are within an inclusive key range.
        /// \tparam ContainerT Container type storing values.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Active transaction wrapper.
        /// \return Container with values from the requested range.
        /// \throws MdbxException if a database error occurs.
        /// \complexity O(log n + m), where m is the number of returned values.
        template<template<class...> class ContainerT = std::vector>
        ContainerT<ValueT> range_values(const KeyT& from_key, const KeyT& to_key,
                                        const Transaction& txn) const {
            return range_values<ContainerT>(from_key, to_key, txn.handle());
        }

        // --- Streaming and range helpers ---

        /// \brief Calls a callback for every key-value pair in an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param callback Invoked as \c callback(const KeyT&, const ValueT&). Return \c true to continue.
        /// \param txn Optional transaction handle.
        /// \return \c true if every pair was visited, \c false if the callback stopped early.
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

        /// \brief Calls a callback for every key-value pair in an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param callback Invoked as \c callback(const KeyT&, const ValueT&). Return \c true to continue.
        /// \param txn Active transaction wrapper.
        /// \return \c true if every pair was visited, \c false if the callback stopped early.
        /// \throws MdbxException if a database error occurs.
        template<typename CallbackT>
        bool for_each_range(const KeyT& from_key, const KeyT& to_key,
                            CallbackT callback, const Transaction& txn) const {
            return for_each_range(from_key, to_key, callback, txn.handle());
        }

        /// \brief Collects key-value pairs matching a predicate within an inclusive range.
        /// \tparam ContainerT Pair-associative container template such as \c std::map or
        /// \c std::multimap, or \c std::vector for \c std::vector<std::pair<KeyT,ValueT>>.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param pred Predicate invoked as \c pred(const KeyT&, const ValueT&). Return \c true to collect.
        /// \param txn Optional transaction handle.
        /// \return Container with matching pairs in MDBX order.
        /// \throws MdbxException if a database error occurs.
        template<template<class...> class ContainerT = std::vector, typename PredicateT>
        typename detail::key_value_result_container<ContainerT, KeyT, ValueT>::type
        filter_range(const KeyT& from_key, const KeyT& to_key,
                     PredicateT pred, MDBX_txn* txn = nullptr) const {
            typedef typename detail::key_value_result_container<ContainerT, KeyT, ValueT>::type ReturnT;
            ReturnT out;
            for_each_range(from_key, to_key, [&out, &pred](const KeyT& key, const ValueT& value) -> bool {
                if (pred(key, value)) {
                    out.insert(out.end(), value_type(key, value));
                }
                return true;
            }, txn);
            return out;
        }

        /// \brief Collects key-value pairs matching a predicate within an inclusive range.
        /// \tparam ContainerT Pair-associative container template such as \c std::map or
        /// \c std::multimap, or \c std::vector for \c std::vector<std::pair<KeyT,ValueT>>.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param pred Predicate invoked as \c pred(const KeyT&, const ValueT&). Return \c true to collect.
        /// \param txn Active transaction wrapper.
        /// \return Container with matching pairs in MDBX order.
        /// \throws MdbxException if a database error occurs.
        template<template<class...> class ContainerT = std::vector, typename PredicateT>
        typename detail::key_value_result_container<ContainerT, KeyT, ValueT>::type
        filter_range(const KeyT& from_key, const KeyT& to_key,
                     PredicateT pred, const Transaction& txn) const {
            return filter_range<ContainerT>(from_key, to_key, pred, txn.handle());
        }

        // --- Bounds / edges ---

#       if __cplusplus >= 201703L

        /// \brief Finds the first key-value pair whose key is not less than the given key.
        /// \param key Key to bound.
        /// \param txn Optional transaction handle.
        /// \return \c std::optional<value_type> with the lower-bound pair, or empty if none.
        std::optional<value_type> lower_bound(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::optional<value_type> result;
            with_transaction([this, &key, &result](MDBX_txn* t) {
                result = db_lower_bound(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds the first key-value pair whose key is not less than the given key.
        /// \param key Key to bound.
        /// \param txn Active transaction wrapper.
        /// \return \c std::optional<value_type> with the lower-bound pair, or empty if none.
        std::optional<value_type> lower_bound(const KeyT& key, const Transaction& txn) const {
            return lower_bound(key, txn.handle());
        }

        /// \brief Finds the first key-value pair whose key is strictly greater than the given key.
        /// \param key Key to bound.
        /// \param txn Optional transaction handle.
        /// \return \c std::optional<value_type> with the upper-bound pair, or empty if none.
        std::optional<value_type> upper_bound(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::optional<value_type> result;
            with_transaction([this, &key, &result](MDBX_txn* t) {
                result = db_upper_bound(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds the first key-value pair whose key is strictly greater than the given key.
        /// \param key Key to bound.
        /// \param txn Active transaction wrapper.
        /// \return \c std::optional<value_type> with the upper-bound pair, or empty if none.
        std::optional<value_type> upper_bound(const KeyT& key, const Transaction& txn) const {
            return upper_bound(key, txn.handle());
        }

        /// \brief Retrieves the pair with the smallest key.
        /// \param txn Optional transaction handle.
        /// \return \c std::optional<value_type> with the first pair, or empty if the table is empty.
        std::optional<value_type> first(MDBX_txn* txn = nullptr) const {
            std::optional<value_type> result;
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_first(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves the pair with the smallest key.
        /// \param txn Active transaction wrapper.
        /// \return \c std::optional<value_type> with the first pair, or empty if the table is empty.
        std::optional<value_type> first(const Transaction& txn) const {
            return first(txn.handle());
        }

        /// \brief Retrieves the pair with the largest key.
        /// \param txn Optional transaction handle.
        /// \return \c std::optional<value_type> with the last pair, or empty if the table is empty.
        std::optional<value_type> last(MDBX_txn* txn = nullptr) const {
            std::optional<value_type> result;
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_last(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves the pair with the largest key.
        /// \param txn Active transaction wrapper.
        /// \return \c std::optional<value_type> with the last pair, or empty if the table is empty.
        std::optional<value_type> last(const Transaction& txn) const {
            return last(txn.handle());
        }

        /// \brief Retrieves the smallest key without deserializing the value.
        /// \param txn Optional transaction handle.
        /// \return \c std::optional<KeyT> with the smallest key, or empty if the table is empty.
        std::optional<KeyT> min_key(MDBX_txn* txn = nullptr) const {
            std::optional<KeyT> result;
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_min_key(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves the smallest key without deserializing the value.
        /// \param txn Active transaction wrapper.
        /// \return \c std::optional<KeyT> with the smallest key, or empty if the table is empty.
        std::optional<KeyT> min_key(const Transaction& txn) const {
            return min_key(txn.handle());
        }

        /// \brief Retrieves the largest key without deserializing the value.
        /// \param txn Optional transaction handle.
        /// \return \c std::optional<KeyT> with the largest key, or empty if the table is empty.
        std::optional<KeyT> max_key(MDBX_txn* txn = nullptr) const {
            std::optional<KeyT> result;
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_max_key(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves the largest key without deserializing the value.
        /// \param txn Active transaction wrapper.
        /// \return \c std::optional<KeyT> with the largest key, or empty if the table is empty.
        std::optional<KeyT> max_key(const Transaction& txn) const {
            return max_key(txn.handle());
        }

#       else

        /// \brief Finds the first key-value pair whose key is not less than the given key.
        /// \param key Key to bound.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> lower_bound(const KeyT& key, MDBX_txn* txn = nullptr) const {
            return lower_bound_compat(key, txn);
        }

        /// \brief Finds the first key-value pair whose key is not less than the given key.
        /// \param key Key to bound.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> lower_bound(const KeyT& key, const Transaction& txn) const {
            return lower_bound(key, txn.handle());
        }

        /// \brief Finds the first key-value pair whose key is not less than the given key (C++11-compatible).
        /// \param key Key to bound.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> lower_bound_compat(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::pair<bool, value_type> result(false, value_type(KeyT(), ValueT()));
            with_transaction([this, &key, &result](MDBX_txn* t) {
                result = db_lower_bound_compat(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds the first key-value pair whose key is not less than the given key (C++11-compatible).
        /// \param key Key to bound.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> lower_bound_compat(const KeyT& key, const Transaction& txn) const {
            return lower_bound_compat(key, txn.handle());
        }

        /// \brief Finds the first key-value pair whose key is strictly greater than the given key.
        /// \param key Key to bound.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> upper_bound(const KeyT& key, MDBX_txn* txn = nullptr) const {
            return upper_bound_compat(key, txn);
        }

        /// \brief Finds the first key-value pair whose key is strictly greater than the given key.
        /// \param key Key to bound.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> upper_bound(const KeyT& key, const Transaction& txn) const {
            return upper_bound(key, txn.handle());
        }

        /// \brief Finds the first key-value pair whose key is strictly greater than the given key (C++11-compatible).
        /// \param key Key to bound.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> upper_bound_compat(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::pair<bool, value_type> result(false, value_type(KeyT(), ValueT()));
            with_transaction([this, &key, &result](MDBX_txn* t) {
                result = db_upper_bound_compat(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds the first key-value pair whose key is strictly greater than the given key (C++11-compatible).
        /// \param key Key to bound.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> upper_bound_compat(const KeyT& key, const Transaction& txn) const {
            return upper_bound_compat(key, txn.handle());
        }

        /// \brief Retrieves the pair with the smallest key.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> first(MDBX_txn* txn = nullptr) const {
            return first_compat(txn);
        }

        /// \brief Retrieves the pair with the smallest key.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> first(const Transaction& txn) const {
            return first(txn.handle());
        }

        /// \brief Retrieves the pair with the smallest key (C++11-compatible).
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> first_compat(MDBX_txn* txn = nullptr) const {
            std::pair<bool, value_type> result(false, value_type(KeyT(), ValueT()));
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_first_compat(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves the pair with the smallest key (C++11-compatible).
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> first_compat(const Transaction& txn) const {
            return first_compat(txn.handle());
        }

        /// \brief Retrieves the pair with the largest key.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> last(MDBX_txn* txn = nullptr) const {
            return last_compat(txn);
        }

        /// \brief Retrieves the pair with the largest key.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> last(const Transaction& txn) const {
            return last(txn.handle());
        }

        /// \brief Retrieves the pair with the largest key (C++11-compatible).
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> last_compat(MDBX_txn* txn = nullptr) const {
            std::pair<bool, value_type> result(false, value_type(KeyT(), ValueT()));
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_last_compat(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves the pair with the largest key (C++11-compatible).
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and key-value pair.
        std::pair<bool, value_type> last_compat(const Transaction& txn) const {
            return last_compat(txn.handle());
        }

        /// \brief Retrieves the smallest key without deserializing the value.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and smallest key.
        std::pair<bool, KeyT> min_key(MDBX_txn* txn = nullptr) const {
            return min_key_compat(txn);
        }

        /// \brief Retrieves the smallest key without deserializing the value.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and smallest key.
        std::pair<bool, KeyT> min_key(const Transaction& txn) const {
            return min_key(txn.handle());
        }

        /// \brief Retrieves the smallest key without deserializing the value (C++11-compatible).
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and smallest key.
        std::pair<bool, KeyT> min_key_compat(MDBX_txn* txn = nullptr) const {
            std::pair<bool, KeyT> result(false, KeyT());
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_min_key_compat(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves the smallest key without deserializing the value (C++11-compatible).
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and smallest key.
        std::pair<bool, KeyT> min_key_compat(const Transaction& txn) const {
            return min_key_compat(txn.handle());
        }

        /// \brief Retrieves the largest key without deserializing the value.
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and largest key.
        std::pair<bool, KeyT> max_key(MDBX_txn* txn = nullptr) const {
            return max_key_compat(txn);
        }

        /// \brief Retrieves the largest key without deserializing the value.
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and largest key.
        std::pair<bool, KeyT> max_key(const Transaction& txn) const {
            return max_key(txn.handle());
        }

        /// \brief Retrieves the largest key without deserializing the value (C++11-compatible).
        /// \param txn Optional transaction handle.
        /// \return Pair of success flag and largest key.
        std::pair<bool, KeyT> max_key_compat(MDBX_txn* txn = nullptr) const {
            std::pair<bool, KeyT> result(false, KeyT());
            with_transaction([this, &result](MDBX_txn* t) {
                result = db_max_key_compat(t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Retrieves the largest key without deserializing the value (C++11-compatible).
        /// \param txn Active transaction wrapper.
        /// \return Pair of success flag and largest key.
        std::pair<bool, KeyT> max_key_compat(const Transaction& txn) const {
            return max_key_compat(txn.handle());
        }
#       endif

        // --- Reverse scan ---

        /// \brief Retrieves key-value pairs within an inclusive range in reverse MDBX order.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Optional transaction handle.
        /// \return Vector containing every pair in descending MDBX key order.
        /// \throws MdbxException if a database error occurs.
        std::vector<value_type> range_reverse(const KeyT& from_key, const KeyT& to_key,
                                              MDBX_txn* txn = nullptr) const {
            std::vector<value_type> out;
            with_transaction([this, &from_key, &to_key, &out](MDBX_txn* t) {
                db_range_reverse(from_key, to_key, out, t);
            }, TransactionMode::READ_ONLY, txn);
            return out;
        }

        /// \brief Retrieves key-value pairs within an inclusive range in reverse MDBX order.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Active transaction wrapper.
        /// \return Vector containing every pair in descending MDBX key order.
        /// \throws MdbxException if a database error occurs.
        std::vector<value_type> range_reverse(const KeyT& from_key, const KeyT& to_key,
                                              const Transaction& txn) const {
            return range_reverse(from_key, to_key, txn.handle());
        }

        /// \brief Retrieves up to \p limit key-value pairs within an inclusive range in reverse MDBX order.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param limit Maximum number of pairs to return.
        /// \param txn Optional transaction handle.
        /// \return Vector containing pairs in descending MDBX key order.
        /// \throws MdbxException if a database error occurs.
        /// \note \c limit == 0 returns an empty vector.
        std::vector<value_type> range_reverse(const KeyT& from_key, const KeyT& to_key,
                                              std::size_t limit, MDBX_txn* txn = nullptr) const {
            std::vector<value_type> out;
            with_transaction([this, &from_key, &to_key, &limit, &out](MDBX_txn* t) {
                db_range_reverse(from_key, to_key, limit, out, t);
            }, TransactionMode::READ_ONLY, txn);
            return out;
        }

        /// \brief Retrieves up to \p limit key-value pairs within an inclusive range in reverse MDBX order.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param limit Maximum number of pairs to return.
        /// \param txn Active transaction wrapper.
        /// \return Vector containing pairs in descending MDBX key order.
        /// \throws MdbxException if a database error occurs.
        std::vector<value_type> range_reverse(const KeyT& from_key, const KeyT& to_key,
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

        /// \brief Counts key-value pairs within an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Optional transaction handle.
        /// \return Number of pairs in the requested range.
        /// \throws MdbxException if a database error occurs.
        std::size_t count_range(const KeyT& from_key, const KeyT& to_key,
                                MDBX_txn* txn = nullptr) const {
            std::size_t result = 0;
            with_transaction([this, &from_key, &to_key, &result](MDBX_txn* t) {
                result = db_count_range(from_key, to_key, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Counts key-value pairs within an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Active transaction wrapper.
        /// \return Number of pairs in the requested range.
        /// \throws MdbxException if a database error occurs.
        std::size_t count_range(const KeyT& from_key, const KeyT& to_key,
                                const Transaction& txn) const {
            return count_range(from_key, to_key, txn.handle());
        }

        /// \brief Removes all key-value pairs within an inclusive range.
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

        /// \brief Removes all key-value pairs within an inclusive range.
        /// \param from_key Start key in MDBX key order.
        /// \param to_key End key in MDBX key order.
        /// \param txn Active transaction wrapper.
        /// \return Number of deleted records.
        /// \throws MdbxException if a database error occurs.
        std::size_t erase_range(const KeyT& from_key, const KeyT& to_key,
                                const Transaction& txn) {
            return erase_range(from_key, to_key, txn.handle());
        }

        /// \brief Appends data to the database.
        /// \tparam ContainerT Container type (e.g., std::map or std::unordered_map).
        /// \param container Container with content to be synchronized.
        /// \param txn Active MDBX transaction.
        /// \throws MdbxException if a database error occurs.
        /// \note Upserts source pairs. Existing keys not present in
        ///       \p container are left unchanged.
        template<template <class...> class ContainerT>
        void append(const ContainerT<KeyT, ValueT>& container, MDBX_txn* txn = nullptr) {
            with_transaction([this, &container](MDBX_txn* txn) {
                db_append(container, txn);
            }, TransactionMode::WRITABLE, txn);
        }
        
        /// \brief Appends data to the database.
        /// \tparam ContainerT Container type (e.g., std::map or std::unordered_map).
        /// \param container Container with content to be synchronized.
        /// \param txn Optional transaction handle.
        /// \throws MdbxException if a database error occurs.
        template<template <class...> class ContainerT>
        void append(const ContainerT<KeyT, ValueT>& container, const Transaction& txn) {
            append(container, txn.handle());
        }
        
        
        /// \brief Appends data from a vector to the database.
        /// \param container Vector with content to be synchronized.
        /// \param txn Active MDBX transaction.
        /// \throws MdbxException if a database error occurs.
        /// \note Upserts source pairs. If the vector contains duplicate keys,
        ///       later elements overwrite earlier ones.
        void append(const std::vector<std::pair<KeyT, ValueT>>& container, MDBX_txn* txn = nullptr) {
            with_transaction([this, &container](MDBX_txn* txn) {
                db_append(container, txn);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Appends data from a vector to the database using a provided transaction.
        /// \param container Vector with content to be synchronized.
        /// \param txn Transaction wrapper.
        /// \throws MdbxException if a database error occurs.
        void append(const std::vector<std::pair<KeyT, ValueT>>& container, const Transaction& txn) {
            append(container, txn.handle());
        }

        /// \brief Reconciles the database with the container.
        /// \tparam ContainerT Container type (e.g., std::map or std::unordered_map).
        /// \param container Container to be reconciled with the database.
        /// \param txn Active MDBX transaction.
        /// \throws MdbxException if a database error occurs.
        /// \note Upserts every source key and removes database keys absent from
        ///       \p container. Matching source keys are written even when their
        ///       value is unchanged.
        template<template <class...> class ContainerT>
        void reconcile(const ContainerT<KeyT, ValueT>& container, MDBX_txn* txn = nullptr) {
            with_transaction([this, &container](MDBX_txn* txn) {
                db_reconcile(container, txn);
            }, TransactionMode::WRITABLE, txn);
        }
        
        /// \brief Reconciles the database with the container.
        /// \tparam ContainerT Container type (e.g., std::map or std::unordered_map).
        /// \param container Container to be reconciled with the database.
        /// \param txn Transaction wrapper used for the operation.
        /// \throws MdbxException if a database error occurs.
        template<template <class...> class ContainerT>
        void reconcile(const ContainerT<KeyT, ValueT>& container, const Transaction& txn) {
            reconcile(container, txn.handle());
        }
        
        /// \brief Reconciles the database with the vector of key-value pairs.
        /// \param container Vector to be reconciled with the database.
        /// \param txn Active MDBX transaction.
        /// \throws MdbxException if a database error occurs.
        /// \note Upserts every source element and removes database keys absent
        ///       from \p container. If the vector contains duplicate keys, later
        ///       elements overwrite earlier ones.
        void reconcile(const std::vector<std::pair<KeyT, ValueT>>& container, MDBX_txn* txn = nullptr) {
            with_transaction([this, &container](MDBX_txn* txn) {
                db_reconcile(container, txn);
            }, TransactionMode::WRITABLE, txn);
        }

        /// \brief Reconciles the database with the vector of key-value pairs using provided transaction.
        /// \param container Vector to be reconciled with the database.
        /// \param txn Transaction wrapper.
        /// \throws MdbxException if a database error occurs.
        void reconcile(const std::vector<std::pair<KeyT, ValueT>>& container, const Transaction& txn) {
            reconcile(container, txn.handle());
        }

        /// \brief Inserts key-value only if key is absent.
        /// \param key The key to be inserted.
        /// \param value The value to be inserted.
        /// \param txn Active MDBX transaction.
        /// \return \c true if the pair was inserted, \c false when the key already exists.
        /// \throws MdbxException if a database error occurs.
        bool insert(const KeyT &key, const ValueT &value, MDBX_txn* txn = nullptr) {
            bool res;
            with_transaction([this, &key, &value, &res](MDBX_txn* txn) {
                res = db_insert_if_absent(key, value, txn);
            }, TransactionMode::WRITABLE, txn);
            return res;
        }
        
        /// \brief Inserts key-value only if key is absent.
        /// \param key The key to be inserted.
        /// \param value The value to be inserted.
        /// \param txn Transaction wrapper used for the insertion.
        /// \return \c true if the pair was inserted, \c false when the key already exists.
        /// \throws MdbxException if a database error occurs.
        bool insert(const KeyT &key, const ValueT &value, const Transaction& txn) {
            return insert(key, value, txn.handle());
        }

        /// \brief Inserts key-value only if key is absent.
        /// \param pair The key-value pair to be inserted.
        /// \param txn Active MDBX transaction.
        /// \return \c true if the pair was inserted, \c false when the key already exists.
        /// \throws MdbxException if a database error occurs.
        bool insert(const std::pair<KeyT, ValueT> &pair, MDBX_txn* txn = nullptr) {
            bool res;
            with_transaction([this, &pair, &res](MDBX_txn* txn) {
                res = db_insert_if_absent(pair.first, pair.second, txn);
            }, TransactionMode::WRITABLE, txn);
            return res;
        }
        
        /// \brief Inserts key-value only if key is absent.
        /// \param pair The key-value pair to be inserted.
        /// \param txn Transaction wrapper used for the operation.
        /// \return \c true if the pair was inserted, \c false when the key already exists.
        /// \throws MdbxException if a database error occurs.
        bool insert(const std::pair<KeyT, ValueT> &pair, const Transaction& txn) {
            return insert(pair, txn.handle());
        }
        
        /// \brief Inserts or replaces key-value pair.
        /// \param key The key to be inserted.
        /// \param value The value to be inserted.
        /// \param txn Active MDBX transaction.
        /// \throws MdbxException if a database error occurs.
        void insert_or_assign(const KeyT &key, const ValueT &value, MDBX_txn* txn = nullptr) {
            with_transaction([this, &key, &value](MDBX_txn* txn) {
                db_insert_or_assign(key, value, txn);
            }, TransactionMode::WRITABLE, txn);
        }
        
        /// \brief Inserts or replaces key-value pair.
        /// \param key The key to be inserted.
        /// \param value The value to be inserted.
        /// \param txn Transaction wrapper used for the operation.
        /// \throws MdbxException if a database error occurs.
        void insert_or_assign(const KeyT &key, const ValueT &value, const Transaction& txn) {
            insert_or_assign(key, value, txn.handle());
        }

        /// \brief Inserts or replaces key-value pair.
        /// \param pair The key-value pair to be inserted.
        /// \param txn Active MDBX transaction.
        /// \throws MdbxException if a database error occurs.
        void insert_or_assign(const std::pair<KeyT, ValueT> &pair, MDBX_txn* txn = nullptr) {
            insert_or_assign(pair.first, pair.second, txn);
        }
        
        /// \brief Inserts or replaces key-value pair.
        /// \param pair The key-value pair to be inserted.
        /// \param txn Transaction wrapper used for the operation.
        /// \throws MdbxException if a database error occurs.
        void insert_or_assign(const std::pair<KeyT, ValueT> &pair, const Transaction& txn) {
            insert_or_assign(pair, txn.handle());
        }
        
        /// \brief Retrieves value by key or throws.
        /// \param key The key to look up.
        /// \param txn Active MDBX transaction.
        /// \return The value associated with the key.
        /// \throws std::out_of_range if key not found.
        /// \throws MdbxException if DB error occurs.
        ValueT at(const KeyT& key, MDBX_txn* txn = nullptr) const {
            ValueT value;
            with_transaction([this, &key, &value](MDBX_txn* txn) {
                if (!db_get(key, value, txn)) {
                    throw std::out_of_range("Key not found in database");
                }
            }, TransactionMode::READ_ONLY, txn);
            return value;
        }
        
        /// \brief Retrieves value by key or throws.
        /// \param key The key to look up.
        /// \param txn Transaction wrapper used for the lookup.
        /// \return The value associated with the key.
        /// \throws std::out_of_range if key not found.
        /// \throws MdbxException if DB error occurs.
        ValueT at(const KeyT& key, const Transaction& txn) const {
            return at(key, txn.handle());
        }
        
        /// \brief Tries to find value by key.
        /// \param key The key to look up.
        /// \param out Reference to output value.
        /// \param txn Active MDBX transaction.
        /// \return True if key exists, false otherwise.
        /// \throws MdbxException if DB error occurs.
        bool try_get(const KeyT& key, ValueT& out, MDBX_txn* txn) const {
            bool res;
            with_transaction([this, &key, &out, &res](MDBX_txn* txn) {
                res = db_get(key, out, txn);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }
        
        /// \brief Tries to find value by key.
        /// \param key The key to look up.
        /// \param out Reference to output value.
        /// \param txn Transaction wrapper used for the lookup.
        /// \return True if key exists, false otherwise.
        /// \throws MdbxException if DB error occurs.
        bool try_get(const KeyT& key, ValueT& out, const Transaction& txn) const {
            return try_get(key, out, txn.handle());
        }

#       if __cplusplus >= 201703L
        /// \brief Finds value by key.
        /// \param key Key to search for.
        /// \param txn Optional active MDBX transaction.
        /// \return std::optional with value if found, std::nullopt otherwise.
        /// \throws MdbxException on DB error.
        /// \note Available when compiling as C++17 or newer.
        std::optional<ValueT> find(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::optional<ValueT> result;
            with_transaction([this, &key, &result](MDBX_txn* txn) {
                ValueT tmp;
                if (db_get(key, tmp, txn)) {
                    result = std::move(tmp);
                }
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds value by key.
        /// \param key Key to search for.
        /// \param txn Transaction wrapper used for the lookup.
        /// \return std::optional with value if found, std::nullopt otherwise.
        /// \throws MdbxException on DB error.
        std::optional<ValueT> find(const KeyT& key, const Transaction& txn) const {
            return find(key, txn.handle());
        }
#       else
        /// \brief Finds value by key.
        /// \param key Key to search for.
        /// \param txn Optional active MDBX transaction.
        /// \return Pair of success flag and value.
        /// \throws MdbxException on DB error.
        std::pair<bool, ValueT> find(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::pair<bool, ValueT> result(false, ValueT());
            with_transaction([this, &key, &result](MDBX_txn* txn) {
                if (db_get(key, result.second, txn)) {
                    result.first = true;
                }
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Finds value by key.
        /// \param key Key to search for.
        /// \param txn Transaction wrapper used for the lookup.
        /// \return Pair of success flag and value.
        /// \throws MdbxException on DB error.
        std::pair<bool, ValueT> find(const KeyT& key, const Transaction& txn) const {
            return find(key, txn.handle());
        }
#       endif

        /// \brief Finds value by key.
        /// \param key Key to search for.
        /// \param txn Optional active MDBX transaction.
        /// \return Pair of success flag and value.
        /// \throws MdbxException on DB error.
        /// \note C++11-compatible lookup form.
        std::pair<bool, ValueT> find_compat(const KeyT& key, MDBX_txn* txn = nullptr) const {
            std::pair<bool, ValueT> result{false, ValueT{}};
            with_transaction([this, &key, &result](MDBX_txn* txn) {
                if (db_get(key, result.second, txn)) {
                    result.first = true;
                }
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }
        
        /// \brief Finds value by key.
        /// \param key Key to search for.
        /// \param txn Transaction wrapper used for the lookup.
        /// \return Pair of success flag and value.
        /// \throws MdbxException on DB error.
        std::pair<bool, ValueT> find_compat(const KeyT& key, const Transaction& txn) const {
            return find_compat(key, txn.handle());
        }

        /// \brief Updates an existing value by calling a mutator function.
        /// \param key Key to update.
        /// \param fn Mutator function invoked as \c fn(ValueT&).
        /// \param txn Optional transaction handle.
        /// \return \c true if the key existed and was updated, \c false if the key was missing.
        /// \throws MdbxException if a database error occurs.
        /// \note If \c txn is \c nullptr and \c fn throws, the internally created
        /// transaction is rolled back. Caller-owned transactions remain caller-managed.
        template<typename Fn>
        bool update(const KeyT& key, Fn fn, MDBX_txn* txn = nullptr) {
            bool result = false;
            with_transaction([this, &key, &fn, &result](MDBX_txn* t) {
                result = db_update(key, fn, t);
            }, TransactionMode::WRITABLE, txn);
            return result;
        }

        /// \brief Updates an existing value by calling a mutator function.
        /// \param key Key to update.
        /// \param fn Mutator function invoked as \c fn(ValueT&).
        /// \param txn Active transaction wrapper.
        /// \return \c true if the key existed and was updated, \c false if the key was missing.
        /// \throws MdbxException if a database error occurs.
        /// \note This overload uses a caller-owned transaction. If \c fn throws,
        /// transaction rollback remains caller-managed.
        template<typename Fn>
        bool update(const KeyT& key, Fn fn, const Transaction& txn) {
            return update(key, fn, txn.handle());
        }

        /// \brief Looks up multiple keys and returns found pairs in a map.
        /// \param keys Vector of keys to search.
        /// \param txn Optional transaction handle.
        /// \return \c std::map with found keys and their values. Missing keys are omitted.
        /// \throws MdbxException if a database error occurs.
        std::map<KeyT, ValueT> find_many(const std::vector<KeyT>& keys,
                                         MDBX_txn* txn = nullptr) const {
            std::map<KeyT, ValueT> result;
            with_transaction([this, &keys, &result](MDBX_txn* t) {
                db_find_many(keys, result, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Looks up multiple keys and returns found pairs in a map.
        /// \param keys Vector of keys to search.
        /// \param txn Active transaction wrapper.
        /// \return \c std::map with found keys and their values. Missing keys are omitted.
        /// \throws MdbxException if a database error occurs.
        std::map<KeyT, ValueT> find_many(const std::vector<KeyT>& keys,
                                         const Transaction& txn) const {
            return find_many(keys, txn.handle());
        }

        /// \brief Looks up multiple keys and returns found pairs in input order.
        /// \param keys Vector of keys to search.
        /// \param txn Optional transaction handle.
        /// \return Vector of found key-value pairs, preserving input key order. Missing keys are omitted.
        /// \throws MdbxException if a database error occurs.
        std::vector<value_type> find_many_vector(const std::vector<KeyT>& keys,
                                                 MDBX_txn* txn = nullptr) const {
            std::vector<value_type> result;
            with_transaction([this, &keys, &result](MDBX_txn* t) {
                db_find_many_vector(keys, result, t);
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        /// \brief Looks up multiple keys and returns found pairs in input order.
        /// \param keys Vector of keys to search.
        /// \param txn Active transaction wrapper.
        /// \return Vector of found key-value pairs, preserving input key order. Missing keys are omitted.
        /// \throws MdbxException if a database error occurs.
        std::vector<value_type> find_many_vector(const std::vector<KeyT>& keys,
                                                 const Transaction& txn) const {
            return find_many_vector(keys, txn.handle());
        }

        /// \brief Checks whether a key exists in the database.
        /// \param key The key to look up.
        /// \param txn Active transaction.
        /// \return True if the key exists, false otherwise.
        /// \throws MdbxException if DB error occurs.
        bool contains(const KeyT& key, MDBX_txn* txn = nullptr) const {
            bool res;
            with_transaction([this, &key, &res](MDBX_txn* txn) {
                res = db_contains(key, txn);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }
        
        /// \brief Checks whether a key exists in the database.
        /// \param key The key to look up.
        /// \param txn Transaction wrapper used for the lookup.
        /// \return True if the key exists, false otherwise.
        /// \throws MdbxException if DB error occurs.
        bool contains(const KeyT& key, const Transaction& txn) const {
            return contains(key, txn.handle());
        }

        /// \brief Returns the number of elements in the database.
        /// \param txn Active transaction.
        /// \return The number of key-value pairs.
        /// \throws MdbxException if DB error occurs.
        std::size_t count(MDBX_txn* txn = nullptr) const {
            std::size_t res = 0;
            with_transaction([this, &res](MDBX_txn* txn) {
                res = db_count(txn);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }
        
        /// \brief Returns the number of elements in the database.
        /// \param txn Transaction wrapper used for the count operation.
        /// \return The number of key-value pairs.
        /// \throws MdbxException if DB error occurs.
        std::size_t count(const Transaction& txn) const {
            return count(txn.handle());
        }

        /// \brief Checks if the database is empty.
        /// \param txn Optional transaction handle.
        /// \return True if the database is empty, false otherwise.
        /// \throws MdbxException if a database error occurs.
        bool empty(MDBX_txn* txn = nullptr) const {
            std::size_t res = 0;
            with_transaction([this, &res](MDBX_txn* txn) {
                res = db_count(txn);
            }, TransactionMode::READ_ONLY, txn);
            return (res == 0);
        }
        
        /// \brief Checks if the database is empty.
        /// \param txn Transaction wrapper used for the check.
        /// \return True if the database is empty, false otherwise.
        /// \throws MdbxException if a database error occurs.
        bool empty(const Transaction& txn) const {
            return empty(txn.handle());
        }

        /// \brief Removes key from DB.
        /// \param key The key of the pair to be removed.
        /// \param txn Active transaction.
        /// \return True if the key was found and deleted, false if the key was not found.
        /// \throws MdbxException if deletion fails.
        bool erase(const KeyT &key, MDBX_txn* txn = nullptr) {
            bool res;
            with_transaction([this, &key, &res](MDBX_txn* txn) {
                res = db_erase(key, txn);
            }, TransactionMode::WRITABLE, txn);
            return res;
        }
        
        /// \brief Removes key from DB.
        /// \param key The key of the pair to be removed.
        /// \param txn Transaction wrapper used for the operation.
        /// \return True if the key was found and deleted, false if the key was not found.
        /// \throws MdbxException if deletion fails.
        bool erase(const KeyT &key, const Transaction& txn) {
            return erase(key, txn.handle());
        }

        /// \brief Clears all key-value pairs from the database.
        /// \param txn Active transaction.
        /// \throws MdbxException if a database error occurs.
        void clear(MDBX_txn* txn = nullptr) {
            with_transaction([this](MDBX_txn* txn) {
                db_clear(txn);
            }, TransactionMode::WRITABLE, txn);
        }
        
        /// \brief Clears all key-value pairs from the database.
        /// \param txn Transaction wrapper used for the operation.
        /// \throws MdbxException if a database error occurs.
        void clear(const Transaction& txn) {
            clear(txn.handle());
        }

    private:
    
        /// \brief Loads data from the database into the container.
        /// \tparam ContainerT Template for the container type.
        /// \param container Container to be synchronized with database content.
        /// \param txn_handle Active transaction handle.
        /// \throws MdbxException if a database error occurs.
        template<template <class...> class ContainerT>
        void db_load(ContainerT<KeyT, ValueT>& container, MDBX_txn* txn_handle) {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn_handle, m_dbi, cursor.out()), "Failed to open MDBX cursor");

            MDBX_val db_key, db_val;
            int rc = MDBX_SUCCESS;
            while ((rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT)) == MDBX_SUCCESS) {
                KeyT key = deserialize_key<KeyT>(db_key);
                ValueT value = deserialize_value<ValueT>(db_val);
                container.emplace(std::move(key), std::move(value));
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to load key-value table");
            }
        }
        
        /// \brief Loads all key-value pairs into a std::vector of pairs.
        /// \param out_vector The output vector to fill.
        /// \param txn Optional transaction.
        /// \throws MdbxException if a database error occurs.
        void db_load(std::vector<std::pair<KeyT, ValueT>>& out_vector, MDBX_txn* txn) {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");

            MDBX_val db_key, db_val;
            int rc = MDBX_SUCCESS;
            while ((rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT)) == MDBX_SUCCESS) {
                KeyT key = deserialize_key<KeyT>(db_key);
                ValueT value = deserialize_value<ValueT>(db_val);
                out_vector.emplace_back(std::move(key), std::move(value));
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to load key-value table");
            }
        }

        template<class ContainerT>
        void db_range(const KeyT& from_key, const KeyT& to_key,
                      ContainerT& pairs, MDBX_txn* txn) const {
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
                KeyT key = deserialize_key<KeyT>(db_key);
                ValueT value = deserialize_value<ValueT>(db_val);
                pairs.insert(pairs.end(), value_type(std::move(key), std::move(value)));
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
            }
            if (!stopped_by_upper_bound && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to read key-value range");
            }
        }

        template<class ContainerT>
        void db_range_values(const KeyT& from_key, const KeyT& to_key,
                             ContainerT& values, MDBX_txn* txn) const {
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
                values.insert(values.end(), deserialize_value<ValueT>(db_val));
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
            }
            if (!stopped_by_upper_bound && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to read key-value range values");
            }
        }

        /// \brief Gets a value by key from the database.
        /// \param key The key to search for.
        /// \param value Reference to store the retrieved value.
        /// \param txn_handle The active transaction.
        /// \return True if found, false otherwise.
        /// \throws MdbxException if a database error occurs.
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
                KeyT key = deserialize_key<KeyT>(db_key);
                ValueT value = deserialize_value<ValueT>(db_val);
                if (!callback(key, value)) {
                    return false;
                }
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
            }
            if (!stopped_by_upper_bound && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to iterate key-value range");
            }
            return true;
        }

#       if __cplusplus >= 201703L
        std::optional<value_type> db_lower_bound(const KeyT& key, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_SUCCESS) {
                return value_type(deserialize_key<KeyT>(db_key), deserialize_value<ValueT>(db_val));
            }

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            if (rc == MDBX_SUCCESS) {
                return value_type(deserialize_key<KeyT>(db_key), deserialize_value<ValueT>(db_val));
            }
            if (rc == MDBX_NOTFOUND) {
                return std::nullopt;
            }
            check_mdbx(rc, "Failed to seek lower bound");
            return std::nullopt;
        }

        std::optional<value_type> db_upper_bound(const KeyT& key, MDBX_txn* txn) const {
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

            if (mdbx_cmp(txn, m_dbi, &db_key, &db_key_exact) == 0) {
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
                if (rc == MDBX_NOTFOUND) {
                    return std::nullopt;
                }
                check_mdbx(rc, "Failed to seek next key for upper bound");
            }
            return value_type(deserialize_key<KeyT>(db_key), deserialize_value<ValueT>(db_val));
        }

        std::optional<value_type> db_first(MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_FIRST);
            if (rc == MDBX_NOTFOUND) {
                return std::nullopt;
            }
            check_mdbx(rc, "Failed to seek first pair");
            return value_type(deserialize_key<KeyT>(db_key), deserialize_value<ValueT>(db_val));
        }

        std::optional<value_type> db_last(MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_LAST);
            if (rc == MDBX_NOTFOUND) {
                return std::nullopt;
            }
            check_mdbx(rc, "Failed to seek last pair");
            return value_type(deserialize_key<KeyT>(db_key), deserialize_value<ValueT>(db_val));
        }

        std::optional<KeyT> db_min_key(MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_FIRST);
            if (rc == MDBX_NOTFOUND) {
                return std::nullopt;
            }
            check_mdbx(rc, "Failed to seek minimum key");
            return deserialize_key<KeyT>(db_key);
        }

        std::optional<KeyT> db_max_key(MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_LAST);
            if (rc == MDBX_NOTFOUND) {
                return std::nullopt;
            }
            check_mdbx(rc, "Failed to seek maximum key");
            return deserialize_key<KeyT>(db_key);
        }
#       else
        std::pair<bool, value_type> db_lower_bound_compat(const KeyT& key, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_SUCCESS) {
                return std::make_pair(true, value_type(deserialize_key<KeyT>(db_key), deserialize_value<ValueT>(db_val)));
            }

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            if (rc == MDBX_SUCCESS) {
                return std::make_pair(true, value_type(deserialize_key<KeyT>(db_key), deserialize_value<ValueT>(db_val)));
            }
            if (rc == MDBX_NOTFOUND) {
                return std::make_pair(false, value_type(KeyT(), ValueT()));
            }
            check_mdbx(rc, "Failed to seek lower bound");
            return std::make_pair(false, value_type(KeyT(), ValueT()));
        }

        std::pair<bool, value_type> db_upper_bound_compat(const KeyT& key, MDBX_txn* txn) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_key_exact = db_key;
            MDBX_val db_val;

            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_SET_RANGE);
            if (rc == MDBX_NOTFOUND) {
                return std::make_pair(false, value_type(KeyT(), ValueT()));
            }
            check_mdbx(rc, "Failed to seek upper bound");

            if (mdbx_cmp(txn, m_dbi, &db_key, &db_key_exact) == 0) {
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
                if (rc == MDBX_NOTFOUND) {
                    return std::make_pair(false, value_type(KeyT(), ValueT()));
                }
                check_mdbx(rc, "Failed to seek next key for upper bound");
            }
            return std::make_pair(true, value_type(deserialize_key<KeyT>(db_key), deserialize_value<ValueT>(db_val)));
        }

        std::pair<bool, value_type> db_first_compat(MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_FIRST);
            if (rc == MDBX_NOTFOUND) {
                return std::make_pair(false, value_type(KeyT(), ValueT()));
            }
            check_mdbx(rc, "Failed to seek first pair");
            return std::make_pair(true, value_type(deserialize_key<KeyT>(db_key), deserialize_value<ValueT>(db_val)));
        }

        std::pair<bool, value_type> db_last_compat(MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_LAST);
            if (rc == MDBX_NOTFOUND) {
                return std::make_pair(false, value_type(KeyT(), ValueT()));
            }
            check_mdbx(rc, "Failed to seek last pair");
            return std::make_pair(true, value_type(deserialize_key<KeyT>(db_key), deserialize_value<ValueT>(db_val)));
        }

        std::pair<bool, KeyT> db_min_key_compat(MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_FIRST);
            if (rc == MDBX_NOTFOUND) {
                return std::make_pair(false, KeyT());
            }
            check_mdbx(rc, "Failed to seek minimum key");
            return std::make_pair(true, deserialize_key<KeyT>(db_key));
        }

        std::pair<bool, KeyT> db_max_key_compat(MDBX_txn* txn) const {
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn, m_dbi, cursor.out()), "Failed to open MDBX cursor");
            MDBX_val db_key, db_val;
            int rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_LAST);
            if (rc == MDBX_NOTFOUND) {
                return std::make_pair(false, KeyT());
            }
            check_mdbx(rc, "Failed to seek maximum key");
            return std::make_pair(true, deserialize_key<KeyT>(db_key));
        }
#       endif

        void db_range_reverse(const KeyT& from_key, const KeyT& to_key,
                              std::vector<value_type>& out, MDBX_txn* txn) const {
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

            while (rc == MDBX_SUCCESS) {
                if (mdbx_cmp(txn, m_dbi, &db_key, &db_from_key) < 0) {
                    break;
                }
                out.push_back(value_type(deserialize_key<KeyT>(db_key), deserialize_value<ValueT>(db_val)));
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_PREV);
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to iterate reverse key-value range");
            }
        }

        void db_range_reverse(const KeyT& from_key, const KeyT& to_key,
                              std::size_t limit, std::vector<value_type>& out, MDBX_txn* txn) const {
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
                out.push_back(value_type(deserialize_key<KeyT>(db_key), deserialize_value<ValueT>(db_val)));
                if (out.size() >= limit) {
                    stopped_by_limit = true;
                    break;
                }
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_PREV);
            }
            if (!stopped_by_limit && !stopped_by_lower_bound && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to iterate reverse key-value range");
            }
        }

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
                check_mdbx(rc, "Failed to count key-value range");
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
                check_mdbx(mdbx_cursor_del(cursor.get(), MDBX_CURRENT), "Failed to erase pair in range");
#               if MDBXC_SYNC_ENABLED
                record_op(txn, sync::ChangeOpType::Delete, kbytes, {});
#               endif
                ++removed;
                rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT);
            }
            if (!stopped_by_upper_bound && rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to erase key-value range");
            }
            return removed;
        }

        bool db_get(const KeyT& key, ValueT& value, MDBX_txn* txn_handle) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn_handle, m_dbi, &db_key, &db_val);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to retrieve value");
            value = deserialize_value<ValueT>(db_val);
            return true;
        }

        /// \brief Checks whether a key exists in the database.
        /// \param key The key to check.
        /// \param txn_handle The active transaction.
        /// \return True if key exists, false otherwise.
        /// \throws MdbxException if a database error occurs.
        bool db_contains(const KeyT& key, MDBX_txn* txn_handle) const {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val; // dummy
            int rc = mdbx_get(txn_handle, m_dbi, &db_key, &db_val);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to check key presence");
            return true;
        }

        /// \brief Returns the number of elements in the database.
        /// \param txn_handle Active transaction handle.
        /// \return The number of key-value pairs in the database.
        /// \throws MdbxException if a database error occurs.
        std::size_t db_count(MDBX_txn* txn_handle) const {
            MDBX_stat stat;
            check_mdbx(mdbx_dbi_stat(txn_handle, m_dbi, &stat, sizeof(stat)), "Failed to query database statistics");
            return stat.ms_entries;
        }

        /// \brief Appends the content of the container to the database.
        /// \tparam ContainerT Template for the container type (map or unordered_map).
        /// \param container Container with content to append.
        /// \param txn_handle Active transaction handle.
        /// \throws MdbxException if a database error occurs.
        template<template <class...> class ContainerT>
        void db_append(const ContainerT<KeyT, ValueT>& container, MDBX_txn* txn_handle) {
            SerializeScratch sc_key;
            SerializeScratch sc_value;
            
            for (const auto& pair : container) {
                MDBX_val db_key = serialize_key<Options::safe_integer_key>(pair.first, sc_key);
                MDBX_val db_val = serialize_value(pair.second, sc_value);
                check_mdbx(
                    mdbx_put(txn_handle, m_dbi, &db_key, &db_val, MDBX_UPSERT),
                    "Failed to write record"
                );
#               if MDBXC_SYNC_ENABLED
                record_op(txn_handle, sync::ChangeOpType::Put,
                          capture_bytes(db_key), capture_bytes(db_val));
#               endif
            }
        }
        
        /// \brief Appends the content of the vector to the database.
        /// \param container Vector with key-value pairs to append.
        /// \param txn_handle Active transaction handle.
        /// \throws MdbxException if a database error occurs.
        void db_append(const std::vector<std::pair<KeyT, ValueT>>& container, MDBX_txn* txn_handle) {
            SerializeScratch sc_key;
            SerializeScratch sc_value;
            
#           if __cplusplus >= 201703L
            for (const auto& [key, value] : container) {
                MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
                MDBX_val db_val = serialize_value(value, sc_value);
                check_mdbx(
                    mdbx_put(txn_handle, m_dbi, &db_key, &db_val, MDBX_UPSERT),
                    "Failed to write record"
                );
#               if MDBXC_SYNC_ENABLED
                record_op(txn_handle, sync::ChangeOpType::Put,
                          capture_bytes(db_key), capture_bytes(db_val));
#               endif
            }
#           else
            for (typename std::vector<std::pair<KeyT, ValueT> >::const_iterator it = container.begin();
                 it != container.end(); ++it) {
                const KeyT& key = it->first;
                const ValueT& value = it->second;
                MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
                MDBX_val db_val = serialize_value(value, sc_value);
                check_mdbx(
                    mdbx_put(txn_handle, m_dbi, &db_key, &db_val, MDBX_UPSERT),
                    "Failed to write record"
                );
#               if MDBXC_SYNC_ENABLED
                record_op(txn_handle, sync::ChangeOpType::Put,
                          capture_bytes(db_key), capture_bytes(db_val));
#               endif
            }
#           endif
        }

        /// \brief Reconciles the content of the database with the container.
        /// Synchronizes the main table with the content of the container by using a temporary table.
        /// Clears old data, inserts new data, and updates existing records in the main table.
        /// \tparam ContainerT Template for the container type (map or unordered_map).
        /// \param container Container with key-value pairs to be reconciled with the database.
        /// \param txn_handle Active MDBX transaction.
        /// \throws MdbxException if a database error occurs.
        template<template <class...> class ContainerT>
        void db_reconcile(const ContainerT<KeyT, ValueT>& container, MDBX_txn* txn_handle) {
            SerializeScratch sc_key;
            SerializeScratch sc_value;
            
            // 1. Collect all keys from the container
            std::unordered_set<KeyT> new_keys;
            for (const auto& pair : container) {
                new_keys.insert(pair.first);
                MDBX_val db_key = serialize_key<Options::safe_integer_key>(pair.first, sc_key);
                MDBX_val db_val = serialize_value(pair.second, sc_value);
                check_mdbx(
                    mdbx_put(txn_handle, m_dbi, &db_key, &db_val, MDBX_UPSERT),
                    "Failed to write record"
                );
#               if MDBXC_SYNC_ENABLED
                record_op(txn_handle, sync::ChangeOpType::Put,
                          capture_bytes(db_key), capture_bytes(db_val));
#               endif
            }

            // 2. Iterate over existing keys in the DB and remove the extras
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn_handle, m_dbi, cursor.out()), "Failed to open MDBX cursor");

            MDBX_val db_key, db_val;
            int rc = MDBX_SUCCESS;
            while ((rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT)) == MDBX_SUCCESS) {
                KeyT key = deserialize_key<KeyT>(db_key);
                if (new_keys.find(key) == new_keys.end()) {
#                   if MDBXC_SYNC_ENABLED
                    const std::vector<std::uint8_t> kbytes = capture_bytes(db_key);
#                   endif
                    check_mdbx(mdbx_cursor_del(cursor.get(), MDBX_CURRENT), "Failed to delete record using cursor");
#                   if MDBXC_SYNC_ENABLED
                    record_op(txn_handle, sync::ChangeOpType::Delete, kbytes, {});
#                   endif
                }
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to reconcile key-value table");
            }
        }
        
        /// \brief Reconciles the content of the database with the given vector of key-value pairs.
        /// Clears existing records not present in the vector, and upserts all from the vector.
        /// \param container Vector of key-value pairs to reconcile with the database.
        /// \param txn_handle Active MDBX transaction.
        /// \throws MdbxException if a database error occurs.
        void db_reconcile(const std::vector<std::pair<KeyT, ValueT>>& container, MDBX_txn* txn_handle) {
            SerializeScratch sc_key;
            SerializeScratch sc_value;
            
            // 1. Collect keys and upsert values
            std::unordered_set<KeyT> new_keys;
            for (size_t i = 0; i < container.size(); ++i) {
                const KeyT& key = container[i].first;
                const ValueT& value = container[i].second;

                new_keys.insert(key);

                MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
                MDBX_val db_val = serialize_value(value, sc_value);
                check_mdbx(
                    mdbx_put(txn_handle, m_dbi, &db_key, &db_val, MDBX_UPSERT),
                    "Failed to write record"
                );
#               if MDBXC_SYNC_ENABLED
                record_op(txn_handle, sync::ChangeOpType::Put,
                          capture_bytes(db_key), capture_bytes(db_val));
#               endif
            }

            // 2. Delete stale keys from DB
            CursorGuard cursor;
            check_mdbx(mdbx_cursor_open(txn_handle, m_dbi, cursor.out()), "Failed to open MDBX cursor");

            MDBX_val db_key, db_val;
            int rc = MDBX_SUCCESS;
            while ((rc = mdbx_cursor_get(cursor.get(), &db_key, &db_val, MDBX_NEXT)) == MDBX_SUCCESS) {
                KeyT key = deserialize_key<KeyT>(db_key);
                if (new_keys.find(key) == new_keys.end()) {
#                   if MDBXC_SYNC_ENABLED
                    const std::vector<std::uint8_t> kbytes = capture_bytes(db_key);
#                   endif
                    check_mdbx(mdbx_cursor_del(cursor.get(), MDBX_CURRENT), "Failed to delete record using cursor");
#                   if MDBXC_SYNC_ENABLED
                    record_op(txn_handle, sync::ChangeOpType::Delete, kbytes, {});
#                   endif
                }
            }
            if (rc != MDBX_NOTFOUND) {
                check_mdbx(rc, "Failed to reconcile key-value table");
            }
        }
        
        /// \brief Inserts a key-value pair only if the key does not already exist.
        /// \param key The key to insert.
        /// \param value The value to insert.
        /// \param txn_handle The active MDBX transaction.
        /// \return true if the key-value pair was inserted, false if the key already existed.
        /// \throws MdbxException if the insert fails for reasons other than key existence.
        bool db_insert_if_absent(const KeyT& key, const ValueT& value, MDBX_txn* txn_handle) {
            SerializeScratch sc_key;
            SerializeScratch sc_value;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val = serialize_value(value, sc_value);
            int rc = mdbx_put(txn_handle, m_dbi, &db_key, &db_val, MDBX_NOOVERWRITE);

            if (rc == MDBX_SUCCESS) {
#               if MDBXC_SYNC_ENABLED
                record_op(txn_handle, sync::ChangeOpType::Put,
                          capture_bytes(db_key), capture_bytes(db_val));
#               endif
                return true;
            }
            if (rc == MDBX_KEYEXIST)
                return false;

            check_mdbx(rc, "Failed to insert key-value pair");
            return false;
        }

        /// \brief Inserts or replaces the key-value pair.
        /// \param key The key to insert or replace.
        /// \param value The value to set.
        /// \param txn_handle The active MDBX transaction.
        /// \throws MdbxException if the operation fails.
        void db_insert_or_assign(const KeyT& key, const ValueT& value, MDBX_txn* txn_handle) {
            SerializeScratch sc_key;
            SerializeScratch sc_value;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val = serialize_value(value, sc_value);
            check_mdbx(
                mdbx_put(txn_handle, m_dbi, &db_key, &db_val, MDBX_UPSERT),  // or 0
                "Failed to insert or assign key-value pair"
            );
#           if MDBXC_SYNC_ENABLED
            record_op(txn_handle, sync::ChangeOpType::Put,
                      capture_bytes(db_key), capture_bytes(db_val));
#           endif
        }

        template<typename Fn>
        bool db_update(const KeyT& key, Fn& fn, MDBX_txn* txn) {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            MDBX_val db_val;
            int rc = mdbx_get(txn, m_dbi, &db_key, &db_val);
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to read value for update");
            ValueT value = deserialize_value<ValueT>(db_val);
            fn(value);
            db_insert_or_assign(key, value, txn);
            return true;
        }

        void db_find_many(const std::vector<KeyT>& keys,
                          std::map<KeyT, ValueT>& out, MDBX_txn* txn) const {
            for (typename std::vector<KeyT>::const_iterator it = keys.begin(); it != keys.end(); ++it) {
                ValueT value;
                if (db_get(*it, value, txn)) {
                    out.emplace(*it, std::move(value));
                }
            }
        }

        void db_find_many_vector(const std::vector<KeyT>& keys,
                                 std::vector<value_type>& out, MDBX_txn* txn) const {
            for (typename std::vector<KeyT>::const_iterator it = keys.begin(); it != keys.end(); ++it) {
                ValueT value;
                if (db_get(*it, value, txn)) {
                    out.push_back(value_type(*it, std::move(value)));
                }
            }
        }

        /// \brief Removes a key from the database.
        /// \param key The key of the pair to be removed.
        /// \param txn_handle The active MDBX transaction.
        /// \return True if the key was found and deleted, false if the key was not found.
        /// \throws MdbxException if deletion fails for other reasons.
        bool db_erase(const KeyT& key, MDBX_txn* txn_handle) {
            SerializeScratch sc_key;
            MDBX_val db_key = serialize_key<Options::safe_integer_key>(key, sc_key);
            const int rc = mdbx_del(txn_handle, m_dbi, &db_key, nullptr);
            if (rc == MDBX_SUCCESS) {
#               if MDBXC_SYNC_ENABLED
                record_op(txn_handle, sync::ChangeOpType::Delete,
                          capture_bytes(db_key), {});
#               endif
                return true;
            }
            if (rc == MDBX_NOTFOUND) return false;
            check_mdbx(rc, "Failed to erase key");
            return false;
        }

        /// \brief Clears all key-value pairs from the database.
        /// \throws MdbxException if an MDBX error occurs.
        void db_clear(MDBX_txn* txn_handle) {
            check_mdbx(mdbx_drop(txn_handle, m_dbi, 0), "Failed to clear table");
#           if MDBXC_SYNC_ENABLED
            record_op(txn_handle, sync::ChangeOpType::ClearTable, {}, {});
#           endif
        }

    }; // KeyValueTable

}; // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_KEY_VALUE_TABLE_HPP_INCLUDED
