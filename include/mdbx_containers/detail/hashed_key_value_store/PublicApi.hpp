#pragma once
#ifndef MDBX_CONTAINERS_HEADER_DETAIL_HASHED_KEY_VALUE_STORE_PUBLIC_API_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_DETAIL_HASHED_KEY_VALUE_STORE_PUBLIC_API_HPP_INCLUDED

/// \file detail/hashed_key_value_store/PublicApi.hpp
/// \brief Internal CRTP implementation of HashedKeyValueStore public methods.

#include "AssignmentProxy.hpp"

namespace mdbxc {
namespace detail {

    template<class Derived, class KeyT, class ValueT>
    class HashedKeyValueStorePublicApi {
    public:
        typedef std::pair<KeyT, ValueT> value_type;
        typedef HashedKeyValueStoreAssignmentProxy<Derived, KeyT, ValueT>
            AssignmentProxy;

        template<template<class...> class ContainerT>
        Derived& operator=(const ContainerT<KeyT, ValueT>& container) {
            derived().reconcile(container);
            return derived();
        }

        Derived& operator=(const std::vector<value_type>& container) {
            derived().reconcile(container);
            return derived();
        }

        template<template<class...> class ContainerT = std::map>
        typename key_value_result_container<ContainerT, KeyT, ValueT>::type
        operator()() const {
            return derived().template retrieve_all<ContainerT>();
        }

        AssignmentProxy operator[](const KeyT& key) {
            return AssignmentProxy(derived(), key);
        }

        template<template<class...> class ContainerT>
        void load(ContainerT<KeyT, ValueT>& container,
                  MDBX_txn* txn = nullptr) const {
            const Derived& self = derived();
            self.with_transaction([&self, &container](MDBX_txn* t) {
                self.db_load(container, t);
            }, TransactionMode::READ_ONLY, txn);
        }

        template<template<class...> class ContainerT>
        void load(ContainerT<KeyT, ValueT>& container,
                  const Transaction& txn) const {
            load(container, txn.handle());
        }

        void load(std::vector<value_type>& container,
                  MDBX_txn* txn = nullptr) const {
            const Derived& self = derived();
            self.with_transaction([&self, &container](MDBX_txn* t) {
                self.db_load(container, t);
            }, TransactionMode::READ_ONLY, txn);
        }

        void load(std::vector<value_type>& container,
                  const Transaction& txn) const {
            load(container, txn.handle());
        }

        template<template<class...> class ContainerT = std::map>
        typename key_value_result_container<ContainerT, KeyT, ValueT>::type
        retrieve_all(MDBX_txn* txn = nullptr) const {
            typename key_value_result_container<ContainerT, KeyT, ValueT>::type
                container;
            load(container, txn);
            return container;
        }

        template<template<class...> class ContainerT = std::map>
        typename key_value_result_container<ContainerT, KeyT, ValueT>::type
        retrieve_all(const Transaction& txn) const {
            return retrieve_all<ContainerT>(txn.handle());
        }

        template<template<class...> class ContainerT>
        void append(const ContainerT<KeyT, ValueT>& container,
                    MDBX_txn* txn = nullptr) {
            Derived& self = derived();
            self.with_transaction([&self, &container](MDBX_txn* t) {
                self.db_append(container, t);
            }, TransactionMode::WRITABLE, txn);
        }

        template<template<class...> class ContainerT>
        void append(const ContainerT<KeyT, ValueT>& container,
                    const Transaction& txn) {
            append(container, txn.handle());
        }

        void append(const std::vector<value_type>& container,
                    MDBX_txn* txn = nullptr) {
            Derived& self = derived();
            self.with_transaction([&self, &container](MDBX_txn* t) {
                self.db_append(container, t);
            }, TransactionMode::WRITABLE, txn);
        }

        void append(const std::vector<value_type>& container,
                    const Transaction& txn) {
            append(container, txn.handle());
        }

        template<template<class...> class ContainerT>
        void reconcile(const ContainerT<KeyT, ValueT>& container,
                       MDBX_txn* txn = nullptr) {
            Derived& self = derived();
            self.with_transaction([&self, &container](MDBX_txn* t) {
                self.db_reconcile(container, t);
            }, TransactionMode::WRITABLE, txn);
        }

        template<template<class...> class ContainerT>
        void reconcile(const ContainerT<KeyT, ValueT>& container,
                       const Transaction& txn) {
            reconcile(container, txn.handle());
        }

        void reconcile(const std::vector<value_type>& container,
                       MDBX_txn* txn = nullptr) {
            Derived& self = derived();
            self.with_transaction([&self, &container](MDBX_txn* t) {
                self.db_reconcile(container, t);
            }, TransactionMode::WRITABLE, txn);
        }

        void reconcile(const std::vector<value_type>& container,
                       const Transaction& txn) {
            reconcile(container, txn.handle());
        }

        bool insert(const KeyT& key, const ValueT& value,
                    MDBX_txn* txn = nullptr) {
            bool res = false;
            Derived& self = derived();
            self.with_transaction([&self, &key, &value, &res](MDBX_txn* t) {
                res = self.db_insert_if_absent(key, value, t);
            }, TransactionMode::WRITABLE, txn);
            return res;
        }

        bool insert(const KeyT& key, const ValueT& value,
                    const Transaction& txn) {
            return insert(key, value, txn.handle());
        }

        bool insert(const value_type& pair, MDBX_txn* txn = nullptr) {
            return insert(pair.first, pair.second, txn);
        }

        bool insert(const value_type& pair, const Transaction& txn) {
            return insert(pair, txn.handle());
        }

        void insert_or_assign(const KeyT& key, const ValueT& value,
                              MDBX_txn* txn = nullptr) {
            Derived& self = derived();
            self.with_transaction([&self, &key, &value](MDBX_txn* t) {
                self.db_insert_or_assign(key, value, t);
            }, TransactionMode::WRITABLE, txn);
        }

        void insert_or_assign(const KeyT& key, const ValueT& value,
                              const Transaction& txn) {
            insert_or_assign(key, value, txn.handle());
        }

        void insert_or_assign(const value_type& pair,
                              MDBX_txn* txn = nullptr) {
            insert_or_assign(pair.first, pair.second, txn);
        }

        void insert_or_assign(const value_type& pair,
                              const Transaction& txn) {
            insert_or_assign(pair, txn.handle());
        }

        ValueT at(const KeyT& key, MDBX_txn* txn = nullptr) const {
            ValueT value;
            const Derived& self = derived();
            self.with_transaction([&self, &key, &value](MDBX_txn* t) {
                if (!self.db_get(key, value, t)) {
                    throw std::out_of_range("Key not found in hashed key-value store");
                }
            }, TransactionMode::READ_ONLY, txn);
            return value;
        }

        ValueT at(const KeyT& key, const Transaction& txn) const {
            return at(key, txn.handle());
        }

        bool try_get(const KeyT& key, ValueT& out,
                     MDBX_txn* txn = nullptr) const {
            bool res = false;
            const Derived& self = derived();
            self.with_transaction([&self, &key, &out, &res](MDBX_txn* t) {
                res = self.db_get(key, out, t);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }

        bool try_get(const KeyT& key, ValueT& out,
                     const Transaction& txn) const {
            return try_get(key, out, txn.handle());
        }

#if __cplusplus >= 201703L
        std::optional<ValueT> find(const KeyT& key,
                                   MDBX_txn* txn = nullptr) const {
            std::optional<ValueT> result;
            const Derived& self = derived();
            self.with_transaction([&self, &key, &result](MDBX_txn* t) {
                ValueT value;
                if (self.db_get(key, value, t)) {
                    result = std::move(value);
                }
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        std::optional<ValueT> find(const KeyT& key,
                                   const Transaction& txn) const {
            return find(key, txn.handle());
        }
#else
        std::pair<bool, ValueT> find(const KeyT& key,
                                     MDBX_txn* txn = nullptr) const {
            return find_compat(key, txn);
        }

        std::pair<bool, ValueT> find(const KeyT& key,
                                     const Transaction& txn) const {
            return find_compat(key, txn.handle());
        }
#endif

        std::pair<bool, ValueT> find_compat(const KeyT& key,
                                            MDBX_txn* txn = nullptr) const {
            std::pair<bool, ValueT> result(false, ValueT());
            const Derived& self = derived();
            self.with_transaction([&self, &key, &result](MDBX_txn* t) {
                if (self.db_get(key, result.second, t)) {
                    result.first = true;
                }
            }, TransactionMode::READ_ONLY, txn);
            return result;
        }

        std::pair<bool, ValueT> find_compat(const KeyT& key,
                                            const Transaction& txn) const {
            return find_compat(key, txn.handle());
        }

        bool contains(const KeyT& key, MDBX_txn* txn = nullptr) const {
            bool res = false;
            const Derived& self = derived();
            self.with_transaction([&self, &key, &res](MDBX_txn* t) {
                res = self.db_contains(key, t);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }

        bool contains(const KeyT& key, const Transaction& txn) const {
            return contains(key, txn.handle());
        }

        std::size_t count(MDBX_txn* txn = nullptr) const {
            std::size_t res = 0;
            const Derived& self = derived();
            self.with_transaction([&self, &res](MDBX_txn* t) {
                res = self.db_count(t);
            }, TransactionMode::READ_ONLY, txn);
            return res;
        }

        std::size_t count(const Transaction& txn) const {
            return count(txn.handle());
        }

        bool empty(MDBX_txn* txn = nullptr) const {
            return count(txn) == 0;
        }

        bool empty(const Transaction& txn) const {
            return empty(txn.handle());
        }

        bool erase(const KeyT& key, MDBX_txn* txn = nullptr) {
            bool res = false;
            Derived& self = derived();
            self.with_transaction([&self, &key, &res](MDBX_txn* t) {
                res = self.db_erase(key, t);
            }, TransactionMode::WRITABLE, txn);
            return res;
        }

        bool erase(const KeyT& key, const Transaction& txn) {
            return erase(key, txn.handle());
        }

        void clear(MDBX_txn* txn = nullptr) {
            Derived& self = derived();
            self.with_transaction([&self](MDBX_txn* t) {
                self.db_clear(t);
            }, TransactionMode::WRITABLE, txn);
        }

        void clear(const Transaction& txn) {
            clear(txn.handle());
        }

    protected:
        Derived& derived() {
            return static_cast<Derived&>(*this);
        }

        const Derived& derived() const {
            return static_cast<const Derived&>(*this);
        }
    };

} // namespace detail
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_DETAIL_HASHED_KEY_VALUE_STORE_PUBLIC_API_HPP_INCLUDED
