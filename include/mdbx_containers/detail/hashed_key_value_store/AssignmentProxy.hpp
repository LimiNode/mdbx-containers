#pragma once
#ifndef MDBX_CONTAINERS_HEADER_DETAIL_HASHED_KEY_VALUE_STORE_ASSIGNMENT_PROXY_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_DETAIL_HASHED_KEY_VALUE_STORE_ASSIGNMENT_PROXY_HPP_INCLUDED

/// \file detail/hashed_key_value_store/AssignmentProxy.hpp
/// \brief Internal proxy used by HashedKeyValueStore::operator[].

#include <utility>

#if __cplusplus >= 201703L
#include <optional>
#endif

namespace mdbxc {
namespace detail {

    template<class Derived, class KeyT, class ValueT>
    class HashedKeyValueStoreAssignmentProxy {
    public:
        HashedKeyValueStoreAssignmentProxy(Derived& store, KeyT key)
            : m_store(store), m_key(std::move(key)) {}

        HashedKeyValueStoreAssignmentProxy& operator=(const ValueT& value) {
            m_store.insert_or_assign(m_key, value);
            return *this;
        }

        operator ValueT() const {
#if __cplusplus >= 201703L
            std::optional<ValueT> found = m_store.find(m_key);
            if (found) return *found;
#else
            std::pair<bool, ValueT> found = m_store.find_compat(m_key);
            if (found.first) return found.second;
#endif
            ValueT value{};
            m_store.insert_or_assign(m_key, value);
            return value;
        }

    private:
        Derived& m_store;
        KeyT m_key;
    };

} // namespace detail
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_DETAIL_HASHED_KEY_VALUE_STORE_ASSIGNMENT_PROXY_HPP_INCLUDED
