#pragma once
#ifndef MDBX_CONTAINERS_HEADER_DETAIL_RESULT_CONTAINERS_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_DETAIL_RESULT_CONTAINERS_HPP_INCLUDED

/// \file detail/result_containers.hpp
/// \brief Internal traits for selecting result container shapes.

#include <utility>
#include <vector>

namespace mdbxc {
namespace detail {

    template<template<class...> class ContainerT, class KeyT, class ValueT>
    struct key_value_result_container {
        typedef ContainerT<KeyT, ValueT> type;
    };

    template<class KeyT, class ValueT>
    struct key_value_result_container<std::vector, KeyT, ValueT> {
        typedef std::vector<std::pair<KeyT, ValueT> > type;
    };

} // namespace detail
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_DETAIL_RESULT_CONTAINERS_HPP_INCLUDED
