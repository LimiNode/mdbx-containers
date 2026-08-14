#pragma once
#ifndef MDBX_CONTAINERS_HEADER_TABLES_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_TABLES_HPP_INCLUDED

/// \file tables.hpp
/// \brief Includes the table wrappers only.
/// \details
/// Pulls in every table wrapper (KeyValue, Key, Value, Sequence,
/// TableSequence, HashedKeyValue, KeyMultiValue, KeyOrderedMultiValue, AnyValue, and
/// hashing utilities) but
/// NOT the sync or vector subsystems. Use when the project only needs the
/// table API.

#include "common.hpp"
#include "mdbx_containers/CompositeKey.hpp"
#include "mdbx_containers/AnyValueTable.hpp"
#include "mdbx_containers/HashedKeyValueStore.hpp"
#include "mdbx_containers/KeyMultiValueTable.hpp"
#include "mdbx_containers/KeyOrderedMultiValueTable.hpp"
#include "mdbx_containers/KeyTable.hpp"
#include "mdbx_containers/KeyValueTable.hpp"
#include "mdbx_containers/SequenceTable.hpp"
#include "mdbx_containers/TableSequence.hpp"
#include "mdbx_containers/ValueTable.hpp"

#endif // MDBX_CONTAINERS_HEADER_TABLES_HPP_INCLUDED
