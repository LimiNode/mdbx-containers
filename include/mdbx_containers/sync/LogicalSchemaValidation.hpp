#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_SCHEMA_VALIDATION_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_SCHEMA_VALIDATION_HPP_INCLUDED

/// \file LogicalSchemaValidation.hpp
/// \brief Persistent marker validation helpers for logical sync adapters.

#include <algorithm>
#include <string>
#include <vector>

#include <mdbx.h>

#include "LogicalTableAdapter.hpp"
#include "stores/SchemaRegistryStore.hpp"

namespace mdbxc {
namespace sync {

    inline bool canonical_logical_dbi_names(
            const std::vector<std::string>& dbi_names,
            std::vector<std::string>& out) {
        out = dbi_names;
        std::sort(out.begin(), out.end());
        return std::unique(out.begin(), out.end()) == out.end();
    }

    inline LogicalApplyResult validate_logical_adapter_marker(
            MDBX_txn* txn,
            MDBX_env* env,
            const ILogicalTableAdapter& adapter) {
        const LogicalSchemaRef ref = adapter.schema_ref();
        if (!is_logical_schema_ref_complete(ref)) {
            return LogicalApplyResult::failure(
                "Logical adapter schema ref is incomplete");
        }

        SchemaRegistryStore schemas(env);
        LogicalSchemaRecord record;
        if (!schemas.get(txn, ref.schema_id, record)) {
            return LogicalApplyResult::failure(
                "Persistent logical schema marker is missing");
        }

        if (record.kind != ref.kind ||
            record.schema_version != ref.schema_version) {
            return LogicalApplyResult::failure(
                "Persistent logical schema marker does not match adapter");
        }

        const std::string primary_dbi = adapter.primary_dbi();
        if (primary_dbi.empty()) {
            return LogicalApplyResult::failure(
                "Logical adapter primary DBI is empty");
        }

        const std::vector<std::string> affected_dbis =
            adapter.affected_dbis();
        std::vector<std::string> adapter_dbis;
        std::vector<std::string> marker_dbis;
        if (!canonical_logical_dbi_names(affected_dbis, adapter_dbis) ||
            !canonical_logical_dbi_names(record.dbi_names, marker_dbis) ||
            adapter_dbis.empty() ||
            marker_dbis.empty() ||
            adapter_dbis != marker_dbis) {
            return LogicalApplyResult::failure(
                "Persistent logical schema marker DBI set does not match adapter");
        }

        if (std::find(adapter_dbis.begin(), adapter_dbis.end(),
                      primary_dbi) == adapter_dbis.end()) {
            return LogicalApplyResult::failure(
                "Logical adapter primary DBI is not listed in affected DBIs");
        }

        if (record.dbi_name != primary_dbi) {
            return LogicalApplyResult::failure(
                "Persistent logical schema marker primary DBI does not match adapter");
        }

        return LogicalApplyResult::success();
    }

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_SCHEMA_VALIDATION_HPP_INCLUDED
