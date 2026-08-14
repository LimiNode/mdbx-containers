#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_SCHEMA_VALIDATION_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_SCHEMA_VALIDATION_HPP_INCLUDED

/// \file logical/logical_schema_validation.hpp
/// \brief Persistent marker validation helpers for logical sync adapters.

#include <algorithm>
#include <string>
#include <vector>

#include <mdbx.h>

#include "LogicalDeliveryEnvelope.hpp"
#include "LogicalTableAdapter.hpp"

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

    /// \brief Verifies the current authoritative origin of an ordered schema.
    LogicalApplyResult validate_ordered_logical_schema_record_origin(
            MDBX_txn* txn,
            MDBX_env* env,
            const std::string& schema_id);

    /// \brief Verifies that an ordered schema marker names this local origin.
    LogicalApplyResult validate_ordered_logical_schema_record_origin(
            MDBX_txn* txn,
            MDBX_env* env,
            const LogicalSchemaRecord& record);

    /// \brief Verifies that an ordered adapter is bound to this local origin.
    /// \details Capture must not create an ordered outbox envelope that its
    /// receiver will reject because the persistent schema marker names another
    /// authoritative origin.
    inline LogicalApplyResult validate_ordered_logical_adapter_origin(
            MDBX_txn* txn,
            MDBX_env* env,
            const ILogicalTableAdapter& adapter) {
        const LogicalSchemaRef ref = adapter.schema_ref();
        const LogicalApplyResult marker_result =
            validate_logical_adapter_marker(txn, env, adapter);
        if (!marker_result.ok) {
            return marker_result;
        }

        if (!adapter.requires_ordered_delivery()) {
            return LogicalApplyResult::failure(
                "Logical adapter does not require ordered delivery");
        }

        return validate_ordered_logical_schema_record_origin(
            txn, env, ref.schema_id);
    }

    /// \brief Verifies the current authoritative origin of an ordered schema.
    /// \details This deliberately rereads the durable marker. Capture paths
    /// call it at their commit boundary so an authority cutover makes a stale
    /// local writer fail before its physical mutation can commit.
    inline LogicalApplyResult validate_ordered_logical_schema_record_origin(
            MDBX_txn* txn,
            MDBX_env* env,
            const std::string& schema_id) {
        if (schema_id.empty()) {
            return LogicalApplyResult::failure(
                "Ordered logical schema id is empty");
        }

        SchemaRegistryStore schemas(env);
        LogicalSchemaRecord record;
        if (!schemas.get(txn, schema_id, record)) {
            return LogicalApplyResult::failure(
                "Persistent logical schema marker is missing");
        }
        return validate_ordered_logical_schema_record_origin(txn, env, record);
    }

    inline LogicalApplyResult validate_ordered_logical_schema_record_origin(
            MDBX_txn* txn,
            MDBX_env* env,
            const LogicalSchemaRecord& record) {
        if (is_zero_sync_id(record.ordered_delivery_origin_node_id)) {
            return LogicalApplyResult::failure(
                "Persistent logical schema marker has no ordered delivery origin");
        }

        MetaStore meta(env);
        meta.open(txn);
        const NodeId local_node_id = meta.get_node_id(txn);
        if (is_zero_sync_id(local_node_id) ||
            compare_node_id(record.ordered_delivery_origin_node_id,
                            local_node_id) != 0) {
            return LogicalApplyResult::failure(
                "Local node does not match ordered logical schema origin");
        }

        return LogicalApplyResult::success();
    }

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_LOGICAL_SCHEMA_VALIDATION_HPP_INCLUDED
