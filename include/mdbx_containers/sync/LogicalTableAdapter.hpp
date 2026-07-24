#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_TABLE_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_TABLE_ADAPTER_HPP_INCLUDED

/// \file LogicalTableAdapter.hpp
/// \brief Registry interfaces for future logical sync table adapters.

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include <mdbx.h>

#include "LogicalChange.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Result of logical adapter preflight or apply.
    struct LogicalApplyResult {
        bool ok = true;             ///< Whether the operation succeeded.
        bool retryable = false;     ///< Whether retry may succeed after progress.
        std::string error;          ///< Human-readable diagnostic.

        static LogicalApplyResult success() {
            return LogicalApplyResult();
        }

        static LogicalApplyResult failure(const std::string& message,
                                          bool is_retryable = false) {
            LogicalApplyResult out;
            out.ok = false;
            out.retryable = is_retryable;
            out.error = message;
            return out;
        }
    };

    /// \brief Type-erased adapter for one logical table schema.
    /// \details Implementations own table-specific decoding and apply logic.
    /// The sync core must call \c preflight() for all logical changes in a
    /// transaction before calling \c apply() for any of them.
    class ILogicalTableAdapter {
    public:
        virtual ~ILogicalTableAdapter() {}

        /// \brief Returns the logical schema served by this adapter.
        virtual LogicalSchemaRef schema_ref() const = 0;

        /// \brief Returns physical DBI names that may be read or written.
        virtual std::vector<std::string> affected_dbis() const = 0;

        /// \brief Validates a logical change without mutating user tables.
        virtual LogicalApplyResult preflight(
                MDBX_txn* txn,
                const LogicalChange& change) const = 0;

        /// \brief Applies a logical change after all preflights succeeded.
        virtual LogicalApplyResult apply(
                MDBX_txn* txn,
                const LogicalChange& change) = 0;
    };

    /// \brief Non-owning registry of logical table adapters.
    /// \thread_safety Not thread-safe. Treat registration as lifecycle setup.
    class LogicalTableRegistry {
    public:
        /// \brief Registers \p adapter for its schema id.
        /// \throws std::invalid_argument for null, incomplete, or duplicate
        /// adapter registrations.
        void register_adapter(ILogicalTableAdapter* adapter) {
            if (adapter == nullptr) {
                throw std::invalid_argument("Logical adapter is null");
            }
            const LogicalSchemaRef ref = adapter->schema_ref();
            if (!is_logical_schema_ref_complete(ref)) {
                throw std::invalid_argument("Logical adapter schema ref is incomplete");
            }
            const AdapterRegistration registration(adapter, ref);
            const std::pair<AdapterMap::iterator, bool> inserted =
                m_adapters.insert(
                    AdapterMap::value_type(ref.schema_id, registration));
            if (!inserted.second) {
                throw std::invalid_argument("Duplicate logical adapter schema id");
            }
        }

        /// \brief Removes adapter for \p schema_id.
        /// \return true when an adapter was removed.
        bool unregister_adapter(const std::string& schema_id) {
            return m_adapters.erase(schema_id) != 0u;
        }

        /// \brief Finds an adapter by schema id.
        ILogicalTableAdapter* find(const std::string& schema_id) const {
            AdapterMap::const_iterator it = m_adapters.find(schema_id);
            return it == m_adapters.end() ? nullptr : it->second.adapter;
        }

        /// \brief Returns number of registered adapters.
        std::size_t size() const { return m_adapters.size(); }

        /// \brief Runs preflight for all changes, then applies all changes.
        /// \details This helper does not open transactions by itself. It only
        /// enforces the two-phase adapter contract for a caller-owned write
        /// transaction. If an adapter returns failure from \c apply() or
        /// throws after mutating data, this helper returns failure and the
        /// caller-owned transaction must be aborted by the caller; the
        /// registry cannot roll back a transaction it does not own.
        LogicalApplyResult preflight_then_apply(
                MDBX_txn* txn,
                const std::vector<LogicalChange>& changes) const {
            std::vector<AdapterRegistration> registrations;
            registrations.reserve(changes.size());

            for (std::size_t i = 0; i < changes.size(); ++i) {
                AdapterMap::const_iterator it =
                    m_adapters.find(changes[i].schema.schema_id);
                if (it == m_adapters.end()) {
                    return LogicalApplyResult::failure(
                        "No logical adapter registered for schema id");
                }
                const LogicalApplyResult validation =
                    validate_change(it->second, changes[i]);
                if (!validation.ok) return validation;
                registrations.push_back(it->second);
            }

            for (std::size_t i = 0; i < changes.size(); ++i) {
                const LogicalApplyResult result =
                    registrations[i].adapter->preflight(txn, changes[i]);
                if (!result.ok) return result;
            }

            for (std::size_t i = 0; i < changes.size(); ++i) {
                LogicalApplyResult result;
                try {
                    result = registrations[i].adapter->apply(txn, changes[i]);
                } catch (const std::exception& e) {
                    return LogicalApplyResult::failure(
                        std::string("Logical adapter apply threw: ") +
                        e.what());
                } catch (...) {
                    return LogicalApplyResult::failure(
                        "Logical adapter apply threw a non-std exception");
                }
                if (!result.ok) return result;
            }

            return LogicalApplyResult::success();
        }

    private:
        struct AdapterRegistration {
            AdapterRegistration()
                : adapter(nullptr) {}

            AdapterRegistration(ILogicalTableAdapter* p,
                                const LogicalSchemaRef& ref)
                : adapter(p),
                  schema(ref) {}

            ILogicalTableAdapter* adapter;
            LogicalSchemaRef schema;
        };

        typedef std::map<std::string, AdapterRegistration> AdapterMap;

        static bool schema_refs_equal(const LogicalSchemaRef& lhs,
                                      const LogicalSchemaRef& rhs) {
            return lhs.schema_id == rhs.schema_id &&
                   lhs.kind == rhs.kind &&
                   lhs.schema_version == rhs.schema_version;
        }

        static LogicalApplyResult validate_change(
                const AdapterRegistration& registration,
                const LogicalChange& change) {
            if (!is_logical_schema_ref_complete(change.schema)) {
                return LogicalApplyResult::failure(
                    "Logical change schema ref is incomplete");
            }
            if (!schema_refs_equal(registration.schema, change.schema)) {
                return LogicalApplyResult::failure(
                    "Logical change schema ref does not match registered adapter");
            }
            if (change.flags != 0) {
                return LogicalApplyResult::failure(
                    "Logical change flags are reserved and must be zero");
            }
            return LogicalApplyResult::success();
        }

        AdapterMap m_adapters;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_TABLE_ADAPTER_HPP_INCLUDED
