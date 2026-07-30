#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_TABLE_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_TABLE_ADAPTER_HPP_INCLUDED

/// \file LogicalTableAdapter.hpp
/// \brief Registry interfaces for future logical sync table adapters.

#include <map>
#include <stdexcept>
#include <string>
#include <utility>
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
    /// The sync core must validate every schema-local batch in a transaction
    /// before calling \c apply() for any logical change.
    class ILogicalTableAdapter {
    public:
        virtual ~ILogicalTableAdapter() {}

        /// \brief Returns the logical schema served by this adapter.
        virtual LogicalSchemaRef schema_ref() const = 0;

        /// \brief Returns physical DBI names that may be read or written.
        virtual std::vector<std::string> affected_dbis() const = 0;

        /// \brief Returns the primary physical DBI name for this schema.
        /// \details The returned name must be included in \c affected_dbis().
        /// The default keeps existing single-DBI adapters source-compatible by
        /// returning the only affected DBI. Multi-DBI adapters must override
        /// this method explicitly; otherwise marker validation fails closed.
        virtual std::string primary_dbi() const {
            const std::vector<std::string> names = affected_dbis();
            return names.size() == 1u ? names[0] : std::string();
        }

        /// \brief Returns whether this adapter requires ordered delivery.
        /// \details The default preserves existing logical adapters. Adapters
        /// whose public state depends on append history override this method so
        /// direct logical frames and unordered delivery fail before preflight
        /// or mutation.
        virtual bool requires_ordered_delivery() const {
            return false;
        }

        /// \brief Validates a logical change without mutating user tables.
        virtual LogicalApplyResult preflight(
                MDBX_txn* txn,
                const LogicalChange& change) const = 0;

        /// \brief Validates all changes for this registered schema at once.
        /// \details The default preserves existing adapters by calling
        /// \c preflight() for every change in relative frame order. Adapters
        /// with frame-local invariants may override this method. The registry
        /// calls it only after all changes have passed schema validation and
        /// before any adapter \c apply() call.
        virtual LogicalApplyResult preflight_batch(
                MDBX_txn* txn,
                const std::vector<LogicalChange>& changes) const {
            for (std::size_t i = 0u; i < changes.size(); ++i) {
                const LogicalApplyResult result = preflight(txn, changes[i]);
                if (!result.ok) return result;
            }
            return LogicalApplyResult::success();
        }

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
                const std::vector<LogicalChange>& changes,
                bool has_ordered_delivery = false) const {
            std::vector<AdapterRegistration> registrations;
            registrations.reserve(changes.size());
            std::vector<AdapterRegistration> batch_registrations;
            std::vector<std::vector<LogicalChange> > batches;
            std::map<std::string, std::size_t> batch_indices;

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
                if (it->second.adapter->requires_ordered_delivery() &&
                    !has_ordered_delivery) {
                    return LogicalApplyResult::failure(
                        "Logical adapter requires ordered delivery");
                }
                registrations.push_back(it->second);

                const std::pair<std::map<std::string, std::size_t>::iterator,
                                bool> inserted = batch_indices.insert(
                    std::make_pair(changes[i].schema.schema_id, batches.size()));
                if (inserted.second) {
                    batch_registrations.push_back(it->second);
                    batches.push_back(std::vector<LogicalChange>());
                }
                batches[inserted.first->second].push_back(changes[i]);
            }

            for (std::size_t i = 0u; i < batches.size(); ++i) {
                const LogicalApplyResult result =
                    batch_registrations[i].adapter->preflight_batch(
                        txn, batches[i]);
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
