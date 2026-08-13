#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file logical/adapters/KeyMultiValueTableLogicalAdapter.hpp
/// \brief Logical adapter for unordered \c KeyMultiValueTable operations.

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "KeyValueTableLogicalAdapter.hpp"
#include "../../capture/ILogicalDbiCapture.hpp"
#include "../../stores/MetaStore.hpp"
#include "../stores/LogicalJournalStore.hpp"
#include "../stores/LogicalOutboxStore.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Opcodes understood by \c KeyMultiValueTableLogicalAdapter.
    enum class KeyMultiValueLogicalOpcode : std::uint32_t {
        InsertOne       = 1u,
        EraseKey        = 2u,
        EraseAllValues  = 3u,
        Clear           = 4u,
        EraseOneValue   = 5u
    };

    constexpr std::uint32_t opcode_value(KeyMultiValueLogicalOpcode opcode) {
        return static_cast<std::uint32_t>(opcode);
    }

    /// \brief Logical adapter for an unordered \c KeyMultiValueTable.
    /// \details The adapter transports public key/value bytes, never the
    /// table's private duplicate sequence prefix. Replaying an insert assigns
    /// a local prefix at the receiving replica and therefore preserves
    /// multiplicity without treating physical duplicate bytes as an identity.
    template<class KeyT, class ValueT,
             class KeyCodec, class ValueCodec,
             class Options = DefaultTableOptions>
    class KeyMultiValueTableLogicalAdapter : public ILogicalTableAdapter {
    private:
        class AutomaticCapture;
        static const std::uint32_t schema_version_v1 = 1u;
        static const std::uint32_t schema_version_v2 = 2u;
        static const std::uint32_t schema_version_v3 = 3u;

        static bool supports_schema_version(std::uint32_t schema_version) {
            return schema_version >= schema_version_v1 &&
                   schema_version <= schema_version_v3;
        }

    public:
        typedef KeyMultiValueTable<KeyT, ValueT, Options> table_type;

        static_assert(std::is_same<typename KeyCodec::value_type,
                                   KeyT>::value,
                      "KeyMultiValueTableLogicalAdapter key codec local type must match KeyT");
        static_assert(std::is_same<typename ValueCodec::value_type,
                                   ValueT>::value,
                      "KeyMultiValueTableLogicalAdapter value codec local type must match ValueT");

        KeyMultiValueTableLogicalAdapter(table_type& table,
                                         const std::string& schema_id,
                                         std::uint32_t schema_version = schema_version_v1)
            : m_table(table),
              m_schema_id(schema_id),
              m_schema_version(schema_version) {
            if (m_schema_id.empty()) {
                throw std::invalid_argument(
                    "KeyMultiValueTableLogicalAdapter schema id is empty");
            }
            if (m_table.dbi_name().empty()) {
                throw std::invalid_argument(
                    "KeyMultiValueTableLogicalAdapter DBI name is empty");
            }
            if (!supports_schema_version(m_schema_version)) {
                throw std::invalid_argument(
                    "KeyMultiValueTableLogicalAdapter schema version is unsupported");
            }
        }

        LogicalSchemaRef schema_ref() const override {
            return LogicalSchemaRef(m_schema_id,
                                    LogicalTableKind::KeyMultiValue,
                                    m_schema_version);
        }

        std::string primary_dbi() const override {
            return m_table.dbi_name();
        }

        std::vector<std::string> affected_dbis() const override {
            std::vector<std::string> out;
            out.push_back(m_table.dbi_name());
            return out;
        }

        LogicalChange make_insert_one(const KeyT& key,
                                      const ValueT& value) const {
            return make_pair_change(opcode_value(KeyMultiValueLogicalOpcode::InsertOne),
                                    key, value);
        }

        LogicalChange make_erase_key(const KeyT& key) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode_value(KeyMultiValueLogicalOpcode::EraseKey);
            encode_key(key, change.payload);
            return change;
        }

        LogicalChange make_erase_all_values(const KeyT& key,
                                             const ValueT& value) const {
            return make_pair_change(opcode_value(KeyMultiValueLogicalOpcode::EraseAllValues),
                                    key, value);
        }

        LogicalChange make_erase_one_value(const KeyT& key,
                                            const ValueT& value) const {
            require_schema_v2();
            return make_pair_change(opcode_value(KeyMultiValueLogicalOpcode::EraseOneValue),
                                    key, value);
        }

        LogicalChange make_clear() const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode_value(KeyMultiValueLogicalOpcode::Clear);
            return change;
        }

        /// \brief Creates connection-owned automatic capture for this adapter.
        /// \details The capture appends a receiver-neutral logical journal frame
        /// during the transaction pre-commit hook and holds no SyncEngine pointer.
        std::shared_ptr<ILogicalDbiCapture> make_automatic_capture(
                const DbId& destination) const {
            return std::shared_ptr<ILogicalDbiCapture>(
                new AutomaticCapture(m_table.connection()->env_handle(),
                                     m_table.dbi_name(), schema_ref(),
                                     destination));
        }

        /// \brief Transaction-bound typed logical capture session.
        /// \details The session owns a writable transaction and suppresses raw
        /// capture for its mutations. It captures only unordered multiset
        /// operations exposed by this class. Direct table calls, bulk table
        /// APIs remain local-only. Typed schema-v2 \c reconcile() is captured
        /// as exact multiset deltas. Schema-v3 \c erase_range() captures a
        /// bounded range as exact \c EraseKey changes.
        /// The adapter, table, and connection referenced by the adapter must
        /// outlive the session. An exception after physical mutation, outbox
        /// enqueue, or native commit processing begins requests rollback of
        /// the transaction and deactivates the session. Any failure during
        /// \c reconcile() planning or application also rolls back and
        /// deactivates the session, including before its first table mutation.
        /// Other single-operation preparation or encoding failures before
        /// mutation leave the session active.
        class LogicalCaptureSession
                : private detail::CapturedLogicalTransaction {
        public:
            explicit LogicalCaptureSession(
                    const KeyMultiValueTableLogicalAdapter& adapter)
                : detail::CapturedLogicalTransaction(
                      *adapter.m_table.connection()),
                  m_adapter(adapter) {
                const LogicalApplyResult marker_result =
                    validate_logical_adapter_marker(
                        m_txn.handle(),
                        adapter.m_table.connection()->env_handle(),
                        adapter);
                if (!marker_result.ok) {
                    throw std::runtime_error(marker_result.error);
                }
            }

            ~LogicalCaptureSession() noexcept {
                rollback();
            }

            LogicalCaptureSession(const LogicalCaptureSession&) = delete;
            LogicalCaptureSession& operator=(
                    const LogicalCaptureSession&) = delete;

            void insert(const KeyT& key, const ValueT& value) {
                ensure_active();
                append_then_mutate(m_adapter.make_insert_one(key, value),
                    [this, &key, &value]() {
                        m_adapter.m_table.insert(
                            key, value, m_txn.handle());
                    });
            }

            /// \brief Appends a batch through typed logical capture.
            /// \details Available for schema versions 1, 2, and 3. Each
            /// input pair becomes an \c InsertOne change in input order;
            /// this method does not introduce a new wire opcode.
            template<template<class...> class ContainerT>
            void append(const ContainerT<KeyT, ValueT>& values) {
                std::vector<typename table_type::value_type> copy;
                for (typename ContainerT<KeyT, ValueT>::const_iterator it =
                         values.begin(); it != values.end(); ++it) {
                    copy.push_back(typename table_type::value_type(
                        it->first, it->second));
                }
                append(copy);
            }

            void append(const std::vector<typename table_type::value_type>& values) {
                ensure_active();
                try {
                    std::vector<LogicalChange> changes;
                    changes.reserve(values.size());
                    std::size_t i = 0u;
                    for (; i < values.size(); ++i) {
                        changes.push_back(m_adapter.make_insert_one(
                            values[i].first, values[i].second));
                    }
                    m_pending.reserve(m_pending.size() + changes.size());
                    for (i = 0u; i < values.size(); ++i) {
                        m_pending.push_back(changes[i]);
                        Connection::SyncCaptureSuppressionScope suppress_capture(
                            *m_adapter.m_table.connection(), m_txn.handle());
                        m_adapter.m_table.insert(
                            values[i].first, values[i].second, m_txn.handle());
                    }
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            bool erase(const KeyT& key) {
                ensure_active();
                const LogicalChange change = m_adapter.make_erase_key(key);
                const std::size_t previous_size = m_pending.size();
                m_pending.push_back(change);
                try {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    const bool removed = m_adapter.m_table.erase(
                        key, m_txn.handle());
                    if (!removed) {
                        m_pending.resize(previous_size);
                    }
                    return removed;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            std::size_t erase(const KeyT& key, const ValueT& value) {
                ensure_active();
                const LogicalChange change =
                    m_adapter.make_erase_all_values(key, value);
                const std::size_t previous_size = m_pending.size();
                m_pending.push_back(change);
                try {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    const std::size_t removed = m_adapter.m_table.erase(
                        key, value, m_txn.handle());
                    if (removed == 0u) {
                        m_pending.resize(previous_size);
                    }
                    return removed;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            bool erase_one(const KeyT& key, const ValueT& value) {
                ensure_active();
                const LogicalChange change =
                    m_adapter.make_erase_one_value(key, value);
                const std::size_t previous_size = m_pending.size();
                m_pending.push_back(change);
                try {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    const bool removed = m_adapter.m_table.erase_one(
                        key, value, m_txn.handle());
                    if (!removed) {
                        m_pending.resize(previous_size);
                    }
                    return removed;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Erases an inclusive key range through bounded typed capture.
            /// \param from_key First key in the table's key order.
            /// \param to_key Last key in the table's key order.
            /// \param max_pairs Maximum physical pairs selected by the range.
            /// \return Number of local physical pairs removed.
            /// \throws std::length_error if more than \p max_pairs pairs match.
            /// \details The session builds exact \c EraseKey changes for all
            /// selected distinct keys before its first table mutation.
            std::size_t erase_range(const KeyT& from_key,
                                    const KeyT& to_key,
                                    std::size_t max_pairs) {
                ensure_active();
                try {
                    m_adapter.require_schema_v3();
                    std::vector<RangeEraseKey> plan;
                    std::vector<std::uint8_t> previous_payload;
                    bool has_previous = false;
                    std::size_t selected_pairs = 0u;
                    m_adapter.m_table.for_each_range(
                        from_key, to_key,
                        [this, &plan, &previous_payload, &has_previous,
                         &selected_pairs, &max_pairs](const KeyT& key,
                                                      const ValueT&) -> bool {
                            if (selected_pairs >= max_pairs) {
                                throw std::length_error(
                                    "KeyMultiValue range exceeds max_pairs");
                            }
                            ++selected_pairs;
                            const LogicalChange change =
                                m_adapter.make_erase_key(key);
                            if (!has_previous ||
                                change.payload != previous_payload) {
                                plan.push_back(RangeEraseKey(key, change));
                                previous_payload = change.payload;
                                has_previous = true;
                            }
                            return true;
                        }, m_txn.handle());
                    m_pending.reserve(m_pending.size() + plan.size());
                    std::size_t i = 0u;
                    for (; i < plan.size(); ++i) {
                        m_pending.push_back(plan[i].change);
                        Connection::SyncCaptureSuppressionScope suppress_capture(
                            *m_adapter.m_table.connection(), m_txn.handle());
                        if (!m_adapter.m_table.erase(
                                plan[i].key, m_txn.handle())) {
                            throw std::logic_error(
                                "KeyMultiValue range lost a planned key");
                        }
                    }
                    return selected_pairs;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            void reconcile(const std::vector<typename table_type::value_type>& desired) {
                ensure_active();
                try {
                    m_adapter.require_schema_v2();
                    const std::vector<typename table_type::value_type> existing =
                        m_adapter.m_table.retrieve_all_vector(m_txn.handle());
                    std::vector<LogicalChange> desired_changes;
                    desired_changes.reserve(desired.size());
                    std::size_t i = 0u;
                    for (; i < desired.size(); ++i) {
                        desired_changes.push_back(m_adapter.make_insert_one(
                            desired[i].first, desired[i].second));
                    }

                    std::vector<bool> desired_matched(desired.size(), false);
                    std::vector<bool> existing_matched(existing.size(), false);
                    std::vector<ReconcileDelta> plan;
                    plan.reserve(existing.size() + desired.size());
                    for (i = 0u; i < existing.size(); ++i) {
                        const LogicalChange existing_change =
                            m_adapter.make_insert_one(existing[i].first,
                                                      existing[i].second);
                        std::size_t j = 0u;
                        for (; j < desired.size(); ++j) {
                            if (!desired_matched[j] &&
                                existing_change.payload ==
                                    desired_changes[j].payload) {
                                desired_matched[j] = true;
                                existing_matched[i] = true;
                                break;
                            }
                        }
                    }
                    for (i = 0u; i < existing.size(); ++i) {
                        if (!existing_matched[i]) {
                            plan.push_back(ReconcileDelta(
                                m_adapter.make_erase_one_value(
                                    existing[i].first, existing[i].second),
                                existing[i], true));
                        }
                    }
                    for (i = 0u; i < desired.size(); ++i) {
                        if (!desired_matched[i]) {
                            plan.push_back(ReconcileDelta(
                                desired_changes[i], desired[i], false));
                        }
                    }

                    m_pending.reserve(m_pending.size() + plan.size());
                    for (i = 0u; i < plan.size(); ++i) {
                        m_pending.push_back(plan[i].change);
                        Connection::SyncCaptureSuppressionScope suppress_capture(
                            *m_adapter.m_table.connection(), m_txn.handle());
                        if (plan[i].erase) {
                            if (!m_adapter.m_table.erase_one(
                                    plan[i].pair.first, plan[i].pair.second,
                                    m_txn.handle())) {
                                throw std::logic_error(
                                    "KeyMultiValue reconcile lost a planned pair");
                            }
                        } else {
                            m_adapter.m_table.insert(
                                plan[i].pair.first, plan[i].pair.second,
                                m_txn.handle());
                        }
                    }
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            void clear() {
                ensure_active();
                append_then_mutate(m_adapter.make_clear(), [this]() {
                    m_adapter.m_table.clear(m_txn.handle());
                });
            }

            /// \brief Commits local mutations and returns their logical changes.
            /// \warning The returned changes are not published atomically with
            /// the table transaction. Use \c commit_to_outbox() when local
            /// durability and delivery enqueueing must share one transaction.
            void commit(std::vector<LogicalChange>& out) {
                ensure_active();
                commit_pending(out);
            }

            /// \brief Commits captured mutations and their delivery atomically.
            LogicalDeliveryEnvelope commit_to_outbox(
                    ILogicalDeliveryOutbox& outbox,
                    const DbId& destination,
                    const NodeId& receiver,
                    const CodecBounds* bounds = nullptr) {
                ensure_active();
                return commit_pending_to_outbox(outbox, destination, receiver,
                                                bounds);
            }

            void rollback() noexcept {
                if (!m_active) {
                    return;
                }
                rollback_and_deactivate();
            }

            std::size_t pending_size() const {
                return m_pending.size();
            }

        private:
            struct RangeEraseKey {
                RangeEraseKey(const KeyT& key_value,
                              const LogicalChange& change_value)
                    : key(key_value), change(change_value) {}

                KeyT key;
                LogicalChange change;
            };

            struct ReconcileDelta {
                ReconcileDelta(const LogicalChange& change_value,
                               const typename table_type::value_type& pair_value,
                               bool erase_value)
                    : change(change_value), pair(pair_value), erase(erase_value) {}

                LogicalChange change;
                typename table_type::value_type pair;
                bool erase;
            };

            template<class Mutation>
            void append_then_mutate(const LogicalChange& change,
                                    const Mutation& mutation) {
                m_pending.push_back(change);
                try {
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    mutation();
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            void ensure_active() const {
                detail::CapturedLogicalTransaction::ensure_active(
                    "KeyMultiValue logical capture session is not active");
            }

            const KeyMultiValueTableLogicalAdapter& m_adapter;
        };

        std::unique_ptr<LogicalCaptureSession> begin_capture_session() const {
            return std::unique_ptr<LogicalCaptureSession>(
                new LogicalCaptureSession(*this));
        }

        LogicalApplyResult preflight(
                MDBX_txn* txn,
                const LogicalChange& change) const override {
            (void)txn;
            return validate_payload(change);
        }

        LogicalApplyResult apply(
                MDBX_txn* txn,
                const LogicalChange& change) override {
            const LogicalApplyResult validation = validate_payload(change);
            if (!validation.ok) {
                return validation;
            }

            try {
                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_table.connection(), txn);
                if (change.opcode == opcode_value(KeyMultiValueLogicalOpcode::InsertOne)) {
                    const std::pair<KeyT, ValueT> pair =
                        decode_pair(change.payload);
                    m_table.insert(pair.first, pair.second, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == opcode_value(KeyMultiValueLogicalOpcode::EraseKey)) {
                    (void)m_table.erase(decode_key(change.payload), txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == opcode_value(KeyMultiValueLogicalOpcode::EraseAllValues)) {
                    const std::pair<KeyT, ValueT> pair =
                        decode_pair(change.payload);
                    (void)m_table.erase(pair.first, pair.second, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == opcode_value(KeyMultiValueLogicalOpcode::EraseOneValue)) {
                    const std::pair<KeyT, ValueT> pair =
                        decode_pair(change.payload);
                    (void)m_table.erase_one(pair.first, pair.second, txn);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == opcode_value(KeyMultiValueLogicalOpcode::Clear)) {
                    m_table.clear(txn);
                    return LogicalApplyResult::success();
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyMultiValue logical adapter apply failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyMultiValue logical adapter apply failed");
            }
            return LogicalApplyResult::failure(
                "KeyMultiValue logical adapter opcode is unsupported");
        }

    private:
        class AutomaticCapture : public ILogicalDbiCapture {
        public:
            AutomaticCapture(MDBX_env* env,
                             const std::string& dbi_name,
                             const LogicalSchemaRef& schema,
                             const DbId& destination)
                : m_env(env), m_dbi_name(dbi_name), m_schema(schema),
                  m_destination(destination) {
                if (m_env == nullptr || !is_logical_schema_ref_complete(m_schema) ||
                    is_zero_sync_id(m_destination)) {
                    throw std::invalid_argument(
                        "KeyMultiValue automatic logical capture is invalid");
                }
            }

            const std::string& dbi_name() const override { return m_dbi_name; }
            const std::string& schema_id() const override { return m_schema.schema_id; }
            std::uint32_t schema_version() const override {
                return m_schema.schema_version;
            }

            void record_insert(MDBX_txn* txn, const void* key,
                               const void* value) override {
                append(txn, make_pair_change(
                    opcode_value(KeyMultiValueLogicalOpcode::InsertOne),
                    checked_key(key), checked_value(value)));
            }

            void record_erase_key(MDBX_txn* txn, const void* key) override {
                LogicalChange change;
                change.schema = m_schema;
                change.opcode = opcode_value(KeyMultiValueLogicalOpcode::EraseKey);
                encode_key(checked_key(key), change.payload);
                append(txn, change);
            }

            void record_erase_all_values(MDBX_txn* txn, const void* key,
                                         const void* value) override {
                append(txn, make_pair_change(
                    opcode_value(KeyMultiValueLogicalOpcode::EraseAllValues),
                    checked_key(key), checked_value(value)));
            }

            void record_erase_one_value(MDBX_txn* txn, const void* key,
                                        const void* value) override {
                if (m_schema.schema_version < schema_version_v2) {
                    throw std::logic_error(
                        "KeyMultiValue automatic exact-one erase requires schema version 2 or newer");
                }
                append(txn, make_pair_change(
                    opcode_value(KeyMultiValueLogicalOpcode::EraseOneValue),
                    checked_key(key), checked_value(value)));
            }

            void record_clear(MDBX_txn* txn) override {
                LogicalChange change;
                change.schema = m_schema;
                change.opcode = opcode_value(KeyMultiValueLogicalOpcode::Clear);
                append(txn, change);
            }

            void flush_in_txn(MDBX_txn* txn) override {
                LogicalChangeFrame frame;
                {
                    std::lock_guard<std::mutex> locker(m_mutex);
                    typename std::map<MDBX_txn*, LogicalChangeFrame>::iterator it =
                        m_pending.find(txn);
                    if (it == m_pending.end() || it->second.changes.empty()) return;
                    frame.changes.swap(it->second.changes);
                    m_pending.erase(it);
                }
                MetaStore meta(m_env);
                LogicalJournalStore journal(m_env);
                LogicalOutboxStore outbox(m_env);
                meta.open(txn);
                const NodeId origin = meta.get_node_id(txn);
                if (is_zero_sync_id(origin)) {
                    throw std::logic_error(
                        "KeyMultiValue automatic logical capture requires local node identity");
                }
                if (!journal.is_initialized(txn)) {
                    if (outbox.has_persistent_state(txn)) {
                        throw std::logic_error(
                            "logical journal cannot start with legacy outbox state");
                    }
                    journal.initialize(txn);
                }
                (void)journal.append(txn, m_destination, origin, frame);
            }

            void discard_txn(MDBX_txn* txn) noexcept override {
                try {
                    std::lock_guard<std::mutex> locker(m_mutex);
                    m_pending.erase(txn);
                } catch (...) {
                }
            }

        private:
            static const KeyT& checked_key(const void* value) {
                if (value == nullptr) {
                    throw std::invalid_argument(
                        "KeyMultiValue automatic capture key is null");
                }
                return *static_cast<const KeyT*>(value);
            }

            static const ValueT& checked_value(const void* value) {
                if (value == nullptr) {
                    throw std::invalid_argument(
                        "KeyMultiValue automatic capture value is null");
                }
                return *static_cast<const ValueT*>(value);
            }

            LogicalChange make_pair_change(std::uint32_t opcode,
                                            const KeyT& key,
                                            const ValueT& value) const {
                LogicalChange change;
                change.schema = m_schema;
                change.opcode = opcode;
                encode_pair(key, value, change.payload);
                return change;
            }

            void append(MDBX_txn* txn, const LogicalChange& change) {
                if (txn == nullptr) {
                    throw std::invalid_argument(
                        "KeyMultiValue automatic capture transaction is null");
                }
                std::lock_guard<std::mutex> locker(m_mutex);
                m_pending[txn].changes.push_back(change);
            }

            MDBX_env* m_env;
            std::string m_dbi_name;
            LogicalSchemaRef m_schema;
            DbId m_destination;
            std::mutex m_mutex;
            std::map<MDBX_txn*, LogicalChangeFrame> m_pending;
        };

#include "KeyMultiValueTableLogicalAdapter.ipp"

    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_MULTI_VALUE_TABLE_LOGICAL_ADAPTER_HPP_INCLUDED
