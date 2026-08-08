#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_DESTRUCTIVE_LOGICAL_ADAPTER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_DESTRUCTIVE_LOGICAL_ADAPTER_HPP_INCLUDED

/// \file logical/adapters/KeyOrderedMultiValueTableDestructiveLogicalAdapter.hpp
/// \brief Destructive ordered logical adapter with persistent element ids.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "KeyOrderedMultiValueDestructiveState.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Opcodes for schema-version-2 destructive ordered tables.
    enum class KeyOrderedMultiValueDestructiveLogicalOpcode : std::uint32_t {
        Append = 1u,
        Erase = 2u
    };

    constexpr std::uint32_t opcode_value(
            KeyOrderedMultiValueDestructiveLogicalOpcode opcode) {
        return static_cast<std::uint32_t>(opcode);
    }

    /// \brief Ordered logical adapter with stable identities and tombstones.
    /// \details Schema version 2 is intentionally distinct from the v1
    /// append-only adapter. Each live value has an immutable
    /// \c OrderedElementId, allowing one exact occurrence to be erased even
    /// when the same value occurs repeatedly for a key.
    template<class KeyT, class ValueT,
             class KeyCodec, class ValueCodec,
             class Options = DefaultTableOptions>
    class KeyOrderedMultiValueTableDestructiveLogicalAdapter
            : public ILogicalTableAdapter {
    private:
        static const std::uint32_t supported_schema_version = 2u;

    public:
        typedef KeyOrderedMultiValueTable<KeyT, ValueT, Options> table_type;

        static_assert(std::is_same<typename KeyCodec::value_type,
                                   KeyT>::value,
                      "KeyOrderedMultiValue destructive logical adapter key codec local type must match KeyT");
        static_assert(std::is_same<typename ValueCodec::value_type,
                                   ValueT>::value,
                      "KeyOrderedMultiValue destructive logical adapter value codec local type must match ValueT");

        KeyOrderedMultiValueTableDestructiveLogicalAdapter(
                table_type& table,
                const std::string& schema_id,
                const std::string& state_dbi_name,
                const std::string& by_key_dbi_name,
                std::uint32_t schema_version = supported_schema_version)
            : m_table(table),
              m_schema_id(schema_id),
              m_schema_version(schema_version),
              m_state(table.connection()->env_handle(), state_dbi_name,
                      by_key_dbi_name) {
            if (m_schema_id.empty()) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValue destructive logical schema id is empty");
            }
            if (m_table.dbi_name().empty()) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValue destructive logical DBI name is empty");
            }
            if (m_schema_version != supported_schema_version) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValue destructive logical adapter supports only schema version 2");
            }
            const std::vector<std::string> names = affected_dbis();
            for (std::size_t i = 0u; i < names.size(); ++i) {
                for (std::size_t j = i + 1u; j < names.size(); ++j) {
                    if (names[i] == names[j]) {
                        throw std::invalid_argument(
                            "KeyOrderedMultiValue destructive logical DBI names must differ");
                    }
                }
            }
        }

        /// \brief Opens the primary table with marker-aware creation policy.
        /// \details A fresh schema preserves the caller's \c MDBX_CREATE flag.
        /// If its persistent schema marker already exists, this helper removes
        /// \c MDBX_CREATE before constructing the table accessor. A missing
        /// primary DBI is then reported as corruption instead of silently being
        /// recreated. Use this helper for every schema-v2 reopen; direct table
        /// construction is the ordinary non-schema-bound table API.
        static std::shared_ptr<table_type> open_primary_for_schema(
                const std::shared_ptr<Connection>& connection,
                const std::string& schema_id,
                const std::string& primary_dbi_name,
                MDBX_db_flags_t flags = MDBX_DB_DEFAULTS | MDBX_CREATE) {
            if (!connection || schema_id.empty() || primary_dbi_name.empty()) {
                throw std::invalid_argument(
                    "KeyOrderedMultiValue destructive primary configuration is invalid");
            }

            bool marker_exists = false;
            {
                Transaction txn = connection->transaction(TransactionMode::READ_ONLY);
                MDBX_dbi schema_dbi = 0;
                const int open_rc = mdbx_dbi_open(
                    txn.handle(), "_mdbxc_sync_schema",
                    static_cast<MDBX_db_flags_t>(0), &schema_dbi);
                if (open_rc != MDBX_NOTFOUND) {
                    check_mdbx(open_rc,
                               "KeyOrderedMultiValue destructive schema registry open failed");
                    SchemaRegistryStore schemas(connection->env_handle());
                    LogicalSchemaRecord marker;
                    if (schemas.get(txn.handle(), schema_id, marker)) {
                        if (marker.kind != LogicalTableKind::KeyOrderedMultiValue ||
                            marker.schema_version != supported_schema_version ||
                            marker.dbi_name != primary_dbi_name) {
                            throw std::runtime_error(
                                "KeyOrderedMultiValue destructive schema marker does not match primary DBI");
                        }
                        marker_exists = true;
                    }
                }
                txn.rollback();
            }

            const MDBX_db_flags_t effective_flags = marker_exists
                ? static_cast<MDBX_db_flags_t>(flags & ~MDBX_CREATE)
                : flags;
            return std::shared_ptr<table_type>(
                new table_type(connection, primary_dbi_name, effective_flags));
        }

        LogicalSchemaRef schema_ref() const override {
            return LogicalSchemaRef(m_schema_id,
                                    LogicalTableKind::KeyOrderedMultiValue,
                                    m_schema_version);
        }

        std::string primary_dbi() const override {
            return m_table.dbi_name();
        }

        std::vector<std::string> affected_dbis() const override {
            std::vector<std::string> out;
            out.push_back(m_table.dbi_name());
            out.push_back(m_state.state_dbi_name());
            out.push_back(m_state.by_key_dbi_name());
            return out;
        }

        bool requires_ordered_delivery() const override {
            return true;
        }

        void initialize_storage(MDBX_txn* txn) const override {
            checked_txn_env(
                txn, m_table.connection()->env_handle(),
                "KeyOrderedMultiValue destructive storage initialization");
            // Fresh v2 setup cannot silently invent ids for an existing table.
            if (m_table.count(txn) != 0u) {
                throw std::runtime_error(
                    "KeyOrderedMultiValue destructive primary DBI is not empty");
            }
            m_state.initialize_empty(txn);
        }

        void verify_storage(MDBX_txn* txn) const override {
            checked_txn_env(
                txn, m_table.connection()->env_handle(),
                "KeyOrderedMultiValue destructive storage verification");
            m_table.count(txn);
            m_state.verify_existing(txn);
        }

        LogicalChange make_append(const OrderedElementId& id,
                                  const KeyT& key,
                                  const ValueT& value) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode_value(
                KeyOrderedMultiValueDestructiveLogicalOpcode::Append);
            encode_append(id, key, value, change.payload);
            return change;
        }

    private:
        LogicalChange make_append_prepared(
                const OrderedElementId& id,
                const std::vector<std::uint8_t>& key_bytes,
                const std::vector<std::uint8_t>& value_bytes) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode_value(
                KeyOrderedMultiValueDestructiveLogicalOpcode::Append);
            encode_append_bytes(id, key_bytes, value_bytes, change.payload);
            return change;
        }

    public:

        LogicalChange make_erase(const OrderedElementId& id) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode_value(
                KeyOrderedMultiValueDestructiveLogicalOpcode::Erase);
            change.payload = encode_ordered_element_id_logical(id);
            return change;
        }

        /// \brief Transaction-bound capture for destructive ordered changes.
        /// \details Only \c commit_to_outbox() is exposed. This preserves the
        /// atomic boundary between the physical table, v2 element state, and
        /// the durable ordered-delivery envelope. An append followed by erase
        /// of the same new id coalesces to no frame operation while its
        /// allocated counter remains monotonic.
        class LogicalCaptureSession {
        private:
            struct ClearPlan {
                std::vector<OrderedElementId> ids;
                std::vector<NodeId> element_origins;
            };

            struct PreparedReplacementAppend {
                const typename table_type::value_type* source;
                OrderedElementId id;
                std::vector<std::uint8_t> key_bytes;
                std::vector<std::uint8_t> value_bytes;
                LogicalChange change;

                PreparedReplacementAppend() : source(nullptr) {}
            };

        public:
            explicit LogicalCaptureSession(
                    const KeyOrderedMultiValueTableDestructiveLogicalAdapter& adapter)
                : m_adapter(adapter),
                  m_txn(adapter.m_table.connection()->transaction(
                      TransactionMode::WRITABLE)),
                  m_identity(
                      std::make_shared<OrderedElementKeyIndexProofIdentity>()),
                  m_active(true),
                  m_mutation_revision(0u) {
                const LogicalApplyResult marker_result =
                    validate_logical_adapter_marker(
                        m_txn.handle(), adapter.m_table.connection()->env_handle(),
                        adapter);
                if (!marker_result.ok) {
                    throw std::runtime_error(marker_result.error);
                }
                const LogicalApplyResult origin_result =
                    validate_ordered_logical_adapter_origin(
                        m_txn.handle(), adapter.m_table.connection()->env_handle(),
                        adapter);
                if (!origin_result.ok) {
                    throw std::runtime_error(origin_result.error);
                }
                MetaStore meta(adapter.m_table.connection()->env_handle());
                meta.open(m_txn.handle());
                m_origin = meta.get_node_id(m_txn.handle());
                if (compare_node_id(m_origin, make_zero_node()) == 0) {
                    throw std::runtime_error(
                        "KeyOrderedMultiValue destructive capture origin is missing");
                }
            }

            ~LogicalCaptureSession() noexcept {
                rollback();
            }

            LogicalCaptureSession(const LogicalCaptureSession&) = delete;
            LogicalCaptureSession& operator=(const LogicalCaptureSession&) = delete;

            /// \brief Appends and returns its durable element identity.
            OrderedElementId append(const KeyT& key, const ValueT& value) {
                ensure_active();
                try {
                    const OrderedElementId id =
                        m_adapter.m_state.allocate_id(m_txn.handle(), m_origin);
                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    m_adapter.append_live_element(m_txn.handle(), id, key, value);
                    m_pending.push_back(m_adapter.make_append(id, key, value));
                    ++m_mutation_revision;
                    return id;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Erases exactly one live value by its immutable identity.
            void erase(const OrderedElementId& id) {
                ensure_active();
                try {
                    erase_resolved(id, nullptr);
                    ++m_mutation_revision;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Validates one key and returns a transaction-bound proof.
            /// \details The proof enables the explicit trusted selector
            /// overloads below to skip their second full state reverse scan.
            /// It becomes invalid after any capture mutation, rollback, or
            /// commit and must be used only with this session.
            OrderedElementKeyIndexProof validate_key_index(
                    const KeyT& key,
                    const BroadEraseBounds& bounds) {
                ensure_active();
                try {
                    const std::vector<std::uint8_t> key_bytes =
                        KeyCodec::encode(key);
                    OrderedElementCandidateSet candidates(bounds);
                    OrderedElementKeyIndexProof proof;
                    proof.transaction = m_txn.handle();
                    proof.identity = m_identity;
                    proof.mutation_revision = m_mutation_revision;
                    proof.key = key_bytes;
                    proof.ids = m_adapter.m_state.validate_live_ids_for_key(
                        m_txn.handle(), key_bytes, &candidates,
                        bounds.max_selected_elements);
                    return proof;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Resolves and erases one current per-key append position.
            bool erase_at(const KeyT& key,
                          std::size_t index,
                          const BroadEraseBounds& bounds) {
                ensure_active();
                try {
                    OrderedElementCandidateSet candidates(bounds);
                    const bool found = resolve_live_elements(
                        key, candidates,
                        [index](std::size_t current,
                                const OrderedElementStateRecord&) {
                            return current == index;
                        });
                    if (!found) return false;
                    erase_selected(candidates.sorted_ids(), &candidates);
                    return true;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Resolves and erases all canonical value matches under a key.
            std::size_t erase_value(const KeyT& key,
                                    const ValueT& value,
                                    const BroadEraseBounds& bounds) {
                ensure_active();
                try {
                    const std::vector<std::uint8_t> value_bytes =
                        ValueCodec::encode(value);
                    OrderedElementCandidateSet candidates(bounds);
                    resolve_live_elements(
                        key, candidates,
                        [&value_bytes](std::size_t,
                                       const OrderedElementStateRecord& record) {
                            return record.value == value_bytes;
                        });
                    const std::vector<OrderedElementId> ids =
                        candidates.sorted_ids();
                    erase_selected(ids, &candidates);
                    return ids.size();
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Resolves and erases all current values under a key.
            std::size_t erase_key(const KeyT& key,
                                  const BroadEraseBounds& bounds) {
                ensure_active();
                try {
                    OrderedElementCandidateSet candidates(bounds);
                    resolve_live_elements(
                        key, candidates,
                        [](std::size_t, const OrderedElementStateRecord&) {
                            return true;
                        });
                    const std::vector<OrderedElementId> ids =
                        candidates.sorted_ids();
                    erase_selected(ids, &candidates);
                    return ids.size();
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Erases one per-key position using a validated proof.
            /// \details Unlike erase_at(), this opt-in path does not repeat
            /// the full reverse scan. The proof must come from
            /// validate_key_index() on this unchanged session.
            bool erase_at_trusted(
                    const KeyT& key,
                    std::size_t index,
                    const OrderedElementKeyIndexProof& proof,
                    const BroadEraseBounds& bounds) {
                ensure_active();
                try {
                    OrderedElementCandidateSet candidates(bounds);
                    const bool found = resolve_live_elements_trusted(
                        key, proof, candidates,
                        [index](std::size_t current,
                                const OrderedElementStateRecord&) {
                            return current == index;
                        });
                    if (!found) return false;
                    erase_selected(candidates.sorted_ids(), &candidates);
                    return true;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Erases matching values using a validated proof.
            std::size_t erase_value_trusted(
                    const KeyT& key,
                    const ValueT& value,
                    const OrderedElementKeyIndexProof& proof,
                    const BroadEraseBounds& bounds) {
                ensure_active();
                try {
                    const std::vector<std::uint8_t> value_bytes =
                        ValueCodec::encode(value);
                    OrderedElementCandidateSet candidates(bounds);
                    resolve_live_elements_trusted(
                        key, proof, candidates,
                        [&value_bytes](std::size_t,
                                       const OrderedElementStateRecord& record) {
                            return record.value == value_bytes;
                        });
                    const std::vector<OrderedElementId> ids =
                        candidates.sorted_ids();
                    erase_selected(ids, &candidates);
                    return ids.size();
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Erases all values under a key using a validated proof.
            std::size_t erase_key_trusted(
                    const KeyT& key,
                    const OrderedElementKeyIndexProof& proof,
                    const BroadEraseBounds& bounds) {
                ensure_active();
                try {
                    OrderedElementCandidateSet candidates(bounds);
                    resolve_live_elements_trusted(
                        key, proof, candidates,
                        [](std::size_t, const OrderedElementStateRecord&) {
                            return true;
                        });
                    const std::vector<OrderedElementId> ids =
                        candidates.sorted_ids();
                    erase_selected(ids, &candidates);
                    return ids.size();
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Resolves and erases every current table element.
            std::size_t clear(const BroadEraseBounds& bounds) {
                ensure_active();
                try {
                    OrderedElementCandidateSet candidates(bounds);
                    const ClearPlan plan = prepare_clear(candidates);
                    erase_selected(plan.ids, &candidates);
                    return plan.ids.size();
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Replaces the complete live set with desired occurrences.
            /// \details The operation prepares canonical logical bytes, fresh
            /// immutable ids, and exact append changes before its first
            /// physical mutation. Existing live occurrences are then erased
            /// exactly and the prepared occurrences are appended without
            /// rescanning the complete state. The operation is single-origin
            /// schema-v2 capture; it introduces no new wire opcode and remains
            /// bounded by both the existing-state and replacement limits.
            void replace_with(
                    const std::vector<typename table_type::value_type>& desired,
                    const ReplaceWithBounds& bounds) {
                ensure_active();
                try {
                    if (desired.size() > bounds.max_replacement_elements) {
                        throw std::length_error(
                            "Ordered destructive replacement limit exceeded");
                    }

                    std::vector<PreparedReplacementAppend> prepared;
                    prepared.reserve(desired.size());
                    for (std::size_t i = 0u; i < desired.size(); ++i) {
                        PreparedReplacementAppend entry;
                        entry.source = &desired[i];
                        entry.key_bytes =
                            KeyCodec::encode(desired[i].first);
                        entry.value_bytes =
                            ValueCodec::encode(desired[i].second);
                        if (KeyCodec::encode(KeyCodec::decode(entry.key_bytes)) !=
                                entry.key_bytes ||
                            ValueCodec::encode(ValueCodec::decode(entry.value_bytes)) !=
                                entry.value_bytes) {
                            throw std::runtime_error(
                                "Ordered destructive replacement value is non-canonical");
                        }
                        prepared.push_back(std::move(entry));
                    }

                    OrderedElementCandidateSet candidates(bounds.existing);
                    const ClearPlan clear_plan = prepare_clear(candidates);
                    const std::vector<OrderedElementId> ids =
                        m_adapter.m_state.plan_id_range(
                            m_txn.handle(), m_origin, prepared.size(),
                            contains_origin(clear_plan.element_origins, m_origin),
                            &candidates);
                    for (std::size_t i = 0u; i < prepared.size(); ++i) {
                        prepared[i].id = ids[i];
                        prepared[i].change = m_adapter.make_append_prepared(
                            prepared[i].id, prepared[i].key_bytes,
                            prepared[i].value_bytes);
                    }

                    reserve_replacement_pending(
                        clear_plan.ids.size(), prepared.size());
                    erase_selected(clear_plan.ids, &candidates);

                    Connection::SyncCaptureSuppressionScope suppress_capture(
                        *m_adapter.m_table.connection(), m_txn.handle());
                    for (std::size_t i = 0u; i < prepared.size(); ++i) {
                        m_adapter.append_live_element_prevalidated(
                            m_txn.handle(), prepared[i].id,
                            prepared[i].source->first, prepared[i].source->second,
                            prepared[i].key_bytes, prepared[i].value_bytes);
                        m_pending.push_back(std::move(prepared[i].change));
                    }
                    if (!ids.empty()) {
                        m_adapter.m_state.commit_id_range(
                            m_txn.handle(), m_origin, ids.back().sequence);
                        ++m_mutation_revision;
                    }
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            /// \brief Commits captured mutations and delivery atomically.
            LogicalDeliveryEnvelope commit_to_outbox(
                    ILogicalDeliveryOutbox& outbox,
                    const DbId& destination,
                    const NodeId& receiver,
                    const CodecBounds* bounds = nullptr) {
                ensure_active();
                try {
                    LogicalChangeFrame frame;
                    frame.changes = m_pending;
                    const LogicalDeliveryEnvelope envelope =
                        outbox.enqueue_logical_delivery(
                            m_txn.handle(), destination, receiver, frame, bounds);
                    if (compare_node_id(envelope.origin_node_id, m_origin) != 0) {
                        throw std::runtime_error(
                            "Ordered destructive outbox origin does not match capture origin");
                    }
                    m_txn.commit();
                    m_pending.clear();
                    m_active = false;
                    return envelope;
                } catch (...) {
                    rollback_and_deactivate();
                    throw;
                }
            }

            void rollback() noexcept {
                if (m_active) rollback_and_deactivate();
            }

            std::size_t pending_size() const {
                return m_pending.size();
            }

        private:
            ClearPlan prepare_clear(OrderedElementCandidateSet& candidates) const {
                const OrderedElementStateScan state_scan =
                    m_adapter.m_state.scan_all_element_records(
                        m_txn.handle(), &candidates, true);
                std::vector<typename table_type::value_type> physical_entries;
                m_adapter.m_table.db_collect_entries(
                    physical_entries, m_txn.handle(),
                    [&candidates]() {
                        candidates.inspect_record();
                    });
                if (physical_entries.size() != state_scan.live_records.size()) {
                    throw std::runtime_error(
                        "Ordered destructive table and state counts differ");
                }
                const std::vector<OrderedElementKeyIndexEntry> index_entries =
                    m_adapter.m_state.key_index_entries(
                        m_txn.handle(), &candidates);
                std::vector<OrderedElementKeyIndexEntry> expected_index_entries;
                std::vector<std::vector<std::uint8_t> > keys;
                std::vector<std::vector<OrderedElementId> > state_ids;
                for (std::size_t i = 0u;
                     i < state_scan.live_records.size(); ++i) {
                    const std::size_t key_index = find_key_group(
                        keys, state_scan.live_records[i].record.key);
                    if (key_index == keys.size()) {
                        keys.push_back(state_scan.live_records[i].record.key);
                        state_ids.push_back(std::vector<OrderedElementId>());
                    }
                    state_ids[key_index].push_back(state_scan.live_records[i].id);
                    OrderedElementKeyIndexEntry expected;
                    expected.key = state_scan.live_records[i].record.key;
                    expected.id = state_scan.live_records[i].id;
                    expected_index_entries.push_back(expected);
                }

                validate_complete_key_index(index_entries, expected_index_entries);
                for (std::size_t i = 0u;
                     i < state_scan.element_origins.size(); ++i) {
                    m_adapter.m_state.verify_introduced_high_water(
                        m_txn.handle(), state_scan.element_origins[i], &candidates);
                }
                for (std::size_t i = 0u; i < keys.size(); ++i) {
                    const KeyT key = KeyCodec::decode(keys[i]);
                    if (KeyCodec::encode(key) != keys[i]) {
                        throw std::runtime_error(
                            "Ordered destructive state key is non-canonical");
                    }
                    validate_live_key_group(
                        key, keys[i], state_ids[i], candidates);
                }

                ClearPlan plan;
                plan.ids = candidates.sorted_ids();
                plan.element_origins = state_scan.element_origins;
                return plan;
            }

            void reserve_replacement_pending(
                    std::size_t erased_count,
                    std::size_t appended_count) {
                const std::size_t maximum =
                    (std::numeric_limits<std::size_t>::max)();
                if (erased_count > maximum - appended_count ||
                    erased_count + appended_count > maximum - m_pending.size()) {
                    throw std::length_error(
                        "Ordered destructive replacement pending frame is too large");
                }
                m_pending.reserve(
                    m_pending.size() + erased_count + appended_count);
            }

            static std::size_t find_key_group(
                    const std::vector<std::vector<std::uint8_t> >& keys,
                    const std::vector<std::uint8_t>& key) {
                for (std::size_t i = 0u; i < keys.size(); ++i) {
                    if (keys[i] == key) {
                        return i;
                    }
                }
                return keys.size();
            }

            static bool contains_origin(const std::vector<NodeId>& origins,
                                        const NodeId& origin) {
                for (std::size_t i = 0u; i < origins.size(); ++i) {
                    if (compare_node_id(origins[i], origin) == 0) {
                        return true;
                    }
                }
                return false;
            }

            static bool key_index_entry_less(
                    const OrderedElementKeyIndexEntry& lhs,
                    const OrderedElementKeyIndexEntry& rhs) {
                if (lhs.key != rhs.key) {
                    return lhs.key < rhs.key;
                }
                return OrderedElementIdLess()(lhs.id, rhs.id);
            }

            static void validate_complete_key_index(
                    std::vector<OrderedElementKeyIndexEntry> actual,
                    std::vector<OrderedElementKeyIndexEntry> expected) {
                for (std::size_t i = 0u; i < actual.size(); ++i) {
                    const KeyT key = KeyCodec::decode(actual[i].key);
                    if (KeyCodec::encode(key) != actual[i].key) {
                        throw std::runtime_error(
                            "Ordered destructive index key is non-canonical");
                    }
                }
                std::sort(actual.begin(), actual.end(), key_index_entry_less);
                std::sort(expected.begin(), expected.end(), key_index_entry_less);
                if (actual != expected) {
                    throw std::runtime_error(
                        "Ordered destructive complete key index and state differ");
                }
            }

            template<typename Selector>
            bool resolve_live_elements(
                    const KeyT& key,
                    OrderedElementCandidateSet& candidates,
                    Selector select) const {
                const std::vector<std::uint8_t> key_bytes = KeyCodec::encode(key);
                const std::vector<OrderedElementId> index_ids =
                    m_adapter.m_state.validate_live_ids_for_key(
                        m_txn.handle(), key_bytes, &candidates);
                return resolve_live_elements_from_ids(
                    key, key_bytes, index_ids, candidates, select);
            }

            template<typename Selector>
            bool resolve_live_elements_trusted(
                    const KeyT& key,
                    const OrderedElementKeyIndexProof& proof,
                    OrderedElementCandidateSet& candidates,
                    Selector select) const {
                const std::vector<std::uint8_t> key_bytes = KeyCodec::encode(key);
                if (proof.identity.get() == nullptr ||
                    proof.identity != m_identity ||
                    proof.transaction != m_txn.handle() ||
                    proof.mutation_revision != m_mutation_revision ||
                    proof.key != key_bytes) {
                    throw std::logic_error(
                        "Ordered key index proof is stale or belongs to another session");
                }
                return resolve_live_elements_from_ids(
                    key, key_bytes, proof.ids, candidates, select);
            }

            template<typename Selector>
            bool resolve_live_elements_from_ids(
                    const KeyT& key,
                    const std::vector<std::uint8_t>& key_bytes,
                    const std::vector<OrderedElementId>& index_ids,
                    OrderedElementCandidateSet& candidates,
                    Selector select) const {

                std::vector<ValueT> physical_values;
                m_adapter.m_table.db_collect_values(
                    key, physical_values, m_txn.handle(),
                    [&candidates]() {
                        candidates.inspect_record();
                    });
                if (physical_values.size() != index_ids.size()) {
                    throw std::runtime_error(
                        "Ordered destructive table and state counts differ");
                }

                std::vector<NodeId> verified_origins;
                bool selected = false;
                for (std::size_t i = 0u; i < index_ids.size(); ++i) {
                    if (!contains_origin(verified_origins, index_ids[i].origin)) {
                        m_adapter.m_state.verify_introduced_high_water(
                            m_txn.handle(), index_ids[i].origin, &candidates);
                        verified_origins.push_back(index_ids[i].origin);
                    }
                    OrderedElementStateRecord record;
                    if (!m_adapter.m_state.get(
                            m_txn.handle(), index_ids[i], record, &candidates) ||
                        !record.live || record.key != key_bytes ||
                        record.value != ValueCodec::encode(physical_values[i])) {
                        throw std::runtime_error(
                            "Ordered destructive table and state value order differs");
                    }
                    if (select(i, record)) {
                        candidates.select(index_ids[i]);
                        selected = true;
                    }
                }
                return selected;
            }

            void validate_live_key_group(
                    const KeyT& key,
                    const std::vector<std::uint8_t>& key_bytes,
                    std::vector<OrderedElementId> expected_ids,
                    OrderedElementCandidateSet& candidates) const {
                std::sort(expected_ids.begin(), expected_ids.end(),
                          OrderedElementIdLess());

                std::vector<ValueT> physical_values;
                m_adapter.m_table.db_collect_values(
                    key, physical_values, m_txn.handle(),
                    [&candidates]() {
                        candidates.inspect_record();
                    });
                if (physical_values.size() != expected_ids.size()) {
                    throw std::runtime_error(
                        "Ordered destructive table and state counts differ");
                }
                for (std::size_t i = 0u; i < expected_ids.size(); ++i) {
                    OrderedElementStateRecord record;
                    if (!m_adapter.m_state.get(
                            m_txn.handle(), expected_ids[i], record, &candidates) ||
                        !record.live || record.key != key_bytes ||
                        record.value != ValueCodec::encode(physical_values[i])) {
                        throw std::runtime_error(
                            "Ordered destructive table and state value order differs");
                    }
                }
            }

            void erase_resolved(const OrderedElementId& id,
                                OrderedElementCandidateSet* candidates) {
                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_adapter.m_table.connection(), m_txn.handle());
                const std::size_t pending_index = find_pending_append(id);
                if (pending_index != m_pending.size()) {
                    m_adapter.erase_live_element(
                        m_txn.handle(), id, false, candidates);
                    m_pending.erase(m_pending.begin() +
                        static_cast<std::ptrdiff_t>(pending_index));
                } else {
                    m_adapter.erase_live_element(
                        m_txn.handle(), id, true, candidates);
                    m_pending.push_back(m_adapter.make_erase(id));
                }
            }

            struct TrustedErase {
                OrderedElementId id;
                std::vector<std::uint8_t> key;
                std::size_t physical_index;
            };

            static bool trusted_erase_position_descending(
                    const TrustedErase& lhs,
                    const TrustedErase& rhs) {
                return lhs.physical_index > rhs.physical_index;
            }

            static bool trusted_erase_id_less(const TrustedErase& lhs,
                                              const TrustedErase& rhs) {
                return OrderedElementIdLess()(lhs.id, rhs.id);
            }

            void erase_selected(const std::vector<OrderedElementId>& ids,
                                OrderedElementCandidateSet* candidates) {
                if (candidates == nullptr) {
                    throw std::logic_error(
                        "Trusted ordered erasure requires bounded prevalidation");
                }
                std::vector<TrustedErase> resolved;
                std::vector<std::vector<std::uint8_t> > keys;
                for (std::size_t i = 0u; i < ids.size(); ++i) {
                    OrderedElementStateRecord record;
                    if (!m_adapter.m_state.get(
                            m_txn.handle(), ids[i], record, candidates) ||
                        !record.live) {
                        throw std::runtime_error(
                            "Prevalidated ordered element is not live");
                    }
                    TrustedErase entry;
                    entry.id = ids[i];
                    entry.key = record.key;
                    entry.physical_index = 0u;
                    resolved.push_back(entry);
                    if (find_key_group(keys, record.key) == keys.size()) {
                        keys.push_back(record.key);
                    }
                }

                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_adapter.m_table.connection(), m_txn.handle());
                for (std::size_t key_index = 0u;
                     key_index < keys.size(); ++key_index) {
                    const KeyT key = decode_canonical_key(keys[key_index]);
                    const std::vector<OrderedElementId> live_ids =
                        m_adapter.m_state.live_ids_for_key(
                            m_txn.handle(), keys[key_index], candidates);
                    std::vector<TrustedErase> group;
                    for (std::size_t i = 0u; i < resolved.size(); ++i) {
                        if (resolved[i].key != keys[key_index]) continue;
                        std::size_t position = live_ids.size();
                        for (std::size_t j = 0u; j < live_ids.size(); ++j) {
                            if (live_ids[j] == resolved[i].id) {
                                position = j;
                                break;
                            }
                        }
                        if (position == live_ids.size()) {
                            throw std::runtime_error(
                                "Prevalidated ordered element index is missing id");
                        }
                        resolved[i].physical_index = position;
                        group.push_back(resolved[i]);
                    }
                    std::sort(group.begin(), group.end(),
                              trusted_erase_position_descending);
                    for (std::size_t i = 0u; i < group.size(); ++i) {
                        if (!m_adapter.m_table.db_erase_at(
                                key, group[i].physical_index, m_txn.handle(),
                                [candidates]() {
                                    candidates->inspect_record();
                                })) {
                            throw std::runtime_error(
                                "Prevalidated ordered element physical value is missing");
                        }
                    }
                }

                std::sort(resolved.begin(), resolved.end(), trusted_erase_id_less);
                for (std::size_t i = 0u; i < resolved.size(); ++i) {
                    const std::size_t pending_index =
                        find_pending_append(resolved[i].id);
                    if (pending_index != m_pending.size()) {
                        m_adapter.m_state.erase_live_prevalidated(
                            m_txn.handle(), resolved[i].id, resolved[i].key);
                        m_pending.erase(m_pending.begin() +
                            static_cast<std::ptrdiff_t>(pending_index));
                    } else {
                        m_adapter.m_state.tombstone_prevalidated(
                            m_txn.handle(), resolved[i].id, resolved[i].key);
                        m_pending.push_back(m_adapter.make_erase(resolved[i].id));
                    }
                }
                ++m_mutation_revision;
            }

            std::size_t find_pending_append(const OrderedElementId& id) const {
                for (std::size_t i = 0u; i < m_pending.size(); ++i) {
                    if (m_pending[i].opcode ==
                            opcode_value(
                                KeyOrderedMultiValueDestructiveLogicalOpcode::Append) &&
                        decode_change(m_pending[i]).id == id) {
                        return i;
                    }
                }
                return m_pending.size();
            }

            void rollback_and_deactivate() noexcept {
                try {
                    m_pending.clear();
                } catch (...) {
                }
                try {
                    m_txn.rollback();
                } catch (...) {
                }
                m_active = false;
            }

            void ensure_active() const {
                if (!m_active) {
                    throw std::logic_error(
                        "KeyOrderedMultiValue destructive capture session is not active");
                }
            }

            const KeyOrderedMultiValueTableDestructiveLogicalAdapter& m_adapter;
            Transaction m_txn;
            NodeId m_origin;
            std::vector<LogicalChange> m_pending;
            std::shared_ptr<const OrderedElementKeyIndexProofIdentity> m_identity;
            bool m_active;
            std::size_t m_mutation_revision;
        };

        std::unique_ptr<LogicalCaptureSession> begin_capture_session() const & {
            return std::unique_ptr<LogicalCaptureSession>(
                new LogicalCaptureSession(*this));
        }

        std::unique_ptr<LogicalCaptureSession> begin_capture_session() const && = delete;

        LogicalApplyResult preflight(MDBX_txn* txn,
                                     const LogicalChange& change) const override {
            try {
                const DecodedChange decoded = decode_change(change);
                return validate_state(txn, decoded);
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyOrderedMultiValue destructive payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyOrderedMultiValue destructive payload is invalid");
            }
        }

        LogicalApplyResult preflight_batch(
                MDBX_txn* txn,
                const LogicalChangeBatchView& changes) const override {
            std::map<OrderedElementId, DecodedChange, OrderedElementIdLess> seen;
            bool have_virtual_high_water = false;
            std::uint64_t virtual_high_water = 0u;
            try {
                for (std::size_t i = 0u; i < changes.size(); ++i) {
                    const DecodedChange decoded = decode_change(changes[i]);
                    const typename std::map<OrderedElementId, DecodedChange,
                        OrderedElementIdLess>::const_iterator prior =
                        seen.find(decoded.id);
                    if (prior != seen.end()) {
                        return LogicalApplyResult::failure(
                            "Duplicate OrderedElementId in destructive logical batch");
                    }
                    const LogicalApplyResult state_result =
                        validate_state(txn, decoded);
                    if (!state_result.ok) return state_result;
                    OrderedElementStateRecord existing;
                    const bool exists = m_state.get(txn, decoded.id, existing);
                    if (decoded.is_append && !exists) {
                        if (!have_virtual_high_water) {
                            virtual_high_water = m_state.highest_introduced(
                                txn, decoded.id.origin);
                            have_virtual_high_water = true;
                        }
                        if (decoded.id.sequence <= virtual_high_water) {
                            return LogicalApplyResult::failure(
                                "OrderedElementId is not increasing in destructive logical batch");
                        }
                        virtual_high_water = decoded.id.sequence;
                    }
                    seen.insert(std::make_pair(decoded.id, decoded));
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyOrderedMultiValue destructive payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyOrderedMultiValue destructive payload is invalid");
            }
            return LogicalApplyResult::success();
        }

        LogicalApplyResult apply(MDBX_txn* txn,
                                 const LogicalChange& change) override {
            try {
                const DecodedChange decoded = decode_change(change);
                Connection::SyncCaptureSuppressionScope suppress_capture(
                    *m_table.connection(), txn);
                if (decoded.is_append) {
                    OrderedElementStateRecord existing;
                    if (m_state.get(txn, decoded.id, existing)) {
                        if (existing.live && existing.key == decoded.key_bytes &&
                            existing.value == decoded.value_bytes) {
                            return LogicalApplyResult::success();
                        }
                        throw std::runtime_error(
                            "OrderedElementId conflicts with persisted state");
                    }
                    append_live_element(txn, decoded.id, decoded.key, decoded.value);
                } else {
                    erase_live_element(txn, decoded.id, true);
                }
                return LogicalApplyResult::success();
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyOrderedMultiValue destructive apply failed: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyOrderedMultiValue destructive apply failed");
            }
        }

        const OrderedElementStateStore& state_store() const {
            return m_state;
        }

    private:
#include "KeyOrderedMultiValueTableDestructiveLogicalAdapter.ipp"

    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_LOGICAL_ADAPTERS_KEY_ORDERED_MULTI_VALUE_TABLE_DESTRUCTIVE_LOGICAL_ADAPTER_HPP_INCLUDED
