        typedef detail::BlobPayloadCursor PayloadCursor;

        struct DecodedChange {
            OrderedElementId id;
            bool is_append;
            KeyT key;
            ValueT value;
            std::vector<std::uint8_t> key_bytes;
            std::vector<std::uint8_t> value_bytes;

            DecodedChange() : is_append(false) {}
        };

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::vector<std::uint8_t>& bytes) {
            detail::append_blob_payload(
                out, bytes, "KeyOrderedMultiValue destructive");
        }

        static std::vector<std::uint8_t> read_blob(PayloadCursor& cursor) {
            return cursor.read_blob("KeyOrderedMultiValue destructive");
        }

        static PayloadCursor make_cursor(
                const std::vector<std::uint8_t>& payload) {
            return PayloadCursor(payload);
        }

        static std::vector<std::uint8_t> read_id(PayloadCursor& cursor) {
            const std::size_t size = NodeId().size() + 8u;
            return cursor.read_bytes(
                size, "KeyOrderedMultiValue destructive");
        }

        static void require_end(const PayloadCursor& cursor) {
            cursor.ensure_end("KeyOrderedMultiValue destructive");
        }

        static KeyT decode_canonical_key(const std::vector<std::uint8_t>& bytes) {
            const KeyT key = KeyCodec::decode(bytes);
            if (KeyCodec::encode(key) != bytes) {
                throw std::runtime_error(
                    "KeyOrderedMultiValue destructive key is non-canonical");
            }
            return key;
        }

        static ValueT decode_canonical_value(
                const std::vector<std::uint8_t>& bytes) {
            const ValueT value = ValueCodec::decode(bytes);
            if (ValueCodec::encode(value) != bytes) {
                throw std::runtime_error(
                    "KeyOrderedMultiValue destructive value is non-canonical");
            }
            return value;
        }

        static void encode_append(const OrderedElementId& id,
                                  const KeyT& key,
                                  const ValueT& value,
                                  std::vector<std::uint8_t>& out) {
            encode_append_bytes(id, KeyCodec::encode(key), ValueCodec::encode(value),
                                out);
        }

        static void encode_append_bytes(
                const OrderedElementId& id,
                const std::vector<std::uint8_t>& key_bytes,
                const std::vector<std::uint8_t>& value_bytes,
                std::vector<std::uint8_t>& out) {
            out = encode_ordered_element_id_logical(id);
            append_blob(out, key_bytes);
            append_blob(out, value_bytes);
        }

        static DecodedChange decode_change(const LogicalChange& change) {
            DecodedChange out;
            if (change.opcode == opcode_value(
                    KeyOrderedMultiValueDestructiveLogicalOpcode::Erase)) {
                out.id = decode_ordered_element_id_logical(change.payload);
                return out;
            }
            if (change.opcode != opcode_value(
                    KeyOrderedMultiValueDestructiveLogicalOpcode::Append)) {
                throw std::runtime_error(
                    "KeyOrderedMultiValue destructive opcode is unsupported");
            }
            PayloadCursor cursor = make_cursor(change.payload);
            out.id = decode_ordered_element_id_logical(read_id(cursor));
            out.key_bytes = read_blob(cursor);
            out.value_bytes = read_blob(cursor);
            require_end(cursor);
            out.key = decode_canonical_key(out.key_bytes);
            out.value = decode_canonical_value(out.value_bytes);
            out.is_append = true;
            return out;
        }

        LogicalApplyResult validate_state(MDBX_txn* txn,
                                          const DecodedChange& decoded) const {
            const LogicalApplyResult marker_result =
                validate_logical_adapter_marker(
                    txn, m_table.connection()->env_handle(), *this);
            if (!marker_result.ok) return marker_result;
            if (decoded.is_append) {
                m_state.verify_introduced_high_water(txn, decoded.id.origin);
                SchemaRegistryStore schemas(m_table.connection()->env_handle());
                LogicalSchemaRecord marker;
                if (!schemas.get(txn, m_schema_id, marker) ||
                    compare_node_id(marker.ordered_delivery_origin_node_id,
                                    decoded.id.origin) != 0) {
                    return LogicalApplyResult::failure(
                        "Append OrderedElementId origin does not match schema marker");
                }
            }
            OrderedElementStateRecord record;
            const bool exists = m_state.get(txn, decoded.id, record);
            const std::uint64_t introduced =
                m_state.highest_introduced(txn, decoded.id.origin);
            if (exists && decoded.id.sequence > introduced) {
                return LogicalApplyResult::failure(
                    "Ordered element introduced high-water mark is corrupt");
            }
            if (decoded.is_append && exists) {
                if (!record.live) {
                    return LogicalApplyResult::failure(
                        "OrderedElementId is tombstoned");
                }
                if (record.key != decoded.key_bytes ||
                    record.value != decoded.value_bytes) {
                    return LogicalApplyResult::failure(
                        "OrderedElementId conflicts with persisted state");
                }
            }
            if (decoded.is_append && !exists &&
                decoded.id.sequence <= introduced) {
                return LogicalApplyResult::failure(
                    "OrderedElementId is not above the introduced high-water mark");
            }
            if (!decoded.is_append && (!exists || !record.live)) {
                return LogicalApplyResult::failure(
                    "OrderedElementId is not live");
            }
            if (decoded.is_append) {
                ensure_key_parity(txn, decoded.key, decoded.key_bytes);
            } else {
                ensure_key_parity(txn, decode_canonical_key(record.key),
                                  record.key);
            }
            return LogicalApplyResult::success();
        }

        void append_live_element(MDBX_txn* txn,
                                 const OrderedElementId& id,
                                 const KeyT& key,
                                 const ValueT& value) const {
            const std::vector<std::uint8_t> key_bytes = KeyCodec::encode(key);
            const std::vector<std::uint8_t> value_bytes = ValueCodec::encode(value);
            m_state.verify_introduced_high_water(txn, id.origin);
            m_table.append(key, value, txn);
            m_state.put_live(txn, id, key_bytes, value_bytes);
            ensure_key_parity(txn, key, key_bytes);
        }

        void append_live_element_prevalidated(
                MDBX_txn* txn,
                const OrderedElementId& id,
                const KeyT& key,
                const ValueT& value,
                const std::vector<std::uint8_t>& key_bytes,
                const std::vector<std::uint8_t>& value_bytes) const {
            m_table.append(key, value, txn);
            m_state.put_live_prevalidated(txn, id, key_bytes, value_bytes);
        }

        void erase_live_element(MDBX_txn* txn,
                                const OrderedElementId& id,
                                bool preserve_tombstone,
                                OrderedElementCandidateSet* candidates = nullptr) const {
            OrderedElementStateRecord record;
            if (!m_state.get(txn, id, record, candidates) || !record.live) {
                throw std::runtime_error("Ordered element is not live");
            }
            const KeyT key = decode_canonical_key(record.key);
            const std::vector<OrderedElementId> ids =
                m_state.live_ids_for_key(txn, record.key, candidates);
            std::size_t index = ids.size();
            for (std::size_t i = 0u; i < ids.size(); ++i) {
                if (ids[i] == id) {
                    index = i;
                    break;
                }
            }
            if (index == ids.size()) {
                throw std::runtime_error("Ordered element key index is missing id");
            }
            bool erased = false;
            if (candidates == nullptr) {
                erased = m_table.erase_at(key, index, txn);
            } else {
                erased = m_table.db_erase_at(
                    key, index, txn,
                    [candidates]() {
                        candidates->inspect_record();
                    });
            }
            if (!erased) {
                throw std::runtime_error("Ordered element physical value is missing");
            }
            if (preserve_tombstone) {
                m_state.tombstone(txn, id, candidates);
            } else {
                m_state.erase_live(txn, id, candidates);
            }
            ensure_key_parity(txn, key, record.key, candidates);
        }

        void ensure_key_parity(MDBX_txn* txn,
                               const KeyT& key,
                               const std::vector<std::uint8_t>& key_bytes,
                               OrderedElementCandidateSet* candidates = nullptr) const {
            std::vector<ValueT> values;
            if (candidates == nullptr) {
                values = m_table.find(key, txn);
            } else {
                m_table.db_collect_values(
                    key, values, txn,
                    [candidates]() {
                        candidates->inspect_record();
                    });
            }
            std::vector<OrderedElementId> ids =
                m_state.live_ids_for_key(txn, key_bytes, candidates);
            std::vector<OrderedElementId> state_ids =
                m_state.live_state_ids_for_key(txn, key_bytes, candidates);
            std::sort(ids.begin(), ids.end(), OrderedElementIdLess());
            std::sort(state_ids.begin(), state_ids.end(), OrderedElementIdLess());
            if (values.size() != ids.size() || ids != state_ids) {
                throw std::runtime_error(
                    "Ordered element state, index, and table counts differ");
            }
            for (std::size_t i = 0u; i < ids.size(); ++i) {
                OrderedElementStateRecord record;
                if (!m_state.get(txn, ids[i], record, candidates) || !record.live ||
                    record.key != key_bytes ||
                    record.value != ValueCodec::encode(values[i])) {
                    throw std::runtime_error(
                        "Ordered element state and table value order differs");
                }
            }
        }

        table_type& m_table;
        std::string m_schema_id;
        std::uint32_t m_schema_version;
        OrderedElementStateStore m_state;
