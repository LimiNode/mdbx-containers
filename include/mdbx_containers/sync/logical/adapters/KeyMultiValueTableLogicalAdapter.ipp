        typedef detail::BlobPayloadCursor PayloadCursor;

        LogicalChange make_pair_change(std::uint32_t opcode,
                                        const KeyT& key,
                                        const ValueT& value) const {
            LogicalChange change;
            change.schema = schema_ref();
            change.opcode = opcode;
            encode_pair(key, value, change.payload);
            return change;
        }

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::vector<std::uint8_t>& bytes) {
            detail::append_blob_payload(out, bytes, "KeyMultiValue logical");
        }

        static std::vector<std::uint8_t> read_blob(PayloadCursor& cursor) {
            return cursor.read_blob("KeyMultiValue logical");
        }

        static PayloadCursor make_cursor(
                const std::vector<std::uint8_t>& payload) {
            return PayloadCursor(payload);
        }

        static void ensure_end(const PayloadCursor& cursor) {
            cursor.ensure_end("KeyMultiValue logical");
        }

        static void encode_key(const KeyT& key,
                               std::vector<std::uint8_t>& out) {
            out.clear();
            append_blob(out, KeyCodec::encode(key));
        }

        static void encode_pair(const KeyT& key,
                                const ValueT& value,
                                std::vector<std::uint8_t>& out) {
            out.clear();
            append_blob(out, KeyCodec::encode(key));
            append_blob(out, ValueCodec::encode(value));
        }

        static KeyT decode_key(const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const KeyT key = KeyCodec::decode(read_blob(cursor));
            ensure_end(cursor);
            return key;
        }

        static std::pair<KeyT, ValueT> decode_pair(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const KeyT key = KeyCodec::decode(read_blob(cursor));
            const ValueT value = ValueCodec::decode(read_blob(cursor));
            ensure_end(cursor);
            return std::make_pair(key, value);
        }

        void require_schema_v2() const {
            if (m_schema_version < schema_version_v2) {
                throw std::logic_error(
                    "KeyMultiValue exact-one erase requires schema version 2 or newer");
            }
        }

        void require_schema_v3() const {
            if (m_schema_version < schema_version_v3) {
                throw std::logic_error(
                    "KeyMultiValue range erase requires schema version 3 or newer");
            }
        }

        LogicalApplyResult validate_payload(
                const LogicalChange& change) const {
            try {
                if (change.opcode == opcode_value(KeyMultiValueLogicalOpcode::InsertOne) ||
                    change.opcode == opcode_value(KeyMultiValueLogicalOpcode::EraseAllValues) ||
                    change.opcode == opcode_value(KeyMultiValueLogicalOpcode::EraseOneValue)) {
                    if (change.opcode == opcode_value(KeyMultiValueLogicalOpcode::EraseOneValue) &&
                        m_schema_version < schema_version_v2) {
                        return LogicalApplyResult::failure(
                            "KeyMultiValue exact-one erase requires schema version 2 or newer");
                    }
                    (void)decode_pair(change.payload);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == opcode_value(KeyMultiValueLogicalOpcode::EraseKey)) {
                    (void)decode_key(change.payload);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == opcode_value(KeyMultiValueLogicalOpcode::Clear)) {
                    if (!change.payload.empty()) {
                        return LogicalApplyResult::failure(
                            "KeyMultiValue logical clear payload must be empty");
                    }
                    return LogicalApplyResult::success();
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyMultiValue logical payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyMultiValue logical payload is invalid");
            }
            return LogicalApplyResult::failure(
                "KeyMultiValue logical adapter opcode is unsupported");
        }

        table_type& m_table;
        std::string m_schema_id;
        std::uint32_t m_schema_version;
