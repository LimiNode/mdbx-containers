        typedef detail::BlobPayloadCursor PayloadCursor;

        static void append_blob(
                std::vector<std::uint8_t>& out,
                const std::vector<std::uint8_t>& value) {
            detail::append_blob_payload(out, value, "KeyValue logical");
        }

        static std::vector<std::uint8_t> read_blob(PayloadCursor& cursor) {
            return cursor.read_blob("KeyValue logical");
        }

        static PayloadCursor make_cursor(
                const std::vector<std::uint8_t>& payload) {
            return PayloadCursor(payload);
        }

        static void ensure_end(const PayloadCursor& cursor) {
            cursor.ensure_end("KeyValue logical");
        }

        static void encode_key_only(const KeyT& key,
                                    std::vector<std::uint8_t>& out) {
            out.clear();
            const std::vector<std::uint8_t> encoded_key =
                KeyCodec::encode(key);
            append_blob(out, encoded_key);
        }

        MDBX_txn* checked_adapter_txn(MDBX_txn* txn,
                                      const char* context) const {
            return checked_txn_env(txn, m_table.connection()->env_handle(),
                                   context);
        }

        static void encode_upsert(const KeyT& key,
                                  const ValueT& value,
                                  std::vector<std::uint8_t>& out) {
            out.clear();
            const std::vector<std::uint8_t> encoded_key =
                KeyCodec::encode(key);
            const std::vector<std::uint8_t> encoded_value =
                ValueCodec::encode(value);
            append_blob(out, encoded_key);
            append_blob(out, encoded_value);
        }

        static KeyT decode_key_only(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const std::vector<std::uint8_t> encoded_key = read_blob(cursor);
            ensure_end(cursor);
            return KeyCodec::decode(encoded_key);
        }

        static std::pair<KeyT, ValueT> decode_upsert(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const std::vector<std::uint8_t> encoded_key = read_blob(cursor);
            const std::vector<std::uint8_t> encoded_value = read_blob(cursor);
            ensure_end(cursor);
            return std::make_pair(
                KeyCodec::decode(encoded_key),
                ValueCodec::decode(encoded_value));
        }

        LogicalApplyResult validate_payload(
                const LogicalChange& change) const {
            try {
                if (change.opcode == opcode_value(KeyValueTableLogicalOpcode::Upsert)) {
                    (void)decode_upsert(change.payload);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == opcode_value(KeyValueTableLogicalOpcode::Delete)) {
                    (void)decode_key_only(change.payload);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == opcode_value(KeyValueTableLogicalOpcode::Clear)) {
                    if (!change.payload.empty()) {
                        return LogicalApplyResult::failure(
                            "KeyValue clear payload must be empty");
                    }
                    return LogicalApplyResult::success();
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyValue logical payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyValue logical payload is invalid");
            }
            return LogicalApplyResult::failure(
                "KeyValue logical adapter opcode is unsupported");
        }

        table_type& m_table;
        std::string m_schema_id;
        std::uint32_t m_schema_version;
