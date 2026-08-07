        typedef detail::BlobPayloadCursor PayloadCursor;

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::vector<std::uint8_t>& value) {
            detail::append_blob_payload(out, value, "KeyTable logical");
        }

        static std::vector<std::uint8_t> read_blob(PayloadCursor& cursor) {
            return cursor.read_blob("KeyTable logical");
        }

        static PayloadCursor make_cursor(
                const std::vector<std::uint8_t>& payload) {
            return PayloadCursor(payload);
        }

        static void ensure_end(const PayloadCursor& cursor) {
            cursor.ensure_end("KeyTable logical");
        }

        static void encode_key_only(const KeyT& key,
                                    std::vector<std::uint8_t>& out) {
            out.clear();
            const std::vector<std::uint8_t> encoded_key =
                KeyCodec::encode(key);
            append_blob(out, encoded_key);
        }

        static KeyT decode_key_only(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const std::vector<std::uint8_t> encoded_key = read_blob(cursor);
            ensure_end(cursor);
            return KeyCodec::decode(encoded_key);
        }

        LogicalApplyResult validate_payload(
                const LogicalChange& change) const {
            try {
                if (change.opcode == opcode_value(KeyTableLogicalOpcode::Insert) ||
                    change.opcode == opcode_value(KeyTableLogicalOpcode::Delete)) {
                    (void)decode_key_only(change.payload);
                    return LogicalApplyResult::success();
                }
                if (change.opcode == opcode_value(KeyTableLogicalOpcode::Clear)) {
                    if (!change.payload.empty()) {
                        return LogicalApplyResult::failure(
                            "KeyTable clear payload must be empty");
                    }
                    return LogicalApplyResult::success();
                }
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string("KeyTable logical payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyTable logical payload is invalid");
            }
            return LogicalApplyResult::failure(
                "KeyTable logical adapter opcode is unsupported");
        }

        table_type& m_table;
        std::string m_schema_id;
        std::uint32_t m_schema_version;
