        typedef detail::BlobPayloadCursor PayloadCursor;

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::vector<std::uint8_t>& bytes) {
            detail::append_blob_payload(
                out, bytes, "KeyOrderedMultiValue logical");
        }

        static std::vector<std::uint8_t> read_blob(PayloadCursor& cursor) {
            return cursor.read_blob("KeyOrderedMultiValue logical");
        }

        static PayloadCursor make_cursor(
                const std::vector<std::uint8_t>& payload) {
            return PayloadCursor(payload);
        }

        static void ensure_end(const PayloadCursor& cursor) {
            cursor.ensure_end("KeyOrderedMultiValue logical");
        }

        static void encode_pair(const KeyT& key,
                                const ValueT& value,
                                std::vector<std::uint8_t>& out) {
            out.clear();
            append_blob(out, KeyCodec::encode(key));
            append_blob(out, ValueCodec::encode(value));
        }

        static std::pair<KeyT, ValueT> decode_pair(
                const std::vector<std::uint8_t>& payload) {
            PayloadCursor cursor = make_cursor(payload);
            const KeyT key = KeyCodec::decode(read_blob(cursor));
            const ValueT value = ValueCodec::decode(read_blob(cursor));
            ensure_end(cursor);
            return std::make_pair(key, value);
        }

        static LogicalApplyResult validate_payload(
                const LogicalChange& change) {
            if (change.opcode != opcode_value(KeyOrderedMultiValueLogicalOpcode::AppendOne)) {
                return LogicalApplyResult::failure(
                    "KeyOrderedMultiValue logical adapter opcode is unsupported");
            }
            try {
                (void)decode_pair(change.payload);
                return LogicalApplyResult::success();
            } catch (const std::exception& e) {
                return LogicalApplyResult::failure(
                    std::string(
                        "KeyOrderedMultiValue logical payload is invalid: ") +
                    e.what());
            } catch (...) {
                return LogicalApplyResult::failure(
                    "KeyOrderedMultiValue logical payload is invalid");
            }
        }

        table_type& m_table;
        std::string m_schema_id;
        std::uint32_t m_schema_version;
