        static const std::uint8_t payload_version = 1u;

        typedef detail::BlobPayloadCursor PayloadCursor;

        struct DecodedChange {
            std::uint32_t opcode;
            std::uint64_t id;
            Embedding embedding;
            std::string text;
            std::string metadata_json;
        };

        enum class RecordStateKind {
            Unused,
            Live,
            Erased,
            Corrupt
        };

        struct RecordState {
            bool ids;
            bool embeddings;
            bool texts;
            bool metadata;

            RecordState()
                : ids(false), embeddings(false), texts(false), metadata(false) {}

            bool any() const {
                return ids || embeddings || texts || metadata;
            }

            bool complete() const {
                return ids && embeddings && texts && metadata;
            }

            RecordStateKind kind() const {
                if (!any()) return RecordStateKind::Unused;
                if (complete()) return RecordStateKind::Live;
                if (ids && !embeddings && !texts && !metadata) {
                    return RecordStateKind::Erased;
                }
                return RecordStateKind::Corrupt;
            }
        };

        struct BatchPreflightState {
            BatchPreflightState()
                : collection_dimension(0u),
                  live_embedding_count(0u),
                  collection_cleared(false) {}

            std::uint32_t collection_dimension;
            std::size_t live_embedding_count;
            bool collection_cleared;
            std::map<std::uint64_t, RecordStateKind> record_states;
        };

        static std::uint8_t read_u8(PayloadCursor& cursor) {
            return cursor.read_u8("VectorStore logical");
        }

        static std::uint64_t read_u64(PayloadCursor& cursor) {
            return cursor.read_u64_le("VectorStore logical");
        }

        static std::vector<std::uint8_t> read_blob(PayloadCursor& cursor) {
            return cursor.read_blob("VectorStore logical");
        }

        static void validate_blob_size(std::size_t size, const char* label) {
            if (size > static_cast<std::size_t>(
                           (std::numeric_limits<std::uint32_t>::max)())) {
                throw std::length_error(
                    std::string("VectorStore logical ") + label +
                    " is too large");
            }
        }

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::vector<std::uint8_t>& value) {
            detail::append_blob_payload(out, value, "VectorStore logical");
        }

        static void append_blob(std::vector<std::uint8_t>& out,
                                const std::string& value) {
            detail::append_blob_payload(out, value, "VectorStore logical");
        }

        static void append_u8(std::vector<std::uint8_t>& out,
                              std::uint8_t value) {
            out.push_back(value);
        }

        static LogicalChange make_add_from_bytes_impl(
                const LogicalSchemaRef& schema,
                std::uint64_t id,
                const std::vector<std::uint8_t>& embedding_bytes,
                const std::string& text,
                const std::string& metadata_json) {
            validate_blob_size(embedding_bytes.size(), "embedding payload");
            validate_blob_size(text.size(), "text payload");
            validate_blob_size(metadata_json.size(), "metadata payload");
            LogicalChange change;
            change.schema = schema;
            change.opcode = opcode_value(VectorStoreLogicalOpcode::Add);
            append_u8(change.payload, payload_version);
            detail::append_u64_le(change.payload, id);
            append_blob(change.payload, embedding_bytes);
            append_blob(change.payload, text);
            append_blob(change.payload, metadata_json);
            return change;
        }

        LogicalChange make_add_from_bytes(
                std::uint64_t id,
                const std::vector<std::uint8_t>& embedding_bytes,
                const std::string& text,
                const std::string& metadata_json) const {
            return make_add_from_bytes_impl(schema_ref(), id, embedding_bytes,
                                            text, metadata_json);
        }

        DecodedChange decode_change(const LogicalChange& change) const {
            if (change.flags != 0u || change.schema.schema_id != m_schema_id ||
                change.schema.kind != LogicalTableKind::VectorStore ||
                change.schema.schema_version != m_schema_version) {
                throw std::runtime_error(
                    "VectorStore logical schema or flags mismatch");
            }
            DecodedChange decoded;
            decoded.opcode = change.opcode;
            decoded.id = 0u;
            PayloadCursor cursor(change.payload);
            if (change.opcode == opcode_value(VectorStoreLogicalOpcode::Clear)) {
                if (!change.payload.empty()) {
                    throw std::runtime_error(
                        "VectorStore logical clear payload is not empty");
                }
                return decoded;
            }
            if (change.opcode != opcode_value(VectorStoreLogicalOpcode::Add) &&
                change.opcode != opcode_value(VectorStoreLogicalOpcode::Erase)) {
                throw std::runtime_error(
                    "VectorStore logical opcode is unsupported");
            }
            if (read_u8(cursor) != payload_version) {
                throw std::runtime_error(
                    "VectorStore logical payload version is unsupported");
            }
            decoded.id = read_u64(cursor);
            if (change.opcode == opcode_value(VectorStoreLogicalOpcode::Add)) {
                const std::vector<std::uint8_t> embedding_bytes =
                    read_blob(cursor);
                decoded.embedding = Embedding::from_bytes(
                    embedding_bytes.empty() ? nullptr : &embedding_bytes[0],
                    embedding_bytes.size());
                const std::vector<std::uint8_t> text_bytes = read_blob(cursor);
                const std::vector<std::uint8_t> metadata_bytes =
                    read_blob(cursor);
                decoded.text = std::string(text_bytes.begin(), text_bytes.end());
                decoded.metadata_json = std::string(
                    metadata_bytes.begin(), metadata_bytes.end());
            }
            cursor.ensure_end("VectorStore logical");
            return decoded;
        }

        RecordState record_state(MDBX_txn* txn, std::uint64_t id) const {
            RecordState state;
            state.ids = m_store.m_ids.contains(id, txn);
            state.embeddings = m_store.m_embeddings.contains(id, txn);
            state.texts = m_store.m_texts.contains(id, txn);
            state.metadata = m_store.m_metadata.contains(id, txn);
            return state;
        }

        void validate_collection_dimension(
                MDBX_txn* txn,
                const Embedding& incoming) const {
            incoming.validate();
            std::uint32_t collection_dimension = 0u;
            validate_collection_dimension(txn, collection_dimension);
            if (collection_dimension != 0u &&
                incoming.dim != collection_dimension) {
                throw std::invalid_argument(
                    "Embedding dimension does not match collection dimension");
            }
        }

        void validate_collection_dimension(
                MDBX_txn* txn,
                std::uint32_t& collection_dimension) const {
            std::size_t live_embedding_count = 0u;
            validate_collection_dimension(
                txn, collection_dimension, live_embedding_count);
        }

        void validate_collection_dimension(
                MDBX_txn* txn,
                std::uint32_t& collection_dimension,
                std::size_t& live_embedding_count) const {
            validate_collection_state(
                txn, collection_dimension, live_embedding_count);
        }

        void validate_collection_state(MDBX_txn* txn) const {
            std::uint32_t collection_dimension = 0u;
            std::size_t live_embedding_count = 0u;
            validate_collection_state(
                txn, collection_dimension, live_embedding_count);
        }

        void validate_collection_state(
                MDBX_txn* txn,
                std::uint32_t& collection_dimension,
                std::size_t& live_embedding_count) const {
            std::vector<std::pair<std::uint64_t, std::uint64_t> > ids;
            std::vector<std::pair<std::uint64_t, Embedding> > entries;
            std::vector<std::pair<std::uint64_t, std::string> > texts;
            std::vector<std::pair<std::uint64_t, std::string> > metadata;
            m_store.m_ids.load_entries(ids, txn);
            m_store.m_embeddings.load(entries, txn);
            m_store.m_texts.load(texts, txn);
            m_store.m_metadata.load(metadata, txn);

            std::map<std::uint64_t, std::uint8_t> record_parts;
            std::map<std::uint64_t, const Embedding*> embeddings_by_id;
            for (std::size_t i = 0u; i < ids.size(); ++i) {
                record_parts[ids[i].first] |= 0x01u;
            }
            for (std::size_t i = 0u; i < entries.size(); ++i) {
                record_parts[entries[i].first] |= 0x02u;
                embeddings_by_id[entries[i].first] = &entries[i].second;
            }
            for (std::size_t i = 0u; i < texts.size(); ++i) {
                record_parts[texts[i].first] |= 0x04u;
            }
            for (std::size_t i = 0u; i < metadata.size(); ++i) {
                record_parts[metadata[i].first] |= 0x08u;
            }

            collection_dimension = 0u;
            live_embedding_count = 0u;
            for (std::size_t i = 0u; i < entries.size(); ++i) {
                entries[i].second.validate();
            }
            for (std::map<std::uint64_t, std::uint8_t>::const_iterator it =
                     record_parts.begin(); it != record_parts.end(); ++it) {
                const std::uint8_t parts = it->second;
                if (parts == 0x01u) continue;
                if (parts != 0x0fu) {
                    throw std::runtime_error(
                        "VectorStore logical record is partially present");
                }
                const std::map<std::uint64_t, const Embedding*>::const_iterator embedding_it =
                    embeddings_by_id.find(it->first);
                if (embedding_it == embeddings_by_id.end()) {
                    throw std::runtime_error(
                        "VectorStore logical embedding state is inconsistent");
                }
                const Embedding& embedding = *embedding_it->second;
                if (collection_dimension == 0u) {
                    collection_dimension = embedding.dim;
                } else if (embedding.dim != collection_dimension) {
                    throw std::runtime_error(
                        "VectorStore collection has mixed embedding dimensions");
                }
                ++live_embedding_count;
            }
        }

        LogicalApplyResult preflight_decoded_change(
                MDBX_txn* txn,
                const DecodedChange& decoded) const {
            if (decoded.opcode == opcode_value(VectorStoreLogicalOpcode::Clear)) {
                return LogicalApplyResult::success();
            }

            const RecordStateKind state_kind =
                record_state(txn, decoded.id).kind();
            if (state_kind == RecordStateKind::Corrupt) {
                return LogicalApplyResult::failure(
                    "VectorStore logical record is partially present");
            }
            if (decoded.opcode == opcode_value(VectorStoreLogicalOpcode::Add)) {
                decoded.embedding.validate();
                if (state_kind == RecordStateKind::Live) {
                    return LogicalApplyResult::failure(
                        "VectorStore logical record id already exists");
                }
                if (state_kind == RecordStateKind::Erased) {
                    return LogicalApplyResult::failure(
                        "VectorStore logical record id was erased");
                }
                validate_collection_dimension(txn, decoded.embedding);
            }
            return LogicalApplyResult::success();
        }

        RecordStateKind batch_record_state(
                MDBX_txn* txn,
                std::uint64_t id,
                const BatchPreflightState& state) const {
            const std::map<std::uint64_t, RecordStateKind>::const_iterator found =
                state.record_states.find(id);
            if (found != state.record_states.end()) return found->second;
            if (state.collection_cleared) return RecordStateKind::Unused;
            return record_state(txn, id).kind();
        }

        LogicalApplyResult preflight_batch_decoded_change(
                MDBX_txn* txn,
                const DecodedChange& decoded,
                BatchPreflightState& state) const {
            if (decoded.opcode == opcode_value(VectorStoreLogicalOpcode::Clear)) {
                state.collection_dimension = 0u;
                state.live_embedding_count = 0u;
                state.collection_cleared = true;
                state.record_states.clear();
                return LogicalApplyResult::success();
            }

            const RecordStateKind state_kind =
                batch_record_state(txn, decoded.id, state);
            if (state_kind == RecordStateKind::Corrupt) {
                return LogicalApplyResult::failure(
                    "VectorStore logical record is partially present");
            }
            if (decoded.opcode == opcode_value(VectorStoreLogicalOpcode::Add)) {
                decoded.embedding.validate();
                if (state_kind == RecordStateKind::Live) {
                    return LogicalApplyResult::failure(
                        "VectorStore logical record id already exists");
                }
                if (state_kind == RecordStateKind::Erased) {
                    return LogicalApplyResult::failure(
                        "VectorStore logical record id was erased");
                }
                if (state.collection_dimension != 0u &&
                    decoded.embedding.dim != state.collection_dimension) {
                    return LogicalApplyResult::failure(
                        "Embedding dimension does not match collection dimension");
                }
                state.collection_dimension = decoded.embedding.dim;
                ++state.live_embedding_count;
                state.record_states[decoded.id] = RecordStateKind::Live;
                return LogicalApplyResult::success();
            }

            if (state_kind == RecordStateKind::Live) {
                if (state.live_embedding_count == 0u) {
                    return LogicalApplyResult::failure(
                        "VectorStore logical live embedding count is invalid");
                }
                --state.live_embedding_count;
                if (state.live_embedding_count == 0u) {
                    state.collection_dimension = 0u;
                }
                state.record_states[decoded.id] = RecordStateKind::Erased;
            }
            return LogicalApplyResult::success();
        }

        LogicalApplyResult preflight_batch_locked(
                MDBX_txn* txn,
                const LogicalChangeBatchView& changes) const {
            BatchPreflightState state;
            validate_collection_state(
                txn, state.collection_dimension, state.live_embedding_count);
            for (std::size_t i = 0u; i < changes.size(); ++i) {
                const DecodedChange decoded = decode_change(changes[i]);
                const LogicalApplyResult result = preflight_batch_decoded_change(
                    txn, decoded, state);
                if (!result.ok) return result;
            }
            return LogicalApplyResult::success();
        }

        void verify_table_names() const {
            const std::vector<std::string> names = affected_dbis();
            for (std::size_t i = 0; i < names.size(); ++i) {
                if (names[i].empty()) {
                    throw std::invalid_argument(
                        "VectorStore logical adapter DBI name is empty");
                }
            }
        }

        void initialize_storage(MDBX_txn* txn) const override {
            (void)txn;
            verify_table_names();
        }

        void verify_storage(MDBX_txn* txn) const override {
            verify_table_names();
            validate_collection_state(txn);
        }

        VectorStore& m_store;
        std::string m_schema_id;
        std::uint32_t m_schema_version;
