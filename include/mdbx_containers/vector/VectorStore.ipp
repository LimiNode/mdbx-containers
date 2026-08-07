#include <algorithm>
#include <stdexcept>
#include <utility>
#include <vector>

namespace mdbxc {

    inline std::shared_ptr<Connection> VectorStore::require_connection(std::shared_ptr<Connection> connection) {
        if (!connection) {
            throw std::invalid_argument("VectorStore connection cannot be null");
        }
        return connection;
    }

    inline std::string VectorStore::validate_collection_name(const std::string& name) {
        if (name.empty()) {
            throw std::invalid_argument("Collection name cannot be empty");
        }
        for (std::size_t i = 0; i < name.size(); ++i) {
            const char c = name[i];
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') || c == '_' || c == '-') {
                continue;
            }
            throw std::invalid_argument(
                "Collection name contains unsupported character");
        }
        return name;
    }

    inline std::string VectorStore::make_table_name(const std::string& collection,
                                                      const std::string& suffix) {
        return "vectors_" + collection + "_" + suffix;
    }

    inline VectorStore::VectorStore(const Config& config,
                                      std::string collection,
                                      VectorMetric metric)
        : VectorStore(Connection::create(config), std::move(collection), metric,
                      MDBX_DB_DEFAULTS | MDBX_CREATE) {}

    inline VectorStore::VectorStore(std::shared_ptr<Connection> connection,
                                      std::string collection,
                                      VectorMetric metric)
        : VectorStore(std::move(connection), std::move(collection), metric,
                      MDBX_DB_DEFAULTS | MDBX_CREATE) {}

    inline VectorStore::VectorStore(std::shared_ptr<Connection> connection,
                                      std::string collection,
                                      VectorMetric metric,
                                      MDBX_db_flags_t table_flags)
        : m_collection(validate_collection_name(collection))
        , m_metric(metric)
        , m_connection(require_connection(std::move(connection)))
        , m_ids(m_connection, make_table_name(m_collection, "ids"), table_flags)
        , m_embeddings(m_connection, make_table_name(m_collection, "embeddings"), table_flags)
        , m_texts(m_connection, make_table_name(m_collection, "texts"), table_flags)
        , m_metadata(m_connection, make_table_name(m_collection, "metadata"), table_flags)
        , m_index(metric)
    {
        rebuild_index();
    }

    inline uint64_t VectorStore::add(const Embedding& embedding,
                                       const std::string& text,
                                       const std::string& metadata_json) {
        embedding.validate();
#if MDBXC_SYNC_ENABLED
        const Connection::SyncApplyWriteGuard guard =
            m_connection->sync_apply_write_guard();
#endif
        std::lock_guard<std::mutex> store_lock(m_store_mutex);
        ensure_index_fresh_locked();
        if (m_index.dim() != 0 && embedding.dim != m_index.dim()) {
            throw std::invalid_argument("Embedding dimension does not match index dimension");
        }

        auto txn = m_connection->transaction(TransactionMode::WRITABLE);
        uint64_t id = m_ids.append(uint64_t(0), txn);
        m_embeddings.insert_or_assign(id, embedding, txn);
        m_texts.insert_or_assign(id, text, txn);
        m_metadata.insert_or_assign(id, metadata_json, txn);
        txn.commit();

        m_index.add(id, embedding);
        return id;
    }

    inline std::vector<SearchResult> VectorStore::search(const Embedding& query,
                                                           std::size_t top_k) const {
        query.validate();
#if MDBXC_SYNC_ENABLED
        for (;;) {
            {
                const Connection::SyncApplyReadGuard read_guard =
                    m_connection->sync_apply_read_guard();
                std::lock_guard<std::mutex> store_lock(m_store_mutex);
                if (is_index_fresh()) {
                    return search_locked(query, top_k);
                }
            }
            {
                const Connection::SyncApplyWriteGuard write_guard =
                    m_connection->sync_apply_write_guard();
                std::lock_guard<std::mutex> store_lock(m_store_mutex);
                ensure_index_fresh_locked();
            }
        }
#else
        std::lock_guard<std::mutex> store_lock(m_store_mutex);
        ensure_index_fresh_locked();
        return search_locked(query, top_k);
#endif
    }

    inline bool VectorStore::erase(uint64_t id) {
#if MDBXC_SYNC_ENABLED
        const Connection::SyncApplyWriteGuard guard =
            m_connection->sync_apply_write_guard();
#endif
        std::lock_guard<std::mutex> store_lock(m_store_mutex);
        ensure_index_fresh_locked();
        auto txn = m_connection->transaction(TransactionMode::WRITABLE);
        bool emb_ok = m_embeddings.erase(id, txn);
        bool txt_ok = m_texts.erase(id, txn);
        bool meta_ok = m_metadata.erase(id, txn);
        txn.commit();

        m_index.erase(id);
        return emb_ok || txt_ok || meta_ok;
    }

    inline void VectorStore::clear() {
#if MDBXC_SYNC_ENABLED
        const Connection::SyncApplyWriteGuard guard =
            m_connection->sync_apply_write_guard();
#endif
        std::lock_guard<std::mutex> store_lock(m_store_mutex);
        auto txn = m_connection->transaction(TransactionMode::WRITABLE);
        m_ids.clear(txn);
        m_embeddings.clear(txn);
        m_texts.clear(txn);
        m_metadata.clear(txn);
        txn.commit();
        m_index.clear();
        m_sync_apply_generation_seen = current_sync_apply_generation();
    }

    inline void VectorStore::rebuild_index() {
#if MDBXC_SYNC_ENABLED
        const Connection::SyncApplyWriteGuard guard =
            m_connection->sync_apply_write_guard();
#endif
        std::lock_guard<std::mutex> store_lock(m_store_mutex);
        rebuild_index_impl_locked();
    }

    inline void VectorStore::rebuild_index_impl_locked() const {
        FlatVectorIndex rebuilt(m_metric);
        std::vector<std::pair<uint64_t, Embedding>> entries;
        m_embeddings.load(entries);
        for (std::size_t i = 0; i < entries.size(); ++i) {
            entries[i].second.validate();
            rebuilt.add(entries[i].first, entries[i].second);
        }

        m_index = std::move(rebuilt);
        m_sync_apply_generation_seen = current_sync_apply_generation();
    }

    inline std::uint64_t VectorStore::current_sync_apply_generation() const {
#if MDBXC_SYNC_ENABLED
        return m_connection->sync_apply_generation();
#else
        return 0;
#endif
    }

    inline void VectorStore::ensure_index_fresh() const {
#if MDBXC_SYNC_ENABLED
        const Connection::SyncApplyWriteGuard guard =
            m_connection->sync_apply_write_guard();
#endif
        std::lock_guard<std::mutex> store_lock(m_store_mutex);
        ensure_index_fresh_locked();
    }

    inline void VectorStore::ensure_index_fresh_locked() const {
        if (!is_index_fresh()) {
            rebuild_index_impl_locked();
        }
    }

    inline bool VectorStore::is_index_fresh() const {
        return current_sync_apply_generation() == m_sync_apply_generation_seen;
    }

    inline std::vector<SearchResult> VectorStore::search_locked(
            const Embedding& query,
            std::size_t top_k) const {
        std::vector<VectorMatch> matches = m_index.search(query, top_k);
        std::vector<SearchResult> results;
        results.reserve(matches.size());
        for (std::size_t i = 0; i < matches.size(); ++i) {
            std::pair<bool, std::string> text_res = m_texts.find_compat(matches[i].id);
            if (!text_res.first) {
                throw std::runtime_error("VectorStore integrity error: text missing for id");
            }
            std::pair<bool, std::string> meta_res = m_metadata.find_compat(matches[i].id);
            if (!meta_res.first) {
                throw std::runtime_error("VectorStore integrity error: metadata missing for id");
            }
            SearchResult sr;
            sr.id = matches[i].id;
            sr.score = matches[i].score;
            sr.collection = m_collection;
            sr.text = text_res.second;
            sr.metadata_json = meta_res.second;
            results.push_back(sr);
        }
        return results;
    }

    inline std::size_t VectorStore::count() const {
#if MDBXC_SYNC_ENABLED
        const Connection::SyncApplyReadGuard guard =
            m_connection->sync_apply_read_guard();
#endif
        std::lock_guard<std::mutex> store_lock(m_store_mutex);
        return m_embeddings.count();
    }

    inline const std::string& VectorStore::collection() const noexcept {
        return m_collection;
    }

} // namespace mdbxc
