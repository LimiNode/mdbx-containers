#include <stdexcept>
#include <utility>

namespace mdbxc {

    inline VectorCollectionDescriptor VectorCollection::validated_descriptor(
            const VectorCollectionDescriptor& descriptor) {
        descriptor.validate();
        return descriptor;
    }

    inline std::shared_ptr<Connection> VectorCollection::require_connection(
            std::shared_ptr<Connection> connection) {
        if (!connection) {
            throw std::invalid_argument("VectorCollection connection cannot be null");
        }
        return connection;
    }

    inline std::string VectorCollection::descriptor_table_name(
            const std::string& collection_id) {
        return "vector_collection_" + collection_id + "_descriptor";
    }

    inline std::string VectorCollection::records_table_name(
            const std::string& collection_id) {
        return "vector_collection_" + collection_id + "_records";
    }

    inline VectorCollection::VectorCollection(
            const Config& config,
            const VectorCollectionDescriptor& descriptor)
        : VectorCollection(Connection::create(config), descriptor) {}

    inline VectorCollection::VectorCollection(
            std::shared_ptr<Connection> connection,
            const VectorCollectionDescriptor& descriptor)
        : m_descriptor(validated_descriptor(descriptor))
        , m_connection(require_connection(std::move(connection)))
        , m_descriptor_table(m_connection,
                             descriptor_table_name(m_descriptor.collection_id)) {
        verify_or_persist_descriptor();
        m_records.reset(new RecordsTable(
            m_connection, records_table_name(m_descriptor.collection_id)));
    }

    inline void VectorCollection::verify_or_persist_descriptor() {
        VectorCollectionDescriptor existing;
        if (m_descriptor_table.try_get(existing)) {
            if (existing != m_descriptor) {
                throw std::invalid_argument("Vector collection descriptor mismatch");
            }
            return;
        }
        m_descriptor_table.set(m_descriptor);
    }

    inline void VectorCollection::validate_record_id(const std::string& record_id) {
        if (record_id.empty()) {
            throw std::invalid_argument("Vector collection record id must not be empty");
        }
    }

    inline void VectorCollection::validate_embedding(const Embedding& embedding) const {
        embedding.validate();
        if (embedding.dim != m_descriptor.dimension) {
            throw std::invalid_argument(
                "Vector collection embedding dimension does not match descriptor");
        }
    }

    inline void VectorCollection::insert_or_assign(
            const std::string& record_id,
            const Embedding& embedding) {
        validate_record_id(record_id);
        validate_embedding(embedding);
        m_records->insert_or_assign(record_id, embedding);
    }

    inline bool VectorCollection::try_get(const std::string& record_id,
                                           Embedding& out) const {
        validate_record_id(record_id);
        return m_records->try_get(record_id, out, nullptr);
    }

    inline bool VectorCollection::erase(const std::string& record_id) {
        validate_record_id(record_id);
        return m_records->erase(record_id);
    }

    inline std::size_t VectorCollection::count() const {
        return m_records->count();
    }

    inline bool VectorCollection::empty() const {
        return m_records->empty();
    }

    inline const VectorCollectionDescriptor& VectorCollection::descriptor() const noexcept {
        return m_descriptor;
    }

} // namespace mdbxc
