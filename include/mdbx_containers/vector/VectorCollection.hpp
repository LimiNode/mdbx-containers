#pragma once
#ifndef MDBX_CONTAINERS_HEADER_VECTOR_VECTOR_COLLECTION_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_VECTOR_VECTOR_COLLECTION_HPP_INCLUDED

/// \file VectorCollection.hpp
/// \brief Descriptor-validated vector records with caller-owned stable ids.

#include "../KeyValueTable.hpp"
#include "../ValueTable.hpp"
#include "Embedding.hpp"
#include "VectorCollectionDescriptor.hpp"

#include <memory>
#include <string>

namespace mdbxc {

    class VectorExactScan;

    /// \brief Persistent vector records governed by an immutable descriptor.
    /// \details This storage foundation deliberately has no search index. It
    /// preserves caller-owned opaque record ids and validates the collection
    /// descriptor before opening the records DBI.
    /// \thread_safety Not thread-safe. Synchronize simultaneous operations on
    /// one instance externally, or use a separate instance per worker thread.
    class VectorCollection {
    public:
        /// \brief Opens a collection using a new connection.
        /// \param config MDBX environment configuration.
        /// \param descriptor Persistent collection compatibility contract.
        /// \throws std::invalid_argument if \p descriptor is invalid.
        /// \throws std::invalid_argument if an existing descriptor differs.
        VectorCollection(const Config& config,
                         const VectorCollectionDescriptor& descriptor);

        /// \brief Opens a collection using an existing connection.
        /// \param connection Shared MDBX connection.
        /// \param descriptor Persistent collection compatibility contract.
        /// \throws std::invalid_argument if \p connection or \p descriptor is invalid.
        /// \throws std::invalid_argument if an existing descriptor differs.
        VectorCollection(std::shared_ptr<Connection> connection,
                         const VectorCollectionDescriptor& descriptor);

        VectorCollection(const VectorCollection&) = delete;
        VectorCollection& operator=(const VectorCollection&) = delete;
        VectorCollection(VectorCollection&&) = delete;
        VectorCollection& operator=(VectorCollection&&) = delete;

        /// \brief Inserts or replaces an embedding using a caller-owned id.
        /// \param record_id Non-empty opaque stable record id.
        /// \param embedding Embedding whose dimension matches the descriptor.
        /// \throws std::invalid_argument if an argument is invalid.
        void insert_or_assign(const std::string& record_id,
                              const Embedding& embedding);

        /// \brief Looks up an embedding by caller-owned id.
        /// \param record_id Non-empty opaque stable record id.
        /// \param out Destination for a found embedding.
        /// \return \c true when a record was found.
        /// \throws std::invalid_argument if \p record_id is empty.
        bool try_get(const std::string& record_id, Embedding& out) const;

        /// \brief Removes an embedding by caller-owned id.
        /// \param record_id Non-empty opaque stable record id.
        /// \return \c true when a record was removed.
        /// \throws std::invalid_argument if \p record_id is empty.
        bool erase(const std::string& record_id);

        /// \brief Returns the number of stored vector records.
        /// \return Number of records.
        std::size_t count() const;

        /// \brief Returns whether no vector records are stored.
        /// \return \c true when the collection has no records.
        bool empty() const;

        /// \brief Returns the immutable persistent descriptor.
        /// \return Collection descriptor.
        const VectorCollectionDescriptor& descriptor() const noexcept;

    private:
        friend class VectorExactScan;

        typedef KeyValueTable<std::string, Embedding> RecordsTable;

        VectorCollectionDescriptor m_descriptor;
        std::shared_ptr<Connection> m_connection;
        ValueTable<VectorCollectionDescriptor> m_descriptor_table;
        std::unique_ptr<RecordsTable> m_records;

        static VectorCollectionDescriptor validated_descriptor(
            const VectorCollectionDescriptor& descriptor);
        static std::shared_ptr<Connection> require_connection(
            std::shared_ptr<Connection> connection);
        static std::string descriptor_table_name(const std::string& collection_id);
        static std::string records_table_name(const std::string& collection_id);
        static void validate_record_id(const std::string& record_id);
        void verify_or_persist_descriptor();
        void validate_embedding(const Embedding& embedding) const;
    };

} // namespace mdbxc

#include "VectorCollection.ipp"

#endif // MDBX_CONTAINERS_HEADER_VECTOR_VECTOR_COLLECTION_HPP_INCLUDED
