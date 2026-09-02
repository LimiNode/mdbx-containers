#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_CORE_SELECTIVE_REPLICATION_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_CORE_SELECTIVE_REPLICATION_HPP_INCLUDED

/// \file core/SelectiveReplication.hpp
/// \brief Wire- and storage-neutral selective-replication descriptor types.

#include <cstdint>
#include <string>
#include <vector>

namespace mdbxc {
template<class KeyT, class ValueT, class Options> class KeyValueTable;
template<class KeyT, class Options> class KeyTable;
template<class ValueT> class ValueTable;
template<class ValueT> class SequenceTable;

namespace sync {

    class SelectiveReplicationStore;
    class SelectiveReplicationProtocolCodec;

    /// \brief Canonical v1 maximum for one scope identity.
    static const std::uint32_t selective_replication_max_scope_id_len = 256u;
    /// \brief Canonical v1 maximum for one scope manifest.
    static const std::uint32_t selective_replication_max_manifest_entries =
        10000u;
    /// \brief Canonical v1 maximum for a manifest DBI name.
    static const std::uint32_t selective_replication_max_dbi_name_len = 256u;

    /// \brief One immutable DBI member of a selective replication scope.
    /// \details Local applications obtain members from raw-capture-supported
    /// table wrappers. The wire codec is a friend so it can reconstruct the
    /// same descriptor without opening application tables.
    class SelectiveReplicationDbi {
    public:
        /// \brief Creates a scope member from a raw-capture-supported table.
        template<class KeyT, class ValueT, class Options>
        static SelectiveReplicationDbi from(
            const ::mdbxc::KeyValueTable<KeyT, ValueT, Options>& table);

        /// \brief Creates a scope member from a raw-capture-supported table.
        template<class KeyT, class Options>
        static SelectiveReplicationDbi from(
            const ::mdbxc::KeyTable<KeyT, Options>& table);

        /// \brief Creates a scope member from a raw-capture-supported table.
        template<class ValueT>
        static SelectiveReplicationDbi from(
            const ::mdbxc::ValueTable<ValueT>& table);

        /// \brief Creates a scope member from a raw-capture-supported table.
        template<class ValueT>
        static SelectiveReplicationDbi from(
            const ::mdbxc::SequenceTable<ValueT>& table);

        const std::string& dbi_name() const { return m_dbi_name; }
        std::uint32_t dbi_flags() const { return m_dbi_flags; }

    private:
        SelectiveReplicationDbi() : m_dbi_flags(0u) {}
        SelectiveReplicationDbi(const std::string& dbi_name,
                                std::uint32_t dbi_flags)
            : m_dbi_name(dbi_name), m_dbi_flags(dbi_flags) {}

        std::string m_dbi_name;
        std::uint32_t m_dbi_flags;

        friend class SelectiveReplicationStore;
        friend class SelectiveReplicationProtocolCodec;
    };

    /// \brief Durable identity and write authority for one selective scope.
    struct SelectiveReplicationDescriptor {
        std::string scope_id;
        NodeId designated_writer_origin{};
        std::vector<SelectiveReplicationDbi> manifest;
    };

    /// \brief Returns whether two complete selective descriptors are equal.
    inline bool selective_replication_descriptors_equal(
            const SelectiveReplicationDescriptor& left,
            const SelectiveReplicationDescriptor& right) {
        if (left.scope_id != right.scope_id ||
            compare_node_id(left.designated_writer_origin,
                            right.designated_writer_origin) != 0 ||
            left.manifest.size() != right.manifest.size()) {
            return false;
        }
        for (std::size_t i = 0u; i < left.manifest.size(); ++i) {
            if (left.manifest[i].dbi_name() != right.manifest[i].dbi_name() ||
                left.manifest[i].dbi_flags() != right.manifest[i].dbi_flags()) {
                return false;
            }
        }
        return true;
    }

    template<class KeyT, class ValueT, class Options>
    inline SelectiveReplicationDbi SelectiveReplicationDbi::from(
            const ::mdbxc::KeyValueTable<KeyT, ValueT, Options>& table) {
        return SelectiveReplicationDbi(table.dbi_name(), table.dbi_flags());
    }

    template<class KeyT, class Options>
    inline SelectiveReplicationDbi SelectiveReplicationDbi::from(
            const ::mdbxc::KeyTable<KeyT, Options>& table) {
        return SelectiveReplicationDbi(table.dbi_name(), table.dbi_flags());
    }

    template<class ValueT>
    inline SelectiveReplicationDbi SelectiveReplicationDbi::from(
            const ::mdbxc::ValueTable<ValueT>& table) {
        return SelectiveReplicationDbi(table.dbi_name(), table.dbi_flags());
    }

    template<class ValueT>
    inline SelectiveReplicationDbi SelectiveReplicationDbi::from(
            const ::mdbxc::SequenceTable<ValueT>& table) {
        return SelectiveReplicationDbi(table.dbi_name(), table.dbi_flags());
    }

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_CORE_SELECTIVE_REPLICATION_HPP_INCLUDED
