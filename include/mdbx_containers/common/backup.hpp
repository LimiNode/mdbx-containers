#pragma once
#ifndef MDBX_CONTAINERS_HEADER_COMMON_BACKUP_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_COMMON_BACKUP_HPP_INCLUDED

/// \file common/backup.hpp
/// \brief MDBX environment backup and durability flush operations.
/// \details
/// Provides thin wrappers over libmdbx backup primitives (`mdbx_env_copy`,
/// `mdbx_env_sync_ex`). Operations are scoped at the \ref Connection level
/// because backup copies the whole MDBX environment, not an individual
/// logical table.

namespace mdbxc {

    /// \brief Backup compaction mode.
    enum class BackupMode {
        Normal, ///< Copy pages verbatim without compaction.
        Compact ///< Copy with page compaction to reduce file size.
    };

    /// \brief Backup options passed to \ref Connection::backup_to.
    struct BackupOptions {
        BackupMode mode = BackupMode::Compact; ///< Backup mode (default: compact).
        bool throttle_mvcc = false;            ///< Throttle MVCC snapshot retention during long backups to allow page reuse.

        /// \brief Do not force a flush of pending writes before copying.
        /// Use only when the caller guarantees durability via prior
        /// \ref Connection::sync_to_disk or external fsync.
        bool dont_flush = false;
        bool force_dynamic_size = false; ///< Allow the copy to dynamically resize the target environment instead of inheriting the source geometry.
    };

} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_COMMON_BACKUP_HPP_INCLUDED
