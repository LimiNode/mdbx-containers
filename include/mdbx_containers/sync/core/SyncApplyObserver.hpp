#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_CORE_SYNCAPPLYOBSERVER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_CORE_SYNCAPPLYOBSERVER_HPP_INCLUDED

/// \file core/SyncApplyObserver.hpp
/// \brief Observer hook for successful sync apply commits.

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace mdbxc {
namespace sync {

    /// \brief Summary of one committed sync apply.
    /// \details Emitted after \c SyncEngine::handle_push() commits at least
    /// one non-empty incoming batch or \c SyncEngine::apply_logical_changes()
    /// commits logical changes, after the connection apply generation has
    /// been incremented, and after the connection apply/write barrier has
    /// been released.
    struct SyncApplyEvent {
        std::uint64_t generation = 0;       ///< New connection apply generation.
        std::size_t applied_batches = 0;    ///< Number of applied non-empty batches.
        std::size_t applied_ops = 0;        ///< Number of applied operations.
        /// \brief Unique DBI names touched by applied operations.
        /// \details Names are reported in first-seen order across applied
        /// batches. Idempotent replays and skipped batches do not emit events.
        std::vector<std::string> affected_dbi_names;
    };

    /// \brief Non-owning observer for successful sync apply commits.
    /// \details Register through \c Connection::add_sync_apply_observer().
    /// Implementations are intended for cache invalidation, metrics, and
    /// structured logging. Callbacks run after the apply write barrier
    /// is released, so an observer may use table and cache-backed APIs on the
    /// same connection. Exceptions thrown by observers are swallowed by the
    /// connection so they cannot roll back an already committed sync apply.
    class ISyncApplyObserver {
    public:
        virtual ~ISyncApplyObserver() = default;

        /// \brief Called after a sync apply commit changes user data.
        virtual void on_sync_apply_committed(
            const SyncApplyEvent& event) = 0;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_CORE_SYNCAPPLYOBSERVER_HPP_INCLUDED
