#pragma once
#ifndef MDBX_CONTAINERS_HEADER_SYNC_ENGINE_DIRECTSYNCPEER_HPP_INCLUDED
#define MDBX_CONTAINERS_HEADER_SYNC_ENGINE_DIRECTSYNCPEER_HPP_INCLUDED

/// \file engine/DirectSyncPeer.hpp
/// \brief In-process transport that forwards \c ISyncPeer calls to a remote
///        \c SyncEngine instance. Intended for tests; no serialization.

#include "ISyncPeer.hpp"
#include "SyncEngine.hpp"

namespace mdbxc {
namespace sync {

    /// \brief Non-owning \c ISyncPeer that delegates to another \c SyncEngine.
    /// \details The remote engine must outlive the peer. Useful for in-process
    /// integration tests of pull / push without a real transport layer.
    class DirectSyncPeer : public ISyncPeer {
    public:
        /// \brief Constructs a peer that forwards to \p remote.
        /// \pre \p remote must not be null.
        explicit DirectSyncPeer(SyncEngine* remote) noexcept : m_remote(remote) {
            assert(m_remote != nullptr && "DirectSyncPeer: remote engine is null");
        }

        PullResponse pull(const PullRequest& request) override {
            assert(m_remote != nullptr);
            return m_remote->handle_pull(request);
        }

        PushResponse push(const PushRequest& request) override {
            assert(m_remote != nullptr);
            return m_remote->handle_push(request);
        }

    private:
        SyncEngine* m_remote;
    };

} // namespace sync
} // namespace mdbxc

#endif // MDBX_CONTAINERS_HEADER_SYNC_ENGINE_DIRECTSYNCPEER_HPP_INCLUDED
