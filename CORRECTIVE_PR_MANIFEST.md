# Corrective PR manifest

## Intended PR

- Repository `LimiNode/mdbx-containers`
- Base `main@4ab32b5eab3cdfce95d1a6c0f05f566019793ec4`
- Branch `audit/independent-20261118`
- State `Draft`
- Merge, tag, and release actions were not performed
- Remote publication is currently blocked by the GitHub connector. No remote branch or PR was created.

## Corrective scope

### P1 persistence and sync fixes

- Add MainDB catalog lookup for optional or expected-existing named DBIs
- Replace negative named-DBI probes in raw sync, snapshot, selective, logical, and adapter paths
- Copy trivially-copyable value bytes into serializer scratch storage
- Validate selective descriptors before transaction and DBI work begins

### P2 correctness and API fixes

- Remove the hidden `std::hash<KeyT>` requirement from `KeyValueTable::reconcile`
- Add explicit C++17 `std::byte` vector, deque, and list decoders
- Reject null table connections
- Reap stale manual transaction map entries after direct transaction completion
- Reject `Connection::configure` after connect

### Packaging and regression tests

- Exclude repository `AGENTS.md` files from installed headers
- Add regressions for absent optional stores, scalar serializer lifetime, composite-key reconciliation, byte decoding, null connections, and stale manual transactions

## Verification recorded before publication

- GCC 14.2 Linux C++17 Debug CTest 234/234
- GCC 14.2 Linux C++11 Debug CTest 233/233
- Release benchmarks built and executed at quick and realistic presets
- Manual AddressSanitizer probe passed for scalar lifetime and missing DBI paths
- Installed package consumer built and ran with a fake external MDBX package config
- `git diff --check` passed

## Remote publication record

- Initial corrective commit SHA: `82d8958d5b1a16212c4d77f2b7411e0bb1af8a27`
- Draft PR number: not created because GitHub branch creation timed out
- Draft PR URL: not available
- Remote head SHA: not available
- CI status: not started because the branch could not be published

## Migration

None. Persisted and wire formats are unchanged.

## Publication blocker

The authenticated GitHub connector timed out repeatedly on branch creation and blob creation. A direct `git push` was also unavailable because this environment has no GitHub credential helper. No merge, tag, release, or remote mutation was performed. The local branch remains ready for publication from an authenticated git or GitHub session.

