# Corrective PR manifest

## Intended PR

- Repository `LimiNode/mdbx-containers`
- Base `main@4ab32b5eab3cdfce95d1a6c0f05f566019793ec4`
- Branch `audit/independent-20261118`
- State `Draft`
- Merge, tag, and release actions were not performed
- Draft PR published as #334: https://github.com/LimiNode/mdbx-containers/pull/334

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

- Remote corrective commit SHA: `ec5011090259c3a67db3c10a6fa7a76c3bb8ea27`
- Draft PR number: `334`
- Draft PR URL: https://github.com/LimiNode/mdbx-containers/pull/334
- Corrective code head SHA: `ec5011090259c3a67db3c10a6fa7a76c3bb8ea27`
- Final branch head: recorded by PR #334 metadata after documentation-only follow-up commits
- CI status: successful. Archcheck run `44502531460` and CI run `44502531351` completed with all jobs green.

## Migration

None. Persisted and wire formats are unchanged.

## Publication record

The branch and Draft PR were published through the authenticated GitHub connector. Merge, tag, and release actions remain unperformed.

