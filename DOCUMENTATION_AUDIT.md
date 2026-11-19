# Documentation audit

## Status

The audit inspected README, README-RU, Doxygen sources, guides, sync design notes, examples, public headers, and install output.

Doxygen was not available in the execution environment, so a generated warning count cannot be claimed. Public headers compiled through the standalone and umbrella header tests in both C++11 and C++17 configurations.

## Findings

| Area | Status | Detail |
| --- | --- | --- |
| README and README-RU repository links | Corrected in follow-up batch | CI badges now use `LimiNode/mdbx-containers` |
| Doxygen main page | Corrected in follow-up batch | `docs/mainpage.dox` now uses the `LimiNode` repository URL |
| Public transaction ownership | Covered | Connection and Transaction headers document thread ownership and lifecycle warnings |
| Sync ownership and atomicity | Covered | Sync docs describe same-transaction capture, durable cursor progression, and fail-closed apply paths |
| Unsupported multi-master semantics | Covered | Current docs describe authoritative-origin and designated-writer paths rather than claiming unrestricted multi-master conflict resolution |
| Installed repository instructions | Corrected | CMake now excludes `AGENTS.md` from public installation |
| Examples and build guides | Consistent | Commands refer to current CMake targets and benchmark presets |
| Performance claims | Needs tightening | README uses broad “high performance” wording without a reproducible benchmark table. The new performance report records bounded measurements and their scope |
| Doxygen warnings | Unverified | Native Doxygen was unavailable. CI should run it and retain owned-header warnings |

## Public API documentation review

The reviewed public headers generally contain `brief`, parameter, return, exception, and thread-safety notes for connection and transaction ownership. The primary remaining documentation risk is distributed contract detail. Optional DBI absence, transaction reuse, and logical schema ownership are described across several sync store headers. A future documentation PR should consolidate these rules into the sync guide and link each store's preconditions to that guide.

## Recommended documentation PR

1. Replace stale `NewYaroslav` links in `README.md`, `README-RU.md`, and `docs/mainpage.dox`.
2. Add the supported package name `mdbx_containers` and the exported target `mdbx_containers::mdbx_containers` to the install guide.
3. Add a compact benchmark table with hardware, compiler, preset, and algorithmic scope.
4. Run Doxygen in CI and fail only on warnings from owned project files after excluding the vendored theme.
5. Document that optional DBI existence checks use the MainDB catalog and do not create missing stores.

## Follow-up status

The stale repository identity links and the release-readiness wording for the
real process HTTP smoke are corrected in the post-audit documentation batch.
Selective HTTP/WebSocket routing, scoped snapshot/resume, retention, and
persisted logical recovery remain explicitly deferred runtime work.

