# Release readiness

## Decision

`BLOCKED_PENDING_CORRECTIVE_MERGE`

The baseline should not be released as an infrastructure dependency until the P1 corrective changes are reviewed and merged. The corrective branch itself passes the local supported Linux matrix described below, but it is not a release because no merge, tag, or release action was performed.

## Supported local contract

| Dimension | Verified locally | Status |
| --- | --- | --- |
| Linux GCC 14.2, C++11 Debug | 233/233 CTest | Ready locally |
| Linux GCC 14.2, C++17 Debug | 234/234 CTest | Ready locally |
| Linux GCC 14.2, C++17 Release benchmarks | All four benchmark targets built and run | Ready locally |
| Installed CMake package | Consumer built and ran with fake external MDBX config | Ready locally |
| AddressSanitizer | Manual scalar and absent-DBI probe passed | Partial, full CMake sanitizer option unavailable |
| UBSan | Not run in this restored worktree | Pending |
| Clang | Toolchain unavailable | Pending CI |
| MinGW | Toolchain unavailable | Pending CI |
| MSVC | Toolchain and external MDBX combination not available | Classify in CI before support claim |
| macOS | Runner unavailable locally | Pending CI |
| Doxygen | Binary unavailable locally | Pending CI |
| GitHub exact-head Archcheck | Run 369, success | Passed |
| GitHub exact-head CI | Run 796 for code head, run 801 for final documentation head, success | Passed |
| Real process raw HTTP smoke | `sync_18_http_node_fleet` master-replica and mesh | Passed |

## Blockers

1. Review and merge F-001, F-002, and F-003 corrective code with their regression tests.
2. Merge PR #334 after review. Its exact-head CI and Archcheck are green.
3. Run the repository Doxygen job and inspect owned-header warnings.
4. Verify the external MDBX package mode used by the release consumer. The local consumer used a fake config because the bundled build does not install an external MDBX package config.

## Non-blocking follow-ups

- Selective replication HTTP/WebSocket routing, scoped snapshot/resume, and retention
- Persisted logical recovery resume from PR #332
- Stale repository links in README and Doxygen, addressed by the follow-up documentation batch
- Sync engine header decomposition
- Reconciliation allocation profiling
- Exact-vector search strategy for large collections
- Additional allocation-failure and process-crash fault injection

## Compatibility and migration

The corrective branch does not alter persisted records, key encoding, DBI names, codec versions, or transport frames. No migration or compatibility fixture update is required.

## Release acceptance criteria

A release can proceed after the P1 branch is merged, exact-head CI is green on the supported platform matrix, and the documentation-only stale-link PR is either merged or explicitly tracked. The branch must remain free of unreviewed wire-format changes and release metadata changes.

