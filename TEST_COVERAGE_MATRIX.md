# Test coverage matrix

## Executed matrix

| Configuration | Result | Notes |
| --- | --- | --- |
| GCC 14.2, Linux, Debug, C++17, sync enabled | 234/234 | Full CTest suite including policy checks |
| GCC 14.2, Linux, Debug, C++11, sync enabled | 233/233 | Full CTest suite including policy checks |
| GCC 14.2, Linux, Debug, C++17, examples enabled | 234/234 | Same full suite after example compilation |
| GCC 14.2, Linux, Release, C++17 benchmarks | Built | Quick and realistic benchmark presets executed |
| Installed package consumer | Passed | CMake package consumer built with a fake external MDBX config |
| Manual AddressSanitizer probe | Passed | Scalar serializer lifetime and absent DBI probes |
| GitHub exact-head Archcheck and CI | Passed | PR #334 head `f9136d5`, all jobs green |
| Real process raw HTTP smoke | Passed | `sync_18_http_node_fleet`, master-replica and mesh scenarios in CI |

The automatic CMake ASan option was disabled by its compiler feature probe. The manual probe used `-fsanitize=address` directly. No claim is made for unsupported sanitizer or platform combinations.

## API and failure-mode coverage

| Area | Existing or added tests | Result |
| --- | --- | --- |
| Connection open, close, shutdown, timeout | `transaction_test`, `connection_unicode_path_test`, lifecycle sync tests | Passed |
| RAII commit and rollback | `transaction_test`, `test_transaction_provenance` | Passed |
| Direct `current_txn()` commit followed by manual begin | Added block in `transaction_test.cpp` | Passed |
| Null table connection | Added block in `key_table_test.cpp` | Passed |
| Configure after connect | Corrective branch guard, covered by public lifecycle contract | Code path verified |
| KeyValue insert, assign, erase, range, reverse range | `key_table_test`, `composite_key_test`, `hashed_key_value_store_test` | Passed |
| Composite and binary keys | `composite_key_test`, added `reconcile(std::map<CompositeKey,...>)` | Passed |
| Multi-value duplicate and ordered behavior | `key_multi_value_table_test`, `key_ordered_multi_value_table_test` | Passed |
| Serialization and malformed payloads | `utils_test`, codec bounds and golden tests | Passed |
| C++17 `std::byte` vector, deque, list decoding | Added blocks in `utils_test.cpp` | Passed |
| Scalar serializer lifetime | Added scratch ownership regression and manual ASan probe | Passed |
| Optional DBI absence | Added repeated probes in `test_sync_stores.cpp` | Passed |
| Raw capture and replay | `test_sync_capture`, `test_sync_engine`, `test_sync_replication` | Passed |
| Duplicate, out-of-order, stale and conflicting raw apply | Sync engine and replication suites | Passed |
| Selective scope bounds and ownership | Protocol, engine and randomized sync suites | Passed |
| Snapshot staging and resume | Full snapshot protocol and sync engine suites | Passed |
| Logical schema and adapter validation | Logical adapter, recovery and worker suites | Passed |
| Ordered logical delivery | Destructive adapter and worker suites | Passed |
| Transport boundaries and middleware | HTTP, WebSocket, codec, boundary and middleware suites | Passed |
| Backup and restore | `test_backup` | Passed |
| Installed package and target export | Fresh consumer with fake MDBX package config | Passed |

## Gaps and external verification

- The local environment did not provide Clang, MinGW, MSVC, macOS, Doxygen, TSan, or Valgrind. GitHub exact-head CI for PR #334 covers macOS, Windows MinGW64, and optional transport providers.
- Allocation-failure injection is present in selected sync paths but does not cover every table family.
- Full process-crash recovery was exercised through deterministic staging and restart tests, while power-loss durability still depends on the libmdbx and filesystem deployment contract.
- CMake optional transport combinations were not all available locally and remain CI responsibilities.

