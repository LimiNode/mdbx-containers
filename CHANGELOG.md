# Changelog

All notable changes to this project will be documented in this file.

## Unreleased
- Added a persistent sync schema registry store for future logical table
  adapters. The registry records logical schema ids, table kinds, schema
  versions, and owned DBI names without enabling logical replication yet.
- Breaking transport-wire change: `TransportMessageCodec` is now version 4.
  Response DTOs include `SyncResponseErrorCode` plus `error_retryable` after
  the human-readable error string, and pull request DTOs include
  `max_single_batch_bytes` after the full-snapshot flag.
- Clarified `PullRequest::max_bytes` as a soft page budget and added
  `PullRequest::max_single_batch_bytes` plus `BatchTooLarge` rejection for
  retained changelog batches that exceed the hard per-batch limit.
- Added `SyncResponseErrorCode::SnapshotRequired` so pull requests behind
  retained changelog history fail explicitly instead of streaming
  non-contiguous batches.
- Breaking API behavior change: `VectorStore` collection names are now
  validated instead of lossy-sanitized. Names must be non-empty and contain
  only ASCII letters, digits, `_`, and `-`.
- Added `Connection::sync_apply_generation()` and made already-open
  `VectorStore` instances lazily refresh their RAM index between completed
  operations after successful remote sync apply commits.
- Added a connection-level sync apply/read barrier so cache-backed
  `VectorStore` operations do not run concurrently with remote
  `SyncEngine::handle_push()` apply commits; one `VectorStore` instance still
  serializes its own public methods.
- Added optional bucket caps and expired-bucket eviction to
  `FixedWindowHttpRateLimitPolicy`.
- Added `WebSocketSyncChannelConfig::exchange_timeout` for the ready-made
  Simple-WebSocket client binding.
- Added installed-package fallback for lowercase `mdbxConfig.cmake` and common
  libmdbx target-name variants.
- Added `CodecBounds` knobs to ready-made Simple-Web HTTP, Simple-WebSocket,
  and Kurlyk HTTP transport configs so oversized concrete transport bodies are
  rejected before sync codec decode.
- Breaking API cleanup: removed the historical
  `SyncEngine::pull_full_snapshot()` compatibility wrapper. Use
  `SyncEngine::pull_changelog_page()` for retained changelog replay; true full
  snapshot export/import remains unsupported.
- Breaking storage-format change: signed integral keys stored with
  `MDBX_INTEGERKEY` now use an order-preserving unsigned rank representation
  instead of raw signed bytes. Existing DBIs created with older signed integer
  key encoding must be rebuilt.
- Breaking storage-format change: all integral key types with `sizeof(T) <= 8`
  now use `MDBX_INTEGERKEY`; narrow integer and character-code-unit keys use
  canonical 4-byte storage, `long`/`long long` keys use canonical 8-byte
  storage, and wider integral keys use a bytewise order-preserving encoding.
  Existing development DBIs using older bytewise integral key storage must be
  rebuilt.
- Breaking storage-format change: floating-point keys now canonicalize `-0.0`
  and `+0.0` to one physical zero key and reject NaN keys. Existing
  development DBIs containing older `-0.0` or NaN keys must be rebuilt.

## [v1.0.2] - 2026-05-02
- Added this changelog to track release history in a compact, release-oriented format.
- Moved bundled dependency infrastructure from `libs/` to `external/`, including the `external/libmdbx` submodule path and related CMake references.
- Removed the unused `MDBXC_BUILD_STATIC_LIB` option and made `mdbx_containers` always build as a header-only CMake `INTERFACE` target.
- Kept examples, tests, and installed consumers linking through `mdbx_containers`, with MDBX propagated through the target interface.
- Updated bundled MDBX dependency documentation to describe `MDBXC_DEPS_MODE=BUNDLED`, `SYSTEM`, and `AUTO` behavior around `external/libmdbx`.
- Added ODR/ABI warnings to `README.md` and `README-RU.md` for projects that mix C++ standards, structure packing, or feature macro configurations across translation units.
- Hardened header-only ODR behavior by keeping `check_mdbx()` inline in the public utility header.
- Fixed string-container deserialization overload selection for `std::set<std::string>` and `std::unordered_set<std::string>`.
- Expanded key-value container tests for string sequence containers, string set containers, and set-like trivially copyable containers in both Debug and Release builds.
- Fixed ASan test wiring so sanitizer flags and test environment are enabled only when the toolchain actually supports ASan.
- Refreshed README, README-RU, Doxygen pages, and agent guidance to match the current public API surface.
- Clarified that `KeyValueTable` and `AnyValueTable` are implemented APIs, while `KeyTable` and `KeyMultiValueTable` remain placeholder headers.
- Clarified that `AnyValueTable` type-tag prefix verification is not fully implemented yet and should not be treated as complete runtime type safety.
- Made project philosophy documentation English-only and kept the Russian project overview in `README-RU.md`.
- Added and updated English agent guidance under `guides/`, including codebase orientation, build/test notes, implementation notes, coding style, and commit conventions.
- Added `include/mdbx_containers/Backup.hpp` exposing `BackupMode` and `BackupOptions` (compact, throttle MVCC, no flush, force dynamic size).
- Added `Connection::backup_to(path, options)` as a thin wrapper over `mdbx_env_copy` for whole-environment backup; the connection mutex is held for the duration of the copy.
- Added `Connection::sync_to_disk(force, nonblock)` as a thin wrapper over `mdbx_env_sync_ex`; treats both `MDBX_SUCCESS` and `MDBX_RESULT_TRUE` as success.
- Added `examples/backup_basic_example.cpp` and `tests/test_backup.cpp` covering compact backup, normal backup with explicit target removal, sync_to_disk, read-only sync_to_disk rejection, and C++17 directory-mode backup.
- Bumped the project version to `v1.0.2`.

## [v1.0.1] - 2025-08-21
- Added C++11 compatibility coverage across public headers, tests, and examples.
- Added C++11 fallback APIs for `AnyValueTable` and expanded tests for storing multiple value types.
- Added support for trivially copyable `std::set` values and `std::bitset<N>` key serialization examples.
- Reworked serialization scratch storage to avoid `thread_local` STL buffers and MinGW/Windows thread-shutdown crashes.
- Added `SerializeScratch` for MDBX value backing storage and documented its lifetime constraints for agents.
- Refactored CMake dependency handling with unified `MDBXC_DEPS_MODE` behavior and feature flags.
- Added path resolution tests for `relative_to_exe` and `no_subdir`.
- Improved CI coverage for MinGW/MSYS2 builds, CTest execution, full libmdbx history/tag availability, and documentation publishing.
- Fixed bundled libmdbx handling when git tags are missing and `VERSION.json` must be kept as a fallback.
- Fixed concurrency race coverage in `kv_container_all_types_test`.
- Documented `AnyValueTable`, configuration behavior, table usage, and C++ version differences in README and Doxygen pages.
- Clarified that `KeyTable` and `KeyMultiValueTable` were not implemented yet.
- Expanded Doxygen comments for core, table, path, transaction, and BaseTable APIs.
- Added repository coding style, naming, Doxygen, and build/test guidance for agents.
- Clarified Windows/MSVC support status and refreshed build instructions.
- Bumped the project version to `v1.0.1`.

## [v1.0.0] - 2025-07-30
- Initial public release of the header-only MDBX Containers library.
- Added `KeyValueTable` for map-like persistent key-value storage over libmdbx.
- Added connection, configuration, transaction, and base-table infrastructure.
- Added serialization helpers for primitive, trivially copyable, string, byte-vector, STL-container, and custom `to_bytes()`/`from_bytes()` data shapes.
- Added automatic and manual transaction workflows around MDBX operations.
- Added examples for key-value usage, manual transactions, multiple tables, custom structs, and configuration initialization.
- Added tests for raw MDBX integration, key-value containers, path utilities, and utility serialization behavior.
- Added Doxygen configuration, groups, custom documentation styling, and generated documentation support.
- Added English and Russian README files, MIT license metadata, NOTICE, and bundled libmdbx license attribution.
- Fixed libmdbx compatibility by ensuring MDBX stat calls use the stable-branch-compatible argument count.
