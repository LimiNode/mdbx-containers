# Changelog

All notable changes to this project will be documented in this file.

## Unreleased
- Added persistent `OrderedElementId` and state-store primitives for the
  planned destructive `KeyOrderedMultiValueTable` logical schema v2.
- Added `KeyOrderedMultiValueTableLogicalAdapter` for append-only logical
  apply through ordered delivery. It preserves repeated values and per-key
  append order for the schema marker's authoritative origin. Its typed capture
  session atomically commits local appends plus an ordered outbox envelope;
  destructive operations remain deferred.
- Restricted `KeyOrderedMultiValueTableLogicalAdapter` to its fixed
  schema-version-1 append payload contract. Future destructive ordered
  semantics require a separate versioned adapter.
- Added batch preflight to `ILogicalTableAdapter`. Existing adapters retain
  per-change preflight behavior by default; adapters with frame-local
  invariants can validate a non-owning schema-local batch view before apply
  without copying logical payload bytes.
- Ordered logical schemas now persist an explicit authoritative origin in
  `_mdbxc_sync_schema`. Ordered delivery validates that binding before replay
  marker insertion or adapter callbacks, so independent origins cannot append
  one ordered dataset in arrival-dependent order. Older ordered markers must
  be migrated explicitly before ordered delivery; duplicate deliveries now
  require their exact replay marker and fail closed after that marker is
  pruned.
- Added an ordered-delivery requirement to the logical adapter contract.
  Existing adapters keep the default unrestricted behavior; append-history
  adapters can reject direct logical frames and unordered delivery before
  preflight or mutation.
- Fixed `LogicalDeliveryStore` probing of absent optional DBIs. It now checks
  the main DB before attempting a read-only named-DBI open, so a missing
  logical-delivery watermark remains a normal legacy-layout condition in both
  Debug and Release builds.
- Added negotiated `CumulativeAcknowledgement` for ordered logical delivery.
  Sender outbox metadata now exposes a durable known-tail bound, allowing a
  receiver-ahead duplicate acknowledgement to clean a verified contiguous
  prefix after sender restart. Legacy envelope-only delivery peers remain on
  the conservative exact-sequence acknowledgement contract.
- Added capability-gated ordered logical delivery dispatch through
  `ILogicalDeliveryPeer`, including the in-process `DirectLogicalDeliveryPeer`.
  Valid cumulative acknowledgements clean the matching sender outbox prefix;
  receiver-side ordered marker pruning is explicit and bounded by its persisted
  contiguous frontier.
- Added persisted receiver-side ordering for the `OrderedDelivery` capability.
  `SyncEngine::apply_ordered_logical_delivery_envelope()` accepts only the next
  contiguous sequence per remote origin, returns a cumulative acknowledgement,
  treats duplicates as no-ops, and reports gaps as retryable without adapter
  mutation. `_mdbxc_logical_delivery_order` is created as a committed sync
  system store; it requires one additional `Config::max_dbs` slot.
- Added the versioned `LogicalDeliveryProtocol` wire contract with capability
  hello, nested delivery, and cumulative acknowledgement messages. The first
  optional capability is `OrderedDelivery`; unknown capability bits are not
  negotiated implicitly. Existing HTTP/WebSocket/raw pull-push transports do
  not advertise this contract until the ordered receiver path is available.
- Added `_mdbxc_logical_outbox` and the sender-side
  `SyncEngine::enqueue_logical_delivery()` lifecycle for future ordered logical
  delivery. The outbox persists independent monotonic streams per destination,
  lists pending envelopes in numeric order, and atomically removes an
  acknowledged prefix through `acknowledge_logical_deliveries()`. It is a local
  durable queue only: transport capability negotiation, receiver ordering, and
  wire acknowledgements remain subsequent protocol work. Sync environments now
  reserve one additional named-DBI slot in `Config::max_dbs` for the committed
  `_mdbxc_logical_outbox` system store.
- Added explicit logical-delivery replay-marker pruning with persistent
  per-origin watermarks. `SyncEngine::prune_logical_delivery_markers()` lazily
  creates its watermark DBI, removes acknowledged markers atomically, and turns
  later replay at or below the saved boundary into a stale no-op. Deployments
  that use pruning need one additional `Config::max_dbs` slot; callers remain
  responsible for deriving a safe boundary from their own
  delivery/acknowledgement protocol.
- Added an opt-in typed `LogicalCaptureSession` to
  `KeyTableLogicalAdapter`. It owns one writable transaction, suppresses raw
  capture, emits logical insert/delete changes only for successful local
  membership mutations, records explicit clear requests, and publishes pending
  changes only after commit.
- Added a non-blocking `Archcheck` GitHub Actions workflow that runs
  `archcheck --diff` through a pinned official action as an advisory
  architecture signal on pull requests, including stacked PRs, and manual
  dispatch.
- Added a persistent sync schema registry store for future logical table
  adapters. The registry records logical schema ids, table kinds, schema
  versions, and canonical sorted unique owned DBI names without enabling
  logical replication yet. `SyncEngine::register_logical_schema()` is the
  normal committed setup entry point for application schema markers.
  `SyncEngine::migrate_logical_schema()` provides an explicit exact-preflight
  marker replacement lifecycle without migrating user data or old changelog
  entries.
- Added public `ChangeDomain`, `LogicalSchemaRef`, and `LogicalChange` model
  types for future logical table adapters. The current wire codec remains
  raw-DBI only.
- Added `LogicalChangeFrame` and `LogicalChangeFrameCodec` as a strict
  little-endian binary frame for explicit logical sync paths. The frame is a
  payload container only: destination routing, origin ordering, and replay
  protection remain the responsibility of an external delivery envelope or
  caller-owned transport contract. Transport pull/push DTOs remain raw-DBI only
  until a later capability-gated PR wires logical delivery into transport
  exchange. The decoder fails closed for unsupported versions, mandatory flags,
  reserved change flags, malformed schema refs, oversized counts, trailing
  bytes, and configured schema or payload bounds. Codec tests also cover
  encode-side bounds and a literal whole-frame golden vector.
- Added explicit `SyncEngine::apply_logical_frame()` and
  `SyncEngine::apply_logical_frame_bytes()` helpers so decoded or encoded
  logical frames can be applied through the existing engine-owned logical
  adapter path. Malformed frame bytes return a failure result before adapter
  preflight or mutation, while apply-stage exceptions are reported separately
  from decode failures.
- Added `sync_23_key_value_logical_frame.cpp` to demonstrate the explicit
  `KeyValueTable` logical capture-session -> logical frame codec -> replica
  apply workflow.
- Added `LogicalDeliveryEnvelope`, `LogicalDeliveryEnvelopeCodec`, and the
  `_mdbxc_logical_delivery` replay marker store. `SyncEngine` can now apply a
  delivery envelope with destination db uuid validation and atomic persisted
  deduplication in the same transaction as logical adapter mutations. Replay
  markers use fixed-size MDBX keys, bind the full delivery identity to
  canonical nested frame bytes, reject identity collisions with different
  payloads, preserve caller codec bounds during marker creation, and skip
  self-origin loopback envelopes as successful no-ops. Read-only marker
  inspection helpers expose marker counts and delivery identity metadata for
  diagnostics and future lifecycle work after validating persisted marker
  key/value consistency. A stateless delivery order helper can classify an
  envelope sequence against a caller-owned expected next sequence, but it does
  not identify duplicates. Persisted ordering and marker retention remain
  external/future lifecycle contracts.
- Added `ISyncCaptureSink::record_change_op(MDBX_txn*, const ChangeOp&)` as the
  preferred capture entry point. The previous raw-field callback remains the
  source-compatible abstract sink contract; new full-`ChangeOp` sinks can
  derive from `FullChangeSyncCaptureSink`. Capture record or flush failures now
  prevent committing the affected explicit transaction, and unmanaged raw
  writable `MDBX_txn*` calls are rejected while sync capture is attached.
- Added `ILogicalTableAdapter` and `LogicalTableRegistry` scaffolding for
  future two-phase logical table preflight/apply support. Apply exceptions are
  converted to failure results so the caller-owned transaction can be aborted.
  `SyncEngine::apply_logical_changes()` now provides an explicit engine-owned
  logical apply path. Adapters expose an explicit `primary_dbi()` contract, and
  engine logical apply validates both the primary DBI and the canonical owned
  DBI set against the persistent schema marker before adapter preflight. The
  default primary keeps existing single-DBI adapters source-compatible;
  multi-DBI adapters must override it explicitly. The transport pull/push wire
  format remains raw-DBI only.
- Added `KeyValueTableLogicalAdapter` as the first concrete logical adapter
  helper for explicit engine or registry logical apply usage.
  It covers typed upsert/delete/clear payloads for `KeyValueTable` with
  explicit key/value codec tags, including little-endian signed/unsigned
  integer wire codecs up to 64 bits, bool, and string codecs. Incoming logical
  apply suppresses local raw capture for the affected transaction. The adapter
  also provides an opt-in transaction-bound logical capture session that
  validates the persistent schema marker before local mutation, buffers typed
  local changes, and publishes them only after successful commit. Transport
  pull/push remains raw-DBI only.
- Added `KeyTableLogicalAdapter` for explicit apply-side logical
  insert/delete/clear operations on `KeyTable`. It reuses the existing explicit
  key codec tags and validates the persistent single-DBI schema marker through
  the normal engine logical apply path. Typed local capture for key-only tables
  remains future work.
- Documented the staged logical sync contract: schema marker, logical wire
  frame, adapter preflight/apply, then capture. Deferred logical tables still
  require single-writer or application-serialized conflicting writes until a
  causal conflict model exists.
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
