# Independent repository audit

## Scope

- Repository `LimiNode/mdbx-containers`
- Audited baseline `main@4ab32b5eab3cdfce95d1a6c0f05f566019793ec4`
- Corrective worktree branch `audit/independent-20261118`
- Submodules checked at the cloned baseline. Bundled libmdbx was `0.13.12`
- No persisted or wire format was changed

The audit covered connection and transaction ownership, all table families, serialization, sequence and vector storage, backup and lifecycle, raw and logical sync, selective replication, snapshots, transports, CMake packaging, documentation, benchmarks, and the available test and sanitizer paths.

## Result

The baseline was not ready for reliable persistence or sync use. The most important defect was a libmdbx DBI lookup pattern. A negative lookup through `mdbx_dbi_open()` can reserve a DBI slot even though the named DBI is absent. Under the bundled Debug libmdbx this caused assertions in seven existing sync and transport tests. A second P1 defect returned an MDBX value pointing at a scalar serializer function parameter whose lifetime had ended. A third P1 defect let an oversized selective descriptor reach MDBX before validation.

The corrective branch replaces optional named-DBI probes with a MainDB catalog lookup, copies trivially-copyable values into serializer scratch storage, validates selective descriptors before opening a transaction, and adds regression coverage. It also fixes several independently reproduced C++ and lifecycle defects.

## Findings

| ID | Severity | Status | Area | Finding |
| --- | --- | --- | --- | --- |
| F-001 | P1 | `CONFIRMED_DEFECT`, corrected in branch | DBI lookup and sync lifecycle | Negative named-DBI probes mutate the MDBX DBI handle table and can trigger Debug assertions |
| F-002 | P1 | `CONFIRMED_DEFECT`, corrected in branch | Serialization | Trivially-copyable value serialization returned a dangling `MDBX_val` view |
| F-003 | P1 | `CONFIRMED_DEFECT`, corrected in branch | Selective replication | Oversized descriptor validation happened after transaction and DBI work had started |
| F-004 | P2 | `CONFIRMED_DEFECT`, corrected in branch | `KeyValueTable` | `reconcile()` required `std::hash<KeyT>` even though the public key contract only requires key serialization and ordering |
| F-005 | P2 | `CONFIRMED_DEFECT`, corrected in branch | C++17 utilities | `std::byte` sequence decoding selected a constructor incompatible with `uint8_t` iterators |
| F-006 | P2 | `CONFIRMED_DEFECT`, corrected in branch | Table construction | A null `Connection` was dereferenced instead of rejected |
| F-007 | P2 | `CONFIRMED_DEFECT`, corrected in branch | Manual transaction lifecycle | Direct commit or rollback of `current_txn()` left a stale manual transaction map entry |
| F-008 | P2 | `CONFIRMED_DEFECT`, corrected in branch | Connection configuration | `configure()` changed configuration after the environment had already been opened |
| F-009 | P3 | `DOCUMENTATION_DEFECT`, deferred | Repository identity | README and Doxygen still contain `NewYaroslav` repository links |
| F-010 | P3 | `DOCUMENTATION_DEFECT`, corrected in branch | Installation | `include/AGENTS.md` files were installed as public headers |
| F-011 | P3 | `ARCHITECTURAL_RISK`, deferred | Sync engine structure | `SyncEngine.hpp` remains a very large header with many unrelated policy and storage paths |
| F-012 | P3 | `PERFORMANCE_OPPORTUNITY`, deferred | Reconciliation | Serialized-key set membership is safe and generic but adds a copy and `O(log n)` lookup per source key |
| F-013 | P3 | `PERFORMANCE_OPPORTUNITY`, deferred | Exact vector search | Exact scan is intentionally exhaustive and becomes the dominant cost at realistic sizes |

## Detailed findings

### F-001. Negative named-DBI probes

**Location in baseline**

`SyncEngine::open_store_ro`, `open_existing_user_dbi`, `read_existing_user_dbi_flags`, `existing_user_dbi_has_entries`, snapshot manifest reopening, `FullSnapshotImportStore::try_open_existing`, `MetaStore::open_existing`, `OriginIndexStore::open_existing`, `ReceiverReplicationModeStore::open_existing`, `VersionedDbiStore::open_existing`, selective scoped stores, logical delivery order and outbox stores, and marker-aware logical adapters. Representative baseline lines are `SyncEngine.hpp:4246-4276`, `SyncEngine.hpp:4289-4300`, and `SyncEngine.hpp:4640-4645`.

**Reproducer**

On an empty environment, repeatedly call an optional store `open_existing()` from a read-only transaction, then exercise another sync path. A minimal direct libmdbx reproducer called `mdbx_dbi_open(txn, "missing", 0, &dbi)` and then opened another named DBI. The first call returned `MDBX_NOTFOUND` while reserving the missing handle slot. The repository baseline reproduced the problem in `test_http_transport`, `test_sync_engine`, `test_sync_randomized`, `test_sync_randomized_lifecycle`, `test_sync_replication`, `test_sync_worker`, and `test_websocket_transport`.

**Expected behavior**

An absent optional DBI returns false and leaves the MDBX catalog and handle table unchanged.

**Observed behavior**

The bundled Debug libmdbx asserted in `dbi_update` after a missing named-DBI probe. Release builds hid the assertion but still allowed the handle table to grow from negative probes.

**Impact**

Sync startup, recovery, transport tests, and any long-running process that repeatedly checks optional stores can fail or exhaust the configured DBI handle budget. The failure is independent of user payload data and is therefore especially difficult to diagnose from application logs.

**Coverage and correction**

The branch adds `sync/detail/NamedDbiLookup.hpp`, which opens only MainDB and checks the named DBI catalog record with `mdbx_get`. Optional paths now perform this lookup before opening an existing DBI. `tests/test_sync_stores.cpp` repeats absent-store probes in one transaction. Full C++11 and C++17 suites pass after the change.

**Migration**

None. No DBI names, flags, persisted bytes, or wire frames changed. The correction is source-compatible.

### F-002. Dangling scalar serializer view

**Location in baseline**

`include/mdbx_containers/detail/utils.hpp:910`, the trivially-copyable `serialize_value` overload.

**Reproducer**

Call `serialize_value(int_value, scratch)`, let the call return, and then copy the bytes from the returned `MDBX_val`. An AddressSanitizer build reported a stack-use-after-scope at the first consumer of a captured scalar value. A small ASan probe on the corrective branch also verifies that changing the source variable after serialization does not change the encoded bytes.

**Expected behavior**

The returned `MDBX_val` remains valid until the associated scratch object is reused, as the other scratch-backed value serializers do.

**Observed behavior**

The baseline returned a view into the address of the function parameter. The parameter ceased to exist on return. The code therefore relied on a dead stack object while `mdbx_put`, capture, or codec code consumed the value.

**Impact**

Scalar values can be persisted or captured with stale or overwritten bytes. The defect affects every trivially-copyable value type that uses this overload, including integer and user-defined scalar payloads.

**Coverage and correction**

The serializer now copies the value into `SerializeScratch::bytes` through `view_copy`. `tests/utils_test.cpp` checks that the encoded value remains unchanged after the source variable is modified. The branch was built with a manual AddressSanitizer probe and passed the full C++11 and C++17 suites.

**Migration**

None. The serialized representation is unchanged. The lifetime guarantee is corrected to match the existing scratch contract.

### F-003. Selective descriptor validation ordering

**Location in baseline**

`SyncEngine::register_selective_replication_scope` in `include/mdbx_containers/sync/engine/SyncEngine.hpp:282`, with descriptor validation in `SelectiveReplicationStore` reached only after transaction and system-store setup.

**Reproducer**

Pass a descriptor whose `scope_id` exceeds `selective_replication_max_scope_id_len`, or whose manifest exceeds the v1 bound. The baseline starts a writable transaction and opens system DBIs before the descriptor is rejected. With an oversized key reaching an MDBX lookup, Debug libmdbx asserted on key-size validation.

**Expected behavior**

A structurally invalid descriptor is rejected before a transaction, DBI lookup, or durable mutation begins.

**Observed behavior**

The baseline validated some descriptor properties only inside `register_or_verify`, after the engine had already initialized stores and iterated manifest entries.

**Impact**

Malformed input can trigger low-level assertions and unnecessary write-side effects. The failure boundary also made rollback and fault-injection behavior dependent on which earlier DBIs happened to exist.

**Coverage and correction**

`SelectiveReplicationStore::validate_descriptor_for_registration` exposes a preflight-only validation seam. The engine invokes it before `Connection::transaction`. Existing protocol bound tests remain green, and the new placement preserves the existing durable format.

**Migration**

None.

### F-004. `CompositeKey` reconciliation required an undocumented hash

`KeyValueTable::db_reconcile` used `std::unordered_set<KeyT>`. `CompositeKey` intentionally provides ordering and canonical byte serialization but no `std::hash` specialization. A public `KeyValueTable<CompositeKey<...>, T>::reconcile(std::map<...>)` therefore failed to compile. The correction stores canonical serialized key bytes in an ordered set. It works for keys that satisfy the documented serialization contract and does not change the database key encoding.

### F-005. C++17 `std::byte` decoding

The baseline grouped `std::byte` with `uint8_t` sequence types and attempted to construct `vector<byte>`, `deque<byte>`, or `list<byte>` from `uint8_t` iterators. GCC rejected the overload during normal public-header compilation. Dedicated byte overloads now copy or append bytes explicitly, and generic trivially-copyable sequence overloads exclude these three types.

### F-006. Null connection handling

`BaseTable` dereferenced the moved `shared_ptr<Connection>` while constructing a table. Passing an empty shared pointer was an immediate undefined dereference. The constructor now throws `std::invalid_argument`. `tests/key_table_test.cpp` covers the public constructor path.

### F-007. Stale manual transaction map entry

`Connection::begin()` checked `m_transactions` before checking whether the mapped `Transaction` had already been directly committed or rolled back through `current_txn()`. The native transaction had ended, but a second manual begin still failed with “Transaction already started”. The begin path now erases an inactive stale entry before enforcing the duplicate guard. The test uses a public `current_txn()` handle and then starts a new manual transaction.

### F-008. Reconfiguration after connect

The public contract says `configure()` must precede `connect()`, but the baseline replaced `m_config` while `m_env` remained live. This could make `is_read_only()` and other configuration queries describe a different environment from the open MDBX handle. `configure()` now rejects a connected environment with `std::logic_error`.

### F-009. Stale repository links

`README.md`, `README-RU.md`, and `docs/mainpage.dox` still point to `github.com/NewYaroslav/mdbx-containers`. The actual repository is under `LimiNode`. This is a documentation-only P3 finding and was left out of the corrective code PR to keep the logic change independently reviewable.

### F-010. AGENTS files in installation

The baseline copied repository-local `AGENTS.md` files under `include/` into installed packages. The branch excludes these files from the install directory. The installed package check found no `AGENTS.md`.

### F-011. Sync engine header size

`SyncEngine.hpp` contains lifecycle, raw apply, snapshot materialization, selective preflight, logical recovery, ordered delivery, and transport policy in one template-heavy header. This is an architectural risk for compile time and reviewability, but no correctness failure was established. A future split should preserve the current public umbrella headers and transaction seams.

### F-012. Reconciliation materialization cost

The generic safe correction serializes each source key into a byte vector and stores it in `std::set`. This avoids a hidden `std::hash<KeyT>` requirement, but it adds one copy and `O(log n)` membership lookup per source and database key. The current behavior is correct and bounded by the existing transaction. A future optimization can use a canonical byte digest or a transparent ordered view only after measuring collision and allocation behavior.

### F-013. Exact vector scan

The exact vector path intentionally evaluates every stored vector. On this host, the quick preset processed 2,000 vectors of dimension 128 in about 14 ms per query batch and the realistic preset processed 50,000 vectors of dimension 384 in about 10.9 to 11.8 seconds for 512 queries, about 43 to 47 queries per second. This is an expected `O(N * D)` algorithmic boundary, not a regression. It becomes a release blocker only if the product contract requires approximate or indexed search at that scale.

## Negative and unavailable results

- No P0 corruption or data-loss defect was reproduced after the baseline findings were isolated.
- Transaction rollback, commit-failure seams, foreign-environment checks, wrong-thread checks, malformed payload paths, snapshot replay, logical recovery, duplicate delivery, and ordered delivery were exercised by the existing suite and targeted tests.
- Full C++17 Debug passed 234 of 234 tests.
- Full C++11 Debug passed 233 of 233 tests.
- A manual AddressSanitizer probe covering the scalar lifetime and missing-DBI paths passed.
- CMake's automatic ASan feature probe disabled its `MDBXC_USE_ASAN` option for this compiler setup. The limitation is recorded rather than presented as a sanitizer matrix result.
- Native Clang, MinGW, MSVC, macOS, Doxygen, TSan, and Valgrind were not available in this execution environment. Their status remains a CI verification item.

## Required for reliable operation

1. Merge the named-DBI lookup correction and keep the absent-store regression test.
2. Merge the scalar serializer lifetime correction and retain sanitizer coverage in supported CI jobs.
3. Merge descriptor preflight before allowing selective replication registration in production.
4. Require Linux and Windows CI to exercise sync-enabled Debug and Release builds from the corrective head.
5. Verify the corrective head on the repository's macOS and MinGW jobs before release.

## Useful but non-blocking

1. Correct stale `NewYaroslav` links in a documentation-only PR.
2. Split `SyncEngine.hpp` behind existing umbrella headers after measuring compile-time impact.
3. Profile reconciliation allocation behavior with large composite and binary keys.
4. Treat exact vector search as a deliberate `O(N * D)` mode and benchmark any future index against the current exact results.
5. Add a native Doxygen job and fail on warnings from owned headers.

