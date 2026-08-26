# Codebase Orientation

## Purpose

This document helps agents enter the `mdbx-containers` codebase quickly and
make safe changes. It is not a full file-by-file inventory; it is a map of the
architecture, reusable mechanisms, and compatibility traps.

It is based on the source files under `include/`, `tests/`, `examples/`,
`docs/*.dox`, and the root `CMakeLists.txt`. Generated output is not a source
of truth.

## Project Structure

| Path | Purpose | Edit when |
| --- | --- | --- |
| `include/mdbx_containers.hpp` | Top-level public include for tables, sync, and vector APIs. | Changing the umbrella API. |
| `include/mdbx_containers/*.hpp` | Public headers: aggregators and table APIs. Shared utility headers live in their owning domain. | Changing table user APIs or reusable user-facing helpers. |
| `include/mdbx_containers/common/` | Core infrastructure: `Config`, `Connection`, `Transaction`, `MdbxException`. | Changing env/config/transaction/error behavior. |
| `include/mdbx_containers/sync/core/` | Sync foundation: cancellation, conflict policy, and apply observers. Reached through `sync/core.hpp`. | Changing lifecycle-neutral sync primitives. |
| `include/mdbx_containers/sync/capture/` | Raw write capture: sink interface, scope, and accumulator. Reached through `sync/capture.hpp`. | Changing capture attachment or changelog emission. |
| `include/mdbx_containers/sync/protocol/` | Raw replication DTOs, bounds, batch/snapshot codecs, and cursors. Reached through `sync/protocol.hpp`. | Changing the raw sync wire contract. |
| `include/mdbx_containers/sync/engine/` | Sync coordinator, peer abstraction, direct peers, worker, and session helpers. Reached through `sync/engine.hpp`. | Changing replication orchestration or worker lifecycle. |
| `include/mdbx_containers/sync/transport/` | Framework-neutral HTTP/WebSocket seams, transport codec, and middleware. Reached through `sync/transport.hpp`. | Changing transport-neutral request handling or policy hooks. |
| `include/mdbx_containers/sync/logical/` | Logical schema, delivery, and durable logical state. Reached through `sync/logical.hpp`. | Changing typed logical replication contracts or durable logical state. |
| `include/mdbx_containers/sync/logical/adapters/` | Table-bound logical adapters. Reached through `sync/adapters.hpp`. | Changing typed logical replication contracts or adapter-local storage. |
| `include/mdbx_containers/sync/stores/` | Durable raw-replication state: metadata, changelog, origin, applied-cursor, and identity indexes. Reached through `sync/storage.hpp`. | Changing raw changelog persistence or apply bookkeeping. |
| `include/mdbx_containers/sync/transports/` | Optional bindings to concrete HTTP/WebSocket libraries. Reached through `sync/transports/*.hpp`. | Changing backend-specific transport integration. |
| `include/mdbx_containers/detail/` | Internal building blocks: `BaseTable`, `TransactionTracker`, serialization, path utilities, vendored/private helpers. Not public API. | Changing shared mechanisms. |
| `tests/` | Standalone CTest executables. | Adding behavior, regressions, serialization, path, or transaction checks. |
| `examples/` | User-facing API examples. | Public usage scenarios change. |
| `docs/*.dox` | Doxygen page sources. | Public API or user-facing behavior changes. |
| `cmake/`, `CMakeLists.txt` | CMake targets, options, install/export, MDBX dependency policy. | Build or dependency behavior changes. |
| `external/` | Vendored dependency infrastructure and the bundled `external/libmdbx` submodule. | Usually only for build/dependency tasks. |
| `docs/html/`, `docs/latex/` | Generated Doxygen output. | Do not edit manually. |
| `build-*`, `Testing/`, `build.log` | Local build/test artifacts. | Do not use as source of truth. |

Primary entry points:

- User API: `include/mdbx_containers.hpp`,
  `include/mdbx_containers/KeyValueTable.hpp`,
  `include/mdbx_containers/HashedKeyValueStore.hpp`,
  `include/mdbx_containers/ValueTable.hpp`,
  `include/mdbx_containers/AnyValueTable.hpp`,
  `include/mdbx_containers/KeyTable.hpp`,
  `include/mdbx_containers/KeyMultiValueTable.hpp`,
  `include/mdbx_containers/KeyOrderedMultiValueTable.hpp`,
  `include/mdbx_containers/MetadataTable.hpp`,
  `include/mdbx_containers/SequenceTable.hpp`.
- Shared public components: `include/mdbx_containers/common.hpp`.
- Transactions/config: `common/Connection.hpp`, `common/Transaction.hpp`,
  `common/Config.hpp`.
- Serialization: `detail/utils.hpp`.
- Path policy: `detail/path_utils.hpp` and `tests/path_resolution_test.cpp`.
- Table choice and method semantics: `guides/table-api-guide.md`.

## Architecture

The project is not a DDD application with bounded contexts, DTOs, events,
HTTP/WebSocket endpoints, or application services. It is a persistence adapter
library over libmdbx.

Real subdomains:

- **Persistent tables**: active `KeyValueTable`, active
  `HashedKeyValueStore`, active `ValueTable`, active `AnyValueTable`, active
  `KeyTable`, active `KeyMultiValueTable`, active
  `KeyOrderedMultiValueTable`, active `MetadataTable`, active `SequenceTable`.
- **MDBX environment and transactions**: `Connection`, `Transaction`,
  `TransactionTracker`, `BaseTable`.
- **Serialization**: `serialize_key`, `serialize_value`,
  `deserialize_value`, traits, `SerializeScratch`.
- **Path/config**: `Config`, `path_utils.hpp`, `Connection::db_init()`.
- **Build/docs/tests/examples**: CMake, Doxygen pages, CTest executables.

Where to find types:

- Data/config structures: `Config` in `common/Config.hpp`,
  `PathComponents` in `detail/path_utils.hpp`, test/example structs in
  `tests/*.cpp` and `examples/*.cpp`.
- Interfaces: there are no dedicated public abstract interfaces.
- Implementations: code is often inline in public headers or `.ipp` files
  (`Connection.ipp`, `Transaction.ipp`, `TransactionTracker.ipp`).
- Events/modules: no event system or module registry exists.

Namespace:

- Library code lives in `namespace mdbxc`.
- There are no separate namespaces for `domain` or `infra`.
- Some implementation-only traits live in `namespace mdbxc::detail`; callers
  must treat both `mdbxc::detail` and `include/mdbx_containers/detail/` as
  private, unstable implementation space.
- `namespace fs = std::filesystem` is used inside `path_utils.hpp` and
  `Connection.ipp` under C++17 guards.

Dependency rules:

- Public table headers may depend on `common.hpp` and through it on
  `Connection`, `BaseTable`, and serialization helpers.
- `common/hashing.hpp` is an internal `common` leaf. Its types are exported by
  `common.hpp` and, for table users, by `tables.hpp`; users should include an
  owning umbrella rather than this leaf directly. `common/backup.hpp` remains
  an explicitly supported direct utility include. Keep exported types
  C++11-compatible unless guarded.
- `common/Connection.*` may depend on `detail/path_utils.hpp`, `Transaction`,
  `Config`, and `check_mdbx`.
- `detail/utils.hpp` should remain low-level: serialization, MDBX error
  checking, key flags. Do not add table-specific logic there.
- Headers under `include/mdbx_containers/detail/` may be included by public
  headers, but they are not a supported direct user include surface.
- `tests/` and `examples/` may depend on public headers, but library headers
  must not depend on tests, examples, or docs.

Avoid:

- Adding HTTP/JSON/WebSocket/logging frameworks; those domains do not exist.
- Using generated copies from `build-*/include`.
- Binding public headers to C++17-only APIs without guards or C++11 fallbacks.

## Existing Patterns

- **RAII**: `Transaction` starts a transaction in the constructor and closes it
  in `commit()`, `rollback()`, or the destructor.
- **Factory method**: `Connection::create(const Config&)` returns
  `std::shared_ptr<Connection>` and opens the environment.
- **Table Gateway / repository-like API**: `KeyValueTable`, `ValueTable`, and
  `AnyValueTable` hide MDBX DBI/cursor handling behind typed methods.
- **Adapter over MDBX C API**: `check_mdbx`, `BaseTable`, `Connection`, and
  private `db_*` methods translate raw MDBX calls into C++ APIs.
- **Template strategy / SFINAE**: `serialize_key`, `serialize_value`, and
  `deserialize_value` are selected by traits in `detail/utils.hpp`.
- **Transaction wrapper helper**: tables use private `with_transaction(...)`
  helpers that accept external `MDBX_txn*`, reuse thread-bound transactions, or
  create their own.
- **Assignment proxy**: `KeyValueTable::AssignmentProxy` implements
  persist-on-assignment `operator[]` behavior.
- **Compatibility guards**: `#if __cplusplus >= 201703L` protects
  `std::optional`, `std::filesystem`, structured bindings, and `std::byte`.

Prefer for new table code:

- Inherit from `BaseTable`, accept `std::shared_ptr<Connection>` and `Config`,
  and add overloads for `MDBX_txn*` and `const Transaction&`.
- Use `BaseTable` for primary DBI read-only behavior: table constructors may
  keep `MDBX_CREATE` defaults, but `BaseTable` strips that flag and uses
  `TransactionMode::READ_ONLY` when `Connection::is_read_only()` is true.
  If a wrapper opens extra DBIs outside `BaseTable`, repeat the same rule there.
- Wrap raw MDBX calls with `check_mdbx`; handle `MDBX_NOTFOUND` and
  `MDBX_KEYEXIST` explicitly.
- Use `SerializeScratch` near each MDBX call that needs an `MDBX_val`.
- Split public methods from private `db_*` implementations, following
  `KeyValueTable`.
- Use `guides/table-api-guide.md` to choose the correct existing table class or
  to keep new table behavior consistent with current method semantics.

Do not copy without review:

- `AnyValueTable` type-tag checks are opt-in. Raw records remain the default
  storage format unless `set_type_tag_check(true)` is enabled on the table
  instance.
- `KeyMultiValueTable` hidden duplicate sequence-prefix storage; do not change
  it without migration notes and compatibility tests.
- `tests/mdbx_test.cpp` is a raw MDBX smoke test, not the library API style.
- `docs/html/`, `docs/latex/`, and `build-*/include` are generated output.

## Code Style

Naming:

- Classes/structs/enums: `CamelCase` (`Connection`, `SerializeScratch`,
  `TransactionMode`).
- Methods/functions: `snake_case` (`insert_or_assign`, `current_txn`,
  `serialize_value`).
- Fields: `m_` prefix (`m_connection`, `m_dbi`, `m_env`).
- Header guards: `_MDBX_CONTAINERS_..._INCLUDED`.
- File names: primary public classes use `CamelCase.hpp`; utility files use
  `snake_case.hpp`.

Namespace style:

- One project namespace: `namespace mdbxc { ... }`.
- Closing comments are used (`} // namespace mdbxc` or `}; // namespace mdbxc`);
  preserve nearby style when editing.

Doxygen:

- Public headers use `///` comments with `\brief`, `\tparam`, `\param`,
  `\return`, and `\throws`.
- `.dox` files use Doxygen pages/groups. Update `docs/*.dox`, not generated
  HTML/LaTeX.
- Keep comments in English for public API.

Errors:

- MDBX return codes go through `check_mdbx(rc, "context")`, which throws
  `MdbxException`.
- Expected misses are not exceptions: `MDBX_NOTFOUND` returns `false`, an empty
  optional, or a compatibility result.
- Type/serialization corruption uses `std::runtime_error`.
- Missing required keys can use `std::out_of_range`, as in `at()` and
  `AnyValueTable::get()`.
- Transaction state errors use `std::logic_error` in `Connection`.

Logging:

- Library headers do not log. Tests/examples may use `std::cout`, `std::cerr`,
  or `printf`.
- Do not add a logging framework for library internals.

Pointers and ownership:

- `Connection::create()` returns `std::shared_ptr<Connection>`.
- Tables store `std::shared_ptr<Connection>` in `BaseTable::m_connection`.
- `Connection` stores manual per-thread transactions as
  `std::shared_ptr<Transaction>`.
- The normal threading model is one shared `Connection` per MDBX environment
  and at most one active transaction per thread.
- `Transaction`, raw `MDBX_txn*`, and MDBX cursors are thread-owned; never move
  them for use by another thread.
- `Config` storage is `std::optional<Config>` in C++17+ and
  `std::unique_ptr<Config>` in the C++11 fallback.
- Raw `MDBX_env*`, `MDBX_txn*`, `MDBX_cursor*`, and `MDBX_dbi` are MDBX handles;
  keep ownership and cleanup local and explicit.

C++ standard:

- Baseline is C++11.
- C++17 features must be guarded. Examples: `std::optional`,
  `std::filesystem`, structured bindings.
- `std::byte` support is C++17-only.

## Reusable Utilities

Reuse:

- `check_mdbx()` from `detail/utils.hpp` for MDBX errors.
- `get_mdbx_flags<KeyT>()` for integer-like MDBX key flags.
- `serialize_key()`, `serialize_value()`, `deserialize_value()` for table
  storage paths.
- `SerializeScratch` for temporary `MDBX_val` backing storage.
- `Connection::transaction()`, `Connection::begin/commit/rollback()` for
  transaction management.
- `BaseTable::thread_txn()` through each table's `with_transaction()` style.
- `is_explicitly_relative()`, `is_absolute_path()`, `get_exec_dir()`,
  `create_directories()`, and compatibility path helpers in `path_utils.hpp`.

Not present in this project:

- Time/date utilities.
- JSON utilities.
- HTTP/WebSocket clients or endpoints.
- Event bus, task manager, or async job framework.
- Structured logging.

Universal vs domain-specific:

- Universal within the library: serialization traits/helpers, path helpers,
  `check_mdbx`, `SerializeScratch`.
- MDBX-domain-specific: `BaseTable`, `Transaction`, `Connection`,
  `get_mdbx_flags`.
- Table-specific: `KeyValueTable` private `db_*` methods,
  `AnyValueTable` typed methods and type-tag toggles.

## How To Extend

### New Domain Or Subdomain

Do not add DDD layers for structure alone. In this architecture a new "domain"
usually means a new public table type, serialization helper set, or
config/path/build capability. Start in the existing directories:

- Public table API: `include/mdbx_containers/NewTable.hpp`.
- Shared infrastructure: `include/mdbx_containers/common/`.
- Internal helper: `include/mdbx_containers/detail/`.
- Test: a focused `tests/*_test.cpp`.
- Example: `examples/*_example.cpp` when the scenario is user-facing.

### New Serializable Data Type

For a custom type, add `to_bytes()` and static `from_bytes()`. Do not write a
separate serializer inside a table.

```cpp
struct PricePoint {
    int64_t ts = 0;
    double price = 0.0;

    std::vector<uint8_t> to_bytes() const {
        std::vector<uint8_t> bytes(sizeof(ts) + sizeof(price));
        std::memcpy(bytes.data(), &ts, sizeof(ts));
        std::memcpy(bytes.data() + sizeof(ts), &price, sizeof(price));
        return bytes;
    }

    static PricePoint from_bytes(const void* data, size_t size) {
        if (size != sizeof(int64_t) + sizeof(double)) {
            throw std::runtime_error("Invalid data size for PricePoint");
        }
        PricePoint out{};
        const uint8_t* p = static_cast<const uint8_t*>(data);
        std::memcpy(&out.ts, p, sizeof(out.ts));
        std::memcpy(&out.price, p + sizeof(out.ts), sizeof(out.price));
        return out;
    }
};
```

Verify through `KeyValueTable<std::string, PricePoint>` in `tests/`.

### New Table Or Module

Add the public header under `include/mdbx_containers/`. Follow this shape:

- `template<class ...> class NewTable final : public BaseTable`.
- Constructor from `std::shared_ptr<Connection>`.
- Constructor from `const Config&`.
- Public methods accept optional `MDBX_txn*`.
- Overload with `const Transaction&` delegates to `.handle()`.
- Private methods perform raw MDBX work and are named `db_*`.
- Use the existing `with_transaction()` pattern.

After adding a table, update tests and examples/docs when needed, and decide
whether it should be included by `include/mdbx_containers.hpp`.

### New Enum

Add an enum next to the type that uses it if it is not shared API. Put shared
modes in `common/`, as `TransactionMode` does in `common/Transaction.hpp`.

Rules:

- Use `enum class`.
- Current enum value names are uppercase (`READ_ONLY`, `WRITABLE`).
- Document `\enum` and each value if the enum is public.
- Do not change existing values casually; they are public API.

### New Interface Implementation

The project currently has no hierarchy of public interfaces. Prefer concrete
template tables and helper functions. If an interface is truly needed, first
check whether an overload/helper beside an existing type solves the problem.
New virtual interfaces must be justified by a testable scenario and must not
pull runtime polymorphism into hot serialization/table paths.

### Extending An Existing Operation

The project has no event handlers. The nearest analogue is a public table
method that delegates to a private `db_*` method.

Example: adding `get_or()` to `KeyValueTable`:

- Public method uses `with_transaction(..., READ_ONLY, txn)`.
- C++17 APIs may return `std::optional`; C++11 fallbacks must remain usable.
- Private raw lookup should reuse `db_get()` rather than calling `mdbx_get`
  again.

```cpp
ValueT get_or(const KeyT& key, ValueT fallback, MDBX_txn* txn = nullptr) const {
    ValueT out{};
    bool found = false;
    with_transaction([this, &key, &out, &found](MDBX_txn* t) {
        found = db_get(key, out, t);
    }, TransactionMode::READ_ONLY, txn);
    return found ? out : fallback;
}
```

Add the `const Transaction&` overload beside the primary method.

### HTTP/WebSocket Endpoint Or Client

This does not apply to the current project because there is no network layer.
If a task genuinely requires an endpoint/client, it needs a separate design
decision first: example, external integration layer, or new library dependency.
Do not add an HTTP/WebSocket framework to persistence-library headers.

## Invariants And Safety

- Do not keep `MDBX_val` longer than the nearest MDBX call. Its memory often
  belongs to `SerializeScratch` or an external object.
- Do not return views or pointers to internal MDBX buffers to users.
- Do not use `thread_local` STL buffers in serialization paths. This is a known
  MinGW/Windows problem.
- Do not silently start nested transactions. `Connection::begin()` forbids a
  second manual transaction on the same thread.
- Do not call `configure()`, `connect()`, `disconnect()`, or `Connection`
  destruction while table operations, transactions, or cursors may still be
  active on worker threads.
- Use `Connection::shutdown()`/`shutdown_for()` for coordinated stop behavior.
  Keep `disconnect()` strict: it closes only when no transaction handle is open
  and must not abort transaction handles owned by another thread.
- Do not pass transaction guards, raw `MDBX_txn*`, or MDBX cursors between
  threads. mdbx-containers does not enable `MDBX_NOSTICKYTHREADS`.
- Preserve C++11 ABI/API fallbacks for public headers.
- Do not change the binary serialization format without an explicit migration
  strategy and backward-compatibility tests.
- Do not change path resolution policy without updating
  `tests/path_resolution_test.cpp` and `docs/configuration.dox`.
- Do not change `Config` defaults casually; they affect env flags, durability,
  file/directory mode, and max DBI.
- Do not bypass read-only DBI opening behavior. A read-only connection must only
  open existing DBIs, with `MDBX_CREATE` removed, and write attempts should still
  fail through MDBX. This applies both to `BaseTable` and to wrapper-specific
  extra DBIs such as the default `HashedKeyValueStore` hash index.
- `Connection` cleanup and transaction registry require care:
  `m_mdbx_mutex`, `m_transactions`, `TransactionTracker::m_thread_txns`.
  `m_mdbx_mutex` protects lifecycle/config state and the manual transaction map;
  `TransactionTracker::m_mutex` protects the raw transaction registry and the
  shutdown wait condition variable. Neither mutex makes MDBX transaction handles
  cross-thread safe.
- Cursor cleanup must be explicit. If early returns/exceptions are added around
  cursor logic, make sure the cursor is closed.
- `AnyValueTable` type-tag checking is opt-in and default tags are
  implementation-defined. Durable schemas should specialize
  `AnyValueTypeTag<T>`.

## Usually Do Not Change

- `external/libmdbx/**`: vendored/bundled dependency.
- `build-*`, `Testing/`, `build.log`: local artifacts.
- `docs/html/**`, `docs/latex/**`: generated docs.
- `build-*/include/**`: header copies created by build targets.
- `tests/mdbx_test.cpp`: raw MDBX diagnostic/smoke test, not the primary
  library style example.

## Agent Checklist

- Find the source file under `include/`, not a build copy.
- Check C++11 and C++17 branches when public templates change.
- Use `Config::pathname`, constructor table names, and `Config::max_dbs` for
  multiple DBIs.
- Remember default `HashedKeyValueStore` uses the LargeValues layout and two
  DBIs per logical store: records plus `name + "__hash_index"`. The opt-in
  SmallValues layout uses one DUPSORT DBI and is size-limited by
  `Config::max_dupsort_value_size`. In read-only mode both LargeValues DBIs must
  already exist.
- Reuse `SerializeScratch` and serialization helpers.
- Wrap MDBX errors through `check_mdbx`.
- Add tests near existing tests for the same mechanism.
- Do not edit generated docs; update `.dox` sources.
- For transaction/lifetime changes, re-check MDBX docs:
  `mdbx_txn_begin()` in https://libmdbx.dqdkfa.ru/group__c__transactions.html
  and `mdbx_env_close_ex()` in
  https://libmdbx.dqdkfa.ru/group__c__opening.html.
