# MDBX-Containers

[![MIT License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE) ![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux%20%7C%20macOS-blue) ![C++ Standard](https://img.shields.io/badge/C++-11--17-orange) [![CI Windows](https://img.shields.io/github/actions/workflow/status/NewYaroslav/mdbx-containers/ci.yml?branch=main&label=Windows&logo=windows)](https://github.com/NewYaroslav/mdbx-containers/actions/workflows/ci.yml) [![CI Linux](https://img.shields.io/github/actions/workflow/status/NewYaroslav/mdbx-containers/ci.yml?branch=main&label=Linux&logo=linux)](https://github.com/NewYaroslav/mdbx-containers/actions/workflows/ci.yml) [![CI macOS](https://img.shields.io/github/actions/workflow/status/NewYaroslav/mdbx-containers/ci.yml?branch=main&label=macOS&logo=apple)](https://github.com/NewYaroslav/mdbx-containers/actions/workflows/ci.yml)

[Russian version](README-RU.md)

**mdbx-containers** is a lightweight header-only C++11/17 library that bridges [libmdbx](https://github.com/erthink/libmdbx) with familiar STL-style APIs. It persists key-value data in MDBX while providing high performance and transaction helpers.

> Note  
> This project values technical merit over personal views of its authors.  
> See [PHILOSOPHY.md](PHILOSOPHY.md) for details.

## ⚙️ Features

### 🧱 Table APIs
- `KeyValueTable<K, V>` is the main implemented table: one value per key with `insert`, `insert_or_assign`, `find`, `range`, `range_values`, `for_each_range`, `filter_range`, `lower_bound`, `upper_bound`, `range_reverse`, `erase_range`, `update`, `find_many`, `operator[]`, and related helpers.
- `HashedKeyValueStore<K, V, H, Layout>` stores one value per string or byte-vector key through a hash index and verifies original key bytes to handle collisions.
- `ValueTable<V>` stores one strongly typed singleton value per named table for metadata, module state, snapshots, and single-object configuration records.
- `AnyValueTable<K>` stores heterogeneous values by caller-selected type and supports typed `set`, `insert`, `get`, `find`, `get_or`, `update`, `contains`, `erase`, and `keys`.
- `KeyTable<K>` stores unique keys with a `std::set`-like API: `insert`, `contains`, `range`, `for_each_range`, `filter_range`, `lower_bound`, `upper_bound`, `range_reverse`, `erase_range`, `clear`, `load`, `reconcile`, and related helpers.
- `KeyMultiValueTable<K, V>` stores multiple values per key with a `std::multimap`-like API, streaming and materialized key-range scans, reverse scans, range erasure, and repeated identical `(key, value)` pair preservation.
- `KeyOrderedMultiValueTable<K, V>` stores multiple values per key where current append order is part of the API; repeated identical values stay visible and `find(key)` returns values in order.
- `SequenceTable<ValueT>` stores values by stable uint64_t id with append-only semantics and sparse index support. Append returns a stable id; erase does not reindex following records.
- `AnyValueTable` type-tag prefix verification is opt-in via `set_type_tag_check(true)` and is disabled by default for compatibility with existing raw records.
- `VectorStore` is an MVP embedded vector store for local RAG: persistent MDBX
  storage with an exact in-memory `FlatVectorIndex`.

### 🔁 Serialization
- Automatic serialization of trivially copyable types.
- Custom types via `to_bytes()` / `from_bytes()`.
- Supports nested STL containers like `std::vector` or `std::list`.

### 🔒 Transactions and Threads
- RAII transactions (`Transaction`).
- Thread-bound automatic and manual transaction reuse.
- Use one shared `Connection` per MDBX environment, with at most one active
  transaction per thread.
- Do not share `Transaction`, raw `MDBX_txn*`, or MDBX cursors across threads.
- Caller-supplied `Transaction` / raw `MDBX_txn*` handles must also belong to
  the same MDBX environment as the table or sync engine that receives them.
  Foreign-environment handles are rejected with `std::invalid_argument` before
  the wrapper uses DBI handles.
- Treat `configure()`, `connect()`, `disconnect()`, and `Connection`
  destruction as lifecycle operations outside concurrent table activity.
- Use `shutdown()` to request a coordinated stop: it rejects new transactions,
  waits for open transaction handles to close on their owning threads, then
  disconnects. Use `shutdown_for(timeout)` when the caller needs a bounded wait.
- Use `disconnect()` only after all transactions/cursors are already gone; it
  fails with `MDBX_BUSY` instead of aborting transactions from other threads.
- This follows MDBX `mdbx_txn_begin()` and `mdbx_env_close_ex()` rules:
  [Transactions](https://libmdbx.dqdkfa.ru/group__c__transactions.html) and
  [Opening & Closing](https://libmdbx.dqdkfa.ru/group__c__opening.html).

### 🔄 Sync Replication
- Experimental sync is opt-in: define `MDBXC_SYNC_ENABLED=1` before including
  `mdbx_containers/sync.hpp`.
- v0.1 captures normal write paths for `KeyValueTable`, `KeyTable`,
  `ValueTable`, and `SequenceTable`; `VectorStore` is replicated indirectly
  through its internal `SequenceTable` and `KeyValueTable` members.
- `AnyValueTable`, `KeyMultiValueTable`, `KeyOrderedMultiValueTable`, and
  `HashedKeyValueStore` are not replicated in v0.1. `KeyMultiValueTable` has a
  deferred unordered multiset sync design in `sync/DESIGN.md`.
  `KeyOrderedMultiValueTable` is the order-sensitive table API, but its sync
  capture/apply contract remains disabled until capture and round-trip tests
  exist. `AnyValueTable` and `HashedKeyValueStore` still need type-tag and
  hash-index identity designs.
- Application CRUD code does not need per-method sync wrappers for supported
  tables. Attach `ThreadLocalChangeAccumulator` to the writing `Connection`;
  use `SyncCaptureScope` for bounded write phases, or the lower-level
  `attach_sync_capture()` / `detach_sync_capture()` pair for a deliberately
  long-lived application capture lifecycle. Nested capture scopes must end in
  strict LIFO order; do not change the connection capture sink directly while a
  scope is active. Committed standalone writes become standalone sync batches,
  while an explicit transaction spanning several supported tables becomes one
  atomic local batch. Read/search calls are not captured. A separate
  `SyncWorker` plus an `ISyncPeer` transport moves committed batches between
  nodes.
- When sync capture is attached, mutating supported table calls must use
  connection-managed transactions (`mdbx_containers::Transaction` or
  `Connection::begin()` / `commit()`). Caller-created raw writable
  `MDBX_txn*` handles cannot run the capture pre-commit hook and are rejected
  before mutation. Caller-created raw read-only transactions remain valid for
  read/search snapshot operations. Any exception from capture recording or
  flushing makes that transaction rollback-only; retrying `commit()` is
  rejected.
- `SyncEngine` exposes pull/push/apply primitives, `DirectSyncPeer` provides
  in-process sync for tests and examples, `HttpSyncPeer` defines an HTTP-shaped
  adapter seam, `WebSocketSyncPeer` defines a binary message seam, and
  `SyncWorker` is the background polling driver. `mdbx_containers/sync/transport.hpp`
  is the transport-layer umbrella. Optional ready-made Simple-Web
  HTTP/WebSocket binding headers live under
  `mdbx_containers/sync/transports/simple_web/`, and the optional Kurlyk/libcurl
  HTTP client binding lives under `mdbx_containers/sync/transports/kurlyk/`.
  `MDBXC_SIMPLE_WEB_HTTP_TRANSPORT`,
  `MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT`, and `MDBXC_KURLYK_HTTP_TRANSPORT`
  enable those dependency targets. Concrete backend targets define
  `MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT`,
  `MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT`, or
  `MDBXC_HAS_KURLYK_HTTP_TRANSPORT` for conditional backend includes.
  Installed packages also export CMake provider functions for these ready-made
  transport targets.
  See [sync transport production notes](guides/sync-transport-production.md)
  for TLS/WSS, token rotation, graceful shutdown, structured logging, and
  offline dependency guidance. See the
  [sync table coverage matrix](guides/sync-table-coverage.md) for the current
  wrapper support status, and the
  [sync v0.1 readiness checklist](guides/sync-v0.1-readiness.md) for release
  readiness and deferred work.
  Socket-backed examples use those bindings instead of reimplementing the
  transport in each file. Specialized table wire formats remain deferred.
  HTTP auth, remote-address checks, and rate-limit headers live in
  adapter-local policy context, not inside sync DTOs.
  See
  `include/mdbx_containers/sync/DESIGN.md`.

### 🗄️ Structure & Configuration
- Multiple logical tables inside one MDBX file.
- Flexible configuration: `read_only`, `writemap_mode`, `readahead`, `no_subdir`, `sync_durable`, `max_readers`, `max_dbs`, `relative_to_exe`.
- In `read_only` mode, table wrappers open existing DBIs with a read-only
  transaction and ignore `MDBX_CREATE`; writes still fail through MDBX.
- See `docs/configuration.dox` for details.

### 🧰 Compatibility
- Header-only usage.
- Depends only on [libmdbx](https://github.com/erthink/libmdbx).
- Requires C++11 or later.
- **Windows (MSVC)**: not supported yet. Use MinGW-w64 (GCC) or Clang on Windows.

## 🛠️ Installation

1. Copy the `include/` directory into your project or add this repository as a submodule.
2. Ensure `libmdbx` is available to your build system. Set
   `MDBXC_DEPS_MODE=BUNDLED` to use the bundled submodule at
   `external/libmdbx`, or use `SYSTEM`/`AUTO` for an installed package. When
   this project is added as a subproject, an existing parent-provided
   `mdbx::mdbx`, `mdbx::mdbx-static`, `libmdbx::mdbx`, or
   `libmdbx::mdbx-static` target is reused before package, submodule, or
   FetchContent lookup. Parent-provided targets take precedence over
   `MDBXC_DEPS_MODE`, including `BUNDLED`.
3. Use a C++11 (or later) compiler.

### Build with CMake

```bash
cmake -S . -B build \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=ON \
    -DMDBXC_BUILD_EXAMPLES=ON \
    -DMDBXC_USE_ASAN=ON \
    -DCMAKE_CXX_STANDARD=17
cmake --build build
ctest --test-dir build --output-on-failure
```

> **Warning**
> Compile every translation unit that uses mdbx-containers with the same C++
> language standard, structure packing, and feature macro configuration. Mixing
> C++11 and C++17 builds, or changing ABI-impacting defines between files, can
> lead to ODR violations and undefined behavior.

Windows users can run the provided `.bat` scripts such as `build-mingw-17-examples.bat`, `build-mingw-17-tests.bat`, or `build-mingw-11-tests.bat`.

## 🧪 Usage Examples

### Basic key-value table

```cpp
#include <mdbx_containers/KeyValueTable.hpp>
#include <iostream>
#include <map>

int main() {
    mdbxc::Config config;
    config.pathname = "example.mdbx";
    config.max_dbs = 4;

    auto conn = mdbxc::Connection::create(config);
    mdbxc::KeyValueTable<int, std::string> table(conn, "my_map");

    table.insert_or_assign(1, "Hello");
    table.insert_or_assign(2, "World");

    std::map<int, std::string> result;
    table.load(result);

    for (const auto& pair : result)
        std::cout << pair.first << ": " << pair.second << "\n";

    return 0;
}
```

### Range scans

`range()` follows the same container style as `retrieve_all()` and `operator()()`:
`KeyTable` defaults to `std::set`, `KeyValueTable` defaults to `std::map`, and
`KeyMultiValueTable` defaults to `std::multimap`. Use `range<std::vector>()`
for ordered key or key-value results in `KeyTable` and `KeyValueTable`; use
`range_vector()` when every physical `KeyMultiValueTable` pair must remain
visible as a vector element. `range_values()` defaults to `std::vector` and can
also target containers such as `std::set`.

```cpp
auto by_key = table.range(10, 20);
auto ordered_pairs = table.range<std::vector>(10, 20);
auto unique_values = table.range_values<std::set>(10, 20);
```

Ordered key-based tables also provide `for_each_range()` for streaming scans,
`filter_range()` as a thin collecting helper, `lower_bound()`/`upper_bound()`,
`first()`/`last()`, `min_key()`/`max_key()`, `range_reverse()`,
`contains_range()`, `count_range()`, and `erase_range()`.

### Embedded vector store

`VectorStore` persists embeddings, text, and metadata in MDBX tables and rebuilds
an exact RAM index on open. It is intended as a local RAG MVP: search is exact
`O(N * dim)`, all embeddings are loaded into RAM, and ANN/HNSW, metadata
filtering, and generated embeddings are out of scope.
Collection names are validated, not rewritten: use non-empty names containing
only ASCII letters, digits, `_`, and `-`.

```cpp
#include <mdbx_containers/vector.hpp>
#include <iostream>

mdbxc::Config cfg;
cfg.pathname = "rag.mdbx";
cfg.max_dbs = 8;

mdbxc::VectorStore store(cfg, "docs");

mdbxc::Embedding e1;
e1.dim = 3;
e1.values = {1.0f, 0.0f, 0.0f};

uint64_t id = store.add(e1, "Hello world", "{\"source\":\"test\"}");

mdbxc::Embedding query;
query.dim = 3;
query.values = {1.0f, 0.1f, 0.0f};

auto results = store.search(query, 5);
for (const auto& r : results) {
    std::cout << r.id << " " << r.score << " " << r.text << "\n";
}
```

### Hash-indexed key-value store

```cpp
#include <mdbx_containers/HashedKeyValueStore.hpp>

// LargeValues layout uses two DBIs: one hash index and one record table.
mdbxc::Config config;
config.pathname = "hashed.mdbx";
config.max_dbs = 4;

auto conn = mdbxc::Connection::create(config);
mdbxc::HashedKeyValueStore<std::string, std::string> cache(conn, "cache");

cache.insert_or_assign("url:https://example.test", "queued");
std::string state = cache.at("url:https://example.test");
```

### Key-only table

```cpp
#include <mdbx_containers/KeyTable.hpp>
#include <set>

mdbxc::KeyTable<std::string> keys(conn, "tags");
keys.insert("active");
keys.insert("archived");

std::set<std::string> restored = keys.retrieve_all();
```

### Single-value table

```cpp
#include <mdbx_containers/ValueTable.hpp>

struct AppState {
    int schema_version = 1;
    int active_profiles = 0;

    std::vector<uint8_t> to_bytes() const;
    static AppState from_bytes(const void* data, size_t size);
};

mdbxc::ValueTable<AppState> state(conn, "app_state");
state.set(AppState{});

AppState loaded = state.get_or(AppState{});
```

### Multi-value table

```cpp
#include <mdbx_containers/KeyMultiValueTable.hpp>

mdbxc::KeyMultiValueTable<int, std::string> events(conn, "events");
events.insert(7, "created");
events.insert(7, "created"); // exact repeats are preserved
events.insert(7, "sent");

std::vector<std::string> values = events.find(7);
```

### Ordered multi-value table

```cpp
#include <mdbx_containers/KeyOrderedMultiValueTable.hpp>

mdbxc::KeyOrderedMultiValueTable<int, std::string> timeline(conn, "timeline");
timeline.append(7, "created");
timeline.append(7, "created"); // exact repeats are preserved
timeline.append(7, "sent");

std::vector<std::string> ordered_values = timeline.find(7);
```

### Manual transaction

```cpp
mdbxc::Config config;
config.pathname = "txn.mdbx";
auto conn = mdbxc::Connection::create(config);
mdbxc::KeyValueTable<int, std::string> table(conn, "demo");
mdbxc::ValueTable<int> schema(conn, "schema");

auto txn = conn->transaction(mdbxc::TransactionMode::WRITABLE);
table.insert_or_assign(10, "ten", txn);
schema.set(1, txn);
txn.commit();
```

### Closing a connection

Use `disconnect()` for a clean lifecycle where all transactions and cursors are
already gone:

```cpp
{
    mdbxc::KeyValueTable<int, std::string> table(conn, "items");
    table.insert_or_assign(1, "done");
}
conn->disconnect();
```

Use `shutdown()` when worker threads may still be finishing their current
transaction. It rejects new transactions, waits for open transaction handles,
and then disconnects:

```cpp
stop_requested.store(true);
conn->shutdown();
worker.join();
```

Use `shutdown_for(timeout)` when service stop must be bounded:

```cpp
if (!conn->shutdown_for(std::chrono::seconds(2))) {
    request_worker_stop();
    worker.join();
    conn->shutdown();
}
```

See `examples/connection_shutdown_example.cpp` for a complete runnable example.

### Custom struct serialization

```cpp
struct MyData {
    int id;
    double value;

    std::vector<uint8_t> to_bytes() const {
        std::vector<uint8_t> bytes(sizeof(MyData));
        std::memcpy(bytes.data(), this, sizeof(MyData));
        return bytes;
    }

    static MyData from_bytes(const void* data, size_t size) {
        MyData out{};
        std::memcpy(&out, data, sizeof(MyData));
        return out;
    }
};

mdbxc::KeyValueTable<int, MyData> table(conn, "my_data");
table.insert_or_assign(42, MyData{42, 3.14});
```

## 📚 Documentation

- See the `examples/` directory for more examples. Sync topology examples are
  summarized in `examples/README-sync.md`.
- Sync benchmark commands and CSV notes are in `benchmarks/README-sync.md`.
- API and architecture information lives in the Doxygen source pages under `docs/*.dox`.
- Documentation can be generated with Doxygen; generated `docs/html/` and `docs/latex/` output should not be edited manually.

## 📄 License

This project is licensed under the MIT License.

It can bundle [libmdbx](https://github.com/erthink/libmdbx) from `external/libmdbx`, released under the Apache License 2.0. See `docs/libmdbx.LICENSE` for details.
