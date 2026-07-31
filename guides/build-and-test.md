# Build And Test

## Requirements

- CMake 3.18 or newer.
- A C++11 or newer compiler.
- libmdbx, provided through `MDBXC_DEPS_MODE`.
- Windows users should use MinGW-w64 or Clang; MSVC is not supported yet.

## CMake Options

All project options use the `MDBXC_` prefix.

| Option | Default | Description |
| --- | --- | --- |
| `MDBXC_DEPS_MODE` | `AUTO` | MDBX dependency mode: `AUTO`, `SYSTEM`, or `BUNDLED`. |
| `MDBXC_BUILD_EXAMPLES` | `ON` | Build examples from `examples/`. |
| `MDBXC_BUILD_TESTS` | `ON` | Build tests from `tests/` and register them with CTest. |
| `MDBXC_BUILD_BENCHMARKS` | `OFF` | Build manual benchmark executables from `benchmarks/`. |
| `MDBXC_ENABLE_STRESS_TESTS` | `OFF` | Register long stress tests with CTest. Stress executables are still built when tests are enabled. |
| `MDBXC_SIMPLE_WEB_HTTP_TRANSPORT` | `OFF` | Enable the optional Simple-Web-Server HTTP transport backend target and backend smoke test. |
| `MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT` | `OFF` | Enable the optional Simple-WebSocket-Server transport backend target and backend smoke test. Requires OpenSSL Crypto because Simple-WebSocket-Server uses it for the WebSocket handshake. |
| `MDBXC_KURLYK_HTTP_TRANSPORT` | `OFF` | Enable the optional Kurlyk/libcurl HTTP client transport backend target and backend smoke test. Requires C++17 and a discoverable libcurl package. |
| `MDBXC_HTTP_SYNC_EXAMPLE` | `OFF` | Build the optional Simple-Web-Server HTTP sync examples. When examples are enabled, this also enables `MDBXC_SIMPLE_WEB_HTTP_TRANSPORT` for compatibility with older commands. |
| `MDBXC_WEBSOCKET_SYNC_EXAMPLE` | `OFF` | Build the optional Simple-WebSocket-Server sync example. When examples are enabled, this also enables `MDBXC_SIMPLE_WEB_WEBSOCKET_TRANSPORT` for compatibility with older commands. |
| `MDBXC_KURLYK_HTTP_SYNC_EXAMPLE` | `OFF` | Build the optional Kurlyk/libcurl HTTP sync client example. When examples are enabled, this also enables `MDBXC_KURLYK_HTTP_TRANSPORT` for compatibility with older commands. |
| `MDBXC_KURLYK_HTTP_SYNC_MINGW_CURL_FALLBACK` | `ON` | Fetch a pinned ready-made Win64 libcurl package for the MinGW Kurlyk HTTP transport when system libcurl is unavailable. |
| `MDBXC_WEBSOCKET_SYNC_MINGW_OPENSSL_FALLBACK` | `ON` | Fetch a pinned ready-made Win64 OpenSSL Crypto package for the MinGW WebSocket transport when system OpenSSL is unavailable. |
| `MDBXC_USE_ASAN` | `ON` | Enable AddressSanitizer for tests/examples when supported. |

The optional transport backend options create concrete dependency targets and,
when tests are enabled, backend smoke tests. The `*_SYNC_EXAMPLE` options only
add repository example executables on top of those backends. Concrete backend
targets define `MDBXC_HAS_SIMPLE_WEB_HTTP_TRANSPORT`,
`MDBXC_HAS_SIMPLE_WEB_WEBSOCKET_TRANSPORT`, or
`MDBXC_HAS_KURLYK_HTTP_TRANSPORT` for consumers that link those dependency
targets. Use those `MDBXC_HAS_*` macros when application code needs conditional
includes around optional sync transport backends. Consumers that wire the same
third-party dependencies manually may define the corresponding `MDBXC_HAS_*`
macro themselves.

After `find_package(mdbx_containers CONFIG REQUIRED)`, installed-package
consumers can create the same ready-made transport usage targets with:

```cmake
mdbx_containers_simple_web_http_transport_provide(OUT_TARGET http_target)
mdbx_containers_simple_web_websocket_transport_provide(OUT_TARGET ws_target)
mdbx_containers_kurlyk_http_transport_provide(OUT_TARGET kurlyk_target)
```

The returned targets are `mdbx_containers::simple_web_http_transport`,
`mdbx_containers::simple_web_websocket_transport`, and
`mdbx_containers::kurlyk_http_transport`. They enable `MDBXC_SYNC_ENABLED`,
link `mdbx_containers::mdbx_containers`, fetch or find the optional backend
dependencies, and propagate the matching `MDBXC_HAS_*_TRANSPORT` macro.

Installed `mdbx-containers` packages first look for `MDBXConfig.cmake` and then
fall back to lowercase `mdbxConfig.cmake`. The installed config normalizes
common target names such as `mdbx`, `libmdbx::mdbx`, and static variants to the
canonical `mdbx::mdbx` target used by the exported library target.

When `mdbx-containers` is added as a subproject, existing parent-provided
`mdbx::mdbx`, `mdbx::mdbx-static`, `libmdbx::mdbx`, and
`libmdbx::mdbx-static` targets are reused before package, submodule, or
FetchContent lookup. Parent-provided targets take precedence over
`MDBXC_DEPS_MODE`, including `BUNDLED`.

## Baseline Commands

Use `tmp/` inside the repository for local verification builds, installs,
consumer projects, and other scratch outputs. Keep these directories untracked
and disposable. Use an external build directory only when isolation from the
working tree is specifically required.

Configure, build, and test C++17:

```bash
cmake -S . -B tmp/build-cpp17 \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=ON \
    -DMDBXC_BUILD_EXAMPLES=ON \
    -DCMAKE_CXX_STANDARD=17

cmake --build tmp/build-cpp17
ctest --test-dir tmp/build-cpp17 --output-on-failure
```

Repeat with C++11 for shared header or template changes:

```bash
cmake -S . -B tmp/build-cpp11 \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=ON \
    -DMDBXC_BUILD_EXAMPLES=ON \
    -DCMAKE_CXX_STANDARD=11

cmake --build tmp/build-cpp11
ctest --test-dir tmp/build-cpp11 --output-on-failure
```

On Windows, the repository includes helper scripts such as
`build-mingw-17-tests.bat`, `build-mingw-11-tests.bat`, and
`build-mingw-17-examples.bat`.

## Stress Tests

Long stress tests are compiled with the normal test targets, but are not
registered in default CTest runs. Enable them explicitly for local or scheduled
stress verification:

```bash
cmake -S . -B tmp/build-stress \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=ON \
    -DMDBXC_ENABLE_STRESS_TESTS=ON \
    -DCMAKE_CXX_STANDARD=17

cmake --build tmp/build-stress --target test_sync_stress
ctest --test-dir tmp/build-stress -L stress --output-on-failure
```

The GitHub Actions `Stress` workflow runs the same stress label on Linux C++17.
It is intentionally separate from the main `CI` workflow and is triggered by
manual dispatch or the nightly schedule, not by ordinary pull requests.

## Benchmarks

Benchmarks are manual tools, not CTest targets. Enable them only for local
measurement runs:

```bash
cmake -S . -B tmp/build-bench \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=OFF \
    -DMDBXC_BUILD_EXAMPLES=OFF \
    -DMDBXC_BUILD_BENCHMARKS=ON \
    -DCMAKE_CXX_STANDARD=17

cmake --build tmp/build-bench --target sync_tick_hub_benchmark
tmp/build-bench/bin/benchmarks/sync_tick_hub_benchmark

cmake --build tmp/build-bench --target ordered_destructive_state_benchmark
tmp/build-bench/bin/benchmarks/ordered_destructive_state_benchmark
```

On Windows the executable has the `.exe` suffix, for example:

```powershell
.\tmp\build-bench\bin\benchmarks\sync_tick_hub_benchmark.exe --preset quick
```

`sync_tick_hub_benchmark` prints CSV rows for full cold-replica sync,
incremental hot sync, and incremental sync after restarting both connections
and sync engines. It uses `DirectSyncPeer` in one process, so the timings measure
the sync core, pagination, and local apply path rather than network transport
latency.
CI also builds this benchmark target and runs a small custom scenario as a
smoke check; full measurement runs remain manual.
See [`benchmarks/README-sync.md`](../benchmarks/README-sync.md) for scenario
parameters, CSV columns, and measurement guidelines.
`ordered_destructive_state_benchmark` is a separate manual CSV baseline for the
full destructive ordered-state reverse scan and per-origin high-water scan. It
accepts optional positional `origins elements_per_origin key_count iterations`
arguments; use it before proposing a cache, index, or bound for those
correctness paths.
The benchmark supports named presets:

```bash
sync_tick_hub_benchmark --preset quick
sync_tick_hub_benchmark --preset realistic
sync_tick_hub_benchmark --list-presets
```

Pass positional arguments to run one custom scenario:

```bash
sync_tick_hub_benchmark \
    origins historical_chunks_per_origin new_chunks_per_origin \
    ticks_per_chunk max_batches max_bytes
```

## CI Expectations

- GitHub Actions builds on Windows with MSYS2/MinGW.
- The matrix covers C++11 and C++17.
- CI uses CMake with Ninja and runs `ctest --output-on-failure`.
- A separate Linux C++17 smoke job builds and runs `sync_tick_hub_benchmark`
  with `MDBXC_BUILD_BENCHMARKS=ON`.
- The separate `Archcheck` workflow runs `archcheck --diff` as an advisory
  architecture check on pull requests, including stacked PRs, and manual
  dispatch. The action uses `fail-on-gate: false`, so architecture findings are
  visible without blocking merges, while configuration or tool execution
  failures still fail the job.
- The separate `Stress` workflow runs `ctest -L stress` on Linux C++17 when
  manually dispatched or triggered by its schedule.

## Generated Outputs

- `tmp/` is the preferred scratch area for agent-created local builds and
  package-consumer checks.
- Build directories such as `tmp/build-*` or legacy `build-*` are generated
  outputs.
- `docs/html/` and `docs/latex/` are generated Doxygen outputs.
- Do not use generated copies under build directories as source when editing.
