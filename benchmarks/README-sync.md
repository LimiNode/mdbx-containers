# Sync Benchmark

`sync_tick_hub_benchmark` measures the core sync path for a hub-style workload:
one central node stores tick-data chunks from many origins and one replica pulls
those changes. The benchmark uses `DirectSyncPeer` in one process, so the
reported timings cover local changelog reads, pagination, decoding, and local
`handle_push()` work. They do not measure network latency or serialization in a
real transport.

## Build

```bash
cmake -S . -B tmp/build-bench \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=OFF \
    -DMDBXC_BUILD_EXAMPLES=OFF \
    -DMDBXC_BUILD_BENCHMARKS=ON \
    -DCMAKE_CXX_STANDARD=17

cmake --build tmp/build-bench --target sync_tick_hub_benchmark
```

The commands below use Linux/MSYS2 paths. On Windows the executable has the
`.exe` suffix, for example:

```powershell
.\tmp\build-bench\bin\benchmarks\sync_tick_hub_benchmark.exe --preset quick
```

## Built-In Scenarios

Without arguments the benchmark runs the `quick` preset.

```bash
tmp/build-bench/bin/benchmarks/sync_tick_hub_benchmark --preset quick
tmp/build-bench/bin/benchmarks/sync_tick_hub_benchmark --preset realistic
```

| Preset | Purpose |
| --- | --- |
| `quick` | Short matrix for checking the build and comparing broad changes. |
| `realistic` | Larger manual matrix with more origins and history. The name means "closer to a hub workload", not a universal production model. |

List available presets:

```bash
tmp/build-bench/bin/benchmarks/sync_tick_hub_benchmark --list-presets
```

## Custom Scenario

```bash
tmp/build-bench/bin/benchmarks/sync_tick_hub_benchmark \
    origins historical_chunks_per_origin new_chunks_per_origin \
    ticks_per_chunk max_batches max_bytes
```

Positional arguments are optional from left to right. Omitted trailing values
use the defaults shown below.

| Argument | Default | Meaning |
| --- | ---: | --- |
| `origins` | 16 | Number of independent origin nodes represented in the changelog. |
| `historical_chunks_per_origin` | 512 | Number of already existing batches per origin before the first pull. |
| `new_chunks_per_origin` | 8 | Number of new batches per origin in each incremental phase. This value is used twice: once for `incremental_hot` and once after restart. |
| `ticks_per_chunk` | 128 | Number of tick records encoded inside one batch. |
| `max_batches` | 64 | Maximum number of batches returned in one `PullResponse` page. |
| `max_bytes` | 4194304 | Approximate byte budget for one response page. |

## CSV Output

Each scenario prints three phases:

- `full_cold_replica` - initial sync of an empty replica from the accumulated
  historical changelog.
- `incremental_hot` - append new batches and sync again without closing
  connections or reconstructing `SyncEngine`.
- `incremental_after_restart` - close and reopen both databases, reconstruct
  `SyncEngine`, append another range of new batches, and perform incremental
  sync from the persisted receiver cursor.

| Column | Meaning |
| --- | --- |
| `scenario` | Scenario name. |
| `phase` | One of the three phases listed above. |
| `origins` | Number of origin nodes in the scenario. |
| `historical_chunks_per_origin` | Historical batches seeded per origin before the cold sync. |
| `new_chunks_per_origin` | New batches per origin for each incremental phase. |
| `chunks_per_origin` | Batches per origin present for the measured phase. |
| `ticks_per_chunk` | Tick records encoded in each batch. |
| `max_batches` | Page limit by batch count. |
| `max_bytes` | Page limit by approximate encoded bytes. |
| `seeded_batches` | Batches written locally before the measured pull. |
| `pulled_batches` | Batches received through `PullResponse` pages. |
| `applied_batches` | Batches applied through `SyncEngine::handle_push()`. |
| `pull_pages` | Number of pull requests needed to finish the phase. |
| `origin_index_entries` | Number of origins registered in `_mdbxc_origins` on the primary. |
| `seed_ms` | Time spent writing local data for the phase. |
| `restart_ms` | Time spent closing, reopening, and reconstructing objects for the restart phase. Zero in other phases. |
| `pull_ms` | Time spent in `DirectSyncPeer::pull()` calls. |
| `apply_ms` | Time spent in `SyncEngine::handle_push()` calls. |
| `total_ms` | `seed_ms + restart_ms + pull_ms + apply_ms`. |
| `sync_ms` | `pull_ms + apply_ms`. This excludes seeding and restart time. |
| `pull_pct` | Share of `sync_ms` spent in pull calls. |
| `apply_pct` | Share of `sync_ms` spent applying pages locally. |
| `batches_per_page` | Average `pulled_batches / pull_pages`. |
| `batches_per_sec` | `applied_batches / (pull_ms + apply_ms)`. Seeding and restart time are excluded. |
| `primary_bytes` | MDBX file usage reported for the primary after the phase. |
| `replica_bytes` | MDBX file usage reported for the replica after the phase. |

`origin_index_entries` normally matches `origins` after historical seeding. The
index allows `SyncEngine::handle_pull()` to skip origins where the requester is
already at the latest known sequence number. For lagging origins, the engine
still seeks directly to `have_seq + 1`.

Use `pull_pct` and `apply_pct` before choosing an optimization target. A low
`pull_pct` means the current run is dominated by local apply work; changing the
pull algorithm is unlikely to move total throughput for that workload.

## Comparing Results

1. Build both versions in the same mode, preferably `Release`.
2. Run the same preset or the same custom arguments at least five times.
3. Treat the first run as a filesystem-cache warmup when results vary.
4. Compare median `pull_ms`, `apply_ms`, `pull_pct`, `apply_pct`,
   `pull_pages`, and `batches_per_sec`.
5. Do not compare runs with different compilers, MDBX settings, presets, or
   scenario arguments.
6. Use this benchmark for sync-core changes only. It does not estimate HTTP,
   WebSocket, IPC, or encryption costs.

## Ordered Destructive State Baseline

`ordered_destructive_state_benchmark` measures the two deliberately linear
validation paths used by destructive schema version 2:

- `live_state_ids_for_key()` reverse-scans the full state DBI to find all live
  element ids for one canonical key and detect entries missing from the DUPSORT
  index;
- `verify_introduced_high_water()` scans one origin prefix before admitting a
  new id.

It is a manual bounds baseline, not a throughput claim and not a CTest target.
Use identical arguments and build settings when comparing revisions:

```bash
cmake --build tmp/build-bench --target ordered_destructive_state_benchmark
tmp/build-bench/bin/benchmarks/ordered_destructive_state_benchmark
tmp/build-bench/bin/benchmarks/ordered_destructive_state_benchmark \
    16 512 32 100 384
```

The optional positional arguments are `origins`, `elements_per_origin`,
`key_count`, `iterations`, and `tombstones_per_origin`. The first
`tombstones_per_origin` ids of every origin are persisted as Tombstone records;
the default is zero. CSV rows separately report total, Live, and Tombstone
element records, the full state-record count, and live DUPSORT index records.
CSV now has separate rows for `live_ids_for_key_index` and
`live_state_ids_for_key`. The first measures the DUPSORT lookup; the second
measures the full reverse validation scan. `estimated_scanned_records` is a
workload bound, not an internal counter. `elapsed_ms` includes read-transaction
open, the measured scan, and rollback. Use results from representative
workloads before adding an index, cache, or fixed limit to either correctness
path.

The reverse-validation row is intentionally the reference path. Any future
trusted no-rescan path must retain this path as a fallback and must document
the proof that makes its bounded scan safe. `BroadEraseBounds::max_scanned_records`
must be large enough for the selected operation while this validation path is
enabled; a smaller bound is expected to reject before mutation.

Bounded destructive capture now uses prevalidated exact ids for its mutation
phase and does not repeat this full reverse scan for every selected id. The
corresponding regression retains a tombstone-heavy state with one Live element
and a deliberately tight scan budget; use this benchmark to compare larger
state and tombstone distributions before changing that trusted path.
