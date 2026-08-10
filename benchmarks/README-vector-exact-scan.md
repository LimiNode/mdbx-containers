# Vector Exact-Scan Benchmark

`vector_exact_scan_benchmark` measures the scalar in-memory exact scan provided
by `VectorExactScan`. It seeds a `VectorCollection`, measures one explicit
snapshot rebuild, then measures query scans over that immutable snapshot.
MDBX seed time is reported separately and is not included in search time.

## Build

```bash
cmake -S . -B tmp/build-vector-bench \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=OFF \
    -DMDBXC_BUILD_EXAMPLES=OFF \
    -DMDBXC_BUILD_BENCHMARKS=ON \
    -DCMAKE_CXX_STANDARD=17

cmake --build tmp/build-vector-bench --target vector_exact_scan_benchmark
```

## Scenarios

```bash
tmp/build-vector-bench/bin/benchmarks/vector_exact_scan_benchmark --preset quick
tmp/build-vector-bench/bin/benchmarks/vector_exact_scan_benchmark --preset realistic
tmp/build-vector-bench/bin/benchmarks/vector_exact_scan_benchmark --list-presets
```

On Windows use the `.exe` suffix.

| Preset | Records | Dimension | Queries | Purpose |
| --- | ---: | ---: | ---: | --- |
| `quick` | 2,000 | 128 | 64 | Short local smoke and broad change comparison. |
| `realistic` | 50,000 | 384 | 512 | Larger manual scalar baseline. |

Each preset runs `COSINE`, `DOT`, and `L2` with a fixed deterministic PRNG
seed. It is a scalar correctness and baseline tool, not an ANN, SIMD, or
cross-library comparison.

## CSV Output

| Column | Meaning |
| --- | --- |
| `scenario` | Selected preset. |
| `metric` | `cosine`, `dot`, or `l2`. |
| `records` | Number of vectors materialized into the snapshot. |
| `dimension` | Float components per vector. |
| `queries` | Number of exact top-10 searches. |
| `seed_ms` | Time spent writing collection records to MDBX. |
| `rebuild_ms` | Time spent materializing the immutable in-memory snapshot. |
| `search_ms` | Time spent only in scalar `VectorExactScan::search()` calls. |
| `queries_per_second` | `queries / search_ms`. |
| `total_matches` | Sum of returned matches; a basic output-consumption check. |
