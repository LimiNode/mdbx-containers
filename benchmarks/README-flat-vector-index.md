# Flat Vector Index Benchmark

`flat_vector_index_benchmark` isolates the in-memory append path of
`FlatVectorIndex`. It uses the DOT metric so the reported time measures vector
copying and index growth rather than cosine normalization.

## Build and run

```bash
cmake -S . -B tmp/build-flat-vector-bench \
    -DMDBXC_DEPS_MODE=BUNDLED \
    -DMDBXC_BUILD_TESTS=OFF \
    -DMDBXC_BUILD_EXAMPLES=OFF \
    -DMDBXC_BUILD_BENCHMARKS=ON \
    -DCMAKE_CXX_STANDARD=17

cmake --build tmp/build-flat-vector-bench --target flat_vector_index_benchmark
tmp/build-flat-vector-bench/bin/benchmarks/flat_vector_index_benchmark --preset quick
```

On Windows use the `.exe` suffix.

| Preset | Records | Dimension | Iterations |
| --- | ---: | ---: | ---: |
| `quick` | 4,000 | 128 | 3 |
| `realistic` | 50,000 | 384 | 3 |

The CSV reports the best append time, vectors per second, and a checksum that
forces the completed index state to remain observable.
