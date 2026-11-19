# Performance report

## Method

Benchmarks were built in Release mode with GCC 14.2 and the bundled libmdbx 0.13.12 on Linux. The numbers below are raw program output from the corrective branch. They include setup and output materialization when the benchmark reports those phases. They are comparative baselines, not a product-wide performance claim.

## Flat vector append

| Preset | Records | Dimension | Iterations | Best append ms | Vectors per second |
| --- | ---: | ---: | ---: | ---: | ---: |
| quick | 4,000 | 128 | 3 | 1.23278 | 3,244,710 |
| realistic | 50,000 | 384 | 3 | 166.735 | 299,876 |

## Exact vector scan

| Preset | Metric | Records | Dimension | Queries | Search ms | Queries per second |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| quick | cosine | 2,000 | 128 | 64 | 14.4005 | 4,444.3 |
| quick | dot | 2,000 | 128 | 64 | 14.5960 | 4,384.8 |
| quick | l2 | 2,000 | 128 | 64 | 14.3438 | 4,461.9 |
| realistic | cosine | 50,000 | 384 | 512 | 11,588 | 44.18 |
| realistic | dot | 50,000 | 384 | 512 | 11,765.2 | 43.52 |
| realistic | l2 | 50,000 | 384 | 512 | 10,900.5 | 46.97 |

The exact path is exhaustive. Its dominant cost scales with records times dimension. The realistic result is therefore a useful boundary for callers deciding whether to use `VectorExactScan` or provide a separate indexed implementation.

## Sync tick hub quick preset

| Scenario | Phase | Origins | Seeded batches | Applied batches | Pull ms | Apply ms | Total ms | Batches per second |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| one origin small chunks | full cold replica | 1 | 512 | 512 | 0.325 | 16.692 | 23.524 | 30,087.6 |
| one origin small chunks | incremental hot | 1 | 64 | 64 | 0.036 | 1.783 | 3.310 | 35,184.2 |
| one origin small chunks | incremental after restart | 1 | 64 | 64 | 0.035 | 1.691 | 5.665 | 37,080.0 |
| ten origins tick chunks | full cold replica | 10 | 5,120 | 5,120 | 8.652 | 222.820 | 370.713 | 22,119.3 |
| ten origins tick chunks | incremental hot | 10 | 320 | 320 | 0.459 | 13.414 | 21.340 | 23,066.4 |
| ten origins tick chunks | incremental after restart | 10 | 320 | 320 | 0.847 | 14.687 | 27.958 | 20,600.0 |
| hundred origins paged | full cold replica | 100 | 12,800 | 12,800 | 49.579 | 813.472 | 998.936 | 14,831.1 |
| hundred origins paged | incremental hot | 100 | 800 | 800 | 3.560 | 50.290 | 63.218 | 14,856.1 |
| hundred origins paged | incremental after restart | 100 | 800 | 800 | 4.740 | 54.701 | 80.452 | 13,458.7 |

The sync numbers show apply time dominating these scenarios. They also include DBI setup, transaction work, codec decode, and durable cursor updates through the benchmark's reported phases. They should not be reduced to a transport-only throughput claim.

## Ordered destructive state scan

The custom scan benchmark used 2 origins, 1,000 elements per origin, 20 keys, 2 iterations, and a 100-element page. The estimated records scanned per iteration were 180 for a key index, 2,004 for the live state scan, and 2,002 for the high-water verification. Elapsed times were 0.377 ms, 1.122 ms, and 0.333 ms respectively. The result confirms that the live-state scan touches substantially more records than the narrow key index.

## Complexity review

- Point lookups use one MDBX lookup after the DBI is open.
- Ordered range operations use one cursor and materialize output in the requested container.
- Reconciliation is one transaction. The corrective generic path uses serialized-key copies in `std::set`, giving `O(n log n)` membership work and avoiding an undocumented hash requirement.
- Exact vector scan is `O(N * D)` per query batch.
- Optional DBI catalog lookup is one MainDB lookup and avoids the side effect of probing an absent named DBI.
- Sync capture and apply perform codec work plus durable changelog or cursor writes in the same transaction. The benchmarks show the write and apply path, not only parsing.

## Deferred optimization work

No optimization PR is required for release from these measurements. A future performance PR should compare serialized-key sets against a transparent byte comparator or bounded digest table, and should preserve byte-level correctness for collisions, malformed keys, and duplicate source entries.

