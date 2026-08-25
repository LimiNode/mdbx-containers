# Test Instructions

These instructions apply to test sources and test infrastructure under
`tests/`.

## Execution Model

- Run registered tests with `ctest --test-dir <build>`, not by launching test
  executables from the repository root.
- CTest assigns isolated `<build>/test-data/<test-name>` working directories and
  cleanup fixtures. Direct root execution can leave MDBX data and lock files in
  the checkout.
- Put configure and build directories under `tmp/`.
- Keep tests non-interactive unless an existing target explicitly gates
  interactivity behind `INTERACTIVE_TEST`.
- Keep paths, timing, and assertions deterministic across Linux, MinGW, and
  macOS.

## Regression Shape

- Reproduce the externally visible failure before fixing behavior when
  practical.
- Assert the committed MDBX state, durable metadata, cursor, or callback
  contract directly rather than relying only on an intermediate return value.
- Cover fail-closed behavior and verify that rejected work did not partially
  mutate user data or sync metadata.
- Keep fixtures and generated artifacts inside the test working directory.
- Avoid broad sleeps for concurrency tests; prefer barriers, bounded waits, and
  observable state transitions.

## Selecting Tests

Use `guides/build-and-test.md` for configure commands. Shared public headers and
templates require relevant C++11 and C++17 runs.

- Table changes: run the table-specific test and example target.
- Serialization changes: include `kv_container_all_types_test`.
- Transaction/lifecycle changes: include `mdbx_test` and related shutdown or
  concurrency coverage.
- Sync changes: follow `include/mdbx_containers/sync/AGENTS.md`.
- CMake policy or agent-instruction changes: run `agent_policy_checks`.

Start focused with `ctest -R`, then broaden when the changed seam has multiple
consumers. Use `--output-on-failure` for every validation run.
