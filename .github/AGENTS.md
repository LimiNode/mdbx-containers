# CI and Build Instructions

These instructions apply to GitHub Actions, packaging, dependency-provider
logic, and other files under `.github/`. Vendored
`.github/doxygen-awesome-css` is a pinned submodule and is not maintained as
project source.

## Workflow Contracts

- Preserve the Linux, MinGW, and macOS coverage expected by
  `guides/build-and-test.md`.
- Shared public-header behavior must remain covered in C++11 and C++17.
- Keep optional HTTP, WebSocket, Kurlyk, installed-provider, benchmark, and
  parent-MDBX jobs separate enough to identify dependency-specific failures.
- Do not claim exact-head success until every required job for that SHA has
  completed successfully.
- `Archcheck` is advisory for architecture findings because its action uses
  `fail-on-gate: false`; configuration or tool execution failures still fail the
  workflow.

When diagnosing a failure, separate repository regressions from configure-time
external dependency failures using the failing command and logs. Do not change
runtime code to mask an unavailable external source.

## Submodules and Dependencies

- Checkout workflows that build bundled dependencies must initialize submodules
  recursively.
- A parent gitlink must reference a commit available from the configured child
  remote before the parent branch is pushed.
- Treat dependency source, mirror, fallback, and offline behavior as explicit
  build contracts. Keep pinned revisions and provider targets reproducible.
- Do not edit files inside `.github/doxygen-awesome-css` unless the task
  explicitly changes that submodule; update the gitlink through a dedicated
  dependency change.

## Validation

- Reconfigure from a clean build directory after CMake/dependency changes.
- Run the narrowest local provider or package-consumer smoke available.
- Validate edited workflow syntax and inspect the rendered Actions diff.
- Run `agent_policy_checks` when agent instructions or repository policy gates
  change.
