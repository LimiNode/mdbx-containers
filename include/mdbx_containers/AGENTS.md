# Public Header Instructions

These instructions apply to public library headers under
`include/mdbx_containers/`. Sync headers also load `sync/AGENTS.md`.

## Public API Boundary

- Treat headers here as the primary implementation and consumer-facing API.
- Preserve C++11 compilation. Guard C++17 conveniences and keep the existing
  C++11 fallback usable.
- Preserve source compatibility unless the task explicitly approves a breaking
  change. Prefer extending an existing overload or options type over adding a
  parallel abstraction.
- Keep project headers standalone where they are public entry points. Internal
  implementation leaves may rely on prerequisites supplied by their owning
  aggregate when a direct include would create an upward dependency or back
  edge.

## Header Structure

- Use `MDBX_CONTAINERS_HEADER_<PATH>_<FILE>_<EXT>_INCLUDED` guards for
  project-owned `.hpp` and `.h` files. Keep `.ipp` implementation fragments
  unguarded and include them from a guarded header.
- Public/configuration macros retain their established domain names, for example
  `MDBX_CONTAINERS_HEADER_ONLY` and `MDBX_CONTAINERS_SEPARATE_COMPILATION`.
- Do not use `using namespace` in headers.
- Avoid lambda default captures. List captures explicitly, including `this`.

The `agent_policy_checks` CTest test verifies guard structure and default-capture
rules mechanically. Standalone public and domain umbrella targets in CMake
verify include closure. If a comment or string literal must demonstrate forbidden
default-capture syntax, add `MDBXC_AGENT_POLICY_ALLOW_DEFAULT_CAPTURE` on that
line as an explicit reviewable scanner exception; do not use it for a lambda.

## Table and Transaction Changes

Read `guides/table-api-guide.md` before changing table semantics and
`guides/critical-defaults.md` before changing transaction, lifecycle, read-only,
or serialization behavior.

- Keep MDBX mutation and capture in the same transaction.
- Preserve automatic and caller-supplied transaction variants.
- Do not create DBIs on read-only paths.
- A wrapper opening companion DBIs must account for `Config::max_dbs` and
  existing handle budgets.

## Verification

Select the narrowest table-specific target from `tests/AGENTS.md`. For public
headers or templates, compile and run the relevant targets in both C++11 and
C++17 and include standalone/umbrella coverage when the include surface changes.
