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
- Do not use include-what-you-use as the default project-header policy. An
  owning public or domain entry point includes shared prerequisites before its
  internal implementation leaves; those leaves may rely on that include
  context instead of creating upward dependencies or back edges.

## Include Ownership

- Standalone compilation is an explicit contract, not an assumption for every
  `.hpp`. The current standalone public set is
  `MDBXC_STANDALONE_PUBLIC_HEADERS` in the root `CMakeLists.txt`: `common.hpp`,
  `common/backup.hpp`, `CompositeKey.hpp`, and the public table entry points.
- Aggregate and domain entry points are covered separately by the explicit
  umbrella tests and `MDBXC_SYNC_DOMAIN_UMBRELLAS`. This includes
  `mdbx_containers.hpp`, `tables.hpp`, `vector.hpp`, `sync.hpp`, and the listed
  sync domain umbrellas.
- Do not make another internal header standalone merely by adding direct
  project includes. Promote it deliberately by documenting the new entry-point
  contract and adding it to the appropriate standalone or umbrella coverage.
- Component-local implementation leaves under `detail/` receive shared project
  prerequisites from their owner. They must not include parent, peer, public,
  or domain project headers through `../` traversal or a
  `mdbx_containers/...` root include. Downward includes within their own
  implementation component remain allowed. See
  [detail instructions](detail/AGENTS.md).

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
