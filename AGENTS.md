# AGENTS.md

This is the repository-wide operating contract for coding agents working in
`mdbx-containers`. Keep this root file limited to rules that apply across the
repository; detailed instructions belong in the nearest owning `AGENTS.md`.

The project is a C++11/17 header-only library over libmdbx. Public headers,
template compatibility, MDBX transaction ownership, and cross-platform tests
are the main compatibility boundaries.

## Repository-Wide Baseline

- Run `git status --short` before editing and preserve unrelated tracked and
  untracked user files.
- Use `rg` / `rg --files` for repository search. Read the nearby implementation,
  tests, and relevant instructions before changing a contract.
- Keep the change focused. Do not reformat, rewrite, or delete neighboring work
  that is outside the requested scope.
- Treat `include/` as source of truth. Do not edit generated copies under build
  directories, `docs/html/`, or `docs/latex/`.
- Put disposable builds, installs, consumers, and scratch output under `tmp/`.
- Preserve C++11 compatibility unless a C++17-only API is explicitly guarded.
- Do not use lambda default captures (`[&]` or `[=]`) in project C++ code.
- Do not introduce `thread_local` STL scratch buffers in serialization paths.
- Keep paired English/Russian documents synchronized when a normative contract
  changes. See [guides/AGENTS.md](guides/AGENTS.md).
- Send changes to `main` through a PR. Do not commit, push, open or merge a PR,
  or write to external services unless the user requested that action.

## Instruction Routing

Load only the rows relevant to the current task.

| When the task touches | Read |
| --- | --- |
| Any repository file edit | [Coding agent workflow](guides/coding-agent-workflow.md) |
| Public tables or choosing table APIs | [Public header instructions](include/mdbx_containers/AGENTS.md), [table API guide](guides/table-api-guide.md), [coding style](guides/coding-style.md) |
| Transactions, lifecycle, read-only behavior, serialization | [Critical runtime defaults](guides/critical-defaults.md), [implementation notes](guides/implementation-notes.md) |
| Sync capture, storage, logical adapters, recovery, transports | [Sync instructions](include/mdbx_containers/sync/AGENTS.md) |
| Tests, fixtures, build directories, test selection | [Test instructions](tests/AGENTS.md), [build and test guide](guides/build-and-test.md) |
| Documentation or EN/RU counterparts | [Documentation instructions](guides/AGENTS.md) |
| CMake, GitHub Actions, packaging, dependency providers | [CI and build instructions](.github/AGENTS.md) |
| Repository structure or extension points | [Codebase orientation](guides/codebase-orientation.md), [project overview](guides/project-overview.md) |
| Commit or publish requested by the user | [Commit conventions](guides/commit-conventions.md) |

## Verification Routing

Run the narrowest checks that prove the changed contract, then broaden them in
proportion to risk.

| Changed surface | Minimum verification |
| --- | --- |
| Markdown or agent instructions | `git diff --check`; run `agent_policy_checks` when available |
| Shared/public headers or templates | Relevant tests in both C++11 and C++17 |
| CMake/build logic | At least one clean configure and build |
| Transactions or concurrency | Relevant transaction/lifecycle tests |
| Sync | Test set selected by `include/mdbx_containers/sync/AGENTS.md` in C++11 and C++17 |

Always inspect `git diff` and `git status --short` after editing. Report what was
verified and any check that could not be run.

## Instruction Ownership

Add new guidance to the nearest directory that owns the affected code. Add a
rule here only when it applies repository-wide or constrains callers that will
not load the owning module's instructions. Keep the root rule short and link to
the detailed rationale, approved alternative, and regression test.

Prefer enforceable contracts: for a mechanical invariant, add or extend a
CTest/CI gate instead of relying on prose alone.

## Submodules

`external/libmdbx` and `.github/doxygen-awesome-css` are pinned submodules.
Change a gitlink only when the task explicitly includes that dependency. Before
pushing a parent commit, verify the pinned commit exists on the configured
submodule remote; an unpublished local submodule commit makes recursive CI
checkout fail before the build starts.

## Evidence and Provenance

Do not present assumptions as confirmed facts. Preserve source attribution,
separate external evidence from interpretation, and state when local or remote
verification was unavailable.
