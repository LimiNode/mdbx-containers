# Coding Agent Workflow

Use this workflow for repository file edits. Domain-specific invariants and test
selection come from the nearest scoped `AGENTS.md` referenced by the root file.

## Default Loop

1. Read the root and nearest scoped `AGENTS.md` files for the target paths.
2. Run `git status --short` and identify user-owned changes before editing.
3. Define a verifiable success criterion for non-trivial work.
4. Search existing code, tests, docs, and extension patterns before adding a new
   concept or file.
5. Make the smallest change that satisfies the requested contract.
6. Add a regression test before or with a behavioral fix. Documentation-only
   and purely mechanical edits do not require artificial tests.
7. Run the narrowest relevant checks, then broaden according to risk and the
   verification routing in the root instructions.
8. Run `git diff --check`, inspect `git diff`, and run `git status --short`.
9. Report the result, completed checks, and any remaining limitation.

## Scope Discipline

- Preserve unrelated dirty-worktree changes and untracked files.
- Do not improve neighboring code, formatting, comments, or naming unless it is
  required by the task.
- Prefer existing abstractions. Avoid speculative features and single-use
  indirection.
- Treat generated Doxygen output and generated build-tree headers as outputs,
  not editable sources.
- Put agent-created builds and scratch consumers under repository-local `tmp/`.

When a request is ambiguous and different interpretations would materially
change the result, state the known facts and ask for the missing choice. Small,
reversible implementation details may use an explicitly stated assumption.

## Git and Publication

All changes reach `main` through PRs.

- Create a focused branch from the intended base for non-trivial work.
- Stage explicit paths in a mixed working tree; do not default to `git add -A`.
- Commit only when requested and follow `guides/commit-conventions.md`.
- Push, open a PR, change PR state, or merge only when requested.
- When a submodule gitlink changes, verify the child commit is available from
  its configured remote before pushing the parent branch.

## Verification Principles

- Run tests through CTest so fixture working directories and cleanup apply.
- Public/shared header and template changes require C++11 and C++17 coverage.
- CMake/build changes require at least one clean configure and build.
- Documentation changes require `git diff --check`, local-link validation, and
  paired EN/RU review when applicable.
- If a check cannot run because of a missing tool, dependency, or external
  service, report the exact skipped command and reason.

## Handoff

Lead with the outcome. Separate changed behavior, verification, and unresolved
limitations. Do not claim remote CI, PR state, local cleanliness, or external
facts without checking them.
