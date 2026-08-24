# Documentation Instructions

These instructions apply to maintained documentation under `guides/`.

## Ownership and Source of Truth

- Update an existing guide on the topic instead of creating a competing note.
- Keep normative details in the guide that owns the contract and link to it from
  summaries or architecture maps.
- Do not call a shortened overview the source of truth. If a document is
  intentionally abbreviated, label it and link to the complete canonical
  contract.
- Preserve source attribution and distinguish verified facts from assumptions
  or proposed design.

## English/Russian Counterparts

The following guides are maintained as normative pairs:

- `sync-architecture.md` / `sync-architecture-RU.md`
- `sync-audit-followups.md` / `sync-audit-followups-RU.md`
- `sync-selective-replication-design.md` /
  `sync-selective-replication-design-RU.md`
- `sync-table-coverage.md` / `sync-table-coverage-RU.md`
- `sync-transport-production.md` / `sync-transport-production-RU.md`
- `sync-v0.1-readiness.md` / `sync-v0.1-readiness-RU.md`

When one side changes a normative behavior, invariant, limitation, test claim,
or navigation path, update the counterpart in the same change. A counterpart
may be idiomatic rather than literal, but it must not omit constraints needed to
use the contract safely. If a one-language edit is intentional, state why in the
PR description.

Apply the same rule to `README.md` / `README-RU.md` and paired example docs.
Keep code identifiers in their canonical form; translate human-readable labels
and prose consistently with nearby documentation.

## Validation

- Run `git diff --check`.
- Run the `agent_policy_checks` CTest test when available; it validates expected
  guide pairs and local Markdown links in maintained agent and guide documents.
- Inspect relative links and anchors changed by renames.
- For contract claims, check the current code and tests rather than copying an
  outdated checklist statement.
- Documentation-only changes do not require a full C++ build unless they alter
  generated inputs, CMake snippets, or executable examples.
