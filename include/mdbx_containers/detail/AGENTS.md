# Internal Header Instructions

These rules apply to implementation headers and fragments under a `detail/`
directory in `include/mdbx_containers/`.

## Include Direction

- Classify a detail header by ownership before adding dependencies. Shared
  foundation headers may own the standard-library and external dependencies
  needed by their reusable contract. Component-local leaves receive shared
  project prerequisites from their owning public or domain entry point.
- Do not apply include-what-you-use mechanically to component-local leaves.
  The owner must include common project prerequisites before including the
  leaf.
- Do not include project headers from a detail leaf through `../` traversal or
  through a root path such as `mdbx_containers/common.hpp`. These edges point
  upward or sideways and can create include cycles.
- A detail leaf may include a lower-level leaf in the same implementation
  component by its local name, for example `#include "AssignmentProxy.hpp"`.
- Detail leaves are not standalone entry points. If one must become independently
  includable, promote that contract explicitly in the nearest owning
  `AGENTS.md` and add corresponding CMake header coverage.

The `agent_policy_checks` CTest gate enforces the mechanical project-include
direction. It intentionally does not infer semantic ownership or parse C++.
