# Critical Runtime Defaults

Load this file when changing transactions, connection lifecycle, table opening,
read-only behavior, or serialization. Repository workflow and scope rules live
in the root `AGENTS.md` and `guides/coding-agent-workflow.md`.

## Transaction Ownership

- Share one `Connection` per MDBX environment.
- Keep at most one active transaction per thread.
- Never pass `Transaction`, raw `MDBX_txn*`, or MDBX cursors across threads.
- Caller-supplied transaction handles must belong to the same MDBX environment
  as the receiving table, store, or sync engine.

These rules follow MDBX transaction ownership. A wrapper mutex does not transfer
transaction ownership and is not a substitute for the invariant. Verify changes
with the manual and automatic transaction tests relevant to the edited path.

## Connection Lifecycle

- Treat `configure()`, `connect()`, `disconnect()`, and `Connection` destruction
  as lifecycle-only operations outside concurrent table activity.
- Use `shutdown()` or `shutdown_for()` for coordinated close paths.
- Keep `disconnect()` strict: it must fail with `MDBX_BUSY` while transaction
  handles are open and must not abort a transaction owned by another thread.

## Read-Only Environments

`BaseTable` strips `MDBX_CREATE` and opens existing DBIs with a read-only
transaction when `Config::read_only` is true. Wrappers and sync stores that open
additional DBIs outside `BaseTable` must preserve the same behavior:

- do not create missing directories or named DBIs;
- do not silently upgrade a read transaction;
- let writes fail through MDBX rather than emulating success.

## Serialization

Use the existing `SerializeScratch` pattern. Do not add `thread_local` STL
containers as serialization scratch; that pattern caused MinGW thread-local
destructor failures. Run `kv_container_all_types_test` plus the affected table
tests when serialization changes.

## Compatibility

Public headers and templates remain C++11-compatible unless newer APIs are
properly guarded and have a C++11 fallback. Check both language modes for shared
header changes. Preserve the repository include-guard convention and standalone
entry-point compilation coverage.
