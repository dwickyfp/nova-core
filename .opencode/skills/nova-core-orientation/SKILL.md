---
name: nova-core-orientation
description: Use at the start of any nova-core session to load repo context, current scope, and CodeGraph exploration workflow before editing.
---

# Nova Core Orientation

Use this skill before making code changes in `nova-core`, especially when the task mentions architecture, an unfamiliar module, or broad refactoring.

## Start Here

1. Read `AGENTS.md` completely enough to identify the current architecture rules and common pitfalls.
2. Read `ROADMAP.md` near the phase/status table before deciding whether a feature belongs in scope.
3. For architecture-sensitive work, read `docs/design/architecture.md` and any focused doc in `docs/design/`.
4. Use CodeGraph before broad file searches:
   - `codegraph status`
   - `codegraph explore "<symbol, file, or question>"`
   - `codegraph query <symbol> --limit 10`
5. Prefer reading current source over relying on stale docs when the two disagree.

## Project Shape

- Workspace root: `Cargo.toml`.
- Shared types and errors: `crates/nova-common`.
- Metadata and storage: `crates/nova-storage`.
- SQL coordination, planning, auth, MySQL protocol, HA: `crates/nova-coordinator`.
- DataFusion execution and worker runtime: `crates/nova-worker`.
- CLI entrypoints: `crates/nova-cli`.

## Non-Negotiable Invariants

- Micro-partitions are immutable Parquet files; updates/deletes use copy-on-write.
- Metadata lives in FoundationDB; workers must remain stateless.
- Use MVCC/version metadata for correctness and cache invalidation.
- Do not add dependencies without explicit justification and user approval.
- Avoid `unwrap()`/`expect()` in production paths; return typed errors instead.

## Output Discipline

When reporting a plan or result, include:

- Files/symbols inspected.
- Architecture invariant that governs the change.
- Tests or checks that should prove the change.
- Any uncertainty caused by stale docs, missing services, or generated code.
