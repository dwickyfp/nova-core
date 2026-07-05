---
name: nova-core-safe-rust-change
description: Use when editing Rust code in nova-core to make small, validated, architecture-safe changes.
---

# Nova Core Safe Rust Change

Use this skill for bug fixes, refactors, tests, and feature work touching Rust files under `crates/`.

## Change Workflow

1. Load `nova-core-orientation` first for repo invariants and current scope.
2. Identify the owning crate and its dependency direction before editing.
3. Use CodeGraph to trace callers/callees for the target symbol.
4. Make the smallest coherent change; avoid drive-by formatting or unrelated cleanup.
5. Add or update tests close to the changed behavior.
6. Run the narrowest useful check first, then broaden if it passes.

## Crate Boundaries

- `nova-common` must not depend on internal crates.
- `nova-storage` may depend on `nova-common` only among internal crates.
- `nova-coordinator` may depend on `nova-storage` and `nova-common`, not `nova-worker`.
- `nova-worker` may depend on `nova-storage` and `nova-common`, not `nova-coordinator`.
- Shared coordinator/worker concepts belong in `nova-common`.

## Rust Standards

- Prefer explicit error variants with `thiserror` and `?` propagation.
- Do not introduce `unwrap()`, `expect()`, or panics in production paths.
- Keep async boundaries clear; avoid blocking I/O in async contexts.
- Preserve `tracing` style for operational observability.
- Public APIs should have rustdoc when behavior or invariants are non-obvious.

## Verification Ladder

Pick the smallest relevant command, then escalate if needed:

```bash
cargo test -p <crate> <test_name>
cargo test -p <crate>
cargo test --all
cargo clippy --all -- -D warnings
cargo fmt --all -- --check
```

Use `cargo nextest run --all` when the repo has nextest available and the task needs broader confidence.

## Completion Checklist

Before saying done:

- Explain which invariant you preserved.
- List exact tests/checks run and their result.
- Mention any checks skipped because services like FoundationDB, MinIO, or Docker were unavailable.
