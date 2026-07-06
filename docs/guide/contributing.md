# Contributing to nova-core

> How to contribute to nova-core development.

---

## Workflow

1. **Read** `AGENTS.md` for agent/developer context
2. **Read** `docs/guide/engineering-rules.md` for mandatory engineering rules
3. **Check** `ROADMAP.md` for current phase and scope
4. **Pick** a milestone or issue from the current phase
5. **Create** a feature branch: `git checkout -b feat/storage-mp-writer`
6. **Implement** following `docs/guide/engineering-rules.md` and `docs/guide/coding-standards.md`
7. **Test** all changes: `cargo test --all`
8. **Lint**: `cargo clippy --all -- -D warnings`
9. **Format**: `cargo fmt --all -- --check`
10. **Commit** with conventional commits when ready
11. **Open PR** with description of what and why

---

## Branch Naming

```
feat/{scope}-{description}     # New feature
fix/{scope}-{description}      # Bug fix
test/{scope}-{description}     # Test additions
docs/{description}              # Documentation
refactor/{scope}-{description} # Refactoring
```

Examples:
```
feat/storage-mp-writer
fix/coordinator-cbo-empty-table
test/worker-cache-hit-miss
docs/getting-started-update
```

---

## PR Checklist

- [ ] Branch name follows convention
- [ ] Commit messages follow convention
- [ ] `docs/guide/engineering-rules.md` was followed
- [ ] Immutable micro-partition and copy-on-write invariants are preserved
- [ ] FoundationDB remains authoritative for metadata
- [ ] Workers remain stateless for durable data
- [ ] MVCC/snapshot correctness is preserved
- [ ] Cache behavior cannot return stale or unauthorized results
- [ ] Relevant unit tests were added or run and results are reported
- [ ] `cargo build --release` passes or blocker is documented
- [ ] `cargo test --all` passes or blocker is documented
- [ ] `cargo clippy --all -- -D warnings` passes or blocker is documented
- [ ] `cargo fmt --all -- --check` passes or blocker is documented
- [ ] No production `.unwrap()`/`.expect()` on fallible paths without documented invariant
- [ ] Public functions have rustdoc
- [ ] Public or non-trivial functions have tests or documented integration coverage
- [ ] No new dependencies without discussion
- [ ] PR description explains what and why
- [ ] ROADMAP.md updated if milestone completed

---

## Issue Reporting

### Bug Report

```
**Describe the bug:** Clear description
**To reproduce:** Steps to reproduce
**Expected:** What should happen
**Actual:** What actually happens
**Environment:** OS, Rust version, nova-core version
**Logs:** Relevant logs/tracing output
```

### Feature Request

```
**Feature:** Describe the feature
**Why:** Why is it needed
**How:** Suggested implementation approach
**Phase:** Which ROADMAP.md phase does this belong to
```
