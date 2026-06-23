# Contributing to nova-core

> How to contribute to nova-core development.

---

## Workflow

1. **Read** `AGENTS.md` and `CLAUDE.md` for project context
2. **Check** `ROADMAP.md` for current phase and scope
3. **Pick** a milestone or issue from the current phase
4. **Create** a feature branch: `git checkout -b feat/storage-mp-writer`
5. **Implement** following `docs/guide/coding-standards.md`
6. **Test** all changes: `cargo test --all`
7. **Lint**: `cargo clippy --all -- -D warnings`
8. **Format**: `cargo fmt --all`
9. **Commit** with conventional commits
10. **Open PR** with description of what and why

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
- [ ] `cargo build --release` passes
- [ ] `cargo test --all` passes
- [ ] `cargo clippy --all -- -D warnings` passes
- [ ] `cargo fmt --all -- --check` passes
- [ ] No `.unwrap()` in non-test code
- [ ] Public functions have rustdoc
- [ ] Public functions have tests
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
