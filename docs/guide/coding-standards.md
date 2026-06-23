# Coding Standards — nova-core

> Rust conventions, patterns, and quality gates for nova-core.

---

## 1. Rust Style

### 1.1 Type Annotations

Always use explicit type annotations on function signatures:

```rust
// ✅ GOOD
fn find_mp(table_id: u64, mp_id: u64) -> Result<Option<MicroPartitionMeta>> { ... }

// ❌ BAD
fn find_mp(table_id, mp_id) { ... }
```

### 1.2 Error Handling

Use `thiserror` for library-level error enums, `anyhow` for application-level:

```rust
#[derive(Debug, thiserror::Error)]
pub enum NovaStorageError {
    #[error("micro-partition not found: table={table_id}, mp={mp_id}")]
    MpNotFound { table_id: u64, mp_id: u64 },

    #[error("FDB transaction conflict: txn_id={txn_id}")]
    TransactionConflict { txn_id: u64 },

    #[error("Parquet write failed: {source}")]
    ParquetWriteFailed {
        #[from]
        source: parquet::errors::ParquetError,
    },
}

pub type Result<T> = std::result::Result<T, NovaStorageError>;
```

Rules:
- NEVER use `.unwrap()` in non-test code
- Use `?` operator for propagation
- Use `.context()` to add context when needed
- Every public function returns `Result<T>`

### 1.3 Async

All I/O operations must be async (tokio runtime):

```rust
// ✅ GOOD
async fn read_mp(&self, mp_id: u64) -> Result<RecordBatch> { ... }

// ❌ BAD — blocks executor thread
fn read_mp(&self, mp_id: u64) -> Result<RecordBatch> {
    std::fs::read("...")  // blocking!
}
```

### 1.4 Shared State

Use `Arc` for shared ownership, never `Rc` (we're multi-threaded):

```rust
struct Worker {
    cache: Arc<NovaCache>,
    storage: Arc<StorageAdapter>,
}
```

### 1.5 Documentation

All public items must have rustdoc comments:

```rust
/// Reads a micro-partition from cache or S3.
///
/// Checks Foyer hybrid cache (L3) first. On miss, reads from S3
/// and caches the result for future queries.
///
/// # Arguments
/// * `mp_id` - Micro-partition ID
/// * `projection` - Column indices to read (None = all columns)
///
/// # Errors
/// Returns `NovaStorageError::MpNotFound` if MP doesn't exist.
pub async fn read_mp(
    &self,
    mp_id: u64,
    projection: Option<&[usize]>,
) -> Result<RecordBatch> { ... }
```

---

## 2. Naming Conventions

| Item | Convention | Example |
|---|---|---|
| Crates | `nova-{name}` | `nova-coordinator`, `nova-storage` |
| Modules | `snake_case` | `metadata`, `mp_writer`, `cache` |
| Structs | `PascalCase` | `MicroPartitionMeta`, `NovaCache` |
| Traits | `PascalCase` | `ExecutionPlan`, `OptimizationRule` |
| Functions | `snake_case` | `read_mp`, `prune_mps`, `commit_txn` |
| Constants | `SCREAMING_SNAKE` | `DEFAULT_BATCH_SIZE`, `MIN_MP_SIZE` |
| Enums | `PascalCase`, variants `PascalCase` | `JoinStrategy::Colocated` |
| Files | `snake_case.rs` | `mp_writer.rs`, `metadata.rs` |
| Tests | `test_{behavior}` | `test_mp_pruning_with_range_predicate` |

---

## 3. Dependency Policy

- **Stdlib first** — if Rust stdlib can do it, don't add a crate
- **No new dependencies without justification** — every new crate must be discussed
- **Apache-2.0 or MIT license only**
- **Must be actively maintained** (commit within last 6 months)
- **Must be compatible with our Rust edition**

---

## 4. Testing Policy

### 4.1 Test Placement

| Test Type | Location | Naming |
|---|---|---|
| Unit tests | `src/**/tests.rs` or inline `#[cfg(test)]` | `test_{behavior}` |
| Integration tests | `tests/` directory per crate | `{feature}_test.rs` |
| Benchmarks | `benches/` directory | `bench_{operation}.rs` |

### 4.2 Test Requirements

- Every public function has unit tests
- Every error path has a test
- Async tests use `#[tokio::test]`
- Integration tests test the full stack (FDB + S3 + execution)

### 4.3 Test Structure (AAA Pattern)

```rust
#[tokio::test]
async fn test_mp_writer_creates_valid_parquet() {
    // Arrange
    let batch = create_test_batch(1000);
    let writer = MpWriter::new(test_store());

    // Act
    let meta = writer.write(1, 1, 1, vec![batch], 1).await.unwrap();

    // Assert
    assert_eq!(meta.row_count, 1000);
    assert!(meta.byte_size > 0);
    assert_eq!(meta.column_stats.len(), 5);
}
```

---

## 5. Commit Convention

Format: `type(scope): description`

```
feat(storage): implement micro-partition writer
fix(coordinator): fix CBO crash on empty table
test(worker): add cache hit/miss tests
docs(guide): update getting-started.md
refactor(common): consolidate error types
chore(ci): add clippy to CI pipeline
perf(coordinator): optimize MP pruning hot path
```

**Types:** `feat`, `fix`, `test`, `docs`, `refactor`, `chore`, `perf`, `style`
**Scopes:** `storage`, `coordinator`, `worker`, `common`, `ci`, `docs`

---

## 6. Quality Gates

All must pass before merge:

```bash
# 1. Build
cargo build --release

# 2. Test
cargo test --all
# or: cargo nextest run --all

# 3. Lint (warnings are errors)
cargo clippy --all -- -D warnings

# 4. Format check
cargo fmt --all -- --check
```

CI will automatically run these on every PR. PRs with failing gates will be blocked.

---

## 7. Code Review Checklist

- [ ] No `.unwrap()` in non-test code
- [ ] All public functions have rustdoc
- [ ] All public functions have tests
- [ ] No new dependencies without discussion
- [ ] No in-place Parquet modification
- [ ] MVCC timestamps present on all MP operations
- [ ] Crate boundaries respected (no circular deps)
- [ ] Commit message follows convention
- [ ] `cargo clippy` passes with no warnings
- [ ] `cargo fmt --check` passes
