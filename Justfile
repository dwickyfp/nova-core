# nova-core Justfile — task runner

# Default: show available commands
default:
    @just --list

# Build debug
build:
    cargo build

# Build release
build-release:
    cargo build --release

# Run all tests
test:
    cargo test --all

# Run tests with nextest (faster)
test-fast:
    cargo nextest run --all

# Run tests for specific crate
test-crate crate:
    cargo test -p {{crate}}

# Run clippy (warnings = errors)
clippy:
    cargo clippy --all -- -D warnings

# Format code
fmt:
    cargo fmt --all

# Check formatting
fmt-check:
    cargo fmt --all -- --check

# Full CI check locally
ci: fmt-check clippy test
    @echo "✅ All CI checks passed"

# Run benchmarks
bench crate:
    cargo bench -p {{crate}}

# Start local dev cluster (Docker)
dev-up:
    docker compose -f docker/docker-compose.yml up -d

# Stop local dev cluster
dev-down:
    docker compose -f docker/docker-compose.yml down

# Clean build artifacts
clean:
    cargo clean

# Update dependencies
update-deps:
    cargo update

# Show dependency tree
deps:
    cargo tree --all

# Generate docs
docs:
    cargo doc --all --no-deps --open
