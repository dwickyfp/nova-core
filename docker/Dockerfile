# ── Build stage ──
FROM rust:1.91-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libclang-dev \
    protobuf-compiler \
    && arch="$(dpkg --print-architecture)" \
    && [ "$arch" = "arm64" ] && fdb_arch="aarch64" || fdb_arch="$arch" \
    && curl -L -f -o /tmp/foundationdb-clients.deb "https://github.com/apple/foundationdb/releases/download/7.4.6/foundationdb-clients_7.4.6-1_${fdb_arch}.deb" \
    && dpkg -i /tmp/foundationdb-clients.deb \
    && rm /tmp/foundationdb-clients.deb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY proto/ proto/
COPY crates/ crates/

RUN cargo build --bin nova

# ── Runtime stage ──
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && arch="$(dpkg --print-architecture)" \
    && [ "$arch" = "arm64" ] && fdb_arch="aarch64" || fdb_arch="$arch" \
    && curl -L -f -o /tmp/foundationdb-clients.deb "https://github.com/apple/foundationdb/releases/download/7.4.6/foundationdb-clients_7.4.6-1_${fdb_arch}.deb" \
    && dpkg -i /tmp/foundationdb-clients.deb \
    && rm /tmp/foundationdb-clients.deb \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/debug/nova /usr/local/bin/nova

ENTRYPOINT ["nova"]
