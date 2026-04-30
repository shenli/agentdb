# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the `agentdb` binary.
#
# Stage 1 (`builder`): full Rust toolchain. Builds the release
# binary and strips it. A chiseled dep-compile pass happens first
# so code-only changes re-link in ~30s rather than refetching
# every crate.
#
# Stage 2 (`runtime`): distroless-slim base (Debian 12 slim —
# stays stable across Rust releases and ships ca-certificates + a
# glibc that rustls happens to depend on transitively). Only the
# binary lands in the final image; the source tree and the
# ~3GB target/ directory stay in the builder.

# -------- builder --------
FROM rust:1.95-bookworm AS builder
WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# Prime the dep cache. We copy just the manifests first so
# `cargo fetch` and the noop-compile happen before the source
# tree changes. A change to any `.rs` file invalidates only the
# `COPY . .` layer, not the downloaded crates.
COPY Cargo.toml Cargo.lock ./
COPY bin/agentdb-server/Cargo.toml bin/agentdb-server/
COPY crates/agentdb-api/Cargo.toml crates/agentdb-api/
COPY crates/agentdb-core/Cargo.toml crates/agentdb-core/
COPY crates/agentdb-dashboard/Cargo.toml crates/agentdb-dashboard/
COPY crates/agentdb-index/Cargo.toml crates/agentdb-index/
COPY crates/agentdb-meta/Cargo.toml crates/agentdb-meta/
COPY crates/agentdb-storage/Cargo.toml crates/agentdb-storage/

# Dummy source files are needed because cargo can't resolve the
# workspace without them. `cargo fetch --locked` materializes the
# registry tree into $CARGO_HOME without touching the source, which
# is the layer cache we want.
RUN mkdir -p bin/agentdb-server/src crates/agentdb-api/src \
        crates/agentdb-core/src crates/agentdb-dashboard/src \
        crates/agentdb-index/src crates/agentdb-meta/src \
        crates/agentdb-storage/src crates/agentdb-index/benches \
    && echo "fn main(){}" > bin/agentdb-server/src/main.rs \
    && for c in api core dashboard index meta storage; do \
           echo "" > crates/agentdb-$c/src/lib.rs; \
       done \
    && echo "fn main(){}" > crates/agentdb-index/benches/vector_bench.rs \
    && cargo fetch --locked

# Now the real build. Source copy invalidates the compile-cache
# layer but not the fetched-crates layer above.
COPY . .
RUN cargo build --release --locked -p agentdb-server \
    && strip target/release/agentdb

# -------- runtime --------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

# Non-root user with a writable data directory. Volume mounts land
# under /data so operators can persist state without bind-mounting
# the root filesystem.
RUN groupadd --system agentdb \
    && useradd --system --gid agentdb --home /data --create-home agentdb \
    && chown agentdb:agentdb /data
USER agentdb
WORKDIR /data

COPY --from=builder --chown=agentdb:agentdb /src/target/release/agentdb /usr/local/bin/agentdb

# REST + Mongo wire. Dashboard shares the REST port.
EXPOSE 7878 7879

# Point the binary at the data dir we just made writable. These
# envs are still overridable at runtime.
ENV AGENTDB_DATA=/data/rocksdb \
    AGENTDB_OBJECTS=/data/objects \
    AGENTDB_META_PATH=/data/meta.sqlite \
    AGENTDB_ADDR=0.0.0.0:7878 \
    AGENTDB_MONGO_ADDR=0.0.0.0:7879 \
    RUST_LOG=info

# Tini reaps zombies cleanly when docker sends SIGTERM. The binary
# doesn't fork, so this is belt-and-braces, but it also gives a
# cleaner shutdown path.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/agentdb"]
CMD ["serve"]
