# syntax=docker/dockerfile:1.7
#
# Multi-stage build for the `agentdb` binary.
#
# Stage 1 (`deps`): resolves the complete workspace from manifests only.
# Stage 2 (`builder`): builds the release binaries and strips them. The
# manifest-only dependency pass happens first so code-only changes reuse the
# downloaded crate cache rather than refetching every crate.
#
# Stage 3 (`runtime`): distroless-slim base (Debian 12 slim —
# stays stable across Rust releases and ships ca-certificates + a
# glibc that rustls happens to depend on transitively). Only the
# two operator binaries land in the final image; the source tree and the
# ~3GB target/ directory stay in the builder.

# -------- dependency cache --------
FROM rust:1.95-bookworm AS deps
WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Prime the dep cache. We copy just the manifests first so
# `cargo fetch` and the noop-compile happen before the source
# tree changes. A change to any `.rs` file invalidates only the
# `COPY . .` layer, not the downloaded crates.
COPY Cargo.toml Cargo.lock ./
COPY bin/agentdb-server/Cargo.toml bin/agentdb-server/
COPY bin/agentdbctl/Cargo.toml bin/agentdbctl/
COPY crates/agentdb-api/Cargo.toml crates/agentdb-api/
COPY crates/agentdb-core/Cargo.toml crates/agentdb-core/
COPY crates/agentdb-dashboard/Cargo.toml crates/agentdb-dashboard/
COPY crates/agentdb-index/Cargo.toml crates/agentdb-index/
COPY crates/agentdb-meta/Cargo.toml crates/agentdb-meta/
COPY crates/agentdb-storage/Cargo.toml crates/agentdb-storage/
COPY crates/agentdb-wire/Cargo.toml crates/agentdb-wire/
COPY tools/agentdb-bench/Cargo.toml tools/agentdb-bench/
COPY tools/agentdb-cluster-smoke/Cargo.toml tools/agentdb-cluster-smoke/

# Dummy source files are needed because cargo can't resolve the
# workspace without them. `cargo fetch --locked` materializes the
# registry tree into $CARGO_HOME without touching the source, which
# is the layer cache we want.
RUN mkdir -p bin/agentdb-server/src bin/agentdbctl/src crates/agentdb-api/src \
        crates/agentdb-core/src crates/agentdb-dashboard/src \
        crates/agentdb-index/src crates/agentdb-meta/src \
        crates/agentdb-storage/src crates/agentdb-wire/src \
        crates/agentdb-index/benches tools/agentdb-bench/src \
        tools/agentdb-cluster-smoke/src \
    && echo "fn main(){}" > bin/agentdb-server/src/main.rs \
    && echo "fn main(){}" > bin/agentdbctl/src/main.rs \
    && echo "fn main(){}" > tools/agentdb-bench/src/main.rs \
    && echo "fn main(){}" > tools/agentdb-cluster-smoke/src/main.rs \
    && for c in api core dashboard index meta storage wire; do \
           echo "" > crates/agentdb-$c/src/lib.rs; \
       done \
    && echo "fn main(){}" > crates/agentdb-index/benches/vector_bench.rs \
    && cargo fetch --locked

# -------- builder --------
FROM deps AS builder

# Now the real build. Source copy invalidates the compile-cache
# layer but not the fetched-crates layer above.
COPY . .
RUN cargo build --release --locked -p agentdb-server -p agentdbctl \
    && strip target/release/agentdb target/release/agentdbctl

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
COPY --from=builder --chown=agentdb:agentdb /src/target/release/agentdbctl /usr/local/bin/agentdbctl

# REST API. Dashboard shares the REST port.
EXPOSE 7878

# Default single-node config. Operators can bind-mount another config file and
# pass `serve --config /path/to/agentdb.toml`.
RUN printf '%s\n' \
    '[server]' \
    'bind = "0.0.0.0:7878"' \
    'dev = false' \
    '' \
    '[storage]' \
    'data_dir = "/data/runtime"' \
    'content_dir = "/data/content"' \
    '' \
    '[storage.tablet]' \
    'layout = "grouped_rocksdb"' \
    'root_dir = "/data/runtime/tablet-stores"' \
    'tablets_per_domain = 32' \
    'max_open_domains = 16' \
    'domain_write_buffer_size_mib = 16' \
    'domain_max_write_buffers = 2' \
    'domain_parallelism = 1' \
    'domain_max_background_jobs = 1' \
    'domain_max_open_files = 128' \
    > /data/agentdb.toml
# Tini reaps zombies cleanly when docker sends SIGTERM. The binary
# doesn't fork, so this is belt-and-braces, but it also gives a
# cleaner shutdown path.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/agentdb"]
CMD ["serve", "--config", "/data/agentdb.toml"]
