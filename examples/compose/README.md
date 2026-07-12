# AgentDB compose example

Single-node AgentDB stack with one container and one named volume. Runtime
metadata and artifact content both live under `/data` in the container.

For local development the fastest path is
`cargo run -p agentdb-server -- serve --dev` — zero config,
ephemeral state, ready in seconds. This compose stack is for checking the
containerized single-node path with persisted local state.

## Bring up

```bash
cd examples/compose
export AGENTDB_API_AUTH_TOKEN='replace-with-at-least-16-characters'
docker compose up --build
```

Compose requires the token because the server listens on `0.0.0.0` inside the
container. It must contain at least 16 visible ASCII characters with no
whitespace. The published host port is restricted to `127.0.0.1`.

A clean ARM64 Docker Desktop validation build took 5 minutes 2 seconds; host
CPU, architecture, and registry cache will change that number. The manifest
layer caches crate downloads, and an unchanged rebuild reuses the final image;
source changes still recompile the affected Rust/native dependency graph.

## Probe it

```bash
# Readiness — should return HTTP 200
curl -i http://localhost:7878/health/ready

# Liveness — process restart signal, not a traffic gate
curl -i http://localhost:7878/health/live

# Dashboard
open http://localhost:7878/dashboard
```

Enter `AGENTDB_API_AUTH_TOKEN` in the dashboard for API views. It stays in page
memory only and must be entered again after reload.

## Drive some traffic

```bash
# Python SDK — recommended for application code
pip install -e clients/python
AGENTDB_TOKEN="$AGENTDB_API_AUTH_TOKEN" \
  python3 examples/agents/python/quickstart.py
```

## Tear down

```bash
docker compose down        # keep volumes
docker compose down -v     # wipe volumes (fresh start next up)
```

## Configuration

The image ships with `/data/agentdb.toml`:

```toml
[server]
bind = "0.0.0.0:7878"
dev = false

[storage]
data_dir = "/data/runtime"
content_dir = "/data/content"

[storage.tablet]
layout = "grouped_rocksdb"
root_dir = "/data/runtime/tablet-stores"
tablets_per_domain = 32
max_open_domains = 16
domain_write_buffer_size_mib = 16
domain_max_write_buffers = 2
domain_parallelism = 1
domain_max_background_jobs = 1
domain_max_open_files = 128
```

To override it, bind-mount your own config file and set the command to
`serve --config /path/to/agentdb.toml`.

## Known limitations

- The `agentdb` container runs as `agentdb` UID inside the image.
  On Linux hosts, bind-mounting a host path into `/data` requires
  the host directory to be owned by that UID. The named-volume
  approach in the compose file sidesteps this — bind mounts are
  for operators who want host-side persistence.
- Single-node only. Use the native `agentdb serve` path for local
  development and this compose stack when you want Docker packaging in the
  loop.
