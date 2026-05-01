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
docker compose up --build
```

First run takes a few minutes (Rust release build). Subsequent
`up` cycles are cached — code changes under `bin/` and `crates/`
re-link in ~30s thanks to the layered Cargo cache in the
Dockerfile.

## Probe it

```bash
# Readiness — should return 200 with status:"ok"
curl -s http://localhost:7878/health/ready | jq

# Dashboard
open http://localhost:7878/dashboard
```

## Drive some traffic

```bash
# Python SDK — recommended for application code
pip install -e clients/python
python examples/agents/python/quickstart.py
```

## Tear down

```bash
docker compose down        # keep volumes
docker compose down -v     # wipe volumes (fresh start next up)
```

## Configuration

Edit the `environment:` block in `docker-compose.yml` to override
single-node settings such as `AGENTDB_BIND`, `AGENTDB_DATA_DIR`, and
`AGENTDB_CONTENT_DIR`.

## Known limitations

- The `agentdb` container runs as `agentdb` UID inside the image.
  On Linux hosts, bind-mounting a host path into `/data` requires
  the host directory to be owned by that UID. The named-volume
  approach in the compose file sidesteps this — bind mounts are
  for operators who want host-side persistence.
- Single-node only. Use the native `agentdb serve` path for local
  development and this compose stack when you want Docker packaging in the
  loop.
