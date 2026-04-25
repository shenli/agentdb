# AgentDB compose example

Single-node-by-default AgentDB + MinIO stack. The AgentDB process
runs the production-shape storage path: SlateDB-backed KV over an
S3-compatible bucket (MinIO here).

For local development the fastest path is
`cargo run -p agentdb-server -- serve --dev` — zero config,
ephemeral state, ready in seconds. This compose stack is for
operators evaluating the production shape without provisioning
real S3.

## Multi-node

Multi-node has worked since the M20 shard/tablet refactor (PR
trail under `docs/superpowers/plans/2026-04-21-v2-design-implementation.md`).
To add a peer:

1. Copy the `agentdb` service block in `docker-compose.yml`,
   give the copy a unique `AGENTDB_NODE_ID` and `ports:` mapping,
   and point both nodes at the same `AGENTDB_META_URL`
   (Postgres) or share the bind-mounted SQLite file.
2. The first node to win leader election seeds the tablets and
   the placement loop spreads them round-robin across alive
   nodes; the second reconciler picks up its share within
   ~one tick (~3s).
3. `docker compose exec agentdb agentdb cluster-status` confirms
   both nodes are alive and tablets are spread.

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

# Prometheus metrics
curl -s http://localhost:7878/metrics | head -20

# Scheduler state via the bundled CLI (runs inside the container)
docker compose exec agentdb agentdb show-scheduler
docker compose exec agentdb agentdb show-tablets
docker compose exec agentdb agentdb show-nodes

# Dashboard
open http://localhost:7878/dashboard

# MinIO console — browse the agentdb bucket, see SlateDB files land
open http://localhost:9001   # user minio / password minio123
```

## Drive some traffic

```bash
# Python SDK — recommended for application code
pip install -e clients/python
python examples/agents/python/quickstart.py

# Mongo wire — works with any MongoDB driver
mongosh "mongodb://localhost:7879" --eval '
  db.getSiblingDB("t-1").events.insertOne({session_id:"s-1",payload:"hello"});
  db.getSiblingDB("t-1").events.find({}).limit(5).toArray();
'

# REST — easy from any HTTP client
curl -s -XPOST http://localhost:7878/api/tenants/t-1/sessions/s-1/append_and_fetch \
  -H 'Content-Type: application/json' \
  -d '{"event":{"text":"from curl"}}' | jq
```

## Tear down

```bash
docker compose down        # keep volumes
docker compose down -v     # wipe volumes (fresh start next up)
```

## Configuration

Every [env var documented in the main
README](../../README.md#configuration-env-vars) works under compose.
Edit the `environment:` block in `docker-compose.yml` to override
(`AGENTDB_BOXES`, `AGENTDB_ADMISSION_CAPACITY`, `AGENTDB_LEASE_*`,
etc.).

## Known limitations

- The `agentdb` container runs as `agentdb` UID inside the image.
  On Linux hosts, bind-mounting a host path into `/data` requires
  the host directory to be owned by that UID. The named-volume
  approach in the compose file sidesteps this — bind mounts are
  for operators who want host-side persistence.
- No authentication. The control-plane and admin endpoints are
  exposed on `localhost` only via the port publishes; inside a
  private network they're open. M19 (SCRAM + tenant-implicit) adds
  auth — tracked, not implemented.
- Single-node only. See "Why not multi-node?" above.
