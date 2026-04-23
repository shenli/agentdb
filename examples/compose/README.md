# AgentDB compose example

Single-node AgentDB + MinIO stack. The AgentDB process runs the
production-shape storage path: SlateDB-backed KV over an S3-
compatible bucket (MinIO here).

## Why not multi-node?

The current architecture acquires a lease for every tablet
(`0..AGENTDB_BOXES`) at boot. Two nodes pointing at the same meta
store would race for tablet 0. Multi-node is blocked on the M20
shard/tablet refactor — tracked in
[`docs/superpowers/plans/2026-04-21-v2-design-implementation.md`](../../docs/superpowers/plans/2026-04-21-v2-design-implementation.md)
§M20.

For local development you usually want `agentdb serve --dev`
directly; this compose example exists for operators evaluating the
production shape without provisioning real S3.

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
# Mongo wire
mongosh "mongodb://localhost:7879" --eval '
  db.getSiblingDB("t-1").events.insertOne({session_id:"s-1",payload:"hello"});
  db.getSiblingDB("t-1").events.find({}).limit(5).toArray();
'

# REST
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
