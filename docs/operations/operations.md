# Operations

This document covers day-2 operation of the **supported** AgentDB deployment
shape: one server node.

For deployment choices, see [deployment.md](deployment.md). For multi-node
status, see [cluster-status.md](cluster-status.md). For human and AI-agent
operations, use [agentdbctl](agentdbctl.md) by default.

Detailed single-node runbooks:

- [single-node-production-checklist.md](single-node-production-checklist.md)
- [backup-verification.md](backup-verification.md)
- [restore-recovery.md](restore-recovery.md)
- [single-node-upgrade.md](single-node-upgrade.md)
- [storage-maintenance.md](storage-maintenance.md)
- [disk-full-recovery.md](disk-full-recovery.md)
- [routine-maintenance.md](routine-maintenance.md)
- [cdc-export.md](cdc-export.md)
- [tiered-storage.md](tiered-storage.md)

## Scope

This document is intentionally about one maintained shape:

- one AgentDB server process
- one config file
- one local node, optionally backed by object storage

It does not claim cluster procedures that the product does not yet support.

## Start

Ephemeral development server:

```bash
agentdb serve --dev
```

From source:

```bash
cargo run -p agentdb-server -- serve --dev
```

Persistent node:

```bash
agentdb init-config --output /etc/agentdb/agentdb.toml
agentdb doctor --config /etc/agentdb/agentdb.toml
agentdb serve --config /etc/agentdb/agentdb.toml
```

Useful checks:

```bash
curl -fsS http://127.0.0.1:7878/health/live
curl -fsS http://127.0.0.1:7878/health/ready
agentdbctl status
agentdbctl -o json status
open http://127.0.0.1:7878/dashboard/
```

Use `/health/live` only for process-restart decisions. Load balancers and
traffic gates must use `/health/ready`, which returns 503 while a node is
draining or cluster routing state is unsafe. Operators should usually use
`agentdbctl status` because it aggregates health, readiness, scheduler, and
drain state in one command. Health routes do not require authentication.

## Configuration

Minimal persistent config:

```toml
[server]
bind = "0.0.0.0:7878"
dev = false

[storage]
data_dir = "/var/lib/agentdb"
# Optional. Defaults to "<data_dir>/content".
content_dir = "/var/lib/agentdb/content"
```

A non-loopback bind is rejected unless `server.api_auth_token` or
`AGENTDB_API_AUTH_TOKEN` contains at least 16 visible ASCII characters with no
whitespace. Keep secrets outside the config repository and pass the same value
to clients as `AGENTDB_TOKEN` or `agentdbctl --token`:

```bash
export AGENTDB_API_AUTH_TOKEN='replace-with-a-random-secret'
export AGENTDB_TOKEN="$AGENTDB_API_AUTH_TOKEN"
agentdb doctor --config /etc/agentdb/agentdb.toml
agentdb serve --config /etc/agentdb/agentdb.toml
```

On a loopback bind, omitting the static token leaves the whole API open only
until the first metadata-backed token is ever created; after that, every
`/api/*` request needs a credential, and revoking all tokens does not reopen
the bootstrap path. Configure the static token even on loopback when other
local users or workloads are not trusted.

The static API token is the installation-wide root credential. Terminate TLS
before sending any token over a network. Rotation accepts a current and a
distinct previous token through `AGENTDB_API_AUTH_PREVIOUS_TOKEN`; the
previous token follows the same visible-ASCII rule, is valid only while a
current token is configured, and should be removed after every client has
moved.

Metadata-backed tokens minted at `/api/admin/tokens` carry one of three
scopes: `admin` (full access to the admin and data planes), `readonly`
(GET/HEAD anywhere), and `tenant` (data-plane access bound to exactly one
tenant — mint with `{"scope": "tenant", "tenant_id": "t_..."}` against an
existing tenant). A tenant-scoped bearer can only reach
`/api/tenants/{its_tenant}/...`, `GET /api/me`, and `POST /api/sessions/open`
(which validates the workspace against the token binding); other routes return
403. `GET /api/me` reports the caller's scope and bound tenant, so an SDK can
discover its workspace from the credential alone. Verification results are
memoized in memory for `AGENTDB_AUTH_CACHE_TTL_MS` (default 60000; 0 disables
caching); mint and revoke invalidate the local node's cache immediately, so on
a multi-node deployment a revoked token can remain valid on other nodes for up
to the TTL. Token hashes live as records under a reserved system tenant inside
AgentDB's own data directory (metadata self-hosting): they are covered by
the same durability, replication, and recovery-manifest machinery as user
data — `agentdb restore-from-bucket` restores them with everything else,
and no separate metadata backup is needed for them.

The dashboard keeps a bearer token in page memory only. It deletes legacy
stored tokens, never accepts a token from URL parameters, and strips `token`
and `access_token` parameters from the address bar. Reloading the page requires
entering the token again. Dashboard assets are embedded in the server binary,
so no source-tree `static/` directory is required at runtime.

The repository also includes a template at
[config/agentdb.toml](../../config/agentdb.toml).
Configuration sections reject unknown fields. A misspelled or obsolete key
therefore makes `doctor`/startup fail instead of silently running with a
default; validate the file before every rollout.

### Startup Hydration Limits

The current single-node runtime hydrates metadata and record indexes into
memory at startup. A preflight scans those persistent namespaces before
hydration and fails startup instead of allowing unbounded memory growth. The
default total scanned key/value budget is 512 MiB. Row defaults are:

| Environment variable | Default rows | Applies to |
|---|---:|---|
| `AGENTDB_STARTUP_MAX_TENANTS` | 10,000 | tenants and tenant aliases |
| `AGENTDB_STARTUP_MAX_AGENTS` | 100,000 | agents and agent aliases |
| `AGENTDB_STARTUP_MAX_SESSIONS` | 250,000 | sessions and session aliases |
| `AGENTDB_STARTUP_MAX_ARTIFACTS` | 250,000 | artifact metadata |
| `AGENTDB_STARTUP_MAX_RECORDS` | 100,000 | records, index outbox, and index commits |
| `AGENTDB_STARTUP_MAX_TENANT_META` | 500,000 | tenant metadata rows |
| `AGENTDB_STARTUP_MAX_IDEMPOTENCY` | 1,000,000 | retained idempotency results |
| `AGENTDB_STARTUP_MAX_CONTENT_OBJECTS` | 500,000 | local content entries listed for orphan reconciliation; separate from the hydrated-byte total |

`AGENTDB_STARTUP_MAX_HYDRATED_BYTES` changes the aggregate byte limit. Only a
positive integer overrides a default; an unset, zero, or invalid value retains
the corresponding default. Raising a cap does not make hydration streaming;
size RSS and startup time on a restored copy first. Startup performs one O(N)
preflight scan and then hydration, and the server log reports the scanned row
and byte totals. Long-term removal of whole-state hydration remains an
architecture scaling item.

## Stop

Foreground process:

```bash
Ctrl-C
```

If managed by your init system or orchestrator, use the platform-native stop
mechanism and wait for the process to exit cleanly before maintenance that
touches local storage.

## Restart

Standard restart flow:

```bash
agentdb doctor --config /etc/agentdb/agentdb.toml
agentdb serve --config /etc/agentdb/agentdb.toml
```

If the node uses local RocksDB storage, avoid copying or mutating the data
directory while writes are active unless you are using a crash-consistent
filesystem snapshot.

## Backup And Restore

For online backup manifests and restore validation, use `agentdbctl`:

```bash
agentdbctl backup create \
  --backup-id nightly-2026-05-05 \
  --directory /var/backups/agentdb

agentdbctl restore validate /var/backups/agentdb/nightly-2026-05-05/manifest.json
```

The online backup command writes a manifest and optional metadata snapshot. It
does not replace a full runtime data backup for local RocksDB deployments.
Use [backup-verification.md](backup-verification.md) for the supported
verification flow, tablet checkpoint checks, scratch install drills, and failure
handling. Use [restore-recovery.md](restore-recovery.md) for offline recovery
procedures.

### Local Storage

Back up a stopped node by copying the data directory:

```bash
rsync -a /var/lib/agentdb/ /var/backups/agentdb/
```

For a running process:

- stop AgentDB first, or
- use a crash-consistent filesystem snapshot

Do not do an ad-hoc file copy of active RocksDB files and assume it is safe.

### Object-Store-Backed Deployments

If you use object-store content:

- back up the config file
- preserve object-store bucket policy and lifecycle configuration
- document the bucket/prefix ownership outside AgentDB

The durable truth may now span:

- local node config
- object-store runtime metadata
- object-store content blobs

For persistent standalone local-filesystem content, startup lists content
objects before opening the listener and removes blobs not referenced by
metadata that still retains raw content. It logs objects scanned, referenced,
deleted, crash-left temporary files deleted, and delete errors. The listing is
bounded by `AGENTDB_STARTUP_MAX_CONTENT_OBJECTS`; exceeding the bound fails
startup rather than allocating an unbounded inventory.

Artifact writes also clean up content immediately when metadata persistence
fails, and local content renames fsync the containing directory. Object-store
content currently has no listing implementation, so startup reconciliation is
reported as unsupported there; bucket inventory/lifecycle checks remain an
operator responsibility.

## Upgrade

For the full preflight, upgrade, verification, and rollback procedure, see
[single-node-upgrade.md](single-node-upgrade.md).

### Local Binary Upgrade

Check version:

```bash
agentdb --version
```

Recommended sequence:

1. stop the running node
2. back up or snapshot storage
3. install the new binary
4. run `agentdb doctor --config <path>`
5. start the node with the same config
6. verify `agentdbctl status` and the dashboard

Example:

```bash
AGENTDB_VERSION=vX.Y.Z \
  curl -sSL https://raw.githubusercontent.com/shenli/agentdb/main/scripts/install.sh | sh
```

### Docker / Compose Upgrade

Rebuild or pull the new image, then recreate the single container while keeping
the persistent volume:

```bash
docker compose down
docker compose up --build -d
```

### Helm Evaluation Upgrade

Use normal Helm upgrade flow, but remember the chart is still single-replica
evaluation:

```bash
helm upgrade my-agentdb ./examples/helm/agentdb
```

Treat it as a single-node restart in Kubernetes, not a rolling multi-node
cluster upgrade.

## Troubleshooting

For disk-full incidents, see [disk-full-recovery.md](disk-full-recovery.md).
For storage inspection and compaction, see
[storage-maintenance.md](storage-maintenance.md).

Port already in use:

```bash
agentdb serve --dev --bind 127.0.0.1:8787
```

Permission denied on data directory:

```bash
mkdir -p ./agentdb-data
chmod u+rwx ./agentdb-data
agentdb serve --data-dir ./agentdb-data
```

Need a fresh local state directory:

```bash
rm -rf ./agentdb-data
agentdb serve --data-dir ./agentdb-data
```

Structured API errors with `doc_url`:

- start at [../runbooks/README.md](../runbooks/README.md)

## What This Document Does Not Cover

This document does not describe:

- adding or removing cluster nodes
- replacing failed cluster members
- routing-table repair across nodes
- rolling cluster upgrades
- shard or placement rebalancing

Those are not yet supported operator workflows in the current repository. See
[cluster-status.md](cluster-status.md).
