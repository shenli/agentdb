# Operations

This document covers day-2 operation of the **supported** AgentDB deployment
shape: one server node.

For deployment choices, see [DEPLOYMENT.md](DEPLOYMENT.md). For multi-node
status, see [CLUSTER_STATUS.md](CLUSTER_STATUS.md).

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

Useful endpoints:

```bash
curl -fsS http://127.0.0.1:7878/health/ready
open http://127.0.0.1:7878/dashboard/
```

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

The repository also includes a template at
[config/agentdb.toml](../../config/agentdb.toml).

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

If you use SlateDB and/or object-store content:

- back up the config file
- preserve object-store bucket policy and lifecycle configuration
- document the bucket/prefix ownership outside AgentDB

The durable truth may now span:

- local node config
- object-store runtime metadata
- object-store content blobs

## Upgrade

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
6. verify `/health/ready` and the dashboard

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
[CLUSTER_STATUS.md](CLUSTER_STATUS.md).
