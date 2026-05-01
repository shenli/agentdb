# Single-Node AgentDB

Single-node mode is the default operating shape for local development,
prototypes, demos, and small self-hosted agent runtimes. It runs one AgentDB
process with the native V2 HTTP API, local runtime metadata, local artifact
content, and the dashboard on one port.

## Fast Start

Ephemeral development server:

```bash
agentdb serve --dev
```

From a source checkout:

```bash
cargo run -p agentdb-server -- serve --dev
```

By default this listens on:

```text
http://127.0.0.1:7878
```

Useful endpoints:

```bash
curl -i http://127.0.0.1:7878/health/ready
open http://127.0.0.1:7878/dashboard/
```

`--dev` is an alias for `--in-memory`; state is lost when the process exits.

## Persistent Local Server

```bash
agentdb serve --data-dir ./agentdb-data
```

This creates:

```text
agentdb-data/
  runtime/   # RocksDB runtime metadata and event state
  content/   # artifact bytes
```

Use a separate artifact directory when large outputs should live on another
disk:

```bash
agentdb serve \
  --data-dir /var/lib/agentdb \
  --content-dir /mnt/agentdb-artifacts
```

## Configuration

CLI flags are the clearest way to configure a single node:

```bash
agentdb serve \
  --bind 0.0.0.0:7878 \
  --data-dir /var/lib/agentdb \
  --content-dir /var/lib/agentdb/content
```

The same settings can come from environment variables:

| Setting | Preferred env | Compatibility env |
|---|---|---|
| Bind address | `AGENTDB_BIND` | `AGENTDB_ADDR`, `AGENTDB_HTTP_ADDR` |
| Runtime data dir | `AGENTDB_DATA_DIR` | `AGENTDB_DATA` |
| Artifact content dir | `AGENTDB_CONTENT_DIR` | `AGENTDB_OBJECTS` |
| In-memory dev mode | `AGENTDB_DEV=true` | `AGENTDB_IN_MEMORY=true` |

CLI flags win over environment variables. Empty env vars are ignored.

## Docker

Build locally:

```bash
docker build -t agentdb:local .
```

Run with a named volume:

```bash
docker run --rm \
  -p 7878:7878 \
  -v agentdb-data:/data \
  agentdb:local
```

The image defaults to:

```text
AGENTDB_BIND=0.0.0.0:7878
AGENTDB_DATA_DIR=/data/rocksdb
AGENTDB_CONTENT_DIR=/data/objects
```

## Docker Compose

The compose example is for evaluating the production-shaped object-storage
path with MinIO:

```bash
cd examples/compose
docker compose up --build
```

Use plain `agentdb serve --dev` for the fastest local loop. Use compose when
you specifically want to exercise S3-compatible artifact/object storage.

## Maintenance

Health check:

```bash
curl -fsS http://127.0.0.1:7878/health/ready
```

Stop:

```bash
Ctrl-C
```

Back up a stopped single node by copying the data and content directories:

```bash
rsync -a ./agentdb-data/ ./agentdb-data.backup/
```

For a running process, first stop AgentDB or take a filesystem snapshot. Avoid
copying RocksDB files while writes are active unless the filesystem snapshot is
crash-consistent.

Upgrade flow:

```bash
agentdb --version
agentdb serve --data-dir ./agentdb-data
```

For a release binary installed by `scripts/install.sh`, rerun the installer with
the target version:

```bash
AGENTDB_VERSION=vX.Y.Z \
  curl -sSL https://raw.githubusercontent.com/shenli/agentdb/main/scripts/install.sh | sh
```

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

Need to start fresh:

```bash
rm -rf ./agentdb-data
agentdb serve --data-dir ./agentdb-data
```

