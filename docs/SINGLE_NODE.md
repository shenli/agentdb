# Single-Node AgentDB

Single-node mode is the default operating shape for local development,
prototypes, demos, and small self-hosted agent runtimes. It runs one AgentDB
process with the native HTTP API, local runtime metadata, local artifact
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
agentdb init-config
agentdb doctor
agentdb serve --config agentdb.toml
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
agentdb init-config --output /etc/agentdb/agentdb.toml
```

Then edit:

```toml
[server]
bind = "127.0.0.1:7878"
dev = false

[storage]
data_dir = "/var/lib/agentdb"
content_dir = "/mnt/agentdb-artifacts"
```

## Configuration

For a maintained node, prefer a config file over a long command line:

```bash
agentdb init-config --output /etc/agentdb/agentdb.toml
agentdb doctor --config /etc/agentdb/agentdb.toml
agentdb serve --config /etc/agentdb/agentdb.toml
```

Minimal config:

```toml
[server]
bind = "0.0.0.0:7878"
dev = false

[storage]
data_dir = "/var/lib/agentdb"
# Optional. Defaults to "<data_dir>/content".
content_dir = "/var/lib/agentdb/content"
```

The repository also includes a template at `config/agentdb.toml`. CLI flags
override config values when you need a temporary one-off change. The server does
not read runtime settings from environment variables.

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

The image ships with `/data/agentdb.toml`:

```toml
[server]
bind = "0.0.0.0:7878"
dev = false

[storage]
data_dir = "/data/runtime"
content_dir = "/data/content"
```

## Docker Compose

The compose example is for evaluating the container packaging path with a named
local volume:

```bash
cd examples/compose
docker compose up --build
```

Use plain `agentdb serve --dev` for the fastest local loop. Use compose when
you specifically want the server to run in a container with persisted local
state.

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
