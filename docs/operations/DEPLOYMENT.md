# Deployment

AgentDB currently supports a single-node deployment shape. One `agentdb`
process serves the native HTTP API, local runtime metadata, artifact content,
and the dashboard.

Use this document to choose the deployment path. Use
[SINGLE_NODE.md](SINGLE_NODE.md) for exact commands, config examples, Docker
usage, backup steps, and troubleshooting.

## Supported Shape

The maintained deployment target is a single AgentDB server with local durable
storage:

```text
clients / SDKs / adapters
        |
        v
agentdb server
  - native HTTP API
  - dashboard
  - RocksDB runtime metadata
  - filesystem artifact content
```

This is the right shape for local development, prototypes, demos, and small
self-hosted agent runtimes. Multi-node operation is design work, not the
documented production path for this repository.

## Deployment Paths

### Source Checkout

Use this path while developing AgentDB itself:

```bash
cargo run -p agentdb-server -- serve --dev
```

For durable local state:

```bash
cargo run -p agentdb-server -- init-config
cargo run -p agentdb-server -- doctor
cargo run -p agentdb-server -- serve --config agentdb.toml
```

See [SINGLE_NODE.md](SINGLE_NODE.md#persistent-local-server).

### Installed Binary

Use this path for a maintained single-node server:

```bash
agentdb init-config --output /etc/agentdb/agentdb.toml
agentdb doctor --config /etc/agentdb/agentdb.toml
agentdb serve --config /etc/agentdb/agentdb.toml
```

See [SINGLE_NODE.md](SINGLE_NODE.md#configuration).

### Docker

Use this path when you want container packaging with persisted local state:

```bash
docker build -t agentdb:local .
docker run --rm -p 7878:7878 -v agentdb-data:/data agentdb:local
```

See [SINGLE_NODE.md](SINGLE_NODE.md#docker) and
[SINGLE_NODE.md](SINGLE_NODE.md#docker-compose).

## Operational Checklist

Before exposing a maintained node:

- Use a config file instead of long CLI flag lists.
- Bind deliberately: `127.0.0.1:7878` for local-only, `0.0.0.0:7878` for
  container or host-network exposure.
- Put runtime metadata and artifact content on durable storage.
- Decide whether artifact content should live under `data_dir` or on a separate
  disk via `content_dir`.
- Run `agentdb doctor --config <path>` before starting the service.
- Monitor `GET /health/ready`.
- Back up stopped data directories or use crash-consistent filesystem
  snapshots for running nodes.
- Use [../runbooks/README.md](../runbooks/README.md) when clients receive
  structured API errors with `doc_url`.

## Related Docs

- [SINGLE_NODE.md](SINGLE_NODE.md) - commands, config, Docker, maintenance, and
  troubleshooting
- [../architecture/ARCHITECTURE.md](../architecture/ARCHITECTURE.md) - process, crate, storage, and routing
  boundaries
- [PERFORMANCE.md](PERFORMANCE.md) - benchmark commands and current performance
  notes
- [../runbooks/README.md](../runbooks/README.md) - operator runbooks for API error
  codes
