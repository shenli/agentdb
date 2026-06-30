# Deployment

This document is the deployment entry point for AgentDB.

It covers:

- supported deployment shapes
- how to choose between local, binary, Docker, Compose, and Helm paths
- what is and is not true cluster deployment today

For start/stop, backup, restore, upgrade, and troubleshooting, see
[operations.md](operations.md). For the current distributed-story status, see
[cluster-status.md](cluster-status.md). For the intended future cluster model,
see [cluster-deployment.md](cluster-deployment.md) and
[cluster-operations.md](cluster-operations.md).

For human operations, prefer [agentdbctl](agentdbctl.md). Raw HTTP endpoints
are the backend contract and are still useful for load balancers, integrations,
and emergency debugging, but normal operator docs should use the CLI.

## Current Support Summary

AgentDB currently has one maintained deployment target:

- **single-node AgentDB server**

That single node can be packaged in several ways:

- source checkout
- installed binary
- Docker
- Docker Compose
- Helm chart for **single-replica evaluation** on Kubernetes

AgentDB does **not** currently have a supported multi-node cluster deployment
guide in this repository. There is routing and distributed-design work in the
codebase and docs, but not a user-facing cluster product you should operate in
production.

## Supported Deployment Shapes

### 1. Local Development

Fastest path, ephemeral state:

```bash
cargo run -p agentdb-server -- serve --dev
```

or, if `agentdb` is installed:

```bash
agentdb serve --dev
```

This is the right path for:

- repository development
- SDK testing
- local demos
- short-lived experiments

State is in-memory and disappears when the process exits.

### 2. Persistent Single Node

Maintained deployment shape for a real local or self-hosted runtime:

```bash
agentdb init-config --output /etc/agentdb/agentdb.toml
agentdb doctor --config /etc/agentdb/agentdb.toml
agentdb serve --config /etc/agentdb/agentdb.toml
```

Typical storage layouts:

- RocksDB + local filesystem
- SlateDB + shared object store
- local filesystem or object-store-backed content blobs

This is the recommended shape for:

- prototypes
- internal tools
- small self-hosted deployments
- evaluation of tiered session storage

### 3. Docker

Containerized single-node deployment:

```bash
docker build -t agentdb:local .
docker run --rm -p 7878:7878 -v agentdb-data:/data agentdb:local
```

Use this when you want:

- container packaging
- a named persistent volume
- parity with future orchestrated deployments

### 4. Docker Compose

Compose is still **single-node**. It is a packaging/evaluation path, not a
cluster topology.

```bash
cd examples/compose
docker compose up --build
```

Use it when you want to check:

- container startup behavior
- mounted config behavior
- persisted local state with Docker volumes

### 5. Helm

The Helm chart is also **single-node**, with one StatefulSet replica for
evaluation on a Kubernetes cluster.

It is useful for:

- local Kind / k3d evaluation
- testing mounted config and PVC behavior
- basic Kubernetes packaging validation

It is **not** a multi-node AgentDB cluster deployment.

See [examples/helm/agentdb/README.md](../../examples/helm/agentdb/README.md).

## Deployment Matrix

| Path | Topology | Persistence | Intended use |
|---|---|---|---|
| `serve --dev` | single process | none | local development |
| config + binary | single node | durable | maintained self-hosted runtime |
| Docker | single node | durable volume | container packaging |
| Compose | single node | durable volume | local container evaluation |
| Helm | single node in k8s | PVC/object store | evaluation on Kubernetes |

## Cluster Deployment Status

AgentDB does **not** currently document or support these as stable operator
workflows:

- create a multi-node cluster
- add nodes to a running cluster
- remove nodes from a running cluster
- replace failed nodes
- rolling restart a cluster
- rolling binary upgrade across nodes
- shard rebalancing or placement migration

If you are looking for those workflows, the right current answer is:

- they are **not yet a supported deployment mode**
- there is **no canonical runbook** in this repo for those actions today

See [cluster-status.md](cluster-status.md) for details.

## Recommended Deployment Choice

Choose the simplest shape that matches your goal:

- developing AgentDB itself: `cargo run -p agentdb-server -- serve --dev`
- evaluating durable local state: persistent single-node config
- testing container packaging: Docker or Compose
- testing Kubernetes packaging: Helm evaluation chart
- operating a true AgentDB cluster: not yet a supported user-facing path

## Preflight Checklist

Before exposing a maintained node:

- use a config file instead of long CLI flag lists
- run `agentdb doctor --config <path>`
- verify operator readiness with `agentdbctl status`
- keep `/health/ready` available for load balancers and uptime probes
- place runtime metadata and content on durable storage
- decide whether content should stay local or move to object storage
- define backup/restore procedure before production traffic
- understand that Helm today is single-replica evaluation, not clustered HA

## Related Docs

- [operations.md](operations.md) - start, stop, backup, restore, upgrade, and
  troubleshooting
- [agentdbctl.md](agentdbctl.md) - recommended CLI for human and AI-agent
  operations
- [cluster-status.md](cluster-status.md) - current multi-node status and
  missing operator workflows
- [cluster-deployment.md](cluster-deployment.md) - target cluster topology and
  bootstrap model
- [cluster-operations.md](cluster-operations.md) - target day-2 cluster
  procedures
- [cluster-alerts.md](cluster-alerts.md) - evaluation cluster alert triage
- [performance.md](performance.md) - benchmark notes and current caveats
- [../architecture/architecture.md](../architecture/architecture.md) - current
  process, storage, routing, and lifecycle implementation
- [../runbooks/README.md](../runbooks/README.md) - API-error runbooks
