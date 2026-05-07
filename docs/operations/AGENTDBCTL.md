# agentdbctl

`agentdbctl` is the recommended human and AI-agent interface for operating
AgentDB. It wraps the admin HTTP API with concise commands, table output for
humans, and JSON output for automation.

The REST API remains the stable machine contract. Operators should not need to
hand-write `curl` payloads for normal day-2 workflows.

## Configuration

Default target:

```bash
agentdbctl status
```

Custom target:

```bash
agentdbctl --url http://127.0.0.1:7878 status
```

Environment variables:

```bash
export AGENTDB_URL=http://127.0.0.1:7878
export AGENTDB_TOKEN=...
```

Use JSON output for scripts and AI agents:

```bash
agentdbctl -o json status
agentdbctl -o json nodes list
```

## Core Workflows

Cluster status:

```bash
agentdbctl status
agentdbctl nodes list
agentdbctl tablets list
agentdbctl raft status
```

Drain and undrain a node:

```bash
agentdbctl drain start
agentdbctl nodes maintenance node-a alive
```

Move ownership:

```bash
agentdbctl tablets rebalance --dry-run
agentdbctl tablets rebalance --max-movements 10
agentdbctl tablets handoff 7 node-b
agentdbctl tablets complete-handoff 7 node-a
agentdbctl tablets prepare-replacement 7 node-b --source-node-id node-a
agentdbctl raft catch-up --tablet-id 7 --target-node-id node-b
agentdbctl tablets complete-replacement 7 node-b --source-node-id node-a --min-applied-index 1024 --retire-source
```

`rebalance` assigns unowned tablets directly. For owned tablets, it prepares a
replacement intent by marking the target as a learner and handoff destination;
the next rebalance pass can complete the ownership handoff after the target is
healthy and caught up. Use `raft catch-up` to accelerate convergence and
`complete-replacement` when you need explicit operator-controlled promotion or
source retirement.

Split/merge lifecycle:

```bash
agentdbctl tablets prepare-split 7 --child-range 0:49:8 --child-range 50:99:9
agentdbctl tablets complete-split 7 --child-range 0:49:8 --child-range 50:99:9 --map-epoch 2 --min-applied-index 1024
agentdbctl tablets prepare-merge --source-tablet-id 8 --source-tablet-id 9 10
agentdbctl tablets complete-merge --source-tablet-id 8 --source-tablet-id 9 10 --map-epoch 3 --min-applied-index 1024
```

Prepare only fences metadata and creates destination tablets. Complete publishes
the range-map cutover and lifecycle states after destination replicas are
healthy and caught up to the source applied index. Use `--allow-unready` only
for emergency/manual recovery after separately proving state safety.

Raft catch-up:

```bash
agentdbctl raft status
agentdbctl raft catch-up --tablet-id 7 --target-node-id node-b --dry-run
agentdbctl raft catch-up --tablet-id 7 --target-node-id node-b --min-lag 128
agentdbctl raft snapshots --tablet-id 7
agentdbctl raft prune-snapshots --dry-run
```

Backup and restore validation:

```bash
agentdbctl backup create --backup-id nightly-2026-05-05 --directory ./backups
agentdbctl backup create --backup-id metadata-only --skip-runtime-summary
agentdbctl restore validate ./backups/nightly-2026-05-05/manifest.json
```

Register or remove nodes:

```bash
agentdbctl nodes register \
  --node-id node-a \
  --address http://10.0.0.10:7878 \
  --region us-west-2 \
  --availability-zone us-west-2a

agentdbctl nodes delete node-a
```

## Raw Escape Hatch

Use `raw` for endpoints that the CLI does not model yet:

```bash
agentdbctl raw get /api/admin/routing
agentdbctl raw post /api/admin/tablets/rebalance --body '{"dry_run":true}'
```

Prefer modeled commands in runbooks. `raw` is for development and emergency
workarounds.

## Design Rules

- Every mutating command maps to a documented admin API.
- `table` output is optimized for humans.
- `json` output is stable enough for scripts and AI agents.
- Dangerous operations should use explicit verbs (`delete`, `relinquish`,
  `maintenance`) rather than hidden flags.
