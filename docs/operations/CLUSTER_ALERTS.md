# Cluster Alerts And Triage

This runbook covers the first alert set for clustered AgentDB evaluation. It is
intended for local and AWS cluster testing while the cluster product is still
evaluation-only.

Use `agentdbctl` first:

```bash
agentdbctl status
agentdbctl nodes list
agentdbctl cells list
agentdbctl tablets list
agentdbctl raft status
agentdbctl raft snapshots
```

Use `agentdbctl -o json ...` when feeding these checks into automation.

## Page-Level Alerts

### Leaderless Tablets

Signal:

- `raft_summary.leaderless_tablet_count > 0`
- writes fail with `lease_lost` or routing errors

Diagnose:

1. Run `agentdbctl raft status`.
2. Check whether affected replicas are stale, dead, or lagging with
   `agentdbctl nodes list` and `agentdbctl tablets list`.
3. Check whether a retained snapshot exists for lagging targets with
   `agentdbctl raft snapshots`.

Remediate:

1. If a quorum of voters is alive, wait one election interval and re-check.
2. If a follower is behind but reachable, run
   `agentdbctl raft catch-up --dry-run`, then `agentdbctl raft catch-up`.
3. If the owner is permanently gone, let ownership expire, then run
   `agentdbctl tablets rebalance --dry-run` and `agentdbctl tablets rebalance`.
4. If quorum is lost, stop automatic movement and recover the missing node or
   restore from the most recent verified backup.

### Lagging Or Catch-Up-Required Replicas

Signal:

- `raft_summary.catch_up_required_replica_count > 0`
- `raft_summary.lagging_replica_count > 0`
- repeated catch-up skipped counts in smoke or scheduler output

Diagnose:

1. Run `agentdbctl raft status` and identify the tablet/node pairs.
2. Check snapshot age and expiry with `agentdbctl raft snapshots`.
3. Inspect the target node's `agentdbctl status` for storage or compaction
   pressure.

Remediate:

1. Run `agentdbctl raft catch-up --dry-run`.
2. Run `agentdbctl raft catch-up` if the dry run shows transferable snapshots.
3. If snapshots are expired, force a fresh source snapshot by writing through
   the current owner or restarting the source only after preserving quorum.

### Stale Routing Or Owner Metadata

Signal:

- writes route to dead nodes
- `agentdbctl nodes list` shows stale/dead owners
- `agentdbctl tablets list` shows an owner that is not fresh and alive

Diagnose:

1. Compare `agentdbctl nodes list` with `agentdbctl tablets list`.
2. Run `agentdbctl raw get /api/admin/routing/decision` for one affected
   tenant/session when a concrete request is failing.
3. Check the scheduler leader in `agentdbctl status`.

Remediate:

1. Wait for heartbeat expiry if the failure is fresh.
2. Run `agentdbctl tablets rebalance --dry-run`.
3. Run `agentdbctl tablets rebalance` only after confirming the target nodes
   are in the same cell and have compatible capacity class.
4. Use explicit `tablets relinquish`, `prepare-replacement`, or
   `complete-replacement` only when the operator accepts the fencing impact.

### Tablet-Store Memory Or Compaction Pressure

Signal:

- open domain count near `max_open_domains`
- `active_memtable_bytes` or `all_memtables_bytes` keeps rising
- `pending_compaction_bytes` keeps rising
- `background_errors > 0`

Diagnose:

1. Run `agentdbctl status` against each node.
2. Check the tablet-store section for layout, `tablets_per_domain`,
   `max_open_domains`, memtable bytes, level-0 files, pending compaction bytes,
   and background errors.
3. Compare against the mutable memtable budget:

```text
max_open_domains * domain_write_buffer_size_mib * domain_max_write_buffers
```

Remediate:

1. Trigger local compaction for heavy churn:
   `agentdbctl tablet-store compact --tablet-id <id>`.
2. Increase `tablets_per_domain` before increasing `max_open_domains` when the
   issue is too many RocksDB domains.
3. Increase `domain_write_buffer_size_mib` or `domain_max_background_jobs` only
   after checking instance memory and IO headroom.
4. Avoid `tablets_per_domain = 1` outside explicit checkpoint/restore tests.

### Checkpoint Or Snapshot Failures

Signal:

- backup manifest lacks expected tablet checkpoints
- snapshot rows age out before catch-up
- restore validation reports checksum or size mismatch

Diagnose:

1. Run `agentdbctl raft snapshots`.
2. Run `agentdbctl backup create --include-tablet-checkpoints` only on nodes
   configured with `grouped_rocksdb` and `tablets_per_domain = 1`.
3. Validate manifests with `agentdbctl restore validate <manifest-path>`.

Remediate:

1. Re-run checkpoint creation on the owning node.
2. Keep nodes stopped while using
   `agentdbctl restore install-tablet-checkpoint`.
3. Do not route traffic to a restored tablet until Raft tail replay and applied
   index verification have completed.

## Non-Page Alerts

- movement backlog: sustained pending rows in `agentdbctl tablets movements`
- scheduler inactivity: stale `last_tick_at_ms` in `agentdbctl status`
- metadata degradation: repeated Postgres errors in server logs
- object-store degradation: repeated snapshot/content object-store errors
- domain sizing drift: tablet-store config differs across nodes in one cell

These should create tickets during evaluation. Promote them to pages only after
the cluster deployment has a measured baseline and clear SLOs.
