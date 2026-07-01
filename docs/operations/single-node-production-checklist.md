# Single-Node Production Checklist

Use this checklist before exposing a persistent single AgentDB node to real
traffic. It is a gate over the supported one-node deployment shape, not a
cluster-readiness checklist.

For the main operations entry point, see [operations.md](operations.md). For
deployment choices, see [deployment.md](deployment.md).

## Supported Shape

- [ ] AgentDB runs as one server process.
- [ ] The node uses one checked-in or managed `agentdb.toml`.
- [ ] The deployment uses a supported package path: binary, Docker, Compose, or
  single-replica Helm evaluation.
- [ ] Operators understand that multi-node cluster operation is not yet the
  default production shape.

## Configuration

- [ ] `agentdb doctor --config <path>` passes.
- [ ] `agentdb serve --config <path>` is used instead of ad hoc flag lists.
- [ ] `storage.data_dir` is on durable storage.
- [ ] `storage.content_dir`, if separate, is on durable storage.
- [ ] Object-store-backed deployments have documented bucket, prefix, IAM,
  lifecycle, and encryption ownership.
- [ ] The config file is backed up with every pre-upgrade and recovery backup.

## Health And Access

- [ ] `/health/ready` is wired to load balancer or uptime checks.
- [ ] `agentdbctl status` returns healthy output.
- [ ] The dashboard is reachable only from an approved operator network or
  protected by deployment-level access controls.
- [ ] Admin tokens or auth configuration are stored outside the repository.
- [ ] The MongoDB compatibility listener, if enabled, is bound to localhost or
  protected by separate access control and TLS.

## Storage And Capacity

- [ ] Runtime data, content, and backup volumes have at least 15% free space
  alerting.
- [ ] Backup output is on a different volume or external storage when possible.
- [ ] `agentdbctl status` has been checked for tablet-store layout, open
  domains, memtable budget, pending compaction bytes, and background errors.
- [ ] `tablets_per_domain = 1` is used only when physical tablet checkpoints or
  high-isolation restore tests are required.
- [ ] Manual compaction procedures are understood:
  [storage-maintenance.md](storage-maintenance.md).

## Backup And Recovery

- [ ] A scheduled backup command is documented.
- [ ] The latest backup manifest validates:

  ```bash
  agentdbctl restore validate /var/backups/agentdb/<backup-id>/manifest.json
  ```

- [ ] Local RocksDB/filesystem content deployments have a stopped-node copy or
  crash-consistent volume snapshot procedure.
- [ ] Restore drill has been run into scratch storage.
- [ ] Tablet checkpoint install drill has been run if tablet checkpoints are
  part of the recovery plan.
- [ ] Operators know that online destructive restore apply is not supported.
- [ ] Recovery procedure is documented:
  [restore-recovery.md](restore-recovery.md).

## Upgrade And Rollback

- [ ] Current version is recorded before upgrade:

  ```bash
  agentdb --version
  ```

- [ ] Pre-upgrade backup and config copy are created and validated.
- [ ] Upgrade procedure is documented:
  [single-node-upgrade.md](single-node-upgrade.md).
- [ ] Rollback means restoring the previous binary/image and the pre-upgrade
  storage snapshot.
- [ ] Offline local data/config rollback uses the restore helper:

  ```bash
  agentdbctl restore apply-local \
    --manifest-path /var/backups/agentdb/pre-upgrade/manifest.json \
    --data-backup-dir /var/backups/agentdb/pre-upgrade/data \
    --target-data-dir /var/lib/agentdb \
    --config-backup /var/backups/agentdb/pre-upgrade/agentdb.toml \
    --target-config /etc/agentdb/agentdb.toml \
    --allow-overwrite
  ```

- [ ] ClickHouse export schema rollback is understood if observability export
  is enabled:

  ```bash
  agentdbctl export clickhouse rollback
  agentdbctl -o json export clickhouse rollback | jq
  ```

- [ ] Operators do not run an older binary against data migrated by a newer
  binary unless release notes explicitly allow it.

## CDC And Export

- [ ] Export capability has been checked for each production tenant.
- [ ] ClickHouse, JSONL, or Parquet export path is documented if used.
- [ ] Managed consumers have stable names and lease owners.
- [ ] Export lag and pending item checks are part of routine operations.
- [ ] Privacy controls are configured before data leaves AgentDB when required.
- [ ] Recovery procedure is documented:
  [cdc-export.md](cdc-export.md).

## Tiered Storage And Lifecycle

- [ ] Lifecycle policy is documented for warm, cold, archive, and
  derivative-only behavior.
- [ ] A test session has been transitioned and replay-verified.
- [ ] Archive restore has been tested if archive is part of the retention plan.
- [ ] Artifact byte purge policy has legal hold and retention review.
- [ ] Artifact lifecycle verification passes:

  ```bash
  TENANT_ID=ten_...
  agentdbctl artifacts verify-lifecycle --tenant-id "$TENANT_ID"
  ```

- [ ] Object-store policy and encryption dependencies are documented.
- [ ] Runbook is documented:
  [tiered-storage.md](tiered-storage.md).

## Routine Operations

- [ ] Daily readiness and status checks are assigned to an owner.
- [ ] Weekly backup validation is assigned to an owner.
- [ ] Monthly restore drill or pre-change restore drill is assigned to an
  owner.
- [ ] Disk-full recovery procedure is known:
  [disk-full-recovery.md](disk-full-recovery.md).
- [ ] Routine maintenance checklist is known:
  [routine-maintenance.md](routine-maintenance.md).
- [ ] API error runbooks are linked from operator notes:
  [../runbooks/README.md](../runbooks/README.md).

## Launch Gate

Do not send production traffic until all of these are true:

- [ ] health checks pass
- [ ] backup validates
- [ ] restore drill has succeeded
- [ ] free disk alerts exist
- [ ] upgrade rollback path is documented
- [ ] export/lifecycle policies are either configured and tested or explicitly
  out of scope
- [ ] the operator knows which workflows remain documented procedures rather
  than automated product features
