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

- [ ] `/health/live` is used only for process liveness and `/health/ready` is
  wired to load balancers/traffic gates.
- [ ] `agentdbctl status` returns healthy output.
- [ ] A random static API bearer token of at least 16 visible ASCII characters
  with no whitespace is injected outside the repository for every non-loopback
  bind.
- [ ] TLS terminates before bearer-authenticated network traffic.
- [ ] Token rotation has a bounded previous-token removal procedure.
- [ ] The dashboard is reachable only from an approved operator network or
  protected by deployment-level access controls; operators know dashboard
  tokens are page-memory-only and disappear on reload.
- [ ] Admin tokens and `<data_dir>/auth-meta.sqlite` have an ownership,
  revocation, and backup procedure.

## Storage And Capacity

- [ ] Runtime data, content, and backup volumes have at least 15% free space
  alerting.
- [ ] Backup output is on a different volume or external storage when possible.
- [ ] `agentdbctl status` has been checked for tablet-store layout, open
  domains, memtable budget, pending compaction bytes, and background errors.
- [ ] `tablets_per_domain = 1` is used only when physical tablet checkpoints or
  high-isolation restore tests are required.
- [ ] Restored production metadata fits under the 512 MiB aggregate startup
  hydration default and the documented `AGENTDB_STARTUP_MAX_*` row caps, or
  higher values have been RSS/startup-time tested on a copy.
- [ ] Local-filesystem startup reconciliation logs zero unexpected orphan
  delete errors and reports any crash-left temporary files it removed. The
  content inventory fits under `AGENTDB_STARTUP_MAX_CONTENT_OBJECTS` (500,000
  by default). Object-store deployments have a separate inventory/lifecycle
  check because object listing reconciliation is not implemented there.
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

## Tenant Envelope Encryption (optional)

Tenant payloads (session events, artifact content and metadata) can be sealed
at rest under per-tenant keys. This is **opt-in** and currently supported for
the **single-node persistent** layout only (the in-memory and replicated
layouts fail closed if it is requested).

- [ ] To enable, set on the server:

  ```bash
  AGENTDB_ENCRYPTION_MODE=envelope
  AGENTDB_ENCRYPTION_KEK_SECRET=<high-entropy secret>   # the key-encryption key
  AGENTDB_ENCRYPTION_KEK_REF=local:v1                   # optional label (default)
  AGENTDB_ENCRYPTION_SUITE=xchacha20-poly1305           # or aes-256-gcm (default xchacha)
  ```

- [ ] **The KEK secret is critical.** It wraps every per-tenant data key; losing
  it makes all encrypted data permanently unreadable (that is also the
  crypto-shred property). Store it in a secrets manager, back it up
  independently of the data volume, and never log it.
- [ ] Enabling on an existing deployment encrypts **new** writes; previously
  written plaintext stays readable (mixed reader). There is no in-place bulk
  migration yet.
- [ ] Wrapped DEKs live in the runtime store under a reserved control-plane
  prefix; they are included in a normal backup and are useless without the KEK.
- [ ] Startup **fails closed** if `envelope` is set without a KEK secret, with
  the in-memory runtime, or with the replicated tablet layout. See
  [../design/encryption-at-rest.md](../design/encryption-at-rest.md).

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
