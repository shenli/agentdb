# Backup Verification

This runbook covers the supported single-node backup verification story. It is
about proving that backup artifacts are present, internally consistent, and
usable for an offline recovery procedure. It does not claim online destructive
restore apply.

For the general single-node operations entry point, see
[operations.md](operations.md). For the CLI surface, see
[agentdbctl.md](agentdbctl.md). For the raw admin API contract, see
[../reference/api-sdk.md](../reference/api-sdk.md).

## Scope

This runbook applies to one AgentDB server node using:

- a config file
- local metadata or configured metadata storage
- local RocksDB runtime storage with local or object-store content backends
- optional `grouped_rocksdb` tablet-store checkpoints when the layout is
  compatible

It does not cover:

- live online restore apply
- cluster-wide cutover after restore
- replacing a failed cluster member
- object-store bucket policy backup outside AgentDB
- application-level disaster recovery objectives

## Backup Artifacts

`agentdbctl backup create` writes a manifest and, when metadata is configured,
a metadata snapshot.

```bash
agentdbctl backup create \
  --backup-id nightly-2026-05-05 \
  --directory /var/backups/agentdb
```

The expected manifest path is:

```text
/var/backups/agentdb/nightly-2026-05-05/manifest.json
```

The manifest is the verification index. It records what AgentDB attempted to
capture, including metadata snapshot details, runtime summary data, and optional
tablet-store checkpoint entries.

Runtime summary data is observability metadata. It is not a complete byte copy
of runtime KV or content data. For local RocksDB/content deployments, pair the
manifest with a stopped-node copy or crash-consistent filesystem snapshot of
the data directory. For object-store-backed deployments, preserve bucket
policy, lifecycle rules, credentials, and prefix ownership outside AgentDB.

## Basic Verification

After every backup, validate the manifest:

```bash
agentdbctl restore validate \
  /var/backups/agentdb/nightly-2026-05-05/manifest.json
```

Validation checks that referenced files exist and that supported metadata
snapshot and tablet checkpoint records still match their recorded size and
checksum.

Use JSON output for automation:

```bash
agentdbctl -o json restore validate \
  /var/backups/agentdb/nightly-2026-05-05/manifest.json
```

A successful validation proves only that the backup set is internally
consistent at the time of validation. It does not prove that an operator has
captured every external dependency, such as object-store configuration or the
deployment config file.

## Local Data Directory Backup

For local RocksDB and filesystem content, the safest full byte backup is a
stopped-node copy:

```bash
systemctl stop agentdb
rsync -a /var/lib/agentdb/ /var/backups/agentdb/data-nightly-2026-05-05/
systemctl start agentdb
```

If stopping the node is not acceptable, use a crash-consistent filesystem or
volume snapshot. Do not copy active RocksDB files with a normal recursive file
copy and treat the result as verified.

Always back up the config that defines where runtime metadata and content bytes
live:

```bash
cp /etc/agentdb/agentdb.toml /var/backups/agentdb/nightly-2026-05-05/
```

## Tablet Checkpoint Verification

Physical tablet-store checkpoints are optional and layout-dependent. They are
supported only when `storage.tablet.layout = "grouped_rocksdb"` and
`tablets_per_domain = 1`, so each checkpoint contains exactly one tablet
domain.

Create a backup with local tablet checkpoints:

```bash
agentdbctl backup create \
  --backup-id tablet-copy-2026-05-05 \
  --directory /var/backups/agentdb \
  --include-tablet-checkpoints
```

Validate it:

```bash
agentdbctl restore validate \
  /var/backups/agentdb/tablet-copy-2026-05-05/manifest.json
```

Validation recomputes checkpoint size and checksum from disk. A mismatch means
the checkpoint artifact is not trustworthy; do not use it for recovery.

## Offline Install Drill

Use an isolated scratch directory to prove that at least one checkpoint can be
installed without overwriting live data:

```bash
mkdir -p /tmp/agentdb-restore-drill/tablet-stores

agentdbctl restore install-tablet-checkpoint \
  --tablet-id 7 \
  --checkpoint-dir /var/backups/agentdb/tablet-copy-2026-05-05/tablet-stores/tablet-00000000000000000007 \
  --tablet-store-root /tmp/agentdb-restore-drill/tablet-stores
```

This is an offline local filesystem operation. It refuses non-empty target
tablet domains. Run it against scratch storage unless you are executing a
planned recovery with the target node stopped.

## Verification Cadence

Recommended minimum cadence for a maintained single node:

- after every scheduled backup: `agentdbctl restore validate <manifest>`
- weekly: stopped-node copy or crash-consistent snapshot verification for local
  data directories
- weekly or after layout changes: one scratch `install-tablet-checkpoint` drill
  when tablet checkpoints are enabled
- before every upgrade: fresh backup, manifest validation, and config copy
- after every upgrade: `agentdbctl status` plus validation of the most recent
  backup manifest

Record the backup ID, manifest path, validation time, command output, AgentDB
version, and storage layout in the operator log.

## Failure Handling

If `restore validate` fails:

1. Treat that backup as unavailable.
2. Preserve the failed manifest and command output for debugging.
3. Create a new backup into a new backup ID.
4. Validate the new backup before deleting older verified backups.
5. Check disk fullness, permissions, object-store reachability, and whether
   any referenced checkpoint path was moved or pruned.

If tablet checkpoint validation reports a checksum or size mismatch:

1. Do not install that checkpoint.
2. Re-run `backup create --include-tablet-checkpoints` on the node that owns
   the local tablet store.
3. Validate the new manifest.
4. Keep the older failed checkpoint quarantined until the root cause is known.

If the backup directory is full:

1. Stop creating new backups into that directory.
2. Move or delete only backups that have a newer verified replacement.
3. Confirm free space before retrying backup creation.
4. Consider moving backup output to storage that is not colocated with the
   runtime data directory.

## Recovery Boundary

AgentDB currently supports:

- online backup manifest creation
- metadata snapshot creation when metadata is configured
- runtime summary recording
- optional physical tablet-store checkpoints for compatible layouts
- manifest and checkpoint validation
- offline checkpoint install into an empty target tablet-store root

AgentDB does not currently support:

- online destructive restore apply through the admin API
- live cluster-wide restore cutover
- automatic reconstruction of object-store policies, credentials, or lifecycle
  configuration
- recovery from a normal recursive copy of active RocksDB files

When doing a real recovery, stop the target node before installing local
runtime data, restore the config and external storage dependencies first, and
only then start AgentDB and verify `agentdbctl status`.
