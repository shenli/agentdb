# Restore And Recovery

This runbook covers the supported single-node recovery procedure. AgentDB can
create and validate backup manifests online, but recovery that replaces local
runtime data is an offline operator action.

For backup validation before an incident, see
[backup-verification.md](backup-verification.md).

## Recovery Boundary

Supported today:

- validate a backup manifest with `agentdbctl restore validate`
- restore deployment config from a known-good copy
- restore local data directories from a stopped-node copy or
  crash-consistent volume snapshot
- install a compatible tablet checkpoint into an empty local tablet-store root
- restart one AgentDB node and verify health

Not supported today:

- online destructive restore apply through the admin API
- live restore into a serving process
- cluster-wide restore cutover
- automatic recreation of object-store bucket policy, credentials, or lifecycle
  rules
- recovery from a normal recursive copy of active RocksDB files

## Before Recovery

1. Stop the AgentDB process.
2. Preserve the current failed data directory for later debugging if space
   allows.
3. Pick the most recent backup that has passed validation.
4. Confirm you have the matching `agentdb.toml`.
5. Confirm external dependencies are available:
   - metadata database, if configured
   - object-store buckets and prefixes, if configured
   - credentials or IAM role used by the server

Example:

```bash
systemctl stop agentdb

agentdbctl restore validate \
  /var/backups/agentdb/nightly-2026-05-05/manifest.json

cp -a /var/lib/agentdb /var/lib/agentdb.failed.$(date +%Y%m%d%H%M%S)
```

## Restore Local Data

Restore the data directory from a stopped-node copy or crash-consistent volume
snapshot:

```bash
agentdbctl restore apply-local \
  --manifest-path /var/backups/agentdb/nightly-2026-05-05/manifest.json \
  --data-backup-dir /var/backups/agentdb/data-nightly-2026-05-05 \
  --target-data-dir /var/lib/agentdb \
  --config-backup /var/backups/agentdb/nightly-2026-05-05/agentdb.toml \
  --target-config /etc/agentdb/agentdb.toml \
  --allow-overwrite
```

`apply-local` is an offline filesystem operation. Stop AgentDB before using it.
It refuses to overwrite an existing target unless `--allow-overwrite` is set,
and it refuses symlinks in the source snapshot. If you do the copy manually,
use a stopped-node copy command such as `rsync -a --delete` and restore the
matching config file.

Then validate config before starting:

```bash
agentdb doctor --config /etc/agentdb/agentdb.toml
```

## Install A Tablet Checkpoint

Use this only for compatible `grouped_rocksdb` layouts with
`tablets_per_domain = 1`.

The target tablet domain must be empty. Stop the target node first.

```bash
agentdbctl restore install-tablet-checkpoint \
  --tablet-id 7 \
  --checkpoint-dir /var/backups/agentdb/tablet-copy-2026-05-05/tablet-stores/tablet-00000000000000000007 \
  --tablet-store-root /var/lib/agentdb/tablet-stores
```

If the command refuses to overwrite a target domain, stop and inspect the
target. Do not delete live tablet data unless this is a planned recovery and
you have preserved a copy of the failed state.

## Restart And Verify

Start AgentDB with the restored config:

```bash
systemctl start agentdb
```

Verify:

```bash
agentdb doctor --config /etc/agentdb/agentdb.toml
agentdbctl status
curl -fsS http://127.0.0.1:7878/health/ready
```

If the dashboard is enabled, open:

```text
http://127.0.0.1:7878/dashboard/
```

For application validation, replay or fetch a known recent session and confirm
the expected last event sequence.

## If Recovery Fails

- If `agentdb doctor` fails, fix config, filesystem permissions, or external
  dependency configuration before starting the node.
- If `agentdbctl status` reports storage errors, check disk fullness,
  permissions, and RocksDB logs under the restored data directory.
- If restore validation fails, do not use that backup. Pick an older verified
  backup or create a new backup if the source node is still healthy.
- If object-store-backed content is missing, restore bucket policy, lifecycle
  rules, credentials, and prefix ownership outside AgentDB before retrying.
