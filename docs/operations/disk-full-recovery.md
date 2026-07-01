# Disk-Full Recovery

This runbook covers the single-node response when AgentDB storage or backup
volumes run out of disk.

Disk fullness often surfaces as API `storage_error` responses, RocksDB IO
errors, failed backup creation, or failed compaction. For the API error shape,
see [../runbooks/storage_error.md](../runbooks/storage_error.md).

## Triage

Check all relevant volumes:

```bash
df -h /var/lib/agentdb
df -h /var/backups/agentdb
du -sh /var/lib/agentdb
du -sh /var/backups/agentdb
```

If `storage.content_dir` points outside `storage.data_dir`, check it
separately:

```bash
df -h /var/lib/agentdb/content
du -sh /var/lib/agentdb/content
```

Then check AgentDB status:

```bash
agentdbctl status
```

Look for storage errors, pending compaction bytes, background errors, and
failed backup or checkpoint output.

## Immediate Response

If the runtime data volume is full:

1. Stop new write traffic if possible.
2. Do not run manual compaction while the disk is critically full.
3. Move or delete local backup artifacts only when a newer verified replacement
   exists.
4. Grow the volume or attach a larger one.
5. Restart AgentDB only after the filesystem has free space and correct
   ownership.

If the backup volume is full:

1. Stop writing new backups to that directory.
2. Move old verified backups to external storage, or delete backups with newer
   verified replacements.
3. Re-run backup creation with a new backup ID.
4. Validate the new manifest.

If object-store content is failing because of quota or lifecycle policy:

1. Stop writes or reduce ingestion.
2. Resolve quota, credentials, policy, or prefix ownership outside AgentDB.
3. Retry the failed operation after object-store health is restored.

## Safe Cleanup Order

Prefer freeing space in this order:

1. old benchmark output or local smoke-test directories
2. old unneeded logs
3. backup artifacts that have a newer verified replacement
4. temporary restore-drill directories
5. application-generated artifacts that product policy allows deleting

Avoid deleting:

- active RocksDB files
- current content blobs
- current metadata database files
- the only validated backup
- object-store prefixes whose ownership is unclear

## Recovery Verification

After freeing space:

```bash
agentdb doctor --config /etc/agentdb/agentdb.toml
agentdbctl status
agentdbctl restore validate /var/backups/agentdb/<backup-id>/manifest.json
```

If AgentDB was returning `storage_error`, retry a small known-safe read first,
then a small write in a non-critical tenant or local test tenant.

## Prevention

- alert when free disk drops below 15%
- keep backup output off the runtime data volume when possible
- validate backups before deleting older backups
- set retention for benchmark and smoke-test artifacts
- record expected data growth after large imports or benchmark runs
- leave headroom for compaction, checkpoints, and restore drills
