# Recovery: Object Storage as the Recovery Root

AgentDB's durability contract has two layers. Replication (the local WAL in
single-node mode; Raft quorum in cluster mode) owns the acknowledged write.
The object store owns survivability: enough state is continuously uploaded
that a node can be rebuilt from the bucket alone, with a bounded, measured
lag. Object storage is never on the performance-critical path.

## Recovery ladder

| Failure | Recovery source | RPO |
|---|---|---|
| One node (cluster) | Raft catch-up / peer snapshot | zero while quorum survives |
| Disk or host loss (single node), quorum loss (cluster) | recovery manifest + object-store content | bounded by `recovery_lag_ms` |

## Recovery manifests

When `[storage.object_store]` is configured and the server runs persistently,
a background worker periodically exports a point-in-time, full-fidelity dump
of the runtime KV state and uploads it to:

```text
{object store}/agentdb/recovery/manifest-latest.jsonl        stable pointer
{object store}/agentdb/recovery/history/manifest-<ts>.jsonl  history copies
```

The manifest is self-describing: the first line is a header carrying the
format version, creation time, node id, entry count, and payload size, so a
reader with only the bucket can identify what it holds. The format is a
versioned durable contract; imports refuse unknown versions and refuse to
write into a non-empty store.

Configuration (defaults shown; the section is optional):

```toml
[recovery]
enabled = true                        # default: on when object_store is set
manifest_backup_interval_ms = 300000  # 5 minutes
manifest_max_bytes = 1073741824       # fail closed past 1 GiB serialized
```

`recovery.enabled = true` without `[storage.object_store]` is a startup
error, not a silent no-op.

## Monitoring: `GET /api/admin/recovery`

```json
{
  "enabled": true,
  "manifest_backup": {
    "destination": "agentdb/recovery/manifest-latest.jsonl",
    "interval_ms": 300000,
    "last_success_ms": 1756500000000,
    "last_error": null,
    "last_entry_count": 18234,
    "last_payload_bytes": 52428800,
    "consecutive_failures": 0
  },
  "content_mirror": { "enabled": true, "queued": 10, "completed": 10, "failed": 0, "backlog": 0 },
  "recovery_lag_ms": 41000
}
```

In mirror mode the local content copy is a **managed cache**
(`storage.content.local_cache_ttl_ms`, default 24h): local objects unread
past the TTL whose object-store copy exists are evicted, so cold content —
sealed segments, archives, idle artifacts — lives only in the object store.
A cold read pulls through the mirror, re-caches locally, and the object goes
cold again after another TTL. `evicted` and `local_cache_ttl_ms` appear in
the `content_mirror` block above.

`recovery_lag_ms` is the headline number: the age of the newest manifest in
the bucket — what would be lost if every local disk disappeared right now
and the bucket were the only survivor. Alert on it the way you would alert
on replication lag. `content_mirror.backlog` covers the artifact-bytes side
of the same question.

## Restoring from the bucket

```bash
# The runtime directory must be empty or absent; restore never merges.
agentdb restore-from-bucket --config agentdb.toml
agentdb serve --config agentdb.toml
```

The restore prints the manifest header (age, node id, entry count) before
importing, so the operator sees the RPO they are accepting. After restart,
artifact bytes are fetched from the object-store mirror on first read and
backfilled locally — no separate content restore step.

Sealed session segments and archive bundles are already in the content
backend and referenced by the restored metadata; they carry their tenant,
session, and sequence-range identity inside the blob.

## Invariants

- **Upload → verify → manifest → only then GC.** Local state may only be
  garbage-collected after its object-store copy is verified. Anything
  deleted locally is therefore provably in the bucket. The content cache
  sweep enforces this mechanically: it never evicts a local object whose
  mirror copy is absent.
- **Restore never merges.** Import into a non-empty runtime directory is
  refused; recovery is always into a fresh directory.
- **The manifest format is a compatibility contract.** Version bumps require
  a reader for the previous version.
- **KMS is part of the recovery root.** Envelope-encrypted content requires
  the key hierarchy; crypto-shredded data is unrecoverable by design.

## Scope notes

- Cluster mode additionally uploads per-tablet Raft snapshots under
  `agentdb/raft-snapshots/` with retention, prune, and a restore worker; see
  `GET /api/admin/raft/snapshots`. Recovery manifests complement, not
  replace, that path.
- Manifest-less scavenge (rebuilding metadata purely from listed objects)
  is not yet implemented; the manifest is the recovery entry point. The
  content backend supports object listing for orphan reconciliation.
- Run restore drills: a recovery path that is never exercised should be
  treated as broken. `agentdb restore-from-bucket` into a scratch directory
  against the production bucket is a safe, non-destructive drill.
