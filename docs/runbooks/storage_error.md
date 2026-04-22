# `storage_error`

## Symptom

Client receives a JSON body with `code: "storage_error"` and HTTP 500:

```json
{
  "code": "storage_error",
  "type": "api_error",
  "message": "storage error: io error: ...",
  "doc_url": "https://github.com/shenli/agentdb/blob/main/docs/runbooks/storage_error.md"
}
```

This is the server saying "the KV or object-store backend failed
under me." It's a 500 rather than a 503 because from the client's
perspective the request is done — there's nothing useful to retry
without operator intervention.

## Diagnose

The message string is the only differentiator between the two very
different root causes. Check the server log for the full backtrace.

**Local KV failure (RocksDB / SlateDB):**

- Look for `RocksDB error`, `Corruption:`, or `IO error: <path>` in
  the log.
- Check free disk on the node:

  ```
  df -h $(dirname $AGENTDB_DATA)
  ```

  A full disk is the single most common cause of RocksDB 500s. The
  DB stops accepting writes with a generic IO error.

- Check fs permissions on `AGENTDB_DATA`. If this directory got
  created by a different user (e.g. root during first boot, then
  the server started as a less-privileged user), reads and
  compactions start failing.

**Object-store failure (local FS / S3):**

- Look for `object_store` or `S3Error` in the log.
- If `AGENTDB_OBJECT_STORE=s3`, check:
  - `AGENTDB_S3_BUCKET` exists and the node's creds can read+write
    it.
  - `AGENTDB_S3_REGION` matches the bucket's region (mismatched
    region errors surface here as 500s, not 400s).
  - If using MinIO or another S3-compatible endpoint, that
    `AGENTDB_S3_ENDPOINT` is reachable from this node.

## Verify

- Re-run the server's object-store liveness probe by restarting. The
  binary runs a `list` under `agentdb/` on startup and fails fast
  if the probe itself errors. If startup succeeds but requests still
  500, the store is degraded in a way the probe doesn't catch —
  usually quota/rate-limit.
- For KV: `du -sh $AGENTDB_DATA` and `ls -la $AGENTDB_DATA/*.log`
  tell you whether the DB is growing or stuck. A stuck DB usually
  means a background compaction failed; check the RocksDB error log
  (`LOG` file in the data dir).

## Remediate

1. **Disk full.** Delete old local backups, grow the volume, or
   flush cold segments to object storage (lifecycle engine should
   do this automatically if enabled). The server resumes accepting
   writes once disk space comes back.
2. **S3 creds / region / bucket mismatch.** Fix the env vars. These
   are read only at startup, so the process needs to restart after
   the fix.
3. **Corruption.** Stop the server, snapshot `AGENTDB_DATA` offsite,
   then let the server rebuild from the last good segment in object
   storage — the recovery path loads the latest snapshot and
   replays the WAL. Don't try to repair RocksDB in place unless
   you have a backup.
4. **Temporary S3 outage.** Retry at the client side with backoff;
   the server itself doesn't buffer writes through an S3 outage
   beyond the WAL rotation window. Longer outages surface as
   `storage_error` until the outage ends.

## Prevent

- Alarm on free disk at 15% remaining. By the time you see 500s,
  you're already past the recoverable point on smaller nodes.
- Run the object store liveness probe against a canary request in
  a health-check job. The probe the server runs at startup is a
  floor, not a ceiling — steady-state degradation won't be caught
  until the next restart unless an external check notices first.
- Separate the RocksDB WAL from the main data directory if your
  filesystem offers tiered storage. A full WAL volume is easier to
  recover from than a full main-data volume.
