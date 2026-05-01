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

This is the server saying "the local KV or artifact-content backend
failed under me." It's a 500 rather than a 503 because from the
client's perspective the request is done — there's nothing useful to
retry without operator intervention.

## Diagnose

The message string is the only differentiator between the two very
different root causes. Check the server log for the full backtrace.

**Local KV failure (RocksDB):**

- Look for `RocksDB error`, `Corruption:`, or `IO error: <path>` in
  the log.
- Check free disk on the node:

  ```
  df -h $(dirname ${AGENTDB_DATA_DIR:-./data})
  ```

  A full disk is the single most common cause of RocksDB 500s. The
  DB stops accepting writes with a generic IO error.

- Check fs permissions on `AGENTDB_DATA_DIR`. If this directory got
  created by a different user (e.g. root during first boot, then
  the server started as a less-privileged user), reads and
  compactions start failing.

**Artifact content failure (local filesystem):**

- Look for local filesystem errors under
  `AGENTDB_CONTENT_DIR` (or `<data-dir>/content` when unset).
- Check free disk and permissions for the artifact content directory.

## Verify

- For KV: `du -sh ${AGENTDB_DATA_DIR:-./data}` and inspect the
  RocksDB `LOG` file under the runtime subdirectory. These checks
  tell you whether the DB is growing or stuck. A stuck DB usually
  means a background compaction failed; check the RocksDB error log
  (`LOG` file in the data dir).

## Remediate

1. **Disk full.** Delete old local backups, grow the volume, or
   move the data/content directories to a larger disk. The server resumes
   accepting writes once disk space comes back.
2. **Permission mismatch.** Stop the server, fix ownership/permissions on
   `AGENTDB_DATA_DIR` and `AGENTDB_CONTENT_DIR`, then restart.
3. **Corruption.** Stop the server and snapshot the data/content directories
   before attempting recovery. Don't try to repair RocksDB in place unless
   you have a backup.

## Prevent

- Alarm on free disk at 15% remaining. By the time you see 500s,
  you're already past the recoverable point on smaller nodes.
- Separate the RocksDB WAL from the main data directory if your
  filesystem offers tiered storage. A full WAL volume is easier to
  recover from than a full main-data volume.
