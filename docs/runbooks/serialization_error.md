# `serialization_error`

## Symptom

Client receives a JSON body with `code: "serialization_error"` and
HTTP 500:

```json
{
  "code": "serialization_error",
  "type": "api_error",
  "message": "serde error: ...",
  "doc_url": "https://github.com/shenli/agentdb/blob/main/docs/runbooks/serialization_error.md"
}
```

Rare in a healthy cluster. The server got a blob out of KV or object
storage and couldn't decode it. This is a signal of either data-at-
rest corruption or a cross-version schema mismatch, never a normal
user-facing error.

## Diagnose

The serde message carries the field path where decoding failed.
Match it against the two classes of cause:

**Corruption (disk bitrot, ECC-less hardware, aborted write that
wrote partial bytes):**

- The serde message usually says `expected value at line 1 column N`
  or `unexpected end of input` — the blob got truncated.
- One event or segment is affected; other requests on the same
  tenant succeed.
- Running `strings` against the underlying segment file shows it's
  shorter than expected or has NUL runs in the middle.

**Cross-version schema mismatch:**

- Two processes at different AgentDB versions are sharing the same
  KV and one of them wrote a record the other can't decode. The
  serde message mentions a specific enum variant or field the
  reader doesn't know about (e.g. `unknown variant: Archive`).
- Restarting all processes to the same version is the
  remediation — not obvious from the message alone.

## Verify

- For suspected corruption: try to deserialize the same key via a
  separate process (e.g. a small script that opens the RocksDB with
  `rocksdb::DB::open_for_read_only` and feeds the raw bytes to
  `serde_json::from_slice::<Event>`). If that also fails, the blob
  is corrupted. If it succeeds, the server-side code path is the
  bug — escalate with the request id.
- For suspected version mismatch: check `agentdb-server --version`
  (once the CLI lands) or the binary hash across the fleet. At
  least one is different.

## Remediate

1. **Corruption, single record.** If the record is an event that
   isn't cross-referenced (most user events aren't), delete the
   key from KV and rebuild the session's index from object-store
   segments. Lossy but recoverable.
2. **Corruption, segment-level.** Fetch the last good segment from
   object storage; truncate the KV keys past that segment's
   HLC cutoff and let the replay path re-derive. This is
   semi-destructive; take a backup first.
3. **Schema mismatch.** Upgrade (or downgrade) the stragglers to
   the same AgentDB version as the majority. Serde errors clear
   immediately on restart with the right binary.
4. **Unknown enum variant at read time.** This usually means a new
   release added a variant and a record with that variant landed
   before the reader was upgraded. Fastest fix: bounce the reader
   to the same version.

## Prevent

- Run AgentDB binaries against a filesystem with checksumming (ZFS,
  btrfs) or at minimum ECC-backed RAM. Serde errors from disk
  bitrot are common enough on commodity hardware to take this
  seriously.
- Keep your deployment's rolling-upgrade window short. The longer
  you have mixed versions, the more likely a new-variant record
  lands on an old reader.
- When a schema grows a new variant/field, bump
  `SEGMENT_SCHEMA_VERSION` in `agentdb-core::segment` (or the
  equivalent index snapshot version). Readers see the version
  mismatch and can fail gracefully rather than with a cryptic
  serde error.
