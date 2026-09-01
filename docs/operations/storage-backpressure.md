# Storage Backpressure

AgentDB stages artifact content on node-local storage and mirrors it to the
object store asynchronously, so hot writes never block on S3. If the object
store drains slower than writes arrive, the **pending upload backlog** would
otherwise grow without bound and consume node memory. Backpressure keeps it
bounded and throttles the responsible tenant instead (hardening
[§5](../design/distributed-safety-hardening.md)).

## How it behaves

The async mirror tracks the pending upload backlog in **bytes** and
per-tenant. When the backlog is at or above its ceiling, new artifact
writes are refused **before** the local write happens (so no half-mirrored
state is left) and the client receives:

```
HTTP 429 Too Many Requests
Retry-After: 1
```

The write is safe to retry once the backlog drains. The refusal is charged
to the tenant producing the most upload debt, which is who a sustained
throttle slows first.

## Knobs

| Env var | Default | Meaning |
|---|---|---|
| `AGENTDB_CONTENT_MIRROR_MAX_BACKLOG_BYTES` | `536870912` (512 MiB) | Byte ceiling on the pending upload backlog. `0` disables backpressure (unbounded — not recommended in production). |
| `AGENTDB_CONTENT_MIRROR_QUEUE_CAPACITY` | `1024` | Max in-flight mirror jobs (count). A hard cap independent of the byte ceiling. |
| `AGENTDB_CONTENT_MIRROR_INJECT_LATENCY_MS` | `0` | **Test/measurement only.** Delays every object-store operation by this many ms, to demonstrate that the backlog stays bounded and writers are throttled under slow storage. Never set in production. |

## Observability

`GET /api/admin/content-mirror` (and the `content_mirror` block of
`GET /api/admin/recovery`) reports:

- `backlog` / `backlog_bytes` — pending mirror jobs and their bytes.
- `max_backlog_bytes` — the configured ceiling.
- `over_limit` — `true` while writes are being throttled.
- `top_tenant` / `top_tenant_backlog_bytes` — the tenant holding the most
  pending bytes (the one a throttle is attributed to).
- `retry_after_ms` — present only while throttled.
- `ms_since_last_drain` — how long since a mirror job last completed; a
  large value with a non-empty backlog means the object store has stopped
  draining (investigate the bucket/credentials/network).

## Running the measurement

To reproduce the "slow storage stays bounded" behavior on a real cluster,
start the nodes with injected latency and a low ceiling, drive artifact
writes, and watch `over_limit` flip while `backlog_bytes` stays near the
ceiling rather than climbing:

```
AGENTDB_CONTENT_MIRROR_INJECT_LATENCY_MS=500 \
AGENTDB_CONTENT_MIRROR_MAX_BACKLOG_BYTES=4194304 \
agentdb serve --config agentdb.toml
```

The property is pinned deterministically by two tests (no bucket needed):
`async_mirror_throttles_writer_when_slow_storage_fills_backlog`
(`crates/agentdb-storage/tests/content_backend.rs`, the storage layer) and
`artifact_writes_are_rate_limited_when_upload_backlog_saturates`
(`crates/agentdb-core/tests/artifact_runtime.rs`, the end-to-end
`create_artifact` → mirror → 429 path). Both inject 500ms mirror latency
and assert the backlog stays bounded near the ceiling while writes are
throttled.

## What is NOT throttled yet

Per §5's "measure first" guidance, only the object-store upload backlog —
the stage that becomes unbounded under slow storage — is wired into
admission today. Other pipeline-debt signals (memtable bytes, compaction
debt, record-index lag, retained Raft-log bytes, export-ledger lag) have
their own gauges under `GET /api/admin/*` but do not yet gate admission.
Extending admission to one of them is warranted once a workload
demonstrates it as the bottleneck.
