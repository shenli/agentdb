# API and SDK Reference

This page is the compact reference for AgentDB's native HTTP API and local SDK
surfaces. The API is tenant-scoped and uses opaque typed IDs such as `ten_*`,
`agt_*`, `ses_*`, `evt_*`, `art_*`, and `rec_*`.

## HTTP API

Humans should usually use `agentdbctl` for operations. The HTTP API below is
the stable backend contract for SDKs, automation, and the CLI.

### Authentication

Set `server.api_auth_token` or `AGENTDB_API_AUTH_TOKEN` to protect every
external `/api/*` request with `Authorization: Bearer <token>`. The server
refuses a non-loopback HTTP bind unless this static installation token contains
at least 16 visible ASCII characters and no whitespace. This keeps the token
safe to place in an HTTP `Authorization` header. For a zero-downtime rotation,
configure the new value as the current token and temporarily set
`server.api_auth_previous_token` or
`AGENTDB_API_AUTH_PREVIOUS_TOKEN`; remove the previous value after all clients
have switched. The previous token follows the same character rule, must differ
from the current token, and cannot be configured by itself. With no static
token, a loopback listener leaves non-admin `/api/*` routes
unauthenticated. Health and dashboard asset routes remain public.

The metadata-backed admin-token API provides narrower credentials for
`/api/admin/*`: `admin` scope permits every method and `readonly` permits only
`GET`/`HEAD`. On a loopback server with no static token, admin routes are open
only while the metadata store has never minted an admin token. Minting the
first token closes that bootstrap path permanently; revoking every token does
not reopen it, so configure a static token before revoking the final admin
credential. The
plaintext is returned once and only its SHA-256 hash is stored. Persistent
standalone nodes use
`<data_dir>/auth-meta.sqlite`, dev mode uses memory, and routed cluster nodes
reuse the shared metadata store:

```text
GET    /api/admin/tokens
POST   /api/admin/tokens             {"description":"operator","scope":"admin"}
DELETE /api/admin/tokens/{token_id}
```

The static installation token is coarse-grained and also authorizes admin
routes; metadata admin tokens do not authorize tenant data routes. This is not
tenant-scoped end-user authorization. Terminate TLS at the ingress, load
balancer, or service mesh before sending bearer tokens over a network. Internal
Raft RPCs continue to use their separate current/previous token pair, with the
same length and visible-ASCII rules; an internal previous token cannot be
configured without a current token.

Both native SDKs accept either a fixed bearer token or a token callback:

```python
from agentdb import AgentDB

db = AgentDB("http://127.0.0.1:7878", token="...")
```

```typescript
import { AgentDB } from "agentdb";

const db = new AgentDB({ baseUrl: "http://127.0.0.1:7878", token: "..." });
```

Health and admin:

```text
GET  /health/live
GET  /health/ready
GET  /metrics
GET  /api/admin/tokens
POST /api/admin/tokens
DELETE /api/admin/tokens/{token_id}
GET  /api/admin/cluster/health[?stale_threshold_ms={ms}]
GET  /api/admin/cluster/readiness[?stale_threshold_ms={ms}]
GET  /api/admin/scheduler
GET  /api/admin/drain
POST /api/admin/drain
GET  /api/admin/cells
POST /api/admin/cells
GET  /api/admin/nodes[?stale_threshold_ms={ms}]
POST /api/admin/nodes
DELETE /api/admin/nodes/{node_id}
POST /api/admin/nodes/{node_id}/maintenance
GET  /api/admin/tablets
GET  /api/admin/tablet-stats[?tablet_id={tablet_id}]
POST /api/admin/tablets/create
POST /api/admin/tablets/rebalance
POST /api/admin/tablets/{tablet_id}/assign
POST /api/admin/tablets/{tablet_id}/relinquish
POST /api/admin/tablets/{tablet_id}/handoff
POST /api/admin/tablets/{tablet_id}/handoff/complete
POST /api/admin/tablets/{tablet_id}/handoff/cancel
GET  /api/admin/routing
GET  /api/admin/routing/decision?tenant_id={tenant_id}[&session_id={session_id}]
GET  /api/admin/indexes/records
POST /api/admin/backups
POST /api/admin/restores
POST /api/policy/worker/run
```

The cluster admin routes (`/api/admin/cluster/*`, `/scheduler`, `/drain`,
`/cells`, `/nodes`, `/tablets*`, `/routing*`) are mounted only when
`deployment.mode = "cluster"`; on a single-node server they return 404.

`/health/live` reports whether the HTTP process is alive. `/health/ready`
returns `503 Service Unavailable` while the local node is draining or cluster
routing state is unsafe; use readiness to remove a node from traffic and
liveness only to decide whether the process should be restarted. Neither
endpoint requires API authentication.

`/metrics` serves Prometheus text-format runtime metrics (also unauthenticated,
outside `/api/`): `agentdb_up`, `agentdb_build_info`, fleet gauges
(`agentdb_tenants`, `agentdb_sessions`, `agentdb_active_sessions`,
`agentdb_sessions_by_lifecycle{lifecycle=...}`), sealed/archived byte counts, and
the advisory disk budget (`agentdb_disk_budget_bytes` / `agentdb_disk_used_bytes`).
The fleet summary enumerates sessions and the disk gauge walks the data dir, so
keep the scrape interval modest on large single-node fleets.

Node maintenance body:

```json
POST /api/admin/nodes/{node_id}/maintenance
{ "state": "draining" }
```

Allowed states are `alive`, `draining`, and `maintenance`. Setting the local
node back to `alive` clears the local drain flag; setting it to `draining` or
`maintenance` refuses new local mutations.

Tablet admin request bodies:

```json
GET /api/admin/tablet-stats?tablet_id=7
{
  "stats": [
    {
      "tablet_id": 7,
      "node_id": "node-a",
      "key_count": 1000,
      "key_bytes": 64000,
      "value_bytes": 1048576,
      "storage_bytes": 1112576,
      "applied_index": 42,
      "sample_interval_ms": 1000,
      "write_ops_delta": 12,
      "write_ops_per_sec_milli": 12000,
      "compaction_debt_ops": 500,
      "updated_at_ms": 1713900000000
    }
  ],
  "aggregates": [
    {
      "tablet_id": 7,
      "replica_count": 1,
      "key_count": 1000,
      "key_bytes": 64000,
      "value_bytes": 1048576,
      "storage_bytes": 1112576,
      "applied_index": 42,
      "sample_interval_ms": 1000,
      "write_ops_delta": 12,
      "write_ops_per_sec_milli": 12000,
      "compaction_debt_ops": 500,
      "updated_at_ms": 1713900000000
    }
  ]
}

POST /api/admin/tablets/create
{ "tablet_id": 0 }

POST /api/admin/tablets/{tablet_id}/assign
{ "node_id": "node-a" }

POST /api/admin/tablets/{tablet_id}/handoff
{ "target_node_id": "node-b" }

POST /api/admin/tablets/{tablet_id}/handoff/complete
{ "expected_source_node_id": "node-a" }

POST /api/admin/tablets/rebalance
{
  "dry_run": true,
  "stale_threshold_ms": 15000,
  "max_movements": 10
}
```

`assign` only pre-claims an unowned tablet for a fresh `alive` node. `handoff`
sets a cooperative movement target and keeps the source owner active until the
source completes the handoff. `rebalance` assigns unowned tablets, moves tablets
off fresh draining nodes through handoff, and balances load across fresh `alive`
nodes; it does not force-move tablets from stale or dead owners.

Backup/restore admin request bodies:

```json
POST /api/admin/backups
{
  "backup_id": "nightly-2026-05-04",
  "directory": "/var/lib/agentdb/backups",
  "include_meta": true,
  "include_runtime_summary": true
}

POST /api/admin/restores
{
  "manifest_path": "/var/lib/agentdb/backups/nightly-2026-05-04/manifest.json",
  "dry_run": true
}
```

The backup endpoint writes an online manifest and, when metadata is configured,
a metadata snapshot. Runtime summary data is observability metadata, not a full
copy of runtime KV/content data. The restore endpoint validates manifests only;
destructive restore apply remains an offline operator procedure.

One-call session opening:

```text
POST   /api/sessions/open
```

`POST /api/sessions/open` takes a JSON body `{"workspace", "agent", "session"}`
(optional `policy`/`config`) and idempotently resolves-or-creates the whole
triple in one request, returning the session. Repeats and concurrent callers
converge on the same rows; a session alias already owned by another agent in the
workspace returns 409 `ALIAS_CONFLICT`. This is the preferred way to start a
session — it replaces the list/create/catch-409/re-list loop below. A
tenant-scoped token may open only within the workspace it is bound to; minting a
new workspace requires a root/admin credential (single-node loopback qualifies).

Tenants, agents, and sessions (lower-level; `open` is preferred):

```text
POST   /api/tenants
GET    /api/tenants/{tenant_id}
DELETE /api/tenants/{tenant_id}
POST   /api/tenants/{tenant_id}/agents
GET    /api/tenants/{tenant_id}/agents/{agent_id}
DELETE /api/tenants/{tenant_id}/agents/{agent_id}
POST   /api/tenants/{tenant_id}/agents/{agent_id}/sessions
GET    /api/tenants/{tenant_id}/sessions/{session_id}
DELETE /api/tenants/{tenant_id}/sessions/{session_id}
```

Events, replay, fork, and context:

```text
POST /api/tenants/{tenant_id}/sessions/{session_id}/events
GET  /api/tenants/{tenant_id}/sessions/{session_id}/events
POST /api/tenants/{tenant_id}/sessions/{session_id}/append-and-fetch  # deprecated
POST /api/tenants/{tenant_id}/sessions/{session_id}/fork
GET  /api/tenants/{tenant_id}/sessions/{session_id}/replay
POST /api/tenants/{tenant_id}/sessions/{session_id}/replay-prefix  # labs
POST /api/tenants/{tenant_id}/sessions/{session_id}/context  # labs
GET  /api/tenants/{tenant_id}/changes
GET  /api/tenants/{tenant_id}/events/search
POST /api/tenants/{tenant_id}/exports/backfill
GET  /api/tenants/{tenant_id}/exports/status
GET  /api/admin/exports
```

Request bodies for the two most-used mutations (the session is in the path):

`POST .../events` (append):

```json
{ "agent_id": "agt_...", "kind": "message", "payload": {}, "fetch_recent": 5 }
```

- `agent_id` (**required**) — the agent that produced the event.
- `kind` (**required**) — event kind, any string (e.g. `message`, `tool_call`).
- `payload` (**required**) — arbitrary JSON.
- `fetch_recent` (optional, N) — also return the last N events in the response
  (folds in append-and-fetch). `artifact_refs` / `source_refs` /
  `credential_refs` are optional ref arrays. Send an `Idempotency-Key` header to
  make a retried append return the original event instead of duplicating it.

`POST .../fork`:

```json
{ "alias": "branch-a", "at_seq": 7 }
```

- `at_seq` (optional) — the fork point; the child inherits parent events up to
  and including this seq. **Omit it to fork at the parent's current tip**
  (inherit the full history) — the common case. Pass an earlier seq to
  rewind-and-branch.
- `alias` (optional) — a stable child session alias.

`replay` returns the stitched logical session history plus scan stats:
`fork_depth`, `segments_scanned`, `events_scanned`, and
`materialized_prefix`. Fork ancestor reads are bounded to the fork point
(`at_seq + 1`) instead of scanning the full ancestor tail. The core runtime can
also materialize a forked session's non-leaf replay prefix via
`POST /replay-prefix`, so later replay reads a compact prefix object plus the
leaf session tail.

Sandbox filesystem snapshots are coordinated through AgentDB snapshot refs and
an external sandbox service. See
`docs/reference/sandbox-snapshot-contract.md` for trigger ownership, restore,
fork, retention, and dangling-ref behavior.

`GET /changes` is the one public changefeed. **It is off by default**: the
export ledger it reads is maintained only when the server is started with
`serve --changefeed` (or `AGENTDB_CHANGEFEED=1`), because that index costs an
extra write per event on the append hot path. With it off, `changes` returns
`PolicyDenied` and appends run several times faster — enable it only when a
consumer needs the feed. When on, it returns commit-ordered items over the
export ledger with `next_cursor`, `high_watermark`, and
`oldest_available_cursor`, an optional session `scope`, an `include` filter, an
optional `backfill=true` mode (a pinned `snapshot_high_watermark` plus a
`complete` flag for paging history to the tail), and optional long-poll
(`wait_ms`). Fork lineage arrives as an item with `object_type = "fork_edge"`.
Records appear only when labs is enabled. The `exports/*` routes are being
consolidated into `changes`, which is now the single public changefeed (the
former `GET /watch` endpoint has been removed in its favor).

`changes` returns stable export envelopes for observability sinks.
Both persistent and in-memory/dev runtimes return durable ledger cursors
(`ledger:...`): persistent runtimes cover events and metadata object mutations,
while the in-memory ledger covers events and fork-edge control items. Use the
optional comma-separated `include` query parameter, for example
`include=events,records`, to filter object types:

Durable export is currently a single-tablet feature. When metadata-backed
cluster routing is active (`routing.mode = snapshot`), the changefeed, export
status, backfill, and admin export status fail closed with HTTP 422 until
a cross-tablet merge frontier exists.

```json
{
  "items": [
    {
      "export_version": "2026-05-14",
      "cursor": "ledger:ten_...",
      "op": "insert",
      "object_type": "event",
      "tenant_id": "ten_...",
      "agent_id": "agt_...",
      "session_id": "ses_...",
      "object_id": "evt_...",
      "seq": 0,
      "created_at_ms": 1778780000000,
      "observed_at_ms": 1778780000001,
      "event_id": "evt_...",
      "kind": "model_call",
      "model": "gpt-...",
      "duration_ms": 123,
      "tombstone": null,
      "data": {}
    }
  ],
  "next_cursor": "ledger:ten_...",
  "high_watermark": "ledger:ten_..."
}
```

New durable cursors have the opaque shape
`ledger:<tenant>/v2/<20-digit-sequence>/<kind>/...`. In a supported
single-tablet runtime, the monotonically increasing per-tenant sequence is
assigned at the serialized persistent commit boundary, so cursor order within
a tenant is commit order rather than wall-clock order. Legacy retained
timestamp cursors remain readable and sort before v2 cursors. Clients must not
construct or parse either form.

`next_cursor` is the cursor of the last item returned on this page and is the
resume point for the next request; it is null when the page is empty.
`high_watermark` is the export head captured before the request; results are
bounded by it, and it may be ahead of `next_cursor` when the page is limited.
Metadata backfill captures `snapshot_high_watermark` on the first page. Send
that same value on every continued page together with `from_cursor`; the
server rejects a continued metadata page without it or with an expired or
foreign ledger boundary.

Delete envelopes use `op = "delete"` and include `tombstone.reason`.
Current reasons are `soft_delete`, `retention_gc`, and `tenant_purge`.

### Event search

`GET /api/tenants/{tenant_id}/events/search` is a core, text-only search over
the durable event log — the "ask an agent about its own past sessions" path. It
is distinct from the Labs record-search index: it scans the events in scope and
ranks them by how many query tokens appear in the event kind and payload text.

Query parameters:

- `q` (required): whitespace-tokenized, case-insensitive query.
- `session_id` / `agent_id` (optional): narrow the scope to one session or one
  agent's sessions. Omit both to search the whole tenant.
- `kind` (optional): exact event-kind filter.
- `limit` (default 20, max 100).

Because it is an unindexed scan, narrow the scope for large histories; a
tenant-wide search is best-effort over the newest events within a fixed scan
budget. The response is `{ "matches": [{ "event": {...}, "score": N }, ...] }`,
ordered by token coverage then recency.

Use `agentdbctl export clickhouse ...` for the first-party ClickHouse path.
`migrate --apply` creates native raw/event/tombstone tables plus optional
OTel-shaped `agentdb_otel_logs` and `agentdb_otel_traces` projection tables,
with ClickStack-compatible `agentdb_clickstack_logs` and
`agentdb_clickstack_traces` views.
Use `run --mode native,otel --backfill --tail` to write both native AgentDB
tables and ClickStack-friendly OTel projections. `doctor` and `status` validate
the ClickHouse schema stamp, required tables/views, query grants, and optional
write-smoke behavior.

Use `agentdbctl export jsonl run --output ./events.jsonl` or an object-store URI
such as `s3://bucket/prefix/events.jsonl` for raw JSONL archives. The JSONL
command reads the same export envelopes from the `changes` changefeed and keeps
its resume cursor in a local checkpoint file.
ClickHouse and JSONL runs also support exporter-side minimization flags:
`--allow-object-type`, `--deny-object-type`, `--allow-event-kind`,
`--deny-event-kind`, `--redact-field`, `--payload-mode full|hash|drop`, and
`--metrics-output`. These controls run in the connector before external sink
writes and the metrics file is refreshed after each processed batch.
Use `agentdbctl export parquet from-jsonl` to convert those archives to Parquet
through DuckDB for warehouse and lakehouse ingestion.
`exports/backfill` supports event snapshots plus tenant-scope metadata
snapshots:

```json
{
  "scope": "tenant",
  "include": ["events"],
  "from_cursor": "ledger:ten_...",
  "limit": 1000
}
```

Metadata snapshot includes use `snapshot:...` cursors and currently support
`["tenants", "agents", "sessions", "artifacts", "records"]`. Do not mix
`events` with metadata includes in one backfill
request; run metadata backfill first, then event backfill/tail. Omit
`snapshot_high_watermark` on the first metadata request, then echo the returned
value on every request that supplies the preceding `from_cursor`.

Consumers hold their own resume cursor — the changefeed and export reads are
stateless. Persist the last `next_cursor` and pass it back as `from_cursor` to
resume; `agentdbctl`'s exporters keep this cursor in a client-side checkpoint
(a ClickHouse table or a local file). Run one exporter per tenant/consumer to
avoid double-writing a sink. To avoid data loss, insert into the sink before
advancing the checkpoint.

For a current-state bootstrap, `POST /exports/backfill` with `include` set to
metadata types (`tenants`, `agents`, `sessions`, `artifacts`, `records`) and no
`events` returns a point-in-time snapshot of current rows using `snapshot:...`
cursors. Page until a response reports `complete=true`, carrying the returned
`snapshot_high_watermark` on each subsequent request, then resume tailing from
that ledger boundary via `GET /changes`. If the completed snapshot is empty,
`cursor` may be null as long as `snapshot_high_watermark` is present.

Artifacts:

```text
POST /api/tenants/{tenant_id}/artifacts
GET  /api/tenants/{tenant_id}/artifacts/verify-lifecycle
GET  /api/tenants/{tenant_id}/artifacts/{artifact_id}
GET  /api/tenants/{tenant_id}/artifacts/{artifact_id}/blob
```

`artifacts/verify-lifecycle` is a non-destructive consistency check for
single-node lifecycle operations. It verifies that live artifacts with raw bytes
still read and hash correctly, and that derivative-only artifacts no longer
have raw content in the content backend. The response includes counts plus
per-artifact issues such as `missing_raw`, `hash_mismatch`, and
`unexpected_raw_after_purge`.

Artifact policy defaults to 16 MiB per object and may be raised to an absolute
128 MiB (134,217,728-byte) limit. The native HTTP route accepts enough transport
headroom for the runtime to return the structured `ARTIFACT_TOO_LARGE` error at
that boundary; policies above 128 MiB are clamped.

### Labs routes

Records and search, server-side context assembly, caller-driven session
lifecycle (`/lifecycle`, `/storage`, `/restore`, `/replay-prefix`), artifact
`verify-lifecycle`, and the tenant policy sweeps are **Labs** routes: they mount
only when the server runs with the `labs` switch (`serve --labs`, or `labs = true`
under `[server]`). Off by default they return 404, so the supported surface stays
narrow — the underlying runtime objects still exist, only the HTTP surface is
gated. The routes below (records) are Labs; `meta` is core.

Every route carries a **Core / Labs / Internal** stability label, recorded in
`route_catalog()` in `crates/agentdb-api/src/routes.rs` (the machine-readable
label table until an OpenAPI document is generated). The `api_route_stability`
test enforces the contract: every Core route is covered by both SDKs, and every
Labs route stays out of the quickstart (the beginner path).

```text
POST   /api/tenants/{tenant_id}/records            # labs
POST   /api/tenants/{tenant_id}/records/upsert     # labs
GET    /api/tenants/{tenant_id}/records/{record_id}     # labs
PATCH  /api/tenants/{tenant_id}/records/{record_id}     # labs
POST   /api/tenants/{tenant_id}/records/{record_id}/touch  # labs
DELETE /api/tenants/{tenant_id}/records/{record_id}     # labs
GET    /api/tenants/{tenant_id}/records/search     # labs
POST   /api/tenants/{tenant_id}/records/search     # labs
GET    /api/tenants/{tenant_id}/meta/{key}
PUT    /api/tenants/{tenant_id}/meta/{key}
```

`POST /records`, `POST /records/upsert`, and `PATCH /records/{record_id}`
return `X-AgentDB-Record-Commit-Seq`. A record body may include
`external_id`; it is unique per tenant. Repeating `POST /records` with the same
`external_id` is idempotent and returns the existing record. `POST
/records/upsert` updates the existing row for that `external_id` or creates a
new row when none exists.

Records have logical `status` (`active` or `superseded`), monotonic `version`,
`access_count`, and `updated_at_ms`. `PATCH /records/{record_id}` accepts
`expected_version` for optimistic compare-and-set updates and returns
`VERSION_CONFLICT` with HTTP 409 when the current version differs. `POST
/records/{record_id}/touch` increments `access_count` without bumping
`version`.

Record search returns `matches`, not bare records. Each match contains
`record`, `score`, `rank`, and per-mode `components` such as `text` and
`vector`. Scores are query-local ranking metadata; do not store them as durable
business values.

Record search accepts `sharding_key` and `min_visible_record_commit_seq` in both
GET query and POST JSON forms. POST search also accepts `metadata_filter`, a
small scalar predicate language for record metadata:

```json
{
  "namespace": ["db0", "memory"],
  "text": "checkout regression",
  "metadata_filter": {
    "all": [
      { "path": "repo", "op": "eq", "value": "agentdb" },
      { "path": "component", "op": "eq", "value": "api" },
      { "path": "confidence", "op": "gte", "value": 0.8 }
    ],
    "none": [
      { "path": "status", "op": "eq", "value": "stale" }
    ]
  }
}
```

Supported metadata operators are `eq`, `neq`, `in`, `exists`, `gte`, and `lte`.
`path` is a dotted object path into the metadata JSON object. Search also
accepts `include_superseded`, `min_score`, `created_after_ms`,
`created_before_ms`, and `touch`. Superseded records are excluded by default;
set `include_superseded=true` to retrieve memory history. With `sharding_key`,
search uses the owner-local LSI-like index. Without it, search uses the
GSI-like global index, which is eventually consistent. If
`min_visible_record_commit_seq` is ahead of the global index watermark, the
runtime falls back to committed base records to preserve same-client
read-after-write visibility. `GET /api/admin/indexes/records` returns
local/global index watermarks, lag tokens, repair counters, and async apply
failures.

Tenant meta
is a small JSON KV surface for adapter cursors and runtime-local configuration,
not a replacement for application tables.

Policy:

```text
POST   /api/tenants/{tenant_id}/policy/lifecycle-sweep
POST   /api/tenants/{tenant_id}/policy/gc-sweep
```

Tenant and agent policy may include `secret_handling`. Setting
`secret_handling.deny_raw_secret_fields` to `true` makes event append reject JSON
payload keys shaped like raw credential fields, including `api_key`, `password`,
`token`, `access_token`, `authorization`, `client_secret`, `refresh_token`,
`secret_access_key`, and `private_key`. Use `credential_refs` for opaque vault /
broker references instead of putting credential values in `payload`.

Common headers:

```text
Idempotency-Key          Optional mutation idempotency key.
X-Sha256                 Optional artifact content checksum.
X-AgentDB-Backpressure   Response header for backpressure signals.
Authorization            Bearer token for the native HTTP API when configured.
```

## Python SDK

Install:

```bash
pip install -e clients/python
pip install -e 'clients/python[async]'
```

Session-first surface:

```python
from agentdb import AgentDB

db = AgentDB("http://127.0.0.1:7878")
workspace = db.workspace("acme")
agent = workspace.agent("sales-agent")
session = agent.session("support-ticket-42")

session.append_user("hello")
events = session.replay(limit=100)
```

`workspace()` first uses `/api/me` when that hosted identity endpoint exists.
Otherwise it lists by `external_key` before creating, and agent/session helpers
likewise reuse existing aliases (including after a client or server restart)
before attempting creation.

Primary sync classes:

```text
AgentDB
  health_ready()
  workspace(external_key=None, *, tenant_id=None, policy=None)
  open_session(workspace, agent, session, *, policy=None, config=None)
  session(tenant_id, session_id=None, *, agent_id=None)
  session_by_id(tenant_id, session_id, *, agent_id=None)

Workspace
  id
  agent(alias=None, *, agent_id=None, config=None)
  agent_by_id(agent_id, *, alias=None)
  session(alias=None, *, agent_id=None, session_id=None)
  session_by_id(session_id, *, agent_id=None)
  changes(cursor=None, session=None, ...)
  search_events(q, *, session=None, agent=None, kind=None, limit=20)
  artifacts.put/get/blob
  records.write/search

AgentHandle
  id
  session(alias=None, *, session_id=None)
  session_by_id(session_id)

SessionHandle / Session
  append(payload, kind="message", idempotency_key=None, fetch_last_n=0, ...)
  append_and_fetch(payload, fetch_last_n=10, ...)   # deprecated; use append(fetch_last_n=)
  append_user(text, ...)
  model_message(text, ...)
  tool_call(...)
  tool_result(...)
  agent_status(status, ...)
  context_pack_built(...)
  fetch(from_seq=0, limit=100)
  fetch_recent(last_n=10)
  replay(from_seq=None, limit=100)
  materialize_replay_prefix(from_seq=None, limit=100)
  fork(child_session_id=None, at_seq=None, alias=None)   # at_seq=None forks at the tip
  context(max_events=None, max_bytes=None, include=None, record_query=None)
```

The append endpoint (`POST .../events`) takes an optional `fetch_recent = N`
body field: it appends the event and returns the last N events in the same
response, folding in `append-and-fetch`. The separate `POST .../append-and-fetch`
route is **deprecated** (kept for one release) — prefer `append` with
`fetch_recent`; the SDK's `append(..., fetch_last_n=N)` already uses it. Use
`append-and-fetch` only when you need an explicit `max_bytes` cap on the tail.

`append` and `append_and_fetch` accept `artifact_refs`, `source_refs`, and
`credential_refs`. Credential refs carry opaque `cred_ref` values resolved by a
vault or broker outside AgentDB; do not put credential values in `payload`. The
Python SDK rejects append payload keys shaped like raw credentials, such as
`api_key`, `access_token`, `password`, and `private_key`, before sending the
request.

The Python SDK sends `Idempotency-Key` only when the caller supplies
`idempotency_key`. This keeps ordinary same-session appends eligible for server
coalescing. Supply a stable key when the application may retry an ambiguous
mutation; requests with an explicit key retain retry-safe replay semantics.

Lower-level sync `Client` methods:

```text
health_ready()
user_info()
list_tenants()
create_tenant(external_key, policy=None)
get_tenant(tenant_id)
list_agents(tenant_id)
create_agent(tenant_id, alias=None, config=None)
list_sessions(tenant_id)
create_session(tenant_id, agent_id, alias=None)
session(tenant_id, session_id=None, *, agent_id=None)
put_artifact(tenant_id, data, content_type=None, sha256=None)
get_artifact(tenant_id, artifact_id)
get_artifact_blob(tenant_id, artifact_id)
write_record(tenant_id, **body)
search_records(tenant_id, **body)
changes(tenant_id, scope="tenant", session_id=None, agent_id=None, from_cursor=None, include=None, limit=100, backfill=False, snapshot_high_watermark=None, wait_ms=None)
search_events(tenant_id, q, *, session_id=None, agent_id=None, kind=None, limit=20)
```

Async support mirrors the lower-level client through `AsyncClient` and
`AsyncSession`.

Python clients remember the latest `X-AgentDB-Record-Commit-Seq` observed from
`write_record` per tenant and attach it as `min_visible_record_commit_seq` on
later `search_records` calls unless the caller supplied one explicitly.

## TypeScript SDK

Build from this workspace:

```bash
cd clients/typescript
npm install
npm run build
```

Session-first surface:

```ts
import { AgentDB } from "agentdb";

const db = new AgentDB({ baseUrl: "http://127.0.0.1:7878" });
const workspace = await db.workspace();
const agent = await workspace.agent("sales-agent");
const session = await agent.session("support-ticket-42");

await session.appendUser("hello");
const events = await session.replay({ limit: 100 });
```

In authenticated deployments, `workspace()` resolves the workspace/tenant from
user-info metadata. For local development against a server without `/api/me`,
pass a workspace alias, such as `workspace("acme-dev")`.

Primary classes:

```text
AgentDB
  healthReady()
  workspace(externalKey?, opts?)
  openSession(workspace, agent, session, opts?)
  session(sessionId, opts?)
  sessionById(sessionId, { tenantId, agentId? })

Workspace
  tenantId
  agent(alias?, opts?)
  agentById(agentId, opts?)
  sessionById(sessionId, opts?)
  changes(opts?)
  searchEvents(q, opts?)
  artifacts.put/get/blob
  records.write/search

AgentHandle
  agentId
  session(alias?, opts?)
  sessionById(sessionId)

SessionHandle / Session
  append(payload, opts?)
  appendAndFetch(payload, opts?)   // deprecated; use append({ fetchLastN })
  appendUser(text, opts?)
  modelMessage(text, opts?)
  toolCall(...)
  toolResult(...)
  agentStatus(status, opts?)
  contextPackBuilt(...)
  fetch(opts?)
  fetchRecent(n)
  replay(opts?)
  materializeReplayPrefix(opts?)
  fork(opts)
  context(opts?)
```

`AppendOptions` supports `artifactRefs`, `sourceRefs`, and `credentialRefs`.
`credentialRefs` are refs only; real secrets stay in the control plane / vault.
The TypeScript SDK rejects append payload keys shaped like raw credentials, such
as `api_key`, `access_token`, `password`, and `private_key`, before sending the
request.

The TypeScript SDK sends `Idempotency-Key` only when `idempotencyKey` is
supplied. Omit it for the ordinary coalescible append path; pass a stable key
when the application needs retry-safe replay after an ambiguous result.

Lower-level `Client` methods:

```text
healthReady()
userInfo()
listTenants()
createTenant(externalKey, policy?)
listAgents(tenantId)
createAgent(tenantId, opts?)
listSessions(tenantId)
createSession(tenantId, agentId, opts?)
session(tenantId, sessionId?, opts?)
putArtifact(tenantId, data, opts?)
getArtifact(tenantId, artifactId)
getArtifactBlob(tenantId, artifactId)
writeRecord(tenantId, body)
searchRecords(tenantId, body)
changes(tenantId, opts?)
searchEvents(tenantId, q, opts?)
```

The TypeScript client mirrors Python's record visibility behavior: after
`writeRecord`, it stores the tenant's latest record commit sequence and adds
`min_visible_record_commit_seq` to later `searchRecords` calls unless the body
already includes that field.
