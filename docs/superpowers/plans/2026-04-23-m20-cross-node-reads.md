# M20 continuation: cross-node session-scoped reads

Status: in progress (2026-04-23).

PR #43 landed cross-node write forwarding for
`append_and_fetch`. The story was honest about what it left
open:

> Reads still route by tenant hash; an overridden session's
> reads will not be visible from a read issued against the
> tenant's hash node.

This PR closes that gap for REST reads and makes the
Mongo-wire case loud instead of silently wrong.

## What's in / out

**In:**
- Rename `route_session_for_write` → `route_session_for_io`.
  Same signature, same body. Same name is the right name
  now that reads use it too.
- Refactor `agentdb_api::forwarding` into a generic
  `forward_request` that takes method + path + optional
  body. `forward_append_and_fetch` becomes a thin wrapper.
  New `forward_session_read` helper for GETs.
- `admin_sessions::get_session` consults
  `route_session_for_io`. On `Remote`, forwards the GET +
  query string to the owning node.
- `fork::fork_handler` consults the same. On `Remote`,
  forwards the POST.
- Mongo wire `find` and `aggregate`: when the filter pins
  `session_id = S` and routing says `Remote`, return
  `code: 10107` (NotWritablePrimary) with an `errmsg` that
  names the owning node's address. Doesn't forward —
  Mongo-wire cross-node forwarding stays deferred. But
  clients see the failure immediately with enough info to
  reconnect.
- Integration tests extending
  `crates/agentdb-api/tests/multi_node.rs`:
  - GET admin session via node A for session owned by B →
    forwarded, returns session detail
  - POST fork via A for session owned by B → forwarded,
    new child session lives on B
  - Mongo find with session filter via A for session owned
    by B → `code: 10107` with B's address in errmsg
- Prometheus counters split: existing
  `forwarded_writes_total` stays specific; add
  `forwarded_reads_total` and `forwarded_read_failures_total`
  so operators can tell the two flows apart.

**Out of scope (explicit):**
- **Mongo wire cross-node forwarding.** Binary stateful
  per-connection protocol; forwarding it requires an
  inter-node trust model (SASL state doesn't transfer).
  Separate PR. This PR only makes the mis-routed-read
  failure mode *loud*, not *right*.
- **Tenant-wide reads** (`find` with no session filter,
  `listDatabases`, `listCollections`). Still tenant-
  scoped; no routing change. A session override doesn't
  move all of a tenant's data.
- **Reaper-driven reassignment for reads.** Reaper (PR #41)
  moves shards when a tablet dies. Reads for a session
  whose shard just got reassigned might see a brief window
  where `find` succeeds against the new owner but has no
  data yet. That's fine for append-only streams; callers
  retrying the read converge.

## Design

### `route_session_for_io`

Pure rename of `route_session_for_write`. All callers
update. The underlying logic (LocationProxy →
NodeAddressProxy → local box or Remote target) is
identical for both read and write paths.

### Forwarding helper refactor

Current `forward_append_and_fetch(http, target, tenant_id,
session_id, body, passthrough)`. Extract the common
parts:

```rust
pub async fn forward_request(
    http: &Client,
    target: &NodeTarget,
    method: reqwest::Method,
    path_with_query: &str,  // already-encoded
    body: Option<Bytes>,
    passthrough: PassthroughHeaders,
) -> Result<Response, ForwardError>;
```

`forward_append_and_fetch` becomes a thin wrapper that
builds the path from `(tenant_id, session_id)` and calls
`forward_request` with `POST`, `Some(body)`. New
`forward_session_read` does the same for `GET` with the
admin path shape.

### Mongo wire cross-node error

`code: 10107` (NotWritablePrimary) is the right Mongo
signal — drivers interpret it as "connect elsewhere."
The response carries `errmsg = "session {id} is owned by
{node_id} at {address}; reconnect there"`. This is
information disclosure to authenticated clients only;
unauthenticated clients can't get far enough to see it.

## Test plan

- Add 3 tests to `multi_node.rs`:
  `admin_get_session_forwards_to_owner`,
  `fork_forwards_to_owner`,
  `mongo_find_with_session_on_peer_returns_10107`.
- All existing multi_node tests stay passing.

## Rollout

Backward compatible. When no overrides are set, no
sessions route off-node; forwarding is a no-op. Operators
running multi-node + overrides see reads land on the
correct owner for the first time.
