# M20 continuation: cross-node REST forwarding

Status: in progress (2026-04-23).

The shard+tablet+lease control plane and the write-path
consultation of `LocationProxy` both landed (PR #38, #41). But
`BoxRegistry::route_session` still has a load-bearing safety
fallback: when the override points at a tablet this node
doesn't own, it silently routes to the tenant-hash box on
**this** node. That's safe for a single-node cluster; it's
silently wrong in a multi-node cluster — the operator set an
override expecting the write to land on the targeted tablet,
and it didn't.

This PR closes that gap for REST `append_and_fetch`. Node A
that receives a write bound for a tablet owned by node B
forwards the request via HTTP to B's `AGENTDB_NODE_ADDRESS`.

## What's in / out

**In:**
- `agentdb-core::forwarding::NodeAddressProxy` — cached
  `tablet_id → (node_id, address)` mapping, rebuilt from
  `tablets × nodes` every `AGENTDB_NODE_ADDR_REFRESH_SECS`
  (default 10s; more aggressive than LocationProxy because
  node address changes are failover-critical). Background
  refresh job.
- `RouteDecision { Local(Arc<Box_>), Remote { address,
  node_id, tablet_id } }` returned by a new
  `BoxRegistry::route_session_for_write`. `route_session`
  stays for call sites that don't care about cross-node
  (admin/tests).
- `agentdb-api::forwarding` — forwarding helper built on the
  shared `reqwest::Client` in `AppState.http`. Preserves:
  - Request body (JSON) verbatim
  - `Authorization` header (so the remote node's auth
    middleware verifies independently)
  - `X-AgentDB-HLC` (so the cluster HLC keeps merging)
  - An `X-AgentDB-Forwarded: 1` loop-guard header.
  Returns the remote response status + body transparently.
- `handlers::append_and_fetch` consults
  `route_session_for_write`; on `Remote`, forwards and
  streams the response back. `X-AgentDB-Forwarded: 1` on
  inbound → refuse to forward again, 421 Misdirected Request
  with a clear message (shouldn't happen unless routing is
  inconsistent across nodes).
- New metrics (per box, because box 0 is where "forwarded
  writes originated by this node" counts):
  `forwarded_writes_total`,
  `forwarded_write_failures_total`.
- Integration test: spawn two AppStates sharing a
  MetaStore, register both in `nodes`, assign tablets
  `0,1` to node A and `2,3` to node B. Set a shard
  override to land on a tablet owned by B. Send the
  write to A; assert A returned 200 and the event
  landed in B's box metrics (not A's).
- Loop-guard integration test: synthesize a request with
  `X-AgentDB-Forwarded: 1` that would route Remote; verify
  node returns 421.

**Out of scope (explicitly):**
- **Mongo wire forwarding.** The wire protocol is binary
  and stateful per-connection; forwarding it requires
  either tunneling OpMsg frames over HTTP or a parallel
  TCP hop. Either is a separate PR. Operators running
  multi-node Mongo should point clients at the tablet's
  owning node (or put a proxy in front).
- **Read-path forwarding.** `find`, `aggregate`, session
  queries still route by tenant hash. An overridden
  session's reads can miss writes that landed on the
  overridden tablet — this is the known "quiesce/data
  handoff" gap called out in the M20 README. This PR
  doesn't close it; it only makes writes end up on the
  right node.
- **Retry logic on forwarding failures.** If the remote
  returns 5xx or times out, we surface it to the client.
  The client (or a load balancer above AgentDB) retries.
  Internal retry masks slow failover which operators need
  to see.
- **Connection pooling tuning.** `reqwest::Client`'s
  defaults (pool per host, HTTP/1.1 keep-alive) are fine
  for prototype scale.

## Design

### `NodeAddressProxy` (new)

```rust
pub struct NodeTarget {
    pub node_id: String,
    pub address: String, // bare "host:port" or absolute URL
}

pub struct NodeAddressProxy {
    meta: Arc<MetaStore>,
    /// tablet_id -> node. `None` = tablet is unowned (reaped)
    /// or its owner isn't in `nodes` yet (race during bootstrap).
    cache: RwLock<HashMap<i64, NodeTarget>>,
    /// This process's own node id. Let callers compare
    /// target.node_id to "am I the owner?" without hitting
    /// state elsewhere.
    self_node_id: String,
}

impl NodeAddressProxy {
    pub async fn new(meta: Arc<MetaStore>, self_node_id: String)
        -> Result<Arc<Self>>;
    pub fn target_for_tablet(&self, tablet_id: i64) -> Option<NodeTarget>;
    pub fn is_self(&self, target: &NodeTarget) -> bool {
        target.node_id == self.self_node_id
    }
    pub async fn refresh(&self) -> Result<()>;
    pub fn self_node_id(&self) -> &str;
}

pub struct NodeAddressRefreshJob { /* RAII handle */ }
```

Refresh:
1. `tablets::list_all` → `Vec<Tablet>`.
2. `nodes::list_all` → `Vec<Node>`. Build `HashMap<node_id, address>`.
3. For each tablet with an owner, look up address; drop
   rows whose owner isn't in the nodes table (benign race
   at bootstrap).

Cache is plain RwLock — the map is ≤ num_tablets entries,
writes are once-per-refresh. No lock-free contention concern
at prototype scale.

### `RouteDecision`

```rust
pub enum RouteDecision {
    /// Local box. Handler proceeds with the existing
    /// code path. Covers three cases collapsed by the
    /// caller: no override set (tenant hash), override
    /// set but tablet is one we own, override pointing
    /// at an out-of-range tablet + node-address lookup
    /// missing (safety fallback from PR #41 still in
    /// place for the degenerate case).
    Local(Arc<Box_>),
    /// Forward the request via HTTP to the named node.
    Remote(NodeTarget, /* tablet_id: */ i64),
}
```

`BoxRegistry::route_session_for_write(tenant, session,
address_proxy)`:
1. Ask LocationProxy for the session's shard.
2. No override → `Local(route_tenant(tenant))`.
3. Override hits a tablet this node owns → `Local(box_by_id(...))`.
4. Override hits a tablet owned by another live node →
   `Remote(node_target, tablet_id)`.
5. Override hits a tablet whose owner isn't resolvable
   (reaped, no address) → `Local(route_tenant(tenant))`
   + WARN log. The shard re-overrides itself on the next
   reaper tick so this state is transient.

### REST forwarding

`agentdb-api::forwarding::forward_append_and_fetch` takes
the original `Request` + `NodeTarget` and:

1. Normalize the target URL: `http://<address>/api/...`.
   If `address` already has a scheme, use as-is; else
   prepend `http://`.
2. Build a request via `state.http` with:
   - Same method, same path, same JSON body
   - `Authorization: ...` (if present)
   - `X-AgentDB-HLC: ...` (if present)
   - `X-AgentDB-Forwarded: 1` (loop guard)
3. Send, await response.
4. Copy status + JSON body back to the client.
5. On network error: 503 with `ApiError::ServiceUnavailable`
   tagged with the target node id.

Loop-guard: `append_and_fetch` checks request headers; if
`X-AgentDB-Forwarded: 1` is present *and* routing still
says Remote, return 421 Misdirected Request. Prevents a
routing bug from creating an infinite forwarding cycle.

### Integration test

New file: `crates/agentdb-api/tests/multi_node.rs`. Single
tokio test process, two full `AppState`s sharing a
MetaStore. Register both nodes in `meta_nodes` with
distinct `node_id` + their `127.0.0.1:<port>` addresses.
Set `tablets` ownership so node A owns tablets 0,1 and
node B owns 2,3. Set a shard override routing session
`"s-forward"` to tablet 3.

Test:
- POST append to node A for `s-forward`. Expect 200.
- Assert via node B's registry that its per-box metrics
  for box 3 shows `appends == 1`.
- Assert node A's box metrics for every box show
  `appends == 0`.
- Assert node A's per-box forwarded-writes counter == 1.

Loop-guard test: POST to node A with
`X-AgentDB-Forwarded: 1` targeting session `s-forward` →
421.

## Rollout

Backward compatible. When no overrides exist (default
state) the hot path is unchanged: route_session_for_write
returns `Local(route_tenant(...))`. Operators who don't
run multi-node see no behavior change.

When operators run multi-node AND set overrides AND
nodes advertise reachable addresses (not `0.0.0.0`), the
forwarding path activates. A compose-two-nodes example is
out of scope for this PR; the integration test is the
proof.
