# `lease_lost`

## Symptom

The client receives a JSON error body with `code: "lease_lost"` and
HTTP 503:

```json
{
  "code": "lease_lost",
  "type": "api_error",
  "message": "box 3: lease lost (epoch 17)",
  "box_id": 3,
  "doc_url": "https://github.com/shenli/agentdb/blob/main/docs/runbooks/lease_lost.md"
}
```

Writes to one or more tenants start failing fast with this shape.
Reads on the same tenants may still succeed — the check only fires on
the write path.

## Diagnose

1. Pull the tablet view:

   ```bash
   agentdbctl --url https://<node> tablets list
   ```

   Look at the row with `tablet_id == box_id` from the error. If
   `owner_node_id` is **not** this node, ownership moved to another
   process — that node is serving these tenants now.

2. Pull the node view:

   ```bash
   agentdbctl --url https://<node> nodes list --stale-threshold-ms 15000
   ```

   If this node's row carries `is_stale: true`, the control plane
   (meta store) is unreachable from here. Check connectivity.

3. Grep the server log for recent lease-renewal warnings:

   ```
   lease renewal failed
   ```

   The last few log lines before the first `lease_lost` response
   usually say why renewal stopped succeeding.

## Verify

- If step 1 showed a different owner: confirm that the other node is healthy
  with `agentdbctl --url https://<other-node> nodes list --stale-threshold-ms 15000`.
  If it reports `is_stale: false` for itself, the tablet has genuinely moved
  and this node is the stale one.
- If step 2 flagged the current node as stale: confirm the meta
  store is actually reachable:

  ```
  sqlite3 $AGENTDB_META_PATH 'SELECT 1'
  ```

  If sqlite errors (locked, missing, corrupt), that's the root cause.

## Remediate

1. **Transient lease loss while the same node still owns the tablet.**
   Clients should retry with backoff. The renewal loop keeps trying
   — once it succeeds the lease extends and subsequent writes go
   through. No operator action beyond watching it recover.
2. **Ownership moved to another node.** The scheduler (once M22 lands)
   will have routed around this node. Clients pointed at this node
   should be updated to hit the new owner. Until the scheduler lands,
   this usually means a human picked up a tablet on the other node
   by restarting it; restarting the old node clears its stale cache
   of `Box_::lease` and it re-acquires or steps down cleanly.
3. **Meta store unreachable.** Fix the meta store first. If using
   SQLite locally: check the file exists, check disk is not full,
   check no other process holds an exclusive lock on the WAL. If
   using Postgres (future backend): check network reachability, check
   that this node's credentials still authenticate.

## Prevent

- Monitor `lease_renewal_failures` counter. Sustained growth means
  something is wrong with either the meta store or this node's
  connectivity to it — investigate before all the leases expire.
- Run the meta store on a filesystem that supports proper fsync
  semantics. SQLite on tmpfs will lose state on reboot and surface
  as `lease_lost` spikes.
- Size `AGENTDB_LEASE_DURATION_SECS` (default 30) and
  `AGENTDB_LEASE_RENEW_SECS` (default 10) with enough margin for
  transient meta-store slowness. The renewal must land within
  `(duration - renew)` seconds or the lease expires.
