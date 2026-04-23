# `node_draining`

## Symptom

Client receives a JSON body with `code: "node_draining"` and HTTP
503. The specific node that's draining is named in `node_id`:

```json
{
  "code": "node_draining",
  "type": "api_error",
  "message": "node node-b7d3: draining; retry at a different node",
  "node_id": "node-b7d3",
  "doc_url": "https://github.com/shenli/agentdb/blob/main/docs/runbooks/node_draining.md"
}
```

Unlike `lease_lost` and `fault_injected`, this error is **permanent
for the process's lifetime** — the node won't un-drain until it
exits and restarts.

## Diagnose

1. Confirm the drain is intentional:

   ```
   curl https://<node>/api/admin/drain
   ```

   Returns `{ "node_id": "...", "is_draining": true, "drained_at_ms": N }`
   if an operator has initiated a drain. The `drained_at_ms` wall
   clock stamps when the drain started — compare against your
   rolling-upgrade window or on-call notes to verify the drain is
   part of planned work.

2. Cross-check `/api/admin/nodes`:

   ```
   curl https://<load-balancer>/api/admin/nodes | jq '.[] | select(.state=="draining")'
   ```

   Drained nodes appear with `state: "draining"` (the drain handler
   updates the control-plane `nodes.state` on activation).

## Verify

- If step 1 returns `is_draining: false`: the client is seeing a
  different error and the `code` field was misread. The remote's
  state isn't actually draining.
- If step 1 returns `is_draining: true` but no operator recalls
  initiating one: someone (or something) drained the node
  intentionally — check the access log for the `POST
  /api/admin/drain` call, check who had credentials that would have
  authored it.

## Remediate

1. **Expected drain (rolling upgrade, planned maintenance).**
   Direct clients at a different node via the load balancer. Wait
   for this node's in-flight writes to drain (observed via
   `/api/admin/boxes/*` `admission_in_flight` reaching 0), then
   SIGTERM the process. Drain is a prelude to shutdown, not a
   runtime state this node stays in indefinitely.

   The drain also stops this node's lease renewal loop, so its
   tablet leases expire naturally after `lease_duration` and the
   stale-lease reaper on another node clears the tablet rows
   without waiting for SIGTERM. An operator who wants a completely
   clean handoff can wait `lease_duration + reaper_safety_margin +
   one reaper tick` (≈45s with defaults) after drain before
   SIGTERMing; by then `/api/admin/tablets` shows
   `owner_node_id = null` for the drained node's rows.

2. **Unexpected drain (someone initiated one and shouldn't have).**
   Because drain is one-way, the only way to restore service is to
   SIGTERM and restart the process. On restart the `draining` flag
   resets to false. Review access controls on `POST
   /api/admin/drain` to prevent recurrence.

3. **Client needs to fail over now.** Drain is a cluster-level
   event signal — the expectation is that clients retry at the
   load balancer, which routes to healthy peers. If your client
   hits nodes directly, either have it re-resolve the fleet on
   `node_draining` errors or front the cluster with a balancer
   that does.

## Prevent

- Put the `/api/admin/drain` endpoint behind operator-only auth (on
  the list for the full M27 admin-auth pass) rather than leaving it
  open to anyone who can reach the admin port.
- Document the drain → drain-verify → SIGTERM sequence in your
  runbook so on-call doesn't SIGTERM prematurely (in-flight
  requests still error) or late (the draining node is a dead-
  weight in your fleet's capacity math).
- Monitor `/api/admin/nodes` for unexpected `draining` states. A
  node that's been draining for longer than a reasonable window
  (usually a few minutes) should be either finished-and-SIGTERMed
  or investigated.
