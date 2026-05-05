# `fault_injected`

## Symptom

Client receives a JSON error body with `code: "fault_injected"` and
HTTP 503:

```json
{
  "code": "fault_injected",
  "type": "api_error",
  "message": "box 2: fault 'Error' injected",
  "box_id": 2,
  "fault_kind": "error",
  "doc_url": "https://github.com/shenli/agentdb/blob/main/docs/runbooks/fault_injected.md"
}
```

Requests that route to the identified `box_id` fail; other boxes are
unaffected. `fault_kind: "latency"` means requests succeed but are
artificially delayed; `fault_kind: "error"` means requests fail with
503.

## Diagnose

1. Check the box's admin view:

   ```bash
   agentdbctl --url https://<node> raw get /api/admin/boxes/<box_id>
   ```

   If `fault` is set with a future `expires_at`, a fault is actively
   injected. That's the direct cause of the error.

2. Check recent admin activity:

   ```
   grep -i 'inject_fault' /var/log/agentdb/*.log
   ```

   Faults only land via explicit operator action — find who ran the
   inject call and when.

## Verify

- The fault is real (not a flake) when `/api/admin/boxes/<box_id>`
  returns `fault.is_active() == true` and the `expires_at` is in
  the future. If `expires_at` is already in the past, the fault
  has expired and subsequent requests will succeed — the 503 the
  client reported was from before expiry.

## Remediate

1. **Intentional chaos drill.** The fault was injected on purpose
   (load test, failure-mode exercise, incident-response drill). Wait
   for `expires_at` to elapse, or clear it explicitly:

   ```bash
   agentdbctl --url https://<node> raw post /api/admin/boxes/<box_id>/inject_fault \
     --body '{"kind":"clear"}'
   ```

   (The `clear` endpoint is a follow-up admin addition; for now,
   inject a 0-duration latency fault to overwrite the active one.)

2. **Unintentional / leftover from a previous test.** Same clear
   step. Then check the access log to understand who injected it
   and whether it should have expired earlier.

3. **Clients can't wait for expiry.** Rebalance traffic off this box
   at the load-balancer layer until the fault clears. Admission
   control does not rate-limit around injected faults — every
   request that routes to this box will fail until the fault is
   gone.

## Prevent

- Never inject faults directly against a production cluster without
  a short, explicit TTL. `expires_at` is the safety net.
- Route chaos experiments through an automation layer that records
  the drill context (owner, duration, revert time) so unintended
  leftovers are easy to audit.
- Consider tagging the server's `StreamEvent::FaultInjected` (if
  exposed) into your observability stack — a fault held past its
  intended window is the kind of thing a dashboard should flag.
