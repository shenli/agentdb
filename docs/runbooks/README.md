# AgentDB runbooks

Operator playbooks for each error code an AgentDB server can surface.
Every `ErrorResponse` emitted by the REST API carries a `doc_url`
field pointing to one of these files (see
[`crates/agentdb-api/src/error.rs`](../../crates/agentdb-api/src/error.rs)'s
`doc_url_for`).

## Structure

Each runbook follows the same five sections so operators can skim one
and then apply the pattern to every other:

1. **Symptom** — the exact shape of the error the client sees, the
   HTTP status it carries, and the client-visible side effects. This
   is what someone searches for when grepping logs or a ticket.
2. **Diagnose** — the first three things to check. Usually involves
   an admin endpoint (`/api/admin/boxes`, `/api/admin/nodes`,
   `/api/admin/tablets`, `/api/admin/hlc`), a metrics snapshot, or a
   log line.
3. **Verify** — how to confirm the diagnosis rather than guess. A
   distinct step because "consistent with" is not the same as "caused
   by."
4. **Remediate** — concrete steps to unblock the request or fix the
   state. Numbered.
5. **Prevent** — config, runbook, or practice changes that make this
   symptom rarer next time.

Keep each runbook focused on one code. If a symptom can be caused by
either of two unrelated codes, write two runbooks and cross-link.

## Currently published

Every `ErrorCode` the REST API can emit has a published runbook:

| Code | Runbook |
|---|---|
| `lease_lost` | [lease_lost.md](./lease_lost.md) |
| `fault_injected` | [fault_injected.md](./fault_injected.md) |
| `quota_exceeded` | [quota_exceeded.md](./quota_exceeded.md) |
| `not_found` | [not_found.md](./not_found.md) |
| `invalid_request` | [invalid_request.md](./invalid_request.md) |
| `storage_error` | [storage_error.md](./storage_error.md) |
| `serialization_error` | [serialization_error.md](./serialization_error.md) |

When a new `ErrorCode` variant is added to
[`crates/agentdb-api/src/error.rs`](../../crates/agentdb-api/src/error.rs),
add a corresponding runbook here and update `doc_url_for` to return
`Some(...)`.
