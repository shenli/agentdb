# Tiered Storage Operations

This runbook covers the single-node warm/cold/archive/derivative-only lifecycle
operator story. It focuses on how to inspect, transition, restore, and verify
session lifecycle state without implying a fully automated storage-class
orchestration product.

For the design contract, see
[../design/lifecycle-storage.md](../design/lifecycle-storage.md). For the API
surface, see [../reference/api-sdk.md](../reference/api-sdk.md).

## Lifecycle States

| State | Operational meaning |
|---|---|
| `warm` | Session is appendable and recent mutable history is in the hot runtime path. |
| `cold` | Older history is sealed out of the hot head; reads should remain logically equivalent. |
| `archive` | History is compacted behind an archive manifest/bundle and may need restore before normal warm/cold operation. |
| `derivative_only` | Raw bytes may be purged while metadata, refs, summaries, records, or other derived state remain. Full replay is not guaranteed. |

The important operator rule is that lifecycle is part of the storage contract,
not just a label. After every transition, verify reads, replay, and policy
state rather than only checking the lifecycle field.

## Transition A Session

Use the native HTTP API:

```bash
curl -fsS \
  -X POST \
  -H 'content-type: application/json' \
  -d '{"target":"cold"}' \
  http://127.0.0.1:7878/api/tenants/ten_.../sessions/ses_.../lifecycle
```

Allowed targets are `warm`, `cold`, `archive`, and `derivative_only`.

## Restore Archived History

Restore archive-backed history to `warm` or `cold`:

```bash
curl -fsS \
  -X POST \
  -H 'content-type: application/json' \
  -d '{"target":"warm"}' \
  http://127.0.0.1:7878/api/tenants/ten_.../sessions/ses_.../restore
```

Restore targets are `warm` and `cold`. Restore from archive is a session
storage operation, not the same as node-level disaster recovery. For node-level
restore, see [restore-recovery.md](restore-recovery.md).

## Run Lifecycle And GC Sweeps

Policies can drive lifecycle and garbage-collection work:

```bash
curl -fsS \
  -X POST \
  http://127.0.0.1:7878/api/tenants/ten_.../policy/lifecycle-sweep

curl -fsS \
  -X POST \
  http://127.0.0.1:7878/api/tenants/ten_.../policy/gc-sweep
```

Use these after changing lifecycle policy, retention policy, legal hold, or
artifact purge settings. Treat sweep output and server logs as the source of
truth for what work was actually performed.

## Verify A Transition

After a lifecycle transition or sweep:

1. Fetch the session and confirm lifecycle/storage summary fields changed.
2. Replay the session and confirm event order is preserved:

   ```bash
   curl -fsS \
     http://127.0.0.1:7878/api/tenants/ten_.../sessions/ses_.../replay
   ```

3. Fetch context for the session and confirm the agent-facing state still
   contains the expected recent events, records, skills, and policy.
4. If the session was archived, restore it to `cold` or `warm` in a test
   tenant before relying on the archive path operationally.
5. If derivative-only purge was used, confirm consumers understand that full
   replay is no longer guaranteed.

Verify artifact lifecycle consistency for the tenant:

```bash
TENANT_ID=ten_...
agentdbctl artifacts verify-lifecycle --tenant-id "$TENANT_ID"
```

The verifier is non-destructive. It reports active artifacts whose raw bytes
are missing or hash incorrectly, and derivative-only artifacts whose raw bytes
still exist after purge.

## Artifact Byte Lifecycle

Artifact metadata and artifact bytes have separate lifecycle concerns. Before
purging raw bytes:

- confirm legal hold and retention policy allow deletion
- confirm derived records, summaries, or references are sufficient for the
  product use case
- confirm export/CDC consumers have observed the tombstone or lifecycle event
- keep one verified backup or archive path until the purge policy is proven

After purge:

- artifact metadata may still exist
- content download may fail or intentionally return a policy error
- replay may preserve refs without being able to recover raw bytes
- `agentdbctl artifacts verify-lifecycle --tenant-id <tenant>` should report
  purged raw bytes without issues

Artifact creation writes content before metadata. On a metadata persistence
error the runtime attempts immediate blob deletion. Local filesystem content
also fsyncs the file and containing directories around rename. On persistent
standalone startup, before accepting traffic, AgentDB lists local content and
deletes objects with no metadata that still retains raw content; inspect the
`completed startup artifact-content reconciliation` log fields for scanned,
deleted, and error counts. Do not run an equivalent sweep concurrently with
writes because content legitimately exists briefly before metadata commits.

## Object-Store Dependencies

For object-store-backed content, segments, archives, or artifacts, AgentDB does
not recreate external storage policy. Operators must preserve:

- bucket and prefix ownership
- lifecycle rules
- retention/lock settings
- credentials or IAM role
- encryption/key availability
- object-store audit logs, if required

The current object-store content backend does not expose listing to AgentDB, so
the standalone startup orphan reconciliation reports `listing_supported=false`
and does not delete remote objects. Use bucket inventory and lifecycle tooling
to detect orphans until listing/reconciliation is implemented.

If cold/archive reads fail after an object-store change, restore those external
dependencies before changing AgentDB state.

## Failure Handling

If a lifecycle transition fails:

- check policy denial, legal hold, quota, storage, and object-store errors
- retry only after the underlying cause is fixed
- do not manually edit lifecycle fields in metadata storage

If archive restore fails:

- preserve the archive manifest and current session row
- check object-store reachability and key availability
- retry restore after dependencies recover
- avoid appending to the session until the expected recovery path is clear

If derivative-only purge removed bytes that a workflow still needs:

- restore from a verified backup or archive if policy allows
- update lifecycle policy so future sessions keep raw bytes longer
- document which product workflow depended on raw artifact access

## Remaining Automation Boundary

The current operator story is documented and usable, and artifact lifecycle
verification has a read-only command. Some behavior remains future product
hardening:

- richer dashboard visibility into lifecycle queues and archive/restore state
- automated storage-class orchestration across object-store tiers
- upgrade/rollback automation around lifecycle metadata changes
