# agentdb (TypeScript / Node)

TypeScript client for AgentDB. It targets the tenant-scoped native `/api/...`
runtime surface and uses Node 20+'s built-in `fetch`.

## Install

```bash
cd clients/typescript
npm install
npm run build
```

## Start A Local Server

```bash
cargo run -p agentdb-server -- serve --dev
```

## Quick Start

```ts
import { AgentDB } from "agentdb";

const db = new AgentDB({
  baseUrl: "http://127.0.0.1:7878",
  token: process.env.AGENTDB_API_KEY,
});

// Hosted deployments resolve the workspace/tenant from authentication.
const workspace = await db.workspace();
const agent = await workspace.agent("sales-agent");
const session = await agent.session("ticket-42");

const result = await session.appendUser("hello", { fetchLastN: 5 });
console.log(result.seq, result.recent.map((event) => event.payload));
```

`tenantId` is infrastructure state. Normal application code should let
authentication resolve the workspace/tenant, then use application-facing agent
and session aliases. For local development against a server without `/api/me`,
pass a workspace alias explicitly:

```ts
const workspace = await db.workspace("acme-dev");
```

`workspace("acme")`, `workspace.agent("sales-agent")`, and
`agent.session("ticket-42")` use application-facing names/aliases. AgentDB still
assigns canonical opaque IDs (`ten_*`, `agt_*`, `ses_*`) and exposes them on the
returned handles as `.id`. Use `agent.sessionById(sessionId)` or
`await db.session(sessionId, { agentId })` when recovering from a persisted
canonical session ID without exposing `tenantId` to normal app code.

## Core Surface

- `AgentDB.workspace`, `AgentDB.sessionById`
- `Workspace.agent`, `Workspace.agentById`, `Workspace.sessionById`, `Workspace.watch`
- `Workspace.artifacts`, `Workspace.records`
- `AgentHandle.session`, `AgentHandle.sessionById`
- `SessionHandle.append`, `appendUser`, `modelMessage`, `toolCall`, `toolResult`, `contextPackBuilt`
- `SessionHandle.fetch`, `fetchRecent`, `replay`, `materializeReplayPrefix`, `fork`, `subscribe`, `context`
- `SessionHandle.artifacts`, `records`
- `eventRef`, `artifactRef`

The low-level tenant-scoped API remains available:

- `Client.createTenant`, `createAgent`, `createSession`, `session`
- `Session.append`, `appendAndFetch`, `fetch`, `fetchRecent`
- `Session.replay`, `materializeReplayPrefix`, `fork`, `subscribe`, `context`
- `Client.watch`
- `Client.putArtifact`, `getArtifact`, `getArtifactBlob`
- `Client.writeRecord`, `searchRecords`

IDs are opaque typed strings such as `ten_*`, `agt_*`, `ses_*`, `evt_*`,
`art_*`, `rec_*`, and `skl_*`. Mutating SDK calls generate an
`Idempotency-Key` only when `idempotencyKey` is supplied. This keeps the
default append path eligible for server-side same-session batching; reuse an
explicit key when the caller may retry a mutation.

For records, `writeRecord` captures the server's
`X-AgentDB-Record-Commit-Seq` header per tenant. Later `searchRecords` calls
automatically include `min_visible_record_commit_seq` unless the body already
sets it. Use `sharding_key` in the search body when you want the owner-local
LSI-like index; omit it for GSI-like global discovery.

## Harness Recovery

AgentDB sessions are durable state outside the harness process. A restarted
harness should keep the agent/session identity and last-seen sequence IDs, then
rebuild a session handle and continue from durable history. Tenant/workspace
scope should come from authentication:

```ts
import { AgentDB } from "agentdb";

const db = new AgentDB({
  baseUrl: "http://127.0.0.1:7878",
  token: process.env.AGENTDB_API_KEY,
});
const session = await db.session(sessionId, { agentId });

const history = await session.replay({ fromSeq: 0, limit: 100 });
const context = await session.context({
  include: ["recent_events", "records"],
});

const result = await session.agentStatus("recovered", { lastSeenSeq });

for await (const event of session.subscribe({ fromSeq: lastSeenSeq + 1 })) {
  handle(event);
}
```

Concept mapping:

| Durable-session concept | TypeScript SDK |
|---|---|
| wake/resume handle | `agent.sessionById(...)` / `db.sessionById(...)` |
| get events | `session.fetch(...)` |
| append event | `session.append(...)` |
| append and fetch | `session.appendAndFetch(...)` |
| subscribe from sequence | `session.subscribe({ fromSeq })` |
| fork session | `session.fork(...)` |
| build context | `session.context(...)` |
| workspace/session watch | `workspace.watch(...)` |

Use `fromSeq: lastSeenSeq + 1` when reconnecting a subscription to avoid
replaying events the harness has already processed. Persisting the last handled
`seq` is the harness's cursor rule.

## Event Conventions

The runtime accepts arbitrary event kinds. The SDK exports advisory convention
helpers for common brain/hand/session events:

```ts
import { eventRef } from "agentdb";

const user = await session.appendUser("run the report");
const call = await session.toolCall(
  "call_1",
  "sandbox.exec",
  { cmd: "python report.py" },
  { sourceRefs: [eventRef(user)] }
);
await session.toolResult("call_1", "Generated report.", { sourceRefs: [eventRef(call)] });
```

Helpers are intentionally client-side only. They do not prevent custom event
kinds or adapter-specific payload fields.

Append payloads reject raw credential-shaped field names such as `api_key`,
`access_token`, `password`, and `private_key` before sending the request. Pass
opaque vault or broker references through `credentialRefs` instead. The
`toolResult` helper also redacts common credential tokens from inline summary,
output, and metadata strings; put large raw stdout/stderr in artifacts.

## Context Provenance

Context is a derived view over the durable session log. When a harness builds
context for a model call, record a `context_pack_built` event with source refs,
then link the model output to that event:

```ts
import {
  contextSourceRefs,
  eventRef,
} from "agentdb";

const context = await session.context({
  include: ["recent_events", "records"],
  maxEvents: 20,
});
const contextEvent = await session.contextPackBuilt(
  "ctx_1",
  ["recent_events", "records"],
  {
    maxEvents: 20,
    selectedEventCount: Array.isArray(context.events) ? context.events.length : 0,
    selectedRecordCount: Array.isArray(context.records) ? context.records.length : 0,
    sourceRefs: contextSourceRefs(context, { maxRefs: 50 }),
  }
);

await session.modelMessage("The report is ready.", {
  contextPackId: "ctx_1",
  sourceRefs: [eventRef(contextEvent)],
});
```

This keeps the prompt/context shape model-specific while preserving the
recoverable events and records that produced it.

## Adapter Readiness

Adapters can run a lightweight convention check over a recorded event sequence:

```ts
import { validateAdapterEvents } from "agentdb";

const warnings = validateAdapterEvents(events);
if (warnings.length) {
  throw new Error(warnings.join("\n"));
}
```

The validator is advisory. It catches common issues such as unpaired tool calls,
large inline tool outputs without artifacts, missing context provenance, missing
resume status, and event sequences that do not carry AgentDB `seq` values.

## Testing

```bash
npm test
npm run build
```

Live smoke tests are skipped when `target/debug/agentdb` is absent.
