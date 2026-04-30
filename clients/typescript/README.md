# agentdb (TypeScript / Node)

TypeScript client for AgentDB V2. It targets the tenant-scoped native
`/api/...` runtime surface and uses Node 20+'s built-in `fetch`.

## Install

```bash
cd clients/typescript
npm install
npm run build
```

## Start A Local Server

```bash
cargo run -p agentdb-server -- serve --in-memory --bind 127.0.0.1:8080
```

## Quick Start

```ts
import { Client } from "agentdb";

const client = new Client({ baseUrl: "http://127.0.0.1:8080" });

const tenant = await client.createTenant("acme");
const agent = await client.createAgent(String(tenant.tenant_id), {
  alias: "sales-agent",
});
const session = await client.createSession(
  String(tenant.tenant_id),
  String(agent.agent_id)
);

const result = await session.append(
  { role: "user", text: "hello" },
  { fetchLastN: 5 }
);
console.log(result.seq, result.recent.map((event) => event.payload));
```

## Core Surface

- `Client.createTenant`, `createAgent`, `createSession`, `session`
- `Session.append`, `appendAndFetch`, `fetch`, `fetchRecent`
- `Session.replay`, `fork`, `subscribe`, `context`
- `Client.watch`
- `Client.putArtifact`, `getArtifact`, `getArtifactBlob`
- `Client.writeRecord`, `searchRecords`
- `Client.registerSkill`, `bindSkill`

IDs are opaque typed strings such as `ten_*`, `agt_*`, `ses_*`, `evt_*`,
`art_*`, `rec_*`, and `skl_*`. Mutating SDK calls generate an
`Idempotency-Key` automatically unless one is supplied.

## Harness Recovery

AgentDB sessions are durable state outside the harness process. A restarted
harness should keep the tenant, agent, session, and last-seen sequence IDs, then
rebuild a session handle and continue from durable history:

```ts
import { Client, SessionEventKind, agentStatus } from "agentdb";

const client = new Client({ baseUrl: "http://127.0.0.1:8080" });
const session = client.session(tenantId, sessionId, { agentId });

const history = await session.replay({ fromSeq: 0, limit: 100 });
const context = await session.context({
  include: ["recent_events", "records", "skills"],
});

const result = await session.append(
  agentStatus("recovered", { lastSeenSeq }),
  { kind: SessionEventKind.AgentStatus }
);

for await (const event of session.subscribe({ fromSeq: lastSeenSeq + 1 })) {
  handle(event);
}
```

Concept mapping:

| Durable-session concept | TypeScript SDK |
|---|---|
| wake/resume handle | `client.session(tenantId, sessionId, { agentId })` |
| get events | `session.fetch(...)` |
| append event | `session.append(...)` |
| append and fetch | `session.appendAndFetch(...)` |
| subscribe from sequence | `session.subscribe({ fromSeq })` |
| fork session | `session.fork(...)` |
| build context | `session.context(...)` |
| tenant/session watch | `client.watch(...)` |

Use `fromSeq: lastSeenSeq + 1` when reconnecting a subscription to avoid
replaying events the harness has already processed. Persisting the last handled
`seq` is the harness's cursor rule.

## Event Conventions

The runtime accepts arbitrary event kinds. The SDK exports advisory convention
helpers for common brain/hand/session events:

```ts
import { SessionEventKind, toolCall, toolResult, userMessage } from "agentdb";

await session.append(
  userMessage("run the report"),
  { kind: SessionEventKind.UserMessage }
);
const call = await session.append(
  toolCall("call_1", "sandbox.exec", { cmd: "python report.py" }),
  { kind: SessionEventKind.ToolCall }
);
await session.append(
  toolResult("call_1", "Generated report."),
  {
    kind: SessionEventKind.ToolResult,
    sourceRefs: [{ event_id: call.eventId }],
  }
);
```

Helpers are intentionally client-side only. They do not prevent custom event
kinds or adapter-specific payload fields.

## Context Provenance

Context is a derived view over the durable session log. When a harness builds
context for a model call, record a `context_pack_built` event with source refs,
then link the model output to that event:

```ts
import {
  SessionEventKind,
  contextPackBuilt,
  contextSourceRefs,
  modelMessage,
} from "agentdb";

const context = await session.context({
  include: ["recent_events", "records"],
  maxEvents: 20,
});
const contextEvent = await session.append(
  contextPackBuilt("ctx_1", ["recent_events", "records"], {
    maxEvents: 20,
    selectedEventCount: Array.isArray(context.events) ? context.events.length : 0,
    selectedRecordCount: Array.isArray(context.records) ? context.records.length : 0,
  }),
  {
    kind: SessionEventKind.ContextPackBuilt,
    sourceRefs: contextSourceRefs(context, { maxRefs: 50 }),
  }
);

await session.append(
  modelMessage("The report is ready.", { contextPackId: "ctx_1" }),
  {
    kind: SessionEventKind.ModelMessage,
    sourceRefs: [{ event_id: contextEvent.eventId }],
  }
);
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
