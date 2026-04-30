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

## Testing

```bash
npm test
npm run build
```

Live smoke tests are skipped when `target/debug/agentdb` is absent.
