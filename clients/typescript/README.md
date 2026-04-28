# agentdb (TypeScript / Node)

TypeScript client for [AgentDB](https://github.com/shenli/agentdb) —
agent-native operational database. Zero runtime dependencies; uses
Node 20+'s built-in `fetch`.

## Install

From this repo (recommended while AgentDB is pre-1.0):

```bash
cd clients/typescript
npm install
npm run build
# Then in your project:
npm install /path/to/agentdb/clients/typescript
```

npm publish lands once the server hits 0.1.0 stable.

## Quick start

Start the server (auth mode):

```bash
cargo run -p agentdb-server -- serve
```

In your TypeScript project:

```ts
import { Client } from "agentdb";

const client = new Client({
  baseUrl: "http://localhost:7878",
  auth: { user: "alice", password: "..." },
});

const session = client.session(`run-${Date.now()}`);
const result = await session.append(
  { role: "user", text: "hello" },
  { fetchLastN: 5 }
);
console.log("seq:", result.seq, result.recent.map((e) => e.payload));
```

Tenant is derived from the `auth` user's record server-side — no
`tenant` param needed anywhere.

## Dev mode (no auth)

```bash
cargo run -p agentdb-server -- serve --dev
```

```ts
const client = new Client({ baseUrl: "http://127.0.0.1:7878" });
const session = client.session("agent-1");
await session.append({ role: "user", text: "hello" });
```

## Fork

```ts
const parent = client.session("agent-1");
await parent.append({ role: "user", text: "hello" });
const child = await parent.fork("agent-1-branch-a");
await child.append({ role: "tool", text: "branch off" });
```

The child sees a frozen snapshot of the parent's events at fork
time, MVCC-backed — see the architecture doc for the semantics.

## Errors

Every server-surfaced failure is one of these subclasses of
`AgentDBError`:

| HTTP | Class           | Typical cause                                 |
|------|-----------------|-----------------------------------------------|
| 400  | `BadRequest`    | malformed body, tenant resolution failed      |
| 401  | `Unauthorized`  | missing/invalid `Authorization`               |
| 403  | `Forbidden`     | authed user can't act on this tenant/session  |
| 404  | `NotFound`      | tenant or session doesn't exist               |
| 409  | `Conflict`      | resource already exists / state collision     |
| 421  | `Misdirected`   | multi-node routing drift                      |
| 429  | `RateLimited`   | quota exhausted; carries `retryAfterMs`       |
| 5xx  | `ServerError`   | forwarding failure, fault injection, draining |

```ts
import { Client, RateLimited, AgentDBError } from "agentdb";

try {
  await session.append({...});
} catch (err) {
  if (err instanceof RateLimited) {
    await new Promise((r) => setTimeout(r, err.retryAfterMs ?? 1000));
    // retry
  } else if (err instanceof AgentDBError) {
    console.error("AgentDB:", err.statusCode, err.code, err.message);
  } else {
    throw err;
  }
}
```

## Why no `Session.recent(n)` for pure reads?

`append + fetchLastN` is the canonical hot path. Every agent turn
writes *something* (a thought, a tool result, an observation) and
wants the recent-N for context. Combining the two saves a round
trip.

For genuine read-only queries (dashboards, replays, audit), use
any MongoDB driver against the wire port:

```ts
import { MongoClient } from "mongodb";
const mongo = new MongoClient("mongodb://127.0.0.1:7879");
const events = await mongo
  .db("demo")
  .collection("events")
  .find({ session_id: "agent-1" })
  .limit(10)
  .toArray();
```

## Testing the SDK

The smoke test spawns `agentdb serve --dev` as a subprocess.
Build the server first:

```bash
cargo build -p agentdb-server
npm test
```

The test is skipped automatically if `cargo build` hasn't run.
