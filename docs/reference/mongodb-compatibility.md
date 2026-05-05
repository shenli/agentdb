# MongoDB Client Compatibility

AgentDB exposes an optional MongoDB wire-protocol listener so MongoDB clients
and tools can inspect and operate AgentDB runtime data.

This is **MongoDB client compatibility**, not MongoDB database compatibility.
AgentDB does not become a general-purpose MongoDB replacement. The listener is
an adapter onto AgentDB runtime primitives.

## When To Use It

Use the MongoDB listener for:

- inspecting sessions, events, records, artifacts, and skills with familiar
  MongoDB tools
- simple event and record writes from existing MongoDB clients
- request/response AgentDB primitives through `db.runCommand(...)`
- migration experiments where a MongoDB client is already available

Prefer the native AgentDB SDKs and HTTP API for:

- performance-critical agent loops
- streaming watch and subscribe
- artifact blob upload/download
- admin and operations workflows
- full AgentDB framework adapters

## Start The Listener

Run AgentDB with both the native HTTP API and MongoDB listener:

```bash
cargo run -p agentdb-server -- serve --dev --mongo-bind 127.0.0.1:7879
```

Connect with `mongosh`:

```bash
mongosh "mongodb://127.0.0.1:7879/?directConnection=true"
```

`directConnection=true` is recommended because AgentDB does not emulate a
MongoDB replica set.

## Authentication And Exposure

By default, the MongoDB listener accepts unauthenticated local connections.
Enable basic username/password authentication with MongoDB `PLAIN`:

```bash
cargo run -p agentdb-server -- serve --dev \
  --mongo-bind 127.0.0.1:7879 \
  --mongo-username agentdb \
  --mongo-password secret
```

Config file equivalent:

```toml
[mongodb]
bind = "127.0.0.1:7879"
username = "agentdb"
password = "secret"
```

You can also set `AGENTDB_MONGODB_USERNAME` and
`AGENTDB_MONGODB_PASSWORD`.

Connect with official MongoDB clients by setting `authMechanism=PLAIN`:

```bash
mongosh "mongodb://agentdb:secret@127.0.0.1:7879/?directConnection=true&authMechanism=PLAIN&authSource=%24external"
```

This is basic listener authentication, not full MongoDB security
compatibility. The listener does not implement SCRAM, role checks, database
users, or TLS termination.

Do not expose `--mongo-bind` on a public interface. Bind it to `127.0.0.1` by
default, or put it behind infrastructure that provides network access control
and TLS.

For authenticated production integrations, prefer the native AgentDB HTTP API
and SDKs.

Copyable examples are available in [`examples/mongodb`](../../examples/mongodb):

```bash
node examples/mongodb/node/quickstart.js
python3 examples/mongodb/python/quickstart.py
```

## Mental Model

MongoDB names map onto AgentDB runtime concepts:

| MongoDB concept | AgentDB meaning |
|---|---|
| database name | tenant external key |
| `sessions` collection | AgentDB session metadata |
| `events` collection | append-only AgentDB event log |
| `records` collection | searchable AgentDB records |
| `artifacts` collection | artifact metadata |
| `skills` collection | skill manifests |

Example:

```javascript
use acme
```

means:

```text
tenant external_key = "acme"
```

You do not need to create collections. AgentDB exposes fixed runtime
collections. Arbitrary application collections are not supported.

You do not need to create indexes. `createIndexes` is accepted as a no-op
compatibility command. AgentDB indexing is owned by AgentDB runtime semantics,
not MongoDB index definitions.

## Write Events

The most common MongoDB-client write is an event append:

```javascript
db.events.insertOne({
  session_alias: "support-ticket-42",
  kind: "message",
  payload: {
    role: "user",
    content: "hello"
  }
})
```

This is not a raw MongoDB document insert. It means:

```text
open or create tenant "acme"
open or create agent alias "mongo-default"
open or create session alias "support-ticket-42"
append one AgentDB event
```

If you already have AgentDB IDs, pass them explicitly:

```javascript
db.events.insertOne({
  agent_id: "agt_...",
  session_id: "ses_...",
  kind: "tool_result",
  payload: {
    tool: "search",
    result: "..."
  },
  idempotency_key: "tool-result-123"
})
```

## Read Runtime Data

Simple equality filters are supported for inspection:

```javascript
db.events.find({ kind: "message" })
db.sessions.find({ alias: "support-ticket-42" })
db.records.find({ namespace: ["memory"] })
db.skills.find({ name: "summarize" })
db.artifacts.find({ content_type: "text/plain" })
```

The query subset is intentionally small. Do not assume arbitrary MongoDB query
operators, projections, sort, cursor continuation, or aggregation behavior.

## AgentDB Commands

AgentDB-specific primitives use `db.runCommand(...)`.

Append and fetch recent history:

```javascript
db.runCommand({
  agentdbAppendAndFetch: "events",
  session_alias: "support-ticket-42",
  kind: "message",
  payload: {
    role: "assistant",
    content: "hi"
  },
  fetch_last_n: 20
})
```

Fork a session:

```javascript
db.runCommand({
  agentdbFork: "sessions",
  session_id: "ses_...",
  at_seq: 42,
  alias: "experiment-branch"
})
```

Replay a session:

```javascript
db.runCommand({
  agentdbReplay: "sessions",
  session_id: "ses_..."
})
```

Build context:

```javascript
db.runCommand({
  agentdbContext: "sessions",
  session_id: "ses_...",
  include: ["events", "records", "skills", "policy"],
  max_events: 20,
  max_bytes: 262144
})
```

Move session lifecycle:

```javascript
db.runCommand({
  agentdbTransitionLifecycle: "sessions",
  session_id: "ses_...",
  target: "cold"
})
```

Restore archived history:

```javascript
db.runCommand({
  agentdbRestoreSession: "sessions",
  session_id: "ses_...",
  target: "warm"
})
```

## Supported Subset

Supported commands:

```text
hello / isMaster
saslStart / saslContinue   PLAIN auth only, when configured
ping
buildInfo
endSessions
listDatabases
listCollections
insert
find
count
aggregate                 limited $match + $count support
createIndexes             no-op compatibility command
agentdbAppendAndFetch
agentdbFork
agentdbReplay
agentdbContext
agentdbTransitionLifecycle
agentdbRestoreSession
```

Unsupported commands return a Mongo-shaped error document with `ok: 0` and
`codeName: "CommandNotFound"`.

## Not Supported

Do not build applications that depend on:

- arbitrary collections
- `createCollection`
- real MongoDB indexes
- `updateOne`, `updateMany`, or arbitrary update expressions
- `deleteOne`, `deleteMany`, or arbitrary delete semantics
- transactions
- change streams
- full cursor continuation
- replica-set or sharding metadata
- SCRAM, role-based auth, database users, or MongoDB auth administration
- MongoDB TLS termination
- broad aggregation pipelines

## AI Coding Agent Guidance

When generating code for AgentDB through a MongoDB client:

- Always connect with `directConnection=true`.
- If username/password auth is configured, use
  `authMechanism=PLAIN&authSource=%24external`.
- Do not generate SCRAM, role, user-management, or database-auth code.
- Bind only to localhost unless separate infrastructure enforces access control.
- Treat the MongoDB database name as the AgentDB tenant external key.
- Use only fixed collections: `sessions`, `events`, `records`, `artifacts`,
  `skills`.
- Do not generate arbitrary application collections such as `users`, `orders`,
  or `tasks`.
- Do not generate `createCollection`.
- Do not rely on `createIndexes`; it is a no-op compatibility command.
- Write event history through `db.events.insertOne(...)` or
  `agentdbAppendAndFetch`.
- Use `session_alias` when you do not already have a `session_id`.
- Use `db.runCommand(...)` for fork, replay, context, lifecycle, and restore.
- Do not generate update/delete/transaction/change-stream code.
- Use the native AgentDB SDKs for full application integrations when possible.

Safe minimal pattern:

```javascript
const { MongoClient } = require("mongodb");

const client = new MongoClient(
  "mongodb://127.0.0.1:7879/?directConnection=true"
);

await client.connect();
const db = client.db("acme");

await db.collection("events").insertOne({
  session_alias: "support-ticket-42",
  kind: "message",
  payload: {
    role: "user",
    content: "hello"
  }
});

const context = await db.command({
  agentdbAppendAndFetch: "events",
  session_alias: "support-ticket-42",
  kind: "message",
  payload: {
    role: "assistant",
    content: "hi"
  },
  fetch_last_n: 20
});
```
