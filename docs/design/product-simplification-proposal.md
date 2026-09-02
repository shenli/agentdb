# AgentDB Product Simplification Proposal

**Status:** Proposal, revised 2026-09-01 after a code inventory at `main`
@ `385069f`; work-in-flight and deployment sections checked against `52cc332`.
Encryption status refreshed 2026-09-02 against `main` @ `af6538c` (M1–M24 merged);
the commit-pinned measurement tables are left as historical snapshots.  
**Scope:** Product boundary, public API, packaging, and sequencing  
**Does not change:** Current runtime behavior, compatibility commitments, or
implementation status

## Decision Summary

AgentDB should focus its first supported product on one promise:

> AgentDB is a durable, replayable session log for AI agents.

The product should be evaluated against one recovery loop:

```text
append -> crash -> resume -> replay or fork
```

The first release should make this loop durable, understandable, observable,
and easy to operate on a single node. Features that do not materially improve
this loop should move out of the core product surface until customer evidence
justifies their maintenance and operational cost.

This proposal narrows the public model to four concepts, consolidates the
application API around six intents on ten paths, names the twelve SDK
methods, classifies every existing route, defines the first single-node persona and
its install path, and reduces the sequencing to one packaging change, two
cut-sized changes, and validation.

## Why Simplify Now

The external positioning is focused: AgentDB is the durable session log for an
agent run. The implemented surface is substantially broader. Measured at
`385069f`:

| Surface | Measured |
|---|---|
| Registered HTTP paths | 101 (45 `/api/admin/*`, 46 `/api/tenants/*`, 10 other) |
| Method-handler pairs | 119 |
| Native runtime object families | 9, plus tenant meta KV and skill bindings |
| Change-consumption mechanisms | 5 (subscribe, watch, export watch, backfill, managed consumers) |
| `agentdb-core/src/runtime.rs` | 14,074 lines (the whole core crate is 20,807) |
| `agentdb-api/src/routes.rs` | 8,642 lines |
| Python SDK public methods | 133 (91 in `client.py`) |
| TypeScript SDK public methods | 66 |
| Cargo feature flags | 0; all cluster, tablet, and Raft code is compiled into every binary |

These numbers are not inherently defects. They indicate that each new user,
SDK, release, and operator workflow must understand and maintain several
product layers before the primary value proposition has been established.
AgentDB is still pre-release, so this is the least expensive point at which to
reduce public commitments.

The August 2026 design review ([`../design-review-2026-08.md`](../design-review-2026-08.md),
finding 6) already recommended freezing new distributed surface until the
single-node runtime is excellent. This proposal extends that recommendation to
the product surface as a whole.

## Relationship To Work In Flight

The proposal is not a retroactive judgment on merged work, but it must state
what happens to the two active streams that fall on its defer list.

- **Distributed hardening (#295 through #301).** Frozen at #301. Existing
  cluster tests stay green and continue to run in CI. No new tablet, routing,
  Raft, or movement surface is added until a measured single-node limit or a
  named design partner requires it.
- **Encryption at rest (#302–#325, M1–M24 merged).** The full effort shipped
  since this proposal was first written, well past the *basic encryption*
  boundary. Basic encryption — authenticated encryption on every durable path
  under a locally configured key — is *core*, and it is done: content, events,
  metadata values, and every primary record (tenant/agent/session/record/skill)
  plus the export ledger are sealed, single-node and cluster, with per-event AAD.
  But the advanced key-management this proposal had listed as *deferred* is also
  built: a wrapped-DEK catalog, a pluggable KMS/HSM seam (`CommandKekProvider`),
  online DEK rotation, two-phase revocation / crypto-shred, a data-plane
  re-encryption walk, verification-gated version retirement, admin endpoints, and
  an `agentdbctl encryption` CLI. This is a body of *not-core* capability
  delivered ahead of a named customer or a measured need — exactly what the
  proposal argues to defer. It should not be undone, but it needs an explicit
  stability label: basic envelope encryption is **Core**; the rotation / revoke /
  re-encrypt / retire admin surface and CLI are **Internal** (operator machinery,
  no application-level compatibility promise); the KMS seam is Core-enabling but
  not advertised in the beginner path. See the reclassification in Decision
  Question 5 and Appendix A.

## Proposed Product Boundary

### Core

The supported core should contain only the capabilities required to record,
recover, inspect, and branch agent execution:

- durable single-node storage;
- workspace isolation and authentication;
- sessions and ordered, append-only events;
- append with optional recent-tail return and durable idempotency;
- recent reads and full fork-aware replay;
- pointer-based forks;
- artifacts and event-to-artifact references;
- one cursor-based changefeed backed by the export ledger;
- bounded retention and cleanup with safe defaults;
- basic encryption at rest as defined above;
- health probes, a metrics endpoint, backup, restore, and crash-recovery
  tooling;
- session importers for Claude Code and Codex, with a watch mode; and
- maintained Python and TypeScript SDKs.

Two items in this list do not exist today and must be built rather than
labeled: there is no `/metrics` endpoint (metrics appear only as JSON fields
inside admin responses), and there is no generated API specification
(`utoipa` is a dependency, but no route is annotated and nothing serves an
OpenAPI document).

### Labs

The following capabilities remain compiled in and callable, gated by a single
`labs` configuration switch, but are not required concepts in the quickstart,
stable SDK, or core compatibility contract:

- records and scoped search, including text and vector indexes;
- versioned skill registry and bindings;
- server-side context assembly;
- lifecycle tier transitions, archive restore, and replay-prefix
  materialization as caller-invoked operations;
- sandbox snapshot metadata;
- framework-specific adapters; and
- rich dashboard views.

Labs capabilities consume or project the session log. They do not add work to
the synchronous append path beyond what is already there (see Appendix B).

One exception is recommended. Scoped text search over **events** within a
workspace or session returns to core, because the first single-node persona
(below) needs "what did we do about this last week" over imported coding-agent
history, and the MCP tool that answers it should not depend on Labs. Search
over records, vector search, and record indexes stay in Labs. This is a
decision for the team to confirm; it is listed under Decision Questions.

### Delete

The following have no consumer outside the core crate and its HTTP routes and
should be removed rather than labeled:

- record graph edges (three routes, no SDK method, adapter, example, or test
  outside core);
- session subscribe (a one-shot SSE framing of the events fetch);
- the frontier-cursor `watch` implementation, once the ledger-backed
  changefeed replaces it;
- managed export consumers (server-side storage of a cursor the client already
  holds); and
- the separate `append-and-fetch` route, once `append` accepts the
  recent-tail option.

### Deferred

The following work should wait for a named design partner, a measured capacity
limit, or a supported hosted-product requirement:

- production distributed tablets and their public control-plane API;
- additional SDK languages or a binary application protocol;
- sink-specific export products beyond the changefeed; and
- automated multi-tier storage orchestration beyond bounded retention.

Customer-managed key catalogs, rotation, and revocation workflows were on this
list; they have since been built (M6–M24) rather than deferred. They stay in the
codebase but are labeled **Internal** rather than advertised — see §Relationship
To Work In Flight and Decision Question 5.

## Simplified Public Model

The primary documentation and SDKs should teach four nouns.

| Concept | Meaning |
|---|---|
| Workspace | Isolation, authentication, quota, and retention boundary. Stored server-side as a tenant; HTTP paths keep the `/api/tenants/{id}` prefix. |
| Session | One durable agent execution or branch. |
| Event | An ordered fact about what happened during the execution. |
| Artifact | Large or binary data referenced by an event. |

The following concepts become attributes, references, projections, or internal
mechanisms:

- Agent identity is a session attribute and an event field. Sessions are
  opened by (workspace, agent, session) aliases; the `/agents/{id}/sessions`
  path shape and the SDK agent handle go away. This is an API break.
- A fork is a relationship between sessions (`forked_from`), not another
  top-level object. This is already how it is implemented.
- A skill is an artifact or external reference recorded by an event. The
  `skill_used` event kind and skill source-ref validation in the append path
  move behind the Labs switch.
- A record is a projection derived from events, not required runtime truth.
  The `record_id` and `skill_id` fields on event source refs become Labs-only
  fields; core source refs carry event and artifact references.
- A context pack is an SDK or harness decision whose provenance can be written
  as an event.
- Lifecycle and policy are workspace configuration with automatic defaults.
  The warm/cold/archive machinery and the auto-thaw on append stay in the
  runtime; only the caller-driven transition routes move to Labs.

## Simplified Application API

The stable application API is organized around six intents: eleven route
handlers on ten paths, plus two health probes. Two paths are new (`open` and
`changes`); the rest exist today.

| Intent | Route | Notes |
|---|---|---|
| Open a session | `POST /api/sessions/open` | Idempotent on (workspace, agent, session) aliases. Creates whatever is missing, returns the session. Replaces the three-level list, create, catch-409, re-list loop the SDKs run today. |
| Append an event | `POST /api/tenants/{t}/sessions/{s}/events` | `Idempotency-Key` header; optional `fetch_recent=N` returns the tail in the same call. Absorbs `append-and-fetch`. |
| Read history | `GET /api/tenants/{t}/sessions/{s}/events` | `latest=true&limit=N` for the tail, `from_seq` for a slice. |
| Read history | `GET /api/tenants/{t}/sessions/{s}/replay` | Fork-aware, from an ancestor-inclusive sequence. |
| Fork a session | `POST /api/tenants/{t}/sessions/{s}/fork` | Explicit `at_seq`; pointer only. Exists today. |
| Inspect a session | `GET /api/tenants/{t}/sessions/{s}` | Sequence head, fork parent, lifecycle, agent. |
| Inspect a session | `GET /api/tenants/{t}/sessions` | List by workspace, optional agent filter. |
| Store or fetch an artifact | `POST /api/tenants/{t}/artifacts` | Raw bytes, `Idempotency-Key` honored (today it is parsed and dropped). |
| Store or fetch an artifact | `GET /api/tenants/{t}/artifacts/{a}` | Metadata. |
| Store or fetch an artifact | `GET /api/tenants/{t}/artifacts/{a}/blob` | Bytes. |
| Read changes | `GET /api/tenants/{t}/changes` | Ledger cursor, optional session scope, long-poll. See below. |

Workspace, agent, and session administration (create, get, delete, meta KV,
storage summaries) remains available on the existing tenant routes as
**Internal**, for operators and the SDK's own use. Cluster and maintenance
routes are not presented as application APIs and are mounted only in cluster
mode (Appendix A).

### The Twelve SDK Methods

| Method | Returns |
|---|---|
| `db.open_session(workspace, agent, session)` | session handle |
| `db.workspace(name)` | workspace handle |
| `session.append(event, fetch_recent=None)` | append result with optional tail |
| `session.recent(n)` | last n events |
| `session.replay(from_seq=None, limit=None)` | fork-aware event iterator |
| `session.fork(at_seq, session=None)` | new session handle |
| `session.info()` | session metadata |
| `workspace.sessions(agent=None)` | session list |
| `workspace.artifacts.put(bytes, media_type)` | artifact ref |
| `workspace.artifacts.get(id)` | artifact metadata |
| `workspace.artifacts.blob(id)` | bytes |
| `workspace.changes(cursor=None, session=None)` | changefeed page |

The event-shape helpers (`user_message`, `tool_call`, `tool_result`, and the
rest of `conventions`) stay as pure functions that build event dictionaries,
not as client methods. Context provenance helpers that walk records and skills
move to a Labs module. Today's 133 Python and 66 TypeScript methods, and the
parity gaps between them (TypeScript lacks lifecycle, storage, and
verification methods; neither SDK covers the nine export routes), collapse
into this table.

### Changefeed Consolidation

Five mechanisms express change consumption today, and they disagree on the
things that matter:

| Mechanism | Cursor | Order | Durable | Covers |
|---|---|---|---|---|
| Session subscribe | none | seq | n/a | one session, one page, SSE framing of fetch |
| Tenant watch | `frontier:v1` per-session seq map | wall-clock time first | recomputed from state each poll | events, fork-edge control items |
| Export watch | `ledger:` durable sequence | commit order | yes, written in the append batch | events plus 8 object types |
| Export backfill | `ledger:` | commit order | yes | same as export watch, plus a completeness flag |
| Managed consumers | server-stored `ledger:` | n/a | yes | claim, ack, rewind bookkeeping |

The export ledger is already written in the same atomic batch as every event,
so the durable commit-ordered substrate the product needs exists and is paid
for. The decision is therefore:

- The ledger cursor is the one public cursor. The `frontier:v1` format is
  retired, including the in-memory fallback that emits it from the export
  endpoint when no persist layer is configured. In-memory mode keeps an
  in-memory ledger.
- `GET /api/tenants/{t}/changes` reads the ledger with explicit workspace
  scope and optional session scope, commit-ordered items, bounded page size,
  long-poll, and `next_cursor`, `high_watermark`, and
  `oldest_available_cursor` for expiration and recovery.
- The `fork_edge` control item survives. Fork remains a pointer; the changefeed
  announces the edge rather than re-emitting ancestor history.
- Core items are events, session metadata, and artifact metadata. Record and
  skill items appear only when Labs is enabled.
- Backfill is the same read with a `snapshot_high_watermark` echo; it is an
  option on `changes`, not a route.
- Sink-specific checkpointing, claims, retries, and schema conversion belong
  in consumer libraries or operators. The managed-consumer routes are deleted.

The export endpoints are already refused in snapshot-routed cluster mode.
That is consistent with deferring the cluster product.

### One-Call Session Opening

Today `POST /api/tenants` with an existing `external_key` returns
`ALIAS_CONFLICT`, no create handler reads the `Idempotency-Key` header, and
both SDKs compensate with a list, create, catch 409, re-list loop at the
workspace, agent, and session levels. A cold start is six to nine requests,
each list an unbounded scan, and the race between concurrent openers is
resolved client-side.

`POST /api/sessions/open` fixes this server-side and defines the noun model's
key shape at the same time:

```python
session = db.open_session(
    workspace="acme",
    agent="sales-agent",
    session="ticket-42",
)
```

The returned handle is immediately usable for append, read, fork, and
artifact operations. Lower-level resource administration remains available to
operators without defining the beginner experience.

## Product And Demo Strategy

AgentDB and CodeMate currently represent two plausible products:

- AgentDB is infrastructure for agent platforms.
- CodeMate is a local engineering-review product for developers.

The recommended strategy is to keep AgentDB as the product and use CodeMate as
the canonical dogfood application. CodeMate today calls four endpoints (tenant,
agent, session create and event append) through raw HTTP with no SDK, and
sends empty artifact and source refs. It is unaffected by every cut in this
proposal, and it also exercises none of the recovery loop. Making it the
dogfood means adding artifact import, resume after interruption, replay, and
fork of a review window. That is product work, not relabeling.

CodeMate should not create an independent platform roadmap until customer
discovery shows that developers want the review product but do not want a
runtime database. If that evidence emerges, the relationship should reverse:
CodeMate becomes the product and AgentDB becomes its internal engine.

## Supported Deployment Shape

### The First Persona

The first single-node user is a developer running Claude Code or Codex on a
laptop who wants their agent sessions kept, searchable, and reviewable. They
will not read a production checklist. Easy means three things: install in one
command, never think about the server again, and see sessions appear without
doing anything per session.

Where that path stands at `52cc332`:

| Requirement | Status |
|---|---|
| Persistent server with no config file | Works: loopback bind, RocksDB under a default dir, no token on loopback, SQLite auth metadata inside the data dir, fsync on by default |
| External services required | None |
| Installable binary or image | None. The repo has no git tags, so the release and GHCR workflows have never run. The only documented install is a pip editable install from a checkout, and building needs clang and protobuf |
| Default data and config location | Relative to the working directory (`./data`, `./agentdb.toml`), so running from another shell creates a second database |
| Generated config template | Copies the checked-in file, which still documents MongoDB, SlateDB, and tablet domains that are removed or cluster-only |
| Getting Claude Code or Codex sessions in | No importer. The only parser is a Codex JSONL reader inside CodeMate's JavaScript; nothing reads Claude Code's `~/.claude/projects` JSONL |
| Running as a background service | No launchd or systemd support |
| Disk budget by default | None; quota and lifecycle machinery exist but ship without a default |
| A reason to keep it running | MCP adapter exposes replay and search; dashboard shows a timeline; CodeMate produces review packets from Codex only |

### The Supported Shape

The first supported release has one easy-to-explain deployment:

- one AgentDB binary, installed by Homebrew, `cargo install`, a release
  archive, or the GHCR image;
- machine-scoped defaults: data under `~/.agentdb` (or the XDG data dir),
  config at `~/.agentdb/agentdb.toml`, loopback bind, no token required on
  loopback;
- RocksDB metadata and local durable artifact storage;
- durable writes and basic encryption at rest enabled by default;
- a startup banner that prints the dashboard URL and the data path;
- `agentdb service install` writing a launchd or systemd user unit so the
  server is up whenever the agent runs;
- a config template regenerated from the single-node shape (bind, data dir,
  disk budget, retention); cluster, object-store, and Labs sections move to a
  separate example file;
- `agentdbctl import claude-code` and `agentdbctl import codex`, implemented
  in Rust with no runtime dependency, scanning `~/.claude/projects` and
  `~/.codex/sessions`: one JSONL file is one session, the repository path is
  the workspace, the session UUID is the alias, and the idempotency key is
  file path plus line offset so re-running is safe; `--watch` tails files for
  live import without hooks;
- a default disk budget in the low tens of gigabytes, shown in the dashboard,
  with large tool outputs spilled to artifacts, enforced by the existing quota
  and lifecycle machinery;
- Docker as the primary packaged example for non-laptop use;
- single-node production with published limits;
- health, metrics, backup, restore, and upgrade workflows; and
- no required background service beyond AgentDB itself.

The quickstart should end at an outcome, not an API call: install, import,
then ask the agent something about last week's session through the MCP
server, or open the dashboard timeline. Pull-based import is first because it
is agent-agnostic and needs no hook configuration, which is the same argument
[`codemate-product-note.md`](codemate-product-note.md) makes for CodeMate.
Claude Code hooks can stream live events later as an optional layer on the
same importer mapping.

### Cluster Mode

There are no Cargo feature flags in the workspace. Distributed mode is gated by
`deployment.mode = "cluster"` in configuration, and the Raft workers are
actually switched on by the presence of `internal_auth_token`. The 35 cluster,
node, cell, store, tablet, routing, and Raft admin routes are mounted
unconditionally. The change is therefore a mounting rule, not a feature flag:
those routes, the Raft workers, and the transport-fault endpoints are mounted
only when the mode is cluster. Single-node deployments never see them.

Helm and cluster documentation may remain for evaluation, but should not imply
the same support level as the single-node product.

## Stability Labels

Every endpoint, SDK method, runtime object, configuration field, and operator
workflow should have one of three labels:

- **Core:** documented in the main path, covered by maintained SDKs, tested for
  compatibility, and supported operationally.
- **Labs:** callable behind the `labs` switch, but explicitly unstable and
  absent from the beginner path and compatibility promise.
- **Internal:** implementation or operator machinery with no application-level
  compatibility promise.

Until an OpenAPI document is generated, the label lives in the route table in
`crates/agentdb-api/src/routes.rs` as a per-route attribute, and a test
asserts that every Core route is covered by both SDKs and every Labs route is
absent from the quickstart. Appendix A is the initial assignment. Labeling
without reducing SDK, documentation, testing, and support obligations does not
simplify the product.

## Documents This Proposal Amends

The current docs tell both humans and coding agents to build the old model.
The same change that adopts this proposal must edit:

- [`product-thesis.md`](product-thesis.md): the Core Primitives section lists
  eight primitives including Record and Skill, and the positioning bullets
  name records, skills, and framework adapters as reasons to adopt.
- [`../../AGENTS.md`](../../AGENTS.md): the "preserve the agent-runtime model"
  principle tells agents that records, skills, context, and lifecycle
  operations are what AgentDB is optimized for.
- [`../../LLMS.md`](../../LLMS.md): "the five operations" include search
  records and build context.
- [`design.md`](design.md): the implementation status table gains the
  Core/Labs/Internal column.
- [`../reference/api-sdk.md`](../reference/api-sdk.md) and
  [`../quickstart.md`](../quickstart.md): reorganized around the ten paths
  and twelve methods.
- [`../product-roadmap.md`](../product-roadmap.md): the Implemented list is
  split by label.

## Release Gates

A simplified first release should not be called ready until it meets these
measurable gates. Status is as of `385069f`.

### Developer Experience

- A developer goes from nothing installed to their first imported Claude Code
  or Codex session in under 5 minutes with no source checkout. *Blocked on a
  release and the importer.*
- A new user reaches the first durable append in under 5 minutes from a clean
  checkout or container environment. *Unmeasured.*
- The first append requires no more than three concepts and one
  resource-opening call. *Blocked on `open_session`.*
- The stable SDK presents the twelve methods above. *Not started.*
- The stable application API presents the ten paths above. *Not started.*
- One example demonstrates append, process crash, resume, artifact recovery,
  replay, and fork without framework-specific code. *Not started; the
  quickstarts cover append and fork only.*

### Reliability

- A SIGKILL recovery test proves that every acknowledged append survives.
  *Done: `ops/local-ci/tests/auto/S14_acknowledged_appends_survive_sigkill.sh`.*
- Idempotent retry is tested across a lost response and server restart.
  *Idempotency rows are written in the same batch as the event; a
  restart-spanning retry test is not verified.*
- Artifact metadata and bytes have a tested failure-cleanup and recovery path.
  *Local failure cleanup and startup orphan reconciliation exist; object-store
  listing does not.*
- Backup restoration is tested into a clean installation. *Runbooks exist; an
  automated test is not verified.*
- Retention and cleanup have byte and item bounds, observable progress, and
  documented failure behavior. *Partially; progress observability is a gap.*

### Capacity And Operations

- Startup time, steady-state memory, append throughput, and recent-tail latency
  are published for at least 1 million and 10 million events.
- Tests state event-size, artifact-size, active-session, total-session, and
  concurrency assumptions.
- Single-node limits are explicit rather than hidden behind a distributed
  roadmap.
- The deployment requires no mandatory service beyond the AgentDB process and
  its durable volumes.
- The default disk budget is enforced and visible, and a full budget degrades
  to a documented behavior rather than a failed process.

## Proposed Sequencing

The inventory the original proposal scheduled as its first phase is done
(Appendices A and B). The remaining work is one packaging change, two
cut-sized changes, and a validation loop. Each change is one PR that includes
its documentation edits. Change 0 does not depend on the others and has the
highest leverage, because nothing else is visible to a user until a binary
exists.

### Change 0: Make It Installable

1. Tag `v0.1.0` so the existing release and Docker workflows publish the four
   platform archives and the GHCR image. Add a Homebrew tap and document
   `cargo install`.
2. Move default data and config locations to the user's home or XDG
   directories; print both in the startup banner.
3. Regenerate the config template from the single-node shape; move cluster,
   object-store, and Labs sections to a separate example.
4. Add `agentdb service install` for launchd and systemd.
5. Add `agentdbctl import claude-code` and `agentdbctl import codex` with
   `--watch`, porting the Codex parser out of CodeMate into Rust and adding a
   Claude Code parser.
6. Ship a default disk budget with dashboard visibility.
7. Rewrite the quickstart to end at the MCP or dashboard outcome.

### Change 1: Narrow The Surface

1. Mount cluster, tablet, routing, Raft, node, cell, store, and
   transport-fault admin routes and workers only when `deployment.mode` is
   `cluster`.
2. Add `POST /api/sessions/open` with server-side idempotency on aliases, and
   honor `Idempotency-Key` on artifact upload.
3. Add `fetch_recent` to append; mark `append-and-fetch` deprecated.
4. Introduce the `labs` configuration switch; gate records, record search,
   skills, context, caller-driven lifecycle routes, replay-prefix, and the
   `skill_used` and record/skill source-ref validation in append behind it.
   If the event-search decision is confirmed, add scoped event search as a
   core route and SDK method, and count it in the Core tables.
5. Delete record graph edges and session subscribe.
6. Add the route label attribute and the SDK-coverage test.
7. Amend the documents listed above.

### Change 2: One Changefeed

1. Add `GET /api/tenants/{t}/changes` on the export ledger with the semantics
   above, including `fork_edge` and the backfill option.
2. Reimplement `watch` as an alias of `changes` for one release, then delete
   it along with the frontier cursor and the in-memory frontier fallback.
3. Delete managed consumers and the separate export watch, backfill, and
   status routes.
4. Add `changes` to both SDKs; remove the methods not in the twelve.
5. Add the `/metrics` endpoint.

### Validation

1. Ship the crash, resume, artifact-recovery, replay, and fork example and
   make it the release smoke test.
2. Extend CodeMate to exercise that loop against real coding-agent sessions.
3. Run at least five integration studies with agent-platform developers.
   Record time to first append, concepts that required explanation, recovery
   failures, and requested missing capabilities.
4. Re-measure route count, SDK method count, binary size, compile time, test
   time, and startup memory.
5. Promote a Labs capability only when multiple users require it and the core
   event/artifact model cannot serve the need through an integration.

Moving Labs code into separately versioned packages is not scheduled. It costs
a release process and buys nothing until a second consumer exists.

## Non-Goals Of This Proposal

This proposal does not argue that search, skills, context, lifecycle, cluster
storage, or encryption management are intrinsically unimportant. It argues
that supporting them all as first-class product concepts before the durable
session-log wedge is validated increases cognitive and operational cost.

It does not authorize deletion beyond the Delete list above, each item of
which was checked for consumers in Appendix B.

## Decision Questions

Answers reflect the inventory; the team should confirm them.

1. **AgentDB the product, CodeMate the dogfood?** Yes. CodeMate is an MVP
   with one commit, one unit test, and four HTTP calls. It is not yet a
   product on its own.
2. **Are records and search required by a design partner?** No. The only
   substantial consumer is the LangGraph adapter, whose `BaseStore` and
   thread index are built on records and tag search. That is an adapter
   proof, not a partner. It moves to Labs with records. The MCP adapter
   loses one of four tools; the Vercel template's record use is trivial; the
   OpenAI Agents adapter is unaffected.
3. **Are skills runtime objects or artifacts?** Artifacts. No adapter,
   example, or application uses the skill registry; only the SDK shape tests
   do.
4. **Which requirement justifies exposing the tablet control plane?** None
   identified. The roadmap and thesis both call it evaluation machinery.
5. **Which customer requires AgentDB-owned BYOK rotation?** None identified —
   yet the full key-management stack (wrapped-DEK catalog, KMS/HSM seam, rotation,
   revocation/crypto-shred, re-encryption, version retirement, CLI) was built
   anyway (M6–M24), ahead of that evidence. Recommendation: keep it, but label
   it. Basic envelope encryption is **Core** and on by default; the
   rotate/revoke/re-encrypt/retire admin endpoints and `agentdbctl encryption`
   CLI are **Internal** (operator-only, no compatibility promise) and stay out of
   the quickstart and the twelve SDK methods; the `CommandKekProvider` KMS seam
   is Core-enabling configuration, not an advertised concept. This keeps the
   beginner surface at the four nouns without discarding shipped, tested work.
6. **Can subscribe, watch, and export be one cursor contract?** Yes. The
   export ledger already carries every event in commit order; watch's only
   unique feature is the `fork_edge` item, which the changefeed keeps.
7. **Does scoped event search belong in core?** Recommended yes, for events
   only. The single-node persona's payoff is asking an agent about its own
   past sessions, and that should not require enabling Labs. Record search,
   vector search, and record indexes stay in Labs. Confirming this adds one
   route and one SDK method to the Core tables.

## Appendix A: Route Classification

Counts are registered paths at `385069f`.

| Label | Family | Paths | Disposition |
|---|---|---|---|
| Core | health | 2 | keep |
| Core | session events, replay, fork, get, list | 5 | `open` is a new path; `append-and-fetch` folds into `events` |
| Core | artifacts | 3 | keep; `verify-lifecycle` moves to Labs |
| Core | open, changes | 2 (new) | not counted in the 101 |
| Internal | tenant create/get/list/delete, storage summary, meta KV | 4 | keep for operators and SDK |
| Internal | agent create/get/list/delete, session create under agent | 3 | keep; path shape no longer required by applications |
| Internal | `/api/me`, `/api/storage/summary`, `/api/policy/worker/run` | 3 | keep |
| Internal | tokens, tenant policies, indexes, content mirror, recovery, backups, restores, exports admin | 10 | keep, always mounted |
| Internal | encryption DEK admin: rotate, revoke, reencrypt, retire-versions | 4 | keep, always mounted; added post-inventory (M15–M23), not in the 101 |
| Internal | cluster, nodes, cells, stores, scheduler, drain, tablets, tablet ranges, movements, routing, Raft | 35 | mounted only in cluster mode |
| Internal | `/api/internal/*` Raft messages, snapshots, faults, movement export | 5 | mounted only in cluster mode |
| Labs | records, upsert, touch, search | 5 | behind `labs` |
| Labs | skills, agent skills, bind | 4 | behind `labs` |
| Labs | context | 1 | behind `labs` |
| Labs | session lifecycle, storage, restore, replay-prefix; policy sweeps; artifact verify-lifecycle | 7 | behind `labs` |
| Delete | record edges | 3 | no consumer |
| Delete | subscribe | 1 | fetch with SSE framing |
| Delete | watch | 1 | replaced by `changes` |
| Delete | exports watch, backfill, status | 3 | replaced by `changes` |
| Delete | export consumers, claim, ack, rewind | 5 | client-side concern |
| Delete | append-and-fetch | 1 | folded into `events` |

The dashboard's five routes are Labs.

## Appendix B: Dependency Inventory

Consumers of each demoted or deleted capability outside `agentdb-core` and
`agentdb-api`, at `385069f`.

| Capability | Consumers | Coupling inside the runtime |
|---|---|---|
| Records, search, indexes | LangGraph adapter (hard: store and thread index), MCP adapter (one tool), Vercel template (stream state, trivial), SDK `write_record`/`search_records`, SDK v2-shape tests | Separable slab (`runtime.rs` 10779 to 12186, `agentdb-index`), but shares the state struct, startup index rebuild, and an unconditional index worker spawned in the server main; append validates record source refs |
| Record graph edges | none | two route handlers, three runtime functions |
| Skills, bindings | SDK methods and v2-shape tests only | append validates `skill_used` and skill source refs |
| Context assembly | two Python examples, SDK `context()`, conventions provenance helpers and tests | pure composition over fetch, search, skills, and policy; nothing else depends on it |
| Lifecycle transitions, replay-prefix | two Python examples and their offline tests, SDK `transition_lifecycle` | auto-thaw on append is inline in the commit path and stays |
| Sandbox snapshot metadata | none | one runtime function |
| Subscribe | two pubsub examples, SDK `subscribe` | none; it is fetch |
| Watch | SDK `watch` | scans in-scope sessions under the state lock |
| Export ledger | no SDK coverage; operator CLI and ClickHouse tooling | four KV ops per event in the append batch; keep |
| Managed consumers | no SDK coverage | in-memory map plus persisted rows |
| Tablets, Raft, routing | cluster smoke tests, Helm example | route context threaded through the runtime; mount-time gating is sufficient |
| CodeMate | tenant, agent, session create and event append only | none |
