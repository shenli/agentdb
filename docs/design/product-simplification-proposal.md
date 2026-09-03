# AgentDB Product Simplification Proposal

**Status:** Proposal, revised 2026-09-02 — skills moved to Delete, CodeMate
strategy removed with the app, and Change 0 marked shipped. Inventory taken at
`main` @ `385069f`; deployment status checked against merged work through
`#330`.
**Scope:** Product boundary, public API, packaging, and sequencing
**Does not change:** Compatibility commitments or runtime behavior

## Decision Summary

AgentDB should focus its first supported product on one promise:

> AgentDB is a durable, replayable session log for AI agents.

Evaluated against one recovery loop:

```text
append -> crash -> resume -> replay or fork
```

The first release should make this loop durable, understandable, observable,
and easy to operate on a single node. Features that do not materially improve
this loop move out of the core product surface until customer evidence
justifies their maintenance and operational cost.

This proposal narrows the public model to four concepts, consolidates the
application API around six intents on ten paths, names the twelve SDK methods,
classifies every route, and defines the single-node persona and its install
path. The packaging change (Change 0) has shipped; two cut-sized changes and a
validation loop remain.

## Why Simplify Now

External positioning is focused: AgentDB is the durable session log for an
agent run. The implemented surface is much broader. Measured at `385069f`:

| Surface | Measured |
|---|---|
| Registered HTTP paths | 101 (45 `/api/admin/*`, 46 `/api/tenants/*`, 10 other) |
| Method-handler pairs | 119 |
| Native runtime object families | 9, plus tenant meta KV and skill bindings |
| Change-consumption mechanisms | 5 (subscribe, watch, export watch, backfill, managed consumers) |
| `agentdb-core/src/runtime.rs` | 14,074 lines (core crate is 20,807) |
| `agentdb-api/src/routes.rs` | 8,642 lines |
| Python / TypeScript SDK public methods | 133 / 66 |
| Cargo feature flags | 0; all cluster, tablet, and Raft code compiles into every binary |

These are not defects, but each new user, SDK, release, and operator workflow
must learn and maintain several product layers before the primary value
proposition is established. AgentDB is pre-release, so this is the least
expensive point at which to reduce public commitments. The August 2026 design
review ([`../design-review-2026-08.md`](../design-review-2026-08.md), finding 6)
recommended freezing new distributed surface until the single-node runtime is
excellent; this proposal extends that to the product surface as a whole.

## Relationship To Work In Flight

- **Distributed hardening (#295–#301).** Frozen at #301. Existing cluster tests
  stay green in CI. No new tablet, routing, Raft, or movement surface is added
  until a measured single-node limit or a named design partner requires it.
- **Encryption at rest.** Authenticated encryption on every durable path
  (content, events, metadata) under a locally configured key, with restore
  validated, is *basic encryption* and is core. Customer-managed key catalogs
  and BYOK rotation remain deferred.

## Proposed Product Boundary

### Core

Only the capabilities required to record, recover, inspect, and branch agent
execution:

- durable single-node storage; workspace isolation and authentication;
- sessions and ordered, append-only events;
- append with optional recent-tail return and durable idempotency;
- recent reads and full fork-aware replay; pointer-based forks;
- artifacts and event-to-artifact references;
- one cursor-based changefeed backed by the export ledger;
- bounded retention and cleanup with safe defaults;
- basic encryption at rest as defined above;
- health probes, a metrics endpoint, backup, restore, and crash-recovery tooling;
- session importers for Claude Code and Codex, with a watch mode; and
- maintained Python and TypeScript SDKs.

Two items still to build rather than label: there is no `/metrics` endpoint
(metrics appear only inside admin JSON), and there is no generated API spec
(`utoipa` is a dependency, but no route is annotated).

### Labs

Compiled in and callable, gated by a single `labs` configuration switch, but not
required concepts in the quickstart, stable SDK, or compatibility contract:

- records and scoped search, including text and vector indexes;
- server-side context assembly;
- lifecycle tier transitions, archive restore, and replay-prefix materialization
  as caller-invoked operations;
- sandbox snapshot metadata;
- framework-specific adapters; and
- rich dashboard views.

Labs capabilities consume or project the session log; they add nothing to the
synchronous append path beyond what is already there (Appendix B).

One exception: scoped text search over **events** within a workspace or session
returns to core, because the single-node persona (below) needs "what did we do
about this last week" over imported coding-agent history, and the MCP tool that
answers it should not depend on Labs. Search over records, vector search, and
record indexes stay in Labs.

### Delete

No consumer outside the core crate and its HTTP routes; remove rather than
label:

- **skill registry and bindings** — the `skill_used` event kind and the skill
  source-ref validation in the append path, the skill/agent-skill/bind routes,
  and the SDK skill methods. No adapter, example, or application uses the skill
  registry; only the SDK shape tests do (Decision Q3);
- record graph edges (three routes, no SDK method, adapter, example, or test
  outside core);
- session subscribe (a one-shot SSE framing of the events fetch);
- the frontier-cursor `watch`, once the ledger-backed changefeed replaces it;
- managed export consumers (server-side storage of a cursor the client holds); and
- the separate `append-and-fetch` route, once `append` accepts the recent-tail
  option.

### Deferred

Wait for a named design partner, a measured capacity limit, or a hosted-product
requirement:

- production distributed tablets and their public control-plane API;
- additional SDK languages or a binary application protocol;
- sink-specific export products beyond the changefeed;
- customer-managed key catalogs, rotation, and revocation workflows; and
- automated multi-tier storage orchestration beyond bounded retention.

## Simplified Public Model

The primary documentation and SDKs teach four nouns.

| Concept | Meaning |
|---|---|
| Workspace | Isolation, authentication, quota, and retention boundary. Stored as a tenant; HTTP paths keep the `/api/tenants/{id}` prefix. |
| Session | One durable agent execution or branch. |
| Event | An ordered fact about what happened during the execution. |
| Artifact | Large or binary data referenced by an event. |

Everything else becomes an attribute, reference, projection, or internal
mechanism:

- Agent identity is a session attribute and an event field. Sessions open by
  (workspace, agent, session) aliases; the `/agents/{id}/sessions` path shape and
  the SDK agent handle go away. This is an API break.
- A fork is a relationship between sessions (`forked_from`), not a top-level
  object. This is already how it is implemented.
- A skill is an artifact or external reference recorded by an event. The
  `skill_used` event kind, skill source-ref validation, and the `skill_id`
  source-ref field are removed.
- A record is a projection derived from events. The `record_id` source-ref field
  becomes Labs-only; core source refs carry event and artifact references.
- A context pack is an SDK or harness decision whose provenance is written as an
  event.
- Lifecycle and policy are workspace configuration with automatic defaults. The
  warm/cold/archive machinery and auto-thaw on append stay in the runtime; only
  the caller-driven transition routes move to Labs.

## Simplified Application API

Six intents: eleven route handlers on ten paths, plus two health probes. Two
paths are new (`open` and `changes`); the rest exist today.

| Intent | Route | Notes |
|---|---|---|
| Open a session | `POST /api/sessions/open` | Idempotent on (workspace, agent, session) aliases. Creates whatever is missing, returns the session. Replaces the list/create/catch-409/re-list loop the SDKs run today. |
| Append an event | `POST /api/tenants/{t}/sessions/{s}/events` | `Idempotency-Key` header; optional `fetch_recent=N` returns the tail in the same call. Absorbs `append-and-fetch`. |
| Read history | `GET /api/tenants/{t}/sessions/{s}/events` | `latest=true&limit=N` for the tail, `from_seq` for a slice. |
| Read history | `GET /api/tenants/{t}/sessions/{s}/replay` | Fork-aware, from an ancestor-inclusive sequence. |
| Fork a session | `POST /api/tenants/{t}/sessions/{s}/fork` | Explicit `at_seq`; pointer only. Exists today. |
| Inspect a session | `GET /api/tenants/{t}/sessions/{s}` | Sequence head, fork parent, lifecycle, agent. |
| Inspect a session | `GET /api/tenants/{t}/sessions` | List by workspace, optional agent filter. |
| Store or fetch an artifact | `POST /api/tenants/{t}/artifacts` | Raw bytes, `Idempotency-Key` honored. |
| Store or fetch an artifact | `GET /api/tenants/{t}/artifacts/{a}` | Metadata. |
| Store or fetch an artifact | `GET /api/tenants/{t}/artifacts/{a}/blob` | Bytes. |
| Read changes | `GET /api/tenants/{t}/changes` | Ledger cursor, optional session scope, long-poll. |

Workspace, agent, and session administration (create, get, delete, meta KV,
storage summaries) stays on the existing tenant routes as **Internal**, for
operators and the SDK's own use. Cluster and maintenance routes are mounted only
in cluster mode (Appendix A).

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
rest of `conventions`) stay as pure functions that build event dictionaries, not
client methods. Record provenance helpers move to a Labs module; skill
provenance helpers are removed. Today's 133 Python and 66 TypeScript methods, and
the parity gaps between them, collapse into this table.

### Changefeed Consolidation

Five mechanisms express change consumption today and disagree on what matters:

| Mechanism | Cursor | Order | Durable | Covers |
|---|---|---|---|---|
| Session subscribe | none | seq | n/a | one session, one page, SSE framing of fetch |
| Tenant watch | `frontier:v1` per-session seq map | wall-clock time first | recomputed each poll | events, fork-edge control items |
| Export watch | `ledger:` durable sequence | commit order | yes | events plus 8 object types |
| Export backfill | `ledger:` | commit order | yes | same, plus a completeness flag |
| Managed consumers | server-stored `ledger:` | n/a | yes | claim, ack, rewind bookkeeping |

The export ledger is already written in the same atomic batch as every event, so
the durable commit-ordered substrate the product needs already exists. Therefore:

- The ledger cursor is the one public cursor. `frontier:v1` is retired, including
  the in-memory fallback; in-memory mode keeps an in-memory ledger.
- `GET /api/tenants/{t}/changes` reads the ledger with explicit workspace scope
  and optional session scope, commit-ordered items, bounded page size, long-poll,
  and `next_cursor`, `high_watermark`, and `oldest_available_cursor`.
- The `fork_edge` control item survives. Fork stays a pointer; the changefeed
  announces the edge rather than re-emitting ancestor history.
- Core items are events, session metadata, and artifact metadata. Record items
  appear only when Labs is enabled.
- Backfill is the same read with a `snapshot_high_watermark` echo — an option on
  `changes`, not a route.
- Sink-specific checkpointing, claims, retries, and schema conversion belong in
  consumer libraries. The managed-consumer routes are deleted.

### One-Call Session Opening

Today `POST /api/tenants` with an existing `external_key` returns
`ALIAS_CONFLICT`, no create handler reads `Idempotency-Key`, and both SDKs
compensate with a list/create/catch-409/re-list loop at three levels — a cold
start is six to nine requests, each list an unbounded scan.
`POST /api/sessions/open` fixes this server-side and defines the key shape:

```python
session = db.open_session(
    workspace="acme",
    agent="sales-agent",
    session="ticket-42",
)
```

The returned handle is immediately usable for append, read, fork, and artifact
operations. Lower-level resource administration stays available to operators
without defining the beginner experience.

## Supported Deployment Shape

### The First Persona

The first single-node user is a developer running Claude Code or Codex on a
laptop who wants their agent sessions kept, searchable, and reviewable. They
will not read a production checklist. Easy means three things: install in one
command, never think about the server again, and see sessions appear without
doing anything per session.

Where that path stands after the Change-0 work merged (#327–#329):

| Requirement | Status |
|---|---|
| Persistent server with no config file | Works: loopback bind, RocksDB under a default dir, no token on loopback, fsync on by default |
| External services required | None |
| Installable binary or image | Still blocked: no git tags, so the release and GHCR workflows have never run. Deferred with release tagging. |
| Default data and config location | **Done** (#327): machine-scoped (`$AGENTDB_HOME`, then `$XDG_DATA_HOME/agentdb`, then `~/.agentdb`), printed in the startup banner |
| Generated config template | **Done** (#327): regenerated single-node template; cluster/object-store example split out |
| Getting Claude Code or Codex sessions in | **Done** (#327): `agentdbctl import claude-code` and `import codex`, Rust, no runtime dependency, idempotent, `--watch` |
| Running as a background service | **Done** (#327): `agentdb service install` for launchd and systemd |
| Disk budget by default | **Done** (#329): advisory ~20 GiB default, banner + dashboard, never blocks writes |
| A reason to keep it running | MCP adapter exposes replay and search; dashboard shows a timeline |

### The Supported Shape

The first supported release has one easy-to-explain deployment:

- one AgentDB binary, installed by Homebrew, `cargo install`, a release archive,
  or the GHCR image;
- machine-scoped defaults: data under `~/.agentdb` (or the XDG data dir), config
  at `~/.agentdb/agentdb.toml`, loopback bind, no token on loopback;
- RocksDB metadata and local durable artifact storage;
- durable writes and basic encryption at rest enabled by default;
- a startup banner that prints the dashboard URL and the data path;
- `agentdb service install` writing a launchd or systemd user unit;
- a single-node config template, with cluster/object-store/Labs sections in a
  separate example file;
- `agentdbctl import claude-code` and `agentdbctl import codex`: one JSONL file is
  one session, the repository path is the workspace, the session UUID is the
  alias, and the idempotency key is file path plus line offset; `--watch` tails
  files for live import without hooks;
- a default disk budget in the low tens of gigabytes, shown in the dashboard,
  with large tool outputs spilled to artifacts;
- Docker as the primary packaged example for non-laptop use;
- single-node production with published limits;
- health, metrics, backup, restore, and upgrade workflows; and
- no required background service beyond AgentDB itself.

The quickstart ends at an outcome, not an API call: install, import, then ask the
agent something about last week's session through the MCP server, or open the
dashboard timeline. Pull-based import is first because it is agent-agnostic and
needs no hook configuration; Claude Code hooks can stream live events later as an
optional layer on the same importer mapping.

### Cluster Mode

There are no Cargo feature flags. Distributed mode is gated by
`deployment.mode = "cluster"` in configuration, and the Raft workers are switched
on by the presence of `internal_auth_token`. The 35 cluster/node/cell/store/
tablet/routing/Raft admin routes are mounted unconditionally today; the change is
a mounting rule, not a feature flag: those routes, the Raft workers, and the
transport-fault endpoints are mounted only when the mode is cluster. Single-node
deployments never see them. Helm and cluster documentation may remain for
evaluation but should not imply the single-node support level.

## Demo Strategy

AgentDB is the product: infrastructure for agent platforms. The demo corpus is
the developer's own Claude Code and Codex history, imported by `agentdbctl` — real
sessions, not a synthetic app. The "reason to keep it running" is the MCP adapter
(replay and search over that history) and the dashboard timeline. The release
smoke test is the recovery-loop example (append, crash, resume, artifact
recovery, replay, fork), run against imported coding-agent sessions.

## Stability Labels

Every endpoint, SDK method, runtime object, configuration field, and operator
workflow should carry one of three labels:

- **Core:** documented in the main path, covered by maintained SDKs, tested for
  compatibility, supported operationally.
- **Labs:** callable behind the `labs` switch, explicitly unstable, absent from
  the beginner path and compatibility promise.
- **Internal:** implementation or operator machinery with no application-level
  compatibility promise.

Until an OpenAPI document is generated, the label lives as a per-route attribute
in `crates/agentdb-api/src/routes.rs`, with a test asserting that every Core route
is covered by both SDKs and every Labs route is absent from the quickstart.
Appendix A is the initial assignment. Labeling without reducing SDK,
documentation, testing, and support obligations does not simplify the product.

## Documents This Proposal Amends

The current docs still tell humans and coding agents to build the old model. The
same change that adopts this proposal must edit:

- [`product-thesis.md`](product-thesis.md): the Core Primitives section lists
  eight primitives including Record and Skill; drop Skill, demote Record.
- [`../../AGENTS.md`](../../AGENTS.md): the "preserve the agent-runtime model"
  principle names skills and records as what AgentDB is optimized for.
- [`../../LLMS.md`](../../LLMS.md): "the five operations" include search records
  and build context.
- [`design.md`](design.md): the implementation status table gains the
  Core/Labs/Internal column.
- [`../reference/api-sdk.md`](../reference/api-sdk.md) and
  [`../quickstart.md`](../quickstart.md): reorganized around the ten paths and
  twelve methods (quickstart already rewritten in #328).
- [`../product-roadmap.md`](../product-roadmap.md): the Implemented list is split
  by label.

## Release Gates

A simplified first release is not ready until it meets these measurable gates.

### Developer Experience

- Nothing installed to first imported Claude Code or Codex session in under 5
  minutes with no source checkout. *Importer done (#327); blocked on a release.*
- First durable append in under 5 minutes from a clean checkout or container.
  *Unmeasured.*
- First append needs no more than three concepts and one resource-opening call.
  *Blocked on `open_session` (Change 1).*
- The stable SDK presents the twelve methods above. *Not started.*
- The stable application API presents the ten paths above. *Not started.*
- One example demonstrates append, crash, resume, artifact recovery, replay, and
  fork without framework-specific code. *Not started; quickstarts cover append
  and fork only.*

### Reliability

- A SIGKILL recovery test proves every acknowledged append survives. *Done:
  `ops/local-ci/tests/auto/S14_acknowledged_appends_survive_sigkill.sh`.*
- Idempotent retry tested across a lost response and server restart. *Idempotency
  rows write in the event batch; a restart-spanning retry test is not verified.*
- Artifact metadata and bytes have a tested failure-cleanup and recovery path.
  *Local cleanup and startup orphan reconciliation exist; object-store listing
  does not.*
- Backup restoration tested into a clean installation. *Runbooks exist; an
  automated test is not verified.*
- Retention and cleanup have byte and item bounds, observable progress, and
  documented failure behavior. *Partially; progress observability is a gap.*

### Capacity And Operations

- Startup time, steady-state memory, append throughput, and recent-tail latency
  published for at least 1M and 10M events.
- Tests state event-size, artifact-size, active-session, total-session, and
  concurrency assumptions.
- Single-node limits are explicit rather than hidden behind a distributed roadmap.
- The deployment requires no mandatory service beyond the AgentDB process and its
  durable volumes.
- The default disk budget is visible (**done**, #329) and degrades to a documented
  behavior rather than a failed process (advisory-only by design).

## Proposed Sequencing

The inventory and the packaging change are done. What remains is two cut-sized
changes and a validation loop. Each change is one PR that includes its
documentation edits.

### Change 0: Make It Installable — **shipped except release tagging**

1. Tag `v0.1.0` so the release and Docker workflows publish the platform
   archives and the GHCR image; add a Homebrew tap and document `cargo install`.
   *Deferred by decision.*
2. Machine-scoped data and config locations, printed in the banner. **Done (#327).**
3. Single-node config template; cluster/object-store example split out. **Done (#327).**
4. `agentdb service install` for launchd and systemd. **Done (#327).**
5. `agentdbctl import claude-code` and `import codex` with `--watch`. **Done (#327).**
6. Default disk budget with dashboard visibility. **Done (#329).**
7. Rewrite the quickstart to end at the MCP or dashboard outcome. **Done (#328).**

### Change 1: Narrow The Surface

1. Mount cluster/tablet/routing/Raft/node/cell/store/transport-fault admin routes
   and workers only when `deployment.mode` is `cluster`.
2. Add `POST /api/sessions/open` with server-side idempotency on aliases, and
   honor `Idempotency-Key` on artifact upload.
3. Add `fetch_recent` to append; mark `append-and-fetch` deprecated.
4. Introduce the `labs` configuration switch; gate records, record search,
   context, caller-driven lifecycle routes, and replay-prefix behind it. If the
   event-search decision is confirmed, add scoped event search as a core route
   and SDK method.
5. Delete the skill registry, bindings, `skill_used`, skill source-ref validation,
   and SDK skill methods; delete record graph edges and session subscribe.
6. Add the route label attribute and the SDK-coverage test.
7. Amend the documents listed above.

### Change 2: One Changefeed

1. Add `GET /api/tenants/{t}/changes` on the export ledger with the semantics
   above, including `fork_edge` and the backfill option.
2. Reimplement `watch` as an alias of `changes` for one release, then delete it
   along with the frontier cursor and the in-memory frontier fallback.
3. Delete managed consumers and the separate export watch, backfill, and status
   routes.
4. Add `changes` to both SDKs; remove the methods not in the twelve.
5. Add the `/metrics` endpoint.

### Validation

1. Ship the crash/resume/artifact-recovery/replay/fork example and make it the
   release smoke test, run against real imported coding-agent sessions.
2. Run at least five integration studies with agent-platform developers. Record
   time to first append, concepts that required explanation, recovery failures,
   and requested missing capabilities.
3. Re-measure route count, SDK method count, binary size, compile time, test
   time, and startup memory.
4. Promote a Labs capability only when multiple users require it and the core
   event/artifact model cannot serve the need through an integration.

Moving Labs code into separately versioned packages is not scheduled; it costs a
release process and buys nothing until a second consumer exists.

## Non-Goals Of This Proposal

This proposal does not argue that search, context, lifecycle, cluster storage, or
encryption management are intrinsically unimportant. It argues that supporting
them all as first-class product concepts before the durable session-log wedge is
validated increases cognitive and operational cost. It does not authorize
deletion beyond the Delete list above, each item checked for consumers in
Appendix B.

## Decision Questions

Answers reflect the inventory; the team should confirm them.

1. **Are records and search required by a design partner?** No. The only
   substantial consumer is the LangGraph adapter, whose `BaseStore` and thread
   index build on records and tag search — an adapter proof, not a partner. It
   moves to Labs with records. The MCP adapter loses one of four tools; the Vercel
   template's record use is trivial; the OpenAI Agents adapter is unaffected.
2. **Are skills runtime objects or artifacts?** Artifacts, and now removed. No
   adapter, example, or application uses the skill registry; only the SDK shape
   tests do. It moves to Delete rather than Labs.
3. **Which requirement justifies exposing the tablet control plane?** None
   identified. The roadmap and thesis both call it evaluation machinery.
4. **Which customer requires AgentDB-owned BYOK rotation?** None identified. Basic
   encryption stays core as defined above.
5. **Can subscribe, watch, and export be one cursor contract?** Yes. The export
   ledger already carries every event in commit order; watch's only unique feature
   is the `fork_edge` item, which the changefeed keeps.
6. **Does scoped event search belong in core?** Yes, for events only —
   **now shipped** as `GET /api/tenants/{t}/events/search` (a scoped, unindexed
   text scan) plus `search_events`/`searchEvents` in both SDKs. The persona's
   payoff is asking an agent about its own past sessions, and that should not
   require Labs. Record search, vector search, and record indexes stay in Labs.

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
| Internal | cluster, nodes, cells, stores, scheduler, drain, tablets, tablet ranges, movements, routing, Raft | 35 | mounted only in cluster mode |
| Internal | `/api/internal/*` Raft messages, snapshots, faults, movement export | 5 | mounted only in cluster mode |
| Labs | records, upsert, touch, search | 5 | behind `labs` |
| Labs | context | 1 | behind `labs` |
| Labs | session lifecycle, storage, restore, replay-prefix; policy sweeps; artifact verify-lifecycle | 7 | behind `labs` |
| Delete | skills, agent skills, bind | 4 | no consumer but SDK shape tests |
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
| Records, search, indexes | LangGraph adapter (hard: store and thread index), MCP adapter (one tool), Vercel template (trivial), SDK `write_record`/`search_records`, SDK v2-shape tests | Separable slab (`runtime.rs` 10779–12186, `agentdb-index`), but shares the state struct, startup index rebuild, and an unconditional index worker in server main; append validates record source refs |
| Record graph edges | none | two route handlers, three runtime functions |
| Skills, bindings | SDK methods and v2-shape tests only | append validates `skill_used` and skill source refs; delete removes both |
| Context assembly | two Python examples, SDK `context()`, provenance helpers and tests | pure composition over fetch, search, and policy; nothing else depends on it |
| Lifecycle transitions, replay-prefix | two Python examples and their offline tests, SDK `transition_lifecycle` | auto-thaw on append is inline in the commit path and stays |
| Sandbox snapshot metadata | none | one runtime function |
| Subscribe | two pubsub examples, SDK `subscribe` | none; it is fetch |
| Watch | SDK `watch` | scans in-scope sessions under the state lock |
| Export ledger | no SDK coverage; operator CLI and ClickHouse tooling | four KV ops per event in the append batch; keep |
| Managed consumers | no SDK coverage | in-memory map plus persisted rows |
| Tablets, Raft, routing | cluster smoke tests, Helm example | route context threaded through the runtime; mount-time gating is sufficient |
