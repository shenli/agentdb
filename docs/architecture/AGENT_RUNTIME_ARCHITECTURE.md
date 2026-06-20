# Agent Runtime Architecture: Where AgentDB Fits

This document is a strategic and architectural reading of the emerging "standard"
AI agent stack and where AgentDB sits inside it. It exists to keep AgentDB's
scope sharp as the surrounding ecosystem converges, and to serve as a reference
articulation of the storage tiers a production agent platform needs.

It is positioning and rationale, not an implementation spec. Where a claim
reflects current code it is marked **(implemented)**; where it reflects design
direction it is marked **(designed)**. See
[ARCHITECTURE.md](ARCHITECTURE.md) and [DESIGN.md](DESIGN.md) for the durable
commitments, and [TRADEOFFS.md](TRADEOFFS.md) for the product boundary.

## Context: The Stack Is Converging On Three Tiers

For two years the agent stack was discussed as two things: the model and its
tools. Production experience has pushed a third, load-bearing tier into the
middle. Anthropic's Managed Agents framing names the same split AgentDB's design
arrived at independently:

- **Brains** - models and harnesses (the reasoning loop)
- **Hands** - sandboxes and tools (where code executes)
- **Sessions** - durable execution state that lets brains recover, inspect,
  resume, replay, fork, and audit work

The reason the session tier is emerging now is that the properties teams need in
production all live there, not in the model or the sandbox: pause/resume,
crash recovery, audit/replay, fork, and credential isolation. Session state is
the structural seam that makes those properties possible. It is the control
point of the agent stack, not a convenience feature.

The convergence is validation of AgentDB's thesis. It is also a commoditization
risk if AgentDB is perceived as "sessions in general" rather than the specific
runtime contract below. The rest of this document draws the line precisely.

## The Three Compute Tiers Map To Three Storage Tiers

Each compute tier has a corresponding storage tier with a fundamentally
different shape. They must be separate systems because they are separate
workloads. Collapsing any two forces one workload onto the wrong engine, which
is exactly the "compose it from generic primitives" failure mode AgentDB exists
to remove.

| Storage tier | Holds | Workload shape | Durability / lifecycle |
|---|---|---|---|
| **Agent-metadata DB** (separate relational DB + vault) | agent definitions, identity, config, **credentials** | low-volume, read-mostly, point lookups | strongly durable, backed up, slow-changing |
| **Session store** (AgentDB) | ordered execution log, fork structure, artifact refs | append-heavy, per-session tail reads, fork/replay | tiered warm -> cold -> archive |
| **Sandbox filesystem** (FS + snapshots) | working files, scratch, tool output bytes | random POSIX read/write, large blobs | ephemeral, snapshot-on-demand |

These are not three log buckets. They are three contracts: a trusted config
store, a runtime source of truth, and an ephemeral filesystem.

### The Access Matrix

Storage tiers do not map one-to-one onto compute tiers. The brain is the busiest
client and spans two stores:

- **Brain** reads the **agent-metadata DB** (definition, config, credentials)
  and is the primary reader/writer of the **session store** (the
  append-and-fetch hot loop is the brain's loop). It does not touch the sandbox
  filesystem directly; it issues tool calls and the hands execute them.
- **Hands** own the **filesystem**. Their outputs flow back through the harness
  and are written into the session log by the brain.
- The brain never exposes secrets to the sandbox; the sandbox never reads the
  agent-metadata DB.

The clean way to state the roles of state:

- **metadata** is the brain's identity and config (slow, trusted)
- **session** is the brain's externalized working memory (hot, append, ordered)
- **filesystem** is the hands' working state (POSIX, ephemeral)

This reframes why the session store exists at all: **the brain is stateless
across pause/resume.** Its entire recoverable continuation lives in metadata plus
session. AgentDB is the brain's durable working set. That property is what
separates it from observability storage (see below).

## The Trust Boundary

Drawing the trust line is what makes "credentials out of the sandbox" a
structural guarantee rather than a hope.

```text
  ABOVE THE LINE (trusted)
    agent-metadata DB + vault   <- credentials live here, ONLY here
  ----------------------------------------------------------------
  BELOW THE LINE (prompt-injection reachable)
    session store (AgentDB)      <- contains model/tool output
    sandbox filesystem           <- where injected code runs
```

Both the session log and the sandbox filesystem contain model and tool output,
so both must be assumed contaminated by prompt injection. Credentials therefore
live exclusively in the agent-metadata DB, which neither the session log nor the
sandbox can read directly.

The resulting invariant for AgentDB is simple and auditable:

> **AgentDB is always below the trust line, so no secret is ever stored in it.**

This keeps AgentDB out of the credential business entirely. Credentials do not
belong in session storage, and they do not belong in AgentDB's own control-plane
metadata either; they belong in a separate agent-metadata store with a vault.

### Enforcing The Invariant In The SDK

A rule that says "no secrets below the line" is not enough; the SDK should make
storing a secret structurally impossible. The agent-security industry has
converged on one pattern: a **credential broker / egress proxy** where secrets
live in a vault and a proxy injects auth into outbound requests at the edge, so
the agent never holds them. Anthropic's Managed Agents use a proxy where the
harness is never made aware of the credentials; Cloudflare attaches them at the
egress layer and Vercel at the sandbox layer. The defense works because a prompt
injection that says "exfiltrate your API keys" finds nothing to exfiltrate. The
threat is real, not hypothetical: a prompt injection in a PR title made a coding
agent's review action post its own API key. Credentials must never enter the
context window, tool schemas, reasoning traces, or stdout (which frameworks feed
back into context); this maps to OWASP LLM02 and LLM07.

Applied to AgentDB's data-plane SDK:

1. **Refs, not values.** AgentDB accepts credential *references*, never secret
   values. Event / record / artifact types carry no secret-shaped fields. An
   event that records "which credential was used" stores an opaque `cred_ref` that
   resolves in the agent-metadata DB / vault. This is the same pattern as
   snapshot-refs, and the event-level `credential_refs` field makes this a
   typed reference surface **(implemented for events)**.
2. **Separate clients, no cross-wiring.** The SDK object that talks to AgentDB
   (data plane) has no vault capability. The vault / agent-metadata DB is reached
   by the harness / control plane through a different client.
3. **Broker at egress.** When a tool needs auth, a broker injects the credential
   into the outbound request at egress. The secret never appears in a tool-call
   argument that would be logged into a session event; AgentDB records the call
   with args reduced to a `cred_ref`.
4. **Defense in depth on the write path.** Because AgentDB sits below the trust
   line, add a backstop: an SDK-level "no raw secrets" validator, an optional
   server-side raw-secret field reject policy on event append, and a tool-output
   sanitization hook before output re-enters context. The server-side append
   backstop is implemented as `secret_handling.deny_raw_secret_fields`; primary
   control is still "secrets never reach here."
5. **Short-lived, audience-bound, least-privilege tokens** scoped per task rather
   than host-inherited environment credentials, so a leak has minimal blast
   radius. This is vault / control-plane policy.

The headline: secrets live above the line in the vault; AgentDB's SDK holds only
`cred_ref`s and cannot express a secret; a broker injects real auth at egress so
nothing transits the loop, the log, or the sandbox.

## What AgentDB Owns And Does Not Own

The convergence narrows AgentDB's job rather than widening it.

**AgentDB owns:**

- the ordered session / execution log (idempotent append-and-fetch)
  **(implemented)**
- fork structure and fork-aware replay **(implemented)**
- session lifecycle: warm -> cold -> archive -> restore, with policy-driven
  sweep and archive-manifest restore **(implemented)**
- references to external state: artifact blobs and sandbox snapshot refs
  **(implemented for refs; sandbox snapshot lifecycle/integration designed)**
- a records / retrieval substrate derived from events: searchable, scored, with
  per-session (read-after-write) and global (async, eventually consistent)
  indexes that a memory system writes into and queries **(partially
  implemented)**
- a changefeed family for consumers: `subscribe` (live session tail), `watch`
  (cursor-based session/tenant/agent changefeed), and `export_watch` (durable
  CDC over all runtime mutations, with claim/ack/rewind managed consumers) -
  this is how derived tiers and external systems attach to the log
  **(implemented, Phase 1)**

**AgentDB explicitly does not own:**

- **credentials** -> agent-metadata DB / vault
- **sandbox working files** -> filesystem; AgentDB holds only refs, never the
  bytes
- **memory derivation policy and recall ranking** -> a separate memory system;
  AgentDB provides the source-of-truth log and the records substrate, not the
  rules / LLM extraction / "dreaming" that populate memory or the semantics that
  rank recall
- **application OLTP, analytics, workflow scheduling** -> their own systems

## Fork And Resume Are Coordinated Cross-Store Operations

The most important integration seam is the boundary between the session store and
the sandbox filesystem. The sandbox is a filesystem (random read/write, large
files, POSIX semantics), which is not what an append-structured log is good at.
Forcing sandbox bytes into the event log is the same category error as putting
blobs in a relational database.

So the division of labor is: **the filesystem stores the bytes; AgentDB stores a
durable reference to a snapshot of them at a given seq.** A session carries
layered snapshot refs (`SnapshotRef` / `SnapshotLayer`) that say "at seq N,
sandbox snapshot layers = `<object-store-keys>`" **(implemented)**.

This makes fork and resume lightweight in *both* stores, because each store only
does what it is good at:

- **Fork.** AgentDB does the O(1) pointer-fork of the log: a child session stores
  only `ForkRef { session_id, at_seq }` and copies no events **(implemented)**.
  The sandbox does a filesystem snapshot / copy-on-write fork, which overlayfs,
  btrfs, and zfs do natively. AgentDB resolves the newest non-expired snapshot
  at or before the fork point across the fork chain, so resume can restore the
  right ancestor filesystem state and replay the child branch **(implemented for
  refs; sandbox restore still external)**.
- **Resume.** Restore the sandbox filesystem from the ref'd snapshot, then replay
  the session tail from AgentDB. Two stores, stitched by the ref.

Fork is lightweight precisely because the session log never tries to capture
filesystem state, only point at it.

### Sandbox Persistence Is An Industry-Validated Requirement

The need to persist sandbox state is not speculative; it is table stakes across
the agent-sandbox ecosystem, which validates both the requirement and the
ref-holding split:

- **E2B** pauses/resumes filesystem plus full memory (running processes,
  variables) via Firecracker snapshots, resuming in 5-30ms, and explicitly
  supports resuming *or forking* from a point.
- **Modal** ships three tiers - directory, filesystem, and memory snapshots
  (CRIU-based) - with different retention per tier.
- **Daytona** persists workspace files with auto-stop / auto-archive / auto-delete
  lifecycle; **Fly.io Sprites** offer VM checkpoint/restore; **gVisor / Docker /
  Podman** use CRIU. Research systems (DeltaBox, Crab, 2026) push semantics-aware
  checkpoint/restore to millisecond rollback and template-fork.

Two findings shape the ref contract:

1. **Snapshotting is layered, not monolithic.** An agent filesystem decomposes
   into base image, platform dependencies, repo / app code, and generated
   artifacts. Surgical per-directory snapshots (Modal Directory Snapshots) beat
   whole-FS images: they enable decoupled base-image upgrades and warm-pool
   mounting. The AgentDB ref should therefore point at a *set of layered refs*,
   not a single opaque image.
2. **Snapshot retention is tiered and shorter than the log.** The market already
   prices this in (memory snapshots ~7 days, directory snapshots ~30 days,
   filesystem images indefinite). Snapshot refs need their own retention policy,
   distinct from the durable session log, and the ref contract must tolerate
   *dangling refs* (snapshot expired -> fall back to an earlier snapshot plus
   replay, or full re-derive).

Notably, no platform stores sandbox state in the conversation/session log; it is
a dedicated persistence system. That is direct external validation of the
three-store separation.

### Fork Cost Characteristics (implemented)

- Fork *creation* is O(1) metadata: one new session record with a parent
  reference and fork point; no events copied.
- Fork *read* is O(total ancestor events): replay walks the fork chain backward
  to the root and stitches each segment truncated at its fork point.
- Nested forks (fork of a fork) work via recursive chain walk, linear in depth.

The open scaling concern is that replay currently re-stitches on every read with
no materialization. If forking becomes a headline behavior, the durable moat is
*cheap replay over deep chains* (segment caching, or compaction that collapses a
fork point into a sealed segment), not the already-cheap create path.

## Memory Is A Derived Retrieval Tier

Memory is not a primary store. It is derived from the session log, which remains
the source of truth. This is the same shape as the Managed Agents "Memory &
Dreaming" feature, and the line to hold is that memory consumes the log; it does
not own runtime state.

Memory has two surfaces with different shapes, and conflating them is a mistake:

- **Derivation (write side).** Memory is produced *from* the session log by one
  or more of:
  - **rules** - deterministic extraction (e.g. promote tool results, decisions,
    or tagged events into records)
  - **LLM extraction** - a model summarizes or distills events into durable facts
  - **periodic "dreaming"** - a batch pass that consolidates across many sessions
    to find patterns, dedupe, and compress recent memory into longer-lived memory

  All three run asynchronously against the log. None of them is in the agent's
  hot loop.

- **Recall (read side).** Recall is a **retrieval system**: semantic / vector
  search, metadata filtering, scoring, and ranking, typically top-k across many
  sessions. Its access shape is search, not the ordered single-session tail read
  of the runtime hot path. It tolerates eventual consistency and approximate
  results.

Note that the recall surface resembles the observability / OLAP shape (scan and
rank across many sessions) far more than the session hot loop. That is the second
reason memory is a separate contract, not a feature bolted onto the session
store.

**Where AgentDB fits.** AgentDB provides the source-of-truth log and a records
substrate that a memory system writes into and queries: records are derived from
source events, searchable, scored, and indexed both per-session (read-after-write)
and globally (async). AgentDB does **not** own the derivation policy (which rule,
which LLM prompt, the dreaming schedule) or the recall ranking semantics. Those
belong to the memory system layered above.

The memory system attaches to the log through the changefeed family. Derivation
is an asynchronous sink that must not miss events and must resume after a crash,
which matches the durable `export_watch` ledger (at-least-once, resumable cursor,
claim/ack) better than the lightweight `watch` feed. Recommended direction:
derivation consumes the durable export ledger; lighter `watch` is for live,
loss-tolerant observers.

Two properties follow from "memory is derived":

- **Memory is rebuildable.** Because it is a projection of a durable log, a
  derivation bug is recoverable: fix the rule or prompt and re-derive by replaying
  the log. The log's durability is what makes memory safely disposable.
- **Memory is below the trust line.** It is derived from injection-reachable
  content, so it must never become a credential side-channel. Secrets stay above
  the line; memory never carries them.

## AgentDB Is Not Observability Storage

A tempting but wrong simplification is "in this narrow scope AgentDB is just
observability storage, like ClickHouse." The resemblance is real at the engine
level and an illusion at the contract level.

**Where the analogy holds (engine mechanics):** both are append-heavy, carry
large total history, tier hot to cold, and lean on LSM-style segments. The
storage machinery rhymes.

**Where it breaks (the runtime contract):**

| | Observability (ClickHouse) | Session store (AgentDB) |
|---|---|---|
| Who reads | humans / dashboards, after the fact | the agent, in its hot loop, every step |
| Read shape | scan + aggregate across many sessions (OLAP) | point / tail read of one session, read-your-write |
| Write semantics | fire-and-forget, loss-tolerant | idempotent, per-session ordered, loss = corruption |
| Mutation | none (immutable facts) | fork, replay-from-point, lifecycle transitions |
| Role of the data | derived shadow of what happened | source of truth the agent is acting on now |
| Consistency | eventually consistent, approximate OK | per-session ordered + read-your-writes |

The deepest difference: **observability data is a derived mirror you can lose
without stopping the system; the AgentDB session log is the runtime state, and
losing it means the agent cannot take its next step.** That is an OLTP-shaped
contract, not an OLAP-shaped one.

The relationship is therefore upstream/downstream, not same-category. A session
log is a near-perfect *source* for an observability pipeline; ClickHouse sits
*behind* AgentDB consuming its stream, the way it sits behind Postgres consuming
a CDC feed. Calling AgentDB "observability storage" is like calling Postgres an
observability store because its write-ahead log looks like an event stream.

Observability attaches through the same changefeed family as memory: the durable
`export_watch` ledger is exactly an export feed for sinks. The unifying picture is
that the changefeed/CDC is how every derived tier reads the source of truth:
**source-of-truth log -> changefeed/CDC -> {memory, observability, external
sinks}.** See [OBSERVABILITY_EXPORT.md](OBSERVABILITY_EXPORT.md) for the export
design.

### Can You Use ClickHouse As The Session Store?

Technically yes for a narrow slice, but you fight the engine on exactly the
points that define a session store, while it excels at the part that looks like
observability.

| Session-store requirement | ClickHouse reality | Fit |
|---|---|---|
| Append one event per step | single-row inserts create one part each ("too many parts"); `async_insert` adds buffering latency | poor |
| Read-your-writes on the tail | async insert + merge-time visibility means a just-written event is not cheaply readable now | poor (the core problem) |
| Idempotent append | `ReplacingMergeTree` dedupes at merge time, not read time; immediate dedup needs `FINAL`/`argMax` (expensive) | poor |
| Tail read of one session | `ORDER BY (session_id, seq)` range-scans a session well | good |
| Lifecycle transitions | modeled as row mutations: heavy async part rewrites | poor |
| Per-session delete | mutations are heavy; lightweight `DELETE` is not OLTP-grade | workable, costly |
| Fork + fork-aware replay | no native concept; stitch in app via range queries | doable in app |
| Durability | `ReplicatedMergeTree` via Keeper is genuinely durable | good |

The two rows that disqualify it for the hot loop are read-your-writes and
immediate idempotency. The agent loop appends an event, may retry, then
immediately fetches its own recent tail to build the next prompt. ClickHouse is
designed to batch writes and read later, with dedup and visibility happening
asynchronously during background merges. That is the opposite of read-after-write
on a single row.

The right answer is a split, not a swap:

- **Hot tier** (append-one, read-your-write, idempotent, fork): an LSM-KV with a
  per-session contract. This is the AgentDB-shaped part.
- **Cold / archive + analytics tier**: seal segments and land them in ClickHouse,
  where cross-session scans, audits, and dashboards are cheap.

A useful test for any candidate hot-tier engine: *can it do idempotent
append-one plus immediate read-your-own-tail at single-digit-ms p99, per
session, without a background merge in the path?* That single question sorts
OLTP-shaped stores (yes) from OLAP-shaped ones like ClickHouse (no).

## The Changefeed Over A Forked Log

Once sessions fork, the log is no longer a single line; it is a DAG of
per-session sub-streams. AgentDB now emits each physical event once on its
session sub-stream and emits fork creation as a first-class `fork_edge` control
item in `watch` and durable `export_watch` **(implemented)**. The remaining
cursor-design question is whether to graduate from the current opaque total-order
cursor to a true encoded frontier. There is no published "branched CDC" standard,
but the two mature models for *position in a branched history* agree on the
essentials:

- **Git / Dolt** represent history as a commit DAG. A position is a
  content-addressed pointer that carries lineage, not a linear offset, and
  branches share ancestry by *pointer, not copy*.
- **Kafka** represents position as *per-partition* offsets; a consumer's cursor
  is a map of offsets, not a single number.
- **CDC** in general only guarantees per-key ordering, at-least-once, and
  dedupe. Fork does not change per-key ordering; it adds a new key.

The recommended model follows directly:

1. **Each session, including each fork child, is its own ordered sub-stream**
   keyed by `session_id` and ordered by `seq`. A fork creates a *new* sub-stream,
   exactly like a new Kafka partition or a new git branch. The changefeed cursor
   should eventually be a frontier `{session_id -> high-water seq}`, opaquely
   encoded. The current implementation uses collision-free opaque item cursors
   and preserves gap-free delivery across forks; a true frontier cursor remains
   future hardening.
2. **Emit each physical event exactly once, on the session where it was written.**
   The child's *replay* stitches `parent[0..fork_point] + child` (already
   implemented), but the *changefeed* carries new mutations only. Parent pre-fork
   events were already emitted on the parent's sub-stream; re-emitting them on the
   child's sub-stream would break at-least-once/dedupe for consumers keyed by
   `(session_id, seq)`. This mirrors git (a commit lives in one place) and matches
   AgentDB's own O(1) no-copy fork: the feed semantics match the storage
   semantics.
3. **Represent the fork as a first-class `fork-edge` control item** carrying
   `(parent_id, fork_point_seq, child_id)`. That is how a consumer discovers the
   child and decides whether to follow it. For tenant- or agent-scoped feeds the
   child appears automatically as a new key in the frontier; for a session-scoped
   feed on the parent, the child does not bleed in, but the fork-edge item
   announces it. A consumer needing the child's full logical history follows
   lineage (parent up to `fork_point`, then child), the same stitch the replay
   API already performs.
4. **Extend the gap-free guarantee across forks.** The v2 fork-edge tests assert
   that across a fork no parent event is dropped or duplicated, the fork-edge is
   delivered exactly once, child events are resumable, and an old cursor from
   before the fork still discovers the child on resume **(implemented)**.

The headline: a fork-aware feed is a feed over a DAG of per-session sub-streams
with a frontier cursor; fork is a pointer event, not a re-emission.

## Strategic Position

Two goals follow from the convergence:

1. **A platform to integrate with.** AgentDB is the durable session store that a
   self-hosted sandbox and any harness plug into. The integration surface, not
   new features, is the priority. The changefeed family (`subscribe` / `watch` /
   durable `export_watch`) that lets an external harness or system attach to the
   log is **already implemented (Phase 1)**, so the work here is positioning and
   hardening it as the integration backbone, not building it. The core
   **snapshot-ref protocol** exists in AgentDB; the remaining contract is with
   the sandbox filesystem: trigger policy, byte lifecycle, and restore/fork
   handoff.
2. **A reference architecture.** AgentDB wins as a reference by being the
   clearest articulation of the standard, not the most featureful product. The
   load-bearing artifact is this taxonomy plus its invariants, codified below.

The risk to manage is commoditization by absorption: if the default managed
platform bundles good-enough session management for free, AgentDB matters only to
teams who will not take the bundle (self-host / VPC, model-agnostic, regulatory,
or fork/replay/lifecycle at a scale the bundle does not expose as primitives).
That is a real and sufficient market, but it means the positioning must target
"you do not want the closed loop," not "we also do sessions."

## Invariants (the reference-architecture rules)

1. **Secrets only above the line.** Credentials live in the agent-metadata DB /
   vault, never in the session log or sandbox.
2. **Sandbox bytes live in the filesystem; AgentDB holds only refs.** The log
   points at snapshots and artifacts; it does not store their bytes.
3. **Memory is derived; the session log is the source of truth.** Memory is
   produced from the log (rules / LLM / periodic dreaming) and read back through a
   retrieval surface; it is rebuildable by replaying the log, and it never owns
   runtime state or carries secrets.
4. **Session identity is stable across topology.** Routing and placement can
   change; the session a brain resumes does not.

## Open Questions

- Who triggers sandbox snapshots, who owns their byte lifecycle, and what
  operational policy chooses snapshot frequency / retention? AgentDB now stores
  layered refs, tracks expiry, tolerates dangling refs, and resolves fallback to
  earlier snapshots plus replay; the sandbox service still has to implement the
  actual filesystem snapshot/restore contract.
- The fork-aware changefeed model is implemented with first-class `fork_edge`
  items and gap-free tests. The residual cursor hardening is an opaque encoded
  frontier `{session_id -> high-water seq}` if deep branched CDC consumers need
  independent per-branch positions instead of the current total-order item
  cursor.
- The credential boundary has an event-level `credential_refs` / `cred_ref`
  surface plus an opt-in server-side append backstop
  (`secret_handling.deny_raw_secret_fields`) that rejects payload keys shaped
  like raw secrets. The residual defense-in-depth work is SDK raw-secret
  validators and tool-output sanitization before output re-enters context.
- What replay materialization or compaction makes deep fork chains cheap enough
  to make fork a headline behavior?
