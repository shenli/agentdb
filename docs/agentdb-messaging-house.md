# AgentDB Messaging House

## Core Positioning

AgentDB is the runtime-state database for AI agents.

It stores what happened while an agent was running: session events, messages,
tool calls, outputs, artifacts, lifecycle state, replay metadata, and context
assembly records.

## Primary Message

AI agent storage has three pillars: Metadata, Memory, and Runtime State.
AgentDB owns Runtime State: the durable session log of what an agent did, so
agents can be inspected, replayed, resumed, and recovered.

Modern agent systems split into a model, a harness, and one or more sandboxes
or tools. The harness and sandboxes should be replaceable. AgentDB keeps the
durable state outside them: every event, artifact, record, source reference,
and context decision needed to resume work safely.

## Research Basis: Session-Log Pain Points

Recent agent-platform writing points to the same set of operational pains:

- Anthropic's Managed Agents architecture treats the session as "the
  append-only log of everything that happened" and separates it from the
  harness and sandbox so failures can be recovered with `wake(sessionId)`,
  `getSession(id)`, and `emitEvent(id, event)`.
  See: <https://www.anthropic.com/engineering/managed-agents>
- Anthropic also calls out that the session is not the model context window.
  Long-horizon work exceeds context limits, and irreversible compaction or
  trimming can lose information future turns need unless recoverable context is
  stored outside the window.
- LangGraph persistence/checkpointing exists because human-in-the-loop,
  memory, time travel, replay, and fault-tolerant execution require durable
  state snapshots organized by thread.
  See: <https://docs.langchain.com/oss/javascript/langgraph/persistence>
- OpenAI's Agents SDK sandbox docs separate the outer runtime from sandbox
  sessions and note that sandbox state/snapshots support reconnecting to prior
  work or seeding fresh sandbox sessions.
  See: <https://openai.github.io/openai-agents-python/sandbox/guide/>
- Agent observability products emphasize that normal logs do not show enough:
  teams need to inspect prompts, outputs, tool calls, file edits, errors,
  costs, and replayable execution timelines to understand what the agent
  actually did.
  Examples: <https://laminar.sh/> and <https://duskfall.app/>

The common pain is not lack of a database. It is lack of an agent-native system
of record that connects execution history, context, memory, artifacts, and
lineage in one durable model.

## Value Pillars

### 1. Durable Execution State

AgentDB gives every agent run durable, replayable state.

Harnesses can crash, restart, move machines, or change implementation without
losing the run. A new harness can wake a session, replay append-only events,
inspect artifacts and records, and continue from the last durable state.

Pain addressed: harness crashes, container failures, long-running jobs, human
approval pauses, and restarts should not require replaying side effects or
reconstructing state from scattered logs.

### 2. Replayable Context And Memory Provenance

AgentDB stores the runtime history that memory systems can learn from and cite.
It does not try to replace the full memory layer.

Harnesses can fetch event slices, rebuild prompts, compact history, search
records, preserve source truth, and record which events, artifacts, or records
influenced a context decision.

Pain addressed: context windows roll over, compaction can be lossy, retrieval
results need provenance, and memory systems need durable source material rather
than scattered logs.

### 3. Scalable, Cost-Efficient Agent Storage

Long-running agents produce more history than should fit in a prompt, RAM, or a
single hot store.

AgentDB separates hot session state, searchable records, artifacts, and
retention/lifecycle policy so platforms can keep the useful operational state
without paying premium costs for everything forever.

Pain addressed: storing every token, tool result, file, artifact, and trace in
one expensive hot path does not scale. Teams need lifecycle-aware storage that
keeps agent state useful without making every run permanently expensive.

### 4. Auditability And Lineage

AgentDB makes agent work traceable by construction.

Every model output, tool call, tool result, artifact, and memory write can be
tied back to session events and source references, giving platform teams the
lineage they need for debugging, policy, retention, and compliance.

Pain addressed: generic logs show that calls happened, but not what the agent
saw, what it used, what it produced, and how a result can be reconstructed or
governed after the run.

## Audience Messages

### For Agent Platform Teams

Build durable, restartable agent infrastructure without inventing your own
session log, artifact store, context lineage, and replay semantics.

### For AI Application Developers

Persist the important state of every agent run: messages, tool calls, outputs,
artifacts, context decisions, and replay history, without coupling your app to
one framework or sandbox provider.

### For Infrastructure Teams

Scale agent state independently from ephemeral compute. Keep hot execution
state fast, move colder history and artifacts to cheaper storage, and preserve
enough lineage to debug and govern long-running work.

## Short Descriptions

One-liner:

> AgentDB is the runtime-state database for AI agents.

Two-liner:

> AgentDB stores what happened while an agent was running: messages, tool
> calls, outputs, artifacts, replay history, and context decisions. Agents can
> be inspected, replayed, resumed, and recovered.

Plain-English version:

> AgentDB is the durable session log for AI agents.

Homepage hero:

> Runtime state for AI agents.

Homepage subhead:

> AgentDB stores the durable session log of what an agent did, including
> messages, tool calls, outputs, artifacts, replay history, and context
> decisions, so agents can be inspected, replayed, resumed, and recovered.

## Positioning Contrast

### Not A Vector Database

Vector databases store embeddings. AgentDB stores the operational state of an
agent run: events, records, artifacts, source references, and replayable
context.

### Not A Memory Platform

Memory platforms decide what agents should learn, recall, rank, forget, and
cite. AgentDB stores the durable runtime history and provenance that memory
systems can build on.

### Not An Agent Framework

Frameworks define how agents think and act. AgentDB persists what happened,
what was produced, and what must survive.

### Not A Workflow Engine

Workflow engines orchestrate predefined steps. AgentDB stores open-ended agent
execution state for harnesses that evolve over time.

### Not A Sandbox

Sandboxes execute untrusted work. AgentDB stores the durable execution state,
context, records, artifacts, and lineage that survive sandbox replacement.

## Proof Points

- Append-only session event logs.
- Replay and fork support.
- Searchable records with local/global index semantics.
- Artifact metadata and blob storage.
- Source references across events, records, artifacts, and skills.
- Context-building APIs.
- SDKs for Python and TypeScript.
- Persistence through KV and content backends.
- Operational APIs for routing, storage, lifecycle, and index state.

## Category

Preferred external category:

> Agent runtime-state database

Recommended technical category:

> Durable session log for AI agents

Alternative category names:

- Durable agent state store.
- Agent session database.
- Agent runtime database.

Avoid leading with "vector database", "memory platform", "agent framework",
"sandbox platform", or "workflow engine". Those categories are either too
narrow or imply AgentDB owns behavior that should remain in the harness or
memory layer.
