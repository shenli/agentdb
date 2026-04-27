# Pi Company — AgentDB Sample App

A 5-agent AI company on AgentDB + pi-mono. Demonstrates AgentDB's signature
primitives — sessions-as-memory, fork-as-on-demand-contractor,
broadcast-as-team-coordination, and config-as-events — in realistic combination.

You play CEO. Four employee agents (CTO, Engineer, Head of Marketing, Head
of Sales) run autonomously. Each agent has its own AgentDB session for
memory. Agents coordinate via three broadcast channels. Each employee can
**hire a contractor on demand** — a sub-agent forked from the parent's
session, inheriting full context for free, running in parallel.

## What this proves about AgentDB

| Primitive | How the demo uses it |
|---|---|
| **Sessions** | One per agent (`{runId}:{role}`). Persistent across runs (`--resume=<run-id>`). The session log IS the agent's memory. |
| **Fork (the headline)** | Every employee has a `hire_contractor` tool. Forking the agent's session is O(1); the contractor inherits the parent's full context. Three narratives — hard-task handoff, speculative parallel exploration, specialist delegation — all map to this single primitive. |
| **Real-time event subscriptions** | The TUI subscribes to all 3 channels with durable SSE. |
| **Ephemeral broadcast channels** | Three channels: `announcements` (CEO ↔ all), `engineering` (CTO ↔ Engineer), `gtm` (Marketing ↔ Sales). In-memory fan-out; not durable. |
| **Config-as-events** | Agent definitions live in code (`src/agents/*.ts`). Runtime overrides go in a `_config:agents` session. The CEO's `update_agent_config` tool appends overrides; agents reload config every loop iteration. Hot-tunable, event-sourced, free history. See [`docs/agent-db.md` § Recommended pattern: config-as-events](../../../../docs/agent-db.md#recommended-pattern-config-as-events). |

## Communication

There are three distinct mechanisms in this app, and each maps to a different AgentDB primitive. Mixing them up is the easiest way to ship a broken multi-agent design.

| Mechanism | What it does | AgentDB primitive |
|---|---|---|
| **Broadcasts (channels)** | Wake agents up. Every event an agent reacts to arrives via a channel subscription — the agent's loop only runs when a channel delivers something. | Ephemeral broadcast channels |
| **Sessions** | Durable memory. Each agent's session log records every directive received, tool call made, deliverable produced. The agent reads recent session events into its prompt context. The agent does **not** wake up on its own session writes. | Sessions + `append_and_fetch` |
| **Direct messages** | Send a private message to a specific agent. Internally publishes to that agent's personal mailbox channel (`directs:{role}`); the agent subscribes to its mailbox and wakes on the publish, just like any other channel. | Broadcast channel (`directs:{role}`) |

The slogan: **broadcasts wake agents; sessions are memory; direct messages flow through personal mailbox channels.** A direct message is just a broadcast on a single-subscriber channel — it uses the same wake-up mechanism as team channels, so it's processed promptly.

The Engineer's behavior in the "launch" scenario is a good illustration of this. When the CEO posts to `announcements`, every employee (CTO, Engineer, Marketing, Sales) wakes up via their `announcements` subscription and decides what to do based on their role's system prompt + current session state. The Engineer's prompt has an explicit delegation rule: *"hire a contractor only when you're already mid-task on something parallelizable; otherwise do it yourself."* So whether `hire_contractor` fires on a given directive depends on what the Engineer was already doing — emergent, not scripted.

## Setup

Prereqs:
- Node ≥22 (for `--experimental-strip-types` and the test glob).
- Rust toolchain (for `cargo run -p agentdb-server`).
- An LLM API key (OpenAI, Anthropic, or MiniMax).

```bash
# Pre-build the AgentDB server for fast first-run:
cargo build -p agentdb-server

# Install + type-check the sample:
cd examples/agents/typescript/pi-company
npm install
npm run build
```

## Running

Set your API key:

```bash
export AGENTDB_DEMO_PROVIDER=anthropic    # or openai, minimax, minimax-cn
export AGENTDB_DEMO_API_KEY=sk-...        # your provider's API key
export AGENTDB_DEMO_MODEL=claude-haiku-4-5    # optional override
```

### Interactive mode (default)

```bash
npm start
```

Type directives at the `> CEO:` prompt. The 4 employee agents pick them up via the `announcements` channel. Type `/quit` to exit.

### Scripted demo (~3-5 min, hands-off)

```bash
node --experimental-strip-types src/index.ts --scenario=launch
```

The "launch" scenario walks the company through:
1. CEO announces a product launch.
2. CTO + Marketing propose plans.
3. CEO directs Engineer to explore approach B in parallel. Whether the Engineer hires a contractor or pursues both threads itself depends on the Engineer's current workload — that's the agent's call (see "Communication" below).
4. CEO hot-tunes Marketing's system prompt mid-run via `update_agent_config`.
5. Marketing redoes the campaign with the new style.
6. Sales reports prospect status.
7. CTO summarizes.

### Resume a prior run

```bash
node --experimental-strip-types src/index.ts --resume=r-1714063200
```

Loads the existing run's sessions (per-agent memory, `_config:agents` overrides, `_meta` lifecycle) and resumes interactive mode. Demonstrates persistence — the company picks up exactly where you left off.

## What this would take without AgentDB

- Postgres for the conversation history (one table per agent, custom MVCC for fork).
- Redis for ephemeral broadcast coordination.
- pgvector if you wanted hybrid retrieval over agent histories (we don't yet, in v1).
- A separate config store (e.g., etcd, Postgres) for runtime overrides.
- Plus a state-management framework to glue it together.

AgentDB ships all of this as one mental model: sessions for events, broadcast for coordination, fork for sub-agents, hybrid retrieval for recall, config-as-events for tunable runtime config.

## Architecture

```
src/
├── index.ts            # entry: parses --scenario / --resume / interactive, boots
├── orchestrator.ts     # boots all 5 agents, owns run lifecycle, writes _meta events
├── agent.ts            # per-agent loop: subscribes to channels, calls pi-mono Agent.prompt
├── memory.ts           # AgentDB session wrapper: load history, append events, fork
├── channels.ts         # AgentDB broadcast wrapper: publish/subscribe per channel
├── config.ts           # layered config: code defaults + _config:agents session overrides
├── llm.ts              # provider router: openai/anthropic/minimax via @mariozechner/pi-ai
├── tui.ts              # chalk + readline: live activity log + > CEO: prompt
├── server.ts           # auto-spawn agentdb serve --dev as a child process
├── tools/              # one file per tool (announce, write_doc, hire_contractor, ...)
└── scenarios/launch.ts # 6-step canned directive sequence
```

## License

Same as the parent agentdb project.
