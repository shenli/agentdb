# Pi Company — Sample App Design

**Date:** 2026-04-27
**Status:** Spec, not yet implemented
**Goal:** Ship a substantial multi-agent sample app on AgentDB that uses pi-mono's agent runtime, demonstrates fork + pub/sub + sessions in realistic combination, and pitches AgentDB to TypeScript agent developers.

---

## 1. Summary

A 5-agent "AI company" running on pi-mono + AgentDB. The user plays CEO; four employee agents (CTO, Engineer, Head of Marketing, Head of Sales) run autonomously. Each agent has its own AgentDB session for memory. Agents coordinate via Phase 2 broadcast channels. Each agent has a `hire_contractor` tool that forks its session to spawn a sub-agent — efficient parallel work with full inherited context.

Agent **definitions** (system prompts, tool lists, default models) live in code; **runtime overrides** live in a per-tenant `_config:agents` session that the CEO can mutate at runtime via an `update_agent_config` tool. Code = defaults; AgentDB session events = overrides; merge at agent-instantiation time. This pattern (config-as-events) is reusable beyond the demo and worth documenting in `agent-db.md` as a recommended AgentDB convention.

The demo's pitch: **"5-agent AI company in 1500 LoC of TypeScript. Persistent memory, real-time coordination, on-demand contractors via fork, hot-tunable agent config — all primitives AgentDB ships, none Postgres+Redis can do without significant glue."**

---

## 2. Use case / narrative

**The story the README and the demo tell:**

> Spin up your AI company. Hand the CEO (you) a chat box. Type `> Launch our new vector search service this quarter.` Five agents wake up, read your directive, and start working — CTO drafts a feature spec, Engineer iterates on docs, Marketing drafts campaigns, Sales builds prospects. They broadcast status to each other in real time. When Engineer hits a hard sub-problem, they hire a contractor — a sub-agent forked from the same session, inheriting full context for free, working in parallel. Three minutes later, you have a launch plan, a spec, draft outreach to 3 prospects, and two campaign variants. Tomorrow, you resume the company and pick up exactly where you left off. Welcome to AgentDB.

The demo runs in two modes:
- **Interactive:** CEO chat at the bottom; agents react to directives in real time. TUI shows a live activity feed.
- **Scripted (`--scenario launch`):** CEO's directives are pre-baked; the whole demo runs in ~3 minutes hands-off. Used for the recorded demo, smoke tests, and CI.

---

## 3. What this proves about AgentDB

| Primitive | How the demo uses it |
|---|---|
| **Sessions** | Three roles: per-agent working memory (`{role}` session per role), per-run lifecycle (`_meta` session), per-tenant config overrides (`_config:agents` session). Persistent across runs (`--resume <run-id>`). |
| **Config-as-events pattern** | Agent definitions in code; runtime overrides in `_config:agents` session events. Merge at instantiation. Free history of config changes; hot-tunable mid-run via `update_agent_config`. Reusable convention beyond the demo. |
| **Phase 1 subscribe (durable)** | The CEO's TUI subscribes to the agent's broadcast channels with `from_seq`-aware reconnect. The activity feed survives a TUI restart with no message loss. |
| **Phase 2 broadcast (ephemeral)** | Agent-to-agent status pings, "I'm thinking", "I'm done" — fan out cheaply, tolerable to lose under heavy load (matches the typing-indicator use case). |
| **Fork (the killer feature)** | Every agent has a `hire_contractor(task_brief)` tool. Forks the agent's session, spawns a sub-agent on the child that inherits the full context for free, runs in parallel. Three narratives — hard-task handoff, speculative exploration, specialist delegation — all map to this single primitive. |
| **HLC ordering** | Implicit in agents reasoning over their own session's event order. |

Reciprocally, what the demo does NOT lean on (out of scope for v1):
- Hybrid retrieval — agents don't query each other's history with BM25/vector. Could in v2.
- Tier transitions / lifecycle — sessions are warm for the demo's lifetime.
- Multi-node — single node only.

---

## 4. Scope & non-goals

### In scope (v1)

- 5 fixed agents: CEO (interactive user), CTO, Engineer, Marketing, Sales.
- 3 broadcast channels: `announcements`, `engineering`, `gtm`.
- 3 tools per agent (max). For non-CEO agents, one of those three is always `hire_contractor`.
- Layered config: code-defined role definitions + per-tenant `_config:agents` session for runtime overrides + `update_agent_config` tool on the CEO.
- One scripted scenario: "Launch a new product."
- Interactive + scripted modes; resume-by-run-id.
- TUI: single chronological log + CEO prompt.
- Self-contained npm package at `examples/agents/typescript/pi-company/`.
- BYO LLM key: OpenAI / Anthropic / MiniMax (selected via env var).
- Auto-start `agentdb serve --dev` as a child process for one-command UX.
- README + smoke test in `scripts/`.

### Out of scope (v2 or never)

- Dynamic agent creation (`hire_employee` for new roles at runtime).
- External tools (real Slack, real GitHub, real email sending).
- Multi-tenant / multi-company.
- Web UI. (TUI only in v1.)
- Cross-node deployment.
- Production-grade error handling. The demo is illustrative; if the LLM API rate-limits, we surface the error and stop.
- Hybrid retrieval / BM25 over agents' history.
- Cost telemetry / token accounting beyond a single-line summary at exit.
- Multi-language ports. TypeScript only in v1.

---

## 5. Architecture

### 5.1 Project layout

```
examples/agents/typescript/pi-company/
├── package.json                  # depends on @mariozechner/pi-* + agentdb (file:../../../../clients/typescript)
├── tsconfig.json
├── README.md                     # setup, scenario walkthrough, "what this proves"
├── src/
│   ├── index.ts                  # entry: parses --scenario / --resume / interactive, boots
│   ├── orchestrator.ts           # spawns the agents, wires channels, drives the run
│   ├── agent.ts                  # base agent loop: read directives → reason → tool call → repeat
│   ├── tools/                    # one file per tool, exporting a pi-mono-compatible tool spec
│   │   ├── announce.ts
│   │   ├── direct_message.ts
│   │   ├── delegate_to_engineer.ts
│   │   ├── write_doc.ts
│   │   ├── submit_for_review.ts
│   │   ├── report_to_ceo.ts
│   │   ├── add_prospect.ts
│   │   ├── draft_outreach.ts
│   │   ├── draft_campaign.ts
│   │   ├── hire_contractor.ts    # the headline tool
│   │   ├── update_agent_config.ts # CEO-only: append a config-override event
│   │   └── index.ts              # bundles tools per role
│   ├── memory.ts                 # AgentDB session wrapper: load history, append turn, fork
│   ├── config.ts                 # Layered config loader: merge code defaults with _config:agents session overrides
│   ├── channels.ts               # AgentDB broadcast wrapper: publish/subscribe per channel
│   ├── tui.ts                    # pi-mono TUI integration: live log + CEO prompt
│   ├── llm.ts                    # provider-router: openai/anthropic/minimax via pi-mono's API layer
│   ├── server.ts                 # spawns `agentdb serve --dev` as a child process
│   └── scenarios/
│       └── launch.ts             # canned "launch a new product" directives
└── tests/
    ├── memory.test.ts            # unit: session wrapper round-trips
    ├── tools/hire_contractor.test.ts  # unit: fork → child session → sub-agent receives task brief
    └── smoke/launch.test.ts      # gated smoke: full run-through (~3 min)
```

### 5.2 The 5 agents

| Role | Owns session | Tools | Subscribes to |
|---|---|---|---|
| **CEO** (interactive) | `pi-company:ceo` | `announce`, `direct_message`, `update_agent_config` | `announcements`, `engineering`, `gtm` |
| **CTO** | `pi-company:cto` | `delegate_to_engineer`, `report_to_ceo`, `hire_contractor` | `announcements`, `engineering` |
| **Engineer** | `pi-company:eng` | `write_doc`, `submit_for_review`, `hire_contractor` | `announcements`, `engineering` |
| **Marketing** | `pi-company:marketing` | `draft_campaign`, `report_to_ceo`, `hire_contractor` | `announcements`, `gtm` |
| **Sales** | `pi-company:sales` | `add_prospect`, `draft_outreach`, `hire_contractor` | `announcements`, `gtm` |

Sessions are bound to a **single tenant** (`pi-company`) for the duration of the run. Run-IDs are encoded as a session-name prefix: `pi-company:{run_id}:ceo`, etc.

### 5.3 The 3 broadcast channels

| Channel | Purpose |
|---|---|
| `pi-company:{run_id}:announcements` | CEO ↔ everyone. CEO directives flow here; agents post major milestones (`launch_complete`). |
| `pi-company:{run_id}:engineering` | CTO ↔ Engineer. Spec deltas, review requests, blocker pings. |
| `pi-company:{run_id}:gtm` | Marketing ↔ Sales. Campaign content, prospect updates. |

Channel naming bakes the run_id in so concurrent runs (or a fork-resume scenario) don't cross-talk.

### 5.4 LLM provider (BYO key)

Three env vars:
- `AGENTDB_DEMO_PROVIDER=openai|anthropic|minimax` (default: `anthropic`).
- `AGENTDB_DEMO_API_KEY=...`
- `AGENTDB_DEMO_MODEL=...` (optional; sane defaults per provider).

Provider routing:
- `openai` → pi-mono's OpenAI client with default base URL.
- `anthropic` → pi-mono's Anthropic client.
- `minimax` → pi-mono's OpenAI client with `baseUrl=https://api.minimax.chat/v1`. Verify pi-mono exposes a `baseUrl` override during planning; if not, write a 50-LoC adapter using pi-mono's lower-level primitives. **No upstream pi-mono changes** unless this verification fails outright.

Model defaults (sane choices per provider):
- OpenAI: `gpt-4o-mini` (cheap, capable enough for the demo).
- Anthropic: `claude-haiku-4-5` (cheap, capable, 1M context — enough for the demo's session lengths).
- MiniMax: `MiniMax-Text-01` or whatever their flagship cheap model is at impl time.

### 5.5 Layered state model

Three orthogonal layers, each at the right place:

| Layer | Where | Mutability | Loaded by |
|---|---|---|---|
| **Definitions** — what an agent CAN be | Source code (`src/agents/{role}.ts`) | Immutable per release; versioned in git | At process startup |
| **Tenant overrides** — what an agent IS for this tenant | `pi-company:_config:agents` session | Mutable, event-sourced (free history) | At agent-instantiation; reduce-over-events |
| **Run-level lifecycle** — what's happening in this run | `pi-company:{run_id}:_meta` session | Append-only events: `run_started`, `scenario_loaded`, `agent_joined`, `run_ended` | At resume; required to reconstruct a run |
| **Per-agent working memory** — what the agent has DONE | `pi-company:{run_id}:{role}` session | Append-only event log | On every wake-up; recent N events into the prompt |

**Code = defaults. Sessions = overrides.** Without any config events, the demo runs purely on code defaults. With them, an operator (the CEO, in the demo's case) can hot-tune any agent's system prompt, tool list, or model selection without redeploying.

The merge function in `config.ts`:

```ts
async function loadAgentConfig(role: string): Promise<AgentConfig> {
  const defaults = CODE_DEFAULTS[role];
  const overrides = await reduceConfigSession(
    "pi-company",
    "_config:agents",
    role
  );
  return mergeDeep(defaults, overrides);
}
```

`reduceConfigSession` reads the events with `kind: "agent_override"` filtered by `role`, applies them in seq order, returns the final patch. Snapshots aren't necessary at v1 scale (config sessions stay tiny — handfuls of events per role lifetime).

The CEO's `update_agent_config(role, patch)` tool appends one event to `_config:agents`:

```json
{ "kind": "agent_override", "role": "engineer", "patch": {"system_prompt_suffix": "be terse"} }
```

The next agent loop iteration (or next contractor hire) picks up the new config. Hot-reloadable without restarts.

### 5.6 Per-agent working memory (event log)

Each agent's `pi-company:{run_id}:{role}` session stores its full history as a sequence of typed events:

- `directive`: a CEO message arrived.
- `peer_message`: a peer sent a direct message.
- `thinking`: the agent's intermediate reasoning trace (kept for debugging; can be filtered from prompt context).
- `tool_call`: tool invocation with input + output.
- `deliverable`: a structured output (a doc, a draft email, a prospect record).
- `contractor_hired` / `contractor_returned`: bookkeeping for fork events.

The agent loop:
1. On wake-up (CEO directive or peer message), the agent loads recent N events from its session via `append_and_fetch`.
2. Constructs a prompt: system prompt (role description) + recent events serialized as a transcript.
3. Calls the LLM. Parses tool calls.
4. Each tool call appends an event to the agent's session and (if relevant) publishes to a channel.
5. Returns to listening on its channels.

`hire_contractor(task_brief)` triggers a fork:
1. Agent's `memory.ts` calls `POST /api/sessions/{parent_session}/fork`, gets a child session ID.
2. Spawns a new pi-mono agent loop on the child session with a system prompt scoped to `task_brief`.
3. Contractor runs, broadcasts progress on the appropriate team channel, terminates when it hits `submit_for_review` or `report_to_ceo`.
4. Parent agent reads contractor's final deliverable from the child session's last event (or via a `report_back` event published on the team channel).

### 5.7 Fork patterns surfaced in the demo

All three patterns map to the SAME `hire_contractor` tool — only the prompt context differs.

1. **Hard-task handoff.** Engineer hits a thorny task. Tool call: `hire_contractor("implement the HNSW gradient-descent inner loop, here's the constraints: ...")`. Contractor inherits all prior context (the spec, the codebase notes, the design discussion), works in parallel.
2. **Speculative exploration.** CEO directs `> CEO: Engineer, also explore approach B in parallel.` Engineer interprets this and calls `hire_contractor("pursue approach B for the indexing problem")`. Original Engineer pursues A; contractor pursues B. Two child sessions diverge from a common parent state.
3. **Specialist delegation.** Marketing hires a contractor specifically for `"write punchy 1-line tagline candidates"`. The contractor focuses narrowly on that sub-task while parent Marketing continues drafting the broader campaign.

### 5.8 TUI

Single chronological log occupies most of the terminal. Each line color-coded by agent (`[CEO]`, `[CTO]`, `[ENG]`, `[MKT]`, `[SAL]`, `[CONTRACTOR]`). CEO prompt at the bottom (`> CEO: `).

Implementation: pi-mono's TUI library (`@mariozechner/pi-tui` or whatever the actual package name is — verify in plan) has differential rendering. If integration is awkward, fall back to plain `chalk` + `readline`. Decision deferred to plan-time.

Activity log entries:
- CEO directives: full text.
- Agent actions: `[ENG] (tool_call) write_doc(title="HNSW spec", words=180)` — short summary, not full content. Long deliverables expand on `> CEO: show me <thing>`.
- Contractor lifecycle: `[ENG] hired contractor: "pursue approach B for indexing"` and `[CONTRACTOR/eng-fork-1] returned with deliverable: ...`.
- Cross-channel chatter: collapsed unless CEO `--verbose`.

### 5.9 Persistence

Sample app auto-starts `agentdb serve --dev` as a child process at startup. The child uses a tempdir (so each run is fresh by default).

`--resume <run-id>` flag: instead of starting a fresh server, looks for a running AgentDB on the conventional port (or starts a non-dev server pointed at a stable directory, TBD). Loads the existing sessions for that run-id. Demonstrates persistence.

The `agentdb serve --dev` process is terminated when the sample exits (catch SIGINT/SIGTERM, kill the child).

### 5.10 The `--scenario launch` script

Pre-baked sequence of CEO directives delivered with timing pauses, runs the company hands-off for ~3 minutes:

```
T+0s:    CEO: Launch our new vector search service this quarter. Engineering and Marketing, propose plans.
T+30s:   (let agents work)
T+60s:   CEO: Engineer, also explore approach B in parallel.   # triggers speculative-exploration fork
T+90s:   (let work continue)
T+105s:  CEO: update_agent_config(role="marketing", patch={"system_prompt_suffix": "Be terse and punchy. Aim for under 80 chars per line."})
T+120s:  CEO: Marketing, redo the campaign with the new style. # demonstrates hot config update — Marketing's next loop picks up the override
T+150s:  CEO: Sales, give me the prospect status.
T+180s:  CEO: Wrap it up. CTO, summary please.
T+200s:  (final summaries print, demo exits)
```

The scenario script is a TypeScript array of `(delay_ms, directive)` pairs.

### 5.11 Pi-mono integration shape

Read pi-mono's published packages during plan-time:
- Use pi-mono's `Agent` (or equivalent) as the LLM-loop runtime per agent. Subclass / configure it.
- Use pi-mono's tool-calling mechanism for our 9 tools. Tools return events that we append to the AgentDB session AND optionally publish to a channel.
- Use pi-mono's TUI library if it fits. Verify package availability; fall back to plain TUI if it's awkward.
- Use pi-mono's LLM provider abstraction; thin wrapper for MiniMax base-URL override.

**Hard rule:** no PRs to pi-mono. Everything we need either already exists or we work around in our sample app. If we hit a blocker, we report up before changing scope.

---

## 6. Tests

### Unit tests (always run, fast)

- `memory.test.ts`: session wrapper round-trips. Mock the AgentDB HTTP client; assert that `recordEvent({kind: "directive", text: ...})` appends one event and `loadHistory(n=10)` returns the last 10.
- `config.test.ts`: layered config loader. Code defaults defined; `_config:agents` session has 2 override events for `engineer`; assert `loadAgentConfig("engineer")` returns deep-merge of code + both overrides applied in seq order. Also assert that an agent without any overrides returns code defaults verbatim.
- `tools/hire_contractor.test.ts`: mock the AgentDB fork endpoint; assert that calling `hire_contractor` (a) forks the parent session, (b) spawns a child agent loop with the task_brief, (c) the child broadcasts a `contractor_returned` event when it terminates.
- `tools/update_agent_config.test.ts`: assert that calling the tool appends one `agent_override` event to `_config:agents` with the role and patch fields populated.
- `channels.test.ts`: publish + subscribe round-trips against a mock fetch.

### Integration smoke (gated)

- `smoke/launch.test.ts`: gated by `AGENTDB_PI_COMPANY_SMOKE=1`. Boots `agentdb serve --dev` as a child, runs the `--scenario launch` end-to-end, asserts:
  - All 5 agents made at least one tool call.
  - At least one `hire_contractor` fork happened.
  - At least one event landed in each of the 3 channels.
  - The demo terminated cleanly within 5 minutes.
- Total runtime: ~3-5 minutes. Requires an LLM API key; skip if `AGENTDB_DEMO_API_KEY` unset.

### Smoke shell script

`scripts/smoke-pi-company.sh`: same as the integration test but in shell — boots the demo with the scenario flag, asserts non-zero exit + at least N log lines containing expected keywords. Used in CI when the env has an API key.

---

## 7. High-level milestones (for the implementation plan)

The plan in `docs/superpowers/plans/...` will detail step-by-step. High level:

1. **Project skeleton.** `package.json`, `tsconfig.json`, depend on pi-mono published packages + `@agentdb/client` (file: link until npm publish).
2. **Memory wrapper.** `memory.ts` — session-as-event-log abstraction wrapping the AgentDB TypeScript SDK from Phase 1. Unit tested.
3. **Channels wrapper.** `channels.ts` — `publish()` + `subscribe()` wrapping the Phase 2 broadcast SDK. Unit tested.
4. **Layered config loader.** `config.ts` — code-defaults table per role + `reduceConfigSession()` over the `_config:agents` session + `loadAgentConfig(role)` merger. Unit tested.
5. **LLM provider router.** `llm.ts` — wires up pi-mono's provider abstraction with our env-var convention. Verify MiniMax via base-URL override.
6. **Base agent loop.** `agent.ts` — pi-mono `Agent` subclass that reads from session + channels, calls LLM via config, dispatches tool calls. Reloads config on each wake-up so overrides take effect mid-run.
7. **The 10 tools.** Each in its own file, returning a pi-mono-compatible tool spec. Includes `update_agent_config` (CEO-only).
8. **`hire_contractor` tool — the headline.** Forks parent session, spawns child agent loop with task_brief, contractor reads config too.
9. **Orchestrator.** Boots all 5 agents on their sessions, wires channel subscriptions, owns the run lifecycle. Writes `_meta` lifecycle events.
10. **TUI.** pi-mono's TUI lib if it fits; chalk+readline fallback.
11. **Server child-process management.** Auto-start `agentdb serve --dev`, terminate on exit.
12. **Scripted scenario.** `scenarios/launch.ts` with the 6-directive timing sequence (now includes a config-update beat at T+105s).
13. **Resume mode.** `--resume <run-id>` loads existing sessions including `_config:agents` overrides.
14. **Smoke + tests.** Unit + gated integration + shell smoke. Smoke asserts a config event was applied.
15. **README + `agent-db.md` convention writeup.** Setup, scenario walkthrough, "what this proves" section, plus a short section in `docs/agent-db.md` documenting the config-as-events pattern as a recommended AgentDB convention.

Estimated diff size: 1700-2200 LoC TypeScript (+200 from base for the config layer + new tool) + 350 LoC docs (incl. agent-db.md addendum) + 150 LoC test/smoke harness.

---

## 8. Open questions

1. **TUI library.** Use pi-mono's TUI? Or chalk+readline? Decision deferred to plan-time, verified by inspecting pi-mono's actual exports.
2. **MiniMax API base URL.** Verify `https://api.minimax.chat/v1` is the OpenAI-compatible endpoint at impl time — their docs may have moved.
3. **Agent timeout / cost cap.** Cap each `hire_contractor` to N LLM calls (default 5)? Probably yes to prevent runaway recursion (a contractor hires another contractor hires another...). Plan-time decision.
4. **CEO chat history.** Store CEO messages in a `pi-company:{run_id}:ceo` session for `--resume`? Yes — without it, resume picks up agent state but loses the directive history.
5. **Fork depth limit.** Should contractors be allowed to hire their own contractors? Recursion-bounded by the cost cap above. v1 says yes (recursive contracting works); v2 may add an explicit depth limit.
6. **`agentdb serve --dev` child process port.** Default is `127.0.0.1:7878`; pick an ephemeral port to avoid collisions if the user already has a dev server running. Plan-time.
7. **TypeScript SDK published or file-linked?** v1 file-links to `clients/typescript/`; demo only runs from a checkout. Once npm publish lands (separate task), switch to a normal dep.

---

## 9. Non-goals (re-emphasized)

- **Not a product.** This is a sample app + demo. Don't add prod-grade error handling, retries, multi-tenant isolation, etc.
- **Not a framework.** Don't generalize "build any AI company" — this is one specific company, one specific scenario, one set of agents.
- **Not a chatbot.** The CEO interaction is directive-style ("Launch the product."), not conversational small talk.
- **Not a replacement for pi-mono.** We're a consumer of pi-mono, not a competitor or alternative.
- **Not multi-language.** TypeScript only in v1. Python/Go ports could happen later if pi-mono-equivalent runtimes exist there (they don't, currently).
