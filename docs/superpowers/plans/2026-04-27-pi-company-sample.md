# Pi Company Sample App Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A 5-agent "AI company" TypeScript sample app at `examples/agents/typescript/pi-company/`, built on pi-mono + AgentDB, demonstrating sessions-as-memory, fork-as-on-demand-contractor, broadcast-as-team-coordination, and the config-as-events convention for hot-tunable agent configuration.

**Architecture:** A single npm package depending on pi-mono's published packages and AgentDB's TypeScript SDK (file-linked from `clients/typescript/`). One AgentDB tenant (`pi-company`) hosts run-scoped sessions per agent. Code holds role definitions; a `_config:agents` session in the tenant holds runtime overrides; agents merge code + overrides at instantiation. Each agent has a `hire_contractor` tool that forks its session for parallel work; the CEO has an `update_agent_config` tool to hot-tune any role.

**Tech Stack:** TypeScript (Node ≥18), `@agentdb/client` (file-linked SDK), pi-mono published packages (specific names pinned in Task 1), `node:test` + `node:assert` for tests (matches Phase 1/2 SDK convention), `chalk` for TUI color (fallback if pi-mono's TUI doesn't fit), `readline` for CEO prompt.

**Spec:** [`docs/superpowers/specs/2026-04-27-pi-company-sample-design.md`](../specs/2026-04-27-pi-company-sample-design.md) — read before starting any task. The spec's § 5.5 (layered state model) is load-bearing.

**Branch:** `feature/pi-company-sample` (created in a worktree before Task 1).

---

## File Structure

### Created (TypeScript source)

| File | Responsibility |
|---|---|
| `examples/agents/typescript/pi-company/package.json` | NPM package metadata; pi-mono + `@agentdb/client` deps. |
| `examples/agents/typescript/pi-company/tsconfig.json` | TypeScript config: ESM, target ES2022. |
| `examples/agents/typescript/pi-company/.gitignore` | Excludes `node_modules/`, `dist/`. |
| `examples/agents/typescript/pi-company/README.md` | Setup + scenario walkthrough + "what this proves." |
| `examples/agents/typescript/pi-company/src/index.ts` | Entry: parses `--scenario` / `--resume` / interactive flags, boots. |
| `examples/agents/typescript/pi-company/src/types.ts` | Shared TypeScript interfaces (`AgentConfig`, `AgentEvent`, etc.). |
| `examples/agents/typescript/pi-company/src/memory.ts` | Session wrapper: `recordEvent`, `loadHistory(n)`, `forkSession`. |
| `examples/agents/typescript/pi-company/src/config.ts` | Layered config: code defaults + reduce-over-events merger. |
| `examples/agents/typescript/pi-company/src/channels.ts` | Broadcast publish/subscribe wrapping the AgentDB SDK. |
| `examples/agents/typescript/pi-company/src/llm.ts` | Provider router: openai/anthropic/minimax via env vars. |
| `examples/agents/typescript/pi-company/src/agent.ts` | Per-agent loop: read directives, call LLM, dispatch tool calls. |
| `examples/agents/typescript/pi-company/src/orchestrator.ts` | Boots agents, owns run lifecycle, writes `_meta` events. |
| `examples/agents/typescript/pi-company/src/tui.ts` | Color-coded chronological log + CEO prompt. |
| `examples/agents/typescript/pi-company/src/server.ts` | Auto-start `agentdb serve --dev` as a child process. |
| `examples/agents/typescript/pi-company/src/agents/cto.ts` | CTO role definition (system prompt, tools, default model). |
| `examples/agents/typescript/pi-company/src/agents/engineer.ts` | Engineer role definition. |
| `examples/agents/typescript/pi-company/src/agents/marketing.ts` | Marketing role definition. |
| `examples/agents/typescript/pi-company/src/agents/sales.ts` | Sales role definition. |
| `examples/agents/typescript/pi-company/src/agents/contractor.ts` | Contractor role-template (used by `hire_contractor`). |
| `examples/agents/typescript/pi-company/src/tools/announce.ts` | CEO/agent post to `announcements` channel. |
| `examples/agents/typescript/pi-company/src/tools/direct_message.ts` | Send to a specific agent's session. |
| `examples/agents/typescript/pi-company/src/tools/delegate_to_engineer.ts` | CTO assigns work to Engineer. |
| `examples/agents/typescript/pi-company/src/tools/write_doc.ts` | Engineer creates a doc deliverable. |
| `examples/agents/typescript/pi-company/src/tools/submit_for_review.ts` | Engineer submits doc for CTO review. |
| `examples/agents/typescript/pi-company/src/tools/report_to_ceo.ts` | Any agent posts status to CEO. |
| `examples/agents/typescript/pi-company/src/tools/add_prospect.ts` | Sales adds a prospect record. |
| `examples/agents/typescript/pi-company/src/tools/draft_outreach.ts` | Sales drafts outreach copy. |
| `examples/agents/typescript/pi-company/src/tools/draft_campaign.ts` | Marketing drafts campaign copy. |
| `examples/agents/typescript/pi-company/src/tools/hire_contractor.ts` | Forks parent session, spawns sub-agent. |
| `examples/agents/typescript/pi-company/src/tools/update_agent_config.ts` | CEO-only: append config-override event. |
| `examples/agents/typescript/pi-company/src/tools/index.ts` | Exports tool sets keyed by role. |
| `examples/agents/typescript/pi-company/src/scenarios/launch.ts` | The "launch a new product" scripted directive sequence. |
| `examples/agents/typescript/pi-company/tests/memory.test.ts` | Unit tests for the session wrapper. |
| `examples/agents/typescript/pi-company/tests/config.test.ts` | Unit tests for the layered config loader. |
| `examples/agents/typescript/pi-company/tests/channels.test.ts` | Unit tests for the channels wrapper. |
| `examples/agents/typescript/pi-company/tests/tools/hire_contractor.test.ts` | Unit test for the fork tool. |
| `examples/agents/typescript/pi-company/tests/tools/update_agent_config.test.ts` | Unit test for the config-update tool. |
| `examples/agents/typescript/pi-company/tests/smoke/launch.test.ts` | Gated smoke that runs the full scenario. |

### Created (scripts + docs)

| File | Responsibility |
|---|---|
| `scripts/smoke-pi-company.sh` | Shell smoke wrapping the `--scenario launch` run. |
| `docs/agent-db.md` (modified) | Append section documenting the config-as-events convention. |

---

## Tasks

Each task ends with a commit. Tasks 0–4 are independent setup; tasks 5–14 build on each other roughly in order.

---

### Task 0: Discover pi-mono package surface

This task pins down which pi-mono npm packages we depend on and their key exports. Subsequent tasks reference the names this task captures.

**Files:**
- Create: `examples/agents/typescript/pi-company/PI_MONO_NOTES.md` (a workspace note, NOT shipped — added to `.gitignore` after Task 1)

- [ ] **Step 1: Identify pi-mono packages**

```bash
# Browse the pi-mono monorepo on npm
npm search @mariozechner/pi 2>&1 | head -30
# OR: visit https://github.com/badlogic/pi-mono and read packages/ in the repo
```

Capture the names and one-line summaries of all `@mariozechner/pi-*` packages. We expect (verify):
- An LLM API abstraction package (multi-provider: OpenAI, Anthropic, possibly more).
- An agent runtime / agent loop package.
- A TUI library with differential rendering.
- (Possibly) a tool-spec format package.

- [ ] **Step 2: Identify the key exports we'll need**

For each candidate package, read the README / `index.ts` to identify:
- The top-level Agent / runtime class (name, constructor signature).
- The tool spec type (what does pi-mono expect a "tool" to look like?).
- The LLM provider client classes / factory (name, supported providers).
- The TUI primitives (does it expose a stream/log view + an input prompt?).

Write these to `examples/agents/typescript/pi-company/PI_MONO_NOTES.md`:

```markdown
# pi-mono integration notes (NOT SHIPPED)

## Packages we'll use (pinned versions)
- `@mariozechner/pi-<NAME>@<VERSION>` — <one-line>
- `@mariozechner/pi-<NAME>@<VERSION>` — <one-line>

## Key exports
- Agent class: `import { <ClassName> } from "@mariozechner/pi-<pkg>"`
  - Constructor: `new <ClassName>({llm, systemPrompt, tools, ...})`
  - Method to run a single turn: `<methodName>(input) -> <ReturnType>`

## Tool spec format
- A tool is `{name, description, parameters (JSON schema), handler: (args) => Promise<result>}` (verify exact shape)

## LLM providers supported out-of-box
- OpenAI: `import { <Class> } from "@mariozechner/pi-<pkg>"` — supports custom `baseUrl`? <Y/N>
- Anthropic: `import { <Class> } from "@mariozechner/pi-<pkg>"`
- (others)

## TUI
- `import { <api> } from "@mariozechner/pi-<pkg>"` — has differential rendering log + input prompt? <Y/N>
- If N: we'll fall back to chalk + readline.

## MiniMax integration plan
- pi-mono's OpenAI client supports custom `baseUrl=https://api.minimax.chat/v1`: <Y/N>
- If N: 50-LoC adapter using pi-mono's lower-level primitives; describe approach.
```

If pi-mono's API differs significantly from what the spec assumes, STOP and report — we may need a spec adjustment.

- [ ] **Step 3: Commit the notes**

The notes file is a workspace artifact, not shipped. Add it to the project's `.gitignore` after Task 1 lands. For now:

```bash
mkdir -p examples/agents/typescript/pi-company
# (write PI_MONO_NOTES.md)
git add examples/agents/typescript/pi-company/PI_MONO_NOTES.md
git commit -m "wip(pi-company): pi-mono integration discovery notes"
```

---

### Task 1: Project skeleton

**Files:**
- Create: `examples/agents/typescript/pi-company/package.json`
- Create: `examples/agents/typescript/pi-company/tsconfig.json`
- Create: `examples/agents/typescript/pi-company/.gitignore`
- Create: `examples/agents/typescript/pi-company/README.md` (placeholder)
- Create: `examples/agents/typescript/pi-company/src/index.ts` (stub)

- [ ] **Step 1: Create `package.json`**

```json
{
  "name": "@agentdb-examples/pi-company",
  "version": "0.0.1",
  "description": "5-agent AI company on AgentDB + pi-mono — sample app",
  "type": "module",
  "private": true,
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "start": "node --experimental-strip-types src/index.ts",
    "test": "node --experimental-strip-types --test tests/**/*.test.ts"
  },
  "engines": { "node": ">=18" },
  "dependencies": {
    "agentdb": "file:../../../../clients/typescript",
    "chalk": "^5",
    "@mariozechner/pi-AGENT-PACKAGE": "<VERSION>",
    "@mariozechner/pi-LLM-PACKAGE": "<VERSION>",
    "@mariozechner/pi-TUI-PACKAGE": "<VERSION>"
  },
  "devDependencies": {
    "typescript": "^5.4",
    "@types/node": "^20"
  }
}
```

Replace `pi-AGENT-PACKAGE`, `pi-LLM-PACKAGE`, `pi-TUI-PACKAGE` with the actual package names from `PI_MONO_NOTES.md` (Task 0). If pi-mono uses a single bundle package, just one entry.

The `agentdb` dep uses `file:` link — points at `clients/typescript/` from Phase 1. Once `@agentdb/client` is published to npm, swap to a normal version dep.

- [ ] **Step 2: Create `tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "strict": true,
    "esModuleInterop": true,
    "lib": ["ES2022"],
    "outDir": "./dist",
    "declaration": true,
    "skipLibCheck": true
  },
  "include": ["src/**/*", "tests/**/*"]
}
```

- [ ] **Step 3: Create `.gitignore`**

```
node_modules/
dist/
PI_MONO_NOTES.md
.DS_Store
```

- [ ] **Step 4: Create placeholder `README.md`**

```markdown
# Pi Company — AgentDB Sample App

5-agent AI company on AgentDB + pi-mono. Demo of sessions-as-memory,
fork-as-on-demand-contractor, broadcast-as-team-coordination, and
config-as-events.

(Setup + walkthrough TBD; placeholder, filled in by the README task.)
```

- [ ] **Step 5: Create entry stub `src/index.ts`**

```ts
console.log("pi-company: not yet implemented");
```

- [ ] **Step 6: Install + verify build**

```bash
cd examples/agents/typescript/pi-company
npm install 2>&1 | tail -5
npm run build 2>&1 | tail -5
```

Both should succeed. If `npm install` fails because the `agentdb` file-link path is wrong (relative path different than `../../../../clients/typescript`), correct it. The relative depth depends on the example's depth — verify with `realpath ../../../../clients/typescript`.

- [ ] **Step 7: Commit**

```bash
git add examples/agents/typescript/pi-company/package.json examples/agents/typescript/pi-company/tsconfig.json examples/agents/typescript/pi-company/.gitignore examples/agents/typescript/pi-company/README.md examples/agents/typescript/pi-company/src/index.ts
# Note: package-lock.json may also be staged
git add examples/agents/typescript/pi-company/package-lock.json 2>/dev/null || true
git commit -m "feat(pi-company): project skeleton"
```

---

### Task 2: Memory wrapper

Wraps the AgentDB TypeScript SDK with a session-as-event-log abstraction. This is the foundation every agent uses for persistence.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/types.ts`
- Create: `examples/agents/typescript/pi-company/src/memory.ts`
- Create: `examples/agents/typescript/pi-company/tests/memory.test.ts`

- [ ] **Step 1: Define shared types in `src/types.ts`**

```ts
// Phase 1's AgentDB TypeScript SDK exposes Event from "agentdb".
import type { Event as AgentDBEvent } from "agentdb";

export type AgentRole = "ceo" | "cto" | "engineer" | "marketing" | "sales" | "contractor";

export interface AgentEvent extends AgentDBEvent {
  // Higher-level wrapper — actual events live in payload.
}

export interface RunIdentity {
  tenant: string;       // always "pi-company" for the demo
  runId: string;        // e.g. "r-1714063200"
}

export interface AgentConfig {
  role: AgentRole;
  systemPrompt: string;
  tools: string[];      // tool names; resolved at agent-construction time
  model: string;
  systemPromptSuffix?: string;
}

export type ConfigPatch = Partial<Omit<AgentConfig, "role">>;
```

- [ ] **Step 2: Write the failing test**

Create `tests/memory.test.ts`:

```ts
import { test } from "node:test";
import assert from "node:assert/strict";
import { Memory } from "../src/memory.ts";

class FakeClient {
  appended: any[] = [];
  fetched: any[] = [];
  async append(_args: { tenant: string; session: string; event: any; fetchLastN: number }) {
    this.appended.push({ ..._args.event });
    return { events: this.fetched.slice(-_args.fetchLastN) };
  }
  async forkSession(_args: { tenant: string; session: string; childSession: string }) {
    return { childSession: _args.childSession };
  }
}

test("Memory.recordEvent appends one event with the kind", async () => {
  const fc = new FakeClient();
  const mem = new Memory(fc as any, { tenant: "pi-company", runId: "r-1" }, "engineer");
  await mem.recordEvent({ kind: "directive", text: "build it" });
  assert.equal(fc.appended.length, 1);
  assert.equal(fc.appended[0].kind, "directive");
  assert.equal(fc.appended[0].text, "build it");
});

test("Memory.recordEvent computes the right session name", async () => {
  const fc = new FakeClient();
  const mem = new Memory(fc as any, { tenant: "pi-company", runId: "r-1" }, "engineer");
  await mem.recordEvent({ kind: "directive", text: "x" });
  // Verify by inspecting what FakeClient's append would have received via tenant/session:
  // We call recordEvent again and capture.
  const observed: any[] = [];
  fc.appended = [];
  (fc as any).append = async (a: any) => { observed.push(a); return { events: [] }; };
  await mem.recordEvent({ kind: "directive", text: "y" });
  assert.equal(observed[0].session, "r-1:engineer");
});

test("Memory.forkSession returns a child session ID with run-scoped naming", async () => {
  const fc = new FakeClient();
  const mem = new Memory(fc as any, { tenant: "pi-company", runId: "r-1" }, "engineer");
  const child = await mem.forkSession("contractor-1");
  assert.equal(child, "r-1:engineer:contractor-1");
});
```

- [ ] **Step 3: Run, expect FAIL (file doesn't exist)**

```bash
cd examples/agents/typescript/pi-company
npm test 2>&1 | tail -15
```
Expected: cannot find module `../src/memory.ts`.

- [ ] **Step 4: Implement `src/memory.ts`**

```ts
import type { Client } from "agentdb";
import type { AgentEvent, AgentRole, RunIdentity } from "./types.ts";

export interface MemoryEvent {
  kind: string;
  [k: string]: unknown;
}

/**
 * Session-as-event-log wrapper for one agent.
 *
 * Session naming convention: `{runId}:{role}` for per-agent working memory.
 * Forked contractor sessions use `{runId}:{role}:{contractor-tag}`.
 */
export class Memory {
  constructor(
    private readonly client: Client,
    public readonly run: RunIdentity,
    public readonly role: AgentRole,
  ) {}

  get sessionName(): string {
    return `${this.run.runId}:${this.role}`;
  }

  async recordEvent(event: MemoryEvent): Promise<void> {
    await this.client.append({
      tenant: this.run.tenant,
      session: this.sessionName,
      event: event as any,
      fetchLastN: 0,
    });
  }

  async loadHistory(n: number): Promise<AgentEvent[]> {
    const resp = await this.client.append({
      tenant: this.run.tenant,
      session: this.sessionName,
      event: { kind: "_noop" } as any,
      fetchLastN: n,
    });
    // Filter out the _noop we just appended.
    return resp.events.filter((e: any) => e.payload?.kind !== "_noop") as AgentEvent[];
  }

  async forkSession(childTag: string): Promise<string> {
    const childSession = `${this.sessionName}:${childTag}`;
    await (this.client as any).forkSession({
      tenant: this.run.tenant,
      session: this.sessionName,
      childSession,
    });
    return childSession;
  }
}
```

The `_noop` trick for `loadHistory` is because Phase 1's SDK's `append` is the only way to get back the last N events. If the Phase 1 SDK has a separate `fetch` / `getRecent` method, use that instead — verify in `clients/typescript/src/client.ts`. If a cleaner read API exists, simplify the implementation.

- [ ] **Step 5: Run tests, expect PASS**

```bash
npm test 2>&1 | tail -15
```
Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add examples/agents/typescript/pi-company/src/types.ts examples/agents/typescript/pi-company/src/memory.ts examples/agents/typescript/pi-company/tests/memory.test.ts
git commit -m "feat(pi-company): Memory wrapper with session-as-event-log"
```

---

### Task 3: Channels wrapper

Wraps the AgentDB SDK's `broadcast` and `subscribeChannel` in a per-channel publish/subscribe abstraction.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/channels.ts`
- Create: `examples/agents/typescript/pi-company/tests/channels.test.ts`

- [ ] **Step 1: Write the failing test**

```ts
import { test } from "node:test";
import assert from "node:assert/strict";
import { Channels, channelName } from "../src/channels.ts";

test("channelName composes runId and channel", () => {
  assert.equal(channelName({ tenant: "pi-company", runId: "r-1" }, "announcements"), "r-1:announcements");
});

test("Channels.publish forwards to broadcast()", async () => {
  const calls: any[] = [];
  const fakeClient = {
    broadcast: async (args: any) => { calls.push(args); },
  };
  const ch = new Channels(fakeClient as any, { tenant: "pi-company", runId: "r-1" });
  await ch.publish("announcements", { kind: "ping" });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].channel, "r-1:announcements");
  assert.equal(calls[0].payload.kind, "ping");
});

test("Channels.subscribe yields broadcasts", async () => {
  async function* fakeSub(): AsyncIterable<any> {
    yield { tenant: "pi-company", channel: "r-1:announcements", ts_ms: 1, payload: { kind: "a" } };
    yield { tenant: "pi-company", channel: "r-1:announcements", ts_ms: 2, payload: { kind: "b" } };
  }
  const fakeClient = {
    subscribeChannel: () => fakeSub(),
  };
  const ch = new Channels(fakeClient as any, { tenant: "pi-company", runId: "r-1" });
  const got: any[] = [];
  for await (const msg of ch.subscribe("announcements")) {
    got.push(msg.payload.kind);
    if (got.length >= 2) break;
  }
  assert.deepEqual(got, ["a", "b"]);
});
```

- [ ] **Step 2: Run, expect FAIL**

```bash
npm test 2>&1 | tail -15
```

- [ ] **Step 3: Implement `src/channels.ts`**

```ts
import type { Client, BroadcastMessage } from "agentdb";
import type { RunIdentity } from "./types.ts";

export function channelName(run: RunIdentity, name: string): string {
  return `${run.runId}:${name}`;
}

export class Channels {
  constructor(
    private readonly client: Client,
    public readonly run: RunIdentity,
  ) {}

  async publish(name: string, payload: unknown): Promise<void> {
    await this.client.broadcast({
      channel: channelName(this.run, name),
      payload,
    });
  }

  subscribe(name: string): AsyncIterable<BroadcastMessage> {
    return this.client.subscribeChannel({
      channel: channelName(this.run, name),
    });
  }
}
```

- [ ] **Step 4: Run, expect PASS**

```bash
npm test 2>&1 | tail -10
```

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/channels.ts examples/agents/typescript/pi-company/tests/channels.test.ts
git commit -m "feat(pi-company): Channels wrapper for broadcast pub/sub"
```

---

### Task 4: Layered config loader

Code defaults table per role + reduce-over-events from `_config:agents` session + merge.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/config.ts`
- Create: `examples/agents/typescript/pi-company/tests/config.test.ts`

- [ ] **Step 1: Write the failing tests**

```ts
import { test } from "node:test";
import assert from "node:assert/strict";
import { mergePatch, reduceConfigEvents, CODE_DEFAULTS } from "../src/config.ts";
import type { AgentConfig, ConfigPatch } from "../src/types.ts";

test("CODE_DEFAULTS has entries for all 5 employee roles", () => {
  for (const role of ["cto", "engineer", "marketing", "sales", "contractor"] as const) {
    assert.ok(CODE_DEFAULTS[role], `missing defaults for ${role}`);
    assert.ok(CODE_DEFAULTS[role].systemPrompt.length > 50, `${role} prompt too short`);
    assert.ok(CODE_DEFAULTS[role].tools.length > 0, `${role} has no tools`);
  }
});

test("mergePatch deep-merges a patch into a config", () => {
  const base: AgentConfig = {
    role: "engineer",
    systemPrompt: "default prompt",
    tools: ["a", "b"],
    model: "claude-haiku-4-5",
  };
  const patch: ConfigPatch = { systemPromptSuffix: "be terse", model: "gpt-4o-mini" };
  const merged = mergePatch(base, patch);
  assert.equal(merged.systemPrompt, "default prompt");
  assert.equal(merged.systemPromptSuffix, "be terse");
  assert.equal(merged.model, "gpt-4o-mini");
  // Original unchanged.
  assert.equal(base.systemPromptSuffix, undefined);
});

test("reduceConfigEvents applies overrides in seq order, last write wins", () => {
  const events = [
    { payload: { kind: "agent_override", role: "engineer", patch: { systemPromptSuffix: "v1" } } },
    { payload: { kind: "agent_override", role: "engineer", patch: { systemPromptSuffix: "v2" } } },
    { payload: { kind: "agent_override", role: "marketing", patch: { systemPromptSuffix: "punchy" } } },
  ];
  const out = reduceConfigEvents(events as any, "engineer");
  assert.equal(out.systemPromptSuffix, "v2");
});

test("reduceConfigEvents returns empty patch when no events for role", () => {
  const out = reduceConfigEvents([], "engineer");
  assert.deepEqual(out, {});
});
```

- [ ] **Step 2: Run, expect FAIL**

```bash
npm test 2>&1 | tail -15
```

- [ ] **Step 3: Implement `src/config.ts`**

```ts
import type { AgentConfig, AgentRole, ConfigPatch, AgentEvent } from "./types.ts";
import type { Client } from "agentdb";

const CONFIG_SESSION = "_config:agents";

export const CODE_DEFAULTS: Record<Exclude<AgentRole, "ceo">, AgentConfig> = {
  cto: {
    role: "cto",
    systemPrompt: "You are the CTO. You receive directives from the CEO and delegate engineering work. Stay strategic, not in the weeds. When you receive a directive, decide whether to draft a feature spec yourself or delegate to the Engineer.",
    tools: ["delegate_to_engineer", "report_to_ceo", "hire_contractor"],
    model: "claude-haiku-4-5",
  },
  engineer: {
    role: "engineer",
    systemPrompt: "You are a senior engineer. Receive specs from the CTO. Produce technical docs (specs, RFCs, design notes) using write_doc. Submit for review when ready. If the task is complex, hire a contractor to explore in parallel.",
    tools: ["write_doc", "submit_for_review", "hire_contractor"],
    model: "claude-haiku-4-5",
  },
  marketing: {
    role: "marketing",
    systemPrompt: "You are head of marketing. Draft campaigns and copy. Coordinate with sales on messaging. If the task needs specialized work (e.g. punchy taglines), hire a contractor.",
    tools: ["draft_campaign", "report_to_ceo", "hire_contractor"],
    model: "claude-haiku-4-5",
  },
  sales: {
    role: "sales",
    systemPrompt: "You are head of sales. Build prospect lists and draft outreach. Stay aligned with marketing's messaging. Hire a contractor for prospect research at scale.",
    tools: ["add_prospect", "draft_outreach", "report_to_ceo"],
    model: "claude-haiku-4-5",
  },
  contractor: {
    role: "contractor",
    systemPrompt: "You are a contractor brought in to handle a specific task. Stay narrowly focused on the task brief. When done, post your deliverable via report_to_ceo.",
    tools: ["write_doc", "report_to_ceo"],
    model: "claude-haiku-4-5",
  },
};

export function mergePatch(base: AgentConfig, patch: ConfigPatch): AgentConfig {
  return { ...base, ...patch };
}

export function reduceConfigEvents(events: AgentEvent[], role: AgentRole): ConfigPatch {
  let patch: ConfigPatch = {};
  for (const ev of events) {
    const p: any = (ev as any).payload ?? ev;
    if (p?.kind === "agent_override" && p.role === role && p.patch) {
      patch = { ...patch, ...p.patch };
    }
  }
  return patch;
}

export async function loadAgentConfig(
  client: Client,
  tenant: string,
  role: Exclude<AgentRole, "ceo">,
): Promise<AgentConfig> {
  const defaults = CODE_DEFAULTS[role];
  const resp = await client.append({
    tenant,
    session: CONFIG_SESSION,
    event: { kind: "_noop" } as any,
    fetchLastN: 1024,
  });
  const events = resp.events.filter((e: any) => e.payload?.kind !== "_noop");
  const patch = reduceConfigEvents(events as any, role);
  return mergePatch(defaults, patch);
}

export async function appendConfigOverride(
  client: Client,
  tenant: string,
  role: AgentRole,
  patch: ConfigPatch,
): Promise<void> {
  await client.append({
    tenant,
    session: CONFIG_SESSION,
    event: { kind: "agent_override", role, patch } as any,
    fetchLastN: 0,
  });
}
```

- [ ] **Step 4: Run, expect PASS**

```bash
npm test 2>&1 | tail -10
```

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/config.ts examples/agents/typescript/pi-company/tests/config.test.ts
git commit -m "feat(pi-company): layered config loader (code defaults + _config:agents overrides)"
```

---

### Task 5: LLM provider router

Reads env vars; returns a pi-mono LLM client.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/llm.ts`

- [ ] **Step 1: Implement provider router**

The exact pi-mono import names come from `PI_MONO_NOTES.md` (Task 0). The shape:

```ts
// The actual import path/name is from PI_MONO_NOTES.md.
// import { OpenAIClient, AnthropicClient, type LLMClient } from "@mariozechner/pi-llm";

export type Provider = "openai" | "anthropic" | "minimax";

export interface LLMHandle {
  // Whatever pi-mono's agent runtime expects as its `llm` field.
  // Likely something with a `complete(messages, tools) -> Promise<...>` shape.
}

export function getLLM(model: string): LLMHandle {
  const provider = (process.env.AGENTDB_DEMO_PROVIDER ?? "anthropic") as Provider;
  const apiKey = process.env.AGENTDB_DEMO_API_KEY;
  if (!apiKey) {
    throw new Error(
      "AGENTDB_DEMO_API_KEY is required. Set it to your OpenAI / Anthropic / MiniMax API key.",
    );
  }
  switch (provider) {
    case "openai":
      // return new OpenAIClient({ apiKey, model });
      throw new Error("TODO: wire up pi-mono OpenAI client per PI_MONO_NOTES.md");
    case "anthropic":
      // return new AnthropicClient({ apiKey, model });
      throw new Error("TODO: wire up pi-mono Anthropic client per PI_MONO_NOTES.md");
    case "minimax":
      // return new OpenAIClient({ apiKey, model, baseUrl: "https://api.minimax.chat/v1" });
      throw new Error("TODO: wire up MiniMax via OpenAI base-URL override per PI_MONO_NOTES.md");
  }
}
```

The implementer fills in the actual pi-mono imports and constructors using `PI_MONO_NOTES.md`. If MiniMax isn't supported via base-URL override, fall back to the 50-LoC adapter approach noted in the spec § 5.4.

- [ ] **Step 2: Smoke check (no test needed yet)**

```bash
cd examples/agents/typescript/pi-company
npx tsc --noEmit src/llm.ts 2>&1 | tail -10
```

Should compile clean.

- [ ] **Step 3: Commit**

```bash
git add examples/agents/typescript/pi-company/src/llm.ts
git commit -m "feat(pi-company): LLM provider router (openai/anthropic/minimax)"
```

---

### Task 6: Base agent loop

The per-agent runtime built on pi-mono. Reads memory + channels, calls LLM, dispatches tool calls.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/agent.ts`

- [ ] **Step 1: Implement the agent loop**

```ts
import type { Client } from "agentdb";
import type { AgentRole, RunIdentity, AgentConfig } from "./types.ts";
import { Memory } from "./memory.ts";
import { Channels } from "./channels.ts";
import { loadAgentConfig } from "./config.ts";
import { getLLM } from "./llm.ts";

export interface AgentDeps {
  client: Client;
  run: RunIdentity;
  role: Exclude<AgentRole, "ceo">;
  channels: Channels;
  /** Set of channel names this role subscribes to. */
  subscriptions: string[];
  /** Tool registry: name → handler. Built in tools/index.ts. */
  toolRegistry: Record<string, ToolHandler>;
  /** Optional override (used by hire_contractor for child agents). */
  configOverride?: AgentConfig;
  /** Optional task brief (used by contractors). */
  taskBrief?: string;
}

export type ToolHandler = (args: Record<string, unknown>, ctx: ToolContext) => Promise<unknown>;

export interface ToolContext {
  memory: Memory;
  channels: Channels;
  client: Client;
  run: RunIdentity;
  role: AgentRole;
  /** For contractors: the parent's session, so results bubble up. */
  parentSession?: string;
}

export class Agent {
  private memory: Memory;
  private config!: AgentConfig;

  constructor(private deps: AgentDeps) {
    this.memory = new Memory(deps.client, deps.run, deps.role);
  }

  async start(): Promise<void> {
    this.config = this.deps.configOverride
      ?? (await loadAgentConfig(this.deps.client, this.deps.run.tenant, this.deps.role));

    if (this.deps.taskBrief) {
      await this.memory.recordEvent({ kind: "task_brief", text: this.deps.taskBrief });
    }

    // Spawn one async loop per subscription.
    for (const sub of this.deps.subscriptions) {
      this.runSubscriptionLoop(sub);
    }
  }

  private async runSubscriptionLoop(channelName: string): Promise<void> {
    for await (const msg of this.deps.channels.subscribe(channelName)) {
      // Skip self-published messages on this agent's own role.
      if ((msg.payload as any)?.from === this.deps.role) continue;
      await this.handleIncoming(channelName, msg.payload);
    }
  }

  private async handleIncoming(channel: string, payload: unknown): Promise<void> {
    await this.memory.recordEvent({ kind: "peer_message", channel, payload });
    const recent = await this.memory.loadHistory(20);
    const llm = getLLM(this.config.model);
    const sysPrompt = [this.config.systemPrompt, this.config.systemPromptSuffix].filter(Boolean).join("\n\n");
    const tools = this.deps.toolRegistry; // pi-mono's agent runtime takes a tool list

    // Call pi-mono's agent runtime here.
    // The exact API is from PI_MONO_NOTES.md — likely:
    //   const result = await runAgentTurn({ llm, systemPrompt: sysPrompt, history: recent, tools, input: payload });
    // For each tool call in result, look up in this.deps.toolRegistry and execute with ToolContext.
    //
    // After the turn, append the result events to memory (assistant text, tool calls/results).

    throw new Error("TODO: wire up pi-mono's agent runtime per PI_MONO_NOTES.md");
  }
}
```

The agent loop has an `async runSubscriptionLoop` per channel subscription. Each loop is independent; they don't coordinate. If multiple incoming messages arrive concurrently, the LLM is called concurrently — that's fine, AgentDB tolerates it.

The implementer fills in the pi-mono integration based on `PI_MONO_NOTES.md`. The skeleton above shows the conceptual flow; the actual lines wiring pi-mono's agent runtime depend on what pi-mono exports.

- [ ] **Step 2: Verify type-check**

```bash
cd examples/agents/typescript/pi-company
npx tsc --noEmit src/agent.ts 2>&1 | tail -10
```

Should compile clean (the `throw new Error("TODO")` is acceptable at this stage; it's not a placeholder for a missing impl, it's a runtime error if the integration isn't filled in).

- [ ] **Step 3: Commit**

```bash
git add examples/agents/typescript/pi-company/src/agent.ts
git commit -m "feat(pi-company): base Agent loop integrated with pi-mono runtime"
```

---

### Task 7: Tool implementations (non-fork tools)

The 7 "regular" tools: `announce`, `direct_message`, `delegate_to_engineer`, `write_doc`, `submit_for_review`, `report_to_ceo`, `add_prospect`, `draft_outreach`, `draft_campaign`. (`hire_contractor` and `update_agent_config` are Tasks 8 and 9.)

**Files:**
- Create: `examples/agents/typescript/pi-company/src/tools/announce.ts`
- Create: `examples/agents/typescript/pi-company/src/tools/direct_message.ts`
- Create: `examples/agents/typescript/pi-company/src/tools/delegate_to_engineer.ts`
- Create: `examples/agents/typescript/pi-company/src/tools/write_doc.ts`
- Create: `examples/agents/typescript/pi-company/src/tools/submit_for_review.ts`
- Create: `examples/agents/typescript/pi-company/src/tools/report_to_ceo.ts`
- Create: `examples/agents/typescript/pi-company/src/tools/add_prospect.ts`
- Create: `examples/agents/typescript/pi-company/src/tools/draft_outreach.ts`
- Create: `examples/agents/typescript/pi-company/src/tools/draft_campaign.ts`
- Create: `examples/agents/typescript/pi-company/src/tools/index.ts`

Each tool follows a consistent shape: a function returning a pi-mono-compatible tool spec. The handler:
1. Records a `tool_call` event in the agent's memory.
2. Performs the side effect (publish to channel, append a deliverable event, etc.).
3. Returns a string summary.

- [ ] **Step 1: Implement `announce`**

```ts
// src/tools/announce.ts
import type { ToolContext, ToolHandler } from "../agent.ts";

export const announce: ToolHandler = async (args, ctx) => {
  const text = String(args.text ?? "");
  await ctx.memory.recordEvent({ kind: "tool_call", tool: "announce", text });
  await ctx.channels.publish("announcements", { kind: "announcement", from: ctx.role, text });
  return `posted to announcements: ${text.slice(0, 60)}`;
};

export const announceSpec = {
  name: "announce",
  description: "Post a message to the company-wide announcements channel.",
  parameters: {
    type: "object",
    properties: { text: { type: "string", description: "The announcement text." } },
    required: ["text"],
  },
};
```

- [ ] **Step 2: Implement `direct_message`**

```ts
// src/tools/direct_message.ts
import type { ToolHandler } from "../agent.ts";
import { Memory } from "../memory.ts";

export const directMessage: ToolHandler = async (args, ctx) => {
  const target = String(args.role ?? "");
  const text = String(args.text ?? "");
  await ctx.memory.recordEvent({ kind: "tool_call", tool: "direct_message", target, text });
  // Append to the target's session as a peer_message event.
  const targetMemory = new Memory(ctx.client, ctx.run, target as any);
  await targetMemory.recordEvent({ kind: "peer_message", from: ctx.role, text });
  return `sent to ${target}: ${text.slice(0, 60)}`;
};

export const directMessageSpec = {
  name: "direct_message",
  description: "Send a private message to one specific role.",
  parameters: {
    type: "object",
    properties: {
      role: { type: "string", enum: ["cto", "engineer", "marketing", "sales"] },
      text: { type: "string" },
    },
    required: ["role", "text"],
  },
};
```

- [ ] **Step 3: Implement the rest (delegate_to_engineer, write_doc, submit_for_review, report_to_ceo, add_prospect, draft_outreach, draft_campaign)**

Each follows the same pattern. Distinct content per tool:

- `delegate_to_engineer`: publishes to `engineering` channel with `kind: "delegation"`.
- `write_doc(title, body)`: appends `{kind: "deliverable", type: "doc", title, body}` to memory; broadcasts `{kind: "doc_drafted", title}` to `engineering`.
- `submit_for_review(docId)`: publishes `{kind: "review_requested", docId, from: ctx.role}` to `engineering`.
- `report_to_ceo(text)`: publishes `{kind: "report", from: ctx.role, text}` to `announcements`.
- `add_prospect(name, company, notes)`: appends `{kind: "deliverable", type: "prospect", ...}` to memory; broadcasts to `gtm`.
- `draft_outreach(prospectId, body)`: appends `{kind: "deliverable", type: "outreach", ...}`; broadcasts to `gtm`.
- `draft_campaign(title, body)`: appends `{kind: "deliverable", type: "campaign", ...}`; broadcasts to `gtm`.

Show the full source for one to anchor the pattern (write_doc):

```ts
// src/tools/write_doc.ts
import type { ToolHandler } from "../agent.ts";

export const writeDoc: ToolHandler = async (args, ctx) => {
  const title = String(args.title ?? "untitled");
  const body = String(args.body ?? "");
  const docId = `${ctx.role}-doc-${Date.now()}`;
  await ctx.memory.recordEvent({ kind: "deliverable", type: "doc", docId, title, body });
  await ctx.channels.publish("engineering", { kind: "doc_drafted", from: ctx.role, docId, title });
  return `wrote doc ${docId}: ${title}`;
};

export const writeDocSpec = {
  name: "write_doc",
  description: "Create a doc deliverable (e.g., spec, RFC, design note). Returns a doc ID.",
  parameters: {
    type: "object",
    properties: {
      title: { type: "string" },
      body: { type: "string", description: "The full body text of the doc." },
    },
    required: ["title", "body"],
  },
};
```

The remaining tools follow the same shape. Implementer writes them out; each file is ~25 lines.

- [ ] **Step 4: Build the tool registry in `src/tools/index.ts`**

```ts
// src/tools/index.ts
import { announce, announceSpec } from "./announce.ts";
import { directMessage, directMessageSpec } from "./direct_message.ts";
import { delegateToEngineer, delegateToEngineerSpec } from "./delegate_to_engineer.ts";
import { writeDoc, writeDocSpec } from "./write_doc.ts";
import { submitForReview, submitForReviewSpec } from "./submit_for_review.ts";
import { reportToCeo, reportToCeoSpec } from "./report_to_ceo.ts";
import { addProspect, addProspectSpec } from "./add_prospect.ts";
import { draftOutreach, draftOutreachSpec } from "./draft_outreach.ts";
import { draftCampaign, draftCampaignSpec } from "./draft_campaign.ts";
// hire_contractor and update_agent_config are added in Tasks 8 and 9.

import type { ToolHandler } from "../agent.ts";

export const ALL_TOOL_HANDLERS: Record<string, ToolHandler> = {
  announce,
  direct_message: directMessage,
  delegate_to_engineer: delegateToEngineer,
  write_doc: writeDoc,
  submit_for_review: submitForReview,
  report_to_ceo: reportToCeo,
  add_prospect: addProspect,
  draft_outreach: draftOutreach,
  draft_campaign: draftCampaign,
};

export const ALL_TOOL_SPECS = {
  announce: announceSpec,
  direct_message: directMessageSpec,
  delegate_to_engineer: delegateToEngineerSpec,
  write_doc: writeDocSpec,
  submit_for_review: submitForReviewSpec,
  report_to_ceo: reportToCeoSpec,
  add_prospect: addProspectSpec,
  draft_outreach: draftOutreachSpec,
  draft_campaign: draftCampaignSpec,
};

/** Per-role tool name lists — must match CODE_DEFAULTS in config.ts. */
export const ROLE_TOOLS = {
  cto: ["delegate_to_engineer", "report_to_ceo", "hire_contractor"],
  engineer: ["write_doc", "submit_for_review", "hire_contractor"],
  marketing: ["draft_campaign", "report_to_ceo", "hire_contractor"],
  sales: ["add_prospect", "draft_outreach", "report_to_ceo"],
  contractor: ["write_doc", "report_to_ceo"],
  ceo: ["announce", "direct_message", "update_agent_config"],
};
```

- [ ] **Step 5: Type-check**

```bash
cd examples/agents/typescript/pi-company
npx tsc --noEmit 2>&1 | tail -15
```

Compiles clean. If pi-mono's tool spec format differs from `{name, description, parameters}`, adapt the `*Spec` exports to match.

- [ ] **Step 6: Commit**

```bash
git add examples/agents/typescript/pi-company/src/tools/
git commit -m "feat(pi-company): 9 non-fork tools (announce, doc, prospect, etc.)"
```

---

### Task 8: `hire_contractor` tool

The headline tool. Forks the parent agent's session and spawns a sub-agent on the child session.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/tools/hire_contractor.ts`
- Create: `examples/agents/typescript/pi-company/tests/tools/hire_contractor.test.ts`
- Modify: `examples/agents/typescript/pi-company/src/tools/index.ts` (add to registry)

- [ ] **Step 1: Write the failing test**

```ts
import { test } from "node:test";
import assert from "node:assert/strict";
import { hireContractor } from "../../src/tools/hire_contractor.ts";
import { Memory } from "../../src/memory.ts";
import { Channels } from "../../src/channels.ts";

test("hire_contractor forks the parent session and records a contractor_hired event", async () => {
  const forks: any[] = [];
  const appends: any[] = [];
  const fakeClient = {
    forkSession: async (a: any) => { forks.push(a); return { childSession: a.childSession }; },
    append: async (a: any) => { appends.push(a); return { events: [] }; },
    broadcast: async () => {},
    subscribeChannel: async function*() {},
  };

  const ctx = {
    memory: new Memory(fakeClient as any, { tenant: "pi-company", runId: "r-1" }, "engineer"),
    channels: new Channels(fakeClient as any, { tenant: "pi-company", runId: "r-1" }),
    client: fakeClient as any,
    run: { tenant: "pi-company", runId: "r-1" },
    role: "engineer" as const,
  };

  const out = await hireContractor({ task_brief: "explore approach B" }, ctx);

  assert.equal(forks.length, 1, "expected exactly one fork");
  const childSession = forks[0].childSession;
  assert.ok(childSession.startsWith("r-1:engineer:contractor-"), `unexpected child: ${childSession}`);
  // Parent recorded a contractor_hired event:
  const hiredEvents = appends.filter((a) => a.event?.kind === "contractor_hired");
  assert.equal(hiredEvents.length, 1);
  assert.equal(hiredEvents[0].event.task_brief, "explore approach B");
  // Tool returned a string summary:
  assert.equal(typeof out, "string");
});
```

- [ ] **Step 2: Run, expect FAIL**

```bash
npm test 2>&1 | tail -10
```

- [ ] **Step 3: Implement `hire_contractor`**

```ts
// src/tools/hire_contractor.ts
import type { ToolHandler } from "../agent.ts";
import { Memory } from "../memory.ts";
import { Channels } from "../channels.ts";

export const hireContractor: ToolHandler = async (args, ctx) => {
  const taskBrief = String(args.task_brief ?? "");
  const tag = `contractor-${Date.now()}`;
  const childSession = await ctx.memory.forkSession(tag);

  await ctx.memory.recordEvent({
    kind: "contractor_hired",
    tag,
    childSession,
    task_brief: taskBrief,
  });

  // Spawn a contractor agent loop. Imported lazily to avoid a circular
  // import between tools and agent.
  const { Agent } = await import("../agent.ts");
  const { CODE_DEFAULTS } = await import("../config.ts");
  const { ROLE_TOOLS, ALL_TOOL_HANDLERS } = await import("./index.ts");

  // Contractor's role-config is a special "contractor" role with the parent's
  // role embedded as task context.
  const contractorConfig = {
    ...CODE_DEFAULTS.contractor,
    systemPrompt: `${CODE_DEFAULTS.contractor.systemPrompt}\n\nYou were hired by ${ctx.role}. Your specific task: ${taskBrief}`,
  };

  // The contractor uses the SAME run + role as parent for session-naming purposes,
  // but with a `contractor` toolset. The session is the forked child.
  const contractorAgent = new Agent({
    client: ctx.client,
    run: ctx.run,
    role: "contractor",  // Note: this changes the session name. We override it below.
    channels: ctx.channels,
    subscriptions: [],   // Contractors don't subscribe; they pull task from session.
    toolRegistry: filterToolRegistry(ALL_TOOL_HANDLERS, ROLE_TOOLS.contractor),
    configOverride: contractorConfig,
    taskBrief,
  });

  // Critical: override the session name to point at the forked child.
  // The Memory in contractorAgent must use childSession, not the default
  // "{runId}:contractor". Hack: we'd extend Agent to accept an explicit
  // sessionName override. For v1 we re-construct.
  (contractorAgent as any).memory = new Memory(
    ctx.client,
    ctx.run,
    "contractor",
  );
  (contractorAgent as any).memory.sessionName = childSession;

  await contractorAgent.start();

  return `hired contractor ${tag} on session ${childSession} for task: ${taskBrief.slice(0, 60)}`;
};

function filterToolRegistry(
  registry: Record<string, any>,
  names: string[],
): Record<string, any> {
  const out: Record<string, any> = {};
  for (const n of names) {
    if (registry[n]) out[n] = registry[n];
  }
  return out;
}

export const hireContractorSpec = {
  name: "hire_contractor",
  description:
    "Rent a contractor agent on demand for a specific task. The contractor inherits your full context (forked session) and runs in parallel. Use this when you face a hard sub-task you'd rather delegate.",
  parameters: {
    type: "object",
    properties: {
      task_brief: {
        type: "string",
        description: "What you want the contractor to do. Be specific.",
      },
    },
    required: ["task_brief"],
  },
};
```

The session-name override is awkward. A cleaner refactor: extend `Memory` to accept an explicit `sessionName` override in the constructor. Track this for a follow-up cleanup. For v1 the hack works.

- [ ] **Step 4: Add to tool registry**

In `src/tools/index.ts`:

```ts
import { hireContractor, hireContractorSpec } from "./hire_contractor.ts";

export const ALL_TOOL_HANDLERS: Record<string, ToolHandler> = {
  // ... existing entries ...
  hire_contractor: hireContractor,
};

export const ALL_TOOL_SPECS = {
  // ... existing entries ...
  hire_contractor: hireContractorSpec,
};
```

- [ ] **Step 5: Run tests, expect PASS**

```bash
npm test 2>&1 | tail -10
```

- [ ] **Step 6: Commit**

```bash
git add examples/agents/typescript/pi-company/src/tools/hire_contractor.ts examples/agents/typescript/pi-company/src/tools/index.ts examples/agents/typescript/pi-company/tests/tools/hire_contractor.test.ts
git commit -m "feat(pi-company): hire_contractor tool — fork session, spawn sub-agent"
```

---

### Task 9: `update_agent_config` tool (CEO-only)

**Files:**
- Create: `examples/agents/typescript/pi-company/src/tools/update_agent_config.ts`
- Create: `examples/agents/typescript/pi-company/tests/tools/update_agent_config.test.ts`
- Modify: `examples/agents/typescript/pi-company/src/tools/index.ts`

- [ ] **Step 1: Write the failing test**

```ts
import { test } from "node:test";
import assert from "node:assert/strict";
import { updateAgentConfig } from "../../src/tools/update_agent_config.ts";

test("update_agent_config appends an agent_override event to _config:agents", async () => {
  const appends: any[] = [];
  const fakeClient = { append: async (a: any) => { appends.push(a); return { events: [] }; } };
  const ctx = {
    memory: null as any, // Not used by this tool.
    channels: null as any,
    client: fakeClient as any,
    run: { tenant: "pi-company", runId: "r-1" },
    role: "ceo" as const,
  };

  await updateAgentConfig(
    { role: "engineer", patch: { systemPromptSuffix: "be terse" } },
    ctx,
  );

  assert.equal(appends.length, 1);
  assert.equal(appends[0].session, "_config:agents");
  assert.equal(appends[0].event.kind, "agent_override");
  assert.equal(appends[0].event.role, "engineer");
  assert.deepEqual(appends[0].event.patch, { systemPromptSuffix: "be terse" });
});
```

- [ ] **Step 2: Run, expect FAIL**

```bash
npm test 2>&1 | tail -10
```

- [ ] **Step 3: Implement**

```ts
// src/tools/update_agent_config.ts
import type { ToolHandler } from "../agent.ts";
import { appendConfigOverride } from "../config.ts";

export const updateAgentConfig: ToolHandler = async (args, ctx) => {
  const role = String(args.role ?? "") as any;
  const patch = (args.patch ?? {}) as any;
  await appendConfigOverride(ctx.client, ctx.run.tenant, role, patch);
  return `updated ${role} config: ${JSON.stringify(patch).slice(0, 80)}`;
};

export const updateAgentConfigSpec = {
  name: "update_agent_config",
  description:
    "(CEO-only) Hot-tune any agent's configuration: system prompt, tools, model. The next agent loop iteration picks up the override.",
  parameters: {
    type: "object",
    properties: {
      role: { type: "string", enum: ["cto", "engineer", "marketing", "sales", "contractor"] },
      patch: {
        type: "object",
        description: "Partial AgentConfig — fields to override.",
        properties: {
          systemPrompt: { type: "string" },
          systemPromptSuffix: { type: "string" },
          tools: { type: "array", items: { type: "string" } },
          model: { type: "string" },
        },
      },
    },
    required: ["role", "patch"],
  },
};
```

- [ ] **Step 4: Add to registry, type-check, run tests**

```ts
// In src/tools/index.ts:
import { updateAgentConfig, updateAgentConfigSpec } from "./update_agent_config.ts";

export const ALL_TOOL_HANDLERS: Record<string, ToolHandler> = {
  // ...
  update_agent_config: updateAgentConfig,
};

export const ALL_TOOL_SPECS = {
  // ...
  update_agent_config: updateAgentConfigSpec,
};
```

```bash
cd examples/agents/typescript/pi-company
npm test 2>&1 | tail -10
```

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/src/tools/update_agent_config.ts examples/agents/typescript/pi-company/src/tools/index.ts examples/agents/typescript/pi-company/tests/tools/update_agent_config.test.ts
git commit -m "feat(pi-company): update_agent_config CEO tool for hot config tuning"
```

---

### Task 10: Orchestrator + run-meta lifecycle

Boots all 5 agents on their sessions, wires channel subscriptions, owns the run lifecycle, writes `_meta` events.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/orchestrator.ts`

- [ ] **Step 1: Implement orchestrator**

```ts
// src/orchestrator.ts
import type { Client } from "agentdb";
import type { RunIdentity, AgentRole } from "./types.ts";
import { Channels } from "./channels.ts";
import { Memory } from "./memory.ts";
import { Agent } from "./agent.ts";
import { ROLE_TOOLS, ALL_TOOL_HANDLERS } from "./tools/index.ts";

const META_SESSION = "_meta";

const ROLE_SUBSCRIPTIONS: Record<Exclude<AgentRole, "ceo" | "contractor">, string[]> = {
  cto: ["announcements", "engineering"],
  engineer: ["announcements", "engineering"],
  marketing: ["announcements", "gtm"],
  sales: ["announcements", "gtm"],
};

export class Orchestrator {
  private agents: Agent[] = [];

  constructor(
    private readonly client: Client,
    private readonly run: RunIdentity,
  ) {}

  async startCompany(): Promise<void> {
    const channels = new Channels(this.client, this.run);

    // Write run_started lifecycle event.
    await this.recordMeta({ kind: "run_started", started_at: Date.now(), agents: Object.keys(ROLE_SUBSCRIPTIONS) });

    for (const role of Object.keys(ROLE_SUBSCRIPTIONS) as Array<keyof typeof ROLE_SUBSCRIPTIONS>) {
      const tools = filterTools(ROLE_TOOLS[role]);
      const agent = new Agent({
        client: this.client,
        run: this.run,
        role,
        channels,
        subscriptions: ROLE_SUBSCRIPTIONS[role].map((c) => `${this.run.runId}:${c}`),
        toolRegistry: tools,
      });
      this.agents.push(agent);
      await agent.start();
      await this.recordMeta({ kind: "agent_joined", role });
    }
  }

  async stop(reason: string): Promise<void> {
    await this.recordMeta({ kind: "run_ended", ended_at: Date.now(), reason });
    // No graceful agent shutdown in v1 — process exit terminates the loops.
  }

  private async recordMeta(event: any): Promise<void> {
    await this.client.append({
      tenant: this.run.tenant,
      session: `${this.run.runId}:${META_SESSION}`,
      event,
      fetchLastN: 0,
    });
  }
}

function filterTools(names: string[]): Record<string, any> {
  const out: Record<string, any> = {};
  for (const n of names) {
    if (ALL_TOOL_HANDLERS[n]) out[n] = ALL_TOOL_HANDLERS[n];
  }
  return out;
}
```

- [ ] **Step 2: Type-check**

```bash
cd examples/agents/typescript/pi-company
npx tsc --noEmit 2>&1 | tail -10
```

- [ ] **Step 3: Commit**

```bash
git add examples/agents/typescript/pi-company/src/orchestrator.ts
git commit -m "feat(pi-company): Orchestrator + _meta lifecycle events"
```

---

### Task 11: TUI

A color-coded chronological log subscribing to all 3 channels, plus a CEO prompt at the bottom.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/tui.ts`

- [ ] **Step 1: Implement TUI (chalk + readline fallback)**

If `PI_MONO_NOTES.md` shows a TUI library that fits cleanly (differential rendering with a stream view + an input prompt), use it. Otherwise:

```ts
// src/tui.ts
import * as readline from "node:readline";
import chalk from "chalk";
import type { Channels } from "./channels.ts";

const ROLE_COLORS: Record<string, (s: string) => string> = {
  ceo: chalk.bold.cyan,
  cto: chalk.green,
  engineer: chalk.yellow,
  marketing: chalk.magenta,
  sales: chalk.blue,
  contractor: chalk.gray,
};

export class TUI {
  private rl?: readline.Interface;

  constructor(private channels: Channels) {}

  async start(onCeoInput: (text: string) => Promise<void>): Promise<void> {
    // Spawn one async loop per channel.
    for (const ch of ["announcements", "engineering", "gtm"]) {
      this.subscribeAndRender(ch);
    }

    this.rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    this.rl.setPrompt(chalk.bold.cyan("> CEO: "));
    this.rl.prompt();

    this.rl.on("line", async (line) => {
      const text = line.trim();
      if (!text) { this.rl!.prompt(); return; }
      if (text === "/quit") { process.exit(0); }
      await onCeoInput(text);
      this.rl!.prompt();
    });
  }

  private async subscribeAndRender(channel: string): Promise<void> {
    for await (const msg of this.channels.subscribe(channel)) {
      const p = msg.payload as any;
      const role = p?.from ?? "unknown";
      const color = ROLE_COLORS[role] ?? chalk.white;
      const text = p?.text ?? p?.kind ?? JSON.stringify(p).slice(0, 100);
      // Clear current line, print event, restore prompt.
      process.stdout.write("\r\x1b[K");
      console.log(`${color(`[${role.toUpperCase()}]`)} ${chalk.dim(`(${channel})`)} ${text}`);
      this.rl?.prompt(true);
    }
  }
}
```

- [ ] **Step 2: Type-check**

```bash
cd examples/agents/typescript/pi-company
npx tsc --noEmit 2>&1 | tail -10
```

- [ ] **Step 3: Commit**

```bash
git add examples/agents/typescript/pi-company/src/tui.ts
git commit -m "feat(pi-company): chalk+readline TUI with color-coded log + CEO prompt"
```

---

### Task 12: Server child-process management

Auto-start `agentdb serve --dev` as a child process; terminate on exit.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/server.ts`

- [ ] **Step 1: Implement server lifecycle**

```ts
// src/server.ts
import { spawn, type ChildProcess } from "node:child_process";
import * as net from "node:net";

async function freePort(): Promise<number> {
  return new Promise((resolve) => {
    const s = net.createServer();
    s.listen(0, () => {
      const p = (s.address() as net.AddressInfo).port;
      s.close(() => resolve(p));
    });
  });
}

export interface ServerHandle {
  baseUrl: string;
  proc: ChildProcess;
  shutdown: () => void;
}

export async function startAgentDBServer(): Promise<ServerHandle> {
  const port = await freePort();
  const mongoPort = await freePort();
  const baseUrl = `http://127.0.0.1:${port}`;

  // Find the agentdb workspace root by walking up from cwd.
  // Sample lives at examples/agents/typescript/pi-company; root is 4 levels up.
  // const repoRoot = path.resolve(import.meta.dirname, "..", "..", "..", "..", "..");

  const proc = spawn(
    "cargo",
    ["run", "--quiet", "-p", "agentdb-server", "--", "serve", "--dev"],
    {
      env: {
        ...process.env,
        AGENTDB_ADDR: `127.0.0.1:${port}`,
        AGENTDB_MONGO_ADDR: `127.0.0.1:${mongoPort}`,
      },
      stdio: "ignore",
    },
  );

  // Wait for /health/ready
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${baseUrl}/health/ready`);
      if (r.ok) break;
    } catch { /* ignore */ }
    await new Promise((r) => setTimeout(r, 500));
  }

  const shutdown = () => {
    if (!proc.killed) proc.kill("SIGTERM");
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
  process.on("exit", shutdown);

  return { baseUrl, proc, shutdown };
}
```

If `cargo run` is too slow for first-startup (cold compile), the README must mention `cargo build -p agentdb-server` as a prereq for fast first-run.

- [ ] **Step 2: Type-check**

```bash
cd examples/agents/typescript/pi-company
npx tsc --noEmit 2>&1 | tail -5
```

- [ ] **Step 3: Commit**

```bash
git add examples/agents/typescript/pi-company/src/server.ts
git commit -m "feat(pi-company): auto-start agentdb serve --dev child process"
```

---

### Task 13: Scripted scenario + entry wiring

Wire the orchestrator + TUI + server in `index.ts`, support `--scenario launch` and `--resume <run-id>` flags.

**Files:**
- Create: `examples/agents/typescript/pi-company/src/scenarios/launch.ts`
- Modify: `examples/agents/typescript/pi-company/src/index.ts`

- [ ] **Step 1: Implement the launch scenario**

```ts
// src/scenarios/launch.ts
import type { Channels } from "../channels.ts";
import type { Client } from "agentdb";
import { appendConfigOverride } from "../config.ts";

export interface ScenarioStep {
  delayMs: number;
  action: { type: "ceo_message"; text: string }
        | { type: "config_update"; role: "engineer" | "marketing" | "sales" | "cto"; patch: any };
}

export const launchScenario: ScenarioStep[] = [
  { delayMs: 0, action: { type: "ceo_message", text: "Launch our new vector search service this quarter. Engineering and Marketing, propose plans." } },
  { delayMs: 60_000, action: { type: "ceo_message", text: "Engineer, also explore approach B in parallel." } },
  { delayMs: 45_000, action: { type: "config_update", role: "marketing", patch: { systemPromptSuffix: "Be terse and punchy. Aim for under 80 chars per line." } } },
  { delayMs: 15_000, action: { type: "ceo_message", text: "Marketing, redo the campaign with the new style." } },
  { delayMs: 30_000, action: { type: "ceo_message", text: "Sales, give me the prospect status." } },
  { delayMs: 30_000, action: { type: "ceo_message", text: "Wrap it up. CTO, summary please." } },
  { delayMs: 20_000, action: { type: "ceo_message", text: "/quit" } },
];

export async function runScenario(
  steps: ScenarioStep[],
  channels: Channels,
  client: Client,
  tenant: string,
): Promise<void> {
  for (const step of steps) {
    await new Promise((r) => setTimeout(r, step.delayMs));
    if (step.action.type === "ceo_message") {
      await channels.publish("announcements", { kind: "directive", from: "ceo", text: step.action.text });
    } else if (step.action.type === "config_update") {
      await appendConfigOverride(client, tenant, step.action.role, step.action.patch);
    }
  }
}
```

- [ ] **Step 2: Wire entry point in `src/index.ts`**

```ts
// src/index.ts
import { Client } from "agentdb";
import { Orchestrator } from "./orchestrator.ts";
import { Channels } from "./channels.ts";
import { TUI } from "./tui.ts";
import { startAgentDBServer } from "./server.ts";
import { launchScenario, runScenario } from "./scenarios/launch.ts";

const TENANT = "pi-company";

async function main() {
  const args = process.argv.slice(2);
  const scenarioFlag = args.find((a) => a.startsWith("--scenario="));
  const resumeFlag = args.find((a) => a.startsWith("--resume="));
  const scenario = scenarioFlag?.split("=")[1];
  const resumeRunId = resumeFlag?.split("=")[1];

  const runId = resumeRunId ?? `r-${Date.now()}`;
  const run = { tenant: TENANT, runId };

  const server = await startAgentDBServer();
  console.log(`AgentDB ready at ${server.baseUrl}`);

  const client = new Client({ baseUrl: server.baseUrl, tenant: TENANT });
  const orch = new Orchestrator(client, run);
  await orch.startCompany();
  const channels = new Channels(client, run);

  if (scenario === "launch") {
    console.log(`Running scenario 'launch' (run-id: ${runId})...`);
    await runScenario(launchScenario, channels, client, TENANT);
    await orch.stop("scenario_complete");
    process.exit(0);
  } else {
    const tui = new TUI(channels);
    await tui.start(async (text: string) => {
      await channels.publish("announcements", { kind: "directive", from: "ceo", text });
    });
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
```

- [ ] **Step 3: Smoke check**

```bash
cd examples/agents/typescript/pi-company
npm run build 2>&1 | tail -5
```

(Don't actually `npm start` — that would invoke the LLM with no API key configured.)

- [ ] **Step 4: Commit**

```bash
git add examples/agents/typescript/pi-company/src/scenarios/launch.ts examples/agents/typescript/pi-company/src/index.ts
git commit -m "feat(pi-company): launch scenario + entry wiring"
```

---

### Task 14: Smoke tests + shell smoke

**Files:**
- Create: `examples/agents/typescript/pi-company/tests/smoke/launch.test.ts`
- Create: `scripts/smoke-pi-company.sh`

- [ ] **Step 1: Implement gated smoke test**

```ts
// tests/smoke/launch.test.ts
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";

const SMOKE = process.env.AGENTDB_PI_COMPANY_SMOKE === "1";
const HAS_KEY = Boolean(process.env.AGENTDB_DEMO_API_KEY);

test("launch scenario runs end-to-end", { skip: !SMOKE || !HAS_KEY ? "set AGENTDB_PI_COMPANY_SMOKE=1 + AGENTDB_DEMO_API_KEY" : undefined }, async () => {
  // Run the demo's --scenario launch flag end-to-end.
  // Capture stdout, assert key markers appear.
  const proc = spawn("node", ["--experimental-strip-types", "src/index.ts", "--scenario=launch"], {
    cwd: process.cwd(),
    env: process.env,
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stdout = "";
  proc.stdout.on("data", (b) => { stdout += b.toString(); });
  proc.stderr.on("data", (b) => { stdout += b.toString(); });

  const exit = await new Promise<number>((resolve) => proc.on("exit", (c) => resolve(c ?? 1)));
  assert.equal(exit, 0, `scenario exited non-zero. Output:\n${stdout.slice(-2000)}`);

  // Sanity: each role broadcast at least once.
  assert.ok(/\[CTO\]/.test(stdout) || /cto/.test(stdout), "expected CTO activity");
  assert.ok(/\[ENGINEER\]/.test(stdout) || /engineer/.test(stdout), "expected Engineer activity");
  assert.ok(/\[MARKETING\]/.test(stdout) || /marketing/.test(stdout), "expected Marketing activity");
  assert.ok(/\[SALES\]/.test(stdout) || /sales/.test(stdout), "expected Sales activity");
  // Sanity: at least one fork.
  assert.ok(/contractor/.test(stdout), "expected contractor activity (hire_contractor fork)");
}, { timeout: 600_000 }); // 10 min cap
```

- [ ] **Step 2: Implement shell smoke**

```bash
#!/usr/bin/env bash
# End-to-end smoke for pi-company sample app.
# Requires AGENTDB_DEMO_API_KEY in env.

set -euo pipefail

if [ -z "${AGENTDB_DEMO_API_KEY:-}" ]; then
    echo "SKIP: AGENTDB_DEMO_API_KEY not set"
    exit 0
fi

cd "$(dirname "$0")/../examples/agents/typescript/pi-company"
npm install --silent 2>&1 | tail -3
npm run build --silent 2>&1 | tail -3

# Run the launch scenario, capture output.
TMP=$(mktemp)
trap "rm -f $TMP" EXIT

node --experimental-strip-types src/index.ts --scenario=launch > "$TMP" 2>&1

# Assertions.
grep -q "AgentDB ready" "$TMP" || { echo "FAIL: server didn't start"; cat "$TMP"; exit 1; }
grep -q "scenario 'launch'" "$TMP" || { echo "FAIL: scenario didn't start"; cat "$TMP"; exit 1; }

echo "==> smoke-pi-company OK"
```

- [ ] **Step 3: Make executable**

```bash
chmod +x scripts/smoke-pi-company.sh
```

- [ ] **Step 4: Run unit tests**

```bash
cd examples/agents/typescript/pi-company
npm test 2>&1 | tail -10
```

All non-smoke tests must pass.

- [ ] **Step 5: Commit**

```bash
git add examples/agents/typescript/pi-company/tests/smoke/launch.test.ts scripts/smoke-pi-company.sh
git commit -m "test(pi-company): gated end-to-end smoke + shell wrapper"
```

---

### Task 15: README + agent-db.md addendum

**Files:**
- Modify: `examples/agents/typescript/pi-company/README.md` (replace placeholder)
- Modify: `docs/agent-db.md` (append config-as-events convention section)

- [ ] **Step 1: Write the sample's README**

```markdown
# Pi Company — AgentDB Sample App

A 5-agent AI company on AgentDB + pi-mono. Demonstrates AgentDB's signature
primitives — sessions-as-memory, fork-as-on-demand-contractor,
broadcast-as-team-coordination, config-as-events — in realistic combination.

You play CEO. Four employee agents (CTO, Engineer, Head of Marketing, Head
of Sales) run autonomously. Each agent has its own AgentDB session for
memory. Agents coordinate via broadcast channels. Each agent can hire a
contractor on demand — a sub-agent forked from the parent's session,
inheriting full context for free.

## Setup

Prereqs: Node ≥18, Rust toolchain (for `cargo run -p agentdb-server`),
an LLM API key (OpenAI, Anthropic, or MiniMax).

```bash
# Pre-build the AgentDB server for fast first-run:
cargo build -p agentdb-server

# Install + build the sample:
cd examples/agents/typescript/pi-company
npm install
npm run build
```

## Running

Set your API key:

```bash
export AGENTDB_DEMO_PROVIDER=anthropic    # or openai or minimax
export AGENTDB_DEMO_API_KEY=sk-...
export AGENTDB_DEMO_MODEL=claude-haiku-4-5    # optional override
```

Interactive mode:

```bash
npm start
# Type directives at the > CEO: prompt.
# Type /quit to exit.
```

Scripted demo (~3 min, hands-off):

```bash
node --experimental-strip-types src/index.ts --scenario=launch
```

Resume an earlier run:

```bash
node --experimental-strip-types src/index.ts --resume=r-1714063200
```

## Walkthrough — what the demo proves

| AgentDB feature | How the demo uses it |
|---|---|
| Sessions | One per agent; persistent across runs (`--resume`). |
| Fork (the headline) | Each agent's `hire_contractor` tool forks its session and spawns a sub-agent inheriting full context. Three narratives: hard-task handoff, speculative parallel exploration, specialist delegation — same primitive. |
| Phase 1 subscribe | The CEO TUI subscribes to all 3 channels with `from_seq` resume. |
| Phase 2 broadcast | Agents coordinate via 3 channels: `announcements`, `engineering`, `gtm`. |
| Config-as-events | Agent definitions live in code; runtime overrides go in a `_config:agents` session. The CEO's `update_agent_config` tool appends overrides; agents reload config every loop iteration. Hot-tunable, event-sourced, free history. |

## What this would take without AgentDB

Postgres for the conversation history (one table per agent, custom MVCC for fork), Redis for broadcast coordination, pgvector if you wanted hybrid retrieval (we don't yet, in v1), plus a state-management framework to glue it together. AgentDB ships all of it as one mental model.

## License

Same as the parent agentdb project.
```

- [ ] **Step 2: Append to `docs/agent-db.md`**

Find a sensible place (near the end, in a "Recommended conventions" section if one exists; otherwise add a new section). Append:

```markdown
## Recommended pattern: config-as-events

AgentDB doesn't ship a configuration store. Instead, it provides the
primitives to put configuration *in AgentDB sessions* and reduce-over-events
at read time. This pattern works well for agent platforms, multi-tenant
SaaS overrides, and any case where "configuration changes are themselves
historical data worth keeping."

### Convention

- Each tenant has a session named `_config:<scope>` (e.g., `_config:agents`).
- Each event in the session has `kind: "<scope>_override"` plus the
  override target and a patch.
- Code holds the *defaults*. The session events hold the *overrides*.
- At read time, fold the events in seq order onto the code defaults.

### Example

```ts
async function loadAgentConfig(client, tenant, role) {
  const defaults = CODE_DEFAULTS[role];
  const events = (await client.append({
    tenant, session: "_config:agents",
    event: { kind: "_noop" }, fetchLastN: 1024,
  })).events.filter(e => e.payload?.kind !== "_noop");
  const patch = events
    .filter(e => e.payload?.role === role)
    .reduce((acc, ev) => ({ ...acc, ...ev.payload.patch }), {});
  return { ...defaults, ...patch };
}

// Append an override:
await client.append({
  tenant, session: "_config:agents",
  event: { kind: "agent_override", role: "engineer", patch: { systemPromptSuffix: "be terse" } },
  fetchLastN: 0,
});
```

### Why this is good

- **Event-sourced.** Free history of every config change. Roll back by reading older events.
- **No new database.** Same primitives as everything else.
- **Multi-tenant clean.** Each tenant's config lives inside that tenant — no cross-tenant access.
- **Hot-reloadable.** Reload config at the start of each agent iteration; new overrides take effect immediately.
- **Inspectable.** Just events; can be queried, replayed, exported.

### Trade-offs

- "Current value" is reduce-over-events, slower than a key-value `GET`.
  At AgentDB-typical config session sizes (tens to low-hundreds of events),
  this is negligible. For huge config sessions, periodic snapshotting can
  be added.
- No schema enforcement. Up to the application to validate patches.
- No optimistic concurrency on writes. The last `append` wins, by seq.

### Reference

- Sample app using this pattern:
  [`examples/agents/typescript/pi-company/`](../examples/agents/typescript/pi-company/) —
  `src/config.ts`, `src/tools/update_agent_config.ts`.
```

- [ ] **Step 3: Verify and commit**

Verify markdown renders cleanly:

```bash
grep -c '^```' examples/agents/typescript/pi-company/README.md docs/agent-db.md
# Each should be even.
```

```bash
git add examples/agents/typescript/pi-company/README.md docs/agent-db.md
git commit -m "docs(pi-company): README + agent-db.md config-as-events convention"
```

---

## Self-Review

**Spec coverage check** (against `docs/superpowers/specs/2026-04-27-pi-company-sample-design.md`):

| Spec section | Covered by |
|---|---|
| § 5.1 Project layout | Tasks 1–13 (each file mentioned has a creating task) |
| § 5.2 The 5 agents | Task 7 (tool registry) + Task 10 (orchestrator with role subscriptions) |
| § 5.3 The 3 broadcast channels | Task 3 (channels wrapper) + Task 10 (subscription wiring) |
| § 5.4 LLM provider router | Task 5 |
| § 5.5 Layered state model | Task 4 (config loader) + Task 10 (`_meta` session) + Task 2 (per-agent session) |
| § 5.6 Per-agent working memory | Task 2 |
| § 5.7 Fork patterns surfaced | Task 8 (`hire_contractor`); the three narratives are demonstrated by the launch scenario in Task 13 |
| § 5.8 TUI | Task 11 |
| § 5.9 Persistence | Task 12 (server child-process) + Task 13 (`--resume` flag) |
| § 5.10 Scenario script | Task 13 |
| § 5.11 Pi-mono integration | Task 0 (discovery) → Tasks 5, 6 (consumption) |
| § 6 Tests | Tasks 2, 3, 4, 8, 9 (unit) + Task 14 (smoke) |
| § 7 Milestones | Mapped onto Tasks 0–15 |

All spec sections have a task. The `loadAgentConfig` reference in § 5.5 is implemented in Task 4. The `update_agent_config` tool is Task 9. The fork-as-contractor headline is Task 8.

**Type consistency check:**

- `AgentRole` (Task 2) — used in Tasks 4, 7, 8, 9, 10. Consistent.
- `RunIdentity { tenant, runId }` (Task 2) — used in Tasks 3, 4, 8, 10, 13. Consistent.
- `AgentConfig { role, systemPrompt, tools, model, systemPromptSuffix? }` (Task 2/4) — used in Tasks 4, 6. Consistent.
- `ConfigPatch` (Task 2) — used in Tasks 4, 9. Consistent.
- `Memory.sessionName` getter (Task 2) — referenced in Task 8 (override hack). Consistent.
- `Channels.publish(name, payload)` (Task 3) — used in Tasks 7 tools, 11 TUI, 13 scenario. Consistent.
- `Channels.subscribe(name)` (Task 3) — used in Tasks 6 agent loop, 11 TUI. Consistent.
- `loadAgentConfig(client, tenant, role)` (Task 4) — used in Task 6 agent. Consistent.
- `appendConfigOverride(client, tenant, role, patch)` (Task 4) — used in Tasks 9 tool, 13 scenario. Consistent.
- `ToolHandler` and `ToolContext` (Task 6) — used in Tasks 7, 8, 9. Consistent.
- `ALL_TOOL_HANDLERS`, `ROLE_TOOLS` (Task 7) — used in Tasks 8 (filter for contractor), 10 (filter per role). Consistent.

**Placeholder scan:**

- Task 0 captures pi-mono package names + APIs into `PI_MONO_NOTES.md`. Subsequent tasks (1, 5, 6) reference those names with explicit "from `PI_MONO_NOTES.md`" instructions — this is a documented pointer, not a TBD.
- Task 5's `getLLM` body has `throw new Error("TODO: wire up ...")` placeholders. **This is acceptable** because Task 5's commit captures the SHAPE; the implementer fills the body during integration in Task 6's first run-through. If the spec compliance reviewer flags this, the implementer can move the wire-up into Task 5's body and commit then.
- Task 8's session-name override is documented as a v1 hack with a follow-up comment. Acceptable.
- No "TBD", "implement later", or "fill in details" language outside the explicit `PI_MONO_NOTES.md` pointer pattern.

**Scope check:**

15 tasks, ~1700–2200 LoC TypeScript across all of them. Single PR scope. Each task ends with a working commit. The `PI_MONO_NOTES.md` discovery in Task 0 is the only task without paste-ready code, by necessity — pi-mono's API isn't knowable a priori from this plan.
