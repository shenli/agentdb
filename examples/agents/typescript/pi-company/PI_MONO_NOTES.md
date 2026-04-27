# pi-mono integration notes

Discovery findings for Task 0 of the pi-company sample app.
Subsequent tasks (1, 5, 6, 11) reference this file for actual package
names + API shapes.

Source of truth:
- Repo: `https://github.com/badlogic/pi-mono` (default branch `main`).
- All packages are at the same version (monorepo): **`0.70.2`**.
- Each package is published to npm under the `@mariozechner` scope.

## Packages we'll use

| Package | Version | Purpose |
|---|---|---|
| `@mariozechner/pi-agent-core` | `^0.70.2` | Stateful `Agent` runtime: tool execution, event streaming, steering/follow-up queues. Built on `pi-ai`. |
| `@mariozechner/pi-ai` | `^0.70.2` | Unified LLM API across providers + global model registry. Re-exports `typebox` (`Type`, `Static`) for tool schemas. |
| `@mariozechner/pi-tui` | `^0.70.2` | Terminal UI library with differential rendering: `TUI`, `Container`, `Editor`, `Markdown`, `SelectList`, `Loader`, etc. |

Packages from the monorepo we are skipping (not needed for this sample):
`@mariozechner/pi-coding-agent` (the `pi` CLI), `@mariozechner/pi-mom` (Slack
bot), `@mariozechner/pi` (vLLM pod manager), `@mariozechner/pi-web-ui` (Lit-based
chat components).

## Agent runtime API

Package: `@mariozechner/pi-agent-core`. Top-level export: **`Agent`** (class).

Construction is via an **options bag** (no positional args). The agent owns its
own transcript and emits events; you don't call a "run one turn" method —
instead you call `prompt()` / `continue()` and subscribe to events.

```ts
import { Agent, type AgentTool } from "@mariozechner/pi-agent-core";
import { getModel } from "@mariozechner/pi-ai";

const agent = new Agent({
  initialState: {
    systemPrompt: "You are the engineer ...",
    model: getModel("anthropic", "claude-sonnet-4-20250514"),
    tools: [/* AgentTool<...>[] */],
    // optional:
    // thinkingLevel: "off" | "minimal" | "low" | "medium" | "high" | "xhigh",
    // messages: AgentMessage[],
  },
  // optional hooks (all may be omitted):
  // convertToLlm: (msgs) => msgs.filter(...),
  // transformContext: async (msgs, signal) => msgs,
  // beforeToolCall: async ({ toolCall, args, context }, signal) => ({ block: false }),
  // afterToolCall:  async ({ toolCall, result, isError, context }, signal) => undefined,
  // getApiKey: async (provider) => "...",
  // toolExecution: "parallel" | "sequential",   // default "parallel"
  // sessionId: "...",
  // thinkingBudgets: { minimal: 128, low: 512, medium: 1024, high: 2048 },
  // streamFn: streamSimple,                     // default: streamSimple from pi-ai
});

// Subscribe to lifecycle events. Listeners are awaited in subscription order.
const unsub = agent.subscribe(async (event, signal) => {
  if (event.type === "message_update" &&
      event.assistantMessageEvent.type === "text_delta") {
    process.stdout.write(event.assistantMessageEvent.delta);
  }
  if (event.type === "agent_end") { /* run is finishing */ }
});

// Drive a turn (this is what we use as "run one turn"):
await agent.prompt("Some directive");           // text
// or
await agent.prompt({ role: "user", content: [{ type: "text", text: "..." }], timestamp: Date.now() });
// or, after error, retry from current transcript (last msg must be user/toolResult):
await agent.continue();

// Steering and follow-ups (queued, drained by the loop):
agent.steer({ role: "user", content: [...], timestamp: Date.now() });
agent.followUp({ role: "user", content: [...], timestamp: Date.now() });

// Inspect/await:
agent.state.messages;          // AgentMessage[]
agent.state.isStreaming;       // boolean
await agent.waitForIdle();     // resolves after agent_end listeners settle
agent.abort();                 // abort current run
```

Key event types (`AgentEvent`):
`agent_start`, `agent_end`, `turn_start`, `turn_end`, `message_start`,
`message_update` (assistant streaming only; carries `assistantMessageEvent`
with `text_delta` / `thinking_delta` / `toolcall_delta`), `message_end`,
`tool_execution_start`, `tool_execution_update`, `tool_execution_end`.

`agent.prompt(...)` resolves only after the run has fully settled (including
awaited `agent_end` subscribers). That makes it a natural "run one turn (and
any tool-call follow-ups it triggers)" primitive for our orchestrator.

### Tool-result terminate hint

A tool can return `{ content, details, terminate: true }` to hint the loop to
stop after the current batch. The loop only stops early when **every** tool
result in the batch sets `terminate: true`. This is exactly what we need for
the `notify_done` tool in our spec.

## Tool spec format

Tools are `AgentTool<TParameters extends TSchema>` from `pi-agent-core`,
which extends `Tool<TParameters>` from `pi-ai`. Schemas are **typebox**
(`@sinclair/typebox`-compatible; pi-ai re-exports `Type` and `Static`).

```ts
import type { AgentTool, AgentToolResult } from "@mariozechner/pi-agent-core";
import { type Static, Type } from "@mariozechner/pi-ai"; // or "typebox"

const writeDocSchema = Type.Object({
  path: Type.String({ description: "Where to write" }),
  body: Type.String({ description: "File contents" }),
});

const writeDoc: AgentTool<typeof writeDocSchema, { bytes: number }> = {
  name: "write_doc",
  description: "Write a markdown document.",
  label: "write_doc",                       // Human-readable label for UI
  parameters: writeDocSchema,
  // optional: prepareArguments?: (args: unknown) => Static<typeof writeDocSchema>;
  // optional: executionMode?: "sequential" | "parallel";
  async execute(toolCallId, params, signal, onUpdate): Promise<AgentToolResult<{ bytes: number }>> {
    // Throw on failure — don't encode errors in `content`.
    const bytes = Buffer.byteLength(params.body, "utf8");
    return {
      content: [{ type: "text", text: `wrote ${bytes} bytes to ${params.path}` }],
      details: { bytes },
      // terminate?: boolean,
    };
  },
};
```

Important:
- The runtime validates `toolCall.arguments` against `parameters` (typebox)
  before calling `execute`. Validation errors become an error `toolResult`
  message automatically.
- `prepareArguments` is an optional shim if the LLM emits arguments that need
  light cleanup before validation.
- `execute` MUST return `{ content, details, terminate? }`. `content` is the
  LLM-visible payload (text/image blocks). `details` is opaque structured data
  for logs/UI.

## LLM clients

There is no per-provider client class. `pi-ai` uses a **registered API
provider** indirection: each model carries an `api` field, and the runtime
looks up the provider that handles that API.

```ts
import {
  getModel,         // (provider, modelId) -> Model<Api>
  streamSimple,     // (model, context, options) -> AssistantMessageEventStream
  completeSimple,   // (model, context, options) -> Promise<AssistantMessage>
  getEnvApiKey,     // (provider) -> string | undefined
  Type, Static,     // typebox re-exports
} from "@mariozechner/pi-ai";
```

`Model<TApi>` shape (see `packages/ai/src/types.ts`):

```ts
interface Model<TApi extends Api> {
  id: string;            // e.g. "claude-sonnet-4-20250514", "MiniMax-M2.7"
  name: string;
  api: TApi;             // "anthropic-messages" | "openai-completions" | "openai-responses" | ...
  provider: Provider;    // "anthropic" | "openai" | "minimax" | "minimax-cn" | ...
  baseUrl: string;       // burned into the model — providers honor this
  reasoning: boolean;
  input: ("text" | "image")[];
  cost: { input; output; cacheRead; cacheWrite };
  contextWindow: number;
  maxTokens: number;
  headers?: Record<string, string>;
  compat?: ...;          // OpenAI-completions / responses / Anthropic compat overrides
}
```

Built-in API providers (registered eagerly at module load — see
`providers/register-builtins.ts`): `anthropic-messages`, `openai-completions`,
`openai-responses`, `azure-openai-responses`, `openai-codex-responses`,
`mistral-conversations`, `google-generative-ai`, `google-gemini-cli`,
`google-vertex`, `bedrock-converse-stream`.

Built-in `KnownProvider`s (subset): `anthropic`, `openai`, `google`,
`google-vertex`, `xai`, `groq`, `cerebras`, `openrouter`, `vercel-ai-gateway`,
`deepseek`, `zai`, `mistral`, **`minimax`**, **`minimax-cn`**, `huggingface`,
`fireworks`, `github-copilot`, `amazon-bedrock`, `kimi-coding`, `opencode*`,
`opencode-go`.

Env-var defaults (`packages/ai/src/env-api-keys.ts`):
- `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` (or `ANTHROPIC_OAUTH_TOKEN`),
  `GEMINI_API_KEY`, `GROQ_API_KEY`, `CEREBRAS_API_KEY`, `XAI_API_KEY`,
  `OPENROUTER_API_KEY`, `MISTRAL_API_KEY`, `DEEPSEEK_API_KEY`,
  **`MINIMAX_API_KEY`**, **`MINIMAX_CN_API_KEY`**, `HF_TOKEN`,
  `FIREWORKS_API_KEY`, etc.

`baseUrl` override: **per-model, not per-client.** The OpenAI-completions
provider hard-passes `baseURL: model.baseUrl` to the OpenAI SDK client (see
`providers/openai-completions.ts:459`). To target a non-standard endpoint you
register a custom `Model<"openai-completions">` (or `"anthropic-messages"`)
with the desired `baseUrl`. There's no `new OpenAIClient({ baseUrl })` knob to
turn — the model literal IS the configuration.

## MiniMax integration

**Decision: Path A — built-in.** No adapter needed.

`pi-ai` ships first-class MiniMax support out of the box:
- Provider id `minimax` with `baseUrl: "https://api.minimax.io/anthropic"`,
  API `anthropic-messages` (uses MiniMax's Anthropic-compatible endpoint).
  Built-in models: `MiniMax-M2.7`, `MiniMax-M2.7-highspeed`.
- Provider id `minimax-cn` with `baseUrl: "https://api.minimaxi.com/anthropic"`
  for the China endpoint (same models).
- Env vars: `MINIMAX_API_KEY` and `MINIMAX_CN_API_KEY` resolved automatically
  by `getEnvApiKey("minimax" | "minimax-cn")`.

Usage from our LLM router (Task 5):

```ts
import { getModel } from "@mariozechner/pi-ai";
const minimaxModel = getModel("minimax", "MiniMax-M2.7"); // Model<"anthropic-messages">
// pass into Agent's initialState.model — done.
```

If we later need a model id that isn't pre-registered, we register a literal
`Model<"anthropic-messages">` with the MiniMax `baseUrl` and use it directly
(no adapter, no client wrapper). The OpenAI baseUrl-override path the spec
worried about is **not on the critical path** because MiniMax is reached via
the Anthropic-compatible API in pi-mono, not OpenAI's.

## TUI library

Package: `@mariozechner/pi-tui`. **Verdict: usable**, no fallback needed.

The TUI exposes a layout system (`TUI`, `Container`, `Component`, `Focusable`,
overlays) plus ready-made components: `Editor` (full multi-line editor with
keybindings, kill-ring, undo), `Input`, `Markdown` (with theme), `Loader`,
`CancellableLoader`, `SelectList`, `SettingsList`, `Box`, `Text`,
`TruncatedText`, `Image` (Kitty/iTerm2 protocols + fallback). Differential
rendering is built in via `ProcessTerminal`.

For our "live log + input prompt" requirement (Task 11): build a `Container`
that stacks a scrolling log region (custom component or `Markdown` blocks
appended) above an `Editor` for input. The `keybindings` module gives us
default bindings. There's no single "REPL" widget but the primitives compose
straightforwardly. We do **not** need to fall back to `chalk + readline`.

If we want to keep Task 11 small, the minimum kit is:
`TUI`, `Container`, `Editor`, `Markdown`, `Text`, `Loader`.

## Sample integration sketch

For Task 6 (base agent loop) — subscribe-driven, one prompt per orchestrator
directive. Tool-call follow-ups happen inside the same `prompt()` invocation
(the agent loop already handles them).

```ts
import { Agent, type AgentTool, type AgentToolResult } from "@mariozechner/pi-agent-core";
import { getModel, type Static, Type } from "@mariozechner/pi-ai";

// 1. Build a tool registry for this role.
const writeDocSchema = Type.Object({
  path: Type.String(),
  body: Type.String(),
});
const writeDocTool: AgentTool<typeof writeDocSchema> = {
  name: "write_doc",
  description: "Write a markdown document to the workspace.",
  label: "write_doc",
  parameters: writeDocSchema,
  async execute(_id, params): Promise<AgentToolResult<unknown>> {
    // ... write to disk, persist via AgentDB memory wrapper, etc ...
    return { content: [{ type: "text", text: `OK: ${params.path}` }], details: {} };
  },
};

// 2. Resolve a model from our LLM router (Task 5).
//    Pre-registered MiniMax models work via `getModel("minimax", "MiniMax-M2.7")`.
const model = getModel("anthropic", "claude-sonnet-4-20250514");

// 3. Construct the agent.
const piAgent = new Agent({
  initialState: {
    systemPrompt: "You are the engineer.",
    model,
    tools: [writeDocTool /* ...registry... */],
  },
});

// 4. Hook events into our channels/log.
piAgent.subscribe(async (event) => {
  if (event.type === "tool_execution_end") {
    // emit to AgentDB pub/sub channel
  }
  if (event.type === "message_end" && event.message.role === "assistant") {
    // persist assistant transcript chunk to AgentDB memory
  }
});

// 5. Drive one turn (with internal tool-call follow-ups).
await piAgent.prompt("Draft the design doc for the search feature.");
// piAgent.state.messages now contains user + assistant + toolResult records.
```

For "stop after this batch" semantics (`notify_done` tool in our spec): set
`terminate: true` in the tool's `AgentToolResult`. The agent loop will skip
the auto-follow-up LLM call once every result in the batch agrees.

## Concerns / deltas from the spec

- The spec hypothesized "OpenAI client with `baseUrl` override" for MiniMax.
  Reality: `pi-ai` has no per-instance LLM client classes — providers are
  registered against the API name and `baseUrl` lives on the **model literal**.
  MiniMax is built-in via the Anthropic-messages API, so we don't touch
  `baseUrl` at all. Task 5's "LLM provider router" can be a thin wrapper over
  `getModel(provider, id)` plus our own per-role config.
- The agent runtime is event-stream-driven, not method-call-driven. The
  natural unit of work is `agent.prompt(...)` (which runs through any tool
  follow-ups) rather than a one-shot "run a turn" method. Task 6's loop
  shape should subscribe to `agent_end` (or `await prompt()`) for completion.
- Tool schemas use **typebox**, not raw JSONSchema. Re-exported as `Type` and
  `Static` from `@mariozechner/pi-ai`. Our existing assumption ("parameters:
  JSONSchema") needs to be "parameters: TSchema (typebox)".
- The TUI is a layout-component framework, not a single REPL widget. Task 11
  will compose `TUI` + `Container` + `Editor` + `Markdown` rather than calling
  one helper.
