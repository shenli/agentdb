import type { Client } from "agentdb";
import { Agent as PiAgent, type AgentEvent as PiAgentEvent, type AgentTool } from "@mariozechner/pi-agent-core";
import type { TSchema } from "typebox";
import type { AgentRole, RunIdentity, AgentConfig, AgentEvent as DbEvent } from "./types.ts";
import { Memory } from "./memory.ts";
import { Channels, type BroadcastMessage } from "./channels.ts";
import { loadAgentConfig } from "./config.ts";
import { getLLM } from "./llm.ts";

/**
 * Context object handed to every tool handler at execution time. Tools
 * close over this to read/write memory, publish to channels, and address
 * messages back to the run-scoped peer set.
 *
 * `parentSession` is set only for contractor agents — it lets a
 * contractor's `report_to_ceo` (or equivalent) post results back to the
 * hiring agent's session.
 */
export interface ToolContext {
  memory: Memory;
  channels: Channels;
  client: Client;
  run: RunIdentity;
  role: AgentRole;
  /** For contractors: the parent's session, so results can be reported back. */
  parentSession?: string;
}

/**
 * Our internal tool descriptor — what `tools/index.ts` (Task 7) registers
 * against role names in code-side defaults / runtime config. We adapt these
 * into pi-mono's `AgentTool` shape inside {@link Agent.handleIncoming}.
 *
 * Handlers take `(args, ctx)` so the same registry entry can serve multiple
 * agent instances, each with its own `ToolContext`. pi-mono's `execute(id,
 * params, signal, onUpdate)` doesn't carry app context, so the wrapper closes
 * over `ctx`.
 *
 * `terminate` mirrors pi-mono's tool-result hint: a tool that signals task
 * completion (e.g. `notify_done` in Task 7) returns `{ terminate: true }`,
 * and pi-mono short-circuits the post-batch LLM call when every tool in the
 * batch agrees. The wrapper forwards it onto the `AgentToolResult`.
 */
export interface CompanyTool<TParameters extends TSchema = TSchema> {
  name: string;
  description: string;
  /** typebox `TSchema` — see PI_MONO_NOTES.md and pi-ai's `Type` re-export. */
  parameters: TParameters;
  /** Human-readable label for pi-tui display. Defaults to `name` if absent. */
  label?: string;
  handler: (args: any, ctx: ToolContext) => Promise<CompanyToolResult>;
}

/**
 * Return shape from a `CompanyTool.handler`. Mirrors pi-mono's
 * `AgentToolResult` minus the typebox generic (we don't need it on our side).
 *
 * `text` is the convenience-string we feed back to the LLM as a single text
 * content block. Tools that need richer payloads (e.g. multiple text blocks
 * or images) can return `content` directly and we forward it untouched.
 */
export interface CompanyToolResult {
  text?: string;
  content?: { type: "text"; text: string }[];
  details?: unknown;
  terminate?: boolean;
}

export interface AgentDeps {
  client: Client;
  run: RunIdentity;
  role: Exclude<AgentRole, "ceo">;
  channels: Channels;
  /** Channel names this role subscribes to (without the runId prefix — Channels adds it). */
  subscriptions: string[];
  /** Tool registry: name → CompanyTool. Built in tools/index.ts (Task 7). */
  toolRegistry: Record<string, CompanyTool>;
  /** Optional config override (used by hire_contractor to inject a contractor's specialized config). */
  configOverride?: AgentConfig;
  /** Optional task brief (used by contractors to scope their work). */
  taskBrief?: string;
  /** Optional explicit session name override (used by hire_contractor for the forked child session). */
  sessionNameOverride?: string;
  /** For contractors: the parent's session, threaded into ToolContext. */
  parentSession?: string;
  /**
   * Optional per-tool wrapper applied at registration time. Used by the
   * autonomous-mode orchestrator to install `wrapToolForObservation` on
   * every employee tool so the autonomous CEO sees observation events.
   * Identity-default (no-op) when undefined.
   */
  wrapTool?: (tool: CompanyTool) => CompanyTool;
}

/**
 * The pi-company agent loop.
 *
 * Lifecycle:
 *   1. `start()` wires up Memory, loads config, records an optional task
 *      brief, and kicks off a subscription loop per inbound channel.
 *   2. Each subscription loop awaits broadcasts on its channel; on every
 *      message it reloads config (hot-reload), constructs a fresh pi-mono
 *      `Agent` with the current system prompt + tool subset, and drives
 *      one `prompt(...)` call.
 *   3. `wait()` resolves once all subscription iterators terminate
 *      (typically when the bus is shut down).
 *
 * pi-mono ownership:
 *   `pi-agent-core`'s `Agent` is stateful and emits lifecycle events. We
 *   construct a *fresh* one per inbound message (rather than carrying one
 *   across messages) for two reasons:
 *     - Config hot-reload: tools, system prompt, model can all change
 *       between messages, and pi-mono's state copy semantics around
 *       `tools` reassignment are easy to get subtly wrong.
 *     - Memory is the durable transcript — not pi-mono's in-memory
 *       `state.messages`. We restore conversational context by stuffing
 *       a recent-history summary into the system prompt.
 *   The cost is one extra `agent_start` per peer message, which is
 *   negligible compared to LLM call latency.
 */
export class Agent {
  private memory!: Memory;
  private config!: AgentConfig;
  private deps: AgentDeps;
  private subscriptionTasks: Promise<void>[] = [];
  private stopped = false;

  constructor(deps: AgentDeps) {
    this.deps = deps;
  }

  async start(): Promise<void> {
    this.memory = new Memory(
      this.deps.client,
      this.deps.run,
      this.deps.role,
      this.deps.sessionNameOverride,
    );

    this.config = this.deps.configOverride
      ?? (await loadAgentConfig(this.deps.client, this.deps.role));

    if (this.deps.taskBrief) {
      await this.memory.recordEvent({ kind: "task_brief", text: this.deps.taskBrief });
    }

    for (const sub of this.deps.subscriptions) {
      this.subscriptionTasks.push(this.runSubscriptionLoop(sub));
    }
  }

  /**
   * Wait for all subscription loops to terminate. Returns when channel
   * sources close (e.g., bus shut down) or once all loops have observed
   * `stop()` and broken out.
   */
  async wait(): Promise<void> {
    await Promise.allSettled(this.subscriptionTasks);
  }

  /**
   * Cooperative stop signal. Each subscription loop checks this between
   * messages and breaks out of its `for await` if set. The orchestrator
   * (Task 10) calls this on `_meta` shutdown.
   *
   * Note: this does NOT cancel an in-flight `pi-agent.prompt()`. If a
   * call is mid-LLM-stream when stop is requested, it finishes naturally
   * and then the loop exits before pulling the next channel message.
   */
  stop(): void {
    this.stopped = true;
  }

  /**
   * Synthesize one self-prompted iteration with a task brief. Used by
   * `hire_contractor` (Task 8) to wake a freshly-forked contractor that
   * has no real channel subscriptions: we feed the brief through the same
   * `handleIncoming` path a peer message would take, so the contractor
   * thinks about its task and produces output via its (narrow) tool set.
   *
   * Returns a promise that resolves when the bootstrap turn completes.
   * Callers may fire-and-forget so the parent doesn't block on the
   * contractor's run.
   */
  async bootstrapTask(taskBrief: string): Promise<void> {
    const synthetic: BroadcastMessage = {
      channel: "(bootstrap)",
      ts_ms: Date.now(),
      sender: "system",
      payload: { kind: "task_brief", from: "system", text: taskBrief },
    };
    await this.handleIncoming("(bootstrap)", synthetic);
  }

  private async runSubscriptionLoop(channelName: string): Promise<void> {
    for await (const msg of this.deps.channels.subscribe(channelName)) {
      if (this.stopped) break;
      const payload = msg.payload as Record<string, unknown> | null | undefined;
      // Skip self-published messages so an agent that broadcasts on a
      // channel it also subscribes to doesn't reply to itself.
      if (payload && (payload as { from?: string }).from === this.deps.role) continue;
      try {
        await this.handleIncoming(channelName, msg);
      } catch (err) {
        await this.recordSafe({
          kind: "loop_error",
          channel: channelName,
          error: String(err),
        });
      }
    }
  }

  private async handleIncoming(channelName: string, msg: BroadcastMessage): Promise<void> {
    await this.memory.recordEvent({
      kind: "peer_message",
      channel: channelName,
      sender: msg.sender,
      payload: msg.payload,
    });

    // Hot-reload config every iteration so CEO overrides take effect on the
    // very next inbound message instead of after a restart. Skip the reload
    // when a fixed override was injected (contractors).
    if (!this.deps.configOverride) {
      this.config = await loadAgentConfig(
        this.deps.client,
        this.deps.role,
      );
    }

    const recent = await this.memory.loadHistory(20);
    const model = getLLM(this.config.model);

    const baseSystem = [this.config.systemPrompt, this.config.systemPromptSuffix]
      .filter(Boolean)
      .join("\n\n");
    const systemPrompt = this.composeSystemPrompt(baseSystem, recent);

    // Filter the registry by this role's tool names. Missing names log
    // (best-effort) but don't fail — config drift between roles + registry
    // is a common edit and shouldn't crash the agent.
    const wrap = this.deps.wrapTool ?? ((t: CompanyTool) => t);
    const tools = this.config.tools
      .map((name) => this.deps.toolRegistry[name])
      .filter((t): t is CompanyTool => t !== undefined)
      .map(wrap);

    if (tools.length !== this.config.tools.length) {
      const missing = this.config.tools.filter((n) => !this.deps.toolRegistry[n]);
      await this.recordSafe({ kind: "missing_tools", missing });
    }

    const ctx: ToolContext = {
      memory: this.memory,
      channels: this.deps.channels,
      client: this.deps.client,
      run: this.deps.run,
      role: this.deps.role,
      parentSession: this.deps.parentSession,
    };

    const piTools: AgentTool<TSchema>[] = tools.map((t) => this.adaptTool(t, ctx));

    const piAgent = new PiAgent({
      initialState: {
        systemPrompt,
        model,
        tools: piTools,
      },
    });

    // Subscribe to lifecycle events so we have a durable record of what the
    // LLM said and which tools fired. The tool wrapper itself records tool
    // results (with `details`); here we capture assistant text + the run
    // boundary.
    const unsub = piAgent.subscribe(async (event: PiAgentEvent) => {
      try {
        if (event.type === "message_end" && (event.message as any).role === "assistant") {
          await this.memory.recordEvent({
            kind: "assistant_message",
            content: (event.message as any).content,
            stopReason: (event.message as any).stopReason,
          });
        } else if (event.type === "tool_execution_end") {
          await this.memory.recordEvent({
            kind: "tool_executed",
            toolName: event.toolName,
            toolCallId: event.toolCallId,
            isError: event.isError,
          });
        } else if (event.type === "agent_end") {
          await this.memory.recordEvent({
            kind: "agent_end",
            messageCount: (event.messages ?? []).length,
          });
        }
      } catch {
        // Listener errors must not propagate; pi-mono awaits them in the
        // run's settlement and an unhandled rejection here would surface as
        // the prompt result. Logging via memory is best-effort.
      }
    });

    // Drive the turn. pi-mono's `prompt()` returns only after the entire
    // run (including tool follow-ups + awaited subscribers) has settled.
    const promptText = this.payloadToPrompt(msg);
    try {
      await piAgent.prompt(promptText);
    } catch (err) {
      await this.recordSafe({ kind: "prompt_error", error: String(err) });
      throw err;
    } finally {
      unsub();
    }
  }

  /**
   * Render recent memory events into a pre-pended context block so the LLM
   * has rough conversational continuity without us having to translate
   * arbitrary `MemoryEvent` shapes into `UserMessage`/`AssistantMessage`
   * pi-ai records. Quick approximation; sufficient for v1. v2 could parse
   * memory into proper structured messages.
   */
  private composeSystemPrompt(baseSystem: string, recent: DbEvent[]): string {
    if (recent.length === 0) return baseSystem;
    const lines: string[] = [];
    for (const e of recent) {
      const p: any = (e as any).payload ?? e;
      const kind = p?.kind ?? "event";
      // Cap each line so a single huge event can't blow the prompt budget.
      const body = JSON.stringify(p).slice(0, 240);
      lines.push(`[${kind}] ${body}`);
    }
    return `${baseSystem}\n\nRecent context (most recent last):\n${lines.join("\n")}`;
  }

  /**
   * Pi-mono's `prompt()` accepts a string or one/many `AgentMessage`s. We
   * pass a string here for v1 — incoming channel payloads are already
   * structured JSON; stringifying once gives the LLM enough to act on
   * without us inventing a wire format.
   */
  private payloadToPrompt(msg: BroadcastMessage): string {
    const senderHint = msg.sender ? `sender=${msg.sender} ` : "";
    return `Inbound message ${senderHint}on channel '${msg.channel}':\n${JSON.stringify(msg.payload)}`;
  }

  /**
   * Adapt a CompanyTool into pi-mono's `AgentTool`. The wrapper:
   *   - Closes over the per-message `ToolContext`.
   *   - Records each tool invocation to memory before delegating, so we
   *     have an audit trail even if the handler throws.
   *   - Re-raises handler errors instead of encoding them in `content`,
   *     per pi-mono's contract — the runtime turns thrown errors into
   *     a tool-result error message automatically.
   *   - Maps `text` shorthand into the `[{ type: "text", text }]` content
   *     shape pi-mono expects.
   */
  private adaptTool(tool: CompanyTool, ctx: ToolContext): AgentTool<TSchema> {
    const recordSafe = (e: Record<string, unknown>) => this.recordSafe(e);
    return {
      name: tool.name,
      description: tool.description,
      label: tool.label ?? tool.name,
      parameters: tool.parameters,
      async execute(toolCallId, params) {
        await recordSafe({
          kind: "tool_call",
          toolCallId,
          toolName: tool.name,
          args: params,
        });
        const out = await tool.handler(params, ctx);
        const content = out.content
          ?? [{ type: "text" as const, text: out.text ?? "" }];
        return {
          content,
          details: out.details ?? {},
          terminate: out.terminate,
        };
      },
    };
  }

  private async recordSafe(event: Record<string, unknown>): Promise<void> {
    try {
      await this.memory.recordEvent(event as any);
    } catch {
      // If memory itself is failing we have nowhere to log; swallow so the
      // primary run isn't taken down by an audit-write failure.
    }
  }
}
