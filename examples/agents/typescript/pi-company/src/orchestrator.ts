import type { Client } from "agentdb";
import type { RunIdentity, AgentRole } from "./types.ts";
import { Channels } from "./channels.ts";
import { Agent } from "./agent.ts";
import { ROLE_TOOLS, filterTools } from "./tools/index.ts";

const META_SESSION_NAME = "_meta";

/**
 * Per-role channel subscriptions. CEO is intentionally absent — the human
 * player owns that role and does not have an autonomous loop. Contractor is
 * also absent — contractors are spawned on demand by `hire_contractor`
 * (Task 8) with no real channel subscriptions; they bootstrap from a task
 * brief instead. See spec § 5.7.
 *
 * Subscriptions are channel BASE names (e.g. `"announcements"`); `Channels`
 * prepends the runId to produce the actual channel id (`{runId}:announcements`).
 * See `Channels.subscribe` in `channels.ts`.
 */
const ROLE_SUBSCRIPTIONS: Record<Exclude<AgentRole, "ceo" | "contractor">, string[]> = {
  cto: ["announcements", "engineering"],
  engineer: ["announcements", "engineering"],
  marketing: ["announcements", "gtm"],
  sales: ["announcements", "gtm"],
};

type EmployeeRole = keyof typeof ROLE_SUBSCRIPTIONS;

/**
 * Boots and supervises one run of the demo "company". Owns:
 *   - the lifecycle log on the `{runId}:_meta` session (`run_started`,
 *     `agent_joined` × N, `run_ended`)
 *   - the four employee agents (cto/engineer/marketing/sales) with their
 *     role-specific channel subscriptions
 *
 * Contractor agents (spawned by `hire_contractor`) are NOT pre-booted here.
 * See `stop()` for the v1 trade-off on contractor shutdown.
 */
export class Orchestrator {
  private agents: Agent[] = [];
  private readonly client: Client;
  private readonly run: RunIdentity;

  constructor(client: Client, run: RunIdentity) {
    this.client = client;
    this.run = run;
  }

  /**
   * Records `run_started`, then constructs and `start()`s each employee
   * agent with its role's tool subset and channel subscriptions. Emits
   * one `agent_joined` event per role.
   */
  async startCompany(): Promise<void> {
    const roles = Object.keys(ROLE_SUBSCRIPTIONS) as EmployeeRole[];
    await this.recordMeta({
      kind: "run_started",
      started_at: Date.now(),
      agents: roles,
    });

    const channels = new Channels(this.client, this.run);

    for (const role of roles) {
      const tools = filterTools(ROLE_TOOLS[role] ?? []);
      const agent = new Agent({
        client: this.client,
        run: this.run,
        role,
        channels,
        subscriptions: ROLE_SUBSCRIPTIONS[role],
        toolRegistry: tools,
      });
      this.agents.push(agent);
      await agent.start();
      await this.recordMeta({ kind: "agent_joined", role });
    }
  }

  /**
   * Soft shutdown: records `run_ended` to `_meta`. Does NOT actively
   * cancel agent subscription loops — they terminate when their channel
   * sources close (e.g. on bus shutdown / process exit).
   *
   * v1: contractor agents (spawned by `hire_contractor`) run until process
   * exit. They're fire-and-forget and not tracked. If they're mid-prompt
   * when stop() is called, they finish their current LLM call before
   * terminating. Task 13's SIGINT/SIGTERM handlers ensure clean process
   * shutdown. v2 may add a contractor registry + active cancellation.
   */
  async stop(reason: string): Promise<void> {
    await this.recordMeta({
      kind: "run_ended",
      ended_at: Date.now(),
      reason,
    });
    for (const agent of this.agents) {
      agent.stop();
    }
  }

  /**
   * Block until all employee agents' subscription loops settle. Used by
   * the entry script (Task 13) to keep the process alive while the run
   * is active.
   */
  async wait(): Promise<void> {
    await Promise.allSettled(this.agents.map((a) => a.wait()));
  }

  private async recordMeta(event: Record<string, unknown>): Promise<void> {
    const sessionName = `${this.run.runId}:${META_SESSION_NAME}`;
    const session = this.client.session(sessionName, { tenant: this.run.tenant });
    await session.append(event as any, { fetchLastN: 0 });
  }
}
