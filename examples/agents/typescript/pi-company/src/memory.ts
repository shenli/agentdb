import type { Client, Session } from "agentdb";
import type { AgentEvent, AgentRole, RunIdentity } from "./types.ts";

export interface MemoryEvent {
  kind: string;
  [k: string]: unknown;
}

/**
 * Per-agent session-as-event-log handle. One Memory instance owns
 * one AgentDB session named `{runId}:{role}`; the session's append
 * log IS the agent's working memory. Forking the session produces a
 * contractor child that inherits the full parent history at fork
 * time (MVCC).
 */
export class Memory {
  readonly run: RunIdentity;
  readonly role: AgentRole;
  private readonly _session: Session;

  constructor(client: Client, run: RunIdentity, role: AgentRole) {
    this.run = run;
    this.role = role;
    this._session = client.session(`${run.runId}:${role}`, { tenant: run.tenant });
  }

  get sessionName(): string {
    return `${this.run.runId}:${this.role}`;
  }

  /** Append one event; don't read back. Hot-path write. */
  async recordEvent(event: MemoryEvent): Promise<void> {
    await this._session.append(event, { fetchLastN: 0 });
  }

  /**
   * Fetch the last N events for this agent's session. Implemented
   * via append+fetch with a `_noop` placeholder, then filtered out
   * — the SDK's only read path is `append({fetchLastN})`. Pure-read
   * MongoDB-port access is out of scope for the demo wrapper.
   */
  async loadHistory(n: number): Promise<AgentEvent[]> {
    const resp = await this._session.append(
      { kind: "_noop" },
      { fetchLastN: n + 1 },
    );
    return resp.recent.filter((e) => {
      const payload = e.payload as { kind?: string } | undefined;
      return payload?.kind !== "_noop";
    }) as AgentEvent[];
  }

  /**
   * Fork this agent's session into a contractor child. Returns the
   * new session id (`{runId}:{role}:{childTag}`) for the caller to
   * spin up a fresh Memory against.
   */
  async forkSession(childTag: string): Promise<string> {
    const childSession = `${this.sessionName}:${childTag}`;
    await this._session.fork(childSession);
    return childSession;
  }
}
