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
 *
 * The optional `sessionNameOverride` constructor arg lets a caller
 * (specifically `hire_contractor` — Task 8) point a fresh `Memory`
 * at an already-forked child session like
 * `r-1:engineer:contractor-feature-x` instead of the auto-derived
 * `{runId}:{role}` name. Without it we'd have to mutate private
 * state, which leaks the abstraction.
 */
export class Memory {
  readonly run: RunIdentity;
  readonly role: AgentRole;
  private readonly _sessionName: string;
  private readonly _session: Session;

  constructor(
    client: Client,
    run: RunIdentity,
    role: AgentRole,
    sessionNameOverride?: string,
  ) {
    this.run = run;
    this.role = role;
    this._sessionName = sessionNameOverride ?? `${run.runId}:${role}`;
    this._session = client.session(this._sessionName);
  }

  get sessionName(): string {
    return this._sessionName;
  }

  /** Append one event; don't read back. Hot-path write. */
  async recordEvent(event: MemoryEvent): Promise<void> {
    await this._session.append(event, { fetchLastN: 0 });
  }

  /**
   * Fetch the last N events for this agent's session via the
   * server's pure-read `fetch_recent` endpoint. Does not write
   * any sentinel event to the session.
   */
  async loadHistory(n: number): Promise<AgentEvent[]> {
    const resp = await this._session.fetchRecent(n);
    return (resp.recent ?? []) as AgentEvent[];
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
