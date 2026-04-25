/**
 * High-level AgentDB client.
 *
 *   const client = new Client("http://127.0.0.1:7878");
 *   const session = client.session("agent-1", { tenant: "demo" });
 *   const result = await session.append(
 *     { role: "user", text: "hello" },
 *     { fetchLastN: 5 }
 *   );
 *   console.log(result.seq, result.recent.map(e => e.payload));
 *
 * Uses Node's built-in `fetch` (Node 20+). Zero runtime deps.
 */

import { AgentDBError, fromResponse } from "./errors.js";

export interface ClientOptions {
  /** HTTP Basic Auth username. Required together with `password`. */
  user?: string;
  password?: string;
  /**
   * Default tenant for sessions. Required when auth is disabled
   * (the server can't derive tenant from credentials in that
   * case). Override per-session via `client.session(id, { tenant: "..." })`.
   */
  tenant?: string;
  /** Per-request timeout in milliseconds. Default 30s. */
  timeoutMs?: number;
  /** Inject a custom fetch (test mocks, instrumented clients). */
  fetch?: typeof globalThis.fetch;
}

export interface Event {
  seq: number;
  tsMs: number;
  payload: unknown;
  actorId?: number;
}

export interface AppendResult {
  seq: number;
  recent: Event[];
}

export interface AppendOptions {
  /**
   * Read back the last N events in the same round trip. 0 (the
   * default) skips the read-back and `recent` comes back empty —
   * cheaper for hot writes.
   */
  fetchLastN?: number;
  /** Optional small int for multi-actor sessions (user vs tool). */
  actorId?: number;
}

export interface SessionOptions {
  /**
   * Override the client's default tenant for this session. Use
   * when one client talks to many tenants — though that's only
   * possible with auth disabled, since auth-mode tenant comes
   * from credentials.
   */
  tenant?: string;
}

const DEFAULT_TIMEOUT_MS = 30_000;

/** An AgentDB connection. Use `client.session(...)` to open a session handle. */
export class Client {
  readonly baseUrl: string;
  readonly timeoutMs: number;
  private readonly _auth?: { user: string; password: string };
  private readonly _tenant?: string;
  private readonly _fetch: typeof globalThis.fetch;

  constructor(baseUrl: string, opts: ClientOptions = {}) {
    if (opts.user !== undefined && opts.password === undefined) {
      throw new Error("`password` is required when `user` is set");
    }
    if (opts.password !== undefined && opts.user === undefined) {
      throw new Error("`user` is required when `password` is set");
    }
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    if (opts.user !== undefined) {
      this._auth = { user: opts.user, password: opts.password! };
    }
    this._tenant = opts.tenant;
    this._fetch = opts.fetch ?? globalThis.fetch.bind(globalThis);
  }

  get authenticated(): boolean {
    return this._auth !== undefined;
  }

  /** Open a handle to one session. Tenant defaults to client default. */
  session(sessionId: string, opts: SessionOptions = {}): Session {
    return new Session(this, sessionId, opts.tenant ?? this._tenant);
  }

  /**
   * Probe `/health/ready`. Returns true on 200, false otherwise.
   * Useful while waiting on a server that's still bootstrapping
   * (dev-mode test harnesses, CI smoke tests).
   */
  async healthReady(): Promise<boolean> {
    try {
      const resp = await this._rawFetch("GET", "/health/ready");
      return resp.status === 200;
    } catch {
      return false;
    }
  }

  /** @internal */
  async _request(method: string, path: string, json?: unknown): Promise<Record<string, unknown>> {
    const resp = await this._rawFetch(method, path, json);
    let body: unknown;
    const text = await resp.text();
    if (text.length === 0) {
      body = null;
    } else {
      try {
        body = JSON.parse(text);
      } catch {
        body = text;
      }
    }
    if (resp.status >= 200 && resp.status < 300) {
      if (body && typeof body === "object" && !Array.isArray(body)) {
        return body as Record<string, unknown>;
      }
      throw new AgentDBError(
        `AgentDB returned 2xx with non-JSON body: ${String(body)}`,
        { statusCode: resp.status, body }
      );
    }
    throw fromResponse(resp.status, body);
  }

  private async _rawFetch(
    method: string,
    path: string,
    json?: unknown
  ): Promise<Response> {
    const headers: Record<string, string> = {};
    if (this._auth) {
      const token = Buffer.from(
        `${this._auth.user}:${this._auth.password}`
      ).toString("base64");
      headers["Authorization"] = `Basic ${token}`;
    }
    if (json !== undefined) {
      headers["Content-Type"] = "application/json";
    }
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), this.timeoutMs);
    try {
      return await this._fetch(`${this.baseUrl}${path}`, {
        method,
        headers,
        body: json !== undefined ? JSON.stringify(json) : undefined,
        signal: ctl.signal,
      });
    } finally {
      clearTimeout(t);
    }
  }
}

/**
 * A single AgentDB session, scoped to one tenant. Construct via
 * `Client.session(...)`. The session is a thin handle — every
 * method makes one round trip; no client-side event caching.
 */
export class Session {
  readonly sessionId: string;
  readonly tenant?: string;
  private readonly _client: Client;

  constructor(client: Client, sessionId: string, tenant?: string) {
    this._client = client;
    this.sessionId = sessionId;
    this.tenant = tenant;
  }

  /**
   * Append one event and (optionally) read back the last N. The
   * canonical hot-path primitive; combines the write and the
   * read-back into one round trip.
   */
  async append(event: unknown, opts: AppendOptions = {}): Promise<AppendResult> {
    const body: Record<string, unknown> = {
      event,
      fetch_last_n: opts.fetchLastN ?? 0,
    };
    if (opts.actorId !== undefined) {
      body.actor_id = opts.actorId;
    }
    const path = this._appendPath();
    const d = await this._client._request("POST", path, body);
    const recent = Array.isArray(d.recent)
      ? d.recent.map(eventFromDict)
      : [];
    return { seq: Number(d.seq), recent };
  }

  /**
   * Fork this session into a child that inherits the parent's
   * events at fork time, MVCC-backed. Returns a `Session` handle
   * for the new id.
   */
  async fork(childSessionId: string): Promise<Session> {
    const body: Record<string, unknown> = {
      new_session_id: childSessionId,
    };
    // No-auth mode: pass tenant through the deprecated body
    // field so the server can resolve tenant for sessions that
    // pre-date the locator (or for single-process tests). M26+
    // servers reject body-vs-auth mismatches with 400.
    if (this.tenant !== undefined && !this._client.authenticated) {
      body.tenant_id = this.tenant;
    }
    await this._client._request(
      "POST",
      `/api/sessions/${encodePathSegment(this.sessionId)}/fork`,
      body
    );
    return new Session(this._client, childSessionId, this.tenant);
  }

  toString(): string {
    const t = this.tenant ? `, tenant=${JSON.stringify(this.tenant)}` : "";
    return `Session(id=${JSON.stringify(this.sessionId)}${t})`;
  }

  private _appendPath(): string {
    if (this._client.authenticated && this.tenant === undefined) {
      // Tenant comes from auth — use the M26 session-keyed route.
      return `/api/sessions/${encodePathSegment(this.sessionId)}/append_and_fetch`;
    }
    if (this.tenant === undefined) {
      throw new Error(
        "`tenant` must be set on the Client or Session when no auth " +
          "credentials are configured. Pass tenant: 'demo' (or your " +
          "actual tenant id) to Client(...) or Client.session(...)."
      );
    }
    return (
      `/api/tenants/${encodePathSegment(this.tenant)}` +
      `/sessions/${encodePathSegment(this.sessionId)}/append_and_fetch`
    );
  }
}

function eventFromDict(d: unknown): Event {
  if (!d || typeof d !== "object") {
    throw new Error(`Expected event object, got ${typeof d}`);
  }
  const rec = d as Record<string, unknown>;
  const seq = Number(rec.seq);
  const tsMs = Number(rec.ts_ms ?? rec.ts ?? 0);
  const ev: Event = { seq, tsMs, payload: rec.payload };
  if (rec.actor_id !== undefined && rec.actor_id !== null) {
    ev.actorId = Number(rec.actor_id);
  }
  return ev;
}

function encodePathSegment(s: string): string {
  // Match the Python SDK's safe set: keep ASCII letters/digits and
  // `-_.~` raw, percent-escape everything else.
  return encodeURIComponent(s).replace(/['()*]/g, (c) => {
    return "%" + c.charCodeAt(0).toString(16).toUpperCase();
  });
}
