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

import { BroadcastMessage, parseBroadcastEnvelope } from "./broadcast.js";
import { AgentDBError, SubscriptionClosed, fromResponse } from "./errors.js";
import { parseSse } from "./sse.js";

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
  /**
   * Static JWT used for `subscribe()` bearer auth. Mutually
   * exclusive with `getToken`. Use `getToken` for tokens that
   * rotate.
   */
  jwt?: string;
  /**
   * Token provider for `subscribe()` bearer auth. Called fresh on
   * every reconnect, so a server-side `jwt_expired` close is
   * recovered transparently. May return a string or a Promise.
   */
  getToken?: () => string | Promise<string>;
}

export interface Event {
  seq: number;
  tsMs: number;
  payload: unknown;
  actorId?: number;
  /** Set on events that came from `subscribe()`. */
  tenant?: string;
  /** Set on events that came from `subscribe()`. */
  session?: string;
  /** Set on events that came from `subscribe()` (when the server tagged a kind). */
  kind?: string;
}

export interface SubscribeOptions {
  /** Filter by event kind(s). */
  kind?: string[];
  /**
   * Replay from this sequence number first, then transition into
   * live tail without dropping events. Inclusive lower bound.
   */
  fromSeq?: number;
}

export interface ClientSubscribeOptions extends SubscribeOptions {
  /** Override the client's default tenant. */
  tenant?: string;
  /** Filter to one or more session ids. */
  session?: string[];
  /**
   * Per-session catch-up cursors for tenant-wide subscribe.
   * The map key is the session id; the value is a `from_seq`.
   * Sent as `from_seq_per_session=sid:seq,sid:seq,...`.
   */
  fromSeqPerSession?: Record<string, number>;
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
  private readonly _jwt?: string;
  private readonly _getToken?: () => string | Promise<string>;

  constructor(baseUrl: string, opts: ClientOptions = {}) {
    if (opts.user !== undefined && opts.password === undefined) {
      throw new Error("`password` is required when `user` is set");
    }
    if (opts.password !== undefined && opts.user === undefined) {
      throw new Error("`user` is required when `password` is set");
    }
    if (opts.jwt !== undefined && opts.getToken !== undefined) {
      throw new Error("pass `jwt` or `getToken`, not both");
    }
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    if (opts.user !== undefined) {
      this._auth = { user: opts.user, password: opts.password! };
    }
    this._tenant = opts.tenant;
    this._fetch = opts.fetch ?? globalThis.fetch.bind(globalThis);
    this._jwt = opts.jwt;
    this._getToken = opts.getToken;
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

  /**
   * Tenant-wide event subscription.
   *
   * Yields envelopes from `/api/tenants/{t}/subscribe` — every
   * appended event in the tenant, optionally filtered by session id
   * and/or kind. AsyncIterable: consume with `for await`.
   *
   * On a `jwt_expired` close frame the client transparently
   * refreshes the token (via `getToken`, if provided) and reconnects
   * with `fromSeq = lastSeen + 1` so the consumer never sees a gap.
   * Other terminal frames raise [`SubscriptionClosed`] for the
   * caller to handle.
   *
   * Use this for fan-out reactors that watch many sessions; for
   * single-session tailing, `session.subscribe()` is cheaper.
   */
  async *subscribe(opts: ClientSubscribeOptions = {}): AsyncIterable<Event> {
    const tenant = opts.tenant ?? this._tenant;
    if (tenant === undefined) {
      throw new Error(
        "`tenant` must be set on Client(...) or passed to " +
          "subscribe({ tenant: '...' }) — there is no auth-derived " +
          "default for the tenant-wide subscribe endpoint."
      );
    }
    const path = `/api/tenants/${encodePathSegment(tenant)}/subscribe`;
    yield* this._subscribeImpl({
      path,
      session: opts.session,
      kind: opts.kind,
      fromSeq: opts.fromSeq,
      fromSeqPerSession: opts.fromSeqPerSession,
    });
  }

  /**
   * Publish an ephemeral broadcast message to a channel.
   *
   * Broadcast is **ephemeral** — no durability, no
   * replay, no per-subscriber acks. Use it for transient signals
   * like typing indicators, cursor positions, or agent-status pings.
   * If you need durability, use `Session.append` + `Session.subscribe`
   * on a session instead.
   *
   * `channel` must match `^[a-zA-Z0-9._:-]{1,256}$` — the server
   * rejects malformed names with `channel_name_invalid`. `payload`
   * is any JSON-serializable value.
   *
   * Tenant resolution mirrors the rest of the SDK: pass `tenant` to
   * override, otherwise the client default is used. Resolves on 204.
   */
  async broadcast(opts: {
    channel: string;
    payload: unknown;
    tenant?: string;
  }): Promise<void> {
    const tenant = opts.tenant ?? this._tenant;
    if (tenant === undefined) {
      throw new Error(
        "`tenant` must be set on Client(...) or passed to " +
          "broadcast({ tenant: '...' }) — the channels endpoint is " +
          "tenant-explicit."
      );
    }
    const path =
      `/api/tenants/${encodePathSegment(tenant)}` +
      `/channels/${encodePathSegment(opts.channel)}/publish`;
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    await this._applyAuthHeaders(headers);
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), this.timeoutMs);
    let resp: Response;
    try {
      resp = await this._fetch(`${this.baseUrl}${path}`, {
        method: "POST",
        headers,
        body: JSON.stringify(opts.payload),
        signal: ctl.signal,
      });
    } finally {
      clearTimeout(t);
    }
    if (resp.status === 204) return;
    let body: unknown = null;
    const text = await resp.text();
    if (text.length > 0) {
      try {
        body = JSON.parse(text);
      } catch {
        body = text;
      }
    }
    throw fromResponse(resp.status, body);
  }

  /**
   * Subscribe to a channel; yield [`BroadcastMessage`] envelopes.
   *
   * Counterpart to [`broadcast`]. AsyncIterable: consume with
   * `for await`. The generator runs until the caller breaks out of
   * the loop or the server closes the connection.
   *
   * Unlike per-session [`subscribe`], there is **no JWT
   * auto-reconnect on expiry** — broadcast is ephemeral by design
   * (spec § 5.5), so loss on disconnect is acceptable. Callers that
   * need durability use `Session.subscribe()` instead.
   *
   * `noEcho: true` asks the server to drop envelopes whose `sender`
   * matches this subscriber's JWT `sub` claim. Only effective with
   * JWT auth — Basic-auth publishers emit no `sender` and never
   * match.
   */
  async *subscribeChannel(opts: {
    channel: string;
    tenant?: string;
    noEcho?: boolean;
  }): AsyncIterable<BroadcastMessage> {
    const tenant = opts.tenant ?? this._tenant;
    if (tenant === undefined) {
      throw new Error(
        "`tenant` must be set on Client(...) or passed to " +
          "subscribeChannel({ tenant: '...' }) — the channels " +
          "endpoint is tenant-explicit."
      );
    }
    const url = new URL(
      `${this.baseUrl}/api/tenants/${encodePathSegment(tenant)}` +
        `/channels/${encodePathSegment(opts.channel)}/subscribe`
    );
    if (opts.noEcho) {
      url.searchParams.set("no_echo", "true");
    }
    const headers: Record<string, string> = { Accept: "text/event-stream" };
    await this._applyAuthHeaders(headers);
    // SSE is long-lived; no AbortController timeout — the consumer
    // controls cancellation by breaking out of the `for await`.
    const resp = await this._fetch(url, { headers });
    if (!resp.ok) {
      let body: unknown = null;
      const text = await resp.text();
      if (text.length > 0) {
        try {
          body = JSON.parse(text);
        } catch {
          body = text;
        }
      }
      throw fromResponse(resp.status, body);
    }
    if (!resp.body) {
      throw new AgentDBError("subscribeChannel response has no body", {
        statusCode: resp.status,
      });
    }
    const reader = resp.body.getReader();
    try {
      for await (const frame of parseSse(reader)) {
        // The broadcast stream uses no named event for envelopes; only
        // `:keepalive` comments (which the parser drops) and the
        // data frames themselves. Tolerate a `subscribed` ack the
        // same way as per-session subscribe in case the server adds
        // one later.
        if (frame.event && frame.event !== "message") continue;
        if (!frame.data) continue;
        yield parseBroadcastEnvelope(frame.data);
      }
    } finally {
      try {
        reader.releaseLock();
      } catch {
        /* lock already released on stream close */
      }
    }
  }

  /**
   * Apply Bearer (preferred) or Basic auth onto a header map. Used
   * by `broadcast` / `subscribeChannel` which both need the same
   * "JWT first, fall back to Basic" plumbing as `subscribe`.
   * @internal
   */
  private async _applyAuthHeaders(
    headers: Record<string, string>
  ): Promise<void> {
    const token = await this._bearerToken();
    if (token !== undefined) {
      headers["Authorization"] = `Bearer ${token}`;
      return;
    }
    if (this._auth) {
      const t = Buffer.from(
        `${this._auth.user}:${this._auth.password}`
      ).toString("base64");
      headers["Authorization"] = `Basic ${t}`;
    }
  }

  /** @internal */
  async _bearerToken(): Promise<string | undefined> {
    if (this._getToken !== undefined) {
      return await this._getToken();
    }
    return this._jwt;
  }

  /** @internal */
  async *_subscribeImpl(args: {
    path: string;
    session?: string[];
    kind?: string[];
    fromSeq?: number;
    fromSeqPerSession?: Record<string, number>;
  }): AsyncGenerator<Event> {
    let cursor = args.fromSeq;
    const perSessionCursor: Record<string, number> | undefined =
      args.fromSeqPerSession ? { ...args.fromSeqPerSession } : undefined;
    for (;;) {
      let lastSeen: number | undefined;
      try {
        for await (const evt of this._streamOnce({
          path: args.path,
          session: args.session,
          kind: args.kind,
          fromSeq: cursor,
          fromSeqPerSession: perSessionCursor,
        })) {
          lastSeen = evt.seq;
          if (perSessionCursor && evt.session) {
            perSessionCursor[evt.session] = evt.seq;
          }
          yield evt;
        }
        return; // server closed cleanly
      } catch (e) {
        if (
          e instanceof SubscriptionClosed &&
          e.errorCode === "jwt_expired" &&
          (this._getToken !== undefined || this._jwt !== undefined)
        ) {
          const fallback = e.lastSeq ?? lastSeen;
          if (fallback !== undefined) {
            cursor = fallback + 1;
          }
          continue;
        }
        throw e;
      }
    }
  }

  private async *_streamOnce(args: {
    path: string;
    session?: string[];
    kind?: string[];
    fromSeq?: number;
    fromSeqPerSession?: Record<string, number>;
  }): AsyncGenerator<Event> {
    const url = new URL(`${this.baseUrl}${args.path}`);
    if (args.fromSeq !== undefined) {
      url.searchParams.set("from_seq", String(args.fromSeq));
    }
    if (args.fromSeqPerSession && Object.keys(args.fromSeqPerSession).length > 0) {
      const joined = Object.entries(args.fromSeqPerSession)
        .map(([sid, seq]) => `${sid}:${seq}`)
        .join(",");
      url.searchParams.set("from_seq_per_session", joined);
    }
    for (const s of args.session ?? []) {
      url.searchParams.append("session", s);
    }
    for (const k of args.kind ?? []) {
      url.searchParams.append("kind", k);
    }
    const headers: Record<string, string> = { Accept: "text/event-stream" };
    const token = await this._bearerToken();
    if (token !== undefined) {
      headers["Authorization"] = `Bearer ${token}`;
    } else if (this._auth) {
      const t = Buffer.from(
        `${this._auth.user}:${this._auth.password}`
      ).toString("base64");
      headers["Authorization"] = `Basic ${t}`;
    }
    // SSE is long-lived; no AbortController timeout — caller
    // controls cancellation by breaking out of the `for await`.
    const resp = await this._fetch(url, { headers });
    if (!resp.ok) {
      let body: unknown = null;
      const text = await resp.text();
      if (text.length > 0) {
        try {
          body = JSON.parse(text);
        } catch {
          body = text;
        }
      }
      throw fromResponse(resp.status, body);
    }
    if (!resp.body) {
      throw new AgentDBError("subscribe response has no body", {
        statusCode: resp.status,
      });
    }
    const reader = resp.body.getReader();
    try {
      for await (const frame of parseSse(reader)) {
        if (frame.event === "subscribed") {
          // Acknowledgement — informational live_seq, no event payload.
          continue;
        }
        if (frame.event === "error") {
          let body: { error?: string; last_seq?: number } = {};
          try {
            body = frame.data ? JSON.parse(frame.data) : {};
          } catch {
            // Server should always emit valid JSON; if not, surface
            // the raw text in the error code so debugging isn't
            // entirely opaque.
          }
          throw new SubscriptionClosed(body.error ?? "unknown", {
            lastSeq: body.last_seq,
          });
        }
        if (!frame.data) continue;
        const env = JSON.parse(frame.data) as Record<string, unknown>;
        const evt: Event = {
          seq: Number(env.seq),
          tsMs: Number(env.ts_ms ?? env.ts ?? 0),
          payload: env.payload,
        };
        if (env.tenant !== undefined) evt.tenant = String(env.tenant);
        if (env.session !== undefined) evt.session = String(env.session);
        if (env.kind !== undefined && env.kind !== null) {
          evt.kind = String(env.kind);
        }
        if (env.actor_id !== undefined && env.actor_id !== null) {
          evt.actorId = Number(env.actor_id);
        }
        yield evt;
      }
    } finally {
      try {
        reader.releaseLock();
      } catch {
        /* lock already released on stream close */
      }
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
   * Stream events for this session via SSE.
   *
   * AsyncIterable: consume with `for await`. Use `fromSeq` to
   * replay from a known cursor; the server transitions from
   * catch-up to live tail without dropping events. Use `kind` to
   * filter on the server side.
   *
   * Reconnect / error semantics mirror [`Client.subscribe`]:
   * `jwt_expired` is auto-recovered when a token provider is set;
   * other terminal close frames raise [`SubscriptionClosed`].
   */
  async *subscribe(opts: SubscribeOptions = {}): AsyncIterable<Event> {
    if (this.tenant === undefined) {
      throw new Error(
        "`tenant` must be set on the Client or Session when subscribing — " +
          "auth-derived tenants on the subscribe path aren't supported yet. " +
          "Pass tenant: 'demo' (or your tenant id) to Client(...) or Client.session()."
      );
    }
    const path =
      `/api/tenants/${encodePathSegment(this.tenant)}` +
      `/sessions/${encodePathSegment(this.sessionId)}/subscribe`;
    yield* this._client._subscribeImpl({
      path,
      kind: opts.kind,
      fromSeq: opts.fromSeq,
    });
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
