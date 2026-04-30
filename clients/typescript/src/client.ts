import { BroadcastMessage, parseBroadcastEnvelope } from "./broadcast.js";
import { AgentDBError, SubscriptionClosed, fromResponse } from "./errors.js";
import { parseSse } from "./sse.js";
import { Buffer } from "node:buffer";
import { randomUUID } from "node:crypto";

export type TenantId = string;
export type AgentId = string;
export type SessionId = string;
export type EventId = string;
export type ArtifactId = string;
export type RecordId = string;
export type SkillId = string;

export interface ClientOptions {
  baseUrl: string;
  auth?: { user: string; password: string };
  token?: string;
  getToken?: () => string | Promise<string>;
  timeoutMs?: number;
  fetch?: typeof globalThis.fetch;
}

export interface Event {
  tenantId: TenantId;
  agentId: AgentId;
  sessionId: SessionId;
  session?: SessionId;
  eventId: EventId;
  seq: number;
  kind: string;
  payload: unknown;
  createdAtMs: number;
}

export interface AppendResult {
  eventId: EventId;
  seq: number;
  recent: Event[];
}

export interface WatchItem {
  cursor: string;
  event: Event;
}

export interface AppendOptions {
  kind?: string;
  agentId?: AgentId;
  artifactRefs?: unknown[];
  sourceRefs?: unknown[];
  idempotencyKey?: string;
  fetchLastN?: number;
  actorId?: number;
}

export interface AppendAndFetchOptions extends AppendOptions {
  last?: number;
  maxBytes?: number;
}

export interface SessionOptions {
  agentId?: AgentId;
}

export interface SubscribeOptions {
  fromSeq?: number;
  limit?: number;
}

export interface ClientSubscribeOptions {
  scope?: "tenant" | "session" | "agent";
  sessionId?: SessionId;
  agentId?: AgentId;
  fromCursor?: string;
  limit?: number;
}

const DEFAULT_TIMEOUT_MS = 30_000;

export class Client {
  readonly baseUrl: string;
  readonly timeoutMs: number;
  private readonly _auth?: { user: string; password: string };
  private readonly _fetch: typeof globalThis.fetch;
  private readonly _token?: string;
  private readonly _getToken?: () => string | Promise<string>;

  constructor(opts: ClientOptions) {
    if (opts.token !== undefined && opts.getToken !== undefined) {
      throw new Error("pass `token` or `getToken`, not both");
    }
    this.baseUrl = opts.baseUrl.replace(/\/+$/, "");
    this.timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this._auth = opts.auth;
    this._fetch = opts.fetch ?? globalThis.fetch.bind(globalThis);
    this._token = opts.token;
    this._getToken = opts.getToken;
  }

  get authenticated(): boolean {
    return this._auth !== undefined;
  }

  async healthReady(): Promise<boolean> {
    try {
      return (await this._rawFetch("GET", "/health/ready")).status === 200;
    } catch {
      return false;
    }
  }

  async createTenant(externalKey: string, policy: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
    return await this._request("POST", "/api/tenants", { external_key: externalKey, policy });
  }

  async createAgent(
    tenantId: TenantId,
    opts: { alias?: string | null; config?: Record<string, unknown> } = {}
  ): Promise<Record<string, unknown>> {
    return await this._request("POST", `/api/tenants/${enc(tenantId)}/agents`, {
      alias: opts.alias ?? null,
      config: opts.config ?? {},
    });
  }

  async createSession(
    tenantId: TenantId,
    agentId: AgentId,
    opts: { alias?: string | null } = {}
  ): Promise<Session> {
    const data = await this._request(
      "POST",
      `/api/tenants/${enc(tenantId)}/agents/${enc(agentId)}/sessions`,
      { alias: opts.alias ?? null }
    );
    return new Session(this, tenantId, String(data.session_id), { agentId: String(data.agent_id) });
  }

  session(tenantId: TenantId, sessionId?: SessionId, opts: SessionOptions = {}): Session {
    if (sessionId === undefined) {
      return new Session(this, tenantId, tenantId, opts);
    }
    return new Session(this, tenantId, sessionId, opts);
  }

  async putArtifact(
    tenantId: TenantId,
    data: BodyInit,
    opts: { contentType?: string; sha256?: string } = {}
  ): Promise<Record<string, unknown>> {
    const headers: Record<string, string> = {};
    if (opts.contentType) headers["Content-Type"] = opts.contentType;
    if (opts.sha256) headers["X-Sha256"] = opts.sha256;
    return await this._requestRawJson("POST", `/api/tenants/${enc(tenantId)}/artifacts`, {
      body: data,
      headers,
    });
  }

  async getArtifact(tenantId: TenantId, artifactId: ArtifactId): Promise<Record<string, unknown>> {
    return await this._request("GET", `/api/tenants/${enc(tenantId)}/artifacts/${enc(artifactId)}`);
  }

  async getArtifactBlob(tenantId: TenantId, artifactId: ArtifactId): Promise<ArrayBuffer> {
    const resp = await this._rawFetch("GET", `/api/tenants/${enc(tenantId)}/artifacts/${enc(artifactId)}/blob`);
    if (resp.ok) return await resp.arrayBuffer();
    throw await errorFrom(resp);
  }

  async writeRecord(tenantId: TenantId, body: Record<string, unknown>): Promise<Record<string, unknown>> {
    return await this._request("POST", `/api/tenants/${enc(tenantId)}/records`, body);
  }

  async searchRecords(tenantId: TenantId, body: Record<string, unknown>): Promise<unknown[]> {
    const data = await this._request("POST", `/api/tenants/${enc(tenantId)}/records/search`, body);
    return Array.isArray(data.records) ? data.records : [];
  }

  async registerSkill(
    tenantId: TenantId,
    body: { name: string; version: string; manifest?: Record<string, unknown>; artifactRefs?: unknown[]; idempotencyKey?: string }
  ): Promise<Record<string, unknown>> {
    return await this._request("POST", `/api/tenants/${enc(tenantId)}/skills`, {
      name: body.name,
      version: body.version,
      manifest: body.manifest ?? {},
      artifact_refs: body.artifactRefs ?? [],
    }, idem(body.idempotencyKey));
  }

  async bindSkill(
    tenantId: TenantId,
    agentId: AgentId,
    skillId: SkillId,
    opts: { idempotencyKey?: string } = {}
  ): Promise<Record<string, unknown>> {
    return await this._request(
      "POST",
      `/api/tenants/${enc(tenantId)}/agents/${enc(agentId)}/skills/bind`,
      { skill_id: skillId },
      idem(opts.idempotencyKey)
    );
  }

  async watch(tenantId: TenantId, opts: ClientSubscribeOptions = {}): Promise<WatchItem[]> {
    const url = new URL(`${this.baseUrl}/api/tenants/${enc(tenantId)}/watch`);
    url.searchParams.set("scope", opts.scope ?? "tenant");
    url.searchParams.set("limit", String(opts.limit ?? 100));
    if (opts.sessionId) url.searchParams.set("session_id", opts.sessionId);
    if (opts.agentId) url.searchParams.set("agent_id", opts.agentId);
    if (opts.fromCursor) url.searchParams.set("from_cursor", opts.fromCursor);
    const data = await this._requestAbsolute("GET", url);
    return Array.isArray(data) ? data.map(watchItemFromDict) : [];
  }

  async *subscribe(_opts: ClientSubscribeOptions = {}): AsyncIterable<Event> {
    throw new Error("tenant-wide streaming requires watch(tenantId, ...) in V2");
  }

  async broadcast(opts: { channel: string; payload: unknown }): Promise<void> {
    const resp = await this._rawFetch("POST", `/api/channels/${enc(opts.channel)}/publish`, opts.payload, {}, true);
    if (resp.status === 204) return;
    throw await errorFrom(resp);
  }

  async *subscribeChannel(opts: { channel: string; noEcho?: boolean }): AsyncIterable<BroadcastMessage> {
    const url = new URL(`${this.baseUrl}/api/channels/${enc(opts.channel)}/subscribe`);
    if (opts.noEcho) url.searchParams.set("no_echo", "true");
    const token = await this._channelBearerToken();
    if (token) url.searchParams.set("access_token", token);
    const resp = await this._fetch(url, { headers: { Accept: "text/event-stream" } });
    if (!resp.ok) throw await errorFrom(resp);
    if (!resp.body) throw new AgentDBError("subscribeChannel response has no body", { statusCode: resp.status });
    const reader = resp.body.getReader();
    try {
      for await (const frame of parseSse(reader)) {
        if (frame.event && frame.event !== "message") continue;
        if (frame.data) yield parseBroadcastEnvelope(frame.data);
      }
    } finally {
      try { reader.releaseLock(); } catch {}
    }
  }

  async _request(method: string, path: string, json?: unknown, headers: Record<string, string> = {}): Promise<Record<string, unknown>> {
    const resp = await this._rawFetch(method, path, json, headers);
    if (!resp.ok) throw await errorFrom(resp);
    const text = await resp.text();
    if (!text) return {};
    const data = JSON.parse(text) as unknown;
    if (data && typeof data === "object" && !Array.isArray(data)) return data as Record<string, unknown>;
    return data as Record<string, unknown>;
  }

  private async _requestAbsolute(method: string, url: URL): Promise<unknown> {
    const resp = await this._fetch(url, { method, headers: await this._authHeaders() });
    if (!resp.ok) throw await errorFrom(resp);
    return JSON.parse(await resp.text());
  }

  private async _requestRawJson(method: string, path: string, init: { body?: BodyInit; headers?: Record<string, string> }): Promise<Record<string, unknown>> {
    const resp = await this._rawFetch(method, path, undefined, init.headers ?? {}, false, init.body);
    if (!resp.ok) throw await errorFrom(resp);
    return JSON.parse(await resp.text()) as Record<string, unknown>;
  }

  async _rawFetch(
    method: string,
    path: string,
    json?: unknown,
    headers: Record<string, string> = {},
    channelAuth = false,
    rawBody?: BodyInit
  ): Promise<Response> {
    const hdrs = { ...headers, ...(await this._authHeaders(channelAuth)) };
    let body: BodyInit | undefined = rawBody;
    if (json !== undefined) {
      hdrs["Content-Type"] = "application/json";
      body = JSON.stringify(json);
    }
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), this.timeoutMs);
    try {
      return await this._fetch(`${this.baseUrl}${path}`, { method, headers: hdrs, body, signal: ctl.signal });
    } finally {
      clearTimeout(t);
    }
  }

  async _fetchUrl(input: string | URL, init: RequestInit): Promise<Response> {
    return await this._fetch(input, init);
  }

  private async _authHeaders(channelAuth = false): Promise<Record<string, string>> {
    const headers: Record<string, string> = {};
    if (channelAuth) {
      const token = await this._channelBearerToken();
      if (token) headers["Authorization"] = `Bearer ${token}`;
    } else if (this._auth) {
      headers["Authorization"] = `Basic ${Buffer.from(`${this._auth.user}:${this._auth.password}`).toString("base64")}`;
    }
    return headers;
  }

  async _channelBearerToken(): Promise<string | undefined> {
    if (this._getToken) return await this._getToken();
    return this._token;
  }
}

export class Session {
  readonly tenantId: TenantId;
  readonly sessionId: SessionId;
  readonly agentId?: AgentId;
  private readonly _client: Client;

  constructor(client: Client, tenantId: TenantId, sessionId: SessionId, opts: SessionOptions = {}) {
    this._client = client;
    this.tenantId = tenantId;
    this.sessionId = sessionId;
    this.agentId = opts.agentId;
  }

  async append(payload: unknown, opts: AppendOptions = {}): Promise<AppendResult> {
    if (opts.fetchLastN && opts.fetchLastN > 0) {
      return await this.appendAndFetch(payload, { ...opts, last: opts.fetchLastN });
    }
    const d = await this._client._request("POST", this.eventsPath(), this.appendBody(payload, opts), idem(opts.idempotencyKey));
    return { eventId: String(d.event_id), seq: Number(d.seq), recent: [] };
  }

  async appendAndFetch(payload: unknown, opts: AppendAndFetchOptions = {}): Promise<AppendResult> {
    const body = { ...this.appendBody(payload, opts), last: opts.last ?? 20, max_bytes: opts.maxBytes ?? 1_000_000 };
    const d = await this._client._request(
      "POST",
      `/api/tenants/${enc(this.tenantId)}/sessions/${enc(this.sessionId)}/append-and-fetch`,
      body,
      idem(opts.idempotencyKey)
    );
    return { eventId: String(d.event_id), seq: Number(d.seq), recent: Array.isArray(d.recent) ? d.recent.map(eventFromDict) : [] };
  }

  async fetch(opts: { fromSeq?: number; limit?: number } = {}): Promise<Event[]> {
    const url = new URL(`${this._client.baseUrl}${this.eventsPath()}`);
    url.searchParams.set("from_seq", String(opts.fromSeq ?? 0));
    url.searchParams.set("limit", String(opts.limit ?? 100));
    const data = await (this._client as unknown as { _requestAbsolute(method: string, url: URL): Promise<unknown> })._requestAbsolute("GET", url);
    return Array.isArray(data) ? data.map(eventFromDict) : [];
  }

  async fetchRecent(n: number): Promise<{ recent: Event[] }> {
    const all = await this.fetch({ fromSeq: 0, limit: 1000 });
    return { recent: n <= 0 ? [] : all.slice(-n) };
  }

  async replay(opts: { fromSeq?: number; limit?: number } = {}): Promise<Event[]> {
    const url = new URL(`${this._client.baseUrl}/api/tenants/${enc(this.tenantId)}/sessions/${enc(this.sessionId)}/replay`);
    if (opts.fromSeq !== undefined) url.searchParams.set("from_seq", String(opts.fromSeq));
    url.searchParams.set("limit", String(opts.limit ?? 100));
    const data = await (this._client as unknown as { _requestAbsolute(method: string, url: URL): Promise<Record<string, unknown>> })._requestAbsolute("GET", url);
    return Array.isArray(data.events) ? data.events.map(eventFromDict) : [];
  }

  async fork(opts: { atSeq: number; alias?: string | null } | string): Promise<Session> {
    const body = typeof opts === "string"
      ? { at_seq: 0, alias: opts }
      : { at_seq: opts.atSeq, alias: opts.alias ?? null };
    const d = await this._client._request("POST", `/api/tenants/${enc(this.tenantId)}/sessions/${enc(this.sessionId)}/fork`, {
      ...body,
    });
    return new Session(this._client, this.tenantId, String(d.session_id), { agentId: String(d.agent_id) });
  }

  async *subscribe(opts: SubscribeOptions = {}): AsyncIterable<Event> {
    const url = new URL(`${this._client.baseUrl}/api/tenants/${enc(this.tenantId)}/sessions/${enc(this.sessionId)}/subscribe`);
    url.searchParams.set("from_seq", String(opts.fromSeq ?? 0));
    url.searchParams.set("limit", String(opts.limit ?? 100));
    const resp = await this._client._fetchUrl(url, { headers: { Accept: "text/event-stream" } });
    if (!resp.ok) throw await errorFrom(resp);
    if (!resp.body) throw new AgentDBError("subscribe response has no body", { statusCode: resp.status });
    const reader = resp.body.getReader();
    try {
      for await (const frame of parseSse(reader)) {
        if (frame.event === "error") throw new SubscriptionClosed("error");
        if (frame.data) yield eventFromDict(JSON.parse(frame.data));
      }
    } finally {
      try { reader.releaseLock(); } catch {}
    }
  }

  async context(opts: { maxEvents?: number; maxBytes?: number; include?: string[]; recordQuery?: Record<string, unknown> } = {}): Promise<Record<string, unknown>> {
    return await this._client._request("POST", `/api/tenants/${enc(this.tenantId)}/sessions/${enc(this.sessionId)}/context`, {
      max_events: opts.maxEvents ?? 20,
      max_bytes: opts.maxBytes ?? 1_000_000,
      include: opts.include ?? ["recent_events"],
      record_query: opts.recordQuery,
    });
  }

  private eventsPath(): string {
    return `/api/tenants/${enc(this.tenantId)}/sessions/${enc(this.sessionId)}/events`;
  }

  private appendBody(payload: unknown, opts: AppendOptions): Record<string, unknown> {
    const agentId = opts.agentId ?? this.agentId;
    if (!agentId) throw new Error("agentId is required for append");
    return {
      agent_id: agentId,
      kind: opts.kind ?? "message",
      payload,
      artifact_refs: opts.artifactRefs ?? [],
      source_refs: opts.sourceRefs ?? [],
    };
  }
}

function eventFromDict(d: unknown): Event {
  const rec = d as Record<string, unknown>;
  return {
    tenantId: String(rec.tenant_id),
    agentId: String(rec.agent_id),
    sessionId: String(rec.session_id),
    session: String(rec.session_id),
    eventId: String(rec.event_id),
    seq: Number(rec.seq),
    kind: String(rec.kind),
    payload: rec.payload,
    createdAtMs: Number(rec.created_at_ms ?? 0),
  };
}

function watchItemFromDict(d: unknown): WatchItem {
  const rec = d as Record<string, unknown>;
  return { cursor: String(rec.cursor), event: eventFromDict(rec.event) };
}

function enc(s: string): string {
  return encodeURIComponent(s).replace(/['()*]/g, c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

function idem(key?: string): Record<string, string> {
  return { "Idempotency-Key": key ?? randomUUID() };
}

async function errorFrom(resp: Response): Promise<AgentDBError> {
  const text = await resp.text();
  let body: unknown = text;
  try { body = text ? JSON.parse(text) : null; } catch {}
  return fromResponse(resp.status, body);
}
