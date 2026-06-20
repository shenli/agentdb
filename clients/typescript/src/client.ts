import { AgentDBError, SubscriptionClosed, fromResponse } from "./errors.js";
import { parseSse } from "./sse.js";
import {
  SessionEventKind,
  agentStatus,
  contextPackBuilt,
  modelMessage,
  toolCall,
  toolResult,
  userMessage,
} from "./conventions.js";
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
  runtimeVersion?: string;
  artifactRefs: Record<string, unknown>[];
  sourceRefs: Record<string, unknown>[];
  credentialRefs: Record<string, unknown>[];
  metadataKeyId?: string;
  metadataKeyVersion?: string;
  createdAtMs: number;
}

export interface AppendResult {
  eventId: EventId;
  seq: number;
  recent: Event[];
  ref(): Record<string, unknown>;
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
  credentialRefs?: unknown[];
  idempotencyKey?: string;
  fetchLastN?: number;
  actorId?: number;
}

export interface AppendAndFetchOptions extends AppendOptions {
  last?: number;
  maxBytes?: number;
}

export interface UserMessageOptions {
  role?: string;
  userId?: string;
  channel?: string;
  metadata?: Record<string, unknown>;
  fetchLastN?: number;
  idempotencyKey?: string;
}

export interface ModelMessageOptions {
  sourceRefs?: Record<string, unknown>[];
  idempotencyKey?: string;
  role?: string;
  model?: string;
  provider?: string;
  finishReason?: string;
  usage?: Record<string, unknown>;
  contextPackId?: string;
  metadata?: Record<string, unknown>;
}

export interface ToolCallOptions {
  sourceRefs?: Record<string, unknown>[];
  credentialRefs?: Record<string, unknown>[];
  idempotencyKey?: string;
  handId?: string;
  timeoutMs?: number;
  metadata?: Record<string, unknown>;
}

export interface ToolResultOptions {
  artifacts?: Record<string, unknown>[];
  sourceRefs?: Record<string, unknown>[];
  idempotencyKey?: string;
  status?: string;
  output?: Record<string, unknown>;
  durationMs?: number;
  exitCode?: number;
  metadata?: Record<string, unknown>;
}

export interface ContextPackBuiltOptions {
  sourceRefs?: Record<string, unknown>[];
  idempotencyKey?: string;
  maxEvents?: number;
  maxBytes?: number;
  tokenBudget?: number;
  selectedEventCount?: number;
  selectedRecordCount?: number;
  promptCacheKey?: string;
  metadata?: Record<string, unknown>;
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

export interface UserInfo {
  tenant_id?: string;
  default_workspace?: string;
  workspace?: Record<string, unknown>;
  workspaces?: Record<string, unknown>[];
  [key: string]: unknown;
}

const DEFAULT_TIMEOUT_MS = 30_000;
const RECORD_COMMIT_SEQ_HEADER = "x-agentdb-record-commit-seq";

export class AgentDB {
  readonly client: Client;

  constructor(opts: ClientOptions | { client: Client }) {
    this.client = "client" in opts ? opts.client : new Client(opts);
  }

  async healthReady(): Promise<boolean> {
    return await this.client.healthReady();
  }

  async workspace(
    name?: string,
    opts: { tenantId?: TenantId; policy?: Record<string, unknown> } = {}
  ): Promise<Workspace> {
    if (opts.tenantId !== undefined) return new Workspace(this.client, opts.tenantId, { name });
    const resolved = await this.resolveTenant(name);
    if (resolved !== undefined) {
      return new Workspace(this.client, resolved.tenantId, {
        name: resolved.name ?? name,
        data: resolved.data,
      });
    }
    const externalKey = name ?? "default";
    for (const tenant of await this.client.listTenants()) {
      if (tenant.external_key === externalKey) {
        return new Workspace(this.client, String(tenant.tenant_id), { name: externalKey, data: tenant });
      }
    }
    const tenant = await this.client.createTenant(externalKey, opts.policy ?? {});
    return new Workspace(this.client, String(tenant.tenant_id), { name: externalKey, data: tenant });
  }

  async session(
    sessionId: SessionId,
    opts: { workspace?: string; tenantId?: TenantId; agentId?: AgentId } = {}
  ): Promise<SessionHandle> {
    if (opts.tenantId !== undefined) {
      return new Workspace(this.client, opts.tenantId).sessionById(sessionId, { agentId: opts.agentId });
    }
    return (await this.workspace(opts.workspace)).sessionById(sessionId, { agentId: opts.agentId });
  }

  sessionById(
    sessionId: SessionId,
    opts: { tenantId: TenantId; agentId?: AgentId }
  ): SessionHandle {
    return new Workspace(this.client, opts.tenantId).sessionById(sessionId, { agentId: opts.agentId });
  }

  private async resolveTenant(name?: string): Promise<{ tenantId: TenantId; name?: string; data: Record<string, unknown> } | undefined> {
    let info: UserInfo | undefined;
    try {
      info = await this.client.userInfo();
    } catch (err) {
      if (!(err instanceof AgentDBError) || err.statusCode !== 404) throw err;
      return undefined;
    }
    const resolved = tenantFromUserInfo(info, name);
    if (resolved === undefined) {
      throw new Error("authenticated user-info did not include an allowed workspace");
    }
    return resolved;
  }
}

export class Workspace {
  readonly artifacts: ArtifactCollection;
  readonly records: RecordCollection;
  readonly skills: SkillCollection;
  readonly data: Record<string, unknown>;

  constructor(
    private readonly client: Client,
    readonly id: TenantId,
    opts: { name?: string; data?: Record<string, unknown> } = {}
  ) {
    this.data = opts.data ?? { tenant_id: id };
    this.artifacts = new ArtifactCollection(client, id);
    this.records = new RecordCollection(client, id);
    this.skills = new SkillCollection(client, id);
  }

  get tenantId(): TenantId {
    return this.id;
  }

  async agent(
    alias?: string,
    opts: { agentId?: AgentId; config?: Record<string, unknown> } = {}
  ): Promise<AgentHandle> {
    if (opts.agentId !== undefined) return new AgentHandle(this, opts.agentId, { alias });
    if (alias !== undefined) {
      const existing = await findAgentByAlias(this.client, this.id, alias);
      if (existing !== undefined) {
        return new AgentHandle(this, String(existing.agent_id), { alias, data: existing });
      }
    }
    let data: Record<string, unknown>;
    try {
      data = await this.client.createAgent(this.id, { alias, config: opts.config });
    } catch (err) {
      if (alias === undefined || !(err instanceof AgentDBError) || err.statusCode !== 409) throw err;
      const existing = await findAgentByAlias(this.client, this.id, alias);
      if (existing === undefined) throw err;
      data = existing;
    }
    return new AgentHandle(this, String(data.agent_id), { alias, data });
  }

  agentById(agentId: AgentId, opts: { alias?: string } = {}): AgentHandle {
    return new AgentHandle(this, agentId, { alias: opts.alias });
  }

  sessionById(sessionId: SessionId, opts: { agentId?: AgentId } = {}): SessionHandle {
    return new SessionHandle(
      this,
      this.client.session(this.id, sessionId, { agentId: opts.agentId })
    );
  }

  async watch(opts: ClientSubscribeOptions = {}): Promise<WatchItem[]> {
    return await this.client.watch(this.id, opts);
  }
}

export class AgentHandle {
  readonly data: Record<string, unknown>;

  constructor(
    readonly workspace: Workspace,
    readonly id: AgentId,
    opts: { alias?: string; data?: Record<string, unknown> } = {}
  ) {
    this.data = opts.data ?? { agent_id: id };
  }

  get agentId(): AgentId {
    return this.id;
  }

  async session(alias?: string, opts: { sessionId?: SessionId } = {}): Promise<SessionHandle> {
    if (opts.sessionId !== undefined) return this.sessionById(opts.sessionId);
    if (alias !== undefined) {
      const existing = await findSessionByAlias(this.workspaceClient(), this.workspace.id, alias);
      if (existing !== undefined) {
        return this.sessionById(String(existing.session_id));
      }
    }
    let session: Session;
    try {
      session = await this.workspaceClient().createSession(this.workspace.id, this.id, { alias });
    } catch (err) {
      if (alias === undefined || !(err instanceof AgentDBError) || err.statusCode !== 409) throw err;
      const existing = await findSessionByAlias(this.workspaceClient(), this.workspace.id, alias);
      if (existing === undefined) throw err;
      session = this.workspaceClient().session(this.workspace.id, String(existing.session_id), { agentId: this.id });
    }
    return new SessionHandle(this.workspace, session, { agent: this });
  }

  sessionById(sessionId: SessionId): SessionHandle {
    return new SessionHandle(
      this.workspace,
      this.workspaceClient().session(this.workspace.id, sessionId, { agentId: this.id }),
      { agent: this }
    );
  }

  async bindSkill(skillId: SkillId, opts: { idempotencyKey?: string } = {}): Promise<Record<string, unknown>> {
    return await this.workspaceClient().bindSkill(this.workspace.id, this.id, skillId, opts);
  }

  private workspaceClient(): Client {
    return (this.workspace as unknown as { client: Client }).client;
  }
}

export class SessionHandle {
  readonly artifacts: ArtifactCollection;
  readonly records: RecordCollection;
  readonly skills: SkillCollection;

  constructor(
    readonly workspace: Workspace,
    private readonly session: Session,
    readonly opts: { agent?: AgentHandle } = {}
  ) {
    this.artifacts = workspace.artifacts;
    this.records = workspace.records;
    this.skills = workspace.skills;
  }

  get id(): SessionId {
    return this.session.sessionId;
  }

  get sessionId(): SessionId {
    return this.session.sessionId;
  }

  get agentId(): AgentId | undefined {
    return this.session.agentId;
  }

  async append(payload: unknown, opts: AppendOptions = {}): Promise<AppendResult> {
    return await this.session.append(payload, opts);
  }

  async appendAndFetch(payload: unknown, opts: AppendAndFetchOptions = {}): Promise<AppendResult> {
    return await this.session.appendAndFetch(payload, opts);
  }

  async appendUser(text: string, opts: UserMessageOptions = {}): Promise<AppendResult> {
    return await this.session.appendUser(text, opts);
  }

  async modelMessage(text: string, opts: ModelMessageOptions = {}): Promise<AppendResult> {
    return await this.session.modelMessage(text, opts);
  }

  async toolCall(
    toolCallId: string,
    tool: string,
    input: Record<string, unknown>,
    opts: ToolCallOptions = {}
  ): Promise<AppendResult> {
    return await this.session.toolCall(toolCallId, tool, input, opts);
  }

  async toolResult(
    toolCallId: string,
    summary: string,
    opts: ToolResultOptions = {}
  ): Promise<AppendResult> {
    return await this.session.toolResult(toolCallId, summary, opts);
  }

  async agentStatus(status: string, opts: { summary?: string; lastSeenSeq?: number; reason?: string; metadata?: Record<string, unknown> } = {}): Promise<AppendResult> {
    return await this.session.agentStatus(status, opts);
  }

  async contextPackBuilt(
    contextPackId: string,
    include: string[],
    opts: ContextPackBuiltOptions = {}
  ): Promise<AppendResult> {
    return await this.session.contextPackBuilt(contextPackId, include, opts);
  }

  async fetch(opts: { fromSeq?: number; limit?: number } = {}): Promise<Event[]> {
    return await this.session.fetch(opts);
  }

  async fetchRecent(n: number): Promise<{ recent: Event[] }> {
    return await this.session.fetchRecent(n);
  }

  async replay(opts: { fromSeq?: number; limit?: number } = {}): Promise<Event[]> {
    return await this.session.replay(opts);
  }

  async fork(opts: { atSeq: number; alias?: string | null } | string): Promise<SessionHandle> {
    return new SessionHandle(this.workspace, await this.session.fork(opts), this.opts);
  }

  subscribe(opts: SubscribeOptions = {}): AsyncIterable<Event> {
    return this.session.subscribe(opts);
  }

  async context(opts: { maxEvents?: number; maxBytes?: number; include?: string[]; recordQuery?: Record<string, unknown> } = {}): Promise<Record<string, unknown>> {
    return await this.session.context(opts);
  }

  raw(): Session {
    return this.session;
  }
}

export class ArtifactCollection {
  constructor(private readonly client: Client, private readonly tenantId: TenantId) {}

  async put(data: BodyInit, opts: { contentType?: string; sha256?: string } = {}): Promise<Record<string, unknown>> {
    return await this.client.putArtifact(this.tenantId, data, opts);
  }

  async get(artifactId: ArtifactId): Promise<Record<string, unknown>> {
    return await this.client.getArtifact(this.tenantId, artifactId);
  }

  async blob(artifactId: ArtifactId): Promise<ArrayBuffer> {
    return await this.client.getArtifactBlob(this.tenantId, artifactId);
  }
}

export class RecordCollection {
  constructor(private readonly client: Client, private readonly tenantId: TenantId) {}

  async write(body: Record<string, unknown>): Promise<Record<string, unknown>> {
    return await this.client.writeRecord(this.tenantId, body);
  }

  async search(body: Record<string, unknown>): Promise<unknown[]> {
    return await this.client.searchRecords(this.tenantId, body);
  }
}

export class SkillCollection {
  constructor(private readonly client: Client, private readonly tenantId: TenantId) {}

  async register(body: { name: string; version: string; manifest?: Record<string, unknown>; artifactRefs?: unknown[]; idempotencyKey?: string }): Promise<Record<string, unknown>> {
    return await this.client.registerSkill(this.tenantId, body);
  }
}

export class Client {
  readonly baseUrl: string;
  readonly timeoutMs: number;
  private readonly _auth?: { user: string; password: string };
  private readonly _fetch: typeof globalThis.fetch;
  private readonly _token?: string;
  private readonly _getToken?: () => string | Promise<string>;
  private readonly _recordCommitSeqByTenant = new Map<TenantId, number>();

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
    return this._auth !== undefined || this._token !== undefined || this._getToken !== undefined;
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

  async userInfo(): Promise<UserInfo> {
    return await this._request("GET", "/api/me") as UserInfo;
  }

  async listTenants(): Promise<Record<string, unknown>[]> {
    const data = await this._request("GET", "/api/tenants");
    return Array.isArray(data.tenants) ? data.tenants as Record<string, unknown>[] : [];
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

  async listAgents(tenantId: TenantId): Promise<Record<string, unknown>[]> {
    const data = await this._request("GET", `/api/tenants/${enc(tenantId)}/agents`);
    return Array.isArray(data.agents) ? data.agents as Record<string, unknown>[] : [];
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

  async listSessions(tenantId: TenantId): Promise<Record<string, unknown>[]> {
    const data = await this._request("GET", `/api/tenants/${enc(tenantId)}/sessions`);
    return Array.isArray(data.sessions) ? data.sessions as Record<string, unknown>[] : [];
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
    const resp = await this._rawFetch("POST", `/api/tenants/${enc(tenantId)}/records`, body);
    if (!resp.ok) throw await errorFrom(resp);
    this._captureRecordCommitSeq(tenantId, resp);
    return await this._decodeObjectResponse(resp);
  }

  async searchRecords(tenantId: TenantId, body: Record<string, unknown>): Promise<unknown[]> {
    const data = await this._request(
      "POST",
      `/api/tenants/${enc(tenantId)}/records/search`,
      this._withRecordCommitSeq(tenantId, body)
    );
    return Array.isArray(data.matches) ? data.matches : [];
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
    throw new Error("tenant-wide streaming requires watch(tenantId, ...)");
  }

  async _request(method: string, path: string, json?: unknown, headers: Record<string, string> = {}): Promise<Record<string, unknown>> {
    const resp = await this._rawFetch(method, path, json, headers);
    if (!resp.ok) throw await errorFrom(resp);
    return await this._decodeObjectResponse(resp);
  }

  private async _decodeObjectResponse(resp: Response): Promise<Record<string, unknown>> {
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
    const resp = await this._rawFetch(method, path, undefined, init.headers ?? {}, init.body);
    if (!resp.ok) throw await errorFrom(resp);
    return JSON.parse(await resp.text()) as Record<string, unknown>;
  }

  async _rawFetch(
    method: string,
    path: string,
    json?: unknown,
    headers: Record<string, string> = {},
    rawBody?: BodyInit
  ): Promise<Response> {
    const hdrs = { ...(await this._authHeaders()), ...headers };
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
    return await this._fetch(input, {
      ...init,
      headers: { ...(await this._authHeaders()), ...headersRecord(init.headers) },
    });
  }

  private async _authHeaders(): Promise<Record<string, string>> {
    const headers: Record<string, string> = {};
    const token = await this._bearerToken();
    if (token !== undefined) {
      headers["Authorization"] = `Bearer ${token}`;
    } else if (this._auth) {
      headers["Authorization"] = `Basic ${Buffer.from(`${this._auth.user}:${this._auth.password}`).toString("base64")}`;
    }
    return headers;
  }

  private async _bearerToken(): Promise<string | undefined> {
    if (this._getToken !== undefined) return await this._getToken();
    return this._token;
  }

  private _captureRecordCommitSeq(tenantId: TenantId, resp: Response): void {
    const raw = resp.headers.get(RECORD_COMMIT_SEQ_HEADER);
    if (!raw) return;
    const seq = Number(raw);
    if (Number.isSafeInteger(seq) && seq > 0) {
      const existing = this._recordCommitSeqByTenant.get(tenantId) ?? 0;
      this._recordCommitSeqByTenant.set(tenantId, Math.max(existing, seq));
    }
  }

  private _withRecordCommitSeq(tenantId: TenantId, body: Record<string, unknown>): Record<string, unknown> {
    const seq = this._recordCommitSeqByTenant.get(tenantId);
    if (seq === undefined || body.min_visible_record_commit_seq !== undefined) return body;
    return { ...body, min_visible_record_commit_seq: seq };
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
    return appendResultFromDict({ ...d, recent: [] });
  }

  async appendAndFetch(payload: unknown, opts: AppendAndFetchOptions = {}): Promise<AppendResult> {
    const body = { ...this.appendBody(payload, opts), last: opts.last ?? 20, max_bytes: opts.maxBytes ?? 1_000_000 };
    const d = await this._client._request(
      "POST",
      `/api/tenants/${enc(this.tenantId)}/sessions/${enc(this.sessionId)}/append-and-fetch`,
      body,
      idem(opts.idempotencyKey)
    );
    return appendResultFromDict(d);
  }

  async appendUser(text: string, opts: UserMessageOptions = {}): Promise<AppendResult> {
    return await this.append(
      userMessage(text, {
        role: opts.role,
        userId: opts.userId,
        channel: opts.channel,
        metadata: opts.metadata,
      }),
      {
        kind: SessionEventKind.UserMessage,
        fetchLastN: opts.fetchLastN,
        idempotencyKey: opts.idempotencyKey,
      }
    );
  }

  async modelMessage(text: string, opts: ModelMessageOptions = {}): Promise<AppendResult> {
    return await this.append(
      modelMessage(text, {
        role: opts.role,
        model: opts.model,
        provider: opts.provider,
        finishReason: opts.finishReason,
        usage: opts.usage,
        contextPackId: opts.contextPackId,
        metadata: opts.metadata,
      }),
      {
        kind: SessionEventKind.ModelMessage,
        sourceRefs: opts.sourceRefs,
        idempotencyKey: opts.idempotencyKey,
      }
    );
  }

  async toolCall(
    toolCallId: string,
    tool: string,
    input: Record<string, unknown>,
    opts: ToolCallOptions = {}
  ): Promise<AppendResult> {
    return await this.append(
      toolCall(toolCallId, tool, input, {
        handId: opts.handId,
        timeoutMs: opts.timeoutMs,
        idempotencyKey: opts.idempotencyKey,
        metadata: opts.metadata,
      }),
      {
        kind: SessionEventKind.ToolCall,
        sourceRefs: opts.sourceRefs,
        credentialRefs: opts.credentialRefs,
        idempotencyKey: opts.idempotencyKey,
      }
    );
  }

  async toolResult(
    toolCallId: string,
    summary: string,
    opts: ToolResultOptions = {}
  ): Promise<AppendResult> {
    return await this.append(
      toolResult(toolCallId, summary, {
        status: opts.status,
        output: opts.output,
        durationMs: opts.durationMs,
        exitCode: opts.exitCode,
        metadata: opts.metadata,
      }),
      {
        kind: SessionEventKind.ToolResult,
        artifactRefs: (opts.artifacts ?? []).map(artifactRef),
        sourceRefs: opts.sourceRefs,
        idempotencyKey: opts.idempotencyKey,
      }
    );
  }

  async agentStatus(
    status: string,
    opts: { summary?: string; lastSeenSeq?: number; reason?: string; metadata?: Record<string, unknown> } = {}
  ): Promise<AppendResult> {
    return await this.append(agentStatus(status, opts), { kind: SessionEventKind.AgentStatus });
  }

  async contextPackBuilt(
    contextPackId: string,
    include: string[],
    opts: ContextPackBuiltOptions = {}
  ): Promise<AppendResult> {
    return await this.append(
      contextPackBuilt(contextPackId, include, {
        maxEvents: opts.maxEvents,
        maxBytes: opts.maxBytes,
        tokenBudget: opts.tokenBudget,
        selectedEventCount: opts.selectedEventCount,
        selectedRecordCount: opts.selectedRecordCount,
        promptCacheKey: opts.promptCacheKey,
        metadata: opts.metadata,
      }),
      {
        kind: SessionEventKind.ContextPackBuilt,
        sourceRefs: opts.sourceRefs,
        idempotencyKey: opts.idempotencyKey,
      }
    );
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
    validateNoRawSecretFields(payload);
    return {
      agent_id: agentId,
      kind: opts.kind ?? "message",
      payload,
      artifact_refs: opts.artifactRefs ?? [],
      source_refs: opts.sourceRefs ?? [],
      credential_refs: opts.credentialRefs ?? [],
    };
  }
}

const RAW_SECRET_FIELD_NAMES = new Set([
  "accesstoken",
  "apikey",
  "authtoken",
  "authorization",
  "bearertoken",
  "clientsecret",
  "openaiapikey",
  "password",
  "passwd",
  "personalaccesstoken",
  "privatekey",
  "pwd",
  "refreshtoken",
  "secret",
  "secretaccesskey",
  "token",
]);

function validateNoRawSecretFields(payload: unknown): void {
  const path = firstRawSecretFieldPath(payload, [], new WeakSet<object>());
  if (path !== undefined) {
    throw new Error(
      `append payload field \`${path}\` looks like a raw secret; use credentialRefs for opaque vault / broker references`
    );
  }
}

function firstRawSecretFieldPath(value: unknown, path: string[], seen: WeakSet<object>): string | undefined {
  if (Array.isArray(value)) {
    if (seen.has(value)) return undefined;
    seen.add(value);
    for (let idx = 0; idx < value.length; idx += 1) {
      path.push(`[${idx}]`);
      const found = firstRawSecretFieldPath(value[idx], path, seen);
      if (found !== undefined) return found;
      path.pop();
    }
    return undefined;
  }
  if (value === null || typeof value !== "object") return undefined;
  if (seen.has(value)) return undefined;
  seen.add(value);
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    path.push(key);
    if (isRawSecretFieldName(key)) return formatSecretPath(path);
    const found = firstRawSecretFieldPath(child, path, seen);
    if (found !== undefined) return found;
    path.pop();
  }
  return undefined;
}

function formatSecretPath(path: string[]): string {
  let out = "";
  for (const part of path) {
    if (part.startsWith("[")) out += part;
    else if (out) out += `.${part}`;
    else out = part;
  }
  return out;
}

function isRawSecretFieldName(key: string): boolean {
  const normalized = key.replace(/[^A-Za-z0-9]/g, "").toLowerCase();
  return RAW_SECRET_FIELD_NAMES.has(normalized);
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
    runtimeVersion: stringOrUndefined(rec.runtime_version),
    artifactRefs: arrayRecords(rec.artifact_refs),
    sourceRefs: arrayRecords(rec.source_refs),
    credentialRefs: arrayRecords(rec.credential_refs),
    metadataKeyId: stringOrUndefined(rec.metadata_key_id),
    metadataKeyVersion: stringOrUndefined(rec.metadata_key_version),
    createdAtMs: Number(rec.created_at_ms ?? 0),
  };
}

function appendResultFromDict(d: Record<string, unknown>): AppendResult {
  const eventId = String(d.event_id);
  return {
    eventId,
    seq: Number(d.seq),
    recent: Array.isArray(d.recent) ? d.recent.map(eventFromDict) : [],
    ref: () => ({ event_id: eventId }),
  };
}

export function eventRef(event: { ref?: () => Record<string, unknown>; eventId?: string; event_id?: string }): Record<string, unknown> {
  if (typeof event.ref === "function") return event.ref();
  return { event_id: event.event_id ?? event.eventId };
}

export function appendResultRef(result: AppendResult): Record<string, unknown> {
  return result.ref();
}

export function artifactRef(artifact: Record<string, unknown>): Record<string, unknown> {
  const ref: Record<string, unknown> = { artifact_id: artifact.artifact_id ?? artifact.artifactId };
  for (const [from, to] of [
    ["content_type", "content_type"],
    ["contentType", "content_type"],
    ["bytes", "bytes"],
    ["sha256", "sha256"],
  ] as const) {
    if (artifact[from] !== undefined) ref[to] = artifact[from];
  }
  return ref;
}

function arrayRecords(value: unknown): Record<string, unknown>[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is Record<string, unknown> => item !== null && typeof item === "object" && !Array.isArray(item));
}

function stringOrUndefined(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function watchItemFromDict(d: unknown): WatchItem {
  const rec = d as Record<string, unknown>;
  return { cursor: String(rec.cursor), event: eventFromDict(rec.event) };
}

function tenantFromUserInfo(
  info: UserInfo,
  workspace?: string
): { tenantId: TenantId; name?: string; data: Record<string, unknown> } | undefined {
  if (workspace === undefined && typeof info.tenant_id === "string") {
    return { tenantId: info.tenant_id, data: info as Record<string, unknown> };
  }

  const workspaces = Array.isArray(info.workspaces) ? info.workspaces : [];
  const target = workspace ?? (typeof info.default_workspace === "string" ? info.default_workspace : undefined);
  if (target === undefined && workspaces.length === 1) {
    const only = workspaces[0]!;
    const tenantId = workspaceTenantId(only);
    if (tenantId !== undefined) return { tenantId, name: workspaceName(only), data: only };
  }
  if (target !== undefined) {
    for (const item of workspaces) {
      if (workspaceNames(item).has(target)) {
        const tenantId = workspaceTenantId(item);
        if (tenantId !== undefined) return { tenantId, name: workspaceName(item), data: item };
      }
    }
  }

  const current = info.workspace;
  if (current !== undefined) {
    if (workspace === undefined || workspaceNames(current).has(workspace)) {
      const tenantId = workspaceTenantId(current);
      if (tenantId !== undefined) return { tenantId, name: workspaceName(current), data: current };
    }
  }
  return undefined;
}

function workspaceTenantId(workspace: Record<string, unknown>): string | undefined {
  const value = workspace.tenant_id ?? workspace.id;
  return typeof value === "string" ? value : undefined;
}

function workspaceName(workspace: Record<string, unknown>): string | undefined {
  for (const key of ["name", "workspace", "external_key"] as const) {
    if (typeof workspace[key] === "string") return workspace[key];
  }
  return undefined;
}

function workspaceNames(workspace: Record<string, unknown>): Set<string> {
  const names = new Set<string>();
  for (const key of ["name", "workspace", "external_key"] as const) {
    if (typeof workspace[key] === "string") names.add(workspace[key]);
  }
  return names;
}

async function findAgentByAlias(client: Client, tenantId: TenantId, alias: string): Promise<Record<string, unknown> | undefined> {
  return (await client.listAgents(tenantId)).find((agent) => agent.alias === alias);
}

async function findSessionByAlias(client: Client, tenantId: TenantId, alias: string): Promise<Record<string, unknown> | undefined> {
  return (await client.listSessions(tenantId)).find((session) => session.alias === alias);
}

function headersRecord(headers: HeadersInit | undefined): Record<string, string> {
  if (headers === undefined) return {};
  if (Array.isArray(headers)) return Object.fromEntries(headers);
  if (typeof (headers as Headers).forEach === "function") {
    const out: Record<string, string> = {};
    (headers as Headers).forEach((value, key) => {
      out[key] = value;
    });
    return out;
  }
  return { ...(headers as Record<string, string>) };
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
