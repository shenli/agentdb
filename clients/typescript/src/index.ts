/**
 * AgentDB TypeScript / Node client.
 *
 * Basic usage:
 *
 *   import { Client } from "agentdb";
 *
 *   const client = new Client({
 *     baseUrl: "http://127.0.0.1:7878",
 *     auth: { user: "alice", password: "hunter2" },
 *   });
 *   const session = client.session("agent-1");
 *   const result = await session.append(
 *     { role: "user", text: "hello" },
 *     { fetchLastN: 5 }
 *   );
 *   console.log(result.seq, result.recent.map(e => e.payload));
 *
 * Tenant is derived from the bearer token / Basic-auth user record
 * server-side — no `tenant` param on the client or per-method calls.
 *
 * See `examples/agents/typescript/quickstart.ts` for a working
 * agent loop. The `append + fetchLastN` pattern is the canonical
 * hot path — write one event, read the last N back in the same
 * round trip.
 */

export { Client, Session } from "./client.js";
export type {
  AppendOptions,
  AppendAndFetchOptions,
  AppendResult,
  ClientOptions,
  ClientSubscribeOptions,
  Event,
  SessionOptions,
  SubscribeOptions,
  TenantId,
  AgentId,
  SessionId,
  EventId,
  ArtifactId,
  RecordId,
  SkillId,
  WatchItem,
} from "./client.js";

export {
  AgentDBError,
  BadRequest,
  Unauthorized,
  Forbidden,
  NotFound,
  Conflict,
  Misdirected,
  RateLimited,
  ServerError,
  SubscriptionClosed,
  fromResponse,
} from "./errors.js";
export type { ServerErrorBody, AgentDBErrorOptions } from "./errors.js";

export { parseBlock, parseSse } from "./sse.js";
export type { SseFrame } from "./sse.js";
