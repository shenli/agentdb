/**
 * AgentDB TypeScript / Node client.
 *
 * Basic usage:
 *
 *   import { AgentDB } from "agentdb";
 *
 *   const db = new AgentDB({
 *     baseUrl: "http://127.0.0.1:7878",
 *     token: process.env.AGENTDB_API_KEY,
 *   });
 *   const workspace = await db.workspace();
 *   const agent = await workspace.agent("support-agent");
 *   const session = await agent.session("ticket-42");
 *   const result = await session.appendUser("hello", { fetchLastN: 5 });
 *   console.log(result.seq, result.recent.map(e => e.payload));
 *
 * `Client` remains available for low-level tenant-scoped calls.
 *
 * See `examples/agents/typescript/quickstart.ts` for a working
 * agent loop. The `append + fetchLastN` pattern is the canonical
 * hot path — write one event, read the last N back in the same
 * round trip.
 */

export {
  AgentDB,
  AgentHandle,
  ArtifactCollection,
  Client,
  RecordCollection,
  Session,
  SessionHandle,
  Workspace,
  appendResultRef,
  artifactRef,
  eventRef,
} from "./client.js";
export {
  SessionEventKind,
  ADAPTER_READINESS_CHECKS,
  agentStatus,
  approvalRequested,
  approvalResult,
  artifactCreated,
  contextPackBuilt,
  contextSourceRefs,
  modelMessage,
  modelThought,
  toolCall,
  toolError,
  toolResult,
  userMessage,
  validateAdapterEvents,
} from "./conventions.js";
export type {
  AdapterReadinessCheck,
  Payload,
  SessionEventKind as SessionEventKindValue,
} from "./conventions.js";
export type {
  AppendOptions,
  AppendAndFetchOptions,
  AppendResult,
  ContextPackBuiltOptions,
  ClientOptions,
  ClientSubscribeOptions,
  Event,
  ModelMessageOptions,
  SessionOptions,
  ToolCallOptions,
  ToolResultOptions,
  TenantId,
  AgentId,
  SessionId,
  UserInfo,
  UserMessageOptions,
  EventId,
  ArtifactId,
  RecordId,
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
  fromResponse,
} from "./errors.js";
export type { ServerErrorBody, AgentDBErrorOptions } from "./errors.js";
