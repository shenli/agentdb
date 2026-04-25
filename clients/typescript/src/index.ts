/**
 * AgentDB TypeScript / Node client.
 *
 * Typical usage (dev mode, no auth):
 *
 *   import { Client } from "agentdb";
 *
 *   const client = new Client("http://127.0.0.1:7878");
 *   const session = client.session("agent-1", { tenant: "demo" });
 *   const result = await session.append(
 *     { role: "user", text: "hello" },
 *     { fetchLastN: 5 }
 *   );
 *   console.log(result.seq, result.recent.map(e => e.payload));
 *
 * In auth mode the tenant is implicit (derived from credentials):
 *
 *   const client = new Client("https://prod.example.com", {
 *     user: "alice",
 *     password: "hunter2",
 *   });
 *   const session = client.session("agent-1");  // tenant from auth
 *   await session.append({ role: "tool", text: "did a thing" });
 *
 * See `examples/agents/typescript/quickstart.ts` for a working
 * agent loop. The `append + fetchLastN` pattern is the canonical
 * hot path — write one event, read the last N back in the same
 * round trip. For pure reads, point a MongoDB driver at the
 * wire port (default :7879).
 */

export { Client, Session } from "./client.js";
export type {
  AppendOptions,
  AppendResult,
  ClientOptions,
  Event,
  SessionOptions,
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
