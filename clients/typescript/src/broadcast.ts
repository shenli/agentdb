/**
 * Ephemeral broadcast message type + envelope parser.
 *
 * Ephemeral pub/sub messages — sent via `Client.broadcast()` and
 * streamed back via `Client.subscribeChannel()`. Distinct from the
 * durable [`Event`] type returned by `append_and_fetch` /
 * per-session `subscribe`: broadcast has no `seq` and no replay; the
 * server fans out to current subscribers and forgets.
 *
 * Wire envelope (spec § 5.2):
 *
 *     {"tenant": "...", "channel": "...", "ts_ms": 0,
 *      "sender": "...", "payload": <any JSON>}
 *
 * `sender` is the publisher's JWT `sub` claim, or `undefined` when
 * the publisher used Basic auth (which has no per-user identity).
 */

export interface BroadcastMessage {
  tenant: string;
  channel: string;
  ts_ms: number;
  /** Set on JWT-authenticated publishes; absent for Basic-auth. */
  sender?: string;
  /** Caller-supplied JSON payload, passed through unchanged. */
  payload: unknown;
}

/**
 * Parse one wire envelope (the `data:` body of an SSE frame from
 * `/channels/:c/subscribe`) into a `BroadcastMessage`.
 *
 * Exposed for unit tests and for callers that drive the SSE stream
 * themselves; production code goes through `Client.subscribeChannel`.
 */
export function parseBroadcastEnvelope(json: string): BroadcastMessage {
  const env = JSON.parse(json) as Record<string, unknown>;
  const msg: BroadcastMessage = {
    tenant: String(env.tenant),
    channel: String(env.channel),
    ts_ms: Number(env.ts_ms),
    payload: env.payload,
  };
  if (env.sender !== undefined && env.sender !== null) {
    msg.sender = String(env.sender);
  }
  return msg;
}
