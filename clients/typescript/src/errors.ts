/**
 * Exception hierarchy for the AgentDB TypeScript client.
 *
 * The server emits structured error bodies of the form
 *
 *   { "error": { "code": "...", "kind": "...", "message": "..." } }
 *
 * which this module surfaces as one of the typed subclasses
 * below. Status codes are also preserved so callers can switch
 * on either the HTTP layer or the error code, whichever fits
 * better.
 */

export interface ServerErrorBody {
  code?: string;
  kind?: string;
  message?: string;
  retry_after_ms?: number;
}

export interface AgentDBErrorOptions {
  statusCode: number;
  code?: string;
  kind?: string;
  body?: unknown;
}

/** Base class for every AgentDB client-surfaced error. */
export class AgentDBError extends Error {
  readonly statusCode: number;
  readonly code?: string;
  readonly kind?: string;
  readonly body?: unknown;

  constructor(message: string, opts: AgentDBErrorOptions) {
    super(message);
    this.name = new.target.name;
    this.statusCode = opts.statusCode;
    this.code = opts.code;
    this.kind = opts.kind;
    this.body = opts.body;
  }
}

/** 400. Malformed body, missing required field, mismatch. */
export class BadRequest extends AgentDBError {}
/** 401. Auth header missing or credentials invalid. */
export class Unauthorized extends AgentDBError {}
/** 403. Authenticated user can't act on the target tenant/session. */
export class Forbidden extends AgentDBError {}
/** 404. Tenant or session doesn't exist. */
export class NotFound extends AgentDBError {}
/** 409. Resource already exists or is in a conflicting state. */
export class Conflict extends AgentDBError {}
/** 421. Routing drift on a forwarded request (multi-node operators). */
export class Misdirected extends AgentDBError {}

/** 429. Per-tenant or per-tablet quota exhausted; carries retry hint. */
export class RateLimited extends AgentDBError {
  readonly retryAfterMs?: number;
  constructor(
    message: string,
    opts: AgentDBErrorOptions & { retryAfterMs?: number }
  ) {
    super(message, opts);
    this.retryAfterMs = opts.retryAfterMs;
  }
}

/**
 * 5xx. Forwarding failed, fault injected, or node draining.
 * Almost always transient — sensible to retry once with backoff.
 */
export class ServerError extends AgentDBError {}

/**
 * Raised when the server closes a `subscribe()` stream with a
 * terminal `event: error` frame.
 *
 * The body of those frames carries `{"error": "<code>", "last_seq":
 * <number?>}`. The `errorCode` field decides whether the client
 * reconnects:
 *
 * - `jwt_expired`: when a token provider is configured the SDK
 *   transparently refreshes and reconnects with `fromSeq =
 *   lastSeq + 1`. Callers don't see this exception unless they
 *   passed neither `jwt` nor `getToken`.
 * - `lag_too_far` / `catchup_failed`: surfaced as-is. Callers can
 *   choose to reconnect from a fresher seq or give up.
 *
 * `lastSeq`, when present, is the sequence number of the last
 * event the server delivered before closing — use it to pick a
 * gap-free resume point.
 *
 * The HTTP status was 200 when the stream opened and the close is
 * mid-stream, so `statusCode` is set to 0.
 */
export class SubscriptionClosed extends AgentDBError {
  /** Server-provided machine code, e.g. `lag_too_far`. */
  readonly errorCode: string;
  /** Last delivered seq, when the server reported one. */
  readonly lastSeq?: number;

  constructor(errorCode: string, opts: { lastSeq?: number; message?: string } = {}) {
    super(opts.message ?? `subscription closed: ${errorCode}`, {
      statusCode: 0,
      code: errorCode,
      body: { error: errorCode, last_seq: opts.lastSeq },
    });
    this.errorCode = errorCode;
    this.lastSeq = opts.lastSeq;
  }
}

const CODE_FOR_STATUS: Record<number, typeof AgentDBError> = {
  400: BadRequest,
  401: Unauthorized,
  403: Forbidden,
  404: NotFound,
  409: Conflict,
  421: Misdirected,
  // 429 handled separately (retryAfterMs)
};

/** Build the right `AgentDBError` subclass for a status + body. */
export function fromResponse(statusCode: number, body: unknown): AgentDBError {
  let errBody: ServerErrorBody | undefined;
  let code: string | undefined;
  let kind: string | undefined;
  let message: string | undefined;
  let retryAfterMs: number | undefined;

  if (body && typeof body === "object" && !Array.isArray(body)) {
    const rec = body as Record<string, unknown>;
    const inner = rec.error;
    if (inner && typeof inner === "object" && !Array.isArray(inner)) {
      errBody = inner as ServerErrorBody;
      code = errBody.code;
      kind = errBody.kind;
      message = errBody.message;
      retryAfterMs = errBody.retry_after_ms;
    } else {
      const errmsg = rec.errmsg;
      message = typeof errmsg === "string" ? errmsg : JSON.stringify(rec);
    }
  } else if (body !== undefined && body !== null) {
    message = String(body);
  }
  if (!message) {
    message = `AgentDB returned HTTP ${statusCode}`;
  }

  if (statusCode === 429) {
    return new RateLimited(message, {
      statusCode,
      code,
      kind,
      body: errBody ?? body,
      retryAfterMs,
    });
  }
  if (statusCode >= 500) {
    return new ServerError(message, {
      statusCode,
      code,
      kind,
      body: errBody ?? body,
    });
  }
  const ctor = CODE_FOR_STATUS[statusCode] ?? AgentDBError;
  return new ctor(message, {
    statusCode,
    code,
    kind,
    body: errBody ?? body,
  });
}
