/**
 * Advisory V2 session event conventions.
 *
 * The runtime accepts arbitrary event kinds and payloads. These helpers provide
 * a shared vocabulary for SDK examples and framework adapters without making
 * the server enforce a closed schema.
 */

export const SessionEventKind = {
  UserMessage: "user_message",
  ModelMessage: "model_message",
  ModelThought: "model_thought",
  ToolCall: "tool_call",
  ToolResult: "tool_result",
  ToolError: "tool_error",
  ArtifactCreated: "artifact_created",
  ApprovalRequested: "approval_requested",
  ApprovalResult: "approval_result",
  ContextPackBuilt: "context_pack_built",
  AgentStatus: "agent_status",
} as const;

export type SessionEventKind = typeof SessionEventKind[keyof typeof SessionEventKind];
export type Payload = Record<string, unknown>;

export function userMessage(
  text: string,
  opts: { role?: string; userId?: string; channel?: string; metadata?: Payload } = {}
): Payload {
  return compact({
    role: opts.role ?? "user",
    text,
    user_id: opts.userId,
    channel: opts.channel,
    metadata: opts.metadata,
  });
}

export function modelMessage(
  text: string,
  opts: {
    role?: string;
    model?: string;
    provider?: string;
    finishReason?: string;
    usage?: Payload;
    contextPackId?: string;
    metadata?: Payload;
  } = {}
): Payload {
  return compact({
    role: opts.role ?? "assistant",
    text,
    model: opts.model,
    provider: opts.provider,
    finish_reason: opts.finishReason,
    usage: opts.usage,
    context_pack_id: opts.contextPackId,
    metadata: opts.metadata,
  });
}

export function modelThought(
  summary: string,
  opts: { text?: string; visibility?: string; model?: string; metadata?: Payload } = {}
): Payload {
  return compact({
    summary,
    text: opts.text,
    visibility: opts.visibility,
    model: opts.model,
    metadata: opts.metadata,
  });
}

export function toolCall(
  toolCallId: string,
  tool: string,
  input: Payload,
  opts: { handId?: string; timeoutMs?: number; idempotencyKey?: string; metadata?: Payload } = {}
): Payload {
  return compact({
    tool_call_id: toolCallId,
    tool,
    input,
    hand_id: opts.handId,
    timeout_ms: opts.timeoutMs,
    idempotency_key: opts.idempotencyKey,
    metadata: opts.metadata,
  });
}

export function toolResult(
  toolCallId: string,
  summary: string,
  opts: { status?: string; output?: Payload; durationMs?: number; exitCode?: number; metadata?: Payload } = {}
): Payload {
  return compact({
    tool_call_id: toolCallId,
    status: opts.status ?? "ok",
    summary,
    output: opts.output,
    duration_ms: opts.durationMs,
    exit_code: opts.exitCode,
    metadata: opts.metadata,
  });
}

export function toolError(
  toolCallId: string,
  summary: string,
  opts: {
    code: string;
    message: string;
    retryable: boolean;
    durationMs?: number;
    exitCode?: number;
    metadata?: Payload;
  }
): Payload {
  return compact({
    tool_call_id: toolCallId,
    retryable: opts.retryable,
    summary,
    error: { code: opts.code, message: opts.message },
    duration_ms: opts.durationMs,
    exit_code: opts.exitCode,
    metadata: opts.metadata,
  });
}

export function artifactCreated(
  artifactId: string,
  summary: string,
  opts: {
    name?: string;
    contentType?: string;
    bytes?: number;
    sha256?: string;
    purpose?: string;
    metadata?: Payload;
  } = {}
): Payload {
  return compact({
    artifact_id: artifactId,
    summary,
    name: opts.name,
    content_type: opts.contentType,
    bytes: opts.bytes,
    sha256: opts.sha256,
    purpose: opts.purpose,
    metadata: opts.metadata,
  });
}

export function approvalRequested(
  approvalId: string,
  action: string,
  summary: string,
  opts: { risk?: string; expiresAtMs?: number; metadata?: Payload } = {}
): Payload {
  return compact({
    approval_id: approvalId,
    action,
    summary,
    risk: opts.risk,
    expires_at_ms: opts.expiresAtMs,
    metadata: opts.metadata,
  });
}

export function approvalResult(
  approvalId: string,
  decision: string,
  actor: Payload,
  opts: { reason?: string; metadata?: Payload } = {}
): Payload {
  return compact({
    approval_id: approvalId,
    decision,
    actor,
    reason: opts.reason,
    metadata: opts.metadata,
  });
}

export function contextPackBuilt(
  contextPackId: string,
  include: string[],
  opts: {
    maxEvents?: number;
    maxBytes?: number;
    tokenBudget?: number;
    selectedEventCount?: number;
    selectedRecordCount?: number;
    promptCacheKey?: string;
    metadata?: Payload;
  } = {}
): Payload {
  return compact({
    context_pack_id: contextPackId,
    include,
    max_events: opts.maxEvents,
    max_bytes: opts.maxBytes,
    token_budget: opts.tokenBudget,
    selected_event_count: opts.selectedEventCount,
    selected_record_count: opts.selectedRecordCount,
    prompt_cache_key: opts.promptCacheKey,
    metadata: opts.metadata,
  });
}

export function contextSourceRefs(
  context: Payload,
  opts: { maxRefs?: number } = {}
): Payload[] {
  const refs: Payload[] = [];
  for (const event of arrayField(context.events)) {
    const eventId = field(event, "event_id", "eventId");
    if (eventId !== undefined) refs.push({ event_id: eventId });
  }
  for (const record of arrayField(context.records)) {
    const recordId = field(record, "record_id", "recordId");
    if (recordId !== undefined) refs.push({ record_id: recordId });
  }
  for (const skill of arrayField(context.skills)) {
    const skillId = field(skill, "skill_id", "skillId");
    if (skillId !== undefined) refs.push({ skill_id: skillId });
  }
  return opts.maxRefs === undefined ? refs : refs.slice(0, opts.maxRefs);
}

export function agentStatus(
  status: string,
  opts: { summary?: string; lastSeenSeq?: number; reason?: string; metadata?: Payload } = {}
): Payload {
  return compact({
    status,
    summary: opts.summary,
    last_seen_seq: opts.lastSeenSeq,
    reason: opts.reason,
    metadata: opts.metadata,
  });
}

function compact(input: Payload): Payload {
  return Object.fromEntries(Object.entries(input).filter(([, value]) => value !== undefined));
}

function arrayField(value: unknown): Payload[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is Payload => item !== null && typeof item === "object" && !Array.isArray(item));
}

function field(value: Payload, ...names: string[]): unknown {
  for (const name of names) {
    if (Object.prototype.hasOwnProperty.call(value, name)) return value[name];
  }
  return undefined;
}
