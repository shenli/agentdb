const PAYLOAD_SCHEMA_VERSION = 1;
const RUNTIME_VERSION = "2026-04-28";
const MESSAGE_EVENT = "vercel_ai_message";
const STREAM_STATE_NAMESPACE = ["vercel", "ai", "stream_state"];

export class AgentDBChatStore {
  constructor(client, { tenantId, agentId, sessionId }) {
    if (!client) throw new Error("client is required");
    if (!tenantId) throw new Error("tenantId is required");
    if (!agentId) throw new Error("agentId is required");
    if (!sessionId) throw new Error("sessionId is required");
    this.client = client;
    this.tenantId = tenantId;
    this.agentId = agentId;
    this.sessionId = sessionId;
  }

  session() {
    return this.client.session(this.tenantId, this.sessionId, { agentId: this.agentId });
  }

  async appendMessage(message, options = {}) {
    const payload = {
      schema_version: PAYLOAD_SCHEMA_VERSION,
      runtime_version: RUNTIME_VERSION,
      message,
      parts: message.parts ?? null,
      stream_id: options.streamId ?? null,
    };
    return await this.session().append(payload, {
      kind: MESSAGE_EVENT,
      artifactRefs: options.artifactRefs ?? [],
      idempotencyKey: options.idempotencyKey ?? `vercel-ai-message:${this.sessionId}:${message.id ?? Date.now()}`,
    });
  }

  async saveMessages(messages, options = {}) {
    for (const message of messages) {
      await this.appendMessage(message, options);
    }
  }

  async loadMessages(options = {}) {
    const events = await this.session().replay({ limit: options.limit ?? 1000 });
    return events
      .filter((event) => event.kind === MESSAGE_EVENT)
      .map((event) => event.payload?.message)
      .filter(Boolean);
  }

  async saveStreamState(streamId, state) {
    return await this.client.writeRecord(this.tenantId, {
      namespace: STREAM_STATE_NAMESPACE,
      text: JSON.stringify({ streamId, state }),
      metadata: {
        schema_version: PAYLOAD_SCHEMA_VERSION,
        runtime_version: RUNTIME_VERSION,
        session_id: this.sessionId,
        stream_id: streamId,
        state,
      },
      tags: ["vercel", "ai", "stream_state", `session:${this.sessionId}`, `stream:${streamId}`],
      source: { session_id: this.sessionId },
    });
  }

  async loadStreamState(streamId) {
    const records = await this.client.searchRecords(this.tenantId, {
      namespace: STREAM_STATE_NAMESPACE,
      tags: [`stream:${streamId}`],
      limit: 1,
    });
    const record = records[0];
    return record?.metadata?.state ?? null;
  }
}

export const VERCEL_AI_AGENTDB_KINDS = {
  message: MESSAGE_EVENT,
  streamStateNamespace: STREAM_STATE_NAMESPACE,
};
