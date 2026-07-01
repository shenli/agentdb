import { createHash } from "node:crypto";

const DEFAULT_AGENTDB_URL = "http://127.0.0.1:7878";

export async function importReviewModel(model, { baseUrl = process.env.AGENTDB_URL || DEFAULT_AGENTDB_URL } = {}) {
  const tenant = await getOrCreateTenant(baseUrl, "codemate-local");
  const agent = await getOrCreateAgent(baseUrl, tenant.tenant_id, "codemate");
  const alias = `codemate-${stableHash(model.session.path || model.session.id).slice(0, 20)}`;
  const session = await getOrCreateSession(baseUrl, tenant.tenant_id, agent.agent_id, alias);
  let appended = 0;
  for (const event of model.events) {
    const idempotencyKey = `codemate:${stableHash(`${model.session.path || model.session.id}:${event.seq}`)}`;
    await request(baseUrl, "POST", `/api/tenants/${enc(tenant.tenant_id)}/sessions/${enc(session.session_id)}/events`, {
      kind: `codemate.${event.category}`,
      agent_id: agent.agent_id,
      payload: {
        source: model.session.host,
        source_seq: event.seq,
        timestamp: event.timestamp,
        title: event.title,
        text: event.text,
        command: event.command,
        exit_code: event.exitCode,
        success: event.success,
        raw: event.raw,
      },
      artifact_refs: [],
      source_refs: [],
      credential_refs: [],
    }, { "Idempotency-Key": idempotencyKey });
    appended += 1;
  }
  return { tenant, agent, session, appended };
}

async function getOrCreateTenant(baseUrl, externalKey) {
  try {
    return await request(baseUrl, "POST", "/api/tenants", { external_key: externalKey });
  } catch (error) {
    const listed = await request(baseUrl, "GET", "/api/tenants");
    const tenants = listed.tenants || listed.items || [];
    const found = tenants.find((tenant) => tenant.external_key === externalKey || tenant.externalKey === externalKey);
    if (!found) throw error;
    return found;
  }
}

async function getOrCreateAgent(baseUrl, tenantId, alias) {
  try {
    return await request(baseUrl, "POST", `/api/tenants/${enc(tenantId)}/agents`, { alias, config: { app: "codemate" } });
  } catch (error) {
    const listed = await request(baseUrl, "GET", `/api/tenants/${enc(tenantId)}/agents`);
    const agents = listed.agents || listed.items || [];
    const found = agents.find((agent) => agent.alias === alias);
    if (!found) throw error;
    return found;
  }
}

async function getOrCreateSession(baseUrl, tenantId, agentId, alias) {
  try {
    return await request(baseUrl, "POST", `/api/tenants/${enc(tenantId)}/agents/${enc(agentId)}/sessions`, { alias });
  } catch (error) {
    const listed = await request(baseUrl, "GET", `/api/tenants/${enc(tenantId)}/sessions`);
    const sessions = listed.sessions || listed.items || [];
    const found = sessions.find((session) => session.alias === alias);
    if (!found) throw error;
    return found;
  }
}

async function request(baseUrl, method, path, body, headers = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: body ? { "content-type": "application/json", ...headers } : headers,
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await response.text();
  let data = {};
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = { text };
    }
  }
  if (!response.ok) {
    throw new Error(`AgentDB ${method} ${path} failed: ${response.status} ${data.error?.message || data.message || text}`);
  }
  return data;
}

function enc(value) {
  return encodeURIComponent(String(value));
}

function stableHash(value) {
  return createHash("sha256").update(String(value)).digest("hex");
}
