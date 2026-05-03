const storeKey = "agentdb.v28.dashboard";
const dashboardBase = "/dashboard";

const state = {
  events: [],
  forks: [],
  tenants: [],
  sessions: [],
  agentsByTenant: {},
  sessionsByTenant: {},
  tenantSummaries: [],
  recentSessions: [],
  selectedEventId: null,
  sessionStorage: null,
};

const $ = (id) => document.getElementById(id);

document.addEventListener("DOMContentLoaded", () => {
  restoreInputs();
  hydrateRouteState();
  document.querySelectorAll("[data-action]").forEach((button) => {
    button.addEventListener("click", () => dispatch(button.dataset.action));
  });
  bindSearchControls();
  renderViewMode();
  renderTimeline();
  renderForkTree();
  renderTenantBrowser();
  renderSessionBrowser();
  renderTenantHealth();
  renderRecentSessions();
  renderOverview();
  renderInspector();
  if (isSessionView()) {
    loadSessionView();
  } else {
    loadOverview();
    maybeAutoloadFromUrl();
  }
});

function currentView() {
  const match = window.location.pathname.match(/\/dashboard\/sessions\/([^/]+)\/([^/]+)/);
  if (match) {
    return {
      mode: "session",
      tenantId: decodeURIComponent(match[1]),
      sessionId: decodeURIComponent(match[2]),
    };
  }
  return { mode: "overview" };
}

function isSessionView() {
  return currentView().mode === "session";
}

function hydrateRouteState() {
  const view = currentView();
  if (view.mode !== "session") return;
  $("tenantId").value = view.tenantId;
  $("sessionId").value = view.sessionId;
}

function renderViewMode() {
  const sessionMode = isSessionView();
  document.body.classList.toggle("session-view", sessionMode);
  $("backToOverview").style.display = sessionMode ? "inline-flex" : "none";
  $("selectionTitle").textContent = sessionMode ? "Session explorer" : "Session explorer";
}

function bindSearchControls() {
  ["tenantSearch", "sessionSearch", "eventSearch", "eventKindFilter"].forEach((id) => {
    const el = $(id);
    if (!el) return;
    el.addEventListener("input", () => {
      if (id === "tenantSearch") {
        renderTenantHealth();
        renderTenantBrowser();
      } else if (id === "sessionSearch") {
        renderRecentSessions();
        renderSessionBrowser();
      } else {
        renderTimeline();
      }
    });
  });
}

function dispatch(action) {
  const actions = {
    tenants: loadTenants,
    overview: loadOverview,
    sessions: loadSessions,
    load: loadTimeline,
    replay: replaySession,
    subscribe: subscribeSession,
    watch: watchScope,
    context: buildContext,
    storage: loadSessionStorage,
    "to-warm": () => transitionLifecycle("warm"),
    "to-cold": () => transitionLifecycle("cold"),
    "to-archive": () => transitionLifecycle("archive"),
    fork: forkSession,
    artifact: loadArtifact,
    records: searchRecords,
    skills: loadSkills,
    clear: () => setRaw(""),
  };
  return actions[action] && actions[action]();
}

function readConfig(requireIds = true) {
  const cfg = {
    tenantId: $("tenantId").value.trim(),
    sessionId: $("sessionId").value.trim(),
    agentId: $("agentId").value.trim(),
    fromSeq: Number($("fromSeq").value || 0),
    limit: Number($("limit").value || 100),
    token: $("token").value.trim(),
  };
  localStorage.setItem(storeKey, JSON.stringify(cfg));
  syncUrl(cfg);
  if (requireIds) {
    if (!cfg.tenantId) throw new Error("tenant_id is required");
    if (!cfg.sessionId) throw new Error("session_id is required");
  }
  return cfg;
}

function restoreInputs() {
  try {
    const cfg = {
      ...JSON.parse(localStorage.getItem(storeKey) || "{}"),
      ...configFromQuery(),
    };
    for (const [key, value] of Object.entries(cfg)) {
      if ($(key) && value !== undefined) $(key).value = value;
    }
  } catch (_) {}
}

function configFromQuery() {
  const params = new URLSearchParams(window.location.search);
  const cfg = {};
  mapQueryParam(params, cfg, "tenantId", ["tenantId", "tenant_id"]);
  mapQueryParam(params, cfg, "sessionId", ["sessionId", "session_id"]);
  mapQueryParam(params, cfg, "agentId", ["agentId", "agent_id"]);
  mapQueryParam(params, cfg, "fromSeq", ["fromSeq", "from_seq"]);
  mapQueryParam(params, cfg, "limit", ["limit"]);
  mapQueryParam(params, cfg, "token", ["token"]);
  return cfg;
}

function mapQueryParam(params, cfg, key, names) {
  for (const name of names) {
    const value = params.get(name);
    if (value) {
      cfg[key] = value;
      return;
    }
  }
}

function syncUrl(cfg) {
  const url = new URL(window.location.href);
  setQueryValue(url.searchParams, "tenant_id", cfg.tenantId);
  setQueryValue(url.searchParams, "session_id", cfg.sessionId);
  setQueryValue(url.searchParams, "agent_id", cfg.agentId);
  setQueryValue(url.searchParams, "from_seq", String(cfg.fromSeq || 0));
  setQueryValue(url.searchParams, "limit", String(cfg.limit || 100));
  if (cfg.token) {
    setQueryValue(url.searchParams, "token", cfg.token);
  } else {
    url.searchParams.delete("token");
  }
  window.history.replaceState({}, "", url);
}

function setQueryValue(params, key, value) {
  if (value && value !== "0" && value !== "100") {
    params.set(key, value);
    return;
  }
  if ((key === "from_seq" && value === "0") || (key === "limit" && value === "100")) {
    params.delete(key);
    return;
  }
  if (value) {
    params.set(key, value);
  } else {
    params.delete(key);
  }
}

function maybeAutoloadFromUrl() {
  const cfg = configFromQuery();
  if (cfg.tenantId && cfg.sessionId) {
    loadTimeline();
  }
}

async function request(path, options = {}) {
  const cfg = readConfig(options.requireIds !== false);
  const headers = new Headers(options.headers || {});
  applyAuth(headers, cfg);
  if (options.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  const resp = await fetch(path, { ...options, headers });
  const contentType = resp.headers.get("content-type") || "";
  const body = contentType.includes("application/json") ? await resp.json() : await resp.text();
  if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}: ${stringify(body)}`);
  setConnection("ok");
  setRaw(body);
  return body;
}

async function loadTenants() {
  try {
    const data = await request("/api/tenants", { requireIds: false });
    state.tenants = data.tenants || [];
    renderTenantBrowser();
    if ($("tenantId").value.trim()) {
      loadSessions();
    }
  } catch (err) {
    fail(err);
  }
}

async function loadOverview() {
  try {
    await loadTenants();
    const tenants = state.tenants.slice();
    const agentPairs = await Promise.all(
      tenants.map(async (tenant) => {
        const data = await request(`/api/tenants/${enc(tenant.tenant_id)}/agents`, { requireIds: false });
        return [tenant.tenant_id, data.agents || []];
      })
    );
    const sessionPairs = await Promise.all(
      tenants.map(async (tenant) => {
        const data = await request(`/api/tenants/${enc(tenant.tenant_id)}/sessions`, { requireIds: false });
        return [tenant.tenant_id, data.sessions || []];
      })
    );
    state.agentsByTenant = Object.fromEntries(agentPairs);
    state.sessionsByTenant = Object.fromEntries(sessionPairs);
    state.tenantSummaries = tenants.map((tenant) => buildTenantSummary(tenant));
    state.recentSessions = state.tenantSummaries
      .flatMap((summary) => summary.sessions.map((session) => ({ ...session, tenant: summary.tenant })))
      .sort((a, b) => (b.last_active_at_ms || 0) - (a.last_active_at_ms || 0))
      .slice(0, 24);
    renderTenantHealth();
    renderRecentSessions();
    renderOverview();
  } catch (err) {
    fail(err);
  }
}

async function loadSessionView() {
  try {
    const view = currentView();
    if (view.mode !== "session") return;
    await loadOverview();
    selectTenant(view.tenantId);
    const session = (state.sessionsByTenant[view.tenantId] || []).find((item) => item.session_id === view.sessionId);
    if (session) {
      $("agentId").value = session.agent_id || "";
    }
    readConfig(false);
    await loadSessionStorage();
    await loadTimeline();
  } catch (err) {
    fail(err);
  }
}

async function loadSessions() {
  try {
    const cfg = readConfig(false);
    if (!cfg.tenantId) {
      state.sessions = [];
      renderSessionBrowser();
      return;
    }
    const data = await request(`/api/tenants/${enc(cfg.tenantId)}/sessions`, { requireIds: false });
    state.sessions = data.sessions || [];
    renderSessionBrowser();
  } catch (err) {
    fail(err);
  }
}

async function loadTimeline() {
  try {
    const cfg = readConfig();
    const params = new URLSearchParams({ from_seq: String(cfg.fromSeq), limit: String(cfg.limit) });
    state.events = await request(`/api/tenants/${enc(cfg.tenantId)}/sessions/${enc(cfg.sessionId)}/events?${params}`);
    if (!state.events.find((event) => event.event_id === state.selectedEventId)) {
      state.selectedEventId = state.events[0]?.event_id || null;
    }
    renderOverview();
    renderTimeline();
    renderInspector();
  } catch (err) {
    fail(err);
  }
}

async function loadSessionStorage() {
  try {
    const cfg = readConfig();
    state.sessionStorage = await request(
      `/api/tenants/${enc(cfg.tenantId)}/sessions/${enc(cfg.sessionId)}/storage`
    );
    renderOverview();
    renderStorageDetail();
  } catch (err) {
    fail(err);
  }
}

async function transitionLifecycle(target) {
  try {
    const cfg = readConfig();
    state.sessionStorage = await request(
      `/api/tenants/${enc(cfg.tenantId)}/sessions/${enc(cfg.sessionId)}/lifecycle`,
      {
        method: "POST",
        body: JSON.stringify({ target }),
      }
    );
    refreshSelectedSession(target);
    await loadTimeline();
    renderOverview();
    renderStorageDetail();
  } catch (err) {
    fail(err);
  }
}

async function replaySession() {
  try {
    const cfg = readConfig();
    const params = new URLSearchParams({ from_seq: String(cfg.fromSeq), limit: String(cfg.limit) });
    const data = await request(`/api/tenants/${enc(cfg.tenantId)}/sessions/${enc(cfg.sessionId)}/replay?${params}`);
    state.events = data.events || [];
    if (!state.events.find((event) => event.event_id === state.selectedEventId)) {
      state.selectedEventId = state.events[0]?.event_id || null;
    }
    renderOverview();
    renderTimeline();
    renderInspector();
    if (data.lineage) {
      state.forks = data.lineage;
      renderForkTree();
    }
  } catch (err) {
    fail(err);
  }
}

async function subscribeSession() {
  try {
    const cfg = readConfig();
    const params = new URLSearchParams({ from_seq: String(cfg.fromSeq), limit: String(cfg.limit) });
    const url = `/api/tenants/${enc(cfg.tenantId)}/sessions/${enc(cfg.sessionId)}/subscribe?${params}`;
    const headers = new Headers({ Accept: "text/event-stream" });
    applyAuth(headers, cfg);
    const resp = await fetch(url, { headers });
    const text = await resp.text();
    if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}: ${text.slice(0, 500)}`);
    state.events = parseSse(text);
    setConnection("sse snapshot");
    setRaw(state.events);
    renderTimeline();
  } catch (err) {
    fail(err);
  }
}

async function watchScope() {
  try {
    const cfg = readConfig();
    const params = new URLSearchParams({ scope: "session", session_id: cfg.sessionId, limit: String(cfg.limit) });
    if (cfg.agentId) params.set("agent_id", cfg.agentId);
    const data = await request(`/api/tenants/${enc(cfg.tenantId)}/watch?${params}`);
    state.events = data.map((item) => item.event || item);
    if (!state.events.find((event) => event.event_id === state.selectedEventId)) {
      state.selectedEventId = state.events[0]?.event_id || null;
    }
    renderOverview();
    renderTimeline();
    renderInspector();
  } catch (err) {
    fail(err);
  }
}

async function buildContext() {
  try {
    const cfg = readConfig();
    await request(`/api/tenants/${enc(cfg.tenantId)}/sessions/${enc(cfg.sessionId)}/context`, {
      method: "POST",
      body: JSON.stringify({
        max_events: 20,
        max_bytes: 1000000,
        include: ["recent_events", "records", "skills", "policy"],
        record_query: recordQuery(),
      }),
    });
  } catch (err) {
    fail(err);
  }
}

async function forkSession() {
  try {
    const cfg = readConfig();
    const body = {
      at_seq: Number($("forkSeq").value || 0),
      alias: $("forkAlias").value.trim() || null,
    };
    const child = await request(`/api/tenants/${enc(cfg.tenantId)}/sessions/${enc(cfg.sessionId)}/fork`, {
      method: "POST",
      body: JSON.stringify(body),
    });
    state.forks.push({ parent_session_id: cfg.sessionId, child_session_id: child.session_id, at_seq: body.at_seq });
    renderForkTree();
  } catch (err) {
    fail(err);
  }
}

async function loadArtifact() {
  try {
    const cfg = readConfig();
    const artifactId = $("artifactId").value.trim();
    if (!artifactId) throw new Error("artifact_id is required");
    const meta = await request(`/api/tenants/${enc(cfg.tenantId)}/artifacts/${enc(artifactId)}`);
    $("artifactView").textContent = stringify(meta);
  } catch (err) {
    fail(err);
  }
}

async function searchRecords() {
  try {
    const cfg = readConfig();
    const data = await request(`/api/tenants/${enc(cfg.tenantId)}/records/search`, {
      method: "POST",
      body: JSON.stringify(recordQuery()),
    });
    renderRows("records", data.records || []);
  } catch (err) {
    fail(err);
  }
}

async function loadSkills() {
  try {
    const cfg = readConfig();
    if (!cfg.agentId) throw new Error("agent_id is required");
    const data = await request(`/api/tenants/${enc(cfg.tenantId)}/agents/${enc(cfg.agentId)}/skills`);
    renderRows("skills", data.skills || []);
  } catch (err) {
    fail(err);
  }
}

function recordQuery() {
  const sourceSession = $("recordSourceSession").value.trim();
  return {
    text: $("recordText").value.trim() || null,
    namespace: csv($("recordNamespace").value),
    tags: csv($("recordTags").value),
    source: sourceSession ? { session_id: sourceSession } : null,
    limit: Number($("limit").value || 100),
  };
}

function renderTimeline() {
  const events = filteredEvents();
  $("eventCount").textContent = `${events.length} events`;
  const root = $("timeline");
  root.textContent = "";
  if (!events.length) {
    root.append(empty(state.events.length ? "No events match the current filters" : "No events loaded"));
    return;
  }
  events
    .slice()
    .sort((a, b) => (a.seq || 0) - (b.seq || 0))
    .forEach((event) => {
      const item = document.createElement("article");
      item.className = `event${event.event_id === state.selectedEventId ? " active" : ""}`;
      item.innerHTML = `
        <div class="event-meta">
          <span class="seq">#${event.seq ?? "-"}</span>
          <span class="kind">${escapeHtml(event.kind || "event")}</span>
          <time>${formatTime(event.created_at_ms)}</time>
        </div>
        <div class="event-body">
          ${renderEventBody(event)}
        </div>
        <pre class="summary">${escapeHtml(summary(event))}</pre>
        <div class="refs">${artifactRefs(event)}</div>
      `;
      item.addEventListener("click", () => {
        state.selectedEventId = event.event_id;
        renderTimeline();
        renderInspector();
        setRaw(event);
      });
      item.querySelectorAll("[data-artifact]").forEach((button) => {
        button.addEventListener("click", (clickEvent) => {
          clickEvent.stopPropagation();
          const artifactId = button.dataset.artifact || "";
          $("artifactId").value = artifactId;
          loadArtifact();
        });
      });
      root.append(item);
    });
}

function renderForkTree() {
  const root = $("forkTree");
  root.textContent = "";
  if (!state.forks.length) {
    root.append(empty("No forks recorded in this browser"));
    return;
  }
  state.forks.forEach((edge) => {
    const row = document.createElement("div");
    row.className = "edge";
    row.textContent = `${edge.parent_session_id || edge.parent || "parent"} -> ${edge.child_session_id || edge.child || "child"} @ ${edge.at_seq ?? "-"}`;
    root.append(row);
  });
}

function renderTenantBrowser() {
  const tenants = filteredTenants();
  const selectedTenant = $("tenantId").value.trim();
  $("tenantCount").textContent = `${tenants.length} tenants`;
  const root = $("tenants");
  root.textContent = "";
  if (!tenants.length) {
    root.append(empty("No tenants"));
    return;
  }
  tenants.forEach((tenant) => {
    const node = document.createElement("button");
    node.className = `row browser-row${tenant.tenant_id === selectedTenant ? " selected" : ""}`;
    node.type = "button";
    node.innerHTML = `
      <strong>${escapeHtml(tenant.external_key || "(no external key)")}</strong>
      <span>${escapeHtml(tenant.tenant_id || "")}</span>
    `;
    node.addEventListener("click", () => selectTenant(tenant.tenant_id));
    root.append(node);
  });
}

function renderSessionBrowser() {
  const sessions = filteredSessions(state.sessions);
  const selectedSession = $("sessionId").value.trim();
  $("sessionCount").textContent = `${sessions.length} sessions`;
  const root = $("sessions");
  root.textContent = "";
  if (!sessions.length) {
    root.append(empty("No sessions for selected tenant"));
    return;
  }
  sessions.forEach((session) => {
    const node = document.createElement("button");
    node.className = `row browser-row${session.session_id === selectedSession ? " selected" : ""}`;
    node.type = "button";
    node.innerHTML = `
      <strong>${escapeHtml(session.alias || "(no alias)")}</strong>
      <span>${escapeHtml(session.session_id || "")}</span>
      <span>agent ${escapeHtml(session.agent_id || "")}</span>
      <span>${escapeHtml(session.status || "unknown")} • ${escapeHtml(session.lifecycle || "warm")} • active ${escapeHtml(formatTime(session.last_active_at_ms) || "-")}</span>
    `;
    node.addEventListener("click", () => selectSession(session.tenant_id, session.session_id, session.agent_id));
    root.append(node);
  });
}

function renderTenantHealth() {
  const summaries = filteredTenantSummaries();
  const selectedTenant = $("tenantId").value.trim();
  $("tenantHealthCount").textContent = `${summaries.length} tenants`;
  const root = $("tenantCards");
  root.textContent = "";
  if (!summaries.length) {
    root.append(empty("No tenants"));
    return;
  }
  summaries.forEach((summary) => {
    const node = document.createElement("button");
    node.className = `row tenant-card${summary.tenant.tenant_id === selectedTenant ? " selected" : ""}`;
    node.type = "button";
    node.innerHTML = `
      <div class="tenant-card-head">
        <strong>${escapeHtml(summary.tenant.external_key || "(no external key)")}</strong>
        <span>${escapeHtml(summary.tenant.tenant_id || "")}</span>
      </div>
      <div class="tenant-card-stats">
        <span>${summary.agentCount} agents</span>
        <span>${summary.sessionCount} sessions</span>
        <span>${summary.activeSessionCount} active</span>
      </div>
      <div class="tenant-card-foot">
        <span>Latest session: ${escapeHtml(summary.latestActivityLabel)}</span>
      </div>
    `;
    node.addEventListener("click", () => selectTenant(summary.tenant.tenant_id));
    root.append(node);
  });
}

function renderRecentSessions() {
  const sessions = filteredSessions(state.recentSessions);
  const selectedSession = $("sessionId").value.trim();
  $("recentSessionCount").textContent = `${sessions.length} sessions`;
  const root = $("recentSessions");
  root.textContent = "";
  if (!sessions.length) {
    root.append(empty("No sessions"));
    return;
  }
  sessions.forEach((session) => {
    const node = document.createElement("button");
    node.className = `row browser-row recent-row${session.session_id === selectedSession ? " selected" : ""}`;
    node.type = "button";
    node.innerHTML = `
      <strong>${escapeHtml(session.alias || "(no alias)")}</strong>
      <span>${escapeHtml(session.tenant.external_key || session.tenant.tenant_id || "")}</span>
      <span>${escapeHtml(session.status || "unknown")} • ${escapeHtml(session.lifecycle || "warm")} • ${escapeHtml(formatTime(session.last_active_at_ms) || "-")}</span>
    `;
    node.addEventListener("click", () => selectSession(session.tenant_id, session.session_id, session.agent_id));
    root.append(node);
  });
}

function renderOverview() {
  const cfg = readConfig(false);
  const latest = state.events[state.events.length - 1];
  const tenantCount = state.tenantSummaries.length;
  const agentCount = state.tenantSummaries.reduce((sum, item) => sum + item.agentCount, 0);
  const sessionCount = state.tenantSummaries.reduce((sum, item) => sum + item.sessionCount, 0);
  const activeSessionCount = state.tenantSummaries.reduce((sum, item) => sum + item.activeSessionCount, 0);
  $("overviewTenantCount").textContent = String(tenantCount);
  $("overviewAgentCount").textContent = String(agentCount);
  $("overviewSessionCount").textContent = String(sessionCount);
  $("overviewActiveSessionCount").textContent = String(activeSessionCount);
  $("overviewTenant").textContent = cfg.tenantId || "-";
  $("overviewSession").textContent = cfg.sessionId || "-";
  $("overviewEventCount").textContent = String(state.events.length);
  $("overviewLatestKind").textContent = latest?.kind || "-";
  $("storageLifecycle").textContent = state.sessionStorage?.lifecycle || "-";
  $("storageWarmHead").textContent =
    state.sessionStorage?.warm_head_from_seq ?? "-";
  $("storageSegments").textContent = String(
    state.sessionStorage?.sealed_segment_count || 0
  );
  $("storageArchive").textContent = archiveSummary(state.sessionStorage);
  $("selectionLabel").textContent = cfg.sessionId ? "Session selected" : "No session selected";
  $("selectionMeta").textContent = cfg.sessionId
    ? `Inspecting ${cfg.sessionId}`
    : "Choose a session from tenant health or recent sessions";
}

function renderStorageDetail() {
  $("storageDetailLabel").textContent = state.sessionStorage
    ? `${state.sessionStorage.lifecycle || "unknown"}`
    : "No storage loaded";
  $("storageView").textContent = state.sessionStorage
    ? stringify(state.sessionStorage)
    : "";
}

function renderInspector() {
  const root = $("eventInspector");
  const event = state.events.find((item) => item.event_id === state.selectedEventId);
  if (!event) {
    $("selectedEventLabel").textContent = "No event selected";
    root.className = "inspector empty-state";
    root.textContent = "Select an event to inspect its payload, refs, and metadata.";
    return;
  }
  $("selectedEventLabel").textContent = `#${event.seq ?? "-"} ${event.kind || "event"}`;
  root.className = "inspector";
  root.innerHTML = `
    <div class="inspector-meta">
      <span>Event ID: ${escapeHtml(event.event_id || "-")}</span>
      <span>Agent: ${escapeHtml(event.agent_id || "-")}</span>
      <span>Created: ${escapeHtml(formatTime(event.created_at_ms) || "-")}</span>
    </div>
    <div class="inspector-section">
      <h3>Payload</h3>
      <pre class="payload-view">${escapeHtml(stringify(event.payload || {}))}</pre>
    </div>
    <div class="inspector-section">
      <h3>Source refs</h3>
      <pre class="payload-view">${escapeHtml(stringify(event.source_refs || []))}</pre>
    </div>
    <div class="inspector-section">
      <h3>Artifact refs</h3>
      <pre class="payload-view">${escapeHtml(stringify(event.artifact_refs || []))}</pre>
    </div>
  `;
}

function renderRows(id, rows) {
  const root = $(id);
  root.textContent = "";
  if (!rows.length) {
    root.append(empty("No rows"));
    return;
  }
  rows.forEach((row) => {
    const node = document.createElement("button");
    node.className = "row";
    node.type = "button";
    node.textContent = row.record_id || row.skill_id || row.id || summary(row);
    node.addEventListener("click", () => setRaw(row));
    root.append(node);
  });
}

function buildTenantSummary(tenant) {
  const agents = state.agentsByTenant[tenant.tenant_id] || [];
  const sessions = state.sessionsByTenant[tenant.tenant_id] || [];
  const latestSession = sessions
    .slice()
    .sort((a, b) => (b.last_active_at_ms || 0) - (a.last_active_at_ms || 0))[0];
  return {
    tenant,
    sessions,
    agentCount: agents.length,
    sessionCount: sessions.length,
    activeSessionCount: sessions.filter((session) => session.status === "active").length,
    latestActivityLabel: latestSession ? formatTime(latestSession.last_active_at_ms) : "No sessions",
  };
}

function filteredTenants() {
  const query = normalizedText($("tenantSearch")?.value);
  if (!query) return state.tenants;
  return state.tenants.filter((tenant) =>
    [tenant.external_key, tenant.tenant_id].some((value) => normalizedText(value).includes(query))
  );
}

function filteredTenantSummaries() {
  const query = normalizedText($("tenantSearch")?.value);
  if (!query) return state.tenantSummaries;
  return state.tenantSummaries.filter((summary) =>
    [summary.tenant.external_key, summary.tenant.tenant_id].some((value) =>
      normalizedText(value).includes(query)
    )
  );
}

function filteredSessions(sessions) {
  const query = normalizedText($("sessionSearch")?.value);
  if (!query) return sessions;
  return sessions.filter((session) =>
    [
      session.alias,
      session.session_id,
      session.agent_id,
      session.tenant?.external_key,
      session.tenant?.tenant_id,
      session.status,
    ].some((value) => normalizedText(value).includes(query))
  );
}

function filteredEvents() {
  const query = normalizedText($("eventSearch")?.value);
  const kind = $("eventKindFilter")?.value || "";
  return state.events.filter((event) => {
    if (kind && event.kind !== kind) return false;
    if (!query) return true;
    return normalizedText(`${event.kind}\n${summary(event)}\n${stringify(event.payload || {})}`).includes(query);
  });
}

function selectTenant(tenantId) {
  $("tenantId").value = tenantId || "";
  $("sessionId").value = "";
  $("agentId").value = "";
  state.sessions = state.sessionsByTenant[tenantId] || [];
  state.events = [];
  state.selectedEventId = null;
  state.sessionStorage = null;
  renderSessionBrowser();
  renderOverview();
  renderTimeline();
  renderInspector();
  renderStorageDetail();
  readConfig(false);
}

function selectSession(tenantId, sessionId, agentId) {
  const path = `${dashboardBase}/sessions/${enc(tenantId)}/${enc(sessionId)}`;
  if (window.location.pathname !== path) {
    window.location.assign(path);
    return;
  }
  $("tenantId").value = tenantId || "";
  $("sessionId").value = sessionId || "";
  $("agentId").value = agentId || "";
  state.sessions = state.sessionsByTenant[tenantId] || [];
  state.selectedEventId = null;
  state.sessionStorage = null;
  renderSessionBrowser();
  readConfig(false);
  loadTimeline();
  loadSessionStorage();
}

function refreshSelectedSession(target) {
  const cfg = readConfig(false);
  const collections = [state.sessions, state.recentSessions, state.sessionsByTenant[cfg.tenantId] || []];
  collections.forEach((sessions) => {
    const session = sessions.find((item) => item.session_id === cfg.sessionId);
    if (session) session.lifecycle = target;
  });
  renderRecentSessions();
  renderSessionBrowser();
}

function upsertEvent(event) {
  const idx = state.events.findIndex((item) => item.event_id === event.event_id);
  if (idx >= 0) state.events[idx] = event;
  else state.events.push(event);
  if (!state.selectedEventId) state.selectedEventId = event.event_id;
}

function parseSse(text) {
  const out = [];
  let data = "";
  text.split(/\r?\n/).forEach((line) => {
    if (line.startsWith("data:")) data += line.slice(5).trimStart();
    if (line === "" && data) {
      out.push(JSON.parse(data));
      data = "";
    }
  });
  if (data) out.push(JSON.parse(data));
  return out;
}

function applyAuth(headers, cfg) {
  if (cfg.token) headers.set("Authorization", `Bearer ${cfg.token}`);
}

function artifactRefs(event) {
  const refs = event.artifact_refs || [];
  if (!refs.length) return "";
  return refs.map((ref) => `
    <button class="ref chip" data-artifact="${escapeHtml(ref.artifact_id || ref)}">
      <span class="chip-label">artifact</span>
      <span class="chip-value">${escapeHtml(ref.artifact_id || ref)}</span>
    </button>
  `).join("");
}

function empty(text) {
  const node = document.createElement("p");
  node.className = "empty";
  node.textContent = text;
  return node;
}

function fail(err) {
  setConnection("error");
  setRaw(String(err && err.message ? err.message : err));
}

function setConnection(text) {
  $("connection").textContent = text;
}

function setRaw(value) {
  $("raw").textContent = typeof value === "string" ? value : stringify(value);
}

function summary(event) {
  const payload = event && event.payload ? event.payload : {};
  const text = eventSummary(event.kind, payload);
  return text.length > 320 ? `${text.slice(0, 320)}...` : text;
}

function renderEventBody(event) {
  const payload = event && event.payload ? event.payload : {};
  const parts = [];
  if (payload.role) parts.push(`<span class="pill">${escapeHtml(payload.role)}</span>`);
  if (payload.status) parts.push(`<span class="pill">${escapeHtml(payload.status)}</span>`);
  if (payload.tool) parts.push(`<span class="pill">${escapeHtml(payload.tool)}</span>`);
  if (payload.context_pack_id) parts.push(`<span class="pill">${escapeHtml(payload.context_pack_id)}</span>`);
  return parts.join("");
}

function eventSummary(kind, payload) {
  const lines = [];
  if (kind === "user_message" || kind === "model_message") {
    if (payload.role) lines.push(`role: ${payload.role}`);
    if (payload.text) lines.push(payload.text);
  } else if (kind === "tool_call") {
    if (payload.tool) lines.push(`tool: ${payload.tool}`);
    if (payload.tool_call_id) lines.push(`call: ${payload.tool_call_id}`);
    if (payload.input && payload.input.cmd) lines.push(`cmd: ${payload.input.cmd}`);
    if (payload.timeout_ms !== undefined) lines.push(`timeout: ${payload.timeout_ms}ms`);
  } else if (kind === "tool_result") {
    if (payload.status) lines.push(`status: ${payload.status}`);
    if (payload.summary) lines.push(payload.summary);
    if (payload.output && payload.output.preview) lines.push(`preview: ${payload.output.preview}`);
    if (payload.duration_ms !== undefined) lines.push(`duration: ${payload.duration_ms}ms`);
  } else if (kind === "context_pack_built") {
    if (payload.context_pack_id) lines.push(`context pack: ${payload.context_pack_id}`);
    if (payload.include) lines.push(`include: ${payload.include.join(", ")}`);
    if (payload.selected_event_count !== undefined) {
      lines.push(`selected events: ${payload.selected_event_count}`);
    }
  } else if (kind === "agent_status") {
    if (payload.status) lines.push(`status: ${payload.status}`);
    if (payload.summary) lines.push(payload.summary);
    if (payload.last_seen_seq !== undefined) lines.push(`last seen seq: ${payload.last_seen_seq}`);
  }
  if (!lines.length) return stringify(payload);
  return lines.join("\n");
}

function stringify(value) {
  return JSON.stringify(value, null, 2);
}

function csv(value) {
  return value.split(",").map((item) => item.trim()).filter(Boolean);
}

function normalizedText(value) {
  return String(value || "").toLowerCase();
}

function archiveSummary(storage) {
  const manifest = storage?.archive_manifest;
  if (!manifest) return "-";
  const id = shortId(manifest.manifest_id || "");
  const count = manifest.event_count ?? 0;
  return `${id} • ${count} evt`;
}

function enc(value) {
  return encodeURIComponent(value);
}

function shortId(value) {
  const text = String(value || "");
  if (!text) return "-";
  return text.length > 18 ? `${text.slice(0, 18)}...` : text;
}

function formatTime(ms) {
  return ms ? new Date(ms).toLocaleString() : "";
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[char]));
}
