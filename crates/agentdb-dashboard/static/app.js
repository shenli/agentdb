// AgentDB dashboard — Alpine component. One top-level
// `dashboard()` function; each admin tab has its own
// sub-object that owns its fetch + render state.
//
// Auth: when the admin surface requires a bearer token,
// paste it into the header input. It lives in localStorage
// (origin-scoped) and is sent as `Authorization: Bearer` on
// every admin fetch. No token == plaintext fetch (OK when
// `AGENTDB_REQUIRE_ADMIN_AUTH` is unset).

const TOKEN_KEY = 'agentdb.admin.token';
const TAB_KEY = 'agentdb.dashboard.tab';

function dashboard() {
  return {
    tab: 'overview',
    adminToken: '',
    // Overview state.
    boxes: [],
    events: [],
    totalAppends: 0,
    totalForks: 0,
    totalTenants: 0,
    _nextId: 0,
    // Admin tabs.
    slow: slowPanel(),
    audit: auditPanel(),
    tokens: tokensPanel(),
    quotas: quotasPanel(),

    async init() {
      try { this.adminToken = localStorage.getItem(TOKEN_KEY) || ''; } catch (_) {}
      try { this.tab = localStorage.getItem(TAB_KEY) || 'overview'; } catch (_) {}
      await this.refreshBoxes();
      this.connectStream();
      setInterval(() => this.refreshBoxes(), 5000);
      // Start a polling loop that refreshes only the visible
      // admin tab. 5s is responsive enough for operator UX
      // without hammering the server.
      setInterval(() => this.pollActiveTab(), 5000);
      this.pollActiveTab();
    },

    switchTab(name) {
      this.tab = name;
      try { localStorage.setItem(TAB_KEY, name); } catch (_) {}
      this.pollActiveTab();
    },

    pollActiveTab() {
      const h = this.tokenHeader();
      if (this.tab === 'slow') this.slow.load(h);
      else if (this.tab === 'audit') this.audit.load(h);
      else if (this.tab === 'tokens') this.tokens.load(h);
      else if (this.tab === 'quotas') this.quotas.load(h);
    },

    saveToken() {
      try {
        if (this.adminToken) localStorage.setItem(TOKEN_KEY, this.adminToken);
        else localStorage.removeItem(TOKEN_KEY);
      } catch (_) {}
      // Immediately refresh the active tab so the operator
      // sees whether the new token unblocked it.
      this.pollActiveTab();
    },

    tokenHeader() {
      return this.adminToken
        ? { 'Authorization': 'Bearer ' + this.adminToken }
        : {};
    },

    async refreshBoxes() {
      try {
        const resp = await fetch('/api/admin/boxes', { headers: this.tokenHeader() });
        if (!resp.ok) return;
        const data = await resp.json();
        this.boxes = data.map(b => ({
          id: b.id,
          warm_sessions: (b.metrics && b.metrics.gauges && b.metrics.gauges.warm_sessions) || 0,
          cold_sessions: (b.metrics && b.metrics.gauges && b.metrics.gauges.cold_sessions) || 0,
          fault_active: b.fault_active,
          num_tenants: b.num_tenants,
          appends: (b.metrics && b.metrics.appends) || 0,
          forks: (b.metrics && b.metrics.forks) || 0,
          open_snapshots: (b.metrics && b.metrics.open_snapshots) || 0,
          segments_pinned_by_snapshots: (b.metrics && b.metrics.segments_pinned_by_snapshots) || 0,
          archive_sessions: (b.metrics && b.metrics.gauges && b.metrics.gauges.archive_sessions) || 0,
          webhook_url: b.webhook_url || null,
          webhook_fires: (b.metrics && b.metrics.webhook_fires) || 0,
          webhook_failures: (b.metrics && b.metrics.webhook_failures) || 0,
        }));
        this.totalTenants = this.boxes.reduce((s, b) => s + b.num_tenants, 0);
        this.totalAppends = this.boxes.reduce((s, b) => s + b.appends, 0);
        this.totalForks = this.boxes.reduce((s, b) => s + b.forks, 0);
      } catch (err) {
        console.error('refreshBoxes', err);
      }
    },

    connectStream() {
      const src = new EventSource('/api/stream');
      src.onmessage = (e) => {
        try {
          const ev = JSON.parse(e.data);
          ev._id = this._nextId++;
          this.events.push(ev);
          if (this.events.length > 200) this.events.shift();
          if (ev.event === 'MetricsTick') {
            for (const tb of ev.boxes) {
              const b = this.boxes.find(x => x.id === tb.id);
              if (b) {
                b.warm_sessions = tb.warm_sessions;
                b.cold_sessions = tb.cold_sessions;
                b.fault_active = tb.fault_active;
                b.appends = tb.appends;
                b.forks = tb.forks;
                b.open_snapshots = tb.open_snapshots || 0;
                b.segments_pinned_by_snapshots = tb.segments_pinned_by_snapshots || 0;
              }
            }
          }
        } catch (err) {
          console.error('sse parse', err);
        }
      };
      src.onerror = () => console.warn('sse error');
    },

    describe(ev) {
      switch (ev.event) {
        case 'TierChange': return `${ev.session_id} ${ev.from} → ${ev.to} (box ${ev.box_id})`;
        case 'ForkCreated': return `${ev.parent} → ${ev.child} (box ${ev.box_id})`;
        case 'FaultInjected': return `box ${ev.box_id} ${ev.kind} for ${ev.duration_s}s`;
        case 'QuotaHit': return ev.tenant
          ? `box ${ev.box_id} quota ${ev.kind} (tenant ${ev.tenant})`
          : `box ${ev.box_id} quota ${ev.kind}`;
        case 'MetricsTick': return `${ev.boxes.length} boxes`;
        case 'SegmentGcRun': return `box ${ev.box_id}: reclaimed ${ev.reclaimed}, skipped ${ev.skipped_pinned} pinned`;
        default: return JSON.stringify(ev);
      }
    },

    snapsLine(b) {
      const snaps = b.open_snapshots || 0;
      const pins = b.segments_pinned_by_snapshots || 0;
      return pins > 0 ? `snapshots: ${snaps} (${pins} seg pins)` : `snapshots: ${snaps}`;
    },

    webhookLine(b) {
      const fires = b.webhook_fires || 0;
      const fails = b.webhook_failures || 0;
      if (!b.webhook_url) return `webhook: none`;
      let host;
      try { host = new URL(b.webhook_url).host; } catch { host = '***'; }
      return `webhook: ${host} (${fires} ok / ${fails} fail)`;
    },

    async demoFork() {
      await fetch('/api/tenants/t-demo/sessions/agent/append_and_fetch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ event: { text: 'hello' }, fetch_last_n: 0 }),
      });
      await fetch('/api/sessions/agent/fork', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tenant_id: 't-demo', new_session_id: 'subagent-' + Date.now() }),
      });
    },
    async demoFault(boxId) {
      await fetch(`/api/admin/boxes/${boxId}/inject_fault`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...this.tokenHeader() },
        body: JSON.stringify({ kind: 'latency', duration_s: 10, latency_ms: 300 }),
      });
    },
    async demoCold() {
      await fetch('/api/admin/tier/force', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...this.tokenHeader() },
        body: JSON.stringify({ tenant_id: 't-demo', session_id: 'agent', target: 'cold' }),
      });
    },
  };
}

// Shared helper: describe a fetch failure for the operator.
async function explainFetchFailure(resp) {
  if (resp.status === 401) return '401 Unauthorized — set an admin token above.';
  if (resp.status === 403) return '403 Forbidden — this token lacks the required scope.';
  if (resp.status === 503) return '503 — meta store not configured on this server.';
  try {
    const body = await resp.text();
    return `${resp.status} ${resp.statusText}: ${body.slice(0, 200)}`;
  } catch (_) {
    return `${resp.status} ${resp.statusText}`;
  }
}

function slowPanel() {
  return {
    rows: [],
    err: '',
    kindFilter: '',
    tenantFilter: '',
    limit: 50,

    async load(headers) {
      const params = new URLSearchParams();
      params.set('limit', String(this.limit || 50));
      if (this.kindFilter) params.set('kind', this.kindFilter);
      if (this.tenantFilter) params.set('tenant', this.tenantFilter);
      try {
        const resp = await fetch('/api/admin/slow-queries?' + params, { headers });
        if (!resp.ok) {
          this.err = await explainFetchFailure(resp);
          this.rows = [];
          return;
        }
        this.rows = await resp.json();
        this.err = '';
      } catch (e) {
        this.err = String(e);
      }
    },
  };
}

function auditPanel() {
  return {
    rows: [],
    err: '',
    pathPrefix: '',
    actor: '',
    limit: 50,

    async load(headers) {
      const params = new URLSearchParams();
      params.set('limit', String(this.limit || 50));
      if (this.pathPrefix) params.set('path_prefix', this.pathPrefix);
      if (this.actor) params.set('actor_id', this.actor);
      try {
        const resp = await fetch('/api/admin/audit?' + params, { headers });
        if (!resp.ok) {
          this.err = await explainFetchFailure(resp);
          this.rows = [];
          return;
        }
        this.rows = await resp.json();
        this.err = '';
      } catch (e) {
        this.err = String(e);
      }
    },
  };
}

function tokensPanel() {
  return {
    rows: [],
    err: '',
    newDesc: '',
    newScope: 'admin',
    lastPlaintext: '',

    async load(headers) {
      try {
        const resp = await fetch('/api/admin/tokens', { headers });
        if (!resp.ok) {
          this.err = await explainFetchFailure(resp);
          this.rows = [];
          return;
        }
        this.rows = await resp.json();
        this.err = '';
      } catch (e) {
        this.err = String(e);
      }
    },

    async mint(headers) {
      try {
        const resp = await fetch('/api/admin/tokens', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', ...headers },
          body: JSON.stringify({ description: this.newDesc, scope: this.newScope }),
        });
        if (!resp.ok) {
          this.err = await explainFetchFailure(resp);
          return;
        }
        const body = await resp.json();
        this.lastPlaintext = body.token;
        this.newDesc = '';
        await this.load(headers);
      } catch (e) {
        this.err = String(e);
      }
    },

    async revoke(id, headers) {
      if (!confirm(`Revoke token ${id}?`)) return;
      try {
        const resp = await fetch('/api/admin/tokens/' + encodeURIComponent(id), {
          method: 'DELETE',
          headers,
        });
        if (!resp.ok && resp.status !== 204) {
          this.err = await explainFetchFailure(resp);
          return;
        }
        await this.load(headers);
      } catch (e) {
        this.err = String(e);
      }
    },
  };
}

function quotasPanel() {
  return {
    rows: [],
    err: '',
    tenantInput: '',
    qpsInput: null,
    burstInput: null,

    async load(headers) {
      try {
        const resp = await fetch('/api/admin/tenant-quotas', { headers });
        if (!resp.ok) {
          this.err = await explainFetchFailure(resp);
          this.rows = [];
          return;
        }
        this.rows = await resp.json();
        this.err = '';
      } catch (e) {
        this.err = String(e);
      }
    },

    async put(headers) {
      if (!this.tenantInput || !this.qpsInput || !this.burstInput) {
        this.err = 'fill tenant, max qps, and burst';
        return;
      }
      try {
        const resp = await fetch(
          '/api/admin/tenant-quotas/' + encodeURIComponent(this.tenantInput),
          {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json', ...headers },
            body: JSON.stringify({ max_qps: this.qpsInput, burst: this.burstInput }),
          },
        );
        if (!resp.ok) {
          this.err = await explainFetchFailure(resp);
          return;
        }
        this.tenantInput = '';
        this.qpsInput = null;
        this.burstInput = null;
        await this.load(headers);
      } catch (e) {
        this.err = String(e);
      }
    },

    async clear(tenantId, headers) {
      if (!confirm(`Clear quota for ${tenantId}?`)) return;
      try {
        const resp = await fetch(
          '/api/admin/tenant-quotas/' + encodeURIComponent(tenantId),
          { method: 'DELETE', headers },
        );
        if (!resp.ok && resp.status !== 204) {
          this.err = await explainFetchFailure(resp);
          return;
        }
        await this.load(headers);
      } catch (e) {
        this.err = String(e);
      }
    },
  };
}
