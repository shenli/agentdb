function dashboard() {
  return {
    boxes: [],
    events: [],
    totalAppends: 0,
    totalForks: 0,
    totalTenants: 0,
    _nextId: 0,

    async init() {
      await this.refreshBoxes();
      this.connectStream();
      setInterval(() => this.refreshBoxes(), 5000);
    },

    async refreshBoxes() {
      try {
        const resp = await fetch('/api/admin/boxes');
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
        case 'QuotaHit': return `box ${ev.box_id} quota ${ev.kind}`;
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
  };
}

async function demoFork() {
  await fetch('/api/tenants/t-demo/sessions/agent/append_and_fetch', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({event:{text:'hello'}, fetch_last_n:0}),
  });
  await fetch('/api/sessions/agent/fork', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({tenant_id: 't-demo', new_session_id: 'subagent-' + Date.now()}),
  });
}
async function demoFault(boxId) {
  await fetch(`/api/admin/boxes/${boxId}/inject_fault`, {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({kind:'latency', duration_s:10, latency_ms:300}),
  });
}
async function demoCold() {
  await fetch('/api/admin/tier/force', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({tenant_id:'t-demo', session_id:'agent', target:'cold'}),
  });
}
