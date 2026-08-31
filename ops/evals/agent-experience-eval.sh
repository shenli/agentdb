#!/usr/bin/env bash
# Agent-experience eval suite (design-review action #7).
#
# LLMS.md is the manual an LLM agent ingests to drive AgentDB. Every claim
# in it that an agent would act on is a contract; this suite executes those
# claims against a real dev server so the manual can never silently rot.
# Each check names the LLMS.md section it validates — a failure means either
# the server broke the contract or the manual is lying, and both are
# regressions.
#
# Also runs the Python SDK quickstart (examples/agents/python/quickstart.py)
# end-to-end when python3 is available, because §4 promises that exact code
# works.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/agentdb-agent-eval.XXXXXX")"
FAILURES=0
CHECKS=0

cleanup() {
  status=$?
  kill "${SERVER_PID:-0}" 2>/dev/null || true
  if [[ ${status} -ne 0 ]]; then
    echo "FAILED: work dir preserved for inspection: ${WORK_DIR}" >&2
  else
    rm -rf "${WORK_DIR}"
  fi
}
trap cleanup EXIT

PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
BASE="http://127.0.0.1:${PORT}"

cat > "${WORK_DIR}/agentdb.toml" <<EOF
[server]
bind = "127.0.0.1:${PORT}"

[storage]
data_dir = "${WORK_DIR}/data"
EOF

cargo build -q -p agentdb-server
./target/debug/agentdb serve --config "${WORK_DIR}/agentdb.toml" \
  > "${WORK_DIR}/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 100); do
  if curl -sf "${BASE}/health/ready" > /dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

check() {
  local name="$1"; shift
  CHECKS=$((CHECKS + 1))
  if "$@" > "${WORK_DIR}/check.out" 2>&1; then
    echo "PASS  ${name}"
  else
    FAILURES=$((FAILURES + 1))
    echo "FAIL  ${name}"
    sed 's/^/      /' "${WORK_DIR}/check.out"
  fi
}

# --- §3.5: /health/ready returns 200 when the server is up -----------------
eval_health() {
  [[ "$(curl -s -o /dev/null -w '%{http_code}' "${BASE}/health/ready")" == 200 ]]
}
check "LLMS.md §3.5 /health/ready is 200" eval_health

# --- §6: every error is the flat JSON envelope -----------------------------
eval_envelope_404() {
  local body
  body="$(curl -s "${BASE}/api/tenants/ten_00000000-0000-0000-0000-000000000000")"
  python3 - "$body" <<'PY'
import json, sys
e = json.loads(sys.argv[1])
assert e["code"] == "TENANT_NOT_FOUND", e
assert isinstance(e["message"], str) and e["message"], e
assert e["retryable"] is False, e
PY
}
check "LLMS.md §6 404 carries {code,message,retryable}" eval_envelope_404

eval_envelope_malformed_body() {
  local out
  out="$(curl -s -w '\n%{http_code}' -X POST "${BASE}/api/tenants" \
    -H 'content-type: application/json' -d '{"not_a_field": 1}')"
  python3 - "$out" <<'PY'
import json, sys
body, status = sys.argv[1].rsplit("\n", 1)
assert 400 <= int(status) < 500, status
e = json.loads(body)   # the whole point: a malformed body must NOT yield plain text
assert e["code"], e
assert e["message"], e
assert e["retryable"] is False, e
PY
}
check "LLMS.md §6 malformed body is a JSON envelope, not plain text" eval_envelope_malformed_body

eval_envelope_unknown_route() {
  local out
  out="$(curl -s -w '\n%{http_code}' "${BASE}/api/definitely/not/a/route")"
  python3 - "$out" <<'PY'
import json, sys
body, status = sys.argv[1].rsplit("\n", 1)
assert int(status) == 404, status
e = json.loads(body)
assert e["code"] == "NOT_FOUND", e
PY
}
check "LLMS.md §6 unknown /api route is a JSON envelope" eval_envelope_unknown_route

# --- §6: 429 quota exhaustion carries Retry-After and retryable:true -------
eval_quota_429() {
  local tid aid out
  tid="$(curl -s -X POST "${BASE}/api/tenants" -H 'content-type: application/json' \
    -d '{"external_key": "eval-quota", "policy": {"quota": {"max_active_sessions": 1}}}' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["tenant_id"])')"
  aid="$(curl -s -X POST "${BASE}/api/tenants/${tid}/agents" -H 'content-type: application/json' \
    -d '{}' | python3 -c 'import json,sys; print(json.load(sys.stdin)["agent_id"])')"
  curl -sf -X POST "${BASE}/api/tenants/${tid}/agents/${aid}/sessions" \
    -H 'content-type: application/json' -d '{}' > /dev/null
  out="$(curl -s -D "${WORK_DIR}/quota.headers" -w '\n%{http_code}' \
    -X POST "${BASE}/api/tenants/${tid}/agents/${aid}/sessions" \
    -H 'content-type: application/json' -d '{}')"
  python3 - "$out" "${WORK_DIR}/quota.headers" <<'PY'
import json, sys
body, status = sys.argv[1].rsplit("\n", 1)
assert int(status) == 429, (status, body)
e = json.loads(body)
assert e["retryable"] is True, e
headers = open(sys.argv[2]).read().lower()
assert "retry-after:" in headers, headers
PY
}
check "LLMS.md §6 quota 429 has Retry-After + retryable:true" eval_quota_429

# --- §2/§3.1/§5: append-and-fetch, canonical ids, first-append semantics ---
eval_append_and_fetch() {
  python3 - "${BASE}" <<'PY'
import json, sys, urllib.request

base = sys.argv[1]

def call(method, path, body=None, want_headers=False):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data,
        headers={"content-type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            parsed = json.loads(raw) if raw else None
            return (parsed, dict(resp.headers)) if want_headers else parsed
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} on {method} {path}: {e.read().decode()}", file=sys.stderr)
        raise

t = call("POST", "/api/tenants", {"external_key": "eval-loop"})
assert t["tenant_id"].startswith("ten_"), t          # §5: canonical id shapes
a = call("POST", f"/api/tenants/{t['tenant_id']}/agents", {"alias": "eval-agent"})
assert a["agent_id"].startswith("agt_"), a
s = call("POST", f"/api/tenants/{t['tenant_id']}/agents/{a['agent_id']}/sessions",
         {"alias": "eval-run"})
assert s["session_id"].startswith("ses_"), s

# §3.1 + §5: one round trip writes and reads back; a fresh session returns
# exactly the event just written.
r = call("POST",
         f"/api/tenants/{t['tenant_id']}/sessions/{s['session_id']}/append-and-fetch",
         {"agent_id": a["agent_id"], "kind": "message",
          "payload": {"role": "user", "text": "checkout failed"}, "last": 5})
recent = r.get("recent", r.get("events"))
assert recent is not None, r
assert len(recent) == 1, recent                       # §5 first-append claim
assert recent[0]["payload"]["text"] == "checkout failed", recent

# Second turn sees both events.
r = call("POST",
         f"/api/tenants/{t['tenant_id']}/sessions/{s['session_id']}/append-and-fetch",
         {"agent_id": a["agent_id"], "kind": "message",
          "payload": {"role": "assistant", "text": "retrying"}, "last": 5})
recent = r.get("recent", r.get("events"))
assert len(recent) == 2, recent

# §3.2 + §5: fork inherits parent events at fork time; both keep accepting
# independent writes afterwards.
# Fork at the current head: at_seq is the seq of the last inherited event
# (inclusive; the server rejects at_seq >= next_seq).
child = call("POST",
             f"/api/tenants/{t['tenant_id']}/sessions/{s['session_id']}/fork",
             {"at_seq": recent[-1]["seq"], "alias": "eval-branch"})
child_id = child["session_id"]
assert child_id.startswith("ses_") and child_id != s["session_id"], child
call("POST", f"/api/tenants/{t['tenant_id']}/sessions/{child_id}/events",
     {"agent_id": a["agent_id"], "kind": "message",
      "payload": {"text": "branch only"}})
call("POST", f"/api/tenants/{t['tenant_id']}/sessions/{s['session_id']}/events",
     {"agent_id": a["agent_id"], "kind": "message",
      "payload": {"text": "parent only"}})
# §3.2 precise semantics: the child's own event stream holds only its own
# writes; the full inherited history (parent prefix + child) comes from
# replay. Both must hold, and the branches must not leak into each other.
child_events = call("GET",
    f"/api/tenants/{t['tenant_id']}/sessions/{child_id}/events?limit=100")
parent_events = call("GET",
    f"/api/tenants/{t['tenant_id']}/sessions/{s['session_id']}/events?limit=100")
child_texts = [e["payload"].get("text") for e in child_events]
parent_texts = [e["payload"].get("text") for e in parent_events]
assert "branch only" in child_texts and "branch only" not in parent_texts
assert "parent only" in parent_texts and "parent only" not in child_texts
assert "checkout failed" not in child_texts, child_texts  # tail is child-only
replay = call("GET",
    f"/api/tenants/{t['tenant_id']}/sessions/{child_id}/replay")
replay_texts = [e["payload"].get("text") for e in replay["events"]]
assert "checkout failed" in replay_texts, replay_texts    # inherited via replay
assert "branch only" in replay_texts, replay_texts
assert "parent only" not in replay_texts, replay_texts    # divergence holds

# §3.3: records write + text search round trip. Global (no sharding key)
# search is eventually consistent; read-your-writes is guaranteed by
# passing the commit seq the write returned
# (X-AgentDB-Record-Commit-Seq -> min_visible_record_commit_seq).
_, write_headers = call("POST", f"/api/tenants/{t['tenant_id']}/records",
     {"namespace": ["support"],
      "text": "Checkout retry is safe after inventory lock timeout.",
      "policy": {"mode": "summary_only", "vector_allowed": False}},
     want_headers=True)
commit_seq = next(v for k, v in write_headers.items()
                  if k.lower() == "x-agentdb-record-commit-seq")
hits = call("GET",
    f"/api/tenants/{t['tenant_id']}/records/search?text=checkout%20retry%20safe"
    f"&namespace=support&limit=5&min_visible_record_commit_seq={commit_seq}")
assert any("inventory lock" in (m["record"].get("text") or "")
           for m in hits["matches"]), hits

# §3.4: a context pack over the session returns provenance-carrying content.
pack = call("POST",
    f"/api/tenants/{t['tenant_id']}/sessions/{s['session_id']}/context",
    {"max_events": 20})
assert pack, "context pack call returned nothing"
print("ok")
PY
}
check "LLMS.md §2/§3/§5 agent loop: append-and-fetch, fork, records, context" eval_append_and_fetch

# --- §4/§7: the Python SDK quickstart runs as documented -------------------
eval_python_quickstart() {
  python3 -m venv "${WORK_DIR}/venv" \
    && "${WORK_DIR}/venv/bin/pip" -q install -e clients/python \
    && AGENTDB_URL="${BASE}" AGENTDB_TENANT="eval-quickstart" \
       "${WORK_DIR}/venv/bin/python" examples/agents/python/quickstart.py
}
check "LLMS.md §4/§7 Python SDK quickstart runs end-to-end" eval_python_quickstart

echo
if [[ ${FAILURES} -gt 0 ]]; then
  echo "AGENT-EXPERIENCE EVAL: ${FAILURES}/${CHECKS} checks FAILED"
  exit 1
fi
echo "AGENT-EXPERIENCE EVAL: all ${CHECKS} checks passed"
