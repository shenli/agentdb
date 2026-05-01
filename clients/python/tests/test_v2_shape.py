from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from agentdb import AgentDB, Client, Event


def response(status=200, body=None):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = body if body is not None else {}
    resp.text = ""
    resp.content = b""
    return resp


def capture(client):
    calls = []

    def fake(method, url, **kwargs):
        calls.append((method, url, kwargs))
        if url.endswith("/api/tenants"):
            return response(body={"tenant_id": "ten_1"})
        if url.endswith("/agents"):
            return response(body={"agent_id": "agt_1"})
        if url.endswith("/sessions"):
            return response(body={"session_id": "ses_1", "agent_id": "agt_1"})
        if url.endswith("/events"):
            return response(body={"event_id": "evt_1", "seq": 0})
        if url.endswith("/append-and-fetch"):
            return response(body={"event_id": "evt_2", "seq": 1, "recent": []})
        if url.endswith("/fork"):
            return response(body={"session_id": "ses_child", "agent_id": "agt_1"})
        if url.endswith("/replay"):
            return response(body={"events": []})
        if url.endswith("/context"):
            return response(body={"events": [], "records": [], "skills": []})
        if url.endswith("/watch"):
            return response(body=[])
        if url.endswith("/artifacts"):
            return response(body={"artifact_id": "art_1"})
        if "/records/search" in url:
            return response(body={"records": []})
        if url.endswith("/records"):
            return response(body={"record_id": "rec_1"})
        if url.endswith("/skills"):
            return response(body={"skill_id": "skl_1"})
        if url.endswith("/skills/bind"):
            return response(body={"skill_id": "skl_1"})
        return response()

    return calls, patch.object(client._http, "request", side_effect=fake)


def test_v2_urls_cover_core_flow():
    c = Client("http://x", auth=("u", "p"))
    calls, patched = capture(c)
    with patched:
        tenant = c.create_tenant("acme")
        agent = c.create_agent(tenant["tenant_id"])
        session = c.create_session(tenant["tenant_id"], agent["agent_id"])
        session.append({"hello": "world"})
        session.append_and_fetch({"hello": "again"})
        session.fork(at_seq=0, alias="branch")
        session.replay()
        session.context(include=["recent_events", "records"])
        c.watch(tenant["tenant_id"], scope="tenant")
        c.put_artifact(tenant["tenant_id"], b"blob")
        c.write_record(tenant["tenant_id"], namespace=["mem"], text="x")
        c.search_records(tenant["tenant_id"], namespace=["mem"], text="x")
        skill = c.register_skill(tenant["tenant_id"], name="tool", version="1")
        c.bind_skill(tenant["tenant_id"], agent["agent_id"], skill["skill_id"])

    urls = [url for _, url, _ in calls]
    assert "/api/tenants" in urls[0]
    assert any("/api/tenants/ten_1/agents" in url for url in urls)
    assert any("/api/tenants/ten_1/sessions/ses_1/events" in url for url in urls)
    assert any("/append-and-fetch" in url for url in urls)
    assert any("/fork" in url for url in urls)
    assert any("/replay" in url for url in urls)
    assert any("/context" in url for url in urls)
    assert any("/watch" in url for url in urls)


def test_friendly_api_binds_workspace_agent_and_session_context():
    c = Client("http://x", auth=("u", "p"))
    calls, patched = capture(c)
    with patched:
        db = AgentDB(_client=c)
        workspace = db.workspace("acme")
        agent = workspace.agent("sales-agent")
        session = agent.session("ticket-42")

        result = session.append_user("hello", fetch_last_n=5)
        artifact = session.artifacts.put(b"blob", content_type="text/plain")
        session.tool_result(
            "call_1",
            "Generated report.",
            artifacts=[artifact],
            source_refs=[{"event_id": result.event_id}],
        )
        workspace.records.write(namespace=["mem"], text="x")
        workspace.records.search(namespace=["mem"], text="x")
        skill = workspace.skills.register(name="tool", version="1")
        agent.bind_skill(skill["skill_id"])
        workspace.watch(scope="agent", agent_id=agent.id)
        recovered = agent.session(session_id=session.id)
        recovered.replay()

    urls = [url for _, url, _ in calls]
    assert urls[0].endswith("/api/tenants")
    assert any("/api/tenants/ten_1/agents" in url for url in urls)
    assert any("/api/tenants/ten_1/agents/agt_1/sessions" in url for url in urls)
    assert any("/api/tenants/ten_1/sessions/ses_1/append-and-fetch" in url for url in urls)
    assert any("/api/tenants/ten_1/artifacts" in url for url in urls)
    assert any("/api/tenants/ten_1/records" in url for url in urls)
    assert any("/api/tenants/ten_1/skills" in url for url in urls)
    assert any("/api/tenants/ten_1/watch" in url for url in urls)
    assert any("/api/tenants/ten_1/sessions/ses_1/replay" in url for url in urls)


def test_idempotency_key_is_generated_for_mutations():
    c = Client("http://x")
    calls, patched = capture(c)
    with patched:
        c.session("ten_1", "ses_1", agent_id="agt_1").append({"x": 1})
        c.register_skill("ten_1", name="tool", version="1")
    headers = [kwargs.get("headers", {}) for _, _, kwargs in calls]
    assert any("Idempotency-Key" in h for h in headers)


def test_session_append_requires_agent_id():
    with pytest.raises(ValueError):
        Client("http://x").session("ten_1", "ses_1").append({"x": 1})


def test_event_from_dict_preserves_envelope_refs_and_key_metadata():
    event = Event.from_dict(
        {
            "tenant_id": "ten_1",
            "agent_id": "agt_1",
            "session_id": "ses_1",
            "event_id": "evt_1",
            "seq": 7,
            "kind": "tool_result",
            "payload": {"tool_call_id": "call_1"},
            "schema_version": 1,
            "artifact_refs": [{"artifact_id": "art_1", "bytes": 4}],
            "source_refs": [{"event_id": "evt_call"}],
            "metadata_key_id": "tenant:ten_1:default",
            "metadata_key_version": "v3",
            "created_at_ms": 123,
        }
    )

    assert event.artifact_refs == [{"artifact_id": "art_1", "bytes": 4}]
    assert event.source_refs == [{"event_id": "evt_call"}]
    assert event.metadata_key_id == "tenant:ten_1:default"
    assert event.metadata_key_version == "v3"
    assert event.created_at_ms == 123
