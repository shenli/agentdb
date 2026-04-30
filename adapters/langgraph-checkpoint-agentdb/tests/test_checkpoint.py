from dataclasses import dataclass

from langgraph_checkpoint_agentdb import AgentDBSaver


@dataclass
class Result:
    seq: int


@dataclass
class Event:
    kind: str
    payload: dict
    seq: int


class FakeSession:
    def __init__(self):
        self.appends = []
        self.events = []

    def append(self, payload, *, kind, idempotency_key):
        self.appends.append((payload, kind, idempotency_key))
        self.events.append(Event(kind, payload, len(self.events)))
        return Result(seq=len(self.events) - 1)

    def replay(self, *, limit):
        return self.events[-limit:]


class FakeClient:
    def __init__(self):
        self.fake_session = FakeSession()
        self.records = []

    def session(self, tenant_id, session_id, *, agent_id=None):
        self.session_args = (tenant_id, session_id, agent_id)
        return self.fake_session

    def write_record(self, tenant_id, **body):
        self.records.append((tenant_id, body))
        return {"record_id": "rec_1", **body}


def test_put_maps_checkpoint_to_event_and_record():
    client = FakeClient()
    saver = AgentDBSaver(client, tenant_id="ten_1", agent_id="agt_1")
    config = {"configurable": {"thread_id": "thread-a", "agentdb_session_id": "ses_1"}}

    next_config = saver.put(config, {"id": "ckpt-1", "values": {"x": 1}}, {"step": 1})

    assert next_config["configurable"]["checkpoint_id"] == "ckpt-1"
    payload, kind, idem = client.fake_session.appends[0]
    assert kind == "langgraph_checkpoint"
    assert idem == "langgraph-checkpoint:thread-a:ckpt-1"
    assert payload["metadata"] == {"step": 1}
    tenant_id, record = client.records[0]
    assert tenant_id == "ten_1"
    assert record["namespace"] == ["langgraph", "checkpoints"]
    assert "thread:thread-a" in record["tags"]
    assert record["source"] == {"session_id": "ses_1", "seq": 0}


def test_list_returns_latest_checkpoint_first():
    client = FakeClient()
    saver = AgentDBSaver(client, tenant_id="ten_1", agent_id="agt_1")
    config = {"configurable": {"thread_id": "thread-a", "agentdb_session_id": "ses_1"}}
    saver.put(config, {"id": "ckpt-1"}, {})
    saver.put(config, {"id": "ckpt-2"}, {})

    rows = list(saver.list(config, limit=10))

    assert [row.config["configurable"]["checkpoint_id"] for row in rows] == ["ckpt-2", "ckpt-1"]


def test_put_writes_uses_pending_writes_event():
    client = FakeClient()
    saver = AgentDBSaver(client, tenant_id="ten_1", agent_id="agt_1")
    config = {
        "configurable": {
            "thread_id": "thread-a",
            "agentdb_session_id": "ses_1",
            "checkpoint_id": "ckpt-1",
        }
    }

    saver.put_writes(config, [("channel", "value")], "task-1", "node-a")

    payload, kind, idem = client.fake_session.appends[0]
    assert kind == "langgraph_pending_writes"
    assert payload["writes"] == [("channel", "value")]
    assert idem == "langgraph-writes:thread-a:ckpt-1:task-1:node-a"
    assert client.records[0][1]["namespace"] == ["langgraph", "pending_writes"]


def test_validate_readiness_accepts_checkpoint_timeline():
    client = FakeClient()
    saver = AgentDBSaver(client, tenant_id="ten_1", agent_id="agt_1")
    config = {"configurable": {"thread_id": "thread-a", "agentdb_session_id": "ses_1"}}
    next_config = saver.put(config, {"id": "ckpt-1"}, {})
    saver.put(next_config, {"id": "ckpt-2"}, {})

    report = saver.validate_readiness(config, limit=10)

    assert report.ready is True
    assert report.event_count == 2
    assert report.checkpoint_count == 2
    assert report.last_seq == 1
    assert saver.assert_ready(config).ready is True


def test_validate_readiness_reports_missing_checkpoint_timeline():
    client = FakeClient()
    saver = AgentDBSaver(client, tenant_id="ten_1", agent_id="agt_1")
    config = {"configurable": {"thread_id": "thread-a", "agentdb_session_id": "ses_1"}}

    report = saver.validate_readiness(config)

    assert report.ready is False
    assert "langgraph_no_session_events" in report.warnings
    assert "langgraph_no_checkpoint_events" in report.warnings


def test_assert_ready_raises_on_warnings():
    client = FakeClient()
    saver = AgentDBSaver(client, tenant_id="ten_1", agent_id="agt_1")
    config = {"configurable": {"thread_id": "thread-a", "agentdb_session_id": "ses_1"}}

    try:
        saver.assert_ready(config)
    except AssertionError as exc:
        assert "langgraph_no_session_events" in str(exc)
    else:
        raise AssertionError("assert_ready should fail when readiness warnings exist")
