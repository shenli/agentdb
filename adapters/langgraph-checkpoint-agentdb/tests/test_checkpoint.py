import asyncio
from dataclasses import dataclass

from langgraph_checkpoint_agentdb import AgentDBAsyncSaver, AgentDBSaver


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


@dataclass
class AsyncSessionRecord:
    session_id: str
    agent_id: str


class FakeAsyncSession(FakeSession):
    async def append(self, payload, *, kind, idempotency_key):
        return super().append(payload, kind=kind, idempotency_key=idempotency_key)

    async def replay(self, *, limit):
        return super().replay(limit=limit)


class FakeAsyncClient:
    def __init__(self):
        self.fake_session = FakeAsyncSession()
        self.sessions = {}
        self.records = []
        self.searches = []
        self.created = []
        self.next_session = 0

    async def create_session(self, tenant_id, agent_id, *, alias=None):
        self.created.append((tenant_id, agent_id, alias))
        self.next_session += 1
        session_id = "ses_created" if self.next_session == 1 else f"ses_created_{self.next_session}"
        self.sessions.setdefault(session_id, FakeAsyncSession())
        self.fake_session = self.sessions[session_id]
        return AsyncSessionRecord(session_id=session_id, agent_id=agent_id)

    def session(self, tenant_id, session_id, *, agent_id=None):
        self.session_args = (tenant_id, session_id, agent_id)
        self.fake_session = self.sessions.setdefault(session_id, FakeAsyncSession())
        return self.fake_session

    async def write_record(self, tenant_id, **body):
        self.records.append((tenant_id, body))
        return {"record_id": f"rec_{len(self.records)}", **body}

    async def search_records(self, tenant_id, **body):
        self.searches.append((tenant_id, body))
        for _, record in reversed(self.records):
            if record.get("namespace") != body.get("namespace"):
                continue
            wanted = set(body.get("tags") or [])
            existing = set(record.get("tags") or [])
            if wanted.issubset(existing):
                return [{"record_id": "rec_found", "metadata": record.get("metadata") or {}}]
        return []


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


def test_async_saver_creates_thread_session_and_maps_checkpoint():
    async def run():
        client = FakeAsyncClient()
        saver = AgentDBAsyncSaver(client, tenant_id="ten_1", agent_id="agt_1")
        config = {"configurable": {"thread_id": "thread-a"}}

        next_config = await saver.aput(config, {"id": "ckpt-1"}, {"step": 1})

        assert next_config["configurable"]["agentdb_session_id"] == "ses_created"
        assert client.created == [("ten_1", "agt_1", "thread-a")]
        assert client.records[0][1]["namespace"] == ["langgraph", "threads"]
        payload, kind, idem = client.fake_session.appends[0]
        assert kind == "langgraph_checkpoint"
        assert idem == "langgraph-checkpoint:thread-a::ckpt-1"
        assert payload["metadata"] == {"step": 1}

    asyncio.run(run())


def test_async_saver_isolates_multiple_threads():
    async def run():
        client = FakeAsyncClient()
        saver = AgentDBAsyncSaver(client, tenant_id="ten_1", agent_id="agt_1")

        await saver.aput(
            {"configurable": {"thread_id": "thread-a"}},
            {"id": "ckpt-a", "channel_values": {"x": "thread-a"}},
            {},
        )
        await saver.aput(
            {"configurable": {"thread_id": "thread-b"}},
            {"id": "ckpt-b", "channel_values": {"x": "thread-b"}},
            {},
        )

        row_a = await saver.aget_tuple({"configurable": {"thread_id": "thread-a"}})
        row_b = await saver.aget_tuple({"configurable": {"thread_id": "thread-b"}})

        assert row_a is not None
        assert row_b is not None
        assert row_a.checkpoint["channel_values"] == {"x": "thread-a"}
        assert row_b.checkpoint["channel_values"] == {"x": "thread-b"}

    asyncio.run(run())


def test_async_saver_aget_returns_checkpoint_only():
    async def run():
        client = FakeAsyncClient()
        saver = AgentDBAsyncSaver(client, tenant_id="ten_1", agent_id="agt_1")

        config = await saver.aput(
            {"configurable": {"thread_id": "thread-a"}},
            {"id": "ckpt-1", "channel_values": {"x": 1}},
            {},
        )

        assert await saver.aget(config) == {"id": "ckpt-1", "channel_values": {"x": 1}}

    asyncio.run(run())


def test_async_saver_recovers_thread_session_from_record():
    async def run():
        client = FakeAsyncClient()
        client.records.append(
            (
                "ten_1",
                {
                    "namespace": ["langgraph", "threads"],
                    "metadata": {"thread_id": "thread-a", "session_id": "ses_existing"},
                    "tags": ["langgraph", "thread", "thread:thread-a"],
                },
            )
        )
        saver = AgentDBAsyncSaver(client, tenant_id="ten_1", agent_id="agt_1")

        next_config = await saver.aput(
            {"configurable": {"thread_id": "thread-a"}},
            {"id": "ckpt-1"},
            {},
        )

        assert next_config["configurable"]["agentdb_session_id"] == "ses_existing"
        assert client.created == []

    asyncio.run(run())


def test_async_saver_delete_thread_hides_checkpoints():
    async def run():
        client = FakeAsyncClient()
        saver = AgentDBAsyncSaver(client, tenant_id="ten_1", agent_id="agt_1")
        config = await saver.aput({"configurable": {"thread_id": "thread-a"}}, {"id": "ckpt-1"}, {})

        rows = [row async for row in saver.alist(config, limit=10)]
        assert len(rows) == 1

        await saver.adelete_thread("thread-a")
        rows = [row async for row in saver.alist(config, limit=10)]
        assert rows == []

    asyncio.run(run())
