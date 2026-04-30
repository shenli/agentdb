from dataclasses import dataclass
import asyncio

from openai_agents_agentdb import AgentDBSession
from openai_agents_agentdb.session import ITEM_ADDED, ITEM_POPPED, SESSION_CLEARED


@dataclass
class Event:
    kind: str
    payload: dict


class FakeAsyncSession:
    def __init__(self):
        self.events = []

    async def append(self, payload, *, kind, idempotency_key):
        assert idempotency_key.startswith("openai-agents:")
        self.events.append(Event(kind, payload))

    async def replay(self, *, limit):
        return self.events[-limit:]


class FakeAsyncClient:
    def __init__(self):
        self.fake_session = FakeAsyncSession()

    def session(self, tenant_id, session_id, *, agent_id=None):
        self.session_args = (tenant_id, session_id, agent_id)
        return self.fake_session


def test_add_and_get_items_round_trip_through_agentdb_events():
    asyncio.run(_test_add_and_get_items_round_trip_through_agentdb_events())


async def _test_add_and_get_items_round_trip_through_agentdb_events():
    client = FakeAsyncClient()
    session = AgentDBSession(client, tenant_id="ten_1", agent_id="agt_1", session_id="ses_1")

    await session.add_items([{"role": "user", "content": "hello"}])

    assert client.fake_session.events[0].kind == ITEM_ADDED
    assert await session.get_items() == [{"role": "user", "content": "hello"}]
    assert client.session_args == ("ten_1", "ses_1", "agt_1")


def test_pop_item_appends_tombstone_and_materializes_view():
    asyncio.run(_test_pop_item_appends_tombstone_and_materializes_view())


async def _test_pop_item_appends_tombstone_and_materializes_view():
    client = FakeAsyncClient()
    session = AgentDBSession(client, tenant_id="ten_1", agent_id="agt_1", session_id="ses_1")
    await session.add_items([
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "hi"},
    ])

    popped = await session.pop_item()

    assert popped == {"role": "assistant", "content": "hi"}
    assert client.fake_session.events[-1].kind == ITEM_POPPED
    assert await session.get_items() == [{"role": "user", "content": "hello"}]


def test_clear_session_appends_clear_event():
    asyncio.run(_test_clear_session_appends_clear_event())


async def _test_clear_session_appends_clear_event():
    client = FakeAsyncClient()
    session = AgentDBSession(client, tenant_id="ten_1", agent_id="agt_1", session_id="ses_1")
    await session.add_items([{"role": "user", "content": "hello"}])

    await session.clear_session()

    assert client.fake_session.events[-1].kind == SESSION_CLEARED
    assert await session.get_items() == []
