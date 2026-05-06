from dataclasses import dataclass
import asyncio

from openai_agents_agentdb import AgentDBSession, AgentDBSessions
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
        self.tenants = []
        self.agents = []
        self.sessions = []

    def session(self, tenant_id, session_id, *, agent_id=None):
        self.session_args = (tenant_id, session_id, agent_id)
        return self.fake_session

    async def user_info(self):
        return {
            "principal_id": "usr_1",
            "default_workspace": "prod",
            "workspaces": [{"name": "prod", "tenant_id": "ten_1", "role": "writer"}],
        }

    async def list_tenants(self):
        return self.tenants

    async def create_tenant(self, external_key):
        tenant = {"tenant_id": "ten_1", "external_key": external_key}
        self.tenants.append(tenant)
        return tenant

    async def list_agents(self, tenant_id):
        self.list_agents_arg = tenant_id
        return self.agents

    async def create_agent(self, tenant_id, *, alias=None, config=None):
        agent = {"agent_id": "agt_1", "tenant_id": tenant_id, "alias": alias}
        self.agents.append(agent)
        return agent

    async def list_sessions(self, tenant_id):
        self.list_sessions_arg = tenant_id
        return self.sessions

    async def create_session(self, tenant_id, agent_id, *, alias=None):
        session = type("CreatedSession", (), {})()
        session.session_id = "ses_1"
        session.agent_id = agent_id
        self.sessions.append({"session_id": "ses_1", "agent_id": agent_id, "alias": alias})
        self.create_session_args = (tenant_id, agent_id, alias)
        return session


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


def test_session_store_resolves_tenant_from_user_info():
    asyncio.run(_test_session_store_resolves_tenant_from_user_info())


async def _test_session_store_resolves_tenant_from_user_info():
    client = FakeAsyncClient()
    store = AgentDBSessions(client, agent="support-agent")

    session = await store.session("ticket-42")
    await session.add_items([{"role": "user", "content": "hello"}])

    assert session.tenant_id == "ten_1"
    assert session.agent_id == "agt_1"
    assert session.session_id == "ses_1"
    assert client.create_session_args == ("ten_1", "agt_1", "ticket-42")
    assert client.session_args == ("ten_1", "ses_1", "agt_1")


def test_session_store_reuses_existing_agent_and_session_aliases():
    asyncio.run(_test_session_store_reuses_existing_agent_and_session_aliases())


async def _test_session_store_reuses_existing_agent_and_session_aliases():
    client = FakeAsyncClient()
    client.agents.append({"agent_id": "agt_existing", "alias": "support-agent"})
    client.sessions.append({"session_id": "ses_existing", "alias": "ticket-42"})
    store = AgentDBSessions(client, workspace="prod")

    session = await store.session("ticket-42", agent="support-agent")

    assert session.tenant_id == "ten_1"
    assert session.agent_id == "agt_existing"
    assert session.session_id == "ses_existing"
    assert not hasattr(client, "create_session_args")
