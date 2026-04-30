from dataclasses import dataclass

from agentdb_mcp import AgentDBMcpServer


@dataclass
class Result:
    event_id: str
    seq: int
    recent: list


@dataclass
class Event:
    tenant_id: str
    agent_id: str
    session_id: str
    event_id: str
    seq: int
    kind: str
    payload: dict
    created_at_ms: int = 0


class FakeSession:
    def __init__(self):
        self.events = [
            Event("ten_1", "agt_1", "ses_1", "evt_1", 0, "message", {"text": "hello"})
        ]
        self.appends = []

    def append(self, payload, **kwargs):
        self.appends.append((payload, kwargs))
        return Result("evt_2", 1, [])

    def replay(self, *, limit):
        return self.events[-limit:]


class FakeClient:
    def __init__(self):
        self.fake_session = FakeSession()

    def session(self, tenant_id, session_id, *, agent_id=None):
        self.session_args = (tenant_id, session_id, agent_id)
        return self.fake_session

    def search_records(self, tenant_id, **body):
        self.search_args = (tenant_id, body)
        return [{"record_id": "rec_1"}]

    def get_artifact(self, tenant_id, artifact_id):
        return {"artifact_id": artifact_id, "tenant_id": tenant_id}


def test_lists_agentdb_tools():
    server = AgentDBMcpServer(FakeClient())
    response = server.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})

    names = {tool["name"] for tool in response["result"]["tools"]}

    assert "agentdb_append_event" in names
    assert "agentdb_replay_session" in names
    assert "agentdb_search_records" in names
    assert "agentdb_get_artifact" in names


def test_append_tool_maps_to_session_append():
    client = FakeClient()
    server = AgentDBMcpServer(client)

    response = server.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "agentdb_append_event",
                "arguments": {
                    "tenant_id": "ten_1",
                    "session_id": "ses_1",
                    "agent_id": "agt_1",
                    "kind": "message",
                    "payload": {"text": "hello"},
                },
            },
        }
    )

    assert response["result"]["content"][0]["type"] == "text"
    assert client.session_args == ("ten_1", "ses_1", "agt_1")
    assert client.fake_session.appends[0][1]["kind"] == "message"


def test_resource_read_replays_session():
    server = AgentDBMcpServer(FakeClient())
    response = server.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "resources/read",
            "params": {"uri": "agentdb://session/ten_1/ses_1"},
        }
    )

    assert response["result"]["contents"][0]["mimeType"] == "application/json"
    assert "evt_1" in response["result"]["contents"][0]["text"]


def test_prompt_get_returns_session_summary_prompt():
    server = AgentDBMcpServer(FakeClient())
    response = server.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "prompts/get",
            "params": {
                "name": "agentdb-session-summary",
                "arguments": {"tenant_id": "ten_1", "session_id": "ses_1"},
            },
        }
    )

    assert "ses_1" in response["result"]["messages"][0]["content"]["text"]
