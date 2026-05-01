"""Dependency-light MCP stdio server for AgentDB."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Callable, Dict, Iterable, Optional

from agentdb import Client


Json = Dict[str, Any]


@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    input_schema: Json


class AgentDBMcpServer:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.tools = {
            "agentdb_append_event": Tool(
                "agentdb_append_event",
                "Append an event to an AgentDB session.",
                object_schema(["tenant_id", "session_id", "agent_id", "kind", "payload"]),
            ),
            "agentdb_replay_session": Tool(
                "agentdb_replay_session",
                "Replay an AgentDB session, including fork lineage.",
                object_schema(["tenant_id", "session_id"]),
            ),
            "agentdb_search_records": Tool(
                "agentdb_search_records",
                "Search AgentDB records by namespace, text, tags, or source.",
                object_schema(["tenant_id"]),
            ),
            "agentdb_get_artifact": Tool(
                "agentdb_get_artifact",
                "Fetch AgentDB artifact metadata.",
                object_schema(["tenant_id", "artifact_id"]),
            ),
        }

    def handle(self, request: Json) -> Json:
        method = request.get("method")
        request_id = request.get("id")
        try:
            result = self._dispatch(method, request.get("params") or {})
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except Exception as exc:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32000, "message": str(exc)},
            }

    def _dispatch(self, method: str, params: Json) -> Json:
        handlers: Dict[str, Callable[[Json], Json]] = {
            "initialize": self.initialize,
            "tools/list": self.list_tools,
            "tools/call": self.call_tool,
            "resources/list": self.list_resources,
            "resources/read": self.read_resource,
            "prompts/list": self.list_prompts,
            "prompts/get": self.get_prompt,
        }
        if method not in handlers:
            raise ValueError(f"unsupported MCP method: {method}")
        return handlers[method](params)

    def initialize(self, _params: Json) -> Json:
        return {
            "protocolVersion": "2024-11-05",
            "serverInfo": {"name": "agentdb", "version": "0.1.0"},
            "capabilities": {"tools": {}, "resources": {}, "prompts": {}},
        }

    def list_tools(self, _params: Json) -> Json:
        return {
            "tools": [
                {
                    "name": tool.name,
                    "description": tool.description,
                    "inputSchema": tool.input_schema,
                }
                for tool in self.tools.values()
            ]
        }

    def call_tool(self, params: Json) -> Json:
        name = params.get("name")
        args = params.get("arguments") or {}
        if name == "agentdb_append_event":
            result = self._session(args).append(
                args.get("payload"),
                kind=args.get("kind", "message"),
                agent_id=args.get("agent_id"),
                artifact_refs=args.get("artifact_refs"),
                source_refs=args.get("source_refs"),
                idempotency_key=args.get("idempotency_key"),
            )
        elif name == "agentdb_replay_session":
            result = [event_to_dict(event) for event in self._session(args).replay(limit=args.get("limit", 100))]
        elif name == "agentdb_search_records":
            result = self.client.search_records(
                args["tenant_id"],
                text=args.get("text"),
                namespace=args.get("namespace") or [],
                tags=args.get("tags") or [],
                source=args.get("source"),
                limit=args.get("limit", 20),
            )
        elif name == "agentdb_get_artifact":
            result = self.client.get_artifact(args["tenant_id"], args["artifact_id"])
        else:
            raise ValueError(f"unknown tool: {name}")
        return content(result)

    def list_resources(self, _params: Json) -> Json:
        return {
            "resources": [
                {
                    "uri": "agentdb://session/{tenant_id}/{session_id}",
                    "name": "AgentDB session replay",
                    "description": "Read a session replay as JSON by replacing tenant_id and session_id.",
                    "mimeType": "application/json",
                }
            ]
        }

    def read_resource(self, params: Json) -> Json:
        uri = params.get("uri", "")
        prefix = "agentdb://session/"
        if not uri.startswith(prefix):
            raise ValueError(f"unsupported resource URI: {uri}")
        tenant_id, session_id = uri[len(prefix) :].split("/", 1)
        events = [event_to_dict(event) for event in self.client.session(tenant_id, session_id).replay(limit=1000)]
        return {"contents": [{"uri": uri, "mimeType": "application/json", "text": dump(events)}]}

    def list_prompts(self, _params: Json) -> Json:
        return {
            "prompts": [
                {
                    "name": "agentdb-session-summary",
                    "description": "Instructions for summarizing an AgentDB session replay.",
                    "arguments": [
                        {"name": "tenant_id", "required": True},
                        {"name": "session_id", "required": True},
                    ],
                }
            ]
        }

    def get_prompt(self, params: Json) -> Json:
        name = params.get("name")
        if name != "agentdb-session-summary":
            raise ValueError(f"unknown prompt: {name}")
        args = params.get("arguments") or {}
        tenant_id = args.get("tenant_id", "<tenant_id>")
        session_id = args.get("session_id", "<session_id>")
        return {
            "description": "Summarize an AgentDB session replay.",
            "messages": [
                {
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": (
                            "Replay AgentDB session "
                            f"{session_id} in tenant {tenant_id}. Summarize key decisions, "
                            "tool calls, artifacts, and unresolved follow-up work."
                        ),
                    },
                }
            ],
        }

    def _session(self, args: Json) -> Any:
        return self.client.session(args["tenant_id"], args["session_id"], agent_id=args.get("agent_id"))


def object_schema(required: Iterable[str]) -> Json:
    return {
        "type": "object",
        "required": list(required),
        "properties": {
            "tenant_id": {"type": "string"},
            "session_id": {"type": "string"},
            "agent_id": {"type": "string"},
            "artifact_id": {"type": "string"},
            "kind": {"type": "string"},
            "payload": {},
            "namespace": {"type": "array", "items": {"type": "string"}},
            "tags": {"type": "array", "items": {"type": "string"}},
            "text": {"type": "string"},
            "source": {"type": "object"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        },
    }


def event_to_dict(event: Any) -> Json:
    return {
        "tenant_id": event.tenant_id,
        "agent_id": event.agent_id,
        "session_id": event.session_id,
        "event_id": event.event_id,
        "seq": event.seq,
        "kind": event.kind,
        "payload": event.payload,
        "created_at_ms": event.created_at_ms,
    }


def content(value: Any) -> Json:
    return {"content": [{"type": "text", "text": dump(normalize(value))}]}


def dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def normalize(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, list):
        return [normalize(item) for item in value]
    if isinstance(value, tuple):
        return [normalize(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize(item) for key, item in value.items()}
    return value


def serve_stdio(server: AgentDBMcpServer, stdin: Any = sys.stdin, stdout: Any = sys.stdout) -> None:
    for line in stdin:
        if not line.strip():
            continue
        response = server.handle(json.loads(line))
        stdout.write(json.dumps(response) + "\n")
        stdout.flush()


def main() -> None:
    client = Client(
        os.environ.get("AGENTDB_BASE_URL", "http://127.0.0.1:7878"),
        token=os.environ.get("AGENTDB_TOKEN") or None,
    )
    serve_stdio(AgentDBMcpServer(client))


if __name__ == "__main__":
    main()
