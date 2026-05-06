"""AgentDB-backed session memory for the OpenAI Agents SDK."""

from __future__ import annotations

import os
import uuid
from typing import Any, Iterable, List, Optional

from agentdb import AsyncClient
from agentdb.errors import AgentDBError, Conflict


PAYLOAD_SCHEMA_VERSION = 1
RUNTIME_VERSION = "2026-04-28"
ITEM_ADDED = "openai_agents_session_items_added"
ITEM_POPPED = "openai_agents_session_item_popped"
SESSION_CLEARED = "openai_agents_session_cleared"


class AgentDBSession:
    """Custom Agents SDK session implementation over an AgentDB session."""

    def __init__(
        self,
        client: Any,
        *,
        tenant_id: str,
        agent_id: str,
        session_id: str,
        event_limit: int = 1000,
    ) -> None:
        self.client = client
        self.tenant_id = tenant_id
        self.agent_id = agent_id
        self.session_id = session_id
        self.event_limit = event_limit

    @classmethod
    async def create(
        cls,
        client: Any,
        *,
        tenant_id: str,
        agent_id: str,
        alias: Optional[str] = None,
        event_limit: int = 1000,
    ) -> "AgentDBSession":
        session = await client.create_session(tenant_id, agent_id, alias=alias)
        return cls(
            client,
            tenant_id=tenant_id,
            agent_id=agent_id,
            session_id=session.session_id,
            event_limit=event_limit,
        )

    async def get_items(self, limit: Optional[int] = None) -> List[dict]:
        items = self._materialize(await self._events())
        return items if limit is None else items[-limit:]

    async def add_items(self, items: Iterable[dict]) -> None:
        normalized = [dict(item) for item in items]
        if not normalized:
            return
        await self._session().append(
            {
                "schema_version": PAYLOAD_SCHEMA_VERSION,
                "runtime_version": RUNTIME_VERSION,
                "session_id": self.session_id,
                "items": normalized,
            },
            kind=ITEM_ADDED,
            idempotency_key=f"openai-agents:add:{self.session_id}:{uuid.uuid4()}",
        )

    async def pop_item(self) -> Optional[dict]:
        items = await self.get_items()
        if not items:
            return None
        item = items[-1]
        await self._session().append(
            {
                "schema_version": PAYLOAD_SCHEMA_VERSION,
                "runtime_version": RUNTIME_VERSION,
                "session_id": self.session_id,
                "item": item,
            },
            kind=ITEM_POPPED,
            idempotency_key=f"openai-agents:pop:{self.session_id}:{uuid.uuid4()}",
        )
        return item

    async def clear_session(self) -> None:
        await self._session().append(
            {
                "schema_version": PAYLOAD_SCHEMA_VERSION,
                "runtime_version": RUNTIME_VERSION,
                "session_id": self.session_id,
            },
            kind=SESSION_CLEARED,
            idempotency_key=f"openai-agents:clear:{self.session_id}:{uuid.uuid4()}",
        )

    async def _events(self) -> list:
        return await self._session().replay(limit=self.event_limit)

    def _session(self) -> Any:
        return self.client.session(self.tenant_id, self.session_id, agent_id=self.agent_id)

    @staticmethod
    def _materialize(events: Iterable[Any]) -> List[dict]:
        items: List[dict] = []
        for event in events:
            payload = event.payload or {}
            if event.kind == ITEM_ADDED:
                items.extend(dict(item) for item in payload.get("items", []))
            elif event.kind == ITEM_POPPED and items:
                items.pop()
            elif event.kind == SESSION_CLEARED:
                items.clear()
        return items


class AgentDBSessions:
    """Developer-facing OpenAI Agents SDK session store.

    This hides AgentDB's canonical `tenant_id` from normal application code.
    Hosted deployments should resolve tenant scope from authentication through
    `/api/me`; local/dev deployments can fall back to a workspace alias.
    """

    def __init__(
        self,
        client: Optional[Any] = None,
        *,
        url: Optional[str] = None,
        api_key: Optional[str] = None,
        workspace: Optional[str] = None,
        agent: Optional[str] = None,
        event_limit: int = 1000,
    ) -> None:
        self.client = client or AsyncClient(
            url or os.environ.get("AGENTDB_URL", "http://127.0.0.1:7878"),
            token=api_key or os.environ.get("AGENTDB_API_KEY"),
        )
        self.workspace = workspace or os.environ.get("AGENTDB_WORKSPACE")
        self.default_agent = agent
        self.event_limit = event_limit
        self._tenant_id: Optional[str] = None
        self._agent_ids: dict[str, str] = {}

    async def session(
        self,
        session: str,
        *,
        agent: Optional[str] = None,
        event_limit: Optional[int] = None,
    ) -> AgentDBSession:
        tenant_id = await self._resolve_tenant_id()
        agent_alias = agent or self.default_agent or "default"
        agent_id = await self._resolve_agent_id(tenant_id, agent_alias)
        session_id = await self._resolve_session_id(tenant_id, agent_id, session)
        return AgentDBSession(
            self.client,
            tenant_id=tenant_id,
            agent_id=agent_id,
            session_id=session_id,
            event_limit=event_limit or self.event_limit,
        )

    async def _resolve_tenant_id(self) -> str:
        if self._tenant_id is not None:
            return self._tenant_id

        info = await self._try_user_info()
        if info is not None:
            tenant_id = _tenant_id_from_user_info(info, self.workspace)
            if tenant_id is None:
                raise ValueError("authenticated user-info did not include an allowed workspace")
            self._tenant_id = tenant_id
            return tenant_id

        if self.workspace is None:
            raise ValueError(
                "AgentDB tenant scope is implicit; configure authentication that exposes "
                "/api/me, or pass workspace=... for local/dev AgentDB servers"
            )

        self._tenant_id = await self._resolve_workspace_alias(self.workspace)
        return self._tenant_id

    async def _try_user_info(self) -> Optional[dict]:
        if not hasattr(self.client, "user_info"):
            return None
        try:
            return await self.client.user_info()
        except AgentDBError as exc:
            if exc.status_code == 404:
                return None
            raise

    async def _resolve_workspace_alias(self, workspace: str) -> str:
        if hasattr(self.client, "list_tenants"):
            for tenant in await self.client.list_tenants():
                if tenant.get("external_key") == workspace:
                    return tenant["tenant_id"]
        try:
            tenant = await self.client.create_tenant(workspace)
        except Conflict:
            for tenant in await self.client.list_tenants():
                if tenant.get("external_key") == workspace:
                    return tenant["tenant_id"]
            raise
        return tenant["tenant_id"]

    async def _resolve_agent_id(self, tenant_id: str, agent_alias: str) -> str:
        if agent_alias.startswith("agt_"):
            return agent_alias
        cached = self._agent_ids.get(agent_alias)
        if cached is not None:
            return cached
        if hasattr(self.client, "list_agents"):
            for agent in await self.client.list_agents(tenant_id):
                if agent.get("alias") == agent_alias:
                    self._agent_ids[agent_alias] = agent["agent_id"]
                    return agent["agent_id"]
        try:
            created = await self.client.create_agent(tenant_id, alias=agent_alias)
        except Conflict:
            for agent in await self.client.list_agents(tenant_id):
                if agent.get("alias") == agent_alias:
                    self._agent_ids[agent_alias] = agent["agent_id"]
                    return agent["agent_id"]
            raise
        self._agent_ids[agent_alias] = created["agent_id"]
        return created["agent_id"]

    async def _resolve_session_id(self, tenant_id: str, agent_id: str, session: str) -> str:
        if session.startswith("ses_"):
            return session
        if hasattr(self.client, "list_sessions"):
            for existing in await self.client.list_sessions(tenant_id):
                if existing.get("alias") == session:
                    return existing["session_id"]
        try:
            created = await self.client.create_session(tenant_id, agent_id, alias=session)
        except Conflict:
            for existing in await self.client.list_sessions(tenant_id):
                if existing.get("alias") == session:
                    return existing["session_id"]
            raise
        return created.session_id


def _tenant_id_from_user_info(info: dict, workspace: Optional[str]) -> Optional[str]:
    direct = info.get("tenant_id")
    if workspace is None and isinstance(direct, str):
        return direct

    workspaces = info.get("workspaces")
    if isinstance(workspaces, list):
        target = workspace or info.get("default_workspace")
        if target is None and len(workspaces) == 1:
            only = workspaces[0]
            return only.get("tenant_id") or only.get("id")
        for item in workspaces:
            if not isinstance(item, dict):
                continue
            names = {item.get("name"), item.get("workspace"), item.get("external_key")}
            if target in names:
                return item.get("tenant_id") or item.get("id")

    current = info.get("workspace")
    if isinstance(current, dict):
        if workspace is None or workspace in {
            current.get("name"),
            current.get("workspace"),
            current.get("external_key"),
        }:
            return current.get("tenant_id") or current.get("id")

    return None
