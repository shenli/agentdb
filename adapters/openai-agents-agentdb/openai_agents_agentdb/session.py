"""AgentDB-backed session memory for the OpenAI Agents SDK."""

from __future__ import annotations

import uuid
from typing import Any, Iterable, List, Optional


SCHEMA_VERSION = "2026-04-28"
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
                "schema_version": SCHEMA_VERSION,
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
                "schema_version": SCHEMA_VERSION,
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
                "schema_version": SCHEMA_VERSION,
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
