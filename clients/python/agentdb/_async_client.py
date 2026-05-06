"""Asynchronous Python SDK for the AgentDB native API."""

from __future__ import annotations

import json as jsonlib
import uuid
from typing import Any, AsyncIterator, Awaitable, Callable, List, Optional, Sequence, Union

try:
    import httpx
except ImportError as e:
    raise ImportError(
        "AgentDB's async client requires the `httpx` extra. Install with:\n"
        "    pip install 'agentdb[async]'\n"
        "(or `pip install httpx` if you want to manage the dependency yourself.)"
    ) from e

from ._sse import parse_lines_async
from .client import (
    AgentId,
    AppendResult,
    ArtifactId,
    Event,
    RecordId,
    SessionId,
    SkillId,
    TenantId,
    WatchItem,
)
from .errors import AgentDBError, SubscriptionClosed, from_response

TokenProvider = Callable[[], Union[str, Awaitable[str]]]
_RECORD_COMMIT_SEQ_HEADER = "X-AgentDB-Record-Commit-Seq"


class AsyncClient:
    def __init__(
        self,
        base_url: str = "",
        *,
        auth: Optional[tuple] = None,
        token: Optional[str] = None,
        get_token: Optional[TokenProvider] = None,
        timeout: float = 30.0,
        _client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        if token is not None and get_token is not None:
            raise ValueError("pass `token` or `get_token`, not both")
        self.base_url = base_url.rstrip("/")
        self._auth = auth
        self._token = token
        self._get_token = get_token
        self.timeout = timeout
        self._owns_client = _client is None
        self._http = _client or httpx.AsyncClient(base_url=self.base_url, timeout=timeout)
        self._record_commit_seq_by_tenant: dict[str, int] = {}

    @property
    def authenticated(self) -> bool:
        return self._auth is not None

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._http.aclose()

    async def health_ready(self) -> bool:
        try:
            resp = await self._http.get("/health/ready")
            return resp.status_code == 200
        except httpx.HTTPError:
            return False

    async def create_tenant(self, external_key: str, policy: Optional[dict] = None) -> dict:
        return await self._request(
            "POST",
            "/api/tenants",
            json={"external_key": external_key, "policy": policy or {}},
        )

    async def get_tenant(self, tenant_id: TenantId) -> dict:
        return await self._request("GET", f"/api/tenants/{_quote(tenant_id)}")

    async def user_info(self) -> dict:
        """Return authenticated principal/workspace metadata.

        Hosted deployments should expose this endpoint so application code can
        derive the tenant scope from authentication instead of passing
        `tenant_id` through normal agent code.
        """

        return await self._request("GET", "/api/me")

    async def list_tenants(self) -> list:
        data = await self._request("GET", "/api/tenants")
        return data.get("tenants", [])

    async def create_agent(
        self, tenant_id: TenantId, *, alias: Optional[str] = None, config: Optional[dict] = None
    ) -> dict:
        return await self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/agents",
            json={"alias": alias, "config": config or {}},
        )

    async def list_agents(self, tenant_id: TenantId) -> list:
        data = await self._request("GET", f"/api/tenants/{_quote(tenant_id)}/agents")
        return data.get("agents", [])

    async def create_session(
        self, tenant_id: TenantId, agent_id: AgentId, *, alias: Optional[str] = None
    ) -> "AsyncSession":
        data = await self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/agents/{_quote(agent_id)}/sessions",
            json={"alias": alias},
        )
        return AsyncSession(self, tenant_id, data["session_id"], data["agent_id"])

    async def list_sessions(self, tenant_id: TenantId) -> list:
        data = await self._request("GET", f"/api/tenants/{_quote(tenant_id)}/sessions")
        return data.get("sessions", [])

    def session(
        self,
        tenant_id: TenantId,
        session_id: Optional[SessionId] = None,
        *,
        agent_id: Optional[AgentId] = None,
    ) -> "AsyncSession":
        if session_id is None:
            session_id = tenant_id
        return AsyncSession(self, tenant_id, session_id, agent_id)

    async def put_artifact(
        self,
        tenant_id: TenantId,
        data: bytes,
        *,
        content_type: Optional[str] = None,
        sha256: Optional[str] = None,
    ) -> dict:
        headers = {}
        if content_type:
            headers["Content-Type"] = content_type
        if sha256:
            headers["X-Sha256"] = sha256
        return await self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/artifacts",
            data=data,
            headers=headers,
        )

    async def get_artifact(self, tenant_id: TenantId, artifact_id: ArtifactId) -> dict:
        return await self._request(
            "GET", f"/api/tenants/{_quote(tenant_id)}/artifacts/{_quote(artifact_id)}"
        )

    async def get_artifact_blob(self, tenant_id: TenantId, artifact_id: ArtifactId) -> bytes:
        resp = await self._raw_request(
            "GET", f"/api/tenants/{_quote(tenant_id)}/artifacts/{_quote(artifact_id)}/blob"
        )
        if 200 <= resp.status_code < 300:
            return resp.content
        raise from_response(resp.status_code, _response_body(resp))

    async def write_record(self, tenant_id: TenantId, **body: Any) -> dict:
        resp = await self._raw_request(
            "POST", f"/api/tenants/{_quote(tenant_id)}/records", json=body
        )
        data = _decode(resp)
        self._capture_record_commit_seq(tenant_id, resp)
        return data

    async def search_records(self, tenant_id: TenantId, **body: Any) -> list:
        body = self._with_record_commit_seq(tenant_id, body)
        data = await self._request(
            "POST", f"/api/tenants/{_quote(tenant_id)}/records/search", json=body
        )
        return data.get("records", [])

    async def register_skill(
        self,
        tenant_id: TenantId,
        *,
        name: str,
        version: str,
        manifest: Optional[dict] = None,
        artifact_refs: Optional[list] = None,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        return await self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/skills",
            json={
                "name": name,
                "version": version,
                "manifest": manifest or {},
                "artifact_refs": artifact_refs or [],
            },
            headers=_idem_header(idempotency_key),
        )

    async def bind_skill(
        self,
        tenant_id: TenantId,
        agent_id: AgentId,
        skill_id: SkillId,
        *,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        return await self._request(
            "POST",
            f"/api/tenants/{_quote(tenant_id)}/agents/{_quote(agent_id)}/skills/bind",
            json={"skill_id": skill_id},
            headers=_idem_header(idempotency_key),
        )

    async def watch(
        self,
        tenant_id: TenantId,
        *,
        scope: str = "tenant",
        session_id: Optional[SessionId] = None,
        agent_id: Optional[AgentId] = None,
        from_cursor: Optional[str] = None,
        limit: int = 100,
    ) -> List[WatchItem]:
        params = {"scope": scope, "limit": str(limit)}
        if session_id:
            params["session_id"] = session_id
        if agent_id:
            params["agent_id"] = agent_id
        if from_cursor:
            params["from_cursor"] = from_cursor
        data = await self._request("GET", f"/api/tenants/{_quote(tenant_id)}/watch", params=params)
        return [WatchItem.from_dict(item) for item in data]

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        resp = await self._raw_request(method, path, **kwargs)
        return _decode(resp)

    async def _raw_request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        data: Optional[bytes] = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        stream: bool = False,
    ) -> httpx.Response:
        hdrs = dict(headers or {})
        token = await self._bearer_token()
        if token is not None and "Authorization" not in hdrs:
            hdrs["Authorization"] = f"Bearer {token}"
        kwargs: dict = {"timeout": httpx.Timeout(self.timeout, read=None) if stream else self.timeout}
        if params:
            kwargs["params"] = params
        if hdrs:
            kwargs["headers"] = hdrs
        if self._auth is not None and "Authorization" not in hdrs:
            kwargs["auth"] = self._auth
        if json is not None:
            kwargs["json"] = json
        if data is not None:
            kwargs["content"] = data
        return await self._http.request(method, path, **kwargs)

    async def _stream(self, method: str, path: str, **kwargs: Any) -> AsyncIterator[httpx.Response]:
        headers = dict(kwargs.pop("headers", {}) or {})
        token = await self._bearer_token()
        if token is not None and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {token}"
        if headers:
            kwargs["headers"] = headers
        if self._auth is not None and "Authorization" not in headers:
            kwargs["auth"] = self._auth
        async with self._http.stream(method, path, **kwargs) as resp:
            yield resp

    async def _bearer_token(self) -> Optional[str]:
        if self._get_token is None:
            return self._token
        result = self._get_token()
        if hasattr(result, "__await__"):
            return await result  # type: ignore[no-any-return]
        return result  # type: ignore[return-value]

    def _capture_record_commit_seq(self, tenant_id: TenantId, resp: httpx.Response) -> None:
        raw = getattr(resp, "headers", {}).get(_RECORD_COMMIT_SEQ_HEADER)
        if not raw:
            return
        try:
            seq = int(raw)
        except ValueError:
            return
        if seq > 0:
            self._record_commit_seq_by_tenant[tenant_id] = max(
                self._record_commit_seq_by_tenant.get(tenant_id, 0), seq
            )

    def _with_record_commit_seq(self, tenant_id: TenantId, body: dict) -> dict:
        seq = self._record_commit_seq_by_tenant.get(tenant_id)
        if seq is None or "min_visible_record_commit_seq" in body:
            return body
        return {**body, "min_visible_record_commit_seq": seq}


class AsyncSession:
    def __init__(
        self,
        client: AsyncClient,
        tenant_id: TenantId,
        session_id: SessionId,
        agent_id: Optional[AgentId] = None,
    ) -> None:
        self._client = client
        self.tenant_id = tenant_id
        self.session_id = session_id
        self.agent_id = agent_id

    async def append(
        self,
        payload: Any,
        *,
        kind: str = "message",
        agent_id: Optional[AgentId] = None,
        fetch_last_n: int = 0,
        artifact_refs: Optional[list] = None,
        source_refs: Optional[list] = None,
        idempotency_key: Optional[str] = None,
    ) -> AppendResult:
        if fetch_last_n > 0:
            return await self.append_and_fetch(
                payload,
                kind=kind,
                agent_id=agent_id,
                last=fetch_last_n,
                artifact_refs=artifact_refs,
                source_refs=source_refs,
                idempotency_key=idempotency_key,
            )
        data = await self._client._request(
            "POST",
            self._events_path(),
            json=self._append_body(kind, payload, agent_id, artifact_refs, source_refs),
            headers=_idem_header(idempotency_key),
        )
        return AppendResult(event_id=data["event_id"], seq=data["seq"], recent=[])

    async def append_and_fetch(
        self,
        payload: Any,
        *,
        kind: str = "message",
        agent_id: Optional[AgentId] = None,
        last: int = 20,
        max_bytes: int = 1_000_000,
        artifact_refs: Optional[list] = None,
        source_refs: Optional[list] = None,
        idempotency_key: Optional[str] = None,
    ) -> AppendResult:
        body = self._append_body(kind, payload, agent_id, artifact_refs, source_refs)
        body["last"] = last
        body["max_bytes"] = max_bytes
        data = await self._client._request(
            "POST",
            f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/append-and-fetch",
            json=body,
            headers=_idem_header(idempotency_key),
        )
        return AppendResult.from_dict(data)

    async def fetch(self, *, from_seq: int = 0, limit: int = 100) -> List[Event]:
        data = await self._client._request(
            "GET", self._events_path(), params={"from_seq": str(from_seq), "limit": str(limit)}
        )
        return [Event.from_dict(e) for e in data]

    async def replay(self, *, from_seq: Optional[int] = None, limit: int = 100) -> List[Event]:
        params = {"limit": str(limit)}
        if from_seq is not None:
            params["from_seq"] = str(from_seq)
        data = await self._client._request(
            "GET",
            f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/replay",
            params=params,
        )
        return [Event.from_dict(e) for e in data.get("events", [])]

    async def fork(
        self, child_session_id: Optional[str] = None, *, at_seq: int = 0, alias: Optional[str] = None
    ) -> "AsyncSession":
        data = await self._client._request(
            "POST",
            f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/fork",
            json={"at_seq": at_seq, "alias": alias or child_session_id},
        )
        return AsyncSession(self._client, self.tenant_id, data["session_id"], data["agent_id"])

    async def fetch_recent(self, *, last_n: int = 10) -> List[Event]:
        events = await self.fetch(from_seq=0, limit=1000)
        return [] if last_n <= 0 else events[-last_n:]

    async def subscribe(self, *, from_seq: int = 0, limit: int = 100) -> AsyncIterator[Event]:
        path = f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/subscribe"
        kwargs = {
            "params": {"from_seq": str(from_seq), "limit": str(limit)},
            "headers": {"Accept": "text/event-stream"},
            "timeout": httpx.Timeout(self._client.timeout, read=None),
        }
        async for resp in self._client._stream("GET", path, **kwargs):
            if resp.status_code != 200:
                await resp.aread()
                raise from_response(resp.status_code, _response_body(resp))
            async for frame in parse_lines_async(resp.aiter_lines()):
                if frame.event == "error":
                    body = jsonlib.loads(frame.data) if frame.data else {}
                    raise SubscriptionClosed(body.get("error", "unknown"), last_seq=body.get("last_seq"))
                if frame.data:
                    yield Event.from_dict(jsonlib.loads(frame.data))

    async def context(
        self,
        *,
        max_events: int = 20,
        max_bytes: int = 1_000_000,
        include: Optional[Sequence[str]] = None,
        record_query: Optional[dict] = None,
    ) -> dict:
        return await self._client._request(
            "POST",
            f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/context",
            json={
                "max_events": max_events,
                "max_bytes": max_bytes,
                "include": list(include or ["recent_events"]),
                "record_query": record_query,
            },
        )

    def _append_body(
        self,
        kind: str,
        payload: Any,
        agent_id: Optional[AgentId],
        artifact_refs: Optional[list],
        source_refs: Optional[list],
    ) -> dict:
        aid = agent_id or self.agent_id
        if aid is None:
            raise ValueError("agent_id is required for append")
        return {
            "agent_id": aid,
            "kind": kind,
            "payload": payload,
            "artifact_refs": artifact_refs or [],
            "source_refs": source_refs or [],
        }

    def _events_path(self) -> str:
        return f"/api/tenants/{_quote(self.tenant_id)}/sessions/{_quote(self.session_id)}/events"


def _decode(resp: httpx.Response) -> Any:
    body = _response_body(resp)
    if 200 <= resp.status_code < 300:
        return body
    raise from_response(resp.status_code, body)


def _response_body(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return resp.text


def _quote(s: str) -> str:
    from urllib.parse import quote

    return quote(s, safe="-_.~")


def _idem_header(key: Optional[str]) -> dict:
    return {"Idempotency-Key": key or str(uuid.uuid4())}


__all__ = [
    "AsyncClient",
    "AsyncSession",
    "AppendResult",
    "Event",
    "TenantId",
    "AgentId",
    "SessionId",
    "ArtifactId",
    "RecordId",
    "SkillId",
]
