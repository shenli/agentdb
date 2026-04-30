"""Advisory V2 session event conventions.

The runtime accepts arbitrary event kinds and payloads. These helpers provide a
shared vocabulary for SDK examples and framework adapters without making the
server enforce a closed schema.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Mapping, Optional, Sequence


class SessionEventKind(str, Enum):
    USER_MESSAGE = "user_message"
    MODEL_MESSAGE = "model_message"
    MODEL_THOUGHT = "model_thought"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    TOOL_ERROR = "tool_error"
    ARTIFACT_CREATED = "artifact_created"
    APPROVAL_REQUESTED = "approval_requested"
    APPROVAL_RESULT = "approval_result"
    CONTEXT_PACK_BUILT = "context_pack_built"
    AGENT_STATUS = "agent_status"


def user_message(
    text: str,
    *,
    role: str = "user",
    user_id: Optional[str] = None,
    channel: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> dict:
    return _compact(
        {
            "role": role,
            "text": text,
            "user_id": user_id,
            "channel": channel,
            "metadata": dict(metadata) if metadata is not None else None,
        }
    )


def model_message(
    text: str,
    *,
    role: str = "assistant",
    model: Optional[str] = None,
    provider: Optional[str] = None,
    finish_reason: Optional[str] = None,
    usage: Optional[Mapping[str, Any]] = None,
    context_pack_id: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> dict:
    return _compact(
        {
            "role": role,
            "text": text,
            "model": model,
            "provider": provider,
            "finish_reason": finish_reason,
            "usage": dict(usage) if usage is not None else None,
            "context_pack_id": context_pack_id,
            "metadata": dict(metadata) if metadata is not None else None,
        }
    )


def model_thought(
    summary: str,
    *,
    text: Optional[str] = None,
    visibility: Optional[str] = None,
    model: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> dict:
    return _compact(
        {
            "summary": summary,
            "text": text,
            "visibility": visibility,
            "model": model,
            "metadata": dict(metadata) if metadata is not None else None,
        }
    )


def tool_call(
    tool_call_id: str,
    tool: str,
    input: Mapping[str, Any],
    *,
    hand_id: Optional[str] = None,
    timeout_ms: Optional[int] = None,
    idempotency_key: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> dict:
    return _compact(
        {
            "tool_call_id": tool_call_id,
            "tool": tool,
            "input": dict(input),
            "hand_id": hand_id,
            "timeout_ms": timeout_ms,
            "idempotency_key": idempotency_key,
            "metadata": dict(metadata) if metadata is not None else None,
        }
    )


def tool_result(
    tool_call_id: str,
    summary: str,
    *,
    status: str = "ok",
    output: Optional[Mapping[str, Any]] = None,
    duration_ms: Optional[int] = None,
    exit_code: Optional[int] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> dict:
    return _compact(
        {
            "tool_call_id": tool_call_id,
            "status": status,
            "summary": summary,
            "output": dict(output) if output is not None else None,
            "duration_ms": duration_ms,
            "exit_code": exit_code,
            "metadata": dict(metadata) if metadata is not None else None,
        }
    )


def tool_error(
    tool_call_id: str,
    summary: str,
    *,
    code: str,
    message: str,
    retryable: bool,
    duration_ms: Optional[int] = None,
    exit_code: Optional[int] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> dict:
    return _compact(
        {
            "tool_call_id": tool_call_id,
            "retryable": retryable,
            "summary": summary,
            "error": {"code": code, "message": message},
            "duration_ms": duration_ms,
            "exit_code": exit_code,
            "metadata": dict(metadata) if metadata is not None else None,
        }
    )


def artifact_created(
    artifact_id: str,
    summary: str,
    *,
    name: Optional[str] = None,
    content_type: Optional[str] = None,
    bytes: Optional[int] = None,
    sha256: Optional[str] = None,
    purpose: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> dict:
    return _compact(
        {
            "artifact_id": artifact_id,
            "summary": summary,
            "name": name,
            "content_type": content_type,
            "bytes": bytes,
            "sha256": sha256,
            "purpose": purpose,
            "metadata": dict(metadata) if metadata is not None else None,
        }
    )


def approval_requested(
    approval_id: str,
    action: str,
    summary: str,
    *,
    risk: Optional[str] = None,
    expires_at_ms: Optional[int] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> dict:
    return _compact(
        {
            "approval_id": approval_id,
            "action": action,
            "summary": summary,
            "risk": risk,
            "expires_at_ms": expires_at_ms,
            "metadata": dict(metadata) if metadata is not None else None,
        }
    )


def approval_result(
    approval_id: str,
    decision: str,
    actor: Mapping[str, Any],
    *,
    reason: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> dict:
    return _compact(
        {
            "approval_id": approval_id,
            "decision": decision,
            "actor": dict(actor),
            "reason": reason,
            "metadata": dict(metadata) if metadata is not None else None,
        }
    )


def context_pack_built(
    context_pack_id: str,
    include: Sequence[str],
    *,
    max_events: Optional[int] = None,
    max_bytes: Optional[int] = None,
    token_budget: Optional[int] = None,
    selected_event_count: Optional[int] = None,
    selected_record_count: Optional[int] = None,
    prompt_cache_key: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> dict:
    return _compact(
        {
            "context_pack_id": context_pack_id,
            "include": list(include),
            "max_events": max_events,
            "max_bytes": max_bytes,
            "token_budget": token_budget,
            "selected_event_count": selected_event_count,
            "selected_record_count": selected_record_count,
            "prompt_cache_key": prompt_cache_key,
            "metadata": dict(metadata) if metadata is not None else None,
        }
    )


def context_source_refs(context: Mapping[str, Any], *, max_refs: Optional[int] = None) -> list:
    """Build `source_refs` from a context response.

    The context pack is a derived view. These refs preserve a compact pointer
    back to the durable events, records, and skills that shaped that view.
    """

    refs = []
    for event in context.get("events") or []:
        event_id = _field(event, "event_id", "eventId")
        if event_id is not None:
            refs.append({"event_id": event_id})
    for record in context.get("records") or []:
        record_id = _field(record, "record_id", "recordId")
        if record_id is not None:
            refs.append({"record_id": record_id})
    for skill in context.get("skills") or []:
        skill_id = _field(skill, "skill_id", "skillId")
        if skill_id is not None:
            refs.append({"skill_id": skill_id})
    if max_refs is not None:
        return refs[:max_refs]
    return refs


def agent_status(
    status: str,
    *,
    summary: Optional[str] = None,
    last_seen_seq: Optional[int] = None,
    reason: Optional[str] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> dict:
    return _compact(
        {
            "status": status,
            "summary": summary,
            "last_seen_seq": last_seen_seq,
            "reason": reason,
            "metadata": dict(metadata) if metadata is not None else None,
        }
    )


def _compact(payload: Mapping[str, Any]) -> dict:
    return {key: value for key, value in payload.items() if value is not None}


def _field(value: Any, *names: str) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
        return None
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return None
