from agentdb import (
    SessionEventKind,
    agent_status,
    approval_requested,
    approval_result,
    artifact_created,
    context_pack_built,
    context_source_refs,
    model_message,
    model_thought,
    tool_call,
    tool_error,
    tool_result,
    user_message,
)


def test_session_event_kind_values_match_conventions():
    assert SessionEventKind.USER_MESSAGE.value == "user_message"
    assert SessionEventKind.MODEL_MESSAGE.value == "model_message"
    assert SessionEventKind.TOOL_CALL.value == "tool_call"
    assert SessionEventKind.TOOL_RESULT.value == "tool_result"
    assert SessionEventKind.AGENT_STATUS.value == "agent_status"


def test_message_payload_builders_compact_optional_fields():
    assert user_message("hello") == {"role": "user", "text": "hello"}
    assert model_message("hi", model="model-a") == {
        "role": "assistant",
        "text": "hi",
        "model": "model-a",
    }
    assert model_thought("inspect output", visibility="audit") == {
        "summary": "inspect output",
        "visibility": "audit",
    }


def test_tool_payload_builders_match_pairing_contract():
    assert tool_call("call_1", "sandbox.exec", {"cmd": "pytest"}) == {
        "tool_call_id": "call_1",
        "tool": "sandbox.exec",
        "input": {"cmd": "pytest"},
    }
    assert tool_result("call_1", "tests passed", duration_ms=12) == {
        "tool_call_id": "call_1",
        "status": "ok",
        "summary": "tests passed",
        "duration_ms": 12,
    }
    assert tool_error(
        "call_1",
        "tests timed out",
        code="TIMEOUT",
        message="deadline exceeded",
        retryable=True,
    ) == {
        "tool_call_id": "call_1",
        "retryable": True,
        "summary": "tests timed out",
        "error": {"code": "TIMEOUT", "message": "deadline exceeded"},
    }


def test_artifact_approval_context_and_status_builders():
    assert artifact_created("art_1", "stdout", content_type="text/plain") == {
        "artifact_id": "art_1",
        "summary": "stdout",
        "content_type": "text/plain",
    }
    assert approval_requested("appr_1", "send_email", "Send email") == {
        "approval_id": "appr_1",
        "action": "send_email",
        "summary": "Send email",
    }
    assert approval_result("appr_1", "approved", {"type": "user", "id": "u1"}) == {
        "approval_id": "appr_1",
        "decision": "approved",
        "actor": {"type": "user", "id": "u1"},
    }
    assert context_pack_built(
        "ctx_1",
        ["recent_events", "records"],
        max_events=20,
        selected_event_count=3,
    ) == {
        "context_pack_id": "ctx_1",
        "include": ["recent_events", "records"],
        "max_events": 20,
        "selected_event_count": 3,
    }
    assert agent_status("recovered", last_seen_seq=41) == {
        "status": "recovered",
        "last_seen_seq": 41,
    }


def test_context_source_refs_extracts_events_records_and_skills():
    context = {
        "events": [{"event_id": "evt_1"}, {"eventId": "evt_2"}, {"seq": 3}],
        "records": [{"record_id": "rec_1"}],
        "skills": [{"skill_id": "skl_1"}],
    }
    assert context_source_refs(context) == [
        {"event_id": "evt_1"},
        {"event_id": "evt_2"},
        {"record_id": "rec_1"},
        {"skill_id": "skl_1"},
    ]
    assert context_source_refs(context, max_refs=2) == [
        {"event_id": "evt_1"},
        {"event_id": "evt_2"},
    ]
