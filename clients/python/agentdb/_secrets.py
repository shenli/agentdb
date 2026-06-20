"""Client-side guards for keeping raw credentials out of event payloads."""

from __future__ import annotations

import re
from typing import Any

_RAW_SECRET_FIELD_NAMES = {
    "accesstoken",
    "apikey",
    "authtoken",
    "authorization",
    "bearertoken",
    "clientsecret",
    "openaiapikey",
    "password",
    "passwd",
    "personalaccesstoken",
    "privatekey",
    "pwd",
    "refreshtoken",
    "secret",
    "secretaccesskey",
    "token",
}

_SECRET_ASSIGNMENT_RE = re.compile(
    r"\b(api[_-]?key|access[_-]?token|auth[_-]?token|"
    r"client[_-]?secret|password|passwd|private[_-]?key|refresh[_-]?token|"
    r"secret[_-]?access[_-]?key)\b(\s*[:=]\s*)([^\s,;]+)",
    re.IGNORECASE,
)
_AUTHORIZATION_RE = re.compile(
    r"\b(authorization\s*[:=]\s*(?:Bearer\s+)?)([^\s,;]+)", re.IGNORECASE
)
_BEARER_RE = re.compile(r"\b(Bearer\s+)[A-Za-z0-9._~+/=-]{20,}\b", re.IGNORECASE)
_OPENAI_KEY_RE = re.compile(r"\bsk-[A-Za-z0-9_-]{16,}\b")
_GITHUB_TOKEN_RE = re.compile(
    r"\b(?:gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"
)
_AWS_ACCESS_KEY_RE = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")


def validate_no_raw_secret_fields(payload: Any) -> None:
    path = _first_raw_secret_field_path(payload, [], set())
    if path is not None:
        raise ValueError(
            "append payload field "
            f"`{path}` looks like a raw secret; use credential_refs for opaque "
            "vault / broker references"
        )


def sanitize_tool_output(value: Any) -> Any:
    return _sanitize_tool_output(value, set())


def _sanitize_tool_output(value: Any, seen: set[int]) -> Any:
    if isinstance(value, str):
        return _sanitize_secret_text(value)
    if isinstance(value, dict):
        obj_id = id(value)
        if obj_id in seen:
            return value
        seen.add(obj_id)
        return {key: _sanitize_tool_output(child, seen) for key, child in value.items()}
    if isinstance(value, list):
        obj_id = id(value)
        if obj_id in seen:
            return value
        seen.add(obj_id)
        return [_sanitize_tool_output(child, seen) for child in value]
    if isinstance(value, tuple):
        obj_id = id(value)
        if obj_id in seen:
            return value
        seen.add(obj_id)
        return tuple(_sanitize_tool_output(child, seen) for child in value)
    return value


def _sanitize_secret_text(text: str) -> str:
    text = _AUTHORIZATION_RE.sub(r"\1[REDACTED]", text)
    text = _BEARER_RE.sub(r"\1[REDACTED]", text)
    text = _SECRET_ASSIGNMENT_RE.sub(r"\1\2[REDACTED]", text)
    text = _OPENAI_KEY_RE.sub("[REDACTED]", text)
    text = _GITHUB_TOKEN_RE.sub("[REDACTED]", text)
    return _AWS_ACCESS_KEY_RE.sub("[REDACTED]", text)


def _first_raw_secret_field_path(
    value: Any, path: list[str], seen: set[int]
) -> str | None:
    if isinstance(value, dict):
        obj_id = id(value)
        if obj_id in seen:
            return None
        seen.add(obj_id)
        for key, child in value.items():
            key_text = str(key)
            path.append(key_text)
            if _is_raw_secret_field_name(key_text):
                return _format_path(path)
            found = _first_raw_secret_field_path(child, path, seen)
            if found is not None:
                return found
            path.pop()
    elif isinstance(value, (list, tuple)):
        obj_id = id(value)
        if obj_id in seen:
            return None
        seen.add(obj_id)
        for idx, child in enumerate(value):
            path.append(f"[{idx}]")
            found = _first_raw_secret_field_path(child, path, seen)
            if found is not None:
                return found
            path.pop()
    return None


def _format_path(path: list[str]) -> str:
    out = ""
    for part in path:
        if part.startswith("["):
            out += part
        elif out:
            out += f".{part}"
        else:
            out = part
    return out


def _is_raw_secret_field_name(key: str) -> bool:
    normalized = "".join(ch.lower() for ch in key if ch.isascii() and ch.isalnum())
    return normalized in _RAW_SECRET_FIELD_NAMES
