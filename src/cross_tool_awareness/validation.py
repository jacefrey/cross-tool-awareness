"""Field-level validators used by contracts.py."""

import re
import uuid as _uuid


_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def uuid(value, *, field):
    try:
        _uuid.UUID(value)
    except (ValueError, AttributeError, TypeError) as exc:
        raise ValueError(f"{field}: not a valid UUID: {value!r}") from exc


def nonempty(value, *, field):
    if not isinstance(value, str) or len(value) == 0:
        raise ValueError(f"{field}: must be a non-empty string")


def enum_member(value, allowed, *, field):
    if value not in allowed:
        raise ValueError(f"{field}: {value!r} not in {sorted(allowed)!r}")


def single_line_control_free(value, *, field, min_len=1, max_len=240):
    if not isinstance(value, str):
        raise ValueError(f"{field}: must be str")
    if not (min_len <= len(value) <= max_len):
        raise ValueError(f"{field}: length {len(value)} not in [{min_len}, {max_len}]")
    if "\n" in value or "\r" in value:
        raise ValueError(f"{field}: must be single-line")
    if _CONTROL_RE.search(value):
        raise ValueError(f"{field}: must be control-char-free")


def summary_bounded(value, *, field, max_len=160):
    """Notice summary: nonempty, ≤160 chars, single-line, control-free,
    markup-free, no credential-like labels; else '[summary omitted]'."""
    if not isinstance(value, str) or len(value) == 0:
        return "[summary omitted]"
    if len(value) > max_len:
        return "[summary omitted]"
    if "\n" in value or "\r" in value:
        return "[summary omitted]"
    if _CONTROL_RE.search(value):
        return "[summary omitted]"
    # Simple markup rejection: angle brackets or markdown link syntax
    if "<" in value or ">" in value or "](" in value:
        return "[summary omitted]"
    # Credential-like labels
    lowered = value.lower()
    for token in ("password", "api_key", "api-key", "secret", "bearer", "token="):
        if token in lowered:
            return "[summary omitted]"
    return value
