"""Contract dataclass tests (§12.2) — v0.2.0 shapes."""

from __future__ import annotations

import uuid as _uuid
from uuid import UUID

import pytest

from cross_tool_awareness import (
    CapabilityDecision,
    DeliveryState,
    Disposition,
    DispositionReceipt,
    PresenceRecord,
    PresenceState,
    QueueAttempt,
    QueueCapability,
    QueueOutcome,
    RegistrationUpdate,
    SUMMARY_OMITTED,
    validate_notice_summary,
)


def _u() -> str:
    return str(_uuid.uuid4())


def test_presence_rejects_invalid_uuid(make_presence_fn):
    with pytest.raises(ValueError):
        make_presence_fn(session_uuid="not-a-uuid")


def test_presence_rejects_bad_enum(make_presence_fn):
    with pytest.raises(ValueError):
        make_presence_fn(tool="martian")


def test_presence_rejects_bool_for_timestamp(make_presence_fn):
    with pytest.raises(ValueError):
        make_presence_fn(started_at=True)


def test_presence_rejects_timestamp_invariant_violation(make_presence_fn):
    with pytest.raises(ValueError):
        make_presence_fn(started_at=100, heartbeat_at=50, expires_at=200)


def test_presence_normalizes_uuid_to_uuid_object(make_presence_fn):
    p = make_presence_fn()
    assert isinstance(p.session_uuid, UUID)
    assert isinstance(p.session_incarnation_id, UUID)


def test_registration_update_rejects_stale_state():
    with pytest.raises(ValueError):
        RegistrationUpdate(
            session_uuid=_u(),
            session_incarnation_id=_u(),
            registration_fence="f",
            state="stale",
            observed_at=1,
            expires_at=10,
        )


def test_queue_attempt_rejects_reason_too_long():
    with pytest.raises(ValueError):
        QueueAttempt(
            message_id=_u(),
            target_session_uuid=_u(),
            target_session_incarnation_id=_u(),
            outcome="rejected",
            attempted_at=1,
            finished_at=2,
            reason="x" * 241,
        )


def test_queue_attempt_requires_empty_reason_for_accepted():
    with pytest.raises(ValueError):
        QueueAttempt(
            message_id=_u(),
            target_session_uuid=_u(),
            target_session_incarnation_id=_u(),
            outcome="accepted_by_cli",
            attempted_at=1,
            finished_at=2,
            reason="not-empty",
        )


def test_queue_attempt_rejects_control_chars_in_reason():
    with pytest.raises(ValueError):
        QueueAttempt(
            message_id=_u(),
            target_session_uuid=_u(),
            target_session_incarnation_id=_u(),
            outcome="rejected",
            attempted_at=1,
            finished_at=2,
            reason="bad\x01reason",
        )


def test_disposition_receipt_8_fields_rejects_unevaluated():
    with pytest.raises(ValueError):
        DispositionReceipt(
            message_id=_u(),
            correlation_id=_u(),
            consumer_id="c",
            receiver_session_uuid=_u(),
            receiver_session_incarnation_id=_u(),
            disposition="unevaluated",
            observed_at=1,
            reason="r",
        )


def test_disposition_receipt_matches_method():
    """8-field receipt matches() lookup used by codex-manager receiver code."""
    mid = _u()
    sid = _u()
    inc = _u()
    r = DispositionReceipt(
        message_id=mid,
        correlation_id=_u(),
        consumer_id="c",
        receiver_session_uuid=sid,
        receiver_session_incarnation_id=inc,
        disposition="accepted",
        observed_at=1,
        reason="",
    )
    assert r.matches(mid, "c", sid, inc) is True
    assert r.matches(mid, "wrong", sid, inc) is False


def test_enum_str_subclass_equals_wire_token():
    """`str, Enum` subclasses: direct equality with wire token works via str
    inheritance; .value gives the wire token; sqlite3 binding uses the
    underlying str value (verified in SQL round-trip tests elsewhere)."""
    assert PresenceState.ACTIVE == "active"
    assert PresenceState.ACTIVE.value == "active"
    assert QueueOutcome.ACCEPTED_BY_CLI.value == "accepted_by_cli"
    assert Disposition.ACCEPTED.value == "accepted"
    assert DeliveryState.PENDING.value == "pending"


def test_disposition_includes_unevaluated_but_receipt_rejects_it():
    """Full enum includes UNEVALUATED (matches codex-manager). Receipts reject it."""
    assert Disposition.UNEVALUATED.value == "unevaluated"
    with pytest.raises(ValueError):
        DispositionReceipt(
            message_id=_u(), correlation_id=_u(), consumer_id="c",
            receiver_session_uuid=_u(), receiver_session_incarnation_id=_u(),
            disposition=Disposition.UNEVALUATED, observed_at=1, reason="",
        )


def test_capability_decision_reason_tokens():
    """Codex-aligned reason tokens: accepted / invalid_contract / revoked /
    expired / identity_mismatch / invalid_bearer."""
    for reason in ("accepted", "invalid_contract", "revoked", "expired",
                   "identity_mismatch", "invalid_bearer"):
        decision = CapabilityDecision(allowed=(reason == "accepted"), reason=reason)
        assert decision.reason == reason


def test_queue_capability_shape():
    """Codex-aligned shape: supported, exact_version, help_sha256, reason."""
    q = QueueCapability(
        supported=True,
        exact_version="codex-cli 0.149.0-alpha.4.1",
        help_sha256="0" * 64,
        reason="supported",
    )
    assert q.supported is True
    q2 = QueueCapability(supported=False, exact_version=None, help_sha256="", reason="x")
    assert q2.exact_version is None


# ---- validate_notice_summary (B4 sanitizer) ----

def test_summary_sanitizer_accepts_clean():
    assert validate_notice_summary("hello world") == "hello world"


def test_summary_sanitizer_omits_control_chars():
    assert validate_notice_summary("bad\x01text") == SUMMARY_OMITTED


def test_summary_sanitizer_omits_newlines():
    assert validate_notice_summary("line1\nline2") == SUMMARY_OMITTED


def test_summary_sanitizer_omits_markup():
    assert validate_notice_summary("<b>hi</b>") == SUMMARY_OMITTED
    assert validate_notice_summary("[link](url)") == SUMMARY_OMITTED


def test_summary_sanitizer_omits_credential_labels():
    assert validate_notice_summary("here is my password") == SUMMARY_OMITTED
    assert validate_notice_summary("api_key=xxx") == SUMMARY_OMITTED
    assert validate_notice_summary("Authorization: Bearer") == SUMMARY_OMITTED


def test_summary_sanitizer_omits_oversize():
    assert validate_notice_summary("x" * 161) == SUMMARY_OMITTED
    assert validate_notice_summary("x" * 160) == "x" * 160


def test_summary_sanitizer_omits_empty_or_nonstring():
    assert validate_notice_summary("") == SUMMARY_OMITTED
    assert validate_notice_summary(None) == SUMMARY_OMITTED
