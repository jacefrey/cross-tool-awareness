"""Contract dataclass tests (§12.2)."""

import uuid as _uuid

import pytest

from cross_tool_awareness import (
    DispositionReceipt,
    PresenceRecord,
    PresenceState,
    QueueAttempt,
    QueueOutcome,
    Disposition,
    DeliveryState,
    RegistrationUpdate,
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


def test_registration_update_rejects_stale_state(make_presence_fn):
    # 'stale' is storage-derived, not an update value
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


def test_disposition_receipt_rejects_unevaluated():
    with pytest.raises(ValueError):
        DispositionReceipt(
            message_id=_u(),
            target_session_uuid=_u(),
            target_session_incarnation_id=_u(),
            correlation_id=_u(),
            consumer_id="c",
            receiver_session_uuid=_u(),
            receiver_session_incarnation_id=_u(),
            disposition="unevaluated",
            observed_at=1,
            reason="r",
            delivered_at=1,
        )


def test_disposition_receipt_rejects_receiver_target_mismatch():
    tgt_uuid = _u()
    tgt_inc = _u()
    with pytest.raises(ValueError):
        DispositionReceipt(
            message_id=_u(),
            target_session_uuid=tgt_uuid,
            target_session_incarnation_id=tgt_inc,
            correlation_id=_u(),
            consumer_id="c",
            receiver_session_uuid=_u(),           # different!
            receiver_session_incarnation_id=tgt_inc,
            disposition="accepted",
            observed_at=1,
            reason="",
            delivered_at=1,
        )


def test_enum_str_matches_wire_token():
    """StrEnum: str(PresenceState.ACTIVE) == 'active' per §5."""
    assert str(PresenceState.ACTIVE) == "active"
    assert str(PresenceState.BUSY) == "busy"
    assert str(PresenceState.IDLE) == "idle"
    assert str(PresenceState.STOPPING) == "stopping"
    assert str(PresenceState.STALE) == "stale"

    assert str(QueueOutcome.ACCEPTED_BY_CLI) == "accepted_by_cli"
    assert str(QueueOutcome.REJECTED) == "rejected"
    assert str(QueueOutcome.AMBIGUOUS_TIMEOUT) == "ambiguous_timeout"
    assert str(QueueOutcome.ADAPTER_DISABLED) == "adapter_disabled"

    assert str(Disposition.ACCEPTED) == "accepted"
    assert str(Disposition.DEFERRED) == "deferred"
    assert str(Disposition.DECLINED) == "declined"
    assert str(Disposition.NEEDS_USER) == "needs_user"

    assert str(DeliveryState.PENDING) == "pending"
    assert str(DeliveryState.CLAIMED) == "claimed"
    assert str(DeliveryState.NOTIFIED) == "notified"
    assert str(DeliveryState.AMBIGUOUS) == "ambiguous"
    assert str(DeliveryState.BLOCKED) == "blocked"
    assert str(DeliveryState.OBSERVED) == "observed"
    assert str(DeliveryState.UNDELIVERABLE) == "undeliverable"


def test_disposition_lacks_unevaluated_as_enum_member():
    """'unevaluated' is deliberately NOT in Disposition enum."""
    assert "UNEVALUATED" not in {m.name for m in Disposition}
    assert "unevaluated" not in {str(m) for m in Disposition}
