"""Capability tests (§12.3)."""

import logging
import pickle
import uuid as _uuid

import pytest

from cross_tool_awareness import (
    BearerCapability,
    PresenceExpiringSoonError,
    issue_capability,
    revoke_capability,
    rotate_capability,
    verify_capability,
)
from cross_tool_awareness.registry import register_session


def _u() -> str:
    return str(_uuid.uuid4())


def _fixed_entropy(seed: bytes):
    """Return an entropy source that yields deterministic bytes."""
    def _src(n):
        assert n == 32
        return (seed * ((n // len(seed)) + 1))[:n]
    return _src


def test_bearer_rejects_wrong_length():
    with pytest.raises(ValueError):
        BearerCapability(b"\x00" * 31)
    with pytest.raises(ValueError):
        BearerCapability(b"\x00" * 33)


def test_bearer_rejects_non_bytes():
    with pytest.raises(TypeError):
        BearerCapability("x" * 32)


def test_bearer_repr_redacted():
    cap = BearerCapability(b"\xff" * 32)
    assert repr(cap) == "<BearerCapability redacted>"
    assert str(cap) == "<BearerCapability redacted>"
    assert "\xff" not in repr(cap)


def test_bearer_refuses_pickle():
    cap = BearerCapability(b"\xaa" * 32)
    with pytest.raises(TypeError):
        pickle.dumps(cap)


def test_issue_and_verify_roundtrip(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    cap = issue_capability(
        db, session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy(b"\x01"),
    )
    decision = verify_capability(
        db, cap.present(),
        expected_sender_uuid=p.session_uuid,
        expected_incarnation=p.session_incarnation_id,
        expected_fence=p.registration_fence,
        now=1_000_020,
    )
    assert decision.allowed is True
    assert decision.reason == "ok"


def test_capability_expiry_bounded_by_presence(db, make_presence_fn):
    presence_expires = 1_000_000 + 60
    p = make_presence_fn(
        started_at=1_000_000,
        heartbeat_at=1_000_010,
        expires_at=presence_expires,
    )
    register_session(db, p, now=1_000_000)
    cap = issue_capability(
        db, session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy(b"\x02"),
        requested_ttl_seconds=3600,
    )
    row = db.execute(
        "SELECT expires_at FROM session_capabilities "
        "WHERE sender_session_uuid=?",
        (p.session_uuid,),
    ).fetchone()
    assert row["expires_at"] == presence_expires


def test_issue_raises_when_presence_expires_soon(db, make_presence_fn):
    """With ttl=0 and presence barely alive, expires_at = min(now, presence.expires_at)
    = now, which triggers the defensive PresenceExpiringSoonError."""
    p = make_presence_fn(
        started_at=1_000_000,
        heartbeat_at=1_000_010,
        expires_at=1_000_020,
    )
    register_session(db, p, now=1_000_000)
    with pytest.raises(PresenceExpiringSoonError):
        issue_capability(
            db, session_uuid=p.session_uuid,
            session_incarnation_id=p.session_incarnation_id,
            now=1_000_015,
            entropy_source=_fixed_entropy(b"\x03"),
            requested_ttl_seconds=0,
        )


@pytest.mark.parametrize("bad_field,reason", [
    ("sender", "sender_mismatch"),
    ("incarnation", "incarnation_mismatch"),
    ("fence", "fence_mismatch"),
])
def test_verify_rejects_mismatches(db, make_presence_fn, bad_field, reason):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    cap = issue_capability(
        db, session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy(b"\x04"),
    )
    kwargs = dict(
        expected_sender_uuid=p.session_uuid,
        expected_incarnation=p.session_incarnation_id,
        expected_fence=p.registration_fence,
        now=1_000_020,
    )
    if bad_field == "sender":
        kwargs["expected_sender_uuid"] = _u()
    elif bad_field == "incarnation":
        kwargs["expected_incarnation"] = _u()
    elif bad_field == "fence":
        kwargs["expected_fence"] = "wrong-fence"
    decision = verify_capability(db, cap.present(), **kwargs)
    assert decision.allowed is False
    assert decision.reason == reason


def test_verify_expired(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    cap = issue_capability(
        db, session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy(b"\x05"),
        requested_ttl_seconds=30,
    )
    decision = verify_capability(
        db, cap.present(),
        expected_sender_uuid=p.session_uuid,
        expected_incarnation=p.session_incarnation_id,
        expected_fence=p.registration_fence,
        now=1_000_010 + 100,  # past expiry
    )
    assert decision.allowed is False
    assert decision.reason == "expired"


def test_verify_revoked(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    cap = issue_capability(
        db, session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy(b"\x06"),
    )
    revoke_capability(db, cap.verifier_sha256(), now=1_000_020)
    decision = verify_capability(
        db, cap.present(),
        expected_sender_uuid=p.session_uuid,
        expected_incarnation=p.session_incarnation_id,
        expected_fence=p.registration_fence,
        now=1_000_030,
    )
    assert decision.allowed is False
    assert decision.reason == "revoked"


def test_verify_no_match(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    decision = verify_capability(
        db, b"\x00" * 32,
        expected_sender_uuid=p.session_uuid,
        expected_incarnation=p.session_incarnation_id,
        expected_fence=p.registration_fence,
        now=1_000_020,
    )
    assert decision.allowed is False
    assert decision.reason == "no_match"


def test_rotate_revokes_old_issues_new(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    old = issue_capability(
        db, session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy(b"\x10"),
    )
    new = rotate_capability(
        db, old.present(),
        expected_sender_uuid=p.session_uuid,
        expected_incarnation=p.session_incarnation_id,
        expected_fence=p.registration_fence,
        now=1_000_020,
        entropy_source=_fixed_entropy(b"\x11"),
    )
    # Old is revoked
    old_dec = verify_capability(
        db, old.present(),
        expected_sender_uuid=p.session_uuid,
        expected_incarnation=p.session_incarnation_id,
        expected_fence=p.registration_fence,
        now=1_000_030,
    )
    assert old_dec.reason == "revoked"
    # New is valid
    new_dec = verify_capability(
        db, new.present(),
        expected_sender_uuid=p.session_uuid,
        expected_incarnation=p.session_incarnation_id,
        expected_fence=p.registration_fence,
        now=1_000_030,
    )
    assert new_dec.allowed is True


def test_revoke_is_idempotent(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    cap = issue_capability(
        db, session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy(b"\x20"),
    )
    revoke_capability(db, cap.verifier_sha256(), now=1_000_020)
    # Second revoke: no raise.
    revoke_capability(db, cap.verifier_sha256(), now=1_000_030)


def test_bearer_bytes_never_in_logs(db, make_presence_fn, caplog):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    seed = b"\xca\xfe" * 16
    with caplog.at_level(logging.DEBUG):
        cap = issue_capability(
            db, session_uuid=p.session_uuid,
            session_incarnation_id=p.session_incarnation_id,
            now=1_000_010,
            entropy_source=lambda n: seed,
        )
        _ = verify_capability(
            db, cap.present(),
            expected_sender_uuid=p.session_uuid,
            expected_incarnation=p.session_incarnation_id,
            expected_fence=p.registration_fence,
            now=1_000_020,
        )
    for record in caplog.records:
        msg = record.getMessage()
        assert seed.hex() not in msg
        assert str(seed) not in msg
