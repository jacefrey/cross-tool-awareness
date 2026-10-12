"""Capability tests (§12.3) — v0.2.0: transaction-neutral helpers + codex reason tokens."""

from __future__ import annotations

import logging
import pickle
import sqlite3
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
    assert decision.reason == "accepted"


def test_capability_expiry_bounded_by_presence(db, make_presence_fn):
    presence_expires = 1_000_000 + 60
    p = make_presence_fn(
        started_at=1_000_000,
        heartbeat_at=1_000_010,
        expires_at=presence_expires,
    )
    register_session(db, p, now=1_000_000)
    _ = issue_capability(
        db, session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy(b"\x02"),
        requested_ttl_seconds=3600,
    )
    row = db.execute(
        "SELECT expires_at FROM session_capabilities "
        "WHERE sender_session_uuid=?",
        (str(p.session_uuid),),
    ).fetchone()
    assert row["expires_at"] == presence_expires


def test_issue_raises_when_presence_expires_soon(db, make_presence_fn):
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


# Codex-aligned reason tokens: identity_mismatch covers UUID/incarnation/fence
@pytest.mark.parametrize("bad_field", ["sender", "incarnation", "fence"])
def test_verify_rejects_identity_mismatch(db, make_presence_fn, bad_field):
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
    assert decision.reason == "identity_mismatch"


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
        now=1_000_010 + 100,
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


def test_verify_invalid_bearer_unknown(db, make_presence_fn):
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
    assert decision.reason == "invalid_bearer"


def test_verify_invalid_contract_wrong_length(db, make_presence_fn):
    decision = verify_capability(
        db, b"\x00" * 10,  # not 32 bytes
        expected_sender_uuid=_u(),
        expected_incarnation=_u(),
        expected_fence="f",
        now=1_000_020,
    )
    assert decision.reason == "invalid_contract"


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
    old_dec = verify_capability(
        db, old.present(),
        expected_sender_uuid=p.session_uuid,
        expected_incarnation=p.session_incarnation_id,
        expected_fence=p.registration_fence,
        now=1_000_030,
    )
    assert old_dec.reason == "revoked"
    new_dec = verify_capability(
        db, new.present(),
        expected_sender_uuid=p.session_uuid,
        expected_incarnation=p.session_incarnation_id,
        expected_fence=p.registration_fence,
        now=1_000_030,
    )
    assert new_dec.allowed is True


def test_rotate_atomic_failure_rolls_back(db, make_presence_fn, monkeypatch):
    """If issue_capability raises mid-rotation, neither the old revoke nor
    any partial new state persists (v0.2.0 B6)."""
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    old = issue_capability(
        db, session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy(b"\xa0"),
    )
    db.commit()  # commit the issuance so rotate's rollback doesn't erase it

    # Precondition: capability exists, not revoked.
    row = db.execute(
        "SELECT revoked_at FROM session_capabilities WHERE sender_session_uuid=?",
        (str(p.session_uuid),),
    ).fetchone()
    assert row["revoked_at"] is None

    # Inject failure in the entropy source so issue_capability raises AFTER
    # the revoke step inside rotate_capability.
    def failing_entropy(n):
        raise RuntimeError("entropy-failure-injected")

    with pytest.raises(RuntimeError, match="entropy-failure-injected"):
        rotate_capability(
            db, old.present(),
            expected_sender_uuid=p.session_uuid,
            expected_incarnation=p.session_incarnation_id,
            expected_fence=p.registration_fence,
            now=1_000_020,
            entropy_source=failing_entropy,
        )

    # Post: original cap still live (revoke was rolled back).
    row = db.execute(
        "SELECT revoked_at FROM session_capabilities WHERE sender_session_uuid=?",
        (str(p.session_uuid),),
    ).fetchone()
    assert row["revoked_at"] is None
    # And no second capability row exists.
    count = db.execute(
        "SELECT COUNT(*) AS c FROM session_capabilities WHERE sender_session_uuid=?",
        (str(p.session_uuid),),
    ).fetchone()["c"]
    assert count == 1


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
