"""Storage API tests (v0.2.0). Covers Codex blocker #1: ReceiptStorage Protocol
implementation, fenced apply_registration_update, authenticated atomic
targeted-send."""

from __future__ import annotations

import uuid as _uuid

import pytest

from cross_tool_awareness.capability import issue_capability
from cross_tool_awareness.contracts import (
    Disposition,
    DispositionReceipt,
    NoticeKey,
    NoticeObservation,
    PresenceState,
    RegistrationUpdate,
)
from cross_tool_awareness.registry import register_session
from cross_tool_awareness.storage import (
    ReceiptStorage,
    TargetedSendResult,
    UpdateResult,
    apply_registration_update,
    create_authenticated_targeted_send,
)


def _fixed_entropy(seed: bytes):
    def _src(n):
        assert n == 32
        return (seed * ((n // len(seed)) + 1))[:n]
    return _src


def _mk_observation(message_id, target_uuid, target_incarnation, *,
                    correlation_id=None, consumer_id="test-consumer",
                    registration_fence="fence-1", expires_at=2_000_000):
    return NoticeObservation(
        key=NoticeKey(
            message_id=message_id,
            target_session_uuid=target_uuid,
            target_session_incarnation_id=target_incarnation,
        ),
        correlation_id=correlation_id or _uuid.uuid4(),
        consumer_id=consumer_id,
        registration_fence=registration_fence,
        message_expires_at=expires_at,
    )


def _mk_receipt(key: NoticeKey, *, correlation_id=None, consumer_id="test-consumer",
                disposition=Disposition.ACCEPTED, reason="", observed_at=1_000_500):
    return DispositionReceipt(
        message_id=key.message_id,
        correlation_id=correlation_id or _uuid.uuid4(),
        consumer_id=consumer_id,
        receiver_session_uuid=key.target_session_uuid,
        receiver_session_incarnation_id=key.target_session_incarnation_id,
        disposition=disposition,
        observed_at=observed_at,
        reason=reason,
    )


def _seed_live_delivery(db, *, message_id, target_uuid, target_incarnation,
                        delivery_state="notified", now=1_000_100,
                        expires_at=2_000_000):
    """Insert a bridge messages row + targeted_deliveries row so receipts have
    something to coalesce against. Returns nothing."""
    db.execute("INSERT OR IGNORE INTO messages (id) VALUES (?)", (str(message_id),))
    db.execute(
        "INSERT INTO targeted_deliveries "
        "(message_id, target_session_uuid, target_session_incarnation_id, "
        " sender_session_uuid, sender_session_incarnation_id, delivery_state, "
        " created_at, next_attempt_at, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (str(message_id), str(target_uuid), str(target_incarnation),
         str(_uuid.uuid4()), str(_uuid.uuid4()),
         delivery_state, now, now, expires_at),
    )


# ---------------------------------------------------------------------------
# ReceiptStorage.claim_notice (4 statuses)
# ---------------------------------------------------------------------------


def test_claim_notice_first_when_no_prior(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = _uuid.uuid4()
    _seed_live_delivery(db, message_id=mid,
                        target_uuid=p.session_uuid,
                        target_incarnation=p.session_incarnation_id)
    obs = _mk_observation(
        mid, p.session_uuid, p.session_incarnation_id,
        registration_fence=p.registration_fence,
    )
    storage = ReceiptStorage(db)
    claim = storage.claim_notice(obs, now=1_000_100, recovery_after=30)
    assert claim.status == "first"
    assert claim.receipt is None


def test_claim_notice_duplicate_when_fresh_claim_exists(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = _uuid.uuid4()
    _seed_live_delivery(db, message_id=mid,
                        target_uuid=p.session_uuid,
                        target_incarnation=p.session_incarnation_id)
    obs = _mk_observation(
        mid, p.session_uuid, p.session_incarnation_id,
        registration_fence=p.registration_fence,
    )
    storage = ReceiptStorage(db)
    first = storage.claim_notice(obs, now=1_000_100, recovery_after=30)
    assert first.status == "first"
    # Same observation, 10s later (within recovery_after=30)
    dup = storage.claim_notice(obs, now=1_000_110, recovery_after=30)
    assert dup.status == "duplicate"
    assert dup.receipt is None


def test_claim_notice_recovered_when_stale_claim(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = _uuid.uuid4()
    _seed_live_delivery(db, message_id=mid,
                        target_uuid=p.session_uuid,
                        target_incarnation=p.session_incarnation_id)
    obs = _mk_observation(
        mid, p.session_uuid, p.session_incarnation_id,
        registration_fence=p.registration_fence,
    )
    storage = ReceiptStorage(db)
    first = storage.claim_notice(obs, now=1_000_100, recovery_after=30)
    assert first.status == "first"
    # 100s later, past recovery_after=30
    recovered = storage.claim_notice(obs, now=1_000_200, recovery_after=30)
    assert recovered.status == "recovered"


def test_claim_notice_observed_when_receipt_exists(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = _uuid.uuid4()
    _seed_live_delivery(db, message_id=mid,
                        target_uuid=p.session_uuid,
                        target_incarnation=p.session_incarnation_id,
                        delivery_state="notified")
    key = NoticeKey(mid, p.session_uuid, p.session_incarnation_id)
    receipt = _mk_receipt(key)
    storage = ReceiptStorage(db)
    storage.record_first_disposition(key, receipt)
    obs = _mk_observation(
        mid, p.session_uuid, p.session_incarnation_id,
        registration_fence=p.registration_fence,
    )
    claim = storage.claim_notice(obs, now=1_000_600, recovery_after=30)
    assert claim.status == "observed"
    assert claim.receipt is not None
    assert claim.receipt.message_id == mid
    assert claim.receipt.disposition == Disposition.ACCEPTED


# ---------------------------------------------------------------------------
# ReceiptStorage.record_first_disposition (idempotency)
# ---------------------------------------------------------------------------


def test_record_first_disposition_inserts_new(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = _uuid.uuid4()
    _seed_live_delivery(db, message_id=mid,
                        target_uuid=p.session_uuid,
                        target_incarnation=p.session_incarnation_id)
    key = NoticeKey(mid, p.session_uuid, p.session_incarnation_id)
    receipt = _mk_receipt(key)
    storage = ReceiptStorage(db)
    stored = storage.record_first_disposition(key, receipt)
    assert stored.message_id == receipt.message_id
    assert stored.disposition == receipt.disposition


def test_record_first_disposition_returns_existing_on_conflict(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = _uuid.uuid4()
    _seed_live_delivery(db, message_id=mid,
                        target_uuid=p.session_uuid,
                        target_incarnation=p.session_incarnation_id)
    key = NoticeKey(mid, p.session_uuid, p.session_incarnation_id)
    first = _mk_receipt(key, disposition=Disposition.ACCEPTED, reason="")
    second = _mk_receipt(key, disposition=Disposition.DECLINED,
                         reason="later disposition", observed_at=1_000_700)
    storage = ReceiptStorage(db)
    stored_first = storage.record_first_disposition(key, first)
    stored_second = storage.record_first_disposition(key, second)
    # Second call returns the FIRST stored, not the second
    assert stored_second.disposition == Disposition.ACCEPTED
    assert stored_second.message_id == first.message_id


# ---------------------------------------------------------------------------
# apply_registration_update (fenced)
# ---------------------------------------------------------------------------


def test_apply_registration_update_applied(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    update = RegistrationUpdate(
        session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        registration_fence=p.registration_fence,
        state="busy",
        observed_at=1_000_020,
        expires_at=1_000_000 + 86400,
    )
    result = apply_registration_update(db, update, now=1_000_020)
    assert result.status == "applied"
    row = db.execute(
        "SELECT state, heartbeat_at FROM session_registrations WHERE session_uuid=?",
        (str(p.session_uuid),),
    ).fetchone()
    assert row["state"] == "busy"
    assert row["heartbeat_at"] == 1_000_020


def test_apply_registration_update_rejects_wrong_fence(db, make_presence_fn):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    update = RegistrationUpdate(
        session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        registration_fence="wrong-fence",
        state="busy",
        observed_at=1_000_020,
        expires_at=1_000_000 + 86400,
    )
    result = apply_registration_update(db, update, now=1_000_020)
    assert result.status == "rejected_identity_mismatch"


def test_apply_registration_update_rejects_not_found(db, make_presence_fn):
    update = RegistrationUpdate(
        session_uuid=_uuid.uuid4(),
        session_incarnation_id=_uuid.uuid4(),
        registration_fence="any",
        state="busy",
        observed_at=1_000_020,
        expires_at=1_000_000 + 86400,
    )
    result = apply_registration_update(db, update, now=1_000_020)
    assert result.status == "rejected_not_found"


def test_apply_registration_update_rejects_expired(db, make_presence_fn):
    p = make_presence_fn(started_at=100, heartbeat_at=500, expires_at=1_000)
    register_session(db, p, now=100)
    update = RegistrationUpdate(
        session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        registration_fence=p.registration_fence,
        state="busy",
        observed_at=2_000,
        expires_at=5_000,
    )
    result = apply_registration_update(db, update, now=2_000)
    assert result.status == "rejected_expired"


def test_apply_registration_update_rejects_stale_observation(db, make_presence_fn):
    p = make_presence_fn()  # heartbeat_at=1_000_010
    register_session(db, p, now=1_000_000)
    update = RegistrationUpdate(
        session_uuid=p.session_uuid,
        session_incarnation_id=p.session_incarnation_id,
        registration_fence=p.registration_fence,
        state="busy",
        observed_at=1_000_005,  # before current heartbeat
        expires_at=1_000_000 + 86400,
    )
    result = apply_registration_update(db, update, now=1_000_020)
    assert result.status == "rejected_stale_observation"


# ---------------------------------------------------------------------------
# create_authenticated_targeted_send
# ---------------------------------------------------------------------------


def test_authenticated_targeted_send_sent(db, make_presence_fn):
    sender = make_presence_fn()
    target = make_presence_fn()
    register_session(db, sender, now=1_000_000)
    register_session(db, target, now=1_000_000)
    cap = issue_capability(
        db, session_uuid=sender.session_uuid,
        session_incarnation_id=sender.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy(b"\x01"),
    )
    result = create_authenticated_targeted_send(
        db,
        message_payload="hello",
        sender_cap_bearer=cap.present(),
        sender_session_uuid=sender.session_uuid,
        sender_session_incarnation_id=sender.session_incarnation_id,
        sender_registration_fence=sender.registration_fence,
        target_session_uuid=target.session_uuid,
        target_session_incarnation_id=target.session_incarnation_id,
        correlation_id=_uuid.uuid4(),
        now=1_000_020,
        expires_at=1_000_020 + 3600,
    )
    assert result.status == "sent"
    assert result.message_id is not None
    row = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id=?",
        (result.message_id,),
    ).fetchone()
    assert row["delivery_state"] == "pending"
    msg_row = db.execute(
        "SELECT id FROM messages WHERE id=?",
        (result.message_id,),
    ).fetchone()
    assert msg_row is not None


def test_authenticated_targeted_send_rejects_invalid_bearer(db, make_presence_fn):
    sender = make_presence_fn()
    target = make_presence_fn()
    register_session(db, sender, now=1_000_000)
    register_session(db, target, now=1_000_000)
    result = create_authenticated_targeted_send(
        db,
        message_payload="hello",
        sender_cap_bearer=b"\x00" * 32,  # not a real capability
        sender_session_uuid=sender.session_uuid,
        sender_session_incarnation_id=sender.session_incarnation_id,
        sender_registration_fence=sender.registration_fence,
        target_session_uuid=target.session_uuid,
        target_session_incarnation_id=target.session_incarnation_id,
        correlation_id=_uuid.uuid4(),
        now=1_000_020,
        expires_at=1_000_020 + 3600,
    )
    assert result.status == "rejected_capability"
    cnt = db.execute("SELECT COUNT(*) AS c FROM targeted_deliveries").fetchone()
    assert cnt["c"] == 0


def test_authenticated_targeted_send_rejects_revoked_target(db, make_presence_fn):
    sender = make_presence_fn()
    target = make_presence_fn()
    register_session(db, sender, now=1_000_000)
    register_session(db, target, now=1_000_000)
    db.execute(
        "UPDATE session_registrations SET revoked_at=1_000_050 WHERE session_uuid=?",
        (str(target.session_uuid),),
    )
    cap = issue_capability(
        db, session_uuid=sender.session_uuid,
        session_incarnation_id=sender.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy(b"\x02"),
    )
    result = create_authenticated_targeted_send(
        db,
        message_payload="hello",
        sender_cap_bearer=cap.present(),
        sender_session_uuid=sender.session_uuid,
        sender_session_incarnation_id=sender.session_incarnation_id,
        sender_registration_fence=sender.registration_fence,
        target_session_uuid=target.session_uuid,
        target_session_incarnation_id=target.session_incarnation_id,
        correlation_id=_uuid.uuid4(),
        now=1_000_100,
        expires_at=1_000_100 + 3600,
    )
    assert result.status == "rejected_target_not_live"
    cnt = db.execute("SELECT COUNT(*) AS c FROM targeted_deliveries").fetchone()
    assert cnt["c"] == 0


def test_authenticated_targeted_send_rejects_duplicate_message_id(db, make_presence_fn):
    sender = make_presence_fn()
    target = make_presence_fn()
    register_session(db, sender, now=1_000_000)
    register_session(db, target, now=1_000_000)
    cap = issue_capability(
        db, session_uuid=sender.session_uuid,
        session_incarnation_id=sender.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy(b"\x03"),
    )
    fixed_id = str(_uuid.uuid4())
    r1 = create_authenticated_targeted_send(
        db, message_payload="first", sender_cap_bearer=cap.present(),
        sender_session_uuid=sender.session_uuid,
        sender_session_incarnation_id=sender.session_incarnation_id,
        sender_registration_fence=sender.registration_fence,
        target_session_uuid=target.session_uuid,
        target_session_incarnation_id=target.session_incarnation_id,
        correlation_id=_uuid.uuid4(),
        now=1_000_020, expires_at=1_000_020 + 3600,
        message_id=fixed_id,
    )
    assert r1.status == "sent"
    r2 = create_authenticated_targeted_send(
        db, message_payload="second", sender_cap_bearer=cap.present(),
        sender_session_uuid=sender.session_uuid,
        sender_session_incarnation_id=sender.session_incarnation_id,
        sender_registration_fence=sender.registration_fence,
        target_session_uuid=target.session_uuid,
        target_session_incarnation_id=target.session_incarnation_id,
        correlation_id=_uuid.uuid4(),
        now=1_000_030, expires_at=1_000_030 + 3600,
        message_id=fixed_id,
    )
    assert r2.status == "rejected_message_exists"
