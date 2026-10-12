"""Schema tests (§12.1)."""

import sqlite3
import uuid as _uuid

import pytest

from cross_tool_awareness.registry import register_session


def _u() -> str:
    return str(_uuid.uuid4())


def _insert_delivery(db, *, message_id, target_uuid, target_incarnation,
                     delivery_state="pending", now=1_000_100,
                     expires_at=1_000_000 + 3600):
    db.execute("INSERT OR IGNORE INTO messages (id) VALUES (?)", (message_id,))
    db.execute(
        "INSERT INTO targeted_deliveries "
        "(message_id, target_session_uuid, target_session_incarnation_id, "
        " sender_session_uuid, sender_session_incarnation_id, delivery_state, "
        " created_at, next_attempt_at, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (message_id, target_uuid, target_incarnation,
         _u(), _u(), delivery_state, now, now, expires_at),
    )


def _insert_registration(db, *, alias=None, revoked_at=None,
                         started_at=1_000_000, heartbeat_at=1_000_010,
                         expires_at=1_000_000 + 86400,
                         session_uuid=None, session_incarnation=None):
    sid = session_uuid or _u()
    inc = session_incarnation or _u()
    db.execute(
        "INSERT INTO session_registrations "
        "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
        " project_root, role, state, wake_capability, registration_fence, "
        " wake_locator, started_at, heartbeat_at, expires_at, revoked_at) "
        "VALUES (?, ?, ?, 'codex', 'p', '/tmp/p', 'manager', 'idle', "
        " 'codex_queue', 'f', 'l', ?, ?, ?, ?)",
        (sid, inc, alias or f"alias-{sid[:8]}", started_at, heartbeat_at,
         expires_at, revoked_at),
    )
    return sid, inc


def test_ddl_parses(db):
    rows = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    names = {r["name"] for r in rows}
    assert "session_registrations" in names
    assert "session_capabilities" in names
    assert "targeted_deliveries" in names
    assert "queue_attempts" in names
    assert "disposition_receipts" in names
    assert "dispatcher_lease" in names
    assert "awareness_schema_version" in names


def test_check_rejects_bad_enum_tool(db):
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO session_registrations "
            "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
            " project_root, role, state, wake_capability, registration_fence, "
            " wake_locator, started_at, heartbeat_at, expires_at) "
            "VALUES (?, ?, 'alias', 'martian', 'p', '/tmp/p', 'manager', 'idle', "
            " 'codex_queue', 'f', 'l', 1, 2, 10)",
            (_u(), _u()),
        )


def test_timestamp_invariant_rejected(db):
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO session_registrations "
            "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
            " project_root, role, state, wake_capability, registration_fence, "
            " wake_locator, started_at, heartbeat_at, expires_at) "
            "VALUES (?, ?, 'alias', 'codex', 'p', '/tmp/p', 'manager', 'idle', "
            " 'codex_queue', 'f', 'l', 100, 50, 200)",  # started_at > heartbeat_at
            (_u(), _u()),
        )


def test_alias_live_unique_rejects_second_live(db):
    _insert_registration(db, alias="shared")
    with pytest.raises(sqlite3.IntegrityError):
        _insert_registration(db, alias="shared")


def test_alias_reusable_after_revoke(db):
    _insert_registration(db, alias="shared", revoked_at=1_000_200)
    _insert_registration(db, alias="shared")  # should succeed


def test_register_session_retires_expired(db, make_presence_fn):
    p1 = make_presence_fn(alias="shared", expires_at=1_000_100)
    register_session(db, p1, now=1_000_000)

    p2 = make_presence_fn(alias="shared")  # same alias, new UUID/incarnation
    # Now = 1_000_500: p1 is expired at 1_000_100
    register_session(db, p2, now=1_000_500)

    rows = db.execute(
        "SELECT session_uuid, revoked_at FROM session_registrations WHERE alias='shared'"
    ).fetchall()
    # p1 should be revoked, p2 should be live
    live = [r for r in rows if r["revoked_at"] is None]
    assert len(live) == 1
    assert live[0]["session_uuid"] == str(p2.session_uuid)


def test_capability_verifier_length_trigger(db):
    sid, inc = _insert_registration(db)
    with pytest.raises(sqlite3.IntegrityError) as e:
        db.execute(
            "INSERT INTO session_capabilities "
            "(verifier_sha256, sender_session_uuid, sender_session_incarnation_id, "
            " registration_fence, issued_at, expires_at) "
            "VALUES (?, ?, ?, 'f', 1, 10)",
            (b"\x00" * 16, sid, inc),
        )
    assert "verifier_sha256 must be exactly 32 bytes" in str(e.value)


def test_receipt_requires_live_registration(db, msg):
    mid = msg()
    sid, inc = _insert_registration(db, revoked_at=1_000_050)
    _insert_delivery(db, message_id=mid, target_uuid=sid, target_incarnation=inc,
                     delivery_state="claimed")
    with pytest.raises(sqlite3.IntegrityError) as e:
        db.execute(
            "INSERT INTO disposition_receipts "
            "(message_id, target_session_uuid, target_session_incarnation_id, "
            " correlation_id, consumer_id, receiver_session_uuid, "
            " receiver_session_incarnation_id, disposition, observed_at, reason, "
            " delivered_at) VALUES (?, ?, ?, ?, 'c', ?, ?, 'accepted', 1, '', 1)",
            (mid, sid, inc, _u(), sid, inc),
        )
    assert "receiver registration not live" in str(e.value)


def test_receipt_requires_live_registration_rejects_expired(db, msg):
    mid = msg()
    sid, inc = _insert_registration(db, started_at=100, heartbeat_at=500, expires_at=1000)
    _insert_delivery(db, message_id=mid, target_uuid=sid, target_incarnation=inc,
                     delivery_state="claimed")
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO disposition_receipts "
            "(message_id, target_session_uuid, target_session_incarnation_id, "
            " correlation_id, consumer_id, receiver_session_uuid, "
            " receiver_session_incarnation_id, disposition, observed_at, reason, "
            " delivered_at) VALUES (?, ?, ?, ?, 'c', ?, ?, 'accepted', 2000, '', 1500)",
            (mid, sid, inc, _u(), sid, inc),
        )


def test_receipt_check_receiver_equals_target_rejects_mismatch(db, msg):
    mid = msg()
    sid, inc = _insert_registration(db)
    other_sid, other_inc = _insert_registration(db)
    _insert_delivery(db, message_id=mid, target_uuid=sid, target_incarnation=inc,
                     delivery_state="claimed")
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO disposition_receipts "
            "(message_id, target_session_uuid, target_session_incarnation_id, "
            " correlation_id, consumer_id, receiver_session_uuid, "
            " receiver_session_incarnation_id, disposition, observed_at, reason, "
            " delivered_at) VALUES (?, ?, ?, ?, 'c', ?, ?, 'accepted', 1, '', 1)",
            (mid, sid, inc, _u(), other_sid, other_inc),
        )


@pytest.mark.parametrize("state", ["pending", "blocked", "observed", "undeliverable"])
def test_receipt_rejected_for_ineligible_delivery_state(db, msg, state):
    mid = msg()
    sid, inc = _insert_registration(db)
    _insert_delivery(db, message_id=mid, target_uuid=sid, target_incarnation=inc,
                     delivery_state=state)
    with pytest.raises(sqlite3.IntegrityError) as e:
        db.execute(
            "INSERT INTO disposition_receipts "
            "(message_id, target_session_uuid, target_session_incarnation_id, "
            " correlation_id, consumer_id, receiver_session_uuid, "
            " receiver_session_incarnation_id, disposition, observed_at, reason, "
            " delivered_at) VALUES (?, ?, ?, ?, 'c', ?, ?, 'accepted', 1, '', 1)",
            (mid, sid, inc, _u(), sid, inc),
        )
    assert "receipt not permitted" in str(e.value)


@pytest.mark.parametrize("state", ["claimed", "notified", "ambiguous"])
def test_receipt_accepted_for_eligible_delivery_state(db, msg, state):
    mid = msg()
    sid, inc = _insert_registration(db)
    _insert_delivery(db, message_id=mid, target_uuid=sid, target_incarnation=inc,
                     delivery_state=state)
    db.execute(
        "INSERT INTO disposition_receipts "
        "(message_id, target_session_uuid, target_session_incarnation_id, "
        " correlation_id, consumer_id, receiver_session_uuid, "
        " receiver_session_incarnation_id, disposition, observed_at, reason, "
        " delivered_at) VALUES (?, ?, ?, ?, 'c', ?, ?, 'accepted', 1, '', 1)",
        (mid, sid, inc, _u(), sid, inc),
    )
    row = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "observed"


def test_tr_receipt_marks_observed_clears_claim_lease(db, msg):
    mid = msg()
    sid, inc = _insert_registration(db)
    _insert_delivery(db, message_id=mid, target_uuid=sid, target_incarnation=inc,
                     delivery_state="claimed")
    db.execute(
        "UPDATE targeted_deliveries SET claim_lease_token='stale-token', "
        "claim_lease_expires_at=9999 WHERE message_id=?",
        (mid,),
    )
    db.execute(
        "INSERT INTO disposition_receipts "
        "(message_id, target_session_uuid, target_session_incarnation_id, "
        " correlation_id, consumer_id, receiver_session_uuid, "
        " receiver_session_incarnation_id, disposition, observed_at, reason, "
        " delivered_at) VALUES (?, ?, ?, ?, 'c', ?, ?, 'accepted', 1, '', 1)",
        (mid, sid, inc, _u(), sid, inc),
    )
    row = db.execute(
        "SELECT delivery_state, claim_lease_token, claim_lease_expires_at "
        "FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "observed"
    assert row["claim_lease_token"] is None
    assert row["claim_lease_expires_at"] is None


def test_blocked_then_reconcile_retry_claim_notified_receipt_lands(db, msg):
    from cross_tool_awareness.leases import claim_delivery, finalize_attempt
    from cross_tool_awareness.reconciliation import reconcile_blocked

    mid = msg()
    sid, inc = _insert_registration(db)
    _insert_delivery(db, message_id=mid, target_uuid=sid, target_incarnation=inc,
                     delivery_state="blocked", now=1_000_100)

    # Earlier blocked-state receipt attempt would be rejected (verified separately).
    # Reconcile → pending → claim → finalize accepted → notified → receipt → observed.
    r = reconcile_blocked(db, message_id=mid, target_session_uuid=sid,
                          target_session_incarnation_id=inc,
                          outcome="retry", now=1_000_200)
    assert r.status == "reconciled"

    claim = claim_delivery(db, now=1_000_300)
    assert claim is not None

    final = finalize_attempt(
        db, claim_lease_token=claim.claim_lease_token,
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        outcome="accepted_by_cli", reason="",
        attempted_at=1_000_300, now=1_000_301, next_attempt_at=1_000_600,
    )
    assert final.status == "finalized"
    assert final.new_state == "notified"

    db.execute(
        "INSERT INTO disposition_receipts "
        "(message_id, target_session_uuid, target_session_incarnation_id, "
        " correlation_id, consumer_id, receiver_session_uuid, "
        " receiver_session_incarnation_id, disposition, observed_at, reason, "
        " delivered_at) VALUES (?, ?, ?, ?, 'c', ?, ?, 'accepted', 1_000_400, '', 1_000_400)",
        (mid, sid, inc, _u(), sid, inc),
    )
    row = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "observed"


def test_dispatcher_lease_singleton(db):
    db.execute(
        "INSERT INTO dispatcher_lease "
        "(singleton_pk, holder_token, holder_pid, holder_proc_start_ns, "
        " acquired_at, expires_at) VALUES (1, 'tok', 100, 200, 1, 100)"
    )
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO dispatcher_lease "
            "(singleton_pk, holder_token, holder_pid, holder_proc_start_ns, "
            " acquired_at, expires_at) VALUES (1, 'tok2', 101, 201, 1, 100)"
        )


def test_queue_attempts_claim_lease_token_unique(db, msg):
    mid = msg()
    sid, inc = _insert_registration(db)
    _insert_delivery(db, message_id=mid, target_uuid=sid, target_incarnation=inc,
                     delivery_state="claimed")
    db.execute(
        "INSERT INTO queue_attempts "
        "(claim_lease_token, message_id, target_session_uuid, "
        " target_session_incarnation_id, outcome, attempted_at, finished_at, reason) "
        "VALUES ('tok1', ?, ?, ?, 'accepted_by_cli', 1, 2, '')",
        (mid, sid, inc),
    )
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "INSERT INTO queue_attempts "
            "(claim_lease_token, message_id, target_session_uuid, "
            " target_session_incarnation_id, outcome, attempted_at, finished_at, reason) "
            "VALUES ('tok1', ?, ?, ?, 'rejected', 1, 2, 'other')",
            (mid, sid, inc),
        )
