"""Dispatcher tests (§12.5) — v0.2.0: notice_factory required; adapter exception → ambiguous."""

from __future__ import annotations

import uuid as _uuid

import pytest

from cross_tool_awareness import (
    SingletonDispatcherLease,
    WakeNotice,
    claim_delivery,
    finalize_attempt,
    reconcile_ambiguous,
    reconcile_blocked,
    run_one_iteration,
)
from cross_tool_awareness.registry import register_session as _register_session
from .fixtures.fake_adapter import FakeAdapter


def register_session(db, presence, *, now):
    return _register_session(
        db,
        presence,
        now=now,
        holder_pid=12_345,
        process_started_at=presence.started_at,
    )


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
        (message_id, str(target_uuid), str(target_incarnation),
         _u(), _u(), delivery_state, now, now, expires_at),
    )


def _insert_receipt(db, *, message_id, target_uuid, target_incarnation,
                    delivered_at, observed_at, disposition="accepted",
                    reason="", consumer_id="c"):
    db.execute(
        "INSERT INTO disposition_receipts "
        "(message_id, target_session_uuid, target_session_incarnation_id, "
        " correlation_id, consumer_id, receiver_session_uuid, "
        " receiver_session_incarnation_id, disposition, observed_at, reason, "
        " delivered_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (message_id, str(target_uuid), str(target_incarnation),
         _u(), consumer_id, str(target_uuid), str(target_incarnation),
         disposition, observed_at, reason, delivered_at),
    )


def _singleton_held(db, now_val=1_000_000):
    lease = SingletonDispatcherLease(db, holder_pid=100, holder_proc_start_ns=1_000)
    assert lease.acquire(now=now_val)
    return lease


def _notice_factory(delivery_row, registration_row):
    """Default test notice factory. Resolves channel + sender_alias from
    registration row (test stub) and uses an empty summary → `[summary omitted]`."""
    return WakeNotice(
        message_id=delivery_row["message_id"],
        channel="test-channel",
        sender_alias_at_send=registration_row["alias"],
        summary="",
    )


@pytest.mark.parametrize("outcome,expected_state", [
    ("accepted_by_cli", "notified"),
    ("rejected", "pending"),
    ("ambiguous_timeout", "ambiguous"),
    ("adapter_disabled", "blocked"),
])
def test_dispatcher_outcomes(db, make_presence_fn, msg, outcome, expected_state):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id)
    adapter = FakeAdapter(outcome=outcome)
    lease = _singleton_held(db)
    result = run_one_iteration(
        db, adapters={"codex_queue": adapter},
        singleton_lease=lease,
        notice_factory=_notice_factory,
        now=lambda: 1_000_100,
    )
    assert result.status == "finalized"
    assert result.outcome == outcome
    assert result.delivery_state == expected_state

    row = db.execute(
        "SELECT delivery_state, attempt_count FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == expected_state
    assert row["attempt_count"] == 1

    attempts = db.execute(
        "SELECT COUNT(*) AS c FROM queue_attempts WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert attempts["c"] == 1


def test_dispatcher_requires_notice_factory(db):
    lease = _singleton_held(db)
    with pytest.raises(TypeError, match="notice_factory"):
        run_one_iteration(
            db, adapters={"codex_queue": FakeAdapter()},
            singleton_lease=lease,
            notice_factory=None,
            now=lambda: 1_000_100,
        )


def test_dispatcher_no_work_returns_no_work(db, make_presence_fn):
    lease = _singleton_held(db)
    adapter = FakeAdapter()
    result = run_one_iteration(
        db, adapters={"codex_queue": adapter},
        singleton_lease=lease,
        notice_factory=_notice_factory,
        now=lambda: 1_000_100,
    )
    assert result.status == "no_work"


def test_lease_lost_returns_lease_lost(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id)
    lease = _singleton_held(db)
    other = SingletonDispatcherLease(db, holder_pid=999, holder_proc_start_ns=9_999)
    assert other.acquire(now=1_000_000 + 10_000)
    result = run_one_iteration(
        db, adapters={"codex_queue": FakeAdapter()},
        singleton_lease=lease,
        notice_factory=_notice_factory,
        now=lambda: 1_000_000 + 10_010,
    )
    assert result.status == "lease_lost"


def test_adapter_not_registered_pre_adapter_release_blocked(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id)
    lease = _singleton_held(db)
    result = run_one_iteration(
        db, adapters={},
        singleton_lease=lease,
        notice_factory=_notice_factory,
        now=lambda: 1_000_100,
    )
    assert result.status == "pre_adapter_released"
    assert result.diagnostic == "adapter_not_registered"
    row = db.execute(
        "SELECT delivery_state, attempt_count FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "blocked"
    assert row["attempt_count"] == 0
    cnt = db.execute("SELECT COUNT(*) AS c FROM queue_attempts").fetchone()
    assert cnt["c"] == 0


def test_adapter_not_enabled_pre_adapter_release_blocked(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id)
    adapter = FakeAdapter(enabled=False)
    lease = _singleton_held(db)
    result = run_one_iteration(
        db, adapters={"codex_queue": adapter},
        singleton_lease=lease,
        notice_factory=_notice_factory,
        now=lambda: 1_000_100,
    )
    assert result.status == "pre_adapter_released"
    assert result.diagnostic == "adapter_not_enabled"
    row = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "blocked"
    cnt = db.execute("SELECT COUNT(*) AS c FROM queue_attempts").fetchone()
    assert cnt["c"] == 0


def test_target_revalidation_fails_pre_adapter_release_pending(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id)
    db.execute(
        "UPDATE session_registrations SET revoked_at=1000050 WHERE session_uuid=?",
        (str(p.session_uuid),),
    )
    adapter = FakeAdapter()
    lease = _singleton_held(db)
    result = run_one_iteration(
        db, adapters={"codex_queue": adapter},
        singleton_lease=lease,
        notice_factory=_notice_factory,
        now=lambda: 1_000_100,
    )
    assert result.status == "pre_adapter_released"
    assert result.diagnostic == "target_not_live"
    row = db.execute(
        "SELECT delivery_state, attempt_count FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "pending"
    assert row["attempt_count"] == 0
    cnt = db.execute("SELECT COUNT(*) AS c FROM queue_attempts").fetchone()
    assert cnt["c"] == 0


def test_adapter_wake_exception_marks_ambiguous(db, make_presence_fn, msg):
    """v0.2.0 B7: adapter.wake raising → outcome='ambiguous_timeout', not 'rejected'."""
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id)
    adapter = FakeAdapter(raise_exc=RuntimeError("adapter boom"))
    lease = _singleton_held(db)
    result = run_one_iteration(
        db, adapters={"codex_queue": adapter},
        singleton_lease=lease,
        notice_factory=_notice_factory,
        now=lambda: 1_000_100,
    )
    assert result.outcome == "ambiguous_timeout"
    assert result.delivery_state == "ambiguous"
    attempts = db.execute(
        "SELECT outcome, reason FROM queue_attempts WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert attempts["outcome"] == "ambiguous_timeout"
    assert "adapter_exception" in attempts["reason"]


# ---- receipt/finalize race (v4) ----

@pytest.mark.parametrize("outcome", ["accepted_by_cli", "ambiguous_timeout"])
def test_receipt_before_finalize_clears_lease_fields(db, make_presence_fn, msg, outcome):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id)
    claim = claim_delivery(db, now=1_000_100)
    assert claim is not None

    _insert_receipt(db, message_id=mid,
                    target_uuid=p.session_uuid,
                    target_incarnation=p.session_incarnation_id,
                    delivered_at=1_000_110, observed_at=1_000_110)

    row = db.execute(
        "SELECT delivery_state, claim_lease_token, claim_lease_expires_at "
        "FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "observed"
    assert row["claim_lease_token"] is None
    assert row["claim_lease_expires_at"] is None

    reason = "" if outcome == "accepted_by_cli" else "timed out"
    r = finalize_attempt(
        db, claim_lease_token=claim.claim_lease_token,
        message_id=mid,
        target_session_uuid=str(p.session_uuid),
        target_session_incarnation_id=str(p.session_incarnation_id),
        outcome=outcome, reason=reason,
        attempted_at=1_000_100, now=1_000_120, next_attempt_at=1_000_200,
    )
    assert r.status == "receipt_already_won"
    assert r.attempt_recorded is True

    attempts = db.execute(
        "SELECT COUNT(*) AS c, outcome FROM queue_attempts WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert attempts["c"] == 1
    assert attempts["outcome"] == outcome


@pytest.mark.parametrize("outcome,expected_intermediate", [
    ("accepted_by_cli", "notified"),
    ("ambiguous_timeout", "ambiguous"),
])
def test_finalize_before_receipt_keeps_lease_fields_null(db, make_presence_fn, msg,
                                                         outcome, expected_intermediate):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id)
    claim = claim_delivery(db, now=1_000_100)

    reason = "" if outcome == "accepted_by_cli" else "timed out"
    r = finalize_attempt(
        db, claim_lease_token=claim.claim_lease_token,
        message_id=mid,
        target_session_uuid=str(p.session_uuid),
        target_session_incarnation_id=str(p.session_incarnation_id),
        outcome=outcome, reason=reason,
        attempted_at=1_000_100, now=1_000_110, next_attempt_at=1_000_200,
    )
    assert r.status == "finalized"
    assert r.new_state == expected_intermediate

    row = db.execute(
        "SELECT delivery_state, claim_lease_token, claim_lease_expires_at "
        "FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == expected_intermediate
    assert row["claim_lease_token"] is None
    assert row["claim_lease_expires_at"] is None

    _insert_receipt(db, message_id=mid,
                    target_uuid=p.session_uuid,
                    target_incarnation=p.session_incarnation_id,
                    delivered_at=1_000_120, observed_at=1_000_120)

    row = db.execute(
        "SELECT delivery_state, claim_lease_token FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "observed"
    assert row["claim_lease_token"] is None


# ---- TTL ordering ----

def test_ttl_sweep_runs_before_no_work_return(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_uuid,  # self-target: just satisfying FK
                     delivery_state="notified",
                     expires_at=1_000_050)

    # Fix: actually insert with correct incarnation
    db.execute("DELETE FROM targeted_deliveries WHERE message_id=?", (mid,))
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id,
                     delivery_state="notified",
                     expires_at=1_000_050)

    lease = _singleton_held(db)
    result = run_one_iteration(
        db, adapters={"codex_queue": FakeAdapter()},
        singleton_lease=lease,
        notice_factory=_notice_factory,
        now=lambda: 1_000_100,
    )
    assert result.status == "no_work"
    assert result.ttl_swept >= 1

    row = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "undeliverable"


def test_ttl_sweep_preserves_live_claimed(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id,
                     now=1_000_020,
                     expires_at=1_000_050)

    claim = claim_delivery(db, now=1_000_040, lease_ttl_seconds=1000)
    assert claim is not None

    lease = _singleton_held(db)
    _ = run_one_iteration(
        db, adapters={"codex_queue": FakeAdapter()},
        singleton_lease=lease,
        notice_factory=_notice_factory,
        now=lambda: 1_000_100,
    )
    row = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "claimed"


# ---- reconciliation ----

def test_reconcile_ambiguous_treat_as_undelivered(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id,
                     delivery_state="ambiguous")
    r = reconcile_ambiguous(
        db, message_id=mid,
        target_session_uuid=str(p.session_uuid),
        target_session_incarnation_id=str(p.session_incarnation_id),
        outcome="treat_as_undelivered", now=1_000_200,
    )
    assert r.status == "reconciled"
    row = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "pending"


def test_reconcile_blocked_retry(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id,
                     delivery_state="blocked")
    r = reconcile_blocked(
        db, message_id=mid,
        target_session_uuid=str(p.session_uuid),
        target_session_incarnation_id=str(p.session_incarnation_id),
        outcome="retry", now=1_000_200,
    )
    assert r.status == "reconciled"
    row = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "pending"


def test_reconcile_refuses_state_mismatch(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id,
                     delivery_state="pending")
    r = reconcile_ambiguous(
        db, message_id=mid,
        target_session_uuid=str(p.session_uuid),
        target_session_incarnation_id=str(p.session_incarnation_id),
        outcome="treat_as_undelivered", now=1_000_200,
    )
    assert r.status == "refused_state_mismatch"
    assert r.previous_state == "pending"


def test_expired_claim_recovered_to_pending(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id)
    claim = claim_delivery(db, now=1_000_100, lease_ttl_seconds=5)
    assert claim is not None

    lease = _singleton_held(db)
    result = run_one_iteration(
        db, adapters={"codex_queue": FakeAdapter()},
        singleton_lease=lease,
        notice_factory=_notice_factory,
        now=lambda: 1_000_200,
    )
    assert result.expired_claims_recovered >= 1
