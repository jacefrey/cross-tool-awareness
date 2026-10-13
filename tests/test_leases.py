"""Lease tests (§12.4)."""

import inspect
import uuid as _uuid

import pytest
import cross_tool_awareness as awareness

from cross_tool_awareness import (
    SingletonDispatcherLease,
    claim_delivery,
    finalize_attempt,
    pre_adapter_release,
)
from cross_tool_awareness.registry import register_session as _register_session


def register_session(db, presence, *, now):
    return _register_session(
        db,
        presence,
        now=now,
        holder_pid=12_345,
        process_started_at=presence.started_at,
    )


def pre_adapter_mark(*args, **kwargs):
    implementation = getattr(awareness, "pre_adapter_mark", None)
    assert implementation is not None, "pre_adapter_mark must be exported"
    return implementation(*args, **kwargs)


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


# ---------------------------------------------------------------------------
# Singleton lease
# ---------------------------------------------------------------------------

def test_singleton_lease_acquire_and_second_fails(db):
    a = SingletonDispatcherLease(db, holder_pid=100, holder_proc_start_ns=1_000)
    b = SingletonDispatcherLease(db, holder_pid=200, holder_proc_start_ns=2_000)
    assert a.acquire(now=1_000_000, lease_ttl_seconds=60)
    assert not b.acquire(now=1_000_010, lease_ttl_seconds=60)


def test_singleton_lease_acquire_after_expiry(db):
    a = SingletonDispatcherLease(db, holder_pid=100, holder_proc_start_ns=1_000)
    b = SingletonDispatcherLease(db, holder_pid=200, holder_proc_start_ns=2_000)
    assert a.acquire(now=1_000_000, lease_ttl_seconds=30)
    assert b.acquire(now=1_000_100, lease_ttl_seconds=30)  # a's lease expired


def test_singleton_refresh_fails_after_steal(db):
    a = SingletonDispatcherLease(db, holder_pid=100, holder_proc_start_ns=1_000)
    b = SingletonDispatcherLease(db, holder_pid=200, holder_proc_start_ns=2_000)
    assert a.acquire(now=1_000_000, lease_ttl_seconds=30)
    assert b.acquire(now=1_000_100)
    assert not a.refresh(now=1_000_110)


def test_singleton_release_deletes_only_if_held(db):
    a = SingletonDispatcherLease(db, holder_pid=100, holder_proc_start_ns=1_000)
    b = SingletonDispatcherLease(db, holder_pid=200, holder_proc_start_ns=2_000)
    assert a.acquire(now=1_000_000)
    a.release()
    # Now b can acquire fresh
    assert b.acquire(now=1_000_010)


# ---------------------------------------------------------------------------
# Per-attempt lease: finalize_attempt
# ---------------------------------------------------------------------------

def test_finalize_already_recorded_returns_already_finalized(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id)
    claim = claim_delivery(db, now=1_000_100)
    assert claim is not None
    r1 = finalize_attempt(
        db, claim_lease_token=claim.claim_lease_token,
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        outcome="accepted_by_cli", reason="",
        attempted_at=1_000_100, now=1_000_101, next_attempt_at=1_000_200,
    )
    assert r1.status == "finalized"

    r2 = finalize_attempt(
        db, claim_lease_token=claim.claim_lease_token,
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        outcome="accepted_by_cli", reason="",
        attempted_at=1_000_100, now=1_000_102, next_attempt_at=1_000_200,
    )
    assert r2.status == "already_finalized"

    rows = db.execute(
        "SELECT COUNT(*) AS c FROM queue_attempts WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert rows["c"] == 1


# ---------------------------------------------------------------------------
# Pre-adapter release
# ---------------------------------------------------------------------------

def test_pre_adapter_release_target_not_live_pending_no_attempt(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id)
    claim = claim_delivery(db, now=1_000_100)
    assert claim is not None

    r = pre_adapter_release(
        db, claim_lease_token=claim.claim_lease_token,
        next_state="pending", diagnostic="target_not_live",
        next_attempt_at=1_000_200,
    )
    assert r.status == "released"
    assert r.diagnostic == "target_not_live"

    row = db.execute(
        "SELECT delivery_state, attempt_count, last_error_class, "
        "       claim_lease_token, claim_lease_expires_at "
        "FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "pending"
    assert row["attempt_count"] == 0
    assert row["last_error_class"] == "target_not_live"
    assert row["claim_lease_token"] is None
    assert row["claim_lease_expires_at"] is None

    # NO queue_attempts row
    cnt = db.execute(
        "SELECT COUNT(*) AS c FROM queue_attempts WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert cnt["c"] == 0


def test_pre_adapter_release_adapter_not_enabled_blocked_no_attempt(db, make_presence_fn, msg):
    p = make_presence_fn()
    register_session(db, p, now=1_000_000)
    mid = msg()
    _insert_delivery(db, message_id=mid,
                     target_uuid=p.session_uuid,
                     target_incarnation=p.session_incarnation_id)
    claim = claim_delivery(db, now=1_000_100)
    r = pre_adapter_release(
        db, claim_lease_token=claim.claim_lease_token,
        next_state="blocked", diagnostic="adapter_not_enabled",
        next_attempt_at=1_000_100,
    )
    assert r.status == "released"

    row = db.execute(
        "SELECT delivery_state, attempt_count FROM targeted_deliveries WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert row["delivery_state"] == "blocked"
    assert row["attempt_count"] == 0

    cnt = db.execute(
        "SELECT COUNT(*) AS c FROM queue_attempts WHERE message_id=?",
        (mid,),
    ).fetchone()
    assert cnt["c"] == 0


def test_pre_adapter_release_rejects_invalid_next_state(db):
    with pytest.raises(AssertionError):
        pre_adapter_release(
            db, claim_lease_token="x",
            next_state="notified", diagnostic="d", next_attempt_at=0,
        )


# ---------------------------------------------------------------------------
# v0.4 C4: pre-adapter mark + expired-claim recovery
# ---------------------------------------------------------------------------

def _claimed_delivery(db, make_presence_fn, msg, *, lease_ttl_seconds=15):
    presence = make_presence_fn()
    register_session(db, presence, now=1_000_000)
    message_id = msg()
    _insert_delivery(
        db,
        message_id=message_id,
        target_uuid=presence.session_uuid,
        target_incarnation=presence.session_incarnation_id,
    )
    claim = claim_delivery(
        db, now=1_000_100, lease_ttl_seconds=lease_ttl_seconds
    )
    assert claim is not None
    return claim


def test_pre_adapter_mark_transitions_exact_owned_claim(
    db, make_presence_fn, msg
):
    signature = inspect.signature(awareness.pre_adapter_mark)
    assert [
        signature.parameters[name].annotation
        for name in (
            "message_id",
            "target_session_uuid",
            "target_session_incarnation_id",
        )
    ] == ["UUID", "UUID", "UUID"]
    assert signature.return_annotation == "bool"
    claim = _claimed_delivery(db, make_presence_fn, msg)

    assert pre_adapter_mark(
        db,
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        lease_token=claim.claim_lease_token,
        now=1_000_105,
    )

    row = db.execute(
        "SELECT delivery_state, dispatch_started_at, claim_lease_token "
        "FROM targeted_deliveries WHERE message_id = ?",
        (claim.message_id,),
    ).fetchone()
    assert dict(row) == {
        "delivery_state": "dispatching",
        "dispatch_started_at": 1_000_105,
        "claim_lease_token": claim.claim_lease_token,
    }


def test_pre_adapter_mark_rejects_missing_wrong_token_and_repeat(
    db, make_presence_fn, msg
):
    claim = _claimed_delivery(db, make_presence_fn, msg)
    common = dict(
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        now=1_000_105,
    )
    assert not pre_adapter_mark(
        db,
        message_id=str(_uuid.uuid4()),
        lease_token=claim.claim_lease_token,
        **common,
    )
    assert not pre_adapter_mark(
        db,
        message_id=claim.message_id,
        lease_token="wrong-token",
        **common,
    )
    assert pre_adapter_mark(
        db,
        message_id=claim.message_id,
        lease_token=claim.claim_lease_token,
        **common,
    )
    assert not pre_adapter_mark(
        db,
        message_id=claim.message_id,
        lease_token=claim.claim_lease_token,
        **common,
    )


def test_pre_adapter_mark_failure_leaves_claim_releasable(
    db, make_presence_fn, msg
):
    claim = _claimed_delivery(db, make_presence_fn, msg)
    db.execute(
        "CREATE TRIGGER fail_dispatch_mark BEFORE UPDATE ON targeted_deliveries "
        "WHEN NEW.delivery_state = 'dispatching' "
        "BEGIN SELECT RAISE(ABORT, 'forced mark failure'); END"
    )

    with pytest.raises(Exception, match="forced mark failure"):
        pre_adapter_mark(
            db,
            message_id=claim.message_id,
            target_session_uuid=claim.target_session_uuid,
            target_session_incarnation_id=claim.target_session_incarnation_id,
            lease_token=claim.claim_lease_token,
            now=1_000_105,
        )

    db.execute("DROP TRIGGER fail_dispatch_mark")
    result = pre_adapter_release(
        db,
        claim_lease_token=claim.claim_lease_token,
        next_state="pending",
        diagnostic="pre_adapter_mark_error",
        next_attempt_at=1_000_200,
    )
    assert result.status == "released"


def test_claim_delivery_recovers_expired_claim_with_new_fence(
    db, make_presence_fn, msg
):
    original = _claimed_delivery(
        db, make_presence_fn, msg, lease_ttl_seconds=5
    )

    recovered = claim_delivery(db, now=1_000_106, lease_ttl_seconds=20)

    assert recovered is not None
    assert recovered.message_id == original.message_id
    assert recovered.claim_lease_token != original.claim_lease_token
    row = db.execute(
        "SELECT delivery_state, claim_lease_token, claim_lease_expires_at "
        "FROM targeted_deliveries WHERE message_id = ?",
        (original.message_id,),
    ).fetchone()
    assert dict(row) == {
        "delivery_state": "claimed",
        "claim_lease_token": recovered.claim_lease_token,
        "claim_lease_expires_at": 1_000_126,
    }
    stale_release = pre_adapter_release(
        db,
        claim_lease_token=original.claim_lease_token,
        next_state="pending",
        diagnostic="stale-worker",
        next_attempt_at=1_000_200,
    )
    assert stale_release.status == "lease_released_or_observed"
    stale_finalize = finalize_attempt(
        db,
        claim_lease_token=original.claim_lease_token,
        message_id=original.message_id,
        target_session_uuid=original.target_session_uuid,
        target_session_incarnation_id=original.target_session_incarnation_id,
        outcome="rejected",
        reason="stale worker",
        attempted_at=1_000_106,
        now=1_000_107,
        next_attempt_at=1_000_200,
    )
    assert stale_finalize.status == "lease_already_released"
    current = db.execute(
        "SELECT delivery_state, claim_lease_token FROM targeted_deliveries "
        "WHERE message_id = ?",
        (original.message_id,),
    ).fetchone()
    assert dict(current) == {
        "delivery_state": "claimed",
        "claim_lease_token": recovered.claim_lease_token,
    }


def test_claim_delivery_does_not_recover_unexpired_claim(
    db, make_presence_fn, msg
):
    _claimed_delivery(db, make_presence_fn, msg, lease_ttl_seconds=20)
    assert claim_delivery(db, now=1_000_119) is None


def test_claim_delivery_never_recovers_dispatching_row(
    db, make_presence_fn, msg
):
    claim = _claimed_delivery(db, make_presence_fn, msg, lease_ttl_seconds=5)
    assert pre_adapter_mark(
        db,
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        lease_token=claim.claim_lease_token,
        now=1_000_101,
    )
    assert claim_delivery(db, now=9_000_000) is None

    release = pre_adapter_release(
        db,
        claim_lease_token=claim.claim_lease_token,
        next_state="pending",
        diagnostic="late-release",
        next_attempt_at=9_000_001,
    )
    assert release.status == "lease_released_or_observed"
    row = db.execute(
        "SELECT delivery_state, claim_lease_token FROM targeted_deliveries "
        "WHERE message_id = ?",
        (claim.message_id,),
    ).fetchone()
    assert dict(row) == {
        "delivery_state": "dispatching",
        "claim_lease_token": claim.claim_lease_token,
    }
