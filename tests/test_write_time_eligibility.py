"""R7 (I01, I02): `now` is scheduling input; the injected clock, read AFTER the
write lock, decides recovery eligibility, new lease expiry and mark eligibility.
Equality at lease expiry loses."""
from __future__ import annotations

import sqlite3
import threading

import pytest

from cross_tool_awareness.leases import claim_delivery, pre_adapter_mark
from tests._owner_fixtures import (
    fresh_db_path, identity, open_db, register, row, seed_pending, snapshot_from_row,
)


def _seed(tmp_path):
    p = fresh_db_path(tmp_path)
    db = open_db(p)
    cid, target = identity(), identity("target-f")
    register(db, cid)
    register(db, target, holder_pid=5678, process_started_at=2_000_000)
    mid = seed_pending(db, target)
    return p, db, cid, target, mid


def _claim(db, cid, *, now, clock, ttl=30, proof=None):
    proof = clock() if proof is None else proof
    return claim_delivery(db, now=now, lease_ttl_seconds=ttl, claimant=cid, clock=clock,
                          claimant_snapshot=snapshot_from_row(db, cid, proof_observed_at=proof),
                          tool="claude", wake_capability="notify_only")


def _mark(db, cid, target, claim, *, clock, claimant_snap, target_snap):
    return pre_adapter_mark(
        db, message_id=claim.message_id, target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        lease_token=claim.claim_lease_token, now=10, claimant=cid, clock=clock,
        claimant_snapshot=claimant_snap, target_snapshot=target_snap,
        tool="claude", wake_capability="notify_only")


def test_lease_expiry_is_clock_plus_ttl(tmp_path):
    p, db, cid, target, mid = _seed(tmp_path)
    claim = _claim(db, cid, now=1_000, clock=lambda: 10, ttl=30)
    assert claim is not None
    assert row(db, mid)["claim_lease_expires_at"] == 40


def test_scheduling_now_cannot_recover_unexpired_lease(tmp_path):
    p, db, cid, target, mid = _seed(tmp_path)
    first = _claim(db, cid, now=10, clock=lambda: 10)
    second = _claim(db, cid, now=40, clock=lambda: 10)
    assert second is None
    assert row(db, mid)["claim_lease_token"] == first.claim_lease_token


def test_clock_at_expiry_equality_recovers_lease(tmp_path):
    p, db, cid, target, mid = _seed(tmp_path)
    first = _claim(db, cid, now=10, clock=lambda: 10)
    second = _claim(db, cid, now=10, clock=lambda: 40)
    assert second is not None and second.claim_lease_token != first.claim_lease_token
    assert row(db, mid)["claim_lease_expires_at"] == 70


@pytest.mark.parametrize("clock_value,expected", [(39, True), (40, False)])
def test_mark_expiry_equality_loses(tmp_path, clock_value, expected):
    p, db, cid, target, mid = _seed(tmp_path)
    claim = _claim(db, cid, now=10, clock=lambda: 10)
    cs = snapshot_from_row(db, cid, proof_observed_at=clock_value)
    ts = snapshot_from_row(db, target, proof_observed_at=clock_value)
    assert _mark(db, cid, target, claim, clock=lambda: clock_value,
                 claimant_snap=cs, target_snap=ts) is expected
    r = row(db, mid)
    assert (r["delivery_state"], r["dispatch_started_at"]) == (
        ("dispatching", clock_value) if expected else ("claimed", None))


def lock_wait(db_path, fn, *, clock_values, before_release=None):
    """fn(db, clock) runs in a worker thread on ITS OWN connection while the main
    thread holds BEGIN IMMEDIATE. Returns (result_or_exception, clock_reads_before_release)."""
    entered, released = threading.Event(), threading.Event()
    early, out = [], []

    def clock():
        if not released.is_set():
            early.append(clock_values[0])
        return clock_values[0]

    def worker():
        db = sqlite3.connect(str(db_path), isolation_level=None, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            entered.set()
            out.append(fn(db, clock))
        except BaseException as exc:  # reported to the main thread, never swallowed
            out.append(exc)
        finally:
            db.close()

    blocker = sqlite3.connect(str(db_path), isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    t = threading.Thread(target=worker, daemon=True)
    t.start()
    try:
        assert entered.wait(5)
        if before_release is not None:
            before_release(blocker)
        clock_values[0] = 40
    finally:
        released.set()
        blocker.execute("COMMIT")
        blocker.close()
    t.join(10)
    assert not t.is_alive()
    return out[0], early


def _mark_fn(cid, target, claim, cs, ts):
    return lambda db, clock: _mark(db, cid, target, claim, clock=clock,
                                   claimant_snap=cs, target_snap=ts)


def test_mark_reads_clock_after_lock_and_refuses_expired_lease(tmp_path):
    p, db, cid, target, mid = _seed(tmp_path)
    claim = _claim(db, cid, now=10, clock=lambda: 10)
    cs = snapshot_from_row(db, cid, proof_observed_at=10)
    ts = snapshot_from_row(db, target, proof_observed_at=10)
    db.close()
    result, early = lock_wait(p, _mark_fn(cid, target, claim, cs, ts), clock_values=[10])
    assert early == [], "clock must not be sampled before the lock is held"
    assert result is False, result
    r = row(open_db(p), mid)
    assert (r["delivery_state"], r["dispatch_started_at"], r["claim_lease_expires_at"]) == ("claimed", None, 40)


def _rotate_claimant(cid):
    return lambda b: b.execute("UPDATE session_registrations SET registration_fence='rotated'"
                               " WHERE session_uuid=?", (str(cid.session_uuid),))


def _change_target_pid(target):
    return lambda b: b.execute("UPDATE session_registrations SET holder_pid=999"
                               " WHERE session_uuid=?", (str(target.session_uuid),))


@pytest.mark.parametrize("case", ["stale_proof", "future_proof", "claimant_changed", "target_changed"])
def test_mark_after_lock_wait_refuses(tmp_path, case):
    p, db, cid, target, mid = _seed(tmp_path)
    claim = _claim(db, cid, now=10, clock=lambda: 10, ttl=500)      # lease stays valid at 40
    proof = {"stale_proof": 5, "future_proof": 41}.get(case, 10)
    cs = snapshot_from_row(db, cid, proof_observed_at=proof)
    ts = snapshot_from_row(db, target, proof_observed_at=proof)
    mutate = {"claimant_changed": _rotate_claimant(cid), "target_changed": _change_target_pid(target)}.get(case)
    db.close()
    result, early = lock_wait(p, _mark_fn(cid, target, claim, cs, ts), clock_values=[10],
                              before_release=mutate)
    assert early == []
    assert result is False, result
    r = row(open_db(p), mid)
    assert (r["delivery_state"], r["dispatch_started_at"]) == ("claimed", None)


def test_claim_reads_clock_after_lock_for_recovery(tmp_path):
    p, db, cid, target, mid = _seed(tmp_path)
    first = _claim(db, cid, now=10, clock=lambda: 10)               # expires 40
    contender = identity("f2")
    register(db, contender, holder_pid=777)
    snap = snapshot_from_row(db, contender, proof_observed_at=40)
    db.close()

    def fn(db, clock):
        return claim_delivery(db, now=10, claimant=contender, clock=clock, claimant_snapshot=snap,
                              tool="claude", wake_capability="notify_only")

    result, early = lock_wait(p, fn, clock_values=[10])
    assert early == []
    assert not isinstance(result, BaseException), result
    assert result is not None and result.claim_lease_token != first.claim_lease_token
    r = row(open_db(p), mid)
    assert r["claimant_registration_fence"] == "f2" and r["claim_lease_expires_at"] == 70
