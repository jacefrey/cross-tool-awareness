"""A3 / R8: contenders are separate OS processes with separate connections;
explicit Barrier/Event synchronization, one shared 10 s deadline, one bounded
cleanup phase; no sleeps for ordering. Unit-level cases that do not need two
processes are marked as such."""
from __future__ import annotations

import multiprocessing as mp
import sqlite3
import threading
import time
import uuid
from uuid import UUID, uuid4

import pytest

from cross_tool_awareness import ClaimantIdentity, QueueAttempt, QueueOutcome, CompositeStorage
from cross_tool_awareness.leases import (
    claim_delivery, finalize_attempt, list_stuck_dispatching, pre_adapter_mark, pre_adapter_release,
)
from tests._owner_fixtures import (
    CHILD_DEADLINE, CLEANUP, counts, fresh_db_path, identity, open_db, register, row,
    run_children, seed_pending, snapshot_from_row,
)


# --- child-side helpers (module-level for spawn picklability) ----------------

def _cid(d):
    return ClaimantIdentity(session_uuid=UUID(d["uuid"]), session_incarnation_id=UUID(d["inc"]),
                            registration_fence=d["fence"])


def _ident(fence):
    cid = identity(fence)
    return cid, {"uuid": str(cid.session_uuid), "inc": str(cid.session_incarnation_id), "fence": fence}


def _claim_dict(claim):
    return {"message_id": claim.message_id, "t": claim.target_session_uuid,
            "ti": claim.target_session_incarnation_id, "token": claim.claim_lease_token}


def _child_claim(db_path, who, clock_value, barrier, q):
    try:
        db = open_db(db_path)
        try:
            cid = _cid(who)
            snap = snapshot_from_row(db, cid, proof_observed_at=clock_value)
            barrier.wait(5)
            c = claim_delivery(db, now=10, claimant=cid, clock=lambda: clock_value, claimant_snapshot=snap,
                               tool="claude", wake_capability="notify_only")
            q.put(("claim", who["fence"], None if c is None else c.claim_lease_token))
        finally:
            db.close()
    except BaseException as exc:
        q.put(("error", type(exc).__name__, str(exc)))


def _child_hold_lock(db_path, started, release, q):
    try:
        db = sqlite3.connect(str(db_path), isolation_level=None, timeout=10)
        try:
            db.execute("BEGIN IMMEDIATE")
            started.set()
            assert release.wait(CHILD_DEADLINE)
            db.execute("COMMIT")
            q.put(("lock", "released"))
        finally:
            db.close()
    except BaseException as exc:
        q.put(("error", type(exc).__name__, str(exc)))


def _child_mark_then_hang(db_path, who, target_d, claim_d, marked, q):
    try:
        db = open_db(db_path)
        try:
            cid = _cid(who)
            ok = pre_adapter_mark(
                db, message_id=claim_d["message_id"], target_session_uuid=claim_d["t"],
                target_session_incarnation_id=claim_d["ti"], lease_token=claim_d["token"],
                now=10, claimant=cid, clock=lambda: 10,
                claimant_snapshot=snapshot_from_row(db, cid, proof_observed_at=10),
                target_snapshot=snapshot_from_row(db, _cid(target_d), proof_observed_at=10),
                tool="claude", wake_capability="notify_only")
            q.put(("marked", ok))
        finally:
            db.close()
        marked.set()
        while True:                       # "adapter running"; the parent SIGKILLs us
            time.sleep(0.05)
    except BaseException as exc:
        q.put(("error", type(exc).__name__, str(exc)))


def _child_insert_receipt(db_path, claim_d, barrier, q):
    try:
        db = open_db(db_path)
        try:
            barrier.wait(5)
            db.execute("INSERT INTO disposition_receipts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                       (claim_d["message_id"], claim_d["t"], claim_d["ti"], str(uuid4()), "receiver",
                        claim_d["t"], claim_d["ti"], "accepted", 20, "", 20))
            q.put(("receipt", "inserted"))
        finally:
            db.close()
    except BaseException as exc:
        q.put(("error", type(exc).__name__, str(exc)))


def _child_finalize(db_path, who, claim_d, barrier, q):
    try:
        db = open_db(db_path)
        try:
            barrier.wait(5)
            r = finalize_attempt(db, claim_lease_token=claim_d["token"], message_id=claim_d["message_id"],
                                 target_session_uuid=claim_d["t"], target_session_incarnation_id=claim_d["ti"],
                                 outcome="accepted_by_cli", reason="", attempted_at=10, now=22,
                                 next_attempt_at=30, claimant=_cid(who), clock=lambda: 22)
            q.put(("finalize", r.status))
        finally:
            db.close()
    except BaseException as exc:
        q.put(("error", type(exc).__name__, str(exc)))


# --- parent-side helpers -----------------------------------------------------

def _drain(q, n, timeout=5.0):
    out = []
    end = time.monotonic() + timeout
    while len(out) < n and time.monotonic() < end:
        try:
            out.append(q.get(timeout=0.2))
        except Exception:
            pass
    return out


def _seed(tmp_path):
    p = fresh_db_path(tmp_path)
    db = open_db(p)
    owner, od = _ident("fa")
    target, td = _ident("target-f")
    register(db, owner, holder_pid=1001)
    register(db, target, holder_pid=5678, process_started_at=2_000_000)
    mid = seed_pending(db, target, expires_at=3610)
    return p, db, owner, od, target, td, mid


def _claim(db, owner, *, clock=10, ttl=30):
    return claim_delivery(db, now=10, lease_ttl_seconds=ttl, claimant=owner, clock=lambda: clock,
                          claimant_snapshot=snapshot_from_row(db, owner, proof_observed_at=clock),
                          tool="claude", wake_capability="notify_only")


def _mark(db, owner, target, claim, *, clock=10, token=None):
    return pre_adapter_mark(
        db, message_id=claim.message_id, target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        lease_token=token or claim.claim_lease_token, now=10, claimant=owner, clock=lambda: clock,
        claimant_snapshot=snapshot_from_row(db, owner, proof_observed_at=clock),
        target_snapshot=snapshot_from_row(db, target, proof_observed_at=clock),
        tool="claude", wake_capability="notify_only")


def _finalize(db, owner, claim, *, token=None, outcome="accepted_by_cli", reason=""):
    return finalize_attempt(db, claim_lease_token=token or claim.claim_lease_token, message_id=claim.message_id,
                            target_session_uuid=claim.target_session_uuid,
                            target_session_incarnation_id=claim.target_session_incarnation_id,
                            outcome=outcome, reason=reason, attempted_at=10, now=22, next_attempt_at=30,
                            claimant=owner, clock=lambda: 22)


# --- two-process cases -------------------------------------------------------

def test_two_processes_one_claim_winner(tmp_path):
    p, db, owner, od, target, td, mid = _seed(tmp_path)
    b, bd = _ident("fb")
    register(db, b, holder_pid=1002)
    db.close()
    ctx = mp.get_context("spawn")
    barrier, q = ctx.Barrier(2), ctx.Queue()
    try:
        run_children(ctx, [(_child_claim, (str(p), od, 10, barrier, q)),
                           (_child_claim, (str(p), bd, 10, barrier, q))])
        results = _drain(q, 2)
    finally:
        q.close()
    assert len(results) == 2 and not any(r[0] == "error" for r in results), results
    tokens = [r[2] for r in results if r[2]]
    assert len(tokens) == 1


def test_recovered_claim_rejects_stale_owner_all_writes(tmp_path):
    p, db, owner, od, target, td, mid = _seed(tmp_path)
    stale = _claim(db, owner, clock=10, ttl=30)                 # expires 40
    b, bd = _ident("fb")
    register(db, b, holder_pid=1002)
    ctx = mp.get_context("spawn")
    barrier, q = ctx.Barrier(1), ctx.Queue()
    try:
        run_children(ctx, [(_child_claim, (str(p), bd, 40, barrier, q))])
        results = _drain(q, 1)
    finally:
        q.close()
    assert results and results[0][0] == "claim" and results[0][2], results
    new_token = results[0][2]
    before = row(db, mid)
    for token in (stale.claim_lease_token, new_token):
        assert _mark(db, owner, target, stale, clock=41, token=token) is False
        rel = pre_adapter_release(db, claim_lease_token=token, next_state="blocked", diagnostic="x",
                                  next_attempt_at=None, claimant=owner, clock=lambda: 41,
                                  message_id=stale.message_id, target_session_uuid=stale.target_session_uuid,
                                  target_session_incarnation_id=stale.target_session_incarnation_id)
        assert rel.status == "lease_released_or_observed"
        assert _finalize(db, owner, stale, token=token).status == "unauthorized"
        assert row(db, mid) == before and counts(db) == (0, 0)


def test_mark_rechecks_proof_and_lease_after_lock_wait(tmp_path):
    p, db, owner, od, target, td, mid = _seed(tmp_path)
    claim = _claim(db, owner, clock=10, ttl=30)                 # expires 40
    cs = snapshot_from_row(db, owner, proof_observed_at=10)
    ts = snapshot_from_row(db, target, proof_observed_at=10)
    db.close()
    ctx = mp.get_context("spawn")
    started, release, q = ctx.Event(), ctx.Event(), ctx.Queue()
    clock = ctx.Value("i", 10)
    entered, out = threading.Event(), []

    def worker():
        wdb = open_db(p)
        try:
            entered.set()
            out.append(pre_adapter_mark(
                wdb, message_id=claim.message_id, target_session_uuid=claim.target_session_uuid,
                target_session_incarnation_id=claim.target_session_incarnation_id,
                lease_token=claim.claim_lease_token, now=10, claimant=owner, clock=lambda: clock.value,
                claimant_snapshot=cs, target_snapshot=ts, tool="claude", wake_capability="notify_only"))
        except BaseException as exc:
            out.append(exc)
        finally:
            wdb.close()

    holder = ctx.Process(target=_child_hold_lock, args=(str(p), started, release, q), daemon=True)
    t = threading.Thread(target=worker, daemon=True)
    try:
        holder.start()
        assert started.wait(CHILD_DEADLINE)
        t.start()
        assert entered.wait(5)
        clock.value = 40                                        # expiry equality while blocked
    finally:
        release.set()
        holder.join(CHILD_DEADLINE)
        if holder.is_alive():
            holder.kill()
            holder.join(CLEANUP)
    t.join(CHILD_DEADLINE)
    assert not t.is_alive() and holder.exitcode == 0
    assert out and out[0] is False, out
    r = row(open_db(p), mid)
    assert (r["delivery_state"], r["dispatch_started_at"]) == ("claimed", None)


def test_receipt_before_finalize_requires_entered_owner_witness(tmp_path):
    p, db, owner, od, target, td, mid = _seed(tmp_path)
    claim = _claim(db, owner)
    assert _mark(db, owner, target, claim)
    ctx = mp.get_context("spawn")
    barrier, q = ctx.Barrier(1), ctx.Queue()
    try:
        run_children(ctx, [(_child_insert_receipt, (str(p), _claim_dict(claim), barrier, q))])
        assert _drain(q, 1) == [("receipt", "inserted")]
    finally:
        q.close()
    assert row(db, mid)["delivery_state"] == "observed"
    assert _finalize(db, owner, claim).status == "receipt_already_won"
    assert counts(db) == (1, 1)
    foreign = identity("foreign-f")
    register(db, foreign, holder_pid=4321)
    assert _finalize(db, foreign, claim).status == "unauthorized"
    assert counts(db) == (1, 1)


def test_finalize_then_valid_receipt_converges_to_observed(tmp_path):
    p, db, owner, od, target, td, mid = _seed(tmp_path)
    claim = _claim(db, owner)
    assert _mark(db, owner, target, claim)
    assert _finalize(db, owner, claim).status == "finalized"
    assert row(db, mid)["delivery_state"] == "notified"
    ctx = mp.get_context("spawn")
    barrier, q = ctx.Barrier(1), ctx.Queue()
    try:
        run_children(ctx, [(_child_insert_receipt, (str(p), _claim_dict(claim), barrier, q))])
        assert _drain(q, 1) == [("receipt", "inserted")]
    finally:
        q.close()
    assert row(db, mid)["delivery_state"] == "observed"
    assert counts(db) == (1, 1)


@pytest.mark.parametrize("observed_first", [False, True], ids=["before_observation", "after_observation"])
def test_foreign_owner_cannot_insert_attempt_with_known_token(tmp_path, observed_first):
    p, db, owner, od, target, td, mid = _seed(tmp_path)
    claim = _claim(db, owner)
    assert _mark(db, owner, target, claim)
    if observed_first:
        db.execute("INSERT INTO disposition_receipts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                   (mid, claim.target_session_uuid, claim.target_session_incarnation_id, str(uuid4()),
                    "receiver", claim.target_session_uuid, claim.target_session_incarnation_id,
                    "accepted", 20, "", 20))
    foreign, fd = _ident("foreign-f")
    register(db, foreign, holder_pid=4321)
    before = row(db, mid)
    ctx = mp.get_context("spawn")
    barrier, q = ctx.Barrier(1), ctx.Queue()
    try:
        run_children(ctx, [(_child_finalize, (str(p), fd, _claim_dict(claim), barrier, q))])
        assert _drain(q, 1) == [("finalize", "unauthorized")]
    finally:
        q.close()
    assert row(db, mid) == before and counts(db) == (1, 0)


def test_killed_dispatcher_never_requeues_dispatching(tmp_path):
    p, db, owner, od, target, td, mid = _seed(tmp_path)
    claim = _claim(db, owner)                                   # lease expires 40
    db.close()
    ctx = mp.get_context("spawn")
    marked, q = ctx.Event(), ctx.Queue()
    child = ctx.Process(target=_child_mark_then_hang, args=(str(p), od, td, _claim_dict(claim), marked, q),
                        daemon=True)
    try:
        child.start()
        assert marked.wait(CHILD_DEADLINE), "child never marked"
        assert _drain(q, 1) == [("marked", True)]
        child.kill()                                            # SIGKILL after the committed mark
        child.join(CLEANUP)
    finally:
        if child.is_alive():
            child.kill()
            child.join(CLEANUP)
        q.close()
    assert child.exitcode is not None and child.exitcode < 0
    db = open_db(p)
    assert row(db, mid)["delivery_state"] == "dispatching"
    assert len(list_stuck_dispatching(db, 110, stale_after_seconds=1)) == 1
    b = identity("fb")
    register(db, b, holder_pid=1002)
    # Delivery is unexpired at the probe time (expires_at 3610) and the lease
    # expired at 40: a correct package still refuses to recover `dispatching`.
    assert claim_delivery(db, now=110, claimant=b, clock=lambda: 110,
                          claimant_snapshot=snapshot_from_row(db, b, proof_observed_at=110),
                          tool="claude", wake_capability="notify_only") is None
    assert row(db, mid)["delivery_state"] == "dispatching"


def test_multi_process_contention_only_one_claims(tmp_path):
    p, db, owner, od, target, td, mid = _seed(tmp_path)
    claimants = [od]
    for i in range(3):
        c, d = _ident(f"f{i}")
        register(db, c, holder_pid=2000 + i)
        claimants.append(d)
    db.close()
    ctx = mp.get_context("spawn")
    barrier, q = ctx.Barrier(4), ctx.Queue()
    try:
        run_children(ctx, [(_child_claim, (str(p), d, 10, barrier, q)) for d in claimants])
        results = _drain(q, 4)
    finally:
        q.close()
    assert len(results) == 4 and not any(r[0] == "error" for r in results), results
    assert sum(1 for r in results if r[2]) == 1
    assert sum(1 for r in results if r[2] is None) == 3


# --- unit-level cases (one process) -----------------------------------------

def test_attempt_and_delivery_update_rollback_together(tmp_path):
    p, db, owner, od, target, td, mid = _seed(tmp_path)
    claim = _claim(db, owner)
    assert _mark(db, owner, target, claim)
    db.execute("CREATE TRIGGER tr_test_abort AFTER UPDATE OF delivery_state ON targeted_deliveries"
               " WHEN NEW.delivery_state='notified' BEGIN SELECT RAISE(ABORT, 'fixture'); END")
    before = row(db, mid)
    with pytest.raises(sqlite3.IntegrityError, match="fixture"):
        _finalize(db, owner, claim)
    assert db.in_transaction is False
    assert row(db, mid) == before and counts(db) == (1, 0)
    db.execute("DROP TRIGGER tr_test_abort")
    assert _finalize(db, owner, claim).status == "finalized"
    assert row(db, mid)["delivery_state"] == "notified" and counts(db) == (1, 1)


def test_exact_finalization_retry_is_idempotent_and_altered_retry_rejected(tmp_path):
    p, db, owner, od, target, td, mid = _seed(tmp_path)
    claim = _claim(db, owner)
    assert _mark(db, owner, target, claim)
    assert _finalize(db, owner, claim, outcome="rejected", reason="r1").status == "finalized"
    before = (row(db, mid), counts(db))
    assert _finalize(db, owner, claim, outcome="rejected", reason="r1").status == "already_finalized"
    assert _finalize(db, owner, claim, outcome="rejected", reason="r2").status == "altered_retry_rejected"
    assert (row(db, mid), counts(db)) == before


def test_rotated_claimant_cannot_mark_but_entered_owner_can_finalize(tmp_path):
    p, db, owner, od, target, td, mid = _seed(tmp_path)
    claim = _claim(db, owner)
    assert _mark(db, owner, target, claim)
    second = seed_pending(db, target, next_attempt_at=5)
    claim2 = _claim(db, owner)
    assert claim2 is not None and claim2.message_id == second
    cs, ts = snapshot_from_row(db, owner, proof_observed_at=10), snapshot_from_row(db, target, proof_observed_at=10)
    db.execute("UPDATE session_registrations SET registration_fence='rotated' WHERE session_uuid=?",
               (str(owner.session_uuid),))
    assert pre_adapter_mark(
        db, message_id=claim2.message_id, target_session_uuid=claim2.target_session_uuid,
        target_session_incarnation_id=claim2.target_session_incarnation_id,
        lease_token=claim2.claim_lease_token, now=10, claimant=owner, clock=lambda: 10,
        claimant_snapshot=cs, target_snapshot=ts, tool="claude", wake_capability="notify_only") is False
    assert row(db, second)["delivery_state"] == "claimed"
    assert _finalize(db, owner, claim).status == "finalized"
    assert row(db, mid)["delivery_state"] == "notified"
