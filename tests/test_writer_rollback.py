"""R6 (I07): every package writer rolls back on ANY exception, including
BaseException subclasses, leaving zero writes. Evidence is read through a
fresh connection on the same file."""
from __future__ import annotations

import sqlite3

import pytest

from cross_tool_awareness.leases import (
    claim_delivery, finalize_attempt, pre_adapter_mark, pre_adapter_release,
)
from tests._owner_fixtures import (
    counts, fresh_db_path, identity, open_db, register, row, seed_pending,
    snapshot_from_row,
)


class _Inject(sqlite3.Connection):
    """Raise `exc` the first time a statement starting with `arm_prefix` runs."""
    arm_prefix: str | None = None
    exc: BaseException | None = None

    def execute(self, sql, parameters=()):
        if self.arm_prefix and sql.lstrip().startswith(self.arm_prefix):
            self.arm_prefix = None
            raise self.exc
        return super().execute(sql, parameters)


EXCS = [KeyboardInterrupt("fixture"), SystemExit(3), sqlite3.OperationalError("synthetic")]
IDS = ["KeyboardInterrupt", "SystemExit", "OperationalError"]


def _setup(tmp_path):
    p = fresh_db_path(tmp_path)
    db = open_db(p, factory=_Inject)
    cid, target = identity(), identity("target-f")
    register(db, cid)
    register(db, target, holder_pid=5678, process_started_at=2_000_000)
    mid = seed_pending(db, target)
    return p, db, cid, target, mid


def _claim(db, cid):
    return claim_delivery(db, now=10, claimant=cid, clock=lambda: 10,
                          claimant_snapshot=snapshot_from_row(db, cid, proof_observed_at=10),
                          tool="claude", wake_capability="notify_only")


def _mark(db, cid, target, claim):
    return pre_adapter_mark(
        db, message_id=claim.message_id, target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        lease_token=claim.claim_lease_token, now=10, claimant=cid, clock=lambda: 10,
        claimant_snapshot=snapshot_from_row(db, cid, proof_observed_at=10),
        target_snapshot=snapshot_from_row(db, target, proof_observed_at=10),
        tool="claude", wake_capability="notify_only")


def _fresh(p):
    return open_db(p, timeout=0.5)


@pytest.mark.parametrize("exc", EXCS, ids=IDS)
def test_claim_interrupted_at_stamp_rolls_back(tmp_path, exc):
    p, db, cid, target, mid = _setup(tmp_path)
    before = row(db, mid)
    db.arm_prefix, db.exc = "UPDATE targeted_deliveries", exc
    with pytest.raises(type(exc)):
        _claim(db, cid)
    assert db.in_transaction is False
    f = _fresh(p)
    assert row(f, mid) == before and counts(f) == (0, 0)


@pytest.mark.parametrize("exc", EXCS, ids=IDS)
def test_mark_interrupted_before_witness_rolls_back_state_change(tmp_path, exc):
    p, db, cid, target, mid = _setup(tmp_path)
    claim = _claim(db, cid)
    assert claim is not None
    before = row(db, mid)
    db.arm_prefix, db.exc = "INSERT INTO dispatch_ownership", exc
    with pytest.raises(type(exc)):
        _mark(db, cid, target, claim)
    assert db.in_transaction is False
    f = _fresh(p)
    assert row(f, mid) == before and counts(f) == (0, 0)


@pytest.mark.parametrize("exc", EXCS, ids=IDS)
def test_release_interrupted_rolls_back(tmp_path, exc):
    p, db, cid, target, mid = _setup(tmp_path)
    claim = _claim(db, cid)
    before = row(db, mid)
    db.arm_prefix, db.exc = "UPDATE targeted_deliveries", exc
    with pytest.raises(type(exc)):
        pre_adapter_release(
            db, claim_lease_token=claim.claim_lease_token, next_state="blocked",
            diagnostic="x", next_attempt_at=99, claimant=cid, clock=lambda: 10,
            message_id=claim.message_id, target_session_uuid=claim.target_session_uuid,
            target_session_incarnation_id=claim.target_session_incarnation_id)
    assert db.in_transaction is False
    f = _fresh(p)
    assert row(f, mid) == before and counts(f) == (0, 0)


@pytest.mark.parametrize("exc", EXCS, ids=IDS)
def test_finalize_interrupted_after_attempt_insert_rolls_back_attempt(tmp_path, exc):
    p, db, cid, target, mid = _setup(tmp_path)
    claim = _claim(db, cid)
    assert _mark(db, cid, target, claim) is True
    before = row(db, mid)
    db.arm_prefix, db.exc = "UPDATE targeted_deliveries", exc
    with pytest.raises(type(exc)):
        finalize_attempt(
            db, claim_lease_token=claim.claim_lease_token, message_id=claim.message_id,
            target_session_uuid=claim.target_session_uuid,
            target_session_incarnation_id=claim.target_session_incarnation_id,
            outcome="accepted_by_cli", reason="", attempted_at=10, now=11,
            next_attempt_at=11, claimant=cid, clock=lambda: 11)
    assert db.in_transaction is False
    f = _fresh(p)
    assert row(f, mid) == before and counts(f) == (1, 0)
