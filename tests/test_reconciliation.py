"""Operator reconciliation invariants (restored from v0.4 tests/test_dispatcher.py
reconciliation section, plus the A3-named reconciliation tests): reconciliation
acts only on ambiguous/blocked, never on claimed/dispatching, and never
disturbs a dispatch witness."""
from __future__ import annotations

import uuid

import pytest

from cross_tool_awareness import reconcile_ambiguous, reconcile_blocked
from cross_tool_awareness.leases import claim_delivery, finalize_attempt, pre_adapter_mark
from tests._owner_fixtures import (
    counts, fresh_db_path, identity, open_db, register, row, seed_pending, snapshot_from_row,
)


def _insert_delivery(db, target, *, delivery_state, owner=None, now=10):
    mid = str(uuid.uuid4())
    db.execute("INSERT INTO messages(id, payload, created_at) VALUES (?,?,?)", (mid, "{}", now))
    if delivery_state in ("claimed", "dispatching"):
        assert owner is not None
        db.execute(
            "INSERT INTO targeted_deliveries (message_id, target_session_uuid,"
            " target_session_incarnation_id, sender_session_uuid, sender_session_incarnation_id,"
            " delivery_state, claim_lease_token, claim_lease_expires_at, dispatch_started_at,"
            " claimant_session_uuid, claimant_session_incarnation_id, claimant_registration_fence,"
            " created_at, next_attempt_at, expires_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (mid, str(target.session_uuid), str(target.session_incarnation_id), str(uuid.uuid4()),
             str(uuid.uuid4()), delivery_state, f"tok-{mid[:8]}", now + 30,
             now if delivery_state == "dispatching" else None,
             str(owner.session_uuid), str(owner.session_incarnation_id), owner.registration_fence,
             now, now, now + 3600))
    else:
        db.execute(
            "INSERT INTO targeted_deliveries (message_id, target_session_uuid,"
            " target_session_incarnation_id, sender_session_uuid, sender_session_incarnation_id,"
            " delivery_state, created_at, next_attempt_at, expires_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (mid, str(target.session_uuid), str(target.session_incarnation_id), str(uuid.uuid4()),
             str(uuid.uuid4()), delivery_state, now, now, now + 3600))
    return mid


def _seed(tmp_path):
    db = open_db(fresh_db_path(tmp_path))
    owner, target = identity(), identity("target-f")
    register(db, owner)
    register(db, target, holder_pid=5678, process_started_at=2_000_000)
    return db, owner, target


def _key(mid, target):
    return dict(message_id=mid, target_session_uuid=str(target.session_uuid),
                target_session_incarnation_id=str(target.session_incarnation_id))


def test_reconcile_ambiguous_treat_as_undelivered(tmp_path):
    db, owner, target = _seed(tmp_path)
    mid = _insert_delivery(db, target, delivery_state="ambiguous")
    r = reconcile_ambiguous(db, outcome="treat_as_undelivered", now=200, **_key(mid, target))
    assert r.status == "reconciled"
    assert row(db, mid)["delivery_state"] == "pending"


def test_reconcile_blocked_retry(tmp_path):
    db, owner, target = _seed(tmp_path)
    mid = _insert_delivery(db, target, delivery_state="blocked")
    r = reconcile_blocked(db, outcome="retry", now=200, **_key(mid, target))
    assert r.status == "reconciled"
    assert row(db, mid)["delivery_state"] == "pending"


def test_reconcile_refuses_state_mismatch(tmp_path):
    db, owner, target = _seed(tmp_path)
    mid = _insert_delivery(db, target, delivery_state="pending")
    r = reconcile_ambiguous(db, outcome="treat_as_undelivered", now=200, **_key(mid, target))
    assert r.status == "refused_state_mismatch"
    assert r.previous_state == "pending"


@pytest.mark.parametrize("state", ["claimed", "dispatching"])
def test_reconciliation_cannot_change_claimed_or_dispatching(tmp_path, state):
    db, owner, target = _seed(tmp_path)
    mid = _insert_delivery(db, target, delivery_state=state, owner=owner)
    before = row(db, mid)
    for fn, outcome in ((reconcile_ambiguous, "treat_as_undelivered"), (reconcile_blocked, "retry")):
        r = fn(db, outcome=outcome, now=200, **_key(mid, target))
        assert r.status == "refused_state_mismatch" and r.previous_state == state
        assert row(db, mid) == before


def test_reconciliation_and_receipts_preserve_dispatch_witness(tmp_path):
    db, owner, target = _seed(tmp_path)
    mid = seed_pending(db, target)
    claim = claim_delivery(db, now=10, claimant=owner, clock=lambda: 10,
                           claimant_snapshot=snapshot_from_row(db, owner, proof_observed_at=10),
                           tool="claude", wake_capability="notify_only")
    assert pre_adapter_mark(
        db, message_id=claim.message_id, target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        lease_token=claim.claim_lease_token, now=10, claimant=owner, clock=lambda: 10,
        claimant_snapshot=snapshot_from_row(db, owner, proof_observed_at=10),
        target_snapshot=snapshot_from_row(db, target, proof_observed_at=10),
        tool="claude", wake_capability="notify_only")
    assert counts(db) == (1, 0)
    db.execute("INSERT INTO disposition_receipts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
               (mid, str(target.session_uuid), str(target.session_incarnation_id), str(uuid.uuid4()),
                "receiver", str(target.session_uuid), str(target.session_incarnation_id),
                "accepted", 20, "", 20))
    assert row(db, mid)["delivery_state"] == "observed" and counts(db) == (1, 0)
    other = _insert_delivery(db, target, delivery_state="ambiguous")
    assert reconcile_ambiguous(db, outcome="treat_as_undelivered", now=200, **_key(other, target)).status == "reconciled"
    assert counts(db) == (1, 0)
    late = finalize_attempt(db, claim_lease_token=claim.claim_lease_token, message_id=claim.message_id,
                            target_session_uuid=claim.target_session_uuid,
                            target_session_incarnation_id=claim.target_session_incarnation_id,
                            outcome="accepted_by_cli", reason="", attempted_at=10, now=22,
                            next_attempt_at=30, claimant=owner, clock=lambda: 22)
    assert late.status == "receipt_already_won" and counts(db) == (1, 1)
