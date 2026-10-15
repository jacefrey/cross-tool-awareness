"""R5 (I05): exact finalization retries are idempotent; altered attempt content
under the same token is rejected with zero row changes. Unauthorized-owner
cases are a separate contract and stay separate."""
from __future__ import annotations

from dataclasses import replace

import pytest

from cross_tool_awareness import CompositeStorage, QueueAttempt, QueueOutcome
from cross_tool_awareness.leases import claim_delivery, pre_adapter_mark
from tests._owner_fixtures import (
    counts, fresh_db_path, identity, open_db, register, row, seed_pending, snapshot_from_row,
)


def _entered(tmp_path, *, receipt_first: bool):
    db = open_db(fresh_db_path(tmp_path))
    owner, target = identity(), identity("target-f")
    register(db, owner)
    register(db, target, holder_pid=5678, process_started_at=2_000_000)
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
    if receipt_first:
        db.execute("INSERT INTO disposition_receipts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                   (claim.message_id, claim.target_session_uuid, claim.target_session_incarnation_id,
                    str(identity().session_uuid), "receiver", claim.target_session_uuid,
                    claim.target_session_incarnation_id, "accepted", 20, "", 20))
    f = CompositeStorage(db, tool="claude", wake_capability="notify_only", claimant=owner,
                         clock=lambda: 12)
    return db, owner, mid, claim, f


def _attempt(claim, **kw):
    base = dict(message_id=claim.message_id, target_session_uuid=claim.target_session_uuid,
                target_session_incarnation_id=claim.target_session_incarnation_id,
                outcome=QueueOutcome("accepted_by_cli"), attempted_at=10, finished_at=11, reason="")
    base.update(kw)
    return QueueAttempt(**base)


def _state(db, mid):
    return (row(db, mid), counts(db),
            [tuple(r) for r in db.execute("SELECT * FROM queue_attempts ORDER BY attempt_id")])


@pytest.mark.parametrize("receipt_first", [False, True], ids=["finalize_first", "receipt_first"])
def test_exact_retry_is_idempotent(tmp_path, receipt_first):
    db, owner, mid, claim, f = _entered(tmp_path, receipt_first=receipt_first)
    a = _attempt(claim)
    first = f.finalize_attempt(claim, a, lease_token=claim.claim_lease_token)
    assert first.status == ("receipt_already_won" if receipt_first else "finalized")
    before = _state(db, mid)
    r = f.finalize_attempt(claim, a, lease_token=claim.claim_lease_token)
    assert r.status == "already_finalized" and r.attempt_recorded is False
    assert _state(db, mid) == before and counts(db)[1] == 1


# Baseline for altered retries is a `rejected` attempt (which carries a reason)
# so each persisted field can be varied one at a time within the QueueAttempt
# contract (accepted attempts must have an empty reason).
ALTERED = [
    ("outcome", QueueOutcome("ambiguous_timeout")),
    ("reason", "r2"),
    ("attempted_at", 9),
    ("finished_at", 12),
]


@pytest.mark.parametrize("receipt_first", [False, True], ids=["finalize_first", "receipt_first"])
@pytest.mark.parametrize("field,value", ALTERED, ids=[f for f, _ in ALTERED])
def test_altered_retry_rejected(tmp_path, receipt_first, field, value):
    db, owner, mid, claim, f = _entered(tmp_path, receipt_first=receipt_first)
    a = _attempt(claim, outcome=QueueOutcome("rejected"), reason="r1")
    assert f.finalize_attempt(claim, a, lease_token=claim.claim_lease_token).status in (
        "finalized", "receipt_already_won")
    before = _state(db, mid)
    b = replace(a, **{field: value})
    r = f.finalize_attempt(claim, b, lease_token=claim.claim_lease_token)
    assert r.status == "altered_retry_rejected" and r.attempt_recorded is False
    assert r.receipt is None and r.new_state is None
    assert _state(db, mid) == before and counts(db)[1] == 1


def test_unauthorized_owner_is_not_an_altered_retry(tmp_path):
    db, owner, mid, claim, f = _entered(tmp_path, receipt_first=False)
    assert f.finalize_attempt(claim, _attempt(claim), lease_token=claim.claim_lease_token).status == "finalized"
    before = _state(db, mid)
    foreign = identity("f-other")
    register(db, foreign, holder_pid=4321)
    g = CompositeStorage(db, tool="claude", wake_capability="notify_only", claimant=foreign,
                         clock=lambda: 12)
    r = g.finalize_attempt(claim, _attempt(claim), lease_token=claim.claim_lease_token)
    assert r.status == "unauthorized" and r.attempt_recorded is False
    assert _state(db, mid) == before
