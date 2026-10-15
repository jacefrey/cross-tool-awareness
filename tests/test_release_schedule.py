"""R5 / A2 (I06): facade pre-entry release clears owner+token and PRESERVES
next_attempt_at; the generic low-level release accepts None (preserve) or an
explicit integer schedule."""
from __future__ import annotations

from cross_tool_awareness import CompositeStorage
from cross_tool_awareness.leases import pre_adapter_release
from tests._owner_fixtures import (
    counts, fresh_db_path, identity, open_db, register, row, seed_pending, snapshot_from_row,
)

CLEARED = ("claim_lease_token", "claim_lease_expires_at", "claimant_session_uuid",
           "claimant_session_incarnation_id", "claimant_registration_fence")
CHANGED = CLEARED + ("delivery_state", "last_error_class")


def _seed(tmp_path, clock_value=20):
    db = open_db(fresh_db_path(tmp_path))
    cid, target = identity(), identity("target-f")
    register(db, cid)
    register(db, target, holder_pid=5678, process_started_at=2_000_000)
    mid = seed_pending(db, target)                       # next_attempt_at = 10
    f = CompositeStorage(db, tool="claude", wake_capability="notify_only",
                         claimant=cid, clock=lambda: clock_value)
    claim = f.claim_delivery(10, recovery_after=30,
                             claimant_snapshot=snapshot_from_row(db, cid, proof_observed_at=clock_value))
    assert claim is not None
    return db, cid, mid, f, claim


def _release_kw(cid, claim):
    return dict(claimant=cid, clock=lambda: 20, message_id=claim.message_id,
                target_session_uuid=claim.target_session_uuid,
                target_session_incarnation_id=claim.target_session_incarnation_id)


def test_facade_blocked_release_preserves_schedule(tmp_path):
    db, cid, mid, f, claim = _seed(tmp_path)
    before = row(db, mid)
    f.release_claim(claim, "blocked", "adapter_disabled", lease_token=claim.claim_lease_token)
    after = row(db, mid)
    assert after["next_attempt_at"] == before["next_attempt_at"] == 10
    assert after["attempt_count"] == before["attempt_count"]
    assert after["delivery_state"] == "blocked" and after["last_error_class"] == "adapter_disabled"
    assert all(after[c] is None for c in CLEARED)
    assert {k: v for k, v in after.items() if k not in CHANGED} == \
           {k: v for k, v in before.items() if k not in CHANGED}
    assert counts(db) == (0, 0)


def test_low_level_release_none_preserves(tmp_path):
    db, cid, mid, f, claim = _seed(tmp_path)
    r = pre_adapter_release(db, claim_lease_token=claim.claim_lease_token, next_state="pending",
                            diagnostic="target_not_live", next_attempt_at=None, **_release_kw(cid, claim))
    assert r.status == "released"
    assert row(db, mid)["next_attempt_at"] == 10


def test_low_level_release_int_sets_schedule(tmp_path):
    db, cid, mid, f, claim = _seed(tmp_path)
    r = pre_adapter_release(db, claim_lease_token=claim.claim_lease_token, next_state="pending",
                            diagnostic="target_not_live", next_attempt_at=75, **_release_kw(cid, claim))
    assert r.status == "released"
    assert row(db, mid)["next_attempt_at"] == 75
