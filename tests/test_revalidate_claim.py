"""R5 (I04): revalidate_claim requires full key, token, owner, unexpired claim,
live CURRENT claimant metadata, the fence captured at claim time, and the
facade route."""
from __future__ import annotations

from cross_tool_awareness import CompositeStorage
from tests._owner_fixtures import (
    fresh_db_path, identity, open_db, register, seed_pending, snapshot_from_row,
)


def _facade(db, cid, *, tool="claude", wake_capability="notify_only"):
    return CompositeStorage(db, tool=tool, wake_capability=wake_capability,
                            claimant=cid, clock=lambda: 10)


def _seed(tmp_path):
    db = open_db(fresh_db_path(tmp_path))
    cid, target = identity(), identity("target-f")
    register(db, cid)
    register(db, target, holder_pid=5678, process_started_at=2_000_000)
    seed_pending(db, target)
    f = _facade(db, cid)
    claim = f.claim_delivery(10, recovery_after=30,
                             claimant_snapshot=snapshot_from_row(db, cid, proof_observed_at=10))
    assert claim is not None
    return db, cid, target, f, claim


def _set(db, cid, column, value):
    db.execute(f"UPDATE session_registrations SET {column}=? WHERE session_uuid=?",
               (value, str(cid.session_uuid)))


def test_revalidate_happy_path_returns_target_presence(tmp_path):
    db, cid, target, f, claim = _seed(tmp_path)
    p = f.revalidate_claim(claim, 10, lease_token=claim.claim_lease_token)
    assert p is not None and str(p.session_uuid) == claim.target_session_uuid


def test_revalidate_refuses_target_fence_rotation(tmp_path):
    db, cid, target, f, claim = _seed(tmp_path)
    _set(db, target, "registration_fence", "rotated")
    assert f.revalidate_claim(claim, 10, lease_token=claim.claim_lease_token) is None


def test_revalidate_refuses_claimant_fence_rotation(tmp_path):
    db, cid, target, f, claim = _seed(tmp_path)
    _set(db, cid, "registration_fence", "rotated")
    assert f.revalidate_claim(claim, 10, lease_token=claim.claim_lease_token) is None


def test_revalidate_refuses_claimant_revocation(tmp_path):
    db, cid, target, f, claim = _seed(tmp_path)
    _set(db, cid, "revoked_at", 11)
    assert f.revalidate_claim(claim, 10, lease_token=claim.claim_lease_token) is None


def test_revalidate_refuses_wrong_token(tmp_path):
    db, cid, target, f, claim = _seed(tmp_path)
    assert f.revalidate_claim(claim, 10, lease_token="wrong") is None


def test_revalidate_refuses_lease_expiry_equality(tmp_path):
    db, cid, target, f, claim = _seed(tmp_path)
    assert f.revalidate_claim(claim, 39, lease_token=claim.claim_lease_token) is not None
    assert f.revalidate_claim(claim, 40, lease_token=claim.claim_lease_token) is None


def test_revalidate_refuses_other_route(tmp_path):
    db, cid, target, f, claim = _seed(tmp_path)
    other = _facade(db, cid, tool="claude", wake_capability="claude_socket")
    assert other.revalidate_claim(claim, 10, lease_token=claim.claim_lease_token) is None
