"""Restored facade invariants (ported from v0.4 tests/test_composite_storage.py
onto the owner-aware v0.5 constructor; every original assertion kept) plus the
A2-named ownership tests."""
from __future__ import annotations

import inspect
import sqlite3

import pytest

from cross_tool_awareness import (
    ClaimantIdentity, CompositeStorage, QueueAttempt, QueueOutcome,
)
from cross_tool_awareness.leases import finalize_attempt
from tests._owner_fixtures import (
    counts, fresh_db_path, identity, open_db, register, row, seed_pending, snapshot_from_row,
)


_SIGNATURES = {
    "pending_count": ["self", "now"],
    "claim_delivery": ["self", "now", "recovery_after", "claimant_snapshot"],
    "revalidate_claim": ["self", "claim", "now", "lease_token"],
    "release_claim": ["self", "claim", "state", "reason", "lease_token"],
    "pre_adapter_mark": ["self", "claim", "now", "lease_token", "claimant_snapshot", "target_snapshot"],
    "finalize_attempt": ["self", "claim", "attempt", "lease_token"],
}
_KEYWORD_ONLY = {"recovery_after", "lease_token", "claimant_snapshot", "target_snapshot"}


def test_protocol_method_signatures_are_exact():
    for name, expected_names in _SIGNATURES.items():
        signature = inspect.signature(getattr(CompositeStorage, name))
        assert list(signature.parameters) == expected_names, name
        for parameter_name in expected_names:
            expected_kind = (inspect.Parameter.KEYWORD_ONLY if parameter_name in _KEYWORD_ONLY
                             else inspect.Parameter.POSITIONAL_OR_KEYWORD)
            assert signature.parameters[parameter_name].kind is expected_kind, (name, parameter_name)


def _facade(db, cid, *, tool="claude", wake_capability="notify_only", clock=10):
    return CompositeStorage(db, tool=tool, wake_capability=wake_capability,
                            claimant=cid, clock=lambda: clock)


def _seed(tmp_path, *, target_tool="claude", target_wake="notify_only"):
    p = fresh_db_path(tmp_path)
    db = open_db(p)
    cid, target = identity(), identity("target-f")
    register(db, cid)
    register(db, target, holder_pid=5678, process_started_at=2_000_000,
             tool=target_tool, wake_capability=target_wake)
    return p, db, cid, target


def _snap(db, cid, proof=10):
    return snapshot_from_row(db, cid, proof_observed_at=proof)


def test_pending_count_and_claim_recovery_after_round_trip(tmp_path):
    p, db, cid, target = _seed(tmp_path)
    seed_pending(db, target, next_attempt_at=5)
    seed_pending(db, target, next_attempt_at=200)
    f = _facade(db, cid)
    assert f.pending_count(10) == 1
    claim = f.claim_delivery(10, recovery_after=37, claimant_snapshot=_snap(db, cid))
    assert claim is not None
    assert row(db, claim.message_id)["claim_lease_expires_at"] == 47


def test_claim_and_registration_fence_capture_share_one_immediate_transaction(tmp_path):
    p, db, cid, target = _seed(tmp_path)
    seed_pending(db, target)
    statements: list[str] = []
    snap = _snap(db, cid)
    db.set_trace_callback(statements.append)
    claim = _facade(db, cid).claim_delivery(10, recovery_after=30, claimant_snapshot=snap)
    db.set_trace_callback(None)
    assert claim is not None and claim.target_registration_fence == "target-f"
    begins = [i for i, s in enumerate(statements) if s.startswith("BEGIN IMMEDIATE")]
    assert len(begins) == 1
    fence = next(i for i, s in enumerate(statements)
                 if s.startswith("SELECT") and "target_registration_fence" in s)
    update = next(i for i, s in enumerate(statements) if s.startswith("UPDATE targeted_deliveries"))
    commit = statements.index(next(s for s in statements if s.startswith("COMMIT")))
    assert begins[0] < fence < update < commit


def test_revalidate_claim_requires_exact_live_registration_and_lease(tmp_path):
    p, db, cid, target = _seed(tmp_path)
    seed_pending(db, target)
    f = _facade(db, cid)
    claim = f.claim_delivery(10, recovery_after=30, claimant_snapshot=_snap(db, cid))
    assert claim is not None
    presence = f.revalidate_claim(claim, 20, lease_token=claim.claim_lease_token)
    assert presence is not None and str(presence.session_uuid) == str(target.session_uuid)
    assert f.revalidate_claim(claim, 20, lease_token="wrong") is None
    assert f.revalidate_claim(claim, 40, lease_token=claim.claim_lease_token) is None   # expiry equality
    assert _facade(db, cid, wake_capability="claude_socket").revalidate_claim(
        claim, 20, lease_token=claim.claim_lease_token) is None
    db.execute("UPDATE session_registrations SET registration_fence='rotated' WHERE session_uuid=?",
               (str(target.session_uuid),))
    assert f.revalidate_claim(claim, 20, lease_token=claim.claim_lease_token) is None
    db.execute("UPDATE session_registrations SET registration_fence='target-f', state='stopping'"
               " WHERE session_uuid=?", (str(target.session_uuid),))
    assert f.revalidate_claim(claim, 20, lease_token=claim.claim_lease_token) is None


def test_codex_and_claude_routing_pairs(tmp_path):
    p = fresh_db_path(tmp_path)
    db = open_db(p)
    cid = identity()
    register(db, cid)
    codex = identity("codex-f")
    claude = identity("claude-f")
    register(db, codex, holder_pid=2001, process_started_at=2_000_001, tool="codex", wake_capability="codex_queue")
    register(db, claude, holder_pid=2002, process_started_at=2_000_002, tool="claude", wake_capability="claude_socket")
    seed_pending(db, codex, next_attempt_at=5)
    seed_pending(db, claude, next_attempt_at=6)

    codex_storage = _facade(db, cid, tool="codex", wake_capability="codex_queue")
    codex_claim = codex_storage.claim_delivery(10, recovery_after=30, claimant_snapshot=_snap(db, cid))
    assert codex_claim is not None and codex_claim.target_session_uuid == str(codex.session_uuid)
    assert codex_storage.revalidate_claim(codex_claim, 20, lease_token=codex_claim.claim_lease_token) is not None

    claude_storage = _facade(db, cid, tool="claude", wake_capability="claude_socket")
    assert claude_storage.revalidate_claim(codex_claim, 20, lease_token=codex_claim.claim_lease_token) is None
    claude_claim = claude_storage.claim_delivery(10, recovery_after=30, claimant_snapshot=_snap(db, cid))
    assert claude_claim is not None and claude_claim.target_session_uuid == str(claude.session_uuid)
    assert claude_storage.revalidate_claim(claude_claim, 20, lease_token=claude_claim.claim_lease_token) is not None


def test_release_claim_is_exactly_fenced(tmp_path):
    p, db, cid, target = _seed(tmp_path)
    mid = seed_pending(db, target)
    f = _facade(db, cid)
    claim = f.claim_delivery(10, recovery_after=30, claimant_snapshot=_snap(db, cid))
    assert claim is not None
    other = seed_pending(db, target, next_attempt_at=200)
    db.execute("UPDATE targeted_deliveries SET delivery_state='claimed', claim_lease_token=?,"
               " claim_lease_expires_at=130, claimant_session_uuid=?, claimant_session_incarnation_id=?,"
               " claimant_registration_fence=? WHERE message_id=?",
               (claim.claim_lease_token, str(cid.session_uuid), str(cid.session_incarnation_id),
                cid.registration_fence, other))
    before, other_before = row(db, mid), row(db, other)

    with pytest.raises(RuntimeError, match="lease"):
        f.release_claim(claim, "pending", "target_not_live", lease_token="wrong")
    assert row(db, mid) == before and row(db, other) == other_before

    assert f.release_claim(claim, "pending", "target_not_live", lease_token=claim.claim_lease_token) is None
    after = row(db, mid)
    assert (after["delivery_state"], after["claim_lease_token"], after["last_error_class"]) == (
        "pending", None, "target_not_live")
    assert after["next_attempt_at"] == before["next_attempt_at"]
    assert row(db, other) == other_before
    assert counts(db) == (0, 0)


def test_claim_and_release_refuse_caller_transaction_without_commit(tmp_path):
    p, db, cid, target = _seed(tmp_path)
    seed_pending(db, target)
    f = _facade(db, cid)
    claim = f.claim_delivery(10, recovery_after=30, claimant_snapshot=_snap(db, cid))
    db.execute("BEGIN")
    db.execute("INSERT INTO messages(id, payload, created_at) VALUES ('caller-work', '', 0)")
    with pytest.raises(sqlite3.ProgrammingError):
        f.release_claim(claim, "blocked", "x", lease_token=claim.claim_lease_token)
    with pytest.raises(sqlite3.ProgrammingError):
        finalize_attempt(db, claim_lease_token=claim.claim_lease_token, message_id=claim.message_id,
                         target_session_uuid=claim.target_session_uuid,
                         target_session_incarnation_id=claim.target_session_incarnation_id,
                         outcome="accepted_by_cli", reason="", attempted_at=10, now=11,
                         next_attempt_at=11, claimant=cid, clock=lambda: 11)
    assert db.in_transaction is True
    fresh = open_db(p, timeout=0.5)
    assert fresh.execute("SELECT COUNT(*) FROM messages WHERE id='caller-work'").fetchone()[0] == 0
    db.execute("ROLLBACK")
    assert db.execute("SELECT COUNT(*) FROM messages WHERE id='caller-work'").fetchone()[0] == 0
    assert row(db, claim.message_id)["delivery_state"] == "claimed"


def test_facade_is_single_fence_and_replacement_does_not_inherit_claims(tmp_path):
    p, db, cid, target = _seed(tmp_path)
    mid = seed_pending(db, target)
    old = _facade(db, cid)
    old_claim = old.claim_delivery(10, recovery_after=30, claimant_snapshot=_snap(db, cid))
    assert old_claim is not None
    # Trusted re-registration under a new fence.
    db.execute("UPDATE session_registrations SET registration_fence='f2' WHERE session_uuid=?",
               (str(cid.session_uuid),))
    rotated = ClaimantIdentity(session_uuid=cid.session_uuid,
                               session_incarnation_id=cid.session_incarnation_id,
                               registration_fence="f2")
    # The old facade cannot claim or revalidate under its stale identity.
    second = seed_pending(db, target, next_attempt_at=5)
    assert old.claim_delivery(10, recovery_after=30, claimant_snapshot=_snap(db, cid)) is None
    assert old.revalidate_claim(old_claim, 20, lease_token=old_claim.claim_lease_token) is None
    # A fresh facade from the verified replacement binding claims fresh work ...
    new = _facade(db, rotated)
    new_claim = new.claim_delivery(10, recovery_after=30, claimant_snapshot=_snap(db, rotated))
    assert new_claim is not None and new_claim.message_id == second
    assert new_claim.claimant_registration_fence == "f2"
    # ... and refuses the old facade's handle for every write.
    before = row(db, mid)
    assert new.pre_adapter_mark(old_claim, 10, lease_token=old_claim.claim_lease_token,
                                claimant_snapshot=_snap(db, rotated),
                                target_snapshot=_snap(db, target)) is False
    attempt = QueueAttempt(message_id=old_claim.message_id, target_session_uuid=old_claim.target_session_uuid,
                           target_session_incarnation_id=old_claim.target_session_incarnation_id,
                           outcome=QueueOutcome("accepted_by_cli"), attempted_at=10, finished_at=11, reason="")
    assert new.finalize_attempt(old_claim, attempt, lease_token=old_claim.claim_lease_token).status == "unauthorized"
    assert row(db, mid) == before and counts(db) == (0, 0)
