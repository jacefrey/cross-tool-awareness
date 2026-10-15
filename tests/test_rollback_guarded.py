"""R4/R9 (I10, plan review P3, ruling R-W): rollback refuses entered-but-
unfinalized witnesses (token AND full delivery key must match a recorded
attempt), runs every refusal check, the source-schema check, the backup and
the drops under one write lock, and permits teardown when history is complete."""
from __future__ import annotations

import sqlite3

import cross_tool_awareness.migrations as m
from cross_tool_awareness import rollback
from cross_tool_awareness.leases import claim_delivery, finalize_attempt, pre_adapter_mark
from tests._owner_fixtures import (
    fresh_db_path, identity, open_db, register, seed_pending, snapshot_from_row,
)


def _entered_then_receipt(tmp_path):
    p = fresh_db_path(tmp_path)
    db = open_db(p)
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
    db.execute("INSERT INTO disposition_receipts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
               (claim.message_id, claim.target_session_uuid, claim.target_session_incarnation_id,
                str(identity().session_uuid), "receiver", claim.target_session_uuid,
                claim.target_session_incarnation_id, "accepted", 20, "", 20))
    return p, db, owner, claim


def _late_finalize(db, owner, claim):
    return finalize_attempt(
        db, claim_lease_token=claim.claim_lease_token, message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        outcome="accepted_by_cli", reason="", attempted_at=10, now=22, next_attempt_at=30,
        claimant=owner, clock=lambda: 22)


def test_rollback_refuses_witness_without_recorded_attempt(tmp_path):
    p, db, owner, claim = _entered_then_receipt(tmp_path)
    r = rollback(p, backup_dir=tmp_path / "rb", now=21)
    assert r.status == "refused" and "no recorded attempt" in r.reason
    assert db.execute("SELECT COUNT(*) FROM dispatch_ownership").fetchone()[0] == 1
    assert _late_finalize(db, owner, claim).status == "receipt_already_won"


def test_rollback_refuses_witness_attempt_key_mismatch(tmp_path):
    p, db, owner, claim = _entered_then_receipt(tmp_path)
    assert _late_finalize(db, owner, claim).status == "receipt_already_won"
    db.execute("PRAGMA foreign_keys = OFF")                  # simulate pathological history drift
    db.execute("UPDATE queue_attempts SET message_id='some-other-message'")
    db.close()
    r = rollback(p, backup_dir=tmp_path / "rb", now=23)
    assert r.status == "refused" and "no recorded attempt" in r.reason


def test_rollback_permits_finalized_witness_and_backup_preserves_history(tmp_path):
    """Review Focus 5."""
    p, db, owner, claim = _entered_then_receipt(tmp_path)
    assert _late_finalize(db, owner, claim).status == "receipt_already_won"
    db.close()
    r = rollback(p, backup_dir=tmp_path / "rb", now=23)
    assert r.status == "rolled_back", r
    with sqlite3.connect(r.backup.path) as b:
        assert b.execute("SELECT COUNT(*) FROM dispatch_ownership").fetchone()[0] == 1
        assert b.execute("SELECT COUNT(*) FROM queue_attempts").fetchone()[0] == 1


def test_rollback_holds_write_lock_from_checks_through_drops(tmp_path, monkeypatch):
    p = fresh_db_path(tmp_path)
    orig = m.create_backup
    blocked = []

    def racing(*a, **k):
        r = orig(*a, **k)
        try:
            with sqlite3.connect(p, timeout=0.2) as c:
                c.execute("INSERT INTO dispatch_ownership VALUES ('t','m','a','b','c','d','e',1)")
        except sqlite3.OperationalError as e:
            blocked.append(str(e))
        return r

    monkeypatch.setattr(m, "create_backup", racing)
    r = rollback(p, backup_dir=tmp_path / "rb", now=5)
    assert r.status == "rolled_back" and blocked and "locked" in blocked[0]


def test_rollback_refuses_non_canonical_source_under_lock(tmp_path):
    p = fresh_db_path(tmp_path)
    with sqlite3.connect(p) as c:
        c.execute("DROP INDEX idx_tool_state")
    r = rollback(p, backup_dir=tmp_path / "rb", now=5)
    assert r.status == "refused" and "canonical" in r.reason
    assert not (tmp_path / "rb").exists()
    with sqlite3.connect(p) as c:
        assert c.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='targeted_deliveries'").fetchone()[0] == 1
