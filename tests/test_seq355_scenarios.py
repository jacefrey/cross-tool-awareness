"""Bounded, assertion-based verifier for the seq-355 review scenarios (plan
review P5). One independent fixture per original probe label from
codex-manager's historical diagnostic scripts (lease-repros.py,
migration-repros.py, receipt-rollback-repro.py), each asserting the CORRECTED
behavior. Run with `python3.12 -m pytest tests/test_seq355_scenarios.py -q -rA`
so every case is listed; waits are bounded; a failure never hides later cases.
The original scripts are preserved in codex-manager as defect evidence."""
from __future__ import annotations

import sqlite3
import uuid
from dataclasses import replace

import pytest

import cross_tool_awareness.migrations as m
from cross_tool_awareness import CompositeStorage, QueueAttempt, QueueOutcome, apply as apply_migration, rollback
from cross_tool_awareness.leases import claim_delivery, finalize_attempt, pre_adapter_mark
from tests._owner_fixtures import (
    counts, fresh_db_path, identity, open_db, register, row, seed_pending, snapshot_from_row,
)
from tests.test_migration_v3_to_v4 import _insert_delivery_v3, _seed_v3
from tests.test_write_time_eligibility import lock_wait


# --- lease-repros.py ---------------------------------------------------------

def _setup(tmp_path, clock=10):
    p = fresh_db_path(tmp_path)
    db = open_db(p)
    owner, target = identity(), identity("target-f")
    register(db, owner)
    register(db, target, holder_pid=5678, process_started_at=2_000_000)
    mid = seed_pending(db, target)
    facade = CompositeStorage(db, tool="claude", wake_capability="notify_only", claimant=owner,
                              clock=lambda: clock)
    claim = facade.claim_delivery(10, recovery_after=30,
                                  claimant_snapshot=snapshot_from_row(db, owner, proof_observed_at=clock))
    assert claim is not None
    return p, db, owner, target, mid, facade, claim


def _mark(db, owner, target, claim, **over):
    args = dict(message_id=claim.message_id, target_session_uuid=claim.target_session_uuid,
                target_session_incarnation_id=claim.target_session_incarnation_id,
                lease_token=claim.claim_lease_token, now=10, claimant=owner, clock=lambda: 10,
                claimant_snapshot=snapshot_from_row(db, owner, proof_observed_at=10),
                target_snapshot=snapshot_from_row(db, target, proof_observed_at=10),
                tool="claude", wake_capability="notify_only")
    args.update(over)
    return pre_adapter_mark(db, **args)


def test_scheduling_now40_recovers_at_clock10(tmp_path):
    p, db, owner, target, mid, facade, claim = _setup(tmp_path)
    second = facade.claim_delivery(40, recovery_after=30,
                                   claimant_snapshot=snapshot_from_row(db, owner, proof_observed_at=10))
    assert second is None
    assert row(db, mid)["claim_lease_token"] == claim.claim_lease_token


def test_stopping_pid0_claimant_can_claim(tmp_path):
    p, db, owner, target, mid, facade, claim = _setup(tmp_path)
    facade.release_claim(claim, "blocked", "test", lease_token=claim.claim_lease_token)
    db.execute("UPDATE targeted_deliveries SET delivery_state='pending'")
    snap = snapshot_from_row(db, owner, proof_observed_at=10)
    db.execute("UPDATE session_registrations SET state='stopping', holder_pid=0 WHERE session_uuid=?",
               (str(owner.session_uuid),))
    assert facade.claim_delivery(10, recovery_after=30, claimant_snapshot=snap) is None


def test_unrelated_claimant_snapshot_can_claim(tmp_path):
    p, db, owner, target, mid, facade, claim = _setup(tmp_path)
    facade.release_claim(claim, "blocked", "test", lease_token=claim.claim_lease_token)
    db.execute("UPDATE targeted_deliveries SET delivery_state='pending'")
    unrelated = snapshot_from_row(db, target, proof_observed_at=10)
    assert facade.claim_delivery(10, recovery_after=30, claimant_snapshot=unrelated) is None


def test_claimant_pid_drift_mark(tmp_path):
    p, db, owner, target, mid, facade, claim = _setup(tmp_path)
    cs = snapshot_from_row(db, owner, proof_observed_at=10)
    db.execute("UPDATE session_registrations SET holder_pid=999 WHERE session_uuid=?", (str(owner.session_uuid),))
    assert _mark(db, owner, target, claim, claimant_snapshot=cs) is False


def test_target_metadata_route_state_drift_mark(tmp_path):
    p, db, owner, target, mid, facade, claim = _setup(tmp_path)
    ts = snapshot_from_row(db, target, proof_observed_at=10)
    db.execute("UPDATE session_registrations SET registration_fence='rotated', project_root='/tmp/other',"
               " state='stopping', tool='codex', wake_capability='codex_queue' WHERE session_uuid=?",
               (str(target.session_uuid),))
    assert _mark(db, owner, target, claim, target_snapshot=ts) is False
    assert row(db, mid)["delivery_state"] == "claimed"


def test_wrong_route_args_mark(tmp_path):
    p, db, owner, target, mid, facade, claim = _setup(tmp_path)
    assert _mark(db, owner, target, claim, tool="codex", wake_capability="codex_queue") is False


def test_stale_claimant_revalidate_returns_presence(tmp_path):
    p, db, owner, target, mid, facade, claim = _setup(tmp_path)
    db.execute("UPDATE session_registrations SET registration_fence='rotated' WHERE session_uuid=?",
               (str(owner.session_uuid),))
    assert facade.revalidate_claim(claim, 10, lease_token=claim.claim_lease_token) is None


def test_target_fence_drift_revalidate_returns_presence(tmp_path):
    p, db, owner, target, mid, facade, claim = _setup(tmp_path)
    db.execute("UPDATE session_registrations SET registration_fence='rotated' WHERE session_uuid=?",
               (str(target.session_uuid),))
    assert facade.revalidate_claim(claim, 10, lease_token=claim.claim_lease_token) is None


def test_altered_retry(tmp_path):
    p, db, owner, target, mid, facade, claim = _setup(tmp_path)
    assert _mark(db, owner, target, claim)
    a = QueueAttempt(claim.message_id, claim.target_session_uuid, claim.target_session_incarnation_id,
                     QueueOutcome("rejected"), 10, 11, "r1")
    assert facade.finalize_attempt(claim, a, lease_token=claim.claim_lease_token).status == "finalized"
    b = replace(a, reason="changed", finished_at=12)
    assert facade.finalize_attempt(claim, b, lease_token=claim.claim_lease_token).status == "altered_retry_rejected"
    assert counts(db) == (1, 1)


def test_blocked_release_schedule(tmp_path):
    p, db, owner, target, mid, facade, claim = _setup(tmp_path, clock=20)
    facade.release_claim(claim, "blocked", "disabled", lease_token=claim.claim_lease_token)
    assert row(db, mid)["next_attempt_at"] == 10


class _InterruptConnection(sqlite3.Connection):
    armed = False

    def execute(self, sql, parameters=()):
        if self.armed and sql.lstrip().startswith("INSERT INTO dispatch_ownership"):
            raise KeyboardInterrupt("fixture interruption after state update")
        return super().execute(sql, parameters)


def test_interrupted_mark_in_transaction(tmp_path):
    p = fresh_db_path(tmp_path)
    db = open_db(p, factory=_InterruptConnection)
    owner, target = identity(), identity("target-f")
    register(db, owner)
    register(db, target, holder_pid=5678, process_started_at=2_000_000)
    mid = seed_pending(db, target)
    claim = claim_delivery(db, now=10, claimant=owner, clock=lambda: 10,
                           claimant_snapshot=snapshot_from_row(db, owner, proof_observed_at=10),
                           tool="claude", wake_capability="notify_only")
    db.armed = True
    with pytest.raises(KeyboardInterrupt):
        _mark(db, owner, target, claim)
    assert db.in_transaction is False
    fresh = open_db(p)
    assert row(fresh, mid)["delivery_state"] == "claimed" and counts(fresh) == (0, 0)


def test_mark_at_lease_expiry_after_lock_wait(tmp_path):
    p, db, owner, target, mid, facade, claim = _setup(tmp_path)   # lease expires 40
    cs = snapshot_from_row(db, owner, proof_observed_at=10)
    ts = snapshot_from_row(db, target, proof_observed_at=10)
    db.close()

    def fn(wdb, clock):
        return pre_adapter_mark(
            wdb, message_id=claim.message_id, target_session_uuid=claim.target_session_uuid,
            target_session_incarnation_id=claim.target_session_incarnation_id,
            lease_token=claim.claim_lease_token, now=10, claimant=owner, clock=clock,
            claimant_snapshot=cs, target_snapshot=ts, tool="claude", wake_capability="notify_only")

    result, early = lock_wait(p, fn, clock_values=[10])
    assert early == [] and result is False
    r = row(open_db(p), mid)
    assert (r["delivery_state"], r["dispatch_started_at"], r["claim_lease_expires_at"]) == ("claimed", None, 40)


# --- migration-repros.py -----------------------------------------------------

def _v(p):
    with sqlite3.connect(p) as c:
        return c.execute("SELECT MAX(version) FROM awareness_schema_version").fetchone()[0]


def test_extra_column(tmp_path):
    p = _seed_v3(tmp_path)
    with sqlite3.connect(p) as c:
        c.execute("ALTER TABLE targeted_deliveries ADD COLUMN external_metadata TEXT")
    r = apply_migration(p, backup_dir=tmp_path / "b", now=100)
    assert r.status == "refused" and _v(p) == 3
    with sqlite3.connect(p) as c:
        assert any(x[1] == "external_metadata" for x in c.execute("PRAGMA table_info(targeted_deliveries)"))


def test_drift_other(tmp_path):
    p = _seed_v3(tmp_path)
    with sqlite3.connect(p) as c:
        c.execute("ALTER TABLE session_registrations ADD COLUMN external_metadata TEXT")
    r = apply_migration(p, backup_dir=tmp_path / "b", now=100)
    assert r.status == "refused" and _v(p) == 3 and r.backup is None


def test_post_check_race(tmp_path, monkeypatch):
    p = _seed_v3(tmp_path)
    _insert_delivery_v3(p, "pending", msg_id=str(uuid.uuid4()))
    orig = m.create_backup
    blocked = []

    def racing(*a, **k):
        r = orig(*a, **k)
        try:
            with sqlite3.connect(p, timeout=0.2) as c:
                c.execute("UPDATE targeted_deliveries SET delivery_state='blocked'")
        except sqlite3.OperationalError as e:
            blocked.append(str(e))
        return r

    monkeypatch.setattr(m, "create_backup", racing)
    r = apply_migration(p, backup_dir=tmp_path / "b", now=100)
    assert r.status == "applied" and blocked and "locked" in blocked[0]
    with sqlite3.connect(p) as c:
        assert c.execute("SELECT delivery_state, claimant_session_uuid FROM targeted_deliveries").fetchall() == [("pending", None)]


def test_v2_second_hop_failure(tmp_path, monkeypatch):
    p = tmp_path / "bridge.db"
    fixtures = __import__("pathlib").Path(__file__).parent / "fixtures"
    with sqlite3.connect(p) as c:
        c.execute("CREATE TABLE messages (id TEXT PRIMARY KEY)")
        c.executescript((fixtures / "v0_3_1_schema.sql").read_text())
        c.execute("INSERT INTO awareness_schema_version VALUES (2, 1, '0.2.0')")

    def boom(conn, *, now):
        raise sqlite3.OperationalError("injected second-hop DDL failure")

    monkeypatch.setattr(m, "_v3_to_v4_ddl", boom)
    r = apply_migration(p, backup_dir=tmp_path / "b", now=100)
    assert r.status == "failed" and r.version == 3 and "injected second-hop" in r.reason
    assert r.backup is not None and r.backup.path.name.endswith(".from-v3.sqlite")
    assert _v(p) == 3


# --- receipt-rollback-repro.py ----------------------------------------------

def test_receipt_rollback(tmp_path):
    p, db, owner, target, mid, facade, claim = _setup(tmp_path)
    assert _mark(db, owner, target, claim)
    db.execute("INSERT INTO disposition_receipts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
               (claim.message_id, claim.target_session_uuid, claim.target_session_incarnation_id,
                str(uuid.uuid4()), "receiver", claim.target_session_uuid, claim.target_session_incarnation_id,
                "accepted", 20, "", 20))
    assert row(db, mid)["delivery_state"] == "observed" and counts(db) == (1, 0)
    r = rollback(p, backup_dir=tmp_path / "rb", now=21)
    assert r.status == "refused" and "no recorded attempt" in r.reason
    late = finalize_attempt(db, claim_lease_token=claim.claim_lease_token, message_id=claim.message_id,
                            target_session_uuid=claim.target_session_uuid,
                            target_session_incarnation_id=claim.target_session_incarnation_id,
                            outcome="accepted_by_cli", reason="", attempted_at=10, now=22,
                            next_attempt_at=30, claimant=owner, clock=lambda: 22)
    assert late.status == "receipt_already_won" and counts(db) == (1, 1)
