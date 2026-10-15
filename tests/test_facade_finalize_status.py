"""I1 regression test — CompositeStorage.finalize_attempt must surface
the full status (finalized / unauthorized / already_finalized /
lease_already_released / receipt_already_won), not collapse everything
except receipt_already_won to None.

Reason: a CXM-consumer must distinguish 'wrote attempt and advanced state'
from 'wrong owner — nothing happened' from 'already recorded, idempotent'
from 'lease was released concurrently.' Collapsing to None forces the
consumer to re-read SQL to figure out what actually happened; worse, it
can mis-interpret unauthorized as success and silently drop a retry.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from cross_tool_awareness import (
    ClaimantIdentity, CompositeStorage, PresenceRecord, QueueAttempt,
    QueueOutcome, RegistrationSnapshot,
    apply as apply_migration,
)


_STUB_MESSAGES_DDL = """
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0
);
"""


def _u() -> str:
    return str(uuid.uuid4())


def _fresh(tmp_path: Path) -> sqlite3.Connection:
    db_path = tmp_path / "bridge.db"
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.commit()
    conn.close()
    apply_migration(db_path, backup_dir=backup_dir, now=1)
    db = sqlite3.connect(str(db_path), isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


def _id(fence: str = "f1") -> ClaimantIdentity:
    return ClaimantIdentity(session_uuid=uuid4(),
                            session_incarnation_id=uuid4(),
                            registration_fence=fence)


def _pres(cid: ClaimantIdentity, now: int = 10) -> PresenceRecord:
    return PresenceRecord(
        session_uuid=cid.session_uuid,
        session_incarnation_id=cid.session_incarnation_id,
        alias="c", tool="claude",
        project_slug="p", project_root="/tmp/p", role="worker",
        state="active", wake_capability="notify_only",
        registration_fence=cid.registration_fence,
        wake_locator="x",
        started_at=now - 100, heartbeat_at=now, expires_at=now + 86400,
    )


def _snap(cid: ClaimantIdentity, *, hp=1234, pst=1_000_000, now=10):
    return RegistrationSnapshot(
        presence=_pres(cid, now=now), holder_pid=hp,
        process_started_at=pst, proof_observed_at=now,
    )


def _register(db, cid, *, hp=1234, pst=1_000_000, now=10):
    db.execute(
        "INSERT INTO session_registrations"
        " (session_uuid, session_incarnation_id, alias, tool, project_slug,"
        "  project_root, role, state, wake_capability, registration_fence,"
        "  wake_locator, started_at, heartbeat_at, expires_at, revoked_at,"
        "  holder_pid, process_started_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (str(cid.session_uuid), str(cid.session_incarnation_id),
         f"a-{str(cid.session_uuid)[:8]}", "claude", "p", "/tmp/p", "worker",
         "active", "notify_only", cid.registration_fence, "x",
         now - 100, now, now + 86400, None, hp, pst),
    )


def _seed_claim_and_mark(db, cid, target, now=10):
    msg = _u()
    db.execute("INSERT INTO messages(id,payload,created_at) VALUES (?,?,?)",
               (msg, "{}", now))
    db.execute(
        "INSERT INTO targeted_deliveries"
        " (message_id, target_session_uuid, target_session_incarnation_id,"
        "  sender_session_uuid, sender_session_incarnation_id,"
        "  delivery_state, created_at, next_attempt_at, expires_at)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        (msg, str(target.session_uuid), str(target.session_incarnation_id),
         _u(), _u(), "pending", now, now, now + 3600),
    )
    facade = CompositeStorage(db, tool="claude", wake_capability="notify_only",
                              claimant=cid, clock=lambda: now)
    claim = facade.claim_delivery(now=now, recovery_after=300,
                                  claimant_snapshot=_snap(cid, now=now))
    assert claim is not None
    ok = facade.pre_adapter_mark(
        claim, now, lease_token=claim.claim_lease_token,
        claimant_snapshot=_snap(cid, now=now),
        target_snapshot=_snap(target, hp=5678, pst=2_000_000, now=now),
    )
    assert ok
    return facade, claim


# --- The I1 regression -----------------------------------------------------


def test_facade_finalize_surfaces_finalized_status(tmp_path):
    """Happy-path finalize must return a result with status=='finalized'
    and new_state=='notified', not None."""
    db = _fresh(tmp_path)
    cid = _id()
    target = _id(fence="target-f")
    _register(db, cid)
    _register(db, target, hp=5678, pst=2_000_000)
    facade, claim = _seed_claim_and_mark(db, cid, target)
    attempt = QueueAttempt(
        message_id=UUID(claim.message_id),
        target_session_uuid=UUID(claim.target_session_uuid),
        target_session_incarnation_id=UUID(claim.target_session_incarnation_id),
        outcome=QueueOutcome.ACCEPTED_BY_CLI,
        attempted_at=11, finished_at=12, reason="",
    )
    result = facade.finalize_attempt(claim, attempt,
                                     lease_token=claim.claim_lease_token)
    # Pre-fix behavior returned None for status=='finalized'. The caller
    # cannot tell this apart from unauthorized / already_finalized.
    assert result is not None, "finalized must surface a result object"
    assert result.status == "finalized"
    assert result.new_state == "notified"
    assert result.attempt_recorded is True


def test_facade_finalize_surfaces_unauthorized_status(tmp_path):
    """A finalize call from the wrong owner must return status=='unauthorized',
    not None. Collapsing to None lets the caller silently drop a retry."""
    db = _fresh(tmp_path)
    cid = _id()
    other = _id(fence="other-f")
    target = _id(fence="target-f")
    _register(db, cid)
    _register(db, other)
    _register(db, target, hp=5678, pst=2_000_000)
    _facade, claim = _seed_claim_and_mark(db, cid, target)

    # Build a SECOND facade bound to the wrong claimant; call finalize via it.
    wrong_facade = CompositeStorage(
        db, tool="claude", wake_capability="notify_only",
        claimant=other, clock=lambda: 15,
    )
    attempt = QueueAttempt(
        message_id=UUID(claim.message_id),
        target_session_uuid=UUID(claim.target_session_uuid),
        target_session_incarnation_id=UUID(claim.target_session_incarnation_id),
        outcome=QueueOutcome.ACCEPTED_BY_CLI,
        attempted_at=12, finished_at=13, reason="",
    )
    result = wrong_facade.finalize_attempt(claim, attempt,
                                           lease_token=claim.claim_lease_token)
    assert result is not None, "unauthorized must surface a result object"
    assert result.status == "unauthorized"
    assert result.attempt_recorded is False


def test_facade_finalize_receipt_already_won_still_surfaces_receipt(tmp_path):
    """Existing behavior: receipt_already_won path must still return a
    DispositionReceipt (or an object carrying one)."""
    from cross_tool_awareness import DispositionReceipt

    db = _fresh(tmp_path)
    cid = _id()
    target = _id(fence="target-f")
    _register(db, cid)
    _register(db, target, hp=5678, pst=2_000_000)
    facade, claim = _seed_claim_and_mark(db, cid, target)
    # Receiver inserts a disposition_receipt → delivery becomes observed.
    db.execute(
        "INSERT INTO disposition_receipts("
        " message_id, target_session_uuid, target_session_incarnation_id,"
        " correlation_id, consumer_id, receiver_session_uuid,"
        " receiver_session_incarnation_id, disposition, observed_at, reason,"
        " delivered_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (claim.message_id, claim.target_session_uuid,
         claim.target_session_incarnation_id, _u(), "codex-receiver",
         claim.target_session_uuid, claim.target_session_incarnation_id,
         "accepted", 20, "", 20),
    )
    attempt = QueueAttempt(
        message_id=UUID(claim.message_id),
        target_session_uuid=UUID(claim.target_session_uuid),
        target_session_incarnation_id=UUID(claim.target_session_incarnation_id),
        outcome=QueueOutcome.ACCEPTED_BY_CLI,
        attempted_at=21, finished_at=22, reason="",
    )
    result = facade.finalize_attempt(claim, attempt,
                                     lease_token=claim.claim_lease_token)
    # Caller needs BOTH: status == "receipt_already_won" AND a receipt object.
    assert result is not None
    assert result.status == "receipt_already_won"
    assert result.receipt is not None
    assert isinstance(result.receipt, DispositionReceipt)
    assert str(result.receipt.receiver_session_uuid) == claim.target_session_uuid
