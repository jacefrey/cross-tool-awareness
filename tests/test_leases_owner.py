"""Owner-aware leases API contract tests (v0.5.0, design R5/R6).

Pins:
- Every public leases writer requires `claimant: ClaimantIdentity` +
  `clock: Callable` kwargs; missing kwargs → TypeError.
- Each entry point rejects an active caller transaction with
  sqlite3.ProgrammingError and leaves the caller's txn untouched (R6).
- `claim_delivery` stamps the owner tuple on the claimed row and returns
  it on `ClaimedDelivery`.
- Default claim lease TTL is 30 s (R8).
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from cross_tool_awareness import (
    ClaimantIdentity,
    PresenceRecord,
    RegistrationSnapshot,
    apply as apply_migration,
)
from cross_tool_awareness.leases import (
    claim_delivery, pre_adapter_mark, pre_adapter_release, finalize_attempt,
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


def _fresh_db(tmp_path: Path) -> sqlite3.Connection:
    db_path = tmp_path / "bridge.db"
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.commit()
    conn.close()
    apply_migration(db_path, backup_dir=backup_dir, now=1)
    # isolation_level=None so BEGIN IMMEDIATE/COMMIT/ROLLBACK aren't
    # intercepted by Python's auto-begin (feedback_python_sqlite3_begin_exclusive).
    db = sqlite3.connect(str(db_path), isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


def _make_identity(fence: str = "f1") -> ClaimantIdentity:
    return ClaimantIdentity(session_uuid=uuid4(),
                            session_incarnation_id=uuid4(),
                            registration_fence=fence)


def _make_presence(cid: ClaimantIdentity, now: int = 10) -> PresenceRecord:
    return PresenceRecord(
        session_uuid=cid.session_uuid,
        session_incarnation_id=cid.session_incarnation_id,
        alias=f"a-{str(cid.session_uuid)[:8]}", tool="claude",
        project_slug="p", project_root="/tmp/p", role="worker",
        state="active", wake_capability="notify_only",
        registration_fence=cid.registration_fence,
        wake_locator="x",
        started_at=now - 100, heartbeat_at=now, expires_at=now + 86400,
    )


def _register(db: sqlite3.Connection, cid: ClaimantIdentity, *,
              holder_pid: int = 1234, process_started_at: int = 1_000_000,
              now: int = 10, tool: str = "claude",
              wake_capability: str = "notify_only",
              state: str = "active") -> None:
    db.execute(
        "INSERT INTO session_registrations"
        " (session_uuid, session_incarnation_id, alias, tool, project_slug,"
        "  project_root, role, state, wake_capability, registration_fence,"
        "  wake_locator, started_at, heartbeat_at, expires_at, revoked_at,"
        "  holder_pid, process_started_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (str(cid.session_uuid), str(cid.session_incarnation_id),
         f"a-{str(cid.session_uuid)[:8]}",
         tool, "p", "/tmp/p", "worker", state, wake_capability,
         cid.registration_fence, "x",
         now - 100, now, now + 86400, None,
         holder_pid, process_started_at),
    )


def _snapshot(cid: ClaimantIdentity, *, holder_pid: int = 1234,
              process_started_at: int = 1_000_000,
              proof_observed_at: int = 10) -> RegistrationSnapshot:
    # Presence mirrors the row _register() writes (registration now=10);
    # proof_observed_at varies independently (I03 exact comparison).
    return RegistrationSnapshot(
        presence=_make_presence(cid, now=10),
        holder_pid=holder_pid, process_started_at=process_started_at,
        proof_observed_at=proof_observed_at,
    )


def _seed_pending_delivery(db: sqlite3.Connection, target_cid: ClaimantIdentity,
                           *, now: int = 10) -> dict:
    msg_id = _u()
    db.execute("INSERT INTO messages(id, payload, created_at) VALUES (?,?,?)",
               (msg_id, "{}", now))
    db.execute(
        "INSERT INTO targeted_deliveries"
        " (message_id, target_session_uuid, target_session_incarnation_id,"
        "  sender_session_uuid, sender_session_incarnation_id,"
        "  delivery_state, created_at, next_attempt_at, expires_at)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        (msg_id, str(target_cid.session_uuid),
         str(target_cid.session_incarnation_id),
         _u(), _u(), "pending", now, now, now + 3600),
    )
    return {"message_id": msg_id,
            "target_uuid": str(target_cid.session_uuid),
            "target_inc": str(target_cid.session_incarnation_id)}


# --- Required-kwargs contract ----------------------------------------------


def test_claim_delivery_requires_claimant(tmp_path):
    db = _fresh_db(tmp_path)
    with pytest.raises(TypeError, match="claimant"):
        claim_delivery(db, now=10)  # type: ignore[call-arg]


def test_pre_adapter_mark_requires_claimant(tmp_path):
    db = _fresh_db(tmp_path)
    with pytest.raises(TypeError, match="claimant"):
        pre_adapter_mark(  # type: ignore[call-arg]
            db, message_id=_u(), target_session_uuid=_u(),
            target_session_incarnation_id=_u(),
            lease_token="t", now=1,
        )


def test_pre_adapter_release_requires_claimant(tmp_path):
    db = _fresh_db(tmp_path)
    with pytest.raises(TypeError, match="claimant"):
        pre_adapter_release(  # type: ignore[call-arg]
            db, claim_lease_token="t", next_state="pending",
            diagnostic="x", next_attempt_at=1,
        )


def test_finalize_attempt_requires_claimant(tmp_path):
    db = _fresh_db(tmp_path)
    with pytest.raises(TypeError, match="claimant"):
        finalize_attempt(  # type: ignore[call-arg]
            db, claim_lease_token="t", message_id=_u(),
            target_session_uuid=_u(), target_session_incarnation_id=_u(),
            outcome="accepted_by_cli", reason="",
            attempted_at=1, now=1, next_attempt_at=10,
        )


# --- Default lease TTL ------------------------------------------------------


def test_claim_delivery_default_ttl_is_30s(tmp_path):
    """R8: default claim lease TTL raised from 15 to 30 seconds."""
    db = _fresh_db(tmp_path)
    cid = _make_identity()
    _register(db, cid)
    target = _make_identity(fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    _seed_pending_delivery(db, target, now=10)
    claim = claim_delivery(db, now=10, claimant=cid, clock=lambda: 10,
                           claimant_snapshot=_snapshot(cid),
                           tool="claude", wake_capability="notify_only")
    assert claim is not None
    row = db.execute(
        "SELECT claim_lease_expires_at FROM targeted_deliveries"
        " WHERE claim_lease_token = ?", (claim.claim_lease_token,),
    ).fetchone()
    # 10 + 30 = 40 (default), not 10 + 15 = 25
    assert row["claim_lease_expires_at"] == 40


# --- Nested transaction rejection (R6, Review Focus item 5) ----------------


def test_claim_delivery_rejects_active_caller_transaction(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _make_identity()
    _register(db, cid)
    db.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(sqlite3.ProgrammingError, match="active caller"):
            claim_delivery(db, now=20, claimant=cid, clock=lambda: 20,
                           claimant_snapshot=_snapshot(cid, proof_observed_at=20),
                           tool="claude", wake_capability="notify_only")
    finally:
        db.execute("ROLLBACK")


def test_pre_adapter_mark_rejects_active_caller_transaction(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _make_identity()
    _register(db, cid)
    db.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(sqlite3.ProgrammingError, match="active caller"):
            pre_adapter_mark(
                db, message_id=_u(), target_session_uuid=_u(),
                target_session_incarnation_id=_u(),
                lease_token="tok", now=1,
                claimant=cid, clock=lambda: 1,
                claimant_snapshot=_snapshot(cid, proof_observed_at=1),
                target_snapshot=_snapshot(cid, proof_observed_at=1),
                tool="claude", wake_capability="notify_only",
            )
    finally:
        db.execute("ROLLBACK")


# --- Owner tuple stamping ---------------------------------------------------


def test_claim_delivery_stamps_owner_tuple_on_row(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _make_identity()
    _register(db, cid)
    target = _make_identity(fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    seed = _seed_pending_delivery(db, target, now=10)
    claim = claim_delivery(db, now=10, claimant=cid, clock=lambda: 10,
                           claimant_snapshot=_snapshot(cid),
                           tool="claude", wake_capability="notify_only")
    assert claim is not None
    assert claim.claimant_session_uuid == str(cid.session_uuid)
    assert claim.claimant_registration_fence == "f1"
    row = db.execute(
        "SELECT claimant_session_uuid, claimant_registration_fence"
        " FROM targeted_deliveries WHERE message_id = ?",
        (seed["message_id"],),
    ).fetchone()
    assert row["claimant_session_uuid"] == str(cid.session_uuid)
    assert row["claimant_registration_fence"] == "f1"


# --- Snapshot freshness window at claim time (design R7) -------------------


def test_claim_delivery_rejects_stale_snapshot(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _make_identity()
    _register(db, cid)
    target = _make_identity(fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    _seed_pending_delivery(db, target, now=100)
    # Snapshot observed 100 s ago; freshness window is 30 s → stale.
    claim = claim_delivery(
        db, now=100, claimant=cid, clock=lambda: 100,
        claimant_snapshot=_snapshot(cid, proof_observed_at=0),
        tool="claude", wake_capability="notify_only",
    )
    assert claim is None


# Rollback-on-exception coverage for every writer (with real failure
# injection, BaseException included) lives in tests/test_writer_rollback.py.
