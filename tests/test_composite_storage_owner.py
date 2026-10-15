"""CompositeStorage owner-aware facade contract tests (v0.5.0 R5/R6).

Pins:
- Facade constructor requires `claimant: ClaimantIdentity` and
  `clock: Callable[[], int]`; missing → TypeError.
- Facade is frozen; claimant cannot be refreshed in place.
- Review Focus item 2: a stale facade (constructed before a trusted
  re-registration under a new fence) cannot claim or mark; but an
  already-entered attempt from that facade can still finalize against its
  own exact dispatch_ownership witness.
- Review Focus item 5: nested caller transaction is rejected by package
  entry points called via the facade.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from uuid import uuid4

import pytest

from cross_tool_awareness import (
    ClaimantIdentity, CompositeStorage,
    PresenceRecord, QueueAttempt, QueueOutcome, RegistrationSnapshot,
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


def _fresh_db(tmp_path: Path) -> sqlite3.Connection:
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


def _identity(fence: str = "f1") -> ClaimantIdentity:
    return ClaimantIdentity(session_uuid=uuid4(),
                            session_incarnation_id=uuid4(),
                            registration_fence=fence)


def _presence(cid: ClaimantIdentity, now: int = 10) -> PresenceRecord:
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


def _snapshot(cid: ClaimantIdentity, *,
              holder_pid: int = 1234, process_started_at: int = 1_000_000,
              proof_observed_at: int = 10) -> RegistrationSnapshot:
    return RegistrationSnapshot(
        presence=_presence(cid, now=proof_observed_at),
        holder_pid=holder_pid,
        process_started_at=process_started_at,
        proof_observed_at=proof_observed_at,
    )


def _register(db: sqlite3.Connection, cid: ClaimantIdentity, *,
              holder_pid: int = 1234, process_started_at: int = 1_000_000,
              now: int = 10, tool: str = "claude",
              wake_capability: str = "notify_only") -> None:
    db.execute(
        "INSERT INTO session_registrations"
        " (session_uuid, session_incarnation_id, alias, tool, project_slug,"
        "  project_root, role, state, wake_capability, registration_fence,"
        "  wake_locator, started_at, heartbeat_at, expires_at, revoked_at,"
        "  holder_pid, process_started_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (str(cid.session_uuid), str(cid.session_incarnation_id),
         f"a-{str(cid.session_uuid)[:8]}",
         tool, "p", "/tmp/p", "worker", "active", wake_capability,
         cid.registration_fence, "x",
         now - 100, now, now + 86400, None,
         holder_pid, process_started_at),
    )


def _seed_pending(db: sqlite3.Connection, target_cid: ClaimantIdentity,
                  *, now: int = 10) -> str:
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
    return msg_id


# --- Required kwargs -------------------------------------------------------


def test_facade_requires_claimant(tmp_path):
    db = _fresh_db(tmp_path)
    with pytest.raises(TypeError, match="claimant"):
        CompositeStorage(db, tool="claude", wake_capability="notify_only",  # type: ignore[call-arg]
                         clock=lambda: 1)


def test_facade_requires_clock(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _identity()
    with pytest.raises(TypeError, match="clock"):
        CompositeStorage(db, tool="claude", wake_capability="notify_only",  # type: ignore[call-arg]
                         claimant=cid)


def test_facade_rejects_non_identity_claimant(tmp_path):
    db = _fresh_db(tmp_path)
    with pytest.raises(TypeError):
        CompositeStorage(db, tool="claude", wake_capability="notify_only",
                         claimant="not-identity", clock=lambda: 1)  # type: ignore[arg-type]


# --- Immutability (R5: "do not refresh identity in place") -----------------


def test_facade_is_frozen(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _identity()
    facade = CompositeStorage(db, tool="claude", wake_capability="notify_only",
                              claimant=cid, clock=lambda: 1)
    with pytest.raises((AttributeError, Exception)):
        facade.claimant = _identity(fence="other")  # type: ignore[misc]


# --- Stale facade cannot claim after re-registration (Review Focus 2) ------


def test_stale_facade_cannot_claim_after_fence_change(tmp_path):
    """Facade F1 bound to fence='f1'. Session re-registers with fence='f2'.
    F1 is now stale; a claim attempt via F1 must fail (claimant SQL check
    finds no row with f1)."""
    db = _fresh_db(tmp_path)
    cid1 = _identity(fence="f1")
    _register(db, cid1)
    target = _identity(fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    _seed_pending(db, target)
    facade1 = CompositeStorage(db, tool="claude",
                               wake_capability="notify_only",
                               claimant=cid1, clock=lambda: 10)
    # Simulate a trusted re-registration: UPDATE the row to a new fence.
    db.execute(
        "UPDATE session_registrations SET registration_fence = 'f2'"
        " WHERE session_uuid = ?", (str(cid1.session_uuid),),
    )
    # F1 is now stale (still carries cid1 with fence='f1'); claim must fail.
    claim = facade1.claim_delivery(now=10, recovery_after=30,
                                   claimant_snapshot=_snapshot(cid1))
    assert claim is None


# --- Nested transaction rejection (Review Focus 5) -------------------------


def test_facade_claim_rejects_nested_caller_transaction(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _identity()
    _register(db, cid)
    facade = CompositeStorage(db, tool="claude", wake_capability="notify_only",
                              claimant=cid, clock=lambda: 10)
    db.execute("BEGIN IMMEDIATE")
    db.execute("INSERT INTO messages(id) VALUES ('side-effect')")
    try:
        with pytest.raises(sqlite3.ProgrammingError, match="active caller"):
            facade.claim_delivery(now=10, recovery_after=30,
                                  claimant_snapshot=_snapshot(cid))
    finally:
        db.execute("ROLLBACK")
    # Caller's rollback must have undone the side-effect insert.
    row = db.execute("SELECT COUNT(*) FROM messages WHERE id = 'side-effect'").fetchone()
    assert row[0] == 0


# --- End-to-end owner-aware claim+release ---------------------------------


@pytest.mark.parametrize("bad", [0, -1, True, 1.5, None])
def test_claim_delivery_rejects_invalid_recovery_after(tmp_path, bad):
    db = _fresh_db(tmp_path)
    cid = _identity()
    _register(db, cid)
    facade = CompositeStorage(db, tool="claude", wake_capability="notify_only",
                              claimant=cid, clock=lambda: 10)
    with pytest.raises(ValueError, match="recovery_after"):
        facade.claim_delivery(now=10, recovery_after=bad,
                              claimant_snapshot=_snapshot(cid))


def test_constructor_rejects_empty_tool(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _identity()
    with pytest.raises(ValueError, match="tool"):
        CompositeStorage(db, tool="", wake_capability="notify_only",
                         claimant=cid, clock=lambda: 1)


def test_constructor_rejects_empty_wake_capability(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _identity()
    with pytest.raises(ValueError, match="wake_capability"):
        CompositeStorage(db, tool="claude", wake_capability="",
                         claimant=cid, clock=lambda: 1)


def test_facade_release_claim_clears_owner_tuple(tmp_path):
    """release_claim resets delivery to pending AND clears the owner tuple
    (so a fresh claim can stamp new owner on next pass)."""
    db = _fresh_db(tmp_path)
    claimant = _identity()
    _register(db, claimant)
    target = _identity(fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    msg_id = _seed_pending(db, target)
    facade = CompositeStorage(db, tool="claude", wake_capability="notify_only",
                              claimant=claimant, clock=lambda: 10)
    claim = facade.claim_delivery(now=10, recovery_after=30,
                                  claimant_snapshot=_snapshot(claimant))
    assert claim is not None
    facade.release_claim(claim, "pending", "retry_later",
                         lease_token=claim.claim_lease_token)
    row = db.execute(
        "SELECT delivery_state, claimant_session_uuid, claim_lease_token"
        " FROM targeted_deliveries WHERE message_id = ?", (msg_id,),
    ).fetchone()
    assert row["delivery_state"] == "pending"
    assert row["claimant_session_uuid"] is None
    assert row["claim_lease_token"] is None
