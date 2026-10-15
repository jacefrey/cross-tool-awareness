"""dispatch_ownership witness semantics (v0.5.0, design R4 + R5).

Pins:
- pre_adapter_mark writes a dispatch_ownership row atomic with the
  claimed→dispatching state change.
- When a receipt arrives (tr_receipt_marks_observed trigger clears
  claim_lease_token/claim_lease_expires_at and sets delivery_state to
  'observed'), the dispatch_ownership row is RETAINED.
- A late finalize_attempt call from the matching owner authenticates
  against the retained witness and returns `receipt_already_won` rather
  than unauthorized.
- A finalize_attempt call with wrong token, wrong owner, or wrong key
  returns `unauthorized` and changes zero rows. (Review Focus item 4.)
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from cross_tool_awareness import (
    ClaimantIdentity, Disposition, DispositionReceipt, PresenceRecord,
    QueueAttempt, QueueOutcome, RegistrationSnapshot,
    apply as apply_migration,
)
from cross_tool_awareness.leases import (
    claim_delivery, pre_adapter_mark, finalize_attempt,
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
        alias=f"a-{str(cid.session_uuid)[:8]}", tool="claude",
        project_slug="p", project_root="/tmp/p", role="worker",
        state="active", wake_capability="notify_only",
        registration_fence=cid.registration_fence,
        wake_locator="x",
        started_at=now - 100, heartbeat_at=now, expires_at=now + 86400,
    )


def _snapshot(cid: ClaimantIdentity, *,
              holder_pid: int = 1234, process_started_at: int = 1_000_000,
              proof_observed_at: int = 10) -> RegistrationSnapshot:
    # Presence mirrors the row _register() writes (registration now=10);
    # proof_observed_at varies independently (I03 exact comparison).
    return RegistrationSnapshot(
        presence=_presence(cid, now=10),
        holder_pid=holder_pid,
        process_started_at=process_started_at,
        proof_observed_at=proof_observed_at,
    )


def _register(db, cid: ClaimantIdentity, *, holder_pid: int = 1234,
              process_started_at: int = 1_000_000, now: int = 10) -> None:
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
         now - 100, now, now + 86400, None,
         holder_pid, process_started_at),
    )


def _seed_pending(db, target_cid: ClaimantIdentity, *, now: int = 10) -> str:
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


def _claim_and_mark(db, cid, target, msg_id, *, now=10):
    """Helper: claim pending, mark dispatching + witness."""
    claim = claim_delivery(
        db, now=now, claimant=cid, clock=lambda: now,
        claimant_snapshot=_snapshot(cid, proof_observed_at=now),
        tool="claude", wake_capability="notify_only",
    )
    assert claim is not None
    ok = pre_adapter_mark(
        db, message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        lease_token=claim.claim_lease_token, now=now,
        claimant=cid, clock=lambda: now,
        claimant_snapshot=_snapshot(cid, proof_observed_at=now),
        target_snapshot=_snapshot(target, holder_pid=5678,
                                   process_started_at=2_000_000,
                                   proof_observed_at=now),
        tool="claude", wake_capability="notify_only",
    )
    assert ok
    return claim


# --- Witness insertion is atomic with state change (R4) --------------------


def test_pre_adapter_mark_inserts_witness_atomic_with_state_change(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _identity()
    _register(db, cid)
    target = _identity(fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    msg_id = _seed_pending(db, target)
    assert db.execute("SELECT COUNT(*) FROM dispatch_ownership").fetchone()[0] == 0
    claim = _claim_and_mark(db, cid, target, msg_id, now=10)
    witness = db.execute(
        "SELECT * FROM dispatch_ownership WHERE claim_lease_token = ?",
        (claim.claim_lease_token,),
    ).fetchone()
    assert witness is not None
    assert witness["claimant_session_uuid"] == str(cid.session_uuid)
    assert witness["claimant_registration_fence"] == cid.registration_fence
    state = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id = ?",
        (claim.message_id,),
    ).fetchone()["delivery_state"]
    assert state == "dispatching"


# --- Receipt-wins path preserves the witness (R4, Review Focus item 4) -----


def test_receipt_clears_claimant_tuple_on_observed_row(tmp_path):
    """I2 regression: the receipt trigger must clear the claimant_* trio on
    the observed delivery row, not just the lease fields. Keeping the
    claimant tuple stamped on an observed row violates "witness is the sole
    provenance" (audit queries like WHERE claimant_session_uuid = ? to find
    'work this session owned' would over-count observed rows)."""
    db = _fresh_db(tmp_path)
    cid = _identity()
    _register(db, cid)
    target = _identity(fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    msg_id = _seed_pending(db, target)
    claim = _claim_and_mark(db, cid, target, msg_id, now=10)
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
    row = db.execute(
        "SELECT delivery_state,"
        " claim_lease_token, claim_lease_expires_at,"
        " claimant_session_uuid, claimant_session_incarnation_id,"
        " claimant_registration_fence"
        " FROM targeted_deliveries WHERE message_id = ?", (claim.message_id,),
    ).fetchone()
    assert row["delivery_state"] == "observed"
    # Pre-fix: these three were still populated with cid's values. Post-fix:
    # all three NULL so the delivery row doesn't lie about who owned it.
    assert row["claimant_session_uuid"] is None
    assert row["claimant_session_incarnation_id"] is None
    assert row["claimant_registration_fence"] is None
    # Lease fields still cleared (unchanged behavior).
    assert row["claim_lease_token"] is None
    assert row["claim_lease_expires_at"] is None


def test_witness_retained_after_receipt_wins(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _identity()
    _register(db, cid)
    target = _identity(fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    msg_id = _seed_pending(db, target)
    claim = _claim_and_mark(db, cid, target, msg_id, now=10)
    # Receiver inserts a disposition_receipt. The tr_receipt_marks_observed
    # trigger clears the claim_lease_token / claim_lease_expires_at on the
    # delivery and sets state to 'observed'.
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
    delivery = db.execute(
        "SELECT delivery_state, claim_lease_token, claim_lease_expires_at"
        " FROM targeted_deliveries WHERE message_id = ?", (claim.message_id,),
    ).fetchone()
    assert delivery["delivery_state"] == "observed"
    assert delivery["claim_lease_token"] is None
    assert delivery["claim_lease_expires_at"] is None
    # Witness MUST still be there.
    witness_count = db.execute(
        "SELECT COUNT(*) FROM dispatch_ownership WHERE claim_lease_token = ?",
        (claim.claim_lease_token,),
    ).fetchone()[0]
    assert witness_count == 1


# --- Late finalize authenticates against the witness (R4) ------------------


def test_late_finalize_authenticates_against_witness(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _identity()
    _register(db, cid)
    target = _identity(fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    msg_id = _seed_pending(db, target)
    claim = _claim_and_mark(db, cid, target, msg_id, now=10)
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
    # Owner's late finalize_attempt — witness authenticates, delivery is
    # already observed so no state advance; receipt_already_won returned.
    result = finalize_attempt(
        db, claim_lease_token=claim.claim_lease_token,
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        outcome="accepted_by_cli", reason="",
        attempted_at=15, now=25, next_attempt_at=100,
        claimant=cid, clock=lambda: 25,
    )
    assert result.status == "receipt_already_won"
    assert result.attempt_recorded is True


# --- Wrong owner / wrong token rejected (unauthorized) ---------------------


def test_finalize_wrong_owner_returns_unauthorized_no_rows_changed(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _identity()
    other = _identity(fence="other-f")  # different claimant identity
    _register(db, cid)
    _register(db, other)
    target = _identity(fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    msg_id = _seed_pending(db, target)
    claim = _claim_and_mark(db, cid, target, msg_id, now=10)
    attempts_before = db.execute(
        "SELECT COUNT(*) FROM queue_attempts").fetchone()[0]

    result = finalize_attempt(
        db, claim_lease_token=claim.claim_lease_token,
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        outcome="accepted_by_cli", reason="",
        attempted_at=15, now=16, next_attempt_at=100,
        claimant=other,                 # WRONG owner — witness lookup fails
        clock=lambda: 16,
    )
    assert result.status == "unauthorized"
    assert result.attempt_recorded is False
    # Zero rows changed in queue_attempts.
    attempts_after = db.execute(
        "SELECT COUNT(*) FROM queue_attempts").fetchone()[0]
    assert attempts_after == attempts_before
    # Delivery still dispatching, owned by cid.
    row = db.execute(
        "SELECT delivery_state, claimant_session_uuid"
        " FROM targeted_deliveries WHERE message_id = ?",
        (claim.message_id,),
    ).fetchone()
    assert row["delivery_state"] == "dispatching"
    assert row["claimant_session_uuid"] == str(cid.session_uuid)


def test_finalize_wrong_token_returns_unauthorized(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _identity()
    _register(db, cid)
    target = _identity(fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    msg_id = _seed_pending(db, target)
    claim = _claim_and_mark(db, cid, target, msg_id, now=10)
    result = finalize_attempt(
        db, claim_lease_token="forged-token",      # WRONG token
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        outcome="accepted_by_cli", reason="",
        attempted_at=15, now=16, next_attempt_at=100,
        claimant=cid, clock=lambda: 16,
    )
    assert result.status == "unauthorized"


# --- Happy path: owner finalizes successfully, state advances --------------


def test_finalize_owner_happy_path_advances_state(tmp_path):
    db = _fresh_db(tmp_path)
    cid = _identity()
    _register(db, cid)
    target = _identity(fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    msg_id = _seed_pending(db, target)
    claim = _claim_and_mark(db, cid, target, msg_id, now=10)
    result = finalize_attempt(
        db, claim_lease_token=claim.claim_lease_token,
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        outcome="accepted_by_cli", reason="",
        attempted_at=15, now=16, next_attempt_at=100,
        claimant=cid, clock=lambda: 16,
    )
    assert result.status == "finalized"
    assert result.new_state == "notified"
    assert result.attempt_recorded is True
    row = db.execute(
        "SELECT delivery_state, claim_lease_token, claimant_session_uuid"
        " FROM targeted_deliveries WHERE message_id = ?", (claim.message_id,),
    ).fetchone()
    assert row["delivery_state"] == "notified"
    assert row["claim_lease_token"] is None
    assert row["claimant_session_uuid"] is None
