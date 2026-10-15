"""Schema v4 fresh install (v0.5.0 claimant ownership).

Pins the schema shape introduced by Phase 2b pre-live hardening (R3, R4):

- targeted_deliveries gains three nullable owner-tuple columns
  (claimant_session_uuid, claimant_session_incarnation_id,
  claimant_registration_fence) under all-or-nothing CHECK and a
  claimed/dispatching requires-full-owner CHECK.
- dispatch_ownership witness table keyed by claim_lease_token holds the
  full delivery key, three owner-tuple fields, and dispatch_started_at.
- awareness_schema_version.version == 4 after fresh install.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from cross_tool_awareness import apply as apply_migration
from cross_tool_awareness.migrations import EXPECTED_SCHEMA_VERSION


_STUB_MESSAGES_DDL = """
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0
);
"""


def _u() -> str:
    return str(uuid.uuid4())


def _fresh_v4_db(tmp_path: Path, now: int = 1_700_000_000) -> sqlite3.Connection:
    """Create a bridge.db, stub messages table, apply v4 schema, return a conn."""
    db_path = tmp_path / "bridge.db"
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    # Create the file + stub messages table (bridge-owned, not awareness-owned).
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.commit()
    conn.close()
    result = apply_migration(db_path, backup_dir=backup_dir, now=now)
    assert result.status == "applied", f"apply failed: {result.reason}"
    assert result.version == 4
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def test_expected_schema_version_is_4():
    assert EXPECTED_SCHEMA_VERSION == 4


def test_schema_version_row_is_4(tmp_path):
    db = _fresh_v4_db(tmp_path)
    row = db.execute(
        "SELECT version FROM awareness_schema_version ORDER BY version DESC LIMIT 1"
    ).fetchone()
    assert row["version"] == 4


def test_targeted_deliveries_has_owner_tuple_columns(tmp_path):
    db = _fresh_v4_db(tmp_path)
    cols = {r["name"] for r in db.execute("PRAGMA table_info(targeted_deliveries)")}
    assert {
        "claimant_session_uuid",
        "claimant_session_incarnation_id",
        "claimant_registration_fence",
    } <= cols


def test_dispatch_ownership_table_exists_with_full_shape(tmp_path):
    db = _fresh_v4_db(tmp_path)
    cols = {r["name"] for r in db.execute("PRAGMA table_info(dispatch_ownership)")}
    assert {
        "claim_lease_token",
        "message_id",
        "target_session_uuid",
        "target_session_incarnation_id",
        "claimant_session_uuid",
        "claimant_session_incarnation_id",
        "claimant_registration_fence",
        "dispatch_started_at",
    } <= cols
    # Primary key is claim_lease_token.
    pk_rows = [r for r in db.execute("PRAGMA table_info(dispatch_ownership)") if r["pk"] > 0]
    assert len(pk_rows) == 1
    assert pk_rows[0]["name"] == "claim_lease_token"


def test_owner_tuple_all_or_nothing(tmp_path):
    """Partial owner tuple on a pending delivery must violate CHECK."""
    db = _fresh_v4_db(tmp_path)
    msg_id, tgt_u, tgt_i, snd_u, snd_i = (_u(), _u(), _u(), _u(), _u())
    with db:
        db.execute(
            "INSERT INTO messages(id, payload, created_at) VALUES (?,?,?)",
            (msg_id, "{}", 1),
        )
    with pytest.raises(sqlite3.IntegrityError):
        with db:
            db.execute(
                "INSERT INTO targeted_deliveries("
                " message_id, target_session_uuid, target_session_incarnation_id,"
                " sender_session_uuid, sender_session_incarnation_id,"
                " delivery_state, created_at, next_attempt_at, expires_at,"
                " claimant_session_uuid)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (msg_id, tgt_u, tgt_i, snd_u, snd_i, "pending", 1, 1, 10, "owner-uuid"),
            )


def test_claimed_requires_full_owner_and_token(tmp_path):
    """A claimed delivery with NULL owner must violate CHECK."""
    db = _fresh_v4_db(tmp_path)
    msg_id, tgt_u, tgt_i, snd_u, snd_i = (_u(), _u(), _u(), _u(), _u())
    with db:
        db.execute(
            "INSERT INTO messages(id, payload, created_at) VALUES (?,?,?)",
            (msg_id, "{}", 1),
        )
    with pytest.raises(sqlite3.IntegrityError):
        with db:
            db.execute(
                "INSERT INTO targeted_deliveries("
                " message_id, target_session_uuid, target_session_incarnation_id,"
                " sender_session_uuid, sender_session_incarnation_id,"
                " delivery_state, claim_lease_token, claim_lease_expires_at,"
                " created_at, next_attempt_at, expires_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (msg_id, tgt_u, tgt_i, snd_u, snd_i, "claimed",
                 "tok", 5, 1, 1, 10),
            )


def test_dispatching_requires_full_owner_and_token(tmp_path):
    """A dispatching delivery with NULL owner must violate CHECK."""
    db = _fresh_v4_db(tmp_path)
    msg_id, tgt_u, tgt_i, snd_u, snd_i = (_u(), _u(), _u(), _u(), _u())
    with db:
        db.execute(
            "INSERT INTO messages(id, payload, created_at) VALUES (?,?,?)",
            (msg_id, "{}", 1),
        )
    with pytest.raises(sqlite3.IntegrityError):
        with db:
            db.execute(
                "INSERT INTO targeted_deliveries("
                " message_id, target_session_uuid, target_session_incarnation_id,"
                " sender_session_uuid, sender_session_incarnation_id,"
                " delivery_state, claim_lease_token, claim_lease_expires_at,"
                " created_at, next_attempt_at, expires_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (msg_id, tgt_u, tgt_i, snd_u, snd_i, "dispatching",
                 "tok", 5, 1, 1, 10),
            )


def test_claimed_with_full_owner_tuple_accepted(tmp_path):
    """Positive: a claimed delivery with full owner tuple + token must succeed."""
    db = _fresh_v4_db(tmp_path)
    msg_id, tgt_u, tgt_i, snd_u, snd_i = (_u(), _u(), _u(), _u(), _u())
    with db:
        db.execute(
            "INSERT INTO messages(id, payload, created_at) VALUES (?,?,?)",
            (msg_id, "{}", 1),
        )
        db.execute(
            "INSERT INTO targeted_deliveries("
            " message_id, target_session_uuid, target_session_incarnation_id,"
            " sender_session_uuid, sender_session_incarnation_id,"
            " delivery_state, claim_lease_token, claim_lease_expires_at,"
            " claimant_session_uuid, claimant_session_incarnation_id,"
            " claimant_registration_fence,"
            " created_at, next_attempt_at, expires_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (msg_id, tgt_u, tgt_i, snd_u, snd_i, "claimed",
             "tok", 5, _u(), _u(), "fence-A", 1, 1, 10),
        )
    row = db.execute(
        "SELECT delivery_state, claimant_registration_fence"
        " FROM targeted_deliveries WHERE message_id = ?", (msg_id,),
    ).fetchone()
    assert row["delivery_state"] == "claimed"
    assert row["claimant_registration_fence"] == "fence-A"
