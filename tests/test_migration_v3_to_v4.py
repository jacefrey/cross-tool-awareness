"""v3 → v4 migration tests (v0.5.0 claimant ownership).

Pins the behavior from the Phase 2b pre-live hardening design (R9):

- A bridge.db at schema v3 is upgraded in place to v4: targeted_deliveries
  gains claimant_* owner-tuple columns and the all-or-nothing + requires-
  full-owner CHECK; dispatch_ownership witness table is created.
- Pre-existing pending/observed/undeliverable rows preserve claimant_*
  as NULL. No owner identities are invented.
- The migration refuses non-quiescent state (any delivery in claimed,
  dispatching, notified, ambiguous, or blocked). Refusal is BEFORE any
  backup is written or any mutation applied — the DB stays byte-identical
  except for the stat atime.
- rollback() refuses when dispatch_ownership rows have no matching
  delivery (R4 "no destructive in-flight teardown").
- v2 bridges still migrate forward via v2→v3→v4 chain (preserved per
  design "do not silently strand the older supported path").
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from cross_tool_awareness import (
    EXPECTED_SCHEMA_VERSION,
    MANAGED_INDEXES,
    MANAGED_TABLES,
    apply as apply_migration,
    rollback as rollback_migration,
)
from cross_tool_awareness.migrations import PACKAGE_VERSION


_STUB_MESSAGES_DDL = """
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0
);
"""


def _v3_schema_sql() -> str:
    return (Path(__file__).parent / "fixtures" / "v0_4_0_schema.sql").read_text()


def _u() -> str:
    return str(uuid.uuid4())


def _seed_v3(tmp_path: Path, now: int = 999_000) -> Path:
    """Create a bridge.db carrying the v3 (v0.4.0) schema + a stub messages table."""
    db_path = tmp_path / "bridge.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(_STUB_MESSAGES_DDL)
        conn.executescript(_v3_schema_sql())
        conn.execute(
            "INSERT INTO awareness_schema_version"
            " (version, applied_at, package_version) VALUES (?, ?, ?)",
            (3, now, "0.4.0"),
        )
        conn.commit()
    finally:
        conn.close()
    return db_path


def _insert_delivery_v3(db_path: Path, state: str, *, msg_id: str,
                       token: str | None = None, lease_exp: int | None = None,
                       next_attempt_at: int = 1, expires_at: int = 10_000) -> None:
    """Insert a legacy v3 delivery row (no owner tuple)."""
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            "INSERT INTO messages(id, payload, created_at) VALUES (?,?,?)",
            (msg_id, "{}", 1),
        )
        conn.execute(
            "INSERT INTO targeted_deliveries("
            " message_id, target_session_uuid, target_session_incarnation_id,"
            " sender_session_uuid, sender_session_incarnation_id,"
            " delivery_state, claim_lease_token, claim_lease_expires_at,"
            " created_at, next_attempt_at, expires_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (msg_id, _u(), _u(), _u(), _u(), state, token, lease_exp,
             1, next_attempt_at, expires_at),
        )
        conn.commit()
    finally:
        conn.close()


def test_v3_to_v4_quiescent_migration_upgrades_in_place(tmp_path):
    db_path = _seed_v3(tmp_path)
    # Seed a benign pending + an observed row to prove history is preserved.
    _insert_delivery_v3(db_path, "pending", msg_id=_u())
    _insert_delivery_v3(db_path, "observed", msg_id=_u())
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    result = apply_migration(db_path, backup_dir=backup_dir, now=1_700_000_000)
    assert result.status == "applied", f"unexpected: {result.reason}"
    assert result.version == 4
    assert result.backup is not None
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    assert conn.execute(
        "SELECT version FROM awareness_schema_version ORDER BY version DESC LIMIT 1"
    ).fetchone()["version"] == 4
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(targeted_deliveries)")}
    assert {"claimant_session_uuid", "claimant_session_incarnation_id",
            "claimant_registration_fence"} <= cols
    # dispatch_ownership exists and is empty on fresh migration.
    assert conn.execute("SELECT COUNT(*) FROM dispatch_ownership").fetchone()[0] == 0
    # pre-existing rows preserved; owner tuple NULL (no inventing).
    rows = conn.execute(
        "SELECT delivery_state, claimant_session_uuid"
        " FROM targeted_deliveries ORDER BY delivery_state"
    ).fetchall()
    assert len(rows) == 2
    for r in rows:
        assert r["claimant_session_uuid"] is None


@pytest.mark.parametrize("state", ["claimed", "dispatching", "notified",
                                   "ambiguous", "blocked"])
def test_v3_to_v4_refuses_non_quiescent(tmp_path, state):
    db_path = _seed_v3(tmp_path)
    kwargs = {}
    if state in ("claimed", "dispatching"):
        kwargs["token"] = "tok"
        kwargs["lease_exp"] = 2
    _insert_delivery_v3(db_path, state, msg_id=_u(), **kwargs)
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    result = apply_migration(db_path, backup_dir=backup_dir, now=1_700_000_000)
    assert result.status == "refused", f"state={state}: {result!r}"
    assert state in result.reason
    # Preserve-unchanged: still v3, delivery row still present, columns unchanged.
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    assert conn.execute(
        "SELECT version FROM awareness_schema_version ORDER BY version DESC LIMIT 1"
    ).fetchone()["version"] == 3
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(targeted_deliveries)")}
    assert "claimant_session_uuid" not in cols, "v3 shape must be unchanged"
    row = conn.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE delivery_state = ?",
        (state,),
    ).fetchone()
    assert row is not None, "delivery row must be preserved"
    conn.close()


def test_v3_to_v4_refusal_creates_no_backup(tmp_path):
    """Review Focus item 3: refused migration must leave zero side effects."""
    db_path = _seed_v3(tmp_path)
    _insert_delivery_v3(db_path, "claimed", msg_id=_u(), token="tok", lease_exp=5)
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    result = apply_migration(db_path, backup_dir=backup_dir, now=1_700_000_000)
    assert result.status == "refused"
    assert result.backup is None, "backup must not be created on refusal"
    assert list(backup_dir.iterdir()) == [], "backup_dir must be empty on refusal"


def test_v4_fresh_install_still_works(tmp_path):
    """Fresh v4 install path is unchanged by migration code added for v3→v4."""
    db_path = tmp_path / "bridge.db"
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.commit()
    conn.close()
    result = apply_migration(db_path, backup_dir=backup_dir, now=1_700_000_000)
    assert result.status == "applied"
    assert result.version == 4


def test_v4_reapply_is_idempotent(tmp_path):
    db_path = tmp_path / "bridge.db"
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.commit()
    conn.close()
    first = apply_migration(db_path, backup_dir=backup_dir, now=1)
    assert first.status == "applied"
    second = apply_migration(db_path, backup_dir=backup_dir, now=2)
    assert second.status == "already_at_version"
    assert second.version == 4


def test_rollback_refuses_unmatched_witness(tmp_path):
    """R4 preservation: dispatch_ownership rows with no matching delivery are a
    history-preservation condition; rollback must refuse rather than drop them.
    """
    db_path = tmp_path / "bridge.db"
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.commit()
    conn.close()
    apply_migration(db_path, backup_dir=backup_dir, now=1)
    # Inject an orphaned witness (manually; this normally can't happen because
    # of the FK constraint — we temporarily disable FK to simulate stray state).
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute(
            "INSERT INTO dispatch_ownership("
            " claim_lease_token, message_id, target_session_uuid,"
            " target_session_incarnation_id,"
            " claimant_session_uuid, claimant_session_incarnation_id,"
            " claimant_registration_fence, dispatch_started_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            ("orphan-tok", _u(), _u(), _u(), _u(), _u(), "fence-X", 500),
        )
        conn.commit()
    finally:
        conn.close()
    result = rollback_migration(db_path, backup_dir=backup_dir, now=2)
    assert result.status == "refused"
    assert "dispatch_ownership" in result.reason or "witness" in result.reason


def test_v2_to_v4_chain_preserves_v2_hop(tmp_path):
    """A v2 bridge still migrates all the way forward — the chain v2→v3→v4
    preserves the accepted v2→v3 hop and then applies v3→v4 atop it.
    """
    # Reuse the existing v2 fixture.
    v2_sql_path = Path(__file__).parent / "fixtures" / "v0_3_1_schema.sql"
    if not v2_sql_path.exists():
        pytest.skip("v2 fixture not available")
    db_path = tmp_path / "bridge.db"
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(_STUB_MESSAGES_DDL)
        conn.executescript(v2_sql_path.read_text())
        conn.execute(
            "INSERT INTO awareness_schema_version"
            " (version, applied_at, package_version) VALUES (?, ?, ?)",
            (2, 999_000, "0.2.0"),
        )
        conn.commit()
    finally:
        conn.close()
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    result = apply_migration(db_path, backup_dir=backup_dir, now=1_700_000_000)
    assert result.status == "applied", f"v2→v4 chain failed: {result.reason}"
    assert result.version == 4
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(targeted_deliveries)")}
    # v3 column stays (dispatching), v4 columns added
    assert "dispatch_started_at" in cols
    assert "claimant_session_uuid" in cols
    conn.close()
