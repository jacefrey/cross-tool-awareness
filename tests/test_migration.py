"""Migration tests (§12.6)."""

import sqlite3
from pathlib import Path

import pytest

from cross_tool_awareness import (
    EXPECTED_SCHEMA_VERSION,
    MANAGED_INDEXES,
    MANAGED_TABLES,
    MANAGED_TRIGGERS,
    apply as apply_migration,
    rollback as rollback_migration,
)


def _collect_managed(path: Path) -> dict:
    with sqlite3.connect(str(path)) as db:
        rows = db.execute(
            "SELECT type, name FROM sqlite_master "
            "WHERE type IN ('table', 'index', 'trigger') "
            "  AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return {
            "tables": {n for t, n in rows if t == "table" and n in MANAGED_TABLES},
            "indexes": {n for t, n in rows if t == "index" and n in MANAGED_INDEXES},
            "triggers": {n for t, n in rows if t == "trigger" and n in MANAGED_TRIGGERS},
        }


def test_apply_on_fresh_bridge_creates_all_managed_state(file_db, tmp_path):
    backup_dir = tmp_path / "backups"
    result = apply_migration(file_db, backup_dir=backup_dir, now=1_000_000)
    assert result.status == "applied"
    assert result.version == EXPECTED_SCHEMA_VERSION
    assert result.backup is not None

    actual = _collect_managed(file_db)
    assert actual["tables"] == MANAGED_TABLES
    assert actual["indexes"] == MANAGED_INDEXES
    assert actual["triggers"] == MANAGED_TRIGGERS


def test_apply_on_already_migrated_returns_already_at_version(file_db_migrated, tmp_path):
    backup_dir = tmp_path / "backups2"
    result = apply_migration(file_db_migrated, backup_dir=backup_dir, now=1_000_010)
    assert result.status == "already_at_version"
    assert result.version == EXPECTED_SCHEMA_VERSION


def test_apply_refuses_partial_managed_state(file_db_migrated, tmp_path):
    # Drop one managed table to create a partial state.
    with sqlite3.connect(str(file_db_migrated)) as db:
        db.execute("DROP TABLE queue_attempts")
    backup_dir = tmp_path / "backups3"
    result = apply_migration(file_db_migrated, backup_dir=backup_dir, now=1_000_020)
    assert result.status == "partial_or_unknown_managed_state"


def test_apply_refuses_managed_present_without_version(file_db, tmp_path):
    # Install the schema manually without the version row.
    schema = (Path(__file__).resolve().parent.parent / "src"
              / "cross_tool_awareness" / "schema.sql").read_text()
    with sqlite3.connect(str(file_db)) as db:
        db.executescript(schema)
        # Deliberately omit INSERT INTO awareness_schema_version
    backup_dir = tmp_path / "backups4"
    result = apply_migration(file_db, backup_dir=backup_dir, now=1_000_030)
    assert result.status == "partial_or_unknown_managed_state"


def test_apply_fails_on_missing_db(tmp_path):
    missing = tmp_path / "does_not_exist.db"
    result = apply_migration(missing, backup_dir=tmp_path / "backups", now=1_000_000)
    assert result.status == "failed"


def test_rollback_refuses_on_in_flight_delivery(file_db_migrated, tmp_path):
    import uuid as _uuid
    with sqlite3.connect(str(file_db_migrated)) as db:
        db.execute("INSERT INTO messages (id) VALUES ('m1')")
        db.execute(
            "INSERT INTO targeted_deliveries "
            "(message_id, target_session_uuid, target_session_incarnation_id, "
            " sender_session_uuid, sender_session_incarnation_id, delivery_state, "
            " created_at, next_attempt_at, expires_at) "
            "VALUES ('m1', ?, ?, ?, ?, 'claimed', 1, 1, 1_000_000)",
            (str(_uuid.uuid4()), str(_uuid.uuid4()),
             str(_uuid.uuid4()), str(_uuid.uuid4())),
        )
    result = rollback_migration(file_db_migrated, backup_dir=tmp_path / "backups5",
                                now=1_000_100)
    assert result.status == "refused"
    assert "in-flight" in result.reason


def test_rollback_refuses_on_dispatching_delivery(file_db_migrated, tmp_path):
    """v0.4 regression (codex-manager mm-cxm-coord seq=229): the `dispatching`
    state must be treated as in-flight by rollback.

    A delivery parked in `dispatching` is deliberately in the
    uncertain-side-effect state — adapter may have already queued the notice
    with the Codex CLI, awaiting reconciliation via receipt or same-token
    finalization. Allowing rollback to drop the table while a `dispatching`
    row exists would silently discard the ambiguity-safe contract and lose
    the ability to reconcile the user-visible side effect.
    """
    import uuid as _uuid
    with sqlite3.connect(str(file_db_migrated)) as db:
        db.execute("INSERT INTO messages (id) VALUES ('m-dispatching')")
        db.execute(
            "INSERT INTO targeted_deliveries "
            "(message_id, target_session_uuid, target_session_incarnation_id, "
            " sender_session_uuid, sender_session_incarnation_id, delivery_state, "
            " claim_lease_token, dispatch_started_at, "
            " created_at, next_attempt_at, expires_at) "
            "VALUES ('m-dispatching', ?, ?, ?, ?, 'dispatching', 'lease-xyz', "
            " 1_000_050, 1, 1, 1_000_000)",
            (str(_uuid.uuid4()), str(_uuid.uuid4()),
             str(_uuid.uuid4()), str(_uuid.uuid4())),
        )
    result = rollback_migration(file_db_migrated,
                                backup_dir=tmp_path / "backups5b",
                                now=1_000_100)
    assert result.status == "refused"
    assert "in-flight" in result.reason


def test_rollback_refuses_on_live_dispatcher_lease(file_db_migrated, tmp_path):
    with sqlite3.connect(str(file_db_migrated)) as db:
        db.execute(
            "INSERT INTO dispatcher_lease "
            "(singleton_pk, holder_token, holder_pid, holder_proc_start_ns, "
            " acquired_at, expires_at) VALUES (1, 'tok', 100, 200, 1_000_000, 1_002_000)"
        )
    result = rollback_migration(file_db_migrated, backup_dir=tmp_path / "backups6",
                                now=1_000_100)
    assert result.status == "refused"
    assert "dispatcher_lease" in result.reason


def test_rollback_refuses_on_live_capability(file_db_migrated, tmp_path):
    import uuid as _uuid
    sid = str(_uuid.uuid4())
    inc = str(_uuid.uuid4())
    with sqlite3.connect(str(file_db_migrated)) as db:
        db.execute(
            "INSERT INTO session_registrations "
            "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
            " project_root, role, state, wake_capability, registration_fence, "
            " wake_locator, started_at, heartbeat_at, expires_at, "
            " holder_pid, process_started_at) "
            "VALUES (?, ?, 'a', 'codex', 'p', '/tmp/p', 'manager', 'idle', "
            " 'codex_queue', 'f', 'l', 1, 2, 1_002_000, 1, 1)",
            (sid, inc),
        )
        db.execute(
            "INSERT INTO session_capabilities "
            "(verifier_sha256, sender_session_uuid, sender_session_incarnation_id, "
            " registration_fence, issued_at, expires_at) "
            "VALUES (?, ?, ?, 'f', 1, 1_002_000)",
            (b"\x00" * 32, sid, inc),
        )
    result = rollback_migration(file_db_migrated, backup_dir=tmp_path / "backups7",
                                now=1_000_100)
    assert result.status == "refused"
    assert "capabilities" in result.reason


def test_rollback_removes_all_managed_state(file_db_migrated, tmp_path):
    result = rollback_migration(file_db_migrated, backup_dir=tmp_path / "backups8",
                                now=1_000_100)
    assert result.status == "rolled_back"
    actual = _collect_managed(file_db_migrated)
    assert actual["tables"] == set()
    assert actual["indexes"] == set()
    assert actual["triggers"] == set()


def test_rollback_preserves_bridge_messages(file_db_migrated, tmp_path):
    with sqlite3.connect(str(file_db_migrated)) as db:
        db.execute("INSERT INTO messages (id) VALUES ('untouched')")
    result = rollback_migration(file_db_migrated, backup_dir=tmp_path / "backups9",
                                now=1_000_100)
    assert result.status == "rolled_back"
    with sqlite3.connect(str(file_db_migrated)) as db:
        row = db.execute("SELECT id FROM messages WHERE id='untouched'").fetchone()
        assert row is not None


def test_apply_rollback_apply_cycle(file_db, tmp_path):
    r1 = apply_migration(file_db, backup_dir=tmp_path / "b1", now=1_000_000)
    assert r1.status == "applied"
    r2 = rollback_migration(file_db, backup_dir=tmp_path / "b2", now=1_000_100)
    assert r2.status == "rolled_back"
    r3 = apply_migration(file_db, backup_dir=tmp_path / "b3", now=1_000_200)
    assert r3.status == "applied"
