"""R9 (I08, I09; plan review P2/P3, re-review B): exact source schema validated
before backup and again under the write lock; namespace extras rejected; the
backup of each hop is taken under that hop's lock via a separate read
connection; destination schema/FK/preservation validated BEFORE commit; every
ordinary failure returns a HopOutcome carrying the current hop's backup and
the actual schema version; only KeyboardInterrupt/SystemExit propagate."""
from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

import cross_tool_awareness.migrations as m
from cross_tool_awareness import apply as apply_migration
from cross_tool_awareness.backup import BackupVerificationFailure
from tests.test_migration_v3_to_v4 import _STUB_MESSAGES_DDL, _insert_delivery_v3, _seed_v3

PKG = Path(m.__file__).parent
FIX = Path(__file__).parent / "fixtures"


def _seed_v2(tmp_path):
    p = tmp_path / "bridge.db"
    with sqlite3.connect(p) as c:
        c.executescript(_STUB_MESSAGES_DDL)
        c.executescript((FIX / "v0_3_1_schema.sql").read_text())
        c.execute("INSERT INTO awareness_schema_version VALUES (2, 1, '0.2.0')")
    return p


def _v(p):
    with sqlite3.connect(p) as c:
        return c.execute("SELECT MAX(version) FROM awareness_schema_version").fetchone()[0]


def _cols(p, table):
    with sqlite3.connect(p) as c:
        return {r[1] for r in c.execute(f"PRAGMA table_info({table})")}


def _apply(p, tmp_path):
    return apply_migration(p, backup_dir=tmp_path / "b", now=100)


# --- historical canonical schemas -------------------------------------------


@pytest.mark.parametrize("pkg_name,fixture", [("schema_v2.sql", "v0_3_1_schema.sql"),
                                              ("schema_v3.sql", "v0_4_0_schema.sql")])
def test_shipped_historical_schema_is_byte_identical_to_fixture(pkg_name, fixture):
    assert (PKG / pkg_name).read_bytes() == (FIX / fixture).read_bytes()


def test_canonical_v3_matches_rebuild_constants():
    v3 = m._canonical_sql_by_name_for(3)
    assert v3["targeted_deliveries"] == m._normalize_sql(m._V3_REBUILT_TARGETED_DELIVERIES)
    assert v3["session_registrations"] == m._normalize_sql(m._V3_REBUILT_SESSION_REGISTRATIONS)
    assert v3["tr_receipt_marks_observed"] == m._normalize_sql(m._V3_TRIGGER_RECEIPT_MARKS_OBSERVED)
    assert "dispatch_ownership" not in v3


def test_single_parser_serves_all_versions():
    assert m._canonical_sql_by_name_for(4) == m._canonical_sql_by_name_from_text(
        (PKG / "schema.sql").read_text())
    assert m._canonical_sql_by_name_for(2) == m._canonical_sql_by_name_from_text(
        (PKG / "schema_v2.sql").read_text())


# --- source validation before backup ----------------------------------------


def test_extra_source_column_refused_before_backup(tmp_path):
    p = _seed_v3(tmp_path)
    with sqlite3.connect(p) as c:
        c.execute("ALTER TABLE targeted_deliveries ADD COLUMN external_metadata TEXT")
        c.execute("INSERT INTO messages VALUES ('m', '', 0)")
    r = _apply(p, tmp_path)
    assert r.status == "refused" and "targeted_deliveries" in r.reason and r.backup is None
    assert _v(p) == 3 and "external_metadata" in _cols(p, "targeted_deliveries")
    assert not (tmp_path / "b").exists()


def test_drift_in_untouched_table_refused_before_backup(tmp_path):
    p = _seed_v3(tmp_path)
    with sqlite3.connect(p) as c:
        c.execute("ALTER TABLE session_registrations ADD COLUMN external_metadata TEXT")
    r = _apply(p, tmp_path)
    assert r.status == "refused" and "session_registrations" in r.reason and r.backup is None
    assert _v(p) == 3 and not (tmp_path / "b").exists()


def test_extra_artifact_introduced_before_lock_is_refused_under_lock(tmp_path, monkeypatch):
    p = _seed_v3(tmp_path)
    orig = m._validate_schema_exact
    calls = {"n": 0}

    def inject_after_outer_check(db, *, version):
        calls["n"] += 1
        result = orig(db, version=version)
        if calls["n"] == 1:                                  # outer pre-backup check passed
            with sqlite3.connect(p) as c:
                c.execute("CREATE TABLE awareness_unexpected (payload TEXT)")
        return result

    monkeypatch.setattr(m, "_validate_schema_exact", inject_after_outer_check)
    r = _apply(p, tmp_path)
    assert r.status == "refused" and "awareness_unexpected" in r.reason
    assert _v(p) == 3 and r.backup is None and not (tmp_path / "b").exists()


# --- lock discipline ---------------------------------------------------------


def test_writer_cannot_slip_between_check_and_mutation(tmp_path, monkeypatch):
    p = _seed_v3(tmp_path)
    _insert_delivery_v3(p, "pending", msg_id=str(uuid.uuid4()))
    orig = m.create_backup
    blocked = []

    def racing_backup(*a, **k):
        r = orig(*a, **k)
        try:
            with sqlite3.connect(p, timeout=0.2) as c:
                c.execute("UPDATE targeted_deliveries SET delivery_state='blocked'")
        except sqlite3.OperationalError as e:
            blocked.append(str(e))
        return r

    monkeypatch.setattr(m, "create_backup", racing_backup)
    r = _apply(p, tmp_path)
    assert r.status == "applied", r
    assert blocked and "locked" in blocked[0]
    with sqlite3.connect(p) as c:
        assert c.execute("SELECT delivery_state FROM targeted_deliveries").fetchone()[0] == "pending"


def test_quiescence_violation_before_lock_refused_under_lock(tmp_path, monkeypatch):
    p = _seed_v3(tmp_path)
    _insert_delivery_v3(p, "pending", msg_id=str(uuid.uuid4()))
    orig = m._v4_quiescence_refusal_reason
    calls = {"n": 0}

    def flip_after_first(db):
        calls["n"] += 1
        result = orig(db)
        if calls["n"] == 1:
            with sqlite3.connect(p) as c:
                c.execute("UPDATE targeted_deliveries SET delivery_state='blocked'")
        return result

    monkeypatch.setattr(m, "_v4_quiescence_refusal_reason", flip_after_first)
    r = _apply(p, tmp_path)
    assert r.status == "refused" and "blocked=1" in r.reason and _v(p) == 3
    assert r.backup is None and not (tmp_path / "b").exists()


@pytest.mark.parametrize("journal", ["DELETE", "WAL"])
def test_guarded_hop_backup_under_lock(tmp_path, journal):
    """Review Focus 1."""
    p = _seed_v3(tmp_path)
    with sqlite3.connect(p) as c:
        c.execute(f"PRAGMA journal_mode={journal}")
    r = _apply(p, tmp_path)
    assert r.status == "applied" and r.backup is not None and _v(p) == 4
    assert r.backup.path.name.endswith(".from-v3.sqlite")


# --- per-hop backups and outcome reporting ---------------------------------


def test_per_hop_backups_suffixed(tmp_path):
    p = _seed_v2(tmp_path)
    r = _apply(p, tmp_path)
    assert r.status == "applied" and _v(p) == 4
    names = sorted(x.name for x in (tmp_path / "b").iterdir())
    assert [n.split(".")[-2] for n in names] == ["from-v2", "from-v3"]
    assert r.backup.path.name.endswith(".from-v3.sqlite")


def test_chain_hop2_prebackup_refusal_reports_hop1_backup(tmp_path, monkeypatch):
    """Review Focus 4a: refusal in hop 2's under-lock recheck, before hop 2 backs up."""
    p = _seed_v2(tmp_path)
    orig = m._v4_quiescence_refusal_reason
    calls = {"n": 0}

    def refuse_in_hop2(db):
        calls["n"] += 1
        if calls["n"] == 3:                                  # outer, hop1 recheck, hop2 recheck
            return "fixture: hop 2 refused under lock"
        return orig(db)

    monkeypatch.setattr(m, "_v4_quiescence_refusal_reason", refuse_in_hop2)
    r = _apply(p, tmp_path)
    assert r.status == "refused" and r.version == 3 and "hop 2 refused" in r.reason
    assert r.backup is not None and r.backup.path.name.endswith(".from-v2.sqlite")
    assert _v(p) == 3 and len(list((tmp_path / "b").iterdir())) == 1


def test_chain_hop2_postbackup_failure_reports_hop2_backup(tmp_path, monkeypatch):
    """Review Focus 4b."""
    p = _seed_v2(tmp_path)

    def boom(conn, *, now):
        raise sqlite3.OperationalError("injected hop-2 DDL failure")

    monkeypatch.setattr(m, "_v3_to_v4_ddl", boom)
    r = _apply(p, tmp_path)
    assert r.status == "failed" and r.version == 3 and "injected hop-2" in r.reason
    assert r.backup is not None and r.backup.path.name.endswith(".from-v3.sqlite")
    assert _v(p) == 3


def test_direct_hop_ddl_failure_reports_current_backup(tmp_path, monkeypatch):
    p = _seed_v3(tmp_path)

    def boom(conn, *, now):
        raise sqlite3.OperationalError("injected DDL failure")

    monkeypatch.setattr(m, "_v3_to_v4_ddl", boom)
    r = _apply(p, tmp_path)
    assert r.status == "failed" and r.version == 3 and "injected DDL" in r.reason
    assert r.backup is not None and r.backup.path.name.endswith(".from-v3.sqlite")
    assert _v(p) == 3 and "claimant_session_uuid" not in _cols(p, "targeted_deliveries")


class _CommitFails(sqlite3.Connection):
    def execute(self, sql, parameters=()):
        if sql.lstrip().startswith("COMMIT"):
            raise sqlite3.OperationalError("fixture: COMMIT failed")
        return super().execute(sql, parameters)


def test_hop_commit_failure_reports_current_backup_and_source_version(tmp_path, monkeypatch):
    p = _seed_v3(tmp_path)
    real = sqlite3.connect
    monkeypatch.setattr(m.sqlite3, "connect",
                        lambda *a, **k: real(*a, factory=_CommitFails, **k))
    r = _apply(p, tmp_path)
    assert r.status == "failed" and r.version == 3 and "COMMIT failed" in r.reason
    assert r.backup is not None and r.backup.path.name.endswith(".from-v3.sqlite")
    assert _v(p) == 3 and "claimant_session_uuid" not in _cols(p, "targeted_deliveries")


class _FkRestoreFails(sqlite3.Connection):
    """Refuse the FK restoration only on the hop's own connection (the one
    that ran BEGIN IMMEDIATE), after its COMMIT."""
    _hop_committed = False

    def execute(self, sql, parameters=()):
        s = sql.strip()
        if s == "COMMIT":
            self._hop_committed = True
        if s == "PRAGMA foreign_keys = ON" and self._hop_committed:
            raise sqlite3.OperationalError("fixture: foreign_keys restore refused")
        return super().execute(sql, parameters)


def test_post_commit_fk_restore_failure_reports_destination_version_and_backup(tmp_path, monkeypatch):
    p = _seed_v3(tmp_path)
    real = sqlite3.connect
    monkeypatch.setattr(m.sqlite3, "connect",
                        lambda *a, **k: real(*a, factory=_FkRestoreFails, **k))
    r = _apply(p, tmp_path)
    assert r.status == "failed" and r.version == 4
    assert "committed" in r.reason and "restore" in r.reason
    assert r.backup is not None and r.backup.path.name.endswith(".from-v3.sqlite")
    assert _v(p) == 4 and "claimant_session_uuid" in _cols(p, "targeted_deliveries")


def test_backup_verification_failure_reports_unverified_path(tmp_path, monkeypatch):
    p = _seed_v3(tmp_path)
    orig = m.create_backup

    def failing(*a, **k):
        r = orig(*a, **k)
        raise BackupVerificationFailure("fixture: verification failed", path=r.path)

    monkeypatch.setattr(m, "create_backup", failing)
    r = _apply(p, tmp_path)
    assert r.status == "failed" and r.version == 3 and r.backup is None
    assert "verification failed" in r.reason and ".from-v3.sqlite" in r.reason
    assert _v(p) == 3


# --- destination validation before commit -----------------------------------


def test_destination_mismatch_rolls_back_before_commit(tmp_path, monkeypatch):
    p = _seed_v3(tmp_path)
    orig = m._v3_to_v4_ddl

    def drifting(conn, *, now):
        orig(conn, now=now)
        conn.execute("ALTER TABLE queue_attempts ADD COLUMN extra TEXT")

    monkeypatch.setattr(m, "_v3_to_v4_ddl", drifting)
    r = _apply(p, tmp_path)
    assert r.status == "failed" and r.version == 3 and "queue_attempts" in r.reason
    assert r.backup is not None
    assert _v(p) == 3 and "extra" not in _cols(p, "queue_attempts")


def test_preservation_count_mismatch_rolls_back(tmp_path, monkeypatch):
    p = _seed_v3(tmp_path)
    _insert_delivery_v3(p, "pending", msg_id=str(uuid.uuid4()))
    orig = m._v3_to_v4_ddl

    def losing(conn, *, now):
        orig(conn, now=now)
        conn.execute("DELETE FROM targeted_deliveries")

    monkeypatch.setattr(m, "_v3_to_v4_ddl", losing)
    r = _apply(p, tmp_path)
    assert r.status == "failed" and r.version == 3 and "targeted_deliveries" in r.reason
    with sqlite3.connect(p) as c:
        assert c.execute("SELECT COUNT(*) FROM targeted_deliveries").fetchone()[0] == 1


def test_interrupt_during_hop_propagates_and_leaves_source(tmp_path, monkeypatch):
    p = _seed_v3(tmp_path)

    def interrupt(conn, *, now):
        raise KeyboardInterrupt("fixture")

    monkeypatch.setattr(m, "_v3_to_v4_ddl", interrupt)
    with pytest.raises(KeyboardInterrupt):
        _apply(p, tmp_path)
    assert _v(p) == 3 and "claimant_session_uuid" not in _cols(p, "targeted_deliveries")
    with sqlite3.connect(p, isolation_level=None, timeout=0.5) as c:
        c.execute("BEGIN IMMEDIATE")                         # no lock left behind
        c.execute("ROLLBACK")
