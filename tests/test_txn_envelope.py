"""R6 / plan ruling R-E: the package transaction envelope protects body AND commit,
never commits unresolved work during cleanup, invalidates a connection whose
ROLLBACK failed, and refuses caller-owned transactions. Persistence is always
checked through a FRESH connection on the same file."""
from __future__ import annotations

import sqlite3

import pytest

from cross_tool_awareness._txn import exclusive, immediate


class _Inject(sqlite3.Connection):
    fail_on: str | None = None
    exc: BaseException | None = None

    def execute(self, sql, parameters=()):
        if self.fail_on and sql.lstrip().startswith(self.fail_on):
            self.fail_on = None
            raise self.exc
        return super().execute(sql, parameters)


ISOLATION = [None, ""]
HELPERS = [immediate, exclusive]


def _file(tmp_path):
    p = tmp_path / "t.db"
    c = sqlite3.connect(str(p))
    c.execute("CREATE TABLE t (n INTEGER)")
    c.commit()
    c.close()
    return p


def _open(p, iso, **attrs):
    db = sqlite3.connect(str(p), isolation_level=iso, factory=_Inject, timeout=0.5)
    for k, v in attrs.items():
        setattr(db, k, v)
    return db


def _fresh_count(p):
    c = sqlite3.connect(str(p), timeout=0.5)
    try:
        return c.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    finally:
        c.close()


def _fresh_can_write(p):
    c = sqlite3.connect(str(p), isolation_level=None, timeout=0.5)
    try:
        c.execute("BEGIN IMMEDIATE")
        c.execute("ROLLBACK")
        return True
    finally:
        c.close()


@pytest.mark.parametrize("iso", ISOLATION)
@pytest.mark.parametrize("helper", HELPERS, ids=["immediate", "exclusive"])
@pytest.mark.parametrize("exc", [KeyboardInterrupt("fixture"), SystemExit(3),
                                 sqlite3.OperationalError("fixture")],
                         ids=["KeyboardInterrupt", "SystemExit", "OperationalError"])
def test_body_exception_persists_nothing(tmp_path, iso, helper, exc):
    p = _file(tmp_path)
    db = _open(p, iso)
    with pytest.raises(type(exc)):
        with helper(db):
            db.execute("INSERT INTO t VALUES (1)")
            raise exc
    assert db.in_transaction is False
    assert _fresh_count(p) == 0
    assert _fresh_can_write(p)


@pytest.mark.parametrize("iso", ISOLATION)
@pytest.mark.parametrize("helper", HELPERS, ids=["immediate", "exclusive"])
def test_commit_failure_persists_nothing(tmp_path, iso, helper):
    p = _file(tmp_path)
    db = _open(p, iso, fail_on="COMMIT", exc=sqlite3.OperationalError("fixture: COMMIT failed"))
    with pytest.raises(sqlite3.OperationalError, match="COMMIT failed"):
        with helper(db):
            db.execute("INSERT INTO t VALUES (1)")
    assert db.in_transaction is False
    assert _fresh_count(p) == 0
    assert _fresh_can_write(p)


@pytest.mark.parametrize("iso", ISOLATION)
@pytest.mark.parametrize("helper", HELPERS, ids=["immediate", "exclusive"])
def test_failed_rollback_preserves_primary_and_invalidates_connection(tmp_path, iso, helper):
    p = _file(tmp_path)
    db = _open(p, iso, fail_on="ROLLBACK", exc=sqlite3.OperationalError("fixture: ROLLBACK failed"))
    with pytest.raises(KeyboardInterrupt, match="orig"):
        with helper(db):
            db.execute("INSERT INTO t VALUES (1)")
            raise KeyboardInterrupt("orig")
    with pytest.raises(sqlite3.ProgrammingError):
        db.execute("SELECT 1")
    assert _fresh_count(p) == 0
    assert _fresh_can_write(p)


@pytest.mark.parametrize("iso", ISOLATION)
@pytest.mark.parametrize("helper", HELPERS, ids=["immediate", "exclusive"])
def test_sqlite_auto_rollback_keeps_connection_usable(tmp_path, iso, helper):
    """SQLITE_FULL (and INTERRUPT/IOERR) roll the transaction back inside SQLite
    itself; the envelope must not mistake its own now-pointless ROLLBACK error
    for an unresolved transaction and close the consumer's connection."""
    p = _file(tmp_path)
    db = _open(p, iso)
    pages = db.execute("PRAGMA page_count").fetchone()[0]
    db.execute(f"PRAGMA max_page_count = {pages}")
    with pytest.raises(sqlite3.OperationalError, match="full"):
        with helper(db):
            for _ in range(64):
                db.execute("INSERT INTO t VALUES (zeroblob(4096))")
    assert db.in_transaction is False
    assert db.execute("SELECT 1").fetchone()[0] == 1          # still open and usable
    assert _fresh_count(p) == 0
    assert _fresh_can_write(p)


@pytest.mark.parametrize("iso", ISOLATION)
def test_caller_transaction_refused_and_untouched(tmp_path, iso):
    p = _file(tmp_path)
    db = _open(p, iso)
    db.execute("BEGIN")
    db.execute("INSERT INTO t VALUES (7)")
    with pytest.raises(sqlite3.OperationalError):
        with immediate(db):
            pass
    assert db.in_transaction is True
    assert db.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 1
    assert _fresh_count(p) == 0
    db.rollback()
    assert _fresh_count(p) == 0


@pytest.mark.parametrize("iso", ISOLATION)
def test_exclusive_still_commits_caller_work_on_entry(tmp_path, iso):
    p = _file(tmp_path)
    db = _open(p, iso)
    db.execute("BEGIN")
    db.execute("INSERT INTO t VALUES (7)")
    with exclusive(db):
        db.execute("INSERT INTO t VALUES (8)")
    assert db.in_transaction is False
    assert _fresh_count(p) == 2
