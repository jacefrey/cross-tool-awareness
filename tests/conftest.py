"""Shared pytest fixtures.

v0.2.0: dataclasses now store UUID objects (normalized at construction).
Fixtures create UUIDs via uuid.uuid4().
"""

from __future__ import annotations

import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_PKG_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PKG_ROOT / "src"))

from cross_tool_awareness.contracts import PresenceRecord  # noqa: E402


_STUB_MESSAGES_DDL = """
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL DEFAULT 0
);
"""


def _schema_sql() -> str:
    return (_PKG_ROOT / "src" / "cross_tool_awareness" / "schema.sql").read_text()


@pytest.fixture
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.executescript(_schema_sql())
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture
def file_db(tmp_path) -> Path:
    path = tmp_path / "bridge.db"
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.close()
    return path


@pytest.fixture
def file_db_migrated(file_db, tmp_path) -> Path:
    from cross_tool_awareness import apply as apply_migration
    backup_dir = tmp_path / "backups"
    result = apply_migration(file_db, backup_dir=backup_dir, now=1_000_000)
    assert result.status == "applied", f"migration failed: {result.reason}"
    return file_db


def make_presence(**overrides) -> PresenceRecord:
    defaults = dict(
        session_uuid=uuid.uuid4(),
        session_incarnation_id=uuid.uuid4(),
        alias="test-alias-" + uuid.uuid4().hex[:8],
        tool="codex",
        project_slug="test-project",
        project_root="/tmp/test-project",
        role="manager",
        state="idle",
        wake_capability="codex_queue",
        registration_fence="fence-" + uuid.uuid4().hex[:8],
        wake_locator="locator-" + uuid.uuid4().hex[:8],
        started_at=1_000_000,
        heartbeat_at=1_000_010,
        expires_at=1_000_000 + 86400,
    )
    defaults.update(overrides)
    return PresenceRecord(**defaults)


@pytest.fixture
def make_presence_fn():
    return make_presence


def insert_message(db: sqlite3.Connection, message_id: str) -> None:
    db.execute("INSERT OR IGNORE INTO messages (id) VALUES (?)", (message_id,))


@pytest.fixture
def msg(db):
    def _msg():
        mid = str(uuid.uuid4())
        insert_message(db, mid)
        return mid
    return _msg


@pytest.fixture
def new_uuid():
    def _u():
        return str(uuid.uuid4())
    return _u


@pytest.fixture
def new_uuid_obj():
    def _u():
        return uuid.uuid4()
    return _u
