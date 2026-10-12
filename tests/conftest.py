"""Shared pytest fixtures.

Produces a fresh in-memory SQLite with the awareness schema applied for each test.
Includes a stub `messages` table so the FK on targeted_deliveries resolves.
"""

import os
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

# Ensure src/ is importable when running tests from the package root
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
    """Fresh in-memory DB with awareness schema + stub messages table."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.executescript(_schema_sql())
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture
def file_db(tmp_path) -> Path:
    """File-backed empty bridge DB (for migration + backup tests)."""
    path = tmp_path / "bridge.db"
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.close()
    return path


@pytest.fixture
def file_db_migrated(file_db, tmp_path) -> Path:
    """File-backed bridge DB with awareness schema applied."""
    from cross_tool_awareness import apply as apply_migration
    backup_dir = tmp_path / "backups"
    result = apply_migration(file_db, backup_dir=backup_dir, now=1_000_000)
    assert result.status == "applied", f"migration failed: {result.reason}"
    return file_db


def _u() -> str:
    return str(uuid.uuid4())


def make_presence(**overrides) -> PresenceRecord:
    defaults = dict(
        session_uuid=_u(),
        session_incarnation_id=_u(),
        alias="test-alias-" + _u()[:8],
        tool="codex",
        project_slug="test-project",
        project_root="/tmp/test-project",
        role="manager",
        state="idle",
        wake_capability="codex_queue",
        registration_fence="fence-" + _u()[:8],
        wake_locator="locator-" + _u()[:8],
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
    """Helper to insert and return a new message_id."""
    def _msg():
        mid = _u()
        insert_message(db, mid)
        return mid
    return _msg


@pytest.fixture
def new_uuid():
    return _u
