"""WAL-safe backup: capture + independent integrity + schema diff + own SHA-256.

See spec §11.1. Does NOT compare source and backup byte-for-byte because under
WAL + concurrent writers the SHAs will never match.
"""

import datetime
import hashlib
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


class BackupVerificationFailure(Exception):
    """A captured backup failed verification. ``path`` names the unverified
    file left on disk; callers must never report it as a verified backup."""

    def __init__(self, message: str, *, path: "Path | None" = None):
        super().__init__(message)
        self.path = path


@dataclass(frozen=True)
class BackupResult:
    path: Path
    backup_sha256: bytes
    backup_byte_count: int
    backup_completed_at: int
    integrity_check_ok: bool
    foreign_key_check_ok: bool
    schema_diff_matches_expected: bool


def _utc_iso_no_colons(now: int) -> str:
    dt = datetime.datetime.fromtimestamp(now, tz=datetime.timezone.utc)
    return dt.strftime("%Y-%m-%dT%H%M%SZ")


def _file_sha256_and_bytes(path: Path) -> tuple[bytes, int]:
    h = hashlib.sha256()
    byte_count = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            h.update(chunk)
            byte_count += len(chunk)
    return h.digest(), byte_count


def _collect_managed_schema(backup_db_path: Path, *, namespace_names: set[str]) -> set[tuple[str, str]]:
    """Return {(type, name)} for managed artifacts in the backup, filtered to the
    awareness-managed namespace. Bridge-owned tables (messages, consumer_cursors,
    etc.) are deliberately excluded from the diff — they are not managed by this
    package."""
    with sqlite3.connect(str(backup_db_path)) as db:
        rows = db.execute(
            "SELECT type, name FROM sqlite_master "
            "WHERE type IN ('table', 'index', 'trigger') "
            "  AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        return {(t, n) for t, n in rows if n in namespace_names}


def create_backup(
    source_db_path: Path,
    *,
    backup_dir: Path,
    expected_managed_artifacts: Iterable[tuple[str, str]],
    managed_namespace_names: Iterable[str],
    now: int,
    suffix: str = "",
) -> BackupResult:
    """Capture a WAL-safe snapshot and verify it independently.

    expected_managed_artifacts is a set of (type, name) pairs the caller
    expects to find in the backup. Used by apply (pre-migration shape = none)
    and rollback (pre-rollback shape = all managed artifacts present).

    The capture always runs on its own read connection (SQLite online backup
    API), so a caller may hold a write transaction on another connection;
    the snapshot is the last committed state. ``suffix`` lets per-hop callers
    name their backups distinctly (e.g. ``.from-v3``).
    """
    source_db_path = Path(source_db_path)
    backup_dir = Path(backup_dir)
    if not source_db_path.exists():
        raise FileNotFoundError(f"source DB does not exist: {source_db_path}")

    backup_dir.mkdir(parents=True, exist_ok=True, mode=0o755)
    backup_name = f"bridge.db.{_utc_iso_no_colons(now)}{suffix}.sqlite"
    backup_path = backup_dir / backup_name
    if backup_path.exists():
        raise FileExistsError(f"backup already exists at timestamp: {backup_path}")

    # Capture via SQLite online backup API.
    src = sqlite3.connect(str(source_db_path))
    try:
        dst = sqlite3.connect(str(backup_path))
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()

    # fsync.
    fd = os.open(str(backup_path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

    backup_completed_at = now

    # Verify backup independently.
    with sqlite3.connect(str(backup_path)) as verify_db:
        integrity_rows = verify_db.execute("PRAGMA integrity_check").fetchall()
        integrity_ok = len(integrity_rows) == 1 and integrity_rows[0][0] == "ok"

        fk_rows = verify_db.execute("PRAGMA foreign_key_check").fetchall()
        fk_ok = len(fk_rows) == 0

    namespace = set(managed_namespace_names)
    actual_managed = _collect_managed_schema(backup_path, namespace_names=namespace)
    expected_set = set(tuple(x) for x in expected_managed_artifacts)
    schema_ok = actual_managed == expected_set

    backup_sha, byte_count = _file_sha256_and_bytes(backup_path)

    if not integrity_ok:
        raise BackupVerificationFailure(
            f"integrity_check failed on backup: {integrity_rows!r}", path=backup_path
        )
    if not fk_ok:
        raise BackupVerificationFailure(
            f"foreign_key_check failed on backup: {fk_rows!r}", path=backup_path
        )
    if not schema_ok:
        raise BackupVerificationFailure(
            f"schema diff on backup: expected={sorted(expected_set)}, actual={sorted(actual_managed)}",
            path=backup_path,
        )

    return BackupResult(
        path=backup_path,
        backup_sha256=backup_sha,
        backup_byte_count=byte_count,
        backup_completed_at=backup_completed_at,
        integrity_check_ok=integrity_ok,
        foreign_key_check_ok=fk_ok,
        schema_diff_matches_expected=schema_ok,
    )
