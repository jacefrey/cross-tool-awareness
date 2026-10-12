"""Backup tests (§12.7)."""

import sqlite3
import time
from pathlib import Path

import pytest

from cross_tool_awareness import (
    BackupVerificationFailure,
    MANAGED_INDEXES,
    MANAGED_TABLES,
    MANAGED_TRIGGERS,
    create_backup,
)
from cross_tool_awareness.migrations import _expected_managed_set, _managed_namespace_names


def _ns():
    return _managed_namespace_names()


def test_create_backup_passes_integrity_check(file_db_migrated, tmp_path):
    backup_dir = tmp_path / "backups"
    result = create_backup(
        file_db_migrated, backup_dir=backup_dir,
        expected_managed_artifacts=_expected_managed_set(),
        managed_namespace_names=_ns(),
        now=1_000_100,
    )
    assert result.integrity_check_ok is True
    assert result.foreign_key_check_ok is True
    assert result.schema_diff_matches_expected is True


def test_create_backup_records_independent_sha(file_db_migrated, tmp_path):
    backup_dir = tmp_path / "backups"
    result = create_backup(
        file_db_migrated, backup_dir=backup_dir,
        expected_managed_artifacts=_expected_managed_set(),
        managed_namespace_names=_ns(),
        now=1_000_200,
    )
    assert len(result.backup_sha256) == 32
    assert result.backup_byte_count > 0
    assert result.path.exists()


def test_backup_sha_does_not_assert_equality_with_source(file_db_migrated, tmp_path):
    backup_dir = tmp_path / "backups"
    result = create_backup(
        file_db_migrated, backup_dir=backup_dir,
        expected_managed_artifacts=_expected_managed_set(),
        managed_namespace_names=_ns(),
        now=1_000_300,
    )
    assert result.integrity_check_ok is True


def test_backup_schema_diff_raises_on_drift(file_db, tmp_path):
    """Create a backup of an empty-schema DB; expect schema mismatch when the
    caller declares a non-empty expected set within the managed namespace."""
    backup_dir = tmp_path / "backups"
    expected_bogus = {("table", "session_registrations")}
    with pytest.raises(BackupVerificationFailure):
        create_backup(
            file_db, backup_dir=backup_dir,
            expected_managed_artifacts=expected_bogus,
            managed_namespace_names=_ns(),
            now=1_000_400,
        )


def test_backup_refuses_duplicate_timestamp(file_db_migrated, tmp_path):
    backup_dir = tmp_path / "backups"
    create_backup(
        file_db_migrated, backup_dir=backup_dir,
        expected_managed_artifacts=_expected_managed_set(),
        managed_namespace_names=_ns(),
        now=1_000_500,
    )
    with pytest.raises(FileExistsError):
        create_backup(
            file_db_migrated, backup_dir=backup_dir,
            expected_managed_artifacts=_expected_managed_set(),
            managed_namespace_names=_ns(),
            now=1_000_500,
        )


def test_backup_under_concurrent_writes_still_verifies(file_db_migrated, tmp_path):
    """Simulate a concurrent writer by inserting rows between backup start and
    verification. The .backup() API captures a consistent snapshot; verification
    uses PRAGMA integrity_check on the backup, not source==backup equality."""
    import threading

    backup_dir = tmp_path / "backups"
    stop_flag = threading.Event()

    def writer():
        conn = sqlite3.connect(str(file_db_migrated))
        try:
            i = 0
            while not stop_flag.is_set():
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO messages (id) VALUES (?)",
                        (f"concurrent-{i}",),
                    )
                    conn.commit()
                except sqlite3.OperationalError:
                    pass
                i += 1
                time.sleep(0.001)
        finally:
            conn.close()

    t = threading.Thread(target=writer)
    t.start()
    try:
        time.sleep(0.01)
        result = create_backup(
            file_db_migrated, backup_dir=backup_dir,
            expected_managed_artifacts=_expected_managed_set(),
            managed_namespace_names=_ns(),
            now=1_000_600,
        )
    finally:
        stop_flag.set()
        t.join(timeout=5)

    assert result.integrity_check_ok is True
