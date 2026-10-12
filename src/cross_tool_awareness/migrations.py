"""Apply + rollback with exact schema+version validation and backup discipline.

See spec §4.
"""

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .backup import BackupResult, create_backup


EXPECTED_SCHEMA_VERSION = 1
PACKAGE_VERSION = "0.1.0"


MANAGED_TABLES = frozenset({
    "session_registrations",
    "session_capabilities",
    "targeted_deliveries",
    "queue_attempts",
    "disposition_receipts",
    "dispatcher_lease",
    "awareness_schema_version",
})

MANAGED_INDEXES = frozenset({
    "idx_alias_live",
    "idx_tool_state",
    "idx_delivery_eligible",
})

MANAGED_TRIGGERS = frozenset({
    "tr_capability_verifier_length",
    "tr_receipt_requires_live_registration",
    "tr_receipt_requires_eligible_delivery_state",
    "tr_receipt_marks_observed",
})


def _expected_managed_set() -> set[tuple[str, str]]:
    out = set()
    for name in MANAGED_TABLES:
        out.add(("table", name))
    for name in MANAGED_INDEXES:
        out.add(("index", name))
    for name in MANAGED_TRIGGERS:
        out.add(("trigger", name))
    return out


def _managed_namespace_names() -> set[str]:
    """Union of all awareness-managed artifact names (across tables/indexes/triggers)."""
    return set(MANAGED_TABLES) | set(MANAGED_INDEXES) | set(MANAGED_TRIGGERS)


@dataclass
class MigrationResult:
    status: str                        # 'applied' | 'already_at_version' | 'partial_or_unknown_managed_state' | 'unknown_managed_state' | 'failed'
    version: Optional[int] = None
    backup: Optional[BackupResult] = None
    reason: str = ""
    tables_created: tuple = ()
    indexes_created: tuple = ()
    triggers_created: tuple = ()


@dataclass
class RollbackResult:
    status: str                        # 'rolled_back' | 'refused' | 'failed'
    reason: str = ""
    backup: Optional[BackupResult] = None
    preserved_message_ids: tuple = ()


def _collect_managed_state(db: sqlite3.Connection) -> dict:
    """Return the set of managed-namespace artifacts actually present."""
    rows = db.execute(
        "SELECT type, name FROM sqlite_master "
        "WHERE type IN ('table', 'index', 'trigger') "
        "  AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    tables = {n for t, n in rows if t == "table" and n in MANAGED_TABLES}
    indexes = {n for t, n in rows if t == "index" and n in MANAGED_INDEXES}
    triggers = {n for t, n in rows if t == "trigger" and n in MANAGED_TRIGGERS}
    # Managed-namespace extras: artifacts matching our naming convention we didn't declare.
    all_names = {n for t, n in rows}
    extras = set()
    for t, n in rows:
        if t == "table" and n not in MANAGED_TABLES and n in {"session_registrations", "session_capabilities",
                                                               "targeted_deliveries", "queue_attempts",
                                                               "disposition_receipts", "dispatcher_lease",
                                                               "awareness_schema_version"}:
            extras.add((t, n))
    return {
        "tables_present": tables,
        "indexes_present": indexes,
        "triggers_present": triggers,
        "any_managed_present": bool(tables | indexes | triggers),
        "all_tables_present": tables == MANAGED_TABLES,
        "all_indexes_present": indexes == MANAGED_INDEXES,
        "all_triggers_present": triggers == MANAGED_TRIGGERS,
    }


def _schema_sql_path() -> Path:
    return Path(__file__).parent / "schema.sql"


def _execute_ddl(db: sqlite3.Connection, sql: str) -> None:
    db.executescript(sql)


def apply(
    db_path: Path | str,
    *,
    backup_dir: Path | str,
    now: int,
    dry_run: bool = False,
) -> MigrationResult:
    """Apply the awareness schema to an existing bridge.db."""
    db_path = Path(db_path)
    backup_dir = Path(backup_dir)
    if not db_path.exists():
        return MigrationResult(status="failed", reason=f"db does not exist: {db_path}")

    db = sqlite3.connect(str(db_path))
    try:
        # PRAGMA foreign_keys = ON and verify.
        db.execute("PRAGMA foreign_keys = ON")
        fk_on = db.execute("PRAGMA foreign_keys").fetchone()
        if fk_on is None or fk_on[0] != 1:
            return MigrationResult(status="failed",
                                   reason="PRAGMA foreign_keys could not be enabled")

        integrity_rows = db.execute("PRAGMA integrity_check").fetchall()
        if not (len(integrity_rows) == 1 and integrity_rows[0][0] == "ok"):
            return MigrationResult(status="failed",
                                   reason=f"integrity_check failed: {integrity_rows!r}")

        state = _collect_managed_state(db)

        if state["any_managed_present"]:
            # Expect fully present + correct version.
            if (state["all_tables_present"] and state["all_indexes_present"]
                    and state["all_triggers_present"]):
                db.row_factory = sqlite3.Row
                row = db.execute(
                    "SELECT version FROM awareness_schema_version "
                    "ORDER BY version DESC LIMIT 1"
                ).fetchone()
                if row is not None and row["version"] == EXPECTED_SCHEMA_VERSION:
                    return MigrationResult(
                        status="already_at_version",
                        version=EXPECTED_SCHEMA_VERSION,
                    )
                return MigrationResult(
                    status="partial_or_unknown_managed_state",
                    reason=f"schema present but version row absent or mismatched",
                )
            return MigrationResult(
                status="partial_or_unknown_managed_state",
                reason=(
                    f"partial managed state: tables_missing="
                    f"{sorted(MANAGED_TABLES - state['tables_present'])}, "
                    f"indexes_missing={sorted(MANAGED_INDEXES - state['indexes_present'])}, "
                    f"triggers_missing={sorted(MANAGED_TRIGGERS - state['triggers_present'])}"
                ),
            )

        if dry_run:
            return MigrationResult(status="applied",
                                   version=EXPECTED_SCHEMA_VERSION,
                                   reason="dry_run")

        # Pre-migration backup against the EMPTY expected set (no managed artifacts yet).
        backup = create_backup(
            db_path, backup_dir=backup_dir,
            expected_managed_artifacts=set(),
            managed_namespace_names=_managed_namespace_names(),
            now=now,
        )

        ddl = _schema_sql_path().read_text()
        with db:
            _execute_ddl(db, ddl)
            db.execute(
                "INSERT INTO awareness_schema_version (version, applied_at, package_version) "
                "VALUES (?, ?, ?)",
                (EXPECTED_SCHEMA_VERSION, now, PACKAGE_VERSION),
            )

        # Post-check.
        post_state = _collect_managed_state(db)
        if not (post_state["all_tables_present"] and post_state["all_indexes_present"]
                and post_state["all_triggers_present"]):
            return MigrationResult(status="failed",
                                   reason="post-check: managed artifacts not all present")
        db.row_factory = sqlite3.Row
        row = db.execute(
            "SELECT version FROM awareness_schema_version "
            "WHERE version = ?",
            (EXPECTED_SCHEMA_VERSION,),
        ).fetchone()
        if row is None:
            return MigrationResult(status="failed",
                                   reason="post-check: version row missing")

        return MigrationResult(
            status="applied",
            version=EXPECTED_SCHEMA_VERSION,
            backup=backup,
            tables_created=tuple(sorted(MANAGED_TABLES)),
            indexes_created=tuple(sorted(MANAGED_INDEXES)),
            triggers_created=tuple(sorted(MANAGED_TRIGGERS)),
        )
    finally:
        db.close()


_DROP_ORDER = [
    ("trigger", "tr_receipt_marks_observed"),
    ("trigger", "tr_receipt_requires_eligible_delivery_state"),
    ("trigger", "tr_receipt_requires_live_registration"),
    ("trigger", "tr_capability_verifier_length"),
    ("index",   "idx_delivery_eligible"),
    ("index",   "idx_tool_state"),
    ("index",   "idx_alias_live"),
    ("table",   "disposition_receipts"),
    ("table",   "queue_attempts"),
    ("table",   "targeted_deliveries"),
    ("table",   "session_capabilities"),
    ("table",   "session_registrations"),
    ("table",   "dispatcher_lease"),
    ("table",   "awareness_schema_version"),
]


def rollback(
    db_path: Path | str,
    *,
    backup_dir: Path | str,
    now: int,
    dry_run: bool = False,
) -> RollbackResult:
    """Rollback the awareness schema. Refuses on in-flight state."""
    db_path = Path(db_path)
    backup_dir = Path(backup_dir)
    if not db_path.exists():
        return RollbackResult(status="failed", reason=f"db does not exist: {db_path}")

    db = sqlite3.connect(str(db_path))
    try:
        db.row_factory = sqlite3.Row

        # Refuse on live dispatcher lease.
        lease_row = db.execute(
            "SELECT expires_at FROM dispatcher_lease WHERE singleton_pk = 1"
        ).fetchone() if _has_table(db, "dispatcher_lease") else None
        if lease_row is not None and lease_row["expires_at"] > now:
            return RollbackResult(
                status="refused",
                reason="dispatcher_lease has a live holder; wait for release",
            )

        # Refuse on in-flight deliveries.
        in_flight = db.execute(
            "SELECT COUNT(*) FROM targeted_deliveries "
            "WHERE delivery_state IN ('claimed', 'notified', 'ambiguous', 'blocked')"
        ).fetchone()[0] if _has_table(db, "targeted_deliveries") else 0
        if in_flight > 0:
            return RollbackResult(
                status="refused",
                reason=f"{in_flight} in-flight deliveries; drive to terminal via reconciliation first",
            )

        # Refuse on live capabilities.
        live_caps = db.execute(
            "SELECT COUNT(*) FROM session_capabilities "
            "WHERE revoked_at IS NULL AND expires_at > ?",
            (now,),
        ).fetchone()[0] if _has_table(db, "session_capabilities") else 0
        if live_caps > 0:
            return RollbackResult(
                status="refused",
                reason=f"{live_caps} live capabilities; revoke or wait for expiry",
            )

        if dry_run:
            return RollbackResult(status="rolled_back", reason="dry_run")

        # Collect preserved message IDs for the result (durable deliveries that
        # had sidecar rows but weren't in-flight — observed/undeliverable/pending).
        preserved_ids = tuple(
            r[0] for r in db.execute(
                "SELECT DISTINCT message_id FROM targeted_deliveries"
            ).fetchall()
        ) if _has_table(db, "targeted_deliveries") else ()

        # Backup pre-rollback (expected shape = all managed artifacts present).
        backup = create_backup(
            db_path, backup_dir=backup_dir,
            expected_managed_artifacts=_expected_managed_set(),
            managed_namespace_names=_managed_namespace_names(),
            now=now,
        )

        with db:
            for kind, name in _DROP_ORDER:
                if kind == "trigger":
                    db.execute(f"DROP TRIGGER IF EXISTS {name}")
                elif kind == "index":
                    db.execute(f"DROP INDEX IF EXISTS {name}")
                elif kind == "table":
                    db.execute(f"DROP TABLE IF EXISTS {name}")

        # Post-check: all managed tables gone.
        remaining = _collect_managed_state(db)
        if remaining["any_managed_present"]:
            return RollbackResult(
                status="failed",
                reason=f"post-check: managed artifacts remain: {remaining!r}",
            )

        return RollbackResult(
            status="rolled_back",
            backup=backup,
            preserved_message_ids=preserved_ids,
        )
    finally:
        db.close()


def _has_table(db: sqlite3.Connection, name: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None
