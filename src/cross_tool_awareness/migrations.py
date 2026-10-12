"""Apply + rollback with exact schema+version validation and backup discipline.

v0.2.0 revisions:
- Atomic apply: schema.sql parsed into individual statements executed inside
  one `with db:` block (not sqlite3.executescript, which has its own txn boundary).
- Exact SQL-text validation: compares sqlite_master.sql to canonical statements
  after whitespace normalization.
- Extras detection: refuses apply if an awareness-namespace artifact exists
  that is not in the expected managed set.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .backup import BackupResult, create_backup


EXPECTED_SCHEMA_VERSION = 2
PACKAGE_VERSION = "0.2.0"


MANAGED_TABLES = frozenset({
    "session_registrations",
    "session_capabilities",
    "targeted_deliveries",
    "queue_attempts",
    "disposition_receipts",
    "dispatcher_lease",
    "awareness_schema_version",
    "notice_claims",
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

# Prefixes used by any awareness-namespace artifact. Any sqlite_master name
# matching one of these prefixes that is NOT in a MANAGED_* set is treated as
# unknown managed state and refused.
_MANAGED_PREFIXES = (
    "session_",
    "targeted_",
    "queue_",
    "disposition_",
    "dispatcher_",
    "awareness_",
    "notice_",
    "idx_alias_",
    "idx_tool_",
    "idx_delivery_",
    "tr_capability_",
    "tr_receipt_",
)


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
    return set(MANAGED_TABLES) | set(MANAGED_INDEXES) | set(MANAGED_TRIGGERS)


@dataclass
class MigrationResult:
    status: str
    version: Optional[int] = None
    backup: Optional[BackupResult] = None
    reason: str = ""
    tables_created: tuple = ()
    indexes_created: tuple = ()
    triggers_created: tuple = ()


@dataclass
class RollbackResult:
    status: str
    reason: str = ""
    backup: Optional[BackupResult] = None
    preserved_message_ids: tuple = ()


def _schema_sql_path() -> Path:
    return Path(__file__).parent / "schema.sql"


# --- Statement parsing -----------------------------------------------------


_COMMENT_RE = re.compile(r"^\s*--.*$", re.MULTILINE)


def _strip_comments(sql_text: str) -> str:
    return _COMMENT_RE.sub("", sql_text)


def parse_statements(sql_text: str) -> list[str]:
    """Parse a SQL script into complete statements using sqlite3.complete_statement.

    Handles CREATE TRIGGER ... BEGIN ... END blocks (which contain semicolons)
    by accumulating lines until sqlite3 reports the statement complete.
    """
    stripped = _strip_comments(sql_text)
    statements = []
    buffer = ""
    for line in stripped.splitlines(keepends=True):
        if not line.strip() and not buffer.strip():
            continue
        buffer += line
        if sqlite3.complete_statement(buffer):
            stmt = buffer.strip()
            if stmt and not stmt.lower().startswith("--"):
                statements.append(stmt)
            buffer = ""
    if buffer.strip():
        raise ValueError(f"incomplete trailing statement in schema.sql: {buffer!r}")
    return statements


# --- Managed state collection + exact validation ---------------------------


_IF_NOT_EXISTS_RE = re.compile(r"\bIF\s+NOT\s+EXISTS\s+", re.IGNORECASE)


def _normalize_sql(sql: str) -> str:
    """Normalize whitespace + strip IF NOT EXISTS for SQL text comparison.

    sqlite_master.sql drops IF NOT EXISTS from the stored CREATE statement, so
    the canonical schema.sql text must have it stripped too to compare equal.
    """
    if sql is None:
        return ""
    s = _strip_comments(sql)
    s = _IF_NOT_EXISTS_RE.sub("", s)
    s = re.sub(r"\s+", " ", s)
    s = s.strip().rstrip(";").strip()
    return s


def _canonical_sql_by_name() -> dict[str, str]:
    """Parse schema.sql and index canonical normalized SQL by artifact name."""
    text = _schema_sql_path().read_text()
    out = {}
    for stmt in parse_statements(text):
        norm = _normalize_sql(stmt)
        # Extract artifact name from CREATE statement
        m = re.match(
            r"CREATE\s+(?:UNIQUE\s+)?(TABLE|INDEX|TRIGGER)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
            norm,
            re.IGNORECASE,
        )
        if m:
            name = m.group(2)
            out[name] = norm
    return out


def _collect_managed_state(db: sqlite3.Connection) -> dict:
    """Return the set of managed-namespace artifacts actually present + their SQL."""
    rows = db.execute(
        "SELECT type, name, sql FROM sqlite_master "
        "WHERE type IN ('table', 'index', 'trigger') "
        "  AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    tables = {n for t, n, _ in rows if t == "table" and n in MANAGED_TABLES}
    indexes = {n for t, n, _ in rows if t == "index" and n in MANAGED_INDEXES}
    triggers = {n for t, n, _ in rows if t == "trigger" and n in MANAGED_TRIGGERS}
    sql_by_name = {n: s for _, n, s in rows}
    # Extras: anything whose name matches an awareness-namespace prefix but
    # isn't in a MANAGED_* set.
    extras = set()
    for t, n, _ in rows:
        if n in MANAGED_TABLES or n in MANAGED_INDEXES or n in MANAGED_TRIGGERS:
            continue
        for pfx in _MANAGED_PREFIXES:
            if n.startswith(pfx):
                extras.add((t, n))
                break
    return {
        "tables_present": tables,
        "indexes_present": indexes,
        "triggers_present": triggers,
        "any_managed_present": bool(tables | indexes | triggers),
        "all_tables_present": tables == MANAGED_TABLES,
        "all_indexes_present": indexes == MANAGED_INDEXES,
        "all_triggers_present": triggers == MANAGED_TRIGGERS,
        "extras": extras,
        "sql_by_name": sql_by_name,
    }


def _validate_sql_text_matches_canonical(state: dict) -> Optional[str]:
    """Return a reason string on mismatch, or None if all OK."""
    canonical = _canonical_sql_by_name()
    for name in MANAGED_TABLES | MANAGED_INDEXES | MANAGED_TRIGGERS:
        if name not in state["sql_by_name"]:
            continue
        actual = _normalize_sql(state["sql_by_name"][name])
        expected = canonical.get(name)
        if expected is None:
            continue
        if actual != expected:
            return (
                f"SQL text mismatch for managed artifact {name!r}: "
                f"actual={actual[:120]!r} expected={expected[:120]!r}"
            )
    return None


# --- apply / rollback ------------------------------------------------------


def apply(
    db_path,
    *,
    backup_dir,
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

        if state["extras"]:
            return MigrationResult(
                status="unknown_managed_state",
                reason=f"unknown managed-namespace artifacts: {sorted(state['extras'])}",
            )

        if state["any_managed_present"]:
            if (state["all_tables_present"] and state["all_indexes_present"]
                    and state["all_triggers_present"]):
                db.row_factory = sqlite3.Row
                row = db.execute(
                    "SELECT version FROM awareness_schema_version "
                    "ORDER BY version DESC LIMIT 1"
                ).fetchone()
                if row is not None and row["version"] == EXPECTED_SCHEMA_VERSION:
                    mismatch = _validate_sql_text_matches_canonical(state)
                    if mismatch:
                        return MigrationResult(
                            status="partial_or_unknown_managed_state",
                            reason=mismatch,
                        )
                    return MigrationResult(
                        status="already_at_version",
                        version=EXPECTED_SCHEMA_VERSION,
                    )
                return MigrationResult(
                    status="partial_or_unknown_managed_state",
                    reason="schema present but version row absent or mismatched",
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

        backup = create_backup(
            db_path, backup_dir=backup_dir,
            expected_managed_artifacts=set(),
            managed_namespace_names=_managed_namespace_names(),
            now=now,
        )

        ddl_text = _schema_sql_path().read_text()
        statements = parse_statements(ddl_text)
        try:
            with db:  # single transaction wrapping all DDL + version row
                for stmt in statements:
                    db.execute(stmt)
                db.execute(
                    "INSERT INTO awareness_schema_version (version, applied_at, package_version) "
                    "VALUES (?, ?, ?)",
                    (EXPECTED_SCHEMA_VERSION, now, PACKAGE_VERSION),
                )
        except sqlite3.Error as exc:
            return MigrationResult(status="failed", reason=f"DDL/version apply failed: {exc}")

        post_state = _collect_managed_state(db)
        if not (post_state["all_tables_present"] and post_state["all_indexes_present"]
                and post_state["all_triggers_present"]):
            return MigrationResult(status="failed",
                                   reason="post-check: managed artifacts not all present")
        mismatch = _validate_sql_text_matches_canonical(post_state)
        if mismatch:
            return MigrationResult(status="failed",
                                   reason=f"post-check SQL mismatch: {mismatch}")
        db.row_factory = sqlite3.Row
        row = db.execute(
            "SELECT version FROM awareness_schema_version WHERE version = ?",
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
    ("index", "idx_delivery_eligible"),
    ("index", "idx_tool_state"),
    ("index", "idx_alias_live"),
    ("table", "disposition_receipts"),
    ("table", "queue_attempts"),
    ("table", "targeted_deliveries"),
    ("table", "session_capabilities"),
    ("table", "session_registrations"),
    ("table", "dispatcher_lease"),
    ("table", "notice_claims"),
    ("table", "awareness_schema_version"),
]


def rollback(
    db_path,
    *,
    backup_dir,
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

        lease_row = (
            db.execute("SELECT expires_at FROM dispatcher_lease WHERE singleton_pk = 1").fetchone()
            if _has_table(db, "dispatcher_lease") else None
        )
        if lease_row is not None and lease_row["expires_at"] > now:
            return RollbackResult(
                status="refused",
                reason="dispatcher_lease has a live holder; wait for release",
            )

        in_flight = (
            db.execute(
                "SELECT COUNT(*) FROM targeted_deliveries "
                "WHERE delivery_state IN ('claimed', 'notified', 'ambiguous', 'blocked')"
            ).fetchone()[0]
            if _has_table(db, "targeted_deliveries") else 0
        )
        if in_flight > 0:
            return RollbackResult(
                status="refused",
                reason=f"{in_flight} in-flight deliveries; drive to terminal via reconciliation first",
            )

        live_caps = (
            db.execute(
                "SELECT COUNT(*) FROM session_capabilities "
                "WHERE revoked_at IS NULL AND expires_at > ?",
                (now,),
            ).fetchone()[0]
            if _has_table(db, "session_capabilities") else 0
        )
        if live_caps > 0:
            return RollbackResult(
                status="refused",
                reason=f"{live_caps} live capabilities; revoke or wait for expiry",
            )

        if dry_run:
            return RollbackResult(status="rolled_back", reason="dry_run")

        preserved_ids = (
            tuple(r[0] for r in db.execute(
                "SELECT DISTINCT message_id FROM targeted_deliveries"
            ).fetchall())
            if _has_table(db, "targeted_deliveries") else ()
        )

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
