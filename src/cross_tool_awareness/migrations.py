"""Apply + rollback with exact schema+version validation and backup discipline.

v0.2.0 revisions:
- Atomic apply: schema.sql parsed into individual statements executed inside
  one `with db:` block (not sqlite3.executescript, which has its own txn boundary).
- Exact SQL-text validation: compares sqlite_master.sql to canonical statements
  after whitespace normalization.
- Extras detection: refuses apply if an awareness-namespace artifact exists
  that is not in the expected managed set.

v0.4.0 (schema v3) revisions:
- EXPECTED_SCHEMA_VERSION bumped from 2 → 3; PACKAGE_VERSION from 0.2.0 → 0.4.0.
- schema.sql canonical shape is now v3 (adds holder_pid + process_started_at
  on session_registrations with the PID-0-only-for-non-live CHECK; adds the
  dispatching delivery_state and dispatch_started_at column on
  targeted_deliveries; extends both receipt triggers to accept dispatching).
- apply() routes a v2-schema DB through migrate_v2_to_v3() instead of refusing
  it as partial managed state.
- migrate_v2_to_v3() performs the atomic table rebuilds (session_registrations,
  targeted_deliveries) and trigger recreations under the required SQLite FK
  PRAGMA discipline (foreign_keys=OFF before BEGIN, foreign_key_check inside
  the transaction, foreign_keys=ON after COMMIT). Legacy rows are preserved
  but marked holder_pid=0 + state='stale' + revoked_at populated so the alias
  is released and truthful v0.4 re-registration can replace them with a fresh
  incarnation UUID.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .backup import BackupResult, create_backup


EXPECTED_SCHEMA_VERSION = 3
PACKAGE_VERSION = "0.4.0"


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


_QUOTED_IDENT_RE = re.compile(r'"([A-Za-z_][A-Za-z0-9_]*)"')


def _normalize_sql(sql: str) -> str:
    """Normalize whitespace + strip IF NOT EXISTS for SQL text comparison.

    sqlite_master.sql drops IF NOT EXISTS from the stored CREATE statement, so
    the canonical schema.sql text must have it stripped too to compare equal.

    Also strips double-quotes around simple identifiers: SQLite's ALTER TABLE
    RENAME TO rewrites the stored CREATE statement with the new name quoted
    (`"session_registrations"`), while the canonical schema.sql has it bare
    (`session_registrations`). Both forms are semantically identical for
    identifier matching, so we normalize to the unquoted form before compare.
    """
    if sql is None:
        return ""
    s = _strip_comments(sql)
    s = _IF_NOT_EXISTS_RE.sub("", s)
    s = _QUOTED_IDENT_RE.sub(r"\1", s)
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


# --- v2 → v3 migration -----------------------------------------------------


# Canonical v3 DDL for the two rebuilt tables + three recreated triggers. These
# strings MUST produce sqlite_master.sql values that match the normalized
# canonical schema.sql after _normalize_sql(); any drift is caught by the
# post-apply _validate_sql_text_matches_canonical check in apply().
_V3_REBUILT_SESSION_REGISTRATIONS = """
CREATE TABLE session_registrations (
    session_uuid                TEXT NOT NULL,
    session_incarnation_id      TEXT NOT NULL,
    alias                       TEXT NOT NULL,
    tool                        TEXT NOT NULL CHECK (tool IN ('codex', 'claude')),
    project_slug                TEXT NOT NULL,
    project_root                TEXT NOT NULL,
    role                        TEXT NOT NULL CHECK (role IN ('manager', 'worker', 'reviewer', 'general')),
    state                       TEXT NOT NULL CHECK (state IN ('active', 'busy', 'idle', 'stopping', 'stale')),
    wake_capability             TEXT NOT NULL CHECK (wake_capability IN ('codex_queue', 'claude_socket', 'notify_only')),
    registration_fence          TEXT NOT NULL,
    wake_locator                TEXT NOT NULL,
    started_at                  INTEGER NOT NULL,
    heartbeat_at                INTEGER NOT NULL,
    expires_at                  INTEGER NOT NULL,
    revoked_at                  INTEGER,
    holder_pid                  INTEGER NOT NULL,
    process_started_at          INTEGER NOT NULL,
    PRIMARY KEY (session_uuid, session_incarnation_id),
    CHECK (started_at <= heartbeat_at),
    CHECK (heartbeat_at <= expires_at),
    CHECK (holder_pid >= 0),
    CHECK (holder_pid > 0 OR state IN ('stale', 'stopping'))
)
"""

_V3_REBUILT_TARGETED_DELIVERIES = """
CREATE TABLE targeted_deliveries (
    message_id                          TEXT NOT NULL,
    target_session_uuid                 TEXT NOT NULL,
    target_session_incarnation_id       TEXT NOT NULL,
    sender_session_uuid                 TEXT NOT NULL,
    sender_session_incarnation_id       TEXT NOT NULL,
    delivery_state                      TEXT NOT NULL CHECK (delivery_state IN (
        'pending', 'claimed', 'dispatching', 'notified', 'ambiguous', 'blocked', 'observed', 'undeliverable'
    )),
    claim_lease_token                   TEXT,
    claim_lease_expires_at              INTEGER,
    dispatch_started_at                 INTEGER,
    created_at                          INTEGER NOT NULL,
    attempt_count                       INTEGER NOT NULL DEFAULT 0,
    next_attempt_at                     INTEGER NOT NULL,
    expires_at                          INTEGER NOT NULL,
    last_error_class                    TEXT,
    PRIMARY KEY (message_id, target_session_uuid, target_session_incarnation_id),
    FOREIGN KEY (message_id) REFERENCES messages(id)
)
"""

_V3_TRIGGER_RECEIPT_ELIGIBLE = """
CREATE TRIGGER tr_receipt_requires_eligible_delivery_state
BEFORE INSERT ON disposition_receipts
BEGIN
    SELECT RAISE(ABORT, 'receipt not permitted: delivery state is not receipt-eligible')
    WHERE NOT EXISTS (
        SELECT 1 FROM targeted_deliveries
        WHERE message_id = NEW.message_id
          AND target_session_uuid = NEW.target_session_uuid
          AND target_session_incarnation_id = NEW.target_session_incarnation_id
          AND delivery_state IN ('claimed', 'dispatching', 'notified', 'ambiguous')
    );
END
"""

_V3_TRIGGER_RECEIPT_MARKS_OBSERVED = """
CREATE TRIGGER tr_receipt_marks_observed
AFTER INSERT ON disposition_receipts
BEGIN
    UPDATE targeted_deliveries
    SET delivery_state = 'observed',
        claim_lease_token = NULL,
        claim_lease_expires_at = NULL
    WHERE message_id = NEW.message_id
      AND target_session_uuid = NEW.target_session_uuid
      AND target_session_incarnation_id = NEW.target_session_incarnation_id
      AND delivery_state IN ('claimed', 'dispatching', 'notified', 'ambiguous');
END
"""


# tr_receipt_requires_live_registration is unchanged by v0.4 (no delta on
# session_registrations liveness semantics), but it references
# session_registrations by name, so SQLite 3.25+'s RENAME TO validation will
# raise if we try to rename the rebuilt table while the trigger exists against
# a mid-rebuild schema. We drop it before the rebuild and recreate it after
# verbatim.
_V3_TRIGGER_RECEIPT_REQUIRES_LIVE_REGISTRATION = """
CREATE TRIGGER tr_receipt_requires_live_registration
BEFORE INSERT ON disposition_receipts
BEGIN
    SELECT RAISE(ABORT, 'receiver registration not live at delivered_at')
    WHERE NOT EXISTS (
        SELECT 1 FROM session_registrations
        WHERE session_uuid = NEW.receiver_session_uuid
          AND session_incarnation_id = NEW.receiver_session_incarnation_id
          AND revoked_at IS NULL
          AND expires_at > NEW.delivered_at
    );
END
"""


def _apply_v2_to_v3_migration(db_path: Path, *, now: int) -> None:
    """Rebuild v2 → v3 under the SQLite FK PRAGMA discipline.

    Must be called with a closed caller-held connection: this function opens
    its own connection and toggles ``PRAGMA foreign_keys`` outside any
    transaction (PRAGMA foreign_keys is a no-op inside one). Raises
    sqlite3.Error on any failure; the finally block restores
    ``PRAGMA foreign_keys=ON`` even on failure.

    Legacy session_registrations rows are preserved for FK + audit history
    but marked ``holder_pid=0``, ``state='stale'``, and ``revoked_at`` is
    populated (or kept if already revoked). This releases the live-alias
    unique index so a truthful v0.4 re-registration can replace them with a
    fresh incarnation UUID. ``proc_start_ns`` nanoseconds are converted to
    Unix seconds for the new ``process_started_at`` column (lossy but
    display-only — v0.4 liveness checks use the stored seconds-granularity
    value against a receiver-owned OS probe, not the pre-v0.4 nanosecond
    value).
    """
    conn = sqlite3.connect(str(db_path))
    try:
        # Precondition: no active transaction. sqlite3's Connection does not
        # auto-begin on connect; ensure in_transaction is False.
        if conn.in_transaction:
            raise sqlite3.OperationalError(
                "v2→v3 migration requires a non-active transaction"
            )

        # FK discipline: toggle OFF BEFORE BEGIN (PRAGMAs inside a transaction
        # are no-ops). Verify it took.
        conn.execute("PRAGMA foreign_keys = OFF")
        fk_off = conn.execute("PRAGMA foreign_keys").fetchone()
        if fk_off is None or fk_off[0] != 0:
            raise sqlite3.OperationalError(
                "PRAGMA foreign_keys=OFF could not be set before migration"
            )

        try:
            conn.execute("BEGIN IMMEDIATE")

            # 0. Drop all triggers that reference session_registrations or
            # targeted_deliveries by name. SQLite 3.25+ validates trigger
            # bodies at ALTER TABLE RENAME TO time; a trigger that references
            # a mid-rebuild table name raises "no such table" during the
            # rename. tr_capability_verifier_length is on session_capabilities
            # and references no other table, so it stays.
            conn.execute("DROP TRIGGER IF EXISTS tr_receipt_marks_observed")
            conn.execute("DROP TRIGGER IF EXISTS tr_receipt_requires_eligible_delivery_state")
            conn.execute("DROP TRIGGER IF EXISTS tr_receipt_requires_live_registration")

            # 1. Rebuild session_registrations: new shape with holder_pid +
            # process_started_at, drop proc_start_ns. Legacy rows forced stale
            # + revoked.
            conn.execute(_V3_REBUILT_SESSION_REGISTRATIONS.replace(
                "CREATE TABLE session_registrations",
                "CREATE TABLE _new_session_registrations",
            ))
            conn.execute(
                "INSERT INTO _new_session_registrations ("
                "  session_uuid, session_incarnation_id, alias, tool,"
                "  project_slug, project_root, role, state, wake_capability,"
                "  registration_fence, wake_locator, started_at, heartbeat_at,"
                "  expires_at, revoked_at, holder_pid, process_started_at"
                ") SELECT "
                "  session_uuid, session_incarnation_id, alias, tool,"
                "  project_slug, project_root, role, 'stale' AS state,"
                "  wake_capability, registration_fence, wake_locator,"
                "  started_at, heartbeat_at, expires_at,"
                "  COALESCE(revoked_at, ?) AS revoked_at,"
                "  0 AS holder_pid,"
                "  COALESCE(proc_start_ns / 1000000000, 0) AS process_started_at "
                "FROM session_registrations",
                (now,),
            )
            conn.execute("DROP TABLE session_registrations")
            conn.execute(
                "ALTER TABLE _new_session_registrations "
                "RENAME TO session_registrations"
            )
            conn.execute(
                "CREATE UNIQUE INDEX idx_alias_live "
                "ON session_registrations(alias) WHERE revoked_at IS NULL"
            )
            conn.execute(
                "CREATE INDEX idx_tool_state "
                "ON session_registrations(tool, state) WHERE revoked_at IS NULL"
            )

            # 2. Rebuild targeted_deliveries: add 'dispatching' CHECK value +
            # dispatch_started_at column. Copy all existing rows unchanged.
            conn.execute(_V3_REBUILT_TARGETED_DELIVERIES.replace(
                "CREATE TABLE targeted_deliveries",
                "CREATE TABLE _new_targeted_deliveries",
            ))
            conn.execute(
                "INSERT INTO _new_targeted_deliveries ("
                "  message_id, target_session_uuid, target_session_incarnation_id,"
                "  sender_session_uuid, sender_session_incarnation_id,"
                "  delivery_state, claim_lease_token, claim_lease_expires_at,"
                "  dispatch_started_at, created_at, attempt_count,"
                "  next_attempt_at, expires_at, last_error_class"
                ") SELECT "
                "  message_id, target_session_uuid, target_session_incarnation_id,"
                "  sender_session_uuid, sender_session_incarnation_id,"
                "  delivery_state, claim_lease_token, claim_lease_expires_at,"
                "  NULL AS dispatch_started_at, created_at, attempt_count,"
                "  next_attempt_at, expires_at, last_error_class "
                "FROM targeted_deliveries"
            )
            conn.execute("DROP TABLE targeted_deliveries")
            conn.execute(
                "ALTER TABLE _new_targeted_deliveries "
                "RENAME TO targeted_deliveries"
            )
            conn.execute(
                "CREATE INDEX idx_delivery_eligible "
                "ON targeted_deliveries(next_attempt_at) "
                "WHERE delivery_state = 'pending'"
            )

            # 3. Recreate the three receipt triggers. Two gain 'dispatching'
            # in their eligible-source WHERE; the live-registration trigger is
            # recreated verbatim because it was dropped in step 0 to unblock
            # the table rename, not because it changed.
            conn.execute(_V3_TRIGGER_RECEIPT_REQUIRES_LIVE_REGISTRATION)
            conn.execute(_V3_TRIGGER_RECEIPT_ELIGIBLE)
            conn.execute(_V3_TRIGGER_RECEIPT_MARKS_OBSERVED)

            # 4. FK integrity check — must be empty before COMMIT.
            fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if fk_violations:
                raise sqlite3.IntegrityError(
                    f"foreign_key_check violations after v2→v3 rebuild: "
                    f"{fk_violations!r}"
                )

            # 5. Record the new version row.
            conn.execute(
                "INSERT INTO awareness_schema_version "
                "(version, applied_at, package_version) VALUES (?, ?, ?)",
                (EXPECTED_SCHEMA_VERSION, now, PACKAGE_VERSION),
            )

            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
    finally:
        # Restore FK enforcement on the way out, even on failure. Behavior
        # per codex-manager seq=229 hardening:
        #   - If a primary exception is already in flight, suppress any
        #     restoration failure so it does not mask the primary error.
        #     The caller sees the original migration failure, not a PRAGMA
        #     side effect.
        #   - If no primary exception is in flight (migration committed
        #     successfully), verify the restoration read back `1` and raise
        #     if it did not. A silent "FK restoration failed on the happy
        #     path" would leave the caller with foreign-key enforcement
        #     globally off until something else re-enables it.
        import sys as _sys
        primary_exc = _sys.exc_info()[0]
        try:
            conn.execute("PRAGMA foreign_keys = ON")
            if primary_exc is None:
                fk_on = conn.execute("PRAGMA foreign_keys").fetchone()
                if fk_on is None or fk_on[0] != 1:
                    raise sqlite3.OperationalError(
                        "PRAGMA foreign_keys=ON could not be restored "
                        "after migration"
                    )
        except sqlite3.Error:
            if primary_exc is None:
                raise
        finally:
            conn.close()


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
                installed_version = row["version"] if row is not None else None
                if installed_version == EXPECTED_SCHEMA_VERSION:
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
                if installed_version == 2 and EXPECTED_SCHEMA_VERSION == 3:
                    # v2 → v3 upgrade path. Close the apply-owned connection
                    # first: the migration must own PRAGMA foreign_keys toggles
                    # outside any transaction, which requires exclusive access.
                    if dry_run:
                        return MigrationResult(
                            status="applied",
                            version=EXPECTED_SCHEMA_VERSION,
                            reason="dry_run v2→v3",
                        )
                    backup = create_backup(
                        db_path, backup_dir=backup_dir,
                        expected_managed_artifacts=_expected_managed_set(),
                        managed_namespace_names=_managed_namespace_names(),
                        now=now,
                    )
                    db.close()
                    try:
                        _apply_v2_to_v3_migration(db_path, now=now)
                    except sqlite3.Error as exc:
                        return MigrationResult(
                            status="failed",
                            reason=f"v2→v3 migration failed: {exc}",
                        )
                    # Reopen for post-apply validation (mirrors fresh-install
                    # path below).
                    db = sqlite3.connect(str(db_path))
                    db.execute("PRAGMA foreign_keys = ON")
                    post_state = _collect_managed_state(db)
                    if not (post_state["all_tables_present"]
                            and post_state["all_indexes_present"]
                            and post_state["all_triggers_present"]):
                        return MigrationResult(
                            status="failed",
                            reason="post-migration: managed artifacts not all present",
                        )
                    mismatch = _validate_sql_text_matches_canonical(post_state)
                    if mismatch:
                        return MigrationResult(
                            status="failed",
                            reason=f"post-migration SQL mismatch: {mismatch}",
                        )
                    db.row_factory = sqlite3.Row
                    row = db.execute(
                        "SELECT version FROM awareness_schema_version "
                        "WHERE version = ?",
                        (EXPECTED_SCHEMA_VERSION,),
                    ).fetchone()
                    if row is None:
                        return MigrationResult(
                            status="failed",
                            reason="post-migration: version row missing",
                        )
                    return MigrationResult(
                        status="applied",
                        version=EXPECTED_SCHEMA_VERSION,
                        backup=backup,
                        tables_created=tuple(sorted(MANAGED_TABLES)),
                        indexes_created=tuple(sorted(MANAGED_INDEXES)),
                        triggers_created=tuple(sorted(MANAGED_TRIGGERS)),
                    )
                return MigrationResult(
                    status="partial_or_unknown_managed_state",
                    reason=(
                        f"schema at version {installed_version}, "
                        f"expected {EXPECTED_SCHEMA_VERSION}"
                    ),
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
                "WHERE delivery_state IN ('claimed', 'dispatching', 'notified', 'ambiguous', 'blocked')"
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
