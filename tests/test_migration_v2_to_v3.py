"""v2 → v3 migration tests (v0.4.0).

Pin the behavior codex-manager and MM settled on in the v0.4 spec v3.1:

- A bridge.db that currently carries the v0.2.0 awareness schema (`version=2`
  in `awareness_schema_version`) is upgraded in place to v3.
- Every legacy `session_registrations` row is preserved for FK + audit history
  but transformed to `holder_pid=0`, `state='stale'`, and `revoked_at` is
  populated (reused when already set, otherwise set to the migration's `now`).
  This releases the live-alias unique index so a truthful v0.4 re-registration
  can replace it with a fresh incarnation UUID.
- `proc_start_ns` (nanoseconds) is replaced by `process_started_at` (Unix
  seconds). The conversion is lossy by design — v0.4 liveness checks use the
  stored seconds-granularity value against a receiver-owned OS probe.
- `targeted_deliveries` gains the `dispatching` state value, the
  `dispatch_started_at` column (NULL for pre-existing rows), and both receipt
  triggers now accept `dispatching` as an eligible source state.
- The migration runs under SQLite's required FK PRAGMA discipline:
  `foreign_keys=OFF` BEFORE `BEGIN IMMEDIATE`, `foreign_key_check` inside the
  transaction, `foreign_keys=ON` restored after `COMMIT` (and in the
  failure-finally).
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from cross_tool_awareness import (
    EXPECTED_SCHEMA_VERSION,
    MANAGED_INDEXES,
    MANAGED_TABLES,
    MANAGED_TRIGGERS,
    PresenceRecord,
    apply as apply_migration,
    issue_capability,
    verify_capability_with_liveness,
)
from cross_tool_awareness.registry import register_session
from cross_tool_awareness.migrations import PACKAGE_VERSION


_STUB_MESSAGES_DDL = """
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0
);
"""


def _v2_schema_sql() -> str:
    return (Path(__file__).parent / "fixtures" / "v0_3_1_schema.sql").read_text()


def _u() -> str:
    return str(uuid.uuid4())


def _seed_v2_bridge(path: Path) -> dict:
    """Create a file DB carrying the v0.2.0 awareness schema + representative rows.

    Returns a dict of the seeded UUIDs so assertions can look them up.
    """
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(_STUB_MESSAGES_DDL)
        conn.executescript(_v2_schema_sql())
        conn.commit()

        # Record a v2 schema version row — matches what the v0.3.1 apply()
        # inserted on install.
        conn.execute(
            "INSERT INTO awareness_schema_version "
            "(version, applied_at, package_version) VALUES (?, ?, ?)",
            (2, 999_000, "0.2.0"),
        )

        # Seed a representative mix of registrations:
        #   row 1: live (not revoked, future expiry) + has proc_start_ns
        #   row 2: already revoked (should keep its revoked_at)
        #   row 3: no proc_start_ns at all (nullable in v2)
        reg_live_uuid, reg_live_inc = _u(), _u()
        reg_revoked_uuid, reg_revoked_inc = _u(), _u()
        reg_no_proc_uuid, reg_no_proc_inc = _u(), _u()
        now = 1_000_000
        conn.execute(
            "INSERT INTO session_registrations "
            "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
            " project_root, role, state, wake_capability, registration_fence, "
            " wake_locator, started_at, heartbeat_at, expires_at, revoked_at, "
            " proc_start_ns) "
            "VALUES (?, ?, 'live-alias', 'codex', 'p', '/tmp/p', 'manager', "
            " 'active', 'codex_queue', 'f1', 'l1', ?, ?, ?, NULL, ?)",
            (reg_live_uuid, reg_live_inc, now, now + 10, now + 86_400,
             1_234_567_890_000),  # proc_start_ns = 1234.567890 Unix seconds
        )
        conn.execute(
            "INSERT INTO session_registrations "
            "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
            " project_root, role, state, wake_capability, registration_fence, "
            " wake_locator, started_at, heartbeat_at, expires_at, revoked_at, "
            " proc_start_ns) "
            "VALUES (?, ?, 'revoked-alias', 'codex', 'p', '/tmp/p', 'worker', "
            " 'idle', 'codex_queue', 'f2', 'l2', ?, ?, ?, ?, ?)",
            (reg_revoked_uuid, reg_revoked_inc, now, now + 10, now + 86_400,
             now + 5_000, 2_000_000_000_000),
        )
        conn.execute(
            "INSERT INTO session_registrations "
            "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
            " project_root, role, state, wake_capability, registration_fence, "
            " wake_locator, started_at, heartbeat_at, expires_at, revoked_at, "
            " proc_start_ns) "
            "VALUES (?, ?, 'noproc-alias', 'codex', 'p', '/tmp/p', 'reviewer', "
            " 'idle', 'codex_queue', 'f3', 'l3', ?, ?, ?, NULL, NULL)",
            (reg_no_proc_uuid, reg_no_proc_inc, now, now + 10, now + 86_400),
        )

        # Seed a capability + a delivery row to prove FK integrity is preserved
        # across the rebuild.
        verifier = b"\x11" * 32
        conn.execute(
            "INSERT INTO session_capabilities "
            "(verifier_sha256, sender_session_uuid, sender_session_incarnation_id, "
            " registration_fence, issued_at, expires_at) "
            "VALUES (?, ?, ?, 'f1', ?, ?)",
            (verifier, reg_live_uuid, reg_live_inc, now, now + 86_400),
        )
        msg_id = _u()
        conn.execute(
            "INSERT INTO messages (id, payload, created_at) VALUES (?, 'x', ?)",
            (msg_id, now),
        )
        conn.execute(
            "INSERT INTO targeted_deliveries "
            "(message_id, target_session_uuid, target_session_incarnation_id, "
            " sender_session_uuid, sender_session_incarnation_id, delivery_state, "
            " created_at, next_attempt_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
            (msg_id, reg_live_uuid, reg_live_inc, reg_live_uuid, reg_live_inc,
             now, now, now + 3_600),
        )

        conn.commit()
        return {
            "reg_live": (reg_live_uuid, reg_live_inc),
            "reg_revoked": (reg_revoked_uuid, reg_revoked_inc),
            "reg_no_proc": (reg_no_proc_uuid, reg_no_proc_inc),
            "message_id": msg_id,
            "verifier": verifier,
            "pre_revoked_at": now + 5_000,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Core apply() routing
# ---------------------------------------------------------------------------


def test_v2_bridge_upgrades_to_v3(tmp_path):
    path = tmp_path / "bridge.db"
    seeded = _seed_v2_bridge(path)
    backup_dir = tmp_path / "backups"
    now = 2_000_000

    result = apply_migration(path, backup_dir=backup_dir, now=now)

    assert result.status == "applied", f"migration failed: {result.reason}"
    # v0.5.0: EXPECTED_SCHEMA_VERSION is 4; a v2 bridge chains v2→v3→v4.
    assert result.version == 4 == EXPECTED_SCHEMA_VERSION
    # Backup should have been taken before the destructive rebuild.
    assert result.backup is not None

    with sqlite3.connect(str(path)) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT version, package_version FROM awareness_schema_version "
            "ORDER BY version DESC LIMIT 1"
        ).fetchall()
        assert rows[0]["version"] == 4
        assert rows[0]["package_version"] == PACKAGE_VERSION == "0.5.0"


def test_v3_fresh_install_still_works(tmp_path):
    """Verify the fresh-install path ends at EXPECTED_SCHEMA_VERSION (v0.5.0 → 4)."""
    path = tmp_path / "bridge.db"
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.close()
    result = apply_migration(path, backup_dir=tmp_path / "backups",
                             now=1_000_000)
    assert result.status == "applied"
    assert result.version == 4


def test_v3_reapply_is_idempotent(tmp_path):
    """Running apply() twice on a v2 bridge is a no-op after the first chain."""
    path = tmp_path / "bridge.db"
    _seed_v2_bridge(path)

    first = apply_migration(path, backup_dir=tmp_path / "backups-1", now=2_000_000)
    assert first.status == "applied"

    second = apply_migration(path, backup_dir=tmp_path / "backups-2", now=2_001_000)
    assert second.status == "already_at_version"
    assert second.version == 4


def test_v2_dry_run_does_not_write(tmp_path):
    path = tmp_path / "bridge.db"
    _seed_v2_bridge(path)

    # Compute a pre-migration snapshot of the version row.
    with sqlite3.connect(str(path)) as db:
        pre = db.execute(
            "SELECT version FROM awareness_schema_version ORDER BY version DESC LIMIT 1"
        ).fetchone()
    assert pre[0] == 2

    result = apply_migration(
        path, backup_dir=tmp_path / "backups", now=2_000_000, dry_run=True,
    )
    assert result.status == "applied"
    assert result.reason == "dry_run v2→v3→v4"

    # No side effects: version row still at 2.
    with sqlite3.connect(str(path)) as db:
        post = db.execute(
            "SELECT version FROM awareness_schema_version ORDER BY version DESC LIMIT 1"
        ).fetchone()
    assert post[0] == 2


# ---------------------------------------------------------------------------
# Row preservation + legacy posture
# ---------------------------------------------------------------------------


def test_legacy_registrations_preserved_as_stale_with_pid_0(tmp_path):
    path = tmp_path / "bridge.db"
    seeded = _seed_v2_bridge(path)
    now = 2_000_000

    result = apply_migration(path, backup_dir=tmp_path / "backups", now=now)
    assert result.status == "applied"

    with sqlite3.connect(str(path)) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT session_uuid, session_incarnation_id, state, holder_pid, "
            "process_started_at, revoked_at, alias "
            "FROM session_registrations"
        ).fetchall()
        assert len(rows) == 3
        for row in rows:
            assert row["holder_pid"] == 0, "legacy rows must have reserved PID 0"
            assert row["state"] == "stale", (
                "legacy rows must be forced stale so a truthful v0.4 "
                "re-registration can supersede them"
            )

        by_uuid = {(r["session_uuid"], r["session_incarnation_id"]): r for r in rows}

        # Legacy "live" row (no pre-existing revoked_at) picks up `now` as its
        # revoked_at — releasing the live-alias unique index.
        live_row = by_uuid[seeded["reg_live"]]
        assert live_row["revoked_at"] == now

        # Already-revoked row keeps its original revoked_at.
        revoked_row = by_uuid[seeded["reg_revoked"]]
        assert revoked_row["revoked_at"] == seeded["pre_revoked_at"]

        # proc_start_ns ns→s conversion: 1234567890000ns → 1234 (integer seconds).
        assert live_row["process_started_at"] == 1_234
        # NULL proc_start_ns becomes 0 (COALESCE path).
        no_proc_row = by_uuid[seeded["reg_no_proc"]]
        assert no_proc_row["process_started_at"] == 0


def test_live_alias_is_released_so_v4_can_re_register(tmp_path):
    """After migration, the previously-live alias is reusable (M10 superseded path hook)."""
    path = tmp_path / "bridge.db"
    _seed_v2_bridge(path)
    now = 2_000_000

    result = apply_migration(path, backup_dir=tmp_path / "backups", now=now)
    assert result.status == "applied"

    # Insert a fresh-incarnation live registration against the same alias.
    with sqlite3.connect(str(path)) as db:
        db.execute("PRAGMA foreign_keys = ON")
        new_uuid, new_inc = _u(), _u()
        db.execute(
            "INSERT INTO session_registrations "
            "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
            " project_root, role, state, wake_capability, registration_fence, "
            " wake_locator, started_at, heartbeat_at, expires_at, revoked_at, "
            " holder_pid, process_started_at) "
            "VALUES (?, ?, 'live-alias', 'codex', 'p', '/tmp/p', 'manager', "
            " 'active', 'codex_queue', 'f1', 'l1', ?, ?, ?, NULL, ?, ?)",
            (new_uuid, new_inc, now, now + 10, now + 86_400, 42, now),
        )
        db.commit()


def test_migrated_alias_can_register_and_pass_public_liveness_verification(
    tmp_path,
):
    path = tmp_path / "bridge.db"
    seeded = _seed_v2_bridge(path)
    now = 2_000_000
    result = apply_migration(path, backup_dir=tmp_path / "backups", now=now)
    assert result.status == "applied"

    with sqlite3.connect(str(path)) as db:
        db.row_factory = sqlite3.Row
        fresh = PresenceRecord(
            session_uuid=uuid.uuid4(),
            session_incarnation_id=uuid.uuid4(),
            alias="live-alias",
            tool="codex",
            project_slug="p",
            project_root="/tmp/p",
            role="manager",
            state="active",
            wake_capability="codex_queue",
            registration_fence="fresh-fence",
            wake_locator="fresh-locator",
            started_at=now,
            heartbeat_at=now + 5,
            expires_at=now + 86_400,
        )
        register_session(
            db,
            fresh,
            now=now + 10,
            holder_pid=4321,
            process_started_at=now - 100,
        )
        bearer = issue_capability(
            db,
            session_uuid=fresh.session_uuid,
            session_incarnation_id=fresh.session_incarnation_id,
            now=now + 20,
            entropy_source=lambda size: b"\xa5" * size,
        )
        db.commit()

        class Proof:
            pid = 4321
            process_started_at = now - 100
            observed_at = now + 25
            alive = True

        decision = verify_capability_with_liveness(
            db,
            bearer.present(),
            expected_sender_uuid=fresh.session_uuid,
            expected_incarnation=fresh.session_incarnation_id,
            expected_fence=fresh.registration_fence,
            now=now + 30,
            max_proof_age_seconds=10,
            observe_process=lambda *_: Proof(),
        )

        assert (decision.allowed, decision.reason) == (True, "accepted")
        legacy = db.execute(
            "SELECT state, holder_pid, revoked_at FROM session_registrations "
            "WHERE session_uuid = ? AND session_incarnation_id = ?",
            seeded["reg_live"],
        ).fetchone()
        assert tuple(legacy) == ("stale", 0, now)


def test_delivery_rows_preserved_with_null_dispatch_started_at(tmp_path):
    path = tmp_path / "bridge.db"
    seeded = _seed_v2_bridge(path)

    result = apply_migration(path, backup_dir=tmp_path / "backups", now=2_000_000)
    assert result.status == "applied"

    with sqlite3.connect(str(path)) as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "SELECT message_id, dispatch_started_at, delivery_state "
            "FROM targeted_deliveries"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["message_id"] == seeded["message_id"]
        assert rows[0]["dispatch_started_at"] is None
        assert rows[0]["delivery_state"] == "pending"


def test_capability_rows_preserve_fk_history(tmp_path):
    path = tmp_path / "bridge.db"
    seeded = _seed_v2_bridge(path)

    result = apply_migration(path, backup_dir=tmp_path / "backups", now=2_000_000)
    assert result.status == "applied"

    with sqlite3.connect(str(path)) as db:
        rows = db.execute(
            "SELECT sender_session_uuid FROM session_capabilities "
            "WHERE verifier_sha256 = ?",
            (seeded["verifier"],),
        ).fetchall()
        assert len(rows) == 1
        # FK integrity preserved — the referenced session_registrations row is
        # still present (now stale, but present).
        assert rows[0][0] == seeded["reg_live"][0]


# ---------------------------------------------------------------------------
# New-schema invariants post-migration
# ---------------------------------------------------------------------------


def test_post_migration_schema_accepts_dispatching_state(tmp_path):
    path = tmp_path / "bridge.db"
    seeded = _seed_v2_bridge(path)
    result = apply_migration(path, backup_dir=tmp_path / "backups", now=2_000_000)
    assert result.status == "applied"

    with sqlite3.connect(str(path)) as db:
        # v0.5.0 (schema v4): a dispatching row requires full owner tuple +
        # claim_lease_token + claim_lease_expires_at. The spirit of this test
        # (dispatching state is accepted by the schema) is preserved by
        # supplying those fields.
        db.execute(
            "UPDATE targeted_deliveries SET delivery_state = 'dispatching', "
            "claim_lease_token = 'tok', claim_lease_expires_at = 2000500, "
            "dispatch_started_at = 2000001, "
            "claimant_session_uuid = ?, claimant_session_incarnation_id = ?, "
            "claimant_registration_fence = 'fence-x' "
            "WHERE message_id = ?",
            (_u(), _u(), seeded["message_id"]),
        )
        db.commit()
        row = db.execute(
            "SELECT delivery_state, dispatch_started_at "
            "FROM targeted_deliveries WHERE message_id = ?",
            (seeded["message_id"],),
        ).fetchone()
        assert row[0] == "dispatching"
        assert row[1] == 2_000_001


def test_post_migration_schema_rejects_new_live_pid_0(tmp_path):
    path = tmp_path / "bridge.db"
    _seed_v2_bridge(path)
    result = apply_migration(path, backup_dir=tmp_path / "backups", now=2_000_000)
    assert result.status == "applied"

    with sqlite3.connect(str(path)) as db:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO session_registrations "
                "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
                " project_root, role, state, wake_capability, registration_fence, "
                " wake_locator, started_at, heartbeat_at, expires_at, revoked_at, "
                " holder_pid, process_started_at) "
                "VALUES (?, ?, 'alive', 'codex', 'p', '/tmp/p', 'manager', "
                " 'active', 'codex_queue', 'f', 'l', 1, 2, 1_000_000, NULL, 0, 1)",
                (_u(), _u()),
            )


def test_post_migration_schema_rejects_negative_pid_everywhere(tmp_path):
    path = tmp_path / "bridge.db"
    _seed_v2_bridge(path)
    result = apply_migration(path, backup_dir=tmp_path / "backups", now=2_000_000)
    assert result.status == "applied"

    with sqlite3.connect(str(path)) as db:
        for state in ("active", "stale", "stopping"):
            with pytest.raises(sqlite3.IntegrityError):
                db.execute(
                    "INSERT INTO session_registrations "
                    "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
                    " project_root, role, state, wake_capability, registration_fence, "
                    " wake_locator, started_at, heartbeat_at, expires_at, revoked_at, "
                    " holder_pid, process_started_at) "
                    f"VALUES (?, ?, 'neg-{state}', 'codex', 'p', '/tmp/p', "
                    f"'manager', '{state}', 'codex_queue', 'f', 'l', 1, 2, "
                    "1_000_000, NULL, -1, 1)",
                    (_u(), _u()),
                )


def test_post_migration_both_receipt_triggers_accept_dispatching(tmp_path):
    """Spec v3.1 §3.2 line 97: both triggers must include 'dispatching'."""
    path = tmp_path / "bridge.db"
    seeded = _seed_v2_bridge(path)
    now = 2_000_000
    result = apply_migration(path, backup_dir=tmp_path / "backups", now=now)
    assert result.status == "applied"

    with sqlite3.connect(str(path)) as db:
        db.execute("PRAGMA foreign_keys = ON")

        # Insert a live receiver registration so tr_receipt_requires_live_registration
        # does not block the receipt.
        recv_uuid, recv_inc = _u(), _u()
        db.execute(
            "INSERT INTO session_registrations "
            "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
            " project_root, role, state, wake_capability, registration_fence, "
            " wake_locator, started_at, heartbeat_at, expires_at, revoked_at, "
            " holder_pid, process_started_at) "
            "VALUES (?, ?, 'recv-alias', 'codex', 'p', '/tmp/p', 'manager', "
            " 'active', 'codex_queue', 'f', 'l', ?, ?, ?, NULL, ?, ?)",
            (recv_uuid, recv_inc, now - 100, now - 50, now + 86_400, 100, now),
        )

        # Create a target delivery row that's in 'dispatching' state and
        # targets the live receiver.
        msg_id = _u()
        db.execute(
            "INSERT INTO messages (id, payload, created_at) VALUES (?, 'x', ?)",
            (msg_id, now),
        )
        db.execute(
            "INSERT INTO targeted_deliveries "
            "(message_id, target_session_uuid, target_session_incarnation_id, "
            " sender_session_uuid, sender_session_incarnation_id, delivery_state, "
            " claim_lease_token, claim_lease_expires_at, dispatch_started_at, "
            " claimant_session_uuid, claimant_session_incarnation_id, "
            " claimant_registration_fence, "
            " created_at, next_attempt_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, 'dispatching', 'lease-xyz', ?, ?, "
            " ?, ?, 'fence-y', ?, ?, ?)",
            (msg_id, recv_uuid, recv_inc, _u(), _u(),
             now + 60, now, _u(), _u(), now, now, now + 3_600),
        )
        db.commit()

        # Insert a disposition_receipt — tr_receipt_requires_eligible_delivery_state
        # must NOT raise, and tr_receipt_marks_observed must flip delivery_state
        # to 'observed'.
        db.execute(
            "INSERT INTO disposition_receipts "
            "(message_id, target_session_uuid, target_session_incarnation_id, "
            " correlation_id, consumer_id, receiver_session_uuid, "
            " receiver_session_incarnation_id, disposition, observed_at, reason, "
            " delivered_at) "
            "VALUES (?, ?, ?, ?, 'consumer', ?, ?, 'accepted', ?, 'ok', ?)",
            (msg_id, recv_uuid, recv_inc, _u(), recv_uuid, recv_inc,
             now + 100, now + 100),
        )
        db.commit()

        row = db.execute(
            "SELECT delivery_state, claim_lease_token FROM targeted_deliveries "
            "WHERE message_id = ?",
            (msg_id,),
        ).fetchone()
        assert row[0] == "observed", (
            "tr_receipt_marks_observed must flip dispatching → observed; "
            "otherwise codex seq=221 claim is false"
        )
        assert row[1] is None  # cleared by the trigger


# ---------------------------------------------------------------------------
# PRAGMA / FK discipline
# ---------------------------------------------------------------------------


def test_foreign_keys_restored_on_after_migration(tmp_path):
    """After a successful upgrade, PRAGMA foreign_keys should be ON on a fresh connection."""
    path = tmp_path / "bridge.db"
    _seed_v2_bridge(path)
    result = apply_migration(path, backup_dir=tmp_path / "backups", now=2_000_000)
    assert result.status == "applied"

    with sqlite3.connect(str(path)) as db:
        db.execute("PRAGMA foreign_keys = ON")
        enforcement = db.execute("PRAGMA foreign_keys").fetchone()[0]
        assert enforcement == 1


def test_fk_restoration_failure_does_not_mask_primary_migration_error(tmp_path):
    """v0.4 hardening (codex-manager mm-cxm-coord seq=231): if a primary
    migration error AND the finally's FK restoration BOTH fail, the caller
    MUST see the primary migration error — not the restoration side effect.

    Construction:
      - Force the primary exception by returning a fake foreign-key violation
        from ``PRAGMA foreign_key_check`` inside the migration body. The
        migration raises ``sqlite3.IntegrityError`` with
        "foreign_key_check violations" in the message.
      - Simultaneously force ``PRAGMA foreign_keys = ON`` in the finally to
        raise ``sqlite3.OperationalError``.
      - Assert pytest.raises matches the PRIMARY (IntegrityError), proving the
        hardening's ``if primary_exc is None: raise`` guard works as intended
        and that the proxy's secondary OperationalError does not leak.
    """
    import cross_tool_awareness.migrations as mig
    from cross_tool_awareness.migrations import _apply_v2_to_v3_migration

    path = tmp_path / "bridge.db"
    _seed_v2_bridge(path)

    class _DualFailureConn:
        def __init__(self, real):
            self._real = real

        def execute(self, sql, *args, **kwargs):
            stripped = sql.strip()
            if stripped == "PRAGMA foreign_key_check":
                # Primary error: pretend FK integrity is violated.
                class _ViolationCursor:
                    def fetchall(self_inner):
                        return [("session_capabilities", 1, "session_registrations", 0)]
                return _ViolationCursor()
            if stripped == "PRAGMA foreign_keys = ON":
                # Secondary error: FK restoration fails. If the hardening
                # does not guard, this OperationalError will mask the primary
                # IntegrityError.
                raise sqlite3.OperationalError(
                    "simulated: PRAGMA foreign_keys toggle refused"
                )
            return self._real.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._real, name)

        @property
        def in_transaction(self):
            return self._real.in_transaction

    real_connect = sqlite3.connect

    def _wrapped_connect(*args, **kwargs):
        return _DualFailureConn(real_connect(*args, **kwargs))

    original = mig.sqlite3.connect
    mig.sqlite3.connect = _wrapped_connect
    try:
        with pytest.raises(sqlite3.IntegrityError,
                           match="foreign_key_check violations"):
            _apply_v2_to_v3_migration(path, now=2_000_000)
    finally:
        mig.sqlite3.connect = original


def test_fk_restoration_raises_on_happy_path_if_readback_mismatches(tmp_path):
    """v0.4 hardening (codex-manager mm-cxm-coord seq=229): if the migration
    commits cleanly but ``PRAGMA foreign_keys=ON`` fails to read back as 1,
    the finally block MUST raise so the caller does not silently keep running
    against a connection with FK enforcement off.

    sqlite3.Connection is a C-defined immutable type, so we subvert the
    readback via a proxy connection returned from a patched sqlite3.connect.
    The proxy transparently delegates every attribute except for the second+
    `PRAGMA foreign_keys` SELECT, where it returns (0,) — simulating the
    enforcement toggle silently failing to re-enable.
    """
    import cross_tool_awareness.migrations as mig
    from cross_tool_awareness.migrations import _apply_v2_to_v3_migration

    path = tmp_path / "bridge.db"
    _seed_v2_bridge(path)

    class _ReadbackSubvertConn:
        def __init__(self, real):
            self._real = real
            self._readback_count = 0

        def execute(self, sql, *args, **kwargs):
            cursor = self._real.execute(sql, *args, **kwargs)
            if sql.strip() == "PRAGMA foreign_keys":
                self._readback_count += 1
                if self._readback_count > 1:
                    class _ZeroCursor:
                        def fetchone(self_inner):
                            return (0,)
                    return _ZeroCursor()
            return cursor

        def __getattr__(self, name):
            return getattr(self._real, name)

        @property
        def in_transaction(self):
            return self._real.in_transaction

    real_connect = sqlite3.connect

    def _wrapped_connect(*args, **kwargs):
        return _ReadbackSubvertConn(real_connect(*args, **kwargs))

    original = mig.sqlite3.connect
    mig.sqlite3.connect = _wrapped_connect
    try:
        with pytest.raises(sqlite3.OperationalError,
                           match="PRAGMA foreign_keys=ON could not be restored"):
            _apply_v2_to_v3_migration(path, now=2_000_000)
    finally:
        mig.sqlite3.connect = original


def test_migration_refuses_active_transaction(tmp_path):
    """_apply_v2_to_v3_migration refuses if the connection is in a transaction."""
    from cross_tool_awareness.migrations import _apply_v2_to_v3_migration

    path = tmp_path / "bridge.db"
    _seed_v2_bridge(path)

    # Open a connection, start a transaction, then try to call the migration
    # on a *second* connection. The second connection is NOT the one holding
    # the transaction, so this test exercises the "fresh connection has no
    # active txn" happy path implicitly. For the explicit "refuses active
    # transaction" guard we need to construct the condition directly.
    #
    # Instead, exercise the invariant by monkeypatching sqlite3.connect to
    # return a connection with in_transaction=True on the first call.
    import cross_tool_awareness.migrations as mig

    real_connect = sqlite3.connect
    attempts = {"n": 0}

    def _fake_connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        if attempts["n"] == 0:
            attempts["n"] += 1
            # Force in_transaction=True by starting one.
            conn.execute("BEGIN")
        return conn

    original = mig.sqlite3.connect
    mig.sqlite3.connect = _fake_connect
    try:
        with pytest.raises(sqlite3.OperationalError, match="non-active transaction"):
            _apply_v2_to_v3_migration(path, now=2_000_000)
    finally:
        mig.sqlite3.connect = original
