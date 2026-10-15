"""Shared owner-aware fixtures (v0.5.1 remediation plan, Shared fixtures section).

Snapshots are built FROM the registered row so the only thing a test varies
deliberately is proof_observed_at (and any field it mutates on purpose
afterwards). Process children are run under one shared deadline with one
bounded cleanup phase.
"""
from __future__ import annotations

import sqlite3
import time
import uuid
from pathlib import Path
from uuid import UUID, uuid4

from cross_tool_awareness import (
    ClaimantIdentity,
    PresenceRecord,
    RegistrationSnapshot,
    apply as apply_migration,
)

STUB_MESSAGES_DDL = (
    "CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, "
    "payload TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL DEFAULT 0);"
)
CHILD_DEADLINE = 10.0
CLEANUP = 1.0


def fresh_db_path(tmp_path: Path) -> Path:
    p = tmp_path / "bridge.db"
    (tmp_path / "backups").mkdir(exist_ok=True)
    c = sqlite3.connect(str(p))
    c.executescript(STUB_MESSAGES_DDL)
    c.commit()
    c.close()
    r = apply_migration(p, backup_dir=tmp_path / "backups", now=1)
    assert r.status == "applied", r
    return p


def open_db(p: Path, *, isolation_level=None, timeout: float = 10.0,
            factory=sqlite3.Connection) -> sqlite3.Connection:
    db = sqlite3.connect(str(p), isolation_level=isolation_level, timeout=timeout,
                         factory=factory)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


def identity(fence: str = "f1") -> ClaimantIdentity:
    return ClaimantIdentity(session_uuid=uuid4(), session_incarnation_id=uuid4(),
                            registration_fence=fence)


def register(db, cid: ClaimantIdentity, *, now: int = 10, holder_pid: int = 1234,
             process_started_at: int = 1_000_000, tool: str = "claude",
             wake_capability: str = "notify_only", state: str = "active") -> None:
    db.execute(
        "INSERT INTO session_registrations (session_uuid, session_incarnation_id, alias, tool,"
        " project_slug, project_root, role, state, wake_capability, registration_fence,"
        " wake_locator, started_at, heartbeat_at, expires_at, revoked_at, holder_pid,"
        " process_started_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (str(cid.session_uuid), str(cid.session_incarnation_id),
         f"a-{str(cid.session_uuid)[:8]}", tool, "p", "/tmp/p", "worker", state,
         wake_capability, cid.registration_fence, "x", now - 100, now, now + 86400, None,
         holder_pid, process_started_at),
    )


def snapshot_from_row(db, cid: ClaimantIdentity, *, proof_observed_at: int) -> RegistrationSnapshot:
    cur = db.cursor()
    cur.row_factory = sqlite3.Row
    r = cur.execute(
        "SELECT * FROM session_registrations WHERE session_uuid=? AND session_incarnation_id=?",
        (str(cid.session_uuid), str(cid.session_incarnation_id)),
    ).fetchone()
    assert r is not None, "snapshot_from_row needs a registered row"
    presence = PresenceRecord(
        session_uuid=UUID(r["session_uuid"]),
        session_incarnation_id=UUID(r["session_incarnation_id"]),
        alias=r["alias"], tool=r["tool"], project_slug=r["project_slug"],
        project_root=r["project_root"], role=r["role"], state=r["state"],
        wake_capability=r["wake_capability"], registration_fence=r["registration_fence"],
        wake_locator=r["wake_locator"], started_at=r["started_at"],
        heartbeat_at=r["heartbeat_at"], expires_at=r["expires_at"],
    )
    return RegistrationSnapshot(presence=presence, holder_pid=r["holder_pid"],
                                process_started_at=r["process_started_at"],
                                proof_observed_at=proof_observed_at)


def seed_pending(db, target: ClaimantIdentity, *, now: int = 10,
                 next_attempt_at: int | None = None, expires_at: int | None = None) -> str:
    mid = str(uuid.uuid4())
    db.execute("INSERT INTO messages(id, payload, created_at) VALUES (?,?,?)", (mid, "{}", now))
    db.execute(
        "INSERT INTO targeted_deliveries (message_id, target_session_uuid,"
        " target_session_incarnation_id, sender_session_uuid, sender_session_incarnation_id,"
        " delivery_state, created_at, next_attempt_at, expires_at) VALUES (?,?,?,?,?,?,?,?,?)",
        (mid, str(target.session_uuid), str(target.session_incarnation_id), str(uuid4()),
         str(uuid4()), "pending", now, now if next_attempt_at is None else next_attempt_at,
         now + 3600 if expires_at is None else expires_at),
    )
    return mid


def row(db, mid: str) -> dict:
    cur = db.cursor()
    cur.row_factory = sqlite3.Row
    return dict(cur.execute("SELECT * FROM targeted_deliveries WHERE message_id=?", (mid,)).fetchone())


def counts(db) -> tuple[int, int]:
    return (db.execute("SELECT COUNT(*) FROM dispatch_ownership").fetchone()[0],
            db.execute("SELECT COUNT(*) FROM queue_attempts").fetchone()[0])


def run_children(ctx, children, *, deadline: float = CHILD_DEADLINE, cleanup: float = CLEANUP,
                 expected_exitcodes=(0,)):
    """Start every (target, args); wait on one shared deadline; one bounded cleanup phase
    kills and reaps every survivor; always returns the processes; asserts exit codes unless
    expected_exitcodes is None."""
    procs = [ctx.Process(target=t, args=a, daemon=True) for t, a in children]
    try:
        for p in procs:
            p.start()
        end = time.monotonic() + deadline
        for p in procs:
            p.join(max(0.0, end - time.monotonic()))
    finally:
        survivors = [p for p in procs if p.is_alive()]
        for p in survivors:
            p.kill()
        end = time.monotonic() + cleanup
        for p in survivors:
            p.join(max(0.0, end - time.monotonic()))
        for p in procs:
            assert p.exitcode is not None, "every child must exit or be reaped"
    if expected_exitcodes is not None:
        assert all(p.exitcode in expected_exitcodes for p in procs), [p.exitcode for p in procs]
    return procs
