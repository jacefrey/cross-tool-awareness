"""Strict v0.4 session-registration API tests."""

from __future__ import annotations

import inspect
import sqlite3
import uuid

import pytest

from cross_tool_awareness.registry import register_session


def test_register_session_requires_process_identity(db, make_presence_fn):
    presence = make_presence_fn()

    signature = inspect.signature(register_session)
    assert signature.parameters["holder_pid"].default is inspect.Parameter.empty
    assert signature.parameters["process_started_at"].default is inspect.Parameter.empty

    with pytest.raises(TypeError):
        register_session(db, presence, now=1_000_000)

    assert db.execute("SELECT COUNT(*) FROM session_registrations").fetchone()[0] == 0


@pytest.mark.parametrize("holder_pid", [None, 0, -1, True, 1.5, "123"])
def test_register_session_rejects_invalid_holder_pid(
    db, make_presence_fn, holder_pid
):
    with pytest.raises(ValueError, match="holder_pid must be a positive integer"):
        register_session(
            db,
            make_presence_fn(),
            now=1_000_000,
            holder_pid=holder_pid,
            process_started_at=999_900,
        )

    assert db.execute("SELECT COUNT(*) FROM session_registrations").fetchone()[0] == 0


@pytest.mark.parametrize("process_started_at", [None, True, 1.5, "999900"])
def test_register_session_rejects_invalid_process_started_at(
    db, make_presence_fn, process_started_at
):
    with pytest.raises(
        ValueError, match="process_started_at must be integer Unix seconds"
    ):
        register_session(
            db,
            make_presence_fn(),
            now=1_000_000,
            holder_pid=123,
            process_started_at=process_started_at,
        )

    assert db.execute("SELECT COUNT(*) FROM session_registrations").fetchone()[0] == 0


def test_register_session_persists_process_identity_atomically(db, make_presence_fn):
    presence = make_presence_fn()

    register_session(
        db,
        presence,
        now=1_000_000,
        holder_pid=4321,
        process_started_at=999_900,
    )

    row = db.execute(
        "SELECT holder_pid, process_started_at FROM session_registrations "
        "WHERE session_uuid = ? AND session_incarnation_id = ?",
        (str(presence.session_uuid), str(presence.session_incarnation_id)),
    ).fetchone()
    assert dict(row) == {"holder_pid": 4321, "process_started_at": 999_900}


def test_register_session_supersedes_legacy_history_with_fresh_incarnation(
    db, make_presence_fn
):
    session_uuid = uuid.uuid4()
    legacy_incarnation = uuid.uuid4()
    fresh_incarnation = uuid.uuid4()
    alias = "migrated-alias"
    db.execute(
        "INSERT INTO session_registrations "
        "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
        " project_root, role, state, wake_capability, registration_fence, "
        " wake_locator, started_at, heartbeat_at, expires_at, revoked_at, "
        " holder_pid, process_started_at) "
        "VALUES (?, ?, ?, 'codex', 'test-project', '/tmp/test-project', "
        " 'manager', 'stale', 'codex_queue', 'legacy-fence', 'legacy-locator', "
        " 900000, 900010, 900100, 900100, 0, 899900)",
        (str(session_uuid), str(legacy_incarnation), alias),
    )
    presence = make_presence_fn(
        session_uuid=session_uuid,
        session_incarnation_id=fresh_incarnation,
        alias=alias,
    )

    register_session(
        db,
        presence,
        now=1_000_000,
        holder_pid=4321,
        process_started_at=999_900,
    )

    rows = db.execute(
        "SELECT session_incarnation_id, state, revoked_at, holder_pid, "
        "process_started_at FROM session_registrations "
        "WHERE session_uuid = ? ORDER BY started_at",
        (str(session_uuid),),
    ).fetchall()
    assert [dict(row) for row in rows] == [
        {
            "session_incarnation_id": str(legacy_incarnation),
            "state": "stale",
            "revoked_at": 900100,
            "holder_pid": 0,
            "process_started_at": 899900,
        },
        {
            "session_incarnation_id": str(fresh_incarnation),
            "state": "idle",
            "revoked_at": None,
            "holder_pid": 4321,
            "process_started_at": 999900,
        },
    ]

    db.execute(
        "UPDATE session_registrations SET revoked_at = 1_000_001 "
        "WHERE session_uuid = ? AND session_incarnation_id = ?",
        (str(session_uuid), str(fresh_incarnation)),
    )
    reused_legacy_identity = make_presence_fn(
        session_uuid=session_uuid,
        session_incarnation_id=legacy_incarnation,
        alias=alias,
    )
    with pytest.raises(sqlite3.IntegrityError):
        register_session(
            db,
            reused_legacy_identity,
            now=1_000_002,
            holder_pid=4321,
            process_started_at=999_900,
        )
