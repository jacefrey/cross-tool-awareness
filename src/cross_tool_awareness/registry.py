"""Session registration helpers.

register_session() implements the transactional expired-row retirement
required by §3.1: UPDATE expired colliding rows → INSERT new row, both in
one implicit transaction so the partial unique index on alias behaves correctly.

UUIDs are serialized to str at the SQL binding boundary (SQLite stores TEXT).
"""

from __future__ import annotations

import sqlite3

from .contracts import PresenceRecord, _unix_seconds


def register_session(
    db: sqlite3.Connection,
    presence: PresenceRecord,
    *,
    now: int,
    holder_pid: int,
    process_started_at: int,
) -> None:
    """Insert a new PresenceRecord, retiring any expired same-alias row first.

    Raises sqlite3.IntegrityError if the alias is held by a LIVE (non-expired,
    non-revoked) different registration.

    ``holder_pid`` must be a positive, non-bool integer. The process start
    time uses the package's strict Unix-second validation. Both values are
    persisted atomically with the registration row.
    """
    if (
        isinstance(holder_pid, bool)
        or not isinstance(holder_pid, int)
        or holder_pid <= 0
    ):
        raise ValueError("holder_pid must be a positive integer")
    process_started_at = _unix_seconds(process_started_at, "process_started_at")
    with db:
        db.execute(
            "UPDATE session_registrations SET revoked_at = ? "
            "WHERE alias = ? AND revoked_at IS NULL AND expires_at <= ?",
            (now, presence.alias, now),
        )
        db.execute(
            "INSERT INTO session_registrations "
            "(session_uuid, session_incarnation_id, alias, tool, project_slug, "
            " project_root, role, state, wake_capability, registration_fence, "
            " wake_locator, started_at, heartbeat_at, expires_at, "
            " revoked_at, holder_pid, process_started_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
            (str(presence.session_uuid), str(presence.session_incarnation_id),
             presence.alias, presence.tool, presence.project_slug,
             presence.project_root, presence.role, presence.state.value,
             presence.wake_capability, presence.registration_fence,
             presence.wake_locator, presence.started_at, presence.heartbeat_at,
             presence.expires_at, holder_pid, process_started_at),
        )


def revoke_session(
    db: sqlite3.Connection,
    *,
    session_uuid,
    session_incarnation_id,
    now: int,
) -> None:
    """Mark a registration revoked. Idempotent."""
    with db:
        db.execute(
            "UPDATE session_registrations SET revoked_at = ? "
            "WHERE session_uuid = ? AND session_incarnation_id = ? "
            "  AND revoked_at IS NULL",
            (now, str(session_uuid), str(session_incarnation_id)),
        )
