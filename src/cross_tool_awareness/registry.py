"""Session registration helpers.

register_session() implements the transactional expired-row retirement
required by §3.1: UPDATE expired colliding rows → INSERT new row, both in
one implicit transaction so the partial unique index on alias behaves correctly.

UUIDs are serialized to str at the SQL binding boundary (SQLite stores TEXT).
"""

from __future__ import annotations

import sqlite3
from typing import Optional

from .contracts import PresenceRecord


def register_session(
    db: sqlite3.Connection,
    presence: PresenceRecord,
    *,
    now: int,
    proc_start_ns: Optional[int] = None,
) -> None:
    """Insert a new PresenceRecord, retiring any expired same-alias row first.

    Raises sqlite3.IntegrityError if the alias is held by a LIVE (non-expired,
    non-revoked) different registration.
    """
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
            " revoked_at, proc_start_ns) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)",
            (str(presence.session_uuid), str(presence.session_incarnation_id),
             presence.alias, presence.tool, presence.project_slug,
             presence.project_root, presence.role, presence.state.value,
             presence.wake_capability, presence.registration_fence,
             presence.wake_locator, presence.started_at, presence.heartbeat_at,
             presence.expires_at, proc_start_ns),
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
