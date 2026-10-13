"""Lease primitives: singleton dispatcher lease, per-attempt lease,
fenced idempotent finalization, and pre-adapter release.

See spec §7.
"""

from __future__ import annotations

import secrets
import sqlite3
from dataclasses import dataclass
from typing import Optional
from uuid import UUID


# ---------------------------------------------------------------------------
# Singleton dispatcher lease
# ---------------------------------------------------------------------------

class SingletonDispatcherLease:
    """Enforces at most one live dispatcher process via the dispatcher_lease table."""

    def __init__(self, db: sqlite3.Connection, *, holder_pid: int, holder_proc_start_ns: int):
        self._db = db
        self._holder_pid = int(holder_pid)
        self._holder_proc_start_ns = int(holder_proc_start_ns)
        self._token: Optional[str] = None

    @property
    def token(self) -> Optional[str]:
        return self._token

    def acquire(self, *, now: int, lease_ttl_seconds: int = 60) -> bool:
        """Return True if the lease was acquired; False if another holder is live."""
        new_token = secrets.token_hex(16)
        with self._db:
            self._db.row_factory = sqlite3.Row
            row = self._db.execute(
                "SELECT holder_token, holder_pid, holder_proc_start_ns, expires_at "
                "FROM dispatcher_lease WHERE singleton_pk = 1"
            ).fetchone()
            if row is not None and row["expires_at"] > now:
                return False
            self._db.execute(
                "INSERT INTO dispatcher_lease "
                "(singleton_pk, holder_token, holder_pid, holder_proc_start_ns, "
                " acquired_at, expires_at) VALUES (1, ?, ?, ?, ?, ?) "
                "ON CONFLICT(singleton_pk) DO UPDATE SET "
                "  holder_token = excluded.holder_token, "
                "  holder_pid = excluded.holder_pid, "
                "  holder_proc_start_ns = excluded.holder_proc_start_ns, "
                "  acquired_at = excluded.acquired_at, "
                "  expires_at = excluded.expires_at",
                (new_token, self._holder_pid, self._holder_proc_start_ns,
                 now, now + int(lease_ttl_seconds)),
            )
        self._token = new_token
        return True

    def refresh(self, *, now: int, lease_ttl_seconds: int = 60) -> bool:
        """Extend the lease; return False if another holder stole it (token mismatch)."""
        if self._token is None:
            return False
        with self._db:
            cur = self._db.execute(
                "UPDATE dispatcher_lease SET expires_at = ? "
                "WHERE singleton_pk = 1 AND holder_token = ?",
                (now + int(lease_ttl_seconds), self._token),
            )
            return cur.rowcount == 1

    def release(self) -> None:
        """DELETE the row only if held by this token; otherwise no-op."""
        if self._token is None:
            return
        with self._db:
            self._db.execute(
                "DELETE FROM dispatcher_lease "
                "WHERE singleton_pk = 1 AND holder_token = ?",
                (self._token,),
            )
        self._token = None


# ---------------------------------------------------------------------------
# Per-attempt lease: claim + pre-adapter release + fenced finalization
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ClaimedDelivery:
    message_id: str
    target_session_uuid: str
    target_session_incarnation_id: str
    claim_lease_token: str


@dataclass(frozen=True)
class PreAdapterReleaseResult:
    status: str                    # 'released' | 'lease_released_or_observed'
    diagnostic: Optional[str] = None


@dataclass(frozen=True)
class FinalizationResult:
    status: str                    # 'finalized' | 'already_finalized' | 'lease_already_released' | 'receipt_already_won'
    new_state: Optional[str] = None
    attempt_recorded: bool = False


_OUTCOME_TO_STATE = {
    "accepted_by_cli":   "notified",
    "rejected":          "pending",
    "ambiguous_timeout": "ambiguous",
    "adapter_disabled":  "blocked",
}


def claim_delivery(
    db: sqlite3.Connection,
    *,
    now: int,
    lease_ttl_seconds: int = 15,
) -> Optional[ClaimedDelivery]:
    """Claim one due pending row or recover one expired claimed row.

    ``dispatching`` rows are never eligible, regardless of age.
    """
    token = secrets.token_hex(16)
    with db:
        db.row_factory = sqlite3.Row
        row = db.execute(
            "SELECT message_id, target_session_uuid, target_session_incarnation_id, "
            "       delivery_state, claim_lease_token "
            "FROM targeted_deliveries "
            "WHERE (delivery_state = 'pending' AND next_attempt_at <= ?) "
            "   OR (delivery_state = 'claimed' AND claim_lease_expires_at <= ?) "
            "ORDER BY CASE WHEN delivery_state = 'pending' THEN next_attempt_at "
            "              ELSE claim_lease_expires_at END ASC "
            "LIMIT 1",
            (now, now),
        ).fetchone()
        if row is None:
            return None
        cur = db.execute(
            "UPDATE targeted_deliveries "
            "SET delivery_state = 'claimed', claim_lease_token = ?, "
            "    claim_lease_expires_at = ? "
            "WHERE message_id = ? AND target_session_uuid = ? "
            "  AND target_session_incarnation_id = ? "
            "  AND ((delivery_state = 'pending') "
            "       OR (delivery_state = 'claimed' "
            "           AND claim_lease_token = ? "
            "           AND claim_lease_expires_at <= ?))",
            (token, now + int(lease_ttl_seconds),
             row["message_id"], row["target_session_uuid"],
             row["target_session_incarnation_id"],
             row["claim_lease_token"], now),
        )
        if cur.rowcount == 0:
            return None
    return ClaimedDelivery(
        message_id=row["message_id"],
        target_session_uuid=row["target_session_uuid"],
        target_session_incarnation_id=row["target_session_incarnation_id"],
        claim_lease_token=token,
    )


def pre_adapter_mark(
    db: sqlite3.Connection,
    *,
    message_id: UUID,
    target_session_uuid: UUID,
    target_session_incarnation_id: UUID,
    lease_token: str,
    now: int,
) -> bool:
    """Transition the exact leased claim to ``dispatching`` at ``now``.

    Returns False when the exact claim and token are no longer in the claimed
    state. The caller must not invoke the adapter after a False result.
    """
    with db:
        cursor = db.execute(
            "UPDATE targeted_deliveries "
            "SET delivery_state = 'dispatching', dispatch_started_at = ? "
            "WHERE message_id = ? AND target_session_uuid = ? "
            "  AND target_session_incarnation_id = ? "
            "  AND delivery_state = 'claimed' AND claim_lease_token = ?",
            (
                int(now),
                str(message_id),
                str(target_session_uuid),
                str(target_session_incarnation_id),
                lease_token,
            ),
        )
        return cursor.rowcount == 1


def pre_adapter_release(
    db: sqlite3.Connection,
    *,
    claim_lease_token: str,
    next_state: str,
    diagnostic: str,
    next_attempt_at: int,
) -> PreAdapterReleaseResult:
    """Release a claim BEFORE calling adapter.wake().

    Does NOT create a queue_attempts row. Does NOT increment attempt_count.
    Used when target revalidation fails (next_state='pending') or adapter
    missing/not-enabled (next_state='blocked').
    """
    assert next_state in ("pending", "blocked"), f"invalid pre-adapter next_state {next_state!r}"
    with db:
        cur = db.execute(
            "UPDATE targeted_deliveries "
            "SET delivery_state = ?, claim_lease_token = NULL, "
            "    claim_lease_expires_at = NULL, next_attempt_at = ?, "
            "    last_error_class = ? "
            "WHERE claim_lease_token = ? "
            "  AND delivery_state = 'claimed'",
            (next_state, int(next_attempt_at), diagnostic, claim_lease_token),
        )
        if cur.rowcount == 0:
            return PreAdapterReleaseResult(status="lease_released_or_observed")
    return PreAdapterReleaseResult(status="released", diagnostic=diagnostic)


def finalize_attempt(
    db: sqlite3.Connection,
    *,
    claim_lease_token: str,
    message_id: str,
    target_session_uuid: str,
    target_session_incarnation_id: str,
    outcome: str,
    reason: str,
    attempted_at: int,
    now: int,
    next_attempt_at: int,
) -> FinalizationResult:
    """Create a QueueAttempt (UNIQUE on claim_lease_token) + fenced delivery advance.

    Called ONLY after adapter.wake() was actually invoked.
    """
    if outcome not in _OUTCOME_TO_STATE:
        raise ValueError(f"invalid outcome {outcome!r}")
    new_state = _OUTCOME_TO_STATE[outcome]

    with db:
        try:
            db.execute(
                "INSERT INTO queue_attempts "
                "(claim_lease_token, message_id, target_session_uuid, "
                " target_session_incarnation_id, outcome, attempted_at, "
                " finished_at, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (claim_lease_token, message_id, target_session_uuid,
                 target_session_incarnation_id, outcome, int(attempted_at),
                 int(now), reason),
            )
        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint failed: queue_attempts.claim_lease_token" in str(e):
                return FinalizationResult(status="already_finalized", attempt_recorded=False)
            raise

        cur = db.execute(
            "UPDATE targeted_deliveries "
            "SET delivery_state = ?, claim_lease_token = NULL, "
            "    claim_lease_expires_at = NULL, "
            "    attempt_count = attempt_count + 1, "
            "    next_attempt_at = ?, last_error_class = ? "
            "WHERE message_id = ? AND target_session_uuid = ? "
            "  AND target_session_incarnation_id = ? "
            "  AND claim_lease_token = ? "
            "  AND delivery_state NOT IN ('observed', 'undeliverable')",
            (new_state, int(next_attempt_at),
             outcome if outcome != "accepted_by_cli" else None,
             message_id, target_session_uuid, target_session_incarnation_id,
             claim_lease_token),
        )
        if cur.rowcount == 0:
            current_state_row = db.execute(
                "SELECT delivery_state FROM targeted_deliveries "
                "WHERE message_id = ? AND target_session_uuid = ? "
                "  AND target_session_incarnation_id = ?",
                (message_id, target_session_uuid, target_session_incarnation_id),
            ).fetchone()
            current_state = current_state_row[0] if current_state_row else None
            if current_state == "observed":
                return FinalizationResult(
                    status="receipt_already_won",
                    attempt_recorded=True,
                )
            return FinalizationResult(
                status="lease_already_released",
                attempt_recorded=True,
            )

    return FinalizationResult(
        status="finalized",
        new_state=new_state,
        attempt_recorded=True,
    )
