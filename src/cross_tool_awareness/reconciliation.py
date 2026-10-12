"""Operator-driven reconciliation for ambiguous and blocked deliveries.

Not automatic. These functions are the only path to return an ambiguous or
blocked delivery to eligibility. See spec §8.
"""

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class ReconciliationResult:
    status: str              # 'reconciled' | 'refused_state_mismatch' | 'not_found'
    new_state: str = ""
    previous_state: str = ""


_AMBIGUOUS_OUTCOMES = {
    "treat_as_delivered":     "notified",
    "treat_as_undelivered":   "pending",
    "treat_as_undeliverable": "undeliverable",
}


_BLOCKED_OUTCOMES = {
    "retry":   "pending",
    "abandon": "undeliverable",
}


def reconcile_ambiguous(
    db: sqlite3.Connection,
    *,
    message_id: str,
    target_session_uuid: str,
    target_session_incarnation_id: str,
    outcome: str,
    now: int,
) -> ReconciliationResult:
    """Operator asserts the true outcome of an 'ambiguous' delivery."""
    if outcome not in _AMBIGUOUS_OUTCOMES:
        raise ValueError(f"invalid ambiguous reconciliation outcome {outcome!r}")
    new_state = _AMBIGUOUS_OUTCOMES[outcome]
    return _apply(
        db,
        message_id=message_id,
        target_session_uuid=target_session_uuid,
        target_session_incarnation_id=target_session_incarnation_id,
        expected_state="ambiguous",
        new_state=new_state,
        now=now,
    )


def reconcile_blocked(
    db: sqlite3.Connection,
    *,
    message_id: str,
    target_session_uuid: str,
    target_session_incarnation_id: str,
    outcome: str,
    now: int,
) -> ReconciliationResult:
    """Operator asserts adapter re-enabled OR delivery should be abandoned."""
    if outcome not in _BLOCKED_OUTCOMES:
        raise ValueError(f"invalid blocked reconciliation outcome {outcome!r}")
    new_state = _BLOCKED_OUTCOMES[outcome]
    return _apply(
        db,
        message_id=message_id,
        target_session_uuid=target_session_uuid,
        target_session_incarnation_id=target_session_incarnation_id,
        expected_state="blocked",
        new_state=new_state,
        now=now,
    )


def _apply(
    db,
    *,
    message_id,
    target_session_uuid,
    target_session_incarnation_id,
    expected_state,
    new_state,
    now,
) -> ReconciliationResult:
    with db:
        db.row_factory = sqlite3.Row
        row = db.execute(
            "SELECT delivery_state FROM targeted_deliveries "
            "WHERE message_id = ? AND target_session_uuid = ? "
            "  AND target_session_incarnation_id = ?",
            (message_id, target_session_uuid, target_session_incarnation_id),
        ).fetchone()
        if row is None:
            return ReconciliationResult(status="not_found")
        if row["delivery_state"] != expected_state:
            return ReconciliationResult(
                status="refused_state_mismatch",
                previous_state=row["delivery_state"],
            )
        db.execute(
            "UPDATE targeted_deliveries "
            "SET delivery_state = ?, next_attempt_at = ?, "
            "    last_error_class = NULL "
            "WHERE message_id = ? AND target_session_uuid = ? "
            "  AND target_session_incarnation_id = ? "
            "  AND delivery_state = ?",
            (new_state, int(now), message_id, target_session_uuid,
             target_session_incarnation_id, expected_state),
        )
    return ReconciliationResult(
        status="reconciled",
        new_state=new_state,
        previous_state=expected_state,
    )
