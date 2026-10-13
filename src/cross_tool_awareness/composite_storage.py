"""Composite-shaped adapter over the package's SQLite primitives."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

from .contracts import (
    DispositionReceipt,
    PresenceRecord,
    QueueAttempt,
)
from .leases import (
    ClaimedDelivery,
    claim_delivery as _claim_delivery,
    finalize_attempt as _finalize_attempt,
    pre_adapter_mark as _pre_adapter_mark,
)


@dataclass(frozen=True, init=False)
class CompositeStorage:
    """Exact six-method storage facade for the foreground composite.

    Routing is bound once at construction. Every write is fenced by the full
    delivery key, current state, and caller-supplied lease token.
    """

    _db: sqlite3.Connection
    tool: str
    wake_capability: str
    _claim_fences: dict[str, str] = field(repr=False, compare=False)

    def __init__(
        self,
        db: sqlite3.Connection,
        *,
        tool: str,
        wake_capability: str,
    ) -> None:
        if not isinstance(tool, str) or not tool:
            raise ValueError("tool must be a nonempty string")
        if not isinstance(wake_capability, str) or not wake_capability:
            raise ValueError("wake_capability must be a nonempty string")
        object.__setattr__(self, "_db", db)
        object.__setattr__(self, "tool", tool)
        object.__setattr__(self, "wake_capability", wake_capability)
        object.__setattr__(self, "_claim_fences", {})
        db.row_factory = sqlite3.Row

    def pending_count(self, now: int) -> int:
        row = self._db.execute(
            "SELECT COUNT(*) FROM targeted_deliveries "
            "WHERE delivery_state = 'pending' AND next_attempt_at <= ?",
            (int(now),),
        ).fetchone()
        return int(row[0])

    def claim_delivery(
        self,
        now: int,
        *,
        recovery_after: int,
    ) -> ClaimedDelivery | None:
        if (
            isinstance(recovery_after, bool)
            or not isinstance(recovery_after, int)
            or recovery_after <= 0
        ):
            raise ValueError("recovery_after must be a positive int")
        claim = _claim_delivery(
            self._db,
            now=now,
            lease_ttl_seconds=recovery_after,
        )
        if claim is not None:
            row = self._db.execute(
                "SELECT registration_fence FROM session_registrations "
                "WHERE session_uuid = ? AND session_incarnation_id = ?",
                (
                    str(claim.target_session_uuid),
                    str(claim.target_session_incarnation_id),
                ),
            ).fetchone()
            if row is not None:
                self._claim_fences[claim.claim_lease_token] = row[0]
        return claim

    def revalidate_claim(
        self,
        claim: ClaimedDelivery,
        now: int,
        *,
        lease_token: str,
    ) -> PresenceRecord | None:
        expected_fence = self._claim_fences.get(lease_token)
        if expected_fence is None:
            return None
        row = self._db.execute(
            "SELECT r.* FROM session_registrations AS r "
            "JOIN targeted_deliveries AS d "
            "  ON d.target_session_uuid = r.session_uuid "
            " AND d.target_session_incarnation_id = r.session_incarnation_id "
            "WHERE d.message_id = ? "
            "  AND d.target_session_uuid = ? "
            "  AND d.target_session_incarnation_id = ? "
            "  AND d.delivery_state = 'claimed' "
            "  AND d.claim_lease_token = ? "
            "  AND d.claim_lease_expires_at > ? "
            "  AND r.revoked_at IS NULL AND r.expires_at > ? "
            "  AND r.registration_fence = ? "
            "  AND r.state IN ('active', 'busy', 'idle') "
            "  AND r.tool = ? AND r.wake_capability = ?",
            (
                str(claim.message_id),
                str(claim.target_session_uuid),
                str(claim.target_session_incarnation_id),
                lease_token,
                int(now),
                int(now),
                expected_fence,
                self.tool,
                self.wake_capability,
            ),
        ).fetchone()
        if row is None:
            return None
        return _presence_from_row(row)

    def release_claim(
        self,
        claim: ClaimedDelivery,
        state: str,
        reason: str,
        *,
        lease_token: str,
    ) -> None:
        if state not in {"pending", "blocked"}:
            raise ValueError("state must be pending or blocked")
        if not isinstance(reason, str) or not reason:
            raise ValueError("reason must be a nonempty string")
        with self._db:
            cursor = self._db.execute(
                "UPDATE targeted_deliveries "
                "SET delivery_state = ?, claim_lease_token = NULL, "
                "    claim_lease_expires_at = NULL, last_error_class = ? "
                "WHERE message_id = ? AND target_session_uuid = ? "
                "  AND target_session_incarnation_id = ? "
                "  AND delivery_state = 'claimed' "
                "  AND claim_lease_token = ?",
                (
                    state,
                    reason,
                    str(claim.message_id),
                    str(claim.target_session_uuid),
                    str(claim.target_session_incarnation_id),
                    lease_token,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("claim lease lost before release")
        self._claim_fences.pop(lease_token, None)

    def pre_adapter_mark(
        self,
        claim: ClaimedDelivery,
        now: int,
        *,
        lease_token: str,
    ) -> bool:
        return _pre_adapter_mark(
            self._db,
            message_id=claim.message_id,
            target_session_uuid=claim.target_session_uuid,
            target_session_incarnation_id=claim.target_session_incarnation_id,
            lease_token=lease_token,
            now=now,
        )

    def finalize_attempt(
        self,
        claim: ClaimedDelivery,
        attempt: QueueAttempt,
        *,
        lease_token: str,
    ) -> DispositionReceipt | None:
        if (
            str(attempt.message_id) != str(claim.message_id)
            or str(attempt.target_session_uuid)
            != str(claim.target_session_uuid)
            or str(attempt.target_session_incarnation_id)
            != str(claim.target_session_incarnation_id)
        ):
            raise ValueError("attempt identity does not match claim")
        result = _finalize_attempt(
            self._db,
            claim_lease_token=lease_token,
            message_id=str(claim.message_id),
            target_session_uuid=str(claim.target_session_uuid),
            target_session_incarnation_id=str(
                claim.target_session_incarnation_id
            ),
            outcome=attempt.outcome.value,
            reason=attempt.reason,
            attempted_at=attempt.attempted_at,
            now=attempt.finished_at,
            next_attempt_at=attempt.finished_at,
        )
        self._claim_fences.pop(lease_token, None)
        if result.status != "receipt_already_won":
            return None
        row = self._db.execute(
            "SELECT * FROM disposition_receipts "
            "WHERE message_id = ? AND target_session_uuid = ? "
            "  AND target_session_incarnation_id = ?",
            (
                str(claim.message_id),
                str(claim.target_session_uuid),
                str(claim.target_session_incarnation_id),
            ),
        ).fetchone()
        return _receipt_from_row(row) if row is not None else None


def _presence_from_row(row: sqlite3.Row) -> PresenceRecord:
    return PresenceRecord(
        session_uuid=row["session_uuid"],
        session_incarnation_id=row["session_incarnation_id"],
        alias=row["alias"],
        tool=row["tool"],
        project_slug=row["project_slug"],
        project_root=row["project_root"],
        role=row["role"],
        state=row["state"],
        wake_capability=row["wake_capability"],
        registration_fence=row["registration_fence"],
        wake_locator=row["wake_locator"],
        started_at=row["started_at"],
        heartbeat_at=row["heartbeat_at"],
        expires_at=row["expires_at"],
    )


def _receipt_from_row(row: sqlite3.Row) -> DispositionReceipt:
    return DispositionReceipt(
        message_id=row["message_id"],
        correlation_id=row["correlation_id"],
        consumer_id=row["consumer_id"],
        receiver_session_uuid=row["receiver_session_uuid"],
        receiver_session_incarnation_id=row[
            "receiver_session_incarnation_id"
        ],
        disposition=row["disposition"],
        observed_at=row["observed_at"],
        reason=row["reason"],
    )
