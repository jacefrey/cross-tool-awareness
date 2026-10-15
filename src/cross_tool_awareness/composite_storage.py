"""Composite-shaped adapter over the package's SQLite primitives.

v0.5.0 (design R5): the facade binds an immutable ``ClaimantIdentity`` and
an injected ``clock`` at construction. Both travel through every write
call to the package. The facade cannot refresh its claimant in place —
after a trusted re-registration or fence change, owner wiring must
construct a NEW facade from a newly verified binding. The old facade
cannot claim or mark under its stale identity; its already-entered
attempt may still finalize against its exact durable dispatch_ownership
witness (authentication done by leases.finalize_attempt).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable

from typing import Optional

from .contracts import (
    ClaimantIdentity,
    DispositionReceipt,
    PresenceRecord,
    QueueAttempt,
    RegistrationSnapshot,
)
from .leases import (
    ClaimedDelivery,
    claim_delivery as _claim_delivery,
    finalize_attempt as _finalize_attempt,
    pre_adapter_mark as _pre_adapter_mark,
    pre_adapter_release as _pre_adapter_release,
)


@dataclass(frozen=True)
class CompositeFinalizationResult:
    """Facade-level finalize result surfacing BOTH the leases status and
    any attached disposition receipt in one object.

    The pre-fix facade collapsed every non-``receipt_already_won`` status
    to ``None``, which forced a CXM-consumer to re-read SQL to tell a
    successful ``finalized`` apart from an ``unauthorized`` rejection or
    an idempotent ``already_finalized``. Returning this dataclass lets
    the caller branch on ``status`` directly.

    Field semantics mirror ``leases.FinalizationResult``, with an extra
    ``receipt`` populated iff ``status == 'receipt_already_won'``.
    """

    status: str
    new_state: Optional[str] = None
    attempt_recorded: bool = False
    receipt: Optional[DispositionReceipt] = None


@dataclass(frozen=True, init=False)
class CompositeStorage:
    """Owner-aware storage facade for the foreground composite.

    Routing (tool + wake_capability) and owner identity (claimant) are both
    bound once at construction and immutable for the facade's lifetime.
    """

    _db: sqlite3.Connection
    tool: str
    wake_capability: str
    claimant: ClaimantIdentity
    _clock: Callable[[], int]

    def __init__(
        self,
        db: sqlite3.Connection,
        *,
        tool: str,
        wake_capability: str,
        claimant: ClaimantIdentity,
        clock: Callable[[], int],
    ) -> None:
        if not isinstance(tool, str) or not tool:
            raise ValueError("tool must be a nonempty string")
        if not isinstance(wake_capability, str) or not wake_capability:
            raise ValueError("wake_capability must be a nonempty string")
        if not isinstance(claimant, ClaimantIdentity):
            raise TypeError("claimant must be a ClaimantIdentity")
        if not callable(clock):
            raise TypeError("clock must be callable")
        object.__setattr__(self, "_db", db)
        object.__setattr__(self, "tool", tool)
        object.__setattr__(self, "wake_capability", wake_capability)
        object.__setattr__(self, "claimant", claimant)
        object.__setattr__(self, "_clock", clock)
        db.row_factory = sqlite3.Row

    def pending_count(self, now: int) -> int:
        """Read-only due-pending count. Ownership-neutral."""
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
        claimant_snapshot: RegistrationSnapshot,
    ) -> ClaimedDelivery | None:
        if (
            isinstance(recovery_after, bool)
            or not isinstance(recovery_after, int)
            or recovery_after <= 0
        ):
            raise ValueError("recovery_after must be a positive int")
        return _claim_delivery(
            self._db,
            now=now,
            lease_ttl_seconds=recovery_after,
            claimant=self.claimant,
            clock=self._clock,
            claimant_snapshot=claimant_snapshot,
            tool=self.tool,
            wake_capability=self.wake_capability,
        )

    def revalidate_claim(
        self,
        claim: ClaimedDelivery,
        now: int,
        *,
        lease_token: str,
    ) -> PresenceRecord | None:
        """Return the current PresenceRecord for the claim's target, iff:

        - this claim's lease is still live under this facade's claimant,
        - the target registration is still live and matches the claim's
          captured target fence,
        - routing still matches the facade's (tool, wake_capability).

        Zero rows → None (caller should release). This is read-only.
        """
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
            "  AND d.claimant_session_uuid = ? "
            "  AND d.claimant_session_incarnation_id = ? "
            "  AND d.claimant_registration_fence = ? "
            "  AND r.revoked_at IS NULL AND r.expires_at > ? "
            "  AND r.state IN ('active', 'busy', 'idle') "
            "  AND r.tool = ? AND r.wake_capability = ?",
            (
                str(claim.message_id),
                str(claim.target_session_uuid),
                str(claim.target_session_incarnation_id),
                lease_token,
                int(now),
                str(self.claimant.session_uuid),
                str(self.claimant.session_incarnation_id),
                self.claimant.registration_fence,
                int(now),
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
        """Release before adapter entry. Raises RuntimeError if the claim's
        lease was lost (concurrent recovery, wrong owner, etc.)."""
        if state not in {"pending", "blocked"}:
            raise ValueError("state must be pending or blocked")
        if not isinstance(reason, str) or not reason:
            raise ValueError("reason must be a nonempty string")
        result = _pre_adapter_release(
            self._db,
            claim_lease_token=lease_token,
            next_state=state,
            diagnostic=reason,
            next_attempt_at=int(self._clock()),
            claimant=self.claimant,
            clock=self._clock,
            message_id=claim.message_id,
            target_session_uuid=claim.target_session_uuid,
            target_session_incarnation_id=claim.target_session_incarnation_id,
        )
        if result.status != "released":
            raise RuntimeError(
                f"claim lease lost before release: {result.status}"
            )

    def pre_adapter_mark(
        self,
        claim: ClaimedDelivery,
        now: int,
        *,
        lease_token: str,
        claimant_snapshot: RegistrationSnapshot,
        target_snapshot: RegistrationSnapshot,
    ) -> bool:
        """Mark the exact owned claim as dispatching AND write the
        dispatch_ownership witness, atomically. False gates the caller from
        invoking the adapter.
        """
        return _pre_adapter_mark(
            self._db,
            message_id=claim.message_id,
            target_session_uuid=claim.target_session_uuid,
            target_session_incarnation_id=claim.target_session_incarnation_id,
            lease_token=lease_token,
            now=now,
            claimant=self.claimant,
            clock=self._clock,
            claimant_snapshot=claimant_snapshot,
            target_snapshot=target_snapshot,
            tool=self.tool,
            wake_capability=self.wake_capability,
        )

    def finalize_attempt(
        self,
        claim: ClaimedDelivery,
        attempt: QueueAttempt,
        *,
        lease_token: str,
    ) -> CompositeFinalizationResult:
        """Finalize an in-flight attempt, surfacing the full status.

        Returns a ``CompositeFinalizationResult``. Possible statuses:
          - ``'finalized'``: attempt recorded + delivery state advanced.
            ``new_state`` carries the new delivery_state. ``receipt`` None.
          - ``'unauthorized'``: wrong owner / wrong token / wrong key.
            Zero rows changed. ``attempt_recorded`` False. ``receipt`` None.
          - ``'already_finalized'``: exact retry of a previously-recorded
            attempt (UNIQUE on claim_lease_token). ``attempt_recorded``
            False (the prior row remains).
          - ``'lease_already_released'``: witness matched but delivery had
            no in-flight state when the UPDATE ran (recovery or operator
            reconciliation after mark).
          - ``'receipt_already_won'``: receipt arrived first; attempt row
            IS recorded (so the attempt history is honest) but the delivery
            remains ``observed``. ``receipt`` is populated with the
            winning receipt.
        """
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
            claimant=self.claimant,
            clock=self._clock,
        )
        receipt: Optional[DispositionReceipt] = None
        if result.status == "receipt_already_won":
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
            if row is not None:
                receipt = _receipt_from_row(row)
        return CompositeFinalizationResult(
            status=result.status,
            new_state=result.new_state,
            attempt_recorded=result.attempt_recorded,
            receipt=receipt,
        )


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
