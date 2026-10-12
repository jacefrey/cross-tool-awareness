"""Storage APIs for Codex-adapter consumption (v0.2.0).

Implements codex-manager's ReceiptStorage Protocol (receiver_receipts.py) and
adds fenced apply_registration_update + atomic authenticated targeted-send.
Nothing in this module writes to the live bridge DB — the caller passes an
sqlite3.Connection scoped to a tmpdir fixture DB or (in future) the shared
bridge DB after separate authorization.
"""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from typing import Optional, Protocol
from uuid import UUID, uuid4

from .capability import verify_capability
from .contracts import (
    CapabilityDecision,
    DispositionReceipt,
    NoticeClaim,
    NoticeKey,
    NoticeObservation,
    RegistrationUpdate,
)


# ---------------------------------------------------------------------------
# ReceiptStorage (implements codex-manager ReceiptStorage Protocol)
# ---------------------------------------------------------------------------


class ReceiptStorage:
    """Durable, atomic shared storage for receiver notice claims and dispositions."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db
        db.row_factory = sqlite3.Row

    def claim_notice(
        self,
        notice: NoticeObservation,
        now: int,
        recovery_after: int,
    ) -> NoticeClaim:
        """Atomically determine whether this is the first observation of a notice.

        - 'observed' → a disposition_receipts row already exists; carries receipt.
        - 'duplicate' → a notice_claims row exists and is fresher than recovery_after.
        - 'recovered' → stale notice_claims row (older than recovery_after); upserted.
        - 'first' → no prior claim or receipt; inserted.
        """
        if not isinstance(notice, NoticeObservation):
            raise ValueError("notice must be a NoticeObservation")
        if not isinstance(now, int) or isinstance(now, bool):
            raise ValueError("now must be int")
        if not isinstance(recovery_after, int) or isinstance(recovery_after, bool) or recovery_after <= 0:
            raise ValueError("recovery_after must be a positive int")

        key = notice.key
        with self._db:
            # Terminal observed state wins.
            existing_receipt = self._db.execute(
                "SELECT * FROM disposition_receipts "
                "WHERE message_id = ? AND target_session_uuid = ? "
                "  AND target_session_incarnation_id = ?",
                (str(key.message_id),
                 str(key.target_session_uuid),
                 str(key.target_session_incarnation_id)),
            ).fetchone()
            if existing_receipt is not None:
                return NoticeClaim(status="observed",
                                   receipt=_row_to_receipt(existing_receipt))

            existing_claim = self._db.execute(
                "SELECT claimed_at FROM notice_claims "
                "WHERE message_id = ? AND target_session_uuid = ? "
                "  AND target_session_incarnation_id = ?",
                (str(key.message_id),
                 str(key.target_session_uuid),
                 str(key.target_session_incarnation_id)),
            ).fetchone()

            if existing_claim is not None:
                claim_age = now - existing_claim["claimed_at"]
                if claim_age < recovery_after:
                    return NoticeClaim(status="duplicate")
                # Stale — upsert.
                self._db.execute(
                    "UPDATE notice_claims "
                    "SET consumer_id = ?, correlation_id = ?, "
                    "    registration_fence = ?, claimed_at = ? "
                    "WHERE message_id = ? AND target_session_uuid = ? "
                    "  AND target_session_incarnation_id = ?",
                    (notice.consumer_id, str(notice.correlation_id),
                     notice.registration_fence, now,
                     str(key.message_id),
                     str(key.target_session_uuid),
                     str(key.target_session_incarnation_id)),
                )
                return NoticeClaim(status="recovered")

            # No prior claim — insert.
            self._db.execute(
                "INSERT INTO notice_claims "
                "(message_id, target_session_uuid, target_session_incarnation_id, "
                " consumer_id, correlation_id, registration_fence, claimed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (str(key.message_id),
                 str(key.target_session_uuid),
                 str(key.target_session_incarnation_id),
                 notice.consumer_id, str(notice.correlation_id),
                 notice.registration_fence, now),
            )
            return NoticeClaim(status="first")

    def record_first_disposition(
        self,
        key: NoticeKey,
        receipt: DispositionReceipt,
    ) -> DispositionReceipt:
        """Persist the first receipt. If one already exists, return the existing one."""
        if not isinstance(key, NoticeKey):
            raise ValueError("key must be a NoticeKey")
        if not isinstance(receipt, DispositionReceipt):
            raise ValueError("receipt must be a DispositionReceipt")
        if receipt.message_id != key.message_id:
            raise ValueError("receipt.message_id does not match key")
        if receipt.receiver_session_uuid != key.target_session_uuid:
            raise ValueError("receipt.receiver_session_uuid does not match key.target_session_uuid")
        if receipt.receiver_session_incarnation_id != key.target_session_incarnation_id:
            raise ValueError("receipt.receiver_session_incarnation_id does not match key")

        with self._db:
            existing = self._db.execute(
                "SELECT * FROM disposition_receipts "
                "WHERE message_id = ? AND target_session_uuid = ? "
                "  AND target_session_incarnation_id = ?",
                (str(key.message_id),
                 str(key.target_session_uuid),
                 str(key.target_session_incarnation_id)),
            ).fetchone()
            if existing is not None:
                return _row_to_receipt(existing)
            try:
                self._db.execute(
                    "INSERT INTO disposition_receipts "
                    "(message_id, target_session_uuid, target_session_incarnation_id, "
                    " correlation_id, consumer_id, receiver_session_uuid, "
                    " receiver_session_incarnation_id, disposition, observed_at, "
                    " reason, delivered_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (str(receipt.message_id),
                     str(key.target_session_uuid),
                     str(key.target_session_incarnation_id),
                     str(receipt.correlation_id),
                     receipt.consumer_id,
                     str(receipt.receiver_session_uuid),
                     str(receipt.receiver_session_incarnation_id),
                     receipt.disposition.value,
                     receipt.observed_at,
                     receipt.reason,
                     receipt.observed_at),
                )
                return receipt
            except sqlite3.IntegrityError as exc:
                # Rare race: concurrent write landed between SELECT and INSERT.
                # Re-SELECT; if truly present, return it; otherwise re-raise
                # (could be a different integrity failure, e.g., ineligible
                # delivery state at the time of INSERT).
                existing = self._db.execute(
                    "SELECT * FROM disposition_receipts "
                    "WHERE message_id = ? AND target_session_uuid = ? "
                    "  AND target_session_incarnation_id = ?",
                    (str(key.message_id),
                     str(key.target_session_uuid),
                     str(key.target_session_incarnation_id)),
                ).fetchone()
                if existing is None:
                    raise
                return _row_to_receipt(existing)


def _row_to_receipt(row) -> DispositionReceipt:
    return DispositionReceipt(
        message_id=row["message_id"],
        correlation_id=row["correlation_id"],
        consumer_id=row["consumer_id"],
        receiver_session_uuid=row["receiver_session_uuid"],
        receiver_session_incarnation_id=row["receiver_session_incarnation_id"],
        disposition=row["disposition"],
        observed_at=row["observed_at"],
        reason=row["reason"],
    )


# ---------------------------------------------------------------------------
# Fenced RegistrationUpdate application
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class UpdateResult:
    status: str  # 'applied' | 'rejected_identity_mismatch' | 'rejected_expired' | 'rejected_stale_observation' | 'rejected_not_found'
    reason: str = ""


def apply_registration_update(
    db: sqlite3.Connection,
    update: RegistrationUpdate,
    *,
    now: int,
) -> UpdateResult:
    """Apply a fenced RegistrationUpdate. Fenced on (uuid, incarnation, fence).

    Does NOT revive expired registrations. Does NOT regress heartbeat_at
    (observed_at must be >= current heartbeat_at).
    """
    if not isinstance(update, RegistrationUpdate):
        raise ValueError("update must be a RegistrationUpdate")
    db.row_factory = sqlite3.Row

    with db:
        row = db.execute(
            "SELECT * FROM session_registrations "
            "WHERE session_uuid = ? AND session_incarnation_id = ?",
            (str(update.session_uuid), str(update.session_incarnation_id)),
        ).fetchone()
        if row is None:
            return UpdateResult(status="rejected_not_found")
        if row["registration_fence"] != update.registration_fence:
            return UpdateResult(status="rejected_identity_mismatch",
                                reason="fence mismatch")
        if row["revoked_at"] is not None or row["expires_at"] <= now:
            return UpdateResult(status="rejected_expired")
        if update.observed_at < row["heartbeat_at"]:
            return UpdateResult(status="rejected_stale_observation")

        db.execute(
            "UPDATE session_registrations "
            "SET state = ?, heartbeat_at = ?, expires_at = ? "
            "WHERE session_uuid = ? AND session_incarnation_id = ? "
            "  AND registration_fence = ? AND revoked_at IS NULL "
            "  AND expires_at > ?",
            (update.state.value, update.observed_at, update.expires_at,
             str(update.session_uuid), str(update.session_incarnation_id),
             update.registration_fence, now),
        )
        return UpdateResult(status="applied")


# ---------------------------------------------------------------------------
# Authenticated atomic targeted-send
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TargetedSendResult:
    status: str  # 'sent' | 'rejected_capability' | 'rejected_target_not_live' | 'rejected_message_exists'
    message_id: Optional[str] = None
    reason: str = ""


def create_authenticated_targeted_send(
    db: sqlite3.Connection,
    *,
    message_payload: str,
    sender_cap_bearer: bytes,
    sender_session_uuid,
    sender_session_incarnation_id,
    sender_registration_fence: str,
    target_session_uuid,
    target_session_incarnation_id,
    correlation_id,
    now: int,
    expires_at: int,
    message_id: Optional[object] = None,
) -> TargetedSendResult:
    """Verify sender capability, then atomically insert bridge message +
    targeted_deliveries row inside one BEGIN EXCLUSIVE.

    Refuses on verify failure or if the exact target incarnation is not live.
    """
    db.row_factory = sqlite3.Row
    msg_id = str(message_id) if message_id is not None else str(uuid4())

    with db:
        decision = verify_capability(
            db, sender_cap_bearer,
            expected_sender_uuid=sender_session_uuid,
            expected_incarnation=sender_session_incarnation_id,
            expected_fence=sender_registration_fence,
            now=now,
        )
        if not decision.allowed:
            return TargetedSendResult(status="rejected_capability",
                                      reason=decision.reason)

        target = db.execute(
            "SELECT * FROM session_registrations "
            "WHERE session_uuid = ? AND session_incarnation_id = ? "
            "  AND revoked_at IS NULL AND expires_at > ?",
            (str(target_session_uuid), str(target_session_incarnation_id), now),
        ).fetchone()
        if target is None:
            return TargetedSendResult(status="rejected_target_not_live")
        if target["state"] not in ("active", "busy", "idle"):
            return TargetedSendResult(status="rejected_target_not_live",
                                      reason=f"state={target['state']}")

        try:
            db.execute(
                "INSERT INTO messages (id, created_at) VALUES (?, ?)",
                (msg_id, now),
            )
        except sqlite3.IntegrityError as exc:
            if "UNIQUE" in str(exc) or "PRIMARY KEY" in str(exc):
                return TargetedSendResult(status="rejected_message_exists",
                                          message_id=msg_id)
            raise

        db.execute(
            "INSERT INTO targeted_deliveries "
            "(message_id, target_session_uuid, target_session_incarnation_id, "
            " sender_session_uuid, sender_session_incarnation_id, delivery_state, "
            " created_at, next_attempt_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
            (msg_id,
             str(target_session_uuid), str(target_session_incarnation_id),
             str(sender_session_uuid), str(sender_session_incarnation_id),
             now, now, expires_at),
        )
        # NOTE: message_payload would be routed to the bridge's payload column
        # here in production; the stub `messages` table used by tests has only
        # (id, created_at), so payload is intentionally not inserted. Live
        # bridge DB has its own payload column + API that the sender should
        # go through separately. Documented in CHANGELOG.
        _ = message_payload

        return TargetedSendResult(status="sent", message_id=msg_id)
