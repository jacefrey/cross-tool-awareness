"""Storage APIs for Codex-adapter consumption (v0.3.0).

Implements codex-manager's ReceiptStorage Protocol (receiver_receipts.py) and
adds fenced apply_registration_update + atomic authenticated targeted-send.
Nothing in this module writes to the live bridge DB — the caller passes an
sqlite3.Connection scoped to a tmpdir fixture DB or (in future) the shared
bridge DB after separate authorization.

v0.3.0 fixes (from codex-manager seq=195 review):

- B1 is now atomic. ``create_authenticated_targeted_send`` requires a
  ``message_writer`` callable, which runs *inside* the same BEGIN EXCLUSIVE
  transaction as the targeted_deliveries insert. The caller supplies the real
  bridge-side INSERT (payload column + whatever the live bridge schema
  requires); the stub test fixture supplies its own. There is no silent
  payload discard anymore — the writer either runs atomically or the whole
  send fails.
- B2 BEGIN EXCLUSIVE is now real. Every method here that does a
  SELECT-then-INSERT or needs serialization uses ``_txn.exclusive`` (explicit
  BEGIN EXCLUSIVE, explicit COMMIT/ROLLBACK) instead of Python's DEFERRED
  ``with db:``.
- B3 ``claim_notice`` and ``record_first_disposition`` additionally recover
  from sqlite3.IntegrityError (defense-in-depth: even if a caller somehow runs
  without EXCLUSIVE serialization — e.g. by passing a WAL-mode connection that
  does not block — the duplicate-key race resolves deterministically by
  re-selecting the durable state).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable, Optional
from uuid import uuid4

from ._txn import exclusive
from .capability import verify_capability
from .contracts import (
    DispositionReceipt,
    NoticeClaim,
    NoticeKey,
    NoticeObservation,
    RegistrationUpdate,
)


# Signature of the message_writer callable supplied by the caller.
# Called with (db, message_id, message_payload, now).
# Must INSERT the message row into whatever table the caller's bridge uses.
# Runs inside the same BEGIN EXCLUSIVE transaction as the delivery insert.
MessageWriter = Callable[[sqlite3.Connection, str, str, int], None]


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
        with exclusive(self._db):
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

            try:
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
            except sqlite3.IntegrityError:
                # Defense in depth: a concurrent writer inserted between our
                # SELECT and INSERT despite EXCLUSIVE. Re-check durable state.
                receipt_row = self._db.execute(
                    "SELECT * FROM disposition_receipts "
                    "WHERE message_id = ? AND target_session_uuid = ? "
                    "  AND target_session_incarnation_id = ?",
                    (str(key.message_id),
                     str(key.target_session_uuid),
                     str(key.target_session_incarnation_id)),
                ).fetchone()
                if receipt_row is not None:
                    return NoticeClaim(status="observed",
                                       receipt=_row_to_receipt(receipt_row))
                return NoticeClaim(status="duplicate")

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

        with exclusive(self._db):
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
            except sqlite3.IntegrityError:
                # Defense in depth under non-EXCLUSIVE connections.
                existing = self._db.execute(
                    "SELECT * FROM disposition_receipts "
                    "WHERE message_id = ? AND target_session_uuid = ? "
                    "  AND target_session_incarnation_id = ?",
                    (str(key.message_id),
                     str(key.target_session_uuid),
                     str(key.target_session_incarnation_id)),
                ).fetchone()
                if existing is None:
                    # Not a duplicate — some other integrity failure
                    # (e.g. the eligible-delivery-state trigger).
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

    with exclusive(db):
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
    message_writer: MessageWriter,
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
    """Verify sender capability, then atomically persist bridge message +
    targeted_deliveries row inside one BEGIN EXCLUSIVE.

    The caller supplies ``message_writer`` — a callable that takes
    (db, message_id, message_payload, now) and inserts the bridge message
    row (payload + whatever other columns the live bridge schema requires).
    The writer runs inside the same EXCLUSIVE transaction as the delivery
    insert, so bridge message and delivery row are atomic together.

    The writer MUST raise sqlite3.IntegrityError if the message_id already
    exists (UNIQUE constraint on messages(id)); this path returns
    ``rejected_message_exists``. Any other IntegrityError is re-raised.
    """
    if message_writer is None or not callable(message_writer):
        raise ValueError("message_writer is required and must be callable")

    db.row_factory = sqlite3.Row
    msg_id = str(message_id) if message_id is not None else str(uuid4())

    with exclusive(db):
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
            message_writer(db, msg_id, message_payload, now)
        except sqlite3.IntegrityError as exc:
            err = str(exc)
            if "UNIQUE" in err or "PRIMARY KEY" in err:
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

        return TargetedSendResult(status="sent", message_id=msg_id)
