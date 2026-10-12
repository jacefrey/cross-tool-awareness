"""Sender capability lifecycle: BearerCapability wrapper + issue/verify/rotate/revoke.

The 32-byte bearer never crosses the storage boundary. Only the SHA-256 verifier
is persisted. See spec §6.
"""

import hashlib
import hmac
import sqlite3
from dataclasses import dataclass
from typing import Callable, Optional

from .contracts import CapabilityDecision


class PresenceExpiringSoonError(Exception):
    """Raised when the bounded capability expires_at would be <= now."""


class BearerCapability:
    """A 32-byte bearer token wrapped so representation and persistence are safe."""

    __slots__ = ("_bearer_bytes",)

    def __init__(self, bearer_bytes: bytes) -> None:
        if not isinstance(bearer_bytes, (bytes, bytearray)):
            raise TypeError("bearer must be bytes")
        if len(bearer_bytes) != 32:
            raise ValueError("bearer must be exactly 32 bytes")
        self._bearer_bytes = bytes(bearer_bytes)

    def verifier_sha256(self) -> bytes:
        return hashlib.sha256(self._bearer_bytes).digest()

    def present(self) -> bytes:
        """Returned to the verify() path only. Caller must not persist."""
        return self._bearer_bytes

    def __repr__(self) -> str:
        return "<BearerCapability redacted>"

    def __str__(self) -> str:
        return "<BearerCapability redacted>"

    def __reduce__(self):
        raise TypeError("BearerCapability refuses pickle serialization")

    def __eq__(self, other):
        if not isinstance(other, BearerCapability):
            return NotImplemented
        return hmac.compare_digest(self._bearer_bytes, other._bearer_bytes)

    def __hash__(self):
        # Hash of the verifier (public), not the bearer.
        return hash(self.verifier_sha256())


@dataclass(frozen=True)
class _PresenceSnapshot:
    """Minimal subset of a PresenceRecord used by capability issuance."""
    session_uuid: str
    session_incarnation_id: str
    registration_fence: str
    state: str
    expires_at: int


def _presence_from_registration(row) -> _PresenceSnapshot:
    return _PresenceSnapshot(
        session_uuid=row["session_uuid"],
        session_incarnation_id=row["session_incarnation_id"],
        registration_fence=row["registration_fence"],
        state=row["state"],
        expires_at=row["expires_at"],
    )


def _fetch_live_registration(db: sqlite3.Connection, *, session_uuid: str,
                             session_incarnation_id: str, now: int):
    db.row_factory = sqlite3.Row
    cur = db.execute(
        "SELECT * FROM session_registrations "
        "WHERE session_uuid = ? AND session_incarnation_id = ? "
        "  AND revoked_at IS NULL AND expires_at > ?",
        (session_uuid, session_incarnation_id, now),
    )
    row = cur.fetchone()
    return row


def issue_capability(
    db: sqlite3.Connection,
    *,
    session_uuid: str,
    session_incarnation_id: str,
    now: int,
    entropy_source: Callable[[int], bytes],
    requested_ttl_seconds: int = 3600,
) -> BearerCapability:
    """Issue a new BearerCapability for the given live presence.

    The capability's expires_at is bounded by presence.expires_at. The bearer
    is returned once; only its SHA-256 verifier is persisted.
    """
    row = _fetch_live_registration(
        db, session_uuid=session_uuid,
        session_incarnation_id=session_incarnation_id, now=now,
    )
    if row is None:
        raise ValueError("no live registration for the given UUID/incarnation")
    if row["state"] not in ("active", "busy", "idle"):
        raise ValueError(
            f"presence state {row['state']!r} not eligible for capability issuance"
        )
    presence = _presence_from_registration(row)

    bearer_bytes = entropy_source(32)
    if not isinstance(bearer_bytes, (bytes, bytearray)) or len(bearer_bytes) != 32:
        raise ValueError("entropy_source must return exactly 32 bytes")

    verifier = hashlib.sha256(bytes(bearer_bytes)).digest()
    expires_at = min(now + int(requested_ttl_seconds), presence.expires_at)
    if expires_at <= now:
        raise PresenceExpiringSoonError(
            f"capability would expire immediately: presence.expires_at={presence.expires_at}, now={now}"
        )

    with db:
        db.execute(
            "INSERT INTO session_capabilities "
            "(verifier_sha256, sender_session_uuid, sender_session_incarnation_id, "
            " registration_fence, issued_at, expires_at, revoked_at) "
            "VALUES (?, ?, ?, ?, ?, ?, NULL)",
            (verifier, presence.session_uuid, presence.session_incarnation_id,
             presence.registration_fence, now, expires_at),
        )

    return BearerCapability(bytes(bearer_bytes))


def verify_capability(
    db: sqlite3.Connection,
    presented_bearer: bytes,
    *,
    expected_sender_uuid: str,
    expected_incarnation: str,
    expected_fence: str,
    now: int,
) -> CapabilityDecision:
    """Verify a presented bearer against persisted state.

    Returns CapabilityDecision(allowed, reason) with reason in the fixed set.
    """
    if not isinstance(presented_bearer, (bytes, bytearray)) or len(presented_bearer) != 32:
        return CapabilityDecision(allowed=False, reason="no_match")

    verifier = hashlib.sha256(bytes(presented_bearer)).digest()
    db.row_factory = sqlite3.Row
    cur = db.execute(
        "SELECT * FROM session_capabilities WHERE verifier_sha256 = ?",
        (verifier,),
    )
    row = cur.fetchone()
    if row is None:
        return CapabilityDecision(allowed=False, reason="no_match")
    # Constant-time compare of stored verifier bytes (defensive; PK already matched).
    if not hmac.compare_digest(bytes(row["verifier_sha256"]), verifier):
        return CapabilityDecision(allowed=False, reason="no_match")
    if row["sender_session_uuid"] != expected_sender_uuid:
        return CapabilityDecision(allowed=False, reason="sender_mismatch")
    if row["sender_session_incarnation_id"] != expected_incarnation:
        return CapabilityDecision(allowed=False, reason="incarnation_mismatch")
    if row["registration_fence"] != expected_fence:
        return CapabilityDecision(allowed=False, reason="fence_mismatch")
    if now >= row["expires_at"]:
        return CapabilityDecision(allowed=False, reason="expired")
    if row["revoked_at"] is not None:
        return CapabilityDecision(allowed=False, reason="revoked")
    return CapabilityDecision(allowed=True, reason="ok")


def rotate_capability(
    db: sqlite3.Connection,
    old_bearer: bytes,
    *,
    expected_sender_uuid: str,
    expected_incarnation: str,
    expected_fence: str,
    now: int,
    entropy_source: Callable[[int], bytes],
    requested_ttl_seconds: int = 3600,
) -> BearerCapability:
    """Validate the old bearer, revoke it, issue a new one for the same binding.

    Both operations happen in one BEGIN EXCLUSIVE transaction.
    """
    with db:
        decision = verify_capability(
            db, old_bearer,
            expected_sender_uuid=expected_sender_uuid,
            expected_incarnation=expected_incarnation,
            expected_fence=expected_fence,
            now=now,
        )
        if not decision.allowed:
            raise ValueError(f"old capability not valid: {decision.reason}")

        old_verifier = hashlib.sha256(bytes(old_bearer)).digest()
        db.execute(
            "UPDATE session_capabilities SET revoked_at = ? "
            "WHERE verifier_sha256 = ? AND revoked_at IS NULL",
            (now, old_verifier),
        )

        new_cap = issue_capability(
            db,
            session_uuid=expected_sender_uuid,
            session_incarnation_id=expected_incarnation,
            now=now,
            entropy_source=entropy_source,
            requested_ttl_seconds=requested_ttl_seconds,
        )
        return new_cap


def revoke_capability(db: sqlite3.Connection, verifier_sha256: bytes, *, now: int) -> None:
    """Idempotent revoke. No-op if already revoked or missing."""
    if not isinstance(verifier_sha256, (bytes, bytearray)) or len(verifier_sha256) != 32:
        raise ValueError("verifier_sha256 must be 32 bytes")
    with db:
        db.execute(
            "UPDATE session_capabilities SET revoked_at = ? "
            "WHERE verifier_sha256 = ? AND revoked_at IS NULL",
            (now, bytes(verifier_sha256)),
        )
