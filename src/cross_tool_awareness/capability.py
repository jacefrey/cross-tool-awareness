"""Sender capability lifecycle: BearerCapability wrapper + issue/verify/rotate/revoke.

Transaction-neutral helpers (v0.3.0): issue/verify/revoke do NOT open their own
transactions. The caller owns transaction scope. rotate_capability opens one
explicit BEGIN EXCLUSIVE (via ``_txn.exclusive``) and calls the helpers inside
it so revoke+issue are atomic — a failure in issue rolls back the revoke.

The 32-byte bearer never crosses the storage boundary. Only the SHA-256 verifier
is persisted.

Reason tokens match codex-manager's session_capability.CapabilityDecision:
accepted, invalid_contract, revoked, expired, identity_mismatch, invalid_bearer.
"""

from __future__ import annotations

import hashlib
import hmac
import sqlite3
from typing import Callable

from ._txn import exclusive
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
        """Return bearer bytes only at the explicit verification boundary."""
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
        return hash(self.verifier_sha256())


def _fetch_live_registration_row(db, *, session_uuid, session_incarnation_id, now):
    """Transaction-neutral read."""
    db.row_factory = sqlite3.Row
    cur = db.execute(
        "SELECT * FROM session_registrations "
        "WHERE session_uuid = ? AND session_incarnation_id = ? "
        "  AND revoked_at IS NULL AND expires_at > ?",
        (str(session_uuid), str(session_incarnation_id), now),
    )
    return cur.fetchone()


def issue_capability(
    db,
    *,
    session_uuid,
    session_incarnation_id,
    now: int,
    entropy_source: Callable[[int], bytes],
    requested_ttl_seconds: int = 3600,
) -> BearerCapability:
    """Transaction-neutral capability issuance. Caller controls txn scope."""
    row = _fetch_live_registration_row(
        db,
        session_uuid=session_uuid,
        session_incarnation_id=session_incarnation_id,
        now=now,
    )
    if row is None:
        raise ValueError("no live registration for the given UUID/incarnation")
    if row["state"] not in ("active", "busy", "idle"):
        raise ValueError(
            f"presence state {row['state']!r} not eligible for capability issuance"
        )

    bearer_bytes = entropy_source(32)
    if not isinstance(bearer_bytes, (bytes, bytearray)) or len(bearer_bytes) != 32:
        raise ValueError("entropy_source must return exactly 32 bytes")

    verifier = hashlib.sha256(bytes(bearer_bytes)).digest()
    expires_at = min(now + int(requested_ttl_seconds), row["expires_at"])
    if expires_at <= now:
        raise PresenceExpiringSoonError(
            f"capability would expire immediately: presence.expires_at={row['expires_at']}, now={now}"
        )

    # No `with db:` — caller owns transaction scope.
    db.execute(
        "INSERT INTO session_capabilities "
        "(verifier_sha256, sender_session_uuid, sender_session_incarnation_id, "
        " registration_fence, issued_at, expires_at, revoked_at) "
        "VALUES (?, ?, ?, ?, ?, ?, NULL)",
        (verifier, row["session_uuid"], row["session_incarnation_id"],
         row["registration_fence"], now, expires_at),
    )

    return BearerCapability(bytes(bearer_bytes))


def verify_capability(
    db,
    presented_bearer: bytes,
    *,
    expected_sender_uuid,
    expected_incarnation,
    expected_fence: str,
    now: int,
) -> CapabilityDecision:
    """Transaction-neutral verification. Returns CapabilityDecision(allowed, reason)
    with reason tokens matching codex-manager: accepted / invalid_contract /
    revoked / expired / identity_mismatch / invalid_bearer."""
    if not isinstance(presented_bearer, (bytes, bytearray)) or len(presented_bearer) != 32:
        return CapabilityDecision(allowed=False, reason="invalid_contract")

    verifier = hashlib.sha256(bytes(presented_bearer)).digest()
    db.row_factory = sqlite3.Row
    cur = db.execute(
        "SELECT * FROM session_capabilities WHERE verifier_sha256 = ?",
        (verifier,),
    )
    row = cur.fetchone()
    if row is None:
        return CapabilityDecision(allowed=False, reason="invalid_bearer")
    if row["revoked_at"] is not None:
        return CapabilityDecision(allowed=False, reason="revoked")
    if now < row["issued_at"] or now >= row["expires_at"]:
        return CapabilityDecision(allowed=False, reason="expired")
    if (
        row["sender_session_uuid"] != str(expected_sender_uuid)
        or row["sender_session_incarnation_id"] != str(expected_incarnation)
        or row["registration_fence"] != expected_fence
    ):
        return CapabilityDecision(allowed=False, reason="identity_mismatch")
    if not hmac.compare_digest(bytes(row["verifier_sha256"]), verifier):
        return CapabilityDecision(allowed=False, reason="invalid_bearer")
    return CapabilityDecision(allowed=True, reason="accepted")


def revoke_capability(db, verifier_sha256: bytes, *, now: int) -> None:
    """Transaction-neutral revoke. Idempotent."""
    if not isinstance(verifier_sha256, (bytes, bytearray)) or len(verifier_sha256) != 32:
        raise ValueError("verifier_sha256 must be 32 bytes")
    db.execute(
        "UPDATE session_capabilities SET revoked_at = ? "
        "WHERE verifier_sha256 = ? AND revoked_at IS NULL",
        (now, bytes(verifier_sha256)),
    )


def rotate_capability(
    db,
    old_bearer: bytes,
    *,
    expected_sender_uuid,
    expected_incarnation,
    expected_fence: str,
    now: int,
    entropy_source: Callable[[int], bytes],
    requested_ttl_seconds: int = 3600,
) -> BearerCapability:
    """Atomic rotation: validate old, revoke old, issue new — all in one txn.

    Uses one explicit BEGIN EXCLUSIVE (via ``_txn.exclusive``) wrapping
    transaction-neutral helpers. If issue_capability raises mid-flight, the
    revoke is rolled back (atomicity).
    """
    with exclusive(db):
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
        revoke_capability(db, old_verifier, now=now)

        new_cap = issue_capability(
            db,
            session_uuid=expected_sender_uuid,
            session_incarnation_id=expected_incarnation,
            now=now,
            entropy_source=entropy_source,
            requested_ttl_seconds=requested_ttl_seconds,
        )
        return new_cap
