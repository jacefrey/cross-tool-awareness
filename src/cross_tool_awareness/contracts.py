"""Shared contract types.

Matches codex-manager/scripts/automatic_awareness/contracts.py field-for-field
so both tools can exchange these values without adapter translation.

Design authority:
machine-manager/docs/superpowers/specs/2026-10-11-shared-awareness-package-design.md
Contract authority:
codex-manager/docs/superpowers/specs/2026-10-05-codex-gate1-contracts.md (0087ebe)
Peer implementation:
codex-manager/scripts/automatic_awareness/contracts.py
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from enum import Enum
from typing import Literal
from uuid import UUID


SUMMARY_OMITTED = "[summary omitted]"
_SUMMARY_MARKUP_DELIMITERS = frozenset("<>[]{}()*_`#")
_CREDENTIAL_LABELS = frozenset(
    {"password", "secret", "token", "api_key", "authorization"}
)
_LINE_SEPARATORS = frozenset({"\r", "\n", " ", " "})


class PresenceState(str, Enum):
    ACTIVE = "active"
    BUSY = "busy"
    IDLE = "idle"
    STOPPING = "stopping"
    STALE = "stale"


class QueueOutcome(str, Enum):
    ACCEPTED_BY_CLI = "accepted_by_cli"
    REJECTED = "rejected"
    AMBIGUOUS_TIMEOUT = "ambiguous_timeout"
    ADAPTER_DISABLED = "adapter_disabled"


class Disposition(str, Enum):
    """Full disposition enum including UNEVALUATED. DispositionReceipt rejects
    UNEVALUATED at construction per the contract."""

    UNEVALUATED = "unevaluated"
    ACCEPTED = "accepted"
    DEFERRED = "deferred"
    DECLINED = "declined"
    NEEDS_USER = "needs_user"


class DeliveryState(str, Enum):
    """Storage-side state machine used by MM's dispatcher (not in the
    published Codex contract; carried here for the dispatcher implementation)."""

    PENDING = "pending"
    CLAIMED = "claimed"
    NOTIFIED = "notified"
    AMBIGUOUS = "ambiguous"
    BLOCKED = "blocked"
    OBSERVED = "observed"
    UNDELIVERABLE = "undeliverable"


def _exact_uuid(value, field):
    if isinstance(value, UUID):
        return value
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a UUID")
    try:
        return UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(f"{field} must be a UUID") from error


def _required_text(value, field):
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a nonempty string")
    return value


def _unix_seconds(value, field):
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} must be integer Unix seconds")
    return value


def _has_line_separator_or_control(text):
    return any(
        character in _LINE_SEPARATORS
        or unicodedata.category(character).startswith("C")
        for character in text
    )


def _concise_reason(value, field):
    if not isinstance(value, str) or not 1 <= len(value) <= 240:
        raise ValueError(f"{field} must be 1 to 240 characters")
    if _has_line_separator_or_control(value):
        raise ValueError(f"{field} must be single-line and control-free")
    return value


def _enum(value, enum_type, field):
    if isinstance(value, enum_type):
        return value
    if not isinstance(value, str):
        raise ValueError(f"{field} is invalid")
    try:
        return enum_type(value)
    except ValueError as error:
        raise ValueError(f"{field} is invalid") from error


@dataclass(frozen=True)
class PresenceRecord:
    session_uuid: object
    session_incarnation_id: object
    alias: str
    tool: str
    project_slug: str
    project_root: str
    role: str
    state: object
    wake_capability: str
    registration_fence: str
    wake_locator: str
    started_at: int
    heartbeat_at: int
    expires_at: int

    def __post_init__(self):
        object.__setattr__(self, "session_uuid", _exact_uuid(self.session_uuid, "session_uuid"))
        object.__setattr__(
            self,
            "session_incarnation_id",
            _exact_uuid(self.session_incarnation_id, "session_incarnation_id"),
        )
        for field in (
            "alias", "tool", "project_slug", "project_root", "role",
            "wake_capability", "registration_fence", "wake_locator",
        ):
            value = _required_text(getattr(self, field), field)
            object.__setattr__(self, field, value)
        if self.tool not in {"codex", "claude"}:
            raise ValueError("tool is invalid")
        if self.role not in {"manager", "worker", "reviewer", "general"}:
            raise ValueError("role is invalid")
        if self.wake_capability not in {"codex_queue", "claude_socket", "notify_only"}:
            raise ValueError("wake_capability is invalid")
        object.__setattr__(self, "state", _enum(self.state, PresenceState, "state"))
        for field in ("started_at", "heartbeat_at", "expires_at"):
            object.__setattr__(self, field, _unix_seconds(getattr(self, field), field))
        if not self.started_at <= self.heartbeat_at <= self.expires_at:
            raise ValueError("presence timestamps must be ordered")


@dataclass(frozen=True)
class RegistrationUpdate:
    session_uuid: object
    session_incarnation_id: object
    registration_fence: str
    state: object
    observed_at: int
    expires_at: int

    def __post_init__(self):
        object.__setattr__(self, "session_uuid", _exact_uuid(self.session_uuid, "session_uuid"))
        object.__setattr__(
            self,
            "session_incarnation_id",
            _exact_uuid(self.session_incarnation_id, "session_incarnation_id"),
        )
        object.__setattr__(
            self, "registration_fence",
            _required_text(self.registration_fence, "registration_fence"),
        )
        state = _enum(self.state, PresenceState, "state")
        if state not in {
            PresenceState.ACTIVE, PresenceState.BUSY,
            PresenceState.IDLE, PresenceState.STOPPING,
        }:
            raise ValueError("state is not a live transition")
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "observed_at", _unix_seconds(self.observed_at, "observed_at"))
        object.__setattr__(self, "expires_at", _unix_seconds(self.expires_at, "expires_at"))
        if self.observed_at > self.expires_at:
            raise ValueError("observed_at must not exceed expires_at")


@dataclass(frozen=True)
class QueueAttempt:
    message_id: object
    target_session_uuid: object
    target_session_incarnation_id: object
    outcome: object
    attempted_at: int
    finished_at: int
    reason: str

    def __post_init__(self):
        for field in ("message_id", "target_session_uuid", "target_session_incarnation_id"):
            object.__setattr__(self, field, _exact_uuid(getattr(self, field), field))
        outcome = _enum(self.outcome, QueueOutcome, "outcome")
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "attempted_at", _unix_seconds(self.attempted_at, "attempted_at"))
        object.__setattr__(self, "finished_at", _unix_seconds(self.finished_at, "finished_at"))
        if self.attempted_at > self.finished_at:
            raise ValueError("attempted_at must not exceed finished_at")
        if not isinstance(self.reason, str):
            raise ValueError("reason must be a string")
        if outcome is QueueOutcome.ACCEPTED_BY_CLI:
            if self.reason:
                raise ValueError("accepted attempts require an empty reason")
        else:
            object.__setattr__(self, "reason", _concise_reason(self.reason, "reason"))


@dataclass(frozen=True)
class DispositionReceipt:
    """Published 8-field contract. target_session_* are derived at the storage
    boundary from the matching delivery row, not carried on the receipt itself."""

    message_id: object
    correlation_id: object
    consumer_id: str
    receiver_session_uuid: object
    receiver_session_incarnation_id: object
    disposition: object
    observed_at: int
    reason: str

    def __post_init__(self):
        for field in (
            "message_id", "correlation_id",
            "receiver_session_uuid", "receiver_session_incarnation_id",
        ):
            object.__setattr__(self, field, _exact_uuid(getattr(self, field), field))
        object.__setattr__(self, "consumer_id", _required_text(self.consumer_id, "consumer_id"))
        disposition = _enum(self.disposition, Disposition, "disposition")
        if disposition is Disposition.UNEVALUATED:
            raise ValueError("receipts require an evaluation disposition")
        object.__setattr__(self, "disposition", disposition)
        object.__setattr__(self, "observed_at", _unix_seconds(self.observed_at, "observed_at"))
        if disposition in {Disposition.DEFERRED, Disposition.DECLINED, Disposition.NEEDS_USER}:
            object.__setattr__(self, "reason", _concise_reason(self.reason, "reason"))
        elif self.reason:
            object.__setattr__(self, "reason", _concise_reason(self.reason, "reason"))
        elif not isinstance(self.reason, str):
            raise ValueError("reason must be a string")

    def matches(self, message_id, consumer_id, receiver_session_uuid,
                receiver_session_incarnation_id):
        try:
            return (
                self.message_id == _exact_uuid(message_id, "message_id")
                and self.consumer_id == _required_text(consumer_id, "consumer_id")
                and self.receiver_session_uuid
                == _exact_uuid(receiver_session_uuid, "receiver_session_uuid")
                and self.receiver_session_incarnation_id
                == _exact_uuid(
                    receiver_session_incarnation_id,
                    "receiver_session_incarnation_id",
                )
            )
        except ValueError:
            return False


@dataclass(frozen=True)
class CapabilityDecision:
    """Reason tokens match codex-manager's session_capability.CapabilityDecision:
    accepted, invalid_contract, revoked, expired, identity_mismatch, invalid_bearer."""
    allowed: bool
    reason: str

    def __post_init__(self):
        if not isinstance(self.allowed, bool):
            raise ValueError("allowed must be bool")
        if not isinstance(self.reason, str) or not self.reason:
            raise ValueError("reason must be a nonempty string")


@dataclass(frozen=True)
class QueueCapability:
    """Shape matches codex-manager's version_fence.QueueCapability."""
    supported: bool
    exact_version: object
    help_sha256: str
    reason: str

    def __post_init__(self):
        if not isinstance(self.supported, bool):
            raise ValueError("supported must be bool")
        if self.exact_version is not None and not isinstance(self.exact_version, str):
            raise ValueError("exact_version must be str or None")
        if not isinstance(self.help_sha256, str):
            raise ValueError("help_sha256 must be str")
        if not isinstance(self.reason, str):
            raise ValueError("reason must be str")


@dataclass(frozen=True)
class NoticeKey:
    message_id: object
    target_session_uuid: object
    target_session_incarnation_id: object

    def __post_init__(self):
        for field in ("message_id", "target_session_uuid", "target_session_incarnation_id"):
            object.__setattr__(self, field, _exact_uuid(getattr(self, field), field))


@dataclass(frozen=True)
class NoticeObservation:
    key: NoticeKey
    correlation_id: object
    consumer_id: str
    registration_fence: str
    message_expires_at: int

    def __post_init__(self):
        if not isinstance(self.key, NoticeKey):
            raise ValueError("key must be a NoticeKey")
        object.__setattr__(self, "correlation_id", _exact_uuid(self.correlation_id, "correlation_id"))
        if not isinstance(self.consumer_id, str) or not self.consumer_id:
            raise ValueError("consumer_id must be a nonempty string")
        if not isinstance(self.registration_fence, str) or not self.registration_fence:
            raise ValueError("registration_fence must be a nonempty string")
        object.__setattr__(
            self, "message_expires_at",
            _unix_seconds(self.message_expires_at, "message_expires_at"),
        )


ClaimStatus = Literal["first", "duplicate", "recovered", "observed"]


@dataclass(frozen=True)
class NoticeClaim:
    status: str
    receipt: object = None

    def __post_init__(self):
        if self.status not in {"first", "duplicate", "recovered", "observed"}:
            raise ValueError("unknown notice claim status")
        if self.status == "observed" and self.receipt is None:
            raise ValueError("observed claims require a receipt")
        if self.status != "observed" and self.receipt is not None:
            raise ValueError("only observed claims carry a receipt")


def validate_notice_summary(text):
    """Return a safe fixed-notice summary, or the required omission marker."""
    if not isinstance(text, str) or not text or len(text) > 160:
        return SUMMARY_OMITTED
    if _has_line_separator_or_control(text):
        return SUMMARY_OMITTED
    if any(character in _SUMMARY_MARKUP_DELIMITERS for character in text):
        return SUMMARY_OMITTED
    lowered = text.casefold()
    if any(label in lowered for label in _CREDENTIAL_LABELS):
        return SUMMARY_OMITTED
    return text
