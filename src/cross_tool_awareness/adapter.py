"""Narrow tool-specific interface the dispatcher invokes to deliver a notice.

WakeNotice sanitizes its summary via contracts.validate_notice_summary at
construction: unsafe input becomes '[summary omitted]' rather than raising,
matching codex-manager's contract behavior.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from . import contracts
from .contracts import PresenceRecord, QueueAttempt
from .leases import ClaimedDelivery as DeliveryClaim


@dataclass(frozen=True)
class WakeNotice:
    """Minimal fixed-template notice. Full payload remains in the bridge messages table."""

    message_id: object
    channel: str
    sender_alias_at_send: str
    summary: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "message_id", contracts._exact_uuid(self.message_id, "message_id"))
        if not isinstance(self.channel, str) or not self.channel:
            raise ValueError("channel must be a nonempty string")
        if not isinstance(self.sender_alias_at_send, str) or not self.sender_alias_at_send:
            raise ValueError("sender_alias_at_send must be a nonempty string")
        object.__setattr__(
            self,
            "summary",
            contracts.validate_notice_summary(self.summary),
        )


class WakeAdapter(ABC):
    """Tool-specific wake adapter. Codex and Claude adapters implement this."""

    @property
    @abstractmethod
    def wake_capability(self) -> str:
        """One of 'codex_queue', 'claude_socket', 'notify_only'."""

    @abstractmethod
    def is_enabled(self) -> bool:
        """Runtime capability check performed by the dispatcher before wake()."""

    @abstractmethod
    def wake(
        self,
        target_session_uuid,
        target_session_incarnation_id,
        notice: WakeNotice,
    ) -> str:
        """Deliver a notice. Returns a QueueOutcome value as a string.

        Must not read message bodies. Must not infer authorization. Must not
        resume a stopped session. Must not retry internally. Must not shell out
        via alias.
        """


class BoundedAdapter(Protocol):
    """Adapter that honors a wall-clock budget.

    Implementations MUST raise ``TimeoutError`` if elapsed wall-clock time
    inside ``invoke`` exceeds ``timeout_seconds`` and MUST NOT return a
    ``QueueAttempt`` once that boundary has been crossed. Callers (e.g.
    the sender-side composite) rely on this contract to bound their claim
    lease; a non-honoring implementation blocks claim recovery.
    """

    def invoke(
        self,
        claim: DeliveryClaim,
        registration: PresenceRecord,
        fixed_notice: str,
        *,
        timeout_seconds: int,
    ) -> QueueAttempt: ...
