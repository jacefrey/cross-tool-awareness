"""Narrow tool-specific interface the dispatcher invokes to deliver a notice."""

from abc import ABC, abstractmethod
from dataclasses import dataclass

from . import validation as validate


@dataclass(frozen=True)
class WakeNotice:
    """Minimal fixed-template notice. Full payload remains in the bridge messages table."""

    message_id: str
    channel: str
    sender_alias_at_send: str
    summary: str

    def __post_init__(self) -> None:
        validate.uuid(self.message_id, field="message_id")
        validate.nonempty(self.channel, field="channel")
        validate.nonempty(self.sender_alias_at_send, field="sender_alias_at_send")
        # Summary is bounded; validation.summary_bounded returns the input or
        # '[summary omitted]' — but since this is a frozen dataclass, callers
        # should pre-filter via validation.summary_bounded() before constructing.
        if not isinstance(self.summary, str):
            raise ValueError("summary must be str")
        if len(self.summary) > 160:
            raise ValueError("summary must be <= 160 chars")


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
        target_session_uuid: str,
        target_session_incarnation_id: str,
        notice: WakeNotice,
    ) -> str:
        """Deliver a notice. Returns a QueueOutcome value as a string.

        Must not read message bodies. Must not infer authorization. Must not
        resume a stopped session. Must not retry internally. Must not shell out
        via alias.
        """
