"""String-valued enums whose str() rendering matches the wire token exactly.

enum.StrEnum (Python 3.11+) is required: plain Enum with a str value yields
str(PresenceState.ACTIVE) == 'PresenceState.ACTIVE' on Python 3.12, which is
NOT the wire token.
"""

import enum


class PresenceState(enum.StrEnum):
    ACTIVE = "active"
    BUSY = "busy"
    IDLE = "idle"
    STOPPING = "stopping"
    STALE = "stale"


class QueueOutcome(enum.StrEnum):
    ACCEPTED_BY_CLI = "accepted_by_cli"
    REJECTED = "rejected"
    AMBIGUOUS_TIMEOUT = "ambiguous_timeout"
    ADAPTER_DISABLED = "adapter_disabled"


class Disposition(enum.StrEnum):
    """Exposed values. 'unevaluated' is NOT a member; DispositionReceipt
    rejects it at construction per the Gate 1 contract."""

    ACCEPTED = "accepted"
    DEFERRED = "deferred"
    DECLINED = "declined"
    NEEDS_USER = "needs_user"


class DeliveryState(enum.StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    NOTIFIED = "notified"
    AMBIGUOUS = "ambiguous"
    BLOCKED = "blocked"
    OBSERVED = "observed"
    UNDELIVERABLE = "undeliverable"


class Tool(enum.StrEnum):
    CODEX = "codex"
    CLAUDE = "claude"


class Role(enum.StrEnum):
    MANAGER = "manager"
    WORKER = "worker"
    REVIEWER = "reviewer"
    GENERAL = "general"


class WakeCapability(enum.StrEnum):
    CODEX_QUEUE = "codex_queue"
    CLAUDE_SOCKET = "claude_socket"
    NOTIFY_ONLY = "notify_only"
