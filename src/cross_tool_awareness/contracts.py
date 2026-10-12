"""Dataclasses mirroring the published Gate 1 shared contracts.

Contract authority: codex-manager/docs/superpowers/specs/2026-10-05-codex-gate1-contracts.md
"""

from dataclasses import dataclass
from typing import Optional

from . import validation as validate
from .enums import (
    Disposition,
    PresenceState,
    QueueOutcome,
    Role,
    Tool,
    WakeCapability,
)


@dataclass(frozen=True)
class PresenceRecord:
    session_uuid: str
    session_incarnation_id: str
    alias: str
    tool: str
    project_slug: str
    project_root: str
    role: str
    state: str
    wake_capability: str
    registration_fence: str
    wake_locator: str
    started_at: int
    heartbeat_at: int
    expires_at: int

    def __post_init__(self) -> None:
        validate.uuid(self.session_uuid, field="session_uuid")
        validate.uuid(self.session_incarnation_id, field="session_incarnation_id")
        validate.nonempty(self.alias, field="alias")
        validate.enum_member(self.tool, {t.value for t in Tool}, field="tool")
        validate.nonempty(self.project_slug, field="project_slug")
        validate.nonempty(self.project_root, field="project_root")
        validate.enum_member(self.role, {r.value for r in Role}, field="role")
        validate.enum_member(self.state, {s.value for s in PresenceState}, field="state")
        validate.enum_member(
            self.wake_capability, {w.value for w in WakeCapability}, field="wake_capability"
        )
        validate.nonempty(self.registration_fence, field="registration_fence")
        validate.nonempty(self.wake_locator, field="wake_locator")
        for ts_field in ("started_at", "heartbeat_at", "expires_at"):
            v = getattr(self, ts_field)
            if not isinstance(v, int) or isinstance(v, bool):
                raise ValueError(f"{ts_field}: must be int (not bool)")
        if not (self.started_at <= self.heartbeat_at <= self.expires_at):
            raise ValueError(
                "timestamp invariant violated: "
                "started_at <= heartbeat_at <= expires_at"
            )


@dataclass(frozen=True)
class RegistrationUpdate:
    session_uuid: str
    session_incarnation_id: str
    registration_fence: str
    state: str
    observed_at: int
    expires_at: int

    def __post_init__(self) -> None:
        validate.uuid(self.session_uuid, field="session_uuid")
        validate.uuid(self.session_incarnation_id, field="session_incarnation_id")
        validate.nonempty(self.registration_fence, field="registration_fence")
        validate.enum_member(
            self.state, {"active", "busy", "idle", "stopping"}, field="state"
        )
        for ts_field in ("observed_at", "expires_at"):
            v = getattr(self, ts_field)
            if not isinstance(v, int) or isinstance(v, bool):
                raise ValueError(f"{ts_field}: must be int (not bool)")
        if self.expires_at < self.observed_at:
            raise ValueError("expires_at must be >= observed_at")


@dataclass(frozen=True)
class QueueAttempt:
    message_id: str
    target_session_uuid: str
    target_session_incarnation_id: str
    outcome: str
    attempted_at: int
    finished_at: int
    reason: str

    def __post_init__(self) -> None:
        validate.uuid(self.message_id, field="message_id")
        validate.uuid(self.target_session_uuid, field="target_session_uuid")
        validate.uuid(self.target_session_incarnation_id, field="target_session_incarnation_id")
        validate.enum_member(
            self.outcome, {o.value for o in QueueOutcome}, field="outcome"
        )
        for ts_field in ("attempted_at", "finished_at"):
            v = getattr(self, ts_field)
            if not isinstance(v, int) or isinstance(v, bool):
                raise ValueError(f"{ts_field}: must be int (not bool)")
        if self.finished_at < self.attempted_at:
            raise ValueError("finished_at must be >= attempted_at")
        if self.outcome == "accepted_by_cli":
            if self.reason != "":
                raise ValueError("reason must be empty for accepted_by_cli")
        else:
            validate.single_line_control_free(self.reason, field="reason", min_len=1, max_len=240)


@dataclass(frozen=True)
class DispositionReceipt:
    message_id: str
    target_session_uuid: str
    target_session_incarnation_id: str
    correlation_id: str
    consumer_id: str
    receiver_session_uuid: str
    receiver_session_incarnation_id: str
    disposition: str
    observed_at: int
    reason: str
    delivered_at: int

    def __post_init__(self) -> None:
        validate.uuid(self.message_id, field="message_id")
        validate.uuid(self.target_session_uuid, field="target_session_uuid")
        validate.uuid(self.target_session_incarnation_id, field="target_session_incarnation_id")
        validate.uuid(self.correlation_id, field="correlation_id")
        validate.nonempty(self.consumer_id, field="consumer_id")
        validate.uuid(self.receiver_session_uuid, field="receiver_session_uuid")
        validate.uuid(
            self.receiver_session_incarnation_id, field="receiver_session_incarnation_id"
        )
        # Contract: DispositionReceipt never carries 'unevaluated'
        validate.enum_member(
            self.disposition, {d.value for d in Disposition}, field="disposition"
        )
        for ts_field in ("observed_at", "delivered_at"):
            v = getattr(self, ts_field)
            if not isinstance(v, int) or isinstance(v, bool):
                raise ValueError(f"{ts_field}: must be int (not bool)")
        if self.disposition == "accepted":
            # Accepted may have empty reason; non-accepted must have 1-240
            if self.reason != "" and (len(self.reason) < 1 or len(self.reason) > 240):
                raise ValueError("reason length out of range")
            if self.reason != "":
                validate.single_line_control_free(self.reason, field="reason")
        else:
            validate.single_line_control_free(self.reason, field="reason", min_len=1, max_len=240)
        # CHECK (receiver_* = target_*) is enforced in schema; also mirror here.
        if self.receiver_session_uuid != self.target_session_uuid:
            raise ValueError("receiver_session_uuid must equal target_session_uuid")
        if self.receiver_session_incarnation_id != self.target_session_incarnation_id:
            raise ValueError(
                "receiver_session_incarnation_id must equal target_session_incarnation_id"
            )


@dataclass(frozen=True)
class CapabilityDecision:
    allowed: bool
    reason: str

    def __post_init__(self) -> None:
        if not isinstance(self.allowed, bool):
            raise ValueError("allowed must be bool")
        validate.enum_member(
            self.reason,
            {"ok", "no_match", "sender_mismatch", "incarnation_mismatch",
             "fence_mismatch", "expired", "revoked"},
            field="reason",
        )


@dataclass(frozen=True)
class QueueCapability:
    """Codex-side runtime capability decision. Not persisted in shared storage;
    this type exists for cross-module typing only."""
    adapter_enabled: bool
    tool_version: str
    help_sha256: Optional[str] = None
    reason: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.adapter_enabled, bool):
            raise ValueError("adapter_enabled must be bool")
        validate.nonempty(self.tool_version, field="tool_version")
