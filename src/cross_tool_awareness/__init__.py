"""cross_tool_awareness: shared storage + dispatcher primitives.

Design authority (v0.5.0):
codex-manager/docs/superpowers/specs/2026-10-14-awareness-pre-live-hardening-design.md
Prior design authority (v0.4.0):
machine-manager/docs/superpowers/specs/2026-10-12-shared-awareness-package-v0.4-design.md

v0.5.0. Fixture-only. Not authorized for live use.
"""

from .adapter import BoundedAdapter, WakeAdapter, WakeNotice
from .backup import BackupResult, BackupVerificationFailure, create_backup
from .capability import (
    BearerCapability,
    PresenceExpiringSoonError,
    issue_capability,
    revoke_capability,
    rotate_capability,
    verify_capability,
    verify_capability_with_liveness,
)
from .composite_storage import CompositeFinalizationResult, CompositeStorage
from .contracts import (
    SUMMARY_OMITTED,
    CapabilityDecision,
    ClaimantIdentity,
    ClaimStatus,
    DeliveryState,
    Disposition,
    DispositionReceipt,
    NoticeClaim,
    NoticeKey,
    NoticeObservation,
    PresenceRecord,
    PresenceState,
    QueueAttempt,
    QueueCapability,
    QueueOutcome,
    RegistrationSnapshot,
    RegistrationUpdate,
    validate_notice_summary,
)
from .dispatcher import (
    BACKOFF_SCHEDULE_SECONDS,
    BUDGET_CPU_PCT_IDLE,
    BUDGET_PEAK_RSS_MB,
    BUDGET_SQLITE_LOCK_WAIT_MS_P95,
    BUDGET_WAKE_FREQ_PER_MIN_IDLE,
    CLAIM_LEASE_TTL_SECONDS,
    DISPATCHER_LEASE_TTL_SECONDS,
    IterationResult,
    NoticeFactory,
    POLL_INTERVAL_ACTIVE_SECONDS,
    POLL_INTERVAL_IDLE_SECONDS,
    # DISABLED in v0.5.0: raises RuntimeError on every call. Kept importable
    # for symbol stability; see CHANGELOG v0.5.0 §Disabled.
    run_one_iteration,
)
from .leases import (
    ClaimedDelivery,
    FinalizationResult,
    PreAdapterReleaseResult,
    SingletonDispatcherLease,
    StuckDispatching,
    claim_delivery,
    finalize_attempt,
    list_stuck_dispatching,
    pre_adapter_mark,
    pre_adapter_release,
)
from .location import DEFAULT_BRIDGE_DB_PATH, resolve_bridge_db_path
from .migrations import (
    EXPECTED_SCHEMA_VERSION,
    MANAGED_INDEXES,
    MANAGED_TABLES,
    MANAGED_TRIGGERS,
    MigrationResult,
    PACKAGE_VERSION,
    RollbackResult,
    apply,
    rollback,
)
from .probe import (
    ProbeResult,
    # DISABLED in v0.5.0: raises RuntimeError BEFORE any DB connection,
    # lease acquisition, clock call, sleep, measurement, or adapter access —
    # including duration_seconds=0. ProbeResult remains importable as an
    # inert data type for v0.4-era saved recordings. See CHANGELOG v0.5.0.
    run_foreground_probe,
)
from .reconciliation import ReconciliationResult, reconcile_ambiguous, reconcile_blocked
from .registry import register_session, revoke_session
from .storage import (
    ReceiptStorage,
    TargetedSendResult,
    UpdateResult,
    apply_registration_update,
    create_authenticated_targeted_send,
)

__version__ = "0.5.0"

__all__ = [
    "__version__",
    # adapters
    "BoundedAdapter", "WakeAdapter", "WakeNotice",
    # backup
    "BackupResult", "BackupVerificationFailure", "create_backup",
    # capability
    "BearerCapability", "PresenceExpiringSoonError",
    "issue_capability", "revoke_capability", "rotate_capability",
    "verify_capability", "verify_capability_with_liveness",
    # contracts
    "CapabilityDecision", "ClaimantIdentity", "ClaimStatus",
    "DeliveryState", "Disposition",
    "DispositionReceipt", "NoticeClaim", "NoticeKey", "NoticeObservation",
    "PresenceRecord", "PresenceState", "QueueAttempt", "QueueCapability",
    "QueueOutcome", "RegistrationSnapshot", "RegistrationUpdate",
    "SUMMARY_OMITTED", "validate_notice_summary",
    # composite storage
    "CompositeFinalizationResult", "CompositeStorage",
    # dispatcher
    "BACKOFF_SCHEDULE_SECONDS", "BUDGET_CPU_PCT_IDLE", "BUDGET_PEAK_RSS_MB",
    "BUDGET_SQLITE_LOCK_WAIT_MS_P95", "BUDGET_WAKE_FREQ_PER_MIN_IDLE",
    "CLAIM_LEASE_TTL_SECONDS", "DISPATCHER_LEASE_TTL_SECONDS",
    "IterationResult", "NoticeFactory",
    "POLL_INTERVAL_ACTIVE_SECONDS", "POLL_INTERVAL_IDLE_SECONDS",
    "run_one_iteration",
    # leases
    "ClaimedDelivery", "FinalizationResult", "PreAdapterReleaseResult",
    "SingletonDispatcherLease", "StuckDispatching", "claim_delivery",
    "finalize_attempt", "list_stuck_dispatching", "pre_adapter_mark",
    "pre_adapter_release",
    # location
    "DEFAULT_BRIDGE_DB_PATH", "resolve_bridge_db_path",
    # migrations
    "EXPECTED_SCHEMA_VERSION", "MANAGED_INDEXES", "MANAGED_TABLES",
    "MANAGED_TRIGGERS", "MigrationResult", "PACKAGE_VERSION",
    "RollbackResult", "apply", "rollback",
    # probe
    "ProbeResult", "run_foreground_probe",
    # reconciliation
    "ReconciliationResult", "reconcile_ambiguous", "reconcile_blocked",
    # registry
    "register_session", "revoke_session",
    # storage
    "ReceiptStorage", "TargetedSendResult", "UpdateResult",
    "apply_registration_update", "create_authenticated_targeted_send",
]
