"""cross_tool_awareness: shared storage + dispatcher primitives.

See the design spec at
machine-manager/docs/superpowers/specs/2026-10-11-shared-awareness-package-design.md

Fixture-only. Not authorized for live use.
"""

from .adapter import WakeAdapter, WakeNotice
from .backup import BackupResult, BackupVerificationFailure, create_backup
from .capability import (
    BearerCapability,
    PresenceExpiringSoonError,
    issue_capability,
    revoke_capability,
    rotate_capability,
    verify_capability,
)
from .contracts import (
    CapabilityDecision,
    DispositionReceipt,
    PresenceRecord,
    QueueAttempt,
    QueueCapability,
    RegistrationUpdate,
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
    POLL_INTERVAL_ACTIVE_SECONDS,
    POLL_INTERVAL_IDLE_SECONDS,
    run_one_iteration,
)
from .enums import (
    DeliveryState,
    Disposition,
    PresenceState,
    QueueOutcome,
    Role,
    Tool,
    WakeCapability,
)
from .leases import (
    ClaimedDelivery,
    FinalizationResult,
    PreAdapterReleaseResult,
    SingletonDispatcherLease,
    claim_delivery,
    finalize_attempt,
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
from .probe import ProbeResult, run_foreground_probe
from .reconciliation import ReconciliationResult, reconcile_ambiguous, reconcile_blocked
from .registry import register_session, revoke_session

__version__ = "0.1.0"

__all__ = [
    # version
    "__version__",
    # adapters
    "WakeAdapter", "WakeNotice",
    # backup
    "BackupResult", "BackupVerificationFailure", "create_backup",
    # capability
    "BearerCapability", "PresenceExpiringSoonError",
    "issue_capability", "revoke_capability", "rotate_capability", "verify_capability",
    # contracts
    "CapabilityDecision", "DispositionReceipt", "PresenceRecord",
    "QueueAttempt", "QueueCapability", "RegistrationUpdate",
    # dispatcher
    "BACKOFF_SCHEDULE_SECONDS", "BUDGET_CPU_PCT_IDLE", "BUDGET_PEAK_RSS_MB",
    "BUDGET_SQLITE_LOCK_WAIT_MS_P95", "BUDGET_WAKE_FREQ_PER_MIN_IDLE",
    "CLAIM_LEASE_TTL_SECONDS", "DISPATCHER_LEASE_TTL_SECONDS",
    "IterationResult", "POLL_INTERVAL_ACTIVE_SECONDS", "POLL_INTERVAL_IDLE_SECONDS",
    "run_one_iteration",
    # enums
    "DeliveryState", "Disposition", "PresenceState", "QueueOutcome",
    "Role", "Tool", "WakeCapability",
    # leases
    "ClaimedDelivery", "FinalizationResult", "PreAdapterReleaseResult",
    "SingletonDispatcherLease", "claim_delivery", "finalize_attempt",
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
]
