"""Dispatcher state machine: run_one_iteration implementing the 7-step contract.

See spec §8.
"""

import sqlite3
import time
from dataclasses import dataclass, field
from typing import Callable, Mapping, Optional

from .adapter import WakeAdapter, WakeNotice
from .leases import (
    SingletonDispatcherLease,
    claim_delivery,
    finalize_attempt,
    pre_adapter_release,
)


# Fixed polling defaults; probe measures pass/fail against explicit budgets.
POLL_INTERVAL_IDLE_SECONDS = 30
POLL_INTERVAL_ACTIVE_SECONDS = 2
BACKOFF_SCHEDULE_SECONDS = (0, 5, 20, 60, 300, 1800)
BACKOFF_CAP_SECONDS = 3600
CLAIM_LEASE_TTL_SECONDS = 15
DISPATCHER_LEASE_TTL_SECONDS = 60

# Explicit budgets the foreground probe verifies.
BUDGET_PEAK_RSS_MB = 48
BUDGET_CPU_PCT_IDLE = 1.0
BUDGET_WAKE_FREQ_PER_MIN_IDLE = 2
BUDGET_SQLITE_LOCK_WAIT_MS_P95 = 50


@dataclass
class IterationResult:
    status: str                             # 'lease_lost' | 'no_work' | 'pre_adapter_released' | 'finalized' | 'target_revalidation_failed' | 'adapter_unavailable'
    diagnostic: str = ""
    ttl_swept: int = 0
    expired_claims_recovered: int = 0
    outcome: Optional[str] = None
    delivery_state: Optional[str] = None
    attempts_recorded: int = 0


def _compute_backoff(attempt_count: int) -> int:
    idx = min(attempt_count, len(BACKOFF_SCHEDULE_SECONDS) - 1)
    base = BACKOFF_SCHEDULE_SECONDS[idx]
    return min(base, BACKOFF_CAP_SECONDS)


def _recover_expired_claims(db: sqlite3.Connection, *, now: int) -> int:
    with db:
        cur = db.execute(
            "UPDATE targeted_deliveries "
            "SET delivery_state = 'pending', claim_lease_token = NULL, "
            "    claim_lease_expires_at = NULL, next_attempt_at = ?, "
            "    last_error_class = 'lease_expired' "
            "WHERE delivery_state = 'claimed' "
            "  AND claim_lease_expires_at IS NOT NULL "
            "  AND claim_lease_expires_at <= ?",
            (now, now),
        )
        return cur.rowcount


def _ttl_sweep(db: sqlite3.Connection, *, now: int) -> int:
    """Advance expired non-claimed rows to 'undeliverable'. Preserve live claims."""
    with db:
        cur = db.execute(
            "UPDATE targeted_deliveries SET delivery_state = 'undeliverable' "
            "WHERE expires_at <= ? "
            "  AND delivery_state IN ('pending', 'notified', 'ambiguous', 'blocked')",
            (now,),
        )
        return cur.rowcount


def _fetch_delivery_and_target(db: sqlite3.Connection, *, message_id: str,
                               target_uuid: str, target_incarnation: str,
                               now: int):
    db.row_factory = sqlite3.Row
    delivery = db.execute(
        "SELECT * FROM targeted_deliveries "
        "WHERE message_id = ? AND target_session_uuid = ? "
        "  AND target_session_incarnation_id = ?",
        (message_id, target_uuid, target_incarnation),
    ).fetchone()
    if delivery is None:
        return None, None
    registration = db.execute(
        "SELECT * FROM session_registrations "
        "WHERE session_uuid = ? AND session_incarnation_id = ? "
        "  AND revoked_at IS NULL AND expires_at > ?",
        (target_uuid, target_incarnation, now),
    ).fetchone()
    return delivery, registration


def _revalidate_target(delivery, registration, *, expected_proc_start_ns: Optional[int]) -> bool:
    if registration is None:
        return False
    if registration["state"] not in ("active", "busy", "idle"):
        return False
    if expected_proc_start_ns is not None:
        if registration["proc_start_ns"] != expected_proc_start_ns:
            return False
    return True


def run_one_iteration(
    db: sqlite3.Connection,
    *,
    adapters: Mapping[str, WakeAdapter],
    now: Callable[[], int] = lambda: int(time.time()),
    singleton_lease: SingletonDispatcherLease,
    expected_proc_start_ns: Optional[int] = None,
) -> IterationResult:
    """Run one dispatcher iteration. Returns metrics; does not raise on normal fail-closed paths."""
    now_s = now()

    # Step 1: refresh singleton lease.
    if not singleton_lease.refresh(now=now_s, lease_ttl_seconds=DISPATCHER_LEASE_TTL_SECONDS):
        return IterationResult(status="lease_lost")

    # Step 2: recover expired claimed leases.
    recovered = _recover_expired_claims(db, now=now_s)

    # Step 3: TTL sweep (BEFORE no_work return). Preserves live 'claimed'.
    swept = _ttl_sweep(db, now=now_s)

    # Step 4: claim one eligible delivery.
    claim = claim_delivery(db, now=now_s, lease_ttl_seconds=CLAIM_LEASE_TTL_SECONDS)
    if claim is None:
        return IterationResult(status="no_work", ttl_swept=swept,
                               expired_claims_recovered=recovered)

    # Step 5: revalidate target.
    delivery, registration = _fetch_delivery_and_target(
        db, message_id=claim.message_id,
        target_uuid=claim.target_session_uuid,
        target_incarnation=claim.target_session_incarnation_id,
        now=now_s,
    )
    if delivery is None:
        return IterationResult(
            status="pre_adapter_released",
            diagnostic="delivery_missing",
            ttl_swept=swept, expired_claims_recovered=recovered,
        )
    if not _revalidate_target(delivery, registration,
                              expected_proc_start_ns=expected_proc_start_ns):
        pre_adapter_release(
            db,
            claim_lease_token=claim.claim_lease_token,
            next_state="pending",
            diagnostic="target_not_live",
            next_attempt_at=now_s + _compute_backoff(delivery["attempt_count"]),
        )
        return IterationResult(
            status="pre_adapter_released",
            diagnostic="target_not_live",
            ttl_swept=swept, expired_claims_recovered=recovered,
        )

    # Step 6: resolve adapter.
    wake_capability = registration["wake_capability"]
    adapter = adapters.get(wake_capability)
    if adapter is None:
        pre_adapter_release(
            db,
            claim_lease_token=claim.claim_lease_token,
            next_state="blocked",
            diagnostic="adapter_not_registered",
            next_attempt_at=now_s,
        )
        return IterationResult(
            status="pre_adapter_released",
            diagnostic="adapter_not_registered",
            ttl_swept=swept, expired_claims_recovered=recovered,
        )
    if not adapter.is_enabled():
        pre_adapter_release(
            db,
            claim_lease_token=claim.claim_lease_token,
            next_state="blocked",
            diagnostic="adapter_not_enabled",
            next_attempt_at=now_s,
        )
        return IterationResult(
            status="pre_adapter_released",
            diagnostic="adapter_not_enabled",
            ttl_swept=swept, expired_claims_recovered=recovered,
        )

    # Step 7: call adapter outside the SQLite transaction.
    # Construct a minimal notice. Summary comes from the bridge messages table
    # externally; here we use a fixed template because the dispatcher does not
    # read message bodies (contract: "The dispatcher does not log or interpret
    # message bodies.").
    notice = WakeNotice(
        message_id=claim.message_id,
        channel="(resolved-by-caller)",
        sender_alias_at_send="(resolved-by-caller)",
        summary="",
    )
    attempted_at = now_s
    try:
        outcome = adapter.wake(
            claim.target_session_uuid,
            claim.target_session_incarnation_id,
            notice,
        )
    except Exception as exc:
        # Adapter raised unexpectedly: treat as rejected with a bounded reason.
        reason = f"adapter_exception: {type(exc).__name__}"[:240]
        outcome = "rejected"
    else:
        reason = ""

    # Step 8: finalize.
    finished_at = now()
    if outcome == "accepted_by_cli":
        finalize_reason = ""
        next_at = finished_at   # unused; awaits receipt or TTL
    else:
        finalize_reason = reason or outcome
        next_at = finished_at + _compute_backoff(delivery["attempt_count"] + 1)

    result = finalize_attempt(
        db,
        claim_lease_token=claim.claim_lease_token,
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        outcome=outcome,
        reason=finalize_reason,
        attempted_at=attempted_at,
        now=finished_at,
        next_attempt_at=next_at,
    )

    return IterationResult(
        status="finalized",
        outcome=outcome,
        delivery_state=result.new_state,
        attempts_recorded=1 if result.attempt_recorded else 0,
        ttl_swept=swept,
        expired_claims_recovered=recovered,
        diagnostic=result.status,
    )
