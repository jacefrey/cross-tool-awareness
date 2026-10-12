"""Dispatcher state machine: run_one_iteration implementing the 7-step contract.

v0.2.0 revisions:
- Requires a notice_factory callable; placeholder WakeNotice is gone.
- Adapter.wake exception → outcome='ambiguous_timeout' (not 'rejected').
  The adapter may have delivered; preserve as ambiguous, no blind retry.

See spec §8.
"""

from __future__ import annotations

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


POLL_INTERVAL_IDLE_SECONDS = 30
POLL_INTERVAL_ACTIVE_SECONDS = 2
BACKOFF_SCHEDULE_SECONDS = (0, 5, 20, 60, 300, 1800)
BACKOFF_CAP_SECONDS = 3600
CLAIM_LEASE_TTL_SECONDS = 15
DISPATCHER_LEASE_TTL_SECONDS = 60

BUDGET_PEAK_RSS_MB = 48
BUDGET_CPU_PCT_IDLE = 1.0
BUDGET_WAKE_FREQ_PER_MIN_IDLE = 2
BUDGET_SQLITE_LOCK_WAIT_MS_P95 = 50


# Signature: notice_factory(delivery_row, registration_row) -> WakeNotice
NoticeFactory = Callable[[sqlite3.Row, sqlite3.Row], WakeNotice]


@dataclass
class IterationResult:
    status: str
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


def _revalidate_target(delivery, registration, *, expected_proc_start_ns):
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
    singleton_lease: SingletonDispatcherLease,
    notice_factory: NoticeFactory,
    now: Callable[[], int] = lambda: int(time.time()),
    expected_proc_start_ns: Optional[int] = None,
) -> IterationResult:
    """Run one dispatcher iteration.

    notice_factory is REQUIRED (v0.2.0). The dispatcher refuses to run without
    one; placeholder WakeNotice metadata is no longer acceptable per spec §4
    (no placeholder channel/sender_alias; summary pipes through sanitizer).
    """
    if not callable(notice_factory):
        raise TypeError("notice_factory is required (callable); placeholder notice removed in v0.2.0")

    now_s = now()

    if not singleton_lease.refresh(now=now_s, lease_ttl_seconds=DISPATCHER_LEASE_TTL_SECONDS):
        return IterationResult(status="lease_lost")

    recovered = _recover_expired_claims(db, now=now_s)
    swept = _ttl_sweep(db, now=now_s)

    claim = claim_delivery(db, now=now_s, lease_ttl_seconds=CLAIM_LEASE_TTL_SECONDS)
    if claim is None:
        return IterationResult(status="no_work", ttl_swept=swept,
                               expired_claims_recovered=recovered)

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

    # Caller-supplied notice factory. Resolves channel + sender_alias + summary
    # from the delivery + registration rows (and, in production, the bridge
    # message row). Dispatcher does not read message bodies itself.
    notice = notice_factory(delivery, registration)
    if not isinstance(notice, WakeNotice):
        raise TypeError("notice_factory must return a WakeNotice")

    attempted_at = now_s
    try:
        outcome = adapter.wake(
            claim.target_session_uuid,
            claim.target_session_incarnation_id,
            notice,
        )
    except Exception as exc:
        # Adapter raised. Delivery MAY have happened — preserve as ambiguous
        # per codex-manager v0.2.0 blocker #7. No blind retry.
        reason = f"adapter_exception: {type(exc).__name__}"[:240]
        outcome = "ambiguous_timeout"
        finalize_reason = reason
    else:
        if outcome == "accepted_by_cli":
            finalize_reason = ""
        else:
            finalize_reason = outcome

    finished_at = now()
    if outcome == "accepted_by_cli":
        next_at = finished_at
    else:
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
