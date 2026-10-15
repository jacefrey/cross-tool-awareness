"""Foreground probe entrypoint — DISABLED in v0.5.0.

The pre-v0.5 probe exercised an owner-less dispatcher iteration loop as the
pre-LaunchAgent cost/lock/singleton measurement surface. v0.5.0 (design R5)
disables that path: ``run_foreground_probe`` raises ``RuntimeError`` before
any side effect — before DB connection, lease acquisition, clock calls,
sleeping, measurement, or adapter access — including a zero-duration call.

The replacement claimant-aware bounded runner is a later roadmap gate.
``ProbeResult`` remains importable as an inert data type for consumers that
destructure results from older recordings; ``run_foreground_probe`` is kept
in ``__all__`` for symbol stability but fails closed on every call.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Mapping, Optional

from .adapter import WakeAdapter
# NB: dispatcher.* budgets are preserved for __init__ re-exports; this module
# does not consume them in the disabled body, but importing them keeps the
# cross-module symbol surface stable for existing recordings.
from .dispatcher import (  # noqa: F401  (re-exported by package __init__)
    BUDGET_CPU_PCT_IDLE,
    BUDGET_PEAK_RSS_MB,
    BUDGET_SQLITE_LOCK_WAIT_MS_P95,
    BUDGET_WAKE_FREQ_PER_MIN_IDLE,
    POLL_INTERVAL_IDLE_SECONDS,
)
from .leases import SingletonDispatcherLease  # noqa: F401  (API-shape stability)


_LEGACY_DISABLED_REASON = (
    "legacy dispatcher disabled: claimant-aware bounded runner required"
)


@dataclass
class ProbeResult:
    """Inert data type retained for v0.4-era consumers that destructure
    saved probe results. v0.5.0 never produces one — run_foreground_probe
    raises before any measurement — but the dataclass is kept importable."""

    status: str
    iterations: int
    attempts: int
    receipts: int
    ttl_sweeps: int
    peak_rss_mb: float
    cpu_pct_idle: float
    lock_wait_ms_p95: float
    wake_freq_per_min_idle: float
    singleton_steal_attempts: int
    budget_results: dict = field(default_factory=dict)


def run_foreground_probe(
    *,
    db_path: Path,
    adapters: Mapping[str, WakeAdapter],
    singleton_lease: SingletonDispatcherLease,
    duration_seconds: int,
    now: Callable[[], int] = None,  # noqa: RUF013 (keep API shape)
    poll_interval_seconds: float = POLL_INTERVAL_IDLE_SECONDS,
    budgets: Optional[dict] = None,
) -> ProbeResult:
    """DISABLED in v0.5.0 — raises RuntimeError before any side effect.

    No DB connection is opened. No lease is acquired. No clock is called
    (``now`` is never invoked). No sleep occurs. No adapter is called. The
    body contains zero logic beyond the raise, so ``duration_seconds=0``
    and any adapter/lease/clock argument shape still raises.
    """
    raise RuntimeError(_LEGACY_DISABLED_REASON)
