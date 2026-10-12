"""Foreground probe entrypoint for pre-LaunchAgent cost/lock/singleton measurement.

See spec §10.
"""

import resource
import sqlite3
import statistics
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Mapping, Optional

from .adapter import WakeAdapter
from .dispatcher import (
    BUDGET_CPU_PCT_IDLE,
    BUDGET_PEAK_RSS_MB,
    BUDGET_SQLITE_LOCK_WAIT_MS_P95,
    BUDGET_WAKE_FREQ_PER_MIN_IDLE,
    POLL_INTERVAL_IDLE_SECONDS,
    run_one_iteration,
)
from .leases import SingletonDispatcherLease


@dataclass
class ProbeResult:
    status: str                    # 'pass' | 'fail'
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
    now: Callable[[], int] = lambda: int(time.time()),
    poll_interval_seconds: float = POLL_INTERVAL_IDLE_SECONDS,
    budgets: Optional[dict] = None,
) -> ProbeResult:
    """Run the dispatcher loop in the foreground for a bounded duration."""
    db = sqlite3.connect(str(db_path))
    try:
        db.execute("PRAGMA foreign_keys = ON")

        if not singleton_lease.acquire(now=now()):
            raise RuntimeError("could not acquire singleton dispatcher lease")

        start_wall = time.time()
        start_cpu = time.process_time()
        iterations = 0
        attempts_recorded = 0
        ttl_swept_total = 0
        singleton_steals = 0
        lock_wait_samples_ms: list[float] = []
        iteration_timestamps: list[float] = []

        try:
            while True:
                elapsed = time.time() - start_wall
                if elapsed >= duration_seconds:
                    break

                iter_start = time.monotonic()
                result = run_one_iteration(
                    db, adapters=adapters, now=now,
                    singleton_lease=singleton_lease,
                )
                iter_end = time.monotonic()
                lock_wait_samples_ms.append((iter_end - iter_start) * 1000.0)
                iteration_timestamps.append(time.time())
                iterations += 1

                if result.status == "lease_lost":
                    singleton_steals += 1
                    break

                attempts_recorded += result.attempts_recorded
                ttl_swept_total += result.ttl_swept

                # Sleep between iterations.
                remaining = duration_seconds - (time.time() - start_wall)
                sleep_for = min(poll_interval_seconds, max(0, remaining))
                if sleep_for > 0:
                    time.sleep(sleep_for)
        finally:
            singleton_lease.release()

        end_wall = time.time()
        end_cpu = time.process_time()

        # Metrics.
        rss_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # On macOS, ru_maxrss is bytes; on Linux, kilobytes. Normalize.
        import sys
        if sys.platform == "darwin":
            peak_rss_mb = rss_bytes / (1024 * 1024)
        else:
            peak_rss_mb = rss_bytes / 1024

        wall_elapsed = max(1e-9, end_wall - start_wall)
        cpu_elapsed = max(0.0, end_cpu - start_cpu)
        cpu_pct_idle = 100.0 * cpu_elapsed / wall_elapsed if wall_elapsed > 0 else 0.0

        if lock_wait_samples_ms:
            try:
                p95 = statistics.quantiles(lock_wait_samples_ms, n=20)[18]
            except statistics.StatisticsError:
                p95 = max(lock_wait_samples_ms)
        else:
            p95 = 0.0

        wake_freq_per_min = (iterations * 60.0) / wall_elapsed if wall_elapsed > 0 else 0.0

        # Count receipts via a lightweight SELECT.
        receipts = db.execute(
            "SELECT COUNT(*) FROM disposition_receipts "
            "WHERE observed_at >= ?",
            (int(start_wall),),
        ).fetchone()[0]

        budget_results = {
            "peak_rss_mb":              (peak_rss_mb, BUDGET_PEAK_RSS_MB,
                                         peak_rss_mb <= BUDGET_PEAK_RSS_MB),
            "cpu_pct_idle":             (cpu_pct_idle, BUDGET_CPU_PCT_IDLE,
                                         cpu_pct_idle <= BUDGET_CPU_PCT_IDLE),
            "lock_wait_ms_p95":         (p95, BUDGET_SQLITE_LOCK_WAIT_MS_P95,
                                         p95 <= BUDGET_SQLITE_LOCK_WAIT_MS_P95),
            "wake_freq_per_min_idle":   (wake_freq_per_min, BUDGET_WAKE_FREQ_PER_MIN_IDLE,
                                         wake_freq_per_min <= BUDGET_WAKE_FREQ_PER_MIN_IDLE),
            "singleton_steal_attempts": (singleton_steals, 0, singleton_steals == 0),
        }
        all_pass = all(v[2] for v in budget_results.values())

        return ProbeResult(
            status="pass" if all_pass else "fail",
            iterations=iterations,
            attempts=attempts_recorded,
            receipts=receipts,
            ttl_sweeps=ttl_swept_total,
            peak_rss_mb=peak_rss_mb,
            cpu_pct_idle=cpu_pct_idle,
            lock_wait_ms_p95=p95,
            wake_freq_per_min_idle=wake_freq_per_min,
            singleton_steal_attempts=singleton_steals,
            budget_results=budget_results,
        )
    finally:
        db.close()
