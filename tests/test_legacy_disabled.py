"""Legacy-disablement tripwires (v0.5.0, design R5).

dispatcher.run_one_iteration and probe.run_foreground_probe both raise
RuntimeError immediately on every call, with no side effect:
- no DB connection opened (even to a path that doesn't exist),
- no lease acquire/refresh/release,
- no clock callable invoked,
- no sleep,
- no adapter method called.

Review Focus item 1: duration_seconds=0 still raises before any of the
above. The stubs below fail loudly if touched, so a passing test proves
the raise happened early enough.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cross_tool_awareness import run_foreground_probe, run_one_iteration


class _ExplodingAdapter:
    """Any call to a method on this adapter is a test failure."""

    def deliver(self, *args, **kwargs):
        raise AssertionError("adapter must not be called")

    def __getattr__(self, name):
        def _no(*a, **kw):
            raise AssertionError(f"adapter.{name} must not be called")
        return _no


class _ExplodingLease:
    """Any call on the singleton lease is a test failure."""

    def __getattr__(self, name):
        def _no(*a, **kw):
            raise AssertionError(f"lease.{name} must not be called")
        return _no


def _exploding_clock() -> int:
    raise AssertionError("clock() must not be called")


# --- probe tripwire --------------------------------------------------------


def test_run_foreground_probe_raises_before_any_side_effect(tmp_path):
    """Review Focus item 1: duration_seconds=0 still raises before DB,
    lease, clock, sleep, measurement, or adapter access."""
    db_path = tmp_path / "does-not-exist.db"
    with pytest.raises(RuntimeError, match="legacy dispatcher disabled"):
        run_foreground_probe(
            db_path=db_path,
            adapters={"codex_queue": _ExplodingAdapter()},
            singleton_lease=_ExplodingLease(),
            duration_seconds=0,
            now=_exploding_clock,
        )
    # DB file was never opened/created.
    assert not db_path.exists(), "probe must not open the DB"


def test_run_foreground_probe_raises_on_nonzero_duration(tmp_path):
    db_path = tmp_path / "whatever.db"
    with pytest.raises(RuntimeError, match="legacy dispatcher disabled"):
        run_foreground_probe(
            db_path=db_path,
            adapters={"codex_queue": _ExplodingAdapter()},
            singleton_lease=_ExplodingLease(),
            duration_seconds=600,  # realistic bounded duration
            now=_exploding_clock,
            poll_interval_seconds=5.0,
            budgets={"peak_rss_mb": 48},
        )
    assert not db_path.exists()


# --- dispatcher tripwire ---------------------------------------------------


def test_run_one_iteration_raises_immediately():
    """Even with no arguments at all (default kwargs land as None), the raise
    is the first and only effect."""
    with pytest.raises(RuntimeError, match="legacy dispatcher disabled"):
        run_one_iteration()


def test_run_one_iteration_raises_with_exploding_args():
    """Passing live-looking args still triggers the raise before anything
    touches them."""
    with pytest.raises(RuntimeError, match="legacy dispatcher disabled"):
        run_one_iteration(
            db=None,
            adapters={"codex_queue": _ExplodingAdapter()},
            singleton_lease=_ExplodingLease(),
            notice_factory=lambda a, b: (_ for _ in ()).throw(
                AssertionError("notice_factory must not be called")
            ),
            now=_exploding_clock,
        )


# --- ProbeResult remains importable as inert data ---------------------------


def test_probe_result_remains_importable():
    """ProbeResult kept for v0.4-era saved recordings; v0.5.0 never
    produces one but the dataclass is still constructible."""
    from cross_tool_awareness import ProbeResult
    r = ProbeResult(status="fail", iterations=0, attempts=0, receipts=0,
                    ttl_sweeps=0, peak_rss_mb=0.0, cpu_pct_idle=0.0,
                    lock_wait_ms_p95=0.0, wake_freq_per_min_idle=0.0,
                    singleton_steal_attempts=0)
    assert r.status == "fail"
