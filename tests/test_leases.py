"""SingletonDispatcherLease tests.

v0.5.0: the per-attempt-lease tests that used to live here have moved to
test_leases_owner.py (API shape changed; owner-less call shapes removed per
design R5). The SingletonDispatcherLease class is unchanged by v0.5.0 —
these four tests pin its invariants.
"""

from cross_tool_awareness import SingletonDispatcherLease


def test_singleton_lease_acquire_and_second_fails(db):
    a = SingletonDispatcherLease(db, holder_pid=100, holder_proc_start_ns=1_000)
    b = SingletonDispatcherLease(db, holder_pid=200, holder_proc_start_ns=2_000)
    assert a.acquire(now=1_000_000, lease_ttl_seconds=60)
    assert not b.acquire(now=1_000_010, lease_ttl_seconds=60)


def test_singleton_lease_acquire_after_expiry(db):
    a = SingletonDispatcherLease(db, holder_pid=100, holder_proc_start_ns=1_000)
    b = SingletonDispatcherLease(db, holder_pid=200, holder_proc_start_ns=2_000)
    assert a.acquire(now=1_000_000, lease_ttl_seconds=30)
    assert b.acquire(now=1_000_100, lease_ttl_seconds=30)


def test_singleton_refresh_fails_after_steal(db):
    a = SingletonDispatcherLease(db, holder_pid=100, holder_proc_start_ns=1_000)
    b = SingletonDispatcherLease(db, holder_pid=200, holder_proc_start_ns=2_000)
    assert a.acquire(now=1_000_000, lease_ttl_seconds=30)
    assert b.acquire(now=1_000_100)
    assert not a.refresh(now=1_000_110)


def test_singleton_release_deletes_only_if_held(db):
    a = SingletonDispatcherLease(db, holder_pid=100, holder_proc_start_ns=1_000)
    b = SingletonDispatcherLease(db, holder_pid=200, holder_proc_start_ns=2_000)
    assert a.acquire(now=1_000_000)
    a.release()
    assert b.acquire(now=1_000_010)
