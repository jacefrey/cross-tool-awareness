"""Internal: explicit BEGIN EXCLUSIVE transaction scope.

Python sqlite3's `with conn:` and default implicit `BEGIN` use DEFERRED mode.
Under DEFERRED, a transaction acquires only a SHARED lock on the first read
and upgrades to RESERVED/EXCLUSIVE only on first write. A SELECT-then-INSERT
pattern under DEFERRED can race against a concurrent writer: both connections
read the same empty state, both try to INSERT, and the loser gets
sqlite3.IntegrityError.

The ``exclusive`` context manager below starts a true BEGIN EXCLUSIVE: the
file lock is acquired up front, so all other connections block (up to the
busy_timeout) until this block commits or rolls back. Any SELECTs inside the
block see a consistent snapshot that cannot be mutated by a concurrent writer.

Usage:

    with exclusive(db):
        row = db.execute("SELECT ...").fetchone()
        if row is None:
            db.execute("INSERT ...")

The context manager:

1. Commits any in-flight implicit transaction on entry. Python's sqlite3
   starts an implicit DEFERRED tx on first DML; a caller who did any writes
   before invoking a storage API still has that tx open. Our APIs are atomic
   entry points — each is the transaction boundary — so persisting the
   caller's pre-existing work before we begin EXCLUSIVE is the right behavior.
   If a caller intentionally wants to roll back pending work, they must do
   so before calling us.
2. Temporarily sets ``conn.isolation_level = None`` so Python's auto-BEGIN
   does not fight the explicit ``BEGIN EXCLUSIVE``.
3. Explicitly ``BEGIN EXCLUSIVE`` / ``COMMIT`` / ``ROLLBACK`` — never relies
   on Python's implicit transaction management inside the block.
4. Restores the prior isolation_level on exit, win or lose.

Rolls back on any exception (including BaseException subclasses such as
KeyboardInterrupt) and re-raises.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def exclusive(db: sqlite3.Connection) -> Iterator[None]:
    """Acquire BEGIN EXCLUSIVE for the duration of the block."""
    if db.in_transaction:
        db.commit()
    saved_isolation = db.isolation_level
    db.isolation_level = None
    try:
        db.execute("BEGIN EXCLUSIVE")
        try:
            yield
        except BaseException:
            db.execute("ROLLBACK")
            raise
        else:
            db.execute("COMMIT")
    finally:
        db.isolation_level = saved_isolation
