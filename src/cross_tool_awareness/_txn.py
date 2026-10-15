"""Internal: explicit package-owned transaction scopes.

Python sqlite3's ``with conn:`` and its implicit ``BEGIN`` use DEFERRED mode,
under which a SELECT-then-write pattern can race a concurrent writer. The two
context managers below issue explicit ``BEGIN IMMEDIATE`` / ``BEGIN EXCLUSIVE``
so the file lock is taken up front.

Envelope contract (design R6, v0.5.1 ruling R-E):

- The body AND the ``COMMIT`` sit inside the rollback boundary. A failed
  ``COMMIT`` is rolled back like any other failure.
- Any ``BaseException`` (including ``KeyboardInterrupt`` / ``SystemExit``)
  triggers ``ROLLBACK`` and then propagates.
- If ``ROLLBACK`` itself fails, the connection is closed so the unresolved
  transaction can never be committed by a later statement or attribute
  assignment; the primary exception still propagates.
- ``isolation_level`` is never assigned while a transaction may be open:
  CPython's setter commits an open transaction when assigned ``None``.
  Once an explicit ``BEGIN`` has run, ``in_transaction`` is true and the
  module issues no implicit ``BEGIN`` of its own, so no toggling is needed.

``immediate`` refuses a caller-owned transaction (the package must not commit
unrelated caller work). ``exclusive`` keeps its historical entry semantics:
in-flight caller work is committed before ``BEGIN EXCLUSIVE``.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator


def _run_owned(db: sqlite3.Connection, begin_sql: str) -> Iterator[None]:
    db.execute(begin_sql)
    try:
        yield
        db.execute("COMMIT")
    except BaseException:
        # SQLITE_FULL / INTERRUPT / IOERR already rolled the transaction back
        # inside SQLite; only an UNRESOLVED transaction needs our ROLLBACK, and
        # only a transaction that survives a failed ROLLBACK justifies
        # invalidating the consumer's connection.
        if _in_transaction(db):
            try:
                db.execute("ROLLBACK")
            except sqlite3.Error:
                if _in_transaction(db):
                    try:
                        db.close()   # never let a later statement or setter commit this
                    except sqlite3.Error:
                        pass
        raise


def _in_transaction(db: sqlite3.Connection) -> bool:
    try:
        return db.in_transaction
    except sqlite3.ProgrammingError:   # already closed
        return False


@contextmanager
def immediate(db: sqlite3.Connection) -> Iterator[None]:
    """Own one BEGIN IMMEDIATE transaction; refuse caller-owned work."""
    if db.in_transaction:
        raise sqlite3.OperationalError(
            "BEGIN IMMEDIATE requires no active caller transaction"
        )
    yield from _run_owned(db, "BEGIN IMMEDIATE")


@contextmanager
def exclusive(db: sqlite3.Connection) -> Iterator[None]:
    """Own one BEGIN EXCLUSIVE transaction; commit in-flight caller work first."""
    if db.in_transaction:
        db.commit()
    yield from _run_owned(db, "BEGIN EXCLUSIVE")
