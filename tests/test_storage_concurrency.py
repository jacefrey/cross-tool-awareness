"""Two-connection concurrency tests for v0.3.0 storage APIs.

Addresses codex-manager seq=195 B2 and B3: prove that
``_txn.exclusive`` actually serializes two concurrent writers on the same
database file. These tests open two separate sqlite3.Connection objects to a
shared on-disk DB (not :memory:, which would be two separate DBs) and use a
threading.Barrier to make both callers enter the API at (as close as possible
to) the same wall-clock moment.

With BEGIN EXCLUSIVE and SQLite's busy_timeout, one caller acquires the lock
first, completes its atomic SELECT-then-INSERT, and commits. The second
caller's BEGIN EXCLUSIVE blocks until the first releases the lock, then
observes the committed state and returns the deterministic duplicate outcome.
Neither caller raises; both get a well-formed result.
"""

from __future__ import annotations

import sqlite3
import threading
import uuid as _uuid
from pathlib import Path
from typing import Tuple

import pytest

from cross_tool_awareness import apply as apply_migration
from cross_tool_awareness.capability import (
    PresenceExpiringSoonError,
    issue_capability,
    rotate_capability,
)
from cross_tool_awareness.contracts import (
    Disposition,
    DispositionReceipt,
    NoticeKey,
    NoticeObservation,
    PresenceRecord,
)
from cross_tool_awareness.storage import (
    ReceiptStorage,
    create_authenticated_targeted_send,
)


def make_presence(**overrides) -> PresenceRecord:
    defaults = dict(
        session_uuid=_uuid.uuid4(),
        session_incarnation_id=_uuid.uuid4(),
        alias="test-alias-" + _uuid.uuid4().hex[:8],
        tool="codex",
        project_slug="test-project",
        project_root="/tmp/test-project",
        role="manager",
        state="idle",
        wake_capability="codex_queue",
        registration_fence="fence-" + _uuid.uuid4().hex[:8],
        wake_locator="locator-" + _uuid.uuid4().hex[:8],
        started_at=1_000_000,
        heartbeat_at=1_000_010,
        expires_at=1_000_000 + 86400,
    )
    defaults.update(overrides)
    return PresenceRecord(**defaults)


def stub_message_writer(conn, message_id, payload, now):
    conn.execute(
        "INSERT INTO messages (id, payload, created_at) VALUES (?, ?, ?)",
        (message_id, payload, now),
    )


_STUB_MESSAGES_DDL = """
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0
);
"""


def _open(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=5.0)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.row_factory = sqlite3.Row
    return conn


@pytest.fixture
def shared_db(tmp_path) -> Path:
    """A file-backed DB with the full awareness schema + stub messages table."""
    path = tmp_path / "shared.db"
    conn = _open(path)
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.close()
    result = apply_migration(path, backup_dir=tmp_path / "backups", now=1_000_000)
    assert result.status == "applied", f"migration failed: {result.reason}"
    return path


def _register(conn: sqlite3.Connection, presence: PresenceRecord, *, now: int) -> None:
    conn.execute(
        "INSERT INTO session_registrations ("
        " session_uuid, session_incarnation_id, alias, tool, project_slug,"
        " project_root, role, state, wake_capability, registration_fence,"
        " wake_locator, started_at, heartbeat_at, expires_at,"
        " holder_pid, process_started_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (str(presence.session_uuid), str(presence.session_incarnation_id),
         presence.alias, presence.tool, presence.project_slug,
         presence.project_root, presence.role, presence.state,
         presence.wake_capability, presence.registration_fence,
         presence.wake_locator, presence.started_at,
         presence.heartbeat_at, presence.expires_at,
         1, presence.started_at),
    )
    conn.commit()


def _notice(target: PresenceRecord, *, message_id, correlation_id) -> NoticeObservation:
    return NoticeObservation(
        key=NoticeKey(
            message_id=message_id,
            target_session_uuid=target.session_uuid,
            target_session_incarnation_id=target.session_incarnation_id,
        ),
        consumer_id="codex-receiver",
        correlation_id=correlation_id,
        registration_fence=target.registration_fence,
        message_expires_at=1_000_000 + 86400,
    )


# ---------------------------------------------------------------------------
# claim_notice
# ---------------------------------------------------------------------------


def test_claim_notice_two_connection_race_deterministic(shared_db):
    """Two threads call claim_notice on the same key simultaneously.

    Exactly one returns status='first', the other returns 'duplicate'. Neither
    raises IntegrityError (BEGIN EXCLUSIVE serializes the second thread).
    """
    target = make_presence()
    conn_setup = _open(shared_db)
    _register(conn_setup, target, now=1_000_000)
    conn_setup.close()

    msg_id = _uuid.uuid4()
    correlation = _uuid.uuid4()
    notice = _notice(target, message_id=msg_id, correlation_id=correlation)

    barrier = threading.Barrier(2)
    results: list = [None, None]
    errors: list = [None, None]

    def worker(idx: int):
        try:
            conn = _open(shared_db)
            storage = ReceiptStorage(conn)
            barrier.wait(timeout=5.0)
            results[idx] = storage.claim_notice(notice, now=1_000_060, recovery_after=60)
            conn.close()
        except Exception as exc:  # noqa: BLE001
            errors[idx] = exc

    t1 = threading.Thread(target=worker, args=(0,))
    t2 = threading.Thread(target=worker, args=(1,))
    t1.start(); t2.start()
    t1.join(timeout=10.0); t2.join(timeout=10.0)

    assert errors == [None, None], f"worker raised: {errors}"
    statuses = sorted(r.status for r in results)
    assert statuses == ["duplicate", "first"], f"got {statuses}"


# ---------------------------------------------------------------------------
# record_first_disposition
# ---------------------------------------------------------------------------


def test_record_first_disposition_two_connection_race_deterministic(shared_db):
    """Two threads record a disposition on the same key simultaneously.

    Both see the same disposition_receipts row in the end; one is the winner
    (its receipt was inserted) and the other is a reader (observes the first
    winner's receipt). Neither raises.
    """
    target = make_presence()
    conn_setup = _open(shared_db)
    _register(conn_setup, target, now=1_000_000)

    msg_id = _uuid.uuid4()
    correlation = _uuid.uuid4()
    # Priming step: create a targeted_deliveries row in a receipt-eligible
    # state. Receipts are only accepted when delivery is in
    # (claimed, notified, ambiguous).
    conn_setup.execute(
        "INSERT INTO messages (id, payload, created_at) VALUES (?, '', ?)",
        (str(msg_id), 1_000_000),
    )
    conn_setup.execute(
        "INSERT INTO targeted_deliveries "
        "(message_id, target_session_uuid, target_session_incarnation_id,"
        " sender_session_uuid, sender_session_incarnation_id, delivery_state,"
        " created_at, next_attempt_at, expires_at) "
        "VALUES (?, ?, ?, ?, ?, 'claimed', ?, ?, ?)",
        (str(msg_id),
         str(target.session_uuid), str(target.session_incarnation_id),
         str(target.session_uuid), str(target.session_incarnation_id),
         1_000_010, 1_000_010, 1_000_000 + 86400),
    )
    conn_setup.commit()
    conn_setup.close()

    key = NoticeKey(
        message_id=msg_id,
        target_session_uuid=target.session_uuid,
        target_session_incarnation_id=target.session_incarnation_id,
    )

    def _mk_receipt(reason: str) -> DispositionReceipt:
        return DispositionReceipt(
            message_id=msg_id,
            correlation_id=correlation,
            consumer_id="codex-receiver",
            receiver_session_uuid=target.session_uuid,
            receiver_session_incarnation_id=target.session_incarnation_id,
            disposition=Disposition.ACCEPTED,
            observed_at=1_000_080,
            reason=reason,
        )

    barrier = threading.Barrier(2)
    results: list = [None, None]
    errors: list = [None, None]

    def worker(idx: int, reason: str):
        try:
            conn = _open(shared_db)
            storage = ReceiptStorage(conn)
            barrier.wait(timeout=5.0)
            results[idx] = storage.record_first_disposition(key, _mk_receipt(reason))
            conn.close()
        except Exception as exc:  # noqa: BLE001
            errors[idx] = exc

    t1 = threading.Thread(target=worker, args=(0, "reason-a"))
    t2 = threading.Thread(target=worker, args=(1, "reason-b"))
    t1.start(); t2.start()
    t1.join(timeout=10.0); t2.join(timeout=10.0)

    assert errors == [None, None], f"worker raised: {errors}"
    # Both results point to the same durable row (same reason).
    reasons = {r.reason for r in results}
    assert len(reasons) == 1, f"durable receipts diverged: {reasons}"
    assert reasons.pop() in ("reason-a", "reason-b")


# ---------------------------------------------------------------------------
# create_authenticated_targeted_send
# ---------------------------------------------------------------------------


def test_targeted_send_two_connection_duplicate_id_deterministic(shared_db):
    """Two threads send with the same message_id — exactly one wins."""
    sender = make_presence()
    target = make_presence()
    conn_setup = _open(shared_db)
    _register(conn_setup, sender, now=1_000_000)
    _register(conn_setup, target, now=1_000_000)
    cap = issue_capability(
        conn_setup,
        session_uuid=sender.session_uuid,
        session_incarnation_id=sender.session_incarnation_id,
        now=1_000_010,
        entropy_source=lambda n: b"\x7f" * n,
    )
    conn_setup.commit()
    conn_setup.close()

    fixed_msg_id = str(_uuid.uuid4())
    correlation = _uuid.uuid4()

    barrier = threading.Barrier(2)
    results: list = [None, None]
    errors: list = [None, None]

    def worker(idx: int):
        try:
            conn = _open(shared_db)
            barrier.wait(timeout=5.0)
            results[idx] = create_authenticated_targeted_send(
                conn,
                message_payload=f"body-{idx}",
                message_writer=stub_message_writer,
                sender_cap_bearer=cap.present(),
                sender_session_uuid=sender.session_uuid,
                sender_session_incarnation_id=sender.session_incarnation_id,
                sender_registration_fence=sender.registration_fence,
                target_session_uuid=target.session_uuid,
                target_session_incarnation_id=target.session_incarnation_id,
                correlation_id=correlation,
                now=1_000_020 + idx,
                expires_at=1_000_020 + 3600,
                message_id=fixed_msg_id,
            )
            conn.close()
        except Exception as exc:  # noqa: BLE001
            errors[idx] = exc

    t1 = threading.Thread(target=worker, args=(0,))
    t2 = threading.Thread(target=worker, args=(1,))
    t1.start(); t2.start()
    t1.join(timeout=10.0); t2.join(timeout=10.0)

    assert errors == [None, None], f"worker raised: {errors}"
    statuses = sorted(r.status for r in results)
    assert statuses == ["rejected_message_exists", "sent"], f"got {statuses}"

    # Only one messages row and one targeted_deliveries row persist.
    conn_check = _open(shared_db)
    msg_count = conn_check.execute(
        "SELECT COUNT(*) AS c FROM messages WHERE id = ?",
        (fixed_msg_id,),
    ).fetchone()["c"]
    del_count = conn_check.execute(
        "SELECT COUNT(*) AS c FROM targeted_deliveries WHERE message_id = ?",
        (fixed_msg_id,),
    ).fetchone()["c"]
    conn_check.close()
    assert msg_count == 1
    assert del_count == 1


# ---------------------------------------------------------------------------
# rotate_capability atomicity under BEGIN EXCLUSIVE
# ---------------------------------------------------------------------------


def test_rotate_capability_atomicity_on_issue_failure(shared_db):
    """If issue_capability raises mid-rotate, the revoke is rolled back."""
    sender = make_presence()
    conn = _open(shared_db)
    _register(conn, sender, now=1_000_000)
    cap_old = issue_capability(
        conn,
        session_uuid=sender.session_uuid,
        session_incarnation_id=sender.session_incarnation_id,
        now=1_000_010,
        entropy_source=lambda n: b"\xaa" * n,
    )
    conn.commit()

    class _EntropyBoom(RuntimeError):
        pass

    def _bad_entropy(_n):
        raise _EntropyBoom("entropy source failed")

    with pytest.raises(_EntropyBoom):
        rotate_capability(
            conn,
            cap_old.present(),
            expected_sender_uuid=sender.session_uuid,
            expected_incarnation=sender.session_incarnation_id,
            expected_fence=sender.registration_fence,
            now=1_000_050,
            entropy_source=_bad_entropy,
            requested_ttl_seconds=3600,
        )
    conn.close()

    # Verify: old cap row is NOT marked revoked (BEGIN EXCLUSIVE rolled back).
    conn_check = _open(shared_db)
    import hashlib
    verifier = hashlib.sha256(cap_old.present()).digest()
    row = conn_check.execute(
        "SELECT revoked_at FROM session_capabilities WHERE verifier_sha256 = ?",
        (verifier,),
    ).fetchone()
    conn_check.close()
    assert row is not None
    assert row["revoked_at"] is None, "revoke must have rolled back"
