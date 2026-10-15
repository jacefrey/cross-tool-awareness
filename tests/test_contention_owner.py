"""Contention + recovery under owner-aware API (v0.5.0, design R8).

Pins:
- Multi-process contention: contenders use separate connections. Only one
  acquires the same eligible pending delivery.
- Expired `claimed` is time-recoverable with a NEW token + owner tuple;
  the old owner is overwritten.
- `dispatching` is NEVER time-recoverable, regardless of age — recovery
  requires explicit owner-initiated release or receipt.
- A wrong-owner operation fails even when the caller supplies the correct
  lease token: the owner-tuple columns gate the UPDATE.
"""

from __future__ import annotations

import multiprocessing as mp
import sqlite3
import uuid
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from cross_tool_awareness import (
    ClaimantIdentity, PresenceRecord, RegistrationSnapshot,
    apply as apply_migration,
)
from cross_tool_awareness.leases import (
    claim_delivery, pre_adapter_mark, pre_adapter_release,
)


_STUB_MESSAGES_DDL = """
CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0
);
"""


def _u() -> str:
    return str(uuid.uuid4())


def _apply_v4(tmp_path: Path) -> Path:
    db_path = tmp_path / "bridge.db"
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    conn = sqlite3.connect(str(db_path))
    conn.executescript(_STUB_MESSAGES_DDL)
    conn.commit()
    conn.close()
    apply_migration(db_path, backup_dir=backup_dir, now=1)
    return db_path


def _open(db_path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(str(db_path), isolation_level=None, timeout=5.0)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    return db


def _presence_from(cid: ClaimantIdentity, *, now: int = 10,
                   tool: str = "claude",
                   wake_capability: str = "notify_only") -> PresenceRecord:
    return PresenceRecord(
        session_uuid=cid.session_uuid,
        session_incarnation_id=cid.session_incarnation_id,
        alias=f"a-{str(cid.session_uuid)[:8]}", tool=tool,
        project_slug="p", project_root="/tmp/p", role="worker",
        state="active", wake_capability=wake_capability,
        registration_fence=cid.registration_fence,
        wake_locator="x",
        started_at=now - 100, heartbeat_at=now, expires_at=now + 86400,
    )


def _snapshot(cid: ClaimantIdentity, *, now: int = 10, holder_pid: int = 1234,
              process_started_at: int = 1_000_000) -> RegistrationSnapshot:
    return RegistrationSnapshot(
        presence=_presence_from(cid, now=now),
        holder_pid=holder_pid, process_started_at=process_started_at,
        proof_observed_at=now,
    )


def _register(db: sqlite3.Connection, cid: ClaimantIdentity, *,
              now: int = 10, holder_pid: int = 1234,
              process_started_at: int = 1_000_000,
              tool: str = "claude",
              wake_capability: str = "notify_only") -> None:
    db.execute(
        "INSERT INTO session_registrations"
        " (session_uuid, session_incarnation_id, alias, tool, project_slug,"
        "  project_root, role, state, wake_capability, registration_fence,"
        "  wake_locator, started_at, heartbeat_at, expires_at, revoked_at,"
        "  holder_pid, process_started_at)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (str(cid.session_uuid), str(cid.session_incarnation_id),
         f"a-{str(cid.session_uuid)[:8]}", tool, "p", "/tmp/p", "worker",
         "active", wake_capability, cid.registration_fence, "x",
         now - 100, now, now + 86400, None,
         holder_pid, process_started_at),
    )


def _seed_pending(db: sqlite3.Connection, target_cid: ClaimantIdentity,
                  *, now: int = 10, next_attempt_at: int | None = None) -> str:
    msg_id = _u()
    db.execute("INSERT INTO messages(id, payload, created_at) VALUES (?,?,?)",
               (msg_id, "{}", now))
    db.execute(
        "INSERT INTO targeted_deliveries"
        " (message_id, target_session_uuid, target_session_incarnation_id,"
        "  sender_session_uuid, sender_session_incarnation_id,"
        "  delivery_state, created_at, next_attempt_at, expires_at)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        (msg_id, str(target_cid.session_uuid),
         str(target_cid.session_incarnation_id),
         _u(), _u(), "pending", now,
         next_attempt_at if next_attempt_at is not None else now,
         now + 3600),
    )
    return msg_id


# --- Expired claimed is recovered with a new token/owner (R8) --------------


def test_expired_claimed_is_recovered_with_new_owner_tuple(tmp_path):
    db_path = _apply_v4(tmp_path)
    db = _open(db_path)
    cid1 = ClaimantIdentity(session_uuid=uuid4(),
                            session_incarnation_id=uuid4(),
                            registration_fence="f1")
    _register(db, cid1)
    target = ClaimantIdentity(session_uuid=uuid4(),
                              session_incarnation_id=uuid4(),
                              registration_fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    msg_id = _seed_pending(db, target)

    claim1 = claim_delivery(
        db, now=10, lease_ttl_seconds=5,
        claimant=cid1, clock=lambda: 10,
        claimant_snapshot=_snapshot(cid1, now=10),
        tool="claude", wake_capability="notify_only",
    )
    assert claim1 is not None

    # Advance past lease expiry; a different claimant can recover.
    cid2 = ClaimantIdentity(session_uuid=uuid4(),
                            session_incarnation_id=uuid4(),
                            registration_fence="f2")
    _register(db, cid2, now=100)
    claim2 = claim_delivery(
        db, now=100, lease_ttl_seconds=30,
        claimant=cid2, clock=lambda: 100,
        claimant_snapshot=_snapshot(cid2, now=100),
        tool="claude", wake_capability="notify_only",
    )
    assert claim2 is not None
    assert claim2.claim_lease_token != claim1.claim_lease_token
    assert claim2.claimant_session_uuid == str(cid2.session_uuid)
    assert claim2.claimant_registration_fence == "f2"
    row = db.execute(
        "SELECT claim_lease_token, claimant_session_uuid,"
        " claimant_registration_fence"
        " FROM targeted_deliveries WHERE message_id = ?", (msg_id,),
    ).fetchone()
    assert row["claim_lease_token"] == claim2.claim_lease_token
    assert row["claimant_session_uuid"] == str(cid2.session_uuid)
    assert row["claimant_registration_fence"] == "f2"
    db.close()


def test_unexpired_claimed_not_recovered_by_time(tmp_path):
    db_path = _apply_v4(tmp_path)
    db = _open(db_path)
    cid1 = ClaimantIdentity(session_uuid=uuid4(),
                            session_incarnation_id=uuid4(),
                            registration_fence="f1")
    _register(db, cid1)
    target = ClaimantIdentity(session_uuid=uuid4(),
                              session_incarnation_id=uuid4(),
                              registration_fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    _seed_pending(db, target)
    claim1 = claim_delivery(
        db, now=10, lease_ttl_seconds=300,
        claimant=cid1, clock=lambda: 10,
        claimant_snapshot=_snapshot(cid1, now=10),
        tool="claude", wake_capability="notify_only",
    )
    assert claim1 is not None
    cid2 = ClaimantIdentity(session_uuid=uuid4(),
                            session_incarnation_id=uuid4(),
                            registration_fence="f2")
    _register(db, cid2, now=50)
    claim2 = claim_delivery(
        db, now=50, lease_ttl_seconds=30,
        claimant=cid2, clock=lambda: 50,
        claimant_snapshot=_snapshot(cid2, now=50),
        tool="claude", wake_capability="notify_only",
    )
    assert claim2 is None, "unexpired claim must not be stolen by time"
    db.close()


def test_dispatching_never_time_recovered(tmp_path):
    """R8 critical invariant: a dispatching row is NEVER re-claimable by
    time, even if we skip arbitrarily far into the future. Recovery requires
    an explicit owner-initiated path (release or receipt)."""
    db_path = _apply_v4(tmp_path)
    db = _open(db_path)
    cid = ClaimantIdentity(session_uuid=uuid4(),
                           session_incarnation_id=uuid4(),
                           registration_fence="f1")
    _register(db, cid)
    target = ClaimantIdentity(session_uuid=uuid4(),
                              session_incarnation_id=uuid4(),
                              registration_fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    msg_id = _seed_pending(db, target)
    claim = claim_delivery(
        db, now=10, lease_ttl_seconds=30,
        claimant=cid, clock=lambda: 10,
        claimant_snapshot=_snapshot(cid, now=10),
        tool="claude", wake_capability="notify_only",
    )
    assert claim is not None
    ok = pre_adapter_mark(
        db, message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        lease_token=claim.claim_lease_token, now=11,
        claimant=cid, clock=lambda: 11,
        claimant_snapshot=_snapshot(cid, now=11),
        target_snapshot=_snapshot(target, now=11, holder_pid=5678,
                                   process_started_at=2_000_000),
        tool="claude", wake_capability="notify_only",
    )
    assert ok
    # Far-future contender cannot steal it.
    cid2 = ClaimantIdentity(session_uuid=uuid4(),
                            session_incarnation_id=uuid4(),
                            registration_fence="f2")
    _register(db, cid2, now=1_000_000)
    stolen = claim_delivery(
        db, now=1_000_000, lease_ttl_seconds=30,
        claimant=cid2, clock=lambda: 1_000_000,
        claimant_snapshot=_snapshot(cid2, now=1_000_000),
        tool="claude", wake_capability="notify_only",
    )
    assert stolen is None, "dispatching must never be time-recovered"
    row = db.execute(
        "SELECT delivery_state, claimant_session_uuid"
        " FROM targeted_deliveries WHERE message_id = ?", (msg_id,),
    ).fetchone()
    assert row["delivery_state"] == "dispatching"
    assert row["claimant_session_uuid"] == str(cid.session_uuid)
    db.close()


# --- Wrong-owner pre_adapter_release fails even with correct token ---------


def test_pre_adapter_release_wrong_owner_fails(tmp_path):
    db_path = _apply_v4(tmp_path)
    db = _open(db_path)
    cid = ClaimantIdentity(session_uuid=uuid4(),
                           session_incarnation_id=uuid4(),
                           registration_fence="f1")
    other = ClaimantIdentity(session_uuid=uuid4(),
                             session_incarnation_id=uuid4(),
                             registration_fence="other-f")
    _register(db, cid)
    _register(db, other)
    target = ClaimantIdentity(session_uuid=uuid4(),
                              session_incarnation_id=uuid4(),
                              registration_fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    msg_id = _seed_pending(db, target)
    claim = claim_delivery(
        db, now=10, lease_ttl_seconds=300,
        claimant=cid, clock=lambda: 10,
        claimant_snapshot=_snapshot(cid, now=10),
        tool="claude", wake_capability="notify_only",
    )
    assert claim is not None

    # Impersonating owner knows the correct token (read from DB) but is
    # not the stamped owner.
    result = pre_adapter_release(
        db, claim_lease_token=claim.claim_lease_token,
        next_state="pending", diagnostic="forged",
        next_attempt_at=100,
        claimant=other, clock=lambda: 11,
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
    )
    assert result.status == "lease_released_or_observed"
    # Delivery still claimed under the original owner.
    row = db.execute(
        "SELECT delivery_state, claim_lease_token, claimant_session_uuid"
        " FROM targeted_deliveries WHERE message_id = ?", (msg_id,),
    ).fetchone()
    assert row["delivery_state"] == "claimed"
    assert row["claim_lease_token"] == claim.claim_lease_token
    assert row["claimant_session_uuid"] == str(cid.session_uuid)
    db.close()


# --- Multi-process contention: only one wins ------------------------------


def _worker(db_path: str, claimant_state: dict, now: int, out: mp.Queue):
    try:
        import sqlite3
        from uuid import UUID
        from cross_tool_awareness import (
            ClaimantIdentity, PresenceRecord, RegistrationSnapshot,
        )
        from cross_tool_awareness.leases import claim_delivery
        cid = ClaimantIdentity(
            session_uuid=UUID(claimant_state["uuid"]),
            session_incarnation_id=UUID(claimant_state["inc"]),
            registration_fence=claimant_state["fence"],
        )
        presence = PresenceRecord(
            session_uuid=cid.session_uuid,
            session_incarnation_id=cid.session_incarnation_id,
            alias=claimant_state["alias"], tool="claude",
            project_slug="p", project_root="/tmp/p", role="worker",
            state="active", wake_capability="notify_only",
            registration_fence=cid.registration_fence,
            wake_locator="x",
            started_at=now - 100, heartbeat_at=now, expires_at=now + 86400,
        )
        snap = RegistrationSnapshot(
            presence=presence, holder_pid=1234,
            process_started_at=1_000_000, proof_observed_at=now,
        )
        conn = sqlite3.connect(db_path, isolation_level=None, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            claim = claim_delivery(
                conn, now=now, lease_ttl_seconds=30,
                claimant=cid, clock=lambda: now,
                claimant_snapshot=snap,
                tool="claude", wake_capability="notify_only",
            )
            out.put(("ok", claim is not None,
                     claim.claim_lease_token if claim else None))
        finally:
            conn.close()
    except Exception as exc:
        out.put(("err", type(exc).__name__, str(exc)))


def test_multi_process_contention_only_one_claims(tmp_path):
    db_path = _apply_v4(tmp_path)
    db = _open(db_path)
    # Register four separate claimant identities (one per worker) + target.
    claimants = []
    for i in range(4):
        cid = ClaimantIdentity(session_uuid=uuid4(),
                               session_incarnation_id=uuid4(),
                               registration_fence=f"f{i}")
        _register(db, cid)
        claimants.append({
            "uuid": str(cid.session_uuid),
            "inc": str(cid.session_incarnation_id),
            "fence": f"f{i}",
            "alias": f"c-{i}",
        })
    target = ClaimantIdentity(session_uuid=uuid4(),
                              session_incarnation_id=uuid4(),
                              registration_fence="target-f")
    _register(db, target, holder_pid=5678, process_started_at=2_000_000)
    _seed_pending(db, target)
    db.close()

    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    procs = [ctx.Process(target=_worker, args=(str(db_path), c, 10, q))
             for c in claimants]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=30.0)
        assert not p.is_alive(), "worker hung"

    results = []
    while not q.empty():
        results.append(q.get(timeout=1.0))
    successes = [r for r in results
                 if r[0] == "ok" and r[1] is True and r[2] is not None]
    assert len(successes) == 1, (
        f"exactly one process must claim the sole pending delivery; "
        f"got {results}"
    )
    # Three "ok" with claim=None (no work for them) and one with a claim.
    nulls = [r for r in results if r[0] == "ok" and r[1] is False]
    assert len(nulls) == 3, f"three must get no-work; got {results}"
