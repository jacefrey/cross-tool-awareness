"""R5 §3.2 / R7 (I03): claim and mark compare BOTH snapshots to the current
registration rows inside the write transaction (every PresenceRecord field
except heartbeat_at, plus PID/start), require an eligible unexpired unrevoked
row, and enforce the facade route. Snapshots are built from the registered row;
drift is produced by mutating the row AFTER the snapshot was taken."""
from __future__ import annotations

from dataclasses import replace

import pytest

from cross_tool_awareness.contracts import PresenceState
from cross_tool_awareness.leases import claim_delivery, pre_adapter_mark
from tests._owner_fixtures import (
    fresh_db_path, identity, open_db, register, row, seed_pending, snapshot_from_row,
)


def _seed(tmp_path):
    p = fresh_db_path(tmp_path)
    db = open_db(p)
    cid, target = identity(), identity("target-f")
    register(db, cid)
    register(db, target, holder_pid=5678, process_started_at=2_000_000)
    mid = seed_pending(db, target)
    return db, cid, target, mid


def _claim(db, cid, snap):
    return claim_delivery(db, now=10, claimant=cid, clock=lambda: 10, claimant_snapshot=snap,
                          tool="claude", wake_capability="notify_only")


def _mark(db, cid, target, claim, *, cs=None, ts=None, tool="claude",
          wake_capability="notify_only", clock=10):
    return pre_adapter_mark(
        db, message_id=claim.message_id, target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        lease_token=claim.claim_lease_token, now=10, claimant=cid, clock=lambda: clock,
        claimant_snapshot=cs if cs is not None else snapshot_from_row(db, cid, proof_observed_at=10),
        target_snapshot=ts if ts is not None else snapshot_from_row(db, target, proof_observed_at=10),
        tool=tool, wake_capability=wake_capability)


def _set(db, cid, column, value):
    db.execute(f"UPDATE session_registrations SET {column}=? WHERE session_uuid=?",
               (value, str(cid.session_uuid)))


def test_claim_refuses_snapshot_of_another_registration(tmp_path):
    db, cid, target, mid = _seed(tmp_path)
    assert _claim(db, cid, snapshot_from_row(db, target, proof_observed_at=10)) is None
    assert row(db, mid)["delivery_state"] == "pending"


def test_claim_refuses_claimant_mutated_to_stopping_pid0(tmp_path):
    db, cid, target, mid = _seed(tmp_path)
    snap = snapshot_from_row(db, cid, proof_observed_at=10)
    db.execute("UPDATE session_registrations SET state='stopping', holder_pid=0 WHERE session_uuid=?",
               (str(cid.session_uuid),))
    assert _claim(db, cid, snap) is None
    assert row(db, mid)["delivery_state"] == "pending"


def test_mark_refuses_claimant_pid_drift(tmp_path):
    db, cid, target, mid = _seed(tmp_path)
    claim = _claim(db, cid, snapshot_from_row(db, cid, proof_observed_at=10))
    cs = snapshot_from_row(db, cid, proof_observed_at=10)
    _set(db, cid, "holder_pid", 999)
    assert _mark(db, cid, target, claim, cs=cs) is False
    assert row(db, mid)["delivery_state"] == "claimed"


@pytest.mark.parametrize("column,value", [
    ("registration_fence", "rotated"), ("project_root", "/tmp/other"), ("state", "stopping"),
    ("tool", "codex"), ("wake_capability", "codex_queue"), ("alias", "renamed"),
    ("role", "manager"), ("wake_locator", "y"), ("started_at", -200), ("expires_at", 10 + 86400 + 5),
])
def test_mark_refuses_target_field_drift(tmp_path, column, value):
    db, cid, target, mid = _seed(tmp_path)
    claim = _claim(db, cid, snapshot_from_row(db, cid, proof_observed_at=10))
    ts = snapshot_from_row(db, target, proof_observed_at=10)
    _set(db, target, column, value)
    assert _mark(db, cid, target, claim, ts=ts) is False
    assert row(db, mid)["delivery_state"] == "claimed"


def test_mark_tolerates_heartbeat_only_change(tmp_path):
    db, cid, target, mid = _seed(tmp_path)
    claim = _claim(db, cid, snapshot_from_row(db, cid, proof_observed_at=10))
    ts = snapshot_from_row(db, target, proof_observed_at=10)
    db.execute("UPDATE session_registrations SET heartbeat_at=heartbeat_at+1 WHERE session_uuid=?",
               (str(target.session_uuid),))
    assert _mark(db, cid, target, claim, ts=ts) is True
    assert row(db, mid)["delivery_state"] == "dispatching"


def test_mark_refuses_extended_expires_at_until_resnapshot(tmp_path):
    """Review Focus 3 / ruling R-S: expires_at is compared; a fresh trusted snapshot
    taken after the extension marks successfully while the claim is still owned."""
    db, cid, target, mid = _seed(tmp_path)
    claim = _claim(db, cid, snapshot_from_row(db, cid, proof_observed_at=10))
    stale = snapshot_from_row(db, target, proof_observed_at=10)
    db.execute("UPDATE session_registrations SET expires_at=expires_at+60 WHERE session_uuid=?",
               (str(target.session_uuid),))
    assert _mark(db, cid, target, claim, ts=stale) is False
    assert row(db, mid)["delivery_state"] == "claimed"
    fresh = snapshot_from_row(db, target, proof_observed_at=10)
    assert _mark(db, cid, target, claim, ts=fresh) is True


def test_mark_refuses_wrong_route_arguments(tmp_path):
    db, cid, target, mid = _seed(tmp_path)
    claim = _claim(db, cid, snapshot_from_row(db, cid, proof_observed_at=10))
    assert _mark(db, cid, target, claim, tool="codex", wake_capability="codex_queue") is False
    assert row(db, mid)["delivery_state"] == "claimed"


@pytest.mark.parametrize("state_value", ["active", PresenceState("active")], ids=["str", "enum"])
def test_mark_accepts_enum_or_str_state(tmp_path, state_value):
    """Review Focus 2."""
    db, cid, target, mid = _seed(tmp_path)
    claim = _claim(db, cid, snapshot_from_row(db, cid, proof_observed_at=10))
    ts = snapshot_from_row(db, target, proof_observed_at=10)
    ts = replace(ts, presence=replace(ts.presence, state=state_value))
    assert _mark(db, cid, target, claim, ts=ts) is True
    assert row(db, mid)["delivery_state"] == "dispatching"


@pytest.mark.parametrize("case", ["revoked", "expired"])
def test_mark_refuses_revoked_or_expired_claimant(tmp_path, case):
    db, cid, target, mid = _seed(tmp_path)
    claim = _claim(db, cid, snapshot_from_row(db, cid, proof_observed_at=10))
    if case == "revoked":
        _set(db, cid, "revoked_at", 11)
    else:
        _set(db, cid, "expires_at", 10)       # equality at `now`=10 is expired
    cs = snapshot_from_row(db, cid, proof_observed_at=10)  # matches the mutated row exactly
    assert _mark(db, cid, target, claim, cs=cs) is False
    assert row(db, mid)["delivery_state"] == "claimed"


def test_claim_captures_target_fence(tmp_path):
    db, cid, target, mid = _seed(tmp_path)
    statements: list[str] = []
    db.set_trace_callback(statements.append)
    claim = _claim(db, cid, snapshot_from_row(db, cid, proof_observed_at=10))
    db.set_trace_callback(None)
    assert claim is not None and claim.target_registration_fence == "target-f"
    begins = [i for i, s in enumerate(statements) if s.startswith("BEGIN IMMEDIATE")]
    assert len(begins) == 1
    select = next(i for i, s in enumerate(statements)
                  if s.startswith("SELECT") and "target_registration_fence" in s)
    update = next(i for i, s in enumerate(statements) if s.startswith("UPDATE targeted_deliveries"))
    commit = next(i for i, s in enumerate(statements) if s.startswith("COMMIT"))
    assert begins[0] < select < update < commit
