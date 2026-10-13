"""C7 receiver-owned process-liveness capability verification."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from uuid import uuid4

import pytest

from cross_tool_awareness import (
    issue_capability,
    verify_capability,
    verify_capability_with_liveness,
)
from cross_tool_awareness.registry import register_session


@dataclass(frozen=True)
class Proof:
    pid: int
    process_started_at: int
    observed_at: int
    alive: bool = True


def _fixed_entropy(n):
    assert n == 32
    return b"\xc7" * 32


def _setup(db, make_presence_fn, *, holder_pid=4321, process_started_at=999_900):
    presence = make_presence_fn()
    register_session(
        db,
        presence,
        now=1_000_000,
        holder_pid=holder_pid,
        process_started_at=process_started_at,
    )
    capability = issue_capability(
        db,
        session_uuid=presence.session_uuid,
        session_incarnation_id=presence.session_incarnation_id,
        now=1_000_010,
        entropy_source=_fixed_entropy,
    )
    db.commit()
    return presence, capability


def _verify(db, presence, capability, observe_process, **overrides):
    kwargs = {
        "expected_sender_uuid": presence.session_uuid,
        "expected_incarnation": presence.session_incarnation_id,
        "expected_fence": presence.registration_fence,
        "now": 1_000_100,
        "max_proof_age_seconds": 10,
        "observe_process": observe_process,
    }
    kwargs.update(overrides)
    return verify_capability_with_liveness(
        db,
        capability.present(),
        **kwargs,
    )


def test_public_signature_is_exact():
    signature = inspect.signature(verify_capability_with_liveness)
    assert list(signature.parameters) == [
        "db",
        "presented_bearer",
        "expected_sender_uuid",
        "expected_incarnation",
        "expected_fence",
        "now",
        "max_proof_age_seconds",
        "observe_process",
    ]
    for name in list(signature.parameters)[2:]:
        assert signature.parameters[name].kind is inspect.Parameter.KEYWORD_ONLY


def test_valid_bearer_live_sender_and_fresh_receiver_proof_are_accepted(
    db, make_presence_fn
):
    presence, capability = _setup(db, make_presence_fn)
    calls = []
    statements = []
    db.set_trace_callback(statements.append)

    def probe(pid, started_at):
        assert db.in_transaction is False
        calls.append((pid, started_at))
        return Proof(pid, started_at, observed_at=1_000_095)

    decision = _verify(db, presence, capability, probe)

    assert (decision.allowed, decision.reason) == (True, "accepted")
    assert calls == [(4321, 999_900)]
    assert db.in_transaction is False
    assert [sql for sql in statements if sql == "BEGIN IMMEDIATE"] == [
        "BEGIN IMMEDIATE",
        "BEGIN IMMEDIATE",
    ]


def test_newer_heartbeat_does_not_replace_os_proof_freshness(
    db, make_presence_fn
):
    presence, capability = _setup(db, make_presence_fn)
    db.execute(
        "UPDATE session_registrations SET heartbeat_at = ? "
        "WHERE session_uuid = ?",
        (1_000_099, str(presence.session_uuid)),
    )
    db.commit()

    decision = _verify(
        db,
        presence,
        capability,
        lambda pid, started: Proof(pid, started, observed_at=1_000_095),
    )

    assert (decision.allowed, decision.reason) == (True, "accepted")


@pytest.mark.parametrize("state", ["stopping", "stale"])
def test_non_live_sender_fails_before_probe(db, make_presence_fn, state):
    presence, capability = _setup(db, make_presence_fn)
    db.execute(
        "UPDATE session_registrations SET state = ? WHERE session_uuid = ?",
        (state, str(presence.session_uuid)),
    )
    db.commit()
    called = False

    def probe(pid, started_at):
        nonlocal called
        called = True
        return Proof(pid, started_at, observed_at=1_000_100)

    decision = _verify(db, presence, capability, probe)

    assert (decision.allowed, decision.reason) == (False, "sender_not_live")
    assert called is False


def test_missing_sender_fails_before_probe(db, make_presence_fn):
    presence, capability = _setup(db, make_presence_fn)
    db.execute("PRAGMA foreign_keys = OFF")
    db.execute(
        "DELETE FROM session_registrations WHERE session_uuid = ?",
        (str(presence.session_uuid),),
    )
    db.commit()

    decision = _verify(
        db,
        presence,
        capability,
        lambda *_: pytest.fail("probe must not run"),
    )

    assert (decision.allowed, decision.reason) == (False, "sender_not_live")


def test_legacy_pid_zero_stale_row_fails_before_probe(db, make_presence_fn):
    presence, capability = _setup(db, make_presence_fn)
    db.execute(
        "UPDATE session_registrations SET state = 'stale', holder_pid = 0 "
        "WHERE session_uuid = ?",
        (str(presence.session_uuid),),
    )
    db.commit()

    decision = _verify(
        db,
        presence,
        capability,
        lambda *_: pytest.fail("probe must not run"),
    )

    assert (decision.allowed, decision.reason) == (False, "sender_not_live")


@pytest.mark.parametrize("observed_at", [1_000_089, 1_000_101])
def test_stale_or_future_proof_fails_closed(
    db, make_presence_fn, observed_at
):
    presence, capability = _setup(db, make_presence_fn)
    decision = _verify(
        db,
        presence,
        capability,
        lambda pid, started: Proof(pid, started, observed_at),
    )
    assert (decision.allowed, decision.reason) == (False, "proof_stale")


@pytest.mark.parametrize(
    ("pid", "started_at"),
    [(4322, 999_900), (4321, 999_899)],
)
def test_process_identity_mismatch_fails_closed(
    db, make_presence_fn, pid, started_at
):
    presence, capability = _setup(db, make_presence_fn)
    decision = _verify(
        db,
        presence,
        capability,
        lambda *_: Proof(pid, started_at, 1_000_095),
    )
    assert (decision.allowed, decision.reason) == (
        False,
        "process_identity_mismatch",
    )


@pytest.mark.parametrize("result", [None, Proof(4321, 999_900, 1_000_095, False)])
def test_missing_or_dead_probe_result_fails_closed(db, make_presence_fn, result):
    presence, capability = _setup(db, make_presence_fn)
    decision = _verify(db, presence, capability, lambda *_: result)
    assert (decision.allowed, decision.reason) == (False, "probe_failure")


def test_probe_exception_fails_closed(db, make_presence_fn):
    presence, capability = _setup(db, make_presence_fn)

    def probe(*_):
        raise OSError("process table unavailable")

    decision = _verify(db, presence, capability, probe)
    assert (decision.allowed, decision.reason) == (False, "probe_failure")


@pytest.mark.parametrize("mutation", ["fence", "state", "pid", "start"])
def test_registration_drift_between_reads_fails_closed(
    db, make_presence_fn, mutation
):
    presence, capability = _setup(db, make_presence_fn)

    def probe(pid, started_at):
        assert db.in_transaction is False
        assignments = {
            "fence": ("registration_fence = ?", "rotated-fence"),
            "state": ("state = ?", "stopping"),
            "pid": ("holder_pid = ?", pid + 1),
            "start": ("process_started_at = ?", started_at + 1),
        }
        sql, value = assignments[mutation]
        db.execute(
            f"UPDATE session_registrations SET {sql} WHERE session_uuid = ?",
            (value, str(presence.session_uuid)),
        )
        db.commit()
        return Proof(pid, started_at, 1_000_095)

    decision = _verify(db, presence, capability, probe)

    expected = (
        "process_identity_mismatch"
        if mutation in {"pid", "start"}
        else "sender_not_live"
    )
    assert (decision.allowed, decision.reason) == (False, expected)


def test_invalid_bearer_is_rejected_without_probe(db, make_presence_fn):
    presence, capability = _setup(db, make_presence_fn)
    decision = verify_capability_with_liveness(
        db,
        b"bad",
        expected_sender_uuid=presence.session_uuid,
        expected_incarnation=presence.session_incarnation_id,
        expected_fence=presence.registration_fence,
        now=1_000_100,
        max_proof_age_seconds=10,
        observe_process=lambda *_: pytest.fail("probe must not run"),
    )
    assert decision.reason == "invalid_contract"


@pytest.mark.parametrize("bad", [0, -1, True, 1.5, None])
def test_invalid_max_proof_age_rejected(db, make_presence_fn, bad):
    presence, capability = _setup(db, make_presence_fn)
    with pytest.raises(ValueError, match="max_proof_age_seconds"):
        _verify(
            db,
            presence,
            capability,
            lambda *_: None,
            max_proof_age_seconds=bad,
        )


def test_bearer_only_verifier_remains_liveness_agnostic(db, make_presence_fn):
    presence, capability = _setup(db, make_presence_fn)
    db.execute(
        "UPDATE session_registrations SET state = 'stopping' "
        "WHERE session_uuid = ?",
        (str(presence.session_uuid),),
    )
    db.commit()

    decision = verify_capability(
        db,
        capability.present(),
        expected_sender_uuid=presence.session_uuid,
        expected_incarnation=presence.session_incarnation_id,
        expected_fence=presence.registration_fence,
        now=1_000_100,
    )

    assert decision.allowed is True
    assert "bearer" in verify_capability.__doc__.lower()
    assert "liveness" in verify_capability.__doc__.lower()
    assert "verify_capability_with_liveness" in verify_capability.__doc__
