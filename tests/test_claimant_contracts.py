"""ClaimantIdentity + RegistrationSnapshot dataclass contracts (v0.5.0).

Pin the design R5 shape:

- ClaimantIdentity: frozen; session_uuid + session_incarnation_id must be
  uuid.UUID instances; registration_fence is a nonempty str.
- RegistrationSnapshot: frozen; presence is a PresenceRecord;
  holder_pid > 0; process_started_at and proof_observed_at are ints.

Owner identity is distinct from sender and target. Facade construction
and every public lease writer require ClaimantIdentity, enforced at
runtime by missing-kwarg TypeError in A2's leases + composite_storage
reshape (tested in test_leases_owner.py + test_composite_storage_owner.py).
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from uuid import UUID, uuid4

import pytest

from cross_tool_awareness import (
    ClaimantIdentity,
    PresenceRecord,
    RegistrationSnapshot,
)


def _presence() -> PresenceRecord:
    return PresenceRecord(
        session_uuid=uuid4(),
        session_incarnation_id=uuid4(),
        alias="a", tool="claude", project_slug="p",
        project_root="/tmp/p", role="worker",
        state="active", wake_capability="notify_only",
        registration_fence="f1", wake_locator="x",
        started_at=1, heartbeat_at=2, expires_at=3,
    )


# ClaimantIdentity ----------------------------------------------------------


def test_claimant_identity_is_frozen():
    cid = ClaimantIdentity(session_uuid=uuid4(),
                           session_incarnation_id=uuid4(),
                           registration_fence="f1")
    with pytest.raises(FrozenInstanceError):
        cid.registration_fence = "f2"  # type: ignore[misc]


def test_claimant_identity_session_uuid_must_be_UUID():
    with pytest.raises(TypeError):
        ClaimantIdentity(session_uuid="not-a-uuid",  # type: ignore[arg-type]
                         session_incarnation_id=uuid4(),
                         registration_fence="f")


def test_claimant_identity_session_incarnation_id_must_be_UUID():
    with pytest.raises(TypeError):
        ClaimantIdentity(session_uuid=uuid4(),
                         session_incarnation_id="x",  # type: ignore[arg-type]
                         registration_fence="f")


def test_claimant_identity_fence_nonempty():
    with pytest.raises(ValueError):
        ClaimantIdentity(session_uuid=uuid4(),
                         session_incarnation_id=uuid4(),
                         registration_fence="")


def test_claimant_identity_equality_by_tuple():
    u, i = uuid4(), uuid4()
    a = ClaimantIdentity(session_uuid=u, session_incarnation_id=i,
                         registration_fence="f1")
    b = ClaimantIdentity(session_uuid=u, session_incarnation_id=i,
                         registration_fence="f1")
    c = ClaimantIdentity(session_uuid=u, session_incarnation_id=i,
                         registration_fence="f2")
    assert a == b
    assert a != c


# RegistrationSnapshot ------------------------------------------------------


def test_registration_snapshot_is_frozen():
    snap = RegistrationSnapshot(presence=_presence(), holder_pid=42,
                                process_started_at=100,
                                proof_observed_at=200)
    with pytest.raises(FrozenInstanceError):
        snap.holder_pid = 43  # type: ignore[misc]


def test_registration_snapshot_presence_must_be_PresenceRecord():
    with pytest.raises(TypeError):
        RegistrationSnapshot(presence="not-a-presence",  # type: ignore[arg-type]
                             holder_pid=1, process_started_at=1,
                             proof_observed_at=2)


def test_registration_snapshot_holder_pid_must_be_positive():
    with pytest.raises(ValueError):
        RegistrationSnapshot(presence=_presence(), holder_pid=0,
                             process_started_at=1, proof_observed_at=2)
    with pytest.raises(ValueError):
        RegistrationSnapshot(presence=_presence(), holder_pid=-5,
                             process_started_at=1, proof_observed_at=2)
