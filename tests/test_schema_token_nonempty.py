"""R3: claimed/dispatching rows and dispatch witnesses require a NONEMPTY lease
token (seq-355 Minor)."""
from __future__ import annotations

import sqlite3

import pytest

from tests._owner_fixtures import fresh_db_path, identity, open_db, register, seed_pending


def test_claimed_row_rejects_empty_lease_token(tmp_path):
    db = open_db(fresh_db_path(tmp_path))
    cid, target = identity(), identity("t")
    register(db, cid)
    register(db, target, holder_pid=5678, process_started_at=2_000_000)
    mid = seed_pending(db, target)
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(
            "UPDATE targeted_deliveries SET delivery_state='claimed', claim_lease_token='',"
            " claim_lease_expires_at=40, claimant_session_uuid=?, claimant_session_incarnation_id=?,"
            " claimant_registration_fence=? WHERE message_id=?",
            (str(cid.session_uuid), str(cid.session_incarnation_id), cid.registration_fence, mid))


def test_witness_rejects_empty_lease_token(tmp_path):
    db = open_db(fresh_db_path(tmp_path))
    target = identity("t")
    register(db, target, holder_pid=5678, process_started_at=2_000_000)
    mid = seed_pending(db, target)
    with pytest.raises(sqlite3.IntegrityError):
        db.execute("INSERT INTO dispatch_ownership VALUES (?,?,?,?,?,?,?,?)",
                   ("", mid, str(target.session_uuid), str(target.session_incarnation_id),
                    "c", "ci", "f", 1))
