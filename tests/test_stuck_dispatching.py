"""C5 stuck-dispatch inspection and reconciliation coverage."""

from __future__ import annotations

import re
from pathlib import Path
from uuid import UUID, uuid4

import cross_tool_awareness as awareness
import pytest

from cross_tool_awareness import (
    Disposition,
    DispositionReceipt,
    NoticeKey,
    ReceiptStorage,
    claim_delivery,
    finalize_attempt,
    pre_adapter_mark,
)
from cross_tool_awareness.registry import register_session


_DOC = Path(__file__).resolve().parents[1] / "docs" / "operator-reconciliation.md"


def _register(db, presence):
    register_session(
        db,
        presence,
        now=1_000_000,
        holder_pid=12_345,
        process_started_at=presence.started_at,
    )


def _dispatching(db, make_presence_fn, *, message_id=None, started_at=1_000_100):
    presence = make_presence_fn()
    _register(db, presence)
    message_id = message_id or uuid4()
    db.execute("INSERT INTO messages (id) VALUES (?)", (str(message_id),))
    db.execute(
        "INSERT INTO targeted_deliveries "
        "(message_id, target_session_uuid, target_session_incarnation_id, "
        " sender_session_uuid, sender_session_incarnation_id, delivery_state, "
        " created_at, next_attempt_at, expires_at) "
        "VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
        (
            str(message_id),
            str(presence.session_uuid),
            str(presence.session_incarnation_id),
            str(uuid4()),
            str(uuid4()),
            1_000_000,
            1_000_000,
            2_000_000,
        ),
    )
    claim = claim_delivery(db, now=1_000_010, lease_ttl_seconds=5)
    assert claim is not None
    assert pre_adapter_mark(
        db,
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        lease_token=claim.claim_lease_token,
        now=started_at,
    )
    return presence, claim


def test_list_stuck_dispatching_boundary_fields_order_and_read_only(
    db, make_presence_fn
):
    late_id = UUID("00000000-0000-0000-0000-000000000003")
    tie_b = UUID("00000000-0000-0000-0000-000000000002")
    tie_a = UUID("00000000-0000-0000-0000-000000000001")
    too_fresh = UUID("00000000-0000-0000-0000-000000000004")
    fixtures = [
        _dispatching(
            db, make_presence_fn, message_id=late_id, started_at=1_000_200
        ),
        _dispatching(
            db, make_presence_fn, message_id=tie_b, started_at=1_000_100
        ),
        _dispatching(
            db, make_presence_fn, message_id=tie_a, started_at=1_000_100
        ),
        _dispatching(
            db, make_presence_fn, message_id=too_fresh, started_at=1_000_201
        ),
    ]

    rows = awareness.list_stuck_dispatching(
        db, 1_000_300, stale_after_seconds=100
    )

    assert [row.message_id for row in rows] == [tie_a, tie_b, late_id]
    exact_presence, exact_claim = fixtures[0]
    assert rows[-1] == awareness.StuckDispatching(
        message_id=late_id,
        target_session_uuid=exact_presence.session_uuid,
        target_session_incarnation_id=exact_presence.session_incarnation_id,
        dispatch_started_at=1_000_200,
        claim_lease_token=exact_claim.claim_lease_token,
    )
    assert all(isinstance(row.message_id, UUID) for row in rows)
    states = db.execute(
        "SELECT DISTINCT delivery_state FROM targeted_deliveries"
    ).fetchall()
    assert [row[0] for row in states] == ["dispatching"]


@pytest.mark.parametrize("bad", [-1, True, 1.5, None])
def test_list_stuck_dispatching_rejects_invalid_threshold(db, bad):
    with pytest.raises(ValueError, match="stale_after_seconds"):
        awareness.list_stuck_dispatching(db, 1_000, stale_after_seconds=bad)


def test_matching_receipt_reconciles_dispatching_to_observed(db, make_presence_fn):
    presence, claim = _dispatching(db, make_presence_fn)
    key = NoticeKey(
        claim.message_id,
        claim.target_session_uuid,
        claim.target_session_incarnation_id,
    )
    receipt = DispositionReceipt(
        message_id=key.message_id,
        correlation_id=uuid4(),
        consumer_id="c5-test",
        receiver_session_uuid=key.target_session_uuid,
        receiver_session_incarnation_id=key.target_session_incarnation_id,
        disposition=Disposition.ACCEPTED,
        observed_at=1_000_200,
        reason="",
    )

    ReceiptStorage(db).record_first_disposition(key, receipt)

    row = db.execute(
        "SELECT delivery_state, claim_lease_token FROM targeted_deliveries "
        "WHERE message_id = ?",
        (claim.message_id,),
    ).fetchone()
    assert dict(row) == {"delivery_state": "observed", "claim_lease_token": None}
    assert presence.session_uuid == receipt.receiver_session_uuid


def test_same_token_finalization_reconciles_dispatching_to_proven_outcome(
    db, make_presence_fn
):
    _, claim = _dispatching(db, make_presence_fn)

    result = finalize_attempt(
        db,
        claim_lease_token=claim.claim_lease_token,
        message_id=claim.message_id,
        target_session_uuid=claim.target_session_uuid,
        target_session_incarnation_id=claim.target_session_incarnation_id,
        outcome="accepted_by_cli",
        reason="",
        attempted_at=1_000_100,
        now=1_000_101,
        next_attempt_at=1_000_200,
    )

    assert result.status == "finalized"
    assert result.new_state == "notified"
    state = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id = ?",
        (claim.message_id,),
    ).fetchone()[0]
    assert state == "notified"


def _recipe(name):
    text = _DOC.read_text()
    block = re.search(rf"<!-- {name} -->\n```sql\n(.*?)\n```", text, re.DOTALL)
    assert block, f"missing documented {name} SQL recipe"
    update = re.search(r"UPDATE targeted_deliveries.*?;", block.group(1), re.DOTALL)
    assert update, f"missing UPDATE in {name} recipe"
    assert "SELECT changes();" in block.group(1)
    return update.group(0)


@pytest.mark.parametrize(
    ("recipe_name", "new_state"),
    [("mark-observed", "observed"), ("mark-undeliverable", "undeliverable")],
)
def test_operator_recipe_is_full_key_and_token_fenced(
    db, make_presence_fn, recipe_name, new_state
):
    _, claim = _dispatching(db, make_presence_fn)
    sql = _recipe(recipe_name)
    params = {
        "message_id": claim.message_id,
        "target_session_uuid": claim.target_session_uuid,
        "target_session_incarnation_id": claim.target_session_incarnation_id,
        "expected_lease_token": "wrong-token",
        "reconciliation_reason": "operator-reviewed",
    }

    db.execute(sql, params)
    assert db.execute("SELECT changes()").fetchone()[0] == 0
    params["message_id"] = str(uuid4())
    params["expected_lease_token"] = claim.claim_lease_token
    db.execute(sql, params)
    assert db.execute("SELECT changes()").fetchone()[0] == 0
    params["message_id"] = claim.message_id
    params["expected_lease_token"] = claim.claim_lease_token
    db.execute(sql, params)
    assert db.execute("SELECT changes()").fetchone()[0] == 1
    state = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id = ?",
        (claim.message_id,),
    ).fetchone()[0]
    assert state == new_state
    db.execute(sql, params)
    assert db.execute("SELECT changes()").fetchone()[0] == 0


def test_operator_document_has_no_retry_recipe():
    text = _DOC.read_text().lower()
    assert "no retry recipe" in text
    assert "delivery_state = 'pending'" not in text
