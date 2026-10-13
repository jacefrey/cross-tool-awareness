"""C6 contract and round-trip tests for CompositeStorage."""

from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError
from uuid import uuid4

import pytest

from cross_tool_awareness import (
    CompositeStorage,
    Disposition,
    DispositionReceipt,
    NoticeKey,
    QueueAttempt,
    QueueOutcome,
    ReceiptStorage,
    claim_delivery as module_claim_delivery,
    pre_adapter_release,
)
from cross_tool_awareness.registry import register_session


# Mirror of codex-manager's accepted six-method Storage Protocol contract at
# review-fixes-mm-task9 1dd30bc, as amended by v0.4 spec v3.1 / plan v3.
_SIGNATURES = {
    "pending_count": ["self", "now"],
    "claim_delivery": ["self", "now", "recovery_after"],
    "revalidate_claim": ["self", "claim", "now", "lease_token"],
    "release_claim": ["self", "claim", "state", "reason", "lease_token"],
    "pre_adapter_mark": ["self", "claim", "now", "lease_token"],
    "finalize_attempt": ["self", "claim", "attempt", "lease_token"],
}

_SIGNATURE_TEXT = {
    "pending_count": "(self, now: 'int') -> 'int'",
    "claim_delivery": (
        "(self, now: 'int', *, recovery_after: 'int') -> "
        "'ClaimedDelivery | None'"
    ),
    "revalidate_claim": (
        "(self, claim: 'ClaimedDelivery', now: 'int', *, "
        "lease_token: 'str') -> 'PresenceRecord | None'"
    ),
    "release_claim": (
        "(self, claim: 'ClaimedDelivery', state: 'str', reason: 'str', *, "
        "lease_token: 'str') -> 'None'"
    ),
    "pre_adapter_mark": (
        "(self, claim: 'ClaimedDelivery', now: 'int', *, "
        "lease_token: 'str') -> 'bool'"
    ),
    "finalize_attempt": (
        "(self, claim: 'ClaimedDelivery', attempt: 'QueueAttempt', *, "
        "lease_token: 'str') -> 'DispositionReceipt | None'"
    ),
}


def _storage(db, *, tool="codex", wake_capability="codex_queue"):
    return CompositeStorage(
        db,
        tool=tool,
        wake_capability=wake_capability,
    )


def _register(db, presence):
    register_session(
        db,
        presence,
        now=1_000_000,
        holder_pid=12_345,
        process_started_at=presence.started_at,
    )


def _pending(db, presence, *, due_at=1_000_050):
    message_id = uuid4()
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
            due_at,
            2_000_000,
        ),
    )
    return message_id


def test_protocol_method_signatures_are_exact():
    for name, expected_names in _SIGNATURES.items():
        signature = inspect.signature(getattr(CompositeStorage, name))
        assert str(signature) == _SIGNATURE_TEXT[name]
        assert list(signature.parameters) == expected_names
        for parameter_name in expected_names:
            expected_kind = (
                inspect.Parameter.KEYWORD_ONLY
                if parameter_name in {"recovery_after", "lease_token"}
                else inspect.Parameter.POSITIONAL_OR_KEYWORD
            )
            assert signature.parameters[parameter_name].kind is expected_kind


def test_constructor_requires_nonempty_immutable_routing(db):
    with pytest.raises(TypeError):
        CompositeStorage(db)
    with pytest.raises(ValueError, match="tool"):
        CompositeStorage(db, tool="", wake_capability="codex_queue")
    with pytest.raises(ValueError, match="wake_capability"):
        CompositeStorage(db, tool="codex", wake_capability="")

    storage = _storage(db)
    with pytest.raises(FrozenInstanceError):
        storage.tool = "claude"


def test_pending_count_and_claim_recovery_after_round_trip(
    db, make_presence_fn
):
    presence = make_presence_fn()
    _register(db, presence)
    _pending(db, presence, due_at=1_000_050)
    _pending(db, presence, due_at=1_000_200)
    storage = _storage(db)

    assert storage.pending_count(1_000_100) == 1
    claim = storage.claim_delivery(1_000_100, recovery_after=37)

    assert claim is not None
    expiry = db.execute(
        "SELECT claim_lease_expires_at FROM targeted_deliveries "
        "WHERE message_id = ?",
        (claim.message_id,),
    ).fetchone()[0]
    assert expiry == 1_000_137


@pytest.mark.parametrize("bad", [0, -1, True, 1.5, None])
def test_claim_delivery_rejects_invalid_recovery_after(db, bad):
    with pytest.raises(ValueError, match="recovery_after"):
        _storage(db).claim_delivery(1_000_100, recovery_after=bad)


def test_revalidate_claim_requires_exact_live_registration_and_lease(
    db, make_presence_fn
):
    presence = make_presence_fn()
    _register(db, presence)
    _pending(db, presence)
    storage = _storage(db)
    claim = storage.claim_delivery(1_000_100, recovery_after=30)
    assert claim is not None

    assert storage.revalidate_claim(
        claim, 1_000_110, lease_token=claim.claim_lease_token
    ) == presence
    assert storage.revalidate_claim(
        claim, 1_000_110, lease_token="wrong"
    ) is None
    assert storage.revalidate_claim(
        claim, 1_000_131, lease_token=claim.claim_lease_token
    ) is None
    assert _storage(db, tool="claude", wake_capability="claude_socket").revalidate_claim(
        claim, 1_000_110, lease_token=claim.claim_lease_token
    ) is None

    db.execute(
        "UPDATE session_registrations SET registration_fence = 'rotated' "
        "WHERE session_uuid = ? AND session_incarnation_id = ?",
        (claim.target_session_uuid, claim.target_session_incarnation_id),
    )
    assert storage.revalidate_claim(
        claim, 1_000_110, lease_token=claim.claim_lease_token
    ) is None

    db.execute(
        "UPDATE session_registrations SET state = 'stopping' "
        "WHERE session_uuid = ? AND session_incarnation_id = ?",
        (claim.target_session_uuid, claim.target_session_incarnation_id),
    )
    assert storage.revalidate_claim(
        claim, 1_000_110, lease_token=claim.claim_lease_token
    ) is None


def test_codex_and_claude_routing_pairs(db, make_presence_fn):
    codex = make_presence_fn(tool="codex", wake_capability="codex_queue")
    claude = make_presence_fn(tool="claude", wake_capability="claude_socket")
    _register(db, codex)
    _register(db, claude)
    _pending(db, codex, due_at=1_000_050)
    _pending(db, claude, due_at=1_000_060)

    codex_storage = _storage(db)
    codex_claim = codex_storage.claim_delivery(1_000_100, recovery_after=30)
    assert codex_claim is not None
    assert codex_storage.revalidate_claim(
        codex_claim, 1_000_110, lease_token=codex_claim.claim_lease_token
    ) == codex

    claude_storage = _storage(
        db, tool="claude", wake_capability="claude_socket"
    )
    assert claude_storage.revalidate_claim(
        codex_claim, 1_000_110, lease_token=codex_claim.claim_lease_token
    ) is None
    claude_claim = claude_storage.claim_delivery(
        1_000_100, recovery_after=30
    )
    assert claude_claim is not None
    assert claude_storage.revalidate_claim(
        claude_claim, 1_000_110, lease_token=claude_claim.claim_lease_token
    ) == claude


def test_release_claim_is_exactly_fenced(db, make_presence_fn):
    presence = make_presence_fn()
    _register(db, presence)
    _pending(db, presence)
    storage = _storage(db)
    claim = storage.claim_delivery(1_000_100, recovery_after=30)
    assert claim is not None
    other_message_id = _pending(db, presence, due_at=1_000_200)
    db.execute(
        "UPDATE targeted_deliveries "
        "SET delivery_state = 'claimed', claim_lease_token = ?, "
        "    claim_lease_expires_at = ? WHERE message_id = ?",
        (claim.claim_lease_token, 1_000_130, str(other_message_id)),
    )
    db.commit()

    with pytest.raises(RuntimeError, match="lease"):
        storage.release_claim(
            claim, "pending", "target_not_live", lease_token="wrong"
        )
    state = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id = ?",
        (claim.message_id,),
    ).fetchone()[0]
    assert state == "claimed"

    assert storage.release_claim(
        claim,
        "pending",
        "target_not_live",
        lease_token=claim.claim_lease_token,
    ) is None
    row = db.execute(
        "SELECT delivery_state, claim_lease_token, last_error_class "
        "FROM targeted_deliveries WHERE message_id = ?",
        (claim.message_id,),
    ).fetchone()
    assert dict(row) == {
        "delivery_state": "pending",
        "claim_lease_token": None,
        "last_error_class": "target_not_live",
    }
    other = db.execute(
        "SELECT delivery_state, claim_lease_token FROM targeted_deliveries "
        "WHERE message_id = ?",
        (str(other_message_id),),
    ).fetchone()
    assert dict(other) == {
        "delivery_state": "claimed",
        "claim_lease_token": claim.claim_lease_token,
    }


def test_pre_adapter_mark_and_finalize_round_trip(db, make_presence_fn):
    presence = make_presence_fn()
    _register(db, presence)
    message_id = _pending(db, presence)
    storage = _storage(db)
    claim = storage.claim_delivery(1_000_100, recovery_after=30)
    assert claim is not None

    assert storage.pre_adapter_mark(
        claim, 1_000_110, lease_token=claim.claim_lease_token
    )
    attempt = QueueAttempt(
        message_id=message_id,
        target_session_uuid=presence.session_uuid,
        target_session_incarnation_id=presence.session_incarnation_id,
        outcome=QueueOutcome.ACCEPTED_BY_CLI,
        attempted_at=1_000_110,
        finished_at=1_000_111,
        reason="",
    )
    assert storage.finalize_attempt(
        claim, attempt, lease_token=claim.claim_lease_token
    ) is None
    state = db.execute(
        "SELECT delivery_state FROM targeted_deliveries WHERE message_id = ?",
        (str(message_id),),
    ).fetchone()[0]
    assert state == "notified"


def test_finalize_returns_receipt_when_receiver_already_won(
    db, make_presence_fn
):
    presence = make_presence_fn()
    _register(db, presence)
    message_id = _pending(db, presence)
    storage = _storage(db)
    claim = storage.claim_delivery(1_000_100, recovery_after=30)
    assert claim is not None
    assert storage.pre_adapter_mark(
        claim, 1_000_110, lease_token=claim.claim_lease_token
    )
    key = NoticeKey(
        message_id,
        presence.session_uuid,
        presence.session_incarnation_id,
    )
    receipt = DispositionReceipt(
        message_id=message_id,
        correlation_id=uuid4(),
        consumer_id="c6-test",
        receiver_session_uuid=presence.session_uuid,
        receiver_session_incarnation_id=presence.session_incarnation_id,
        disposition=Disposition.ACCEPTED,
        observed_at=1_000_111,
        reason="",
    )
    ReceiptStorage(db).record_first_disposition(key, receipt)
    attempt = QueueAttempt(
        message_id=message_id,
        target_session_uuid=presence.session_uuid,
        target_session_incarnation_id=presence.session_incarnation_id,
        outcome=QueueOutcome.ACCEPTED_BY_CLI,
        attempted_at=1_000_110,
        finished_at=1_000_112,
        reason="",
    )

    assert storage.finalize_attempt(
        claim, attempt, lease_token=claim.claim_lease_token
    ) == receipt


def test_existing_module_functions_remain_usable(db, make_presence_fn):
    presence = make_presence_fn()
    _register(db, presence)
    _pending(db, presence)

    claim = module_claim_delivery(db, now=1_000_100, lease_ttl_seconds=30)
    assert claim is not None
    result = pre_adapter_release(
        db,
        claim_lease_token=claim.claim_lease_token,
        next_state="blocked",
        diagnostic="module-path",
        next_attempt_at=1_000_200,
    )
    assert result.status == "released"
