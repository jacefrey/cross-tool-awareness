# Operator reconciliation for stuck dispatches

A `dispatching` delivery represents an adapter call that began but whose
outcome was not durably finalized. Time alone must never return it to
`pending`: the external side effect may already have happened. First inspect
the external receiver and durable receipt evidence, then use exactly one of
the fenced recipes below inside a manually controlled transaction.

Bind all five named parameters. The delivery key and expected lease token must
come from the same `StuckDispatching` observation. After the `UPDATE`, execute
`SELECT changes();` and commit only when it returns exactly `1`; otherwise
roll back and investigate the changed or missing row.

## Confirmed observed

Use this only when independent evidence proves the intended receiver observed
the notice.

<!-- mark-observed -->
```sql
BEGIN IMMEDIATE;
UPDATE targeted_deliveries
SET delivery_state = 'observed',
    claim_lease_token = NULL,
    claim_lease_expires_at = NULL,
    claimant_session_uuid = NULL,
    claimant_session_incarnation_id = NULL,
    claimant_registration_fence = NULL,
    last_error_class = :reconciliation_reason
WHERE message_id = :message_id
  AND target_session_uuid = :target_session_uuid
  AND target_session_incarnation_id = :target_session_incarnation_id
  AND delivery_state = 'dispatching'
  AND claim_lease_token = :expected_lease_token;
SELECT changes();
-- COMMIT only when changes() = 1; otherwise ROLLBACK.
```

## Confirmed undeliverable

Use this only when independent evidence proves delivery did not and cannot
complete.

<!-- mark-undeliverable -->
```sql
BEGIN IMMEDIATE;
UPDATE targeted_deliveries
SET delivery_state = 'undeliverable',
    claim_lease_token = NULL,
    claim_lease_expires_at = NULL,
    claimant_session_uuid = NULL,
    claimant_session_incarnation_id = NULL,
    claimant_registration_fence = NULL,
    last_error_class = :reconciliation_reason
WHERE message_id = :message_id
  AND target_session_uuid = :target_session_uuid
  AND target_session_incarnation_id = :target_session_incarnation_id
  AND delivery_state = 'dispatching'
  AND claim_lease_token = :expected_lease_token;
SELECT changes();
-- COMMIT only when changes() = 1; otherwise ROLLBACK.
```

There is deliberately **no retry recipe**. A retry based only on age could
duplicate a side effect whose original completion is merely unrecorded.

## Dispatch witnesses

Every adapter entry writes an immutable `dispatch_ownership` row (the
witness) in the same transaction that marks the delivery `dispatching`. The
recipes above clear the delivery's active lease and owner fields; they
never touch the witness, and neither does operator reconciliation of
`ambiguous`/`blocked` rows. The witness is the only proof that lets the
entered owner record its outcome later (`finalize_attempt` returns
`receipt_already_won` after a receiver receipt has converged the row).

`rollback` refuses to tear the schema down while any witness has no
`queue_attempts` row matching its token AND full delivery key (an entered
attempt whose outcome was never recorded), in addition to witnesses whose
delivery row is missing. The only remedies are:

- the entered owner's own `finalize_attempt` (its exact token and identity), or
- a separately authorized, evidenced manual reconciliation that records the
  attempt row fenced to the exact existing witness (same token and delivery
  key), with its provenance preserved.

An outcome is never invented merely to unblock rollback. If the outcome is
unknown, the witness stays and the rollback stays refused.
