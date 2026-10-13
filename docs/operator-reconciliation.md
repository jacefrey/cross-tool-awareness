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
