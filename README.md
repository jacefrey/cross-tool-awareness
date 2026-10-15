# cross-tool-awareness

Shared storage + dispatcher primitives for cross-tool awareness between Codex and Claude Code sessions on a single Mac. Owned by machine-manager.

**Status:** v0.5.1 — fixture-only candidate pending independent acceptance. Not authorized for live use.

Design authority (Phase 2b pre-live hardening, lives in `codex-manager`):
`codex-manager/docs/superpowers/specs/2026-10-14-awareness-pre-live-hardening-design.md` (revision `abc1864`, R1–R14).

Prior design authority (v0.4.0): `machine-manager/docs/superpowers/specs/2026-10-12-shared-awareness-package-v0.4-design.md`.

Contract authority (lives in `codex-manager`, the consuming adapter's owner):
`codex-manager/docs/superpowers/specs/2026-10-05-codex-gate1-contracts.md` (commit 0087ebe).

## Install

```bash
python3.12 -m pip install -e /path/to/cross-tool-awareness
```

## Import

```python
from cross_tool_awareness import (
    PresenceRecord, RegistrationUpdate, QueueAttempt, DispositionReceipt,
    ClaimantIdentity, RegistrationSnapshot,
    CompositeStorage, CompositeFinalizationResult,
    CapabilityDecision, BearerCapability,
    SingletonDispatcherLease, claim_delivery, pre_adapter_mark,
    pre_adapter_release, finalize_attempt, WakeAdapter, WakeNotice,
    issue_capability, verify_capability, rotate_capability, revoke_capability,
    reconcile_ambiguous, reconcile_blocked,
    apply as apply_migration, rollback as rollback_migration,
    create_backup,
    run_one_iteration, run_foreground_probe, ProbeResult,   # disabled legacy, see below
)
```

## Owner-aware usage (v0.5+)

Every write is fenced by a durable claimant identity bound once, at facade
construction, together with an injected clock:

```python
import os, time
from cross_tool_awareness import ClaimantIdentity, CompositeStorage, RegistrationSnapshot

claimant = ClaimantIdentity(
    session_uuid=my_session_uuid,
    session_incarnation_id=my_incarnation_uuid,
    registration_fence=my_registration_fence,
)
facade = CompositeStorage(
    db, tool="claude", wake_capability="notify_only",
    claimant=claimant, clock=lambda: int(time.time()),
)

# A snapshot is issued by owner wiring after a positive OS observation of the
# registration's durable PID/start pair. Build it from the CURRENT row.
snapshot = RegistrationSnapshot(
    presence=my_presence_record, holder_pid=os.getpid(),
    process_started_at=my_process_started_at, proof_observed_at=int(time.time()),
)

claim = facade.claim_delivery(now, recovery_after=30, claimant_snapshot=snapshot)
if claim is not None:
    presence = facade.revalidate_claim(claim, now, lease_token=claim.claim_lease_token)
    if presence is None:
        facade.release_claim(claim, "blocked", "target_not_live", lease_token=claim.claim_lease_token)
    elif facade.pre_adapter_mark(claim, now, lease_token=claim.claim_lease_token,
                                 claimant_snapshot=snapshot, target_snapshot=target_snapshot):
        ...  # adapter call happens OUTSIDE any SQL transaction
        result = facade.finalize_attempt(claim, attempt, lease_token=claim.claim_lease_token)
        # result.status ∈ finalized | unauthorized | already_finalized |
        #                 altered_retry_rejected | lease_already_released | receipt_already_won
```

Rules the package enforces (design R5–R8):

- **Single-fence facade lifetime.** A facade is bound to one
  UUID/incarnation/fence for its whole life. After a trusted
  re-registration or fence change, construct a NEW facade from the new
  verified binding; never refresh identity in place. The old facade cannot
  claim or mark; its already-entered attempt may still finalize against its
  durable `dispatch_ownership` witness.
- **Write-time eligibility uses the injected clock, read after the lock.**
  `now` is scheduling input only. Lease expiry is `clock() + recovery_after`;
  equality at expiry loses.
- **Snapshots are compared field-for-field to the current registration.**
  Every `PresenceRecord` field except `heartbeat_at` — including
  `expires_at` — plus PID/start must match, the row must be unrevoked,
  unexpired and in `active`/`busy`/`idle`, and the target must still route
  to the facade's `(tool, wake_capability)`. If a registration's
  `expires_at` was extended between your snapshot and your mark, the mark
  refuses: re-observe and re-snapshot outside the transaction while you
  still own a valid pre-entry claim, or release to `blocked`. Never
  auto-retry an entered or ambiguous dispatch. The 30 s proof-age window
  is a maximum age, not a guarantee the snapshot stays valid.
- **Pre-entry release preserves `next_attempt_at`.**
- **Finalization is exactly-once.** An exact retry is idempotent
  (`already_finalized`); any altered field under the same token is
  rejected (`altered_retry_rejected`); a wrong owner with a known token is
  `unauthorized`. A valid receiver receipt converges the delivery to
  `observed` in either order.
- **Package entry points own their transactions** and refuse a caller's
  open transaction with `sqlite3.ProgrammingError`.

## Migration and rollback

`apply(db_path, backup_dir=..., now=...)` upgrades v2 or v3 bridges to
schema 4 through guarded hops: each hop takes the write lock, rechecks the
version, the exact canonical source schema and quiescence, backs up through
a separate read connection (`bridge.db.<ts>.from-v<N>.sqlite`), applies the
DDL, validates the exact destination schema, foreign keys and preserved row
counts, and only then commits. Non-canonical sources are `refused` before
any backup; a `refused`/`failed` result after a committed hop carries that
hop's backup and the actual version. `rollback` refuses in-flight
deliveries, orphaned or unfinalized dispatch witnesses, live capabilities
and non-canonical source state, all under one write lock. See
`docs/operator-reconciliation.md` for the witness rules.

## Disabled legacy entry points

`run_one_iteration` and `run_foreground_probe` remain exported for
compatibility but raise
`RuntimeError('legacy dispatcher disabled: claimant-aware bounded runner required')`
immediately — before any database connection, lease, clock, sleep,
measurement or adapter access, including `duration_seconds=0`. `ProbeResult`
is an inert data type. A claimant-aware bounded runner is a later gate.

## Scope

Fixture-only: no live bridge DB writes, no feature gates, no background
services, no deployment. See the design spec §1 and §15.
