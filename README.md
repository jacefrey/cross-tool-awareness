# cross-tool-awareness

Shared storage + dispatcher primitives for cross-tool awareness between Codex and Claude Code sessions on a single Mac. Owned by machine-manager.

**Status:** v0.1.0 — fixture-only. Not authorized for live use.

Design authority (lives in `machine-manager`, the owning project):
`machine-manager/docs/superpowers/specs/2026-10-11-shared-awareness-package-design.md` (v4).

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
    CapabilityDecision, BearerCapability,
    SingletonDispatcherLease, claim_delivery, finalize_attempt,
    pre_adapter_release, run_one_iteration, WakeAdapter, WakeNotice,
    issue_capability, verify_capability, rotate_capability, revoke_capability,
    reconcile_ambiguous, reconcile_blocked,
    apply as apply_migration, rollback as rollback_migration,
    create_backup, run_foreground_probe,
)
```

## Scope

See the design spec §1 and §15. This package is fixture-only in Gate 1 scope: no live bridge DB writes, no feature gates, no background services, no deployment.
