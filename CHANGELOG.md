# Changelog

## 0.2.0 (2026-10-12)

Remediation of codex-manager's seq=193 package review (durable at codex-manager
`feat/codex-gate1-composite` commit `3fd2e94`). All 8 implementation blockers
addressed. 122 tests pass.

- **B1 Storage APIs.** New `storage.py` with concrete `ReceiptStorage` class
  implementing codex-manager's Protocol (`claim_notice` + `record_first_disposition`),
  plus `apply_registration_update` fenced on (uuid, incarnation, fence), and
  `create_authenticated_targeted_send` that verifies the sender capability and
  atomically inserts bridge message + delivery row in one `BEGIN EXCLUSIVE`. New
  `notice_claims` table added to schema for claim tracking.
- **B2 Contract compatibility.** `contracts.py` rewritten field-for-field to
  match `codex-manager/scripts/automatic_awareness/contracts.py`:
  `DispositionReceipt` is the published 8-field shape (target_* derived at
  storage boundary); UUIDs normalized to `UUID` objects via `_exact_uuid`;
  enums are `str, Enum` subclasses with codex's reason-token vocabulary;
  `Disposition` includes `UNEVALUATED` (rejected at receipt construction);
  `QueueCapability` + `CapabilityDecision` shapes match codex-manager.
  `validation.py` and `enums.py` deleted — all helpers live in `contracts.py`.
- **B3 FK + atomic.** `targeted_deliveries` carries FK to `messages(id)`.
  `create_authenticated_targeted_send` provides the atomic insert API.
- **B4 WakeNotice sanitizer + notice factory.** `WakeNotice.__post_init__`
  pipes `summary` through `contracts.validate_notice_summary` (control chars,
  markup delimiters, credential labels, oversized → `[summary omitted]`).
  `run_one_iteration` requires a `notice_factory` callable; raises if omitted.
- **B5 Migration atomicity + validation + extras.** `migrations.apply` parses
  `schema.sql` into individual statements and executes each in one `with db:`
  block. Validation compares `sqlite_master.sql` text (normalized) to canonical.
  Extras detection scans sqlite_master for artifacts whose names match the
  awareness-managed prefix set but are not in the expected set, and refuses
  as `unknown_managed_state`.
- **B6 Capability transaction-neutral.** `issue_capability` / `verify_capability` /
  `revoke_capability` carry no inner `with db:` block. `rotate_capability`
  wraps the whole sequence in one `with db:` so a failure mid-rotation leaves
  neither the revoke nor the issue persisted.
- **B7 Adapter exception → ambiguous_timeout.** Dispatcher's try/except around
  `adapter.wake` now maps uncaught exceptions to `ambiguous_timeout` (preserve
  as ambiguous, no blind retry) rather than `rejected`.
- **B8 Digest script git ls-files.** `scripts/compute-package-digest.py`
  rewritten: primary enumeration via `git -C <root> ls-files -z`; walk-mode
  only as fallback for non-git fixtures. `--count` prints tracked-file count
  on stderr; `--require-git` fails on non-git roots.

## 0.1.0 (2026-10-11)

Initial release. Implements v4 of the design spec at
`machine-manager/docs/superpowers/specs/2026-10-11-shared-awareness-package-design.md`.

Fixture-only; not authorized for live use.
