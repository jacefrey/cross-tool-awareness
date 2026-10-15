# Changelog

## 0.5.0 (2026-10-15)

Phase 2b pre-live hardening of CTA against process-contention and legacy
owner-less entry points. v0.4.0 remains supported for existing consumers
until they adopt v0.5.0; v0.5.0 is a successor candidate pending
independent acceptance. The package remains fixture-only and is not
authorized for live use.

Design authority:
[2026-10-14-awareness-pre-live-hardening-design.md](https://github.com/jacefrey/codex-manager/blob/main/docs/superpowers/specs/2026-10-14-awareness-pre-live-hardening-design.md)
(revision tip `abc1864`, MM-approved at bridge seq 343).

### Added
- `ClaimantIdentity` and `RegistrationSnapshot` frozen dataclasses in
  `contracts`. `ClaimantIdentity` requires strict `uuid.UUID` for its two
  identity fields and a nonempty `registration_fence` str.
- `dispatch_ownership` package-owned witness table keyed by
  `claim_lease_token`, with FK to `targeted_deliveries`. Retained across
  receipt-wins so a late `finalize_attempt` from the matching owner can
  still authenticate against it (R4).
- Schema 4 adds nullable `claimant_session_uuid`,
  `claimant_session_incarnation_id`, `claimant_registration_fence` to
  `targeted_deliveries` under all-or-nothing CHECK and claimed/dispatching
  requires-full-owner CHECK.
- Migration v3→v4 with quiescent-only guard (refuses claimed, dispatching,
  notified, ambiguous, or blocked state) and preserve-unchanged refusal:
  no backup is written, no mutation applied, and no owner identities are
  invented. v2 bridges still migrate forward via the v2→v3→v4 chain.
- 16 new tests (3 files): tripwire coverage for the disabled dispatcher
  and probe; witness retention + late-finalize authentication; multi-
  process contention + wrong-owner rejection.

### Changed
- `CompositeStorage` constructor now requires `claimant: ClaimantIdentity`
  and `clock: Callable[[], int]`. Claimant is immutable for the facade's
  lifetime; no refresh-in-place. After a trusted re-registration or fence
  change, owner wiring must construct a NEW facade.
- Every public writer in `leases.py` requires `claimant` + `clock`
  keyword args. Entry points own `BEGIN IMMEDIATE` transactions, reject
  an active caller transaction with `sqlite3.ProgrammingError`, and roll
  back on all exceptions.
- Default claim lease TTL raised from 15 to 30 seconds (R8).
- `+/- 30 s` freshness window enforced at the mark boundary against the
  injected clock (R7); no timestamp clamping.
- `pre_adapter_mark` now atomically inserts the `dispatch_ownership`
  witness with the `claimed → dispatching` state change in one txn (R4).
- `finalize_attempt` authenticates against the dispatch_ownership witness
  before any attempt insert; wrong owner / wrong token / wrong key
  changes zero rows (returns `unauthorized`).
- Rollback refuses when `dispatch_ownership` rows have no matching
  delivery — no destructive teardown of unmatched witnesses (R4).

### Breaking
- No owner-less compatibility mode. v0.4.0 call sites that constructed
  `CompositeStorage` without `claimant`/`clock`, or called `leases.*`
  without the new kwargs, raise `TypeError`.
- Schema 4 is required. v0.4.0 clients cannot read a v4 DB (`claimed` /
  `dispatching` without owner tuple violates CHECK).

### Disabled (fail-close)
- `dispatcher.run_one_iteration` raises
  `RuntimeError('legacy dispatcher disabled: claimant-aware bounded
  runner required')` immediately. Even a bare `run_one_iteration()` call
  raises before any arg is read.
- `probe.run_foreground_probe` raises the same `RuntimeError` BEFORE DB
  connection, lease acquisition, clock call, sleep, measurement, or
  adapter entry — including `duration_seconds=0`.
- Both callables remain in `__all__` with explicit disabled-legacy
  comments in `__init__.py`. `ProbeResult` retained as inert data type
  for v0.4-era saved recordings. A claimant-aware bounded runner is
  deferred to a later roadmap gate.

### Not changed
- Registry, capability, backup, reconciliation, authenticated-targeted-
  send paths retain their accepted v0.4.0 contracts.
- Schema migration v2→v3 hop preserved as a separate validated step.
- `SingletonDispatcherLease` class is unchanged.

### Verification
- 241/241 tests pass locally on tip of `feat/v05-claimant-ownership`.
- Independent acceptance is pending (CXM fresh-context review + separate
  MM fresh-session acceptance before merge).

## 0.4.0 (2026-10-13)

Task 9 follow-up batch implementing M3, M6, M1, M9, and M10 from the
[shared-awareness package design](https://github.com/jacefrey/machine-manager/blob/main/docs/superpowers/specs/2026-10-11-shared-awareness-package-design.md).
The package remains fixture-only and is not authorized for live use.

- Added the six public interfaces required by the downstream composite:
  `BoundedAdapter`, `pre_adapter_mark`, `StuckDispatching`,
  `list_stuck_dispatching`, `CompositeStorage`, and
  `verify_capability_with_liveness`.
- Added schema v3 and the guarded v2→v3 migration. Existing registrations are
  retained for foreign-key and audit history but forced to the legacy-safe
  posture: `state='stale'`, `holder_pid=0`, and a populated `revoked_at`.
  Existing delivery rows are preserved while gaining the `dispatching` state
  and `dispatch_started_at` field.
- Changed `register_session` to require truthful keyword-only `holder_pid` and
  `process_started_at` values, with strict validation and same-primary-key
  rejection across fresh incarnation UUIDs.
- Added bounded adapter conformance, pre-adapter dispatch fencing, stuck-row
  inspection and operator reconciliation recipes, the immutable six-method
  storage facade, and receiver-owned two-read process-liveness verification.
- Closed the pre-release acceptance findings with migration row-count checks,
  atomic claim/fence capture, caller-transaction-safe liveness verification,
  and an end-to-end migrated-registration regression.
- The tracked release artifact now contains 45 files; 220 tests pass at the
  pre-C8 checkpoint.

## 0.3.1 (2026-10-12)

Point release correcting a version-constant drift surfaced by machine-manager's
Task 9 independent review of codex-manager's Gate #1 composite (finding L4).
No API, schema, or behavior changes.

- **src/cross_tool_awareness/__init__.py:83** — `__version__` was `"0.2.0"`,
  stale from the v0.2.0 → v0.3.0 release that bumped `pyproject.toml` and
  `manifest.toml` but missed the module-level constant. Now `"0.3.1"`.
- **src/cross_tool_awareness/__init__.py:6** — module docstring claimed
  `"v0.2.0. Fixture-only."`; same drift. Now `"v0.3.1. Fixture-only."`.

A naive Python-side fence (`cross_tool_awareness.__version__ == "0.3.0"`) would
have fail-closed against the correctly-released v0.3.0 build. The 0.3.0 package
tree at commit `bd8cfe031471197c3d0697997c75f8c43b14b159` remains an accepted
checkout for anything that pins by git SHA; this release updates the Python
constant so a `.__version__` check can work.

## 0.3.0 (2026-10-12)

Remediation of codex-manager's seq=195 package review (durable at codex-manager
commit `c7b15db`). All 3 implementation blockers addressed. 128 tests pass
(up from 122): +2 send-API injection tests, +4 two-connection concurrency
tests.

- **R1 Atomic bridge-message + delivery send.** `create_authenticated_targeted_send`
  now requires a `message_writer` callable from the caller. The writer runs
  inside the same `BEGIN EXCLUSIVE` transaction as the targeted_deliveries
  INSERT, so bridge message and delivery row are atomic together. The caller
  owns the live bridge's message schema (payload column + whatever else),
  and the API is correctness-verifiable because the writer either runs or
  the whole send fails. There is no silent payload discard anymore. A new
  `tests/test_storage.py::test_authenticated_targeted_send_requires_message_writer`
  verifies the ValueError; a new
  `test_authenticated_targeted_send_message_writer_failure_rolls_back`
  verifies rollback on writer failure. The stub `messages` table in
  `tests/conftest.py` gained a `payload TEXT NOT NULL` column and a
  `stub_message_writer` reference implementation.
- **R2 BEGIN EXCLUSIVE is now real.** New `src/cross_tool_awareness/_txn.py`
  module exposes `exclusive(db)` — an explicit `BEGIN EXCLUSIVE` /
  `COMMIT` / `ROLLBACK` context manager that temporarily sets
  `conn.isolation_level = None` to prevent Python's auto-BEGIN from fighting
  the manual BEGIN. Every SELECT-then-INSERT path in `storage.py` and the
  composite rotation in `capability.py` now uses `exclusive(db)` instead of
  Python's DEFERRED-mode `with db:`. The file lock is acquired up front, so
  pre-write reads cannot race with concurrent writers.
  `test_rotate_capability_atomicity_on_issue_failure` exercises mid-rotation
  failure and asserts the revoke rolls back.
- **R3 Deterministic uniqueness-race recovery.** `claim_notice` and
  `record_first_disposition` wrap INSERT in try/except for
  `sqlite3.IntegrityError` and, on duplicate, re-SELECT and return the
  durable terminal state (`duplicate` / `observed`). This is defense in
  depth: `BEGIN EXCLUSIVE` already serializes concurrent writers on the
  same DB file, so the race never fires under the normal path, but the
  catch guarantees a well-formed return even under a non-locking caller
  (e.g. a WAL-mode connection or a programming error). New
  `tests/test_storage_concurrency.py` opens two threads with two separate
  sqlite3 connections to the same on-disk DB and exercises claim_notice,
  record_first_disposition, and the authenticated send under
  threading.Barrier-coordinated contention; all three cases return
  deterministic outcomes with zero IntegrityError escapes.

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
