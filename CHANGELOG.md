# Changelog

## 0.5.2 (2026-10-15)

Successor candidate to 0.5.1 (`c4ff7da`, never released; superseded).
Carries the one Important finding (I11) from codex-manager's independent
recheck of the 0.5.1 tuple (`mm-cxm-coord` seq 374; review
`codex-manager/docs/superpowers/reviews/2026-10-15-cta-v051-independent-recheck.md`).
Implemented by codex-manager under the review-fix-validate operating mode
(commit `36a850c`) and validated by machine-manager from an isolated clone.
No design change: every 0.5.1 entry below stands. The package remains
fixture-only and is not authorized for live use.

### Fixed (correctness)
- **I11 / R9** — `_collect_managed_state` now reads `sqlite_master.tbl_name`
  and treats any non-managed index or trigger attached to a managed table as
  an unknown artifact regardless of its name prefix. Because that collector
  feeds `_validate_schema_exact`, the refusal applies before the first backup,
  again under each hop's write lock, and in `rollback` before any drop.
  Previously a foreign-named `UNIQUE` index or `AFTER INSERT` trigger on
  `targeted_deliveries` passed the exact-source check and was destroyed by
  the table rebuild while `apply` reported `applied`, silently removing the
  database's enforcement and audit behavior. SQLite internal indexes and
  objects on unrelated bridge-owned tables remain permitted.
  Tests: `test_foreign_schema_objects.py` — unknown index/trigger refused with
  enforcement and audit behavior intact; object introduced after the outer
  inspection but before the write lock refused; rollback refuses without
  destroying; unrelated bridge objects allowed and functional (versions
  2/3/4 × index/trigger, 17 cases).

### Changed
- Version surfaces 0.5.2; `docs/releases/v0.5.2.md`; the 0.5.1 release doc
  is bannered superseded. The 0.5.1 deferred-minor list is unchanged except
  that "foreign-named index/trigger on a managed table" is now I11 (fixed).

## 0.5.1 (2026-10-15) — candidate superseded by 0.5.2

Remediation candidate for the ten Important correctness findings
(I01–I10), the deleted-coverage obligations and the Minor from
codex-manager's independent review of the withdrawn 0.5.0 candidate
(`mm-cxm-coord` seq 355; review `codex-manager/docs/superpowers/reviews/
2026-10-15-cta-v05-independent-review.md`, commit `1d52ef0`). Executed
under the MM remediation plan (rev 3, approved by codex-manager at seq
371). No design change: every item implements the approved R3–R9 text.
The package remains fixture-only and is not authorized for live use.

### Fixed (correctness)
- **I07 / R6** — `_txn.immediate` / `_txn.exclusive` keep the body AND the
  `COMMIT` inside the rollback boundary, roll back on any `BaseException`,
  close the connection if `ROLLBACK` itself fails, and no longer assign
  `isolation_level` at all (CPython's setter commits an open transaction
  when assigned `None`). Every `leases` writer uses `immediate`.
  Tests: `test_txn_envelope.py`, `test_writer_rollback.py`.
- **I01 / R7** — `pre_adapter_mark` and `claim_delivery` read the injected
  clock after `BEGIN IMMEDIATE`; mark refuses at lease-expiry equality.
  Tests: `test_write_time_eligibility.py` (lock-wait cases assert zero
  clock reads before the lock).
- **I02 / R7–R8** — lease recovery eligibility and the new lease expiry use
  the post-lock clock (`clock() + recovery_after`); scheduling `now` only
  gates pending due-ness. Test: `test_scheduling_now_cannot_recover_unexpired_lease`.
- **I03 / R5 §3.2** — claim and mark compare BOTH snapshots to the current
  registration rows inside the transaction: identity binding, every
  `PresenceRecord` field except `heartbeat_at` (`expires_at` included),
  `holder_pid`/`process_started_at`, unrevoked, unexpired, eligible state,
  and the target's `(tool, wake_capability)` route. Tests:
  `test_snapshot_binding.py`.
- **I04 / R5** — `ClaimedDelivery.target_registration_fence` is captured in
  the claim transaction; `revalidate_claim` requires it and the claimant's
  CURRENT live registration. Tests: `test_revalidate_claim.py`.
- **I05 / R5** — a duplicate-token finalization compares every persisted
  attempt field: identical → `already_finalized`, different →
  `altered_retry_rejected`; both change zero rows. Tests:
  `test_finalize_retry.py`.
- **I06 / A2** — `pre_adapter_release(next_attempt_at=None)` preserves the
  stored schedule; the facade's pre-entry release always passes `None`.
  Tests: `test_release_schedule.py`.
- **I08 / R9** — exact source-schema validation (artifact set, no
  managed-namespace extras, canonical SQL text) before any backup and again
  under the migration lock; canonical v2/v3 schemas ship as package data
  (`schema_v2.sql`, `schema_v3.sql`) pinned byte-identical to the test
  fixtures; one SQL parser serves every version. Tests:
  `test_migration_guarded.py`.
- **I09 / R9** — each hop runs through `_guarded_hop`: lock → recheck
  version / source schema / quiescence → per-hop backup (suffix
  `.from-v2` / `.from-v3`) through `create_backup`'s separate read
  connection → DDL → exact destination schema + `foreign_key_check` +
  preserved-table row counts + version row BEFORE `COMMIT`. Every ordinary
  error returns a `HopOutcome` carrying this hop's backup (or the
  unverified file path) and the actual version; a failure after a
  successful commit reports the destination version; only
  `KeyboardInterrupt` / `SystemExit` propagate. `rollback` runs every
  refusal check, an exact source-schema check for the installed version,
  the backup and the drops under one write lock.
- **I10 / R4** — `rollback` refuses any `dispatch_ownership` row with no
  `queue_attempts` row matching token AND full delivery key (entered,
  unfinalized), in addition to delivery orphans. Tests:
  `test_rollback_guarded.py`.
- **Minor / R3** — `claimed`/`dispatching` rows and witnesses require a
  nonempty lease token (schema CHECK + migration DDL). Tests:
  `test_schema_token_nonempty.py`.

### Added (coverage obligations)
- Restored facade/lease/reconciliation invariants deleted with the legacy
  dispatcher (`test_claimant_ownership.py`, `test_reconciliation.py`,
  `test_leases_owner.py`).
- Two-process harness (`tests/_owner_fixtures.run_children`): Barrier/Event
  synchronization, one shared 10 s deadline, one bounded 1 s cleanup phase,
  SIGKILL after a committed mark, both receipt/finalize orders
  (`test_claimant_concurrency.py`).
- Bounded scenario verifier for every seq-355 diagnostic label
  (`test_seq355_scenarios.py`; run with `-rA`).
- Tripwire completion for the disabled legacy entry points
  (`test_legacy_disabled.py`).
- `tests/test_version_consistency.py`.

### Changed (consumer-visible)
- `ClaimedDelivery` gains `target_registration_fence: str`.
- `FinalizationResult.status` / `CompositeFinalizationResult.status` gain
  `'altered_retry_rejected'`.
- `leases.pre_adapter_release(next_attempt_at: Optional[int])`.
- `migrations.apply` returns `refused` for non-canonical v2/v3 sources
  before any backup; a non-`applied` result after a committed hop carries
  that hop's backup; `MigrationRefused` and `HopOutcome` are new internals.
- `create_backup(..., suffix="")`; `BackupVerificationFailure.path`.
- `leases.pre_adapter_mark(..., target_registration_fence: Optional[str] = None)`;
  `CompositeStorage.pre_adapter_mark` passes the fence captured on the
  `ClaimedDelivery`, so a target whose fence rotated since the claim is
  refused at the adapter gate even after a re-snapshot (fresh-review fix).
- `leases.pre_adapter_release` raises `ValueError` (not a strippable
  `assert`) for a `next_state` outside `pending`/`blocked`.
- The transaction envelope tolerates SQLite's own auto-rollback
  (`SQLITE_FULL`, `interrupt()`, I/O errors): the consumer's connection
  stays open and usable; it is closed only when a transaction survives a
  failed `ROLLBACK`.
- Consumer rule for ruling R-S (confirmed by codex-manager): a registration
  expiry extension between snapshot and mark invalidates the snapshot —
  refuse, re-observe/re-snapshot outside the transaction while still
  owning a valid pre-entry claim, or release to blocked; never auto-retry
  an entered/ambiguous dispatch. 30 s is a maximum proof age, not a
  validity guarantee.

## 0.5.0 (2026-10-15) — candidate withdrawn before acceptance

Candidate `1ac8e5a` (digest `a91f82243b86…`) returned REVISIONS REQUIRED in
codex-manager's independent review (`mm-cxm-coord` seq 355) and was never
released, pushed or integrated. Superseded by 0.5.1 above. The original
entry is retained below for history.

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
- 245/245 tests pass locally on tip of `feat/v05-claimant-ownership`.
- Independent acceptance is pending (CXM fresh-context review + separate
  MM fresh-session acceptance before merge).

### Fix pass (post-fresh-review)
Two Important findings from MM's fresh-context Opus whole-branch review
addressed in commit `<fix pass>`:

- **I1:** `CompositeStorage.finalize_attempt` now returns a
  `CompositeFinalizationResult` surfacing status + new_state +
  attempt_recorded + receipt, rather than collapsing all non-receipt-wins
  statuses to `None`. A CXM-consumer can now distinguish `finalized` from
  `unauthorized` from `already_finalized` from `lease_already_released`
  from `receipt_already_won` without re-reading SQL.
- **I2:** `tr_receipt_marks_observed` trigger (v4 body) now also clears
  `claimant_session_uuid`, `claimant_session_incarnation_id`, and
  `claimant_registration_fence` on the observed row — alongside the lease
  fields. The dispatch_ownership witness remains the sole retained
  provenance after a receipt wins, so audit queries like
  `WHERE claimant_session_uuid = ?` to find "work this session owned"
  do not over-count observed rows. v3→v4 migration recreates the
  trigger with the v4 body; v2→v3 keeps its v3 body (claimant_* columns
  do not exist until v3→v4 completes).

Minor findings (M1–M5) deferred, see release evidence doc.

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
