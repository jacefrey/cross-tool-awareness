"""Lease primitives: singleton dispatcher lease, per-attempt lease,
fenced idempotent finalization, and pre-adapter release.

v0.5.0 (claimant ownership — design R5/R6/R7/R8):

- Every public writer requires a ``ClaimantIdentity`` kwarg and an injected
  ``clock: Callable[[], int]``. No owner-less compatibility mode.
- Package entry points own ``BEGIN IMMEDIATE`` transactions. They raise
  ``sqlite3.ProgrammingError`` if the caller has an active transaction, and
  roll back on all exceptions. They must not commit unrelated caller work.
- ``pre_adapter_mark`` inserts a ``dispatch_ownership`` witness atomic with
  the ``claimed → dispatching`` transition. ``finalize_attempt`` requires a
  matching witness (or an already-observed receipt) to authenticate; wrong
  owner / wrong token / wrong key changes zero rows.
- A ``+/- 30 s`` freshness window at the write boundary rejects stale
  snapshots: ``0 <= clock() - snapshot.proof_observed_at <= 30`` for both
  the claimant and target snapshots.
- Default claim lease TTL raised from 15 to 30 s per R8; callers that want
  the old value must pass it explicitly.
"""

from __future__ import annotations

import secrets
import sqlite3
from dataclasses import dataclass
from typing import Callable, Optional
from uuid import UUID

from .contracts import ClaimantIdentity, RegistrationSnapshot


# Freshness window at the write boundary (seconds). Design R7:
#   0 <= clock_now - snapshot.proof_observed_at <= 30
_FRESHNESS_WINDOW_SECONDS = 30


# ---------------------------------------------------------------------------
# Singleton dispatcher lease
# ---------------------------------------------------------------------------

class SingletonDispatcherLease:
    """Enforces at most one live dispatcher process via the dispatcher_lease table."""

    def __init__(self, db: sqlite3.Connection, *, holder_pid: int, holder_proc_start_ns: int):
        self._db = db
        self._holder_pid = int(holder_pid)
        self._holder_proc_start_ns = int(holder_proc_start_ns)
        self._token: Optional[str] = None

    @property
    def token(self) -> Optional[str]:
        return self._token

    def acquire(self, *, now: int, lease_ttl_seconds: int = 60) -> bool:
        """Return True if the lease was acquired; False if another holder is live."""
        new_token = secrets.token_hex(16)
        with self._db:
            self._db.row_factory = sqlite3.Row
            row = self._db.execute(
                "SELECT holder_token, holder_pid, holder_proc_start_ns, expires_at "
                "FROM dispatcher_lease WHERE singleton_pk = 1"
            ).fetchone()
            if row is not None and row["expires_at"] > now:
                return False
            self._db.execute(
                "INSERT INTO dispatcher_lease "
                "(singleton_pk, holder_token, holder_pid, holder_proc_start_ns, "
                " acquired_at, expires_at) VALUES (1, ?, ?, ?, ?, ?) "
                "ON CONFLICT(singleton_pk) DO UPDATE SET "
                "  holder_token = excluded.holder_token, "
                "  holder_pid = excluded.holder_pid, "
                "  holder_proc_start_ns = excluded.holder_proc_start_ns, "
                "  acquired_at = excluded.acquired_at, "
                "  expires_at = excluded.expires_at",
                (new_token, self._holder_pid, self._holder_proc_start_ns,
                 now, now + int(lease_ttl_seconds)),
            )
        self._token = new_token
        return True

    def refresh(self, *, now: int, lease_ttl_seconds: int = 60) -> bool:
        """Extend the lease; return False if another holder stole it (token mismatch)."""
        if self._token is None:
            return False
        with self._db:
            cur = self._db.execute(
                "UPDATE dispatcher_lease SET expires_at = ? "
                "WHERE singleton_pk = 1 AND holder_token = ?",
                (now + int(lease_ttl_seconds), self._token),
            )
            return cur.rowcount == 1

    def release(self) -> None:
        """DELETE the row only if held by this token; otherwise no-op."""
        if self._token is None:
            return
        with self._db:
            self._db.execute(
                "DELETE FROM dispatcher_lease "
                "WHERE singleton_pk = 1 AND holder_token = ?",
                (self._token,),
            )
        self._token = None


# ---------------------------------------------------------------------------
# Per-attempt lease: claim + pre-adapter release + fenced finalization
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ClaimedDelivery:
    """Owner-bearing claim handle. The three claimant_* fields pin the exact
    identity that owns this lease; they must be presented to every subsequent
    package call for this claim (mark, release, finalize). The caller cannot
    construct a ClaimedDelivery with a different claimant — the package
    stamps it from the facade's immutable claimant at claim time."""

    message_id: str
    target_session_uuid: str
    target_session_incarnation_id: str
    claim_lease_token: str
    claimant_session_uuid: str
    claimant_session_incarnation_id: str
    claimant_registration_fence: str


@dataclass(frozen=True)
class StuckDispatching:
    """A delivery whose bounded adapter call has not reached finalization."""

    message_id: UUID
    target_session_uuid: UUID
    target_session_incarnation_id: UUID
    dispatch_started_at: int
    claim_lease_token: str


@dataclass(frozen=True)
class PreAdapterReleaseResult:
    status: str                    # 'released' | 'lease_released_or_observed'
    diagnostic: Optional[str] = None


@dataclass(frozen=True)
class FinalizationResult:
    status: str                    # 'finalized' | 'already_finalized' | 'lease_already_released' | 'receipt_already_won'
    new_state: Optional[str] = None
    attempt_recorded: bool = False


_OUTCOME_TO_STATE = {
    "accepted_by_cli":   "notified",
    "rejected":          "pending",
    "ambiguous_timeout": "ambiguous",
    "adapter_disabled":  "blocked",
}


# --- Shared package-ownership helpers --------------------------------------


def _assert_no_active_transaction(db: sqlite3.Connection) -> None:
    """Reject an active caller transaction (design R6).

    A package entry point owning ``BEGIN IMMEDIATE`` cannot run inside a
    caller's open transaction without silently committing or rolling back the
    caller's unrelated work. Raising here protects the caller from both
    footguns.
    """
    if db.in_transaction:
        raise sqlite3.ProgrammingError(
            "cross-tool-awareness package entry point refuses to run inside "
            "an active caller transaction; use a dedicated connection or "
            "close the outer transaction first (design R6)"
        )


def _check_snapshot_freshness(
    completion_now: int,
    snapshot: RegistrationSnapshot,
    *,
    label: str,
) -> Optional[str]:
    """Return a diagnostic string if the snapshot is stale, else None."""
    lag = completion_now - snapshot.proof_observed_at
    if lag < 0:
        return f"{label}_snapshot_proof_in_future"
    if lag > _FRESHNESS_WINDOW_SECONDS:
        return f"{label}_snapshot_stale"
    return None


def _claimant_live_in_sql(
    db: sqlite3.Connection,
    claimant: ClaimantIdentity,
    *,
    now: int,
) -> bool:
    """Return True iff the claimant's session_registrations row is live.

    Live means: exact (session_uuid, session_incarnation_id) match AND the
    stored registration_fence matches AND not revoked AND not expired at
    ``now``. Caller must hold the write lock (``BEGIN IMMEDIATE``) before
    calling so the result is read-consistent with the subsequent UPDATE.
    """
    row = db.execute(
        "SELECT 1 FROM session_registrations"
        " WHERE session_uuid = ? AND session_incarnation_id = ?"
        "   AND registration_fence = ?"
        "   AND revoked_at IS NULL"
        "   AND expires_at > ?",
        (str(claimant.session_uuid), str(claimant.session_incarnation_id),
         claimant.registration_fence, int(now)),
    ).fetchone()
    return row is not None


# --- claim_delivery --------------------------------------------------------


def claim_delivery(
    db: sqlite3.Connection,
    *,
    now: int,
    lease_ttl_seconds: int = 30,
    claimant: ClaimantIdentity,
    clock: Callable[[], int],
    claimant_snapshot: RegistrationSnapshot,
    tool: str,
    wake_capability: str,
) -> Optional[ClaimedDelivery]:
    """Claim one due pending row or recover one expired claimed row.

    Owner-aware (v0.5.0). The claimed row's claimant_* columns are stamped
    with ``claimant``'s identity; the returned ``ClaimedDelivery`` carries
    the three owner fields that every downstream call must present.

    ``dispatching`` rows are never time-recoverable (R8).

    Routing: only picks eligible deliveries whose target session_registration
    matches ``tool`` and ``wake_capability``. For now this is a facade-level
    gate on the writer; see composite_storage for the full facade.
    """
    _assert_no_active_transaction(db)
    completion_now = int(clock())
    freshness = _check_snapshot_freshness(completion_now, claimant_snapshot,
                                          label="claimant")
    if freshness is not None:
        return None
    db.execute("BEGIN IMMEDIATE")
    try:
        if not _claimant_live_in_sql(db, claimant, now=completion_now):
            db.execute("ROLLBACK")
            return None
        result = _claim_delivery_in_transaction(
            db, now=now, lease_ttl_seconds=lease_ttl_seconds,
            claimant=claimant, tool=tool, wake_capability=wake_capability,
        )
        db.execute("COMMIT")
        return result
    except Exception:
        try:
            db.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise


def _claim_delivery_in_transaction(
    db: sqlite3.Connection,
    *,
    now: int,
    lease_ttl_seconds: int,
    claimant: ClaimantIdentity,
    tool: str,
    wake_capability: str,
) -> Optional[ClaimedDelivery]:
    """Claim inside a transaction already owned by the package.

    Writes the full owner tuple into the claimed row.
    """
    token = secrets.token_hex(16)
    db.row_factory = sqlite3.Row
    row = db.execute(
        "SELECT td.message_id, td.target_session_uuid,"
        "       td.target_session_incarnation_id,"
        "       td.delivery_state, td.claim_lease_token "
        "FROM targeted_deliveries td "
        "JOIN session_registrations sr"
        "  ON sr.session_uuid = td.target_session_uuid"
        " AND sr.session_incarnation_id = td.target_session_incarnation_id "
        "WHERE sr.tool = ? AND sr.wake_capability = ? "
        "  AND ((td.delivery_state = 'pending' AND td.next_attempt_at <= ?) "
        "    OR (td.delivery_state = 'claimed' AND td.claim_lease_expires_at <= ?)) "
        "ORDER BY CASE WHEN td.delivery_state = 'pending' THEN td.next_attempt_at "
        "              ELSE td.claim_lease_expires_at END ASC "
        "LIMIT 1",
        (tool, wake_capability, now, now),
    ).fetchone()
    if row is None:
        return None
    cur = db.execute(
        "UPDATE targeted_deliveries "
        "SET delivery_state = 'claimed', claim_lease_token = ?, "
        "    claim_lease_expires_at = ?, "
        "    claimant_session_uuid = ?, claimant_session_incarnation_id = ?, "
        "    claimant_registration_fence = ? "
        "WHERE message_id = ? AND target_session_uuid = ? "
        "  AND target_session_incarnation_id = ? "
        "  AND ((delivery_state = 'pending') "
        "       OR (delivery_state = 'claimed' "
        "           AND claim_lease_token = ? "
        "           AND claim_lease_expires_at <= ?))",
        (token, now + int(lease_ttl_seconds),
         str(claimant.session_uuid), str(claimant.session_incarnation_id),
         claimant.registration_fence,
         row["message_id"], row["target_session_uuid"],
         row["target_session_incarnation_id"],
         row["claim_lease_token"], now),
    )
    if cur.rowcount == 0:
        return None
    return ClaimedDelivery(
        message_id=row["message_id"],
        target_session_uuid=row["target_session_uuid"],
        target_session_incarnation_id=row["target_session_incarnation_id"],
        claim_lease_token=token,
        claimant_session_uuid=str(claimant.session_uuid),
        claimant_session_incarnation_id=str(claimant.session_incarnation_id),
        claimant_registration_fence=claimant.registration_fence,
    )


# --- pre_adapter_mark ------------------------------------------------------


def pre_adapter_mark(
    db: sqlite3.Connection,
    *,
    message_id,
    target_session_uuid,
    target_session_incarnation_id,
    lease_token: str,
    now: int,
    claimant: ClaimantIdentity,
    clock: Callable[[], int],
    claimant_snapshot: RegistrationSnapshot,
    target_snapshot: RegistrationSnapshot,
    tool: str,
    wake_capability: str,
) -> bool:
    """Atomically transition the exact owned claim to ``dispatching`` AND write
    the ``dispatch_ownership`` witness, in a single ``BEGIN IMMEDIATE`` txn.

    Returns False when any gate fails: stale snapshot, claimant not live in
    SQL, wrong owner / token / key / state, or any insert collision. On False
    the caller MUST NOT invoke the adapter — the delivery stays claimed with
    its lease intact (or whatever state the gate found it in).

    Design R7: the +/- 30 s freshness window is evaluated against the
    injected ``clock()``, not against the stale ``now`` scheduling input.
    Design R4: the witness is inserted inside the same transaction as the
    state change, so a crash between the state advance and the witness
    insert is impossible.
    """
    _assert_no_active_transaction(db)
    completion_now = int(clock())
    if _check_snapshot_freshness(completion_now, claimant_snapshot,
                                 label="claimant") is not None:
        return False
    if _check_snapshot_freshness(completion_now, target_snapshot,
                                 label="target") is not None:
        return False
    db.execute("BEGIN IMMEDIATE")
    try:
        if not _claimant_live_in_sql(db, claimant, now=completion_now):
            db.execute("ROLLBACK")
            return False
        # Verify the target is still a live registration matching the
        # target_snapshot's PID/started_at. target_snapshot is already
        # fresh-window-vetted above; the SQL join here checks persistence.
        target_live = db.execute(
            "SELECT 1 FROM session_registrations"
            " WHERE session_uuid = ? AND session_incarnation_id = ?"
            "   AND holder_pid = ? AND process_started_at = ?"
            "   AND revoked_at IS NULL AND expires_at > ?",
            (str(target_session_uuid), str(target_session_incarnation_id),
             target_snapshot.holder_pid, target_snapshot.process_started_at,
             completion_now),
        ).fetchone()
        if target_live is None:
            db.execute("ROLLBACK")
            return False
        # Exact-owner UPDATE: wrong claimant, wrong token, or wrong key
        # changes zero rows.
        cursor = db.execute(
            "UPDATE targeted_deliveries "
            "SET delivery_state = 'dispatching', dispatch_started_at = ? "
            "WHERE message_id = ? AND target_session_uuid = ? "
            "  AND target_session_incarnation_id = ? "
            "  AND claim_lease_token = ? "
            "  AND claimant_session_uuid = ? "
            "  AND claimant_session_incarnation_id = ? "
            "  AND claimant_registration_fence = ? "
            "  AND delivery_state = 'claimed' "
            "  AND claim_lease_expires_at > ?",
            (
                completion_now,
                str(message_id), str(target_session_uuid),
                str(target_session_incarnation_id),
                lease_token,
                str(claimant.session_uuid),
                str(claimant.session_incarnation_id),
                claimant.registration_fence,
                completion_now,
            ),
        )
        if cursor.rowcount != 1:
            db.execute("ROLLBACK")
            return False
        # Insert the witness inside the SAME transaction (R4). PRIMARY KEY on
        # claim_lease_token means a duplicate here is a fatal programming
        # error — the token was issued by this very claim.
        db.execute(
            "INSERT INTO dispatch_ownership("
            " claim_lease_token, message_id, target_session_uuid,"
            " target_session_incarnation_id,"
            " claimant_session_uuid, claimant_session_incarnation_id,"
            " claimant_registration_fence, dispatch_started_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (lease_token, str(message_id), str(target_session_uuid),
             str(target_session_incarnation_id),
             str(claimant.session_uuid),
             str(claimant.session_incarnation_id),
             claimant.registration_fence, completion_now),
        )
        db.execute("COMMIT")
        return True
    except Exception:
        try:
            db.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise


def list_stuck_dispatching(
    db: sqlite3.Connection,
    now: int,
    *,
    stale_after_seconds: int,
) -> list[StuckDispatching]:
    """Return old ``dispatching`` rows without mutating or recovering them.

    A row whose age is exactly ``stale_after_seconds`` is included.  This is
    an inspection surface only: elapsed time never makes a dispatching row
    eligible for automatic retry.
    """
    if (
        isinstance(stale_after_seconds, bool)
        or not isinstance(stale_after_seconds, int)
        or stale_after_seconds < 0
    ):
        raise ValueError("stale_after_seconds must be a nonnegative int")
    db.row_factory = sqlite3.Row
    rows = db.execute(
        "SELECT message_id, target_session_uuid, "
        "       target_session_incarnation_id, dispatch_started_at, "
        "       claim_lease_token "
        "FROM targeted_deliveries "
        "WHERE delivery_state = 'dispatching' "
        "  AND (? - dispatch_started_at) >= ? "
        "ORDER BY dispatch_started_at ASC, message_id ASC",
        (int(now), stale_after_seconds),
    ).fetchall()
    return [
        StuckDispatching(
            message_id=UUID(row["message_id"]),
            target_session_uuid=UUID(row["target_session_uuid"]),
            target_session_incarnation_id=UUID(
                row["target_session_incarnation_id"]
            ),
            dispatch_started_at=row["dispatch_started_at"],
            claim_lease_token=row["claim_lease_token"],
        )
        for row in rows
    ]


def pre_adapter_release(
    db: sqlite3.Connection,
    *,
    claim_lease_token: str,
    next_state: str,
    diagnostic: str,
    next_attempt_at: int,
    claimant: ClaimantIdentity,
    clock: Callable[[], int],
    message_id,
    target_session_uuid,
    target_session_incarnation_id,
) -> PreAdapterReleaseResult:
    """Release a claim BEFORE calling adapter.wake().

    Does NOT create a queue_attempts row. Does NOT increment attempt_count.
    Used when target revalidation fails (next_state='pending') or adapter
    missing/not-enabled (next_state='blocked').

    v0.5.0: requires exact owner/key/token. Still only operates on
    delivery_state == 'claimed' — a release cannot enter an adapter and must
    remain possible when route configuration or claimant liveness has
    changed, so no snapshot/route re-check at this point (design R5
    rationale on release not taking route params).
    """
    assert next_state in ("pending", "blocked"), f"invalid pre-adapter next_state {next_state!r}"
    _assert_no_active_transaction(db)
    _ = int(clock())  # kwarg consumed for API-shape consistency; not read here
    db.execute("BEGIN IMMEDIATE")
    try:
        cur = db.execute(
            "UPDATE targeted_deliveries "
            "SET delivery_state = ?, claim_lease_token = NULL, "
            "    claim_lease_expires_at = NULL, "
            "    claimant_session_uuid = NULL, "
            "    claimant_session_incarnation_id = NULL, "
            "    claimant_registration_fence = NULL, "
            "    next_attempt_at = ?, "
            "    last_error_class = ? "
            "WHERE message_id = ? AND target_session_uuid = ? "
            "  AND target_session_incarnation_id = ? "
            "  AND claim_lease_token = ? "
            "  AND claimant_session_uuid = ? "
            "  AND claimant_session_incarnation_id = ? "
            "  AND claimant_registration_fence = ? "
            "  AND delivery_state = 'claimed'",
            (next_state, int(next_attempt_at), diagnostic,
             str(message_id), str(target_session_uuid),
             str(target_session_incarnation_id),
             claim_lease_token,
             str(claimant.session_uuid),
             str(claimant.session_incarnation_id),
             claimant.registration_fence),
        )
        if cur.rowcount == 0:
            db.execute("ROLLBACK")
            return PreAdapterReleaseResult(status="lease_released_or_observed")
        db.execute("COMMIT")
    except Exception:
        try:
            db.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    return PreAdapterReleaseResult(status="released", diagnostic=diagnostic)


def finalize_attempt(
    db: sqlite3.Connection,
    *,
    claim_lease_token: str,
    message_id: str,
    target_session_uuid: str,
    target_session_incarnation_id: str,
    outcome: str,
    reason: str,
    attempted_at: int,
    now: int,
    next_attempt_at: int,
    claimant: ClaimantIdentity,
    clock: Callable[[], int],
) -> FinalizationResult:
    """Create a queue_attempts row + fenced delivery advance.

    v0.5.0: authenticates against the dispatch_ownership witness FIRST. A
    missing witness (wrong token or wrong owner) returns ``unauthorized`` and
    changes zero rows. A matching witness proceeds:

      - If the delivery is still ``dispatching`` with matching owner, insert
        the attempt and advance state.
      - If the delivery is already ``observed`` (receipt won first), insert
        the attempt (to record what we tried) but preserve ``observed`` —
        return ``receipt_already_won``.
      - If the attempt-insert hits ``UNIQUE constraint failed:
        queue_attempts.claim_lease_token``, this is an exact retry of a
        previously-finalized attempt — return ``already_finalized`` without
        state change.

    Design R4: receipt handling may clear active lease/owner fields on the
    delivery (via tr_receipt_marks_observed) but the dispatch_ownership
    witness is retained — this authentication path requires it.
    """
    if outcome not in _OUTCOME_TO_STATE:
        raise ValueError(f"invalid outcome {outcome!r}")
    new_state = _OUTCOME_TO_STATE[outcome]
    _assert_no_active_transaction(db)
    _ = int(clock())  # kwarg consumed for API-shape consistency
    db.execute("BEGIN IMMEDIATE")
    try:
        # Authenticate against the durable witness. Exact owner + token + key
        # required; wrong anything → zero rows → unauthorized.
        witness = db.execute(
            "SELECT 1 FROM dispatch_ownership"
            " WHERE claim_lease_token = ?"
            "   AND message_id = ? AND target_session_uuid = ?"
            "   AND target_session_incarnation_id = ?"
            "   AND claimant_session_uuid = ?"
            "   AND claimant_session_incarnation_id = ?"
            "   AND claimant_registration_fence = ?",
            (claim_lease_token, str(message_id), str(target_session_uuid),
             str(target_session_incarnation_id),
             str(claimant.session_uuid),
             str(claimant.session_incarnation_id),
             claimant.registration_fence),
        ).fetchone()
        if witness is None:
            db.execute("ROLLBACK")
            return FinalizationResult(status="unauthorized",
                                      attempt_recorded=False)

        # Attempt insert. UNIQUE on claim_lease_token → exact retry.
        try:
            db.execute(
                "INSERT INTO queue_attempts "
                "(claim_lease_token, message_id, target_session_uuid, "
                " target_session_incarnation_id, outcome, attempted_at, "
                " finished_at, reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (claim_lease_token, str(message_id),
                 str(target_session_uuid),
                 str(target_session_incarnation_id), outcome,
                 int(attempted_at), int(now), reason),
            )
        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint failed: queue_attempts.claim_lease_token" in str(e):
                db.execute("ROLLBACK")
                return FinalizationResult(status="already_finalized",
                                          attempt_recorded=False)
            raise

        # Advance delivery state, but only if the owner still matches an
        # in-flight state. If a receipt won first (delivery_state='observed'),
        # the UPDATE finds zero rows and we return receipt_already_won.
        cur = db.execute(
            "UPDATE targeted_deliveries "
            "SET delivery_state = ?, claim_lease_token = NULL, "
            "    claim_lease_expires_at = NULL, "
            "    claimant_session_uuid = NULL, "
            "    claimant_session_incarnation_id = NULL, "
            "    claimant_registration_fence = NULL, "
            "    attempt_count = attempt_count + 1, "
            "    next_attempt_at = ?, last_error_class = ? "
            "WHERE message_id = ? AND target_session_uuid = ? "
            "  AND target_session_incarnation_id = ? "
            "  AND claimant_session_uuid = ? "
            "  AND claimant_session_incarnation_id = ? "
            "  AND claimant_registration_fence = ? "
            "  AND delivery_state NOT IN ('observed', 'undeliverable')",
            (new_state, int(next_attempt_at),
             outcome if outcome != "accepted_by_cli" else None,
             str(message_id), str(target_session_uuid),
             str(target_session_incarnation_id),
             str(claimant.session_uuid),
             str(claimant.session_incarnation_id),
             claimant.registration_fence),
        )
        if cur.rowcount == 0:
            current_state_row = db.execute(
                "SELECT delivery_state FROM targeted_deliveries "
                "WHERE message_id = ? AND target_session_uuid = ? "
                "  AND target_session_incarnation_id = ?",
                (str(message_id), str(target_session_uuid),
                 str(target_session_incarnation_id)),
            ).fetchone()
            current_state = current_state_row[0] if current_state_row else None
            db.execute("COMMIT")
            if current_state == "observed":
                return FinalizationResult(
                    status="receipt_already_won",
                    attempt_recorded=True,
                )
            return FinalizationResult(
                status="lease_already_released",
                attempt_recorded=True,
            )

        db.execute("COMMIT")
    except Exception:
        try:
            db.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise

    return FinalizationResult(
        status="finalized",
        new_state=new_state,
        attempt_recorded=True,
    )
