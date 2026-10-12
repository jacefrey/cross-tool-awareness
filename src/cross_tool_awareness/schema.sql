-- Shared awareness schema v2 (v0.2.0)
-- Design authority: machine-manager/docs/superpowers/specs/2026-10-11-shared-awareness-package-design.md v4
-- Contract authority: codex-manager/docs/superpowers/specs/2026-10-05-codex-gate1-contracts.md (0087ebe)

-- 3.1 session_registrations
CREATE TABLE IF NOT EXISTS session_registrations (
    session_uuid                TEXT NOT NULL,
    session_incarnation_id      TEXT NOT NULL,
    alias                       TEXT NOT NULL,
    tool                        TEXT NOT NULL CHECK (tool IN ('codex', 'claude')),
    project_slug                TEXT NOT NULL,
    project_root                TEXT NOT NULL,
    role                        TEXT NOT NULL CHECK (role IN ('manager', 'worker', 'reviewer', 'general')),
    state                       TEXT NOT NULL CHECK (state IN ('active', 'busy', 'idle', 'stopping', 'stale')),
    wake_capability             TEXT NOT NULL CHECK (wake_capability IN ('codex_queue', 'claude_socket', 'notify_only')),
    registration_fence          TEXT NOT NULL,
    wake_locator                TEXT NOT NULL,
    started_at                  INTEGER NOT NULL,
    heartbeat_at                INTEGER NOT NULL,
    expires_at                  INTEGER NOT NULL,
    revoked_at                  INTEGER,
    proc_start_ns               INTEGER,
    PRIMARY KEY (session_uuid, session_incarnation_id),
    CHECK (started_at <= heartbeat_at),
    CHECK (heartbeat_at <= expires_at)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_alias_live
    ON session_registrations(alias)
    WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_tool_state
    ON session_registrations(tool, state)
    WHERE revoked_at IS NULL;

-- 3.2 session_capabilities
CREATE TABLE IF NOT EXISTS session_capabilities (
    verifier_sha256                     BLOB NOT NULL PRIMARY KEY,
    sender_session_uuid                 TEXT NOT NULL,
    sender_session_incarnation_id       TEXT NOT NULL,
    registration_fence                  TEXT NOT NULL,
    issued_at                           INTEGER NOT NULL,
    expires_at                          INTEGER NOT NULL,
    revoked_at                          INTEGER,
    CHECK (issued_at <= expires_at),
    FOREIGN KEY (sender_session_uuid, sender_session_incarnation_id)
        REFERENCES session_registrations(session_uuid, session_incarnation_id)
);

CREATE TRIGGER IF NOT EXISTS tr_capability_verifier_length
BEFORE INSERT ON session_capabilities
BEGIN
    SELECT RAISE(ABORT, 'verifier_sha256 must be exactly 32 bytes')
    WHERE length(NEW.verifier_sha256) != 32;
END;

-- 3.3 targeted_deliveries (FK to bridge messages(id))
CREATE TABLE IF NOT EXISTS targeted_deliveries (
    message_id                          TEXT NOT NULL,
    target_session_uuid                 TEXT NOT NULL,
    target_session_incarnation_id       TEXT NOT NULL,
    sender_session_uuid                 TEXT NOT NULL,
    sender_session_incarnation_id       TEXT NOT NULL,
    delivery_state                      TEXT NOT NULL CHECK (delivery_state IN (
        'pending', 'claimed', 'notified', 'ambiguous', 'blocked', 'observed', 'undeliverable'
    )),
    claim_lease_token                   TEXT,
    claim_lease_expires_at              INTEGER,
    created_at                          INTEGER NOT NULL,
    attempt_count                       INTEGER NOT NULL DEFAULT 0,
    next_attempt_at                     INTEGER NOT NULL,
    expires_at                          INTEGER NOT NULL,
    last_error_class                    TEXT,
    PRIMARY KEY (message_id, target_session_uuid, target_session_incarnation_id),
    FOREIGN KEY (message_id) REFERENCES messages(id)
);

CREATE INDEX IF NOT EXISTS idx_delivery_eligible
    ON targeted_deliveries(next_attempt_at)
    WHERE delivery_state = 'pending';

-- 3.4 queue_attempts
CREATE TABLE IF NOT EXISTS queue_attempts (
    attempt_id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    claim_lease_token                   TEXT NOT NULL UNIQUE,
    message_id                          TEXT NOT NULL,
    target_session_uuid                 TEXT NOT NULL,
    target_session_incarnation_id       TEXT NOT NULL,
    outcome                             TEXT NOT NULL CHECK (outcome IN (
        'accepted_by_cli', 'rejected', 'ambiguous_timeout', 'adapter_disabled'
    )),
    attempted_at                        INTEGER NOT NULL,
    finished_at                         INTEGER NOT NULL,
    reason                              TEXT NOT NULL,
    CHECK (attempted_at <= finished_at),
    CHECK (
        (outcome = 'accepted_by_cli' AND reason = '')
        OR (outcome != 'accepted_by_cli' AND length(reason) BETWEEN 1 AND 240)
    ),
    FOREIGN KEY (message_id, target_session_uuid, target_session_incarnation_id)
        REFERENCES targeted_deliveries(message_id, target_session_uuid, target_session_incarnation_id)
);

-- 3.5 disposition_receipts
CREATE TABLE IF NOT EXISTS disposition_receipts (
    message_id                          TEXT NOT NULL,
    target_session_uuid                 TEXT NOT NULL,
    target_session_incarnation_id       TEXT NOT NULL,
    correlation_id                      TEXT NOT NULL,
    consumer_id                         TEXT NOT NULL,
    receiver_session_uuid               TEXT NOT NULL,
    receiver_session_incarnation_id     TEXT NOT NULL,
    disposition                         TEXT NOT NULL CHECK (disposition IN (
        'accepted', 'deferred', 'declined', 'needs_user'
    )),
    observed_at                         INTEGER NOT NULL,
    reason                              TEXT NOT NULL,
    delivered_at                        INTEGER NOT NULL,
    PRIMARY KEY (message_id, target_session_uuid, target_session_incarnation_id),
    CHECK (receiver_session_uuid = target_session_uuid),
    CHECK (receiver_session_incarnation_id = target_session_incarnation_id),
    FOREIGN KEY (message_id, target_session_uuid, target_session_incarnation_id)
        REFERENCES targeted_deliveries(message_id, target_session_uuid, target_session_incarnation_id)
);

CREATE TRIGGER IF NOT EXISTS tr_receipt_requires_live_registration
BEFORE INSERT ON disposition_receipts
BEGIN
    SELECT RAISE(ABORT, 'receiver registration not live at delivered_at')
    WHERE NOT EXISTS (
        SELECT 1 FROM session_registrations
        WHERE session_uuid = NEW.receiver_session_uuid
          AND session_incarnation_id = NEW.receiver_session_incarnation_id
          AND revoked_at IS NULL
          AND expires_at > NEW.delivered_at
    );
END;

CREATE TRIGGER IF NOT EXISTS tr_receipt_requires_eligible_delivery_state
BEFORE INSERT ON disposition_receipts
BEGIN
    SELECT RAISE(ABORT, 'receipt not permitted: delivery state is not receipt-eligible')
    WHERE NOT EXISTS (
        SELECT 1 FROM targeted_deliveries
        WHERE message_id = NEW.message_id
          AND target_session_uuid = NEW.target_session_uuid
          AND target_session_incarnation_id = NEW.target_session_incarnation_id
          AND delivery_state IN ('claimed', 'notified', 'ambiguous')
    );
END;

CREATE TRIGGER IF NOT EXISTS tr_receipt_marks_observed
AFTER INSERT ON disposition_receipts
BEGIN
    UPDATE targeted_deliveries
    SET delivery_state = 'observed',
        claim_lease_token = NULL,
        claim_lease_expires_at = NULL
    WHERE message_id = NEW.message_id
      AND target_session_uuid = NEW.target_session_uuid
      AND target_session_incarnation_id = NEW.target_session_incarnation_id
      AND delivery_state IN ('claimed', 'notified', 'ambiguous');
END;

-- 3.6 dispatcher_lease
CREATE TABLE IF NOT EXISTS dispatcher_lease (
    singleton_pk                INTEGER PRIMARY KEY CHECK (singleton_pk = 1),
    holder_token                TEXT NOT NULL,
    holder_pid                  INTEGER NOT NULL,
    holder_proc_start_ns        INTEGER NOT NULL,
    acquired_at                 INTEGER NOT NULL,
    expires_at                  INTEGER NOT NULL,
    CHECK (acquired_at <= expires_at)
);

-- 3.7 awareness_schema_version
CREATE TABLE IF NOT EXISTS awareness_schema_version (
    version                     INTEGER PRIMARY KEY,
    applied_at                  INTEGER NOT NULL,
    package_version             TEXT NOT NULL
);

-- 3.8 notice_claims (v0.2.0): receiver-side atomic claim slot for duplicate-safe
-- disposition. Tracks the first observer of a notice until a receipt is
-- durably persisted. Receipt existence is the terminal state; a stale claim
-- past recovery_after seconds is reclaimable as 'recovered'.
CREATE TABLE IF NOT EXISTS notice_claims (
    message_id                          TEXT NOT NULL,
    target_session_uuid                 TEXT NOT NULL,
    target_session_incarnation_id       TEXT NOT NULL,
    consumer_id                         TEXT NOT NULL,
    correlation_id                      TEXT NOT NULL,
    registration_fence                  TEXT NOT NULL,
    claimed_at                          INTEGER NOT NULL,
    PRIMARY KEY (message_id, target_session_uuid, target_session_incarnation_id)
);
