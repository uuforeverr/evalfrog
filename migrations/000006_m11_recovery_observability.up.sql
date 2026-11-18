-- M11 keeps recovery as a durable wake-up concern. MySQL remains the
-- authority for Runtime state; this migration adds only recovery de-duplication,
-- trace correlation and audit facts.

ALTER TABLE workflow_runs
	ADD COLUMN trace_id VARCHAR(255) NOT NULL,
    ADD CONSTRAINT workflow_runs_trace_id_check
        CHECK (char_length(trace_id) BETWEEN 1 AND 200);

-- One row per still-actionable wake-up identity. Repeated scans may emit a
-- new event only after the configured cooldown; the Engine Inbox and CAS make
-- every emitted event safe to deliver at least once.
CREATE TABLE runtime_recovery_wakeups (
    project_id BINARY(16) NOT NULL,
    run_id BINARY(16) NOT NULL,
    event_type VARCHAR(255) NOT NULL CHECK (event_type IN (
        'run.created', 'run.cancel_requested', 'attempt.completed',
        'attempt.lost', 'retry.due', 'run.deadline_reached'
    )),
    aggregate_id BINARY(16) NOT NULL,
    last_event_id BINARY(16) NOT NULL,
    last_emitted_at DATETIME(6) NOT NULL,
    emit_count INTEGER NOT NULL DEFAULT 1 CHECK (emit_count > 0),
    PRIMARY KEY (project_id, run_id, event_type, aggregate_id),
    FOREIGN KEY (project_id, run_id)
        REFERENCES workflow_runs(project_id, run_id)
);

-- Audit records intentionally contain identifiers, event type and safe
-- operational metadata only: never Workflow input/output, Secret references,
-- lease tokens or executor stderr.
CREATE TABLE runtime_audit_events (
    audit_id BINARY(16) PRIMARY KEY,
    project_id BINARY(16) NOT NULL,
    run_id BINARY(16) NOT NULL,
    action VARCHAR(255) NOT NULL CHECK (action IN (
        'run.cancel_requested', 'recovery.wakeup_emitted', 'recovery.manual_replay'
    )),
    actor_type VARCHAR(255) NOT NULL CHECK (actor_type IN ('principal', 'system')),
    actor_id VARCHAR(255) NOT NULL CHECK (char_length(actor_id) BETWEEN 1 AND 200),
    trace_id VARCHAR(255) NOT NULL CHECK (char_length(trace_id) BETWEEN 1 AND 200),
    details_json JSON NOT NULL DEFAULT (JSON_OBJECT()) CHECK (JSON_TYPE(details_json) = 'OBJECT'),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    FOREIGN KEY (project_id, run_id)
        REFERENCES workflow_runs(project_id, run_id)
);

CREATE INDEX workflow_runs_deadline_idx
	ON workflow_runs(state, deadline_at, run_id);
CREATE INDEX runtime_audit_events_run_idx
    ON runtime_audit_events(project_id, run_id, created_at DESC, audit_id);
