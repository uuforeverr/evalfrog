ALTER TABLE node_runs
    ADD COLUMN operation_type VARCHAR(255),
    ADD COLUMN operation_version INTEGER,
    ADD COLUMN resource_class VARCHAR(255);

ALTER TABLE node_runs
	MODIFY COLUMN operation_type VARCHAR(255) NOT NULL,
	MODIFY COLUMN operation_version INTEGER NOT NULL,
    ADD CONSTRAINT node_runs_operation_identity_check
        CHECK (char_length(operation_type) BETWEEN 1 AND 200 AND operation_version > 0),
    ADD CONSTRAINT node_runs_resource_class_check
        CHECK (
            (kind = 'control' AND resource_class IS NULL)
            OR
            (kind = 'task' AND resource_class IN ('builtin', 'sandbox'))
        );

CREATE TABLE node_task_outbox (
    task_id BINARY(16) PRIMARY KEY,
    project_id BINARY(16) NOT NULL,
    run_id BINARY(16) NOT NULL,
    node_run_id BINARY(16) NOT NULL,
    execution_node_id VARCHAR(255) NOT NULL CHECK (char_length(execution_node_id) BETWEEN 1 AND 200),
    attempt_id BINARY(16) NOT NULL,
    attempt_seq INTEGER NOT NULL CHECK (attempt_seq > 0),
    resource_class VARCHAR(255) NOT NULL CHECK (resource_class IN ('builtin', 'sandbox')),
    message_version INTEGER NOT NULL CHECK (message_version = 1),
    occurred_at DATETIME(6) NOT NULL,
    trace_id VARCHAR(255) NOT NULL CHECK (char_length(trace_id) BETWEEN 1 AND 200),
    available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    claim_owner VARCHAR(255),
    claim_token BINARY(16),
    claim_expires_at DATETIME(6),
    publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    published_at DATETIME(6),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE (project_id, run_id, task_id),
    UNIQUE (project_id, run_id, attempt_id),
    FOREIGN KEY (project_id, run_id, node_run_id)
        REFERENCES node_runs(project_id, run_id, node_run_id),
    FOREIGN KEY (project_id, run_id, attempt_id)
        REFERENCES node_attempts(project_id, run_id, attempt_id),
    CHECK (
        (claim_token IS NULL AND claim_owner IS NULL AND claim_expires_at IS NULL)
        OR
        (claim_token IS NOT NULL AND claim_owner IS NOT NULL AND claim_expires_at IS NOT NULL)
    )
);

CREATE INDEX node_task_outbox_relay_idx
	ON node_task_outbox(published_at, resource_class, available_at, occurred_at, task_id);

CREATE INDEX node_task_outbox_claim_idx
	ON node_task_outbox(published_at, claim_token, claim_expires_at, task_id);

CREATE INDEX node_attempts_project_inflight_idx
	ON node_attempts(state, project_id, attempt_id);
