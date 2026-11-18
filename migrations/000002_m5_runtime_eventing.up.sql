ALTER TABLE workflow_versions
    ADD CONSTRAINT workflow_versions_snapshot_identity_unique
    UNIQUE (project_id, workflow_id, version_id, execution_snapshot_id);

CREATE TABLE workflow_draft_test_snapshots (
    project_id BINARY(16) NOT NULL,
    workflow_id BINARY(16) NOT NULL,
    draft_revision_id BINARY(16) NOT NULL,
    snapshot_id BINARY(16) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (project_id, workflow_id, draft_revision_id, snapshot_id),
    FOREIGN KEY (project_id, workflow_id, draft_revision_id)
        REFERENCES workflow_draft_revisions(project_id, workflow_id, draft_revision_id),
    FOREIGN KEY (project_id, workflow_id, snapshot_id)
        REFERENCES workflow_execution_snapshots(project_id, workflow_id, snapshot_id)
);

CREATE TABLE workflow_runs (
    run_id BINARY(16) PRIMARY KEY,
	project_id BINARY(16) NOT NULL,
    workflow_id BINARY(16) NOT NULL,
    snapshot_id BINARY(16) NOT NULL,
    published_version_id BINARY(16),
    execution_identity_id BINARY(16) NOT NULL,
    purpose VARCHAR(255) NOT NULL CHECK (purpose IN ('test', 'production')),
    definition_source VARCHAR(255) NOT NULL CHECK (definition_source IN ('draft_snapshot', 'published_version')),
    definition_hash VARCHAR(255) NOT NULL CHECK (REGEXP_LIKE(definition_hash, '^[0-9a-f]{64}$')),
    state VARCHAR(255) NOT NULL CHECK (state IN ('pending', 'running', 'succeeded', 'failed', 'canceled', 'timed_out')),
    state_version BIGINT NOT NULL CHECK (state_version > 0),
    input_json JSON NOT NULL CHECK (JSON_TYPE(input_json) = 'OBJECT'),
    workflow_output_json JSON CHECK (workflow_output_json IS NULL OR JSON_TYPE(workflow_output_json) = 'OBJECT'),
    execution_node_ids JSON NOT NULL DEFAULT (JSON_ARRAY()) CHECK (JSON_TYPE(execution_node_ids) = 'ARRAY'),
    termination_intent_json JSON CHECK (termination_intent_json IS NULL OR JSON_TYPE(termination_intent_json) = 'OBJECT'),
    deadline_at DATETIME(6) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE (project_id, run_id),
	FOREIGN KEY (project_id) REFERENCES projects(project_id),
    FOREIGN KEY (project_id, workflow_id)
        REFERENCES workflows(project_id, workflow_id),
    FOREIGN KEY (project_id, workflow_id, snapshot_id)
        REFERENCES workflow_execution_snapshots(project_id, workflow_id, snapshot_id),
    FOREIGN KEY (project_id, execution_identity_id)
        REFERENCES project_execution_identities(project_id, execution_identity_id),
    FOREIGN KEY (project_id, workflow_id, published_version_id, snapshot_id)
        REFERENCES workflow_versions(project_id, workflow_id, version_id, execution_snapshot_id),
    CHECK (deadline_at > created_at),
    CHECK (
        (purpose = 'test' AND definition_source = 'draft_snapshot' AND published_version_id IS NULL)
        OR
        (purpose = 'production' AND definition_source = 'published_version' AND published_version_id IS NOT NULL)
    ),
    CHECK ((state = 'pending' AND execution_node_ids = (JSON_ARRAY())) OR state <> 'pending')
);

CREATE TABLE node_runs (
    node_run_id BINARY(16) PRIMARY KEY,
    project_id BINARY(16) NOT NULL,
    run_id BINARY(16) NOT NULL,
    execution_node_id VARCHAR(255) NOT NULL CHECK (char_length(execution_node_id) BETWEEN 1 AND 200),
    kind VARCHAR(255) NOT NULL CHECK (kind IN ('control', 'task')),
    state VARCHAR(255) NOT NULL CHECK (state IN (
        'pending', 'ready', 'queued', 'running', 'retry_wait',
        'succeeded', 'failed', 'timed_out', 'skipped', 'canceled'
    )),
    state_version BIGINT NOT NULL CHECK (state_version > 0),
    activated BOOLEAN NOT NULL DEFAULT FALSE,
    selected_route VARCHAR(255) NOT NULL DEFAULT '',
    resolved_inputs_json JSON CHECK (resolved_inputs_json IS NULL OR JSON_TYPE(resolved_inputs_json) = 'OBJECT'),
    current_attempt_id BINARY(16),
    effective_attempt_id BINARY(16),
    next_attempt_seq INTEGER NOT NULL DEFAULT 0 CHECK (next_attempt_seq >= 0),
    business_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (business_attempt_count >= 0),
    recovery_count INTEGER NOT NULL DEFAULT 0 CHECK (recovery_count >= 0),
    next_attempt_kind VARCHAR(255) NOT NULL DEFAULT 'initial' CHECK (next_attempt_kind IN ('initial', 'business_retry', 'recovery')),
    priority INTEGER NOT NULL DEFAULT 0,
    ready_at DATETIME(6),
    next_retry_at DATETIME(6),
    failure_json JSON CHECK (failure_json IS NULL OR JSON_TYPE(failure_json) = 'OBJECT'),
    cancel_reason VARCHAR(255) NOT NULL DEFAULT '',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE (project_id, run_id, execution_node_id),
    UNIQUE (project_id, run_id, node_run_id),
    FOREIGN KEY (project_id, run_id)
        REFERENCES workflow_runs(project_id, run_id),
    CHECK ((state = 'skipped' AND activated = FALSE) OR state <> 'skipped'),
    CHECK ((effective_attempt_id IS NULL) OR state = 'succeeded'),
    CHECK ((state = 'ready' AND ready_at IS NOT NULL) OR state <> 'ready'),
    CHECK ((state = 'retry_wait' AND next_retry_at IS NOT NULL) OR state <> 'retry_wait'),
    CHECK ((kind = 'control' AND current_attempt_id IS NULL AND effective_attempt_id IS NULL AND next_attempt_seq = 0) OR kind = 'task')
);

CREATE TABLE node_attempts (
    attempt_id BINARY(16) PRIMARY KEY,
    project_id BINARY(16) NOT NULL,
    run_id BINARY(16) NOT NULL,
    node_run_id BINARY(16) NOT NULL,
    attempt_seq INTEGER NOT NULL CHECK (attempt_seq > 0),
    attempt_kind VARCHAR(255) NOT NULL CHECK (attempt_kind IN ('initial', 'business_retry', 'recovery')),
    state VARCHAR(255) NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'timed_out', 'canceled', 'lost')),
    state_version BIGINT NOT NULL CHECK (state_version > 0),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    recovery_count INTEGER NOT NULL DEFAULT 0 CHECK (recovery_count >= 0),
    lease_token BINARY(16),
    lease_owner VARCHAR(255),
    lease_expires_at DATETIME(6),
    fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    executor_build VARCHAR(255),
    error_json JSON CHECK (error_json IS NULL OR JSON_TYPE(error_json) = 'OBJECT'),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE (project_id, run_id, node_run_id, attempt_seq),
    UNIQUE (project_id, run_id, attempt_id),
    UNIQUE (project_id, run_id, node_run_id, attempt_id),
    UNIQUE (attempt_id),
    FOREIGN KEY (project_id, run_id, node_run_id)
        REFERENCES node_runs(project_id, run_id, node_run_id),
    CHECK (
        (state = 'running' AND lease_token IS NOT NULL AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL AND fencing_token > 0)
        OR state <> 'running'
    )
);

ALTER TABLE node_runs
    ADD CONSTRAINT node_runs_current_attempt_fk
        FOREIGN KEY (project_id, run_id, node_run_id, current_attempt_id)
        REFERENCES node_attempts(project_id, run_id, node_run_id, attempt_id),
    ADD CONSTRAINT node_runs_effective_attempt_fk
        FOREIGN KEY (project_id, run_id, node_run_id, effective_attempt_id)
        REFERENCES node_attempts(project_id, run_id, node_run_id, attempt_id);

CREATE TABLE node_output_values (
    project_id BINARY(16) NOT NULL,
    attempt_id BINARY(16) NOT NULL,
    run_id BINARY(16) NOT NULL,
    node_run_id BINARY(16) NOT NULL,
    value_json JSON NOT NULL CHECK (JSON_TYPE(value_json) = 'OBJECT'),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 2 AND size_bytes <= 1048576),
    content_hash VARCHAR(255) NOT NULL CHECK (REGEXP_LIKE(content_hash, '^[0-9a-f]{64}$')),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (project_id, attempt_id),
    FOREIGN KEY (project_id, run_id, attempt_id)
        REFERENCES node_attempts(project_id, run_id, attempt_id),
    FOREIGN KEY (project_id, run_id, node_run_id)
        REFERENCES node_runs(project_id, run_id, node_run_id)
);

CREATE TABLE outbox_events (
    event_id BINARY(16) PRIMARY KEY,
    project_id BINARY(16) NOT NULL,
    run_id BINARY(16) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL CHECK (aggregate_type IN ('workflow_run', 'node_attempt')),
    aggregate_id BINARY(16) NOT NULL,
    event_type VARCHAR(255) NOT NULL CHECK (event_type IN (
        'run.created', 'run.cancel_requested', 'attempt.completed',
        'attempt.lost', 'retry.due', 'run.deadline_reached'
    )),
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
    UNIQUE (project_id, run_id, event_id),
    FOREIGN KEY (project_id, run_id)
        REFERENCES workflow_runs(project_id, run_id),
    CHECK (
        (claim_token IS NULL AND claim_owner IS NULL AND claim_expires_at IS NULL)
        OR
        (claim_token IS NOT NULL AND claim_owner IS NOT NULL AND claim_expires_at IS NOT NULL)
    )
);

CREATE TABLE inbox_events (
    project_id BINARY(16) NOT NULL,
    run_id BINARY(16) NOT NULL,
    consumer_name VARCHAR(255) NOT NULL CHECK (char_length(consumer_name) BETWEEN 1 AND 200),
    event_id BINARY(16) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    received_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    processed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (consumer_name, event_id),
    FOREIGN KEY (project_id, run_id)
        REFERENCES workflow_runs(project_id, run_id)
);

CREATE TABLE runtime_idempotency (
	project_id BINARY(16) NOT NULL,
	command_name VARCHAR(50) NOT NULL CHECK (command_name IN ('test_draft', 'create_run', 'create_historical_version_run')),
	target_scope VARCHAR(200) NOT NULL CHECK (char_length(target_scope) BETWEEN 1 AND 200),
	idempotency_key VARCHAR(200) NOT NULL CHECK (char_length(idempotency_key) BETWEEN 8 AND 200),
	request_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL CHECK (REGEXP_LIKE(request_hash, '^[0-9a-f]{64}$')),
    run_id BINARY(16) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	PRIMARY KEY (project_id, command_name, target_scope, idempotency_key),
	FOREIGN KEY (project_id) REFERENCES projects(project_id)
);

CREATE INDEX node_runs_ready_idx
	ON node_runs(state, project_id, priority DESC, ready_at, node_run_id);
CREATE INDEX node_runs_retry_idx
	ON node_runs(state, next_retry_at, node_run_id);
CREATE INDEX node_attempts_lease_idx
	ON node_attempts(state, lease_expires_at, attempt_id);
CREATE INDEX workflow_runs_query_idx
    ON workflow_runs(project_id, state, updated_at DESC, run_id);
CREATE INDEX outbox_events_relay_idx
	ON outbox_events(published_at, available_at, occurred_at, event_id);
CREATE INDEX outbox_events_claim_idx
	ON outbox_events(published_at, claim_token, claim_expires_at, event_id);
CREATE INDEX inbox_events_retention_idx
    ON inbox_events(processed_at, event_id);
