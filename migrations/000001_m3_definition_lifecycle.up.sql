CREATE TABLE principals (
    principal_id BINARY(16) PRIMARY KEY,
    kind VARCHAR(255) NOT NULL CHECK (kind IN ('user', 'service_account')),
    display_name VARCHAR(255) NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 200),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

CREATE TABLE principal_credentials (
    credential_id BINARY(16) PRIMARY KEY,
    principal_id BINARY(16) NOT NULL,
    credential_hash BINARY(32) NOT NULL UNIQUE CHECK (octet_length(credential_hash) = 32),
    expires_at DATETIME(6),
    revoked_at DATETIME(6),
	created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	FOREIGN KEY (principal_id) REFERENCES principals(principal_id)
);

CREATE TABLE projects (
    project_id BINARY(16) PRIMARY KEY,
    name VARCHAR(255) NOT NULL CHECK (char_length(name) BETWEEN 1 AND 200),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

CREATE TABLE project_memberships (
	project_id BINARY(16) NOT NULL,
	principal_id BINARY(16) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	PRIMARY KEY (project_id, principal_id),
	FOREIGN KEY (project_id) REFERENCES projects(project_id),
	FOREIGN KEY (principal_id) REFERENCES principals(principal_id)
);

CREATE TABLE project_membership_permissions (
    project_id BINARY(16) NOT NULL,
    principal_id BINARY(16) NOT NULL,
    permission VARCHAR(255) NOT NULL CHECK (permission IN (
        'workflow.read', 'workflow.write', 'workflow.test', 'workflow.publish',
        'run.create', 'run.read', 'run.cancel', 'connection.use',
        'service.use', 'project.admin'
    )),
    PRIMARY KEY (project_id, principal_id, permission),
    FOREIGN KEY (project_id, principal_id)
        REFERENCES project_memberships(project_id, principal_id)
);

CREATE TABLE project_execution_identities (
    execution_identity_id BINARY(16) PRIMARY KEY,
	project_id BINARY(16) NOT NULL UNIQUE,
    display_name VARCHAR(255) NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 200),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	UNIQUE (project_id, execution_identity_id),
	FOREIGN KEY (project_id) REFERENCES projects(project_id)
);

CREATE TABLE secret_references (
    secret_reference_id BINARY(16) PRIMARY KEY,
	project_id BINARY(16) NOT NULL,
    provider VARCHAR(255) NOT NULL CHECK (char_length(provider) BETWEEN 1 AND 100),
    external_reference VARCHAR(1000) NOT NULL CHECK (char_length(external_reference) BETWEEN 1 AND 1000),
	 external_reference_hash BINARY(32) GENERATED ALWAYS AS (UNHEX(SHA2(external_reference, 256))) STORED,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE (project_id, secret_reference_id),
	UNIQUE (project_id, provider, external_reference_hash),
	FOREIGN KEY (project_id) REFERENCES projects(project_id)
);

CREATE TABLE connections (
    connection_id BINARY(16) PRIMARY KEY,
	project_id BINARY(16) NOT NULL,
    reference VARCHAR(255) NOT NULL CHECK (REGEXP_LIKE(reference, '^[a-z][a-z0-9_]{0,127}$')),
    base_url VARCHAR(4096) NOT NULL CHECK (char_length(base_url) BETWEEN 1 AND 4096),
    secret_reference_id BINARY(16),
    policy_json JSON NOT NULL DEFAULT (JSON_OBJECT()) CHECK (JSON_TYPE(policy_json) = 'OBJECT'),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE (project_id, connection_id),
    UNIQUE (project_id, reference),
	FOREIGN KEY (project_id) REFERENCES projects(project_id),
    FOREIGN KEY (project_id, secret_reference_id)
        REFERENCES secret_references(project_id, secret_reference_id)
);

CREATE TABLE connection_execution_grants (
    project_id BINARY(16) NOT NULL,
    connection_id BINARY(16) NOT NULL,
    execution_identity_id BINARY(16) NOT NULL,
    purpose VARCHAR(255) NOT NULL CHECK (purpose IN ('test', 'production')),
    PRIMARY KEY (project_id, connection_id, execution_identity_id, purpose),
    FOREIGN KEY (project_id, connection_id)
        REFERENCES connections(project_id, connection_id),
    FOREIGN KEY (project_id, execution_identity_id)
        REFERENCES project_execution_identities(project_id, execution_identity_id)
);

CREATE TABLE rpc_services (
    service_id BINARY(16) PRIMARY KEY,
	project_id BINARY(16) NOT NULL,
    reference VARCHAR(255) NOT NULL CHECK (REGEXP_LIKE(reference, '^[a-z][a-z0-9_]{0,127}$')),
    protocol VARCHAR(255) NOT NULL CHECK (char_length(protocol) BETWEEN 1 AND 50),
    discovery_reference VARCHAR(1000) NOT NULL CHECK (char_length(discovery_reference) BETWEEN 1 AND 1000),
    secret_reference_id BINARY(16),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE (project_id, service_id),
    UNIQUE (project_id, reference),
	FOREIGN KEY (project_id) REFERENCES projects(project_id),
    FOREIGN KEY (project_id, secret_reference_id)
        REFERENCES secret_references(project_id, secret_reference_id)
);

CREATE TABLE rpc_service_operations (
    project_id BINARY(16) NOT NULL,
    service_id BINARY(16) NOT NULL,
    operation VARCHAR(255) NOT NULL CHECK (char_length(operation) BETWEEN 1 AND 200),
    contract_revision VARCHAR(255) NOT NULL CHECK (char_length(contract_revision) BETWEEN 1 AND 200),
    request_schema JSON NOT NULL CHECK (JSON_TYPE(request_schema) = 'OBJECT'),
    response_schema JSON NOT NULL CHECK (JSON_TYPE(response_schema) = 'OBJECT'),
    idempotent BOOLEAN NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (project_id, service_id, operation),
    FOREIGN KEY (project_id, service_id)
        REFERENCES rpc_services(project_id, service_id)
);

CREATE TABLE rpc_service_execution_grants (
    project_id BINARY(16) NOT NULL,
    service_id BINARY(16) NOT NULL,
    execution_identity_id BINARY(16) NOT NULL,
    purpose VARCHAR(255) NOT NULL CHECK (purpose IN ('test', 'production')),
    PRIMARY KEY (project_id, service_id, execution_identity_id, purpose),
    FOREIGN KEY (project_id, service_id)
        REFERENCES rpc_services(project_id, service_id),
    FOREIGN KEY (project_id, execution_identity_id)
        REFERENCES project_execution_identities(project_id, execution_identity_id)
);

CREATE TABLE workflows (
    workflow_id BINARY(16) PRIMARY KEY,
	project_id BINARY(16) NOT NULL,
    name VARCHAR(255) NOT NULL CHECK (char_length(name) BETWEEN 1 AND 200),
    active_version_id BINARY(16),
    cloned_from_version_id BINARY(16),
	next_version_number BIGINT NOT NULL DEFAULT 1 CHECK (next_version_number > 0),
	created_by BINARY(16) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	UNIQUE (project_id, workflow_id),
	FOREIGN KEY (project_id) REFERENCES projects(project_id),
	FOREIGN KEY (created_by) REFERENCES principals(principal_id)
);

CREATE TABLE workflow_draft_revisions (
    draft_revision_id BINARY(16) PRIMARY KEY,
    project_id BINARY(16) NOT NULL,
    workflow_id BINARY(16) NOT NULL,
    revision_number BIGINT NOT NULL CHECK (revision_number > 0),
    ir_json JSON NOT NULL CHECK (JSON_TYPE(ir_json) = 'OBJECT'),
    catalog_revision VARCHAR(255) NOT NULL CHECK (char_length(catalog_revision) BETWEEN 1 AND 200),
    cloned_from_version_id BINARY(16),
	created_by BINARY(16) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE (project_id, workflow_id, revision_number),
    UNIQUE (project_id, workflow_id, draft_revision_id),
    UNIQUE (project_id, workflow_id, revision_number, draft_revision_id),
    FOREIGN KEY (project_id, workflow_id)
		REFERENCES workflows(project_id, workflow_id),
	FOREIGN KEY (created_by) REFERENCES principals(principal_id)
);

CREATE TABLE workflow_drafts (
    project_id BINARY(16) NOT NULL,
    workflow_id BINARY(16) NOT NULL,
    current_revision BIGINT NOT NULL CHECK (current_revision > 0),
    current_revision_id BINARY(16) NOT NULL,
    state_version BIGINT NOT NULL CHECK (state_version > 0),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (project_id, workflow_id),
    FOREIGN KEY (project_id, workflow_id)
        REFERENCES workflows(project_id, workflow_id),
    FOREIGN KEY (project_id, workflow_id, current_revision, current_revision_id)
        REFERENCES workflow_draft_revisions(project_id, workflow_id, revision_number, draft_revision_id)
);

CREATE TABLE workflow_execution_snapshots (
    snapshot_id BINARY(16) PRIMARY KEY,
    project_id BINARY(16) NOT NULL,
    workflow_id BINARY(16) NOT NULL,
    origin_kind VARCHAR(255) NOT NULL CHECK (origin_kind IN ('draft_test', 'published')),
    origin_id BINARY(16) NOT NULL,
    ir_json JSON NOT NULL CHECK (JSON_TYPE(ir_json) = 'OBJECT'),
    dsl_json JSON NOT NULL CHECK (JSON_TYPE(dsl_json) = 'OBJECT'),
    source_map_json JSON NOT NULL CHECK (JSON_TYPE(source_map_json) = 'OBJECT'),
    compiler_manifest_json JSON NOT NULL CHECK (JSON_TYPE(compiler_manifest_json) = 'OBJECT'),
    ir_hash VARCHAR(255) NOT NULL CHECK (REGEXP_LIKE(ir_hash, '^[0-9a-f]{64}$')),
    dsl_hash VARCHAR(255) NOT NULL CHECK (REGEXP_LIKE(dsl_hash, '^[0-9a-f]{64}$')),
    source_map_hash VARCHAR(255) NOT NULL CHECK (REGEXP_LIKE(source_map_hash, '^[0-9a-f]{64}$')),
    definition_hash VARCHAR(255) NOT NULL CHECK (REGEXP_LIKE(definition_hash, '^[0-9a-f]{64}$')),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE (project_id, workflow_id, snapshot_id),
    UNIQUE (project_id, workflow_id, definition_hash),
    FOREIGN KEY (project_id, workflow_id)
        REFERENCES workflows(project_id, workflow_id)
);

CREATE TABLE workflow_versions (
    version_id BINARY(16) PRIMARY KEY,
    project_id BINARY(16) NOT NULL,
    workflow_id BINARY(16) NOT NULL,
    version_number BIGINT NOT NULL CHECK (version_number > 0),
    source_draft_revision_id BINARY(16) NOT NULL,
    execution_snapshot_id BINARY(16) NOT NULL,
    change_log VARCHAR(4000) NOT NULL CHECK (char_length(change_log) <= 4000),
	created_by BINARY(16) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE (project_id, workflow_id, version_number),
    UNIQUE (project_id, workflow_id, version_id),
    UNIQUE (project_id, version_id),
    FOREIGN KEY (project_id, workflow_id)
        REFERENCES workflows(project_id, workflow_id),
    FOREIGN KEY (project_id, workflow_id, source_draft_revision_id)
        REFERENCES workflow_draft_revisions(project_id, workflow_id, draft_revision_id),
    FOREIGN KEY (project_id, workflow_id, execution_snapshot_id)
		REFERENCES workflow_execution_snapshots(project_id, workflow_id, snapshot_id),
	FOREIGN KEY (created_by) REFERENCES principals(principal_id)
);

ALTER TABLE workflows
    ADD CONSTRAINT workflows_active_version_fk
    FOREIGN KEY (project_id, workflow_id, active_version_id)
	REFERENCES workflow_versions(project_id, workflow_id, version_id),
    ADD CONSTRAINT workflows_cloned_from_version_fk
    FOREIGN KEY (project_id, cloned_from_version_id)
    REFERENCES workflow_versions(project_id, version_id);

ALTER TABLE workflow_draft_revisions
    ADD CONSTRAINT draft_revisions_cloned_from_version_fk
    FOREIGN KEY (project_id, cloned_from_version_id)
    REFERENCES workflow_versions(project_id, version_id);

CREATE TABLE workflow_definition_audits (
    audit_id BINARY(16) PRIMARY KEY,
    project_id BINARY(16) NOT NULL,
    workflow_id BINARY(16) NOT NULL,
    event_type VARCHAR(255) NOT NULL CHECK (event_type IN ('publish', 'rollback')),
    version_id BINARY(16) NOT NULL,
	principal_id BINARY(16) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    FOREIGN KEY (project_id, workflow_id, version_id)
		REFERENCES workflow_versions(project_id, workflow_id, version_id),
	FOREIGN KEY (principal_id) REFERENCES principals(principal_id)
);

CREATE TABLE definition_idempotency (
	project_id BINARY(16) NOT NULL,
	command_name VARCHAR(100) NOT NULL CHECK (char_length(command_name) BETWEEN 1 AND 100),
	target_scope VARCHAR(200) NOT NULL CHECK (char_length(target_scope) BETWEEN 1 AND 200),
	idempotency_key VARCHAR(200) NOT NULL CHECK (char_length(idempotency_key) BETWEEN 8 AND 200),
	request_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL CHECK (REGEXP_LIKE(request_hash, '^[0-9a-f]{64}$')),
	response_kind VARCHAR(100) NOT NULL CHECK (char_length(response_kind) BETWEEN 1 AND 100),
    response_id BINARY(16) NOT NULL,
    response_aux_id BINARY(16),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	PRIMARY KEY (project_id, command_name, target_scope, idempotency_key),
	FOREIGN KEY (project_id) REFERENCES projects(project_id)
);

CREATE INDEX workflows_project_updated_idx
    ON workflows(project_id, updated_at DESC, workflow_id);
CREATE INDEX workflow_versions_latest_idx
    ON workflow_versions(project_id, workflow_id, version_number DESC);
CREATE INDEX draft_revisions_history_idx
    ON workflow_draft_revisions(project_id, workflow_id, revision_number DESC);

CREATE TRIGGER workflow_draft_revisions_immutable
	BEFORE UPDATE ON workflow_draft_revisions FOR EACH ROW
	SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'workflow_draft_revisions is immutable';
CREATE TRIGGER workflow_draft_revisions_delete_immutable
	BEFORE DELETE ON workflow_draft_revisions FOR EACH ROW
	SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'workflow_draft_revisions is immutable';
CREATE TRIGGER workflow_execution_snapshots_immutable
	BEFORE UPDATE ON workflow_execution_snapshots FOR EACH ROW
	SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'workflow_execution_snapshots is immutable';
CREATE TRIGGER workflow_versions_immutable
	BEFORE UPDATE ON workflow_versions FOR EACH ROW
	SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'workflow_versions is immutable';
CREATE TRIGGER workflow_versions_delete_immutable
	BEFORE DELETE ON workflow_versions FOR EACH ROW
	SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'workflow_versions is immutable';
