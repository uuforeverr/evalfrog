package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/uu999/evalfrog/internal/resources"
)

func (store *Store) ListConnections(ctx context.Context, projectID string) ([]resources.ConnectionSummary, error) {
	rows, err := store.pool.Query(ctx, `
		SELECT BIN_TO_UUID(connection_id), reference, enabled, resource_revision
		FROM connections WHERE project_id=UUID_TO_BIN($1) ORDER BY reference`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]resources.ConnectionSummary, 0)
	for rows.Next() {
		var item resources.ConnectionSummary
		if err = rows.Scan(&item.ID, &item.Reference, &item.Enabled, &item.Revision); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

type RuntimeResourceResolver struct {
	store   *Store
	secrets resources.SecretResolver
}

func NewRuntimeResourceResolver(store *Store, secrets resources.SecretResolver) *RuntimeResourceResolver {
	if secrets == nil {
		secrets = resources.NoopSecretResolver{}
	}
	return &RuntimeResourceResolver{store: store, secrets: secrets}
}

func (resolver *RuntimeResourceResolver) ResolveConnection(ctx context.Context, command resources.RuntimeResolveCommand) (resources.ConnectionRuntime, error) {
	value, err := resolver.store.ResolveConnectionRuntime(ctx, command)
	if err != nil {
		return resources.ConnectionRuntime{}, err
	}
	if value.SecretReferenceID != "" {
		value.SecretHeaders, err = resolver.secrets.Resolve(ctx, value.SecretReferenceID)
		if err != nil {
			return resources.ConnectionRuntime{}, err
		}
	}
	_, err = resolver.store.pool.Exec(ctx, `INSERT INTO attempt_resource_revisions (project_id, run_id, attempt_id, resource_kind, resource_id, resource_revision) VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),UUID_TO_BIN($3),'connection',UUID_TO_BIN($4),$5)`, command.ProjectID, command.RunID, command.AttemptID, value.ID, value.Revision)
	if err != nil && !isDuplicateKey(err) {
		return resources.ConnectionRuntime{}, err
	}
	return value, nil
}

func (resolver *RuntimeResourceResolver) ResolveServiceOperation(ctx context.Context, command resources.RuntimeResolveCommand) (resources.ServiceOperationRuntime, error) {
	value, err := resolver.store.ResolveServiceOperationRuntime(ctx, command)
	if err != nil {
		return resources.ServiceOperationRuntime{}, err
	}
	if value.SecretReferenceID != "" {
		value.SecretHeaders, err = resolver.secrets.Resolve(ctx, value.SecretReferenceID)
		if err != nil {
			return resources.ServiceOperationRuntime{}, err
		}
	}
	_, err = resolver.store.pool.Exec(ctx, `INSERT INTO attempt_resource_revisions (project_id, run_id, attempt_id, resource_kind, resource_id, resource_revision, contract_revision) VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),UUID_TO_BIN($3),'service_operation',UUID_TO_BIN($4),$5,$6)`, command.ProjectID, command.RunID, command.AttemptID, value.ServiceID, value.Revision, value.ContractRevision)
	if err != nil && !isDuplicateKey(err) {
		return resources.ServiceOperationRuntime{}, err
	}
	return value, nil
}

func (store *Store) ResolveConnectionRuntime(ctx context.Context, command resources.RuntimeResolveCommand) (resources.ConnectionRuntime, error) {
	var result resources.ConnectionRuntime
	var policy []byte
	var secret *string
	err := store.pool.QueryRow(ctx, `
		SELECT BIN_TO_UUID(c.connection_id), BIN_TO_UUID(c.project_id), c.resource_revision, c.base_url,
		       BIN_TO_UUID(c.secret_reference_id), c.policy_json
		FROM connections c JOIN workflow_runs r ON r.project_id=c.project_id
		JOIN connection_execution_grants g ON g.project_id=c.project_id AND g.connection_id=c.connection_id
		WHERE c.project_id=UUID_TO_BIN($1) AND c.connection_id=UUID_TO_BIN($2) AND r.run_id=UUID_TO_BIN($3) AND c.enabled
		  AND g.execution_identity_id=r.execution_identity_id AND g.purpose=r.purpose
		  AND EXISTS (SELECT 1 FROM node_attempts a WHERE a.project_id=UUID_TO_BIN($1) AND a.run_id=UUID_TO_BIN($3) AND a.attempt_id=UUID_TO_BIN($4) AND a.attempt_seq=$5 AND a.state='running' AND a.lease_token=UUID_TO_BIN($6) AND a.fencing_token=$7 AND a.lease_expires_at >= CURRENT_TIMESTAMP(6))`,
		command.ProjectID, command.ConnectionID, command.RunID, command.AttemptID, command.AttemptSequence, command.LeaseToken, command.FencingToken).
		Scan(&result.ID, &result.ProjectID, &result.Revision, &result.BaseURL, &secret, &policy)
	if errors.Is(err, sql.ErrNoRows) {
		return resources.ConnectionRuntime{}, resources.ErrResourceNotFound
	}
	if err != nil {
		return resources.ConnectionRuntime{}, err
	}
	result.SecretReferenceID = ""
	if secret != nil {
		result.SecretReferenceID = *secret
	}
	result.AllowedMethods = map[string]bool{"GET": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true}
	result.MaxRequestBytes, result.MaxResponseBytes = 1<<20, 1<<20
	var policyValue struct {
		AllowedMethods      []string `json:"allowed_methods"`
		AllowedPathPrefixes []string `json:"allowed_path_prefixes"`
		MaxRequestBytes     int64    `json:"max_request_bytes"`
		MaxResponseBytes    int64    `json:"max_response_bytes"`
	}
	if json.Unmarshal(policy, &policyValue) == nil {
		if len(policyValue.AllowedMethods) > 0 {
			result.AllowedMethods = make(map[string]bool, len(policyValue.AllowedMethods))
			for _, method := range policyValue.AllowedMethods {
				result.AllowedMethods[method] = true
			}
		}
		result.AllowedPathPrefixes = append([]string(nil), policyValue.AllowedPathPrefixes...)
		if policyValue.MaxRequestBytes > 0 {
			result.MaxRequestBytes = policyValue.MaxRequestBytes
		}
		if policyValue.MaxResponseBytes > 0 {
			result.MaxResponseBytes = policyValue.MaxResponseBytes
		}
	}
	return result, nil
}

func (store *Store) ResolveServiceOperationRuntime(ctx context.Context, command resources.RuntimeResolveCommand) (resources.ServiceOperationRuntime, error) {
	var result resources.ServiceOperationRuntime
	var secret *string
	var requestSchema, responseSchema []byte
	err := store.pool.QueryRow(ctx, `
		SELECT BIN_TO_UUID(s.service_id), BIN_TO_UUID(s.project_id), s.reference, o.operation, o.contract_revision,
		       o.idempotent, s.resource_revision, s.protocol, s.discovery_reference,
		       o.request_schema, o.response_schema, BIN_TO_UUID(s.secret_reference_id)
		FROM rpc_services s JOIN rpc_service_operations o ON o.project_id=s.project_id AND o.service_id=s.service_id
		JOIN rpc_service_execution_grants g ON g.project_id=s.project_id AND g.service_id=s.service_id
		JOIN workflow_runs r ON r.project_id=s.project_id
		WHERE s.project_id=UUID_TO_BIN($1) AND s.service_id=UUID_TO_BIN($2) AND r.run_id=UUID_TO_BIN($3) AND s.enabled AND o.enabled
		  AND o.operation=$4 AND o.contract_revision=$5 AND g.execution_identity_id=r.execution_identity_id AND g.purpose=r.purpose
		  AND EXISTS (SELECT 1 FROM node_attempts a WHERE a.project_id=UUID_TO_BIN($1) AND a.run_id=UUID_TO_BIN($3) AND a.attempt_id=UUID_TO_BIN($6) AND a.attempt_seq=$7 AND a.state='running' AND a.lease_token=UUID_TO_BIN($8) AND a.fencing_token=$9 AND a.lease_expires_at >= CURRENT_TIMESTAMP(6))`,
		command.ProjectID, command.ServiceID, command.RunID, command.Operation, command.ContractRevision, command.AttemptID, command.AttemptSequence, command.LeaseToken, command.FencingToken).
		Scan(&result.ServiceID, &result.ProjectID, &result.ServiceReference, &result.Operation, &result.ContractRevision, &result.Idempotent, &result.Revision, &result.Protocol, &result.DiscoveryReference, &requestSchema, &responseSchema, &secret)
	if errors.Is(err, sql.ErrNoRows) {
		return resources.ServiceOperationRuntime{}, resources.ErrResourceNotFound
	}
	if err != nil {
		return resources.ServiceOperationRuntime{}, err
	}
	_ = json.Unmarshal(requestSchema, &result.RequestSchema)
	_ = json.Unmarshal(responseSchema, &result.ResponseSchema)
	if secret != nil {
		result.SecretReferenceID = *secret
	}
	return result, nil
}

func (store *Store) ResolveConnection(ctx context.Context, projectID, executionIdentityID, reference string, purpose resources.Purpose) (resources.Connection, error) {
	var result resources.Connection
	err := store.pool.QueryRow(ctx, `
		SELECT BIN_TO_UUID(c.connection_id), BIN_TO_UUID(c.project_id), c.reference
		FROM connections c
		JOIN connection_execution_grants g
		  ON g.project_id = c.project_id AND g.connection_id = c.connection_id
		WHERE c.project_id = UUID_TO_BIN($1) AND c.reference = $2 AND c.enabled
		  AND g.execution_identity_id = UUID_TO_BIN($3) AND g.purpose = $4`,
		projectID, reference, executionIdentityID, purpose,
	).Scan(&result.ID, &result.ProjectID, &result.Reference)
	if errors.Is(err, sql.ErrNoRows) {
		return resources.Connection{}, resources.ErrResourceNotFound
	}
	return result, err
}

func (store *Store) ResolveServiceOperation(ctx context.Context, projectID, executionIdentityID, reference, operation string, purpose resources.Purpose) (resources.ServiceOperation, error) {
	var result resources.ServiceOperation
	err := store.pool.QueryRow(ctx, `
		SELECT BIN_TO_UUID(s.service_id), BIN_TO_UUID(s.project_id), s.reference,
		       o.operation, o.contract_revision, o.idempotent
		FROM rpc_services s
		JOIN rpc_service_operations o
		  ON o.project_id = s.project_id AND o.service_id = s.service_id
		JOIN rpc_service_execution_grants g
		  ON g.project_id = s.project_id AND g.service_id = s.service_id
		WHERE s.project_id = UUID_TO_BIN($1) AND s.reference = $2 AND s.enabled
		  AND o.operation = $3 AND o.enabled
		  AND g.execution_identity_id = UUID_TO_BIN($4) AND g.purpose = $5`,
		projectID, reference, operation, executionIdentityID, purpose,
	).Scan(&result.ServiceID, &result.ProjectID, &result.ServiceReference, &result.Operation, &result.ContractRevision, &result.Idempotent)
	if errors.Is(err, sql.ErrNoRows) {
		return resources.ServiceOperation{}, resources.ErrResourceNotFound
	}
	return result, err
}
