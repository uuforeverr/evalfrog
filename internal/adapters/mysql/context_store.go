package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/uu999/evalfrog/internal/runtime/attempt"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
)

func (store *Store) LoadAttemptMetadataBatch(ctx context.Context, commands []runtimecontext.LoadCommand) []runtimecontext.MetadataResult {
	results := make([]runtimecontext.MetadataResult, len(commands))
	if len(commands) == 0 {
		return results
	}
	const commandRow = `SELECT ? AS ordinal, UUID_TO_BIN(?) AS project_id, UUID_TO_BIN(?) AS run_id,
		UUID_TO_BIN(?) AS attempt_id, ? AS attempt_seq, UUID_TO_BIN(?) AS lease_token, ? AS fencing_token`
	parts := make([]string, len(commands))
	arguments := make([]any, 0, len(commands)*7)
	for index, command := range commands {
		parts[index] = commandRow
		arguments = append(arguments, index, command.ProjectID, command.RunID, command.AttemptID,
			command.AttemptSequence, command.LeaseToken, command.FencingToken)
		results[index].Err = attempt.ErrNotFound
	}
	query := `SELECT requested.ordinal, BIN_TO_UUID(a.project_id), BIN_TO_UUID(a.run_id), BIN_TO_UUID(a.node_run_id),
		n.execution_node_id, BIN_TO_UUID(a.attempt_id), a.attempt_seq,
		BIN_TO_UUID(r.snapshot_id), n.operation_type, n.operation_version,
		n.resource_class, n.resolved_inputs_json
		FROM (` + strings.Join(parts, ` UNION ALL `) + `) AS requested
		JOIN node_attempts a ON a.project_id=requested.project_id AND a.run_id=requested.run_id
			AND a.attempt_id=requested.attempt_id AND a.attempt_seq=requested.attempt_seq
			AND a.lease_token=requested.lease_token AND a.fencing_token=requested.fencing_token
			AND a.state='running' AND a.lease_expires_at >= CURRENT_TIMESTAMP(6)
		JOIN node_runs n ON n.project_id=a.project_id AND n.run_id=a.run_id AND n.node_run_id=a.node_run_id
		JOIN workflow_runs r ON r.project_id=a.project_id AND r.run_id=a.run_id`
	rows, err := store.pool.Query(ctx, query, arguments...)
	if err != nil {
		return metadataBatchError(results, err)
	}
	defer rows.Close()
	for rows.Next() {
		var index int
		var value runtimecontext.Metadata
		var inputs []byte
		if err = rows.Scan(&index, &value.ProjectID, &value.RunID, &value.NodeRunID, &value.ExecutionNodeID,
			&value.AttemptID, &value.AttemptSequence, &value.SnapshotID,
			&value.Operation.Type, &value.Operation.Version, &value.ResourceClass, &inputs); err != nil {
			return metadataBatchError(results, err)
		}
		if index < 0 || index >= len(results) {
			return metadataBatchError(results, attempt.ErrNotFound)
		}
		if err = json.Unmarshal(inputs, &value.ResolvedInputs); err != nil {
			results[index].Err = err
			continue
		}
		results[index] = runtimecontext.MetadataResult{Metadata: value}
	}
	if err = rows.Err(); err != nil {
		return metadataBatchError(results, err)
	}
	return results
}

func metadataBatchError(results []runtimecontext.MetadataResult, err error) []runtimecontext.MetadataResult {
	for index := range results {
		results[index] = runtimecontext.MetadataResult{Err: err}
	}
	return results
}

func (store *Store) LoadSnapshotDSL(ctx context.Context, projectID, snapshotID string) (json.RawMessage, error) {
	var value []byte
	err := store.pool.QueryRow(ctx, `SELECT dsl_json FROM workflow_execution_snapshots WHERE project_id=UUID_TO_BIN($1) AND snapshot_id=UUID_TO_BIN($2)`, projectID, snapshotID).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, attempt.ErrNotFound
	}
	return json.RawMessage(value), err
}

func (store *Store) LoadRunContext(ctx context.Context, projectID, runID string, executionNodeIDs []string) (json.RawMessage, map[string]json.RawMessage, error) {
	ids := append([]string(nil), executionNodeIDs...)
	sort.Strings(ids)
	query := `
		SELECT 'run_input' AS value_kind, '' AS execution_node_id, input_json AS value_json
		FROM workflow_runs WHERE project_id=UUID_TO_BIN(?) AND run_id=UUID_TO_BIN(?)`
	arguments := []any{projectID, runID}
	if len(ids) > 0 {
		query += ` UNION ALL
			SELECT 'output', n.execution_node_id, o.value_json
			FROM node_runs n JOIN node_output_values o
			  ON o.project_id=n.project_id AND o.run_id=n.run_id AND o.attempt_id=n.effective_attempt_id
			WHERE n.project_id=UUID_TO_BIN(?) AND n.run_id=UUID_TO_BIN(?) AND n.state='succeeded'
			  AND n.execution_node_id IN (` + strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",") + `)`
		arguments = append(arguments, projectID, runID)
		for _, id := range ids {
			arguments = append(arguments, id)
		}
	}
	rows, err := store.pool.Query(ctx, query, arguments...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var input json.RawMessage
	outputs := make(map[string]json.RawMessage, len(ids))
	for rows.Next() {
		var kind, nodeID string
		var value []byte
		if err = rows.Scan(&kind, &nodeID, &value); err != nil {
			return nil, nil, err
		}
		if kind == "run_input" {
			input = append(json.RawMessage(nil), value...)
		} else {
			outputs[nodeID] = append(json.RawMessage(nil), value...)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, nil, err
	}
	if len(input) == 0 {
		return nil, nil, attempt.ErrNotFound
	}
	return input, outputs, nil
}

var _ runtimecontext.Repository = (*Store)(nil)
