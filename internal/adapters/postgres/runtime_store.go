package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/projection"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/engine"
	"github.com/uu999/evalfrog/internal/scheduling"
	"github.com/uu999/evalfrog/internal/sourcemap"
)

func (store *Store) CreatePendingRun(ctx context.Context, record runtime.CreatePendingRunRecord) (runtime.WorkflowRunRecord, error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	defer tx.Rollback(ctx)
	scope := record.WorkflowID
	lockKey := record.ProjectID + ":" + commandName(record.Purpose) + ":" + scope + ":" + record.IdempotencyKey
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	var existingRun, existingHash string
	err = tx.QueryRow(ctx, `
		SELECT run_id::text, request_hash FROM runtime_idempotency
		WHERE project_id=$1 AND command_name=$2 AND target_scope=$3 AND idempotency_key=$4`,
		record.ProjectID, commandName(record.Purpose), scope, record.IdempotencyKey).Scan(&existingRun, &existingHash)
	if err == nil {
		if existingHash != record.RequestHash {
			return runtime.WorkflowRunRecord{}, runtime.ErrRunIdempotencyReuse
		}
		result, loadErr := loadRun(ctx, tx, record.ProjectID, existingRun, false)
		if loadErr != nil {
			return runtime.WorkflowRunRecord{}, loadErr
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return runtime.WorkflowRunRecord{}, commitErr
		}
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return runtime.WorkflowRunRecord{}, err
	}

	var snapshotID, definitionHash, definitionSource, versionID string
	if record.Purpose == runtime.RunPurposeTest {
		err = tx.QueryRow(ctx, `
			SELECT s.snapshot_id::text, s.definition_hash
			FROM workflow_draft_revisions r
			JOIN workflow_draft_test_snapshots b
			  ON b.project_id=r.project_id AND b.workflow_id=r.workflow_id
			 AND b.draft_revision_id=r.draft_revision_id
			JOIN workflow_execution_snapshots s
			  ON s.project_id=b.project_id AND s.workflow_id=b.workflow_id AND s.snapshot_id=b.snapshot_id
			WHERE s.project_id=$1 AND s.workflow_id=$2 AND s.snapshot_id=$3
			  AND r.revision_number=$4`, record.ProjectID, record.WorkflowID,
			record.SnapshotID, record.DraftRevisionNumber).Scan(&snapshotID, &definitionHash)
		definitionSource = string(runtime.DefinitionDraftSnapshot)
	} else {
		var nullableSnapshot, nullableHash, nullableVersion *string
		err = tx.QueryRow(ctx, `
			SELECT s.snapshot_id::text, s.definition_hash, v.version_id::text
			FROM workflows w
			LEFT JOIN workflow_versions v
			  ON v.project_id=w.project_id AND v.workflow_id=w.workflow_id AND v.version_id=w.active_version_id
			LEFT JOIN workflow_execution_snapshots s
			  ON s.project_id=v.project_id AND s.workflow_id=v.workflow_id AND s.snapshot_id=v.execution_snapshot_id
			WHERE w.project_id=$1 AND w.workflow_id=$2
			FOR SHARE OF w`, record.ProjectID, record.WorkflowID).Scan(&nullableSnapshot, &nullableHash, &nullableVersion)
		if err == nil {
			if nullableSnapshot == nil || nullableHash == nil || nullableVersion == nil {
				return runtime.WorkflowRunRecord{}, runtime.ErrRunWorkflowNotPublished
			}
			snapshotID, definitionHash, versionID = *nullableSnapshot, *nullableHash, *nullableVersion
		}
		definitionSource = string(runtime.DefinitionPublishedVersion)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return runtime.WorkflowRunRecord{}, runtime.ErrRunSourceInvalid
	}
	if err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	var executionIdentityID string
	if err = tx.QueryRow(ctx, `
		SELECT execution_identity_id::text FROM project_execution_identities
		WHERE project_id=$1 AND enabled=true`, record.ProjectID).Scan(&executionIdentityID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return runtime.WorkflowRunRecord{}, runtime.ErrRunSourceInvalid
		}
		return runtime.WorkflowRunRecord{}, err
	}
	var nullableVersion any
	if versionID != "" {
		nullableVersion = versionID
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO workflow_runs (
			run_id, project_id, workflow_id, snapshot_id, published_version_id,
			execution_identity_id, purpose, definition_source, definition_hash,
			state, state_version, input_json, deadline_at, trace_id, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending',1,$10,$11,$12,$13,$13)`,
		record.RunID, record.ProjectID, record.WorkflowID, snapshotID, nullableVersion,
		executionIdentityID, record.Purpose, definitionSource, definitionHash,
		record.WorkflowInput, record.DeadlineAt, record.TraceID, record.CreatedAt)
	if err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	event := eventing.RuntimeEvent{
		MessageVersion: eventing.RuntimeMessageVersion, EventID: record.EventID,
		ProjectID: record.ProjectID, RunID: record.RunID, AggregateType: eventing.WorkflowRunAggregate,
		AggregateID: record.RunID, EventType: eventing.RunCreated, OccurredAt: record.CreatedAt, TraceID: record.TraceID,
	}
	if err = insertOutbox(ctx, tx, event); err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO runtime_idempotency (
			project_id, command_name, target_scope, idempotency_key, request_hash, run_id, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7)`, record.ProjectID, commandName(record.Purpose), scope,
		record.IdempotencyKey, record.RequestHash, record.RunID, record.CreatedAt)
	if err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	result := runtime.WorkflowRunRecord{
		ID: record.RunID, ProjectID: record.ProjectID, WorkflowID: record.WorkflowID,
		Purpose: record.Purpose,
		Definition: runtime.DefinitionReference{
			SnapshotID: snapshotID, DefinitionHash: definitionHash,
			Source: runtime.DefinitionSource(definitionSource), PublishedVersionID: versionID,
		},
		WorkflowInput: append([]byte(nil), record.WorkflowInput...), TraceID: record.TraceID,
		DeadlineAt: record.DeadlineAt, CreatedAt: record.CreatedAt,
		State: runtime.RunPending, StateVersion: 1,
	}
	if err = tx.Commit(ctx); err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	store.invalidateRunView(ctx, record.RunID)
	return result, nil
}

// RequestCancellation persists only the cancellation intent signal. The
// Engine remains the owner of Run/Node state transition and consumes this
// durable Outbox event with the same Inbox/CAS rules as every other event.
func (store *Store) RequestCancellation(ctx context.Context, record runtime.CancelRunRecord) (runtime.WorkflowRunRecord, bool, error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return runtime.WorkflowRunRecord{}, false, err
	}
	defer tx.Rollback(ctx)
	run, err := loadRun(ctx, tx, record.ProjectID, record.RunID, true)
	if err != nil {
		return runtime.WorkflowRunRecord{}, false, err
	}
	if run.State.Terminal() || !run.CancelRequestedAt.IsZero() {
		if err = tx.Commit(ctx); err != nil {
			return runtime.WorkflowRunRecord{}, false, err
		}
		return run, false, nil
	}
	requestedAt := record.RequestedAt.UTC().Truncate(time.Microsecond)
	if requestedAt.IsZero() || requestedAt.Before(run.CreatedAt) {
		return runtime.WorkflowRunRecord{}, false, runtime.ErrInvalidRun
	}
	tag, err := tx.Exec(ctx, `
		UPDATE workflow_runs SET cancel_requested_at=$1, updated_at=$1
		WHERE project_id=$2 AND run_id=$3 AND cancel_requested_at IS NULL
		  AND state NOT IN ('succeeded','failed','canceled','timed_out')`,
		requestedAt, record.ProjectID, record.RunID)
	if err != nil {
		return runtime.WorkflowRunRecord{}, false, err
	}
	if tag.RowsAffected() != 1 {
		result, loadErr := loadRun(ctx, tx, record.ProjectID, record.RunID, false)
		if loadErr != nil {
			return runtime.WorkflowRunRecord{}, false, loadErr
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return runtime.WorkflowRunRecord{}, false, commitErr
		}
		return result, false, nil
	}
	event := eventing.RuntimeEvent{
		MessageVersion: eventing.RuntimeMessageVersion, EventID: record.EventID,
		ProjectID: record.ProjectID, RunID: record.RunID, AggregateType: eventing.WorkflowRunAggregate,
		AggregateID: record.RunID, EventType: eventing.RunCancelRequested,
		OccurredAt: requestedAt, TraceID: record.TraceID,
	}
	if err = insertOutbox(ctx, tx, event); err != nil {
		return runtime.WorkflowRunRecord{}, false, err
	}
	// A cancellation is an operator-visible durable intent. Record only its
	// identifiers and trace correlation; never copy user input or lease data
	// into the audit stream.
	auditID, err := uuid.NewV7()
	if err != nil {
		return runtime.WorkflowRunRecord{}, false, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO runtime_audit_events (
			audit_id, project_id, run_id, action, actor_type, actor_id, trace_id, details_json, created_at
		) VALUES ($1,$2,$3,'run.cancel_requested','principal',$4,$5,'{}'::jsonb,$6)`,
		auditID.String(), record.ProjectID, record.RunID, record.PrincipalID, record.TraceID, requestedAt); err != nil {
		return runtime.WorkflowRunRecord{}, false, err
	}
	result, err := loadRun(ctx, tx, record.ProjectID, record.RunID, false)
	if err != nil {
		return runtime.WorkflowRunRecord{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return runtime.WorkflowRunRecord{}, false, err
	}
	store.invalidateRunView(ctx, record.RunID)
	return result, true, nil
}

func (store *Store) GetRunView(ctx context.Context, projectID, runID string) (projection.RunView, error) {
	var result projection.RunView
	var output, termination, sourceMapRaw []byte
	var cancelRequested *time.Time
	err := store.pool.QueryRow(ctx, `
		SELECT r.run_id::text, r.project_id::text, r.workflow_id::text, r.purpose,
		       r.state, r.state_version, r.snapshot_id::text, r.deadline_at, r.created_at,
		       r.updated_at, r.workflow_output_json, r.termination_intent_json,
		       r.cancel_requested_at, s.source_map_json
		FROM workflow_runs r
		JOIN workflow_execution_snapshots s ON s.project_id=r.project_id AND s.workflow_id=r.workflow_id AND s.snapshot_id=r.snapshot_id
		WHERE r.project_id=$1 AND r.run_id=$2`, projectID, runID).Scan(
		&result.RunID, &result.ProjectID, &result.WorkflowID, &result.Purpose,
		&result.State, &result.StateVersion, &result.SnapshotID, &result.DeadlineAt, &result.CreatedAt,
		&result.UpdatedAt, &output, &termination, &cancelRequested, &sourceMapRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return projection.RunView{}, runtime.ErrRunNotFound
	}
	if err != nil {
		return projection.RunView{}, err
	}
	result.Output = append(json.RawMessage(nil), output...)
	result.CancelRequested = cancelRequested != nil
	var sourceMap sourcemap.Document
	if err = json.Unmarshal(sourceMapRaw, &sourceMap); err != nil {
		return projection.RunView{}, fmt.Errorf("decode immutable source map: %w", err)
	}
	if len(termination) != 0 {
		var intent runtime.TerminationIntent
		if err = json.Unmarshal(termination, &intent); err != nil {
			return projection.RunView{}, err
		}
		result.Failure = &intent.Cause
		result.FailureLocation = projection.LocateFailure(sourceMap, result.Failure)
	}
	rows, err := store.pool.Query(ctx, `
		SELECT execution_node_id, state, activated, current_attempt_id::text, failure_json
		FROM node_runs WHERE project_id=$1 AND run_id=$2 ORDER BY execution_node_id`, projectID, runID)
	if err != nil {
		return projection.RunView{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var node projection.NodeView
		var attemptID *string
		var failure []byte
		if err = rows.Scan(&node.ExecutionNodeID, &node.State, &node.Activated, &attemptID, &failure); err != nil {
			return projection.RunView{}, err
		}
		if attemptID != nil {
			node.AttemptID = *attemptID
		}
		if len(failure) != 0 {
			var decoded runtime.Failure
			if err = json.Unmarshal(failure, &decoded); err != nil {
				return projection.RunView{}, err
			}
			node.Failure = &decoded
			node.Location = projection.LocateFailure(sourceMap, node.Failure)
		}
		result.Nodes = append(result.Nodes, node)
	}
	if err = rows.Err(); err != nil {
		return projection.RunView{}, err
	}
	return result, nil
}

// OldestUnpublishedOutboxAge is a PostgreSQL health fact across both durable
// delivery boundaries. A zero duration means neither the Runtime nor Task
// Outbox has unpublished records.
func (store *Store) OldestUnpublishedOutboxAge(ctx context.Context) (time.Duration, error) {
	var seconds float64
	err := store.pool.QueryRow(ctx, `
		SELECT COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - MIN(created_at)), 0)
		FROM (
			SELECT created_at FROM outbox_events WHERE published_at IS NULL
			UNION ALL
			SELECT created_at FROM node_task_outbox WHERE published_at IS NULL
		) AS unpublished`).Scan(&seconds)
	if err != nil {
		return 0, err
	}
	if seconds < 0 {
		seconds = 0
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

func (store *Store) GetDiagnosticView(ctx context.Context, projectID, runID string) (projection.DiagnosticView, error) {
	run, err := store.GetRunView(ctx, projectID, runID)
	if err != nil {
		return projection.DiagnosticView{}, err
	}
	result := projection.DiagnosticView{Run: run}
	attemptRows, err := store.pool.Query(ctx, `
		SELECT a.attempt_id::text, n.execution_node_id, a.attempt_seq, a.attempt_kind, a.state,
		       a.lease_owner, a.lease_expires_at, a.error_json, a.created_at, a.updated_at
		FROM node_attempts a
		JOIN node_runs n ON n.project_id=a.project_id AND n.run_id=a.run_id AND n.node_run_id=a.node_run_id
		WHERE a.project_id=$1 AND a.run_id=$2
		ORDER BY n.execution_node_id, a.attempt_seq`, projectID, runID)
	if err != nil {
		return projection.DiagnosticView{}, err
	}
	defer attemptRows.Close()
	for attemptRows.Next() {
		var value projection.AttemptView
		var errorJSON []byte
		var leaseOwner *string
		var leaseExpiresAt *time.Time
		if err = attemptRows.Scan(&value.AttemptID, &value.ExecutionNodeID, &value.Sequence, &value.Kind, &value.State,
			&leaseOwner, &leaseExpiresAt, &errorJSON, &value.CreatedAt, &value.UpdatedAt); err != nil {
			return projection.DiagnosticView{}, err
		}
		if leaseOwner != nil {
			value.LeaseOwner = *leaseOwner
		}
		value.LeaseExpiresAt = leaseExpiresAt
		if len(errorJSON) != 0 {
			var failure struct {
				ErrorCode string `json:"error_code"`
				DSLField  string `json:"dsl_field"`
			}
			if err = json.Unmarshal(errorJSON, &failure); err != nil {
				return projection.DiagnosticView{}, err
			}
			value.ErrorCode, value.DSLField = failure.ErrorCode, failure.DSLField
		}
		result.Attempts = append(result.Attempts, value)
	}
	if err = attemptRows.Err(); err != nil {
		return projection.DiagnosticView{}, err
	}
	auditRows, err := store.pool.Query(ctx, `
		SELECT action, actor_type, actor_id, trace_id, details_json, created_at
		FROM runtime_audit_events
		WHERE project_id=$1 AND run_id=$2
		ORDER BY created_at DESC, audit_id DESC
		LIMIT 100`, projectID, runID)
	if err != nil {
		return projection.DiagnosticView{}, err
	}
	defer auditRows.Close()
	for auditRows.Next() {
		var value projection.AuditView
		var details []byte
		if err = auditRows.Scan(&value.Action, &value.ActorType, &value.ActorID, &value.TraceID, &details, &value.CreatedAt); err != nil {
			return projection.DiagnosticView{}, err
		}
		if err = json.Unmarshal(details, &value.Details); err != nil {
			return projection.DiagnosticView{}, err
		}
		result.Audit = append(result.Audit, value)
	}
	if err = auditRows.Err(); err != nil {
		return projection.DiagnosticView{}, err
	}
	return result, nil
}

func commandName(purpose runtime.RunPurpose) string {
	if purpose == runtime.RunPurposeTest {
		return "test_draft"
	}
	return "create_run"
}

type runtimeTransaction struct {
	tx              pgx.Tx
	router          scheduling.Router
	snapshot        *engine.Snapshot
	store           *Store
	dirtyRunID      string
	readyCandidates []scheduling.Candidate
}

func (store *Store) WithRunTransaction(ctx context.Context, event eventing.RuntimeEvent, operation func(engine.RunTransaction) error) error {
	return store.withRuntimeTransaction(ctx, operation)
}

func (store *Store) WithRunBatchTransaction(ctx context.Context, operation func(engine.RunTransaction) error) error {
	return store.withRuntimeTransaction(ctx, operation)
}

func (store *Store) withRuntimeTransaction(ctx context.Context, operation func(engine.RunTransaction) error) error {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	adapter := &runtimeTransaction{tx: tx, router: store.router, store: store}
	if err = operation(adapter); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	store.invalidateRunView(ctx, adapter.dirtyRunID)
	store.registerReady(ctx, adapter.readyCandidates)
	return nil
}

func (transaction *runtimeTransaction) AcceptInbox(ctx context.Context, consumer string, event eventing.RuntimeEvent) (bool, error) {
	tag, err := transaction.tx.Exec(ctx, `
		INSERT INTO inbox_events (project_id, run_id, consumer_name, event_id, event_type, received_at, processed_at)
		VALUES ($1,$2,$3,$4,$5,clock_timestamp(),clock_timestamp())
		ON CONFLICT (consumer_name, event_id) DO NOTHING`,
		event.ProjectID, event.RunID, consumer, event.EventID, event.EventType)
	return tag.RowsAffected() == 1, err
}

// AuthorityNow makes time-based semantic decisions replica-safe. Recovery
// messages can be delayed or reordered, so Engine must not treat their
// historical OccurredAt as the present deadline clock.
func (transaction *runtimeTransaction) AuthorityNow(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := transaction.tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, err
	}
	return now.UTC(), nil
}

func (transaction *runtimeTransaction) LoadRun(ctx context.Context, projectID, runID string) (runtime.WorkflowRunRecord, error) {
	return loadRun(ctx, transaction.tx, projectID, runID, true)
}

func (transaction *runtimeTransaction) LoadSnapshot(ctx context.Context, projectID, snapshotID string) (engine.Snapshot, error) {
	var result engine.Snapshot
	var raw []byte
	err := transaction.tx.QueryRow(ctx, `
		SELECT snapshot_id::text, definition_hash, dsl_json
		FROM workflow_execution_snapshots WHERE project_id=$1 AND snapshot_id=$2`, projectID, snapshotID).
		Scan(&result.ID, &result.DefinitionHash, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return engine.Snapshot{}, runtime.ErrRunSourceInvalid
	}
	if err != nil {
		return engine.Snapshot{}, err
	}
	if err = json.Unmarshal(raw, &result.DSL); err != nil {
		return engine.Snapshot{}, fmt.Errorf("decode immutable runtime DSL: %w", err)
	}
	transaction.snapshot = &result
	return result, nil
}

func (transaction *runtimeTransaction) LoadEngineState(ctx context.Context, projectID, runID string) (engine.State, error) {
	run, err := loadRun(ctx, transaction.tx, projectID, runID, true)
	if err != nil {
		return engine.State{}, err
	}
	rows, err := transaction.tx.Query(ctx, `
		SELECT n.execution_node_id, n.kind, n.state, n.state_version, n.activated,
		       n.selected_route, n.resolved_inputs_json, n.current_attempt_id::text,
		       n.effective_attempt_id::text, n.next_attempt_seq, n.business_attempt_count,
		       n.recovery_count, n.next_attempt_kind, n.next_retry_at, n.failure_json,
		       n.cancel_reason, o.value_json
		FROM node_runs n
		LEFT JOIN node_output_values o
		  ON o.project_id=n.project_id AND o.attempt_id=n.effective_attempt_id
		WHERE n.project_id=$1 AND n.run_id=$2
		ORDER BY n.execution_node_id
		FOR UPDATE OF n`, projectID, runID)
	if err != nil {
		return engine.State{}, err
	}
	defer rows.Close()
	state := engine.State{Run: run}
	for rows.Next() {
		var node runtime.NodeRunRecord
		var inputs, failure, outputs []byte
		var current, effective *string
		var nextRetry *time.Time
		err = rows.Scan(&node.ExecutionNodeID, &node.Kind, &node.State, &node.StateVersion, &node.Activated,
			&node.SelectedRoute, &inputs, &current, &effective, &node.NextAttemptSeq,
			&node.BusinessAttemptCount, &node.RecoveryCount, &node.NextAttemptKind, &nextRetry,
			&failure, &node.CancelReason, &outputs)
		if err != nil {
			return engine.State{}, err
		}
		node.RunID = runID
		if current != nil {
			node.CurrentAttemptID = *current
		}
		if effective != nil {
			node.EffectiveAttemptID = *effective
		}
		if nextRetry != nil {
			node.NextRetryAt = *nextRetry
		}
		if err = decodeJSONMap(inputs, &node.ResolvedInputs); err != nil {
			return engine.State{}, err
		}
		if err = decodeOptionalJSON(failure, &node.Failure); err != nil {
			return engine.State{}, err
		}
		if err = decodeJSONMap(outputs, &node.EffectiveOutputs); err != nil {
			return engine.State{}, err
		}
		state.Nodes = append(state.Nodes, node)
	}
	if err = rows.Err(); err != nil {
		return engine.State{}, err
	}
	attemptRows, err := transaction.tx.Query(ctx, `
		SELECT a.attempt_id::text, n.execution_node_id, a.attempt_seq, a.attempt_kind,
		       a.state, a.state_version, a.error_json, o.value_json
		FROM node_attempts a
		JOIN node_runs n ON n.project_id=a.project_id AND n.run_id=a.run_id AND n.node_run_id=a.node_run_id
		LEFT JOIN node_output_values o ON o.project_id=a.project_id AND o.attempt_id=a.attempt_id
		WHERE a.project_id=$1 AND a.run_id=$2
		ORDER BY a.attempt_seq, a.attempt_id
		FOR UPDATE OF a`, projectID, runID)
	if err != nil {
		return engine.State{}, err
	}
	defer attemptRows.Close()
	for attemptRows.Next() {
		var attempt runtime.NodeAttemptRecord
		var executionNodeID string
		var errorJSON, outputJSON []byte
		if err = attemptRows.Scan(&attempt.ID, &executionNodeID, &attempt.Sequence, &attempt.Kind,
			&attempt.State, &attempt.StateVersion, &errorJSON, &outputJSON); err != nil {
			return engine.State{}, err
		}
		attempt.NodeRunID = runID + ":" + executionNodeID
		if attempt.State.Terminal() {
			result := runtime.AttemptResult{State: attempt.State}
			if len(errorJSON) != 0 {
				var failure struct {
					ErrorCode    string         `json:"error_code"`
					Message      string         `json:"message"`
					DSLField     string         `json:"dsl_field"`
					ErrorDetails map[string]any `json:"error_details"`
				}
				if err = json.Unmarshal(errorJSON, &failure); err != nil {
					return engine.State{}, err
				}
				result.ErrorCode, result.Message, result.DSLField, result.ErrorDetails = failure.ErrorCode, failure.Message, failure.DSLField, failure.ErrorDetails
			}
			if err = decodeJSONMap(outputJSON, &result.Outputs); err != nil {
				return engine.State{}, err
			}
			attempt.Result = &result
		}
		state.Attempts = append(state.Attempts, attempt)
	}
	return state, attemptRows.Err()
}

func (transaction *runtimeTransaction) InitializeRun(ctx context.Context, before runtime.WorkflowRunRecord, after engine.State, at time.Time) error {
	if transaction.snapshot == nil || transaction.router == nil {
		return fmt.Errorf("runtime snapshot and routing policy are required for initialization")
	}
	definitions := make(map[string]dsl.Node, len(transaction.snapshot.DSL.Nodes))
	for _, definition := range transaction.snapshot.DSL.Nodes {
		definitions[string(definition.ID)] = definition
	}
	type nodeInsert struct {
		NodeRunID            string                    `json:"node_run_id"`
		ProjectID            string                    `json:"project_id"`
		RunID                string                    `json:"run_id"`
		ExecutionNodeID      string                    `json:"execution_node_id"`
		Kind                 string                    `json:"kind"`
		State                string                    `json:"state"`
		StateVersion         uint64                    `json:"state_version"`
		OperationType        string                    `json:"operation_type"`
		OperationVersion     uint32                    `json:"operation_version"`
		ResourceClass        *scheduling.ResourceClass `json:"resource_class"`
		Activated            bool                      `json:"activated"`
		SelectedRoute        string                    `json:"selected_route"`
		ResolvedInputs       any                       `json:"resolved_inputs"`
		NextAttemptSequence  uint32                    `json:"next_attempt_sequence"`
		BusinessAttemptCount uint32                    `json:"business_attempt_count"`
		RecoveryCount        uint32                    `json:"recovery_count"`
		NextAttemptKind      runtime.RetryKind         `json:"next_attempt_kind"`
		ReadyAt              *time.Time                `json:"ready_at"`
		NextRetryAt          *time.Time                `json:"next_retry_at"`
		Failure              any                       `json:"failure"`
		CancelReason         string                    `json:"cancel_reason"`
	}
	inserts := make([]nodeInsert, 0, len(after.Nodes))
	for _, node := range after.Nodes {
		definition, exists := definitions[node.ExecutionNodeID]
		if !exists {
			return fmt.Errorf("runtime node %q is absent from immutable snapshot", node.ExecutionNodeID)
		}
		var resourceClass *scheduling.ResourceClass
		if node.Kind == runtime.NodeTask {
			resolved, routable := transaction.router.Resolve(definition.Operation.Coordinate())
			if !routable {
				return fmt.Errorf("runtime operation %s@%d has no routing policy", definition.Operation.Type, definition.Operation.Version)
			}
			resourceClass = &resolved
		}
		nodeRunID := deterministicNodeRunID(after.Run.ID, node.ExecutionNodeID)
		var readyAt *time.Time
		if node.State == runtime.NodeReady {
			value := at
			readyAt = &value
		}
		var nextRetryAt *time.Time
		if !node.NextRetryAt.IsZero() {
			value := node.NextRetryAt
			nextRetryAt = &value
		}
		inserts = append(inserts, nodeInsert{
			NodeRunID: nodeRunID, ProjectID: after.Run.ProjectID, RunID: after.Run.ID,
			ExecutionNodeID: node.ExecutionNodeID, Kind: string(node.Kind), State: string(node.State), StateVersion: node.StateVersion,
			OperationType: definition.Operation.Type, OperationVersion: definition.Operation.Version, ResourceClass: resourceClass,
			Activated: node.Activated, SelectedRoute: node.SelectedRoute, ResolvedInputs: node.ResolvedInputs,
			NextAttemptSequence: node.NextAttemptSeq, BusinessAttemptCount: node.BusinessAttemptCount,
			RecoveryCount: node.RecoveryCount, NextAttemptKind: node.NextAttemptKind,
			ReadyAt: readyAt, NextRetryAt: nextRetryAt, Failure: node.Failure, CancelReason: node.CancelReason,
		})
		if node.State == runtime.NodeReady && node.Kind == runtime.NodeTask {
			transaction.readyCandidates = append(transaction.readyCandidates, scheduling.Candidate{
				ProjectID: after.Run.ProjectID, RunID: after.Run.ID, NodeRunID: nodeRunID,
				ExecutionNodeID: node.ExecutionNodeID, StateVersion: node.StateVersion,
				ReadyAt: at, ResourceClass: *resourceClass,
			}.Normalized())
		}
	}
	rawInserts, err := json.Marshal(inserts)
	if err != nil {
		return err
	}
	tag, err := transaction.tx.Exec(ctx, `
		INSERT INTO node_runs (
			node_run_id, project_id, run_id, execution_node_id, kind, state, state_version,
			operation_type, operation_version, resource_class, activated, selected_route,
			resolved_inputs_json, next_attempt_seq, business_attempt_count, recovery_count,
			next_attempt_kind, priority, ready_at, next_retry_at, failure_json,
			cancel_reason, created_at, updated_at
		)
		SELECT value.node_run_id::uuid, value.project_id::uuid, value.run_id::uuid,
		       value.execution_node_id, value.kind, value.state, value.state_version,
		       value.operation_type, value.operation_version, value.resource_class,
		       value.activated, value.selected_route, value.resolved_inputs,
		       value.next_attempt_sequence, value.business_attempt_count, value.recovery_count,
		       value.next_attempt_kind, 0, value.ready_at, value.next_retry_at,
		       value.failure, value.cancel_reason, $2, $2
		FROM jsonb_to_recordset($1::jsonb) AS value(
			node_run_id text, project_id text, run_id text, execution_node_id text,
			kind text, state text, state_version bigint, operation_type text,
			operation_version integer, resource_class text, activated boolean,
			selected_route text, resolved_inputs jsonb, next_attempt_sequence integer,
			business_attempt_count integer, recovery_count integer, next_attempt_kind text,
			ready_at timestamptz, next_retry_at timestamptz, failure jsonb, cancel_reason text
		)`, rawInserts, at)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(inserts)) {
		return runtime.ErrRunConflict
	}
	if err := transaction.updateRunCAS(ctx, before, after.Run, at); err != nil {
		return err
	}
	transaction.dirtyRunID = after.Run.ID
	return nil
}

func (transaction *runtimeTransaction) FailRunInitialization(ctx context.Context, before, after runtime.WorkflowRunRecord, at time.Time) error {
	if err := transaction.updateRunCAS(ctx, before, after, at); err != nil {
		return err
	}
	transaction.dirtyRunID = after.ID
	return nil
}

func (transaction *runtimeTransaction) AdvanceRun(ctx context.Context, before, after engine.State, at time.Time) error {
	definitions := map[string]dsl.Node{}
	if transaction.snapshot != nil {
		definitions = make(map[string]dsl.Node, len(transaction.snapshot.DSL.Nodes))
		for _, definition := range transaction.snapshot.DSL.Nodes {
			definitions[string(definition.ID)] = definition
		}
	}
	type nodeChange struct {
		ProjectID            string            `json:"project_id"`
		RunID                string            `json:"run_id"`
		ExecutionNodeID      string            `json:"execution_node_id"`
		PreviousStateVersion uint64            `json:"previous_state_version"`
		State                string            `json:"state"`
		StateVersion         uint64            `json:"state_version"`
		Activated            bool              `json:"activated"`
		SelectedRoute        string            `json:"selected_route"`
		ResolvedInputs       any               `json:"resolved_inputs"`
		CurrentAttemptID     *string           `json:"current_attempt_id"`
		EffectiveAttemptID   *string           `json:"effective_attempt_id"`
		NextAttemptSequence  uint32            `json:"next_attempt_sequence"`
		BusinessAttemptCount uint32            `json:"business_attempt_count"`
		RecoveryCount        uint32            `json:"recovery_count"`
		NextAttemptKind      runtime.RetryKind `json:"next_attempt_kind"`
		ReadyAt              *time.Time        `json:"ready_at"`
		NextRetryAt          *time.Time        `json:"next_retry_at"`
		Failure              any               `json:"failure"`
		CancelReason         string            `json:"cancel_reason"`
	}
	nodesChanged := false
	changes := make([]nodeChange, 0, len(after.Nodes))
	beforeNodes := make(map[string]runtime.NodeRunRecord, len(before.Nodes))
	for _, node := range before.Nodes {
		beforeNodes[node.ExecutionNodeID] = node
	}
	for _, node := range after.Nodes {
		previous, exists := beforeNodes[node.ExecutionNodeID]
		if !exists {
			return fmt.Errorf("%w: node set changed during advancement", runtime.ErrRunConflict)
		}
		if node.StateVersion == previous.StateVersion {
			continue
		}
		var readyAt *time.Time
		if node.State == runtime.NodeReady {
			value := at
			readyAt = &value
		}
		var currentAttemptID, effectiveAttemptID *string
		if node.CurrentAttemptID != "" {
			value := node.CurrentAttemptID
			currentAttemptID = &value
		}
		if node.EffectiveAttemptID != "" {
			value := node.EffectiveAttemptID
			effectiveAttemptID = &value
		}
		var nextRetryAt *time.Time
		if !node.NextRetryAt.IsZero() {
			value := node.NextRetryAt
			nextRetryAt = &value
		}
		changes = append(changes, nodeChange{
			ProjectID: after.Run.ProjectID, RunID: after.Run.ID, ExecutionNodeID: node.ExecutionNodeID,
			PreviousStateVersion: previous.StateVersion, State: string(node.State), StateVersion: node.StateVersion,
			Activated: node.Activated, SelectedRoute: node.SelectedRoute, ResolvedInputs: node.ResolvedInputs,
			CurrentAttemptID: currentAttemptID, EffectiveAttemptID: effectiveAttemptID,
			NextAttemptSequence: node.NextAttemptSeq, BusinessAttemptCount: node.BusinessAttemptCount,
			RecoveryCount: node.RecoveryCount, NextAttemptKind: node.NextAttemptKind,
			ReadyAt: readyAt, NextRetryAt: nextRetryAt, Failure: node.Failure, CancelReason: node.CancelReason,
		})
		if node.State == runtime.NodeReady && node.Kind == runtime.NodeTask {
			if transaction.snapshot == nil || transaction.router == nil {
				return fmt.Errorf("runtime snapshot and routing policy are required to register Ready nodes")
			}
			definition, definitionExists := definitions[node.ExecutionNodeID]
			if !definitionExists {
				return fmt.Errorf("runtime node %q is absent from immutable snapshot", node.ExecutionNodeID)
			}
			resourceClass, routable := transaction.router.Resolve(definition.Operation.Coordinate())
			if !routable {
				return fmt.Errorf("runtime operation %s@%d has no routing policy", definition.Operation.Type, definition.Operation.Version)
			}
			transaction.readyCandidates = append(transaction.readyCandidates, scheduling.Candidate{
				ProjectID: after.Run.ProjectID, RunID: after.Run.ID,
				NodeRunID:       deterministicNodeRunID(after.Run.ID, node.ExecutionNodeID),
				ExecutionNodeID: node.ExecutionNodeID, StateVersion: node.StateVersion,
				ReadyAt: at, ResourceClass: resourceClass,
			}.Normalized())
		}
		nodesChanged = true
	}
	if len(changes) > 0 {
		rawChanges, err := json.Marshal(changes)
		if err != nil {
			return err
		}
		tag, err := transaction.tx.Exec(ctx, `
			UPDATE node_runs target SET
				state=change_row.state, state_version=change_row.state_version,
				activated=change_row.activated, selected_route=change_row.selected_route,
				resolved_inputs_json=change_row.resolved_inputs,
				current_attempt_id=change_row.current_attempt_id::uuid,
				effective_attempt_id=change_row.effective_attempt_id::uuid,
				next_attempt_seq=change_row.next_attempt_sequence,
				business_attempt_count=change_row.business_attempt_count,
				recovery_count=change_row.recovery_count,
				next_attempt_kind=change_row.next_attempt_kind,
				ready_at=change_row.ready_at, next_retry_at=change_row.next_retry_at,
				failure_json=change_row.failure, cancel_reason=change_row.cancel_reason, updated_at=$2
			FROM jsonb_to_recordset($1::jsonb) AS change_row(
				project_id text, run_id text, execution_node_id text,
				previous_state_version bigint, state text, state_version bigint,
				activated boolean, selected_route text, resolved_inputs jsonb,
				current_attempt_id text, effective_attempt_id text,
				next_attempt_sequence integer, business_attempt_count integer,
				recovery_count integer, next_attempt_kind text, ready_at timestamptz,
				next_retry_at timestamptz, failure jsonb, cancel_reason text
			)
			WHERE target.project_id=change_row.project_id::uuid AND target.run_id=change_row.run_id::uuid
			  AND target.execution_node_id=change_row.execution_node_id
			  AND target.state_version=change_row.previous_state_version`, rawChanges, at)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != int64(len(changes)) {
			return runtime.ErrRunConflict
		}
	}
	if after.Run.StateVersion != before.Run.StateVersion {
		if err := transaction.updateRunCAS(ctx, before.Run, after.Run, at); err != nil {
			return err
		}
	}
	// A projection contains both Run and Node state. Node-only transitions are
	// therefore observable changes too, even when the aggregate Run version is
	// unchanged (for example a dispatched task becoming running).
	if nodesChanged || after.Run.StateVersion != before.Run.StateVersion {
		transaction.dirtyRunID = after.Run.ID
	}
	return nil
}

func (transaction *runtimeTransaction) updateRunCAS(ctx context.Context, before, after runtime.WorkflowRunRecord, at time.Time) error {
	ids, err := json.Marshal(after.ExecutionNodeIDs)
	if err != nil {
		return err
	}
	tag, err := transaction.tx.Exec(ctx, `
		UPDATE workflow_runs SET
			state=$1, state_version=$2, workflow_output_json=$3,
			execution_node_ids=$4, termination_intent_json=$5, updated_at=$6
		WHERE project_id=$7 AND run_id=$8 AND state=$9 AND state_version=$10`,
		after.State, after.StateVersion, nullableRaw(after.WorkflowOutput), ids,
		nullableJSON(after.Termination), at, after.ProjectID, after.ID, before.State, before.StateVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return runtime.ErrRunConflict
	}
	return nil
}

func loadRun(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, projectID, runID string, forUpdate bool) (runtime.WorkflowRunRecord, error) {
	query := `
		SELECT run_id::text, project_id::text, workflow_id::text, trace_id, purpose,
		       snapshot_id::text, definition_hash, definition_source,
		       published_version_id::text, input_json, workflow_output_json,
		       deadline_at, created_at, state, state_version,
		       execution_node_ids, termination_intent_json, cancel_requested_at
		FROM workflow_runs WHERE project_id=$1 AND run_id=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var result runtime.WorkflowRunRecord
	var versionID *string
	var cancelRequestedAt *time.Time
	var source runtime.DefinitionSource
	var output, nodeIDs, termination []byte
	err := queryer.QueryRow(ctx, query, projectID, runID).Scan(
		&result.ID, &result.ProjectID, &result.WorkflowID, &result.TraceID, &result.Purpose,
		&result.Definition.SnapshotID, &result.Definition.DefinitionHash, &source,
		&versionID, &result.WorkflowInput, &output, &result.DeadlineAt, &result.CreatedAt,
		&result.State, &result.StateVersion, &nodeIDs, &termination, &cancelRequestedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return runtime.WorkflowRunRecord{}, runtime.ErrRunNotFound
	}
	if err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	result.Definition.Source = source
	if cancelRequestedAt != nil {
		result.CancelRequestedAt = *cancelRequestedAt
	}
	if versionID != nil {
		result.Definition.PublishedVersionID = *versionID
	}
	result.WorkflowOutput = output
	if err = json.Unmarshal(nodeIDs, &result.ExecutionNodeIDs); err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	if err = decodeOptionalJSON(termination, &result.Termination); err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	return result, nil
}

func insertOutbox(ctx context.Context, tx pgx.Tx, event eventing.RuntimeEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, project_id, run_id, aggregate_type, aggregate_id,
			event_type, message_version, occurred_at, trace_id, available_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,clock_timestamp())`, event.EventID, event.ProjectID,
		event.RunID, event.AggregateType, event.AggregateID, event.EventType,
		event.MessageVersion, event.OccurredAt, event.TraceID)
	return err
}

func deterministicNodeRunID(runID, executionNodeID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(runID+":"+executionNodeID)).String()
}

func nullableJSON(value any) any {
	if value == nil {
		return nil
	}
	bytes, err := json.Marshal(value)
	if err != nil || string(bytes) == "null" {
		return nil
	}
	return bytes
}

func nullableRaw(value json.RawMessage) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func decodeJSONMap(raw []byte, target *map[string]json.RawMessage) error {
	if len(raw) == 0 {
		*target = nil
		return nil
	}
	return json.Unmarshal(raw, target)
}

func decodeOptionalJSON[T any](raw []byte, target **T) error {
	if len(raw) == 0 {
		*target = nil
		return nil
	}
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	*target = &value
	return nil
}

var _ engine.TransactionManager = (*Store)(nil)
var _ runtime.RunRepository = (*Store)(nil)
var _ = dsl.Document{}
