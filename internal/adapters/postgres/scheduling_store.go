package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/scheduling"
)

func (store *Store) LoadSchedulingSnapshot(ctx context.Context, candidateWindow int) (scheduling.AuthoritySnapshot, error) {
	if store.router == nil {
		return scheduling.AuthoritySnapshot{}, fmt.Errorf("runtime routing policy is required")
	}
	if candidateWindow < 0 {
		return scheduling.AuthoritySnapshot{}, fmt.Errorf("candidate window cannot be negative")
	}
	result := scheduling.AuthoritySnapshot{}
	if candidateWindow > 0 {
		rows, err := store.pool.Query(ctx, `
			SELECT n.project_id::text, n.run_id::text, n.node_run_id::text,
			       n.execution_node_id, n.state_version, n.priority,
			       n.ready_at, n.operation_type, n.operation_version,
			       n.resource_class
			FROM node_runs n
			JOIN workflow_runs r ON r.project_id=n.project_id AND r.run_id=n.run_id
			WHERE n.state='ready' AND r.state='running' AND r.termination_intent_json IS NULL
			  AND r.deadline_at > clock_timestamp()
			ORDER BY n.ready_at, n.project_id, n.node_run_id
			LIMIT $1`, candidateWindow)
		if err != nil {
			return scheduling.AuthoritySnapshot{}, err
		}
		for rows.Next() {
			var candidate scheduling.Candidate
			var coordinate dsl.Coordinate
			var persistedClass scheduling.ResourceClass
			if err = rows.Scan(&candidate.ProjectID, &candidate.RunID, &candidate.NodeRunID,
				&candidate.ExecutionNodeID, &candidate.StateVersion, &candidate.Priority,
				&candidate.ReadyAt, &coordinate.Type, &coordinate.Version, &persistedClass); err != nil {
				rows.Close()
				return scheduling.AuthoritySnapshot{}, err
			}
			class, exists := store.router.Resolve(coordinate)
			if !exists || class != persistedClass {
				rows.Close()
				return scheduling.AuthoritySnapshot{}, fmt.Errorf("operation %s@%d has no runtime routing policy", coordinate.Type, coordinate.Version)
			}
			candidate.ResourceClass = class
			candidate = candidate.Normalized()
			result.Candidates = append(result.Candidates, candidate)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return scheduling.AuthoritySnapshot{}, err
		}
		rows.Close()
	}
	rows, err := store.pool.Query(ctx, `
		SELECT a.attempt_id::text, a.project_id::text, a.state,
		       n.operation_type, n.operation_version, n.resource_class
		FROM node_attempts a
		JOIN node_runs n ON n.project_id=a.project_id AND n.run_id=a.run_id AND n.node_run_id=a.node_run_id
		WHERE a.state IN ('queued','running')
		ORDER BY a.project_id, a.attempt_id`)
	if err != nil {
		return scheduling.AuthoritySnapshot{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var value scheduling.Inflight
		var state runtime.AttemptState
		var coordinate dsl.Coordinate
		var persistedClass scheduling.ResourceClass
		if err = rows.Scan(&value.AttemptID, &value.ProjectID, &state, &coordinate.Type, &coordinate.Version, &persistedClass); err != nil {
			return scheduling.AuthoritySnapshot{}, err
		}
		class, exists := store.router.Resolve(coordinate)
		if !exists || class != persistedClass {
			return scheduling.AuthoritySnapshot{}, fmt.Errorf("operation %s@%d has no runtime routing policy", coordinate.Type, coordinate.Version)
		}
		value.ResourceClass = class
		value.QueueOccupied = state == runtime.AttemptQueued
		result.Inflight = append(result.Inflight, value)
	}
	return result, rows.Err()
}

func (store *Store) DispatchReady(ctx context.Context, command scheduling.DispatchCommand) (scheduling.Task, error) {
	if err := command.Candidate.Validate(); err != nil {
		return scheduling.Task{}, err
	}
	if command.AttemptID == "" || command.TaskID == "" || command.Now.IsZero() {
		return scheduling.Task{}, fmt.Errorf("dispatch attempt, task and time are required")
	}
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return scheduling.Task{}, err
	}
	defer tx.Rollback(ctx)
	var record runtime.NodeRunRecord
	var resolvedInputs, failure []byte
	var current, effective *string
	var nextRetry *time.Time
	var coordinate dsl.Coordinate
	var persistedClass scheduling.ResourceClass
	var runTraceID string
	err = tx.QueryRow(ctx, `
		SELECT n.run_id::text, n.execution_node_id, n.kind, n.state, n.state_version,
		       n.activated, n.selected_route, n.resolved_inputs_json,
		       n.current_attempt_id::text, n.effective_attempt_id::text,
		       n.next_attempt_seq, n.business_attempt_count, n.recovery_count,
		       n.next_attempt_kind, n.next_retry_at, n.failure_json, n.cancel_reason,
		       n.operation_type, n.operation_version, n.resource_class, r.trace_id
		FROM node_runs n
		JOIN workflow_runs r ON r.project_id=n.project_id AND r.run_id=n.run_id
		WHERE n.project_id=$1 AND n.run_id=$2 AND n.node_run_id=$3
		  AND r.state='running' AND r.termination_intent_json IS NULL
		  AND r.deadline_at > clock_timestamp()
		FOR UPDATE OF n`, command.Candidate.ProjectID, command.Candidate.RunID, command.Candidate.NodeRunID).
		Scan(&record.RunID, &record.ExecutionNodeID, &record.Kind, &record.State, &record.StateVersion,
			&record.Activated, &record.SelectedRoute, &resolvedInputs, &current, &effective,
			&record.NextAttemptSeq, &record.BusinessAttemptCount, &record.RecoveryCount,
			&record.NextAttemptKind, &nextRetry, &failure, &record.CancelReason,
			&coordinate.Type, &coordinate.Version, &persistedClass, &runTraceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return scheduling.Task{}, scheduling.ErrCandidateStale
	}
	if err != nil {
		return scheduling.Task{}, err
	}
	class, exists := store.router.Resolve(coordinate)
	if !exists || class != persistedClass || class != command.Candidate.ResourceClass || record.ExecutionNodeID != command.Candidate.ExecutionNodeID || record.State != runtime.NodeReady || record.StateVersion != command.Candidate.StateVersion {
		return scheduling.Task{}, scheduling.ErrCandidateStale
	}
	if current != nil {
		record.CurrentAttemptID = *current
	}
	if effective != nil {
		record.EffectiveAttemptID = *effective
	}
	if nextRetry != nil {
		record.NextRetryAt = *nextRetry
	}
	if err = decodeJSONMap(resolvedInputs, &record.ResolvedInputs); err != nil {
		return scheduling.Task{}, err
	}
	if err = decodeOptionalJSON(failure, &record.Failure); err != nil {
		return scheduling.Task{}, err
	}
	node, err := runtime.RestoreNodeRun(record)
	if err != nil {
		return scheduling.Task{}, err
	}
	sequence, kind, err := node.QueueAttempt(command.AttemptID)
	if err != nil {
		return scheduling.Task{}, err
	}
	attempt, err := runtime.NewNodeAttempt(command.AttemptID, record.RunID+":"+record.ExecutionNodeID, sequence, kind)
	if err != nil {
		return scheduling.Task{}, err
	}
	after := node.Snapshot()
	queued := attempt.Snapshot()
	retryCount := uint32(0)
	if after.BusinessAttemptCount > 0 {
		retryCount = after.BusinessAttemptCount - 1
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO node_attempts (
			attempt_id, project_id, run_id, node_run_id, attempt_seq, attempt_kind,
			state, state_version, retry_count, recovery_count, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11)`,
		queued.ID, command.Candidate.ProjectID, record.RunID, command.Candidate.NodeRunID,
		queued.Sequence, queued.Kind, queued.State, queued.StateVersion, retryCount,
		after.RecoveryCount, command.Now)
	if err != nil {
		return scheduling.Task{}, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE node_runs SET state=$1, state_version=$2, current_attempt_id=$3,
		       next_attempt_seq=$4, business_attempt_count=$5, recovery_count=$6,
		       ready_at=NULL, next_retry_at=NULL, updated_at=$7
		WHERE project_id=$8 AND run_id=$9 AND node_run_id=$10
		  AND state='ready' AND state_version=$11`, after.State, after.StateVersion,
		after.CurrentAttemptID, after.NextAttemptSeq, after.BusinessAttemptCount,
		after.RecoveryCount, command.Now, command.Candidate.ProjectID, record.RunID,
		command.Candidate.NodeRunID, record.StateVersion)
	if err != nil {
		return scheduling.Task{}, err
	}
	if tag.RowsAffected() != 1 {
		return scheduling.Task{}, scheduling.ErrCandidateStale
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO node_task_outbox (
			task_id, project_id, run_id, node_run_id, execution_node_id,
			attempt_id, attempt_seq, resource_class, message_version,
			occurred_at, trace_id, available_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,$9,$10,clock_timestamp())`, command.TaskID,
		command.Candidate.ProjectID, record.RunID, command.Candidate.NodeRunID,
		record.ExecutionNodeID, command.AttemptID, sequence, class, command.Now, runTraceID)
	if err != nil {
		return scheduling.Task{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return scheduling.Task{}, err
	}
	return scheduling.Task{MessageVersion: 1, TaskID: command.TaskID,
		ProjectID: command.Candidate.ProjectID, RunID: record.RunID,
		NodeRunID: command.Candidate.NodeRunID, ExecutionNodeID: record.ExecutionNodeID,
		AttemptID: command.AttemptID, AttemptSequence: sequence, ResourceClass: class,
		OccurredAt: command.Now, TraceID: runTraceID}, nil
}

var _ scheduling.Authority = (*Store)(nil)
