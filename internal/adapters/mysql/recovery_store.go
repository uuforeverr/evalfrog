package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/recovery"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
)

func (store *Store) ListExpiredAttempts(ctx context.Context, grace time.Duration, batch int) ([]attempt.MarkLostCommand, error) {
	rows, err := store.pool.Query(ctx, `
		SELECT BIN_TO_UUID(a.project_id), BIN_TO_UUID(a.run_id), BIN_TO_UUID(a.attempt_id), a.attempt_seq, r.trace_id
		FROM node_attempts a
		JOIN node_runs n ON n.project_id=a.project_id AND n.run_id=a.run_id AND n.node_run_id=a.node_run_id
		JOIN workflow_runs r ON r.project_id=a.project_id AND r.run_id=a.run_id
		WHERE a.state='running' AND n.current_attempt_id=a.attempt_id
		  AND TIMESTAMPADD(MICROSECOND,$1,a.lease_expires_at) <= CURRENT_TIMESTAMP(6)
		ORDER BY a.lease_expires_at, a.attempt_id
		LIMIT $2`, grace.Microseconds(), batch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make([]attempt.MarkLostCommand, 0, batch)
	for rows.Next() {
		var value attempt.MarkLostCommand
		if err = rows.Scan(&value.ProjectID, &value.RunID, &value.AttemptID, &value.AttemptSequence, &value.TraceID); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (store *Store) ListRetryDue(ctx context.Context, batch int) ([]recovery.Wakeup, error) {
	rows, err := store.pool.Query(ctx, `
		SELECT BIN_TO_UUID(n.project_id), BIN_TO_UUID(n.run_id), BIN_TO_UUID(n.current_attempt_id), r.trace_id
		FROM node_runs n
		JOIN workflow_runs r ON r.project_id=n.project_id AND r.run_id=n.run_id
		WHERE n.state='retry_wait' AND n.next_retry_at <= CURRENT_TIMESTAMP(6)
		  AND n.current_attempt_id IS NOT NULL
		  AND r.state='running' AND r.termination_intent_json IS NULL
		  AND r.deadline_at > CURRENT_TIMESTAMP(6)
		ORDER BY n.next_retry_at, n.node_run_id
		LIMIT $1`, batch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWakeups(rows, eventing.RetryDue)
}

func (store *Store) ListDeadlinesDue(ctx context.Context, batch int) ([]recovery.Wakeup, error) {
	rows, err := store.pool.Query(ctx, `
		SELECT BIN_TO_UUID(project_id), BIN_TO_UUID(run_id), BIN_TO_UUID(run_id), trace_id
		FROM workflow_runs
		WHERE state IN ('pending','running') AND deadline_at <= CURRENT_TIMESTAMP(6)
		ORDER BY deadline_at, run_id
		LIMIT $1`, batch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanWakeups(rows, eventing.RunDeadlineReached)
}

// ListReconciliationWakeups identifies only durable facts that are already
// actionable. It cannot repair a state by itself: it re-emits a signal and
// lets the Engine Inbox/CAS reload the same rows.
func (store *Store) ListReconciliationWakeups(ctx context.Context, batch int) ([]recovery.Wakeup, error) {
	rows, err := store.pool.Query(ctx, `
		WITH candidates AS (
		  SELECT r.project_id, r.run_id, r.run_id AS aggregate_id, 'run.created' AS event_type, r.created_at AS due_at, r.trace_id
		  FROM workflow_runs r
		  WHERE r.state='pending' AND r.cancel_requested_at IS NULL
		  UNION ALL
		  SELECT r.project_id, r.run_id, r.run_id, 'run.cancel_requested', r.cancel_requested_at, r.trace_id
		  FROM workflow_runs r
		  WHERE r.state IN ('pending','running') AND r.cancel_requested_at IS NOT NULL
		  UNION ALL
		  SELECT a.project_id, a.run_id, a.attempt_id, 'attempt.completed', a.updated_at, r.trace_id
		  FROM node_attempts a
		  JOIN node_runs n ON n.project_id=a.project_id AND n.run_id=a.run_id AND n.node_run_id=a.node_run_id
		  JOIN workflow_runs r ON r.project_id=a.project_id AND r.run_id=a.run_id
		  WHERE a.state IN ('succeeded','failed','timed_out','canceled')
		    AND n.current_attempt_id=a.attempt_id
		    AND n.state IN ('queued','running')
		    AND r.state='running'
		  UNION ALL
		  SELECT a.project_id, a.run_id, a.attempt_id, 'attempt.lost', a.updated_at, r.trace_id
		  FROM node_attempts a
		  JOIN node_runs n ON n.project_id=a.project_id AND n.run_id=a.run_id AND n.node_run_id=a.node_run_id
		  JOIN workflow_runs r ON r.project_id=a.project_id AND r.run_id=a.run_id
		  WHERE a.state='lost' AND n.current_attempt_id=a.attempt_id
		    AND n.state IN ('queued','running')
		    AND r.state='running'
		)
		SELECT BIN_TO_UUID(project_id), BIN_TO_UUID(run_id), BIN_TO_UUID(aggregate_id), event_type, trace_id
		FROM candidates
		ORDER BY due_at, aggregate_id
		LIMIT $1`, batch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make([]recovery.Wakeup, 0, batch)
	for rows.Next() {
		var value recovery.Wakeup
		var eventType eventing.RuntimeEventType
		if err = rows.Scan(&value.ProjectID, &value.RunID, &value.AggregateID, &eventType, &value.TraceID); err != nil {
			return nil, err
		}
		value.EventType = eventType
		if err = value.Validate(); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func scanWakeups(rows *sql.Rows, eventType eventing.RuntimeEventType) ([]recovery.Wakeup, error) {
	values := make([]recovery.Wakeup, 0)
	for rows.Next() {
		var value recovery.Wakeup
		if err := rows.Scan(&value.ProjectID, &value.RunID, &value.AggregateID, &value.TraceID); err != nil {
			return nil, err
		}
		value.EventType = eventType
		if err := value.Validate(); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (store *Store) EmitWakeup(ctx context.Context, emission recovery.WakeupEmission) (bool, error) {
	if err := emission.Wakeup.Validate(); err != nil || emission.EventID == "" || emission.AuditID == "" || emission.TraceID == "" || emission.ActorID == "" || emission.At.IsZero() || emission.Cooldown < 0 {
		return false, fmt.Errorf("recovery wakeup emission is invalid: %w", err)
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	// Every SQL predicate repeats the scanner condition while the row is
	// locked. A stale scanner result consequently becomes a no-op rather than
	// an event for an obsolete Node/Run state.
	valid, err := wakeupStillActionable(ctx, tx, emission.Wakeup)
	if err != nil || !valid {
		return false, err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO runtime_recovery_wakeups (
			project_id, run_id, event_type, aggregate_id, last_event_id, last_emitted_at, emit_count
		) VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),$3,UUID_TO_BIN($4),UUID_TO_BIN($5),$6,1)`,
		emission.ProjectID, emission.RunID, emission.EventType, emission.AggregateID, emission.EventID, emission.At)
	if isDuplicateKey(err) {
		var lastEmitted time.Time
		err = tx.QueryRow(ctx, `SELECT last_emitted_at FROM runtime_recovery_wakeups
			WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND event_type=$3 AND aggregate_id=UUID_TO_BIN($4) FOR UPDATE`,
			emission.ProjectID, emission.RunID, emission.EventType, emission.AggregateID).Scan(&lastEmitted)
		if err != nil {
			return false, err
		}
		if lastEmitted.Add(emission.Cooldown).After(emission.At) {
			if err = tx.Commit(ctx); err != nil {
				return false, err
			}
			return false, nil
		}
		tag, err = tx.Exec(ctx, `UPDATE runtime_recovery_wakeups SET last_event_id=UUID_TO_BIN($1),last_emitted_at=$2,emit_count=emit_count+1
			WHERE project_id=UUID_TO_BIN($3) AND run_id=UUID_TO_BIN($4) AND event_type=$5 AND aggregate_id=UUID_TO_BIN($6)`,
			emission.EventID, emission.At, emission.ProjectID, emission.RunID, emission.EventType, emission.AggregateID)
	}
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		if err = tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	event := eventing.RuntimeEvent{MessageVersion: eventing.RuntimeMessageVersion, EventID: emission.EventID,
		ProjectID: emission.ProjectID, RunID: emission.RunID, AggregateType: emission.AggregateType(), AggregateID: emission.AggregateID,
		EventType: emission.EventType, OccurredAt: emission.At, TraceID: emission.TraceID}
	if err = insertOutbox(ctx, tx, event); err != nil {
		return false, err
	}
	action := "recovery.wakeup_emitted"
	if emission.Manual {
		action = "recovery.manual_replay"
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO runtime_audit_events (
			audit_id, project_id, run_id, action, actor_type, actor_id, trace_id, details_json, created_at
		) VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),UUID_TO_BIN($3),$4,$5,$6,$7,JSON_OBJECT('event_type',$8,'aggregate_id',$9),$10)`,
		emission.AuditID, emission.ProjectID, emission.RunID, action, emission.ActorType, emission.ActorID, emission.TraceID,
		emission.EventType, emission.AggregateID, emission.At)
	if err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	store.notifyRunUpdate(ctx, emission.RunID)
	return true, nil
}

func wakeupStillActionable(ctx context.Context, tx *Tx, wakeup recovery.Wakeup) (bool, error) {
	var found bool
	var err error
	switch wakeup.EventType {
	case eventing.RunCreated:
		err = tx.QueryRow(ctx, `SELECT TRUE FROM workflow_runs WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2)
			AND state='pending' AND cancel_requested_at IS NULL FOR UPDATE`, wakeup.ProjectID, wakeup.RunID).Scan(&found)
	case eventing.RunCancelRequested:
		err = tx.QueryRow(ctx, `SELECT TRUE FROM workflow_runs WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2)
			AND state IN ('pending','running') AND cancel_requested_at IS NOT NULL FOR UPDATE`, wakeup.ProjectID, wakeup.RunID).Scan(&found)
	case eventing.RunDeadlineReached:
		err = tx.QueryRow(ctx, `SELECT TRUE FROM workflow_runs WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2)
			AND state IN ('pending','running') AND deadline_at <= CURRENT_TIMESTAMP(6) FOR UPDATE`, wakeup.ProjectID, wakeup.RunID).Scan(&found)
	case eventing.RetryDue:
		err = tx.QueryRow(ctx, `SELECT TRUE FROM node_runs n JOIN workflow_runs r ON r.project_id=n.project_id AND r.run_id=n.run_id
			WHERE n.project_id=UUID_TO_BIN($1) AND n.run_id=UUID_TO_BIN($2) AND n.current_attempt_id=UUID_TO_BIN($3)
			AND n.state='retry_wait' AND n.next_retry_at <= CURRENT_TIMESTAMP(6)
			AND r.state='running' AND r.termination_intent_json IS NULL AND r.deadline_at > CURRENT_TIMESTAMP(6) FOR UPDATE`, wakeup.ProjectID, wakeup.RunID, wakeup.AggregateID).Scan(&found)
	case eventing.AttemptCompleted:
		err = tx.QueryRow(ctx, `SELECT TRUE FROM node_attempts a JOIN node_runs n ON n.project_id=a.project_id AND n.run_id=a.run_id AND n.node_run_id=a.node_run_id
			JOIN workflow_runs r ON r.project_id=a.project_id AND r.run_id=a.run_id
			WHERE a.project_id=UUID_TO_BIN($1) AND a.run_id=UUID_TO_BIN($2) AND a.attempt_id=UUID_TO_BIN($3)
			AND a.state IN ('succeeded','failed','timed_out','canceled') AND n.current_attempt_id=a.attempt_id
			AND n.state IN ('queued','running') AND r.state='running' FOR UPDATE`, wakeup.ProjectID, wakeup.RunID, wakeup.AggregateID).Scan(&found)
	case eventing.AttemptLost:
		err = tx.QueryRow(ctx, `SELECT TRUE FROM node_attempts a JOIN node_runs n ON n.project_id=a.project_id AND n.run_id=a.run_id AND n.node_run_id=a.node_run_id
			JOIN workflow_runs r ON r.project_id=a.project_id AND r.run_id=a.run_id
			WHERE a.project_id=UUID_TO_BIN($1) AND a.run_id=UUID_TO_BIN($2) AND a.attempt_id=UUID_TO_BIN($3)
			AND a.state='lost' AND n.current_attempt_id=a.attempt_id
			AND n.state IN ('queued','running') AND r.state='running' FOR UPDATE`, wakeup.ProjectID, wakeup.RunID, wakeup.AggregateID).Scan(&found)
	default:
		return false, fmt.Errorf("unsupported recovery wakeup type %q", wakeup.EventType)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return found, err
}

var _ recovery.Repository = (*Store)(nil)
var _ recovery.WakeupRepository = (*Store)(nil)
