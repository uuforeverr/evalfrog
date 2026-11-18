package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	"github.com/uu999/evalfrog/internal/scheduling"
)

func (store *Store) Claim(ctx context.Context, record attempt.ClaimRecord) (attempt.Lease, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return attempt.Lease{}, err
	}
	defer tx.Rollback(ctx)
	lease, err := claimAttempt(ctx, tx, record)
	if err != nil {
		return attempt.Lease{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return attempt.Lease{}, err
	}
	return lease, nil
}

// ClaimBatch deliberately uses one short transaction and a deterministic lock
// order. Expected stale or duplicate records are item outcomes and do not
// abort the transaction; a MySQL failure rolls the entire batch back.
func (store *Store) ClaimBatch(ctx context.Context, records []attempt.ClaimRecord) ([]attempt.ClaimResult, error) {
	type indexed struct {
		index  int
		record attempt.ClaimRecord
	}
	ordered := make([]indexed, 0, len(records))
	results := make([]attempt.ClaimResult, len(records))
	seen := make(map[string]struct{}, len(records))
	for index, record := range records {
		key := record.ProjectID + "\x00" + record.RunID + "\x00" + record.AttemptID
		if _, duplicate := seen[key]; duplicate {
			results[index].Err = attempt.ErrStateConflict
			continue
		}
		seen[key] = struct{}{}
		ordered = append(ordered, indexed{index: index, record: record})
	}
	sort.Slice(ordered, func(left, right int) bool {
		a, b := ordered[left].record, ordered[right].record
		if a.ProjectID != b.ProjectID {
			return a.ProjectID < b.ProjectID
		}
		if a.RunID != b.RunID {
			return a.RunID < b.RunID
		}
		if a.NodeRunID != b.NodeRunID {
			return a.NodeRunID < b.NodeRunID
		}
		return a.AttemptID < b.AttemptID
	})
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	for _, item := range ordered {
		lease, claimErr := claimAttempt(ctx, tx, item.record)
		if claimErr != nil && !expectedClaimOutcome(claimErr) {
			return nil, claimErr
		}
		results[item.index] = attempt.ClaimResult{Lease: lease, Err: claimErr}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return results, nil
}

func expectedClaimOutcome(err error) bool {
	return errors.Is(err, attempt.ErrNotFound) || errors.Is(err, attempt.ErrNotCurrent) ||
		errors.Is(err, attempt.ErrStateConflict) || errors.Is(err, attempt.ErrLeaseMismatch) ||
		errors.Is(err, attempt.ErrCapabilityMismatch)
}

func claimAttempt(ctx context.Context, tx *Tx, record attempt.ClaimRecord) (attempt.Lease, error) {
	var state runtime.AttemptState
	var stateVersion uint64
	var sequence uint32
	var nodeState runtime.NodeState
	var currentAttemptID *string
	var nodeRunID string
	var existingToken, existingOwner *string
	var existingFencing uint64
	var existingExpiry *time.Time
	var existingLeaseValid, cancelRequested bool
	var operation dsl.Coordinate
	var resourceClass scheduling.ResourceClass
	err := tx.QueryRow(ctx, `
		SELECT BIN_TO_UUID(node_run_id) FROM node_attempts
		WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND attempt_id=UUID_TO_BIN($3)`,
		record.ProjectID, record.RunID, record.AttemptID).Scan(&nodeRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return attempt.Lease{}, attempt.ErrNotFound
	}
	if err != nil {
		return attempt.Lease{}, err
	}
	if record.NodeRunID != "" && record.NodeRunID != nodeRunID {
		return attempt.Lease{}, attempt.ErrNotCurrent
	}
	// Keep the same Node -> Attempt row-lock order used by Engine state restore.
	err = tx.QueryRow(ctx, `
		SELECT n.state, BIN_TO_UUID(n.current_attempt_id), n.operation_type, n.operation_version, n.resource_class,
		       r.cancel_requested_at IS NOT NULL OR r.termination_intent_json IS NOT NULL OR r.deadline_at <= CURRENT_TIMESTAMP(6)
		FROM node_runs n JOIN workflow_runs r ON r.project_id=n.project_id AND r.run_id=n.run_id
		WHERE n.project_id=UUID_TO_BIN($1) AND n.run_id=UUID_TO_BIN($2) AND n.node_run_id=UUID_TO_BIN($3)
		FOR UPDATE`, record.ProjectID, record.RunID, nodeRunID).
		Scan(&nodeState, &currentAttemptID, &operation.Type, &operation.Version, &resourceClass, &cancelRequested)
	if err != nil {
		return attempt.Lease{}, err
	}
	err = tx.QueryRow(ctx, `
		SELECT state, state_version, attempt_seq, BIN_TO_UUID(lease_token), lease_owner,
		       fencing_token, lease_expires_at,
		       COALESCE(lease_expires_at >= CURRENT_TIMESTAMP(6), false)
		FROM node_attempts
		WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND attempt_id=UUID_TO_BIN($3)
		FOR UPDATE`, record.ProjectID, record.RunID, record.AttemptID).
		Scan(&state, &stateVersion, &sequence,
			&existingToken, &existingOwner, &existingFencing, &existingExpiry, &existingLeaseValid)
	if err != nil {
		return attempt.Lease{}, err
	}
	if sequence != record.AttemptSequence || currentAttemptID == nil || *currentAttemptID != record.AttemptID {
		return attempt.Lease{}, attempt.ErrNotCurrent
	}
	if resourceClass != record.ResourceClass || !containsCapability(record.Capabilities, operation) {
		return attempt.Lease{}, attempt.ErrCapabilityMismatch
	}
	if state == runtime.AttemptRunning {
		if existingOwner != nil && *existingOwner == record.WorkerID && existingToken != nil && existingExpiry != nil && existingLeaseValid {
			return attempt.Lease{Token: *existingToken, Owner: *existingOwner, FencingToken: existingFencing, ExpiresAt: *existingExpiry, CancelRequested: cancelRequested}, nil
		}
		return attempt.Lease{}, attempt.ErrStateConflict
	}
	if state != runtime.AttemptQueued || nodeState != runtime.NodeQueued {
		return attempt.Lease{}, attempt.ErrStateConflict
	}
	fencing := uint64(record.AttemptSequence)
	var expiresAt time.Time
	tag, err := tx.Exec(ctx, `
		UPDATE node_attempts SET state='running', state_version=state_version+1,
		       lease_token=UUID_TO_BIN($1), lease_owner=$2,
		       lease_expires_at=TIMESTAMPADD(MICROSECOND,$3,CURRENT_TIMESTAMP(6)),
		       fencing_token=$4, executor_build=$5, updated_at=CURRENT_TIMESTAMP(6)
		WHERE project_id=UUID_TO_BIN($6) AND run_id=UUID_TO_BIN($7) AND attempt_id=UUID_TO_BIN($8)
		  AND state='queued' AND state_version=$9 AND attempt_seq=$10
		`, record.LeaseToken, record.WorkerID, record.LeaseDuration.Microseconds(), fencing,
		record.ExecutorBuild, record.ProjectID, record.RunID, record.AttemptID,
		stateVersion, record.AttemptSequence)
	if err != nil {
		return attempt.Lease{}, err
	}
	if tag.RowsAffected() != 1 {
		return attempt.Lease{}, attempt.ErrStateConflict
	}
	if err = tx.QueryRow(ctx, `SELECT lease_expires_at FROM node_attempts
		WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND attempt_id=UUID_TO_BIN($3)`,
		record.ProjectID, record.RunID, record.AttemptID).Scan(&expiresAt); err != nil {
		return attempt.Lease{}, err
	}
	nodeTag, err := tx.Exec(ctx, `
		UPDATE node_runs SET state='running', state_version=state_version+1, updated_at=$1
		WHERE project_id=UUID_TO_BIN($2) AND run_id=UUID_TO_BIN($3) AND current_attempt_id=UUID_TO_BIN($4) AND state='queued'`,
		record.Now, record.ProjectID, record.RunID, record.AttemptID)
	if err != nil || nodeTag.RowsAffected() != 1 {
		if err == nil {
			err = attempt.ErrStateConflict
		}
		return attempt.Lease{}, err
	}
	return attempt.Lease{Token: record.LeaseToken, Owner: record.WorkerID, FencingToken: fencing, ExpiresAt: expiresAt, CancelRequested: cancelRequested}, nil
}

func containsCapability(capabilities []dsl.Coordinate, required dsl.Coordinate) bool {
	for _, capability := range capabilities {
		if capability == required {
			return true
		}
	}
	return false
}

func (store *Store) Heartbeat(ctx context.Context, record attempt.HeartbeatRecord) (attempt.Lease, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return attempt.Lease{}, err
	}
	defer tx.Rollback(ctx)
	var owner string
	var expiresAt time.Time
	var cancelRequested bool
	tag, err := tx.Exec(ctx, `
		UPDATE node_attempts SET
		       lease_expires_at=TIMESTAMPADD(MICROSECOND,$1,CURRENT_TIMESTAMP(6)),
		       updated_at=CURRENT_TIMESTAMP(6)
		WHERE project_id=UUID_TO_BIN($2) AND run_id=UUID_TO_BIN($3) AND attempt_id=UUID_TO_BIN($4) AND attempt_seq=$5
		  AND state='running' AND lease_token=UUID_TO_BIN($6) AND fencing_token=$7
		  AND lease_expires_at >= CURRENT_TIMESTAMP(6)`, record.ExtendBy.Microseconds(),
		record.ProjectID, record.RunID, record.AttemptID, record.AttemptSequence,
		record.LeaseToken, record.FencingToken)
	if err != nil {
		return attempt.Lease{}, err
	}
	if tag.RowsAffected() != 1 {
		return attempt.Lease{}, attempt.ErrLeaseMismatch
	}
	err = tx.QueryRow(ctx, `SELECT a.lease_owner,a.lease_expires_at,
		(r.cancel_requested_at IS NOT NULL OR r.termination_intent_json IS NOT NULL OR r.deadline_at <= CURRENT_TIMESTAMP(6))
		FROM node_attempts a JOIN workflow_runs r ON r.project_id=a.project_id AND r.run_id=a.run_id
		WHERE a.project_id=UUID_TO_BIN($1) AND a.run_id=UUID_TO_BIN($2) AND a.attempt_id=UUID_TO_BIN($3) FOR UPDATE`,
		record.ProjectID, record.RunID, record.AttemptID).Scan(&owner, &expiresAt, &cancelRequested)
	if err != nil {
		return attempt.Lease{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return attempt.Lease{}, err
	}
	return attempt.Lease{Token: record.LeaseToken, Owner: owner, FencingToken: record.FencingToken, ExpiresAt: expiresAt, CancelRequested: cancelRequested}, nil
}

func (store *Store) Complete(ctx context.Context, record attempt.CompleteRecord) (bool, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var state runtime.AttemptState
	var stateVersion uint64
	var sequence uint32
	var nodeRunID, currentAttemptID string
	var leaseToken *string
	var fencing uint64
	var leaseExpiresAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT BIN_TO_UUID(node_run_id) FROM node_attempts
		WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND attempt_id=UUID_TO_BIN($3)`,
		record.ProjectID, record.RunID, record.AttemptID).Scan(&nodeRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, attempt.ErrNotFound
	}
	if err != nil {
		return false, err
	}
	// Lock the owning Node before its Attempt so all runtime transactions agree
	// on one row-lock order and MySQL only needs deadlock retries for true
	// cross-aggregate conflicts.
	err = tx.QueryRow(ctx, `
		SELECT BIN_TO_UUID(current_attempt_id) FROM node_runs
		WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND node_run_id=UUID_TO_BIN($3)
		FOR UPDATE`, record.ProjectID, record.RunID, nodeRunID).Scan(&currentAttemptID)
	if err != nil {
		return false, err
	}
	err = tx.QueryRow(ctx, `
		SELECT state, state_version, attempt_seq, BIN_TO_UUID(lease_token), fencing_token, lease_expires_at
		FROM node_attempts
		WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND attempt_id=UUID_TO_BIN($3)
		FOR UPDATE`, record.ProjectID, record.RunID, record.AttemptID).
		Scan(&state, &stateVersion, &sequence, &leaseToken, &fencing, &leaseExpiresAt)
	if err != nil {
		return false, err
	}
	if sequence != record.AttemptSequence {
		return false, attempt.ErrNotCurrent
	}
	if leaseToken == nil || *leaseToken != record.LeaseToken || fencing != record.FencingToken {
		return false, attempt.ErrLeaseMismatch
	}
	if state.Terminal() {
		var existingError []byte
		var existingOutput []byte
		if err = tx.QueryRow(ctx, `
			SELECT a.error_json, o.value_json
			FROM node_attempts a LEFT JOIN node_output_values o
			  ON o.project_id=a.project_id AND o.attempt_id=a.attempt_id
			WHERE a.project_id=UUID_TO_BIN($1) AND a.attempt_id=UUID_TO_BIN($2)`, record.ProjectID, record.AttemptID).
			Scan(&existingError, &existingOutput); err != nil {
			return false, err
		}
		if !sameCompletion(state, existingError, existingOutput, record.Result) {
			return false, attempt.ErrStateConflict
		}
		if err = tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	if currentAttemptID != record.AttemptID {
		return false, attempt.ErrNotCurrent
	}
	if state != runtime.AttemptRunning || leaseExpiresAt == nil {
		return false, attempt.ErrLeaseMismatch
	}
	errorJSON := completionError(record.Result)
	tag, err := tx.Exec(ctx, `
		UPDATE node_attempts SET state=$1, state_version=state_version+1,
		       error_json=$2, updated_at=$3
		WHERE project_id=UUID_TO_BIN($4) AND run_id=UUID_TO_BIN($5) AND attempt_id=UUID_TO_BIN($6) AND state='running'
		  AND state_version=$7 AND attempt_seq=$8 AND lease_token=UUID_TO_BIN($9) AND fencing_token=$10
		  AND lease_expires_at >= CURRENT_TIMESTAMP(6)`,
		record.Result.State, nullableRaw(errorJSON), record.Now, record.ProjectID, record.RunID,
		record.AttemptID, stateVersion, record.AttemptSequence, record.LeaseToken, record.FencingToken)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, attempt.ErrStateConflict
	}
	if record.Result.State == runtime.AttemptSucceeded {
		valueJSON, marshalErr := json.Marshal(record.Result.Outputs)
		if marshalErr != nil {
			return false, marshalErr
		}
		digest := sha256.Sum256(valueJSON)
		_, err = tx.Exec(ctx, `
			INSERT INTO node_output_values (
				project_id, attempt_id, run_id, node_run_id, value_json, size_bytes, content_hash, created_at
			) VALUES (UUID_TO_BIN($1),UUID_TO_BIN($2),UUID_TO_BIN($3),UUID_TO_BIN($4),$5,$6,$7,$8)`, record.ProjectID, record.AttemptID,
			record.RunID, nodeRunID, valueJSON, len(valueJSON), hex.EncodeToString(digest[:]), record.Now)
		if err != nil {
			return false, err
		}
	}
	event := eventing.RuntimeEvent{
		MessageVersion: eventing.RuntimeMessageVersion, EventID: record.EventID,
		ProjectID: record.ProjectID, RunID: record.RunID, AggregateType: eventing.NodeAttemptAggregate,
		AggregateID: record.AttemptID, EventType: eventing.AttemptCompleted,
		OccurredAt: record.Now, TraceID: record.TraceID,
	}
	if err = insertOutbox(ctx, tx, event); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (store *Store) MarkExpiredLost(ctx context.Context, record attempt.MarkLostRecord) (bool, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var state runtime.AttemptState
	var stateVersion uint64
	var sequence uint32
	var currentAttemptID string
	var nodeRunID string
	err = tx.QueryRow(ctx, `
		SELECT BIN_TO_UUID(node_run_id) FROM node_attempts
		WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND attempt_id=UUID_TO_BIN($3)`,
		record.ProjectID, record.RunID, record.AttemptID).Scan(&nodeRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, attempt.ErrNotFound
	}
	if err != nil {
		return false, err
	}
	err = tx.QueryRow(ctx, `
		SELECT BIN_TO_UUID(current_attempt_id) FROM node_runs
		WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND node_run_id=UUID_TO_BIN($3)
		FOR UPDATE`, record.ProjectID, record.RunID, nodeRunID).Scan(&currentAttemptID)
	if err != nil {
		return false, err
	}
	err = tx.QueryRow(ctx, `
		SELECT state, state_version, attempt_seq FROM node_attempts
		WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND attempt_id=UUID_TO_BIN($3)
		FOR UPDATE`, record.ProjectID, record.RunID, record.AttemptID).
		Scan(&state, &stateVersion, &sequence)
	if err != nil {
		return false, err
	}
	if sequence != record.AttemptSequence || currentAttemptID != record.AttemptID {
		return false, attempt.ErrNotCurrent
	}
	if state == runtime.AttemptLost {
		if err = tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	if state != runtime.AttemptRunning {
		return false, attempt.ErrStateConflict
	}
	errorJSON := completionError(runtime.AttemptResult{State: runtime.AttemptLost, ErrorCode: "LEASE_LOST", Message: "attempt lease expired"})
	tag, err := tx.Exec(ctx, `
		UPDATE node_attempts SET state='lost', state_version=state_version+1,
		       error_json=$1, updated_at=CURRENT_TIMESTAMP(6)
		WHERE project_id=UUID_TO_BIN($2) AND run_id=UUID_TO_BIN($3) AND attempt_id=UUID_TO_BIN($4)
		  AND state='running' AND state_version=$5 AND attempt_seq=$6
		  AND lease_expires_at <= CURRENT_TIMESTAMP(6)`, errorJSON, record.ProjectID,
		record.RunID, record.AttemptID, stateVersion, record.AttemptSequence)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, attempt.ErrLeaseMismatch
	}
	event := eventing.RuntimeEvent{
		MessageVersion: eventing.RuntimeMessageVersion, EventID: record.EventID,
		ProjectID: record.ProjectID, RunID: record.RunID, AggregateType: eventing.NodeAttemptAggregate,
		AggregateID: record.AttemptID, EventType: eventing.AttemptLost,
		OccurredAt: record.Now, TraceID: record.TraceID,
	}
	if err = insertOutbox(ctx, tx, event); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func completionError(result runtime.AttemptResult) json.RawMessage {
	if result.State == runtime.AttemptSucceeded {
		return nil
	}
	value, _ := json.Marshal(struct {
		ErrorCode    string         `json:"error_code"`
		Message      string         `json:"message"`
		DSLField     string         `json:"dsl_field,omitempty"`
		ErrorDetails map[string]any `json:"error_details,omitempty"`
	}{result.ErrorCode, result.Message, result.DSLField, result.ErrorDetails})
	return value
}

func sameCompletion(state runtime.AttemptState, errorJSON, outputJSON []byte, result runtime.AttemptResult) bool {
	if state != result.State {
		return false
	}
	if state == runtime.AttemptSucceeded {
		candidate, err := json.Marshal(result.Outputs)
		return err == nil && jsonEqual(candidate, outputJSON)
	}
	return jsonEqual(completionError(result), errorJSON)
}

func jsonEqual(left, right []byte) bool {
	var a, b any
	return json.Unmarshal(left, &a) == nil && json.Unmarshal(right, &b) == nil && reflect.DeepEqual(a, b)
}

var _ attempt.Repository = (*Store)(nil)
