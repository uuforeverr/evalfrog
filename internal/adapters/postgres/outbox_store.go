package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/runtime"
)

func (store *Store) ClaimOutbox(ctx context.Context, owner string, batch int, lease time.Duration) ([]eventing.ClaimedEvent, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
		SELECT event_id::text, project_id::text, run_id::text, aggregate_type,
		       aggregate_id::text, event_type, message_version, occurred_at, trace_id
		FROM outbox_events
		WHERE published_at IS NULL AND available_at <= clock_timestamp()
		  AND (claim_token IS NULL OR claim_expires_at <= clock_timestamp())
		ORDER BY available_at, event_id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return nil, err
	}
	var result []eventing.ClaimedEvent
	for rows.Next() {
		var claimed eventing.ClaimedEvent
		if err = rows.Scan(&claimed.Event.EventID, &claimed.Event.ProjectID, &claimed.Event.RunID,
			&claimed.Event.AggregateType, &claimed.Event.AggregateID, &claimed.Event.EventType,
			&claimed.Event.MessageVersion, &claimed.Event.OccurredAt, &claimed.Event.TraceID); err != nil {
			rows.Close()
			return nil, err
		}
		token, tokenErr := uuid.NewV7()
		if tokenErr != nil {
			rows.Close()
			return nil, tokenErr
		}
		claimed.ClaimToken = token.String()
		result = append(result, claimed)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if len(result) > 0 {
		ids, tokens := make([]string, len(result)), make([]string, len(result))
		for index, claimed := range result {
			ids[index], tokens[index] = claimed.Event.EventID, claimed.ClaimToken
		}
		tag, updateErr := tx.Exec(ctx, `
			WITH claimed(event_id, claim_token) AS (
			  SELECT * FROM unnest($1::uuid[], $2::uuid[])
			)
			UPDATE outbox_events target
			SET claim_owner=$3, claim_token=claimed.claim_token,
			    claim_expires_at=clock_timestamp()+($4 * interval '1 millisecond'),
			    publish_attempts=target.publish_attempts+1
			FROM claimed
			WHERE target.event_id=claimed.event_id AND target.published_at IS NULL`, ids, tokens, owner, lease.Milliseconds())
		if updateErr != nil {
			return nil, updateErr
		}
		if tag.RowsAffected() != int64(len(result)) {
			return nil, runtime.ErrRunConflict
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (store *Store) MarkOutboxPublished(ctx context.Context, eventID, claimToken string) error {
	tag, err := store.pool.Exec(ctx, `
		UPDATE outbox_events SET published_at=clock_timestamp(), claim_owner=NULL,
		       claim_token=NULL, claim_expires_at=NULL
		WHERE event_id=$1 AND claim_token=$2 AND published_at IS NULL`, eventID, claimToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return runtime.ErrRunConflict
	}
	return nil
}

func (store *Store) MarkOutboxPublishedBatch(ctx context.Context, values []eventing.ClaimedIdentity) error {
	if len(values) == 0 {
		return nil
	}
	ids, tokens := claimedArrays(values)
	tag, err := store.pool.Exec(ctx, `
		WITH claimed(event_id, claim_token) AS (
		  SELECT * FROM unnest($1::uuid[], $2::uuid[])
		)
		UPDATE outbox_events target
		SET published_at=clock_timestamp(), claim_owner=NULL,
		    claim_token=NULL, claim_expires_at=NULL
		FROM claimed
		WHERE target.event_id=claimed.event_id AND target.claim_token=claimed.claim_token
		  AND target.published_at IS NULL`, ids, tokens)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(values)) {
		return runtime.ErrRunConflict
	}
	return nil
}

func (store *Store) ReleaseOutboxClaim(ctx context.Context, eventID, claimToken string, delay time.Duration) error {
	tag, err := store.pool.Exec(ctx, `
		UPDATE outbox_events SET available_at=clock_timestamp()+($1 * interval '1 millisecond'),
		       claim_owner=NULL, claim_token=NULL, claim_expires_at=NULL
		WHERE event_id=$2 AND claim_token=$3 AND published_at IS NULL`, delay.Milliseconds(), eventID, claimToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return runtime.ErrRunConflict
	}
	return nil
}

func (store *Store) ReleaseOutboxClaimsBatch(ctx context.Context, values []eventing.ClaimedIdentity, delay time.Duration) error {
	if len(values) == 0 {
		return nil
	}
	ids, tokens := claimedArrays(values)
	tag, err := store.pool.Exec(ctx, `
		WITH claimed(event_id, claim_token) AS (
		  SELECT * FROM unnest($1::uuid[], $2::uuid[])
		)
		UPDATE outbox_events target
		SET available_at=clock_timestamp()+($3 * interval '1 millisecond'),
		    claim_owner=NULL, claim_token=NULL, claim_expires_at=NULL
		FROM claimed
		WHERE target.event_id=claimed.event_id AND target.claim_token=claimed.claim_token
		  AND target.published_at IS NULL`, ids, tokens, delay.Milliseconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(values)) {
		return runtime.ErrRunConflict
	}
	return nil
}

func (store *Store) ClaimTaskOutbox(ctx context.Context, owner string, batch int, lease time.Duration) ([]eventing.ClaimedTask, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
		SELECT task_id::text, project_id::text, run_id::text, node_run_id::text,
		       execution_node_id, attempt_id::text, attempt_seq, resource_class,
		       message_version, occurred_at, trace_id
		FROM node_task_outbox
		WHERE published_at IS NULL AND available_at <= clock_timestamp()
		  AND (claim_token IS NULL OR claim_expires_at <= clock_timestamp())
		ORDER BY available_at, task_id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return nil, err
	}
	var result []eventing.ClaimedTask
	for rows.Next() {
		var claimed eventing.ClaimedTask
		if err = rows.Scan(&claimed.Message.TaskID, &claimed.Message.ProjectID,
			&claimed.Message.RunID, &claimed.Message.NodeRunID,
			&claimed.Message.ExecutionNodeID, &claimed.Message.AttemptID,
			&claimed.Message.AttemptSequence, &claimed.Message.ResourceClass,
			&claimed.Message.MessageVersion, &claimed.Message.OccurredAt,
			&claimed.Message.TraceID); err != nil {
			rows.Close()
			return nil, err
		}
		token, tokenErr := uuid.NewV7()
		if tokenErr != nil {
			rows.Close()
			return nil, tokenErr
		}
		claimed.ClaimToken = token.String()
		result = append(result, claimed)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(result) > 0 {
		ids, tokens := make([]string, len(result)), make([]string, len(result))
		for index, claimed := range result {
			ids[index], tokens[index] = claimed.Message.TaskID, claimed.ClaimToken
		}
		tag, updateErr := tx.Exec(ctx, `
			WITH claimed(task_id, claim_token) AS (
			  SELECT * FROM unnest($1::uuid[], $2::uuid[])
			)
			UPDATE node_task_outbox target
			SET claim_owner=$3, claim_token=claimed.claim_token,
			    claim_expires_at=clock_timestamp()+($4 * interval '1 millisecond'),
			    publish_attempts=target.publish_attempts+1
			FROM claimed
			WHERE target.task_id=claimed.task_id AND target.published_at IS NULL`, ids, tokens, owner, lease.Milliseconds())
		if updateErr != nil {
			return nil, updateErr
		}
		if tag.RowsAffected() != int64(len(result)) {
			return nil, runtime.ErrRunConflict
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (store *Store) MarkTaskOutboxPublished(ctx context.Context, taskID, claimToken string) error {
	tag, err := store.pool.Exec(ctx, `
		UPDATE node_task_outbox SET published_at=clock_timestamp(), claim_owner=NULL,
		       claim_token=NULL, claim_expires_at=NULL
		WHERE task_id=$1 AND claim_token=$2 AND published_at IS NULL`, taskID, claimToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return runtime.ErrRunConflict
	}
	return nil
}

func (store *Store) MarkTaskOutboxPublishedBatch(ctx context.Context, values []eventing.ClaimedIdentity) error {
	if len(values) == 0 {
		return nil
	}
	ids, tokens := claimedArrays(values)
	tag, err := store.pool.Exec(ctx, `
		WITH claimed(task_id, claim_token) AS (
		  SELECT * FROM unnest($1::uuid[], $2::uuid[])
		)
		UPDATE node_task_outbox target
		SET published_at=clock_timestamp(), claim_owner=NULL,
		    claim_token=NULL, claim_expires_at=NULL
		FROM claimed
		WHERE target.task_id=claimed.task_id AND target.claim_token=claimed.claim_token
		  AND target.published_at IS NULL`, ids, tokens)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(values)) {
		return runtime.ErrRunConflict
	}
	return nil
}

func (store *Store) ReleaseTaskOutboxClaim(ctx context.Context, taskID, claimToken string, delay time.Duration) error {
	tag, err := store.pool.Exec(ctx, `
		UPDATE node_task_outbox SET available_at=clock_timestamp()+($1 * interval '1 millisecond'),
		       claim_owner=NULL, claim_token=NULL, claim_expires_at=NULL
		WHERE task_id=$2 AND claim_token=$3 AND published_at IS NULL`, delay.Milliseconds(), taskID, claimToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return runtime.ErrRunConflict
	}
	return nil
}

func (store *Store) ReleaseTaskOutboxClaimsBatch(ctx context.Context, values []eventing.ClaimedIdentity, delay time.Duration) error {
	if len(values) == 0 {
		return nil
	}
	ids, tokens := claimedArrays(values)
	tag, err := store.pool.Exec(ctx, `
		WITH claimed(task_id, claim_token) AS (
		  SELECT * FROM unnest($1::uuid[], $2::uuid[])
		)
		UPDATE node_task_outbox target
		SET available_at=clock_timestamp()+($3 * interval '1 millisecond'),
		    claim_owner=NULL, claim_token=NULL, claim_expires_at=NULL
		FROM claimed
		WHERE target.task_id=claimed.task_id AND target.claim_token=claimed.claim_token
		  AND target.published_at IS NULL`, ids, tokens, delay.Milliseconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != int64(len(values)) {
		return runtime.ErrRunConflict
	}
	return nil
}

func claimedArrays(values []eventing.ClaimedIdentity) ([]string, []string) {
	ids := make([]string, len(values))
	tokens := make([]string, len(values))
	for index, value := range values {
		ids[index] = value.ID
		tokens[index] = value.ClaimToken
	}
	return ids, tokens
}

var _ eventing.OutboxRepository = (*Store)(nil)
var _ eventing.BatchOutboxRepository = (*Store)(nil)
var _ eventing.TaskOutboxRepository = (*Store)(nil)
var _ eventing.BatchTaskOutboxRepository = (*Store)(nil)
