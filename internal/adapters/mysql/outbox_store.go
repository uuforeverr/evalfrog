package mysql

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
	rows, err := tx.Query(ctx, `SELECT BIN_TO_UUID(event_id), BIN_TO_UUID(project_id), BIN_TO_UUID(run_id), aggregate_type,
		BIN_TO_UUID(aggregate_id), event_type, message_version, occurred_at, trace_id FROM outbox_events
		WHERE published_at IS NULL AND available_at <= CURRENT_TIMESTAMP(6)
		AND (claim_token IS NULL OR claim_expires_at <= CURRENT_TIMESTAMP(6))
		ORDER BY occurred_at, event_id LIMIT $1 FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return nil, err
	}
	var result []eventing.ClaimedEvent
	for rows.Next() {
		var value eventing.ClaimedEvent
		if err = rows.Scan(&value.Event.EventID, &value.Event.ProjectID, &value.Event.RunID, &value.Event.AggregateType,
			&value.Event.AggregateID, &value.Event.EventType, &value.Event.MessageVersion, &value.Event.OccurredAt, &value.Event.TraceID); err != nil {
			rows.Close()
			return nil, err
		}
		token, tokenErr := uuid.NewV7()
		if tokenErr != nil {
			rows.Close()
			return nil, tokenErr
		}
		value.ClaimToken = token.String()
		result = append(result, value)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for _, value := range result {
		tag, updateErr := tx.Exec(ctx, `UPDATE outbox_events SET claim_owner=$1, claim_token=UUID_TO_BIN($2),
			claim_expires_at=TIMESTAMPADD(MICROSECOND,$3,CURRENT_TIMESTAMP(6)), publish_attempts=publish_attempts+1
			WHERE event_id=UUID_TO_BIN($4) AND published_at IS NULL`, owner, value.ClaimToken, lease.Microseconds(), value.Event.EventID)
		if updateErr != nil {
			return nil, updateErr
		}
		if tag.RowsAffected() != 1 {
			return nil, runtime.ErrRunConflict
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (store *Store) MarkOutboxPublished(ctx context.Context, id, token string) error {
	return store.updateOutbox(ctx, store.pool, id, token, 0, true)
}
func (store *Store) ReleaseOutboxClaim(ctx context.Context, id, token string, delay time.Duration) error {
	return store.updateOutbox(ctx, store.pool, id, token, delay, false)
}
func (store *Store) MarkOutboxPublishedBatch(ctx context.Context, values []eventing.ClaimedIdentity) error {
	return store.updateOutboxBatch(ctx, values, 0, true)
}
func (store *Store) ReleaseOutboxClaimsBatch(ctx context.Context, values []eventing.ClaimedIdentity, delay time.Duration) error {
	return store.updateOutboxBatch(ctx, values, delay, false)
}

type sqlExecutor interface {
	Exec(context.Context, string, ...any) (CommandTag, error)
}

func (store *Store) updateOutbox(ctx context.Context, executor sqlExecutor, id, token string, delay time.Duration, published bool) error {
	var tag CommandTag
	var err error
	if published {
		tag, err = executor.Exec(ctx, `UPDATE outbox_events SET published_at=CURRENT_TIMESTAMP(6), claim_owner=NULL,
			claim_token=NULL, claim_expires_at=NULL WHERE event_id=UUID_TO_BIN($1) AND claim_token=UUID_TO_BIN($2) AND published_at IS NULL`, id, token)
	} else {
		tag, err = executor.Exec(ctx, `UPDATE outbox_events SET available_at=TIMESTAMPADD(MICROSECOND,$1,CURRENT_TIMESTAMP(6)),
			claim_owner=NULL, claim_token=NULL, claim_expires_at=NULL WHERE event_id=UUID_TO_BIN($2) AND claim_token=UUID_TO_BIN($3) AND published_at IS NULL`, delay.Microseconds(), id, token)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return runtime.ErrRunConflict
	}
	return nil
}

func (store *Store) updateOutboxBatch(ctx context.Context, values []eventing.ClaimedIdentity, delay time.Duration, published bool) error {
	if len(values) == 0 {
		return nil
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, value := range values {
		if err = store.updateOutbox(ctx, tx, value.ID, value.ClaimToken, delay, published); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (store *Store) ClaimTaskOutbox(ctx context.Context, owner string, batch int, lease time.Duration) ([]eventing.ClaimedTask, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT BIN_TO_UUID(task_id), BIN_TO_UUID(project_id), BIN_TO_UUID(run_id), BIN_TO_UUID(node_run_id),
		execution_node_id, BIN_TO_UUID(attempt_id), attempt_seq, resource_class, message_version, occurred_at, trace_id
		FROM node_task_outbox WHERE published_at IS NULL AND available_at <= CURRENT_TIMESTAMP(6)
		AND (claim_token IS NULL OR claim_expires_at <= CURRENT_TIMESTAMP(6))
		ORDER BY occurred_at, task_id LIMIT $1 FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return nil, err
	}
	var result []eventing.ClaimedTask
	for rows.Next() {
		var value eventing.ClaimedTask
		if err = rows.Scan(&value.Message.TaskID, &value.Message.ProjectID, &value.Message.RunID, &value.Message.NodeRunID,
			&value.Message.ExecutionNodeID, &value.Message.AttemptID, &value.Message.AttemptSequence, &value.Message.ResourceClass,
			&value.Message.MessageVersion, &value.Message.OccurredAt, &value.Message.TraceID); err != nil {
			rows.Close()
			return nil, err
		}
		token, tokenErr := uuid.NewV7()
		if tokenErr != nil {
			rows.Close()
			return nil, tokenErr
		}
		value.ClaimToken = token.String()
		result = append(result, value)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for _, value := range result {
		tag, updateErr := tx.Exec(ctx, `UPDATE node_task_outbox SET claim_owner=$1, claim_token=UUID_TO_BIN($2),
			claim_expires_at=TIMESTAMPADD(MICROSECOND,$3,CURRENT_TIMESTAMP(6)), publish_attempts=publish_attempts+1
			WHERE task_id=UUID_TO_BIN($4) AND published_at IS NULL`, owner, value.ClaimToken, lease.Microseconds(), value.Message.TaskID)
		if updateErr != nil {
			return nil, updateErr
		}
		if tag.RowsAffected() != 1 {
			return nil, runtime.ErrRunConflict
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (store *Store) MarkTaskOutboxPublished(ctx context.Context, id, token string) error {
	return store.updateTaskOutbox(ctx, store.pool, id, token, 0, true)
}
func (store *Store) ReleaseTaskOutboxClaim(ctx context.Context, id, token string, delay time.Duration) error {
	return store.updateTaskOutbox(ctx, store.pool, id, token, delay, false)
}
func (store *Store) MarkTaskOutboxPublishedBatch(ctx context.Context, values []eventing.ClaimedIdentity) error {
	return store.updateTaskBatch(ctx, values, 0, true)
}
func (store *Store) ReleaseTaskOutboxClaimsBatch(ctx context.Context, values []eventing.ClaimedIdentity, delay time.Duration) error {
	return store.updateTaskBatch(ctx, values, delay, false)
}

func (store *Store) updateTaskOutbox(ctx context.Context, executor sqlExecutor, id, token string, delay time.Duration, published bool) error {
	var tag CommandTag
	var err error
	if published {
		tag, err = executor.Exec(ctx, `UPDATE node_task_outbox SET published_at=CURRENT_TIMESTAMP(6), claim_owner=NULL,
			claim_token=NULL, claim_expires_at=NULL WHERE task_id=UUID_TO_BIN($1) AND claim_token=UUID_TO_BIN($2) AND published_at IS NULL`, id, token)
	} else {
		tag, err = executor.Exec(ctx, `UPDATE node_task_outbox SET available_at=TIMESTAMPADD(MICROSECOND,$1,CURRENT_TIMESTAMP(6)),
			claim_owner=NULL, claim_token=NULL, claim_expires_at=NULL WHERE task_id=UUID_TO_BIN($2) AND claim_token=UUID_TO_BIN($3) AND published_at IS NULL`, delay.Microseconds(), id, token)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return runtime.ErrRunConflict
	}
	return nil
}

func (store *Store) updateTaskBatch(ctx context.Context, values []eventing.ClaimedIdentity, delay time.Duration, published bool) error {
	if len(values) == 0 {
		return nil
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, value := range values {
		if err = store.updateTaskOutbox(ctx, tx, value.ID, value.ClaimToken, delay, published); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

var _ eventing.OutboxRepository = (*Store)(nil)
var _ eventing.BatchOutboxRepository = (*Store)(nil)
var _ eventing.TaskOutboxRepository = (*Store)(nil)
var _ eventing.BatchTaskOutboxRepository = (*Store)(nil)
