package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/runtime"
)

const ConsumerName = "runtime-engine-v1"

type TransactionManager interface {
	WithRunTransaction(context.Context, eventing.RuntimeEvent, func(RunTransaction) error) error
}

type BatchTransactionManager interface {
	WithRunBatchTransaction(context.Context, func(RunTransaction) error) error
}

type RunTransaction interface {
	AcceptInbox(context.Context, string, eventing.RuntimeEvent) (bool, error)
	AuthorityNow(context.Context) (time.Time, error)
	LoadRun(context.Context, string, string) (runtime.WorkflowRunRecord, error)
	LoadSnapshot(context.Context, string, string) (Snapshot, error)
	LoadEngineState(context.Context, string, string) (State, error)
	InitializeRun(context.Context, runtime.WorkflowRunRecord, State, time.Time) error
	AdvanceRun(context.Context, State, State, time.Time) error
	FailRunInitialization(context.Context, runtime.WorkflowRunRecord, runtime.WorkflowRunRecord, time.Time) error
}

type Consumer struct {
	transactions TransactionManager
	maxInflight  int
}

func NewConsumer(transactions TransactionManager) (Consumer, error) {
	return NewConsumerWithConcurrency(transactions, 1)
}

func NewConsumerWithConcurrency(transactions TransactionManager, maxInflight int) (Consumer, error) {
	if transactions == nil || maxInflight <= 0 {
		return Consumer{}, fmt.Errorf("engine transaction manager is required")
	}
	return Consumer{transactions: transactions, maxInflight: maxInflight}, nil
}

func (consumer Consumer) Consume(ctx context.Context, event eventing.RuntimeEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	return consumer.transactions.WithRunTransaction(ctx, event, func(tx RunTransaction) error {
		return consumer.consumeInTransaction(ctx, tx, event)
	})
}

func (consumer Consumer) consumeInTransaction(ctx context.Context, tx RunTransaction, event eventing.RuntimeEvent) error {
	accepted, err := tx.AcceptInbox(ctx, ConsumerName, event)
	if err != nil || !accepted {
		return err
	}
	switch event.EventType {
	case eventing.RunCreated:
		return consumer.initialize(ctx, tx, event)
	case eventing.AttemptCompleted, eventing.AttemptLost, eventing.RetryDue,
		eventing.RunCancelRequested, eventing.RunDeadlineReached:
		return consumer.advance(ctx, tx, event)
	default:
		return fmt.Errorf("runtime event type %q is unsupported by engine", event.EventType)
	}
}

// ConsumeBatch groups a Kafka poll by Run. Events for one Run remain ordered
// in one PostgreSQL transaction; different Runs execute with explicit bounded
// concurrency so Kafka batch size never becomes database connection demand.
func (consumer Consumer) ConsumeBatch(ctx context.Context, events []eventing.RuntimeEvent) error {
	if len(events) == 0 {
		return nil
	}
	groups := make(map[string][]eventing.RuntimeEvent)
	order := make([]string, 0)
	for _, event := range events {
		if err := event.Validate(); err != nil {
			return err
		}
		if _, exists := groups[event.RunID]; !exists {
			order = append(order, event.RunID)
		}
		groups[event.RunID] = append(groups[event.RunID], event)
	}
	jobs := make(chan []eventing.RuntimeEvent)
	results := make(chan error, len(order))
	workers := min(consumer.maxInflight, len(order))
	var wait sync.WaitGroup
	for index := 0; index < workers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for group := range jobs {
				results <- consumer.consumeRunBatch(ctx, group)
			}
		}()
	}
	for _, runID := range order {
		select {
		case jobs <- groups[runID]:
		case <-ctx.Done():
			close(jobs)
			wait.Wait()
			close(results)
			return ctx.Err()
		}
	}
	close(jobs)
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			return err
		}
	}
	return nil
}

func (consumer Consumer) consumeRunBatch(ctx context.Context, events []eventing.RuntimeEvent) error {
	if transactions, ok := consumer.transactions.(BatchTransactionManager); ok {
		return transactions.WithRunBatchTransaction(ctx, func(tx RunTransaction) error {
			for _, event := range events {
				if err := consumer.consumeInTransaction(ctx, tx, event); err != nil {
					return err
				}
			}
			return nil
		})
	}
	for _, event := range events {
		if err := consumer.Consume(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (consumer Consumer) initialize(ctx context.Context, tx RunTransaction, event eventing.RuntimeEvent) error {
	runRecord, err := tx.LoadRun(ctx, event.ProjectID, event.RunID)
	if err != nil {
		return err
	}
	if runRecord.State != runtime.RunPending {
		return nil
	}
	now, err := tx.AuthorityNow(ctx)
	if err != nil {
		return err
	}
	// Cancellation is a durable authority fact. A replayed RunCreated must not
	// initialize executable Nodes after the cancel request was committed but
	// before its own wake-up reached this consumer.
	if cancelWinsDeadline(runRecord) {
		return consumer.advancePendingCancellation(ctx, tx, event, runRecord)
	}
	// A delayed RunCreated is only a wake-up. PostgreSQL time, rather than the
	// message timestamp, decides whether its immutable Run has already expired.
	if !now.Before(runRecord.DeadlineAt) {
		return consumer.advancePendingDeadline(ctx, tx, event, runRecord, now)
	}
	snapshot, err := tx.LoadSnapshot(ctx, event.ProjectID, runRecord.Definition.SnapshotID)
	if err != nil {
		return err
	}
	instance, err := NewBuiltinV1(snapshot, runtime.CreateRunCommand{
		RunID: runRecord.ID, ProjectID: runRecord.ProjectID, WorkflowID: runRecord.WorkflowID,
		Purpose: runRecord.Purpose, Definition: runRecord.Definition, WorkflowInput: runRecord.WorkflowInput,
		DeadlineAt: runRecord.DeadlineAt, CreatedAt: runRecord.CreatedAt,
	})
	if err != nil {
		code := "RUNTIME_DSL_INVALID"
		var runtimeError *Error
		if errors.As(err, &runtimeError) && runtimeError.Code != "" {
			code = runtimeError.Code
		}
		failure := runtime.Failure{
			Code: code, Phase: "run_initialization", Retryable: false,
			RunID: runRecord.ID, SnapshotID: runRecord.Definition.SnapshotID,
			DefinitionHash: runRecord.Definition.DefinitionHash, Message: err.Error(),
		}
		if runtimeError != nil {
			failure.ExecutionNodeID = runtimeError.NodeID
			failure.DSLField = runtimeError.Field
		}
		failed, restoreErr := runtime.RestoreWorkflowRun(runRecord)
		if restoreErr != nil {
			return restoreErr
		}
		if failErr := failed.FailInitialization(failure, event.OccurredAt); failErr != nil {
			return failErr
		}
		return tx.FailRunInitialization(ctx, runRecord, failed.Snapshot(), now)
	}
	return tx.InitializeRun(ctx, runRecord, instance.SnapshotState(), now)
}

func (consumer Consumer) advancePendingDeadline(ctx context.Context, tx RunTransaction, event eventing.RuntimeEvent, before runtime.WorkflowRunRecord, at time.Time) error {
	run, err := runtime.RestoreWorkflowRun(before)
	if err != nil {
		return err
	}
	applied, err := run.RequestTermination(runtime.TerminationIntent{
		Kind: runtime.TerminationTimedOut, RequestedAt: at,
		Cause: runtime.Failure{Code: FailureRunTimedOut, Phase: "run_control", RunID: run.ID(), SnapshotID: run.Definition().SnapshotID, DefinitionHash: run.Definition().DefinitionHash, Message: "workflow deadline reached"},
	})
	if err != nil || !applied {
		return err
	}
	if err = run.CompleteTermination(nil); err != nil {
		return err
	}
	after := State{Run: run.Snapshot()}
	return tx.AdvanceRun(ctx, State{Run: before}, after, at)
}

func (consumer Consumer) advancePendingCancellation(ctx context.Context, tx RunTransaction, event eventing.RuntimeEvent, before runtime.WorkflowRunRecord) error {
	run, err := runtime.RestoreWorkflowRun(before)
	if err != nil {
		return err
	}
	requestedAt := before.CancelRequestedAt
	if requestedAt.IsZero() {
		requestedAt = event.OccurredAt
	}
	applied, err := run.RequestTermination(runtime.TerminationIntent{
		Kind: runtime.TerminationCanceled, RequestedAt: requestedAt,
		Cause: runtime.Failure{Code: FailureRunCanceled, Phase: "run_control", RunID: run.ID(), SnapshotID: run.Definition().SnapshotID, DefinitionHash: run.Definition().DefinitionHash, Message: "run cancellation requested"},
	})
	if err != nil || !applied {
		return err
	}
	if err = run.CompleteTermination(nil); err != nil {
		return err
	}
	after := State{Run: run.Snapshot()}
	return tx.AdvanceRun(ctx, State{Run: before}, after, event.OccurredAt)
}

func (consumer Consumer) advance(ctx context.Context, tx RunTransaction, event eventing.RuntimeEvent) error {
	before, err := tx.LoadEngineState(ctx, event.ProjectID, event.RunID)
	if err != nil {
		return err
	}
	if before.Run.State.Terminal() {
		return nil
	}
	// A Run can be cancelled after its durable creation transaction but before
	// RunCreated initializes the graph. There are intentionally no Node Runs in
	// that interval, so do not try to restore a complete Engine graph.
	if before.Run.State == runtime.RunPending {
		now, nowErr := tx.AuthorityNow(ctx)
		if nowErr != nil {
			return nowErr
		}
		if cancelWinsDeadline(before.Run) {
			return consumer.advancePendingCancellation(ctx, tx, event, before.Run)
		}
		if !now.Before(before.Run.DeadlineAt) {
			return consumer.advancePendingDeadline(ctx, tx, event, before.Run, now)
		}
		if event.EventType != eventing.RunCancelRequested && before.Run.CancelRequestedAt.IsZero() {
			return nil
		}
		return consumer.advancePendingCancellation(ctx, tx, event, before.Run)
	}
	snapshot, err := tx.LoadSnapshot(ctx, event.ProjectID, before.Run.Definition.SnapshotID)
	if err != nil {
		return err
	}
	instance, err := RestoreBuiltinV1(snapshot, before)
	if err != nil {
		return err
	}
	now, err := tx.AuthorityNow(ctx)
	if err != nil {
		return err
	}
	// The durable cancellation timestamp and deadline are authority facts even
	// when their Kafka wake-up is delayed. Completing a just-cancelled Attempt
	// must never accidentally turn the Run into a business failure.
	if before.Run.Termination == nil && cancelWinsDeadline(before.Run) {
		if _, err = instance.RequestCancel(before.Run.CancelRequestedAt, "run cancellation requested"); err != nil {
			return err
		}
		return tx.AdvanceRun(ctx, before, instance.SnapshotState(), now)
	} else if before.Run.Termination == nil && !now.Before(before.Run.DeadlineAt) {
		if _, err = instance.DeadlineReached(now); err != nil {
			return err
		}
		return tx.AdvanceRun(ctx, before, instance.SnapshotState(), now)
	}
	switch event.EventType {
	case eventing.AttemptCompleted, eventing.AttemptLost:
		err = instance.HandleAttemptCompleted(event.AggregateID, event.OccurredAt)
	case eventing.RetryDue:
		nodeID, exists := attemptNodeID(before, event.AggregateID)
		if !exists {
			return nil
		}
		err = instance.RetryDue(nodeID, event.OccurredAt)
	case eventing.RunCancelRequested:
		_, err = instance.RequestCancel(event.OccurredAt, "run cancellation requested")
	case eventing.RunDeadlineReached:
		_, err = instance.DeadlineReached(event.OccurredAt)
	}
	if err != nil {
		return err
	}
	return tx.AdvanceRun(ctx, before, instance.SnapshotState(), now)
}

// cancelWinsDeadline turns two durable source facts into the first terminal
// intent. A cancel request issued before the immutable deadline must not be
// overwritten merely because its Kafka wake-up was delayed past that deadline.
// A request made after the deadline cannot revive the Run or block timeout.
func cancelWinsDeadline(run runtime.WorkflowRunRecord) bool {
	return !run.CancelRequestedAt.IsZero() && !run.CancelRequestedAt.After(run.DeadlineAt)
}

func attemptNodeID(state State, attemptID string) (dsl.NodeID, bool) {
	for _, attempt := range state.Attempts {
		if attempt.ID != attemptID {
			continue
		}
		for _, node := range state.Nodes {
			if attempt.NodeRunID == node.RunID+":"+node.ExecutionNodeID {
				return dsl.NodeID(node.ExecutionNodeID), true
			}
		}
	}
	return "", false
}
