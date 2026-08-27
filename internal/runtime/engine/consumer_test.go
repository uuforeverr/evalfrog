package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/runtime"
)

type fakeTransactions struct{ tx *fakeRunTx }

func (manager *fakeTransactions) WithRunTransaction(_ context.Context, _ eventing.RuntimeEvent, operation func(RunTransaction) error) error {
	return operation(manager.tx)
}

type failingTransactions struct{ err error }

func (manager failingTransactions) WithRunTransaction(context.Context, eventing.RuntimeEvent, func(RunTransaction) error) error {
	return manager.err
}

type fakeRunTx struct {
	accepted          bool
	run               runtime.WorkflowRunRecord
	snapshot          Snapshot
	state             State
	initialized       bool
	advanced          bool
	failedInit        bool
	loadRunError      error
	loadSnapshotError error
	loadStateError    error
	inboxError        error
	initializeError   error
	advanceError      error
	authorityNow      time.Time
	authorityNowError error
	failInitError     error
	advancedBefore    State
	advancedAfter     State
	initializedState  State
	acceptedEvents    []string
}

func (tx *fakeRunTx) AcceptInbox(_ context.Context, _ string, event eventing.RuntimeEvent) (bool, error) {
	tx.acceptedEvents = append(tx.acceptedEvents, event.EventID)
	return tx.accepted, tx.inboxError
}
func (tx *fakeRunTx) AuthorityNow(context.Context) (time.Time, error) {
	if tx.authorityNowError != nil || !tx.authorityNow.IsZero() {
		return tx.authorityNow, tx.authorityNowError
	}
	if !tx.state.Run.CreatedAt.IsZero() {
		return tx.state.Run.CreatedAt, nil
	}
	return tx.run.CreatedAt, nil
}
func (tx *fakeRunTx) LoadRun(context.Context, string, string) (runtime.WorkflowRunRecord, error) {
	return tx.run, tx.loadRunError
}
func (tx *fakeRunTx) LoadSnapshot(context.Context, string, string) (Snapshot, error) {
	return tx.snapshot, tx.loadSnapshotError
}
func (tx *fakeRunTx) LoadEngineState(context.Context, string, string) (State, error) {
	return tx.state, tx.loadStateError
}
func (tx *fakeRunTx) InitializeRun(_ context.Context, _ runtime.WorkflowRunRecord, state State, _ time.Time) error {
	tx.initialized = true
	tx.initializedState = state
	return tx.initializeError
}

type consumerIDs string

func (value consumerIDs) New() (string, error) { return string(value), nil }
func (tx *fakeRunTx) AdvanceRun(_ context.Context, before, after State, _ time.Time) error {
	tx.advanced = true
	tx.advancedBefore, tx.advancedAfter = before, after
	return tx.advanceError
}
func (tx *fakeRunTx) FailRunInitialization(_ context.Context, _, _ runtime.WorkflowRunRecord, _ time.Time) error {
	tx.failedInit = true
	return tx.failInitError
}

func TestConsumerInitializesPendingRunAndDeduplicatesInbox(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	state := harness.Engine.SnapshotState()
	pending, err := runtime.NewWorkflowRun(runtime.CreateRunCommand{
		RunID: state.Run.ID, ProjectID: state.Run.ProjectID, WorkflowID: state.Run.WorkflowID,
		Purpose: state.Run.Purpose, Definition: state.Run.Definition, WorkflowInput: state.Run.WorkflowInput,
		CreatedAt: state.Run.CreatedAt, DeadlineAt: state.Run.DeadlineAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	tx := &fakeRunTx{accepted: true, authorityNow: harness.Now(), run: pending.Snapshot(), snapshot: harness.Engine.snapshot}
	consumer, _ := NewConsumerWithIdentity(&fakeTransactions{tx}, consumerIDs("00000000-0000-7000-8000-000000000001"), 1)
	event := testRuntimeEvent(eventing.RunCreated, state.Run.ID, state.Run.ID, state.Run.CreatedAt)
	if err = consumer.Consume(context.Background(), event); err != nil || !tx.initialized {
		t.Fatalf("initialized=%v err=%v", tx.initialized, err)
	}
	ready, queued := 0, 0
	for _, node := range tx.initializedState.Nodes {
		if node.State == runtime.NodeReady {
			ready++
		}
		if node.State == runtime.NodeQueued {
			queued++
		}
	}
	if ready != 0 || queued != 1 || len(tx.initializedState.Attempts) != 1 || tx.initializedState.Attempts[0].State != runtime.AttemptQueued {
		t.Fatalf("ready=%d queued=%d attempts=%+v", ready, queued, tx.initializedState.Attempts)
	}
	tx.accepted, tx.initialized = false, false
	if err = consumer.Consume(context.Background(), event); err != nil || tx.initialized {
		t.Fatalf("duplicate initialized=%v err=%v", tx.initialized, err)
	}
}

type batchCountingTransactions struct {
	mu           sync.Mutex
	active       int
	maxActive    int
	transactions []*fakeRunTx
	delay        time.Duration
}

func (manager *batchCountingTransactions) WithRunTransaction(_ context.Context, _ eventing.RuntimeEvent, operation func(RunTransaction) error) error {
	tx := &fakeRunTx{accepted: true, run: runtime.WorkflowRunRecord{State: runtime.RunRunning}}
	return operation(tx)
}

func (manager *batchCountingTransactions) WithRunBatchTransaction(_ context.Context, operation func(RunTransaction) error) error {
	tx := &fakeRunTx{accepted: true, run: runtime.WorkflowRunRecord{State: runtime.RunRunning}}
	manager.mu.Lock()
	manager.active++
	manager.maxActive = max(manager.maxActive, manager.active)
	manager.transactions = append(manager.transactions, tx)
	manager.mu.Unlock()
	delay := manager.delay
	if delay == 0 {
		delay = 10 * time.Millisecond
	}
	time.Sleep(delay)
	err := operation(tx)
	manager.mu.Lock()
	manager.active--
	manager.mu.Unlock()
	return err
}

func TestConsumerBatchCapsOneThousandRunBurstAtDatabaseBudget(t *testing.T) {
	manager := &batchCountingTransactions{delay: time.Millisecond}
	consumer, err := NewConsumerWithConcurrency(manager, 7)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	events := make([]eventing.RuntimeEvent, 1000)
	for index := range events {
		runID := fmt.Sprintf("run-%04d", index)
		events[index] = testRuntimeEvent(eventing.RunCreated, runID, runID, at)
		events[index].EventID = fmt.Sprintf("event-%04d", index)
	}
	if err = consumer.ConsumeBatch(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.transactions) != 1000 || manager.maxActive != 7 {
		t.Fatalf("transactions=%d max_active=%d", len(manager.transactions), manager.maxActive)
	}
}

func TestConsumerBatchSerializesOneRunAndBoundsDifferentRunTransactions(t *testing.T) {
	manager := &batchCountingTransactions{}
	consumer, err := NewConsumerWithConcurrency(manager, 2)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	events := []eventing.RuntimeEvent{
		testRuntimeEvent(eventing.RunCreated, "run-a", "run-a", at),
		testRuntimeEvent(eventing.RunCreated, "run-b", "run-b", at),
		testRuntimeEvent(eventing.RunCreated, "run-a", "run-a", at.Add(time.Millisecond)),
		testRuntimeEvent(eventing.RunCreated, "run-c", "run-c", at),
	}
	for index := range events {
		events[index].EventID = fmt.Sprintf("batch-event-%d", index)
	}
	if err = consumer.ConsumeBatch(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.transactions) != 3 || manager.maxActive != 2 {
		t.Fatalf("run transactions=%d max_active=%d", len(manager.transactions), manager.maxActive)
	}
	foundOrderedRun := false
	for _, tx := range manager.transactions {
		if len(tx.acceptedEvents) == 2 {
			foundOrderedRun = tx.acceptedEvents[0] == "batch-event-0" && tx.acceptedEvents[1] == "batch-event-2"
		}
	}
	if !foundOrderedRun {
		t.Fatalf("same-Run events were not serialized in Kafka order: %+v", manager.transactions)
	}
}

func TestConsumerBatchValidatesInputAndSupportsLegacyTransactionManager(t *testing.T) {
	consumer, _ := NewConsumer(&fakeTransactions{&fakeRunTx{accepted: true, run: runtime.WorkflowRunRecord{State: runtime.RunRunning}}})
	if err := consumer.ConsumeBatch(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := consumer.ConsumeBatch(context.Background(), []eventing.RuntimeEvent{{}}); err == nil {
		t.Fatal("invalid batch event was accepted")
	}
	at := time.Now().UTC()
	events := []eventing.RuntimeEvent{
		testRuntimeEvent(eventing.RunCreated, "run", "run", at),
		testRuntimeEvent(eventing.RunCreated, "run", "run", at.Add(time.Millisecond)),
	}
	events[0].EventID, events[1].EventID = "one", "two"
	if err := consumer.ConsumeBatch(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("transaction")
	consumer, _ = NewConsumer(failingTransactions{err: failure})
	if err := consumer.ConsumeBatch(context.Background(), events[:1]); !errors.Is(err, failure) {
		t.Fatalf("legacy transaction failure=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	manager := &batchCountingTransactions{}
	consumer, _ = NewConsumerWithConcurrency(manager, 1)
	otherRun := testRuntimeEvent(eventing.RunCreated, "other-run", "other-run", at)
	otherRun.EventID = "other"
	if err := consumer.ConsumeBatch(canceled, append(events[:1], otherRun)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled batch=%v", err)
	}
}

func TestConsumerBatchTransactionRollsBackRunGroupOnEventFailure(t *testing.T) {
	manager := &batchCountingTransactions{}
	consumer, _ := NewConsumerWithConcurrency(manager, 1)
	event := testRuntimeEvent(eventing.RuntimeEventType("unknown"), "run", "run", time.Now().UTC())
	if err := consumer.ConsumeBatch(context.Background(), []eventing.RuntimeEvent{event}); err == nil {
		t.Fatal("batch transaction event failure was hidden")
	}
}

func TestConsumerIgnoresRunCreatedAfterInitialization(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	state := harness.Engine.SnapshotState()
	tx := &fakeRunTx{accepted: true, authorityNow: harness.Now(), run: state.Run}
	consumer, _ := NewConsumer(&fakeTransactions{tx})
	event := testRuntimeEvent(eventing.RunCreated, state.Run.ID, state.Run.ID, state.Run.CreatedAt)
	if err := consumer.Consume(context.Background(), event); err != nil || tx.initialized || tx.failedInit {
		t.Fatalf("initialized=%v failedInit=%v err=%v", tx.initialized, tx.failedInit, err)
	}
}

func TestConsumerFailsUnsupportedInitializationAndRollsBackMissingFact(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	pending, _ := runtime.NewWorkflowRun(runtime.CreateRunCommand{
		RunID: "run", ProjectID: "project", WorkflowID: "workflow", Purpose: runtime.RunPurposeTest,
		Definition:    runtime.DefinitionReference{SnapshotID: "snapshot", DefinitionHash: "hash", Source: runtime.DefinitionDraftSnapshot},
		WorkflowInput: []byte(`{}`), CreatedAt: harness.Now(), DeadlineAt: harness.Now().Add(time.Hour),
	})
	unsupported := harness.Engine.snapshot
	unsupported.DSL.Nodes[1].Operation.Version = 99
	tx := &fakeRunTx{accepted: true, authorityNow: harness.Now(), run: pending.Snapshot(), snapshot: unsupported}
	consumer, _ := NewConsumer(&fakeTransactions{tx})
	event := testRuntimeEvent(eventing.RunCreated, "run", "run", harness.Now())
	if err := consumer.Consume(context.Background(), event); err != nil || !tx.failedInit {
		t.Fatalf("failedInit=%v err=%v", tx.failedInit, err)
	}
	tx = &fakeRunTx{accepted: true, authorityNow: harness.Now(), loadStateError: errors.New("attempt result missing")}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	event = testRuntimeEvent(eventing.AttemptCompleted, "run", "attempt", harness.Now())
	if err := consumer.Consume(context.Background(), event); err == nil {
		t.Fatal("missing authoritative attempt fact accepted")
	}
}

func TestConsumerIgnoresTerminalRunEvent(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	completeAllReady(t, harness)
	tx := &fakeRunTx{accepted: true, authorityNow: harness.Now(), state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot}
	consumer, _ := NewConsumer(&fakeTransactions{tx})
	event := testRuntimeEvent(eventing.RunDeadlineReached, "run", "run", harness.Now().Add(time.Hour))
	if err := consumer.Consume(context.Background(), event); err != nil || tx.advanced {
		t.Fatalf("advanced=%v err=%v", tx.advanced, err)
	}
}

func TestConsumerCancelBeforeRunInitializationConvergesWithoutNodeRuns(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	state := harness.Engine.SnapshotState()
	pending, err := runtime.NewWorkflowRun(runtime.CreateRunCommand{
		RunID: state.Run.ID, ProjectID: state.Run.ProjectID, WorkflowID: state.Run.WorkflowID,
		Purpose: state.Run.Purpose, Definition: state.Run.Definition, WorkflowInput: state.Run.WorkflowInput,
		CreatedAt: state.Run.CreatedAt, DeadlineAt: state.Run.DeadlineAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := State{Run: pending.Snapshot()}
	consumer, _ := NewConsumer(&fakeTransactions{tx: &fakeRunTx{accepted: true, authorityNow: harness.Now(), state: before}})
	event := testRuntimeEvent(eventing.RunCancelRequested, before.Run.ID, before.Run.ID, harness.Now())
	if err = consumer.Consume(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	tx := consumer.transactions.(*fakeTransactions).tx
	if !tx.advanced || tx.advancedBefore.Run.State != runtime.RunPending || tx.advancedAfter.Run.State != runtime.RunCanceled || tx.advancedAfter.Run.Termination == nil || tx.advancedAfter.Run.Termination.Cause.Code != FailureRunCanceled || len(tx.advancedAfter.Nodes) != 0 {
		t.Fatalf("before=%+v after=%+v advanced=%v", tx.advancedBefore.Run, tx.advancedAfter.Run, tx.advanced)
	}

	ignoredTx := &fakeRunTx{accepted: true, authorityNow: harness.Now(), state: before}
	ignored, _ := NewConsumer(&fakeTransactions{tx: ignoredTx})
	if err = ignored.Consume(context.Background(), testRuntimeEvent(eventing.RunDeadlineReached, before.Run.ID, before.Run.ID, harness.Now())); err != nil || ignoredTx.advanced {
		t.Fatalf("pending non-cancel advanced=%v err=%v", ignoredTx.advanced, err)
	}
}

func TestConsumerPendingTerminationUsesDurableFactsAndPropagatesPersistenceFailure(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	state := harness.Engine.SnapshotState()
	pending, err := runtime.NewWorkflowRun(runtime.CreateRunCommand{
		RunID: state.Run.ID, ProjectID: state.Run.ProjectID, WorkflowID: state.Run.WorkflowID,
		Purpose: state.Run.Purpose, Definition: state.Run.Definition, WorkflowInput: state.Run.WorkflowInput,
		CreatedAt: state.Run.CreatedAt, DeadlineAt: state.Run.DeadlineAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := State{Run: pending.Snapshot()}

	t.Run("unrelated wakeup cannot terminate an unexpired pending run", func(t *testing.T) {
		tx := &fakeRunTx{accepted: true, authorityNow: harness.Now(), state: before}
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		if err := consumer.Consume(context.Background(), testRuntimeEvent(eventing.RunDeadlineReached, before.Run.ID, before.Run.ID, harness.Now())); err != nil {
			t.Fatal(err)
		}
		if tx.advanced {
			t.Fatal("unexpired pending run was changed by an unrelated wakeup")
		}
	})

	t.Run("cancel persists the terminal transition atomically", func(t *testing.T) {
		tx := &fakeRunTx{accepted: true, authorityNow: harness.Now(), state: before, advanceError: errors.New("persist cancel")}
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		if err := consumer.Consume(context.Background(), testRuntimeEvent(eventing.RunCancelRequested, before.Run.ID, before.Run.ID, harness.Now())); err == nil || !tx.advanced {
			t.Fatalf("advance=%t err=%v", tx.advanced, err)
		}
	})

	t.Run("deadline persists the terminal transition atomically", func(t *testing.T) {
		tx := &fakeRunTx{accepted: true, authorityNow: before.Run.DeadlineAt, run: before.Run, advanceError: errors.New("persist deadline")}
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		if err := consumer.Consume(context.Background(), testRuntimeEvent(eventing.RunCreated, before.Run.ID, before.Run.ID, before.Run.CreatedAt)); err == nil || !tx.advanced {
			t.Fatalf("advance=%t err=%v", tx.advanced, err)
		}
	})
}

func TestConsumerPendingTerminationRejectsInvalidOrNoopTransitions(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	state := harness.Engine.SnapshotState()
	pending, err := runtime.NewWorkflowRun(runtime.CreateRunCommand{
		RunID: state.Run.ID, ProjectID: state.Run.ProjectID, WorkflowID: state.Run.WorkflowID,
		Purpose: state.Run.Purpose, Definition: state.Run.Definition, WorkflowInput: state.Run.WorkflowInput,
		CreatedAt: state.Run.CreatedAt, DeadlineAt: state.Run.DeadlineAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := pending.Snapshot()
	consumer, _ := NewConsumer(&fakeTransactions{tx: &fakeRunTx{accepted: true, authorityNow: harness.Now(), state: State{Run: before}}})

	// An Attempt event cannot create a terminal intent for an uninitialized Run.
	if err = consumer.Consume(context.Background(), testRuntimeEvent(eventing.AttemptLost, before.ID, "attempt", harness.Now())); err != nil {
		t.Fatal(err)
	}
	tx := consumer.transactions.(*fakeTransactions).tx
	if tx.advanced {
		t.Fatal("attempt wakeup terminated a pending run")
	}

	// A deadline wakeup before the durable deadline is a no-op, even if it was
	// emitted by a stale scanner.
	tx = &fakeRunTx{accepted: true, authorityNow: harness.Now(), run: before, snapshot: harness.Engine.snapshot}
	consumer, _ = NewConsumer(&fakeTransactions{tx: tx})
	if err = consumer.Consume(context.Background(), testRuntimeEvent(eventing.RunCreated, before.ID, before.ID, before.CreatedAt)); err != nil {
		t.Fatal(err)
	}
	if !tx.initialized {
		t.Fatal("unexpired pending run was not initialized")
	}

	// A persisted cancellation that wins must remain idempotent when duplicate
	// durable wakeups arrive after the transition has already been applied.
	canceled := before
	canceled.CancelRequestedAt = harness.Now()
	tx = &fakeRunTx{accepted: true, authorityNow: harness.Now(), state: State{Run: canceled}}
	consumer, _ = NewConsumer(&fakeTransactions{tx: tx})
	if err = consumer.Consume(context.Background(), testRuntimeEvent(eventing.RunCancelRequested, canceled.ID, canceled.ID, harness.Now())); err != nil {
		t.Fatal(err)
	}
	if !tx.advanced || tx.advancedAfter.Run.State != runtime.RunCanceled {
		t.Fatalf("first cancellation did not converge: %+v", tx.advancedAfter.Run)
	}
	tx.state = tx.advancedAfter
	tx.advanced = false
	if err = consumer.Consume(context.Background(), testRuntimeEvent(eventing.RunCancelRequested, canceled.ID, canceled.ID, harness.Now())); err != nil {
		t.Fatal(err)
	}
	if tx.advanced {
		t.Fatal("terminal cancellation advanced twice")
	}
}

func TestConsumerPendingTerminationRejectsCorruptPersistedRun(t *testing.T) {
	consumer, _ := NewConsumer(&fakeTransactions{tx: &fakeRunTx{accepted: true}})
	event := testRuntimeEvent(eventing.RunDeadlineReached, "run", "run", time.Now().UTC())
	if err := consumer.advancePendingDeadline(context.Background(), consumer.transactions.(*fakeTransactions).tx, event, runtime.WorkflowRunRecord{}, event.OccurredAt); err == nil {
		t.Fatal("corrupt pending run was accepted by deadline recovery")
	}
	if err := consumer.advancePendingCancellation(context.Background(), consumer.transactions.(*fakeTransactions).tx, event, runtime.WorkflowRunRecord{}); err == nil {
		t.Fatal("corrupt pending run was accepted by cancellation recovery")
	}
	if _, exists := attemptNodeID(State{Attempts: []runtime.NodeAttemptRecord{{ID: "attempt", NodeRunID: "unknown"}}}, "attempt"); exists {
		t.Fatal("attempt without a persisted node owner was accepted")
	}
}

func TestConsumerRunCreatedHonorsPersistedCancellationIntent(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	state := harness.Engine.SnapshotState()
	pending, err := runtime.NewWorkflowRun(runtime.CreateRunCommand{
		RunID: state.Run.ID, ProjectID: state.Run.ProjectID, WorkflowID: state.Run.WorkflowID,
		Purpose: state.Run.Purpose, Definition: state.Run.Definition, WorkflowInput: state.Run.WorkflowInput,
		CreatedAt: state.Run.CreatedAt, DeadlineAt: state.Run.DeadlineAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := pending.Snapshot()
	record.CancelRequestedAt = harness.Now().Add(time.Millisecond)
	tx := &fakeRunTx{accepted: true, authorityNow: harness.Now(), run: record, snapshot: harness.Engine.snapshot}
	consumer, _ := NewConsumer(&fakeTransactions{tx})
	if err = consumer.Consume(context.Background(), testRuntimeEvent(eventing.RunCreated, record.ID, record.ID, record.CreatedAt)); err != nil {
		t.Fatal(err)
	}
	if tx.initialized || !tx.advanced || tx.advancedAfter.Run.State != runtime.RunCanceled || len(tx.advancedAfter.Nodes) != 0 {
		t.Fatalf("initialized=%t advanced=%t after=%+v", tx.initialized, tx.advanced, tx.advancedAfter)
	}
}

func TestConsumerUsesAuthorityTimeForDelayedWakeups(t *testing.T) {
	t.Run("expired pending run is not initialized", func(t *testing.T) {
		harness := newTestHarness(t, linearDocument(1))
		state := harness.Engine.SnapshotState()
		pending, err := runtime.NewWorkflowRun(runtime.CreateRunCommand{
			RunID: state.Run.ID, ProjectID: state.Run.ProjectID, WorkflowID: state.Run.WorkflowID,
			Purpose: state.Run.Purpose, Definition: state.Run.Definition, WorkflowInput: state.Run.WorkflowInput,
			CreatedAt: state.Run.CreatedAt, DeadlineAt: state.Run.DeadlineAt,
		})
		if err != nil {
			t.Fatal(err)
		}
		tx := &fakeRunTx{accepted: true, authorityNow: pending.DeadlineAt(), run: pending.Snapshot(), snapshot: harness.Engine.snapshot}
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		if err = consumer.Consume(context.Background(), testRuntimeEvent(eventing.RunCreated, pending.ID(), pending.ID(), pending.CreatedAt())); err != nil {
			t.Fatal(err)
		}
		if tx.initialized || !tx.advanced || tx.advancedAfter.Run.State != runtime.RunTimedOut {
			t.Fatalf("initialized=%t advanced=%t state=%s", tx.initialized, tx.advanced, tx.advancedAfter.Run.State)
		}
	})

	t.Run("late retry due cannot revive expired run", func(t *testing.T) {
		policy := dsl.ExecutionPolicy{MaxAttempts: 2, MaxRecoveries: 1, AttemptTimeoutMS: 1000, RetryBackoff: &dsl.RetryBackoff{Kind: "fixed", DelayMS: 10}, RetryableErrorCodes: []string{"TEMP"}}
		harness := newTestHarness(t, linearDocumentWithPolicy(1, policy))
		attempts, _ := harness.StartReady()
		nodeID := harness.Engine.attemptNodes[attempts[0]]
		_, _ = harness.Engine.RecordAttemptResult(attempts[0], runtime.AttemptResult{State: runtime.AttemptFailed, ErrorCode: "TEMP"})
		if err := harness.Engine.HandleAttemptCompleted(attempts[0], harness.Now()); err != nil {
			t.Fatal(err)
		}
		state := harness.Engine.SnapshotState()
		tx := &fakeRunTx{accepted: true, authorityNow: state.Run.DeadlineAt, state: state, snapshot: harness.Engine.snapshot}
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		if err := consumer.Consume(context.Background(), testRuntimeEvent(eventing.RetryDue, state.Run.ID, attempts[0], harness.Now().Add(10*time.Millisecond))); err != nil {
			t.Fatal(err)
		}
		if !tx.advanced || tx.advancedAfter.Run.State != runtime.RunTimedOut {
			t.Fatalf("node=%s advanced=%t state=%s", nodeID, tx.advanced, tx.advancedAfter.Run.State)
		}
	})

	t.Run("cancel requested before deadline wins delayed delivery race", func(t *testing.T) {
		harness := newTestHarness(t, linearDocument(1))
		state := harness.Engine.SnapshotState()
		state.Run.CancelRequestedAt = state.Run.DeadlineAt.Add(-time.Nanosecond)
		tx := &fakeRunTx{accepted: true, authorityNow: state.Run.DeadlineAt.Add(time.Second), state: state, snapshot: harness.Engine.snapshot}
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		if err := consumer.Consume(context.Background(), testRuntimeEvent(eventing.AttemptCompleted, state.Run.ID, "unknown", harness.Now())); err != nil {
			t.Fatal(err)
		}
		if !tx.advanced || tx.advancedAfter.Run.State != runtime.RunCanceled {
			t.Fatalf("advanced=%t state=%s", tx.advanced, tx.advancedAfter.Run.State)
		}
	})

	t.Run("cancellation persisted after deadline cannot override timeout", func(t *testing.T) {
		harness := newTestHarness(t, linearDocument(1))
		state := harness.Engine.SnapshotState()
		state.Run.CancelRequestedAt = state.Run.DeadlineAt.Add(time.Nanosecond)
		tx := &fakeRunTx{accepted: true, authorityNow: state.Run.DeadlineAt.Add(time.Second), state: state, snapshot: harness.Engine.snapshot}
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		if err := consumer.Consume(context.Background(), testRuntimeEvent(eventing.RunCancelRequested, state.Run.ID, state.Run.ID, harness.Now())); err != nil {
			t.Fatal(err)
		}
		if !tx.advanced || tx.advancedAfter.Run.State != runtime.RunTimedOut {
			t.Fatalf("advanced=%t state=%s", tx.advanced, tx.advancedAfter.Run.State)
		}
	})
}

func TestConsumerAdvancesCompletionRetryCancelAndDeadlineSignals(t *testing.T) {
	t.Run("attempt completed", func(t *testing.T) {
		harness := newTestHarness(t, linearDocument(1))
		attempts, _ := harness.StartReady()
		nodeID := harness.Engine.attemptNodes[attempts[0]]
		_, _ = harness.Engine.RecordAttemptResult(attempts[0], runtime.AttemptResult{State: runtime.AttemptSucceeded, Outputs: outputsFor(harness.Engine.nodeDefs[nodeID], 1)})
		tx := &fakeRunTx{accepted: true, state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot}
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		event := testRuntimeEvent(eventing.AttemptCompleted, "run", attempts[0], harness.Now())
		if err := consumer.Consume(context.Background(), event); err != nil || !tx.advanced {
			t.Fatalf("advanced=%v err=%v", tx.advanced, err)
		}
	})
	t.Run("retry due", func(t *testing.T) {
		harness := newTestHarness(t, linearDocumentWithPolicy(1, dsl.ExecutionPolicy{MaxAttempts: 2, MaxRecoveries: 1, AttemptTimeoutMS: 1000, RetryBackoff: &dsl.RetryBackoff{Kind: "fixed", DelayMS: 10}, RetryableErrorCodes: []string{"TEMP"}}))
		attempts, _ := harness.StartReady()
		nodeID := harness.Engine.attemptNodes[attempts[0]]
		_, _ = harness.Engine.RecordAttemptResult(attempts[0], runtime.AttemptResult{State: runtime.AttemptFailed, ErrorCode: "TEMP"})
		_ = harness.Engine.HandleAttemptCompleted(attempts[0], harness.Now())
		tx := &fakeRunTx{accepted: true, state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot}
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		event := testRuntimeEvent(eventing.RetryDue, "run", attempts[0], harness.Now().Add(10*time.Millisecond))
		if err := consumer.Consume(context.Background(), event); err != nil || !tx.advanced {
			t.Fatalf("node=%s advanced=%v err=%v", nodeID, tx.advanced, err)
		}
	})
	for _, signal := range []eventing.RuntimeEventType{eventing.RunCancelRequested, eventing.RunDeadlineReached} {
		t.Run(string(signal), func(t *testing.T) {
			harness := newTestHarness(t, linearDocument(1))
			tx := &fakeRunTx{accepted: true, state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot}
			consumer, _ := NewConsumer(&fakeTransactions{tx})
			at := harness.Now()
			if signal == eventing.RunDeadlineReached {
				at = harness.Engine.Run().DeadlineAt()
			}
			event := testRuntimeEvent(signal, "run", "run", at)
			if err := consumer.Consume(context.Background(), event); err != nil || !tx.advanced {
				t.Fatalf("advanced=%v err=%v", tx.advanced, err)
			}
		})
	}
}

func TestConsumerValidatesDependencyEventAndUnknownAttempt(t *testing.T) {
	if _, err := NewConsumer(nil); err == nil {
		t.Fatal("consumer without transactions accepted")
	}
	consumer, _ := NewConsumer(&fakeTransactions{&fakeRunTx{}})
	if err := consumer.Consume(context.Background(), eventing.RuntimeEvent{}); err == nil {
		t.Fatal("invalid event accepted")
	}
	harness := newTestHarness(t, linearDocument(1))
	tx := &fakeRunTx{accepted: true, state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	event := testRuntimeEvent(eventing.RetryDue, "run", "missing", harness.Now())
	if err := consumer.Consume(context.Background(), event); err != nil || tx.advanced {
		t.Fatalf("unknown retry advanced=%v err=%v", tx.advanced, err)
	}
	tx = &fakeRunTx{accepted: true, state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	unknownType := testRuntimeEvent(eventing.RuntimeEventType("unknown"), "run", "run", harness.Now())
	if err := consumer.Consume(context.Background(), unknownType); err == nil {
		t.Fatal("unknown event type accepted")
	}
}

func TestConsumerPropagatesTransactionAndStorageErrors(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	pending, _ := runtime.NewWorkflowRun(runtime.CreateRunCommand{
		RunID: "run", ProjectID: "project", WorkflowID: "workflow", Purpose: runtime.RunPurposeTest,
		Definition:    runtime.DefinitionReference{SnapshotID: "snapshot", DefinitionHash: "hash", Source: runtime.DefinitionDraftSnapshot},
		WorkflowInput: []byte(`{}`), CreatedAt: harness.Now(), DeadlineAt: harness.Now().Add(time.Hour),
	})
	event := testRuntimeEvent(eventing.RunCreated, "run", "run", harness.Now())
	for _, tx := range []*fakeRunTx{
		{accepted: true, loadRunError: errors.New("load run")},
		{accepted: true, run: pending.Snapshot(), authorityNowError: errors.New("authority clock")},
		{accepted: true, run: pending.Snapshot(), loadSnapshotError: errors.New("load snapshot")},
	} {
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		if err := consumer.Consume(context.Background(), event); err == nil {
			t.Fatal("invalid authoritative facts were accepted")
		}
	}
	tx := &fakeRunTx{accepted: true, loadStateError: errors.New("load state")}
	consumer, _ := NewConsumer(&fakeTransactions{tx})
	if err := consumer.Consume(context.Background(), testRuntimeEvent(eventing.RunDeadlineReached, "run", "run", harness.Now())); err == nil {
		t.Fatal("state load error was hidden")
	}
	tx = &fakeRunTx{accepted: true, state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot, authorityNowError: errors.New("authority clock")}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	if err := consumer.Consume(context.Background(), testRuntimeEvent(eventing.RunDeadlineReached, "run", "run", harness.Now())); err == nil {
		t.Fatal("authority clock error was hidden")
	}
	for _, tx = range []*fakeRunTx{
		{accepted: true, inboxError: errors.New("inbox")},
		{accepted: true, run: pending.Snapshot(), snapshot: harness.Engine.snapshot, initializeError: errors.New("initialize")},
	} {
		consumer, _ = NewConsumer(&fakeTransactions{tx})
		if err := consumer.Consume(context.Background(), event); err == nil {
			t.Fatal("transaction error was hidden")
		}
	}
	unsupported := harness.Engine.snapshot
	unsupported.DSL.Nodes = append([]dsl.Node(nil), harness.Engine.snapshot.DSL.Nodes...)
	unsupported.DSL.Nodes[1].Operation.Version = 99
	tx = &fakeRunTx{accepted: true, run: pending.Snapshot(), snapshot: unsupported, failInitError: errors.New("persist failure")}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	if err := consumer.Consume(context.Background(), event); err == nil {
		t.Fatal("initialization failure persistence error was hidden")
	}
	state := harness.Engine.SnapshotState()
	tx = &fakeRunTx{accepted: true, state: state, loadSnapshotError: errors.New("load snapshot")}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	deadline := testRuntimeEvent(eventing.RunDeadlineReached, "run", "run", harness.Engine.Run().DeadlineAt())
	if err := consumer.Consume(context.Background(), deadline); err == nil {
		t.Fatal("advance snapshot error was hidden")
	}
	tx = &fakeRunTx{accepted: true, state: state, snapshot: harness.Engine.snapshot, advanceError: errors.New("advance")}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	if err := consumer.Consume(context.Background(), deadline); err == nil || !tx.advanced {
		t.Fatalf("advance persistence error was hidden: advanced=%v err=%v", tx.advanced, err)
	}
}

func testRuntimeEvent(eventType eventing.RuntimeEventType, runID, aggregateID string, at time.Time) eventing.RuntimeEvent {
	aggregateType := eventing.NodeAttemptAggregate
	if eventType == eventing.RunCreated || eventType == eventing.RunCancelRequested || eventType == eventing.RunDeadlineReached {
		aggregateType = eventing.WorkflowRunAggregate
	}
	return eventing.RuntimeEvent{MessageVersion: 1, EventID: "event", ProjectID: "project", RunID: runID, AggregateType: aggregateType, AggregateID: aggregateID, EventType: eventType, OccurredAt: at, TraceID: "trace"}
}
