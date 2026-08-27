package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	platformruntime "github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
	"github.com/uu999/evalfrog/internal/scheduling"
)

func TestClaimThenAckDoesNotWaitForExecutionAndNoSlotPrefetch(t *testing.T) {
	executor := &blockingExecutor{started: make(chan struct{}), release: make(chan struct{})}
	worker, consumer, attempts := runtimeFixture(t, executor)
	if worker.Name() != "worker-runtime" {
		t.Fatal("runtime name missing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("executor did not start")
	}
	if consumer.acks.Load() != 1 || consumer.receives.Load() != 1 {
		t.Fatalf("acks=%d receives=%d", consumer.acks.Load(), consumer.receives.Load())
	}
	time.Sleep(20 * time.Millisecond)
	if consumer.receives.Load() != 1 {
		t.Fatalf("worker prefetched without a free slot: %d", consumer.receives.Load())
	}
	close(executor.release)
	select {
	case <-attempts.completed:
	case <-time.After(time.Second):
		t.Fatal("attempt was not completed")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runtime did not stop")
	}
	_ = worker.Shutdown(context.Background())
}

func TestDuplicateStaleAndPoisonDeliveriesAreContained(t *testing.T) {
	executor := EchoTestExecutor{Operation: dsl.Coordinate{Type: "task.python", Version: 1}}
	worker, consumer, attempts := runtimeFixture(t, executor)
	attempts.claimErr = attempt.ErrStateConflict
	if err := worker.receiveAndExecute(context.Background()); err != nil || consumer.acks.Load() != 1 {
		t.Fatalf("duplicate err=%v ack=%d", err, consumer.acks.Load())
	}
	consumer.payload = []byte(`{"message_version":99,"attempt_id":"a"}`)
	consumer.receives.Store(0)
	attempts.claimErr = nil
	if err := worker.receiveAndExecute(context.Background()); err != nil || consumer.dlqs.Load() != 1 {
		t.Fatalf("poison err=%v dlq=%d", err, consumer.dlqs.Load())
	}
	worker2, consumer2, attempts2 := runtimeFixture(t, executor)
	var recognizable map[string]any
	_ = json.Unmarshal(consumer2.payload, &recognizable)
	recognizable["message_version"] = 2
	consumer2.payload, _ = json.Marshal(recognizable)
	if err := worker2.receiveAndExecute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if consumer2.acks.Load() != 1 || attempts2.completes.Load() != 1 {
		t.Fatal("recognizable poison task was not settled")
	}
}

func TestWrongResourceClassAndCapabilityMismatchGoToDeadLetter(t *testing.T) {
	executor := EchoTestExecutor{Operation: dsl.Coordinate{Type: "task.python", Version: 1}}
	worker, consumer, _ := runtimeFixture(t, executor)
	var message eventing.TaskMessage
	_ = json.Unmarshal(consumer.payload, &message)
	message.ResourceClass = scheduling.ResourceBuiltin
	consumer.payload, _ = json.Marshal(message)
	if err := worker.receiveAndExecute(context.Background()); err != nil || consumer.dlqs.Load() != 1 {
		t.Fatalf("wrong class err=%v dlq=%d", err, consumer.dlqs.Load())
	}

	worker, consumer, attempts := runtimeFixture(t, executor)
	attempts.claimErr = attempt.ErrCapabilityMismatch
	if err := worker.receiveAndExecute(context.Background()); err != nil || consumer.dlqs.Load() != 1 {
		t.Fatalf("capability err=%v dlq=%d", err, consumer.dlqs.Load())
	}
}

func TestContextFailureDefersToLeaseRecovery(t *testing.T) {
	worker, _, attempts := runtimeFixture(t, EchoTestExecutor{Operation: dsl.Coordinate{Type: "task.python", Version: 1}})
	attempts.loadErr = errors.New("postgres down")
	if err := worker.receiveAndExecute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts.completes.Load() != 0 {
		t.Fatal("infrastructure context failure consumed business completion")
	}
}

func TestClaimFailureIsNackedAndAckFailurePreventsExecution(t *testing.T) {
	worker, consumer, attempts := runtimeFixture(t, EchoTestExecutor{Operation: dsl.Coordinate{Type: "task.python", Version: 1}})
	attempts.claimErr = errors.New("control plane unavailable")
	if err := worker.receiveAndExecute(context.Background()); err == nil || consumer.nacks.Load() != 1 || consumer.acks.Load() != 0 {
		t.Fatalf("claim err=%v nacks=%d acks=%d", err, consumer.nacks.Load(), consumer.acks.Load())
	}

	worker, consumer, attempts = runtimeFixture(t, EchoTestExecutor{Operation: dsl.Coordinate{Type: "task.python", Version: 1}})
	consumer.ackErr = errors.New("offset commit failed")
	if err := worker.receiveAndExecute(context.Background()); err == nil || attempts.loads.Load() != 0 || attempts.completes.Load() != 0 {
		t.Fatalf("ack err=%v loads=%d completes=%d", err, attempts.loads.Load(), attempts.completes.Load())
	}
}

func TestExecutorProtocolTimeoutAndMissingExecutorAreSettled(t *testing.T) {
	tests := []struct {
		name     string
		executor Executor
		mutate   func(*fakeAttempts)
	}{
		{name: "non-terminal", executor: resultExecutor{coordinate: dsl.Coordinate{Type: "task.python", Version: 1}, result: platformruntime.AttemptResult{State: platformruntime.AttemptRunning}}},
		{name: "timeout", executor: resultExecutor{coordinate: dsl.Coordinate{Type: "task.python", Version: 1}, waitForCancellation: true}},
		{name: "missing executor", executor: EchoTestExecutor{Operation: dsl.Coordinate{Type: "task.python", Version: 1}}, mutate: func(value *fakeAttempts) {
			value.operation = dsl.Coordinate{Type: "task.http", Version: 1}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			worker, _, attempts := runtimeFixture(t, test.executor)
			if test.mutate != nil {
				test.mutate(attempts)
			}
			if test.name == "timeout" {
				attempts.attemptTimeout = time.Millisecond
			}
			if err := worker.receiveAndExecute(context.Background()); err != nil || attempts.completes.Load() != 1 {
				t.Fatalf("err=%v completes=%d", err, attempts.completes.Load())
			}
		})
	}
}

func TestExecutorTimeoutResultKeepsSandboxErrorCode(t *testing.T) {
	executor := resultExecutor{coordinate: dsl.Coordinate{Type: "task.python", Version: 1}, waitForCancellation: true, result: platformruntime.AttemptResult{State: platformruntime.AttemptTimedOut, ErrorCode: "SANDBOX_EXECUTION_TIMEOUT", Message: "sandbox timeout", DSLField: "operation.config.source_code"}}
	worker, _, attempts := runtimeFixture(t, executor)
	attempts.attemptTimeout = time.Millisecond
	if err := worker.receiveAndExecute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if attempts.complete.Result.ErrorCode != "SANDBOX_EXECUTION_TIMEOUT" || attempts.complete.Result.DSLField != "operation.config.source_code" {
		t.Fatalf("result=%+v", attempts.complete.Result)
	}
}

func TestStaleCompletionAndHeartbeatFailureCannotBecomeEffective(t *testing.T) {
	executor := &blockingExecutor{started: make(chan struct{}), release: make(chan struct{})}
	worker, consumer, attempts := runtimeFixture(t, executor)
	worker.settings.HeartbeatInterval = 5 * time.Millisecond
	attempts.heartbeatErr = attempt.ErrLeaseMismatch
	done := make(chan error, 1)
	go func() { done <- worker.receiveAndExecute(context.Background()) }()
	<-executor.started
	<-attempts.heartbeatCalled
	close(executor.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if attempts.completes.Load() != 0 || consumer.acks.Load() != 1 {
		t.Fatalf("stale result completed=%d ack=%d", attempts.completes.Load(), consumer.acks.Load())
	}

	worker2, _, attempts2 := runtimeFixture(t, EchoTestExecutor{Operation: dsl.Coordinate{Type: "task.python", Version: 1}})
	attempts2.completeErr = attempt.ErrNotCurrent
	if err := worker2.receiveAndExecute(context.Background()); err != nil {
		t.Fatalf("stale completion killed worker: %v", err)
	}
	if attempts2.completes.Load() != 1 {
		t.Fatal("completion was not attempted")
	}
}

func TestCancellationStopsClaimedAttemptAndPropagatesHeartbeatCancellation(t *testing.T) {
	t.Run("already cancelled when claimed", func(t *testing.T) {
		worker, consumer, attempts := runtimeFixture(t, EchoTestExecutor{Operation: dsl.Coordinate{Type: "task.python", Version: 1}})
		attempts.claimLease = attempt.Lease{Token: "lease", Owner: "worker", FencingToken: 1, ExpiresAt: time.Now().Add(time.Minute), CancelRequested: true}
		if err := worker.receiveAndExecute(context.Background()); err != nil {
			t.Fatal(err)
		}
		if consumer.acks.Load() != 1 || attempts.loads.Load() != 0 || attempts.completes.Load() != 1 || attempts.complete.Result.State != platformruntime.AttemptCanceled {
			t.Fatalf("acks=%d loads=%d completed=%d result=%+v", consumer.acks.Load(), attempts.loads.Load(), attempts.completes.Load(), attempts.complete.Result)
		}
	})
	t.Run("heartbeat cancellation interrupts executor", func(t *testing.T) {
		executor := resultExecutor{coordinate: dsl.Coordinate{Type: "task.python", Version: 1}, waitForCancellation: true}
		worker, _, attempts := runtimeFixture(t, executor)
		worker.settings.HeartbeatInterval = time.Millisecond
		attempts.heartbeatLease.CancelRequested = true
		if err := worker.receiveAndExecute(context.Background()); err != nil {
			t.Fatal(err)
		}
		if attempts.completes.Load() != 1 || attempts.complete.Result.State != platformruntime.AttemptCanceled {
			t.Fatalf("completed=%d result=%+v", attempts.completes.Load(), attempts.complete.Result)
		}
	})
}

func TestCatalogResourceClassAndSettingsValidation(t *testing.T) {
	python := EchoTestExecutor{Operation: dsl.Coordinate{Type: "task.python", Version: 1}}
	if _, err := NewCatalog(scheduling.ResourceBuiltin, python); err == nil {
		t.Fatal("wrong resource class accepted")
	}
	if _, err := NewCatalog(scheduling.ResourceSandbox, python, python); err == nil {
		t.Fatal("duplicate executor accepted")
	}
	catalog, err := NewCatalog(scheduling.ResourceSandbox, python)
	if err != nil || len(catalog.Capabilities()) != 1 {
		t.Fatal(err)
	}
	if _, err = New(nil, nil, catalog, Settings{}, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("invalid runtime accepted")
	}
}

type fakeDelivery struct{ owner *fakeConsumer }

func (value *fakeDelivery) Topic() string   { return "tasks" }
func (value *fakeDelivery) Key() string     { return "attempt" }
func (value *fakeDelivery) Payload() []byte { return append([]byte(nil), value.owner.payload...) }
func (value *fakeDelivery) Ack(context.Context) error {
	value.owner.acks.Add(1)
	return value.owner.ackErr
}
func (value *fakeDelivery) Nack() { value.owner.nacks.Add(1) }
func (value *fakeDelivery) DeadLetter(context.Context, string) error {
	value.owner.dlqs.Add(1)
	return nil
}

type fakeConsumer struct {
	payload                     []byte
	receives, acks, nacks, dlqs atomic.Int32
	ackErr                      error
	mu                          sync.Mutex
}

func (value *fakeConsumer) Receive(ctx context.Context) (eventing.Delivery, error) {
	value.receives.Add(1)
	if value.receives.Load() > 1 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &fakeDelivery{owner: value}, nil
}

type fakeAttempts struct {
	claimErr, heartbeatErr, completeErr error
	loadErr                             error
	completes                           atomic.Int32
	loads                               atomic.Int32
	completed                           chan struct{}
	heartbeatCalled                     chan struct{}
	operation                           dsl.Coordinate
	attemptTimeout                      time.Duration
	complete                            attempt.CompleteCommand
	claimLease, heartbeatLease          attempt.Lease
}

func (value *fakeAttempts) Claim(context.Context, attempt.ClaimCommand) (attempt.Lease, error) {
	if value.claimErr != nil {
		return attempt.Lease{}, value.claimErr
	}
	lease := value.claimLease
	if lease.Token == "" {
		lease = attempt.Lease{Token: "lease", Owner: "worker", FencingToken: 1, ExpiresAt: time.Now().Add(time.Minute)}
	}
	return lease, nil
}
func (value *fakeAttempts) Heartbeat(context.Context, attempt.HeartbeatCommand) (attempt.Lease, error) {
	select {
	case value.heartbeatCalled <- struct{}{}:
	default:
	}
	return value.heartbeatLease, value.heartbeatErr
}
func (value *fakeAttempts) Complete(_ context.Context, command attempt.CompleteCommand) (bool, error) {
	value.complete = command
	value.completes.Add(1)
	select {
	case value.completed <- struct{}{}:
	default:
	}
	return true, value.completeErr
}
func (value *fakeAttempts) Load(context.Context, runtimecontext.LoadCommand) (runtimecontext.ExecutionContext, error) {
	value.loads.Add(1)
	if value.loadErr != nil {
		return runtimecontext.ExecutionContext{}, value.loadErr
	}
	coordinate := value.operation
	if coordinate.Type == "" {
		coordinate = dsl.Coordinate{Type: "task.python", Version: 1}
	}
	timeout := value.attemptTimeout
	if timeout == 0 {
		timeout = time.Second
	}
	return runtimecontext.ExecutionContext{Operation: dsl.Operation{Type: coordinate.Type, Version: coordinate.Version}, ExecutionPolicy: dsl.ExecutionPolicy{AttemptTimeoutMS: uint64(timeout.Milliseconds())}, OutputContract: map[dsl.PortName]dsl.DataType{"result": dsl.TypeInteger}, Inputs: map[string]json.RawMessage{"result": json.RawMessage(`7`)}}, nil
}

type resultExecutor struct {
	coordinate          dsl.Coordinate
	result              platformruntime.AttemptResult
	waitForCancellation bool
}

func (value resultExecutor) Coordinate() dsl.Coordinate { return value.coordinate }
func (value resultExecutor) Execute(ctx context.Context, _ runtimecontext.ExecutionContext) platformruntime.AttemptResult {
	if value.waitForCancellation {
		<-ctx.Done()
	}
	return value.result
}

type blockingExecutor struct {
	started, release chan struct{}
	once             sync.Once
}

func (value *blockingExecutor) Coordinate() dsl.Coordinate {
	return dsl.Coordinate{Type: "task.python", Version: 1}
}
func (value *blockingExecutor) Execute(ctx context.Context, _ runtimecontext.ExecutionContext) platformruntime.AttemptResult {
	value.once.Do(func() { close(value.started) })
	select {
	case <-value.release:
	case <-ctx.Done():
	}
	return platformruntime.AttemptResult{State: platformruntime.AttemptSucceeded, Outputs: map[string]json.RawMessage{"result": json.RawMessage(`7`)}}
}

func runtimeFixture(t *testing.T, executor Executor) (*Runtime, *fakeConsumer, *fakeAttempts) {
	t.Helper()
	message := eventing.TaskMessage{MessageVersion: 1, TaskID: "task", ProjectID: "project", RunID: "run", NodeRunID: "node-run", ExecutionNodeID: "code", AttemptID: "attempt", AttemptSequence: 1, ResourceClass: scheduling.ResourceSandbox, OccurredAt: time.Now().UTC(), TraceID: "trace"}
	payload, _ := message.MarshalJSONMessage()
	consumer := &fakeConsumer{payload: payload}
	attempts := &fakeAttempts{completed: make(chan struct{}, 1), heartbeatCalled: make(chan struct{}, 1)}
	catalog, err := NewCatalog(scheduling.ResourceSandbox, executor)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := New(consumer, attempts, catalog, Settings{WorkerID: "worker", ExecutorBuild: "test", ResourceClass: scheduling.ResourceSandbox, Slots: 1, LeaseDuration: 300 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, ClaimTimeout: time.Second, CompleteTimeout: time.Second}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return worker, consumer, attempts
}

var _ = errors.New
