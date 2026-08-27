package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	platformruntime "github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
	"github.com/uu999/evalfrog/internal/scheduling"
)

type AttemptClient interface {
	Claim(context.Context, attempt.ClaimCommand) (attempt.Lease, error)
	Heartbeat(context.Context, attempt.HeartbeatCommand) (attempt.Lease, error)
	Complete(context.Context, attempt.CompleteCommand) (bool, error)
	Load(context.Context, runtimecontext.LoadCommand) (runtimecontext.ExecutionContext, error)
}

type Executor interface {
	Coordinate() dsl.Coordinate
	Execute(context.Context, runtimecontext.ExecutionContext) platformruntime.AttemptResult
}

type Catalog struct {
	values      map[dsl.Coordinate]Executor
	coordinates []dsl.Coordinate
}

func NewCatalog(resourceClass scheduling.ResourceClass, executors ...Executor) (Catalog, error) {
	if !resourceClass.Valid() || len(executors) == 0 {
		return Catalog{}, fmt.Errorf("worker resource class and executors are required")
	}
	router := scheduling.BuiltinV1Router()
	result := Catalog{values: make(map[dsl.Coordinate]Executor, len(executors))}
	for _, executor := range executors {
		if executor == nil || executor.Coordinate().Type == "" || executor.Coordinate().Version == 0 {
			return Catalog{}, fmt.Errorf("executor coordinate is invalid")
		}
		coordinate := executor.Coordinate()
		routed, exists := router.Resolve(coordinate)
		if !exists || routed != resourceClass {
			return Catalog{}, fmt.Errorf("executor %s@%d does not belong to %s", coordinate.Type, coordinate.Version, resourceClass)
		}
		if _, duplicate := result.values[coordinate]; duplicate {
			return Catalog{}, fmt.Errorf("executor %s@%d is duplicated", coordinate.Type, coordinate.Version)
		}
		result.values[coordinate] = executor
		result.coordinates = append(result.coordinates, coordinate)
	}
	required := scheduling.RequiredCapabilities(resourceClass)
	if len(result.coordinates) != len(required) {
		return Catalog{}, fmt.Errorf("executor catalog must provide the complete %s capability set", resourceClass)
	}
	for _, coordinate := range required {
		if _, exists := result.values[coordinate]; !exists {
			return Catalog{}, fmt.Errorf("executor catalog is missing %s@%d", coordinate.Type, coordinate.Version)
		}
	}
	return result, nil
}

func (catalog Catalog) Capabilities() []dsl.Coordinate {
	return append([]dsl.Coordinate(nil), catalog.coordinates...)
}

type Settings struct {
	WorkerID, ExecutorBuild string
	ResourceClass           scheduling.ResourceClass
	Slots                   int
	LeaseDuration           time.Duration
	HeartbeatInterval       time.Duration
	ClaimTimeout            time.Duration
	CompleteTimeout         time.Duration
}

func (settings Settings) validate() error {
	if settings.WorkerID == "" || settings.ExecutorBuild == "" || !settings.ResourceClass.Valid() || settings.Slots < 1 || settings.LeaseDuration <= 0 || settings.HeartbeatInterval <= 0 || settings.HeartbeatInterval >= settings.LeaseDuration/3 || settings.ClaimTimeout <= 0 || settings.CompleteTimeout <= 0 {
		return fmt.Errorf("worker runtime identity, resource class and timing settings are invalid")
	}
	return nil
}

type Runtime struct {
	consumer   eventing.Consumer
	attempts   AttemptClient
	catalog    Catalog
	settings   Settings
	logger     *slog.Logger
	deliveryMu sync.Mutex
	stop       context.CancelFunc
}

func New(consumer eventing.Consumer, attempts AttemptClient, catalog Catalog, settings Settings, logger *slog.Logger) (*Runtime, error) {
	if consumer == nil || attempts == nil || len(catalog.values) == 0 || logger == nil {
		return nil, fmt.Errorf("worker runtime dependencies are required")
	}
	if err := settings.validate(); err != nil {
		return nil, err
	}
	return &Runtime{consumer: consumer, attempts: attempts, catalog: catalog, settings: settings, logger: logger}, nil
}

func (worker *Runtime) Name() string { return "worker-runtime" }

// Run starts exactly one receive loop per local slot. Each loop holds at most
// one execution; the Kafka adapter may batch only to the configured slot
// bound, so local outstanding work can never grow without limit.
func (worker *Runtime) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	worker.stop = cancel
	errorsFound := make(chan error, worker.settings.Slots)
	var wait sync.WaitGroup
	for slot := 0; slot < worker.settings.Slots; slot++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for runCtx.Err() == nil {
				if err := worker.receiveAndExecute(runCtx); err != nil && !errors.Is(err, context.Canceled) {
					select {
					case errorsFound <- err:
					default:
					}
					return
				}
			}
		}()
	}
	select {
	case <-runCtx.Done():
	case err := <-errorsFound:
		cancel()
		wait.Wait()
		return err
	}
	wait.Wait()
	return runCtx.Err()
}

func (worker *Runtime) Shutdown(context.Context) error {
	if worker.stop != nil {
		worker.stop()
	}
	return nil
}

func (worker *Runtime) receiveAndExecute(ctx context.Context) error {
	// A single Kafka consumer may feed many executor slots. Poll, Claim and ACK
	// stay serialized so a later offset can never be committed past a task
	// whose authoritative Claim has not succeeded.
	worker.deliveryMu.Lock()
	locked := true
	defer func() {
		if locked {
			worker.deliveryMu.Unlock()
		}
	}()
	delivery, err := worker.consumer.Receive(ctx)
	if err != nil {
		return err
	}
	task, err := eventing.ParseTaskMessage(delivery.Payload())
	if err != nil {
		identity, identityErr := eventing.ParseTaskIdentity(delivery.Payload())
		if identityErr != nil {
			return delivery.DeadLetter(ctx, "INVALID_TASK_MESSAGE")
		}
		return worker.settlePoison(ctx, delivery, identity, err)
	}
	if task.ResourceClass != worker.settings.ResourceClass {
		return delivery.DeadLetter(ctx, "RESOURCE_CLASS_MISMATCH")
	}
	claimCtx, cancelClaim := context.WithTimeout(ctx, worker.settings.ClaimTimeout)
	lease, err := worker.attempts.Claim(claimCtx, attempt.ClaimCommand{
		ProjectID: task.ProjectID, RunID: task.RunID, AttemptID: task.AttemptID,
		AttemptSequence: task.AttemptSequence, WorkerID: worker.settings.WorkerID,
		ExecutorBuild: worker.settings.ExecutorBuild, ResourceClass: task.ResourceClass,
		Capabilities: worker.catalog.Capabilities(), LeaseDuration: worker.settings.LeaseDuration,
	})
	cancelClaim()
	if err != nil {
		if errors.Is(err, attempt.ErrNotFound) || errors.Is(err, attempt.ErrNotCurrent) || errors.Is(err, attempt.ErrStateConflict) || errors.Is(err, attempt.ErrLeaseMismatch) {
			return delivery.Ack(ctx)
		}
		if errors.Is(err, attempt.ErrCapabilityMismatch) {
			return delivery.DeadLetter(ctx, "CAPABILITY_MISMATCH")
		}
		delivery.Nack()
		return err
	}
	// ACK follows durable Claim, not executor completion.
	if err = delivery.Ack(ctx); err != nil {
		return err
	}
	worker.deliveryMu.Unlock()
	locked = false
	// Claim acknowledges the durable responsibility transfer. If a cancellation
	// or deadline was observed while Claim acquired the lease, settle a safe
	// canceled Attempt immediately and never expose execution context or invoke
	// user code.
	if lease.CancelRequested {
		return worker.complete(ctx, task, lease, platformruntime.AttemptResult{State: platformruntime.AttemptCanceled, ErrorCode: "RUN_TERMINATING", Message: "run cancellation or deadline was requested before execution"})
	}
	executionContext, err := worker.attempts.Load(ctx, runtimecontext.LoadCommand{ProjectID: task.ProjectID, RunID: task.RunID, AttemptID: task.AttemptID, AttemptSequence: task.AttemptSequence, LeaseToken: lease.Token, FencingToken: lease.FencingToken})
	if err != nil {
		// Context assembly is infrastructure work. It must not turn into a node
		// failure or consume the workflow's business retry budget. Keep the
		// claimed attempt Running; the lease reaper will fence it as Lost and
		// drive an infrastructure recovery attempt.
		worker.logger.Warn("execution context unavailable; deferred to lease recovery", "component", worker.Name(),
			"project_id", task.ProjectID, "run_id", task.RunID, "attempt_id", task.AttemptID, "trace_id", task.TraceID, "error", err)
		return nil
	}
	executor, exists := worker.catalog.values[executionContext.Operation.Coordinate()]
	if !exists {
		return worker.completeFailure(ctx, task, lease, "EXECUTOR_NOT_AVAILABLE", "claimed operation has no local executor")
	}
	timeout := time.Duration(executionContext.ExecutionPolicy.AttemptTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = worker.settings.LeaseDuration
	}
	execCtx, cancelExec := context.WithTimeout(ctx, timeout)
	defer cancelExec()
	heartbeatStopped := make(chan struct{})
	heartbeatFailed := make(chan error, 1)
	cancelRequested := make(chan struct{})
	go worker.heartbeat(execCtx, task, lease, heartbeatStopped, heartbeatFailed, cancelRequested, cancelExec)
	result := executor.Execute(execCtx, executionContext)
	close(heartbeatStopped)
	select {
	case heartbeatErr := <-heartbeatFailed:
		worker.logger.Warn("attempt heartbeat failed; stale result discarded", "component", worker.Name(),
			"project_id", task.ProjectID, "run_id", task.RunID, "attempt_id", task.AttemptID, "trace_id", task.TraceID, "error", heartbeatErr)
		return nil
	default:
	}
	select {
	case <-cancelRequested:
		result = platformruntime.AttemptResult{State: platformruntime.AttemptCanceled, ErrorCode: "RUN_TERMINATING", Message: "run cancellation or deadline was requested during execution"}
	default:
	}
	if errors.Is(execCtx.Err(), context.DeadlineExceeded) && !result.State.Terminal() {
		result = platformruntime.AttemptResult{State: platformruntime.AttemptTimedOut, ErrorCode: "NODE_TIMEOUT", Message: "executor exceeded attempt timeout"}
	}
	if !result.State.Terminal() || result.State == platformruntime.AttemptLost {
		result = platformruntime.AttemptResult{State: platformruntime.AttemptFailed, ErrorCode: "EXECUTOR_PROTOCOL_ERROR", Message: "executor returned a non-terminal result"}
	}
	if err = worker.complete(ctx, task, lease, result); err != nil {
		worker.logger.Warn("attempt completion deferred to lease recovery", "component", worker.Name(),
			"project_id", task.ProjectID, "run_id", task.RunID, "attempt_id", task.AttemptID, "trace_id", task.TraceID, "error", err)
	}
	return nil
}

func (worker *Runtime) settlePoison(ctx context.Context, delivery eventing.Delivery, task eventing.TaskMessage, contractErr error) error {
	if task.ResourceClass != worker.settings.ResourceClass {
		return delivery.DeadLetter(ctx, "RESOURCE_CLASS_MISMATCH")
	}
	claimCtx, cancelClaim := context.WithTimeout(ctx, worker.settings.ClaimTimeout)
	lease, err := worker.attempts.Claim(claimCtx, attempt.ClaimCommand{ProjectID: task.ProjectID, RunID: task.RunID, AttemptID: task.AttemptID, AttemptSequence: task.AttemptSequence, WorkerID: worker.settings.WorkerID, ExecutorBuild: worker.settings.ExecutorBuild, ResourceClass: task.ResourceClass, Capabilities: worker.catalog.Capabilities(), LeaseDuration: worker.settings.LeaseDuration})
	cancelClaim()
	if err != nil {
		if errors.Is(err, attempt.ErrNotFound) || errors.Is(err, attempt.ErrNotCurrent) || errors.Is(err, attempt.ErrStateConflict) || errors.Is(err, attempt.ErrLeaseMismatch) {
			return delivery.Ack(ctx)
		}
		if errors.Is(err, attempt.ErrCapabilityMismatch) {
			return delivery.DeadLetter(ctx, "CAPABILITY_MISMATCH")
		}
		delivery.Nack()
		return err
	}
	if err = delivery.Ack(ctx); err != nil {
		return err
	}
	if err = worker.completeFailure(ctx, task, lease, "TASK_MESSAGE_INVALID", contractErr.Error()); err != nil {
		worker.logger.Warn("poison task completion deferred to lease recovery", "component", worker.Name(),
			"project_id", task.ProjectID, "run_id", task.RunID, "attempt_id", task.AttemptID, "trace_id", task.TraceID, "error", err)
	}
	return nil
}

func (worker *Runtime) heartbeat(ctx context.Context, task eventing.TaskMessage, lease attempt.Lease, stopped <-chan struct{}, failed chan<- error, cancellation chan<- struct{}, cancel context.CancelFunc) {
	ticker := time.NewTicker(worker.settings.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stopped:
			return
		case <-ticker.C:
			updated, err := worker.attempts.Heartbeat(ctx, attempt.HeartbeatCommand{ProjectID: task.ProjectID, RunID: task.RunID, AttemptID: task.AttemptID, AttemptSequence: task.AttemptSequence, LeaseToken: lease.Token, FencingToken: lease.FencingToken, ExtendBy: worker.settings.LeaseDuration})
			if err != nil {
				failed <- err
				cancel()
				return
			}
			if updated.CancelRequested {
				close(cancellation)
				cancel()
				return
			}
		}
	}
}

func (worker *Runtime) completeFailure(ctx context.Context, task eventing.TaskMessage, lease attempt.Lease, code, message string) error {
	return worker.complete(ctx, task, lease, platformruntime.AttemptResult{State: platformruntime.AttemptFailed, ErrorCode: code, Message: message})
}

func (worker *Runtime) complete(ctx context.Context, task eventing.TaskMessage, lease attempt.Lease, result platformruntime.AttemptResult) error {
	completeCtx, cancel := context.WithTimeout(ctx, worker.settings.CompleteTimeout)
	defer cancel()
	_, err := worker.attempts.Complete(completeCtx, attempt.CompleteCommand{ProjectID: task.ProjectID, RunID: task.RunID, AttemptID: task.AttemptID, AttemptSequence: task.AttemptSequence, LeaseToken: lease.Token, FencingToken: lease.FencingToken, Result: result, TraceID: task.TraceID})
	if errors.Is(err, attempt.ErrLeaseMismatch) || errors.Is(err, attempt.ErrNotCurrent) || errors.Is(err, attempt.ErrStateConflict) {
		return nil
	}
	return err
}

// EchoTestExecutor is enabled only in local/test profiles. It proves the M7
// transport protocol without pretending to be an M8/M9 production executor.
type EchoTestExecutor struct{ Operation dsl.Coordinate }

func (executor EchoTestExecutor) Coordinate() dsl.Coordinate { return executor.Operation }
func (executor EchoTestExecutor) Execute(_ context.Context, value runtimecontext.ExecutionContext) platformruntime.AttemptResult {
	outputs := make(map[string]json.RawMessage, len(value.OutputContract))
	for name, dataType := range value.OutputContract {
		if input, exists := value.Inputs[string(name)]; exists {
			outputs[string(name)] = input
		} else {
			outputs[string(name)] = testValue(dataType)
		}
	}
	return platformruntime.AttemptResult{State: platformruntime.AttemptSucceeded, Outputs: outputs}
}

func testValue(dataType dsl.DataType) json.RawMessage {
	switch dataType {
	case dsl.TypeString:
		return json.RawMessage(`""`)
	case dsl.TypeInteger, dsl.TypeNumber:
		return json.RawMessage(`0`)
	case dsl.TypeBoolean:
		return json.RawMessage(`false`)
	case dsl.TypeArray:
		return json.RawMessage(`[]`)
	default:
		return json.RawMessage(`{}`)
	}
}
