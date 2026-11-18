package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/platform/clock"
	"github.com/uu999/evalfrog/internal/platform/identity"
)

// Wakeup describes a re-checkable Runtime condition. It deliberately carries
// no result or state transition: the Engine reloads MySQL authority after
// consuming the event.
type Wakeup struct {
	ProjectID, RunID, AggregateID string
	EventType                     eventing.RuntimeEventType
	TraceID                       string
}

func (wakeup Wakeup) Validate() error {
	if wakeup.ProjectID == "" || wakeup.RunID == "" || wakeup.AggregateID == "" {
		return fmt.Errorf("recovery wakeup identity is required")
	}
	switch wakeup.EventType {
	case eventing.RunCreated, eventing.RunCancelRequested, eventing.RunDeadlineReached:
		if wakeup.AggregateID != wakeup.RunID {
			return fmt.Errorf("run recovery wakeup must target its run")
		}
	case eventing.AttemptCompleted, eventing.AttemptLost, eventing.RetryDue:
	default:
		return fmt.Errorf("unsupported recovery wakeup type %q", wakeup.EventType)
	}
	return nil
}

func (wakeup Wakeup) AggregateType() eventing.AggregateType {
	switch wakeup.EventType {
	case eventing.RunCreated, eventing.RunCancelRequested, eventing.RunDeadlineReached:
		return eventing.WorkflowRunAggregate
	default:
		return eventing.NodeAttemptAggregate
	}
}

type WakeupRepository interface {
	ListRetryDue(context.Context, int) ([]Wakeup, error)
	ListDeadlinesDue(context.Context, int) ([]Wakeup, error)
	ListReconciliationWakeups(context.Context, int) ([]Wakeup, error)
	EmitWakeup(context.Context, WakeupEmission) (bool, error)
}

// Observer keeps recovery independent from a concrete telemetry backend. Its
// labels must remain bounded; Run, Attempt and Project identifiers belong in
// logs/audit, never metric labels.
type Observer interface {
	ObserveRecoveryWakeup(source, eventType, outcome string)
	ObserveLeaseLost(source string)
}

// ManualReplayer is deliberately narrower than a generic Kafka replay. An
// operator asks the authority to re-check one current Run condition; arbitrary
// event payload injection is never an External API capability.
type ManualReplayer struct {
	repository WakeupRepository
	emitter    Emitter
	access     AccessControl
}

type AccessControl interface {
	Require(context.Context, string, string, access.Permission) error
}

func NewManualReplayer(repository WakeupRepository, emitter Emitter, accessControl AccessControl) (ManualReplayer, error) {
	if repository == nil || emitter.repository == nil || accessControl == nil {
		return ManualReplayer{}, fmt.Errorf("manual replay repository and access control are required")
	}
	return ManualReplayer{repository: repository, emitter: emitter, access: accessControl}, nil
}

func (replayer ManualReplayer) Replay(ctx context.Context, wakeup Wakeup, traceID, principalID string) (bool, error) {
	if err := replayer.access.Require(ctx, wakeup.ProjectID, principalID, access.PermissionProjectAdmin); err != nil {
		return false, err
	}
	return replayer.emitter.Emit(ctx, wakeup, traceID, "principal", principalID, 0, true)
}

// WakeupEmission is persisted atomically with Runtime Outbox and Audit.
// Cooldown is enforced by the repository with MySQL time, not timer
// process memory, so replicas may safely race.
type WakeupEmission struct {
	Wakeup
	EventID, AuditID, TraceID, ActorID string
	ActorType                          string
	At                                 time.Time
	Cooldown                           time.Duration
	Manual                             bool
}

type Emitter struct {
	repository WakeupRepository
	ids        identity.Generator
	clock      clock.Clock
}

func NewEmitter(repository WakeupRepository, ids identity.Generator, valueClock clock.Clock) (Emitter, error) {
	if repository == nil || ids == nil || valueClock == nil {
		return Emitter{}, fmt.Errorf("recovery emitter dependencies are required")
	}
	return Emitter{repository: repository, ids: ids, clock: valueClock}, nil
}

func NewBuiltinEmitter(repository WakeupRepository) Emitter {
	emitter, err := NewEmitter(repository, identity.UUIDv7Generator{}, clock.System{})
	if err != nil {
		panic(err)
	}
	return emitter
}

func (emitter Emitter) Emit(ctx context.Context, wakeup Wakeup, traceID, actorType, actorID string, cooldown time.Duration, manual bool) (bool, error) {
	if err := wakeup.Validate(); err != nil {
		return false, err
	}
	if traceID == "" || actorID == "" || cooldown < 0 || (actorType != "system" && actorType != "principal") {
		return false, fmt.Errorf("recovery wakeup trace, actor and cooldown are invalid")
	}
	eventID, err := emitter.ids.New()
	if err != nil {
		return false, err
	}
	auditID, err := emitter.ids.New()
	if err != nil {
		return false, err
	}
	return emitter.repository.EmitWakeup(ctx, WakeupEmission{
		Wakeup: wakeup, EventID: eventID, AuditID: auditID, TraceID: traceID,
		ActorType: actorType, ActorID: actorID, At: emitter.clock.Now().UTC(), Cooldown: cooldown, Manual: manual,
	})
}

// Scanner is a bounded, idempotent periodic source of Runtime wake-up events.
// It never updates Workflow/Node/Attempt state itself.
type Scanner struct {
	name, traceID string
	repository    WakeupRepository
	emitter       Emitter
	list          func(context.Context, int) ([]Wakeup, error)
	interval      time.Duration
	batch         int
	cooldown      time.Duration
	logger        *slog.Logger
	observer      Observer
	stop          context.CancelFunc
}

func NewRetryTimer(repository WakeupRepository, emitter Emitter, interval time.Duration, batch int, traceID string, logger *slog.Logger, observers ...Observer) (*Scanner, error) {
	return newScanner("retry-timer", repository, emitter, func(ctx context.Context, limit int) ([]Wakeup, error) { return repository.ListRetryDue(ctx, limit) }, interval, batch, traceID, interval, logger, observers...)
}

func NewDeadlineScanner(repository WakeupRepository, emitter Emitter, interval time.Duration, batch int, traceID string, logger *slog.Logger, observers ...Observer) (*Scanner, error) {
	return newScanner("deadline-scanner", repository, emitter, func(ctx context.Context, limit int) ([]Wakeup, error) { return repository.ListDeadlinesDue(ctx, limit) }, interval, batch, traceID, interval, logger, observers...)
}

func NewReconciler(repository WakeupRepository, emitter Emitter, interval time.Duration, batch int, traceID string, logger *slog.Logger, observers ...Observer) (*Scanner, error) {
	return newScanner("runtime-reconciler", repository, emitter, func(ctx context.Context, limit int) ([]Wakeup, error) {
		return repository.ListReconciliationWakeups(ctx, limit)
	}, interval, batch, traceID, interval, logger, observers...)
}

func newScanner(name string, repository WakeupRepository, emitter Emitter, list func(context.Context, int) ([]Wakeup, error), interval time.Duration, batch int, traceID string, cooldown time.Duration, logger *slog.Logger, observers ...Observer) (*Scanner, error) {
	if name == "" || repository == nil || emitter.repository == nil || list == nil || interval <= 0 || batch < 1 || traceID == "" || cooldown < 0 || logger == nil {
		return nil, fmt.Errorf("recovery scanner dependencies and settings are required")
	}
	var observer Observer
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &Scanner{name: name, traceID: traceID, repository: repository, emitter: emitter, list: list, interval: interval, batch: batch, cooldown: cooldown, logger: logger, observer: observer}, nil
}

func (scanner *Scanner) Name() string { return scanner.name }

func (scanner *Scanner) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	scanner.stop = cancel
	ticker := time.NewTicker(scanner.interval)
	defer ticker.Stop()
	for {
		if err := scanner.ScanOnce(runCtx); err != nil && runCtx.Err() == nil {
			scanner.logger.Warn("runtime recovery scan failed", "component", scanner.name, "error", err)
		}
		select {
		case <-runCtx.Done():
			return runCtx.Err()
		case <-ticker.C:
		}
	}
}

func (scanner *Scanner) ScanOnce(ctx context.Context) error {
	wakeups, err := scanner.list(ctx, scanner.batch)
	if err != nil {
		if scanner.observer != nil {
			scanner.observer.ObserveRecoveryWakeup(scanner.name, "scan", "error")
		}
		return err
	}
	for _, wakeup := range wakeups {
		traceID := wakeup.TraceID
		if traceID == "" {
			traceID = scanner.traceID
		}
		emitted, emitErr := scanner.emitter.Emit(ctx, wakeup, traceID, "system", scanner.name, scanner.cooldown, false)
		if emitErr != nil {
			err = emitErr
			if scanner.observer != nil {
				scanner.observer.ObserveRecoveryWakeup(scanner.name, string(wakeup.EventType), "error")
			}
			return err
		}
		if scanner.observer != nil {
			outcome := "stale_or_cooled_down"
			if emitted {
				outcome = "emitted"
			}
			scanner.observer.ObserveRecoveryWakeup(scanner.name, string(wakeup.EventType), outcome)
		}
		if emitted {
			scanner.logger.Info("runtime recovery wakeup emitted",
				"component", scanner.name, "event_type", wakeup.EventType,
				"project_id", wakeup.ProjectID, "run_id", wakeup.RunID,
				"aggregate_id", wakeup.AggregateID, "trace_id", traceID)
		}
	}
	return nil
}

func (scanner *Scanner) Shutdown(context.Context) error {
	if scanner.stop != nil {
		scanner.stop()
	}
	return nil
}
