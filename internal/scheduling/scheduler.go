package scheduling

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/uu999/evalfrog/internal/platform/clock"
	"github.com/uu999/evalfrog/internal/platform/identity"
)

type Scheduler struct {
	authority Authority
	store     CoordinationStore
	ids       identity.Generator
	clock     clock.Clock
	owner     string
	settings  Settings
	observer  Observer
}

type Observer interface {
	ObserveReadyToQueued(time.Duration)
	ObserveSchedulingRedisRebuild(outcome string)
}

type TopicObserver interface {
	ObserveTopicQueue(resourceClass string, window, occupancy int, ewma float64)
}

func New(authority Authority, store CoordinationStore, ids identity.Generator, valueClock clock.Clock, owner string, settings Settings, observers ...Observer) (*Scheduler, error) {
	if authority == nil || store == nil || ids == nil || valueClock == nil || owner == "" {
		return nil, fmt.Errorf("scheduler dependencies and owner are required")
	}
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	var observer Observer
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &Scheduler{authority: authority, store: store, ids: ids, clock: valueClock, owner: owner, settings: settings, observer: observer}, nil
}

// Reconcile repairs Redis from PostgreSQL. It is deliberately independent of
// the continuous admission loop and is expected to run much less frequently.
func (scheduler *Scheduler) Reconcile(ctx context.Context) (ReconcileResult, error) {
	growthAllowed, err := scheduler.store.RefreshMemoryPressure(ctx, scheduler.settings.Memory)
	if err != nil {
		return ReconcileResult{}, err
	}
	if !growthAllowed {
		return ReconcileResult{}, ErrMemoryPressure
	}
	lease, err := scheduler.store.AcquireReconcileLease(ctx, scheduler.owner, scheduler.settings.ReconcileLease)
	if err != nil {
		return ReconcileResult{}, err
	}
	reservations, err := scheduler.store.ListReservations(ctx)
	if err != nil {
		return ReconcileResult{}, err
	}
	snapshot, err := scheduler.authority.LoadSchedulingSnapshot(ctx, scheduler.settings.CandidateBatch)
	if err != nil {
		return ReconcileResult{}, err
	}
	reservedNodes := make(map[string]struct{}, len(reservations))
	for _, reservation := range reservations {
		reservedNodes[reservation.Candidate.NodeRunID] = struct{}{}
	}
	candidates := snapshot.Candidates[:0]
	for _, candidate := range snapshot.Candidates {
		if _, reserved := reservedNodes[candidate.NodeRunID]; reserved {
			continue
		}
		candidate = candidate.Normalized()
		if err = candidate.Validate(); err != nil {
			return ReconcileResult{}, err
		}
		candidates = append(candidates, candidate)
	}
	snapshot.Candidates = candidates
	result, err := scheduler.store.Rebuild(ctx, lease, snapshot, scheduler.settings.TopicWindow)
	if scheduler.observer != nil {
		outcome := "success"
		if err != nil {
			outcome = "failure"
		}
		scheduler.observer.ObserveSchedulingRedisRebuild(outcome)
	}
	if err == nil {
		scheduler.observeTopics(result.Topics)
	}
	return result, err
}

// Rebalance remains as a narrow compatibility name for callers from the M6
// boundary. Its behavior is now reconciliation only; it neither calculates
// credits nor performs admission.
func (scheduler *Scheduler) Rebalance(ctx context.Context) (ReconcileResult, error) {
	return scheduler.Reconcile(ctx)
}

func (scheduler *Scheduler) Calibrate(ctx context.Context) (map[ResourceClass]TopicState, error) {
	lease, err := scheduler.store.AcquireReconcileLease(ctx, scheduler.owner, scheduler.settings.ReconcileLease)
	if err != nil {
		return nil, err
	}
	states, err := scheduler.store.CalibrateTopicWindows(ctx, lease, scheduler.settings.TopicWindow)
	if err == nil {
		scheduler.observeTopics(states)
	}
	return states, err
}

func (scheduler *Scheduler) observeTopics(states map[ResourceClass]TopicState) {
	observer, ok := scheduler.observer.(TopicObserver)
	if !ok {
		return
	}
	for class, state := range states {
		observer.ObserveTopicQueue(string(class), state.Window, state.Occupancy, state.EWMA)
	}
}

// AdmitClass reserves in strict Redis order, then performs PostgreSQL CAS in a
// bounded worker set. Reservation already accounts for Project Load and Topic
// occupancy, so concurrent Scheduler replicas cannot over-admit.
func (scheduler *Scheduler) AdmitClass(ctx context.Context, class ResourceClass, limit int, traceID string) ([]Task, error) {
	if !class.Valid() || limit <= 0 || limit > scheduler.settings.CandidateBatch || traceID == "" {
		return nil, fmt.Errorf("admission class, bounded limit and trace are required")
	}
	reservations := make([]Reservation, 0, limit)
	for len(reservations) < limit {
		attemptID, err := scheduler.ids.New()
		if err != nil {
			scheduler.abortReservations(ctx, reservations, true)
			return nil, err
		}
		reservation, exists, err := scheduler.store.ReserveNext(ctx, class, attemptID, scheduler.settings.ReservationTTL)
		if err != nil {
			scheduler.abortReservations(ctx, reservations, true)
			return nil, err
		}
		if !exists {
			break
		}
		reservations = append(reservations, reservation)
	}
	if len(reservations) == 0 {
		return nil, nil
	}
	type result struct {
		task Task
		err  error
	}
	jobs := make(chan Reservation)
	results := make(chan result, len(reservations))
	workers := min(len(reservations), scheduler.settings.AdmissionConcurrency)
	var wait sync.WaitGroup
	for index := 0; index < workers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for reservation := range jobs {
				task, restore, dispatchErr := scheduler.dispatchReservation(ctx, reservation, traceID)
				if dispatchErr != nil {
					// Failure before Dispatch may restore the candidate. Once Dispatch
					// starts, its commit outcome can be ambiguous, so reconciliation
					// repairs the candidate without risking a duplicate Attempt.
					_ = scheduler.store.AbortReservation(ctx, reservation, restore)
				}
				results <- result{task: task, err: dispatchErr}
			}
		}()
	}
	for _, reservation := range reservations {
		select {
		case jobs <- reservation:
		case <-ctx.Done():
			close(jobs)
			wait.Wait()
			close(results)
			return nil, ctx.Err()
		}
	}
	close(jobs)
	wait.Wait()
	close(results)
	tasks := make([]Task, 0, len(reservations))
	var firstErr error
	for value := range results {
		if value.err != nil {
			if firstErr == nil {
				firstErr = value.err
			}
			continue
		}
		tasks = append(tasks, value.task)
	}
	return tasks, firstErr
}

func (scheduler *Scheduler) abortReservations(ctx context.Context, reservations []Reservation, restore bool) {
	for index := len(reservations) - 1; index >= 0; index-- {
		_ = scheduler.store.AbortReservation(ctx, reservations[index], restore)
	}
}

func (scheduler *Scheduler) dispatchReservation(ctx context.Context, reservation Reservation, traceID string) (Task, bool, error) {
	taskID, err := scheduler.ids.New()
	if err != nil {
		return Task{}, true, err
	}
	task, err := scheduler.authority.DispatchReady(ctx, DispatchCommand{
		Candidate: reservation.Candidate, AttemptID: reservation.AttemptID, TaskID: taskID,
		TraceID: traceID, Now: scheduler.clock.Now().UTC(),
	})
	if err != nil {
		return Task{}, false, err
	}
	if scheduler.observer != nil {
		scheduler.observer.ObserveReadyToQueued(scheduler.clock.Now().UTC().Sub(reservation.Candidate.ReadyAt))
	}
	// PostgreSQL already contains the authoritative queued Attempt. A missed
	// Redis confirmation is conservative and repaired by reconciliation.
	_ = scheduler.store.ConfirmReservation(ctx, reservation)
	return task, false, nil
}

type Service struct {
	scheduler *Scheduler
	traceID   string
	logger    *slog.Logger
	stop      context.CancelFunc
}

func NewService(scheduler *Scheduler, traceID string, logger *slog.Logger) (*Service, error) {
	if scheduler == nil || traceID == "" || logger == nil {
		return nil, fmt.Errorf("scheduler service, trace and logger are required")
	}
	return &Service{scheduler: scheduler, traceID: traceID, logger: logger}, nil
}

func (service *Service) Name() string { return "fifo-topic-window-scheduler" }

func (service *Service) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	service.stop = cancel
	lastReconcile := time.Time{}
	lastCalibration := time.Time{}
	idle := service.scheduler.settings.IdlePoll
	for {
		now := service.scheduler.clock.Now()
		if lastReconcile.IsZero() || now.Sub(lastReconcile) >= service.scheduler.settings.ReadyReconcileInterval {
			_, err := service.scheduler.Reconcile(runCtx)
			if err == nil || errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrMemoryPressure) {
				lastReconcile = now
			} else if runCtx.Err() == nil {
				service.logger.Warn("scheduler authority reconciliation failed closed", "error", err)
			}
		}
		if lastCalibration.IsZero() || now.Sub(lastCalibration) >= service.scheduler.settings.CapacityCalibrationInterval {
			_, err := service.scheduler.Calibrate(runCtx)
			if err == nil || errors.Is(err, ErrLeaseLost) {
				lastCalibration = now
			} else if runCtx.Err() == nil {
				service.logger.Warn("scheduler topic window calibration failed", "error", err)
			}
		}
		admitted := 0
		for _, class := range ResourceClasses() {
			tasks, err := service.scheduler.AdmitClass(runCtx, class, service.scheduler.settings.CandidateBatch, service.traceID)
			admitted += len(tasks)
			if err != nil && !errors.Is(err, ErrAdmissionPaused) && runCtx.Err() == nil {
				service.logger.Warn("scheduler admission failed", "resource_class", class, "error", err)
			}
		}
		if admitted > 0 {
			idle = service.scheduler.settings.IdlePoll
		} else {
			idle = min(idle*2, service.scheduler.settings.IdlePollMax)
		}
		timer := time.NewTimer(idle)
		select {
		case <-runCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return runCtx.Err()
		case <-timer.C:
		}
	}
}

func (service *Service) Shutdown(context.Context) error {
	if service.stop != nil {
		service.stop()
	}
	return nil
}
