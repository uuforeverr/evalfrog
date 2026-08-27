package scheduling

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/platform/clock"
)

type sequenceIDs struct {
	mu     sync.Mutex
	next   int
	fails  bool
	failAt int
}

func (ids *sequenceIDs) New() (string, error) {
	ids.mu.Lock()
	defer ids.mu.Unlock()
	ids.next++
	if ids.fails || ids.failAt == ids.next {
		return "", errors.New("id failure")
	}
	return fmt.Sprintf("id-%d", ids.next), nil
}

type fakeAuthority struct {
	snapshot AuthoritySnapshot
	dispatch func(DispatchCommand) (Task, error)
	window   int
	loadErr  error
}

func (authority *fakeAuthority) LoadSchedulingSnapshot(_ context.Context, window int) (AuthoritySnapshot, error) {
	authority.window = window
	return authority.snapshot, authority.loadErr
}

func (authority *fakeAuthority) DispatchReady(_ context.Context, command DispatchCommand) (Task, error) {
	if authority.dispatch != nil {
		return authority.dispatch(command)
	}
	return Task{TaskID: command.TaskID, AttemptID: command.AttemptID, ProjectID: command.Candidate.ProjectID, ResourceClass: command.Candidate.ResourceClass}, nil
}

type memoryCoordination struct {
	mu                sync.Mutex
	growthAllowed     bool
	reservations      []Reservation
	reserve           []Reservation
	rebuilt           AuthoritySnapshot
	confirmed         []string
	aborted           []string
	abortRestore      []bool
	calibrated        bool
	claimed           []string
	terminal          []string
	terminalCompleted []bool
	memoryErr         error
	leaseErr          error
	listErr           error
	rebuildErr        error
	calibrateErr      error
	reserveErr        error
	confirmErr        error
	abortErr          error
	rebuildTopics     map[ResourceClass]TopicState
	calibrateTopics   map[ResourceClass]TopicState
}

func (store *memoryCoordination) RegisterReady(context.Context, []Candidate) error { return nil }
func (store *memoryCoordination) MarkClaimed(_ context.Context, attemptID string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.claimed = append(store.claimed, attemptID)
	return nil
}
func (store *memoryCoordination) MarkTerminal(_ context.Context, attemptID string, completed bool) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.terminal = append(store.terminal, attemptID)
	store.terminalCompleted = append(store.terminalCompleted, completed)
	return nil
}
func (store *memoryCoordination) AcquireReconcileLease(_ context.Context, owner string, duration time.Duration) (ReconcileLease, error) {
	if store.leaseErr != nil {
		return ReconcileLease{}, store.leaseErr
	}
	return ReconcileLease{Owner: owner, Token: "token", FencingToken: 7, ExpiresAt: time.Now().Add(duration)}, nil
}
func (store *memoryCoordination) RefreshMemoryPressure(context.Context, MemoryPolicy) (bool, error) {
	return store.growthAllowed, store.memoryErr
}
func (store *memoryCoordination) ListReservations(context.Context) ([]Reservation, error) {
	return append([]Reservation(nil), store.reservations...), store.listErr
}
func (store *memoryCoordination) Rebuild(_ context.Context, lease ReconcileLease, snapshot AuthoritySnapshot, _ TopicWindowPolicy) (ReconcileResult, error) {
	store.rebuilt = snapshot
	return ReconcileResult{Generation: lease.FencingToken, CandidateCount: len(snapshot.Candidates), InflightCount: len(snapshot.Inflight), Topics: store.rebuildTopics}, store.rebuildErr
}
func (store *memoryCoordination) CalibrateTopicWindows(context.Context, ReconcileLease, TopicWindowPolicy) (map[ResourceClass]TopicState, error) {
	store.calibrated = true
	if store.calibrateTopics != nil {
		return store.calibrateTopics, store.calibrateErr
	}
	return map[ResourceClass]TopicState{ResourceBuiltin: {Window: 10}}, store.calibrateErr
}
func (store *memoryCoordination) ReserveNext(_ context.Context, class ResourceClass, _ string, _ time.Duration) (Reservation, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.reserveErr != nil {
		return Reservation{}, false, store.reserveErr
	}
	for index, reservation := range store.reserve {
		if reservation.ResourceClass != class {
			continue
		}
		store.reserve = append(store.reserve[:index], store.reserve[index+1:]...)
		return reservation, true, nil
	}
	return Reservation{}, false, nil
}
func (store *memoryCoordination) ConfirmReservation(_ context.Context, reservation Reservation) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.confirmed = append(store.confirmed, reservation.AttemptID)
	return store.confirmErr
}
func (store *memoryCoordination) AbortReservation(_ context.Context, reservation Reservation, restore bool) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.aborted = append(store.aborted, reservation.AttemptID)
	store.abortRestore = append(store.abortRestore, restore)
	return store.abortErr
}

func schedulerSettings() Settings {
	return Settings{
		CandidateBatch: 8, AdmissionConcurrency: 2,
		CapacityCalibrationInterval: 5 * time.Second, ReadyReconcileInterval: 20 * time.Second,
		ReconcileLease: 4 * time.Second, ReservationTTL: 10 * time.Second,
		IdlePoll: 10 * time.Millisecond, IdlePollMax: 100 * time.Millisecond,
		TopicWindow: TopicWindowPolicy{
			BufferDuration: 5 * time.Second, SampleInterval: 5 * time.Second, EWMAAlpha: 0.4,
			Minimum: map[ResourceClass]int{ResourceBuiltin: 1, ResourceSandbox: 1},
			Maximum: map[ResourceClass]int{ResourceBuiltin: 100, ResourceSandbox: 100},
		},
		Memory: MemoryPolicy{HighWatermark: 0.8, ResumeWatermark: 0.7},
	}
}

func candidate(project, node string, at time.Time, class ResourceClass) Candidate {
	return Candidate{ProjectID: project, RunID: project + "-run", NodeRunID: node, ExecutionNodeID: "xn-" + node, StateVersion: 1, ReadyAt: at, ResourceClass: class}.Normalized()
}

func TestCandidateUsesOneSecondUTCReadyBucket(t *testing.T) {
	at := time.Date(2026, 8, 27, 10, 0, 0, 987654321, time.FixedZone("CST", 8*60*60))
	value := candidate("project", "node", at, ResourceBuiltin)
	if value.ReadyAt.Location() != time.UTC || value.ReadyBucket != value.ReadyAt.UnixMilli()/1000 || value.ReadyOrderKey == "" {
		t.Fatalf("candidate was not normalized to the one-second UTC bucket: %+v", value)
	}
	value.ReadyBucket++
	if err := value.Validate(); err == nil {
		t.Fatal("mismatched derived bucket was accepted")
	}
}

func TestSettingsRejectsInvalidWindowAndMemoryPolicy(t *testing.T) {
	settings := schedulerSettings()
	if err := settings.Validate(); err != nil {
		t.Fatal(err)
	}
	settings.TopicWindow.Minimum[ResourceBuiltin] = 0
	if err := settings.Validate(); err == nil {
		t.Fatal("zero bootstrap queue was accepted")
	}
	settings = schedulerSettings()
	settings.Memory.ResumeWatermark = settings.Memory.HighWatermark
	if err := settings.Validate(); err == nil {
		t.Fatal("invalid memory hysteresis was accepted")
	}
}

func TestReconcileFiltersReservedCandidateAndUsesBoundedAuthorityRead(t *testing.T) {
	now := time.Now().UTC()
	reserved := candidate("project-a", "node-a", now, ResourceBuiltin)
	ready := candidate("project-b", "node-b", now.Add(time.Second), ResourceSandbox)
	authority := &fakeAuthority{snapshot: AuthoritySnapshot{Candidates: []Candidate{reserved, ready}, Inflight: []Inflight{{AttemptID: "running", ProjectID: "project-a", ResourceClass: ResourceBuiltin}}}}
	store := &memoryCoordination{growthAllowed: true, reservations: []Reservation{{AttemptID: "reserved", ProjectID: reserved.ProjectID, ResourceClass: reserved.ResourceClass, Candidate: reserved}}}
	scheduler, err := New(authority, store, &sequenceIDs{}, clock.NewFake(now), "scheduler", schedulerSettings())
	if err != nil {
		t.Fatal(err)
	}
	result, err := scheduler.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if authority.window != schedulerSettings().CandidateBatch || result.CandidateCount != 1 || len(store.rebuilt.Candidates) != 1 || store.rebuilt.Candidates[0].NodeRunID != ready.NodeRunID {
		t.Fatalf("unexpected reconciliation: window=%d result=%+v snapshot=%+v", authority.window, result, store.rebuilt)
	}
}

func TestReconcileStopsGrowthButDoesNotMutateExistingGenerationAtHighWatermark(t *testing.T) {
	now := time.Now().UTC()
	authority := &fakeAuthority{snapshot: AuthoritySnapshot{Candidates: []Candidate{candidate("project", "node", now, ResourceBuiltin)}}}
	store := &memoryCoordination{growthAllowed: false}
	scheduler, _ := New(authority, store, &sequenceIDs{}, clock.NewFake(now), "scheduler", schedulerSettings())
	if _, err := scheduler.Reconcile(context.Background()); !errors.Is(err, ErrMemoryPressure) {
		t.Fatalf("expected memory-pressure draining, got %v", err)
	}
	if authority.window != 0 || len(store.rebuilt.Candidates) != 0 {
		t.Fatal("authority growth read or rebuild ran while Redis was draining")
	}
}

func TestAdmissionDispatchesInBoundedParallelAndConfirmsPostCommit(t *testing.T) {
	now := time.Now().UTC()
	store := &memoryCoordination{growthAllowed: true}
	for index := 0; index < 4; index++ {
		value := candidate(fmt.Sprintf("project-%d", index), fmt.Sprintf("node-%d", index), now, ResourceBuiltin)
		store.reserve = append(store.reserve, Reservation{AttemptID: fmt.Sprintf("attempt-%d", index), ProjectID: value.ProjectID, ResourceClass: value.ResourceClass, Candidate: value})
	}
	var mu sync.Mutex
	active, maximum := 0, 0
	authority := &fakeAuthority{dispatch: func(command DispatchCommand) (Task, error) {
		mu.Lock()
		active++
		maximum = max(maximum, active)
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		return Task{TaskID: command.TaskID, AttemptID: command.AttemptID}, nil
	}}
	scheduler, _ := New(authority, store, &sequenceIDs{}, clock.NewFake(now), "scheduler", schedulerSettings())
	tasks, err := scheduler.AdmitClass(context.Background(), ResourceBuiltin, 4, "trace")
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 4 || len(store.confirmed) != 4 || maximum != 2 {
		t.Fatalf("unexpected bounded admission tasks=%d confirms=%d max_parallel=%d", len(tasks), len(store.confirmed), maximum)
	}
}

func TestAmbiguousDispatchFailureDoesNotRestoreCandidate(t *testing.T) {
	now := time.Now().UTC()
	value := candidate("project", "node", now, ResourceSandbox)
	store := &memoryCoordination{growthAllowed: true, reserve: []Reservation{{AttemptID: "attempt", ProjectID: value.ProjectID, ResourceClass: value.ResourceClass, Candidate: value}}}
	authority := &fakeAuthority{dispatch: func(DispatchCommand) (Task, error) { return Task{}, errors.New("commit outcome unknown") }}
	scheduler, _ := New(authority, store, &sequenceIDs{}, clock.NewFake(now), "scheduler", schedulerSettings())
	if _, err := scheduler.AdmitClass(context.Background(), ResourceSandbox, 1, "trace"); err == nil {
		t.Fatal("dispatch failure was hidden")
	}
	if len(store.aborted) != 1 || store.abortRestore[0] {
		t.Fatalf("ambiguous dispatch was restored eagerly: %+v", store.abortRestore)
	}
}

func TestCalibrateDoesNotReadWorkerSlotCapacity(t *testing.T) {
	now := time.Now().UTC()
	store := &memoryCoordination{growthAllowed: true}
	scheduler, _ := New(&fakeAuthority{}, store, &sequenceIDs{}, clock.NewFake(now), "scheduler", schedulerSettings())
	states, err := scheduler.Calibrate(context.Background())
	if err != nil || !store.calibrated || states[ResourceBuiltin].Window != 10 {
		t.Fatalf("topic throughput calibration failed: states=%+v err=%v", states, err)
	}
}

func TestNewAndAdmissionValidateInputs(t *testing.T) {
	settings := schedulerSettings()
	if _, err := New(nil, &memoryCoordination{}, &sequenceIDs{}, clock.NewFake(time.Now()), "scheduler", settings); err == nil {
		t.Fatal("nil authority was accepted")
	}
	scheduler, _ := New(&fakeAuthority{}, &memoryCoordination{growthAllowed: true}, &sequenceIDs{}, clock.NewFake(time.Now()), "scheduler", settings)
	for _, test := range []struct {
		class ResourceClass
		limit int
		trace string
	}{{"unknown", 1, "trace"}, {ResourceBuiltin, 0, "trace"}, {ResourceBuiltin, settings.CandidateBatch + 1, "trace"}, {ResourceBuiltin, 1, ""}} {
		if _, err := scheduler.AdmitClass(context.Background(), test.class, test.limit, test.trace); err == nil {
			t.Fatalf("invalid admission accepted: %+v", test)
		}
	}
}

func TestRoutingCapabilitiesWorkerRegistrationAndFixedCapacity(t *testing.T) {
	router := BuiltinV1Router()
	if class, ok := router.Resolve(dsl.Coordinate{Type: "task.python", Version: 1}); !ok || class != ResourceSandbox {
		t.Fatalf("python route=%q exists=%t", class, ok)
	}
	if _, ok := router.Resolve(dsl.Coordinate{Type: "unknown", Version: 1}); ok {
		t.Fatal("unknown operation was routable")
	}
	builtin := RequiredCapabilities(ResourceBuiltin)
	wantBuiltin := []dsl.Coordinate{{Type: "task.http", Version: 1}, {Type: "task.rpc", Version: 1}}
	if !slices.Equal(builtin, wantBuiltin) || len(RequiredCapabilities("unknown")) != 0 {
		t.Fatalf("builtin capabilities=%+v", builtin)
	}
	if CapabilityFingerprint(ResourceBuiltin) == "" || CapabilityFingerprint(ResourceBuiltin) == CapabilityFingerprint(ResourceSandbox) {
		t.Fatal("capability fingerprints are empty or do not separate resource classes")
	}
	valid := WorkerRegistration{
		WorkerID: "worker", ExecutorBuild: "build", ResourceClass: ResourceBuiltin,
		Slots: 2, Capabilities: builtin, TTL: time.Minute,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := []WorkerRegistration{
		{},
		{WorkerID: "worker", ExecutorBuild: "build", ResourceClass: ResourceBuiltin, Slots: 1, TTL: time.Minute, Capabilities: []dsl.Coordinate{{Version: 1}}},
		{WorkerID: "worker", ExecutorBuild: "build", ResourceClass: ResourceBuiltin, Slots: 1, TTL: time.Minute, Capabilities: []dsl.Coordinate{{Type: "task.python", Version: 1}}},
		{WorkerID: "worker", ExecutorBuild: "build", ResourceClass: ResourceBuiltin, Slots: 1, TTL: time.Minute, Capabilities: []dsl.Coordinate{{Type: "task.http", Version: 1}, {Type: "task.http", Version: 1}}},
		{WorkerID: "worker", ExecutorBuild: "build", ResourceClass: ResourceBuiltin, Slots: 1, TTL: time.Minute, Capabilities: []dsl.Coordinate{{Type: "task.http", Version: 1}}},
	}
	for index, registration := range invalid {
		if err := registration.Validate(); err == nil {
			t.Fatalf("invalid registration %d was accepted", index)
		}
	}
	provider := FixedCapacity{Pools: map[ResourceClass]int{ResourceBuiltin: 3}}
	capacity, err := provider.HealthyCapacity(context.Background())
	if err != nil || capacity.Pools[ResourceBuiltin] != 3 {
		t.Fatalf("fixed capacity=%+v err=%v", capacity, err)
	}
	capacity.Pools[ResourceBuiltin] = 99
	again, _ := provider.HealthyCapacity(context.Background())
	if again.Pools[ResourceBuiltin] != 3 {
		t.Fatal("fixed capacity leaked its mutable map")
	}
}

func TestCandidateValidationAndCurrentStableOrder(t *testing.T) {
	if err := (Candidate{}).Validate(); err == nil {
		t.Fatal("empty candidate was accepted")
	}
	base := time.Date(2026, 8, 27, 1, 2, 3, 0, time.UTC)
	invalidOrder := candidate("project", "node", base, ResourceBuiltin)
	invalidOrder.ReadyOrderKey = "wrong"
	if err := invalidOrder.Validate(); err == nil {
		t.Fatal("mismatched ready order key was accepted")
	}
	values := []Candidate{
		candidate("project-a", "later-bucket", base.Add(time.Second), ResourceBuiltin),
		candidate("project-b", "project-b", base, ResourceBuiltin),
		candidate("project-a", "priority-low", base.Add(300*time.Microsecond), ResourceBuiltin),
		candidate("project-a", "time-later", base.Add(200*time.Microsecond), ResourceBuiltin),
		candidate("project-a", "node-z", base.Add(100*time.Microsecond), ResourceBuiltin),
		candidate("project-a", "node-a", base.Add(100*time.Microsecond), ResourceBuiltin),
	}
	values[2].Priority = 1
	values[3].Priority = 2
	values[4].Priority = 2
	values[5].Priority = 2
	sortCandidates(values)
	got := make([]string, len(values))
	for index := range values {
		got[index] = values[index].NodeRunID
	}
	want := []string{"node-a", "node-z", "time-later", "priority-low", "project-b", "later-bucket"}
	if !slices.Equal(got, want) {
		t.Fatalf("candidate order=%v want=%v", got, want)
	}
}

func TestPolicyAndSettingsValidationBranches(t *testing.T) {
	valid := schedulerSettings()
	invalidPolicies := []TopicWindowPolicy{
		{},
		{BufferDuration: time.Second, SampleInterval: time.Second, EWMAAlpha: 2, Minimum: valid.TopicWindow.Minimum, Maximum: valid.TopicWindow.Maximum},
		{BufferDuration: time.Second, SampleInterval: time.Second, EWMAAlpha: .5, Minimum: map[ResourceClass]int{ResourceBuiltin: 1}, Maximum: valid.TopicWindow.Maximum},
		{BufferDuration: time.Second, SampleInterval: time.Second, EWMAAlpha: .5, Minimum: valid.TopicWindow.Minimum, Maximum: map[ResourceClass]int{ResourceBuiltin: 0, ResourceSandbox: 2}},
	}
	for index, policy := range invalidPolicies {
		if err := policy.Validate(); err == nil {
			t.Fatalf("invalid topic policy %d was accepted", index)
		}
	}
	for index, policy := range []MemoryPolicy{{}, {HighWatermark: .5, ResumeWatermark: .5}, {HighWatermark: 1, ResumeWatermark: .5}} {
		if err := policy.Validate(); err == nil {
			t.Fatalf("invalid memory policy %d was accepted", index)
		}
	}
	invalid := valid
	invalid.CandidateBatch = 0
	if err := invalid.Validate(); err == nil {
		t.Fatal("invalid scheduler batching was accepted")
	}
	invalid = valid
	invalid.TopicWindow.SampleInterval = time.Second
	if err := invalid.Validate(); err == nil {
		t.Fatal("mismatched calibration interval was accepted")
	}
	invalid = valid
	invalid.TopicWindow.EWMAAlpha = 0
	if err := invalid.Validate(); err == nil {
		t.Fatal("invalid topic policy passed settings validation")
	}
}

type recordingObserver struct {
	readyDurations []time.Duration
	rebuilds       []string
	topics         []string
}

func (observer *recordingObserver) ObserveReadyToQueued(value time.Duration) {
	observer.readyDurations = append(observer.readyDurations, value)
}
func (observer *recordingObserver) ObserveSchedulingRedisRebuild(outcome string) {
	observer.rebuilds = append(observer.rebuilds, outcome)
}
func (observer *recordingObserver) ObserveTopicQueue(class string, _ int, _ int, _ float64) {
	observer.topics = append(observer.topics, class)
}

func TestReconcileRebalanceCalibrationObserversAndFailures(t *testing.T) {
	now := time.Now().UTC()
	observer := &recordingObserver{}
	store := &memoryCoordination{
		growthAllowed: true,
		rebuildTopics: map[ResourceClass]TopicState{
			ResourceBuiltin: {Window: 2}, ResourceSandbox: {Window: 3},
		},
		calibrateTopics: map[ResourceClass]TopicState{ResourceBuiltin: {Window: 4}},
	}
	scheduler, err := New(&fakeAuthority{}, store, &sequenceIDs{}, clock.NewFake(now), "scheduler", schedulerSettings(), observer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = scheduler.Rebalance(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = scheduler.Calibrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(observer.rebuilds, []string{"success"}) || len(observer.topics) != 3 {
		t.Fatalf("observer rebuilds=%v topics=%v", observer.rebuilds, observer.topics)
	}

	boom := errors.New("boom")
	tests := []struct {
		name      string
		authority *fakeAuthority
		store     *memoryCoordination
	}{
		{"memory", &fakeAuthority{}, &memoryCoordination{growthAllowed: true, memoryErr: boom}},
		{"lease", &fakeAuthority{}, &memoryCoordination{growthAllowed: true, leaseErr: boom}},
		{"reservations", &fakeAuthority{}, &memoryCoordination{growthAllowed: true, listErr: boom}},
		{"authority", &fakeAuthority{loadErr: boom}, &memoryCoordination{growthAllowed: true}},
		{"candidate", &fakeAuthority{snapshot: AuthoritySnapshot{Candidates: []Candidate{{}}}}, &memoryCoordination{growthAllowed: true}},
		{"rebuild", &fakeAuthority{}, &memoryCoordination{growthAllowed: true, rebuildErr: boom}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value, newErr := New(test.authority, test.store, &sequenceIDs{}, clock.NewFake(now), "scheduler", schedulerSettings(), observer)
			if newErr != nil {
				t.Fatal(newErr)
			}
			if _, reconcileErr := value.Reconcile(context.Background()); reconcileErr == nil {
				t.Fatal("reconciliation failure was hidden")
			}
		})
	}
	if observer.rebuilds[len(observer.rebuilds)-1] != "failure" {
		t.Fatalf("rebuild failure was not observed: %v", observer.rebuilds)
	}
	leaseFailure, _ := New(&fakeAuthority{}, &memoryCoordination{leaseErr: boom}, &sequenceIDs{}, clock.NewFake(now), "scheduler", schedulerSettings())
	if _, err = leaseFailure.Calibrate(context.Background()); err == nil {
		t.Fatal("calibration lease failure was hidden")
	}
	calibrationFailure, _ := New(&fakeAuthority{}, &memoryCoordination{calibrateErr: boom}, &sequenceIDs{}, clock.NewFake(now), "scheduler", schedulerSettings())
	if _, err = calibrationFailure.Calibrate(context.Background()); err == nil {
		t.Fatal("calibration store failure was hidden")
	}
}

func TestAdmissionRestoresOnlyPreDispatchFailures(t *testing.T) {
	now := time.Now().UTC()
	value := candidate("project", "node", now, ResourceBuiltin)
	reservation := Reservation{AttemptID: "attempt", ProjectID: value.ProjectID, ResourceClass: value.ResourceClass, Candidate: value}
	store := &memoryCoordination{growthAllowed: true, reserve: []Reservation{reservation}}
	scheduler, _ := New(&fakeAuthority{}, store, &sequenceIDs{failAt: 2}, clock.NewFake(now), "scheduler", schedulerSettings())
	if _, err := scheduler.AdmitClass(context.Background(), ResourceBuiltin, 1, "trace"); err == nil {
		t.Fatal("task ID failure was hidden")
	}
	if len(store.aborted) != 1 || !store.abortRestore[0] {
		t.Fatalf("pre-dispatch failure releases=%v restore=%v", store.aborted, store.abortRestore)
	}

	first := reservation
	first.AttemptID = "first"
	secondStore := &memoryCoordination{growthAllowed: true, reserve: []Reservation{first}}
	secondScheduler, _ := New(&fakeAuthority{}, secondStore, &sequenceIDs{failAt: 2}, clock.NewFake(now), "scheduler", schedulerSettings())
	if _, err := secondScheduler.AdmitClass(context.Background(), ResourceBuiltin, 2, "trace"); err == nil {
		t.Fatal("reservation ID failure was hidden")
	}
	if len(secondStore.aborted) != 1 || !secondStore.abortRestore[0] {
		t.Fatalf("previous reservations were not restored: %+v", secondStore.abortRestore)
	}

	reserveFailure, _ := New(&fakeAuthority{}, &memoryCoordination{growthAllowed: true, reserveErr: errors.New("reserve")}, &sequenceIDs{}, clock.NewFake(now), "scheduler", schedulerSettings())
	if _, err := reserveFailure.AdmitClass(context.Background(), ResourceBuiltin, 1, "trace"); err == nil {
		t.Fatal("Redis reserve failure was hidden")
	}
	confirmStore := &memoryCoordination{growthAllowed: true, reserve: []Reservation{reservation}, confirmErr: errors.New("confirm")}
	confirmScheduler, _ := New(&fakeAuthority{}, confirmStore, &sequenceIDs{}, clock.NewFake(now), "scheduler", schedulerSettings())
	if tasks, err := confirmScheduler.AdmitClass(context.Background(), ResourceBuiltin, 1, "trace"); err != nil || len(tasks) != 1 {
		t.Fatalf("post-commit Redis confirmation affected dispatch: tasks=%v err=%v", tasks, err)
	}
}

func TestSchedulerServiceLifecycle(t *testing.T) {
	settings := schedulerSettings()
	settings.CapacityCalibrationInterval = time.Millisecond
	settings.TopicWindow.SampleInterval = time.Millisecond
	settings.ReadyReconcileInterval = 4 * time.Millisecond
	settings.ReconcileLease = time.Millisecond
	settings.IdlePoll = time.Millisecond
	settings.IdlePollMax = 2 * time.Millisecond
	scheduler, err := New(&fakeAuthority{}, &memoryCoordination{growthAllowed: true}, &sequenceIDs{}, clock.System{}, "scheduler", settings)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewService(nil, "trace", slog.Default()); err == nil {
		t.Fatal("nil scheduler service was accepted")
	}
	service, err := NewService(scheduler, "trace", slog.Default())
	if err != nil || service.Name() != "fifo-topic-window-scheduler" {
		t.Fatalf("service=%+v err=%v", service, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = service.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("service exit=%v", err)
	}
	if err = service.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	unused, _ := NewService(scheduler, "trace", slog.Default())
	if err = unused.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
