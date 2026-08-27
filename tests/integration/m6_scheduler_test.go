//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/adapters/postgres"
	"github.com/uu999/evalfrog/internal/adapters/schedulingredis"
	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/platform/clock"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/platform/identity"
	"github.com/uu999/evalfrog/internal/platform/migrations"
	"github.com/uu999/evalfrog/internal/resources"
	runtimepkg "github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	enginepkg "github.com/uu999/evalfrog/internal/runtime/engine"
	"github.com/uu999/evalfrog/internal/scheduling"
)

func TestM6MigrationUpgradesExistingM5RuntimeRows(t *testing.T) {
	harness, root := newM5OnlyHarness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m6-upgrade")
	seedM5RuntimeNodes(t, harness, run, snapshot)
	// Current Run creation persists trace correlation. The fixture starts from
	// M5, so temporarily add the future column to create the historical row,
	// then remove it before exercising the real append-only upgrade. M11 must
	// recreate it from the pre-existing RunCreated Outbox event.
	mustExec(t, harness.ctx, harness.client.Pool(), `ALTER TABLE workflow_runs DROP COLUMN trace_id`)

	runner := migrations.Runner{Pool: harness.client.Pool(), Schema: harness.schema,
		Directory: filepath.Join(root, "migrations"), LockTimeout: 5 * time.Second}
	if err := runner.Up(harness.ctx); err != nil {
		t.Fatal(err)
	}
	// The fixture begins at M5 specifically to prove M6 can populate the
	// durable scheduling fields for historical rows. Once the M11 migration is
	// applied, current Store code legitimately reads the new Run trace column;
	// keep that current-code assertion after the full upgrade, not against an
	// intentionally historical schema.
	rows, err := harness.client.Pool().Query(harness.ctx, `
		SELECT kind, operation_type, operation_version, resource_class
		FROM node_runs WHERE project_id=$1 AND run_id=$2 ORDER BY execution_node_id`, harness.projectID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	control, task := 0, 0
	for rows.Next() {
		var kind, operationType string
		var version int
		var resourceClass *string
		if err = rows.Scan(&kind, &operationType, &version, &resourceClass); err != nil {
			t.Fatal(err)
		}
		if operationType == "" || version != 1 {
			t.Fatalf("operation=%s@%d", operationType, version)
		}
		if kind == "control" && resourceClass == nil {
			control++
		} else if kind == "task" && resourceClass != nil && *resourceClass == string(scheduling.ResourceSandbox) {
			task++
		} else {
			t.Fatalf("kind=%s resource_class=%v", kind, resourceClass)
		}
	}
	if err = rows.Err(); err != nil || control != 2 || task != 1 {
		t.Fatalf("control=%d task=%d err=%v", control, task, err)
	}
	authority, err := harness.store.LoadSchedulingSnapshot(harness.ctx, 1)
	if err != nil || len(authority.Candidates) != 1 || authority.Candidates[0].ResourceClass != scheduling.ResourceSandbox {
		t.Fatalf("authority=%+v err=%v", authority, err)
	}
}

func TestM6AuthorityBoundsOneGlobalOldestReadyRead(t *testing.T) {
	harness := newM5Harness(t)
	createReadyTasksForProject(t, harness, harness.projectID, harness.principalID, 1)

	busyProject, busyPrincipal, busyExecution := newID(t), newID(t), newID(t)
	harness.seedProject(t, busyProject, busyPrincipal, busyExecution, "m6-busy-"+newID(t), allPermissions())
	createReadyTasksForProject(t, harness, busyProject, busyPrincipal, 6)

	authority, err := harness.store.LoadSchedulingSnapshot(harness.ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, candidate := range authority.Candidates {
		counts[candidate.ProjectID]++
	}
	if len(authority.Candidates) != 4 || counts[harness.projectID]+counts[busyProject] != 4 {
		t.Fatalf("bounded candidates=%v total=%d", counts, len(authority.Candidates))
	}
}

func TestM6DispatchIsAtomicAndConcurrentSchedulersCreateOneAttempt(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m6-dispatch")
	harness.initializeRun(t, run)
	authority, err := harness.store.LoadSchedulingSnapshot(harness.ctx, 16)
	if err != nil || len(authority.Candidates) != 1 {
		t.Fatalf("candidates=%v err=%v", authority.Candidates, err)
	}
	candidate := authority.Candidates[0]
	start := make(chan struct{})
	var wait sync.WaitGroup
	results := make(chan error, 12)
	for index := 0; index < 12; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, dispatchErr := harness.store.DispatchReady(harness.ctx, scheduling.DispatchCommand{
				Candidate: candidate, AttemptID: newID(t), TaskID: newID(t), TraceID: "m6-race", Now: time.Now().UTC(),
			})
			results <- dispatchErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	succeeded := 0
	for result := range results {
		if result == nil {
			succeeded++
			continue
		}
		if !errors.Is(result, scheduling.ErrCandidateStale) {
			t.Fatalf("dispatch error=%v", result)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful dispatches=%d", succeeded)
	}
	var nodeState string
	var attempts, tasks int
	if err = harness.client.Pool().QueryRow(harness.ctx, `SELECT state FROM node_runs WHERE project_id=$1 AND node_run_id=$2`, harness.projectID, candidate.NodeRunID).Scan(&nodeState); err != nil {
		t.Fatal(err)
	}
	if err = harness.client.Pool().QueryRow(harness.ctx, `SELECT count(*) FROM node_attempts WHERE project_id=$1 AND node_run_id=$2`, harness.projectID, candidate.NodeRunID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err = harness.client.Pool().QueryRow(harness.ctx, `SELECT count(*) FROM node_task_outbox WHERE project_id=$1 AND node_run_id=$2`, harness.projectID, candidate.NodeRunID).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if nodeState != "queued" || attempts != 1 || tasks != 1 {
		t.Fatalf("node=%s attempts=%d tasks=%d", nodeState, attempts, tasks)
	}
}

func TestM6RedisReconcileFencingReservationAndDispatch(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m6-redis")
	harness.initializeRun(t, run)
	configuration := localM6Config(t)
	configuration.Redis.Scheduling.KeyPrefix = "evalfrog:local:m6:" + uuid.NewString() + ":"
	store := schedulingredis.Open(configuration.Redis.Scheduling)
	defer store.Close()
	scheduler, err := scheduling.New(harness.store, store, identity.UUIDv7Generator{}, clock.System{}, "scheduler-a", m6Settings(configuration))
	if err != nil {
		t.Fatal(err)
	}
	result, err := scheduler.Reconcile(harness.ctx)
	if err != nil || result.CandidateCount != 1 || result.Topics[scheduling.ResourceSandbox].Window != configuration.Scheduler.SandboxMinQueue {
		t.Fatalf("reconcile=%+v err=%v", result, err)
	}
	follower, err := scheduling.New(harness.store, store, identity.UUIDv7Generator{}, clock.System{}, "scheduler-b", m6Settings(configuration))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = follower.Reconcile(harness.ctx); !errors.Is(err, scheduling.ErrLeaseLost) {
		t.Fatalf("follower acquired reconciliation role: %v", err)
	}
	tasks, err := follower.AdmitClass(harness.ctx, scheduling.ResourceSandbox, 1, "m6-redis-trace")
	if err != nil || len(tasks) != 1 || tasks[0].ResourceClass != scheduling.ResourceSandbox {
		t.Fatalf("tasks=%v err=%v", tasks, err)
	}
	_, err = scheduler.Reconcile(harness.ctx)
	if err != nil {
		t.Fatal(err)
	}
	more, err := scheduler.AdmitClass(harness.ctx, scheduling.ResourceSandbox, 1, "m6-redis-trace")
	if err != nil || len(more) != 0 {
		t.Fatalf("duplicate tasks=%v err=%v", more, err)
	}
	firstLease, err := store.AcquireReconcileLease(harness.ctx, "scheduler-a", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.AcquireReconcileLease(harness.ctx, "scheduler-b", time.Second); !errors.Is(err, scheduling.ErrLeaseLost) {
		t.Fatalf("competing lease=%v", err)
	}
	time.Sleep(30 * time.Millisecond)
	secondLease, err := store.AcquireReconcileLease(harness.ctx, "scheduler-b", time.Second)
	if err != nil || secondLease.FencingToken <= firstLease.FencingToken {
		t.Fatalf("second lease=%+v err=%v", secondLease, err)
	}
	if _, err = store.Rebuild(harness.ctx, firstLease, scheduling.AuthoritySnapshot{}, m6Settings(configuration).TopicWindow); !errors.Is(err, scheduling.ErrLeaseLost) {
		t.Fatalf("stale reconciliation lease accepted: %v", err)
	}
}

func TestM6RedisLossFailsClosedThenRebuildsFromPostgres(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m6-rebuild")
	harness.initializeRun(t, run)
	configuration := localM6Config(t)
	configuration.Redis.Scheduling.KeyPrefix = "evalfrog:local:m6-rebuild:" + uuid.NewString() + ":"
	store := schedulingredis.Open(configuration.Redis.Scheduling)
	defer store.Close()
	if _, _, err := store.ReserveNext(harness.ctx, scheduling.ResourceSandbox, newID(t), time.Minute); !errors.Is(err, scheduling.ErrAdmissionPaused) {
		t.Fatalf("empty Redis admitted: %v", err)
	}
	scheduler, err := scheduling.New(harness.store, store, identity.UUIDv7Generator{}, clock.System{}, "rebuild-scheduler", m6Settings(configuration))
	if err != nil {
		t.Fatal(err)
	}
	initialPlan, err := scheduler.Reconcile(harness.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ClearDerivedState(harness.ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.ReserveNext(harness.ctx, scheduling.ResourceSandbox, newID(t), time.Minute); !errors.Is(err, scheduling.ErrAdmissionPaused) {
		t.Fatalf("cleared Redis admitted before rebuild: %v", err)
	}
	rebuiltPlan, err := scheduler.Reconcile(harness.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rebuiltPlan.Generation <= initialPlan.Generation {
		t.Fatalf("generation regressed after Redis loss: before=%d after=%d", initialPlan.Generation, rebuiltPlan.Generation)
	}
	tasks, err := scheduler.AdmitClass(harness.ctx, scheduling.ResourceSandbox, 1, "m6-rebuild-trace")
	if err != nil || len(tasks) != 1 {
		t.Fatalf("rebuilt tasks=%v err=%v", tasks, err)
	}
}

func TestM6ConcurrentRedisAdmissionCannotExceedTopicWindow(t *testing.T) {
	harness := newM5Harness(t)
	configuration := localM6Config(t)
	configuration.Redis.Scheduling.KeyPrefix = "evalfrog:local:m6-window:" + uuid.NewString() + ":"
	store := schedulingredis.Open(configuration.Redis.Scheduling)
	defer store.Close()
	lease, err := store.AcquireReconcileLease(harness.ctx, "window-reconciler", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	const window = 7
	projectID := newID(t)
	candidates := make([]scheduling.Candidate, 50)
	for index := range candidates {
		candidates[index] = scheduling.Candidate{
			ProjectID: projectID, RunID: newID(t), NodeRunID: newID(t), ExecutionNodeID: "xn_window",
			StateVersion: 1, ReadyAt: time.Now().UTC().Add(time.Duration(index) * time.Millisecond), ResourceClass: scheduling.ResourceBuiltin,
		}.Normalized()
	}
	policy := m6Settings(configuration).TopicWindow
	policy.Minimum[scheduling.ResourceBuiltin], policy.Maximum[scheduling.ResourceBuiltin] = window, window
	if _, err = store.Rebuild(harness.ctx, lease, scheduling.AuthoritySnapshot{Candidates: candidates}, policy); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	var mutex sync.Mutex
	reserved := 0
	for index := 0; index < 50; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, exists, reserveErr := store.ReserveNext(harness.ctx, scheduling.ResourceBuiltin, newID(t), time.Minute)
			if reserveErr != nil {
				t.Errorf("reserve: %v", reserveErr)
				return
			}
			if exists {
				mutex.Lock()
				reserved++
				mutex.Unlock()
			}
		}()
	}
	wait.Wait()
	if reserved != window {
		t.Fatalf("reservations=%d window=%d", reserved, window)
	}
}

func TestM6RebuildClampsExistingTopicWindowToNewBounds(t *testing.T) {
	harness := newM5Harness(t)
	configuration := localM6Config(t)
	configuration.Redis.Scheduling.KeyPrefix = "evalfrog:local:m6-window-clamp:" + uuid.NewString() + ":"
	store := schedulingredis.Open(configuration.Redis.Scheduling)
	defer store.Close()
	lease, err := store.AcquireReconcileLease(harness.ctx, "window-clamp-reconciler", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	policy := m6Settings(configuration).TopicWindow
	policy.Minimum[scheduling.ResourceBuiltin], policy.Maximum[scheduling.ResourceBuiltin] = 10, 10
	if _, err = store.Rebuild(harness.ctx, lease, scheduling.AuthoritySnapshot{}, policy); err != nil {
		t.Fatal(err)
	}
	policy.Minimum[scheduling.ResourceBuiltin], policy.Maximum[scheduling.ResourceBuiltin] = 2, 3
	result, err := store.Rebuild(harness.ctx, lease, scheduling.AuthoritySnapshot{}, policy)
	if err != nil {
		t.Fatal(err)
	}
	if result.Topics[scheduling.ResourceBuiltin].Window != 3 {
		t.Fatalf("rebuilt window=%d want=3", result.Topics[scheduling.ResourceBuiltin].Window)
	}
}

func TestM6RedisReservationRetryIsIdempotent(t *testing.T) {
	harness := newM5Harness(t)
	configuration := localM6Config(t)
	configuration.Redis.Scheduling.KeyPrefix = "evalfrog:local:m6-reservation-retry:" + uuid.NewString() + ":"
	store := schedulingredis.Open(configuration.Redis.Scheduling)
	defer store.Close()
	lease, err := store.AcquireReconcileLease(harness.ctx, "retry-reconciler", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	projectID := newID(t)
	candidate := scheduling.Candidate{ProjectID: projectID, RunID: newID(t), NodeRunID: newID(t), ExecutionNodeID: "xn_retry", StateVersion: 1, ReadyAt: time.Now().UTC(), ResourceClass: scheduling.ResourceBuiltin}.Normalized()
	policy := m6Settings(configuration).TopicWindow
	policy.Minimum[scheduling.ResourceBuiltin], policy.Maximum[scheduling.ResourceBuiltin] = 1, 1
	if _, err = store.Rebuild(harness.ctx, lease, scheduling.AuthoritySnapshot{Candidates: []scheduling.Candidate{candidate}}, policy); err != nil {
		t.Fatal(err)
	}
	attemptID := newID(t)
	first, exists, err := store.ReserveNext(harness.ctx, scheduling.ResourceBuiltin, attemptID, time.Minute)
	if err != nil || !exists {
		t.Fatalf("first reservation=%+v exists=%t err=%v", first, exists, err)
	}
	second, exists, err := store.ReserveNext(harness.ctx, scheduling.ResourceBuiltin, attemptID, time.Minute)
	if err != nil || !exists || second.AttemptID != first.AttemptID || second.Candidate.NodeRunID != candidate.NodeRunID {
		t.Fatalf("retry reservation=%+v exists=%t err=%v", second, exists, err)
	}
	if _, exists, err = store.ReserveNext(harness.ctx, scheduling.ResourceBuiltin, newID(t), time.Minute); err != nil || exists {
		t.Fatalf("duplicate reservation consumed another credit exists=%t err=%v", exists, err)
	}
}

func TestM6ExpiredReservationReleasesTopicOccupancy(t *testing.T) {
	harness := newM5Harness(t)
	configuration := localM6Config(t)
	configuration.Redis.Scheduling.KeyPrefix = "evalfrog:local:m6-reservation-expiry:" + uuid.NewString() + ":"
	store := schedulingredis.Open(configuration.Redis.Scheduling)
	defer store.Close()
	lease, err := store.AcquireReconcileLease(harness.ctx, "expiry-reconciler", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	candidates := []scheduling.Candidate{
		{ProjectID: newID(t), RunID: newID(t), NodeRunID: "expiry-first", ExecutionNodeID: "xn_first", StateVersion: 1, ReadyAt: base, ResourceClass: scheduling.ResourceBuiltin},
		{ProjectID: newID(t), RunID: newID(t), NodeRunID: "expiry-second", ExecutionNodeID: "xn_second", StateVersion: 1, ReadyAt: base, ResourceClass: scheduling.ResourceBuiltin},
	}
	for index := range candidates {
		candidates[index] = candidates[index].Normalized()
	}
	policy := m6Settings(configuration).TopicWindow
	policy.Minimum[scheduling.ResourceBuiltin], policy.Maximum[scheduling.ResourceBuiltin] = 1, 1
	if _, err = store.Rebuild(harness.ctx, lease, scheduling.AuthoritySnapshot{Candidates: candidates}, policy); err != nil {
		t.Fatal(err)
	}
	if _, exists, reserveErr := store.ReserveNext(harness.ctx, scheduling.ResourceBuiltin, "expiring-attempt", 20*time.Millisecond); reserveErr != nil || !exists {
		t.Fatalf("initial reservation exists=%t err=%v", exists, reserveErr)
	}
	time.Sleep(40 * time.Millisecond)
	reservations, err := store.ListReservations(harness.ctx)
	if err != nil || len(reservations) != 0 {
		t.Fatalf("expired reservations=%+v err=%v", reservations, err)
	}
	if _, exists, reserveErr := store.ReserveNext(harness.ctx, scheduling.ResourceBuiltin, "next-attempt", time.Minute); reserveErr != nil || !exists {
		t.Fatalf("expired reservation did not release occupancy: exists=%t err=%v", exists, reserveErr)
	}
}

func TestM6RedisOrdersOldestBucketThenProjectLoadAndProjectPriority(t *testing.T) {
	harness := newM5Harness(t)
	configuration := localM6Config(t)
	configuration.Redis.Scheduling.KeyPrefix = "evalfrog:local:m6-order:" + uuid.NewString() + ":"
	store := schedulingredis.Open(configuration.Redis.Scheduling)
	defer store.Close()
	lease, err := store.AcquireReconcileLease(harness.ctx, "order-reconciler", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Truncate(time.Second)
	projectA, projectB, projectC := newID(t), newID(t), newID(t)
	values := []scheduling.Candidate{
		{ProjectID: projectA, RunID: newID(t), NodeRunID: "a-low", ExecutionNodeID: "xn_a_low", StateVersion: 1, Priority: 1, ReadyAt: base.Add(100 * time.Millisecond), ResourceClass: scheduling.ResourceBuiltin},
		{ProjectID: projectA, RunID: newID(t), NodeRunID: "a-high", ExecutionNodeID: "xn_a_high", StateVersion: 1, Priority: 10, ReadyAt: base.Add(200 * time.Millisecond), ResourceClass: scheduling.ResourceBuiltin},
		{ProjectID: projectC, RunID: newID(t), NodeRunID: "c", ExecutionNodeID: "xn_c", StateVersion: 1, ReadyAt: base.Add(300 * time.Millisecond), ResourceClass: scheduling.ResourceBuiltin},
		{ProjectID: projectB, RunID: newID(t), NodeRunID: "b-later", ExecutionNodeID: "xn_b", StateVersion: 1, ReadyAt: base.Add(1100 * time.Millisecond), ResourceClass: scheduling.ResourceBuiltin},
	}
	for index := range values {
		values[index] = values[index].Normalized()
	}
	policy := m6Settings(configuration).TopicWindow
	policy.Minimum[scheduling.ResourceBuiltin], policy.Maximum[scheduling.ResourceBuiltin] = 4, 4
	snapshot := scheduling.AuthoritySnapshot{Candidates: values, Inflight: []scheduling.Inflight{
		{AttemptID: newID(t), ProjectID: projectA, ResourceClass: scheduling.ResourceBuiltin},
		{AttemptID: newID(t), ProjectID: projectA, ResourceClass: scheduling.ResourceSandbox},
	}}
	if _, err = store.Rebuild(harness.ctx, lease, snapshot, policy); err != nil {
		t.Fatal(err)
	}
	want := []string{"c", "a-high", "a-low", "b-later"}
	for index, nodeRunID := range want {
		reservation, exists, reserveErr := store.ReserveNext(harness.ctx, scheduling.ResourceBuiltin, newID(t), time.Minute)
		if reserveErr != nil || !exists || reservation.Candidate.NodeRunID != nodeRunID {
			t.Fatalf("reservation %d=%+v exists=%t err=%v want_node=%s", index, reservation, exists, reserveErr, nodeRunID)
		}
	}
}

func TestM6ClaimReleasesTopicOccupancyAndTerminalReleasesProjectLoad(t *testing.T) {
	harness := newM5Harness(t)
	configuration := localM6Config(t)
	configuration.Redis.Scheduling.KeyPrefix = "evalfrog:local:m6-lifecycle:" + uuid.NewString() + ":"
	store := schedulingredis.Open(configuration.Redis.Scheduling)
	defer store.Close()
	lease, err := store.AcquireReconcileLease(harness.ctx, "lifecycle-reconciler", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	candidates := []scheduling.Candidate{
		{ProjectID: newID(t), RunID: newID(t), NodeRunID: "first", ExecutionNodeID: "xn_first", StateVersion: 1, ReadyAt: base, ResourceClass: scheduling.ResourceBuiltin},
		{ProjectID: newID(t), RunID: newID(t), NodeRunID: "second", ExecutionNodeID: "xn_second", StateVersion: 1, ReadyAt: base, ResourceClass: scheduling.ResourceBuiltin},
	}
	for index := range candidates {
		candidates[index] = candidates[index].Normalized()
	}
	policy := m6Settings(configuration).TopicWindow
	policy.Minimum[scheduling.ResourceBuiltin], policy.Maximum[scheduling.ResourceBuiltin] = 1, 1
	if _, err = store.Rebuild(harness.ctx, lease, scheduling.AuthoritySnapshot{Candidates: candidates}, policy); err != nil {
		t.Fatal(err)
	}
	first, exists, err := store.ReserveNext(harness.ctx, scheduling.ResourceBuiltin, "attempt-first", time.Minute)
	if err != nil || !exists {
		t.Fatalf("first reservation=%+v exists=%t err=%v", first, exists, err)
	}
	if err = store.ConfirmReservation(harness.ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, exists, err = store.ReserveNext(harness.ctx, scheduling.ResourceBuiltin, "blocked", time.Minute); err != nil || exists {
		t.Fatalf("topic window admitted before Claim: exists=%t err=%v", exists, err)
	}
	if err = store.MarkClaimed(harness.ctx, first.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkClaimed(harness.ctx, first.AttemptID); err != nil {
		t.Fatal(err)
	}
	second, exists, err := store.ReserveNext(harness.ctx, scheduling.ResourceBuiltin, "attempt-second", time.Minute)
	if err != nil || !exists {
		t.Fatalf("Claim did not release topic occupancy: reservation=%+v exists=%t err=%v", second, exists, err)
	}
	if err = store.MarkTerminal(harness.ctx, first.AttemptID, true); err != nil {
		t.Fatal(err)
	}
	if err = store.MarkTerminal(harness.ctx, first.AttemptID, true); err != nil {
		t.Fatal(err)
	}
	states, err := store.CalibrateTopicWindows(harness.ctx, lease, policy)
	if err != nil || states[scheduling.ResourceBuiltin].Window != 1 || states[scheduling.ResourceBuiltin].Occupancy != 1 {
		t.Fatalf("calibration=%+v err=%v", states, err)
	}
}

func TestM6IndexesServeSchedulingAccessPaths(t *testing.T) {
	harness := newM5Harness(t)
	queries := map[string]string{
		"node_runs_ready_fifo_idx": `SELECT node_run_id FROM node_runs
			WHERE state='ready' ORDER BY ready_at, project_id, node_run_id LIMIT 100`,
		"node_attempts_scheduling_inflight_idx": `SELECT attempt_id FROM node_attempts
			WHERE state IN ('queued','running')
			ORDER BY project_id, attempt_id LIMIT 100`,
		"node_task_outbox_relay_idx": `SELECT task_id FROM node_task_outbox
			WHERE resource_class='builtin' AND published_at IS NULL AND available_at <= clock_timestamp()
			ORDER BY available_at, task_id LIMIT 100`,
	}
	for index, query := range queries {
		t.Run(index, func(t *testing.T) {
			tx, err := harness.client.Pool().Begin(harness.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(harness.ctx)
			mustExec(t, harness.ctx, tx, `SET LOCAL enable_seqscan=off`)
			rows, err := tx.Query(harness.ctx, "EXPLAIN (COSTS OFF) "+query)
			if err != nil {
				t.Fatal(err)
			}
			var plan []string
			for rows.Next() {
				var line string
				if err = rows.Scan(&line); err != nil {
					t.Fatal(err)
				}
				plan = append(plan, line)
			}
			rows.Close()
			joined := strings.Join(plan, "\n")
			if !strings.Contains(joined, index) || strings.Contains(joined, "Seq Scan") {
				t.Fatalf("query did not use %s:\n%s", index, joined)
			}
		})
	}
}

func m6Settings(configuration config.Config) scheduling.Settings {
	return scheduling.Settings{
		CandidateBatch:              configuration.Scheduler.RedisCandidateBatch,
		AdmissionConcurrency:        configuration.Scheduler.AdmissionConcurrency,
		CapacityCalibrationInterval: configuration.Scheduler.CapacityCalibrationInterval.Duration(),
		ReadyReconcileInterval:      configuration.Scheduler.ReadyReconcileInterval.Duration(),
		ReconcileLease:              configuration.Scheduler.ReconcileLease.Duration(),
		ReservationTTL:              configuration.Scheduler.ReservationTTL.Duration(),
		IdlePoll:                    configuration.Scheduler.IdlePoll.Duration(), IdlePollMax: configuration.Scheduler.IdlePollMax.Duration(),
		TopicWindow: scheduling.TopicWindowPolicy{
			BufferDuration: configuration.Scheduler.TopicQueueBuffer.Duration(),
			SampleInterval: configuration.Scheduler.CapacityCalibrationInterval.Duration(),
			EWMAAlpha:      configuration.Scheduler.TopicEWMAAlpha,
			Minimum: map[scheduling.ResourceClass]int{
				scheduling.ResourceBuiltin: configuration.Scheduler.BuiltinMinQueue,
				scheduling.ResourceSandbox: configuration.Scheduler.SandboxMinQueue,
			},
			Maximum: map[scheduling.ResourceClass]int{
				scheduling.ResourceBuiltin: configuration.Scheduler.BuiltinMaxQueue,
				scheduling.ResourceSandbox: configuration.Scheduler.SandboxMaxQueue,
			},
		},
		Memory: scheduling.MemoryPolicy{HighWatermark: configuration.Scheduler.MemoryHighWatermark, ResumeWatermark: configuration.Scheduler.MemoryResumeWatermark},
	}
}

func localM6Config(t *testing.T) config.Config {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(config.LoadOptions{Directory: filepath.Join(root, "configs"), Profile: "local"})
	if err != nil {
		t.Fatal(err)
	}
	return configuration
}

func createReadyTasksForProject(t *testing.T, harness *m5Harness, projectID, principalID string, count int) {
	t.Helper()
	workflow, _, diagnostics, err := harness.definitions.CreateWorkflow(harness.ctx, definition.CreateWorkflowCommand{
		ProjectID: projectID, PrincipalID: principalID, Name: "M6 Candidate Window",
		IRJSON: singleCodeIR(), IdempotencyKey: "m6-create-" + newID(t),
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	snapshot, diagnostics, err := harness.definitions.CompileDraftTestSnapshot(harness.ctx, projectID, principalID, workflow.ID, 1)
	assertNoDefinitionFailure(t, diagnostics, err)
	creator := runtimepkg.NewBuiltinRunCreator(harness.store, harness.access)
	for index := 0; index < count; index++ {
		run, createErr := creator.TestDraft(harness.ctx, runtimepkg.TestDraftRunCommand{
			ProjectID: projectID, PrincipalID: principalID, WorkflowID: workflow.ID,
			SnapshotID: snapshot.ID, DraftRevisionNumber: 1, WorkflowInput: json.RawMessage(`{"value":7}`),
			DeadlineAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "m6-run-" + newID(t), TraceID: "m6-window-" + newID(t),
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		var event eventing.RuntimeEvent
		queryErr := harness.client.Pool().QueryRow(harness.ctx, `
			SELECT message_version, event_id::text, project_id::text, run_id::text,
			       aggregate_type, aggregate_id::text, event_type, occurred_at, trace_id
			FROM outbox_events WHERE project_id=$1 AND event_type=$2 AND aggregate_id=$3`,
			projectID, eventing.RunCreated, run.ID).Scan(&event.MessageVersion, &event.EventID,
			&event.ProjectID, &event.RunID, &event.AggregateType, &event.AggregateID,
			&event.EventType, &event.OccurredAt, &event.TraceID)
		if queryErr != nil {
			t.Fatal(queryErr)
		}
		if consumeErr := harness.consumer.Consume(harness.ctx, event); consumeErr != nil {
			t.Fatal(consumeErr)
		}
	}
}

func newM5OnlyHarness(t *testing.T) (*m5Harness, string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	dsn := os.Getenv("EVALFROG_INTEGRATION_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://evalfrog:evalfrog@localhost:15432/evalfrog?sslmode=disable"
	}
	t.Setenv("EVALFROG_POSTGRES_DSN", dsn)
	configuration, err := config.Load(config.LoadOptions{Directory: filepath.Join(root, "configs"), Profile: "local"})
	if err != nil {
		t.Fatal(err)
	}
	configuration.Postgres.Schema = "m6_upgrade_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	client, err := postgres.Open(ctx, configuration.Postgres)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	previousDirectory := t.TempDir()
	for _, name := range []string{"000001_m3_definition_lifecycle.up.sql", "000002_m5_runtime_eventing.up.sql"} {
		content, readErr := os.ReadFile(filepath.Join(root, "migrations", name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if writeErr := os.WriteFile(filepath.Join(previousDirectory, name), content, 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	runner := migrations.Runner{Pool: client.Pool(), Schema: configuration.Postgres.Schema,
		Directory: previousDirectory, LockTimeout: 5 * time.Second}
	if err = runner.Up(ctx); err != nil {
		client.Close()
		cancel()
		t.Fatal(err)
	}
	// Runtime code at the current revision writes trace_id. This compatibility
	// column lets the fixture create M5-shaped rows before the test explicitly
	// removes it and upgrades from M5 through the current migration set.
	if _, err = client.Pool().Exec(ctx, `ALTER TABLE workflow_runs ADD COLUMN trace_id TEXT NOT NULL DEFAULT 'm5-compatibility'`); err != nil {
		client.Close()
		cancel()
		t.Fatal(err)
	}
	base := &m3Harness{ctx: ctx, cancel: cancel, client: client, store: postgres.NewStore(client.Pool()),
		projectID: newID(t), principalID: newID(t), executionID: newID(t),
		token: "m6-upgrade-" + uuid.NewString(), schema: configuration.Postgres.Schema}
	base.access = access.NewService(base.store)
	base.definitions = definition.NewBuiltinService(base.store, base.access, resources.NewResolver(base.store, base.access))
	base.seedProject(t, base.projectID, base.principalID, base.executionID, base.token, allPermissions())
	consumer, err := enginepkg.NewConsumer(base.store)
	if err != nil {
		t.Fatal(err)
	}
	harness := &m5Harness{m3Harness: base, creator: runtimepkg.NewBuiltinRunCreator(base.store, base.access),
		consumer: consumer, coordinator: attempt.NewBuiltinCoordinator(base.store)}
	t.Cleanup(func() {
		_, _ = client.Pool().Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{base.schema}.Sanitize()+" CASCADE")
		client.Close()
		cancel()
	})
	return harness, root
}

func seedM5RuntimeNodes(t *testing.T, harness *m5Harness, run runtimepkg.WorkflowRunRecord, snapshot definition.ExecutionSnapshot) {
	t.Helper()
	var graph dsl.Document
	if err := json.Unmarshal(snapshot.DSLJSON, &graph); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := harness.client.Pool().Exec(harness.ctx, `
		UPDATE workflow_runs SET state='running', state_version=state_version+1,
		       execution_node_ids=$1, updated_at=$2
		WHERE project_id=$3 AND run_id=$4`, executionNodeIDs(graph.Nodes), now, harness.projectID, run.ID); err != nil {
		t.Fatal(err)
	}
	for _, node := range graph.Nodes {
		state, activated := "pending", false
		var readyAt any
		if node.Kind == dsl.KindControl && node.Operation.Type == "control.start" {
			state, activated = "succeeded", true
		}
		if node.Kind == dsl.KindTask {
			state, activated, readyAt = "ready", true, now
		}
		if _, err := harness.client.Pool().Exec(harness.ctx, `
			INSERT INTO node_runs (
			  node_run_id, project_id, run_id, execution_node_id, kind, state,
			  state_version, activated, next_attempt_kind, priority, ready_at,
			  created_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,1,$7,'initial',0,$8,$9,$9)`,
			newID(t), harness.projectID, run.ID, node.ID, node.Kind, state, activated, readyAt, now); err != nil {
			t.Fatal(err)
		}
	}
}

func executionNodeIDs(nodes []dsl.Node) []string {
	result := make([]string, len(nodes))
	for index, node := range nodes {
		result[index] = string(node.ID)
	}
	return result
}

var _ context.Context
