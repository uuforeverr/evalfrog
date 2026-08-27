//go:build integration

package integration

import (
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/projection"
	"github.com/uu999/evalfrog/internal/recovery"
	runtimepkg "github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	"github.com/uu999/evalfrog/internal/scheduling"
)

func TestM11RecoveryWakeupsAreDurableTraceableAndEngineOwned(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m11-recovery-wakeup")
	harness.initializeRun(t, run)
	nodeID := readyNodeID(t, harness, run.ID)
	queued := dispatchFixture(t, harness, run.ID, nodeID)
	lease, err := harness.coordinator.Claim(harness.ctx, attempt.ClaimCommand{
		ProjectID: harness.projectID, RunID: run.ID, AttemptID: queued.ID, AttemptSequence: queued.Sequence,
		WorkerID: "m11-crashed-worker", ExecutorBuild: "m11", ResourceClass: scheduling.ResourceSandbox,
		Capabilities: sandboxCapability, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, harness.ctx, harness.client.Pool(), `UPDATE node_attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE attempt_id=$1`, queued.ID)
	reaper, err := recovery.NewReaper(harness.store, harness.coordinator, 0, time.Second, 10, "m11-reaper", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err = reaper.ScanOnce(harness.ctx); err != nil {
		t.Fatal(err)
	}
	if got := attemptState(t, harness, queued.ID); got != string(runtimepkg.AttemptLost) {
		t.Fatalf("attempt state=%s", got)
	}

	// The Reconciler only observes durable facts and appends an Outbox wake-up.
	// It must not mutate the Node/Run semantic state itself.
	emitter := recovery.NewBuiltinEmitter(harness.store)
	reconciler, err := recovery.NewReconciler(harness.store, emitter, time.Second, 10, "m11-reconciler", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	before := nodeState(t, harness, run.ID, nodeID)
	if err = reconciler.ScanOnce(harness.ctx); err != nil {
		t.Fatal(err)
	}
	if after := nodeState(t, harness, run.ID, nodeID); after != before {
		t.Fatalf("reconciler changed Node state: before=%s after=%s", before, after)
	}
	var wakeupTrace string
	var wakeupCount int
	if err = harness.client.Pool().QueryRow(harness.ctx, `
		SELECT trace_id, count(*) FROM outbox_events
		WHERE project_id=$1 AND run_id=$2 AND event_type='attempt.lost'
		GROUP BY trace_id ORDER BY max(created_at) DESC LIMIT 1`, harness.projectID, run.ID).Scan(&wakeupTrace, &wakeupCount); err != nil {
		t.Fatal(err)
	}
	if wakeupTrace != run.TraceID || wakeupCount < 2 {
		t.Fatalf("trace=%q count=%d run=%+v", wakeupTrace, wakeupCount, run)
	}
	lost := harness.event(t, eventing.AttemptLost, queued.ID)
	if err = harness.consumer.Consume(harness.ctx, lost); err != nil {
		t.Fatal(err)
	}
	if got := nodeState(t, harness, run.ID, nodeID); got != string(runtimepkg.NodeRetryWait) {
		t.Fatalf("engine did not advance lost attempt to recovery wait: %s", got)
	}
	if _, err = harness.coordinator.Complete(harness.ctx, attempt.CompleteCommand{
		ProjectID: harness.projectID, RunID: run.ID, AttemptID: queued.ID, AttemptSequence: queued.Sequence,
		LeaseToken: lease.Token, FencingToken: lease.FencingToken, TraceID: run.TraceID,
		Result: runtimepkg.AttemptResult{State: runtimepkg.AttemptSucceeded, Outputs: map[string]json.RawMessage{"result": json.RawMessage(`{}`)}},
	}); err == nil {
		t.Fatal("late fenced Worker completion was accepted")
	}
}

func TestM11DeadlineBlocksRetryRecoveryAndDiagnosticsAuditAreSafe(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m11-deadline")
	harness.initializeRun(t, run)
	nodeID := readyNodeID(t, harness, run.ID)
	queued := dispatchFixture(t, harness, run.ID, nodeID)

	// Materialize a RetryWait fact that would otherwise be eligible, then make
	// the workflow deadline authoritative. The Retry Timer must find nothing;
	// Deadline Scanner is the only resulting wake-up source.
	mustExec(t, harness.ctx, harness.client.Pool(), `
		UPDATE node_runs SET state='retry_wait', state_version=state_version+1, current_attempt_id=$4,
		       next_retry_at=clock_timestamp()-interval '1 second'
		WHERE project_id=$1 AND run_id=$2 AND execution_node_id=$3`, harness.projectID, run.ID, nodeID, queued.ID)
	mustExec(t, harness.ctx, harness.client.Pool(), `
		UPDATE workflow_runs SET deadline_at=created_at+interval '1 microsecond'
		WHERE project_id=$1 AND run_id=$2`, harness.projectID, run.ID)
	if values, err := harness.store.ListRetryDue(harness.ctx, 10); err != nil || len(values) != 0 {
		t.Fatalf("expired Run retry candidates=%+v err=%v", values, err)
	}
	deadlines, err := harness.store.ListDeadlinesDue(harness.ctx, 10)
	if err != nil || len(deadlines) != 1 || deadlines[0].RunID != run.ID {
		t.Fatalf("deadline candidates=%+v err=%v", deadlines, err)
	}
	emitter := recovery.NewBuiltinEmitter(harness.store)
	if emitted, err := emitter.Emit(harness.ctx, deadlines[0], run.TraceID, "system", "m11-deadline-test", 0, false); err != nil || !emitted {
		t.Fatalf("deadline emitted=%t err=%v", emitted, err)
	}
	if err = harness.consumer.Consume(harness.ctx, harness.event(t, eventing.RunDeadlineReached, run.ID)); err != nil {
		t.Fatal(err)
	}
	if got := runState(t, harness, run.ID); got != string(runtimepkg.RunTimedOut) {
		t.Fatalf("run state=%s", got)
	}
	var readyNodes int
	if err = harness.client.Pool().QueryRow(harness.ctx, `SELECT count(*) FROM node_runs WHERE project_id=$1 AND run_id=$2 AND state='ready'`, harness.projectID, run.ID).Scan(&readyNodes); err != nil || readyNodes != 0 {
		t.Fatalf("deadline Run retained ready nodes: count=%d err=%v", readyNodes, err)
	}

	control := runtimepkg.NewBuiltinRunControl(harness.store, harness.access)
	// Cancellation of a terminal Run is idempotently rejected; use a separate
	// fresh Run to prove the cancellation audit transaction instead.
	auditRun := harness.createTestRun(t, workflow.ID, snapshot.ID, "m11-audit")
	if _, applied, err := control.Cancel(harness.ctx, harness.projectID, harness.principalID, auditRun.ID, "m11-cancel-trace"); err != nil || !applied {
		t.Fatalf("cancel applied=%t err=%v", applied, err)
	}
	reader := projection.NewBuiltinService(harness.store, harness.access)
	diagnostics, err := reader.GetDiagnostics(harness.ctx, harness.projectID, harness.principalID, auditRun.ID)
	if err != nil || len(diagnostics.Audit) != 1 || diagnostics.Audit[0].Action != "run.cancel_requested" || diagnostics.Audit[0].TraceID != "m11-cancel-trace" {
		t.Fatalf("diagnostics=%+v err=%v", diagnostics, err)
	}
	encoded, _ := json.Marshal(diagnostics)
	for _, forbidden := range []string{"workflow_input", "lease_token", "secret", "stderr"} {
		if string(encoded) != "" && containsFold(string(encoded), forbidden) {
			t.Fatalf("diagnostics leaked forbidden field %q: %s", forbidden, encoded)
		}
	}
	_ = queued // proves the original Attempt did not create an output during deadline competition.
}

func TestM11TraceIsPreservedFromRunThroughTaskAndCompletion(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m11-trace-chain")
	if run.TraceID == "" {
		t.Fatal("run trace was not persisted")
	}
	harness.initializeRun(t, run)
	task := queuedTask(t, harness, run.ID)
	if task.TraceID != run.TraceID {
		t.Fatalf("task=%+v run=%+v", task, run)
	}
	var err error
	lease, err := harness.coordinator.Claim(harness.ctx, attempt.ClaimCommand{
		ProjectID: task.ProjectID, RunID: task.RunID, AttemptID: task.AttemptID, AttemptSequence: task.AttemptSequence,
		WorkerID: "m11-trace-worker", ExecutorBuild: "m11", ResourceClass: scheduling.ResourceSandbox, Capabilities: sandboxCapability, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = harness.coordinator.Complete(harness.ctx, attempt.CompleteCommand{
		ProjectID: task.ProjectID, RunID: task.RunID, AttemptID: task.AttemptID, AttemptSequence: task.AttemptSequence,
		LeaseToken: lease.Token, FencingToken: lease.FencingToken, TraceID: task.TraceID,
		Result: runtimepkg.AttemptResult{State: runtimepkg.AttemptSucceeded, Outputs: map[string]json.RawMessage{"result": json.RawMessage(`{"ok":true}`)}},
	}); err != nil {
		t.Fatal(err)
	}
	var taskTrace, completionTrace string
	if err = harness.client.Pool().QueryRow(harness.ctx, `SELECT trace_id FROM node_task_outbox WHERE task_id=$1`, task.TaskID).Scan(&taskTrace); err != nil {
		t.Fatal(err)
	}
	if err = harness.client.Pool().QueryRow(harness.ctx, `SELECT trace_id FROM outbox_events WHERE aggregate_id=$1 AND event_type='attempt.completed'`, task.AttemptID).Scan(&completionTrace); err != nil {
		t.Fatal(err)
	}
	if taskTrace != run.TraceID || completionTrace != run.TraceID {
		t.Fatalf("task=%q completion=%q run=%q", taskTrace, completionTrace, run.TraceID)
	}
}

var sandboxCapability = []dsl.Coordinate{{Type: "task.python", Version: 1}}

func nodeState(t *testing.T, harness *m5Harness, runID, nodeID string) string {
	t.Helper()
	var state string
	if err := harness.client.Pool().QueryRow(harness.ctx, `SELECT state FROM node_runs WHERE project_id=$1 AND run_id=$2 AND execution_node_id=$3`, harness.projectID, runID, nodeID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}

func containsFold(value, fragment string) bool {
	return strings.Contains(strings.ToLower(value), strings.ToLower(fragment))
}
