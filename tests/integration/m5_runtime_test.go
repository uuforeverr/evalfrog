//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	runtimepkg "github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	enginepkg "github.com/uu999/evalfrog/internal/runtime/engine"
	"github.com/uu999/evalfrog/internal/scheduling"
)

type m5Harness struct {
	*m3Harness
	creator     runtimepkg.RunCreator
	consumer    enginepkg.Consumer
	coordinator attempt.Coordinator
}

func newM5Harness(t *testing.T) *m5Harness {
	t.Helper()
	base := newM3Harness(t)
	consumer, err := enginepkg.NewConsumer(base.store)
	if err != nil {
		t.Fatal(err)
	}
	return &m5Harness{
		m3Harness:   base,
		creator:     runtimepkg.NewBuiltinRunCreator(base.store, base.access),
		consumer:    consumer,
		coordinator: attempt.NewBuiltinCoordinator(base.store),
	}
}

func (harness *m5Harness) createCodeWorkflow(t *testing.T, publish bool) (definition.Workflow, definition.ExecutionSnapshot) {
	t.Helper()
	workflow, _, diagnostics, err := harness.definitions.CreateWorkflow(harness.ctx, definition.CreateWorkflowCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, Name: "M5 Runtime",
		IRJSON: singleCodeIR(), IdempotencyKey: "m5-create-workflow-" + newID(t),
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	snapshot, diagnostics, err := harness.definitions.CompileDraftTestSnapshot(harness.ctx, harness.projectID, harness.principalID, workflow.ID, 1)
	assertNoDefinitionFailure(t, diagnostics, err)
	if publish {
		_, publishedSnapshot, publishDiagnostics, publishErr := harness.definitions.Publish(harness.ctx, definition.PublishCommand{
			ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID,
			ExpectedRevision: 1, ChangeLog: "M5", IdempotencyKey: "m5-publish-" + newID(t),
		})
		assertNoDefinitionFailure(t, publishDiagnostics, publishErr)
		snapshot = publishedSnapshot
	}
	resolved, resolveErr := harness.definitions.ResolveDraftTestSnapshot(harness.ctx, harness.projectID, harness.principalID, workflow.ID, 1)
	if resolveErr != nil || resolved.DefinitionHash != snapshot.DefinitionHash {
		t.Fatalf("resolved test snapshot=%+v err=%v", resolved, resolveErr)
	}
	return workflow, snapshot
}

func (harness *m5Harness) createEntryRefCodeWorkflow(t *testing.T) (definition.Workflow, definition.ExecutionSnapshot) {
	t.Helper()
	workflow, _, diagnostics, err := harness.definitions.CreateWorkflow(harness.ctx, definition.CreateWorkflowCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, Name: "M5 Entry Ref",
		IRJSON: entryRefCodeIR(), IdempotencyKey: "m5-create-entry-ref-" + newID(t),
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	snapshot, diagnostics, err := harness.definitions.CompileDraftTestSnapshot(harness.ctx, harness.projectID, harness.principalID, workflow.ID, 1)
	assertNoDefinitionFailure(t, diagnostics, err)
	return workflow, snapshot
}

func (harness *m5Harness) createTestRun(t *testing.T, workflowID, snapshotID, key string) runtimepkg.WorkflowRunRecord {
	t.Helper()
	run, err := harness.creator.TestDraft(harness.ctx, runtimepkg.TestDraftRunCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflowID,
		SnapshotID: snapshotID, DraftRevisionNumber: 1, WorkflowInput: json.RawMessage(`{"value":7}`),
		DeadlineAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: key, TraceID: "trace-" + key,
	})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func (harness *m5Harness) initializeRun(t *testing.T, run runtimepkg.WorkflowRunRecord) eventing.RuntimeEvent {
	t.Helper()
	event := harness.event(t, eventing.RunCreated, run.ID)
	if err := harness.consumer.Consume(harness.ctx, event); err != nil {
		t.Fatal(err)
	}
	return event
}

func (harness *m5Harness) event(t *testing.T, eventType eventing.RuntimeEventType, aggregateID string) eventing.RuntimeEvent {
	t.Helper()
	var value eventing.RuntimeEvent
	err := harness.client.Pool().QueryRow(harness.ctx, `
		SELECT message_version, event_id::text, project_id::text, run_id::text,
		       aggregate_type, aggregate_id::text, event_type, occurred_at, trace_id
		FROM outbox_events WHERE project_id=$1 AND event_type=$2 AND aggregate_id=$3
		ORDER BY created_at DESC, event_id DESC LIMIT 1`, harness.projectID, eventType, aggregateID).
		Scan(&value.MessageVersion, &value.EventID, &value.ProjectID, &value.RunID,
			&value.AggregateType, &value.AggregateID, &value.EventType, &value.OccurredAt, &value.TraceID)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestM5CreateRunBindsValidatedImmutableSourceAndOnlyWritesPendingIdentity(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	if _, err := harness.creator.CreateProduction(harness.ctx, runtimepkg.ProductionRunCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID,
		WorkflowInput: json.RawMessage(`{}`), DeadlineAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "m5-unpublished-production", TraceID: "trace-unpublished",
	}); !errors.Is(err, runtimepkg.ErrRunWorkflowNotPublished) {
		t.Fatalf("unpublished production source error=%v", err)
	}
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m5-test-create-01")
	if run.State != runtimepkg.RunPending || run.Definition.Source != runtimepkg.DefinitionDraftSnapshot || len(run.ExecutionNodeIDs) != 0 {
		t.Fatalf("pending run=%+v", run)
	}
	for table, want := range map[string]int{"workflow_runs": 1, "node_runs": 0, "node_attempts": 0, "outbox_events": 1, "inbox_events": 0} {
		assertCount(t, harness.m3Harness, table, want)
	}
	replayed, err := harness.creator.TestDraft(harness.ctx, runtimepkg.TestDraftRunCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID,
		SnapshotID: snapshot.ID, DraftRevisionNumber: 1, WorkflowInput: json.RawMessage(`{"value":7}`),
		DeadlineAt: run.DeadlineAt, IdempotencyKey: "m5-test-create-01", TraceID: "trace-replay",
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ID != run.ID {
		t.Fatalf("idempotent TestDraft changed run identity: %s != %s", replayed.ID, run.ID)
	}
	_, err = harness.creator.TestDraft(harness.ctx, runtimepkg.TestDraftRunCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID,
		SnapshotID: snapshot.ID, DraftRevisionNumber: 1, WorkflowInput: json.RawMessage(`{"different":true}`),
		DeadlineAt: run.DeadlineAt, IdempotencyKey: "m5-test-create-01", TraceID: "trace-reused",
	})
	if !errors.Is(err, runtimepkg.ErrRunIdempotencyReuse) {
		t.Fatalf("idempotency reuse error=%v", err)
	}
	_, publishedSnapshot, diagnostics, err := harness.definitions.Publish(harness.ctx, definition.PublishCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID,
		ExpectedRevision: 1, ChangeLog: "publish", IdempotencyKey: "m5-publish-active-01",
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	production, err := harness.creator.CreateProduction(harness.ctx, runtimepkg.ProductionRunCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID,
		WorkflowInput: json.RawMessage(`{}`), DeadlineAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "m5-production-create-01", TraceID: "trace-production",
	})
	if err != nil || production.Definition.Source != runtimepkg.DefinitionPublishedVersion || production.Definition.PublishedVersionID == "" || production.Definition.SnapshotID != publishedSnapshot.ID {
		t.Fatalf("production run=%+v err=%v", production, err)
	}
	if _, err = harness.creator.TestDraft(harness.ctx, runtimepkg.TestDraftRunCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID,
		SnapshotID: newID(t), DraftRevisionNumber: 1, WorkflowInput: json.RawMessage(`{}`), DeadlineAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "m5-invalid-snapshot", TraceID: "trace-invalid-snapshot",
	}); !errors.Is(err, runtimepkg.ErrRunSourceInvalid) {
		t.Fatalf("invalid test snapshot error=%v", err)
	}
}

func TestM5RunInitializationCrashRollsBackWholeGraphAndReplayCreatesOneGraph(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m5-init-crash-01")
	event := harness.event(t, eventing.RunCreated, run.ID)
	mustExec(t, harness.ctx, harness.client.Pool(), `
		CREATE FUNCTION fail_second_node_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		  IF (SELECT count(*) FROM node_runs WHERE run_id=NEW.run_id) >= 1 THEN
		    RAISE EXCEPTION 'injected second node insert failure';
		  END IF;
		  RETURN NEW;
		END; $$`)
	mustExec(t, harness.ctx, harness.client.Pool(), `
		CREATE TRIGGER inject_node_insert_failure BEFORE INSERT ON node_runs
		FOR EACH ROW EXECUTE FUNCTION fail_second_node_insert()`)
	if err := harness.consumer.Consume(harness.ctx, event); err == nil {
		t.Fatal("injected initialization failure unexpectedly committed")
	}
	assertCount(t, harness.m3Harness, "node_runs", 0)
	assertCount(t, harness.m3Harness, "inbox_events", 0)
	assertState(t, harness, run.ID, runtimepkg.RunPending)
	mustExec(t, harness.ctx, harness.client.Pool(), `DROP TRIGGER inject_node_insert_failure ON node_runs`)
	mustExec(t, harness.ctx, harness.client.Pool(), `DROP FUNCTION fail_second_node_insert()`)
	if err := harness.consumer.Consume(harness.ctx, event); err != nil {
		t.Fatal(err)
	}
	assertCount(t, harness.m3Harness, "node_runs", 3)
	assertCount(t, harness.m3Harness, "inbox_events", 1)
	assertState(t, harness, run.ID, runtimepkg.RunRunning)
	for index := 0; index < 100; index++ {
		if err := harness.consumer.Consume(harness.ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	assertCount(t, harness.m3Harness, "node_runs", 3)
	assertCount(t, harness.m3Harness, "inbox_events", 1)
}

func TestM5MultipleEngineInstancesCompeteByInboxAndRunLock(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m5-concurrent-engine")
	event := harness.event(t, eventing.RunCreated, run.ID)
	const workers = 12
	errorsFound := make(chan error, workers)
	var start sync.WaitGroup
	start.Add(1)
	for index := 0; index < workers; index++ {
		go func() {
			start.Wait()
			errorsFound <- harness.consumer.Consume(harness.ctx, event)
		}()
	}
	start.Done()
	for index := 0; index < workers; index++ {
		if err := <-errorsFound; err != nil {
			t.Fatal(err)
		}
	}
	assertCount(t, harness.m3Harness, "node_runs", 3)
	assertCount(t, harness.m3Harness, "inbox_events", 1)
}

func TestM5AttemptCompletionIsTwoAtomicTransactionsAndOnlyCurrentBecomesEffective(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m5-attempt-two-phase")
	harness.initializeRun(t, run)
	nodeID := readyNodeID(t, harness, run.ID)
	queued := dispatchFixture(t, harness, run.ID, nodeID)
	var err error
	if err != nil {
		t.Fatal(err)
	}
	lease, err := harness.coordinator.Claim(harness.ctx, attempt.ClaimCommand{
		ProjectID: harness.projectID, RunID: run.ID, AttemptID: queued.ID, AttemptSequence: queued.Sequence,
		WorkerID: "worker-1", ExecutorBuild: "test-build", ResourceClass: scheduling.ResourceSandbox,
		Capabilities: []dsl.Coordinate{{Type: "task.python", Version: 1}}, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, harness.ctx, harness.client.Pool(), `
		CREATE FUNCTION fail_completion_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
		  IF NEW.event_type='attempt.completed' THEN RAISE EXCEPTION 'injected outbox failure'; END IF;
		  RETURN NEW;
		END; $$`)
	mustExec(t, harness.ctx, harness.client.Pool(), `
		CREATE TRIGGER inject_completion_outbox_failure BEFORE INSERT ON outbox_events
		FOR EACH ROW EXECUTE FUNCTION fail_completion_outbox()`)
	complete := attempt.CompleteCommand{
		ProjectID: harness.projectID, RunID: run.ID, AttemptID: queued.ID, AttemptSequence: queued.Sequence,
		LeaseToken: lease.Token, FencingToken: lease.FencingToken,
		Result:  runtimepkg.AttemptResult{State: runtimepkg.AttemptSucceeded, Outputs: map[string]json.RawMessage{"result": json.RawMessage(`{"ok":true}`)}},
		TraceID: "trace-attempt-complete",
	}
	if _, err = harness.coordinator.Complete(harness.ctx, complete); err == nil {
		t.Fatal("injected completion outbox failure unexpectedly committed")
	}
	assertAttemptState(t, harness, queued.ID, runtimepkg.AttemptRunning)
	assertCount(t, harness.m3Harness, "node_output_values", 0)
	mustExec(t, harness.ctx, harness.client.Pool(), `DROP TRIGGER inject_completion_outbox_failure ON outbox_events`)
	mustExec(t, harness.ctx, harness.client.Pool(), `DROP FUNCTION fail_completion_outbox()`)
	applied, err := harness.coordinator.Complete(harness.ctx, complete)
	if err != nil || !applied {
		t.Fatalf("complete applied=%v err=%v", applied, err)
	}
	assertAttemptState(t, harness, queued.ID, runtimepkg.AttemptSucceeded)
	assertCount(t, harness.m3Harness, "node_output_values", 1)
	assertNodeEffective(t, harness, run.ID, nodeID, runtimepkg.NodeRunning, "")
	completion := harness.event(t, eventing.AttemptCompleted, queued.ID)
	if err = harness.consumer.Consume(harness.ctx, completion); err != nil {
		t.Fatal(err)
	}
	assertNodeEffective(t, harness, run.ID, nodeID, runtimepkg.NodeSucceeded, queued.ID)
	assertState(t, harness, run.ID, runtimepkg.RunSucceeded)
	for index := 0; index < 100; index++ {
		if err = harness.consumer.Consume(harness.ctx, completion); err != nil {
			t.Fatal(err)
		}
	}
	applied, err = harness.coordinator.Complete(harness.ctx, complete)
	if err != nil || applied {
		t.Fatalf("idempotent completion applied=%v err=%v", applied, err)
	}
}

func TestM5DuplicateOutOfOrderAndLateEventsCannotReplaceCurrentAttempt(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m5-late-events")
	harness.initializeRun(t, run)
	nodeID := readyNodeID(t, harness, run.ID)
	first := dispatchFixture(t, harness, run.ID, nodeID)
	var err error
	lease, err := harness.coordinator.Claim(harness.ctx, attempt.ClaimCommand{
		ProjectID: harness.projectID, RunID: run.ID, AttemptID: first.ID, AttemptSequence: first.Sequence,
		WorkerID: "worker-lost", ExecutorBuild: "test-build", ResourceClass: scheduling.ResourceSandbox,
		Capabilities: []dsl.Coordinate{{Type: "task.python", Version: 1}}, LeaseDuration: time.Minute,
	})
	if err != nil || lease.Token == "" {
		t.Fatalf("claim=%+v err=%v", lease, err)
	}
	mustExec(t, harness.ctx, harness.client.Pool(), `UPDATE node_attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE attempt_id=$1`, first.ID)
	if applied, markErr := harness.coordinator.MarkExpiredLost(harness.ctx, attempt.MarkLostCommand{
		ProjectID: harness.projectID, RunID: run.ID, AttemptID: first.ID,
		AttemptSequence: first.Sequence, TraceID: "trace-lost",
	}); markErr != nil || !applied {
		t.Fatalf("mark lost applied=%v err=%v", applied, markErr)
	}
	lost := harness.event(t, eventing.AttemptLost, first.ID)
	if err = harness.consumer.Consume(harness.ctx, lost); err != nil {
		t.Fatal(err)
	}
	assertNodeState(t, harness, run.ID, nodeID, runtimepkg.NodeRetryWait)
	retry := eventing.RuntimeEvent{
		MessageVersion: 1, EventID: newID(t), ProjectID: harness.projectID, RunID: run.ID,
		AggregateType: eventing.NodeAttemptAggregate, AggregateID: first.ID, EventType: eventing.RetryDue,
		OccurredAt: lost.OccurredAt.Add(2 * time.Second), TraceID: "trace-retry-due",
	}
	if err = harness.consumer.Consume(harness.ctx, retry); err != nil {
		t.Fatal(err)
	}
	second := dispatchFixture(t, harness, run.ID, nodeID)
	stale := lost
	stale.EventID = newID(t)
	stale.OccurredAt = stale.OccurredAt.Add(3 * time.Second)
	stale.TraceID = "trace-stale-lost"
	if err = harness.consumer.Consume(harness.ctx, stale); err != nil {
		t.Fatal(err)
	}
	assertNodeCurrent(t, harness, run.ID, nodeID, runtimepkg.NodeQueued, second.ID)
	early := eventing.RuntimeEvent{
		MessageVersion: 1, EventID: newID(t), ProjectID: harness.projectID, RunID: run.ID,
		AggregateType: eventing.NodeAttemptAggregate, AggregateID: second.ID, EventType: eventing.AttemptCompleted,
		OccurredAt: time.Now().UTC(), TraceID: "trace-early-completion",
	}
	if err = harness.consumer.Consume(harness.ctx, early); err == nil {
		t.Fatal("completion signal before authoritative Attempt Result unexpectedly advanced")
	}
	var earlyInbox int
	if err = harness.client.Pool().QueryRow(harness.ctx, `SELECT count(*) FROM inbox_events WHERE event_id=$1`, early.EventID).Scan(&earlyInbox); err != nil || earlyInbox != 0 {
		t.Fatalf("failed early event left inbox fact count=%d err=%v", earlyInbox, err)
	}
}

func TestM5ConcurrentWorkersClaimExactlyOneLeaseAndStaleFencingIsRejected(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m5-claim-race")
	harness.initializeRun(t, run)
	nodeID := readyNodeID(t, harness, run.ID)
	queued := dispatchFixture(t, harness, run.ID, nodeID)
	var err error
	type claimResult struct {
		lease attempt.Lease
		err   error
	}
	results := make(chan claimResult, 8)
	var start sync.WaitGroup
	start.Add(1)
	for index := 0; index < 8; index++ {
		index := index
		go func() {
			start.Wait()
			lease, claimErr := harness.coordinator.Claim(harness.ctx, attempt.ClaimCommand{
				ProjectID: harness.projectID, RunID: run.ID, AttemptID: queued.ID,
				AttemptSequence: queued.Sequence, WorkerID: fmt.Sprintf("worker-%d", index),
				ExecutorBuild: "test-build", ResourceClass: scheduling.ResourceSandbox,
				Capabilities: []dsl.Coordinate{{Type: "task.python", Version: 1}}, LeaseDuration: time.Minute,
			})
			results <- claimResult{lease: lease, err: claimErr}
		}()
	}
	start.Done()
	winners := 0
	var winner attempt.Lease
	for index := 0; index < 8; index++ {
		result := <-results
		if result.err == nil {
			winners++
			winner = result.lease
		} else if !errors.Is(result.err, attempt.ErrStateConflict) {
			t.Fatalf("unexpected claim result: %v", result.err)
		}
	}
	if winners != 1 {
		t.Fatalf("claim winners=%d want=1", winners)
	}
	stale := attempt.CompleteCommand{
		ProjectID: harness.projectID, RunID: run.ID, AttemptID: queued.ID, AttemptSequence: queued.Sequence,
		LeaseToken: newID(t), FencingToken: winner.FencingToken, TraceID: "trace-stale-worker",
		Result: runtimepkg.AttemptResult{State: runtimepkg.AttemptSucceeded, Outputs: map[string]json.RawMessage{"result": json.RawMessage(`{}`)}},
	}
	if _, err = harness.coordinator.Complete(harness.ctx, stale); !errors.Is(err, attempt.ErrLeaseMismatch) {
		t.Fatalf("stale completion error=%v", err)
	}
	assertAttemptState(t, harness, queued.ID, runtimepkg.AttemptRunning)
}

func TestM5UnsupportedOperationFailsPendingRunBeforeNodeInitialization(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	var raw map[string]any
	if err := json.Unmarshal(snapshot.DSLJSON, &raw); err != nil {
		t.Fatal(err)
	}
	nodes := raw["nodes"].([]any)
	for _, value := range nodes {
		node := value.(map[string]any)
		operation := node["operation"].(map[string]any)
		if operation["type"] == "task.python" {
			operation["version"] = float64(99)
		}
	}
	mutatedDSL, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, harness.ctx, harness.client.Pool(), `
		ALTER TABLE workflow_execution_snapshots DISABLE TRIGGER workflow_execution_snapshots_immutable`)
	mustExec(t, harness.ctx, harness.client.Pool(), `
		UPDATE workflow_execution_snapshots SET dsl_json=$1 WHERE snapshot_id=$2`, mutatedDSL, snapshot.ID)
	mustExec(t, harness.ctx, harness.client.Pool(), `
		ALTER TABLE workflow_execution_snapshots ENABLE TRIGGER workflow_execution_snapshots_immutable`)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m5-unsupported-operation")
	event := harness.event(t, eventing.RunCreated, run.ID)
	if err = harness.consumer.Consume(harness.ctx, event); err != nil {
		t.Fatal(err)
	}
	assertState(t, harness, run.ID, runtimepkg.RunFailed)
	assertCount(t, harness.m3Harness, "node_runs", 0)
	var code string
	err = harness.client.Pool().QueryRow(harness.ctx, `SELECT termination_intent_json->'cause'->>'code' FROM workflow_runs WHERE run_id=$1`, run.ID).Scan(&code)
	if err != nil || code != "RUNTIME_OPERATION_UNSUPPORTED" {
		t.Fatalf("initialization failure code=%q err=%v", code, err)
	}
}

type recordingPublisher struct {
	mu     sync.Mutex
	events []eventing.RuntimeEvent
	fail   bool
}

func (publisher *recordingPublisher) PublishRuntimeEvent(_ context.Context, event eventing.RuntimeEvent) error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if publisher.fail {
		return errors.New("injected publish failure")
	}
	publisher.events = append(publisher.events, event)
	return nil
}

type failMarkRepository struct {
	eventing.OutboxRepository
	fail bool
}

func (repository *failMarkRepository) MarkOutboxPublished(ctx context.Context, eventID, token string) error {
	if repository.fail {
		repository.fail = false
		return errors.New("injected crash after publish")
	}
	return repository.OutboxRepository.MarkOutboxPublished(ctx, eventID, token)
}

func TestM5OutboxRelayRecoversBeforeAndAfterPublishCrash(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m5-relay-crash")
	publisher := &recordingPublisher{fail: true}
	relay, err := eventing.NewRelay(harness.store, publisher, "relay-before", 1, time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = relay.RelayOnce(harness.ctx); err == nil {
		t.Fatal("publish-before crash was not surfaced")
	}
	publisher.fail = false
	repository := &failMarkRepository{OutboxRepository: harness.store, fail: true}
	relay, err = eventing.NewRelay(repository, publisher, "relay-after", 1, time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = relay.RelayOnce(harness.ctx); err == nil {
		t.Fatal("publish-after crash was not surfaced")
	}
	if len(publisher.events) != 1 {
		t.Fatalf("published events=%d want=1", len(publisher.events))
	}
	mustExec(t, harness.ctx, harness.client.Pool(), `UPDATE outbox_events SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE event_id=$1`, publisher.events[0].EventID)
	relay, _ = eventing.NewRelay(harness.store, publisher, "relay-recovery", 1, time.Minute, 0)
	if count, relayErr := relay.RelayOnce(harness.ctx); relayErr != nil || count != 1 {
		t.Fatalf("recovery count=%d err=%v", count, relayErr)
	}
	if len(publisher.events) != 2 || publisher.events[0].EventID != publisher.events[1].EventID {
		t.Fatalf("at-least-once replay=%+v", publisher.events)
	}
	for _, event := range publisher.events {
		if err = harness.consumer.Consume(harness.ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	assertCount(t, harness.m3Harness, "inbox_events", 1)
	assertCount(t, harness.m3Harness, "node_runs", 3)
	assertState(t, harness, run.ID, runtimepkg.RunRunning)
}

func TestM5RuntimeIndexesServeFrozenAccessPaths(t *testing.T) {
	harness := newM5Harness(t)
	queries := map[string]string{
		"node_runs_retry_idx":     `SELECT node_run_id FROM node_runs WHERE state='retry_wait' AND next_retry_at <= clock_timestamp() ORDER BY next_retry_at, node_run_id LIMIT 100`,
		"node_attempts_lease_idx": `SELECT attempt_id FROM node_attempts WHERE state='running' AND lease_expires_at <= clock_timestamp() ORDER BY lease_expires_at, attempt_id LIMIT 100`,
		"outbox_events_relay_idx": `SELECT event_id FROM outbox_events WHERE published_at IS NULL AND available_at <= clock_timestamp() ORDER BY available_at, event_id LIMIT 100`,
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

func assertState(t *testing.T, harness *m5Harness, runID string, want runtimepkg.RunState) {
	t.Helper()
	var got runtimepkg.RunState
	if err := harness.client.Pool().QueryRow(harness.ctx, `SELECT state FROM workflow_runs WHERE project_id=$1 AND run_id=$2`, harness.projectID, runID).Scan(&got); err != nil || got != want {
		t.Fatalf("run state=%s want=%s err=%v", got, want, err)
	}
}

func assertAttemptState(t *testing.T, harness *m5Harness, attemptID string, want runtimepkg.AttemptState) {
	t.Helper()
	var got runtimepkg.AttemptState
	if err := harness.client.Pool().QueryRow(harness.ctx, `SELECT state FROM node_attempts WHERE project_id=$1 AND attempt_id=$2`, harness.projectID, attemptID).Scan(&got); err != nil || got != want {
		t.Fatalf("attempt state=%s want=%s err=%v", got, want, err)
	}
}

func assertNodeState(t *testing.T, harness *m5Harness, runID, nodeID string, want runtimepkg.NodeState) {
	t.Helper()
	var got runtimepkg.NodeState
	if err := harness.client.Pool().QueryRow(harness.ctx, `SELECT state FROM node_runs WHERE project_id=$1 AND run_id=$2 AND execution_node_id=$3`, harness.projectID, runID, nodeID).Scan(&got); err != nil || got != want {
		t.Fatalf("node state=%s want=%s err=%v", got, want, err)
	}
}

func assertNodeEffective(t *testing.T, harness *m5Harness, runID, nodeID string, wantState runtimepkg.NodeState, wantAttempt string) {
	t.Helper()
	var state runtimepkg.NodeState
	var effective *string
	err := harness.client.Pool().QueryRow(harness.ctx, `SELECT state, effective_attempt_id::text FROM node_runs WHERE project_id=$1 AND run_id=$2 AND execution_node_id=$3`, harness.projectID, runID, nodeID).Scan(&state, &effective)
	got := ""
	if effective != nil {
		got = *effective
	}
	if err != nil || state != wantState || got != wantAttempt {
		t.Fatalf("node state=%s/%s effective=%s/%s err=%v", state, wantState, got, wantAttempt, err)
	}
}

func assertNodeCurrent(t *testing.T, harness *m5Harness, runID, nodeID string, wantState runtimepkg.NodeState, wantAttempt string) {
	t.Helper()
	var state runtimepkg.NodeState
	var current string
	err := harness.client.Pool().QueryRow(harness.ctx, `SELECT state, current_attempt_id::text FROM node_runs WHERE project_id=$1 AND run_id=$2 AND execution_node_id=$3`, harness.projectID, runID, nodeID).Scan(&state, &current)
	if err != nil || state != wantState || current != wantAttempt {
		t.Fatalf("node state=%s/%s current=%s/%s err=%v", state, wantState, current, wantAttempt, err)
	}
}

func readyNodeID(t *testing.T, harness *m5Harness, runID string) string {
	t.Helper()
	var id string
	if err := harness.client.Pool().QueryRow(harness.ctx, `
		SELECT execution_node_id FROM node_runs
		WHERE project_id=$1 AND run_id=$2 AND kind='task' AND state IN ('ready','queued')
		ORDER BY execution_node_id LIMIT 1`, harness.projectID, runID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// dispatchFixture returns the Attempt queued atomically by Engine. The fallback
// keeps older state-transition fixtures usable when they deliberately construct
// a Ready Node without passing through the production initialization path.
func dispatchFixture(t *testing.T, harness *m5Harness, runID, nodeID string) runtimepkg.NodeAttemptRecord {
	t.Helper()
	tx, err := harness.client.Pool().Begin(harness.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(harness.ctx)
	var nodeRunID string
	var state runtimepkg.NodeState
	var stateVersion uint64
	var sequence, business, recovery uint32
	var kind runtimepkg.RetryKind
	var currentAttemptID *string
	err = tx.QueryRow(harness.ctx, `
		SELECT node_run_id::text, state, state_version, next_attempt_seq,
		       next_attempt_kind, business_attempt_count, recovery_count,
		       current_attempt_id::text
		FROM node_runs WHERE project_id=$1 AND run_id=$2 AND execution_node_id=$3
		FOR UPDATE`, harness.projectID, runID, nodeID).
		Scan(&nodeRunID, &state, &stateVersion, &sequence, &kind, &business, &recovery, &currentAttemptID)
	if err == nil && state == runtimepkg.NodeQueued && currentAttemptID != nil {
		var queued runtimepkg.NodeAttemptRecord
		err = tx.QueryRow(harness.ctx, `
			SELECT attempt_id::text, attempt_seq, attempt_kind, state, state_version
			FROM node_attempts
			WHERE project_id=$1 AND run_id=$2 AND attempt_id=$3`,
			harness.projectID, runID, *currentAttemptID).
			Scan(&queued.ID, &queued.Sequence, &queued.Kind, &queued.State, &queued.StateVersion)
		if err != nil {
			t.Fatal(err)
		}
		queued.NodeRunID = runID + ":" + nodeID
		return queued
	}
	if err != nil || state != runtimepkg.NodeReady {
		t.Fatalf("dispatch fixture state=%s err=%v", state, err)
	}
	sequence++
	if kind == runtimepkg.AttemptRecovery {
		recovery++
	} else {
		business++
	}
	attemptID := newID(t)
	mustExec(t, harness.ctx, tx, `
		INSERT INTO node_attempts (
			attempt_id, project_id, run_id, node_run_id, attempt_seq, attempt_kind,
			state, state_version, retry_count, recovery_count
		) VALUES ($1,$2,$3,$4,$5,$6,'queued',1,$7,$8)`,
		attemptID, harness.projectID, runID, nodeRunID, sequence, kind, business-1, recovery)
	tag, err := tx.Exec(harness.ctx, `
		UPDATE node_runs SET state='queued', state_version=state_version+1,
		       current_attempt_id=$1, next_attempt_seq=$2,
		       business_attempt_count=$3, recovery_count=$4,
		       ready_at=NULL, next_retry_at=NULL, updated_at=clock_timestamp()
		WHERE project_id=$5 AND run_id=$6 AND execution_node_id=$7
		  AND state='ready' AND state_version=$8`, attemptID, sequence, business, recovery,
		harness.projectID, runID, nodeID, stateVersion)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("dispatch fixture CAS rows=%d err=%v", tag.RowsAffected(), err)
	}
	if err = tx.Commit(harness.ctx); err != nil {
		t.Fatal(err)
	}
	return runtimepkg.NodeAttemptRecord{
		ID: attemptID, NodeRunID: runID + ":" + nodeID, Sequence: sequence,
		Kind: kind, State: runtimepkg.AttemptQueued, StateVersion: 1,
	}
}

func singleCodeIR() []byte {
	return []byte(`{
		"ir_version":"1",
		"nodes":[
			{"id":"start","type":"start","title":"Start","inputs":[],"outputs":[{"name":"workflow_input","data_type":"object"}]},
			{"id":"transform","type":"code","title":"Transform","inputs":[{"name":"source_code","data_type":"string","source":"literal","value":"def main(inputs):\n    return {'result': {'ok': True}}"}],"outputs":[{"name":"result","data_type":"object"}]},
			{"id":"end","type":"end","title":"End","inputs":[{"name":"workflow_output","data_type":"object","source":"ref","ref_node":"transform","ref_output":"result"}],"outputs":[]}
		],
		"edges":[{"id":"start_to_transform","source":"start","target":"transform"},{"id":"transform_to_end","source":"transform","target":"end"}],
		"layout":{"start":{"x":0,"y":0},"transform":{"x":100,"y":0},"end":{"x":200,"y":0}}
	}`)
}

func entryRefCodeIR() []byte {
	return []byte(`{
		"ir_version":"1",
		"nodes":[
			{"id":"start","type":"start","title":"Start","inputs":[],"outputs":[{"name":"workflow_input","data_type":"object"}]},
			{"id":"transform","type":"code","title":"Transform","inputs":[
				{"name":"source_code","data_type":"string","source":"literal","value":"def main(inputs):\n    return {'result': {'ok': True}}"},
				{"name":"request","data_type":"object","source":"ref","ref_node":"start","ref_output":"workflow_input"}
			],"outputs":[{"name":"result","data_type":"object"}]},
			{"id":"end","type":"end","title":"End","inputs":[{"name":"workflow_output","data_type":"object","source":"ref","ref_node":"transform","ref_output":"result"}],"outputs":[]}
		],
		"edges":[{"id":"start_to_transform","source":"start","target":"transform"},{"id":"transform_to_end","source":"transform","target":"end"}],
		"layout":{"start":{"x":0,"y":0},"transform":{"x":100,"y":0},"end":{"x":200,"y":0}}
	}`)
}
