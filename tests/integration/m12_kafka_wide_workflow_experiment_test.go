//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
)

const (
	kafkaWideExperimentEnv  = "EVALFROG_KAFKA_WIDE_EXPERIMENT"
	kafkaWideRepetitionsEnv = "EVALFROG_KAFKA_WIDE_REPETITIONS"
)

func TestM12KafkaWideWorkflowExperiment(t *testing.T) {
	if os.Getenv(kafkaWideExperimentEnv) != "1" {
		t.Skip("set EVALFROG_KAFKA_WIDE_EXPERIMENT=1 to run width 8/32 Kafka batching experiments")
	}
	repetitions := positiveEnvInt(t, kafkaWideRepetitionsEnv, 10)
	for _, width := range []int{8, 32} {
		width := width
		t.Run(fmt.Sprintf("width_%d", width), func(t *testing.T) {
			runKafkaWideWorkflow(t, width, repetitions)
		})
	}
}

func runKafkaWideWorkflow(t *testing.T, width, repetitions int) {
	harness := newM5HarnessWithTimeout(t, 20*time.Minute)
	environment := newKafkaFaultEnvironment(t, harness, fmt.Sprintf("wide-%d", width))
	workflow, snapshot := createWideCodeWorkflow(t, harness, width)
	runIDs := make([]string, 0, repetitions)
	totalTasks := width * repetitions
	for repetition := 0; repetition < repetitions; repetition++ {
		run := harness.createTestRun(t, workflow.ID, snapshot.ID, fmt.Sprintf("wide-%d-%d", width, repetition))
		harness.initializeRun(t, run)
		if _, err := harness.client.Pool().Exec(harness.ctx, `
			UPDATE outbox_events SET published_at=CURRENT_TIMESTAMP(6)
			WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND event_type='run.created'`, harness.projectID, run.ID); err != nil {
			t.Fatal(err)
		}
		assertWideDispatchAtomicity(t, harness, run.ID, width)
		runIDs = append(runIDs, run.ID)
	}

	// Fail alternating records after the Relay has claimed a full batch. Only
	// Kafka-acknowledged records may be marked published; the rest must remain
	// durable and retryable in MySQL.
	partial := &alternatingTaskPublisher{delegate: environment.publisher}
	relay, err := eventing.NewTaskRelay(harness.store, partial, "wide-partial", totalTasks, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	published, relayErr := relay.RelayOnce(harness.ctx)
	if !errors.Is(relayErr, errInjectedPartialPublish) {
		t.Fatalf("partial relay error=%v", relayErr)
	}
	if published != (totalTasks+1)/2 {
		t.Fatalf("partial published=%d want=%d", published, (totalTasks+1)/2)
	}
	assertTaskOutboxCounts(t, harness, totalTasks, published)

	normalRelay, err := eventing.NewTaskRelay(harness.store, environment.publisher, "wide-drain", totalTasks, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	relayExactly(t, harness.ctx, normalRelay, totalTasks-published)
	assertTaskOutboxCounts(t, harness, totalTasks, totalTasks)

	completed := 0
	for completed < totalTasks {
		batch, receiveErr := environment.taskConsumer.ReceivePartitionBatch(harness.ctx, min(8, totalTasks-completed))
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		messages := batch.Messages()
		commands := make([]attempt.ClaimCommand, len(messages))
		tasks := make([]eventing.TaskMessage, len(messages))
		for index, raw := range messages {
			task, parseErr := eventing.ParseTaskMessage(raw.Payload)
			if parseErr != nil {
				batch.Nack()
				t.Fatal(parseErr)
			}
			tasks[index] = task
			commands[index] = claimCommand(task, "wide-worker", time.Minute)
		}
		claims, claimErr := harness.coordinator.ClaimBatch(harness.ctx, commands)
		if claimErr != nil {
			batch.Nack()
			t.Fatal(claimErr)
		}
		for index := range claims {
			if claims[index].Err != nil {
				batch.Nack()
				t.Fatalf("claim batch item %d: %v", index, claims[index].Err)
			}
		}
		if ackErr := batch.Ack(harness.ctx); ackErr != nil {
			t.Fatal(ackErr)
		}
		for index := range tasks {
			applied, completeErr := harness.coordinator.Complete(harness.ctx, completionCommand(tasks[index], claims[index].Lease, "wide-complete"))
			if completeErr != nil || !applied {
				t.Fatalf("complete item %d applied=%v err=%v", index, applied, completeErr)
			}
		}
		completed += len(tasks)
	}

	runtimeRelay, err := eventing.NewRelay(harness.store, environment.publisher, "wide-runtime", totalTasks, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	relayExactly(t, harness.ctx, runtimeRelay, totalTasks)
	consumed := 0
	for consumed < totalTasks {
		batch, receiveErr := environment.eventConsumer.ReceivePartitionBatch(harness.ctx, min(8, totalTasks-consumed))
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		messages := batch.Messages()
		events := make([]eventing.RuntimeEvent, len(messages))
		for index, raw := range messages {
			value, parseErr := eventing.ParseRuntimeEvent(raw.Payload)
			if parseErr != nil {
				batch.Nack()
				t.Fatal(parseErr)
			}
			if value.EventType != eventing.AttemptCompleted {
				batch.Nack()
				t.Fatalf("runtime event type=%s", value.EventType)
			}
			events[index] = value
		}
		if consumeErr := harness.consumer.ConsumeBatch(harness.ctx, events); consumeErr != nil {
			batch.Nack()
			t.Fatal(consumeErr)
		}
		if ackErr := batch.Ack(harness.ctx); ackErr != nil {
			t.Fatal(ackErr)
		}
		consumed += len(events)
	}

	for _, runID := range runIDs {
		if state := runState(t, harness, runID); state != "succeeded" {
			t.Fatalf("run %s state=%s", runID, state)
		}
		var outputs, inbox int
		if err = harness.client.Pool().QueryRow(harness.ctx, `
			SELECT
			  (SELECT count(*) FROM node_output_values WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2)),
			  (SELECT count(*) FROM inbox_events WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND event_type='attempt.completed')`,
			harness.projectID, runID).Scan(&outputs, &inbox); err != nil {
			t.Fatal(err)
		}
		if outputs != width || inbox != width {
			t.Fatalf("run %s outputs=%d inbox=%d want=%d", runID, outputs, inbox, width)
		}
	}
}

func createWideCodeWorkflow(t *testing.T, harness *m5Harness, width int) (definition.Workflow, definition.ExecutionSnapshot) {
	t.Helper()
	workflow, _, diagnostics, err := harness.definitions.CreateWorkflow(harness.ctx, definition.CreateWorkflowCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID,
		Name: fmt.Sprintf("M12 Wide %d", width), IRJSON: wideCodeIR(t, width),
		IdempotencyKey: fmt.Sprintf("m12-wide-create-%d-%s", width, newID(t)),
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	snapshot, diagnostics, err := harness.definitions.CompileDraftTestSnapshot(harness.ctx, harness.projectID, harness.principalID, workflow.ID, 1)
	assertNoDefinitionFailure(t, diagnostics, err)
	return workflow, snapshot
}

func wideCodeIR(t *testing.T, width int) []byte {
	t.Helper()
	nodes := make([]map[string]any, 0, width+2)
	edges := make([]map[string]any, 0, width*2)
	layout := make(map[string]map[string]int, width+2)
	nodes = append(nodes, map[string]any{
		"id": "start", "type": "start", "title": "Start", "inputs": []any{},
		"outputs": []map[string]any{{"name": "workflow_input", "data_type": "object"}},
	})
	layout["start"] = map[string]int{"x": 0, "y": 0}
	for index := 0; index < width; index++ {
		id := fmt.Sprintf("task_%02d", index)
		nodes = append(nodes, map[string]any{
			"id": id, "type": "code", "title": id,
			"inputs": []map[string]any{{
				"name": "source_code", "data_type": "string", "source": "literal",
				"value": "def main(inputs):\n    return {'result': {'ok': True}}",
			}},
			"outputs": []map[string]any{{"name": "result", "data_type": "object"}},
		})
		edges = append(edges,
			map[string]any{"id": "start_to_" + id, "source": "start", "target": id},
			map[string]any{"id": id + "_to_end", "source": id, "target": "end"})
		layout[id] = map[string]int{"x": 100, "y": index * 50}
	}
	nodes = append(nodes, map[string]any{
		"id": "end", "type": "end", "title": "End",
		"inputs": []map[string]any{{
			"name": "workflow_output", "data_type": "object", "source": "ref",
			"ref_node": "task_00", "ref_output": "result",
		}}, "outputs": []any{},
	})
	layout["end"] = map[string]int{"x": 200, "y": 0}
	payload, err := json.Marshal(map[string]any{
		"ir_version": "1", "nodes": nodes, "edges": edges, "layout": layout,
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func assertWideDispatchAtomicity(t *testing.T, harness *m5Harness, runID string, width int) {
	t.Helper()
	var queued, attempts, tasks int
	err := harness.client.Pool().QueryRow(harness.ctx, `
		SELECT
		  count(DISTINCT CASE WHEN n.kind='task' AND n.state='queued' THEN n.node_run_id END),
		  count(DISTINCT a.attempt_id),
		  count(DISTINCT o.task_id)
		FROM node_runs n
		LEFT JOIN node_attempts a ON a.project_id=n.project_id AND a.run_id=n.run_id AND a.node_run_id=n.node_run_id
		LEFT JOIN node_task_outbox o ON o.project_id=a.project_id AND o.run_id=a.run_id AND o.attempt_id=a.attempt_id
		WHERE n.project_id=UUID_TO_BIN($1) AND n.run_id=UUID_TO_BIN($2)`, harness.projectID, runID).Scan(&queued, &attempts, &tasks)
	if err != nil {
		t.Fatal(err)
	}
	if queued != width || attempts != width || tasks != width {
		t.Fatalf("atomic dispatch queued=%d attempts=%d tasks=%d want=%d", queued, attempts, tasks, width)
	}
}

func assertTaskOutboxCounts(t *testing.T, harness *m5Harness, total, published int) {
	t.Helper()
	var rows, marked int
	if err := harness.client.Pool().QueryRow(harness.ctx, `
		SELECT count(*), SUM(CASE WHEN published_at IS NOT NULL THEN 1 ELSE 0 END)
		FROM node_task_outbox WHERE project_id=UUID_TO_BIN($1)`, harness.projectID).Scan(&rows, &marked); err != nil {
		t.Fatal(err)
	}
	if rows != total || marked != published {
		t.Fatalf("task outbox rows=%d/%d published=%d/%d", rows, total, marked, published)
	}
}

var errInjectedPartialPublish = errors.New("injected partial Kafka publish")

type alternatingTaskPublisher struct {
	delegate eventing.TaskPublisher
}

func (publisher *alternatingTaskPublisher) PublishTask(ctx context.Context, message eventing.TaskMessage) error {
	return publisher.delegate.PublishTask(ctx, message)
}

func (publisher *alternatingTaskPublisher) PublishTasks(ctx context.Context, messages []eventing.TaskMessage) []error {
	results := make([]error, len(messages))
	for index := range messages {
		if index%2 == 1 {
			results[index] = errInjectedPartialPublish
			continue
		}
		results[index] = publisher.delegate.PublishTask(ctx, messages[index])
	}
	return results
}
