//go:build integration

package integration

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/uu999/evalfrog/internal/adapters/kafka"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/recovery"
	runtimepkg "github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
)

const (
	kafkaFaultExperimentEnv = "EVALFROG_KAFKA_FAULT_EXPERIMENT"
	kafkaFaultSamplesEnv    = "EVALFROG_KAFKA_FAULT_SAMPLES"
	kafkaFaultLeaseEnv      = "EVALFROG_KAFKA_FAULT_LEASE"
	kafkaFaultBrokersEnv    = "EVALFROG_KAFKA_EXPERIMENT_BROKERS"
	kafkaFaultRFEnv         = "EVALFROG_KAFKA_EXPERIMENT_REPLICATION_FACTOR"
	kafkaFaultMinISREnv     = "EVALFROG_KAFKA_EXPERIMENT_MIN_ISR"
)

type kafkaFaultCase struct {
	run         runtimepkg.WorkflowRunRecord
	task        eventing.TaskMessage
	lease       attempt.Lease
	injectedAt  time.Time
	ackedAt     time.Time
	recoveredAt time.Time
	terminalAt  time.Time
	eventID     string
}

type kafkaFaultRecord struct {
	window, runID, eventID, attemptID, outcome string
	injectedAt, recoveredAt, terminalAt        time.Time
	duplicateDeliveries, inboxConflicts        int
	staleCompleteAccepted, invariantViolation  bool
}

type kafkaFaultEnvironment struct {
	configuration config.Config
	publisher     *kafka.Client
	taskConsumer  *kafka.Client
	eventConsumer *kafka.Client
	identifier    string
	taskGroup     string
}

func TestKafkaFaultWindowExperiment(t *testing.T) {
	if os.Getenv(kafkaFaultExperimentEnv) != "1" {
		t.Skip("set EVALFROG_KAFKA_FAULT_EXPERIMENT=1 to run the destructive fault-window experiment")
	}
	samples := positiveEnvInt(t, kafkaFaultSamplesEnv, 100)
	leaseDuration := durationEnv(t, kafkaFaultLeaseEnv, 60*time.Second)
	var records []kafkaFaultRecord
	var controls []kafkaFaultRecord

	t.Run("F1_task_outbox_committed_before_publish", func(t *testing.T) {
		records = append(records, runKafkaFaultF1(t, samples)...)
	})
	t.Run("F2_task_published_before_mark", func(t *testing.T) {
		records = append(records, runKafkaFaultF2(t, samples)...)
	})
	t.Run("F3_worker_acked_before_complete", func(t *testing.T) {
		records = append(records, runKafkaFaultF3(t, samples, leaseDuration)...)
	})
	t.Run("F4_runtime_event_published_before_mark", func(t *testing.T) {
		records = append(records, runKafkaFaultF4(t, samples)...)
	})
	t.Run("control_without_fault", func(t *testing.T) {
		controls = append(controls, runKafkaFaultControl(t, samples*4)...)
	})

	if len(records) != samples*4 {
		t.Fatalf("fault records=%d, want %d", len(records), samples*4)
	}
	if len(controls) != samples*4 {
		t.Fatalf("control records=%d, want %d", len(controls), samples*4)
	}
	writeKafkaFaultArtifacts(t, records, controls, samples, leaseDuration)
}

func runKafkaFaultF1(t *testing.T, samples int) []kafkaFaultRecord {
	harness := newM5HarnessWithTimeout(t, 10*time.Minute)
	environment := newKafkaFaultEnvironment(t, harness, "f1")
	cases := prepareKafkaFaultCases(t, harness, samples, "f1")
	claimLease := 250 * time.Millisecond
	claimed, err := harness.store.ClaimTaskOutbox(harness.ctx, "f1-crashed-relay", samples, claimLease)
	if err != nil || len(claimed) != samples {
		t.Fatalf("F1 claimed=%d err=%v", len(claimed), err)
	}
	injectedAt := time.Now().UTC()
	time.Sleep(claimLease + 100*time.Millisecond)
	relay, err := eventing.NewTaskRelay(harness.store, environment.publisher, "f1-restarted-relay", samples, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	relayExactly(t, harness.ctx, relay, samples)
	consumeAndCompleteTasks(t, harness, environment.taskConsumer, cases, samples, false)
	consumeCompletionsDirect(t, harness, cases)

	records := make([]kafkaFaultRecord, 0, samples)
	for index := range cases {
		verifySuccessfulCase(t, harness, &cases[index], 1)
		records = append(records, kafkaFaultRecord{
			window: "F1", runID: cases[index].run.ID, eventID: cases[index].eventID,
			attemptID: cases[index].task.AttemptID, outcome: "recovered",
			injectedAt: injectedAt, recoveredAt: cases[index].ackedAt, terminalAt: cases[index].terminalAt,
		})
	}
	return records
}

func runKafkaFaultF2(t *testing.T, samples int) []kafkaFaultRecord {
	harness := newM5HarnessWithTimeout(t, 10*time.Minute)
	environment := newKafkaFaultEnvironment(t, harness, "f2")
	cases := prepareKafkaFaultCases(t, harness, samples, "f2")
	claimLease := 250 * time.Millisecond
	claimed, err := harness.store.ClaimTaskOutbox(harness.ctx, "f2-crashed-relay", samples, claimLease)
	if err != nil || len(claimed) != samples {
		t.Fatalf("F2 claimed=%d err=%v", len(claimed), err)
	}
	injectedAt := time.Now().UTC()
	for _, value := range claimed {
		if err = environment.publisher.PublishTask(harness.ctx, value.Message); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(claimLease + 100*time.Millisecond)
	relay, err := eventing.NewTaskRelay(harness.store, environment.publisher, "f2-restarted-relay", samples, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	relayExactly(t, harness.ctx, relay, samples)
	duplicates := consumeAndCompleteTasks(t, harness, environment.taskConsumer, cases, samples*2, true)
	if duplicates < samples {
		t.Fatalf("F2 duplicate task deliveries=%d, want at least %d", duplicates, samples)
	}
	consumeCompletionsDirect(t, harness, cases)

	records := make([]kafkaFaultRecord, 0, samples)
	for index := range cases {
		verifySuccessfulCase(t, harness, &cases[index], 2)
		records = append(records, kafkaFaultRecord{
			window: "F2", runID: cases[index].run.ID, eventID: cases[index].eventID,
			attemptID: cases[index].task.AttemptID, outcome: "deduplicated",
			injectedAt: injectedAt, recoveredAt: cases[index].ackedAt, terminalAt: cases[index].terminalAt,
			duplicateDeliveries: 1,
		})
	}
	return records
}

func runKafkaFaultF3(t *testing.T, samples int, leaseDuration time.Duration) []kafkaFaultRecord {
	harness := newM5HarnessWithTimeout(t, leaseDuration+15*time.Minute)
	environment := newKafkaFaultEnvironment(t, harness, "f3")
	cases := prepareKafkaFaultCases(t, harness, samples, "f3")
	taskRelay, err := eventing.NewTaskRelay(harness.store, environment.publisher, "f3-task-relay", samples, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	relayExactly(t, harness.ctx, taskRelay, samples)
	claimAndAckWithoutComplete(t, harness, environment.taskConsumer, cases, leaseDuration)

	var latestExpiry time.Time
	for index := range cases {
		if cases[index].lease.ExpiresAt.After(latestExpiry) {
			latestExpiry = cases[index].lease.ExpiresAt
		}
	}
	if wait := time.Until(latestExpiry.Add(100 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reaper, err := recovery.NewReaper(harness.store, harness.coordinator, 0, time.Second, samples, "fault-f3-reaper", logger)
	if err != nil {
		t.Fatal(err)
	}
	if err = reaper.ScanOnce(harness.ctx); err != nil {
		t.Fatal(err)
	}
	staleAccepted := 0
	for index := range cases {
		if attemptState(t, harness, cases[index].task.AttemptID) != string(runtimepkg.AttemptLost) {
			t.Fatalf("F3 attempt %s was not marked lost", cases[index].task.AttemptID)
		}
		applied, completeErr := harness.coordinator.Complete(harness.ctx, completionCommand(cases[index].task, cases[index].lease, "f3-late"))
		if completeErr == nil && applied {
			staleAccepted++
		}
	}
	if staleAccepted != 0 {
		t.Fatalf("F3 stale completions accepted=%d", staleAccepted)
	}

	runtimeRelay, err := eventing.NewRelay(harness.store, environment.publisher, "f3-runtime-relay", samples, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	relayExactly(t, harness.ctx, runtimeRelay, samples)
	consumeRuntimeEvents(t, harness, environment.eventConsumer, samples, eventing.AttemptLost)
	waitForRetryDue(t, harness, samples)
	retryTimer, err := recovery.NewRetryTimer(harness.store, recovery.NewBuiltinEmitter(harness.store), time.Second, samples, "fault-f3-retry", logger)
	if err != nil {
		t.Fatal(err)
	}
	if err = retryTimer.ScanOnce(harness.ctx); err != nil {
		t.Fatal(err)
	}
	relayExactly(t, harness.ctx, runtimeRelay, samples)
	consumeRuntimeEvents(t, harness, environment.eventConsumer, samples, eventing.RetryDue)

	recoveryTasks := dispatchRecoveryTasks(t, harness, cases)
	for index := range cases {
		cases[index].task = recoveryTasks[cases[index].run.ID]
	}
	relayExactly(t, harness.ctx, taskRelay, samples)
	consumeAndCompleteTasks(t, harness, environment.taskConsumer, cases, samples, false)
	for index := range cases {
		cases[index].recoveredAt = cases[index].ackedAt
	}
	relayExactly(t, harness.ctx, runtimeRelay, samples)
	events := consumeRuntimeEvents(t, harness, environment.eventConsumer, samples, eventing.AttemptCompleted)
	for index := range cases {
		cases[index].eventID = events[cases[index].run.ID]
		cases[index].terminalAt = time.Now().UTC()
		verifySuccessfulCase(t, harness, &cases[index], 1)
	}

	records := make([]kafkaFaultRecord, 0, samples)
	for index := range cases {
		records = append(records, kafkaFaultRecord{
			window: "F3", runID: cases[index].run.ID, eventID: cases[index].eventID,
			attemptID: cases[index].task.AttemptID, outcome: "recovered",
			injectedAt: cases[index].injectedAt, recoveredAt: cases[index].recoveredAt,
			terminalAt: cases[index].terminalAt, staleCompleteAccepted: false,
		})
	}
	return records
}

func runKafkaFaultF4(t *testing.T, samples int) []kafkaFaultRecord {
	harness := newM5HarnessWithTimeout(t, 10*time.Minute)
	environment := newKafkaFaultEnvironment(t, harness, "f4")
	cases := prepareKafkaFaultCases(t, harness, samples, "f4")
	for index := range cases {
		lease, err := harness.coordinator.Claim(harness.ctx, claimCommand(cases[index].task, "f4-worker", time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		cases[index].lease = lease
		if _, err = harness.coordinator.Complete(harness.ctx, completionCommand(cases[index].task, lease, "f4-complete")); err != nil {
			t.Fatal(err)
		}
	}

	claimLease := 250 * time.Millisecond
	claimed, err := harness.store.ClaimOutbox(harness.ctx, "f4-crashed-relay", samples, claimLease)
	if err != nil || len(claimed) != samples {
		t.Fatalf("F4 claimed=%d err=%v", len(claimed), err)
	}
	injectedAt := time.Now().UTC()
	for _, value := range claimed {
		if value.Event.EventType != eventing.AttemptCompleted {
			t.Fatalf("F4 unexpected event type %s", value.Event.EventType)
		}
		if err = environment.publisher.PublishRuntimeEvent(harness.ctx, value.Event); err != nil {
			t.Fatal(err)
		}
	}
	firstEvents := consumeRuntimeEvents(t, harness, environment.eventConsumer, samples, eventing.AttemptCompleted)
	time.Sleep(claimLease + 100*time.Millisecond)
	relay, err := eventing.NewRelay(harness.store, environment.publisher, "f4-restarted-relay", samples, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	relayExactly(t, harness.ctx, relay, samples)
	consumeRuntimeEvents(t, harness, environment.eventConsumer, samples, eventing.AttemptCompleted)

	records := make([]kafkaFaultRecord, 0, samples)
	for index := range cases {
		cases[index].eventID = firstEvents[cases[index].run.ID]
		cases[index].terminalAt = time.Now().UTC()
		verifySuccessfulCase(t, harness, &cases[index], 0)
		var eventPublishAttempts int
		if err = harness.client.Pool().QueryRow(harness.ctx, `
			SELECT publish_attempts FROM outbox_events WHERE event_id=UUID_TO_BIN($1)`,
			cases[index].eventID).Scan(&eventPublishAttempts); err != nil || eventPublishAttempts < 2 {
			t.Fatalf("F4 event publish attempts=%d err=%v", eventPublishAttempts, err)
		}
		var inboxRows int
		if err = harness.client.Pool().QueryRow(harness.ctx, `
			SELECT count(*) FROM inbox_events
			WHERE consumer_name=$1 AND event_id=UUID_TO_BIN($2)`, "runtime-engine-v1", cases[index].eventID).Scan(&inboxRows); err != nil || inboxRows != 1 {
			t.Fatalf("F4 inbox rows=%d err=%v", inboxRows, err)
		}
		records = append(records, kafkaFaultRecord{
			window: "F4", runID: cases[index].run.ID, eventID: cases[index].eventID,
			attemptID: cases[index].task.AttemptID, outcome: "deduplicated",
			injectedAt: injectedAt, recoveredAt: cases[index].terminalAt, terminalAt: cases[index].terminalAt,
			duplicateDeliveries: 1, inboxConflicts: 1,
		})
	}
	return records
}

func runKafkaFaultControl(t *testing.T, samples int) []kafkaFaultRecord {
	harness := newM5HarnessWithTimeout(t, 15*time.Minute)
	environment := newKafkaFaultEnvironment(t, harness, "control")
	cases := prepareKafkaFaultCases(t, harness, samples, "control")
	startedAt := time.Now().UTC()
	taskRelay, err := eventing.NewTaskRelay(harness.store, environment.publisher, "control-task-relay", samples, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	relayExactly(t, harness.ctx, taskRelay, samples)
	consumeAndCompleteTasks(t, harness, environment.taskConsumer, cases, samples, false)
	runtimeRelay, err := eventing.NewRelay(harness.store, environment.publisher, "control-runtime-relay", samples, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	relayExactly(t, harness.ctx, runtimeRelay, samples)
	events := consumeRuntimeEvents(t, harness, environment.eventConsumer, samples, eventing.AttemptCompleted)

	records := make([]kafkaFaultRecord, 0, samples)
	for index := range cases {
		cases[index].eventID = events[cases[index].run.ID]
		cases[index].terminalAt = time.Now().UTC()
		verifySuccessfulCase(t, harness, &cases[index], 1)
		records = append(records, kafkaFaultRecord{
			window: "CONTROL", runID: cases[index].run.ID, eventID: cases[index].eventID,
			attemptID: cases[index].task.AttemptID, outcome: "succeeded",
			injectedAt: startedAt, recoveredAt: cases[index].ackedAt, terminalAt: cases[index].terminalAt,
		})
	}
	return records
}

func prepareKafkaFaultCases(t *testing.T, harness *m5Harness, samples int, prefix string) []kafkaFaultCase {
	t.Helper()
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	cases := make([]kafkaFaultCase, samples)
	for index := 0; index < samples; index++ {
		key := fmt.Sprintf("fault-%s-%04d-%s", prefix, index, uuid.NewString())
		run := harness.createTestRun(t, workflow.ID, snapshot.ID, key)
		harness.initializeRun(t, run)
		if _, err := harness.client.Pool().Exec(harness.ctx, `
			UPDATE outbox_events SET published_at=CURRENT_TIMESTAMP(6)
			WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND event_type='run.created'`,
			harness.projectID, run.ID); err != nil {
			t.Fatal(err)
		}
		cases[index].run = run
		cases[index].task = taskMessageForRun(t, harness, run.ID)
	}
	return cases
}

func dispatchRecoveryTasks(t *testing.T, harness *m5Harness, cases []kafkaFaultCase) map[string]eventing.TaskMessage {
	t.Helper()
	result := make(map[string]eventing.TaskMessage, len(cases))
	for index := range cases {
		result[cases[index].run.ID] = taskMessageForRun(t, harness, cases[index].run.ID)
	}
	return result
}

func taskMessageForRun(t *testing.T, harness *m5Harness, runID string) eventing.TaskMessage {
	t.Helper()
	var message eventing.TaskMessage
	err := harness.client.Pool().QueryRow(harness.ctx, `
		SELECT message_version, BIN_TO_UUID(task_id), BIN_TO_UUID(project_id), BIN_TO_UUID(run_id),
		       BIN_TO_UUID(node_run_id), execution_node_id, BIN_TO_UUID(attempt_id), attempt_seq,
		       resource_class, occurred_at, trace_id
		FROM node_task_outbox
		WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2)
		ORDER BY occurred_at DESC, task_id DESC LIMIT 1`, harness.projectID, runID).
		Scan(&message.MessageVersion, &message.TaskID, &message.ProjectID, &message.RunID,
			&message.NodeRunID, &message.ExecutionNodeID, &message.AttemptID,
			&message.AttemptSequence, &message.ResourceClass, &message.OccurredAt, &message.TraceID)
	if err != nil {
		t.Fatal(err)
	}
	if err = message.Validate(); err != nil {
		t.Fatal(err)
	}
	return message
}

func newKafkaFaultEnvironment(t *testing.T, harness *m5Harness, window string) *kafkaFaultEnvironment {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(config.LoadOptions{Directory: filepath.Join(root, "configs"), Profile: "local"})
	if err != nil {
		t.Fatal(err)
	}
	if raw := strings.TrimSpace(os.Getenv(kafkaFaultBrokersEnv)); raw != "" {
		configuration.Kafka.Brokers = strings.Split(raw, ",")
	}
	configuration.Kafka.ReplicationFactor = positiveEnvInt(t, kafkaFaultRFEnv, configuration.Kafka.ReplicationFactor)
	configuration.Kafka.MinISR = positiveEnvInt(t, kafkaFaultMinISREnv, configuration.Kafka.MinISR)
	if configuration.Kafka.MinISR > configuration.Kafka.ReplicationFactor {
		t.Fatalf("Kafka experiment min ISR %d exceeds replication factor %d", configuration.Kafka.MinISR, configuration.Kafka.ReplicationFactor)
	}
	identifier := window + "." + uuid.NewString()
	taskGroup := "fault-task-" + identifier
	configuration.Kafka.TopicPrefix = "evalfrog.fault." + identifier
	createKafkaTopics(t, harness.ctx, configuration.Kafka)
	publisher, err := kafka.Open(configuration.Kafka, "fault-publisher-"+identifier)
	if err != nil {
		t.Fatal(err)
	}
	taskConsumer, err := kafka.OpenConsumer(configuration.Kafka, taskGroup, taskGroup, []config.KafkaTopicConfig{configuration.Kafka.Topics.SandboxTask}, 100)
	if err != nil {
		publisher.Close()
		t.Fatal(err)
	}
	eventConsumer, err := kafka.OpenConsumer(configuration.Kafka, "fault-event-"+identifier, "fault-event-"+identifier, []config.KafkaTopicConfig{configuration.Kafka.Topics.RuntimeEvent}, 100)
	if err != nil {
		taskConsumer.Close()
		publisher.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		eventConsumer.Close()
		taskConsumer.Close()
		publisher.Close()
	})
	return &kafkaFaultEnvironment{
		configuration: configuration, publisher: publisher,
		taskConsumer: taskConsumer, eventConsumer: eventConsumer,
		identifier: identifier, taskGroup: taskGroup,
	}
}

func claimCommand(task eventing.TaskMessage, worker string, lease time.Duration) attempt.ClaimCommand {
	return attempt.ClaimCommand{
		ProjectID: task.ProjectID, RunID: task.RunID, NodeRunID: task.NodeRunID, AttemptID: task.AttemptID,
		AttemptSequence: task.AttemptSequence, WorkerID: worker, ExecutorBuild: "fault-experiment",
		ResourceClass: task.ResourceClass,
		Capabilities:  []dsl.Coordinate{{Type: "task.python", Version: 1}},
		LeaseDuration: lease,
	}
}

func completionCommand(task eventing.TaskMessage, lease attempt.Lease, trace string) attempt.CompleteCommand {
	return attempt.CompleteCommand{
		ProjectID: task.ProjectID, RunID: task.RunID, AttemptID: task.AttemptID,
		AttemptSequence: task.AttemptSequence, LeaseToken: lease.Token, FencingToken: lease.FencingToken,
		TraceID: trace,
		Result: runtimepkg.AttemptResult{
			State:   runtimepkg.AttemptSucceeded,
			Outputs: map[string]json.RawMessage{"result": json.RawMessage(`{"ok":true}`)},
		},
	}
}

func consumeAndCompleteTasks(t *testing.T, harness *m5Harness, consumer eventing.Consumer, cases []kafkaFaultCase, deliveries int, expectDuplicates bool) int {
	t.Helper()
	byAttempt := make(map[string]int, len(cases))
	for index := range cases {
		byAttempt[cases[index].task.AttemptID] = index
	}
	seen := make(map[string]bool, len(cases))
	duplicates := 0
	for received := 0; received < deliveries; received++ {
		delivery, err := consumer.Receive(harness.ctx)
		if err != nil {
			t.Fatal(err)
		}
		message, err := eventing.ParseTaskMessage(delivery.Payload())
		if err != nil {
			t.Fatal(err)
		}
		index, ok := byAttempt[message.AttemptID]
		if !ok {
			t.Fatalf("unexpected task attempt %s", message.AttemptID)
		}
		if seen[message.AttemptID] {
			duplicates++
			if _, claimErr := harness.coordinator.Claim(harness.ctx, claimCommand(cases[index].task, "duplicate-worker", time.Minute)); claimErr == nil {
				t.Fatalf("duplicate task %s acquired a second lease", message.AttemptID)
			}
			if err = delivery.Ack(harness.ctx); err != nil {
				t.Fatal(err)
			}
			continue
		}
		seen[message.AttemptID] = true
		lease, err := harness.coordinator.Claim(harness.ctx, claimCommand(cases[index].task, "fault-worker", time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if err = delivery.Ack(harness.ctx); err != nil {
			t.Fatal(err)
		}
		cases[index].ackedAt = time.Now().UTC()
		cases[index].lease = lease
		if _, err = harness.coordinator.Complete(harness.ctx, completionCommand(cases[index].task, lease, "fault-complete")); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != len(cases) {
		t.Fatalf("unique task deliveries=%d, want %d", len(seen), len(cases))
	}
	if expectDuplicates && duplicates == 0 {
		t.Fatal("expected duplicate task deliveries")
	}
	return duplicates
}

func claimAndAckWithoutComplete(t *testing.T, harness *m5Harness, consumer eventing.Consumer, cases []kafkaFaultCase, leaseDuration time.Duration) {
	t.Helper()
	byAttempt := make(map[string]int, len(cases))
	for index := range cases {
		byAttempt[cases[index].task.AttemptID] = index
	}
	for received := 0; received < len(cases); received++ {
		delivery, err := consumer.Receive(harness.ctx)
		if err != nil {
			t.Fatal(err)
		}
		message, err := eventing.ParseTaskMessage(delivery.Payload())
		if err != nil {
			t.Fatal(err)
		}
		index, ok := byAttempt[message.AttemptID]
		if !ok {
			t.Fatalf("unexpected F3 attempt %s", message.AttemptID)
		}
		lease, err := harness.coordinator.Claim(harness.ctx, claimCommand(cases[index].task, "crashed-worker", leaseDuration))
		if err != nil {
			t.Fatal(err)
		}
		if err = delivery.Ack(harness.ctx); err != nil {
			t.Fatal(err)
		}
		cases[index].lease = lease
		cases[index].injectedAt = time.Now().UTC()
		cases[index].ackedAt = time.Now().UTC()
	}
}

func consumeRuntimeEvents(t *testing.T, harness *m5Harness, consumer eventing.Consumer, count int, expected eventing.RuntimeEventType) map[string]string {
	t.Helper()
	result := make(map[string]string, count)
	for received := 0; received < count; received++ {
		delivery, err := consumer.Receive(harness.ctx)
		if err != nil {
			t.Fatal(err)
		}
		value, err := eventing.ParseRuntimeEvent(delivery.Payload())
		if err != nil {
			t.Fatal(err)
		}
		if value.EventType != expected {
			t.Fatalf("runtime event type=%s, want %s", value.EventType, expected)
		}
		if err = harness.consumer.Consume(harness.ctx, value); err != nil {
			t.Fatal(err)
		}
		if err = delivery.Ack(harness.ctx); err != nil {
			t.Fatal(err)
		}
		result[value.RunID] = value.EventID
	}
	return result
}

func consumeCompletionsDirect(t *testing.T, harness *m5Harness, cases []kafkaFaultCase) {
	t.Helper()
	for index := range cases {
		value := harness.event(t, eventing.AttemptCompleted, cases[index].task.AttemptID)
		if err := harness.consumer.Consume(harness.ctx, value); err != nil {
			t.Fatal(err)
		}
		cases[index].eventID = value.EventID
		cases[index].terminalAt = time.Now().UTC()
	}
}

func waitForRetryDue(t *testing.T, harness *m5Harness, samples int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for {
		values, err := harness.store.ListRetryDue(harness.ctx, samples)
		if err != nil {
			t.Fatal(err)
		}
		if len(values) == samples {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("retry due candidates=%d, want %d", len(values), samples)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func relayExactly(t *testing.T, ctx context.Context, relay oneShotRelay, expected int) {
	t.Helper()
	total := 0
	for total < expected {
		count, err := relay.RelayOnce(ctx)
		if err != nil {
			t.Fatal(err)
		}
		total += count
		if count == 0 {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	if total != expected {
		t.Fatalf("relay published=%d, want %d", total, expected)
	}
}

func verifySuccessfulCase(t *testing.T, harness *m5Harness, value *kafkaFaultCase, minimumPublishAttempts int) {
	t.Helper()
	if state := runState(t, harness, value.run.ID); state != string(runtimepkg.RunSucceeded) {
		t.Fatalf("run %s state=%s", value.run.ID, state)
	}
	var outputCandidates, publishAttempts int
	if err := harness.client.Pool().QueryRow(harness.ctx, `
		SELECT count(*) FROM node_output_values
		WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2) AND attempt_id=UUID_TO_BIN($3)`,
		harness.projectID, value.run.ID, value.task.AttemptID).Scan(&outputCandidates); err != nil {
		t.Fatal(err)
	}
	if outputCandidates != 1 {
		t.Fatalf("attempt %s output candidates=%d", value.task.AttemptID, outputCandidates)
	}
	if err := harness.client.Pool().QueryRow(harness.ctx, `
		SELECT publish_attempts FROM node_task_outbox WHERE task_id=UUID_TO_BIN($1)`,
		value.task.TaskID).Scan(&publishAttempts); err != nil {
		t.Fatal(err)
	}
	if publishAttempts < minimumPublishAttempts {
		t.Fatalf("task %s publish attempts=%d, want >=%d", value.task.TaskID, publishAttempts, minimumPublishAttempts)
	}
}

func positiveEnvInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		t.Fatalf("%s must be a positive integer", name)
	}
	return value
}

func durationEnv(t *testing.T, name string, fallback time.Duration) time.Duration {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		t.Fatalf("%s must be a positive Go duration", name)
	}
	return value
}

func writeKafkaFaultArtifacts(t *testing.T, records, controls []kafkaFaultRecord, samples int, leaseDuration time.Duration) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "artifacts", "kafka-faults")
	if err = os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	writeKafkaFaultRecords(t, filepath.Join(directory, "fault-results.csv"), records)
	writeKafkaFaultRecords(t, filepath.Join(directory, "control-results.csv"), controls)

	f3 := make([]float64, 0, samples)
	violations := 0
	for _, record := range records {
		if record.window == "F3" {
			f3 = append(f3, record.recoveredAt.Sub(record.injectedAt).Seconds()*1000)
		}
		if record.invariantViolation {
			violations++
		}
	}
	sort.Float64s(f3)
	summary, err := os.Create(filepath.Join(directory, "fault-summary.csv"))
	if err != nil {
		t.Fatal(err)
	}
	summaryWriter := csv.NewWriter(summary)
	_ = summaryWriter.Write([]string{"key", "value"})
	_ = summaryWriter.Write([]string{"profile", "local-infrastructure-production-semantics"})
	_ = summaryWriter.Write([]string{"samples_per_window", strconv.Itoa(samples)})
	_ = summaryWriter.Write([]string{"fault_count", strconv.Itoa(len(records))})
	_ = summaryWriter.Write([]string{"control_count", strconv.Itoa(len(controls))})
	_ = summaryWriter.Write([]string{"lease_duration", leaseDuration.String()})
	_ = summaryWriter.Write([]string{"worker_crash_recovery_p95_ms", fmt.Sprintf("%.3f", percentileFloat(f3, 0.95))})
	_ = summaryWriter.Write([]string{"invariant_violations", strconv.Itoa(violations)})
	summaryWriter.Flush()
	if err = summaryWriter.Error(); err != nil {
		summary.Close()
		t.Fatal(err)
	}
	if err = summary.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeKafkaFaultRecords(t *testing.T, path string, records []kafkaFaultRecord) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	_ = writer.Write([]string{
		"fault_window", "run_id", "event_id", "attempt_id", "injected_at",
		"recovered_at", "terminal_at", "recovery_ms", "terminal_ms", "duplicate_deliveries",
		"inbox_conflicts", "stale_complete_accepted", "invariant_violation", "outcome",
	})
	for _, record := range records {
		recoveryMS := record.recoveredAt.Sub(record.injectedAt).Seconds() * 1000
		terminalMS := record.terminalAt.Sub(record.injectedAt).Seconds() * 1000
		_ = writer.Write([]string{
			record.window, record.runID, record.eventID, record.attemptID,
			record.injectedAt.Format(time.RFC3339Nano), record.recoveredAt.Format(time.RFC3339Nano),
			record.terminalAt.Format(time.RFC3339Nano), fmt.Sprintf("%.3f", recoveryMS),
			fmt.Sprintf("%.3f", terminalMS), strconv.Itoa(record.duplicateDeliveries),
			strconv.Itoa(record.inboxConflicts), strconv.FormatBool(record.staleCompleteAccepted),
			strconv.FormatBool(record.invariantViolation), record.outcome,
		})
	}
	writer.Flush()
	if err = writer.Error(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
}

func percentileFloat(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(len(values))*percentile)) - 1
	if index < 0 {
		index = 0
	}
	return values[index]
}
