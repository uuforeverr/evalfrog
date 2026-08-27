//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/uu999/evalfrog/internal/adapters/cacheredis"
	"github.com/uu999/evalfrog/internal/adapters/kafka"
	"github.com/uu999/evalfrog/internal/adapters/workerapi"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/recovery"
	platformruntime "github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
	"github.com/uu999/evalfrog/internal/scheduling"
	workerruntime "github.com/uu999/evalfrog/internal/worker/runtime"
)

func TestM7KafkaWorkerCoordinatorEngineEndToEnd(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m7-e2e-run")
	harness.initializeRun(t, run)
	task := queuedTask(t, harness, run.ID)
	attemptID := task.AttemptID
	queued := struct {
		ID       string
		Sequence uint32
	}{task.AttemptID, task.AttemptSequence}

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(config.LoadOptions{Directory: filepath.Join(root, "configs"), Profile: "local"})
	if err != nil {
		t.Fatal(err)
	}
	identifier := uuid.NewString()
	configuration.Kafka.TopicPrefix = "evalfrog.m7." + identifier
	createKafkaTopics(t, harness.ctx, configuration.Kafka)
	publisher, err := kafka.Open(configuration.Kafka, "m7-publisher-"+identifier)
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	cacheConfig := configuration.Redis.Cache
	cacheConfig.KeyPrefix += "m7:" + identifier + ":"
	cacheConfig.OperationTimeout = config.Duration(20 * time.Millisecond)
	cacheConfig.Address = "127.0.0.1:1" // prove Cache Redis failure falls back to PostgreSQL
	cache := cacheredis.Open(cacheConfig)
	defer cache.Close()
	gateway, err := runtimecontext.NewGateway(harness.store, cache, time.Hour, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	controlPlanes := []*httptest.Server{
		httptest.NewServer(workerapi.NewHandler(harness.coordinator, gateway)),
		httptest.NewServer(workerapi.NewHandler(harness.coordinator, gateway)),
	}
	defer controlPlanes[0].Close()
	defer controlPlanes[1].Close()
	workerClients := []*workerapi.Client{
		workerapi.New(controlPlanes[0].URL, time.Second),
		workerapi.New(controlPlanes[1].URL, time.Second),
	}
	catalog, err := workerruntime.NewCatalog(scheduling.ResourceSandbox, workerruntime.EchoTestExecutor{Operation: dsl.Coordinate{Type: "task.python", Version: 1}})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(harness.ctx, 60*time.Second)
	defer cancel()
	done := make(chan error, 4)
	for replica := 0; replica < 2; replica++ {
		suffix := fmt.Sprintf("-%d", replica)
		taskConsumer, consumerErr := kafka.OpenConsumer(configuration.Kafka, "m7-task-"+identifier+suffix, "m7-task-"+identifier, []config.KafkaTopicConfig{configuration.Kafka.Topics.SandboxTask}, 1)
		if consumerErr != nil {
			t.Fatal(consumerErr)
		}
		defer taskConsumer.Close()
		workerID := "worker-" + identifier + suffix
		worker, workerErr := workerruntime.New(&projectConsumer{Consumer: taskConsumer, projectID: harness.projectID, runID: run.ID, expectedKey: attemptID}, workerClients[replica], catalog, workerruntime.Settings{WorkerID: workerID, ExecutorBuild: "m7-test", ResourceClass: scheduling.ResourceSandbox, Slots: 1, LeaseDuration: 3 * time.Second, HeartbeatInterval: 200 * time.Millisecond, ClaimTimeout: time.Second, CompleteTimeout: time.Second}, logger)
		if workerErr != nil {
			t.Fatal(workerErr)
		}
		runtimeConsumer, consumerErr := kafka.OpenConsumer(configuration.Kafka, "m7-runtime-"+identifier+suffix, "m7-runtime-"+identifier, []config.KafkaTopicConfig{configuration.Kafka.Topics.RuntimeEvent}, 1)
		if consumerErr != nil {
			t.Fatal(consumerErr)
		}
		defer runtimeConsumer.Close()
		engineService, engineErr := eventing.NewRuntimeConsumerService(&projectConsumer{Consumer: runtimeConsumer, projectID: harness.projectID, runID: run.ID, expectedKey: run.ID}, harness.consumer, logger)
		if engineErr != nil {
			t.Fatal(engineErr)
		}
		go func() { done <- worker.Run(ctx) }()
		go func() { done <- engineService.Run(ctx) }()
	}
	// Both group members must enter Poll before publishing, otherwise the first
	// record can be consumed before Kafka has exercised replica assignment.
	time.Sleep(250 * time.Millisecond)

	taskRelay, err := eventing.NewTaskRelay(harness.store, publisher, "task-relay-"+identifier, 10, time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if count, relayErr := relayUntilPublished(ctx, taskRelay); relayErr != nil || count != 1 {
		t.Fatalf("task relay count=%d err=%v", count, relayErr)
	}
	waitFor(t, ctx, func() bool { return attemptState(t, harness, queued.ID) == "succeeded" }, "worker completion")
	runtimeRelay, err := eventing.NewRelay(harness.store, publisher, "runtime-relay-"+identifier, 10, time.Second, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if count, relayErr := relayUntilPublished(ctx, runtimeRelay); relayErr != nil || count < 1 {
		t.Fatalf("runtime relay count=%d err=%v", count, relayErr)
	}
	waitFor(t, ctx, func() bool { return runState(t, harness, run.ID) == "succeeded" }, "engine effective output")
	// A Kafka redelivery after completion must be ACKed as stale and cannot
	// create a second output candidate or effective result.
	if err = publisher.PublishTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	var outputCandidates int
	if err = harness.client.Pool().QueryRow(ctx, `SELECT count(*) FROM node_output_values WHERE project_id=$1 AND run_id=$2 AND attempt_id=$3`, harness.projectID, run.ID, attemptID).Scan(&outputCandidates); err != nil || outputCandidates != 1 {
		t.Fatalf("duplicate task output candidates=%d err=%v", outputCandidates, err)
	}
	cancel()
	for stopped := 0; stopped < 4; stopped++ {
		select {
		case serviceErr := <-done:
			if serviceErr != nil && !errors.Is(serviceErr, context.Canceled) {
				t.Fatalf("replica stopped with error: %v", serviceErr)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("distributed execution replica did not stop")
		}
	}
}

func TestM7ExecutionContextLoadsEntryNodeInput(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createEntryRefCodeWorkflow(t)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m7-entry-input")
	harness.initializeRun(t, run)
	task := queuedTask(t, harness, run.ID)
	var err error
	lease, err := harness.coordinator.Claim(harness.ctx, attempt.ClaimCommand{
		ProjectID: task.ProjectID, RunID: task.RunID, AttemptID: task.AttemptID,
		AttemptSequence: task.AttemptSequence, WorkerID: "worker-entry-input", ExecutorBuild: "m7-test",
		ResourceClass: scheduling.ResourceSandbox,
		Capabilities:  []dsl.Coordinate{{Type: "task.python", Version: 1}},
		LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(config.LoadOptions{Directory: filepath.Join(root, "configs"), Profile: "local"})
	if err != nil {
		t.Fatal(err)
	}
	cacheConfig := configuration.Redis.Cache
	cacheConfig.KeyPrefix += "m7-entry:" + uuid.NewString() + ":"
	cacheConfig.OperationTimeout = config.Duration(20 * time.Millisecond)
	cacheConfig.Address = "127.0.0.1:1" // prove Cache Redis failure falls back to PostgreSQL
	cache := cacheredis.Open(cacheConfig)
	defer cache.Close()
	gateway, err := runtimecontext.NewGateway(harness.store, cache, time.Hour, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	value, err := gateway.Load(harness.ctx, runtimecontext.LoadCommand{
		ProjectID: task.ProjectID, RunID: task.RunID, AttemptID: task.AttemptID,
		AttemptSequence: task.AttemptSequence, LeaseToken: lease.Token, FencingToken: lease.FencingToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	var resolved any
	if err = json.Unmarshal(value.Inputs["request"], &resolved); err != nil {
		t.Fatal(err)
	}
	var workflowInput any
	if err = json.Unmarshal([]byte(`{"value":7}`), &workflowInput); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(resolved, workflowInput) {
		t.Fatalf("resolved entry input=%s", value.Inputs["request"])
	}
	if len(value.UpstreamOutputs) != 1 {
		t.Fatalf("upstream outputs=%+v", value.UpstreamOutputs)
	}
	for _, raw := range value.UpstreamOutputs {
		var upstream any
		if err = json.Unmarshal(raw, &upstream); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(upstream, workflowInput) {
			t.Fatalf("upstream entry value=%s", raw)
		}
	}
}

func createKafkaTopics(t *testing.T, ctx context.Context, configuration config.KafkaConfig) {
	t.Helper()
	admin, err := kgo.NewClient(kgo.SeedBrokers(configuration.Brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	request := kmsg.NewPtrCreateTopicsRequest()
	for _, topic := range []config.KafkaTopicConfig{configuration.Topics.BuiltinTask, configuration.Topics.SandboxTask, configuration.Topics.RuntimeEvent, configuration.Topics.DLQ} {
		value := kmsg.NewCreateTopicsRequestTopic()
		value.Topic = configuration.TopicPrefix + "." + topic.Name
		value.NumPartitions = int32(topic.Partitions)
		value.ReplicationFactor = int16(configuration.ReplicationFactor)
		request.Topics = append(request.Topics, value)
	}
	response, err := request.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range response.Topics {
		if topicErr := kerr.ErrorForCode(topic.ErrorCode); topicErr != nil {
			t.Fatalf("create Kafka topic %s: %v", topic.Topic, topicErr)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanup, cleanupErr := kgo.NewClient(kgo.SeedBrokers(configuration.Brokers...))
		if cleanupErr != nil {
			return
		}
		defer cleanup.Close()
		remove := kmsg.NewPtrDeleteTopicsRequest()
		for _, topic := range request.Topics {
			value := kmsg.NewDeleteTopicsRequestTopic()
			value.Topic = &topic.Topic
			remove.Topics = append(remove.Topics, value)
		}
		_, _ = remove.RequestWith(cleanupCtx, cleanup)
	})
}

func TestM7ExpiredLeaseBecomesLostAndStaleResultIsRejected(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m7-lease-recovery")
	harness.initializeRun(t, run)
	task := queuedTask(t, harness, run.ID)
	var err error
	lease, err := harness.coordinator.Claim(harness.ctx, attempt.ClaimCommand{ProjectID: task.ProjectID, RunID: task.RunID, AttemptID: task.AttemptID, AttemptSequence: task.AttemptSequence, WorkerID: "crashed-worker", ExecutorBuild: "m7-test", ResourceClass: scheduling.ResourceSandbox, Capabilities: []dsl.Coordinate{{Type: "task.python", Version: 1}}, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, harness.ctx, harness.client.Pool(), `UPDATE node_attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE attempt_id=$1`, task.AttemptID)
	reaper, err := recovery.NewReaper(harness.store, harness.coordinator, 0, time.Second, 10, "m7-reaper", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err = reaper.ScanOnce(harness.ctx); err != nil {
		t.Fatal(err)
	}
	if state := attemptState(t, harness, task.AttemptID); state != "lost" {
		t.Fatalf("attempt state=%s", state)
	}
	_, err = harness.coordinator.Complete(harness.ctx, attempt.CompleteCommand{ProjectID: task.ProjectID, RunID: task.RunID, AttemptID: task.AttemptID, AttemptSequence: task.AttemptSequence, LeaseToken: lease.Token, FencingToken: lease.FencingToken, TraceID: "late", Result: platformruntime.AttemptResult{State: platformruntime.AttemptSucceeded, Outputs: map[string]json.RawMessage{"result": json.RawMessage(`{}`)}}})
	if err == nil {
		t.Fatal("stale completion accepted")
	}
	runtimeEvent := harness.event(t, eventing.AttemptLost, task.AttemptID)
	if err = harness.consumer.Consume(harness.ctx, runtimeEvent); err != nil {
		t.Fatal(err)
	}
	var nodeState string
	if err = harness.client.Pool().QueryRow(harness.ctx, `SELECT state FROM node_runs WHERE run_id=$1 AND current_attempt_id=$2`, run.ID, task.AttemptID).Scan(&nodeState); err != nil || nodeState != "retry_wait" {
		t.Fatalf("node=%s err=%v", nodeState, err)
	}
}

// M7 uses a unique topic prefix. This wrapper additionally verifies ownership
// and message keys so a transport isolation regression fails loudly.
type projectConsumer struct {
	eventing.Consumer
	projectID, runID, expectedKey string
}

func queuedTask(t *testing.T, harness *m5Harness, runID string) eventing.TaskMessage {
	t.Helper()
	var task eventing.TaskMessage
	err := harness.client.Pool().QueryRow(harness.ctx, `
		SELECT message_version, task_id::text, project_id::text, run_id::text,
		       node_run_id::text, execution_node_id, attempt_id::text,
		       attempt_seq, resource_class, occurred_at, trace_id
		FROM node_task_outbox
		WHERE project_id=$1 AND run_id=$2
		ORDER BY created_at, task_id
		LIMIT 1`, harness.projectID, runID).Scan(
		&task.MessageVersion, &task.TaskID, &task.ProjectID, &task.RunID,
		&task.NodeRunID, &task.ExecutionNodeID, &task.AttemptID,
		&task.AttemptSequence, &task.ResourceClass, &task.OccurredAt, &task.TraceID)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func (value *projectConsumer) Receive(ctx context.Context) (eventing.Delivery, error) {
	for {
		delivery, err := value.Consumer.Receive(ctx)
		if err != nil {
			return nil, err
		}
		var coordinate struct {
			ProjectID string `json:"project_id"`
			RunID     string `json:"run_id"`
		}
		if json.Unmarshal(delivery.Payload(), &coordinate) == nil && coordinate.ProjectID == value.projectID && coordinate.RunID == value.runID {
			if delivery.Key() != value.expectedKey {
				return nil, fmt.Errorf("Kafka key=%q, want %q", delivery.Key(), value.expectedKey)
			}
			return delivery, nil
		}
		if err = delivery.Ack(ctx); err != nil {
			return nil, err
		}
	}
}

func attemptState(t *testing.T, harness *m5Harness, attemptID string) string {
	t.Helper()
	var state string
	_ = harness.client.Pool().QueryRow(harness.ctx, `SELECT state FROM node_attempts WHERE attempt_id=$1`, attemptID).Scan(&state)
	return state
}
func runState(t *testing.T, harness *m5Harness, runID string) string {
	t.Helper()
	var state string
	_ = harness.client.Pool().QueryRow(harness.ctx, `SELECT state FROM workflow_runs WHERE run_id=$1`, runID).Scan(&state)
	return state
}
func waitFor(t *testing.T, ctx context.Context, predicate func() bool, name string) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if predicate() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", name)
		case <-ticker.C:
		}
	}
}

type oneShotRelay interface {
	RelayOnce(context.Context) (int, error)
}

func relayUntilPublished(ctx context.Context, relay oneShotRelay) (int, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		count, err := relay.RelayOnce(ctx)
		if err != nil || count > 0 {
			return count, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
		}
	}
}
