package eventing

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/scheduling"
)

func TestTaskContractIsStrictLightweightAndBounded(t *testing.T) {
	message := testTask()
	converted := TaskFromScheduling(scheduling.Task{MessageVersion: 1, TaskID: message.TaskID, ProjectID: message.ProjectID, RunID: message.RunID, NodeRunID: message.NodeRunID, ExecutionNodeID: message.ExecutionNodeID, AttemptID: message.AttemptID, AttemptSequence: message.AttemptSequence, ResourceClass: message.ResourceClass, OccurredAt: message.OccurredAt, TraceID: message.TraceID})
	if !reflect.DeepEqual(converted, message) {
		t.Fatalf("converted=%+v", converted)
	}
	payload, err := message.MarshalJSONMessage()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseTaskMessage(payload)
	if err != nil || !reflect.DeepEqual(parsed, message) {
		t.Fatalf("parsed=%+v err=%v", parsed, err)
	}
	for _, forbidden := range [][]byte{[]byte(`"dsl"`), []byte(`"inputs"`), []byte(`"outputs"`), []byte(`"secret"`), []byte(`"execution_context"`)} {
		if bytes.Contains(payload, forbidden) {
			t.Fatalf("forbidden payload field %s", forbidden)
		}
	}
	invalid := [][]byte{
		[]byte(`{"message_version":2}`),
		[]byte(`{"message_version":1,"unknown":true}`),
		append(payload, []byte(` {}`)...),
		bytes.Repeat([]byte("x"), EnvelopeMaxBytes+1),
	}
	for _, value := range invalid {
		if _, err = ParseTaskMessage(value); err == nil {
			t.Fatalf("invalid task accepted")
		}
	}
}

func TestTaskRelayIsAtLeastOnce(t *testing.T) {
	repository := &fakeTaskOutbox{claimed: []ClaimedTask{{Message: testTask(), ClaimToken: "claim"}}}
	publisher := &fakeTaskPublisher{err: errors.New("unavailable")}
	relay, err := NewTaskRelay(repository, publisher, "owner", 1, time.Second, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := relay.RelayOnce(context.Background()); count != 0 || err == nil || repository.released != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	publisher.err = nil
	if count, err := relay.RelayOnce(context.Background()); count != 1 || err != nil || repository.marked != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if _, err := NewTaskRelay(nil, publisher, "owner", 1, time.Second, 0); err == nil {
		t.Fatal("invalid relay accepted")
	}
}

func TestTaskRelayBatchMarksAndReleasesPartialKafkaResults(t *testing.T) {
	first, second := testTask(), testTask()
	first.TaskID, second.TaskID = "task-1", "task-2"
	repository := &batchFakeTaskOutbox{fakeTaskOutbox: fakeTaskOutbox{claimed: []ClaimedTask{{Message: first, ClaimToken: "claim-1"}, {Message: second, ClaimToken: "claim-2"}}}}
	publisher := &batchFakeTaskPublisher{outcomes: []error{errors.New("first failed"), nil}}
	relay, _ := NewTaskRelay(repository, publisher, "owner", 2, time.Second, time.Millisecond)
	count, err := relay.RelayOnce(context.Background())
	if count != 1 || err == nil || len(repository.batchMarked) != 1 || repository.batchMarked[0].ID != second.TaskID || len(repository.batchReleased) != 1 || repository.batchReleased[0].ID != first.TaskID {
		t.Fatalf("count=%d err=%v marked=%+v released=%+v", count, err, repository.batchMarked, repository.batchReleased)
	}
}

type fakeTaskOutbox struct {
	claimed          []ClaimedTask
	marked, released int
}

func (value *fakeTaskOutbox) ClaimTaskOutbox(context.Context, string, int, time.Duration) ([]ClaimedTask, error) {
	return value.claimed, nil
}
func (value *fakeTaskOutbox) MarkTaskOutboxPublished(context.Context, string, string) error {
	value.marked++
	return nil
}
func (value *fakeTaskOutbox) ReleaseTaskOutboxClaim(context.Context, string, string, time.Duration) error {
	value.released++
	return nil
}

type fakeTaskPublisher struct{ err error }

func (value *fakeTaskPublisher) PublishTask(context.Context, TaskMessage) error { return value.err }

type batchFakeTaskOutbox struct {
	fakeTaskOutbox
	batchMarked, batchReleased []ClaimedIdentity
}

func (repository *batchFakeTaskOutbox) MarkTaskOutboxPublishedBatch(_ context.Context, values []ClaimedIdentity) error {
	repository.batchMarked = append(repository.batchMarked, values...)
	return nil
}
func (repository *batchFakeTaskOutbox) ReleaseTaskOutboxClaimsBatch(_ context.Context, values []ClaimedIdentity, _ time.Duration) error {
	repository.batchReleased = append(repository.batchReleased, values...)
	return nil
}

type batchFakeTaskPublisher struct{ outcomes []error }

func (*batchFakeTaskPublisher) PublishTask(context.Context, TaskMessage) error { return nil }
func (publisher *batchFakeTaskPublisher) PublishTasks(context.Context, []TaskMessage) []error {
	return publisher.outcomes
}

func testTask() TaskMessage {
	return TaskMessage{MessageVersion: 1, TaskID: "task", ProjectID: "project", RunID: "run", NodeRunID: "node-run", ExecutionNodeID: "node", AttemptID: "attempt", AttemptSequence: 1, ResourceClass: scheduling.ResourceSandbox, OccurredAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), TraceID: "trace"}
}
