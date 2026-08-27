package eventing

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeEventContractIsStrictVersionedAndLightweight(t *testing.T) {
	event := testEvent()
	payload, err := event.MarshalJSONMessage()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseRuntimeEvent(payload)
	if err != nil || parsed.EventID != event.EventID {
		t.Fatalf("parsed=%+v err=%v", parsed, err)
	}
	for _, payload := range [][]byte{
		[]byte(`{"message_version":2}`),
		[]byte(`{"message_version":1,"unknown":true}`),
		[]byte(`{"message_version":1,"event_id":"event","project_id":"project","run_id":"run","aggregate_type":"workflow_run","aggregate_id":"other","event_type":"run.created","occurred_at":"2026-01-01T00:00:00Z","trace_id":"trace"}`),
	} {
		if _, err := ParseRuntimeEvent(payload); err == nil {
			t.Fatalf("invalid event accepted: %s", payload)
		}
	}
	for _, eventType := range []RuntimeEventType{AttemptCompleted, AttemptLost, RetryDue} {
		attemptEvent := event
		attemptEvent.EventType = eventType
		attemptEvent.AggregateType = NodeAttemptAggregate
		attemptEvent.AggregateID = "attempt"
		if err := attemptEvent.Validate(); err != nil {
			t.Fatalf("valid %s rejected: %v", eventType, err)
		}
	}
	wrongAggregate := event
	wrongAggregate.EventType = AttemptCompleted
	wrongAggregate.AggregateID = "attempt"
	if err := wrongAggregate.Validate(); err == nil {
		t.Fatal("attempt event with workflow-run aggregate accepted")
	}
	invalid := event
	invalid.EventType = "unknown"
	if err := invalid.Validate(); err == nil {
		t.Fatal("unknown event type accepted")
	}
	invalid = event
	invalid.EventID = ""
	if _, err := invalid.MarshalJSONMessage(); err == nil {
		t.Fatal("invalid event marshaled")
	}
}

func TestRuntimeConsumerAcksSuccessDeadLettersPoisonAndNacksRetryableFailure(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	valid, _ := testEvent().MarshalJSONMessage()
	consumer := &sequenceConsumer{deliveries: []*testDelivery{{payload: valid}}}
	ctx, cancel := context.WithCancel(context.Background())
	handler := &testRuntimeHandler{after: cancel}
	service, err := NewRuntimeConsumerService(consumer, handler, logger)
	if err != nil {
		t.Fatal(err)
	}
	if service.Name() != "runtime-event-consumer" {
		t.Fatal("consumer service name missing")
	}
	_ = service.Run(ctx)
	if consumer.deliveries[0].acked.Load() != 1 || handler.calls.Load() != 1 {
		t.Fatal("successful event was not handled and acked")
	}

	ctx, cancel = context.WithCancel(context.Background())
	consumer = &sequenceConsumer{deliveries: []*testDelivery{{payload: []byte(`{"bad":true}`)}}}
	consumer.after = cancel
	service, _ = NewRuntimeConsumerService(consumer, &testRuntimeHandler{}, logger)
	_ = service.Run(ctx)
	if consumer.deliveries[0].dead.Load() != 1 {
		t.Fatal("poison event was not dead-lettered")
	}

	ctx, cancel = context.WithCancel(context.Background())
	consumer = &sequenceConsumer{deliveries: []*testDelivery{{payload: valid}}}
	handler = &testRuntimeHandler{err: errors.New("postgres down"), after: cancel}
	service, _ = NewRuntimeConsumerService(consumer, handler, logger)
	_ = service.Run(ctx)
	if consumer.deliveries[0].nacked.Load() != 1 || consumer.deliveries[0].acked.Load() != 0 {
		t.Fatal("retryable failure was not nacked")
	}
	_ = service.Shutdown(context.Background())
	if _, err = NewRuntimeConsumerService(nil, handler, logger); err == nil {
		t.Fatal("invalid consumer service accepted")
	}
	consumer = &sequenceConsumer{deliveries: []*testDelivery{{payload: valid, ackErr: errors.New("commit failed")}}}
	service, _ = NewRuntimeConsumerService(consumer, &testRuntimeHandler{}, logger)
	if err = service.Run(context.Background()); err == nil {
		t.Fatal("ack failure hidden")
	}
	consumer = &sequenceConsumer{deliveries: []*testDelivery{{payload: []byte(`{`), deadErr: errors.New("dlq failed")}}}
	service, _ = NewRuntimeConsumerService(consumer, &testRuntimeHandler{}, logger)
	if err = service.Run(context.Background()); err == nil {
		t.Fatal("DLQ failure hidden")
	}
}

func TestRuntimeConsumerProcessesKafkaPollAsOneEngineBatch(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	valid, _ := testEvent().MarshalJSONMessage()
	ctx, cancel := context.WithCancel(context.Background())
	batch := &testDeliveryBatch{messages: []BatchMessage{{Payload: valid}, {Payload: []byte(`{"bad":true}`)}}, after: cancel}
	consumer := &testBatchConsumer{batch: batch}
	handler := &testBatchRuntimeHandler{}
	service, err := NewRuntimeConsumerService(consumer, handler, logger)
	if err != nil {
		t.Fatal(err)
	}
	_ = service.Run(ctx)
	if batch.acked.Load() != 1 || batch.dead.Load() != 1 || batch.nacked.Load() != 0 || handler.events.Load() != 1 {
		t.Fatalf("acked=%d dead=%d nacked=%d events=%d", batch.acked.Load(), batch.dead.Load(), batch.nacked.Load(), handler.events.Load())
	}

	ctx, cancel = context.WithCancel(context.Background())
	batch = &testDeliveryBatch{messages: []BatchMessage{{Payload: valid}}, after: cancel}
	consumer = &testBatchConsumer{batch: batch}
	handler = &testBatchRuntimeHandler{err: errors.New("postgres unavailable")}
	service, _ = NewRuntimeConsumerService(consumer, handler, logger)
	if err = service.Run(ctx); err == nil || batch.nacked.Load() != 1 || batch.acked.Load() != 0 {
		t.Fatalf("batch failure err=%v acked=%d nacked=%d", err, batch.acked.Load(), batch.nacked.Load())
	}
}

func TestRelayServiceBackoffAndShutdown(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	relay := &testBatchRelay{after: cancel}
	service, err := NewRelayService("relay", relay, time.Millisecond, 2*time.Millisecond, logger)
	if err != nil {
		t.Fatal(err)
	}
	_ = service.Run(ctx)
	if relay.calls.Load() == 0 || service.Name() != "relay" {
		t.Fatal("relay did not run")
	}
	_ = service.Shutdown(context.Background())
	if _, err = NewRelayService("", relay, 0, 0, logger); err == nil {
		t.Fatal("invalid relay service accepted")
	}
}

type testDelivery struct {
	payload             []byte
	acked, nacked, dead atomic.Int32
	ackErr, deadErr     error
}

func (value *testDelivery) Topic() string             { return "topic" }
func (value *testDelivery) Key() string               { return "key" }
func (value *testDelivery) Payload() []byte           { return value.payload }
func (value *testDelivery) Ack(context.Context) error { value.acked.Add(1); return value.ackErr }
func (value *testDelivery) Nack()                     { value.nacked.Add(1) }
func (value *testDelivery) DeadLetter(context.Context, string) error {
	value.dead.Add(1)
	return value.deadErr
}

type sequenceConsumer struct {
	deliveries []*testDelivery
	index      int
	after      context.CancelFunc
}

func (value *sequenceConsumer) Receive(ctx context.Context) (Delivery, error) {
	if value.index >= len(value.deliveries) {
		if value.after != nil {
			value.after()
			value.after = nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	delivery := value.deliveries[value.index]
	value.index++
	return delivery, nil
}

type testRuntimeHandler struct {
	calls atomic.Int32
	err   error
	after context.CancelFunc
}

type testDeliveryBatch struct {
	messages            []BatchMessage
	acked, nacked, dead atomic.Int32
	after               context.CancelFunc
}

func (batch *testDeliveryBatch) Messages() []BatchMessage { return batch.messages }
func (batch *testDeliveryBatch) Ack(context.Context) error {
	batch.acked.Add(1)
	if batch.after != nil {
		batch.after()
		batch.after = nil
	}
	return nil
}
func (batch *testDeliveryBatch) Nack() {
	batch.nacked.Add(1)
	if batch.after != nil {
		batch.after()
		batch.after = nil
	}
}
func (batch *testDeliveryBatch) DeadLetter(context.Context, int, string) error {
	batch.dead.Add(1)
	return nil
}

type testBatchConsumer struct {
	batch *testDeliveryBatch
	used  bool
}

func (consumer *testBatchConsumer) Receive(context.Context) (Delivery, error) {
	return nil, errors.New("single receive should not be used")
}
func (consumer *testBatchConsumer) ReceiveBatch(ctx context.Context) (DeliveryBatch, error) {
	if !consumer.used {
		consumer.used = true
		return consumer.batch, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type testBatchRuntimeHandler struct {
	events atomic.Int32
	err    error
}

func (handler *testBatchRuntimeHandler) Consume(context.Context, RuntimeEvent) error {
	return errors.New("single handler should not be used")
}
func (handler *testBatchRuntimeHandler) ConsumeBatch(_ context.Context, events []RuntimeEvent) error {
	handler.events.Add(int32(len(events)))
	return handler.err
}

func (value *testRuntimeHandler) Consume(context.Context, RuntimeEvent) error {
	value.calls.Add(1)
	if value.after != nil {
		value.after()
		value.after = nil
	}
	return value.err
}

type testBatchRelay struct {
	calls atomic.Int32
	after context.CancelFunc
}

func (value *testBatchRelay) RelayOnce(context.Context) (int, error) {
	value.calls.Add(1)
	if value.after != nil {
		value.after()
		value.after = nil
	}
	return 0, nil
}

func TestRelayIsAtLeastOnceAndReleasesPublishFailure(t *testing.T) {
	repository := &fakeOutbox{claimed: []ClaimedEvent{{Event: testEvent(), ClaimToken: "claim"}}}
	publisher := &fakePublisher{err: errors.New("publish failed")}
	relay, err := NewRelay(repository, publisher, "relay", 10, time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if count, relayErr := relay.RelayOnce(context.Background()); count != 0 || relayErr == nil || repository.released != 1 {
		t.Fatalf("count=%d err=%v released=%d", count, relayErr, repository.released)
	}
	publisher.err = nil
	if count, relayErr := relay.RelayOnce(context.Background()); count != 1 || relayErr != nil || repository.marked != 1 {
		t.Fatalf("count=%d err=%v marked=%d", count, relayErr, repository.marked)
	}
}

func TestRelayValidatesConstructionAndPropagatesRepositoryFailures(t *testing.T) {
	if _, err := NewRelay(nil, &fakePublisher{}, "relay", 1, time.Second, 0); err == nil {
		t.Fatal("relay without repository accepted")
	}
	repository := &fakeOutbox{claimErr: errors.New("claim failed")}
	relay, _ := NewRelay(repository, &fakePublisher{}, "relay", 1, time.Second, 0)
	if _, err := relay.RelayOnce(context.Background()); err == nil {
		t.Fatal("claim failure was hidden")
	}
	repository.claimErr = nil
	repository.claimed = []ClaimedEvent{{Event: testEvent(), ClaimToken: "claim"}}
	repository.markErr = errors.New("mark failed")
	if count, err := relay.RelayOnce(context.Background()); count != 0 || err == nil {
		t.Fatalf("count=%d mark error=%v", count, err)
	}
}

func TestRelayBatchMarksOnlyKafkaAcknowledgedRows(t *testing.T) {
	first, second := testEvent(), testEvent()
	first.EventID, second.EventID = "event-1", "event-2"
	repository := &batchFakeOutbox{fakeOutbox: fakeOutbox{claimed: []ClaimedEvent{{Event: first, ClaimToken: "claim-1"}, {Event: second, ClaimToken: "claim-2"}}}}
	publisher := &batchFakePublisher{outcomes: []error{nil, errors.New("broker rejected second record")}}
	relay, _ := NewRelay(repository, publisher, "relay", 10, time.Minute, time.Second)
	count, err := relay.RelayOnce(context.Background())
	if count != 1 || err == nil || len(repository.batchMarked) != 1 || repository.batchMarked[0].ID != first.EventID || len(repository.batchReleased) != 1 || repository.batchReleased[0].ID != second.EventID {
		t.Fatalf("count=%d err=%v marked=%+v released=%+v", count, err, repository.batchMarked, repository.batchReleased)
	}
}

type fakeOutbox struct {
	claimed  []ClaimedEvent
	marked   int
	released int
	claimErr error
	markErr  error
}

func (repository *fakeOutbox) ClaimOutbox(context.Context, string, int, time.Duration) ([]ClaimedEvent, error) {
	return repository.claimed, repository.claimErr
}
func (repository *fakeOutbox) MarkOutboxPublished(context.Context, string, string) error {
	repository.marked++
	return repository.markErr
}
func (repository *fakeOutbox) ReleaseOutboxClaim(context.Context, string, string, time.Duration) error {
	repository.released++
	return nil
}

type fakePublisher struct{ err error }

func (publisher *fakePublisher) PublishRuntimeEvent(context.Context, RuntimeEvent) error {
	return publisher.err
}

type batchFakeOutbox struct {
	fakeOutbox
	batchMarked, batchReleased []ClaimedIdentity
}

func (repository *batchFakeOutbox) MarkOutboxPublishedBatch(_ context.Context, values []ClaimedIdentity) error {
	repository.batchMarked = append(repository.batchMarked, values...)
	return nil
}
func (repository *batchFakeOutbox) ReleaseOutboxClaimsBatch(_ context.Context, values []ClaimedIdentity, _ time.Duration) error {
	repository.batchReleased = append(repository.batchReleased, values...)
	return nil
}

type batchFakePublisher struct{ outcomes []error }

func (*batchFakePublisher) PublishRuntimeEvent(context.Context, RuntimeEvent) error { return nil }
func (publisher *batchFakePublisher) PublishRuntimeEvents(context.Context, []RuntimeEvent) []error {
	return publisher.outcomes
}

func testEvent() RuntimeEvent {
	return RuntimeEvent{
		MessageVersion: 1, EventID: "event", ProjectID: "project", RunID: "run",
		AggregateType: WorkflowRunAggregate, AggregateID: "run", EventType: RunCreated,
		OccurredAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), TraceID: "trace",
	}
}
