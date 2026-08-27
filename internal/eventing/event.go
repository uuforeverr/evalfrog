// Package eventing owns Outbox, Inbox, and versioned runtime message contracts.
package eventing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const RuntimeMessageVersion = 1

type RuntimeEventType string

const (
	RunCreated         RuntimeEventType = "run.created"
	RunCancelRequested RuntimeEventType = "run.cancel_requested"
	AttemptCompleted   RuntimeEventType = "attempt.completed"
	AttemptLost        RuntimeEventType = "attempt.lost"
	RetryDue           RuntimeEventType = "retry.due"
	RunDeadlineReached RuntimeEventType = "run.deadline_reached"
)

type AggregateType string

const (
	WorkflowRunAggregate AggregateType = "workflow_run"
	NodeAttemptAggregate AggregateType = "node_attempt"
)

// RuntimeEvent is deliberately a lightweight wake-up signal. Consumers must
// reload DSL, Attempt Result and Output from PostgreSQL authority.
type RuntimeEvent struct {
	MessageVersion int              `json:"message_version"`
	EventID        string           `json:"event_id"`
	ProjectID      string           `json:"project_id"`
	RunID          string           `json:"run_id"`
	AggregateType  AggregateType    `json:"aggregate_type"`
	AggregateID    string           `json:"aggregate_id"`
	EventType      RuntimeEventType `json:"event_type"`
	OccurredAt     time.Time        `json:"occurred_at"`
	TraceID        string           `json:"trace_id"`
}

func (event RuntimeEvent) Validate() error {
	if event.MessageVersion != RuntimeMessageVersion || event.EventID == "" || event.ProjectID == "" || event.RunID == "" || event.AggregateID == "" || event.TraceID == "" || event.OccurredAt.IsZero() {
		return fmt.Errorf("runtime event identity, v1 version, occurrence and trace are required")
	}
	switch event.EventType {
	case RunCreated, RunCancelRequested, RunDeadlineReached:
		if event.AggregateType != WorkflowRunAggregate || event.AggregateID != event.RunID {
			return fmt.Errorf("run event must target its workflow run")
		}
	case AttemptCompleted, AttemptLost, RetryDue:
		if event.AggregateType != NodeAttemptAggregate {
			return fmt.Errorf("attempt event must target a node attempt")
		}
	default:
		return fmt.Errorf("runtime event type %q is unsupported", event.EventType)
	}
	return nil
}

func (event RuntimeEvent) MarshalJSONMessage() ([]byte, error) {
	if err := event.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	if len(payload) > EnvelopeMaxBytes {
		return nil, fmt.Errorf("runtime event envelope exceeds %d bytes", EnvelopeMaxBytes)
	}
	return payload, nil
}

func ParseRuntimeEvent(payload []byte) (RuntimeEvent, error) {
	if len(payload) == 0 || len(payload) > EnvelopeMaxBytes {
		return RuntimeEvent{}, fmt.Errorf("runtime event envelope size must be in [1,%d]", EnvelopeMaxBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var event RuntimeEvent
	if err := decoder.Decode(&event); err != nil {
		return RuntimeEvent{}, fmt.Errorf("decode runtime event: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return RuntimeEvent{}, fmt.Errorf("runtime event contains trailing JSON")
	}
	if err := event.Validate(); err != nil {
		return RuntimeEvent{}, err
	}
	return event, nil
}

type MessagePublisher interface {
	PublishRuntimeEvent(context.Context, RuntimeEvent) error
}

// BatchMessagePublisher preserves one result per input message so a Relay can
// mark only Kafka-acknowledged Outbox rows after a partially successful send.
type BatchMessagePublisher interface {
	PublishRuntimeEvents(context.Context, []RuntimeEvent) []error
}

type ClaimedEvent struct {
	Event      RuntimeEvent
	ClaimToken string
}

type OutboxRepository interface {
	ClaimOutbox(context.Context, string, int, time.Duration) ([]ClaimedEvent, error)
	MarkOutboxPublished(context.Context, string, string) error
	ReleaseOutboxClaim(context.Context, string, string, time.Duration) error
}

type ClaimedIdentity struct {
	ID         string
	ClaimToken string
}

type BatchOutboxRepository interface {
	MarkOutboxPublishedBatch(context.Context, []ClaimedIdentity) error
	ReleaseOutboxClaimsBatch(context.Context, []ClaimedIdentity, time.Duration) error
}

type Relay struct {
	repository OutboxRepository
	publisher  MessagePublisher
	owner      string
	batch      int
	claimLease time.Duration
	retryDelay time.Duration
}

func NewRelay(repository OutboxRepository, publisher MessagePublisher, owner string, batch int, claimLease, retryDelay time.Duration) (Relay, error) {
	if repository == nil || publisher == nil || owner == "" || batch < 1 || claimLease <= 0 || retryDelay < 0 {
		return Relay{}, fmt.Errorf("outbox relay dependencies and positive limits are required")
	}
	return Relay{repository: repository, publisher: publisher, owner: owner, batch: batch, claimLease: claimLease, retryDelay: retryDelay}, nil
}

// RelayOnce is intentionally at-least-once. A crash after Publish and before
// MarkPublished republishes the event; Inbox makes the business effect once.
func (relay Relay) RelayOnce(ctx context.Context) (int, error) {
	claimed, err := relay.repository.ClaimOutbox(ctx, relay.owner, relay.batch, relay.claimLease)
	if err != nil {
		return 0, err
	}
	valid := make([]ClaimedEvent, 0, len(claimed))
	failed := make([]ClaimedIdentity, 0)
	var firstErr error
	for _, message := range claimed {
		if err := message.Event.Validate(); err != nil {
			failed = append(failed, ClaimedIdentity{ID: message.Event.EventID, ClaimToken: message.ClaimToken})
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		valid = append(valid, message)
	}
	outcomes := make([]error, len(valid))
	if publisher, ok := relay.publisher.(BatchMessagePublisher); ok && len(valid) > 0 {
		events := make([]RuntimeEvent, len(valid))
		for index := range valid {
			events[index] = valid[index].Event
		}
		outcomes = publisher.PublishRuntimeEvents(ctx, events)
		if len(outcomes) != len(valid) {
			return 0, fmt.Errorf("runtime batch publisher returned %d outcomes for %d messages", len(outcomes), len(valid))
		}
	} else {
		for index := range valid {
			outcomes[index] = relay.publisher.PublishRuntimeEvent(ctx, valid[index].Event)
		}
	}
	succeeded := make([]ClaimedIdentity, 0, len(valid))
	for index, outcome := range outcomes {
		identity := ClaimedIdentity{ID: valid[index].Event.EventID, ClaimToken: valid[index].ClaimToken}
		if outcome == nil {
			succeeded = append(succeeded, identity)
			continue
		}
		failed = append(failed, identity)
		if firstErr == nil {
			firstErr = outcome
		}
	}
	if err = relay.markPublished(ctx, succeeded); err != nil {
		return 0, err
	}
	if releaseErr := relay.releaseClaims(ctx, failed); releaseErr != nil && firstErr == nil {
		firstErr = releaseErr
	}
	return len(succeeded), firstErr
}

func (relay Relay) markPublished(ctx context.Context, values []ClaimedIdentity) error {
	if len(values) == 0 {
		return nil
	}
	if repository, ok := relay.repository.(BatchOutboxRepository); ok {
		return repository.MarkOutboxPublishedBatch(ctx, values)
	}
	for _, value := range values {
		if err := relay.repository.MarkOutboxPublished(ctx, value.ID, value.ClaimToken); err != nil {
			return err
		}
	}
	return nil
}

func (relay Relay) releaseClaims(ctx context.Context, values []ClaimedIdentity) error {
	if len(values) == 0 {
		return nil
	}
	if repository, ok := relay.repository.(BatchOutboxRepository); ok {
		return repository.ReleaseOutboxClaimsBatch(ctx, values, relay.retryDelay)
	}
	for _, value := range values {
		if err := relay.repository.ReleaseOutboxClaim(ctx, value.ID, value.ClaimToken, relay.retryDelay); err != nil {
			return err
		}
	}
	return nil
}
