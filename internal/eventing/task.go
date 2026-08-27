package eventing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/uu999/evalfrog/internal/scheduling"
)

const (
	TaskMessageVersion = 1
	EnvelopeMaxBytes   = 64 << 10
)

// TaskMessage is a lightweight execution wake-up. It deliberately excludes
// DSL, resolved inputs, outputs and credentials; workers load those through
// the authoritative Execution Context Gateway after claiming the Attempt.
type TaskMessage struct {
	MessageVersion  int                      `json:"message_version"`
	TaskID          string                   `json:"task_id"`
	ProjectID       string                   `json:"project_id"`
	RunID           string                   `json:"run_id"`
	NodeRunID       string                   `json:"node_run_id"`
	ExecutionNodeID string                   `json:"execution_node_id"`
	AttemptID       string                   `json:"attempt_id"`
	AttemptSequence uint32                   `json:"attempt_sequence"`
	ResourceClass   scheduling.ResourceClass `json:"resource_class"`
	OccurredAt      time.Time                `json:"occurred_at"`
	TraceID         string                   `json:"trace_id"`
}

func (message TaskMessage) Validate() error {
	if message.MessageVersion != TaskMessageVersion || message.TaskID == "" ||
		message.ProjectID == "" || message.RunID == "" || message.NodeRunID == "" ||
		message.ExecutionNodeID == "" || message.AttemptID == "" ||
		message.AttemptSequence == 0 || !message.ResourceClass.Valid() ||
		message.OccurredAt.IsZero() || message.TraceID == "" {
		return fmt.Errorf("task identity, v1 version, resource class, occurrence and trace are required")
	}
	return nil
}

func (message TaskMessage) MarshalJSONMessage() ([]byte, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}
	if len(payload) > EnvelopeMaxBytes {
		return nil, fmt.Errorf("task envelope exceeds %d bytes", EnvelopeMaxBytes)
	}
	return payload, nil
}

func ParseTaskMessage(payload []byte) (TaskMessage, error) {
	if len(payload) == 0 || len(payload) > EnvelopeMaxBytes {
		return TaskMessage{}, fmt.Errorf("task envelope size must be in [1,%d]", EnvelopeMaxBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var message TaskMessage
	if err := decoder.Decode(&message); err != nil {
		return TaskMessage{}, fmt.Errorf("decode task message: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return TaskMessage{}, fmt.Errorf("task message contains trailing JSON")
	}
	if err := message.Validate(); err != nil {
		return TaskMessage{}, err
	}
	return message, nil
}

// ParseTaskIdentity is intentionally looser than ParseTaskMessage. It exists
// only so a well-formed but contract-incompatible delivery can still be
// claimed and settled as a platform failure instead of leaving its Attempt
// queued forever. It never makes the payload executable.
func ParseTaskIdentity(payload []byte) (TaskMessage, error) {
	if len(payload) == 0 || len(payload) > EnvelopeMaxBytes {
		return TaskMessage{}, fmt.Errorf("task identity envelope size is invalid")
	}
	var value TaskMessage
	if err := json.Unmarshal(payload, &value); err != nil {
		return TaskMessage{}, err
	}
	if value.TaskID == "" || value.ProjectID == "" || value.RunID == "" || value.NodeRunID == "" || value.ExecutionNodeID == "" || value.AttemptID == "" || value.AttemptSequence == 0 || !value.ResourceClass.Valid() || value.TraceID == "" {
		return TaskMessage{}, fmt.Errorf("recognizable task identity is incomplete")
	}
	return value, nil
}

type TaskPublisher interface {
	PublishTask(context.Context, TaskMessage) error
}

type BatchTaskPublisher interface {
	PublishTasks(context.Context, []TaskMessage) []error
}

type ClaimedTask struct {
	Message    TaskMessage
	ClaimToken string
}

type TaskOutboxRepository interface {
	ClaimTaskOutbox(context.Context, string, int, time.Duration) ([]ClaimedTask, error)
	MarkTaskOutboxPublished(context.Context, string, string) error
	ReleaseTaskOutboxClaim(context.Context, string, string, time.Duration) error
}

type BatchTaskOutboxRepository interface {
	MarkTaskOutboxPublishedBatch(context.Context, []ClaimedIdentity) error
	ReleaseTaskOutboxClaimsBatch(context.Context, []ClaimedIdentity, time.Duration) error
}

type TaskRelay struct {
	repository TaskOutboxRepository
	publisher  TaskPublisher
	owner      string
	batch      int
	claimLease time.Duration
	retryDelay time.Duration
}

func NewTaskRelay(repository TaskOutboxRepository, publisher TaskPublisher, owner string, batch int, claimLease, retryDelay time.Duration) (TaskRelay, error) {
	if repository == nil || publisher == nil || owner == "" || batch < 1 || claimLease <= 0 || retryDelay < 0 {
		return TaskRelay{}, fmt.Errorf("task outbox relay dependencies and positive limits are required")
	}
	return TaskRelay{repository: repository, publisher: publisher, owner: owner, batch: batch, claimLease: claimLease, retryDelay: retryDelay}, nil
}

func (relay TaskRelay) RelayOnce(ctx context.Context) (int, error) {
	claimed, err := relay.repository.ClaimTaskOutbox(ctx, relay.owner, relay.batch, relay.claimLease)
	if err != nil {
		return 0, err
	}
	valid := make([]ClaimedTask, 0, len(claimed))
	failed := make([]ClaimedIdentity, 0)
	var firstErr error
	for _, task := range claimed {
		if err := task.Message.Validate(); err != nil {
			failed = append(failed, ClaimedIdentity{ID: task.Message.TaskID, ClaimToken: task.ClaimToken})
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		valid = append(valid, task)
	}
	outcomes := make([]error, len(valid))
	if publisher, ok := relay.publisher.(BatchTaskPublisher); ok && len(valid) > 0 {
		messages := make([]TaskMessage, len(valid))
		for index := range valid {
			messages[index] = valid[index].Message
		}
		outcomes = publisher.PublishTasks(ctx, messages)
		if len(outcomes) != len(valid) {
			return 0, fmt.Errorf("task batch publisher returned %d outcomes for %d messages", len(outcomes), len(valid))
		}
	} else {
		for index := range valid {
			outcomes[index] = relay.publisher.PublishTask(ctx, valid[index].Message)
		}
	}
	succeeded := make([]ClaimedIdentity, 0, len(valid))
	for index, outcome := range outcomes {
		identity := ClaimedIdentity{ID: valid[index].Message.TaskID, ClaimToken: valid[index].ClaimToken}
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

func (relay TaskRelay) markPublished(ctx context.Context, values []ClaimedIdentity) error {
	if len(values) == 0 {
		return nil
	}
	if repository, ok := relay.repository.(BatchTaskOutboxRepository); ok {
		return repository.MarkTaskOutboxPublishedBatch(ctx, values)
	}
	for _, value := range values {
		if err := relay.repository.MarkTaskOutboxPublished(ctx, value.ID, value.ClaimToken); err != nil {
			return err
		}
	}
	return nil
}

func (relay TaskRelay) releaseClaims(ctx context.Context, values []ClaimedIdentity) error {
	if len(values) == 0 {
		return nil
	}
	if repository, ok := relay.repository.(BatchTaskOutboxRepository); ok {
		return repository.ReleaseTaskOutboxClaimsBatch(ctx, values, relay.retryDelay)
	}
	for _, value := range values {
		if err := relay.repository.ReleaseTaskOutboxClaim(ctx, value.ID, value.ClaimToken, relay.retryDelay); err != nil {
			return err
		}
	}
	return nil
}
