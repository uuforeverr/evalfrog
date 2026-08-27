package engine

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/runtime"
)

// Harness is deterministic test infrastructure. It does not model persistence,
// Kafka or Worker internals; it only feeds domain facts into Engine operations.
type Harness struct {
	Engine     *Engine
	now        time.Time
	attemptSeq uint64
}

func NewHarness(snapshot Snapshot, command runtime.CreateRunCommand) (*Harness, error) {
	instance, err := NewBuiltinV1(snapshot, command)
	if err != nil {
		return nil, err
	}
	return &Harness{Engine: instance, now: command.CreatedAt}, nil
}

func (harness *Harness) Advance(duration time.Duration) { harness.now = harness.now.Add(duration) }
func (harness *Harness) Now() time.Time                 { return harness.now }

func (harness *Harness) StartReady() ([]string, error) {
	ids := harness.Engine.ReadyNodeIDs()
	attempts := make([]string, 0, len(ids))
	for _, id := range ids {
		harness.attemptSeq++
		attemptID := fmt.Sprintf("attempt-%06d", harness.attemptSeq)
		if _, err := harness.Engine.QueueNode(id, attemptID); err != nil {
			return nil, err
		}
		if err := harness.Engine.StartAttempt(attemptID); err != nil {
			return nil, err
		}
		attempts = append(attempts, attemptID)
	}
	return attempts, nil
}

func (harness *Harness) Succeed(attemptID string, outputs map[string]json.RawMessage) error {
	return harness.Complete(attemptID, runtime.AttemptResult{State: runtime.AttemptSucceeded, Outputs: outputs})
}

func (harness *Harness) Fail(attemptID, code string) error {
	return harness.Complete(attemptID, runtime.AttemptResult{State: runtime.AttemptFailed, ErrorCode: code, Message: code})
}

func (harness *Harness) Complete(attemptID string, result runtime.AttemptResult) error {
	_, err := harness.Engine.RecordAttemptResult(attemptID, result)
	if err != nil {
		return err
	}
	return harness.Engine.HandleAttemptCompleted(attemptID, harness.now)
}

func (harness *Harness) ReadyIDs() []dsl.NodeID { return harness.Engine.ReadyNodeIDs() }
