package attempt

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/platform/clock"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/scheduling"
)

type fixedIDs struct{ value string }

func (ids fixedIDs) New() (string, error) { return ids.value, nil }

type failingIDs struct{}

func (failingIDs) New() (string, error) { return "", errors.New("id generation failed") }

type fakeRepository struct {
	claim      ClaimRecord
	heartbeat  HeartbeatRecord
	complete   CompleteRecord
	lost       MarkLostRecord
	completeOK bool
}

func (repository *fakeRepository) Claim(_ context.Context, record ClaimRecord) (Lease, error) {
	repository.claim = record
	return Lease{Token: record.LeaseToken, Owner: record.WorkerID, FencingToken: 1, ExpiresAt: record.Now.Add(record.LeaseDuration)}, nil
}
func (repository *fakeRepository) Heartbeat(_ context.Context, record HeartbeatRecord) (Lease, error) {
	repository.heartbeat = record
	return Lease{Token: record.LeaseToken, FencingToken: record.FencingToken, ExpiresAt: record.Now.Add(record.ExtendBy)}, nil
}
func (repository *fakeRepository) Complete(_ context.Context, record CompleteRecord) (bool, error) {
	repository.complete = record
	return repository.completeOK, nil
}
func (repository *fakeRepository) MarkExpiredLost(_ context.Context, record MarkLostRecord) (bool, error) {
	repository.lost = record
	return true, nil
}

func TestCoordinatorValidatesAndCoordinatesLeaseOperations(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repository := &fakeRepository{completeOK: true}
	coordinator, err := NewCoordinator(repository, fixedIDs{"generated"}, clock.NewFake(now))
	if err != nil {
		t.Fatal(err)
	}
	claim := validClaim()
	claim.LeaseDuration = time.Minute
	lease, err := coordinator.Claim(context.Background(), claim)
	if err != nil || lease.Token != "generated" || repository.claim.Now != now {
		t.Fatalf("lease=%+v record=%+v err=%v", lease, repository.claim, err)
	}
	heartbeat := HeartbeatCommand{ProjectID: "p", RunID: "r", AttemptID: "a", AttemptSequence: 1, LeaseToken: lease.Token, FencingToken: 1, ExtendBy: time.Minute}
	if _, err = coordinator.Heartbeat(context.Background(), heartbeat); err != nil || repository.heartbeat.Now != now {
		t.Fatalf("heartbeat=%+v err=%v", repository.heartbeat, err)
	}
	complete := CompleteCommand{
		ProjectID: "p", RunID: "r", AttemptID: "a", AttemptSequence: 1,
		LeaseToken: lease.Token, FencingToken: 1, TraceID: "trace",
		Result: runtime.AttemptResult{State: runtime.AttemptSucceeded, Outputs: map[string]json.RawMessage{"result": json.RawMessage(`{}`)}},
	}
	if applied, completeErr := coordinator.Complete(context.Background(), complete); completeErr != nil || !applied || repository.complete.EventID != "generated" {
		t.Fatalf("applied=%v record=%+v err=%v", applied, repository.complete, completeErr)
	}
	if applied, lostErr := coordinator.MarkExpiredLost(context.Background(), MarkLostCommand{ProjectID: "p", RunID: "r", AttemptID: "a", AttemptSequence: 1, TraceID: "trace"}); lostErr != nil || !applied || repository.lost.EventID != "generated" {
		t.Fatalf("lost=%v record=%+v err=%v", applied, repository.lost, lostErr)
	}
}

func TestCoordinatorRejectsInvalidCoordinatesAndResults(t *testing.T) {
	coordinator, _ := NewCoordinator(&fakeRepository{}, fixedIDs{"id"}, clock.NewFake(time.Now()))
	if _, err := coordinator.Claim(context.Background(), ClaimCommand{}); err == nil {
		t.Fatal("invalid claim accepted")
	}
	if _, err := coordinator.Heartbeat(context.Background(), HeartbeatCommand{}); err == nil {
		t.Fatal("invalid heartbeat accepted")
	}
	if _, err := coordinator.Complete(context.Background(), CompleteCommand{}); err == nil {
		t.Fatal("invalid completion coordinate accepted")
	}
	base := CompleteCommand{ProjectID: "p", RunID: "r", AttemptID: "a", AttemptSequence: 1, LeaseToken: "l", FencingToken: 1, TraceID: "t"}
	for _, result := range []runtime.AttemptResult{
		{State: runtime.AttemptRunning},
		{State: runtime.AttemptLost, ErrorCode: "LEASE_LOST"},
		{State: runtime.AttemptFailed},
		{State: runtime.AttemptSucceeded, ErrorCode: "wrong", Outputs: map[string]json.RawMessage{}},
		{State: runtime.AttemptSucceeded, Outputs: map[string]json.RawMessage{"bad": json.RawMessage(`{`)}},
	} {
		base.Result = result
		if _, err := coordinator.Complete(context.Background(), base); err == nil {
			t.Fatalf("invalid result accepted: %+v", result)
		}
	}
	if _, err := coordinator.MarkExpiredLost(context.Background(), MarkLostCommand{}); err == nil {
		t.Fatal("invalid lost command accepted")
	}
	if _, err := NewCoordinator(nil, fixedIDs{"id"}, clock.NewFake(time.Now())); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("dependency error=%v", err)
	}
}

func TestCoordinatorNormalizesNilSuccessfulOutputsBeforePersistence(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repository := &fakeRepository{completeOK: true}
	coordinator, err := NewCoordinator(repository, fixedIDs{"event"}, clock.NewFake(now))
	if err != nil {
		t.Fatal(err)
	}
	command := CompleteCommand{
		ProjectID: "p", RunID: "r", AttemptID: "a", AttemptSequence: 1,
		LeaseToken: "lease", FencingToken: 1, TraceID: "trace",
		Result: runtime.AttemptResult{State: runtime.AttemptSucceeded},
	}
	applied, err := coordinator.Complete(context.Background(), command)
	if err != nil || !applied {
		t.Fatalf("applied=%v err=%v", applied, err)
	}
	if repository.complete.Result.Outputs == nil || len(repository.complete.Result.Outputs) != 0 {
		t.Fatalf("persisted outputs=%v", repository.complete.Result.Outputs)
	}
}

func TestCoordinatorPropagatesIdentityGenerationFailuresAndBuiltinWiring(t *testing.T) {
	repository := &fakeRepository{}
	if value := NewBuiltinCoordinator(repository); value.repository == nil {
		t.Fatal("builtin coordinator was not wired")
	}
	coordinator, _ := NewCoordinator(repository, failingIDs{}, clock.NewFake(time.Now()))
	if _, err := coordinator.Claim(context.Background(), validClaim()); err == nil {
		t.Fatal("claim identity failure was hidden")
	}
	complete := CompleteCommand{
		ProjectID: "p", RunID: "r", AttemptID: "a", AttemptSequence: 1,
		LeaseToken: "l", FencingToken: 1, TraceID: "t",
		Result: runtime.AttemptResult{State: runtime.AttemptFailed, ErrorCode: "FAILED"},
	}
	if _, err := coordinator.Complete(context.Background(), complete); err == nil {
		t.Fatal("completion identity failure was hidden")
	}
	if _, err := coordinator.MarkExpiredLost(context.Background(), MarkLostCommand{ProjectID: "p", RunID: "r", AttemptID: "a", AttemptSequence: 1, TraceID: "t"}); err == nil {
		t.Fatal("lost identity failure was hidden")
	}
}

func validClaim() ClaimCommand {
	return ClaimCommand{ProjectID: "p", RunID: "r", AttemptID: "a", AttemptSequence: 1,
		WorkerID: "w", ExecutorBuild: "b", ResourceClass: scheduling.ResourceSandbox,
		Capabilities: []dsl.Coordinate{{Type: "task.python", Version: 1}}, LeaseDuration: time.Second}
}
