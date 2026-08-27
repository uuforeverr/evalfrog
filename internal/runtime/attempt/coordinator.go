// Package attempt owns Claim, Lease, Fencing, Heartbeat, and Completion coordination.
package attempt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/platform/clock"
	"github.com/uu999/evalfrog/internal/platform/identity"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/scheduling"
)

var (
	ErrNotFound           = errors.New("attempt not found")
	ErrNotCurrent         = errors.New("attempt is not current")
	ErrLeaseMismatch      = errors.New("attempt lease or fencing token does not match")
	ErrStateConflict      = errors.New("attempt state conflict")
	ErrCapabilityMismatch = errors.New("worker capability does not match attempt")
)

type Lease struct {
	Token           string
	Owner           string
	FencingToken    uint64
	ExpiresAt       time.Time
	CancelRequested bool
}

type ClaimCommand struct {
	ProjectID, RunID, AttemptID string
	AttemptSequence             uint32
	WorkerID, ExecutorBuild     string
	ResourceClass               scheduling.ResourceClass
	Capabilities                []dsl.Coordinate
	LeaseDuration               time.Duration
}

type HeartbeatCommand struct {
	ProjectID, RunID, AttemptID, LeaseToken string
	AttemptSequence                         uint32
	FencingToken                            uint64
	ExtendBy                                time.Duration
}

type CompleteCommand struct {
	ProjectID, RunID, AttemptID, LeaseToken string
	AttemptSequence                         uint32
	FencingToken                            uint64
	Result                                  runtime.AttemptResult
	TraceID                                 string
}

type ClaimRecord struct {
	ClaimCommand
	LeaseToken string
	Now        time.Time
}

type HeartbeatRecord struct {
	HeartbeatCommand
	Now time.Time
}

type CompleteRecord struct {
	CompleteCommand
	EventID string
	Now     time.Time
}

type MarkLostCommand struct {
	ProjectID, RunID, AttemptID string
	AttemptSequence             uint32
	TraceID                     string
}

type MarkLostRecord struct {
	MarkLostCommand
	EventID string
	Now     time.Time
}

type Repository interface {
	Claim(context.Context, ClaimRecord) (Lease, error)
	Heartbeat(context.Context, HeartbeatRecord) (Lease, error)
	Complete(context.Context, CompleteRecord) (bool, error)
	MarkExpiredLost(context.Context, MarkLostRecord) (bool, error)
}

func (coordinator Coordinator) MarkExpiredLost(ctx context.Context, command MarkLostCommand) (bool, error) {
	if command.ProjectID == "" || command.RunID == "" || command.AttemptID == "" || command.AttemptSequence == 0 || command.TraceID == "" {
		return false, fmt.Errorf("lost attempt coordinate and trace are required")
	}
	eventID, err := coordinator.ids.New()
	if err != nil {
		return false, err
	}
	return coordinator.repository.MarkExpiredLost(ctx, MarkLostRecord{MarkLostCommand: command, EventID: eventID, Now: coordinator.clock.Now().UTC()})
}

type Coordinator struct {
	repository Repository
	ids        identity.Generator
	clock      clock.Clock
}

func NewCoordinator(repository Repository, ids identity.Generator, valueClock clock.Clock) (Coordinator, error) {
	if repository == nil || ids == nil || valueClock == nil {
		return Coordinator{}, fmt.Errorf("attempt coordinator dependencies are required")
	}
	return Coordinator{repository: repository, ids: ids, clock: valueClock}, nil
}

func NewBuiltinCoordinator(repository Repository) Coordinator {
	value, err := NewCoordinator(repository, identity.UUIDv7Generator{}, clock.System{})
	if err != nil {
		panic(err)
	}
	return value
}

func (coordinator Coordinator) Claim(ctx context.Context, command ClaimCommand) (Lease, error) {
	if command.ProjectID == "" || command.RunID == "" || command.AttemptID == "" || command.AttemptSequence == 0 || command.WorkerID == "" || command.ExecutorBuild == "" || !command.ResourceClass.Valid() || len(command.Capabilities) == 0 || command.LeaseDuration <= 0 {
		return Lease{}, fmt.Errorf("claim identity, worker, build and positive lease are required")
	}
	for _, capability := range command.Capabilities {
		if capability.Type == "" || capability.Version == 0 {
			return Lease{}, fmt.Errorf("claim capability coordinate is invalid")
		}
	}
	token, err := coordinator.ids.New()
	if err != nil {
		return Lease{}, err
	}
	return coordinator.repository.Claim(ctx, ClaimRecord{ClaimCommand: command, LeaseToken: token, Now: coordinator.clock.Now().UTC()})
}

func (coordinator Coordinator) Heartbeat(ctx context.Context, command HeartbeatCommand) (Lease, error) {
	if command.ProjectID == "" || command.RunID == "" || command.AttemptID == "" || command.AttemptSequence == 0 || command.LeaseToken == "" || command.FencingToken == 0 || command.ExtendBy <= 0 {
		return Lease{}, fmt.Errorf("heartbeat coordinate and positive extension are required")
	}
	return coordinator.repository.Heartbeat(ctx, HeartbeatRecord{HeartbeatCommand: command, Now: coordinator.clock.Now().UTC()})
}

func (coordinator Coordinator) Complete(ctx context.Context, command CompleteCommand) (bool, error) {
	if command.ProjectID == "" || command.RunID == "" || command.AttemptID == "" || command.AttemptSequence == 0 || command.LeaseToken == "" || command.FencingToken == 0 || command.TraceID == "" {
		return false, fmt.Errorf("completion coordinate and trace are required")
	}
	if !command.Result.State.Terminal() || command.Result.State == runtime.AttemptLost {
		return false, fmt.Errorf("worker completion requires a non-lost terminal result")
	}
	if command.Result.State == runtime.AttemptSucceeded {
		if command.Result.ErrorCode != "" {
			return false, fmt.Errorf("successful completion cannot contain an error")
		}
		if command.Result.Outputs == nil {
			command.Result.Outputs = map[string]json.RawMessage{}
		}
		for name, value := range command.Result.Outputs {
			if name == "" || !json.Valid(value) {
				return false, fmt.Errorf("successful output candidate is not valid JSON")
			}
		}
	} else if command.Result.ErrorCode == "" {
		return false, fmt.Errorf("unsuccessful completion requires an error code")
	}
	eventID, err := coordinator.ids.New()
	if err != nil {
		return false, err
	}
	return coordinator.repository.Complete(ctx, CompleteRecord{CompleteCommand: command, EventID: eventID, Now: coordinator.clock.Now().UTC()})
}
