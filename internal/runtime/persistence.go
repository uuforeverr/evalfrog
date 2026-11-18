package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/platform/clock"
	"github.com/uu999/evalfrog/internal/platform/identity"
)

var (
	ErrRunNotFound             = errors.New("workflow run not found")
	ErrRunConflict             = errors.New("workflow run state conflict")
	ErrRunSourceInvalid        = errors.New("workflow run definition source is invalid")
	ErrRunWorkflowNotPublished = errors.New("workflow has no active published version")
	ErrRunIdempotencyReuse     = errors.New("workflow run idempotency key reused")
)

type CreatePendingRunRecord struct {
	RunID, EventID, ProjectID, WorkflowID, SnapshotID string
	DraftRevisionNumber                               int64
	Purpose                                           RunPurpose
	WorkflowInput                                     json.RawMessage
	DeadlineAt, CreatedAt                             time.Time
	PrincipalID, IdempotencyKey, RequestHash, TraceID string
}

type RunRepository interface {
	CreatePendingRun(context.Context, CreatePendingRunRecord) (WorkflowRunRecord, error)
}

type RunAccessControl interface {
	Require(context.Context, string, string, access.Permission) error
}

type RunCreator struct {
	repository RunRepository
	access     RunAccessControl
	ids        identity.Generator
	clock      clock.Clock
}

func NewRunCreator(repository RunRepository, accessControl RunAccessControl, ids identity.Generator, valueClock clock.Clock) (RunCreator, error) {
	if repository == nil || accessControl == nil || ids == nil || valueClock == nil {
		return RunCreator{}, fmt.Errorf("run creator dependencies are required")
	}
	return RunCreator{repository: repository, access: accessControl, ids: ids, clock: valueClock}, nil
}

func NewBuiltinRunCreator(repository RunRepository, accessControl RunAccessControl) RunCreator {
	creator, err := NewRunCreator(repository, accessControl, identity.UUIDv7Generator{}, clock.System{})
	if err != nil {
		panic(err)
	}
	return creator
}

type TestDraftRunCommand struct {
	ProjectID, PrincipalID, WorkflowID, SnapshotID string
	DraftRevisionNumber                            int64
	WorkflowInput                                  json.RawMessage
	DeadlineAt                                     time.Time
	IdempotencyKey, TraceID                        string
}

type ProductionRunCommand struct {
	ProjectID, PrincipalID, WorkflowID string
	WorkflowInput                      json.RawMessage
	DeadlineAt                         time.Time
	IdempotencyKey, TraceID            string
}

func (creator RunCreator) TestDraft(ctx context.Context, command TestDraftRunCommand) (WorkflowRunRecord, error) {
	if err := creator.access.Require(ctx, command.ProjectID, command.PrincipalID, access.PermissionWorkflowTest); err != nil {
		return WorkflowRunRecord{}, err
	}
	return creator.create(ctx, CreatePendingRunRecord{
		ProjectID: command.ProjectID, WorkflowID: command.WorkflowID, SnapshotID: command.SnapshotID,
		DraftRevisionNumber: command.DraftRevisionNumber,
		Purpose:             RunPurposeTest, WorkflowInput: command.WorkflowInput, DeadlineAt: command.DeadlineAt,
		PrincipalID: command.PrincipalID, IdempotencyKey: command.IdempotencyKey, TraceID: command.TraceID,
	})
}

func (creator RunCreator) CreateProduction(ctx context.Context, command ProductionRunCommand) (WorkflowRunRecord, error) {
	if err := creator.access.Require(ctx, command.ProjectID, command.PrincipalID, access.PermissionRunCreate); err != nil {
		return WorkflowRunRecord{}, err
	}
	return creator.create(ctx, CreatePendingRunRecord{
		ProjectID: command.ProjectID, WorkflowID: command.WorkflowID, Purpose: RunPurposeProduction,
		WorkflowInput: command.WorkflowInput, DeadlineAt: command.DeadlineAt,
		PrincipalID: command.PrincipalID, IdempotencyKey: command.IdempotencyKey, TraceID: command.TraceID,
	})
}

func (creator RunCreator) create(ctx context.Context, record CreatePendingRunRecord) (WorkflowRunRecord, error) {
	if record.ProjectID == "" || record.WorkflowID == "" || record.PrincipalID == "" || len(record.IdempotencyKey) < 8 || record.TraceID == "" {
		return WorkflowRunRecord{}, fmt.Errorf("%w: run identity, idempotency key and trace are required", ErrInvalidRun)
	}
	if record.Purpose == RunPurposeTest && (record.SnapshotID == "" || record.DraftRevisionNumber < 1) {
		return WorkflowRunRecord{}, fmt.Errorf("%w: draft test requires a snapshot", ErrInvalidRun)
	}
	if _, err := cloneJSONObject(record.WorkflowInput); err != nil {
		return WorkflowRunRecord{}, fmt.Errorf("%w: workflow input: %v", ErrInvalidRun, err)
	}
	// MySQL timestamptz has microsecond precision. Normalize before request
	// hashing so an idempotent replay using the returned deadline hashes equally.
	record.CreatedAt = creator.clock.Now().UTC().Truncate(time.Microsecond)
	record.DeadlineAt = record.DeadlineAt.UTC().Truncate(time.Microsecond)
	if record.DeadlineAt.IsZero() || !record.DeadlineAt.After(record.CreatedAt) {
		return WorkflowRunRecord{}, fmt.Errorf("%w: deadline must be after creation", ErrInvalidRun)
	}
	request := struct {
		WorkflowID string          `json:"workflow_id"`
		SnapshotID string          `json:"snapshot_id,omitempty"`
		Revision   int64           `json:"draft_revision,omitempty"`
		Purpose    RunPurpose      `json:"purpose"`
		Input      json.RawMessage `json:"input"`
		Deadline   time.Time       `json:"deadline_at"`
	}{record.WorkflowID, record.SnapshotID, record.DraftRevisionNumber, record.Purpose, record.WorkflowInput, record.DeadlineAt.UTC()}
	payload, err := json.Marshal(request)
	if err != nil {
		return WorkflowRunRecord{}, err
	}
	digest := sha256.Sum256(payload)
	record.RequestHash = hex.EncodeToString(digest[:])
	record.RunID, err = creator.ids.New()
	if err != nil {
		return WorkflowRunRecord{}, err
	}
	record.EventID, err = creator.ids.New()
	if err != nil {
		return WorkflowRunRecord{}, err
	}
	return creator.repository.CreatePendingRun(ctx, record)
}
