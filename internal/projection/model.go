// Package projection owns client-oriented, read-only Runtime views. It never
// advances Engine state: MySQL Runtime tables remain the authority; Runtime
// views are rebuilt directly from those tables.
package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/sourcemap"
)

type FailureLocation struct {
	LogicalNodeID string         `json:"logical_node_id,omitempty"`
	LogicalEdgeID string         `json:"logical_edge_id,omitempty"`
	IRField       string         `json:"ir_field,omitempty"`
	Details       map[string]any `json:"details,omitempty"`
}

type NodeView struct {
	ExecutionNodeID string            `json:"execution_node_id"`
	State           runtime.NodeState `json:"state"`
	Activated       bool              `json:"activated"`
	AttemptID       string            `json:"attempt_id,omitempty"`
	Failure         *runtime.Failure  `json:"failure,omitempty"`
	Location        *FailureLocation  `json:"location,omitempty"`
}

// AttemptView is diagnostic metadata, not an execution command surface. It
// intentionally excludes lease tokens, workflow inputs/outputs and raw
// executor logs so the read model remains safe for ordinary Run readers.
type AttemptView struct {
	AttemptID       string               `json:"attempt_id"`
	ExecutionNodeID string               `json:"execution_node_id"`
	Sequence        uint32               `json:"sequence"`
	Kind            runtime.RetryKind    `json:"kind"`
	State           runtime.AttemptState `json:"state"`
	LeaseOwner      string               `json:"lease_owner,omitempty"`
	LeaseExpiresAt  *time.Time           `json:"lease_expires_at,omitempty"`
	ErrorCode       string               `json:"error_code,omitempty"`
	DSLField        string               `json:"dsl_field,omitempty"`
	CreatedAt       time.Time            `json:"created_at"`
	UpdatedAt       time.Time            `json:"updated_at"`
}

type AuditView struct {
	Action    string         `json:"action"`
	ActorType string         `json:"actor_type"`
	ActorID   string         `json:"actor_id"`
	TraceID   string         `json:"trace_id"`
	Details   map[string]any `json:"details"`
	CreatedAt time.Time      `json:"created_at"`
}

type DiagnosticView struct {
	Run      RunView       `json:"run"`
	Attempts []AttemptView `json:"attempts"`
	Audit    []AuditView   `json:"audit"`
}

type RunView struct {
	RunID           string             `json:"run_id"`
	ProjectID       string             `json:"project_id"`
	WorkflowID      string             `json:"workflow_id"`
	Purpose         runtime.RunPurpose `json:"purpose"`
	State           runtime.RunState   `json:"state"`
	StateVersion    uint64             `json:"state_version"`
	SnapshotID      string             `json:"snapshot_id"`
	DeadlineAt      time.Time          `json:"deadline_at"`
	CreatedAt       time.Time          `json:"created_at"`
	UpdatedAt       time.Time          `json:"updated_at"`
	CancelRequested bool               `json:"cancel_requested"`
	Output          json.RawMessage    `json:"output,omitempty"`
	Failure         *runtime.Failure   `json:"failure,omitempty"`
	FailureLocation *FailureLocation   `json:"failure_location,omitempty"`
	Nodes           []NodeView         `json:"nodes"`
}

type RunVersion struct {
	State        runtime.RunState `json:"state"`
	StateVersion uint64           `json:"state_version"`
}

type Repository interface {
	GetRunView(context.Context, string, string) (RunView, error)
	GetRunVersion(context.Context, string, string) (RunVersion, error)
	GetDiagnosticView(context.Context, string, string) (DiagnosticView, error)
}

type AccessControl interface {
	Require(context.Context, string, string, access.Permission) error
}

type Service struct {
	repository Repository
	access     AccessControl
}

func NewService(repository Repository, accessControl AccessControl) (Service, error) {
	if repository == nil || accessControl == nil {
		return Service{}, fmt.Errorf("run projection dependencies are required")
	}
	return Service{repository: repository, access: accessControl}, nil
}

func NewBuiltinService(repository Repository, accessControl AccessControl) Service {
	value, err := NewService(repository, accessControl)
	if err != nil {
		panic(err)
	}
	return value
}

func (service Service) GetRun(ctx context.Context, projectID, principalID, runID string) (RunView, error) {
	if err := service.access.Require(ctx, projectID, principalID, access.PermissionRunRead); err != nil {
		return RunView{}, err
	}
	if projectID == "" || runID == "" {
		return RunView{}, runtime.ErrRunNotFound
	}
	return service.repository.GetRunView(ctx, projectID, runID)
}

func (service Service) GetRunVersion(ctx context.Context, projectID, principalID, runID string) (RunVersion, error) {
	if err := service.access.Require(ctx, projectID, principalID, access.PermissionRunRead); err != nil {
		return RunVersion{}, err
	}
	if projectID == "" || runID == "" {
		return RunVersion{}, runtime.ErrRunNotFound
	}
	return service.repository.GetRunVersion(ctx, projectID, runID)
}

func (service Service) GetDiagnostics(ctx context.Context, projectID, principalID, runID string) (DiagnosticView, error) {
	if err := service.access.Require(ctx, projectID, principalID, access.PermissionRunRead); err != nil {
		return DiagnosticView{}, err
	}
	if projectID == "" || runID == "" {
		return DiagnosticView{}, runtime.ErrRunNotFound
	}
	return service.repository.GetDiagnosticView(ctx, projectID, runID)
}

func LocateFailure(document sourcemap.Document, failure *runtime.Failure) *FailureLocation {
	if failure == nil {
		return nil
	}
	location := document.Resolve(dsl.NodeID(failure.ExecutionNodeID), dsl.EdgeID(failure.ExecutionEdgeID), failure.DSLField)
	return &FailureLocation{
		LogicalNodeID: location.LogicalNodeID,
		LogicalEdgeID: location.LogicalEdgeID,
		IRField:       location.IRField,
		Details:       cloneDetails(failure.Details),
	}
}

func cloneDetails(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	copy := make(map[string]any, len(value))
	for key, item := range value {
		copy[key] = item
	}
	return copy
}
