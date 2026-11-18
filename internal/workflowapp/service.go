// Package workflowapp is the application-command boundary shared by External
// HTTP, Agent CLI and Human Web. It composes Definition and Runtime modules
// without allowing either client to bypass server validation or snapshot
// binding.
package workflowapp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/uu999/evalfrog/internal/catalog"
	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/projection"
	"github.com/uu999/evalfrog/internal/recovery"
	"github.com/uu999/evalfrog/internal/resources"
	"github.com/uu999/evalfrog/internal/runtime"
)

type DefinitionService interface {
	CreateWorkflow(context.Context, definition.CreateWorkflowCommand) (definition.Workflow, definition.DraftRevision, []ir.Diagnostic, error)
	GetWorkflow(context.Context, string, string, string) (definition.Workflow, error)
	GetDraft(context.Context, string, string, string) (definition.Draft, error)
	SaveDraft(context.Context, definition.SaveDraftCommand) (definition.DraftRevision, []ir.Diagnostic, error)
	ValidateDraft(context.Context, string, string, string, int64) ([]ir.Diagnostic, error)
	CompileDraftTestSnapshot(context.Context, string, string, string, int64) (definition.ExecutionSnapshot, []ir.Diagnostic, error)
	Publish(context.Context, definition.PublishCommand) (definition.PublishedVersion, definition.ExecutionSnapshot, []ir.Diagnostic, error)
	Rollback(context.Context, string, string, string, int64) (definition.PublishedVersion, error)
	CopyPublishedVersion(context.Context, definition.CopyCommand) (definition.Workflow, definition.DraftRevision, error)
}

type RunCreator interface {
	TestDraft(context.Context, runtime.TestDraftRunCommand) (runtime.WorkflowRunRecord, error)
	CreateProduction(context.Context, runtime.ProductionRunCommand) (runtime.WorkflowRunRecord, error)
}

type RunController interface {
	Cancel(context.Context, string, string, string, string) (runtime.WorkflowRunRecord, bool, error)
}

type RunReader interface {
	GetRun(context.Context, string, string, string) (projection.RunView, error)
	GetRunVersion(context.Context, string, string, string) (projection.RunVersion, error)
	GetDiagnostics(context.Context, string, string, string) (projection.DiagnosticView, error)
}

type RunReplayer interface {
	Replay(context.Context, recovery.Wakeup, string, string) (bool, error)
}

type NodeDirectory interface {
	Descriptions() []catalog.NodeDescription
}

type ConnectionDirectory interface {
	List(context.Context, string, string) ([]resources.ConnectionSummary, error)
}

type Service struct {
	definitions DefinitionService
	runs        RunCreator
	control     RunController
	reader      RunReader
	replayer    RunReplayer
	catalog     NodeDirectory
	connections ConnectionDirectory
}

func New(definitions DefinitionService, runs RunCreator, control RunController, reader RunReader, catalog NodeDirectory, connections ConnectionDirectory, replayers ...RunReplayer) (Service, error) {
	var replayer RunReplayer
	if len(replayers) > 0 {
		replayer = replayers[0]
	}
	if definitions == nil || runs == nil || control == nil || reader == nil || catalog == nil || connections == nil {
		return Service{}, fmt.Errorf("workflow application dependencies are required")
	}
	return Service{definitions: definitions, runs: runs, control: control, reader: reader, replayer: replayer, catalog: catalog, connections: connections}, nil
}

func (service Service) CreateWorkflow(ctx context.Context, command definition.CreateWorkflowCommand) (definition.Workflow, definition.DraftRevision, []ir.Diagnostic, error) {
	return service.definitions.CreateWorkflow(ctx, command)
}
func (service Service) GetWorkflow(ctx context.Context, projectID, principalID, workflowID string) (definition.Workflow, error) {
	return service.definitions.GetWorkflow(ctx, projectID, principalID, workflowID)
}
func (service Service) GetDraft(ctx context.Context, projectID, principalID, workflowID string) (definition.Draft, error) {
	return service.definitions.GetDraft(ctx, projectID, principalID, workflowID)
}
func (service Service) SaveDraft(ctx context.Context, command definition.SaveDraftCommand) (definition.DraftRevision, []ir.Diagnostic, error) {
	return service.definitions.SaveDraft(ctx, command)
}
func (service Service) ValidateDraft(ctx context.Context, projectID, principalID, workflowID string, revision int64) ([]ir.Diagnostic, error) {
	return service.definitions.ValidateDraft(ctx, projectID, principalID, workflowID, revision)
}
func (service Service) Publish(ctx context.Context, command definition.PublishCommand) (definition.PublishedVersion, definition.ExecutionSnapshot, []ir.Diagnostic, error) {
	return service.definitions.Publish(ctx, command)
}
func (service Service) Rollback(ctx context.Context, projectID, principalID, workflowID string, version int64) (definition.PublishedVersion, error) {
	return service.definitions.Rollback(ctx, projectID, principalID, workflowID, version)
}
func (service Service) CopyPublishedVersion(ctx context.Context, command definition.CopyCommand) (definition.Workflow, definition.DraftRevision, error) {
	return service.definitions.CopyPublishedVersion(ctx, command)
}

type TestDraftCommand struct {
	ProjectID, PrincipalID, WorkflowID string
	Revision                           int64
	WorkflowInput                      json.RawMessage
	DeadlineAt                         time.Time
	IdempotencyKey, TraceID            string
}

func (service Service) TestDraft(ctx context.Context, command TestDraftCommand) (runtime.WorkflowRunRecord, []ir.Diagnostic, error) {
	snapshot, diagnostics, err := service.definitions.CompileDraftTestSnapshot(ctx, command.ProjectID, command.PrincipalID, command.WorkflowID, command.Revision)
	if err != nil || ir.HasErrors(diagnostics) {
		return runtime.WorkflowRunRecord{}, diagnostics, err
	}
	run, err := service.runs.TestDraft(ctx, runtime.TestDraftRunCommand{
		ProjectID: command.ProjectID, PrincipalID: command.PrincipalID, WorkflowID: command.WorkflowID,
		SnapshotID: snapshot.ID, DraftRevisionNumber: command.Revision, WorkflowInput: command.WorkflowInput,
		DeadlineAt: command.DeadlineAt, IdempotencyKey: command.IdempotencyKey, TraceID: command.TraceID,
	})
	return run, nil, err
}

type CreateRunCommand struct {
	ProjectID, PrincipalID, WorkflowID string
	WorkflowInput                      json.RawMessage
	DeadlineAt                         time.Time
	IdempotencyKey, TraceID            string
}

func (service Service) CreateRun(ctx context.Context, command CreateRunCommand) (runtime.WorkflowRunRecord, error) {
	return service.runs.CreateProduction(ctx, runtime.ProductionRunCommand{
		ProjectID: command.ProjectID, PrincipalID: command.PrincipalID, WorkflowID: command.WorkflowID,
		WorkflowInput: command.WorkflowInput, DeadlineAt: command.DeadlineAt,
		IdempotencyKey: command.IdempotencyKey, TraceID: command.TraceID,
	})
}

func (service Service) CancelRun(ctx context.Context, projectID, principalID, runID, traceID string) (runtime.WorkflowRunRecord, bool, error) {
	return service.control.Cancel(ctx, projectID, principalID, runID, traceID)
}
func (service Service) GetRun(ctx context.Context, projectID, principalID, runID string) (projection.RunView, error) {
	return service.reader.GetRun(ctx, projectID, principalID, runID)
}

func (service Service) GetRunVersion(ctx context.Context, projectID, principalID, runID string) (projection.RunVersion, error) {
	return service.reader.GetRunVersion(ctx, projectID, principalID, runID)
}
func (service Service) GetDiagnostics(ctx context.Context, projectID, principalID, runID string) (projection.DiagnosticView, error) {
	return service.reader.GetDiagnostics(ctx, projectID, principalID, runID)
}
func (service Service) ReplayRun(ctx context.Context, projectID, principalID, runID, eventType, aggregateID, traceID string) (bool, error) {
	if service.replayer == nil {
		return false, fmt.Errorf("manual replay is unavailable")
	}
	return service.replayer.Replay(ctx, recovery.Wakeup{ProjectID: projectID, RunID: runID, AggregateID: aggregateID, EventType: eventing.RuntimeEventType(eventType)}, traceID, principalID)
}
func (service Service) NodeTypes() []catalog.NodeDescription { return service.catalog.Descriptions() }

func (service Service) NodeTypesForRevision(revision string) ([]catalog.NodeDescription, bool) {
	if revision == "" {
		return service.NodeTypes(), true
	}
	if directory, ok := service.catalog.(interface {
		DescriptionsForRevision(string) ([]catalog.NodeDescription, bool)
	}); ok {
		return directory.DescriptionsForRevision(revision)
	}
	if current, ok := service.catalog.(interface{ Revision() catalog.RevisionID }); ok && string(current.Revision()) == revision {
		return service.NodeTypes(), true
	}
	return nil, false
}
func (service Service) Connections(ctx context.Context, projectID, principalID string) ([]resources.ConnectionSummary, error) {
	return service.connections.List(ctx, projectID, principalID)
}
