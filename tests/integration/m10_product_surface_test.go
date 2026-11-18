//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/catalog"
	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/projection"
	"github.com/uu999/evalfrog/internal/resources"
	runtimepkg "github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/workflowapp"
)

func TestM10DraftTestPublishProductionCancelAndRunReadModel(t *testing.T) {
	harness := newM5Harness(t)
	reader := projection.NewBuiltinService(harness.store, harness.access)
	control := runtimepkg.NewBuiltinRunControl(harness.store, harness.access)
	application, err := workflowapp.New(harness.definitions, harness.creator, control, reader, catalog.BuiltinV1(), nilConnectionDirectory{})
	if err != nil {
		t.Fatal(err)
	}
	workflow, _, diagnostics, err := application.CreateWorkflow(harness.ctx, definition.CreateWorkflowCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, Name: "M10 product loop",
		IRJSON: minimalIR("M10", 0), IdempotencyKey: "m10-create-" + newID(t),
	})
	assertNoDefinitionFailure(t, diagnostics, err)

	// A Test Run compiles the selected Draft server-side and pins its immutable
	// snapshot. It does not need a published version.
	testRun, diagnostics, err := application.TestDraft(harness.ctx, workflowapp.TestDraftCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID, Revision: 1,
		WorkflowInput: json.RawMessage(`{}`), DeadlineAt: time.Now().UTC().Add(time.Hour),
		IdempotencyKey: "m10-test-run", TraceID: "trace-m10-test",
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	if testRun.Purpose != runtimepkg.RunPurposeTest || testRun.Definition.Source != runtimepkg.DefinitionDraftSnapshot {
		t.Fatalf("test run did not pin a Draft snapshot: %+v", testRun)
	}

	if _, err = application.CreateRun(harness.ctx, workflowapp.CreateRunCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID, WorkflowInput: json.RawMessage(`{}`),
		DeadlineAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "m10-before-publish", TraceID: "trace-m10-unpublished",
	}); !errors.Is(err, runtimepkg.ErrRunWorkflowNotPublished) {
		t.Fatalf("unpublished production run error=%v", err)
	}

	version, _, diagnostics, err := application.Publish(harness.ctx, definition.PublishCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID, ExpectedRevision: 1,
		ChangeLog: "M10 active", IdempotencyKey: "m10-publish-" + newID(t),
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	if version.VersionNumber != 1 {
		t.Fatalf("unexpected published version: %+v", version)
	}
	// Copy starts as an editable Draft. A minimal IR change and publish create
	// an independently immutable version rather than mutating the source.
	copied, copiedDraft, err := application.CopyPublishedVersion(harness.ctx, definition.CopyCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, SourceWorkflowID: workflow.ID,
		SourceVersionNumber: version.VersionNumber, Name: "M10 copied", IdempotencyKey: "m10-copy-" + newID(t),
	})
	if err != nil || copiedDraft.RevisionNumber != 1 {
		t.Fatalf("copy workflow=%+v draft=%+v err=%v", copied, copiedDraft, err)
	}
	updatedCopy, diagnostics, err := application.SaveDraft(harness.ctx, definition.SaveDraftCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: copied.ID,
		ExpectedRevision: copiedDraft.RevisionNumber, IRJSON: minimalIR("M10 copied and changed", 48),
		IdempotencyKey: "m10-copy-update-" + newID(t),
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	copiedVersion, _, diagnostics, err := application.Publish(harness.ctx, definition.PublishCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: copied.ID,
		ExpectedRevision: updatedCopy.RevisionNumber, ChangeLog: "M10 copied minimal change", IdempotencyKey: "m10-copy-publish-" + newID(t),
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	if copiedVersion.VersionNumber != 1 || copiedVersion.WorkflowID != copied.ID {
		t.Fatalf("copied version=%+v", copiedVersion)
	}
	production, err := application.CreateRun(harness.ctx, workflowapp.CreateRunCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID, WorkflowInput: json.RawMessage(`{}`),
		DeadlineAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "m10-production-run", TraceID: "trace-m10-production",
	})
	if err != nil || production.Definition.Source != runtimepkg.DefinitionPublishedVersion || production.Definition.PublishedVersionID != version.ID {
		t.Fatalf("production run=%+v err=%v", production, err)
	}

	// Cancellation only sets intent and writes the durable event. Engine owns
	// the subsequent state transition even when no graph was initialized yet.
	canceled, applied, err := application.CancelRun(harness.ctx, harness.projectID, harness.principalID, production.ID, "trace-m10-cancel")
	if err != nil || !applied || canceled.State != runtimepkg.RunPending || canceled.CancelRequestedAt.IsZero() {
		t.Fatalf("cancel command result=%+v applied=%v err=%v", canceled, applied, err)
	}
	cancelEvent := harness.event(t, eventing.RunCancelRequested, production.ID)
	if err = harness.consumer.Consume(harness.ctx, cancelEvent); err != nil {
		t.Fatal(err)
	}
	view, err := application.GetRun(harness.ctx, harness.projectID, harness.principalID, production.ID)
	if err != nil || view.State != runtimepkg.RunCanceled || !view.CancelRequested || view.FailureLocation == nil || view.FailureLocation.Details != nil {
		t.Fatalf("run view=%+v err=%v", view, err)
	}
}

func TestM10RunReadModelMapsPersistedRuntimeFailureToIRLocation(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m10-source-map-failure")
	harness.initializeRun(t, run)
	nodeID := readyNodeID(t, harness, run.ID)
	failure := runtimepkg.Failure{
		Code: "OUTPUT_CONTRACT", Phase: "node_execution", Retryable: false, RunID: run.ID,
		SnapshotID: run.Definition.SnapshotID, DefinitionHash: run.Definition.DefinitionHash,
		ExecutionNodeID: nodeID, DSLField: "outputs.result", Message: "output contract failed", Details: map[string]any{"actual": "string"},
	}
	mustExec(t, harness.ctx, harness.client.Pool(), `
		UPDATE node_runs SET state='failed', state_version=state_version+1, failure_json=$1
		WHERE project_id=UUID_TO_BIN($2) AND run_id=UUID_TO_BIN($3) AND execution_node_id=$4`, mustJSONBytes(t, failure), harness.projectID, run.ID, nodeID)
	reader := projection.NewBuiltinService(harness.store, harness.access)
	view, err := reader.GetRun(harness.ctx, harness.projectID, harness.principalID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Nodes) != 3 {
		t.Fatalf("nodes=%+v", view.Nodes)
	}
	var failed *projection.NodeView
	for index := range view.Nodes {
		if view.Nodes[index].ExecutionNodeID == nodeID {
			failed = &view.Nodes[index]
		}
	}
	if failed == nil || failed.Location == nil || failed.Location.LogicalNodeID != "transform" || failed.Location.IRField == "" || failed.Location.Details["actual"] != "string" {
		t.Fatalf("failure mapping=%+v location=%+v failure=%+v", failed, func() any {
			if failed == nil {
				return nil
			}
			return failed.Location
		}(), func() any {
			if failed == nil {
				return nil
			}
			return failed.Failure
		}())
	}
}

func TestM10UserAndServiceAccountUseSameRunReadPermission(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m10-principal-run-read")
	servicePrincipal := uuid.NewString()
	mustExec(t, harness.ctx, harness.client.Pool(), `INSERT INTO principals (principal_id, kind, display_name) VALUES ($1,'service_account','M10 Service Account')`, servicePrincipal)
	mustExec(t, harness.ctx, harness.client.Pool(), `INSERT INTO project_memberships (project_id, principal_id) VALUES ($1,$2)`, harness.projectID, servicePrincipal)
	mustExec(t, harness.ctx, harness.client.Pool(), `INSERT INTO project_membership_permissions (project_id, principal_id, permission) VALUES ($1,$2,$3)`, harness.projectID, servicePrincipal, access.PermissionRunRead)
	reader := projection.NewBuiltinService(harness.store, harness.access)
	for _, principal := range []string{harness.principalID, servicePrincipal} {
		if _, err := reader.GetRun(context.Background(), harness.projectID, principal, run.ID); err != nil {
			t.Fatalf("principal %s run read error=%v", principal, err)
		}
	}
}

type nilConnectionDirectory struct{}

func (nilConnectionDirectory) List(context.Context, string, string) ([]resources.ConnectionSummary, error) {
	return nil, nil
}

func mustJSONBytes(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
