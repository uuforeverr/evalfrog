//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/catalog"
	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/projection"
	runtimepkg "github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/workflowapp"
)

func TestM10V2UpgradePublishAndRunUsesImmutableCatalog(t *testing.T) {
	harness := newM5Harness(t)
	reader := projection.NewBuiltinService(harness.store, harness.access)
	control := runtimepkg.NewBuiltinRunControl(harness.store, harness.access)
	application, err := workflowapp.New(harness.definitions, harness.creator, control, reader, catalog.BuiltinV2(), nilConnectionDirectory{})
	if err != nil {
		t.Fatal(err)
	}
	workflow, base, diagnostics, err := application.CreateWorkflow(harness.ctx, definition.CreateWorkflowCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, Name: "v2 upgrade",
		IRJSON: minimalIR("Start", 0), IdempotencyKey: "m10-v2-create-" + newID(t),
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	if base.CatalogRevision != "catalog-v1" {
		t.Fatalf("base catalog: %s", base.CatalogRevision)
	}
	upgradedIR := json.RawMessage(`{"ir_version":"2","nodes":[{"id":"start","type":"start","title":"Start","inputs":[],"outputs":[{"name":"workflow_input","data_type":"object"}]},{"id":"end","type":"end","title":"End","inputs":[{"name":"workflow_output","data_type":"object","source":"ref","ref_node":"start","ref_output":"workflow_input"}],"outputs":[{"name":"workflow_output","data_type":"object"}]}],"edges":[{"id":"start_to_end","source":"start","target":"end"}],"layout":{"start":{"x":0,"y":0},"end":{"x":100,"y":0}}}`)
	command := definition.SaveDraftCommand{ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID, ExpectedRevision: 1, IRJSON: upgradedIR, IdempotencyKey: "m10-v2-upgrade-" + newID(t)}
	revision, diagnostics, err := application.SaveDraft(harness.ctx, command)
	assertNoDefinitionFailure(t, diagnostics, err)
	if revision.RevisionNumber != 2 || revision.CatalogRevision != "catalog-v2" {
		t.Fatalf("upgraded revision: %+v", revision)
	}
	replayed, diagnostics, err := application.SaveDraft(harness.ctx, command)
	assertNoDefinitionFailure(t, diagnostics, err)
	if replayed.ID != revision.ID {
		t.Fatalf("upgrade retry created another revision: %s != %s", replayed.ID, revision.ID)
	}
	old, err := harness.store.GetDraftRevision(harness.ctx, harness.projectID, workflow.ID, 1)
	if err != nil || old.CatalogRevision != "catalog-v1" {
		t.Fatalf("old immutable revision: %+v err=%v", old, err)
	}
	version, _, diagnostics, err := application.Publish(harness.ctx, definition.PublishCommand{ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID, ExpectedRevision: 2, ChangeLog: "v2", IdempotencyKey: "m10-v2-publish-" + newID(t)})
	assertNoDefinitionFailure(t, diagnostics, err)
	if version.VersionNumber != 1 {
		t.Fatalf("published version: %+v", version)
	}
	run, err := application.CreateRun(harness.ctx, workflowapp.CreateRunCommand{ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID, WorkflowInput: json.RawMessage(`{"workflow_input":{"amount":7}}`), DeadlineAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "m10-v2-run-" + newID(t), TraceID: "trace-m10-v2-run"})
	if err != nil {
		t.Fatal(err)
	}
	harness.initializeRun(t, run)
	view, err := application.GetRun(harness.ctx, harness.projectID, harness.principalID, run.ID)
	if err != nil || view.State != runtimepkg.RunSucceeded || string(view.Output) != `{"workflow_output":{"amount":7}}` {
		t.Fatalf("v2 run view=%+v err=%v", view, err)
	}
	badRun, err := application.CreateRun(harness.ctx, workflowapp.CreateRunCommand{ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID, WorkflowInput: json.RawMessage(`{"wrong":7}`), DeadlineAt: time.Now().UTC().Add(time.Hour), IdempotencyKey: "m10-v2-bad-run-" + newID(t), TraceID: "trace-m10-v2-bad"})
	if err != nil {
		t.Fatal(err)
	}
	harness.initializeRun(t, badRun)
	failed, err := application.GetRun(harness.ctx, harness.projectID, harness.principalID, badRun.ID)
	if err != nil || failed.State != runtimepkg.RunFailed || failed.Failure == nil || failed.Failure.Code != "RUNTIME_INPUT_CONTRACT_VIOLATION" {
		t.Fatalf("bad input view=%+v err=%v", failed, err)
	}
}
