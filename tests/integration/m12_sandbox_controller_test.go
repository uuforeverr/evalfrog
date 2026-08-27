//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/adapters/postgres"
	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/resources"
	runtimepkg "github.com/uu999/evalfrog/internal/runtime"
)

// This is deliberately opt-in because it needs the deployed local Compose
// stack. It proves the path that an in-process test cannot prove:
// Engine Task Outbox -> Kafka -> socket-free Sandbox Worker -> private Controller ->
// OCI container -> Worker API -> Engine. The CI integration job enables it.
func TestM12ComposeSandboxControllerCompletesProductionRun(t *testing.T) {
	if os.Getenv("EVALFROG_M12_COMPOSE") != "1" {
		t.Skip("requires the local Compose stack; set EVALFROG_M12_COMPOSE=1")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(config.LoadOptions{Directory: filepath.Join(root, "configs"), Profile: "local"})
	if err != nil {
		t.Fatal(err)
	}
	if dsn := os.Getenv("EVALFROG_M12_POSTGRES_DSN"); dsn != "" {
		configuration.Postgres.DSN = dsn
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, err := postgres.Open(ctx, configuration.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	store := postgres.NewStore(client.Pool())
	projectID, principalID, executionID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	token := "m12-compose-token-" + uuid.NewString()
	harness := &m3Harness{ctx: ctx, client: client, store: store, projectID: projectID, principalID: principalID, executionID: executionID, token: token}
	harness.access = access.NewService(store)
	harness.definitions = definition.NewBuiltinService(store, harness.access, resources.NewResolver(store, harness.access))
	harness.seedProject(t, projectID, principalID, executionID, token, allPermissions())
	t.Cleanup(func() {
		cleanupM12ComposeProject(t, client, projectID, principalID)
		client.Close()
	})

	workflow, _, diagnostics, err := harness.definitions.CreateWorkflow(ctx, definition.CreateWorkflowCommand{
		ProjectID: projectID, PrincipalID: principalID, Name: "M12 sandbox controller",
		IRJSON: singleCodeIR(), IdempotencyKey: "m12-compose-create-" + uuid.NewString(),
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	_, _, diagnostics, err = harness.definitions.Publish(ctx, definition.PublishCommand{
		ProjectID: projectID, PrincipalID: principalID, WorkflowID: workflow.ID, ExpectedRevision: 1,
		ChangeLog: "M12 deployed controller E2E", IdempotencyKey: "m12-compose-publish-" + uuid.NewString(),
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	creator := runtimepkg.NewBuiltinRunCreator(store, harness.access)
	run, err := creator.CreateProduction(ctx, runtimepkg.ProductionRunCommand{
		ProjectID: projectID, PrincipalID: principalID, WorkflowID: workflow.ID,
		WorkflowInput: json.RawMessage(`{"value":7}`), DeadlineAt: time.Now().UTC().Add(30 * time.Second),
		IdempotencyKey: "m12-compose-run-" + uuid.NewString(), TraceID: "m12-compose-sandbox-controller",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, func() bool { return terminalM12RunState(t, client, projectID, run.ID) != "" }, "sandbox production run")
	var state string
	var output json.RawMessage
	err = client.Pool().QueryRow(ctx, `SELECT state, workflow_output_json FROM workflow_runs WHERE project_id=$1 AND run_id=$2`, projectID, run.ID).Scan(&state, &output)
	var decoded map[string]bool
	if err == nil {
		err = json.Unmarshal(output, &decoded)
	}
	if err != nil || state != "succeeded" || len(decoded) != 1 || !decoded["ok"] {
		t.Fatalf("sandbox controller run state=%q output=%s decoded=%v err=%v", state, output, decoded, err)
	}
}

func terminalM12RunState(t *testing.T, client *postgres.Client, projectID, runID string) string {
	t.Helper()
	var state string
	err := client.Pool().QueryRow(context.Background(), `SELECT state FROM workflow_runs WHERE project_id=$1 AND run_id=$2`, projectID, runID).Scan(&state)
	if err != nil {
		t.Fatalf("load deployed sandbox run state: %v", err)
	}
	if state == "succeeded" || state == "failed" || state == "canceled" || state == "timed_out" {
		if state != "succeeded" {
			var failure []byte
			_ = client.Pool().QueryRow(context.Background(), `SELECT termination_intent_json FROM workflow_runs WHERE project_id=$1 AND run_id=$2`, projectID, runID).Scan(&failure)
			t.Fatalf("deployed sandbox run reached %s: %s", state, failure)
		}
		return state
	}
	return ""
}

func cleanupM12ComposeProject(t *testing.T, client *postgres.Client, projectID, principalID string) {
	t.Helper()
	statements := []string{
		`DELETE FROM attempt_resource_revisions WHERE project_id=$1`,
		`DELETE FROM runtime_idempotency WHERE project_id=$1`,
		`DELETE FROM runtime_recovery_wakeups WHERE project_id=$1`,
		`DELETE FROM runtime_audit_events WHERE project_id=$1`,
		`DELETE FROM workflow_definition_audits WHERE project_id=$1`,
		`DELETE FROM definition_idempotency WHERE project_id=$1`,
		`DELETE FROM inbox_events WHERE project_id=$1`,
		`DELETE FROM outbox_events WHERE project_id=$1`,
		`DELETE FROM node_output_values WHERE project_id=$1`,
		`DELETE FROM node_task_outbox WHERE project_id=$1`,
		`UPDATE node_runs SET current_attempt_id=NULL, effective_attempt_id=NULL WHERE project_id=$1`,
		`DELETE FROM node_attempts WHERE project_id=$1`,
		`DELETE FROM node_runs WHERE project_id=$1`,
		`DELETE FROM workflow_runs WHERE project_id=$1`,
		`DELETE FROM workflow_draft_test_snapshots WHERE project_id=$1`,
		`DELETE FROM workflow_definition_audits WHERE project_id=$1`,
		`UPDATE workflows SET active_version_id=NULL WHERE project_id=$1`,
		`ALTER TABLE workflow_versions DISABLE TRIGGER workflow_versions_immutable`,
		`ALTER TABLE workflow_draft_revisions DISABLE TRIGGER workflow_draft_revisions_immutable`,
		`DELETE FROM workflow_versions WHERE project_id=$1`,
		`DELETE FROM workflow_execution_snapshots WHERE project_id=$1`,
		`DELETE FROM workflow_drafts WHERE project_id=$1`,
		`DELETE FROM workflow_draft_revisions WHERE project_id=$1`,
		`ALTER TABLE workflow_draft_revisions ENABLE TRIGGER workflow_draft_revisions_immutable`,
		`ALTER TABLE workflow_versions ENABLE TRIGGER workflow_versions_immutable`,
		`DELETE FROM workflows WHERE project_id=$1`,
		`DELETE FROM project_membership_permissions WHERE project_id=$1`,
		`DELETE FROM project_memberships WHERE project_id=$1`,
		`DELETE FROM project_execution_identities WHERE project_id=$1`,
		`DELETE FROM projects WHERE project_id=$1`,
		`DELETE FROM principal_credentials WHERE principal_id=$1`,
		`DELETE FROM principals WHERE principal_id=$1`,
	}
	for _, statement := range statements {
		if statement == `ALTER TABLE workflow_versions DISABLE TRIGGER workflow_versions_immutable` ||
			statement == `ALTER TABLE workflow_versions ENABLE TRIGGER workflow_versions_immutable` ||
			statement == `ALTER TABLE workflow_draft_revisions DISABLE TRIGGER workflow_draft_revisions_immutable` ||
			statement == `ALTER TABLE workflow_draft_revisions ENABLE TRIGGER workflow_draft_revisions_immutable` {
			if _, err := client.Pool().Exec(context.Background(), statement); err != nil {
				t.Errorf("M12 Compose cleanup %q: %v", statement, err)
			}
			continue
		}
		argument := projectID
		if statement == `DELETE FROM principal_credentials WHERE principal_id=$1` || statement == `DELETE FROM principals WHERE principal_id=$1` {
			argument = principalID
		}
		if _, err := client.Pool().Exec(context.Background(), statement, argument); err != nil {
			t.Errorf("M12 Compose cleanup %q: %v", statement, err)
		}
	}
}
