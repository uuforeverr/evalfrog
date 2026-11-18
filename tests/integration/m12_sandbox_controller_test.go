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
	"github.com/uu999/evalfrog/internal/adapters/mysql"
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
	if dsn := os.Getenv("EVALFROG_M12_MYSQL_DSN"); dsn != "" {
		configuration.MySQL.DSN = dsn
		configuration.MySQL.Database = mysqlDatabaseFromDSN(t, dsn)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client, err := mysql.Open(ctx, configuration.MySQL)
	if err != nil {
		t.Fatal(err)
	}
	store := mysql.NewStore(client.Pool())
	projectID, principalID, executionID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	token := "m12-compose-token-" + uuid.NewString()
	harness := &m3Harness{ctx: ctx, client: client, store: store, projectID: projectID, principalID: principalID, executionID: executionID, token: token}
	harness.access = access.NewService(store)
	harness.definitions = definition.NewBuiltinService(store, harness.access, resources.NewResolver(store, harness.access))
	harness.seedProject(t, projectID, principalID, executionID, token, allPermissions())
	t.Cleanup(func() {
		// Definition revisions and versions are immutable by database trigger.
		// This opt-in deployed-stack test uses unique IDs and deliberately leaves
		// its diagnostic fixture in the local database for post-failure analysis.
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
	err = client.Pool().QueryRow(ctx, `SELECT state, workflow_output_json FROM workflow_runs WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2)`, projectID, run.ID).Scan(&state, &output)
	var decoded map[string]bool
	if err == nil {
		err = json.Unmarshal(output, &decoded)
	}
	if err != nil || state != "succeeded" || len(decoded) != 1 || !decoded["ok"] {
		t.Fatalf("sandbox controller run state=%q output=%s decoded=%v err=%v", state, output, decoded, err)
	}
}

func terminalM12RunState(t *testing.T, client *mysql.Client, projectID, runID string) string {
	t.Helper()
	var state string
	err := client.Pool().QueryRow(context.Background(), `SELECT state FROM workflow_runs WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2)`, projectID, runID).Scan(&state)
	if err != nil {
		t.Fatalf("load deployed sandbox run state: %v", err)
	}
	if state == "succeeded" || state == "failed" || state == "canceled" || state == "timed_out" {
		if state != "succeeded" {
			var failure []byte
			_ = client.Pool().QueryRow(context.Background(), `SELECT termination_intent_json FROM workflow_runs WHERE project_id=UUID_TO_BIN($1) AND run_id=UUID_TO_BIN($2)`, projectID, runID).Scan(&failure)
			t.Fatalf("deployed sandbox run reached %s: %s", state, failure)
		}
		return state
	}
	return ""
}
