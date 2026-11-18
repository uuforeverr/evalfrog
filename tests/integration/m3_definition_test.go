//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/adapters/mysql"
	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/platform/migrations"
	"github.com/uu999/evalfrog/internal/resources"
)

type m3Harness struct {
	ctx         context.Context
	cancel      context.CancelFunc
	client      *mysql.Client
	store       *mysql.Store
	access      access.Service
	definitions definition.Service
	projectID   string
	principalID string
	executionID string
	token       string
	database    string
}

func newM3Harness(t *testing.T) *m3Harness {
	return newM3HarnessWithTimeout(t, 60*time.Second)
}

func newM3HarnessWithTimeout(t *testing.T, timeout time.Duration) *m3Harness {
	t.Helper()
	if timeout <= 0 {
		t.Fatal("integration harness timeout must be positive")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	databaseName, dsn := isolatedMySQLDatabase(t)
	t.Setenv("EVALFROG_MYSQL_DSN", dsn)
	configuration, err := config.Load(config.LoadOptions{Directory: filepath.Join(root, "configs"), Profile: "local"})
	if err != nil {
		t.Fatal(err)
	}
	configuration.MySQL.Database = databaseName
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	client, err := mysql.Open(ctx, configuration.MySQL)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	runner := migrations.Runner{
		Database: client.Pool().Raw(), DatabaseName: configuration.MySQL.Database,
		Directory: filepath.Join(root, "migrations"), LockTimeout: 5 * time.Second,
	}
	if err := runner.Up(ctx); err != nil {
		client.Close()
		cancel()
		t.Fatal(err)
	}
	harness := &m3Harness{
		ctx: ctx, cancel: cancel, client: client, store: mysql.NewStore(client.Pool()),
		projectID: newID(t), principalID: newID(t), executionID: newID(t),
		token: "m3-integration-token-" + uuid.NewString(), database: configuration.MySQL.Database,
	}
	harness.access = access.NewService(harness.store)
	resolver := resources.NewResolver(harness.store, harness.access)
	harness.definitions = definition.NewBuiltinService(harness.store, harness.access, resolver)
	harness.seedProject(t, harness.projectID, harness.principalID, harness.executionID, harness.token, allPermissions())
	t.Cleanup(func() {
		harness.client.Close()
		harness.cancel()
	})
	return harness
}

func (harness *m3Harness) seedProject(t *testing.T, projectID, principalID, executionID, token string, permissions []access.Permission) {
	t.Helper()
	digest := sha256.Sum256([]byte(token))
	tx, err := harness.client.Pool().Begin(harness.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(harness.ctx)
	mustExec(t, harness.ctx, tx, `INSERT INTO principals (principal_id, kind, display_name) VALUES ($1,'user','M3 Test User')`, principalID)
	mustExec(t, harness.ctx, tx, `INSERT INTO principal_credentials (credential_id, principal_id, credential_hash) VALUES ($1,$2,$3)`, newID(t), principalID, digest[:])
	mustExec(t, harness.ctx, tx, `INSERT INTO projects (project_id, name) VALUES ($1,'M3 Test Project')`, projectID)
	mustExec(t, harness.ctx, tx, `INSERT INTO project_memberships (project_id, principal_id) VALUES ($1,$2)`, projectID, principalID)
	for _, permission := range permissions {
		mustExec(t, harness.ctx, tx, `INSERT INTO project_membership_permissions (project_id, principal_id, permission) VALUES ($1,$2,$3)`, projectID, principalID, permission)
	}
	mustExec(t, harness.ctx, tx, `INSERT INTO project_execution_identities (execution_identity_id, project_id, display_name) VALUES ($1,$2,'M3 Runtime')`, executionID, projectID)
	if err := tx.Commit(harness.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestM3DraftPublishRollbackCopyLifecycle(t *testing.T) {
	harness := newM3Harness(t)
	workflow, _, diagnostics, err := harness.definitions.CreateWorkflow(harness.ctx, definition.CreateWorkflowCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, Name: "Lifecycle",
		IRJSON: minimalIR("initial", 0), IdempotencyKey: "create-lifecycle",
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	base, err := harness.store.GetDraftRevision(harness.ctx, harness.projectID, workflow.ID, 1)
	if err != nil || !json.Valid(base.IRJSON) {
		t.Fatalf("base revision IR unavailable: err=%v ir=%q", err, base.IRJSON)
	}

	if _, err := harness.definitions.ResolveProductionDefinition(harness.ctx, harness.projectID, harness.principalID, workflow.ID); definitionCode(err) != definition.CodeWorkflowNotPublished {
		t.Fatalf("unpublished resolution error = %v", err)
	}

	commands := []definition.SaveDraftCommand{
		{ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID, ExpectedRevision: 1, IRJSON: minimalIR("human", 1), IdempotencyKey: "save-human-0001"},
		{ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID, ExpectedRevision: 1, IRJSON: minimalIR("agent", 2), IdempotencyKey: "save-agent-0001"},
	}
	type saveResult struct {
		revision definition.DraftRevision
		err      error
	}
	results := make(chan saveResult, 2)
	var start sync.WaitGroup
	start.Add(1)
	for _, command := range commands {
		command := command
		go func() {
			start.Wait()
			revision, _, err := harness.definitions.SaveDraft(harness.ctx, command)
			results <- saveResult{revision: revision, err: err}
		}()
	}
	start.Done()
	var winner definition.DraftRevision
	conflicts := 0
	for range commands {
		result := <-results
		if result.err == nil {
			winner = result.revision
		} else if definitionCode(result.err) == definition.CodeDraftRevisionConflict {
			conflicts++
			var conflict *definition.Error
			if !errors.As(result.err, &conflict) || conflict.Details["base_revision"] != int64(1) || conflict.Details["current_revision"] != int64(2) {
				t.Fatalf("conflict merge context missing: %v", result.err)
			}
			for _, field := range []string{"base_ir", "current_ir", "submitted_ir"} {
				var payload []byte
				switch value := conflict.Details[field].(type) {
				case json.RawMessage:
					payload = value
				case []byte:
					payload = value
				}
				if !json.Valid(payload) {
					t.Fatalf("invalid %s: type=%T value=%q", field, conflict.Details[field], payload)
				}
			}
		} else {
			t.Fatalf("unexpected concurrent save error: %v", result.err)
		}
	}
	if winner.RevisionNumber != 2 || conflicts != 1 {
		t.Fatalf("winner=%+v conflicts=%d", winner, conflicts)
	}

	diagnostics, err = harness.definitions.ValidateDraft(harness.ctx, harness.projectID, harness.principalID, workflow.ID, 2)
	assertNoDefinitionFailure(t, diagnostics, err)
	testSnapshot, diagnostics, err := harness.definitions.CompileDraftTestSnapshot(harness.ctx, harness.projectID, harness.principalID, workflow.ID, 2)
	assertNoDefinitionFailure(t, diagnostics, err)
	if testSnapshot.OriginKind != definition.SnapshotOriginDraftTest {
		t.Fatalf("test snapshot origin = %s", testSnapshot.OriginKind)
	}

	publish := definition.PublishCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID,
		ExpectedRevision: 2, ChangeLog: "v1", IdempotencyKey: "publish-lifecycle-v1",
	}
	version1, snapshot1, diagnostics, err := harness.definitions.Publish(harness.ctx, publish)
	assertNoDefinitionFailure(t, diagnostics, err)
	if testSnapshot.DefinitionHash != snapshot1.DefinitionHash || testSnapshot.ID != snapshot1.ID {
		t.Fatalf("test and publish did not reuse the same compiler snapshot: test=%s publish=%s", testSnapshot.ID, snapshot1.ID)
	}
	replayed, replayedSnapshot, diagnostics, err := harness.definitions.Publish(harness.ctx, publish)
	assertNoDefinitionFailure(t, diagnostics, err)
	if version1.ID != replayed.ID || version1.VersionNumber != 1 || snapshot1.ID != replayedSnapshot.ID {
		t.Fatalf("publish replay changed identity: first=%+v replay=%+v", version1, replayed)
	}
	assertCount(t, harness, "workflow_versions", 1)
	assertCount(t, harness, "workflow_definition_audits", 1)

	publish.IdempotencyKey = "publish-lifecycle-v2"
	publish.ChangeLog = "v2"
	version2, _, diagnostics, err := harness.definitions.Publish(harness.ctx, publish)
	assertNoDefinitionFailure(t, diagnostics, err)
	if version2.VersionNumber != 2 {
		t.Fatalf("second version number = %d", version2.VersionNumber)
	}
	active, err := harness.definitions.ResolveProductionDefinition(harness.ctx, harness.projectID, harness.principalID, workflow.ID)
	if err != nil || active.Version.ID != version2.ID {
		t.Fatalf("automatic activation failed: %+v %v", active.Version, err)
	}
	rolledBack, err := harness.definitions.Rollback(harness.ctx, harness.projectID, harness.principalID, workflow.ID, 1)
	if err != nil || rolledBack.ID != version1.ID {
		t.Fatalf("rollback failed: %+v %v", rolledBack, err)
	}
	active, err = harness.definitions.ResolveProductionDefinition(harness.ctx, harness.projectID, harness.principalID, workflow.ID)
	if err != nil || active.Version.ID != version1.ID {
		t.Fatalf("rollback did not change only active pointer: %+v %v", active.Version, err)
	}

	for name, statement := range map[string]string{
		"version update":  `UPDATE workflow_versions SET change_log = 'changed' WHERE version_id = UUID_TO_BIN($1)`,
		"version delete":  `DELETE FROM workflow_versions WHERE version_id = UUID_TO_BIN($1)`,
		"snapshot update": `UPDATE workflow_execution_snapshots SET definition_hash = repeat('a',64) WHERE snapshot_id = UUID_TO_BIN($1)`,
		"snapshot delete": `DELETE FROM workflow_execution_snapshots WHERE snapshot_id = UUID_TO_BIN($1)`,
	} {
		id := version1.ID
		if strings.Contains(name, "snapshot") {
			id = snapshot1.ID
		}
		if _, err := harness.client.Pool().Exec(harness.ctx, statement, id); err == nil {
			t.Fatalf("%s unexpectedly succeeded", name)
		}
	}

	copyWorkflow, copyDraft, err := harness.definitions.CopyPublishedVersion(harness.ctx, definition.CopyCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, SourceWorkflowID: workflow.ID,
		SourceVersionNumber: 1, Name: "Copied", IdempotencyKey: "copy-lifecycle-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	canonicalCopy := mustCanonical(t, copyDraft.IRJSON)
	canonicalSource := mustCanonical(t, snapshot1.IRJSON)
	if !bytes.Equal(canonicalCopy, canonicalSource) || copyWorkflow.ActiveVersionID != nil || copyDraft.ClonedFromVersionID == nil || *copyDraft.ClonedFromVersionID != version1.ID {
		t.Fatalf("copy did not use saved IR snapshot: workflow=%+v draft=%+v", copyWorkflow, copyDraft)
	}
	var copyVersions int
	if err := harness.client.Pool().QueryRow(harness.ctx, `SELECT count(*) FROM workflow_versions WHERE project_id=UUID_TO_BIN($1) AND workflow_id=UUID_TO_BIN($2)`, harness.projectID, copyWorkflow.ID).Scan(&copyVersions); err != nil || copyVersions != 0 {
		t.Fatalf("copy version count=%d err=%v", copyVersions, err)
	}
	if _, err := harness.definitions.ResolveProductionDefinition(harness.ctx, harness.projectID, harness.principalID, copyWorkflow.ID); definitionCode(err) != definition.CodeWorkflowNotPublished {
		t.Fatalf("copied workflow production resolution error = %v", err)
	}
}

func TestM3ConflictMergeSaveAndIdempotency(t *testing.T) {
	harness := newM3Harness(t)
	workflow, _, diagnostics, err := harness.definitions.CreateWorkflow(harness.ctx, definition.CreateWorkflowCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, Name: "Merge",
		IRJSON: minimalIR("base", 0), IdempotencyKey: "merge-create",
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	command := func(expected int64, title, key string) definition.SaveDraftCommand {
		return definition.SaveDraftCommand{ProjectID: harness.projectID, PrincipalID: harness.principalID,
			WorkflowID: workflow.ID, ExpectedRevision: expected, IRJSON: minimalIR(title, 0), IdempotencyKey: key}
	}
	first, diagnostics, err := harness.definitions.SaveDraft(harness.ctx, command(1, "A", "merge-save-a"))
	assertNoDefinitionFailure(t, diagnostics, err)
	if first.RevisionNumber != 2 {
		t.Fatalf("first revision=%d", first.RevisionNumber)
	}
	_, _, err = harness.definitions.SaveDraft(harness.ctx, command(1, "B", "merge-save-b"))
	var conflict *definition.Error
	if !errors.As(err, &conflict) || conflict.Code != definition.CodeDraftRevisionConflict {
		t.Fatalf("expected merge conflict, got %v", err)
	}
	if conflict.Details["base_revision"] != int64(1) || conflict.Details["current_revision"] != int64(2) {
		t.Fatalf("incorrect merge revisions: %+v", conflict.Details)
	}
	current, err := harness.store.GetCurrentDraft(harness.ctx, harness.projectID, workflow.ID)
	if err != nil || current.CurrentRevision != 2 {
		t.Fatalf("conflict changed draft: %+v %v", current, err)
	}
	merged, diagnostics, err := harness.definitions.SaveDraft(harness.ctx, command(2, "chosen", "merge-confirm"))
	assertNoDefinitionFailure(t, diagnostics, err)
	if merged.RevisionNumber != 3 {
		t.Fatalf("merged revision=%d", merged.RevisionNumber)
	}
	replay, diagnostics, err := harness.definitions.SaveDraft(harness.ctx, command(2, "chosen", "merge-confirm"))
	assertNoDefinitionFailure(t, diagnostics, err)
	if replay.ID != merged.ID {
		t.Fatalf("replay created another revision: %s != %s", replay.ID, merged.ID)
	}
	_, _, err = harness.definitions.SaveDraft(harness.ctx, command(2, "different", "merge-confirm"))
	if definitionCode(err) != definition.CodeIdempotencyKeyReused {
		t.Fatalf("key reuse error=%v", err)
	}
}

func TestM3PublishTransactionRollsBackEveryFact(t *testing.T) {
	harness := newM3Harness(t)
	workflow, _, diagnostics, err := harness.definitions.CreateWorkflow(harness.ctx, definition.CreateWorkflowCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, Name: "Atomic",
		IRJSON: minimalIR("atomic", 0), IdempotencyKey: "create-atomic-01",
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	mustExec(t, harness.ctx, harness.client.Pool(), `
		CREATE TRIGGER inject_publish_failure BEFORE INSERT ON workflow_definition_audits
		FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'injected publish failure'`)
	_, _, diagnostics, err = harness.definitions.Publish(harness.ctx, definition.PublishCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID,
		ExpectedRevision: 1, ChangeLog: "must rollback", IdempotencyKey: "publish-atomic-01",
	})
	if err == nil || ir.HasErrors(diagnostics) {
		t.Fatalf("expected injected infrastructure error, diagnostics=%+v err=%v", diagnostics, err)
	}
	for _, table := range []string{"workflow_execution_snapshots", "workflow_versions", "workflow_definition_audits"} {
		assertCount(t, harness, table, 0)
	}
	var publishKeys int
	if err := harness.client.Pool().QueryRow(harness.ctx, `SELECT count(*) FROM definition_idempotency WHERE command_name='publish_workflow'`).Scan(&publishKeys); err != nil || publishKeys != 0 {
		t.Fatalf("publish idempotency survived failed transaction: count=%d err=%v", publishKeys, err)
	}
	var active *string
	if err := harness.client.Pool().QueryRow(harness.ctx, `SELECT BIN_TO_UUID(active_version_id) FROM workflows WHERE project_id=UUID_TO_BIN($1) AND workflow_id=UUID_TO_BIN($2)`, harness.projectID, workflow.ID).Scan(&active); err != nil || active != nil {
		t.Fatalf("active pointer survived failed transaction: %v %v", active, err)
	}
}

func TestM3PermissionsManagedResourcesAndProjectIsolation(t *testing.T) {
	harness := newM3Harness(t)
	workflow, _, diagnostics, err := harness.definitions.CreateWorkflow(harness.ctx, definition.CreateWorkflowCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, Name: "Resources",
		IRJSON: httpIR("shared_api"), IdempotencyKey: "create-resource-01",
	})
	assertNoDefinitionFailure(t, diagnostics, err)

	otherProject, otherPrincipal, otherExecution := newID(t), newID(t), newID(t)
	harness.seedProject(t, otherProject, otherPrincipal, otherExecution, "other-project-token-0001", allPermissions())
	seedConnection(t, harness, otherProject, otherExecution, "shared_api", true, true)
	diagnostics, err = harness.definitions.ValidateDraft(harness.ctx, harness.projectID, harness.principalID, workflow.ID, 1)
	if err != nil || !hasDiagnostic(diagnostics, definition.CodeResourceNotFound) {
		t.Fatalf("cross-project resource was not hidden: diagnostics=%+v err=%v", diagnostics, err)
	}

	connectionID := seedConnection(t, harness, harness.projectID, harness.executionID, "shared_api", false, true)
	diagnostics, err = harness.definitions.ValidateDraft(harness.ctx, harness.projectID, harness.principalID, workflow.ID, 1)
	if err != nil || !hasDiagnostic(diagnostics, definition.CodeResourceNotFound) {
		t.Fatalf("test-purpose execution grant was not enforced: diagnostics=%+v err=%v", diagnostics, err)
	}
	mustExec(t, harness.ctx, harness.client.Pool(), `
		INSERT INTO connection_execution_grants (project_id, connection_id, execution_identity_id, purpose)
		VALUES ($1,$2,$3,'test')`, harness.projectID, connectionID, harness.executionID)
	diagnostics, err = harness.definitions.ValidateDraft(harness.ctx, harness.projectID, harness.principalID, workflow.ID, 1)
	assertNoDefinitionFailure(t, diagnostics, err)
	publish := definition.PublishCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, WorkflowID: workflow.ID,
		ExpectedRevision: 1, ChangeLog: "resource publish", IdempotencyKey: "publish-resource-01",
	}
	published, _, diagnostics, err := harness.definitions.Publish(harness.ctx, publish)
	assertNoDefinitionFailure(t, diagnostics, err)
	mustExec(t, harness.ctx, harness.client.Pool(), `
		DELETE FROM connection_execution_grants
		WHERE project_id=UUID_TO_BIN($1) AND connection_id=UUID_TO_BIN($2) AND execution_identity_id=UUID_TO_BIN($3) AND purpose='production'`,
		harness.projectID, connectionID, harness.executionID)
	replayed, _, diagnostics, err := harness.definitions.Publish(harness.ctx, publish)
	assertNoDefinitionFailure(t, diagnostics, err)
	if replayed.ID != published.ID {
		t.Fatalf("publish replay after grant revocation created a different version: %s != %s", replayed.ID, published.ID)
	}
	publish.ChangeLog = "different request"
	if _, _, _, err := harness.definitions.Publish(harness.ctx, publish); definitionCode(err) != definition.CodeIdempotencyKeyReused {
		t.Fatalf("same key with different request error = %v", err)
	}

	rpcWorkflow, _, diagnostics, err := harness.definitions.CreateWorkflow(harness.ctx, definition.CreateWorkflowCommand{
		ProjectID: harness.projectID, PrincipalID: harness.principalID, Name: "RPC Resources",
		IRJSON: rpcIR("orders", "Create"), IdempotencyKey: "create-rpc-resource-01",
	})
	assertNoDefinitionFailure(t, diagnostics, err)
	diagnostics, err = harness.definitions.ValidateDraft(harness.ctx, harness.projectID, harness.principalID, rpcWorkflow.ID, 1)
	if err != nil || !hasDiagnostic(diagnostics, definition.CodeResourceNotFound) {
		t.Fatalf("missing RPC grant was not hidden: diagnostics=%+v err=%v", diagnostics, err)
	}
	serviceID := seedRPCService(t, harness, harness.projectID, harness.executionID, "orders", "Create", true, true)
	diagnostics, err = harness.definitions.ValidateDraft(harness.ctx, harness.projectID, harness.principalID, rpcWorkflow.ID, 1)
	assertNoDefinitionFailure(t, diagnostics, err)
	if serviceID == "" {
		t.Fatal("RPC service seed returned an empty identity")
	}

	limitedPrincipal := newID(t)
	harness.seedAdditionalPrincipal(t, limitedPrincipal, "limited-principal-token", []access.Permission{access.PermissionWorkflowRead, access.PermissionWorkflowWrite})
	diagnostics, err = harness.definitions.ValidateDraft(harness.ctx, harness.projectID, limitedPrincipal, workflow.ID, 1)
	if definitionCode(err) != definition.CodePermissionDenied || len(diagnostics) != 0 {
		t.Fatalf("connection.use permission was not enforced: diagnostics=%+v err=%v", diagnostics, err)
	}

	if _, err := harness.store.GetWorkflow(harness.ctx, otherProject, workflow.ID); !errors.Is(err, definition.ErrResourceNotFound) {
		t.Fatalf("workflow repository crossed project boundary: %v", err)
	}
	if _, err := harness.store.GetDraftRevision(harness.ctx, otherProject, workflow.ID, 1); !errors.Is(err, definition.ErrResourceNotFound) {
		t.Fatalf("draft repository crossed project boundary: %v", err)
	}
	if _, err := harness.store.ResolveConnection(harness.ctx, otherProject, otherExecution, "missing", resources.PurposeProduction); !errors.Is(err, resources.ErrResourceNotFound) {
		t.Fatalf("resource repository crossed project boundary: %v", err)
	}
}

func (harness *m3Harness) seedAdditionalPrincipal(t *testing.T, principalID, token string, permissions []access.Permission) {
	t.Helper()
	digest := sha256.Sum256([]byte(token))
	mustExec(t, harness.ctx, harness.client.Pool(), `INSERT INTO principals (principal_id, kind, display_name) VALUES ($1,'service_account','Limited Agent')`, principalID)
	mustExec(t, harness.ctx, harness.client.Pool(), `INSERT INTO principal_credentials (credential_id, principal_id, credential_hash) VALUES ($1,$2,$3)`, newID(t), principalID, digest[:])
	mustExec(t, harness.ctx, harness.client.Pool(), `INSERT INTO project_memberships (project_id, principal_id) VALUES ($1,$2)`, harness.projectID, principalID)
	for _, permission := range permissions {
		mustExec(t, harness.ctx, harness.client.Pool(), `INSERT INTO project_membership_permissions (project_id, principal_id, permission) VALUES ($1,$2,$3)`, harness.projectID, principalID, permission)
	}
}

func seedConnection(t *testing.T, harness *m3Harness, projectID, executionID, reference string, grantTest, grantProduction bool) string {
	t.Helper()
	connectionID := newID(t)
	mustExec(t, harness.ctx, harness.client.Pool(), `
		INSERT INTO connections (connection_id, project_id, reference, base_url)
		VALUES ($1,$2,$3,'https://example.invalid')`, connectionID, projectID, reference)
	for purpose, granted := range map[string]bool{"test": grantTest, "production": grantProduction} {
		if granted {
			mustExec(t, harness.ctx, harness.client.Pool(), `
				INSERT INTO connection_execution_grants (project_id, connection_id, execution_identity_id, purpose)
				VALUES ($1,$2,$3,$4)`, projectID, connectionID, executionID, purpose)
		}
	}
	return connectionID
}

func seedRPCService(t *testing.T, harness *m3Harness, projectID, executionID, reference, operation string, grantTest, grantProduction bool) string {
	t.Helper()
	serviceID := newID(t)
	mustExec(t, harness.ctx, harness.client.Pool(), `
		INSERT INTO rpc_services (service_id, project_id, reference, protocol, discovery_reference)
		VALUES ($1,$2,$3,'grpc','service://orders')`, serviceID, projectID, reference)
	mustExec(t, harness.ctx, harness.client.Pool(), `
		INSERT INTO rpc_service_operations (
			project_id, service_id, operation, contract_revision,
			request_schema, response_schema, idempotent
		) VALUES ($1,$2,$3,'contract-1','{}','{}',true)`, projectID, serviceID, operation)
	for purpose, granted := range map[string]bool{"test": grantTest, "production": grantProduction} {
		if granted {
			mustExec(t, harness.ctx, harness.client.Pool(), `
				INSERT INTO rpc_service_execution_grants (project_id, service_id, execution_identity_id, purpose)
				VALUES ($1,$2,$3,$4)`, projectID, serviceID, executionID, purpose)
		}
	}
	return serviceID
}

func minimalIR(title string, x int) []byte {
	return []byte(fmt.Sprintf(`{
		"ir_version":"1","nodes":[
			{"id":"start","type":"start","title":%q,"inputs":[],"outputs":[{"name":"workflow_input","data_type":"object"}]},
			{"id":"end","type":"end","title":"End","inputs":[{"name":"workflow_output","data_type":"object","source":"literal","value":{}}],"outputs":[]}
		],"edges":[{"id":"start_to_end","source":"start","target":"end"}],
		"layout":{"start":{"x":%d,"y":0},"end":{"x":100,"y":0}}
	}`, title, x))
}

func httpIR(reference string) []byte {
	value := map[string]any{
		"ir_version": "1",
		"nodes": []any{
			map[string]any{"id": "start", "type": "start", "title": "Start", "inputs": []any{}, "outputs": []any{map[string]any{"name": "workflow_input", "data_type": "object"}}},
			map[string]any{"id": "call", "type": "http", "title": "Call", "inputs": []any{
				map[string]any{"name": "connection_ref", "data_type": "string", "source": "literal", "value": reference},
				map[string]any{"name": "method", "data_type": "string", "source": "literal", "value": "GET"},
				map[string]any{"name": "relative_path", "data_type": "string", "source": "literal", "value": "/v1/frog"},
			}, "outputs": []any{map[string]any{"name": "response", "data_type": "object"}}},
			map[string]any{"id": "end", "type": "end", "title": "End", "inputs": []any{map[string]any{"name": "workflow_output", "data_type": "object", "source": "literal", "value": map[string]any{}}}, "outputs": []any{}},
		},
		"edges":  []any{map[string]any{"id": "start_to_call", "source": "start", "target": "call"}, map[string]any{"id": "call_to_end", "source": "call", "target": "end"}},
		"layout": map[string]any{"start": map[string]any{"x": 0, "y": 0}, "call": map[string]any{"x": 100, "y": 0}, "end": map[string]any{"x": 200, "y": 0}},
	}
	raw, _ := json.Marshal(value)
	return raw
}

func rpcIR(reference, operation string) []byte {
	value := map[string]any{
		"ir_version": "1",
		"nodes": []any{
			map[string]any{"id": "start", "type": "start", "title": "Start", "inputs": []any{}, "outputs": []any{map[string]any{"name": "workflow_input", "data_type": "object"}}},
			map[string]any{"id": "call", "type": "rpc", "title": "Call", "inputs": []any{
				map[string]any{"name": "service_ref", "data_type": "string", "source": "literal", "value": reference},
				map[string]any{"name": "operation", "data_type": "string", "source": "literal", "value": operation},
				map[string]any{"name": "request", "data_type": "object", "source": "literal", "value": map[string]any{}},
			}, "outputs": []any{map[string]any{"name": "response", "data_type": "object"}}},
			map[string]any{"id": "end", "type": "end", "title": "End", "inputs": []any{map[string]any{"name": "workflow_output", "data_type": "object", "source": "literal", "value": map[string]any{}}}, "outputs": []any{}},
		},
		"edges":  []any{map[string]any{"id": "start_to_call", "source": "start", "target": "call"}, map[string]any{"id": "call_to_end", "source": "call", "target": "end"}},
		"layout": map[string]any{"start": map[string]any{"x": 0, "y": 0}, "call": map[string]any{"x": 100, "y": 0}, "end": map[string]any{"x": 200, "y": 0}},
	}
	raw, _ := json.Marshal(value)
	return raw
}

func allPermissions() []access.Permission {
	return []access.Permission{
		access.PermissionWorkflowRead, access.PermissionWorkflowWrite, access.PermissionWorkflowTest,
		access.PermissionWorkflowPublish, access.PermissionRunCreate, access.PermissionRunRead,
		access.PermissionRunCancel, access.PermissionConnectionUse, access.PermissionServiceUse,
		access.PermissionProjectAdmin,
	}
}

type execer interface {
	Exec(context.Context, string, ...any) (mysql.CommandTag, error)
}

func mustExec(t *testing.T, ctx context.Context, target execer, sql string, arguments ...any) {
	t.Helper()
	for index, argument := range arguments {
		value, ok := argument.(string)
		if !ok {
			continue
		}
		parsed, err := uuid.Parse(value)
		wrapped := regexp.MustCompile(fmt.Sprintf(`(?i)UUID_TO_BIN\(\s*\$%d\s*\)`, index+1)).MatchString(sql)
		if err == nil && !wrapped {
			binary := make([]byte, 16)
			copy(binary, parsed[:])
			arguments[index] = binary
		}
	}
	if _, err := target.Exec(ctx, sql, arguments...); err != nil {
		t.Fatal(err)
	}
}

func newID(t *testing.T) string {
	t.Helper()
	value, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	return value.String()
}

func assertNoDefinitionFailure(t *testing.T, diagnostics []ir.Diagnostic, err error) {
	t.Helper()
	if err != nil || ir.HasErrors(diagnostics) {
		t.Fatalf("diagnostics=%+v err=%v", diagnostics, err)
	}
}

func definitionCode(err error) string {
	var value *definition.Error
	if errors.As(err, &value) {
		return value.Code
	}
	return ""
}

func hasDiagnostic(diagnostics []ir.Diagnostic, code string) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return true
		}
	}
	return false
}

func assertCount(t *testing.T, harness *m3Harness, table string, expected int) {
	t.Helper()
	var count int
	query := fmt.Sprintf("SELECT count(*) FROM %s", mysqlIdentifier(table))
	if err := harness.client.Pool().QueryRow(harness.ctx, query).Scan(&count); err != nil || count != expected {
		t.Fatalf("%s count=%d want=%d err=%v", table, count, expected, err)
	}
}

func mustCanonical(t *testing.T, raw []byte) []byte {
	t.Helper()
	canonical, err := ir.CanonicalizeJSON(raw, ir.DefaultParseLimits)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}
