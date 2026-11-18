package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuilderAssemblesIRInSmallSteps(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	session := filepath.Join(root, "order-builder")
	app, output := builderTestApp(root)
	run := func(want int, arguments ...string) map[string]any {
		t.Helper()
		output.Reset()
		if got := app.Run(context.Background(), append([]string{"workflow", "builder"}, arguments...)); got != want {
			t.Fatalf("command=%v code=%d want=%d output=%s", arguments, got, want, output.String())
		}
		return decodeBuilderEnvelope(t, output.Bytes())
	}

	run(0, "init", "--session", session)
	run(0, "add-node", "--session", session, "--id", "start", "--type", "start", "--title", "Start")
	run(0, "set-output", "--session", session, "--node", "start", "--name", "workflow_input", "--data-type", "object")
	run(0, "add-node", "--session", session, "--id", "normalize_request", "--type", "code", "--title", "Normalize request")
	run(0, "set-input", "--session", session, "--node", "normalize_request", "--name", "source_code", "--data-type", "string", "--literal", `"def main(inputs):\n    return {'result': inputs['request']}"`)
	run(0, "bind", "--session", session, "--node", "normalize_request", "--name", "request", "--data-type", "object", "--source-node", "start", "--source-output", "workflow_input")
	run(0, "set-output", "--session", session, "--node", "normalize_request", "--name", "result", "--data-type", "object")
	run(0, "add-node", "--session", session, "--id", "end", "--type", "end", "--title", "End")
	run(0, "bind", "--session", session, "--node", "end", "--name", "workflow_output", "--data-type", "object", "--source-node", "normalize_request", "--source-output", "result")
	run(0, "add-edge", "--session", session, "--id", "start_to_normalize", "--source", "start", "--target", "normalize_request")
	run(0, "add-edge", "--session", session, "--id", "normalize_to_end", "--source", "normalize_request", "--target", "end")
	run(0, "set-layout", "--session", session, "--node", "normalize_request", "--x", "320", "--y", "160")

	check := run(0, "check", "--session", session)
	checkData := check["data"].(map[string]any)
	if valid, _ := checkData["valid"].(bool); !valid {
		t.Fatalf("incrementally assembled IR is structurally invalid: %s", output.String())
	}
	previewFile := filepath.Join(root, "preview.ir.json")
	preview := run(0, "preview", "--session", session, "--out", previewFile)
	previewData := preview["data"].(map[string]any)
	if dirty, _ := previewData["dirty"].(bool); !dirty {
		t.Fatalf("local mutation was not marked dirty: %s", output.String())
	}
	if _, exists := previewData["ir"].(map[string]any); !exists {
		t.Fatalf("preview does not return IR: %s", output.String())
	}
	if written, err := os.ReadFile(previewFile); err != nil || !bytes.Contains(written, []byte(`"normalize_request"`)) {
		t.Fatalf("preview export=%s err=%v", written, err)
	}

	loaded, err := loadBuilderSession(session)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Document.Nodes) != 3 || len(loaded.Document.Edges) != 2 || loaded.Document.Layout["normalize_request"].X.String() != "320" {
		t.Fatalf("builder session=%+v", loaded.Document)
	}
}

func TestBuilderRemoteLifecycleUsesIRAndPreventsDirtyPull(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	session := filepath.Join(root, "remote-builder")
	irFile := filepath.Join(root, "initial.ir.json")
	if err := os.WriteFile(irFile, validCLIIR("Initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	project := "11111111-1111-4111-8111-111111111111"
	workflow := "22222222-2222-4222-8222-222222222222"
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.Method+" "+request.URL.Path)
		if request.Header.Get("Authorization") != "Bearer token-value" {
			t.Fatalf("authorization=%q", request.Header.Get("Authorization"))
		}
		if strings.Contains(strings.ToLower(request.URL.Path), "dsl") || strings.Contains(strings.ToLower(request.URL.Path), "source_map") {
			t.Fatalf("builder reached forbidden path %s", request.URL.Path)
		}
		switch request.Method + " " + request.URL.Path {
		case "POST /v1/projects/" + project + "/workflows":
			var body map[string]json.RawMessage
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body["ir"] == nil || body["dsl"] != nil || body["source_map"] != nil {
				t.Fatalf("create body=%v err=%v", body, err)
			}
			writeCLIJSON(t, writer, http.StatusCreated, map[string]any{
				"workflow":       completeWorkflowResponse(project, workflow, "Order normalizer"),
				"draft_revision": completeDraftRevisionResponse(project, workflow, 1, validCLIIR("Initial")),
			})
		case "PUT /v1/projects/" + project + "/workflows/" + workflow + "/draft":
			var body struct {
				ExpectedRevision int64           `json:"expected_revision"`
				IR               json.RawMessage `json:"ir"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.ExpectedRevision != 1 || body.IR == nil {
				t.Fatalf("push body=%+v err=%v", body, err)
			}
			writeCLIJSON(t, writer, http.StatusCreated, completeDraftRevisionResponse(project, workflow, 2, validCLIIR("Updated")))
		case "POST /v1/projects/" + project + "/workflows/" + workflow + "/draft/validate":
			writeCLIJSON(t, writer, http.StatusOK, map[string]any{"valid": true, "diagnostics": []any{}})
		case "GET /v1/projects/" + project + "/workflows/" + workflow + "/draft":
			writeCLIJSON(t, writer, http.StatusOK, map[string]any{
				"project_id":       project,
				"workflow_id":      workflow,
				"current_revision": 3,
				"state_version":    3,
				"current":          completeDraftRevisionResponse(project, workflow, 3, validCLIIR("Pulled")),
			})
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	app, output := builderTestApp(root)
	run := func(want int, arguments ...string) map[string]any {
		t.Helper()
		output.Reset()
		if got := app.Run(context.Background(), append([]string{"workflow", "builder"}, arguments...)); got != want {
			t.Fatalf("command=%v code=%d want=%d output=%s", arguments, got, want, output.String())
		}
		return decodeBuilderEnvelope(t, output.Bytes())
	}
	contextFlags := []string{"--server", server.URL, "--token", "token-value", "--project", project}
	run(0, "init", "--session", session, "--from-ir", irFile)
	run(0, append([]string{"create", "--session", session, "--name", "Order normalizer", "--idempotency-key", "builder-create-001"}, contextFlags...)...)
	run(0, "set-title", "--session", session, "--node", "end", "--title", "Local edit")
	conflict := run(1, append([]string{"pull", "--session", session, "--workflow", workflow}, contextFlags...)...)
	if conflict["error"].(map[string]any)["code"] != "LOCAL_CHANGES_NOT_PUSHED" {
		t.Fatalf("dirty pull response=%v", conflict)
	}
	run(0, "push", "--session", session, "--token", "token-value", "--idempotency-key", "builder-push-001")
	validated := run(0, "validate", "--session", session, "--token", "token-value")
	if valid, _ := validated["data"].(map[string]any)["valid"].(bool); !valid {
		t.Fatalf("validate response=%v", validated)
	}
	run(0, "set-title", "--session", session, "--node", "end", "--title", "Discard this")
	run(0, append([]string{"pull", "--session", session, "--workflow", workflow, "--discard-local"}, contextFlags...)...)
	loaded, err := loadBuilderSession(session)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Meta.Revision != 3 || loaded.Document.Nodes[1].Title != "Pulled" || loaded.dirty() {
		t.Fatalf("pull did not synchronize session: %+v %+v", loaded.Meta, loaded.Document.Nodes[1])
	}
	wantPaths := []string{
		"POST /v1/projects/" + project + "/workflows",
		"PUT /v1/projects/" + project + "/workflows/" + workflow + "/draft",
		"POST /v1/projects/" + project + "/workflows/" + workflow + "/draft/validate",
		"GET /v1/projects/" + project + "/workflows/" + workflow + "/draft",
	}
	if strings.Join(paths, ",") != strings.Join(wantPaths, ",") {
		t.Fatalf("paths=%v want=%v", paths, wantPaths)
	}
	meta, err := os.ReadFile(builderMetaPath(session))
	if err != nil || bytes.Contains(meta, []byte("token-value")) {
		t.Fatalf("metadata leaked token or read failed: %s err=%v", meta, err)
	}
}

func TestBuilderCopyCreatesCleanBoundSession(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	session := filepath.Join(root, "copied-builder")
	project := "11111111-1111-4111-8111-111111111111"
	sourceWorkflow := "22222222-2222-4222-8222-222222222222"
	copiedWorkflow := "33333333-3333-4333-8333-333333333333"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/projects/"+project+"/workflows:copy" {
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
		var body map[string]json.RawMessage
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if string(body["source_workflow_id"]) != `"`+sourceWorkflow+`"` || string(body["source_version_number"]) != "1" || body["ir"] != nil {
			t.Fatalf("copy body=%v", body)
		}
		writeCLIJSON(t, writer, http.StatusCreated, map[string]any{
			"workflow":       completeWorkflowResponse(project, copiedWorkflow, "Copied workflow"),
			"draft_revision": completeDraftRevisionResponse(project, copiedWorkflow, 1, validCLIIR("Copied")),
		})
	}))
	defer server.Close()

	app, output := builderTestApp(root)
	arguments := []string{"workflow", "builder", "copy", "--session", session, "--source-workflow", sourceWorkflow, "--version", "1", "--name", "Copied workflow", "--idempotency-key", "builder-copy-001", "--server", server.URL, "--token", "token-value", "--project", project}
	if got := app.Run(context.Background(), arguments); got != 0 {
		t.Fatalf("copy code=%d output=%s", got, output.String())
	}
	loaded, err := loadBuilderSession(session)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Meta.WorkflowID != copiedWorkflow || loaded.Meta.Revision != 1 || loaded.dirty() || loaded.Document.Nodes[1].Title != "Copied" {
		t.Fatalf("copied session=%+v document=%+v", loaded.Meta, loaded.Document)
	}
}

func TestBuilderRejectsInvalidLiteralAndUnknownGraphTargets(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	session := filepath.Join(root, "safe-builder")
	app, output := builderTestApp(root)
	run := func(want int, arguments ...string) map[string]any {
		t.Helper()
		output.Reset()
		if got := app.Run(context.Background(), append([]string{"workflow", "builder"}, arguments...)); got != want {
			t.Fatalf("command=%v code=%d want=%d output=%s", arguments, got, want, output.String())
		}
		return decodeBuilderEnvelope(t, output.Bytes())
	}
	run(0, "init", "--session", session)
	run(0, "add-node", "--session", session, "--id", "start", "--type", "start", "--title", "Start")
	invalidLiteral := run(1, "set-input", "--session", session, "--node", "start", "--name", "invalid", "--data-type", "string", "--literal", "null")
	if invalidLiteral["error"].(map[string]any)["code"] != "BUILDER_ERROR" {
		t.Fatalf("invalid literal response=%v", invalidLiteral)
	}
	missingOutput := run(1, "bind", "--session", session, "--node", "start", "--name", "input", "--data-type", "object", "--source-node", "start", "--source-output", "missing")
	if missingOutput["error"].(map[string]any)["code"] != "SOURCE_OUTPUT_NOT_FOUND" {
		t.Fatalf("missing output response=%v", missingOutput)
	}
	missingTarget := run(1, "add-edge", "--session", session, "--id", "start_to_missing", "--source", "start", "--target", "missing")
	if missingTarget["error"].(map[string]any)["code"] != "TARGET_NODE_NOT_FOUND" {
		t.Fatalf("missing target response=%v", missingTarget)
	}
}

// These fixtures mirror the complete External API payload, rather than the
// narrow subset that a command happens to print. The API client intentionally
// rejects unknown JSON fields, so this guards the CLI against treating a
// successful server-side definition write as a client-side decode failure.
func completeWorkflowResponse(projectID, workflowID, name string) map[string]any {
	return map[string]any{
		"workflow_id":       workflowID,
		"project_id":        projectID,
		"name":              name,
		"active_version_id": nil,
		"created_by":        "44444444-4444-4444-8444-444444444444",
		"created_at":        "2026-08-16T00:00:00Z",
		"updated_at":        "2026-08-16T00:00:00Z",
	}
}

func completeDraftRevisionResponse(projectID, workflowID string, revision int64, ir []byte) map[string]any {
	return map[string]any{
		"draft_revision_id":      "55555555-5555-4555-8555-555555555555",
		"project_id":             projectID,
		"workflow_id":            workflowID,
		"revision_number":        revision,
		"ir":                     json.RawMessage(ir),
		"catalog_revision":       "catalog-2026-08-16",
		"created_by":             "44444444-4444-4444-8444-444444444444",
		"created_at":             "2026-08-16T00:00:00Z",
		"cloned_from_version_id": nil,
	}
}

func completePublishedVersionResponse(projectID, workflowID string, number int64) map[string]any {
	return map[string]any{
		"version_id":               "66666666-6666-4666-8666-666666666666",
		"project_id":               projectID,
		"workflow_id":              workflowID,
		"version_number":           number,
		"source_draft_revision_id": "55555555-5555-4555-8555-555555555555",
		"execution_snapshot_id":    "77777777-7777-4777-8777-777777777777",
		"change_log":               "published by test",
		"created_by":               "44444444-4444-4444-8444-444444444444",
		"created_at":               "2026-08-16T00:00:00Z",
	}
}

func builderTestApp(home string) (*App, *bytes.Buffer) {
	output := &bytes.Buffer{}
	return &App{Output: output, Error: output, Home: home}, output
}

func decodeBuilderEnvelope(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode builder output %q: %v", raw, err)
	}
	return value
}
