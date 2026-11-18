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
	"time"
)

func TestM10CLIProductLoopUsesOnlyIRAndStableExternalPaths(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	project := "11111111-1111-4111-8111-111111111111"
	workflow := "22222222-2222-4222-8222-222222222222"
	copyWorkflow := "33333333-3333-4333-8333-333333333333"
	input := filepath.Join(root, "input.json")
	initialIR := filepath.Join(root, "initial.json")
	updatedIR := filepath.Join(root, "updated.json")
	if err := os.WriteFile(input, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(initialIR, validCLIIR("Initial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(updatedIR, validCLIIR("Updated"), 0o600); err != nil {
		t.Fatal(err)
	}
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.Method+" "+request.URL.Path)
		if request.Header.Get("Authorization") != "Bearer token-value" {
			t.Fatalf("missing authorization: %q", request.Header.Get("Authorization"))
		}
		if strings.Contains(strings.ToLower(request.URL.Path), "dsl") || strings.Contains(strings.ToLower(request.URL.Path), "source_map") {
			t.Fatalf("CLI reached forbidden artifact path: %s", request.URL.Path)
		}
		switch request.Method + " " + request.URL.Path {
		case "POST /v1/projects/" + project + "/workflows":
			writeCLIJSON(t, writer, http.StatusCreated, map[string]any{
				"workflow":       completeWorkflowResponse(project, workflow, "Loop"),
				"draft_revision": completeDraftRevisionResponse(project, workflow, 1, validCLIIR("Initial")),
			})
		case "GET /v1/projects/" + project + "/workflows/" + workflow + "/draft":
			writeCLIJSON(t, writer, http.StatusOK, map[string]any{
				"project_id":       project,
				"workflow_id":      workflow,
				"current_revision": 1,
				"state_version":    1,
				"current":          completeDraftRevisionResponse(project, workflow, 1, validCLIIR("Initial")),
			})
		case "PUT /v1/projects/" + project + "/workflows/" + workflow + "/draft":
			writeCLIJSON(t, writer, http.StatusCreated, completeDraftRevisionResponse(project, workflow, 2, validCLIIR("Updated")))
		case "POST /v1/projects/" + project + "/workflows/" + workflow + "/draft/validate":
			writeCLIJSON(t, writer, http.StatusOK, map[string]any{"valid": true, "diagnostics": []any{}})
		case "POST /v1/projects/" + project + "/workflows/" + workflow + "/draft/test":
			writeCLIJSON(t, writer, http.StatusCreated, map[string]any{"run_id": "test-run", "purpose": "test", "state": "pending", "created_at": time.Now().UTC()})
		case "POST /v1/projects/" + project + "/workflows/" + workflow + "/publish":
			writeCLIJSON(t, writer, http.StatusCreated, map[string]any{"version": completePublishedVersionResponse(project, workflow, 1)})
		case "POST /v1/projects/" + project + "/workflows/" + workflow + "/runs":
			writeCLIJSON(t, writer, http.StatusCreated, map[string]any{"run_id": "production-run", "purpose": "production", "state": "pending", "created_at": time.Now().UTC()})
		case "GET /v1/projects/" + project + "/runs/production-run":
			writeCLIJSON(t, writer, http.StatusOK, map[string]any{"run_id": "production-run", "project_id": project, "workflow_id": workflow, "purpose": "production", "state": "running", "state_version": 2, "snapshot_id": "snapshot", "deadline_at": time.Now().UTC(), "created_at": time.Now().UTC(), "updated_at": time.Now().UTC(), "cancel_requested": false, "nodes": []any{}})
		case "GET /v1/projects/" + project + "/runs/production-run/diagnostics":
			writeCLIJSON(t, writer, http.StatusOK, map[string]any{"run": map[string]any{"run_id": "production-run"}, "attempts": []any{}, "audit": []any{}})
		case "POST /v1/projects/" + project + "/runs/production-run/cancel":
			writeCLIJSON(t, writer, http.StatusAccepted, map[string]any{"accepted": true, "run": map[string]any{"run_id": "production-run", "purpose": "production", "state": "running", "created_at": time.Now().UTC()}})
		case "POST /v1/projects/" + project + "/runs/production-run/replay":
			writeCLIJSON(t, writer, http.StatusAccepted, map[string]any{"accepted": true})
		case "POST /v1/projects/" + project + "/workflows:copy":
			writeCLIJSON(t, writer, http.StatusCreated, map[string]any{
				"workflow":       completeWorkflowResponse(project, copyWorkflow, "Copied"),
				"draft_revision": completeDraftRevisionResponse(project, copyWorkflow, 1, validCLIIR("Copied")),
			})
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()
	base := []string{"--server", server.URL, "--token", "token-value", "--project", project}
	runCLI := func(arguments ...string) {
		var output bytes.Buffer
		app := App{Output: &output, Error: &output, Home: root}
		if code := app.Run(context.Background(), arguments); code != 0 {
			t.Fatalf("command %q code=%d output=%s", arguments, code, output.String())
		}
	}
	runCLI(append([]string{"workflow", "create", "--name", "Loop", "--ir", initialIR, "--idempotency-key", "m10-create-0001"}, base...)...)
	runCLI(append([]string{"workflow", "pull", "--workflow", workflow}, base...)...)
	runCLI(append([]string{"draft", "push", "--workflow", workflow, "--ir", updatedIR, "--idempotency-key", "m10-push-0001"}, base...)...)
	runCLI(append([]string{"draft", "validate", "--workflow", workflow}, base...)...)
	deadline := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	runCLI(append([]string{"run", "test", "--workflow", workflow, "--input", input, "--deadline", deadline, "--idempotency-key", "m10-test-0001"}, base...)...)
	runCLI(append([]string{"publish", "--workflow", workflow, "--change-log", "M10", "--idempotency-key", "m10-publish-0001"}, base...)...)
	runCLI(append([]string{"run", "create", "--workflow", workflow, "--input", input, "--deadline", deadline, "--idempotency-key", "m10-run-0001"}, base...)...)
	runCLI(append([]string{"run", "status", "--run", "production-run"}, base...)...)
	runCLI(append([]string{"run", "diagnose", "--run", "production-run"}, base...)...)
	runCLI(append([]string{"run", "cancel", "--run", "production-run"}, base...)...)
	runCLI(append([]string{"run", "replay", "--run", "production-run", "--event-type", "attempt.lost", "--aggregate-id", "attempt-id"}, base...)...)
	runCLI(append([]string{"workflow", "copy", "--source-workflow", workflow, "--version", "1", "--name", "Copied", "--idempotency-key", "m10-copy-0001"}, base...)...)
	if len(paths) != 12 {
		t.Fatalf("paths=%v", paths)
	}
}

func validCLIIR(title string) []byte {
	return []byte(`{"ir_version":"1","nodes":[{"id":"start","type":"start","title":"Start","inputs":[],"outputs":[{"name":"workflow_input","data_type":"object"}]},{"id":"end","type":"end","title":"` + title + `","inputs":[{"name":"workflow_output","data_type":"object","source":"literal","value":{}}],"outputs":[]}],"edges":[{"id":"start_to_end","source":"start","target":"end"}],"layout":{"start":{"x":0,"y":0},"end":{"x":100,"y":0}}}`)
}

func writeCLIJSON(t *testing.T, writer http.ResponseWriter, status int, value any) {
	t.Helper()
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Fatal(err)
	}
}
