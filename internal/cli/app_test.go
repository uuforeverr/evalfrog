package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	code := (App{Output: &output, Error: &output}).Run(context.Background(), []string{"version"})
	if code != 0 || !strings.Contains(output.String(), "version") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestCommonFlagsDefaultToEvalFrogEnvironmentAndAllowExplicitOverride(t *testing.T) {
	t.Parallel()
	values := map[string]string{
		"EF_SERVER":  "http://from-environment",
		"EF_PROJECT": "environment-project",
		"EF_TOKEN":   "environment-token",
	}
	app := App{Env: func(name string) string { return values[name] }}
	flags := flag.NewFlagSet("environment", flag.ContinueOnError)
	common := app.common(flags, true)
	if err := flags.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if common.server != values["EF_SERVER"] || common.project != values["EF_PROJECT"] || common.token != values["EF_TOKEN"] {
		t.Fatalf("environment defaults=%+v", common)
	}

	flags = flag.NewFlagSet("override", flag.ContinueOnError)
	common = app.common(flags, true)
	if err := flags.Parse([]string{"--server", "http://explicit", "--project", "explicit-project", "--token", "explicit-token"}); err != nil {
		t.Fatal(err)
	}
	if common.server != "http://explicit" || common.project != "explicit-project" || common.token != "explicit-token" {
		t.Fatalf("explicit flags did not override environment: %+v", common)
	}
}

func TestNodeTypeListUsesExternalAPISurface(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/v1/node-types" || request.Header.Get("Authorization") != "Bearer token-value" {
			t.Fatalf("request=%s %s auth=%q", request.Method, request.URL.Path, request.Header.Get("Authorization"))
		}
		_, _ = writer.Write([]byte(`{"node_types":[]}`))
	}))
	defer server.Close()
	var output bytes.Buffer
	code := (App{Output: &output, Error: &output, Home: t.TempDir()}).Run(context.Background(), []string{"node-type", "list", "--server", server.URL, "--token", "token-value", "--project", "project-id"})
	if code != 0 || !strings.Contains(output.String(), "node_types") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestDraftPushReportsOptimisticConflict(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	project, workflow := "project-id", "workflow-id"
	if err := saveWorkspace(root, Workspace{ProjectID: project, WorkflowID: workflow, Revision: 1, IR: json.RawMessage(`{"ir_version":"1"}`)}); err != nil {
		t.Fatal(err)
	}
	irFile := root + "/draft.json"
	if err := os.WriteFile(irFile, []byte(`{"ir_version":"1","nodes":[],"edges":[],"layout":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"error":{"code":"DRAFT_REVISION_CONFLICT","message":"changed","details":{}}}`))
	}))
	defer server.Close()
	var output bytes.Buffer
	code := (App{Output: &output, Error: &output, Home: root}).Run(context.Background(), []string{"draft", "push", "--server", server.URL, "--token", "token-value", "--project", project, "--workflow", workflow, "--ir", irFile, "--idempotency-key", "draft-push-0001"})
	if code != 1 || !strings.Contains(output.String(), "draft revision conflict") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	directory, err := filepath.Abs(filepath.Join("..", "..", "configs"))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	code := (App{Output: &output, Error: &output}).Run(context.Background(), []string{"config", "validate", "--profile", "local", "--config-dir", directory})
	if code != 0 || !strings.Contains(output.String(), "configuration valid") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestRunDiagnosticsAndReplayUseControlledExternalPaths(t *testing.T) {
	t.Parallel()
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.Method+" "+request.URL.Path)
		if request.Header.Get("Authorization") != "Bearer token-value" {
			t.Fatalf("authorization=%q", request.Header.Get("Authorization"))
		}
		if request.URL.Path == "/v1/projects/project-id/runs/run-id/replay" {
			var body map[string]string
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body["event_type"] != "attempt.lost" || body["aggregate_id"] != "attempt-id" {
				t.Fatalf("replay body=%v err=%v", body, err)
			}
		}
		_, _ = writer.Write([]byte(`{"accepted":true}`))
	}))
	defer server.Close()
	args := []string{"--server", server.URL, "--token", "token-value", "--project", "project-id"}
	for _, command := range [][]string{
		append([]string{"run", "diagnose", "--run", "run-id"}, args...),
		append([]string{"run", "replay", "--run", "run-id", "--event-type", "attempt.lost", "--aggregate-id", "attempt-id"}, args...),
	} {
		var output bytes.Buffer
		if code := (App{Output: &output, Error: &output, Home: t.TempDir()}).Run(context.Background(), command); code != 0 {
			t.Fatalf("command=%v code=%d output=%q", command, code, output.String())
		}
	}
	if strings.Join(paths, ",") != "GET /v1/projects/project-id/runs/run-id/diagnostics,POST /v1/projects/project-id/runs/run-id/replay" {
		t.Fatalf("paths=%v", paths)
	}
}
