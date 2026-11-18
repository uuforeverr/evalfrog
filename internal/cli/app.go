package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/platform/buildinfo"
	"github.com/uu999/evalfrog/internal/platform/config"
)

type App struct {
	Output io.Writer
	Error  io.Writer
	HTTP   *http.Client
	Home   string
	Env    func(string) string
}

func (app App) Run(ctx context.Context, arguments []string) int {
	if len(arguments) == 0 {
		app.usage()
		return 2
	}
	switch arguments[0] {
	case "version":
		value, _ := json.Marshal(buildinfo.Current())
		fmt.Fprintln(app.Output, string(value))
		return 0
	case "config":
		return app.config(arguments[1:])
	case "doctor":
		return app.doctor(ctx, arguments[1:])
	case "workflow":
		return app.workflow(ctx, arguments[1:])
	case "draft":
		return app.draft(ctx, arguments[1:])
	case "publish":
		return app.publish(ctx, arguments[1:])
	case "run":
		return app.run(ctx, arguments[1:])
	case "node-type":
		return app.nodeType(ctx, arguments[1:])
	case "connection":
		return app.connection(ctx, arguments[1:])
	default:
		fmt.Fprintf(app.Error, "unknown command %q\n", arguments[0])
		app.usage()
		return 2
	}
}

type commonFlags struct {
	server  string
	token   string
	project string
}

func (app App) common(flags *flag.FlagSet, requiredProject bool) *commonFlags {
	value := &commonFlags{}
	flags.StringVar(&value.server, "server", app.environment("EF_SERVER"), "EvalFrog Control Plane base URL")
	flags.StringVar(&value.token, "token", app.environment("EF_TOKEN"), "bearer API token")
	if requiredProject {
		flags.StringVar(&value.project, "project", app.environment("EF_PROJECT"), "project UUID")
	}
	return value
}

func (app App) environment(name string) string {
	if app.Env != nil {
		return app.Env(name)
	}
	return os.Getenv(name)
}

func (app App) api(value *commonFlags) (*apiClient, bool) {
	client, err := newAPIClient(value.server, value.token, app.HTTP)
	if err != nil {
		fmt.Fprintln(app.Error, err)
		return nil, false
	}
	if value.project == "" {
		fmt.Fprintln(app.Error, "--project is required")
		return nil, false
	}
	return client, true
}

func (app App) workspaceRoot() string {
	if app.Home != "" {
		return app.Home
	}
	if value, err := os.UserHomeDir(); err == nil {
		return filepath.Join(value, ".evalfrog")
	}
	return ".evalfrog"
}

func (app App) workflow(ctx context.Context, arguments []string) int {
	if len(arguments) == 0 {
		fmt.Fprintln(app.Error, "usage: evalfrog workflow <create|pull|copy|builder> ...")
		return 2
	}
	switch arguments[0] {
	case "builder":
		return app.builder(ctx, arguments[1:])
	case "create":
		flags := flag.NewFlagSet("workflow create", flag.ContinueOnError)
		flags.SetOutput(app.Error)
		common := app.common(flags, true)
		name := flags.String("name", "", "workflow name")
		irFile := flags.String("ir", "", "IR JSON file")
		key := flags.String("idempotency-key", "", "idempotency key")
		if flags.Parse(arguments[1:]) != nil {
			return 2
		}
		client, ok := app.api(common)
		if !ok || *name == "" || *irFile == "" || *key == "" {
			if ok {
				fmt.Fprintln(app.Error, "--name, --ir and --idempotency-key are required")
			}
			return 2
		}
		raw, err := os.ReadFile(*irFile)
		if err != nil {
			fmt.Fprintln(app.Error, err)
			return 1
		}
		if code := app.localValidate(raw); code != 0 {
			return code
		}
		var response struct {
			Workflow apiWorkflowResponse      `json:"workflow"`
			Draft    apiDraftRevisionResponse `json:"draft_revision"`
		}
		err = client.request(ctx, http.MethodPost, "/v1/projects/"+common.project+"/workflows", *key, map[string]any{"name": *name, "ir": json.RawMessage(raw)}, &response)
		if app.reportAPIError(err) {
			return 1
		}
		if err = saveWorkspace(app.workspaceRoot(), Workspace{Server: common.server, ProjectID: common.project, WorkflowID: response.Workflow.ID, Revision: response.Draft.Revision, IR: response.Draft.IR}); err != nil {
			fmt.Fprintln(app.Error, err)
			return 1
		}
		fmt.Fprintf(app.Output, "workflow created: %s revision=%d\n", response.Workflow.ID, response.Draft.Revision)
		return 0
	case "pull":
		flags := flag.NewFlagSet("workflow pull", flag.ContinueOnError)
		flags.SetOutput(app.Error)
		common := app.common(flags, true)
		workflowID := flags.String("workflow", "", "workflow UUID")
		if flags.Parse(arguments[1:]) != nil {
			return 2
		}
		client, ok := app.api(common)
		if !ok || *workflowID == "" {
			if ok {
				fmt.Fprintln(app.Error, "--workflow is required")
			}
			return 2
		}
		var response apiDraftResponse
		err := client.request(ctx, http.MethodGet, "/v1/projects/"+common.project+"/workflows/"+*workflowID+"/draft", "", nil, &response)
		if app.reportAPIError(err) {
			return 1
		}
		if err = saveWorkspace(app.workspaceRoot(), Workspace{Server: common.server, ProjectID: common.project, WorkflowID: *workflowID, Revision: response.Current.Revision, IR: response.Current.IR}); err != nil {
			fmt.Fprintln(app.Error, err)
			return 1
		}
		fmt.Fprintf(app.Output, "workspace updated: workflow=%s revision=%d\n", *workflowID, response.Current.Revision)
		return 0
	case "copy":
		flags := flag.NewFlagSet("workflow copy", flag.ContinueOnError)
		flags.SetOutput(app.Error)
		common := app.common(flags, true)
		source := flags.String("source-workflow", "", "source workflow UUID")
		version := flags.Int64("version", 0, "published version")
		name := flags.String("name", "", "new workflow name")
		key := flags.String("idempotency-key", "", "idempotency key")
		if flags.Parse(arguments[1:]) != nil {
			return 2
		}
		client, ok := app.api(common)
		if !ok || *source == "" || *version < 1 || *name == "" || *key == "" {
			if ok {
				fmt.Fprintln(app.Error, "--source-workflow, --version, --name and --idempotency-key are required")
			}
			return 2
		}
		var response struct {
			Workflow apiWorkflowResponse      `json:"workflow"`
			Draft    apiDraftRevisionResponse `json:"draft_revision"`
		}
		err := client.request(ctx, http.MethodPost, "/v1/projects/"+common.project+"/workflows:copy", *key, map[string]any{"source_workflow_id": *source, "source_version_number": *version, "name": *name}, &response)
		if app.reportAPIError(err) {
			return 1
		}
		if err = saveWorkspace(app.workspaceRoot(), Workspace{Server: common.server, ProjectID: common.project, WorkflowID: response.Workflow.ID, Revision: response.Draft.Revision, IR: response.Draft.IR}); err != nil {
			fmt.Fprintln(app.Error, err)
			return 1
		}
		fmt.Fprintf(app.Output, "workflow copied: %s revision=%d\n", response.Workflow.ID, response.Draft.Revision)
		return 0
	default:
		fmt.Fprintln(app.Error, "usage: evalfrog workflow <create|pull|copy|builder> ...")
		return 2
	}
}

func (app App) draft(ctx context.Context, arguments []string) int {
	if len(arguments) == 0 {
		fmt.Fprintln(app.Error, "usage: evalfrog draft <push|validate> ...")
		return 2
	}
	switch arguments[0] {
	case "push":
		flags := flag.NewFlagSet("draft push", flag.ContinueOnError)
		flags.SetOutput(app.Error)
		common := app.common(flags, true)
		workflowID := flags.String("workflow", "", "workflow UUID")
		irFile := flags.String("ir", "", "IR JSON file")
		key := flags.String("idempotency-key", "", "idempotency key")
		if flags.Parse(arguments[1:]) != nil {
			return 2
		}
		client, ok := app.api(common)
		if !ok || *workflowID == "" || *irFile == "" || *key == "" {
			if ok {
				fmt.Fprintln(app.Error, "--workflow, --ir and --idempotency-key are required")
			}
			return 2
		}
		workspace, err := loadWorkspace(app.workspaceRoot(), common.project, *workflowID)
		if err != nil {
			fmt.Fprintf(app.Error, "pull workflow first: %v\n", err)
			return 1
		}
		raw, err := os.ReadFile(*irFile)
		if err != nil {
			fmt.Fprintln(app.Error, err)
			return 1
		}
		if code := app.localValidate(raw); code != 0 {
			return code
		}
		var response apiDraftRevisionResponse
		err = client.request(ctx, http.MethodPut, "/v1/projects/"+common.project+"/workflows/"+*workflowID+"/draft", *key, map[string]any{"expected_revision": workspace.Revision, "ir": json.RawMessage(raw)}, &response)
		if app.reportAPIError(err) {
			return 1
		}
		if err = saveWorkspace(app.workspaceRoot(), Workspace{Server: common.server, ProjectID: common.project, WorkflowID: *workflowID, Revision: response.Revision, IR: response.IR}); err != nil {
			fmt.Fprintln(app.Error, err)
			return 1
		}
		fmt.Fprintf(app.Output, "draft saved: revision=%d\n", response.Revision)
		return 0
	case "validate":
		flags := flag.NewFlagSet("draft validate", flag.ContinueOnError)
		flags.SetOutput(app.Error)
		common := app.common(flags, true)
		workflowID := flags.String("workflow", "", "workflow UUID")
		if flags.Parse(arguments[1:]) != nil {
			return 2
		}
		client, ok := app.api(common)
		if !ok || *workflowID == "" {
			if ok {
				fmt.Fprintln(app.Error, "--workflow is required")
			}
			return 2
		}
		workspace, err := loadWorkspace(app.workspaceRoot(), common.project, *workflowID)
		if err != nil {
			fmt.Fprintf(app.Error, "pull workflow first: %v\n", err)
			return 1
		}
		var response struct {
			Valid       bool  `json:"valid"`
			Diagnostics []any `json:"diagnostics"`
		}
		err = client.request(ctx, http.MethodPost, "/v1/projects/"+common.project+"/workflows/"+*workflowID+"/draft/validate", "", map[string]any{"revision": workspace.Revision}, &response)
		if app.reportAPIError(err) {
			return 1
		}
		encoded, _ := json.Marshal(response.Diagnostics)
		fmt.Fprintf(app.Output, "valid=%t diagnostics=%s\n", response.Valid, encoded)
		if !response.Valid {
			return 1
		}
		return 0
	default:
		fmt.Fprintln(app.Error, "usage: evalfrog draft <push|validate> ...")
		return 2
	}
}

func (app App) publish(ctx context.Context, arguments []string) int {
	flags := flag.NewFlagSet("publish", flag.ContinueOnError)
	flags.SetOutput(app.Error)
	common := app.common(flags, true)
	workflowID := flags.String("workflow", "", "workflow UUID")
	changeLog := flags.String("change-log", "", "publication change log")
	key := flags.String("idempotency-key", "", "idempotency key")
	if flags.Parse(arguments) != nil {
		return 2
	}
	client, ok := app.api(common)
	if !ok || *workflowID == "" || *key == "" {
		if ok {
			fmt.Fprintln(app.Error, "--workflow and --idempotency-key are required")
		}
		return 2
	}
	workspace, err := loadWorkspace(app.workspaceRoot(), common.project, *workflowID)
	if err != nil {
		fmt.Fprintf(app.Error, "pull workflow first: %v\n", err)
		return 1
	}
	var response struct {
		Version apiPublishedVersionResponse `json:"version"`
	}
	err = client.request(ctx, http.MethodPost, "/v1/projects/"+common.project+"/workflows/"+*workflowID+"/publish", *key, map[string]any{"expected_revision": workspace.Revision, "change_log": *changeLog}, &response)
	if app.reportAPIError(err) {
		return 1
	}
	fmt.Fprintf(app.Output, "workflow published and activated: version=%d\n", response.Version.Number)
	return 0
}

func (app App) run(ctx context.Context, arguments []string) int {
	if len(arguments) == 0 {
		fmt.Fprintln(app.Error, "usage: evalfrog run <test|create|status|diagnose|cancel|replay> ...")
		return 2
	}
	switch arguments[0] {
	case "test", "create":
		flags := flag.NewFlagSet("run "+arguments[0], flag.ContinueOnError)
		flags.SetOutput(app.Error)
		common := app.common(flags, true)
		workflowID := flags.String("workflow", "", "workflow UUID")
		inputFile := flags.String("input", "", "JSON object input file")
		deadline := flags.String("deadline", "", "RFC3339 deadline")
		key := flags.String("idempotency-key", "", "idempotency key")
		if flags.Parse(arguments[1:]) != nil {
			return 2
		}
		client, ok := app.api(common)
		if !ok || *workflowID == "" || *inputFile == "" || *deadline == "" || *key == "" {
			if ok {
				fmt.Fprintln(app.Error, "--workflow, --input, --deadline and --idempotency-key are required")
			}
			return 2
		}
		input, err := os.ReadFile(*inputFile)
		if err != nil {
			fmt.Fprintln(app.Error, err)
			return 1
		}
		if !isJSONObject(input) {
			fmt.Fprintln(app.Error, "input must be a JSON object")
			return 2
		}
		deadlineAt, err := time.Parse(time.RFC3339, *deadline)
		if err != nil {
			fmt.Fprintln(app.Error, "--deadline must be RFC3339")
			return 2
		}
		path := "/v1/projects/" + common.project + "/workflows/" + *workflowID + "/runs"
		body := map[string]any{"input": json.RawMessage(input), "deadline_at": deadlineAt}
		if arguments[0] == "test" {
			workspace, loadErr := loadWorkspace(app.workspaceRoot(), common.project, *workflowID)
			if loadErr != nil {
				fmt.Fprintf(app.Error, "pull workflow first: %v\n", loadErr)
				return 1
			}
			path = "/v1/projects/" + common.project + "/workflows/" + *workflowID + "/draft/test"
			body["revision"] = workspace.Revision
		}
		var response struct {
			RunID     string    `json:"run_id"`
			State     string    `json:"state"`
			Purpose   string    `json:"purpose"`
			CreatedAt time.Time `json:"created_at"`
		}
		err = client.request(ctx, http.MethodPost, path, *key, body, &response)
		if app.reportAPIError(err) {
			return 1
		}
		fmt.Fprintf(app.Output, "run created: %s state=%s\n", response.RunID, response.State)
		return 0
	case "status", "diagnose":
		flags := flag.NewFlagSet("run "+arguments[0], flag.ContinueOnError)
		flags.SetOutput(app.Error)
		common := app.common(flags, true)
		runID := flags.String("run", "", "run UUID")
		if flags.Parse(arguments[1:]) != nil {
			return 2
		}
		client, ok := app.api(common)
		if !ok || *runID == "" {
			if ok {
				fmt.Fprintln(app.Error, "--run is required")
			}
			return 2
		}
		var response any
		path := "/v1/projects/" + common.project + "/runs/" + *runID
		if arguments[0] == "diagnose" {
			path += "/diagnostics"
		}
		err := client.request(ctx, http.MethodGet, path, "", nil, &response)
		if app.reportAPIError(err) {
			return 1
		}
		encoded, _ := json.Marshal(response)
		fmt.Fprintln(app.Output, string(encoded))
		return 0
	case "cancel":
		flags := flag.NewFlagSet("run cancel", flag.ContinueOnError)
		flags.SetOutput(app.Error)
		common := app.common(flags, true)
		runID := flags.String("run", "", "run UUID")
		if flags.Parse(arguments[1:]) != nil {
			return 2
		}
		client, ok := app.api(common)
		if !ok || *runID == "" {
			if ok {
				fmt.Fprintln(app.Error, "--run is required")
			}
			return 2
		}
		var response any
		err := client.request(ctx, http.MethodPost, "/v1/projects/"+common.project+"/runs/"+*runID+"/cancel", "", nil, &response)
		if app.reportAPIError(err) {
			return 1
		}
		encoded, _ := json.Marshal(response)
		fmt.Fprintln(app.Output, string(encoded))
		return 0
	case "replay":
		flags := flag.NewFlagSet("run replay", flag.ContinueOnError)
		flags.SetOutput(app.Error)
		common := app.common(flags, true)
		runID := flags.String("run", "", "run UUID")
		eventType := flags.String("event-type", "", "current runtime event type")
		aggregateID := flags.String("aggregate-id", "", "run or attempt UUID")
		if flags.Parse(arguments[1:]) != nil {
			return 2
		}
		client, ok := app.api(common)
		if !ok || *runID == "" || *eventType == "" || *aggregateID == "" {
			if ok {
				fmt.Fprintln(app.Error, "--run, --event-type and --aggregate-id are required")
			}
			return 2
		}
		var response any
		err := client.request(ctx, http.MethodPost, "/v1/projects/"+common.project+"/runs/"+*runID+"/replay", "", map[string]string{
			"event_type": *eventType, "aggregate_id": *aggregateID,
		}, &response)
		if app.reportAPIError(err) {
			return 1
		}
		encoded, _ := json.Marshal(response)
		fmt.Fprintln(app.Output, string(encoded))
		return 0
	default:
		fmt.Fprintln(app.Error, "usage: evalfrog run <test|create|status|diagnose|cancel|replay> ...")
		return 2
	}
}

func (app App) nodeType(ctx context.Context, arguments []string) int {
	if len(arguments) == 0 || arguments[0] != "list" {
		fmt.Fprintln(app.Error, "usage: evalfrog node-type list --server URL --token TOKEN --project ID")
		return 2
	}
	flags := flag.NewFlagSet("node-type list", flag.ContinueOnError)
	flags.SetOutput(app.Error)
	common := app.common(flags, true)
	if flags.Parse(arguments[1:]) != nil {
		return 2
	}
	client, ok := app.api(common)
	if !ok {
		return 2
	}
	var response any
	err := client.request(ctx, http.MethodGet, "/v1/node-types", "", nil, &response)
	if app.reportAPIError(err) {
		return 1
	}
	encoded, _ := json.Marshal(response)
	fmt.Fprintln(app.Output, string(encoded))
	return 0
}

func (app App) connection(ctx context.Context, arguments []string) int {
	if len(arguments) == 0 || arguments[0] != "list" {
		fmt.Fprintln(app.Error, "usage: evalfrog connection list --server URL --token TOKEN --project ID")
		return 2
	}
	flags := flag.NewFlagSet("connection list", flag.ContinueOnError)
	flags.SetOutput(app.Error)
	common := app.common(flags, true)
	if flags.Parse(arguments[1:]) != nil {
		return 2
	}
	client, ok := app.api(common)
	if !ok {
		return 2
	}
	var response any
	err := client.request(ctx, http.MethodGet, "/v1/projects/"+common.project+"/connections", "", nil, &response)
	if app.reportAPIError(err) {
		return 1
	}
	encoded, _ := json.Marshal(response)
	fmt.Fprintln(app.Output, string(encoded))
	return 0
}

func (app App) localValidate(raw []byte) int {
	_, diagnostics := ir.DefaultParser().ParseDraft(raw)
	if ir.HasErrors(diagnostics) {
		encoded, _ := json.Marshal(diagnostics)
		fmt.Fprintf(app.Error, "local IR validation failed: %s\n", encoded)
		return 1
	}
	return 0
}

func isJSONObject(raw []byte) bool {
	var value map[string]any
	return json.Unmarshal(raw, &value) == nil
}
func (app App) reportAPIError(err error) bool {
	if err == nil {
		return false
	}
	if api, ok := err.(*apiError); ok && api.Code == "DRAFT_REVISION_CONFLICT" {
		fmt.Fprintf(app.Error, "draft revision conflict: pull the latest Draft before retrying (details=%v)\n", api.Details)
		return true
	}
	fmt.Fprintln(app.Error, err)
	return true
}

func (app App) config(arguments []string) int {
	if len(arguments) == 0 || arguments[0] != "validate" {
		fmt.Fprintln(app.Error, "usage: evalfrog config validate [--profile PROFILE] [--config-dir DIR]")
		return 2
	}
	flags := flag.NewFlagSet("config validate", flag.ContinueOnError)
	flags.SetOutput(app.Error)
	profile := flags.String("profile", "", "configuration profile")
	directory := flags.String("config-dir", "", "configuration directory")
	if err := flags.Parse(arguments[1:]); err != nil {
		return 2
	}
	value, err := config.Load(config.LoadOptions{Directory: *directory, Profile: *profile})
	if err != nil {
		fmt.Fprintln(app.Error, err)
		return 1
	}
	fmt.Fprintf(app.Output, "configuration valid: profile=%s namespace=%s\n", value.Profile, value.Namespace)
	return 0
}

func (app App) doctor(ctx context.Context, arguments []string) int {
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	flags.SetOutput(app.Error)
	profile := flags.String("profile", "", "configuration profile")
	directory := flags.String("config-dir", "", "configuration directory")
	if err := flags.Parse(arguments); err != nil {
		return 2
	}
	value, err := config.Load(config.LoadOptions{Directory: *directory, Profile: *profile})
	if err != nil {
		fmt.Fprintln(app.Error, err)
		return 1
	}
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, strings.TrimRight(value.Endpoints.ControlPlaneURL, "/")+"/health/ready", nil)
	if err != nil {
		fmt.Fprintln(app.Error, err)
		return 1
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		fmt.Fprintln(app.Error, err)
		return 1
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		fmt.Fprintf(app.Error, "control plane is not ready: %s\n", response.Status)
		return 1
	}
	fmt.Fprintln(app.Output, "EvalFrog local stack is ready")
	return 0
}

func (app App) usage() {
	fmt.Fprintln(app.Error, "usage: evalfrog <workflow|draft|publish|run|node-type|connection|version|config validate|doctor>")
	fmt.Fprintln(app.Error, "workflow supports create|pull|copy|builder")
}
