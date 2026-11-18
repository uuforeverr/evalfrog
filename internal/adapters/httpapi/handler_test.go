package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/catalog"
	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/projection"
	"github.com/uu999/evalfrog/internal/resources"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/workflowapp"
)

type authenticatorStub struct{}

func (authenticatorStub) Authenticate(_ context.Context, token string) (access.Principal, error) {
	if token != "0123456789abcdef" {
		return access.Principal{}, access.ErrUnauthenticated
	}
	return access.Principal{ID: "principal-1", Kind: access.PrincipalServiceAccount}, nil
}

type applicationStub struct {
	createCalled  bool
	getError      error
	saveError     error
	createRunErr  error
	run           projection.RunView
	cancelApplied bool
	cancelCalled  bool
	replayCalled  bool
	versions      []projection.RunVersion
	versionCalls  int
}

func (stub *applicationStub) CreateWorkflow(context.Context, definition.CreateWorkflowCommand) (definition.Workflow, definition.DraftRevision, []ir.Diagnostic, error) {
	stub.createCalled = true
	return definition.Workflow{ID: "workflow-1"}, definition.DraftRevision{RevisionNumber: 1}, nil, nil
}
func (stub *applicationStub) GetWorkflow(context.Context, string, string, string) (definition.Workflow, error) {
	return definition.Workflow{}, stub.getError
}
func (*applicationStub) GetDraft(context.Context, string, string, string) (definition.Draft, error) {
	return definition.Draft{}, nil
}
func (stub *applicationStub) SaveDraft(context.Context, definition.SaveDraftCommand) (definition.DraftRevision, []ir.Diagnostic, error) {
	return definition.DraftRevision{}, nil, stub.saveError
}
func (*applicationStub) ValidateDraft(context.Context, string, string, string, int64) ([]ir.Diagnostic, error) {
	return nil, nil
}
func (*applicationStub) CompileDraftTestSnapshot(context.Context, string, string, string, int64) (definition.ExecutionSnapshot, []ir.Diagnostic, error) {
	return definition.ExecutionSnapshot{}, nil, nil
}
func (*applicationStub) Publish(context.Context, definition.PublishCommand) (definition.PublishedVersion, definition.ExecutionSnapshot, []ir.Diagnostic, error) {
	return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, nil, nil
}
func (*applicationStub) Rollback(context.Context, string, string, string, int64) (definition.PublishedVersion, error) {
	return definition.PublishedVersion{}, nil
}
func (*applicationStub) CopyPublishedVersion(context.Context, definition.CopyCommand) (definition.Workflow, definition.DraftRevision, error) {
	return definition.Workflow{}, definition.DraftRevision{}, nil
}
func (*applicationStub) TestDraft(context.Context, workflowapp.TestDraftCommand) (runtime.WorkflowRunRecord, []ir.Diagnostic, error) {
	return runtime.WorkflowRunRecord{ID: "run-test", Purpose: runtime.RunPurposeTest}, nil, nil
}
func (stub *applicationStub) CreateRun(context.Context, workflowapp.CreateRunCommand) (runtime.WorkflowRunRecord, error) {
	if stub.createRunErr != nil {
		return runtime.WorkflowRunRecord{}, stub.createRunErr
	}
	return runtime.WorkflowRunRecord{ID: "run-production", Purpose: runtime.RunPurposeProduction}, nil
}
func (stub *applicationStub) CancelRun(context.Context, string, string, string, string) (runtime.WorkflowRunRecord, bool, error) {
	stub.cancelCalled = true
	return runtime.WorkflowRunRecord{ID: "run-1"}, stub.cancelApplied, nil
}
func (stub *applicationStub) GetRun(context.Context, string, string, string) (projection.RunView, error) {
	return stub.run, stub.getError
}
func (stub *applicationStub) GetRunVersion(context.Context, string, string, string) (projection.RunVersion, error) {
	if len(stub.versions) > 0 {
		index := min(stub.versionCalls, len(stub.versions)-1)
		stub.versionCalls++
		return stub.versions[index], stub.getError
	}
	return projection.RunVersion{State: stub.run.State, StateVersion: stub.run.StateVersion}, stub.getError
}
func (stub *applicationStub) GetDiagnostics(context.Context, string, string, string) (projection.DiagnosticView, error) {
	return projection.DiagnosticView{Run: stub.run}, stub.getError
}
func (stub *applicationStub) ReplayRun(context.Context, string, string, string, string, string, string) (bool, error) {
	stub.replayCalled = true
	return true, nil
}
func (*applicationStub) NodeTypes() []catalog.NodeDescription { return nil }
func (*applicationStub) NodeTypesForRevision(revision string) ([]catalog.NodeDescription, bool) {
	return catalog.BuiltinDirectory().DescriptionsForRevision(revision)
}
func (*applicationStub) Connections(context.Context, string, string) ([]resources.ConnectionSummary, error) {
	return nil, nil
}

func TestAuthoringAPIRejectsClientDSLAndSourceMap(t *testing.T) {
	for _, forbidden := range []string{"dsl", "source_map"} {
		application := &applicationStub{}
		handler := New(authenticatorStub{}, application)
		body := `{"name":"frog","ir":{},"` + forbidden + `":{}}`
		request := httptest.NewRequest(http.MethodPost, "/v1/projects/project-1/workflows", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer 0123456789abcdef")
		request.Header.Set("Idempotency-Key", "create-frog-0001")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || application.createCalled {
			t.Fatalf("field %s status=%d create_called=%v body=%s", forbidden, response.Code, application.createCalled, response.Body.String())
		}
	}
}

func TestNodeTypesResolveImmutableCatalogRevision(t *testing.T) {
	handler := New(authenticatorStub{}, &applicationStub{})
	for _, test := range []struct {
		revision string
		status   int
		mode     catalog.OutputMode
	}{
		{"catalog-v1", http.StatusOK, catalog.OutputFixed},
		{"catalog-v2", http.StatusOK, catalog.OutputAuthorDeclared},
		{"catalog-missing", http.StatusNotFound, ""},
	} {
		request := httptest.NewRequest(http.MethodGet, "/v1/node-types?catalog_revision="+test.revision, nil)
		request.Header.Set("Authorization", "Bearer 0123456789abcdef")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("revision %s status=%d body=%s", test.revision, response.Code, response.Body.String())
		}
		if test.status != http.StatusOK {
			continue
		}
		var body struct {
			NodeTypes []catalog.NodeDescription `json:"node_types"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		for _, node := range body.NodeTypes {
			if node.Type == "start" && node.Outputs.Mode != test.mode {
				t.Fatalf("revision %s Start mode=%s", test.revision, node.Outputs.Mode)
			}
		}
	}
}

func TestThereIsNoDSLUploadRoute(t *testing.T) {
	handler := New(authenticatorStub{}, &applicationStub{})
	request := httptest.NewRequest(http.MethodPost, "/v1/projects/project-1/workflows/workflow-1/dsl", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer 0123456789abcdef")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("DSL upload route status=%d", response.Code)
	}
}

func TestDomainErrorsUseStableHTTPContract(t *testing.T) {
	application := &applicationStub{getError: &definition.Error{Code: definition.CodeDraftRevisionConflict, Message: "draft revision has changed", Cause: definition.ErrDraftRevisionConflict}}
	handler := New(authenticatorStub{}, application)
	request := httptest.NewRequest(http.MethodGet, "/v1/projects/project-1/workflows/workflow-1", nil)
	request.Header.Set("Authorization", "Bearer 0123456789abcdef")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != definition.CodeDraftRevisionConflict || envelope.Error.RequestID == "" {
		t.Fatalf("unexpected error envelope: %+v", envelope)
	}
}

func TestSaveConflictReturnsThreeWayIR(t *testing.T) {
	base := json.RawMessage(`{"ir_version":"1","nodes":[],"edges":[],"layout":{}}`)
	application := &applicationStub{saveError: &definition.Error{
		Code: definition.CodeDraftRevisionConflict, Message: "draft revision has changed",
		Details: map[string]any{"base_revision": int64(1), "current_revision": int64(2),
			"base_ir": base, "current_ir": base, "submitted_ir": base},
	}}
	handler := New(authenticatorStub{}, application)
	request := httptest.NewRequest(http.MethodPut, "/v1/projects/project-1/workflows/workflow-1/draft", strings.NewReader(`{"expected_revision":1,"ir":{"ir_version":"1"}}`))
	request.Header.Set("Authorization", "Bearer 0123456789abcdef")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Code    string                     `json:"code"`
			Details map[string]json.RawMessage `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != definition.CodeDraftRevisionConflict {
		t.Fatalf("code=%s", envelope.Error.Code)
	}
	for _, key := range []string{"base_ir", "current_ir", "submitted_ir"} {
		if !json.Valid(envelope.Error.Details[key]) {
			t.Fatalf("missing %s: %s", key, response.Body.String())
		}
	}
}

func TestAuthenticationFailsClosed(t *testing.T) {
	handler := New(authenticatorStub{}, &applicationStub{})
	request := httptest.NewRequest(http.MethodGet, "/v1/projects/project-1/workflows/workflow-1", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestRunCommandsExposeDraftTestProductionAndIdempotentCancellation(t *testing.T) {
	application := &applicationStub{run: projection.RunView{RunID: "run-1", ProjectID: "project-1", State: runtime.RunRunning}, cancelApplied: true}
	handler := New(authenticatorStub{}, application)
	for _, test := range []struct {
		name, method, path, body string
		want                     int
	}{
		{"draft test", http.MethodPost, "/v1/projects/project-1/workflows/workflow-1/draft/test", `{"revision":1,"input":{},"deadline_at":"2030-01-01T00:00:00Z"}`, http.StatusCreated},
		{"production", http.MethodPost, "/v1/projects/project-1/workflows/workflow-1/runs", `{"input":{},"deadline_at":"2030-01-01T00:00:00Z"}`, http.StatusCreated},
		{"run view", http.MethodGet, "/v1/projects/project-1/runs/run-1", "", http.StatusOK},
		{"cancel accepted", http.MethodPost, "/v1/projects/project-1/runs/run-1/cancel", "", http.StatusAccepted},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			request.Header.Set("Authorization", "Bearer 0123456789abcdef")
			if test.method == http.MethodPost && strings.Contains(test.path, "/runs") && !strings.Contains(test.path, "cancel") {
				request.Header.Set("Idempotency-Key", "run-command-0001")
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	if !application.cancelCalled {
		t.Fatal("cancel command was not delegated")
	}
}

func TestProductionRunRequiresActivePublishedVersion(t *testing.T) {
	handler := New(authenticatorStub{}, &applicationStub{createRunErr: runtime.ErrRunWorkflowNotPublished})
	request := httptest.NewRequest(http.MethodPost, "/v1/projects/project-1/workflows/workflow-1/runs", strings.NewReader(`{"input":{},"deadline_at":"2030-01-01T00:00:00Z"}`))
	request.Header.Set("Authorization", "Bearer 0123456789abcdef")
	request.Header.Set("Idempotency-Key", "run-command-0001")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), definition.CodeWorkflowNotPublished) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

type updateSubscriberStub struct{ updates <-chan string }

func (stub updateSubscriberStub) SubscribeRunUpdates(context.Context, string) (<-chan string, func()) {
	return stub.updates, func() {}
}

func TestRunEventsAreWakeupsAndNeverCarryRuntimeState(t *testing.T) {
	updates := make(chan string, 1)
	updates <- "run-1"
	close(updates)
	handler := New(authenticatorStub{}, &applicationStub{
		run:      projection.RunView{RunID: "run-1", ProjectID: "project-1", State: runtime.RunRunning},
		versions: []projection.RunVersion{{State: runtime.RunRunning, StateVersion: 1}, {State: runtime.RunFailed, StateVersion: 2}},
	}, updateSubscriberStub{updates: updates})
	request := httptest.NewRequest(http.MethodGet, "/v1/projects/project-1/runs/run-1/events", nil)
	request.Header.Set("Authorization", "Bearer 0123456789abcdef")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Content-Type"), "text/event-stream") || !strings.Contains(body, "event: ready") || !strings.Contains(body, "event: update") || strings.Contains(body, "failed") {
		t.Fatalf("status=%d header=%q body=%q", response.Code, response.Header().Get("Content-Type"), body)
	}
}

func TestRunEventsPollRunVersionAcrossInstances(t *testing.T) {
	updates := make(chan string)
	application := &applicationStub{
		run:      projection.RunView{RunID: "run-1", ProjectID: "project-1", State: runtime.RunRunning},
		versions: []projection.RunVersion{{State: runtime.RunRunning, StateVersion: 3}, {State: runtime.RunSucceeded, StateVersion: 4}},
	}
	handler := New(authenticatorStub{}, application, updateSubscriberStub{updates: updates}).WithRunUpdatePollInterval(time.Millisecond)
	request := httptest.NewRequest(http.MethodGet, "/v1/projects/project-1/runs/run-1/events", nil)
	request.Header.Set("Authorization", "Bearer 0123456789abcdef")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, "event: update") || application.versionCalls < 2 {
		t.Fatalf("status=%d version_calls=%d body=%q", response.Code, application.versionCalls, body)
	}
}

func TestReplayRequestsOnlyAnActionableWakeup(t *testing.T) {
	stub := &applicationStub{}
	handler := New(authenticatorStub{}, stub)
	request := httptest.NewRequest(http.MethodPost, "/v1/projects/project-1/runs/run-1/replay", strings.NewReader(`{"event_type":"attempt.lost","aggregate_id":"attempt-1"}`))
	request.Header.Set("Authorization", "Bearer 0123456789abcdef")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || !stub.replayCalled || !strings.Contains(response.Body.String(), `"accepted":true`) {
		t.Fatalf("status=%d called=%t body=%s", response.Code, stub.replayCalled, response.Body.String())
	}
}
