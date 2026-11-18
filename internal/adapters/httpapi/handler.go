// Package httpapi adapts the versioned HTTP/JSON command surface to Definition
// application commands. It never accesses MySQL directly.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/catalog"
	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/platform/identity"
	"github.com/uu999/evalfrog/internal/platform/traceid"
	"github.com/uu999/evalfrog/internal/projection"
	"github.com/uu999/evalfrog/internal/resources"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/workflowapp"
)

const maxRequestBytes = 2 << 20

type Authenticator interface {
	Authenticate(context.Context, string) (access.Principal, error)
}

type RunUpdateSubscriber interface {
	SubscribeRunUpdates(context.Context, string) (<-chan string, func())
}

type Application interface {
	CreateWorkflow(context.Context, definition.CreateWorkflowCommand) (definition.Workflow, definition.DraftRevision, []ir.Diagnostic, error)
	GetWorkflow(context.Context, string, string, string) (definition.Workflow, error)
	GetDraft(context.Context, string, string, string) (definition.Draft, error)
	SaveDraft(context.Context, definition.SaveDraftCommand) (definition.DraftRevision, []ir.Diagnostic, error)
	ValidateDraft(context.Context, string, string, string, int64) ([]ir.Diagnostic, error)
	Publish(context.Context, definition.PublishCommand) (definition.PublishedVersion, definition.ExecutionSnapshot, []ir.Diagnostic, error)
	Rollback(context.Context, string, string, string, int64) (definition.PublishedVersion, error)
	CopyPublishedVersion(context.Context, definition.CopyCommand) (definition.Workflow, definition.DraftRevision, error)
	TestDraft(context.Context, workflowapp.TestDraftCommand) (runtime.WorkflowRunRecord, []ir.Diagnostic, error)
	CreateRun(context.Context, workflowapp.CreateRunCommand) (runtime.WorkflowRunRecord, error)
	CancelRun(context.Context, string, string, string, string) (runtime.WorkflowRunRecord, bool, error)
	GetRun(context.Context, string, string, string) (projection.RunView, error)
	GetRunVersion(context.Context, string, string, string) (projection.RunVersion, error)
	GetDiagnostics(context.Context, string, string, string) (projection.DiagnosticView, error)
	ReplayRun(context.Context, string, string, string, string, string, string) (bool, error)
	NodeTypes() []catalog.NodeDescription
	Connections(context.Context, string, string) ([]resources.ConnectionSummary, error)
}

type Handler struct {
	authenticator Authenticator
	application   Application
	ids           identity.Generator
	router        *http.ServeMux
	updates       RunUpdateSubscriber
	runUpdatePoll time.Duration
}

func New(authenticator Authenticator, application Application, updates ...RunUpdateSubscriber) *Handler {
	handler := &Handler{authenticator: authenticator, application: application, ids: identity.UUIDv7Generator{}, router: http.NewServeMux(), runUpdatePoll: 2 * time.Second}
	if len(updates) != 0 {
		handler.updates = updates[0]
	}
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows", handler.createWorkflow)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows:copy", handler.copyWorkflow)
	handler.router.HandleFunc("GET /v1/projects/{project_id}/workflows/{workflow_id}", handler.getWorkflow)
	handler.router.HandleFunc("GET /v1/projects/{project_id}/workflows/{workflow_id}/draft", handler.getDraft)
	handler.router.HandleFunc("PUT /v1/projects/{project_id}/workflows/{workflow_id}/draft", handler.saveDraft)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows/{workflow_id}/draft/validate", handler.validateDraft)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows/{workflow_id}/draft/test", handler.testDraft)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows/{workflow_id}/publish", handler.publish)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows/{workflow_id}/versions/{version_number}/activate", handler.rollback)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows/{workflow_id}/runs", handler.createRun)
	handler.router.HandleFunc("GET /v1/projects/{project_id}/runs/{run_id}", handler.getRun)
	handler.router.HandleFunc("GET /v1/projects/{project_id}/runs/{run_id}/diagnostics", handler.getDiagnostics)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/runs/{run_id}/replay", handler.replayRun)
	handler.router.HandleFunc("GET /v1/projects/{project_id}/runs/{run_id}/events", handler.runEvents)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/runs/{run_id}/cancel", handler.cancelRun)
	handler.router.HandleFunc("GET /v1/node-types", handler.nodeTypes)
	handler.router.HandleFunc("GET /v1/projects/{project_id}/connections", handler.connections)
	return handler
}

func (handler *Handler) WithRunUpdatePollInterval(interval time.Duration) *Handler {
	if interval > 0 {
		handler.runUpdatePoll = interval
	}
	return handler
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	handler.router.ServeHTTP(writer, request)
}

type createWorkflowRequest struct {
	Name string          `json:"name"`
	IR   json.RawMessage `json:"ir"`
}

// runCommandResponse is intentionally smaller than the Runtime persistence
// record. Clients use GET Run for the current projected state; creating or
// cancelling a Run must not leak snapshot/definition implementation details.
type runCommandResponse struct {
	RunID     string             `json:"run_id"`
	Purpose   runtime.RunPurpose `json:"purpose"`
	State     runtime.RunState   `json:"state"`
	CreatedAt time.Time          `json:"created_at"`
}

func newRunCommandResponse(run runtime.WorkflowRunRecord) runCommandResponse {
	return runCommandResponse{RunID: run.ID, Purpose: run.Purpose, State: run.State, CreatedAt: run.CreatedAt}
}

func (handler *Handler) createWorkflow(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	var body createWorkflowRequest
	if !handler.decode(writer, request, &body) {
		return
	}
	workflow, revision, diagnostics, err := handler.application.CreateWorkflow(request.Context(), definition.CreateWorkflowCommand{
		ProjectID: request.PathValue("project_id"), PrincipalID: principal.ID, Name: body.Name,
		IRJSON: body.IR, IdempotencyKey: request.Header.Get("Idempotency-Key"),
	})
	if handler.writeResultError(writer, request, diagnostics, err) {
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"workflow": workflow, "draft_revision": revision})
}

func (handler *Handler) copyWorkflow(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	var body struct {
		SourceWorkflowID    string `json:"source_workflow_id"`
		SourceVersionNumber int64  `json:"source_version_number"`
		Name                string `json:"name"`
	}
	if !handler.decode(writer, request, &body) {
		return
	}
	workflow, revision, err := handler.application.CopyPublishedVersion(request.Context(), definition.CopyCommand{
		ProjectID: request.PathValue("project_id"), PrincipalID: principal.ID,
		SourceWorkflowID: body.SourceWorkflowID, SourceVersionNumber: body.SourceVersionNumber,
		Name: body.Name, IdempotencyKey: request.Header.Get("Idempotency-Key"),
	})
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"workflow": workflow, "draft_revision": revision})
}

func (handler *Handler) getWorkflow(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	workflow, err := handler.application.GetWorkflow(request.Context(), request.PathValue("project_id"), principal.ID, request.PathValue("workflow_id"))
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusOK, workflow)
}

func (handler *Handler) getDraft(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	draft, err := handler.application.GetDraft(request.Context(), request.PathValue("project_id"), principal.ID, request.PathValue("workflow_id"))
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusOK, draft)
}

func (handler *Handler) saveDraft(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	var body struct {
		ExpectedRevision int64           `json:"expected_revision"`
		IR               json.RawMessage `json:"ir"`
	}
	if !handler.decode(writer, request, &body) {
		return
	}
	revision, diagnostics, err := handler.application.SaveDraft(request.Context(), definition.SaveDraftCommand{
		ProjectID: request.PathValue("project_id"), PrincipalID: principal.ID, WorkflowID: request.PathValue("workflow_id"),
		ExpectedRevision: body.ExpectedRevision, IRJSON: body.IR, IdempotencyKey: request.Header.Get("Idempotency-Key"),
	})
	if handler.writeResultError(writer, request, diagnostics, err) {
		return
	}
	writeJSON(writer, http.StatusCreated, revision)
}

func (handler *Handler) validateDraft(writer http.ResponseWriter, request *http.Request) {
	principal, revision, ok := handler.principalAndRevision(writer, request)
	if !ok {
		return
	}
	diagnostics, err := handler.application.ValidateDraft(request.Context(), request.PathValue("project_id"), principal.ID, request.PathValue("workflow_id"), revision)
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"valid": !ir.HasErrors(diagnostics), "diagnostics": diagnostics})
}

func (handler *Handler) testDraft(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	var body struct {
		Revision   int64           `json:"revision"`
		Input      json.RawMessage `json:"input"`
		DeadlineAt time.Time       `json:"deadline_at"`
	}
	if !handler.decode(writer, request, &body) {
		return
	}
	if body.Revision < 1 || len(body.Input) == 0 || body.DeadlineAt.IsZero() {
		handler.writeError(writer, request, http.StatusBadRequest, definition.CodeInvalidArgument, "revision, input and deadline_at are required", nil)
		return
	}
	run, diagnostics, err := handler.application.TestDraft(request.Context(), workflowapp.TestDraftCommand{
		ProjectID: request.PathValue("project_id"), PrincipalID: principal.ID, WorkflowID: request.PathValue("workflow_id"),
		Revision: body.Revision, WorkflowInput: body.Input, DeadlineAt: body.DeadlineAt,
		IdempotencyKey: request.Header.Get("Idempotency-Key"), TraceID: traceid.From(request.Context()),
	})
	if handler.writeResultError(writer, request, diagnostics, err) {
		return
	}
	writeJSON(writer, http.StatusCreated, newRunCommandResponse(run))
}

func (handler *Handler) publish(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	var body struct {
		ExpectedRevision int64  `json:"expected_revision"`
		ChangeLog        string `json:"change_log"`
	}
	if !handler.decode(writer, request, &body) {
		return
	}
	version, _, diagnostics, err := handler.application.Publish(request.Context(), definition.PublishCommand{
		ProjectID: request.PathValue("project_id"), PrincipalID: principal.ID, WorkflowID: request.PathValue("workflow_id"),
		ExpectedRevision: body.ExpectedRevision, ChangeLog: body.ChangeLog, IdempotencyKey: request.Header.Get("Idempotency-Key"),
	})
	if handler.writeResultError(writer, request, diagnostics, err) {
		return
	}
	// DSL and Source Map are immutable server-side execution artifacts. The
	// authoring contract returns only the Published Version; clients continue to
	// author canonical IR and never depend on a reverse compilation boundary.
	writeJSON(writer, http.StatusCreated, map[string]any{"version": version})
}

func (handler *Handler) rollback(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	versionNumber, err := strconv.ParseInt(request.PathValue("version_number"), 10, 64)
	if err != nil || versionNumber < 1 {
		handler.writeError(writer, request, http.StatusBadRequest, definition.CodeInvalidArgument, "version_number must be positive", nil)
		return
	}
	version, err := handler.application.Rollback(request.Context(), request.PathValue("project_id"), principal.ID, request.PathValue("workflow_id"), versionNumber)
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusOK, version)
}

func (handler *Handler) createRun(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	var body struct {
		Input      json.RawMessage `json:"input"`
		DeadlineAt time.Time       `json:"deadline_at"`
	}
	if !handler.decode(writer, request, &body) {
		return
	}
	if len(body.Input) == 0 || body.DeadlineAt.IsZero() {
		handler.writeError(writer, request, http.StatusBadRequest, definition.CodeInvalidArgument, "input and deadline_at are required", nil)
		return
	}
	run, err := handler.application.CreateRun(request.Context(), workflowapp.CreateRunCommand{
		ProjectID: request.PathValue("project_id"), PrincipalID: principal.ID, WorkflowID: request.PathValue("workflow_id"),
		WorkflowInput: body.Input, DeadlineAt: body.DeadlineAt, IdempotencyKey: request.Header.Get("Idempotency-Key"), TraceID: traceid.From(request.Context()),
	})
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusCreated, newRunCommandResponse(run))
}

func (handler *Handler) getRun(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	run, err := handler.application.GetRun(request.Context(), request.PathValue("project_id"), principal.ID, request.PathValue("run_id"))
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusOK, run)
}

func (handler *Handler) getDiagnostics(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	value, err := handler.application.GetDiagnostics(request.Context(), request.PathValue("project_id"), principal.ID, request.PathValue("run_id"))
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

// replayRun never accepts an arbitrary message payload. It asks the authority
// to re-emit one currently actionable Runtime fact, which preserves the Engine
// Inbox/CAS ownership even when an operator is recovering an incident.
func (handler *Handler) replayRun(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	var body struct {
		EventType   string `json:"event_type"`
		AggregateID string `json:"aggregate_id"`
	}
	if !handler.decode(writer, request, &body) {
		return
	}
	if body.EventType == "" || body.AggregateID == "" {
		handler.writeError(writer, request, http.StatusBadRequest, definition.CodeInvalidArgument, "event_type and aggregate_id are required", nil)
		return
	}
	accepted, err := handler.application.ReplayRun(request.Context(), request.PathValue("project_id"), principal.ID,
		request.PathValue("run_id"), body.EventType, body.AggregateID, traceid.From(request.Context()))
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	status := http.StatusOK
	if accepted {
		status = http.StatusAccepted
	}
	writeJSON(writer, status, map[string]any{"accepted": accepted})
}

// runEvents is a lossy SSE wake-up channel. It never carries Runtime state;
// the browser must GET the Run view first and after every notification.
func (handler *Handler) runEvents(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	if handler.updates == nil {
		handler.writeError(writer, request, http.StatusServiceUnavailable, "RUN_UPDATES_UNAVAILABLE", "run updates are temporarily unavailable", nil)
		return
	}
	runID, projectID := request.PathValue("run_id"), request.PathValue("project_id")
	initialVersion, err := handler.application.GetRunVersion(request.Context(), projectID, principal.ID, runID)
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	flusher, supported := writer.(http.Flusher)
	if !supported {
		handler.writeError(writer, request, http.StatusInternalServerError, "STREAMING_UNSUPPORTED", "response streaming is unavailable", nil)
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("X-Accel-Buffering", "no")
	fmt.Fprintf(writer, "event: ready\ndata: {\"run_id\":%q}\n\n", runID)
	flusher.Flush()
	if initialVersion.State.Terminal() {
		return
	}
	messages, closeSubscription := handler.updates.SubscribeRunUpdates(request.Context(), runID)
	defer closeSubscription()
	poll := time.NewTimer(jitterRunUpdatePoll(handler.runUpdatePoll))
	defer poll.Stop()
	knownVersion := initialVersion.StateVersion
	sendChange := func() bool {
		version, versionErr := handler.application.GetRunVersion(request.Context(), projectID, principal.ID, runID)
		if versionErr != nil {
			return false
		}
		if version.StateVersion != knownVersion {
			knownVersion = version.StateVersion
			fmt.Fprintf(writer, "event: update\ndata: {\"run_id\":%q}\n\n", runID)
			flusher.Flush()
		}
		return !version.State.Terminal()
	}
	for {
		select {
		case <-request.Context().Done():
			return
		case _, available := <-messages:
			if !available {
				return
			}
			if !sendChange() {
				return
			}
		case <-poll.C:
			if !sendChange() {
				return
			}
			poll.Reset(jitterRunUpdatePoll(handler.runUpdatePoll))
		}
	}
}

func jitterRunUpdatePoll(base time.Duration) time.Duration {
	spread := base / 10
	if spread <= 0 {
		return base
	}
	return base - spread + time.Duration(rand.Int64N(int64(2*spread)+1))
}

func (handler *Handler) cancelRun(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	run, applied, err := handler.application.CancelRun(request.Context(), request.PathValue("project_id"), principal.ID, request.PathValue("run_id"), traceid.From(request.Context()))
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	status := http.StatusAccepted
	if !applied {
		status = http.StatusOK
	}
	writeJSON(writer, status, map[string]any{"run": newRunCommandResponse(run), "accepted": applied})
}

func (handler *Handler) nodeTypes(writer http.ResponseWriter, request *http.Request) {
	if _, ok := handler.authenticate(writer, request); !ok {
		return
	}
	revision := request.URL.Query().Get("catalog_revision")
	if revision == "" {
		writeJSON(writer, http.StatusOK, map[string]any{"node_types": handler.application.NodeTypes()})
		return
	}
	directory, ok := handler.application.(interface {
		NodeTypesForRevision(string) ([]catalog.NodeDescription, bool)
	})
	if !ok {
		handler.writeError(writer, request, http.StatusNotFound, definition.CodeCatalogUnavailable, "catalog revision is unavailable", nil)
		return
	}
	descriptions, exists := directory.NodeTypesForRevision(revision)
	if !exists {
		handler.writeError(writer, request, http.StatusNotFound, definition.CodeCatalogUnavailable, "catalog revision is unavailable", nil)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"node_types": descriptions, "catalog_revision": revision})
}

func (handler *Handler) connections(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	connections, err := handler.application.Connections(request.Context(), request.PathValue("project_id"), principal.ID)
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"connections": connections})
}

func (handler *Handler) principalAndRevision(writer http.ResponseWriter, request *http.Request) (access.Principal, int64, bool) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return access.Principal{}, 0, false
	}
	var body struct {
		Revision int64 `json:"revision"`
	}
	if !handler.decode(writer, request, &body) {
		return access.Principal{}, 0, false
	}
	if body.Revision < 1 {
		handler.writeError(writer, request, http.StatusBadRequest, definition.CodeInvalidArgument, "revision must be positive", nil)
		return access.Principal{}, 0, false
	}
	return principal, body.Revision, true
}

func (handler *Handler) authenticate(writer http.ResponseWriter, request *http.Request) (access.Principal, bool) {
	header := request.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		handler.writeError(writer, request, http.StatusUnauthorized, "UNAUTHENTICATED", "bearer credential is required", nil)
		return access.Principal{}, false
	}
	principal, err := handler.authenticator.Authenticate(request.Context(), strings.TrimPrefix(header, "Bearer "))
	if err != nil {
		status, code := http.StatusInternalServerError, "INTERNAL_ERROR"
		if errors.Is(err, access.ErrUnauthenticated) {
			status, code = http.StatusUnauthorized, "UNAUTHENTICATED"
		}
		handler.writeError(writer, request, status, code, "authentication failed", nil)
		return access.Principal{}, false
	}
	return principal, true
}

func (handler *Handler) decode(writer http.ResponseWriter, request *http.Request, target any) bool {
	reader := http.MaxBytesReader(writer, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		handler.writeError(writer, request, http.StatusBadRequest, definition.CodeInvalidArgument, "request JSON is invalid", nil)
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		handler.writeError(writer, request, http.StatusBadRequest, definition.CodeInvalidArgument, "request must contain one JSON object", nil)
		return false
	}
	return true
}

func (handler *Handler) writeResultError(writer http.ResponseWriter, request *http.Request, diagnostics []ir.Diagnostic, err error) bool {
	if err == nil && !ir.HasErrors(diagnostics) {
		return false
	}
	if err == nil {
		handler.writeError(writer, request, http.StatusUnprocessableEntity, "VALIDATION_FAILED", "workflow definition is invalid", map[string]any{"diagnostics": diagnostics})
		return true
	}
	status, code, message, details := http.StatusInternalServerError, "INTERNAL_ERROR", "internal error", map[string]any(nil)
	var domainError *definition.Error
	if errors.As(err, &domainError) {
		code, message, details = domainError.Code, domainError.Message, domainError.Details
		switch code {
		case definition.CodeInvalidArgument:
			status = http.StatusBadRequest
		case definition.CodePermissionDenied:
			status = http.StatusForbidden
		case definition.CodeResourceNotFound:
			status = http.StatusNotFound
		case definition.CodeDraftRevisionConflict, definition.CodeIdempotencyKeyReused, definition.CodeWorkflowNotPublished:
			status = http.StatusConflict
		case definition.CodeCatalogUnavailable:
			status = http.StatusUnprocessableEntity
		}
	}
	if errors.Is(err, runtime.ErrRunNotFound) {
		status, code, message = http.StatusNotFound, "RUN_NOT_FOUND", "workflow run was not found"
	}
	if errors.Is(err, runtime.ErrRunWorkflowNotPublished) {
		status, code, message = http.StatusConflict, definition.CodeWorkflowNotPublished, "workflow has no active published version"
	}
	if errors.Is(err, access.ErrPermissionDenied) {
		status, code, message = http.StatusForbidden, definition.CodePermissionDenied, "permission denied"
	}
	if errors.Is(err, runtime.ErrInvalidRun) {
		status, code, message = http.StatusBadRequest, definition.CodeInvalidArgument, "run request is invalid"
	}
	handler.writeError(writer, request, status, code, message, details)
	return true
}

func (handler *Handler) writeError(writer http.ResponseWriter, request *http.Request, status int, code, message string, details map[string]any) {
	requestID, err := handler.ids.New()
	if err != nil {
		requestID = traceid.From(request.Context())
	}
	writeJSON(writer, status, map[string]any{"error": map[string]any{
		"code": code, "message": message, "retryable": status >= 500,
		"request_id": requestID, "trace_id": traceid.From(request.Context()), "details": details,
	}})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
