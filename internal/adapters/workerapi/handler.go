package workerapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/resources"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
	"github.com/uu999/evalfrog/internal/scheduling"
)

const maxInternalRequestBytes = 2 << 20

type AttemptCoordinator interface {
	Claim(context.Context, attempt.ClaimCommand) (attempt.Lease, error)
	Heartbeat(context.Context, attempt.HeartbeatCommand) (attempt.Lease, error)
	Complete(context.Context, attempt.CompleteCommand) (bool, error)
}

type ContextGateway interface {
	Load(context.Context, runtimecontext.LoadCommand) (runtimecontext.ExecutionContext, error)
}

type ResourceResolver interface {
	ResolveConnection(context.Context, resources.RuntimeResolveCommand) (resources.ConnectionRuntime, error)
	ResolveServiceOperation(context.Context, resources.RuntimeResolveCommand) (resources.ServiceOperationRuntime, error)
}

type Handler struct {
	coordinator AttemptCoordinator
	context     ContextGateway
	resources   ResourceResolver
	router      *http.ServeMux
}

func NewHandler(coordinator AttemptCoordinator, gateway ContextGateway, resolvers ...ResourceResolver) *Handler {
	var resourceResolver ResourceResolver
	if len(resolvers) > 0 {
		resourceResolver = resolvers[0]
	}
	handler := &Handler{coordinator: coordinator, context: gateway, resources: resourceResolver, router: http.NewServeMux()}
	handler.router.HandleFunc("POST /internal/v1/attempts/{attempt_id}/claim", handler.claim)
	handler.router.HandleFunc("POST /internal/v1/attempts/{attempt_id}/heartbeat", handler.heartbeat)
	handler.router.HandleFunc("POST /internal/v1/attempts/{attempt_id}/complete", handler.complete)
	handler.router.HandleFunc("POST /internal/v1/attempts/{attempt_id}/context", handler.loadContext)
	handler.router.HandleFunc("POST /internal/v1/attempts/{attempt_id}/resources/connection", handler.resolveConnection)
	handler.router.HandleFunc("POST /internal/v1/attempts/{attempt_id}/resources/service-operation", handler.resolveServiceOperation)
	return handler
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	handler.router.ServeHTTP(writer, request)
}

type claimRequest struct {
	ProjectID       string                   `json:"project_id"`
	RunID           string                   `json:"run_id"`
	AttemptSequence uint32                   `json:"attempt_sequence"`
	WorkerID        string                   `json:"worker_id"`
	ExecutorBuild   string                   `json:"executor_build"`
	ResourceClass   scheduling.ResourceClass `json:"resource_class"`
	Capabilities    []capability             `json:"capabilities"`
	LeaseDurationMS int64                    `json:"lease_duration_ms"`
}

type capability struct {
	Type    string `json:"type"`
	Version uint32 `json:"version"`
}

type heartbeatRequest struct {
	ProjectID       string `json:"project_id"`
	RunID           string `json:"run_id"`
	AttemptSequence uint32 `json:"attempt_sequence"`
	LeaseToken      string `json:"lease_token"`
	FencingToken    uint64 `json:"fencing_token"`
	ExtendByMS      int64  `json:"extend_by_ms"`
}

type completeRequest struct {
	ProjectID       string                     `json:"project_id"`
	RunID           string                     `json:"run_id"`
	AttemptSequence uint32                     `json:"attempt_sequence"`
	LeaseToken      string                     `json:"lease_token"`
	FencingToken    uint64                     `json:"fencing_token"`
	State           runtime.AttemptState       `json:"state"`
	Outputs         map[string]json.RawMessage `json:"outputs"`
	ErrorCode       string                     `json:"error_code"`
	Message         string                     `json:"message"`
	DSLField        string                     `json:"dsl_field,omitempty"`
	ErrorDetails    map[string]any             `json:"error_details,omitempty"`
	TraceID         string                     `json:"trace_id"`
}

type contextRequest struct {
	ProjectID       string `json:"project_id"`
	RunID           string `json:"run_id"`
	AttemptSequence uint32 `json:"attempt_sequence"`
	LeaseToken      string `json:"lease_token"`
	FencingToken    uint64 `json:"fencing_token"`
}

type resourceRequest struct {
	ProjectID, RunID string
	AttemptSequence  uint32 `json:"attempt_sequence"`
	LeaseToken       string `json:"lease_token"`
	FencingToken     uint64 `json:"fencing_token"`
	ConnectionID     string `json:"connection_id,omitempty"`
	ServiceID        string `json:"service_id,omitempty"`
	Operation        string `json:"operation,omitempty"`
	ContractRevision string `json:"contract_revision,omitempty"`
}

type leaseResponse struct {
	LeaseToken      string    `json:"lease_token"`
	Owner           string    `json:"owner"`
	FencingToken    uint64    `json:"fencing_token"`
	ExpiresAt       time.Time `json:"expires_at"`
	CancelRequested bool      `json:"cancel_requested"`
}

func (handler *Handler) claim(writer http.ResponseWriter, request *http.Request) {
	var body claimRequest
	if !decode(writer, request, &body) {
		return
	}
	capabilities := make([]dsl.Coordinate, len(body.Capabilities))
	for index, value := range body.Capabilities {
		capabilities[index] = dsl.Coordinate{Type: value.Type, Version: value.Version}
	}
	lease, err := handler.coordinator.Claim(request.Context(), attempt.ClaimCommand{
		ProjectID: body.ProjectID, RunID: body.RunID, AttemptID: request.PathValue("attempt_id"),
		AttemptSequence: body.AttemptSequence, WorkerID: body.WorkerID, ExecutorBuild: body.ExecutorBuild,
		ResourceClass: body.ResourceClass, Capabilities: capabilities,
		LeaseDuration: time.Duration(body.LeaseDurationMS) * time.Millisecond,
	})
	if writeDomainError(writer, err) {
		return
	}
	writeJSON(writer, http.StatusOK, leaseResponse{lease.Token, lease.Owner, lease.FencingToken, lease.ExpiresAt, lease.CancelRequested})
}

func (handler *Handler) heartbeat(writer http.ResponseWriter, request *http.Request) {
	var body heartbeatRequest
	if !decode(writer, request, &body) {
		return
	}
	lease, err := handler.coordinator.Heartbeat(request.Context(), attempt.HeartbeatCommand{
		ProjectID: body.ProjectID, RunID: body.RunID, AttemptID: request.PathValue("attempt_id"),
		AttemptSequence: body.AttemptSequence, LeaseToken: body.LeaseToken,
		FencingToken: body.FencingToken, ExtendBy: time.Duration(body.ExtendByMS) * time.Millisecond,
	})
	if writeDomainError(writer, err) {
		return
	}
	writeJSON(writer, http.StatusOK, leaseResponse{lease.Token, lease.Owner, lease.FencingToken, lease.ExpiresAt, lease.CancelRequested})
}

func (handler *Handler) complete(writer http.ResponseWriter, request *http.Request) {
	var body completeRequest
	if !decode(writer, request, &body) {
		return
	}
	accepted, err := handler.coordinator.Complete(request.Context(), attempt.CompleteCommand{
		ProjectID: body.ProjectID, RunID: body.RunID, AttemptID: request.PathValue("attempt_id"),
		AttemptSequence: body.AttemptSequence, LeaseToken: body.LeaseToken,
		FencingToken: body.FencingToken, TraceID: body.TraceID,
		Result: runtime.AttemptResult{State: body.State, Outputs: body.Outputs, ErrorCode: body.ErrorCode, Message: body.Message, DSLField: body.DSLField, ErrorDetails: body.ErrorDetails},
	})
	if writeDomainError(writer, err) {
		return
	}
	writeJSON(writer, http.StatusOK, map[string]bool{"accepted": accepted})
}

func (handler *Handler) loadContext(writer http.ResponseWriter, request *http.Request) {
	var body contextRequest
	if !decode(writer, request, &body) {
		return
	}
	value, err := handler.context.Load(request.Context(), runtimecontext.LoadCommand{
		ProjectID: body.ProjectID, RunID: body.RunID, AttemptID: request.PathValue("attempt_id"), AttemptSequence: body.AttemptSequence,
		LeaseToken: body.LeaseToken, FencingToken: body.FencingToken,
	})
	if writeDomainError(writer, err) {
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (handler *Handler) resolveConnection(writer http.ResponseWriter, request *http.Request) {
	var body resourceRequest
	if !decode(writer, request, &body) {
		return
	}
	if handler.resources == nil {
		writeError(writer, http.StatusServiceUnavailable, "RESOURCE_RESOLVER_UNAVAILABLE", "managed resource resolver is unavailable")
		return
	}
	value, err := handler.resources.ResolveConnection(request.Context(), resources.RuntimeResolveCommand{ProjectID: body.ProjectID, RunID: body.RunID, AttemptID: request.PathValue("attempt_id"), AttemptSequence: body.AttemptSequence, LeaseToken: body.LeaseToken, FencingToken: body.FencingToken, ConnectionID: body.ConnectionID})
	if writeDomainError(writer, err) {
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func (handler *Handler) resolveServiceOperation(writer http.ResponseWriter, request *http.Request) {
	var body resourceRequest
	if !decode(writer, request, &body) {
		return
	}
	if handler.resources == nil {
		writeError(writer, http.StatusServiceUnavailable, "RESOURCE_RESOLVER_UNAVAILABLE", "managed resource resolver is unavailable")
		return
	}
	value, err := handler.resources.ResolveServiceOperation(request.Context(), resources.RuntimeResolveCommand{ProjectID: body.ProjectID, RunID: body.RunID, AttemptID: request.PathValue("attempt_id"), AttemptSequence: body.AttemptSequence, LeaseToken: body.LeaseToken, FencingToken: body.FencingToken, ServiceID: body.ServiceID, Operation: body.Operation, ContractRevision: body.ContractRevision})
	if writeDomainError(writer, err) {
		return
	}
	writeJSON(writer, http.StatusOK, value)
}

func decode(writer http.ResponseWriter, request *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxInternalRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(writer, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(writer, http.StatusBadRequest, "INVALID_ARGUMENT", "request must contain one JSON object")
		return false
	}
	return true
}

func writeDomainError(writer http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	status, code := http.StatusInternalServerError, "INTERNAL_ERROR"
	switch {
	case errors.Is(err, attempt.ErrNotFound):
		status, code = http.StatusNotFound, "ATTEMPT_NOT_FOUND"
	case errors.Is(err, attempt.ErrNotCurrent):
		status, code = http.StatusConflict, "ATTEMPT_NOT_CURRENT"
	case errors.Is(err, attempt.ErrLeaseMismatch):
		status, code = http.StatusConflict, "LEASE_MISMATCH"
	case errors.Is(err, attempt.ErrStateConflict):
		status, code = http.StatusConflict, "ATTEMPT_STATE_CONFLICT"
	case errors.Is(err, attempt.ErrCapabilityMismatch):
		status, code = http.StatusConflict, "CAPABILITY_MISMATCH"
	case strings.Contains(err.Error(), "required") || strings.Contains(err.Error(), "invalid"):
		status, code = http.StatusBadRequest, "INVALID_ARGUMENT"
	}
	writeError(writer, status, code, err.Error())
	return true
}

func writeError(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, map[string]any{"error": map[string]any{"code": code, "message": message}})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
