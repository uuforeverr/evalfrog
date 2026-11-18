package sandbox

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	domainsandbox "github.com/uu999/evalfrog/internal/sandbox"
)

func TestRuntimeHandlerExecutesBoundedPrivateProtocol(t *testing.T) {
	t.Parallel()
	orchestrator := &runtimeOrchestrator{result: domainsandbox.Result{Outputs: json.RawMessage(`{"total":3}`)}}
	handler, err := NewRuntimeHandler(orchestrator, "runtime-token")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/attempts/attempt-1/execute", strings.NewReader(`{"source_code":"def main(inputs): return {}","inputs":{"items":[1,2]}}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer runtime-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || orchestrator.request.AttemptID != "attempt-1" || string(orchestrator.request.Inputs["items"]) != `[1,2]` || !strings.Contains(response.Body.String(), `"status":"ok"`) {
		t.Fatalf("status=%d request=%+v body=%s", response.Code, orchestrator.request, response.Body.String())
	}
}

func TestRuntimeHandlerRejectsUnsafeRoutesAndDoesNotExposeAdapterError(t *testing.T) {
	t.Parallel()
	orchestrator := &runtimeOrchestrator{err: context.DeadlineExceeded}
	handler, err := NewRuntimeHandler(orchestrator, "runtime-token")
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodPost, "/v1/attempts/a%2Fb/execute", strings.NewReader(`{}`)),
		httptest.NewRequest(http.MethodPost, "/v1/attempts/attempt/execute", strings.NewReader(`{}`)),
	} {
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer runtime-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if strings.Contains(response.Body.String(), "deadline") || response.Code < http.StatusBadRequest {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	}
}

func TestRuntimeHandlerCleanupIsIdempotentEndpoint(t *testing.T) {
	t.Parallel()
	orchestrator := &runtimeOrchestrator{}
	handler, _ := NewRuntimeHandler(orchestrator, "runtime-token")
	request := httptest.NewRequest(http.MethodDelete, "/v1/attempts/attempt-1", nil)
	request.Header.Set("Authorization", "Bearer runtime-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || orchestrator.cleaned != "attempt-1" {
		t.Fatalf("status=%d cleaned=%q", response.Code, orchestrator.cleaned)
	}
}

func TestRuntimeHandlerRejectsMissingServiceCredential(t *testing.T) {
	t.Parallel()
	handler, err := NewRuntimeHandler(&runtimeOrchestrator{}, "runtime-token")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/attempts/attempt-1/execute", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || strings.Contains(response.Body.String(), "runtime-token") {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

type runtimeOrchestrator struct {
	request domainsandbox.Request
	result  domainsandbox.Result
	err     error
	cleaned string
}

func (orchestrator *runtimeOrchestrator) Run(_ context.Context, request domainsandbox.Request) (domainsandbox.Result, error) {
	orchestrator.request = request
	return orchestrator.result, orchestrator.err
}

func (orchestrator *runtimeOrchestrator) Cleanup(_ context.Context, attemptID string) error {
	orchestrator.cleaned = attemptID
	return nil
}
