package sandbox

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	domainsandbox "github.com/uu999/evalfrog/internal/sandbox"
)

const maxRuntimeRequestBytes = 1 << 20

// NewRuntimeHandler exposes the narrow private protocol used by sandbox
// workers. Deploy it only on the dedicated Sandbox Runtime Controller: it is
// the sole process permitted to own the container-runtime credential.
func NewRuntimeHandler(orchestrator domainsandbox.Orchestrator, runtimeToken string) (http.Handler, error) {
	if orchestrator == nil || runtimeToken == "" {
		return nil, fmt.Errorf("sandbox orchestrator and runtime token are required")
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if subtle.ConstantTimeCompare([]byte(request.Header.Get("Authorization")), []byte("Bearer "+runtimeToken)) != 1 {
			writeRuntimeError(writer, http.StatusUnauthorized, "SANDBOX_RUNTIME_UNAUTHORIZED", "sandbox runtime authentication failed")
			return
		}
		attemptID, action, ok := runtimeRoute(request.URL.Path)
		if !ok {
			writeRuntimeError(writer, http.StatusNotFound, "SANDBOX_RUNTIME_ROUTE", "sandbox runtime route is invalid")
			return
		}
		switch {
		case request.Method == http.MethodPost && action == "execute":
			handleExecute(writer, request, orchestrator, attemptID)
		case request.Method == http.MethodDelete && action == "":
			handleCleanup(writer, request.Context(), orchestrator, attemptID)
		default:
			writer.Header().Set("Allow", "POST, DELETE")
			writeRuntimeError(writer, http.StatusMethodNotAllowed, "SANDBOX_RUNTIME_METHOD", "sandbox runtime method is invalid")
		}
	}), nil
}

func runtimeRoute(path string) (attemptID, action string, ok bool) {
	const prefix = "/v1/attempts/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(path, prefix), "/")
	if len(parts) == 1 && validAttemptID(parts[0]) {
		return parts[0], "", true
	}
	if len(parts) == 2 && parts[1] == "execute" && validAttemptID(parts[0]) {
		return parts[0], "execute", true
	}
	return "", "", false
}

func validAttemptID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z') && !(character >= 'A' && character <= 'Z') && !(character >= '0' && character <= '9') && character != '-' {
			return false
		}
	}
	return true
}

func handleExecute(writer http.ResponseWriter, request *http.Request, orchestrator domainsandbox.Orchestrator, attemptID string) {
	if request.Header.Get("Content-Type") != "application/json" {
		writeRuntimeError(writer, http.StatusUnsupportedMediaType, "SANDBOX_RUNTIME_CONTENT_TYPE", "sandbox runtime requires application/json")
		return
	}
	defer request.Body.Close()
	var payload struct {
		SourceCode string                     `json:"source_code"`
		Inputs     map[string]json.RawMessage `json:"inputs"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxRuntimeRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil || payload.SourceCode == "" {
		writeRuntimeError(writer, http.StatusBadRequest, "SANDBOX_RUNTIME_REQUEST", "sandbox runtime request is invalid")
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		writeRuntimeError(writer, http.StatusBadRequest, "SANDBOX_RUNTIME_REQUEST", "sandbox runtime request is invalid")
		return
	}
	result, err := orchestrator.Run(request.Context(), domainsandbox.Request{AttemptID: attemptID, SourceCode: payload.SourceCode, Inputs: payload.Inputs})
	if err != nil {
		writeRuntimeError(writer, http.StatusServiceUnavailable, "SANDBOX_RUNTIME_UNAVAILABLE", "sandbox runtime could not execute the attempt")
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	if result.Failure != nil {
		_ = json.NewEncoder(writer).Encode(map[string]any{"status": "error", "code": result.Failure.Code, "message": result.Failure.Message, "details": result.Failure.Details})
		return
	}
	_ = json.NewEncoder(writer).Encode(map[string]any{"status": "ok", "output": json.RawMessage(result.Outputs)})
}

func handleCleanup(writer http.ResponseWriter, ctx context.Context, orchestrator domainsandbox.Orchestrator, attemptID string) {
	if err := orchestrator.Cleanup(ctx, attemptID); err != nil {
		writeRuntimeError(writer, http.StatusServiceUnavailable, "SANDBOX_RUNTIME_UNAVAILABLE", "sandbox runtime could not clean up the attempt")
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func writeRuntimeError(writer http.ResponseWriter, status int, code, message string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"error": map[string]string{"code": code, "message": message}})
}
