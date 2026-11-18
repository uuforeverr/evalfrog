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

func TestHTTPOrchestratorUsesPerAttemptHTTPSContract(t *testing.T) {
	t.Parallel()
	var gotPath, source string
	var inputs map[string]json.RawMessage
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotPath = request.URL.EscapedPath()
		if request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" || request.Header.Get("Authorization") != "Bearer runtime-token" {
			t.Errorf("request method=%s headers=%v", request.Method, request.Header)
		}
		var received struct {
			SourceCode string                     `json:"source_code"`
			Inputs     map[string]json.RawMessage `json:"inputs"`
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Fatal(err)
		}
		source, inputs = received.SourceCode, received.Inputs
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ok","output":{"total":3}}`))
	}))
	defer server.Close()
	orchestrator, err := NewHTTPOrchestrator(server.URL+"/v1", domainsandbox.DefaultProfile("image@sha256:abc", "runsc"), server.Client(), false, "runtime-token")
	if err != nil {
		t.Fatal(err)
	}
	result, err := orchestrator.Run(context.Background(), domainsandbox.Request{AttemptID: "attempt/a", SourceCode: "def main(inputs): return {}", Inputs: map[string]json.RawMessage{"items": json.RawMessage(`[1,2]`)}})
	if err != nil || result.Failure != nil || string(result.Outputs) != `{"total":3}` {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if gotPath != "/v1/attempts/attempt%2Fa/execute" || !strings.Contains(source, "main") || string(inputs["items"]) != `[1,2]` {
		t.Fatalf("path=%q source=%q inputs=%v", gotPath, source, inputs)
	}
}

func TestHTTPOrchestratorRejectsNonHTTPSAndBoundsResponse(t *testing.T) {
	t.Parallel()
	profile := domainsandbox.DefaultProfile("image@sha256:abc", "runsc")
	if _, err := NewHTTPOrchestrator("http://sandbox.example", profile, nil, false, "runtime-token"); err == nil {
		t.Fatal("non-HTTPS runtime accepted")
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write(make([]byte, profile.OutputBytes+1))
	}))
	defer server.Close()
	orchestrator, err := NewHTTPOrchestrator(server.URL, profile, server.Client(), false, "runtime-token")
	if err != nil {
		t.Fatal(err)
	}
	result, err := orchestrator.Run(context.Background(), domainsandbox.Request{AttemptID: "attempt", SourceCode: "def main(inputs): return {}"})
	if err != nil || result.Failure == nil || result.Failure.Code != "SANDBOX_OUTPUT_TOO_LARGE" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestHTTPOrchestratorDoesNotFollowControllerRedirects(t *testing.T) {
	t.Parallel()
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", "https://untrusted.example/v1")
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	orchestrator, err := NewHTTPOrchestrator(server.URL, domainsandbox.DefaultProfile("image@sha256:abc", "runsc"), nil, false, "runtime-token")
	if err != nil {
		t.Fatal(err)
	}
	// Replace only the transport so the test client trusts httptest's TLS
	// certificate while preserving the production redirect policy.
	orchestrator.client.Transport = server.Client().Transport
	result, err := orchestrator.Run(context.Background(), domainsandbox.Request{AttemptID: "attempt", SourceCode: "def main(inputs): return {}"})
	if err == nil || result.Failure != nil || !strings.Contains(err.Error(), "307") {
		t.Fatalf("redirect result=%+v err=%v", result, err)
	}
}
