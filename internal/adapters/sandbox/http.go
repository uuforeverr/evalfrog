package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	domainsandbox "github.com/uu999/evalfrog/internal/sandbox"
)

// HTTPOrchestrator is the production-side client for a separately deployed,
// hardened Sandbox Runtime Controller. It deliberately contains no Docker or
// container-runtime capability: a Sandbox Worker can only request one
// per-attempt execution over its constrained network path.
type HTTPOrchestrator struct {
	baseURL      string
	profile      domainsandbox.Profile
	client       *http.Client
	runtimeToken string
}

func NewHTTPOrchestrator(baseURL string, profile domainsandbox.Profile, client *http.Client, allowInsecure bool, runtimeToken string) (HTTPOrchestrator, error) {
	parsed, err := url.ParseRequestURI(baseURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || !profile.Valid() || runtimeToken == "" || (parsed.Scheme != "https" && (!allowInsecure || parsed.Scheme != "http")) {
		return HTTPOrchestrator{}, fmt.Errorf("sandbox HTTPS runtime URL and fixed profile are required")
	}
	if client == nil {
		client = &http.Client{
			Timeout: profile.ExecutionTimeout + profile.CleanupTimeout,
			// The controller endpoint is deployment-owned, but following a
			// redirect would still let a compromised or misconfigured private
			// endpoint turn the Worker into an unintended HTTP client.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return HTTPOrchestrator{baseURL: strings.TrimRight(baseURL, "/"), profile: profile, client: client, runtimeToken: runtimeToken}, nil
}

func (orchestrator HTTPOrchestrator) Run(ctx context.Context, request domainsandbox.Request) (domainsandbox.Result, error) {
	if request.AttemptID == "" || request.SourceCode == "" || orchestrator.client == nil {
		return domainsandbox.Result{}, fmt.Errorf("sandbox request and runtime client are required")
	}
	payload, err := json.Marshal(struct {
		SourceCode string                     `json:"source_code"`
		Inputs     map[string]json.RawMessage `json:"inputs"`
	}{SourceCode: request.SourceCode, Inputs: request.Inputs})
	if err != nil {
		return domainsandbox.Result{}, err
	}
	started := time.Now()
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, orchestrator.baseURL+"/attempts/"+url.PathEscape(request.AttemptID)+"/execute", bytes.NewReader(payload))
	if err != nil {
		return domainsandbox.Result{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+orchestrator.runtimeToken)
	response, err := orchestrator.client.Do(httpRequest)
	result := domainsandbox.Result{Telemetry: domainsandbox.Telemetry{Runtime: orchestrator.profile.Runtime, Duration: time.Since(started)}}
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return result, fmt.Errorf("sandbox runtime returned %s", response.Status)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, orchestrator.profile.OutputBytes+1))
	if err != nil {
		return result, err
	}
	if int64(len(raw)) > orchestrator.profile.OutputBytes {
		result.Failure = &domainsandbox.Failure{Code: "SANDBOX_OUTPUT_TOO_LARGE", Message: "sandbox output exceeds the fixed limit"}
		return result, nil
	}
	return decodeFrame(raw, result)
}

// Cleanup is intentionally best-effort. The controller owns its own container
// lifecycle and must make this endpoint idempotent for late Worker retries.
func (orchestrator HTTPOrchestrator) Cleanup(ctx context.Context, attemptID string) error {
	if attemptID == "" || orchestrator.client == nil {
		return fmt.Errorf("sandbox attempt and runtime client are required")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, orchestrator.baseURL+"/attempts/"+url.PathEscape(attemptID), nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+orchestrator.runtimeToken)
	response, err := orchestrator.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("sandbox runtime cleanup returned %s", response.Status)
	}
	return nil
}

var _ domainsandbox.Orchestrator = HTTPOrchestrator{}
