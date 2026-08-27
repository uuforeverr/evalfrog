package workerapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/uu999/evalfrog/internal/resources"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
)

type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string, timeout time.Duration) *Client {
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: &http.Client{Timeout: timeout}}
}

func (client *Client) Check(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.baseURL+"/health/ready", nil)
	if err != nil {
		return err
	}
	response, err := client.http.Do(request)
	if err != nil {
		return fmt.Errorf("control-plane readiness: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("control-plane readiness returned %s", response.Status)
	}
	return nil
}

func (client *Client) Claim(ctx context.Context, command attempt.ClaimCommand) (attempt.Lease, error) {
	capabilities := make([]capability, len(command.Capabilities))
	for index, value := range command.Capabilities {
		capabilities[index] = capability{Type: value.Type, Version: value.Version}
	}
	body := claimRequest{ProjectID: command.ProjectID, RunID: command.RunID,
		AttemptSequence: command.AttemptSequence, WorkerID: command.WorkerID,
		ExecutorBuild: command.ExecutorBuild, ResourceClass: command.ResourceClass,
		Capabilities: capabilities, LeaseDurationMS: command.LeaseDuration.Milliseconds()}
	var response leaseResponse
	if err := client.post(ctx, "/internal/v1/attempts/"+command.AttemptID+"/claim", body, &response); err != nil {
		return attempt.Lease{}, err
	}
	return attempt.Lease{Token: response.LeaseToken, Owner: response.Owner, FencingToken: response.FencingToken, ExpiresAt: response.ExpiresAt, CancelRequested: response.CancelRequested}, nil
}

func (client *Client) Heartbeat(ctx context.Context, command attempt.HeartbeatCommand) (attempt.Lease, error) {
	body := heartbeatRequest{ProjectID: command.ProjectID, RunID: command.RunID,
		AttemptSequence: command.AttemptSequence, LeaseToken: command.LeaseToken,
		FencingToken: command.FencingToken, ExtendByMS: command.ExtendBy.Milliseconds()}
	var response leaseResponse
	if err := client.post(ctx, "/internal/v1/attempts/"+command.AttemptID+"/heartbeat", body, &response); err != nil {
		return attempt.Lease{}, err
	}
	return attempt.Lease{Token: response.LeaseToken, Owner: response.Owner, FencingToken: response.FencingToken, ExpiresAt: response.ExpiresAt, CancelRequested: response.CancelRequested}, nil
}

func (client *Client) Complete(ctx context.Context, command attempt.CompleteCommand) (bool, error) {
	body := completeRequest{ProjectID: command.ProjectID, RunID: command.RunID,
		AttemptSequence: command.AttemptSequence, LeaseToken: command.LeaseToken,
		FencingToken: command.FencingToken, State: command.Result.State,
		Outputs: command.Result.Outputs, ErrorCode: command.Result.ErrorCode,
		Message: command.Result.Message, DSLField: command.Result.DSLField, ErrorDetails: command.Result.ErrorDetails, TraceID: command.TraceID}
	var response struct {
		Accepted bool `json:"accepted"`
	}
	if err := client.post(ctx, "/internal/v1/attempts/"+command.AttemptID+"/complete", body, &response); err != nil {
		return false, err
	}
	return response.Accepted, nil
}

func (client *Client) Load(ctx context.Context, command runtimecontext.LoadCommand) (runtimecontext.ExecutionContext, error) {
	body := contextRequest{ProjectID: command.ProjectID, RunID: command.RunID, AttemptSequence: command.AttemptSequence, LeaseToken: command.LeaseToken, FencingToken: command.FencingToken}
	var response runtimecontext.ExecutionContext
	if err := client.post(ctx, "/internal/v1/attempts/"+command.AttemptID+"/context", body, &response); err != nil {
		return runtimecontext.ExecutionContext{}, err
	}
	response.LeaseToken, response.FencingToken = command.LeaseToken, command.FencingToken
	return response, nil
}

func (client *Client) ResolveConnection(ctx context.Context, command resources.RuntimeResolveCommand) (resources.ConnectionRuntime, error) {
	body := resourceRequest{ProjectID: command.ProjectID, RunID: command.RunID, AttemptSequence: command.AttemptSequence, LeaseToken: command.LeaseToken, FencingToken: command.FencingToken, ConnectionID: command.ConnectionID}
	var response resources.ConnectionRuntime
	if err := client.post(ctx, "/internal/v1/attempts/"+command.AttemptID+"/resources/connection", body, &response); err != nil {
		return resources.ConnectionRuntime{}, err
	}
	return response, nil
}

func (client *Client) ResolveServiceOperation(ctx context.Context, command resources.RuntimeResolveCommand) (resources.ServiceOperationRuntime, error) {
	body := resourceRequest{ProjectID: command.ProjectID, RunID: command.RunID, AttemptSequence: command.AttemptSequence, LeaseToken: command.LeaseToken, FencingToken: command.FencingToken, ServiceID: command.ServiceID, Operation: command.Operation, ContractRevision: command.ContractRevision}
	var response resources.ServiceOperationRuntime
	if err := client.post(ctx, "/internal/v1/attempts/"+command.AttemptID+"/resources/service-operation", body, &response); err != nil {
		return resources.ServiceOperationRuntime{}, err
	}
	return response, nil
}

func (client *Client) post(ctx context.Context, path string, body, result any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		if err = json.NewDecoder(response.Body).Decode(result); err != nil {
			return err
		}
		return nil
	}
	var failure struct {
		Error struct{ Code, Message string } `json:"error"`
	}
	_ = json.NewDecoder(response.Body).Decode(&failure)
	switch failure.Error.Code {
	case "ATTEMPT_NOT_FOUND":
		err = attempt.ErrNotFound
	case "ATTEMPT_NOT_CURRENT":
		err = attempt.ErrNotCurrent
	case "LEASE_MISMATCH":
		err = attempt.ErrLeaseMismatch
	case "ATTEMPT_STATE_CONFLICT":
		err = attempt.ErrStateConflict
	case "CAPABILITY_MISMATCH":
		err = attempt.ErrCapabilityMismatch
	default:
		err = errors.New(failure.Error.Message)
	}
	return fmt.Errorf("worker API %s: %w", failure.Error.Code, err)
}

var _ interface {
	Claim(context.Context, attempt.ClaimCommand) (attempt.Lease, error)
	Heartbeat(context.Context, attempt.HeartbeatCommand) (attempt.Lease, error)
	Complete(context.Context, attempt.CompleteCommand) (bool, error)
	Load(context.Context, runtimecontext.LoadCommand) (runtimecontext.ExecutionContext, error)
	ResolveConnection(context.Context, resources.RuntimeResolveCommand) (resources.ConnectionRuntime, error)
	ResolveServiceOperation(context.Context, resources.RuntimeResolveCommand) (resources.ServiceOperationRuntime, error)
} = (*Client)(nil)
