package workerapi

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	platformruntime "github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
	"github.com/uu999/evalfrog/internal/scheduling"
)

func TestWorkerAPIClientAndHandlerRoundTrip(t *testing.T) {
	coordinator := &fakeCoordinator{}
	gateway := &fakeGateway{}
	server := httptest.NewServer(NewHandler(coordinator, gateway))
	defer server.Close()
	client := New(server.URL+"/", time.Second)
	ctx := context.Background()
	capabilities := []dsl.Coordinate{{Type: "task.python", Version: 1}}
	lease, err := client.Claim(ctx, attempt.ClaimCommand{ProjectID: "project", RunID: "run", AttemptID: "attempt", AttemptSequence: 1, WorkerID: "worker", ExecutorBuild: "build", ResourceClass: scheduling.ResourceSandbox, Capabilities: capabilities, LeaseDuration: time.Minute})
	if err != nil || lease.Token != "lease" || coordinator.claim.AttemptID != "attempt" {
		t.Fatalf("lease=%+v claim=%+v err=%v", lease, coordinator.claim, err)
	}
	lease, err = client.Heartbeat(ctx, attempt.HeartbeatCommand{ProjectID: "project", RunID: "run", AttemptID: "attempt", AttemptSequence: 1, LeaseToken: "lease", FencingToken: 1, ExtendBy: time.Minute})
	if err != nil || coordinator.heartbeat.ExtendBy != time.Minute {
		t.Fatalf("lease=%+v heartbeat=%+v err=%v", lease, coordinator.heartbeat, err)
	}
	value, err := client.Load(ctx, runtimecontext.LoadCommand{ProjectID: "project", RunID: "run", AttemptID: "attempt", AttemptSequence: 1, LeaseToken: "lease", FencingToken: 1})
	if err != nil || value.ContextVersion != 1 || gateway.command.AttemptID != "attempt" {
		t.Fatalf("context=%+v command=%+v err=%v", value, gateway.command, err)
	}
	accepted, err := client.Complete(ctx, attempt.CompleteCommand{ProjectID: "project", RunID: "run", AttemptID: "attempt", AttemptSequence: 1, LeaseToken: "lease", FencingToken: 1, TraceID: "trace", Result: platformruntime.AttemptResult{State: platformruntime.AttemptFailed, ErrorCode: "CODE_RUNTIME_ERROR", Message: "bad", DSLField: "operation.config.source_code", ErrorDetails: map[string]any{"source_line": 3}}})
	if err != nil || !accepted || coordinator.complete.Result.DSLField != "operation.config.source_code" || coordinator.complete.Result.ErrorDetails["source_line"] != float64(3) {
		t.Fatalf("accepted=%v complete=%+v err=%v", accepted, coordinator.complete, err)
	}
}

func TestWorkerAPIRejectsUnknownFieldsAndMapsDomainErrors(t *testing.T) {
	coordinator := &fakeCoordinator{err: attempt.ErrCapabilityMismatch}
	server := httptest.NewServer(NewHandler(coordinator, &fakeGateway{}))
	defer server.Close()
	client := New(server.URL, time.Second)
	_, err := client.Claim(context.Background(), attempt.ClaimCommand{ProjectID: "project", RunID: "run", AttemptID: "attempt", AttemptSequence: 1, WorkerID: "worker", ExecutorBuild: "build", ResourceClass: scheduling.ResourceSandbox, Capabilities: []dsl.Coordinate{{Type: "task.python", Version: 1}}, LeaseDuration: time.Minute})
	if !errors.Is(err, attempt.ErrCapabilityMismatch) {
		t.Fatalf("mapped error=%v", err)
	}
	response, err := http.Post(server.URL+"/internal/v1/attempts/attempt/claim", "application/json", bytes.NewBufferString(`{"unknown":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", response.StatusCode)
	}
}

type fakeCoordinator struct {
	claim     attempt.ClaimCommand
	heartbeat attempt.HeartbeatCommand
	complete  attempt.CompleteCommand
	err       error
}

func (value *fakeCoordinator) Claim(_ context.Context, command attempt.ClaimCommand) (attempt.Lease, error) {
	value.claim = command
	return attempt.Lease{Token: "lease", Owner: "worker", FencingToken: 1, ExpiresAt: time.Now().Add(time.Minute)}, value.err
}
func (value *fakeCoordinator) Heartbeat(_ context.Context, command attempt.HeartbeatCommand) (attempt.Lease, error) {
	value.heartbeat = command
	return attempt.Lease{Token: command.LeaseToken, Owner: "worker", FencingToken: command.FencingToken, ExpiresAt: time.Now().Add(time.Minute)}, value.err
}
func (value *fakeCoordinator) Complete(_ context.Context, command attempt.CompleteCommand) (bool, error) {
	value.complete = command
	return value.err == nil, value.err
}

type fakeGateway struct{ command runtimecontext.LoadCommand }

func (value *fakeGateway) Load(_ context.Context, command runtimecontext.LoadCommand) (runtimecontext.ExecutionContext, error) {
	value.command = command
	return runtimecontext.ExecutionContext{ContextVersion: 1, AttemptID: command.AttemptID}, nil
}
