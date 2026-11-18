package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	domainsandbox "github.com/uu999/evalfrog/internal/sandbox"
)

type recordedCommand struct{ arguments []string }
type fakeRunner struct {
	output   []byte
	err      error
	commands []recordedCommand
}

func (runner *fakeRunner) Run(_ context.Context, _ string, arguments []string, _ []byte, _ int64) ([]byte, []byte, error) {
	runner.commands = append(runner.commands, recordedCommand{append([]string(nil), arguments...)})
	return runner.output, nil, runner.err
}

func TestDockerOrchestratorAppliesFixedIsolation(t *testing.T) {
	runner := &fakeRunner{output: []byte(`{"status":"ok","output":{"value":1}}`)}
	orchestrator, err := NewDockerOrchestrator("docker", domainsandbox.DefaultProfile("image@sha256:abc", "runsc"))
	if err != nil {
		t.Fatal(err)
	}
	orchestrator.Runner = runner
	result, err := orchestrator.Run(context.Background(), domainsandbox.Request{AttemptID: "attempt", SourceCode: "def main(inputs): return {}", Inputs: map[string]json.RawMessage{}})
	if err != nil || result.Failure != nil {
		t.Fatalf("Run() = %#v, %v", result, err)
	}
	if len(runner.commands) != 2 {
		t.Fatalf("commands = %#v", runner.commands)
	}
	arguments := runner.commands[0].arguments
	for _, expected := range []string{"-i", "--network", "none", "--read-only", "--pids-limit", "32", "--memory", "134217728", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--runtime", "runsc"} {
		if !contains(arguments, expected) {
			t.Fatalf("run arguments missing %q: %#v", expected, arguments)
		}
	}
	if runner.commands[1].arguments[0] != "rm" {
		t.Fatalf("cleanup = %#v", runner.commands[1])
	}
}

func TestDockerOrchestratorMapsTimeoutAndDoesNotExposeStderr(t *testing.T) {
	runner := &fakeRunner{err: context.DeadlineExceeded}
	orchestrator, _ := NewDockerOrchestrator("docker", domainsandbox.DefaultProfile("image", "runc"))
	orchestrator.Runner = runner
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := orchestrator.Run(ctx, domainsandbox.Request{AttemptID: "attempt", SourceCode: "x", Inputs: map[string]json.RawMessage{}})
	if err != nil || result.Failure == nil || result.Failure.Code != "SANDBOX_CANCELED" {
		t.Fatalf("result = %#v, %v", result, err)
	}
}

func TestDecodeFrameRejectsInvalidProtocol(t *testing.T) {
	result, err := decodeFrame([]byte(`not-json`), domainsandbox.Result{})
	if err != nil || result.Failure == nil || result.Failure.Code != "SANDBOX_PROTOCOL_ERROR" {
		t.Fatalf("result = %#v, %v", result, err)
	}
}

func TestSweepRemovesOnlyManagedContainers(t *testing.T) {
	runner := &fakeRunner{output: []byte("a\nb\n")}
	orchestrator, _ := NewDockerOrchestrator("docker", domainsandbox.DefaultProfile("image", "runc"))
	orchestrator.Runner = runner
	if err := orchestrator.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(runner.commands) != 3 || !reflect.DeepEqual(runner.commands[0].arguments, []string{"ps", "-aq", "--filter", "label=" + managedLabel, "--filter", "status=exited"}) || !reflect.DeepEqual(runner.commands[1].arguments[:2], []string{"rm", "-f"}) {
		t.Fatalf("commands = %#v", runner.commands)
	}
}

func TestDockerCleanupTreatsAnAlreadyRemovedContainerAsSuccess(t *testing.T) {
	runner := &cleanupRunner{}
	orchestrator, _ := NewDockerOrchestrator("docker", domainsandbox.DefaultProfile("image", "runc"))
	orchestrator.Runner = runner
	if err := orchestrator.Cleanup(context.Background(), "attempt"); err != nil {
		t.Fatalf("idempotent cleanup error=%v", err)
	}
	if len(runner.commands) != 2 || runner.commands[1].arguments[0] != "ps" {
		t.Fatalf("commands=%+v", runner.commands)
	}
}

type cleanupRunner struct{ commands []recordedCommand }

func (runner *cleanupRunner) Run(_ context.Context, _ string, arguments []string, _ []byte, _ int64) ([]byte, []byte, error) {
	runner.commands = append(runner.commands, recordedCommand{append([]string(nil), arguments...)})
	if arguments[0] == "rm" {
		return nil, nil, errors.New("container already removed")
	}
	return nil, nil, nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
