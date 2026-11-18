//go:build integration

package sandbox

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	domainsandbox "github.com/uu999/evalfrog/internal/sandbox"
)

func TestDockerSandboxIntegration(t *testing.T) {
	profile := domainsandbox.DefaultProfile("evalfrog-sandbox-python:test", sandboxIntegrationRuntime(t))
	orchestrator, err := NewDockerOrchestrator("docker", profile)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := orchestrator.Run(ctx, domainsandbox.Request{AttemptID: "integration-success", SourceCode: "def main(inputs):\n    return {'total': sum(inputs['items'])}", Inputs: map[string]json.RawMessage{"items": json.RawMessage(`[1,2,3]`)}})
	if err != nil || result.Failure != nil || string(result.Outputs) != `{"total":6}` {
		t.Fatalf("success result=%#v err=%v", result, err)
	}
	result, err = orchestrator.Run(ctx, domainsandbox.Request{AttemptID: "integration-forbidden", SourceCode: "import socket\ndef main(inputs):\n    return {}", Inputs: map[string]json.RawMessage{}})
	if err != nil || result.Failure == nil || result.Failure.Code != "CODE_IMPORT_FORBIDDEN" {
		t.Fatalf("forbidden result=%#v err=%v", result, err)
	}
	result, err = orchestrator.Run(ctx, domainsandbox.Request{AttemptID: "integration-filesystem", SourceCode: "def main(inputs):\n    return open('/tmp/x', 'w')", Inputs: map[string]json.RawMessage{}})
	if err != nil || result.Failure == nil || result.Failure.Code != "CODE_RUNTIME_ERROR" || result.Failure.Details["source_line"] != float64(2) {
		t.Fatalf("filesystem result=%#v err=%v", result, err)
	}
	// Do not mistake the Python allowlist for the security boundary. This probe
	// intentionally recovers Python's import machinery through a standard
	// runtime object, then checks that OCI isolation still prevents network
	// egress, Docker-socket access, root writes, and controller-token exposure.
	// It is deliberately executed with whichever runtime the release gate chose.
	result, err = orchestrator.Run(ctx, domainsandbox.Request{AttemptID: "integration-containment", SourceCode: containmentProbeSource, Inputs: map[string]json.RawMessage{}})
	if err != nil || result.Failure != nil {
		t.Fatalf("containment probe result=%#v err=%v", result, err)
	}
	var containment struct {
		NetworkEgress       bool `json:"network_egress"`
		DockerSocket        bool `json:"docker_socket"`
		RootWritable        bool `json:"root_writable"`
		ControllerTokenSeen bool `json:"controller_token_seen"`
	}
	if err := json.Unmarshal(result.Outputs, &containment); err != nil || containment.NetworkEgress || containment.DockerSocket || containment.RootWritable || containment.ControllerTokenSeen {
		t.Fatalf("sandbox containment=%+v output=%s err=%v", containment, result.Outputs, err)
	}
	profile.ExecutionTimeout = 50 * time.Millisecond
	orchestrator, err = NewDockerOrchestrator("docker", profile)
	if err != nil {
		t.Fatal(err)
	}
	result, err = orchestrator.Run(ctx, domainsandbox.Request{AttemptID: "integration-timeout", SourceCode: "def main(inputs):\n    while True:\n        pass", Inputs: map[string]json.RawMessage{}})
	if err != nil || result.Failure == nil || result.Failure.Code != "SANDBOX_EXECUTION_TIMEOUT" {
		t.Fatalf("timeout result=%#v err=%v", result, err)
	}
}

const containmentProbeSource = `def main(inputs):
    loader = [value for value in ().__class__.__bases__[0].__subclasses__() if value.__name__ == 'FileLoader'][0]
    globals_ = loader.__init__.__globals__
    builtins_ = globals_['__builtins__']
    system = builtins_['__import__']('os')
    root_writable = False
    try:
        handle = builtins_['open']('/opt/evalfrog/containment-probe', 'w')
        handle.close()
        root_writable = True
    except:
        pass
    network_egress = False
    try:
        socket = builtins_['__import__']('socket')
        connection = socket.create_connection(('198.51.100.1', 1), 0.1)
        connection.close()
        network_egress = True
    except:
        pass
    return {
        'network_egress': network_egress,
        'docker_socket': system.path.exists('/var/run/docker.sock'),
        'root_writable': root_writable,
        'controller_token_seen': 'EVALFROG_SANDBOX_RUNTIME_TOKEN' in system.environ,
    }`

// sandboxIntegrationRuntime defaults to runc for local protocol coverage.
// The dedicated release gate sets it to runsc and therefore executes the
// identical contract against the hardened OCI runtime rather than treating a
// mock or an argument assertion as runsc evidence.
func sandboxIntegrationRuntime(t *testing.T) string {
	t.Helper()
	runtime := os.Getenv("EVALFROG_SANDBOX_INTEGRATION_RUNTIME")
	if runtime == "" {
		return "runc"
	}
	if runtime != "runc" && runtime != "runsc" {
		t.Fatalf("unsupported integration sandbox runtime %q", runtime)
	}
	return runtime
}
