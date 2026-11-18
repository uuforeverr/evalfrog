// Package sandbox adapts a dedicated OCI sandbox runtime. It must be deployed
// on a dedicated sandbox node; it is not a Worker-main-process Python runner.
package sandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	domainsandbox "github.com/uu999/evalfrog/internal/sandbox"
)

const managedLabel = "evalfrog.sandbox=python-v1"

var errOutputTooLarge = errors.New("sandbox output exceeds limit")

type CommandRunner interface {
	Run(context.Context, string, []string, []byte, int64) ([]byte, []byte, error)
}

type DockerOrchestrator struct {
	Command string
	Profile domainsandbox.Profile
	Runner  CommandRunner
}

func NewDockerOrchestrator(command string, profile domainsandbox.Profile) (DockerOrchestrator, error) {
	if command == "" || !profile.Valid() {
		return DockerOrchestrator{}, fmt.Errorf("sandbox command and fixed profile are required")
	}
	return DockerOrchestrator{Command: command, Profile: profile, Runner: systemRunner{}}, nil
}

func (orchestrator DockerOrchestrator) Run(ctx context.Context, request domainsandbox.Request) (domainsandbox.Result, error) {
	if request.AttemptID == "" || request.SourceCode == "" || orchestrator.Runner == nil {
		return domainsandbox.Result{}, fmt.Errorf("sandbox request, profile, and runner are required")
	}
	name := containerName(request.AttemptID)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), orchestrator.Profile.CleanupTimeout)
		defer cancel()
		_ = orchestrator.cleanupName(cleanupCtx, name)
	}()
	payload, err := json.Marshal(struct {
		SourceCode string                     `json:"source_code"`
		Inputs     map[string]json.RawMessage `json:"inputs"`
	}{request.SourceCode, request.Inputs})
	if err != nil {
		return domainsandbox.Result{}, err
	}
	started := time.Now()
	stdout, stderr, err := orchestrator.Runner.Run(ctx, orchestrator.Command, orchestrator.runArgs(name), payload, orchestrator.Profile.OutputBytes)
	result := domainsandbox.Result{Telemetry: domainsandbox.Telemetry{Runtime: orchestrator.Profile.Runtime, ContainerID: name, Duration: time.Since(started)}}
	if errors.Is(err, errOutputTooLarge) {
		result.Failure = &domainsandbox.Failure{Code: "SANDBOX_OUTPUT_TOO_LARGE", Message: "sandbox output exceeds the fixed limit"}
		return result, nil
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		result.Failure = &domainsandbox.Failure{Code: "SANDBOX_EXECUTION_TIMEOUT", Message: "sandbox execution exceeded the attempt timeout"}
		return result, nil
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		result.Failure = &domainsandbox.Failure{Code: "SANDBOX_CANCELED", Message: "sandbox execution was canceled"}
		return result, nil
	}
	if err != nil {
		_ = stderr // Docker output is intentionally not passed to the workflow error surface.
		result.Failure = &domainsandbox.Failure{Code: "SANDBOX_RUNTIME_UNAVAILABLE", Message: "sandbox runtime could not execute the attempt"}
		return result, nil
	}
	return decodeFrame(stdout, result)
}

func (orchestrator DockerOrchestrator) Cleanup(ctx context.Context, attemptID string) error {
	if attemptID == "" {
		return fmt.Errorf("attempt ID is required")
	}
	return orchestrator.cleanupName(ctx, containerName(attemptID))
}

// Check proves that the dedicated Sandbox Runtime Controller can reach its
// container runtime. It is never called by a Worker process.
func (orchestrator DockerOrchestrator) Check(ctx context.Context) error {
	if orchestrator.Runner == nil {
		return fmt.Errorf("sandbox command runner is required")
	}
	_, _, err := orchestrator.Runner.Run(ctx, orchestrator.Command, []string{"version", "--format", "{{.Server.Version}}"}, nil, 4<<10)
	if err != nil {
		return fmt.Errorf("sandbox container runtime: %w", err)
	}
	return nil
}

func (orchestrator DockerOrchestrator) Sweep(ctx context.Context) error {
	if orchestrator.Runner == nil {
		return fmt.Errorf("sandbox command runner is required")
	}
	// Do not touch running containers: another Worker replica may own a valid
	// Attempt. The fixed runner timeout bounds a crashed Worker's orphan; a
	// later startup reaps it after it exits.
	stdout, _, err := orchestrator.Runner.Run(ctx, orchestrator.Command, []string{"ps", "-aq", "--filter", "label=" + managedLabel, "--filter", "status=exited"}, nil, 64<<10)
	if err != nil || len(bytes.TrimSpace(stdout)) == 0 {
		return err
	}
	for _, id := range strings.Fields(string(stdout)) {
		if _, _, removeErr := orchestrator.Runner.Run(ctx, orchestrator.Command, []string{"rm", "-f", "--volumes", id}, nil, 64<<10); removeErr != nil {
			return removeErr
		}
	}
	return nil
}

func (orchestrator DockerOrchestrator) cleanupName(ctx context.Context, name string) error {
	_, _, err := orchestrator.Runner.Run(ctx, orchestrator.Command, []string{"rm", "-f", "--volumes", name}, nil, 64<<10)
	if err == nil {
		return nil
	}
	// Docker returns a non-zero status when a retry races with a successful
	// cleanup. Confirm the name is absent so the controller's DELETE endpoint
	// remains idempotent without masking a live daemon or permission failure.
	remaining, _, inspectErr := orchestrator.Runner.Run(ctx, orchestrator.Command, []string{"ps", "-aq", "--filter", "name=^/" + name + "$"}, nil, 64<<10)
	if inspectErr == nil && len(bytes.TrimSpace(remaining)) == 0 {
		return nil
	}
	return err
}

func (orchestrator DockerOrchestrator) runArgs(name string) []string {
	profile := orchestrator.Profile
	return []string{
		"run", "-i", "--name", name,
		"--label", managedLabel,
		"--network", "none", "--read-only",
		"--tmpfs", fmt.Sprintf("/tmp:rw,noexec,nosuid,nodev,size=%d", profile.TemporaryBytes),
		"--tmpfs", "/run:rw,noexec,nosuid,nodev,size=1048576",
		"--pids-limit", fmt.Sprintf("%d", profile.ProcessLimit),
		"--memory", fmt.Sprintf("%d", profile.MemoryBytes), "--memory-swap", fmt.Sprintf("%d", profile.MemoryBytes),
		"--cpus", profile.CPU, "--ulimit", "nofile=64:64",
		"--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--user", "65532:65532",
		"--env", "PYTHONDONTWRITEBYTECODE=1", "--env", "PYTHONHASHSEED=0",
		"--env", fmt.Sprintf("EVALFROG_SANDBOX_TIMEOUT_MS=%d", profile.ExecutionTimeout.Milliseconds()),
		"--runtime", profile.Runtime, profile.Image,
	}
}

func containerName(attemptID string) string {
	digest := sha256.Sum256([]byte(attemptID))
	return "evalfrog-sbx-" + hex.EncodeToString(digest[:12])
}

func decodeFrame(raw json.RawMessage, result domainsandbox.Result) (domainsandbox.Result, error) {
	var frame struct {
		Status  string          `json:"status"`
		Output  json.RawMessage `json:"output"`
		Code    string          `json:"code"`
		Message string          `json:"message"`
		Details map[string]any  `json:"details"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil || (frame.Status != "ok" && frame.Status != "error") {
		result.Failure = &domainsandbox.Failure{Code: "SANDBOX_PROTOCOL_ERROR", Message: "sandbox returned an invalid result"}
		return result, nil
	}
	if frame.Status == "error" {
		if frame.Code == "" || frame.Message == "" {
			result.Failure = &domainsandbox.Failure{Code: "SANDBOX_PROTOCOL_ERROR", Message: "sandbox returned an invalid failure"}
			return result, nil
		}
		result.Failure = &domainsandbox.Failure{Code: frame.Code, Message: frame.Message, Details: frame.Details}
		return result, nil
	}
	result.Outputs = append(json.RawMessage(nil), frame.Output...)
	return result, nil
}

type systemRunner struct{}

func (systemRunner) Run(ctx context.Context, command string, arguments []string, input []byte, limit int64) ([]byte, []byte, error) {
	process := exec.CommandContext(ctx, command, arguments...)
	process.Stdin = bytes.NewReader(input)
	stdout, stderr := &limitedBuffer{limit: limit}, &limitedBuffer{limit: limit}
	process.Stdout, process.Stderr = stdout, stderr
	err := process.Run()
	if stdout.exceeded || stderr.exceeded {
		return stdout.Bytes(), stderr.Bytes(), errOutputTooLarge
	}
	return stdout.Bytes(), stderr.Bytes(), err
}

type limitedBuffer struct {
	bytes.Buffer
	limit    int64
	exceeded bool
}

func (buffer *limitedBuffer) Write(value []byte) (int, error) {
	if int64(buffer.Len()+len(value)) > buffer.limit {
		remaining := max(0, int(buffer.limit)-buffer.Len())
		_, _ = buffer.Buffer.Write(value[:remaining])
		buffer.exceeded = true
		return len(value), errOutputTooLarge
	}
	return buffer.Buffer.Write(value)
}

var _ domainsandbox.Orchestrator = DockerOrchestrator{}
var _ domainsandbox.OrphanSweeper = DockerOrchestrator{}
