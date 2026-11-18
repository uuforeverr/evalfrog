//go:build integration && stress

package sandbox

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	domainsandbox "github.com/uu999/evalfrog/internal/sandbox"
)

// TestDockerSandboxRuncCreateDestroyStress performs the local/CI protocol
// stress check required before a release candidate can enter the dedicated
// runsc security environment. It is deliberately opt-in: ordinary PR
// integration must not create one thousand containers on every run.
//
// This is not evidence for the production runsc escape gate. The runtime is
// intentionally explicit in both the test name and profile so its result
// cannot be mistaken for hardened-runtime coverage.
func TestDockerSandboxRuncCreateDestroyStress(t *testing.T) {
	if os.Getenv("EVALFROG_M12_SANDBOX_STRESS") != "1" {
		t.Skip("set EVALFROG_M12_SANDBOX_STRESS=1 to run the 1000-container runc stress check")
	}
	runSandboxCreateDestroyStress(t, "runc")
}

// TestDockerSandboxRunscCreateDestroyStress is intentionally separate from
// the local runc check: production evidence must prove that the identical
// per-attempt lifecycle works through the hardened runtime itself.
func TestDockerSandboxRunscCreateDestroyStress(t *testing.T) {
	if os.Getenv("EVALFROG_M12_RUNSC_STRESS") != "1" {
		t.Skip("set EVALFROG_M12_RUNSC_STRESS=1 to run the 1000-container runsc stress check")
	}
	runSandboxCreateDestroyStress(t, "runsc")
}

func runSandboxCreateDestroyStress(t *testing.T, runtime string) {
	t.Helper()
	profile := domainsandbox.DefaultProfile("evalfrog-sandbox-python:test", runtime)
	orchestrator, err := NewDockerOrchestrator("docker", profile)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	started := time.Now()
	for index := 0; index < 1000; index++ {
		result, runErr := orchestrator.Run(ctx, domainsandbox.Request{
			AttemptID:  stressAttemptID(runtime, index),
			SourceCode: "def main(inputs):\n    return {'sequence': inputs['sequence']}",
			Inputs: map[string]json.RawMessage{
				"sequence": json.RawMessage("0"),
			},
		})
		if runErr != nil || result.Failure != nil || string(result.Outputs) != `{"sequence":0}` {
			t.Fatalf("attempt %d result=%#v err=%v", index, result, runErr)
		}
		if (index+1)%100 == 0 {
			t.Logf("completed %d/1000 %s sandbox create/destroy cycles", index+1, runtime)
		}
	}

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cleanupCancel()
	if err := orchestrator.Sweep(cleanupCtx); err != nil {
		t.Fatalf("sweep exited sandbox containers: %v", err)
	}
	for index := 0; index < 1000; index++ {
		name := containerName(stressAttemptID(runtime, index))
		remaining, _, inspectErr := orchestrator.Runner.Run(cleanupCtx, "docker", []string{"ps", "-aq", "--filter", "name=^/" + name + "$"}, nil, 4<<10)
		if inspectErr != nil {
			t.Fatalf("inspect cleanup for attempt %d: %v", index, inspectErr)
		}
		if len(remaining) != 0 {
			t.Fatalf("sandbox container leaked for attempt %d: %s", index, name)
		}
	}
	t.Logf("completed 1000 %s sandbox create/destroy cycles in %s", runtime, time.Since(started).Round(time.Millisecond))
}

func stressAttemptID(runtime string, index int) string {
	return fmt.Sprintf("m12-%s-stress-%04d", runtime, index)
}
