package process

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	sandboxadapter "github.com/uu999/evalfrog/internal/adapters/sandbox"
	"github.com/uu999/evalfrog/internal/platform/config"
	domainsandbox "github.com/uu999/evalfrog/internal/sandbox"
)

func TestWorkerProfilesValidateWithoutDatabaseAccess(t *testing.T) {
	t.Parallel()
	directory, err := filepath.Abs(filepath.Join("..", "..", "..", "configs"))
	if err != nil {
		t.Fatal(err)
	}
	for _, resourceClass := range []string{"builtin", "sandbox"} {
		resourceClass := resourceClass
		t.Run(resourceClass, func(t *testing.T) {
			t.Parallel()
			var output bytes.Buffer
			code := RunProcess(context.Background(), []string{"--profile", "local", "--config-dir", directory, "--check-config"}, resourceClass, &output, &output)
			if code != 0 || !strings.Contains(output.String(), "configuration valid") {
				t.Fatalf("code=%d output=%q", code, output.String())
			}
		})
	}
}

func TestProductionSandboxWorkerUsesHTTPSRuntimeClient(t *testing.T) {
	t.Parallel()
	directory, err := filepath.Abs(filepath.Join("..", "..", "..", "configs"))
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(config.LoadOptions{Directory: directory, Profile: config.ProductionDefault, LookupEnv: emptyEnvironment})
	if err != nil {
		t.Fatal(err)
	}
	configuration.Sandbox.RuntimeToken = "production-runtime-token"
	orchestrator, err := newSandboxOrchestrator(configuration)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := orchestrator.(sandboxadapter.HTTPOrchestrator); !ok {
		t.Fatalf("production orchestrator=%T, want HTTPOrchestrator", orchestrator)
	}
}

func TestProductionSandboxWorkerRequiresInjectedRuntimeToken(t *testing.T) {
	t.Parallel()
	directory, err := filepath.Abs(filepath.Join("..", "..", "..", "configs"))
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(config.LoadOptions{Directory: directory, Profile: config.ProductionDefault, LookupEnv: emptyEnvironment})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = newSandboxOrchestrator(configuration); err == nil {
		t.Fatal("production sandbox runtime accepted an absent injected token")
	}
}

func TestProductionBuiltinWorkerDoesNotRequireSandboxControllerCredential(t *testing.T) {
	t.Parallel()
	directory, err := filepath.Abs(filepath.Join("..", "..", "..", "configs"))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	code := RunProcess(context.Background(), []string{"--profile", config.ProductionDefault, "--config-dir", directory, "--check-config"}, "builtin", &output, &output)
	if code != 0 || !strings.Contains(output.String(), "configuration valid") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestSandboxRuntimeRoleValidatesWithoutAuthorityStoreAccess(t *testing.T) {
	t.Parallel()
	directory, err := filepath.Abs(filepath.Join("..", "..", "..", "configs"))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	code := RunSandboxRuntime(context.Background(), []string{"--profile", "local", "--config-dir", directory, "--check-config"}, &output, &output)
	if code != 0 || !strings.Contains(output.String(), "sandbox-runtime") {
		t.Fatalf("code=%d output=%q", code, output.String())
	}
}

func TestSandboxWorkerDoesNotAttemptDockerSweepWhenUsingController(t *testing.T) {
	t.Parallel()
	directory, err := filepath.Abs(filepath.Join("..", "..", "..", "configs"))
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(config.LoadOptions{Directory: directory, Profile: "local", LookupEnv: emptyEnvironment})
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := newSandboxOrchestrator(configuration)
	if err != nil {
		t.Fatal(err)
	}
	if _, ownsOCI := orchestrator.(domainsandbox.OrphanSweeper); ownsOCI {
		t.Fatalf("sandbox Worker controller client must not own OCI cleanup: %T", orchestrator)
	}
}

func emptyEnvironment(string) (string, bool) { return "", false }
