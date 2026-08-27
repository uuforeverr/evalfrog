package process

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/uu999/evalfrog/internal/adapters/kafka"
	sandboxadapter "github.com/uu999/evalfrog/internal/adapters/sandbox"
	"github.com/uu999/evalfrog/internal/adapters/workerapi"
	"github.com/uu999/evalfrog/internal/platform/bootstrap"
	"github.com/uu999/evalfrog/internal/platform/buildinfo"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/platform/health"
	"github.com/uu999/evalfrog/internal/platform/httpserver"
	"github.com/uu999/evalfrog/internal/platform/identity"
	"github.com/uu999/evalfrog/internal/platform/lifecycle"
	"github.com/uu999/evalfrog/internal/platform/logging"
	"github.com/uu999/evalfrog/internal/platform/metrics"
	"github.com/uu999/evalfrog/internal/sandbox"
	"github.com/uu999/evalfrog/internal/scheduling"
	codeexecutor "github.com/uu999/evalfrog/internal/worker/executor/code"
	hexecutor "github.com/uu999/evalfrog/internal/worker/executor/http"
	rpcexecutor "github.com/uu999/evalfrog/internal/worker/executor/rpc"
	workerruntime "github.com/uu999/evalfrog/internal/worker/runtime"
)

func RunProcess(ctx context.Context, arguments []string, resourceClass string, output, errorOutput io.Writer) int {
	service := "evalfrog-worker-" + resourceClass
	options, err := bootstrap.Parse(service, arguments, errorOutput)
	if err != nil {
		return 2
	}
	if options.HealthcheckURL != "" {
		if err := bootstrap.Probe(ctx, options.HealthcheckURL); err != nil {
			fmt.Fprintln(errorOutput, err)
			return 1
		}
		return 0
	}
	if options.Migrate {
		fmt.Fprintln(errorOutput, "workers cannot run PostgreSQL migrations")
		return 2
	}
	configuration, err := bootstrap.Load(options)
	if err != nil {
		fmt.Fprintln(errorOutput, err)
		return 1
	}
	if options.CheckConfig {
		if resourceClass == string(scheduling.ResourceSandbox) && configuration.Profile == config.ProductionDefault && configuration.Sandbox.RuntimeToken == "" {
			fmt.Fprintln(errorOutput, "production sandbox runtime token is required at process startup")
			return 1
		}
		fmt.Fprintf(output, "configuration valid: service=%s profile=%s\n", service, configuration.Profile)
		return 0
	}
	logger, err := logging.New(output, service, configuration.Observability.LogLevel)
	if err != nil {
		fmt.Fprintln(errorOutput, err)
		return 1
	}
	class := scheduling.ResourceClass(resourceClass)
	topic := configuration.Kafka.Topics.BuiltinTask
	slots := configuration.Worker.BuiltinSlots
	apiTimeout := max(configuration.Worker.ClaimTimeout.Duration(), configuration.Worker.CompleteTimeout.Duration())
	controlPlane := workerapi.New(configuration.Endpoints.ControlPlaneURL, apiTimeout)
	executors := []workerruntime.Executor{
		hexecutor.NewExecutor(controlPlane, nil),
		rpcexecutor.NewExecutor(controlPlane, rpcexecutor.JSONInvoker{}),
	}
	if class == scheduling.ResourceSandbox {
		topic = configuration.Kafka.Topics.SandboxTask
		slots = configuration.Worker.SandboxSlots
		orchestrator, constructionErr := newSandboxOrchestrator(configuration)
		if constructionErr != nil {
			fmt.Fprintln(errorOutput, constructionErr)
			return 1
		}
		// Only the controller directly owns an OCI runtime and can sweep
		// orphaned containers. The Worker deliberately has no Docker client or
		// socket, so an HTTP controller client must not turn startup into a
		// misleading best-effort Docker check.
		if sweeper, supported := any(orchestrator).(sandbox.OrphanSweeper); supported {
			sweepCtx, cancelSweep := context.WithTimeout(ctx, sandbox.DefaultProfile(configuration.Sandbox.Image, configuration.Sandbox.Runtime).CleanupTimeout)
			sweepErr := sweeper.Sweep(sweepCtx)
			cancelSweep()
			if sweepErr != nil {
				logger.Warn("sandbox orphan sweep deferred; runtime will retry on next worker start", "error", sweepErr)
			}
		}
		executors = []workerruntime.Executor{codeexecutor.NewExecutor(orchestrator, sandboxTelemetry{logger: logger})}
	}
	if !class.Valid() {
		logger.Error("worker resource class is invalid", "resource_class", resourceClass)
		return 1
	}
	// A fetched batch holds the Kafka rebalance gate until every record has
	// reached Claim+ACK. Keep the theoretical worst-case claim time below half
	// the rebalance window as well as below the local free-slot bound.
	safePollLimit := int(configuration.Kafka.MaxPollInterval.Duration() / configuration.Worker.ClaimTimeout.Duration() / 2)
	pollLimit := min(slots, configuration.Worker.TaskPollLimit, configuration.Kafka.TaskMaxPollRecords, max(1, safePollLimit))
	kafkaClient, err := kafka.OpenConsumer(configuration.Kafka, service, "worker-"+resourceClass+"-v1", []config.KafkaTopicConfig{topic}, pollLimit)
	if err != nil {
		logger.Error("Kafka client creation failed", "error", err)
		return 1
	}
	defer kafkaClient.Close()
	readiness := health.New(configuration.Redis.Cache.OperationTimeout.Duration())
	mustRegister(logger, readiness, "kafka", kafkaClient.Check)
	mustRegister(logger, readiness, "control-plane", controlPlane.Check)
	address := configuration.HTTP.BuiltinWorkerAddress
	if resourceClass == "sandbox" {
		address = configuration.HTTP.SandboxWorkerAddress
	}
	server := httpserver.New(
		service, address, configuration.HTTP.ReadHeaderTimeout.Duration(), configuration.HTTP.IdleTimeout.Duration(),
		logger, readiness, metrics.New(service),
	)
	workerID, err := (identity.UUIDv7Generator{}).New()
	if err != nil {
		logger.Error("worker identity generation failed", "error", err)
		return 1
	}
	catalog, err := workerruntime.NewCatalog(class, executors...)
	if err != nil {
		logger.Error("worker executor catalog invalid", "error", err)
		return 1
	}
	version := buildinfo.Current().Version + "+" + buildinfo.Current().Commit
	workerRuntime, err := workerruntime.New(kafkaClient, controlPlane, catalog, workerruntime.Settings{
		WorkerID: workerID, ExecutorBuild: version, ResourceClass: class, Slots: slots,
		LeaseDuration: configuration.Worker.LeaseDuration.Duration(), HeartbeatInterval: configuration.Worker.HeartbeatInterval.Duration(),
		ClaimTimeout: configuration.Worker.ClaimTimeout.Duration(), CompleteTimeout: configuration.Worker.CompleteTimeout.Duration(),
	}, logger)
	if err != nil {
		logger.Error("worker runtime construction failed", "error", err)
		return 1
	}
	if err := lifecycle.Run(ctx, configuration.Shutdown.Timeout.Duration(), logger, server, workerRuntime); err != nil {
		logger.Error("worker stopped with error", "error", err)
		return 1
	}
	return 0
}

// RunSandboxRuntime is the only process role that owns the Docker / OCI
// credential in local development. It intentionally reuses the worker-sandbox
// binary so the first phase retains four product executables; production
// deploys this role on dedicated sandbox nodes behind the private HTTPS port.
func RunSandboxRuntime(ctx context.Context, arguments []string, output, errorOutput io.Writer) int {
	const service = "evalfrog-sandbox-runtime"
	options, err := bootstrap.Parse(service, arguments, errorOutput)
	if err != nil {
		return 2
	}
	if options.HealthcheckURL != "" {
		if err := bootstrap.Probe(ctx, options.HealthcheckURL); err != nil {
			fmt.Fprintln(errorOutput, err)
			return 1
		}
		return 0
	}
	if options.Migrate {
		fmt.Fprintln(errorOutput, "sandbox runtime cannot run PostgreSQL migrations")
		return 2
	}
	configuration, err := bootstrap.Load(options)
	if err != nil {
		fmt.Fprintln(errorOutput, err)
		return 1
	}
	if options.CheckConfig {
		if configuration.Profile == config.ProductionDefault && configuration.Sandbox.RuntimeToken == "" {
			fmt.Fprintln(errorOutput, "production sandbox runtime token is required at process startup")
			return 1
		}
		fmt.Fprintf(output, "configuration valid: service=%s profile=%s\n", service, configuration.Profile)
		return 0
	}
	logger, err := logging.New(output, service, configuration.Observability.LogLevel)
	if err != nil {
		fmt.Fprintln(errorOutput, err)
		return 1
	}
	orchestrator, err := sandboxadapter.NewDockerOrchestrator(configuration.Sandbox.Command, sandbox.DefaultProfile(configuration.Sandbox.Image, configuration.Sandbox.Runtime))
	if err != nil {
		fmt.Fprintln(errorOutput, err)
		return 1
	}
	// The controller, rather than the socket-free Worker, owns orphan cleanup.
	// Only exited containers with the platform label are eligible, so a
	// controller restart cannot delete a live Attempt owned by another replica.
	sweepCtx, cancelSweep := context.WithTimeout(ctx, sandbox.DefaultProfile(configuration.Sandbox.Image, configuration.Sandbox.Runtime).CleanupTimeout)
	if sweepErr := orchestrator.Sweep(sweepCtx); sweepErr != nil {
		logger.Warn("sandbox orphan sweep deferred; controller will retry on next startup", "error", sweepErr)
	}
	cancelSweep()
	readiness := health.New(configuration.Shutdown.Timeout.Duration())
	mustRegister(logger, readiness, "sandbox-container-runtime", orchestrator.Check)
	server := httpserver.New(service, configuration.HTTP.SandboxRuntimeAddress,
		configuration.HTTP.ReadHeaderTimeout.Duration(), configuration.HTTP.IdleTimeout.Duration(), logger, readiness, metrics.New(service))
	handler, err := sandboxadapter.NewRuntimeHandler(orchestrator, configuration.Sandbox.RuntimeToken)
	if err != nil {
		logger.Error("sandbox runtime handler construction failed", "error", err)
		return 1
	}
	server.Handle("/v1/", handler)
	if err := lifecycle.Run(ctx, configuration.Shutdown.Timeout.Duration(), logger, server); err != nil {
		logger.Error("sandbox runtime stopped with error", "error", err)
		return 1
	}
	return 0
}

func newSandboxOrchestrator(configuration config.Config) (sandbox.Orchestrator, error) {
	profile := sandbox.DefaultProfile(configuration.Sandbox.Image, configuration.Sandbox.Runtime)
	if configuration.Sandbox.RuntimeURL != "" {
		if configuration.Sandbox.RuntimeToken == "" {
			return nil, fmt.Errorf("sandbox runtime token is required")
		}
		return sandboxadapter.NewHTTPOrchestrator(configuration.Sandbox.RuntimeURL, profile, nil, configuration.Profile != config.ProductionDefault, configuration.Sandbox.RuntimeToken)
	}
	return sandboxadapter.NewDockerOrchestrator(configuration.Sandbox.Command, profile)
}

type sandboxTelemetry struct{ logger *slog.Logger }

func (telemetry sandboxTelemetry) Record(value sandbox.Telemetry, outcome string) {
	telemetry.logger.Info("sandbox attempt completed", "outcome", outcome, "runtime", value.Runtime, "duration_ms", value.Duration.Milliseconds())
}

func mustRegister(logger *slog.Logger, registry *health.Registry, name string, check health.Check) {
	if err := registry.Register(name, check); err != nil {
		logger.Error("health check registration failed", "check", name, "error", err)
		panic(err)
	}
}
