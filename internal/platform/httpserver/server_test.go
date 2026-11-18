package httpserver

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/platform/health"
	"github.com/uu999/evalfrog/internal/platform/metrics"
)

func TestServerStartsAndShutsDown(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := New("test", "127.0.0.1:0", time.Second, time.Second, logger, health.New(time.Second), metrics.New("test"))
	server.Handle("/v1/", http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run(context.Background()) }()
	select {
	case <-server.Ready():
	case <-time.After(time.Second):
		t.Fatal("server did not start")
	}
	response, err := http.Get("http://" + server.Address() + "/health/live")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", response.StatusCode)
	}
	apiResponse, err := http.Get("http://" + server.Address() + "/v1/probe")
	if err != nil {
		t.Fatal(err)
	}
	apiResponse.Body.Close()
	if apiResponse.StatusCode != http.StatusNoContent {
		t.Fatalf("API status=%d", apiResponse.StatusCode)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}

func TestMetricRouteNeverUsesTenantOrRunIdentifiers(t *testing.T) {
	for path, want := range map[string]string{
		"/v1/projects/project-1/workflows/workflow-1/runs": "/v1/projects/{project_id}/workflows/{workflow_id}/runs",
		"/v1/projects/project-1/runs/run-1/diagnostics":    "/v1/projects/{project_id}/runs/{run_id}/diagnostics",
		"/v1/attempts/attempt-1/execute":                   "/v1/attempts/{attempt_id}/execute",
		"/health/ready":                                    "/health/ready",
	} {
		if got := metricRoute(path); got != want {
			t.Fatalf("path=%q got=%q want=%q", path, got, want)
		}
	}
}
