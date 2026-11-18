package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/uu999/evalfrog/internal/platform/buildinfo"
	"github.com/uu999/evalfrog/internal/platform/health"
	"github.com/uu999/evalfrog/internal/platform/identity"
	"github.com/uu999/evalfrog/internal/platform/metrics"
	"github.com/uu999/evalfrog/internal/platform/traceid"
)

type Server struct {
	service   string
	address   string
	logger    *slog.Logger
	readiness *health.Registry
	metrics   *metrics.Registry
	http      *http.Server
	mux       *http.ServeMux
	ready     chan struct{}
	mu        sync.RWMutex
	bound     string
}

func New(service, address string, readHeaderTimeout, idleTimeout time.Duration, logger *slog.Logger, readiness *health.Registry, metricRegistry *metrics.Registry) *Server {
	server := &Server{
		service: service, address: address, logger: logger, readiness: readiness, metrics: metricRegistry, ready: make(chan struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", server.info)
	mux.HandleFunc("GET /health/live", server.live)
	mux.HandleFunc("GET /health/ready", server.readyHandler)
	mux.Handle("GET /metrics", metricRegistry.Handler())
	handler := traceid.Middleware(identity.UUIDv7Generator{}, server.observe(mux))
	server.http = &http.Server{
		Addr: address, Handler: handler, ReadHeaderTimeout: readHeaderTimeout, IdleTimeout: idleTimeout,
	}
	server.mux = mux
	return server
}

// Handle mounts an application adapter before the server starts. Platform
// health and metrics remain owned by this package; domain APIs own their own
// versioned paths below the mounted prefix.
func (server *Server) Handle(pattern string, handler http.Handler) {
	server.mux.Handle(pattern, handler)
}

func (server *Server) Name() string { return "http-server" }

func (server *Server) Run(context.Context) error {
	listener, err := net.Listen("tcp", server.address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", server.address, err)
	}
	server.mu.Lock()
	server.bound = listener.Addr().String()
	server.mu.Unlock()
	close(server.ready)
	server.logger.Info("HTTP server ready", "address", listener.Addr().String())
	err = server.http.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (server *Server) Shutdown(ctx context.Context) error {
	return server.http.Shutdown(ctx)
}

func (server *Server) Ready() <-chan struct{} { return server.ready }

func (server *Server) Address() string {
	server.mu.RLock()
	defer server.mu.RUnlock()
	return server.bound
}

func (server *Server) info(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"service": server.service, "build": buildinfo.Current(), "status": "m0-shell"})
}

func (server *Server) live(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok", "service": server.service})
}

func (server *Server) readyHandler(writer http.ResponseWriter, request *http.Request) {
	report := server.readiness.Check(request.Context())
	status := http.StatusOK
	if report.Status != "ok" {
		status = http.StatusServiceUnavailable
	}
	writeJSON(writer, status, report)
}

func (server *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		recorder := &statusRecorder{ResponseWriter: writer, status: http.StatusOK}
		next.ServeHTTP(recorder, request)
		server.metrics.Requests.WithLabelValues(server.service, metricRoute(request.URL.Path), strconv.Itoa(recorder.status)).Inc()
	})
}

// metricRoute removes tenant and execution identifiers before they become a
// Prometheus label. A raw URL path would create one time series per Project,
// Run or Workflow and eventually make the observability system unhealthy.
func metricRoute(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for index := 0; index+1 < len(parts); index++ {
		switch parts[index] {
		case "projects":
			parts[index+1] = "{project_id}"
		case "workflows":
			parts[index+1] = "{workflow_id}"
		case "runs":
			parts[index+1] = "{run_id}"
		case "versions":
			parts[index+1] = "{version_number}"
		case "attempts":
			parts[index+1] = "{attempt_id}"
		}
	}
	if len(parts) == 0 || parts[0] == "" {
		return "/"
	}
	return "/" + strings.Join(parts, "/")
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (recorder *statusRecorder) WriteHeader(status int) {
	recorder.status = status
	recorder.ResponseWriter.WriteHeader(status)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
