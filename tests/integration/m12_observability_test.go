//go:build integration

package integration

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/adapters/mysql"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/platform/metrics"
)

// Capacity reporting needs an actual pool acquisition sample, not only a unit
// test of the observer callback. Pinging a real database/sql pool proves the
// Controller can export the bounded acquisition metric.
func TestM12MySQLPoolAcquireIsExported(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(config.LoadOptions{Directory: filepath.Join(root, "configs"), Profile: "local"})
	if err != nil {
		t.Fatal(err)
	}
	databaseName, dsn := isolatedMySQLDatabase(t)
	configuration.MySQL.DSN = dsn
	configuration.MySQL.Database = databaseName
	registry := metrics.New("m12-mysql-observability")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := mysql.OpenWithAcquireObserver(ctx, configuration.MySQL, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Check(ctx); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, request)
	body, err := io.ReadAll(response.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || !strings.Contains(string(body), `evalfrog_mysql_pool_acquire_seconds_count{outcome="success"} `) {
		t.Fatalf("MySQL pool metric is missing after a real acquire: status=%d body=%s", response.Code, body)
	}
}
