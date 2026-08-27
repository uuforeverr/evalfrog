//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/uu999/evalfrog/internal/adapters/cacheredis"
	"github.com/uu999/evalfrog/internal/adapters/kafka"
	"github.com/uu999/evalfrog/internal/adapters/postgres"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/platform/migrations"
)

func TestLocalDependenciesAndMigrationRunner(t *testing.T) {
	dsn := os.Getenv("EVALFROG_INTEGRATION_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://evalfrog:evalfrog@localhost:15432/evalfrog?sslmode=disable"
	}
	t.Setenv("EVALFROG_POSTGRES_DSN", dsn)
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(config.LoadOptions{Directory: filepath.Join(root, "configs"), Profile: "local"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	postgresClient, err := postgres.Open(ctx, configuration.Postgres)
	if err != nil {
		t.Fatal(err)
	}
	defer postgresClient.Close()
	if err := postgresClient.Check(ctx); err != nil {
		t.Fatal(err)
	}
	cacheClient := cacheredis.Open(configuration.Redis.Cache)
	defer cacheClient.Close()
	if err := cacheClient.Check(ctx); err != nil {
		t.Fatal(err)
	}
	kafkaClient, err := kafka.Open(configuration.Kafka, "m0-integration-test")
	if err != nil {
		t.Fatal(err)
	}
	defer kafkaClient.Close()
	if err := kafkaClient.Check(ctx); err != nil {
		t.Fatal(err)
	}

	schema := "m0_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	identifier := pgx.Identifier{schema}.Sanitize()
	t.Cleanup(func() {
		_, _ = postgresClient.Pool().Exec(context.Background(), "DROP SCHEMA IF EXISTS "+identifier+" CASCADE")
	})
	directory := t.TempDir()
	migrationPath := filepath.Join(directory, "000001_create_probe.up.sql")
	if err := os.WriteFile(migrationPath, []byte("CREATE TABLE probe (id BIGINT PRIMARY KEY);"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := migrations.Runner{Pool: postgresClient.Pool(), Schema: schema, Directory: directory, LockTimeout: 5 * time.Second}
	if err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("idempotent rerun failed: %v", err)
	}
	var count int
	if err := postgresClient.Pool().QueryRow(ctx, "SELECT count(*) FROM "+identifier+".schema_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("migration count=%d, want 1", count)
	}
	if err := os.WriteFile(migrationPath, []byte("CREATE TABLE changed (id BIGINT PRIMARY KEY);"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runner.Up(ctx); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch, got %v", err)
	}
}
