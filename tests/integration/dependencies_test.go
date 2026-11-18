//go:build integration

package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/adapters/kafka"
	"github.com/uu999/evalfrog/internal/adapters/mysql"
	"github.com/uu999/evalfrog/internal/platform/config"
	"github.com/uu999/evalfrog/internal/platform/migrations"
)

func TestLocalDependenciesAndMigrationRunner(t *testing.T) {
	databaseName, dsn := isolatedMySQLDatabase(t)
	t.Setenv("EVALFROG_MYSQL_DSN", dsn)
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := config.Load(config.LoadOptions{Directory: filepath.Join(root, "configs"), Profile: "local"})
	if err != nil {
		t.Fatal(err)
	}
	configuration.MySQL.Database = databaseName
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	mysqlClient, err := mysql.Open(ctx, configuration.MySQL)
	if err != nil {
		t.Fatal(err)
	}
	defer mysqlClient.Close()
	if err := mysqlClient.Check(ctx); err != nil {
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

	directory := t.TempDir()
	migrationPath := filepath.Join(directory, "000001_create_probe.up.sql")
	if err := os.WriteFile(migrationPath, []byte("CREATE TABLE probe (id BIGINT PRIMARY KEY);"), 0o600); err != nil {
		t.Fatal(err)
	}
	runner := migrations.Runner{Database: mysqlClient.Pool().Raw(), DatabaseName: databaseName, Directory: directory, LockTimeout: 5 * time.Second}
	if err := runner.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runner.Up(ctx); err != nil {
		t.Fatalf("idempotent rerun failed: %v", err)
	}
	var count int
	if err := mysqlClient.Pool().QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&count); err != nil {
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
