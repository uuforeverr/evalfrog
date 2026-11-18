//go:build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

func isolatedMySQLDatabase(t *testing.T) (string, string) {
	t.Helper()
	dsn := os.Getenv("EVALFROG_INTEGRATION_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:evalfrog-root@tcp(localhost:13306)/"
	}
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "evalfrog_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	adminConfig := *parsed
	adminConfig.DBName = ""
	adminConfig.MultiStatements = true
	admin, err := sql.Open("mysql", adminConfig.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err = admin.ExecContext(ctx, "CREATE DATABASE `"+name+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if _, dropErr := admin.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+name+"`"); dropErr != nil {
			t.Errorf("drop MySQL test database %s: %v", name, dropErr)
		}
		_ = admin.Close()
	})
	appConfig := *parsed
	appConfig.DBName = name
	appConfig.MultiStatements = true
	return name, appConfig.FormatDSN()
}

func mysqlIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func mysqlDatabaseFromDSN(t *testing.T, dsn string) string {
	t.Helper()
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.DBName == "" {
		t.Fatal("MySQL integration DSN must select a database")
	}
	return parsed.DBName
}

func integrationDatabaseDescription(name string) string {
	return fmt.Sprintf("MySQL database %s", name)
}
