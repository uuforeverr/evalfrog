package mysql

import (
	"context"
	"reflect"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/uu999/evalfrog/internal/platform/config"
)

func TestBindReordersNumberedParameters(t *testing.T) {
	query, arguments, err := bind("UPDATE items SET value=$3 WHERE project_id=$1 AND id=$2 AND previous=$3", []any{"project", "id", "next"})
	if err != nil {
		t.Fatal(err)
	}
	if query != "UPDATE items SET value=? WHERE project_id=? AND id=? AND previous=?" {
		t.Fatalf("query=%q", query)
	}
	if !reflect.DeepEqual(arguments, []any{"next", "project", "id", "next"}) {
		t.Fatalf("arguments=%#v", arguments)
	}
}

func TestOpenRejectsDSNDatabaseMismatchBeforeConnecting(t *testing.T) {
	_, err := Open(context.Background(), config.MySQLConfig{
		DSN:      "user:secret@tcp(localhost:3306)/actual",
		Database: "expected",
	})
	if err == nil {
		t.Fatal("database mismatch was accepted")
	}
}

func TestRetryableTransactionErrors(t *testing.T) {
	for _, number := range []uint16{1205, 1213} {
		if !isRetryableTransactionError(&mysqldriver.MySQLError{Number: number}) {
			t.Fatalf("MySQL error %d must be retryable", number)
		}
	}
	if isRetryableTransactionError(&mysqldriver.MySQLError{Number: 1062}) {
		t.Fatal("duplicate key must not be retried as a transaction failure")
	}
}

func TestBindRejectsMissingParameter(t *testing.T) {
	if _, _, err := bind("SELECT $2", []any{"only-one"}); err == nil {
		t.Fatal("missing numbered parameter was accepted")
	}
}
