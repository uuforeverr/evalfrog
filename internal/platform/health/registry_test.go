package health

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRegistryReportsStableSortedResults(t *testing.T) {
	t.Parallel()
	registry := New(time.Second)
	if err := registry.Register("redis", func(context.Context) error { return errors.New("offline") }); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("mysql", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	report := registry.Check(context.Background())
	if report.Status != "unavailable" || report.Checks[0].Name != "mysql" || report.Checks[1].Name != "redis" {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestRegistryRejectsDuplicate(t *testing.T) {
	t.Parallel()
	registry := New(time.Second)
	check := func(context.Context) error { return nil }
	if err := registry.Register("mysql", check); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("mysql", check); err == nil {
		t.Fatal("expected duplicate registration error")
	}
}
