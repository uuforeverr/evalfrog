package recovery

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/runtime/attempt"
)

func TestReaperMarksExpiredAttemptsAndIgnoresSettledRaces(t *testing.T) {
	repository := &fakeRepository{values: []attempt.MarkLostCommand{
		{ProjectID: "project", RunID: "run", AttemptID: "lost", AttemptSequence: 1},
		{ProjectID: "project", RunID: "run", AttemptID: "already-settled", AttemptSequence: 2},
	}}
	coordinator := &fakeCoordinator{errors: []error{nil, attempt.ErrStateConflict}}
	reaper, err := NewReaper(repository, coordinator, time.Second, time.Millisecond, 10, "reaper-trace", discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err = reaper.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if coordinator.calls.Load() != 2 || coordinator.commands[0].TraceID != "reaper-trace" {
		t.Fatalf("calls=%d commands=%+v", coordinator.calls.Load(), coordinator.commands)
	}
}

func TestReaperPropagatesAuthorityFailuresAndStops(t *testing.T) {
	repository := &fakeRepository{err: errors.New("mysql unavailable")}
	reaper, err := NewReaper(repository, &fakeCoordinator{}, 0, time.Millisecond, 1, "trace", discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err = reaper.ScanOnce(context.Background()); err == nil {
		t.Fatal("repository failure was hidden")
	}
	repository.err = nil
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- reaper.Run(ctx) }()
	time.Sleep(3 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reaper did not stop")
	}
	_ = reaper.Shutdown(context.Background())
	if _, err = NewReaper(nil, nil, -1, 0, 0, "", discardLogger()); err == nil {
		t.Fatal("invalid reaper accepted")
	}
}

type fakeRepository struct {
	values []attempt.MarkLostCommand
	err    error
}

func (value *fakeRepository) ListExpiredAttempts(context.Context, time.Duration, int) ([]attempt.MarkLostCommand, error) {
	return value.values, value.err
}

type fakeCoordinator struct {
	calls    atomic.Int32
	commands []attempt.MarkLostCommand
	errors   []error
}

func (value *fakeCoordinator) MarkExpiredLost(_ context.Context, command attempt.MarkLostCommand) (bool, error) {
	index := int(value.calls.Add(1)) - 1
	value.commands = append(value.commands, command)
	if index < len(value.errors) {
		return value.errors[index] == nil, value.errors[index]
	}
	return true, nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
