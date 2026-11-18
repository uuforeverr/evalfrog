package mysql

import (
	"context"

	"github.com/uu999/evalfrog/internal/scheduling"
)

// Store implements Access, Managed Resource, and Definition repository ports
// over one MySQL pool. Domain packages never import this adapter.
type Store struct {
	pool       *Pool
	router     scheduling.Router
	runUpdates runUpdateNotifier
}

type runUpdateNotifier interface {
	PublishRunUpdate(context.Context, string)
}

func NewStore(pool *Pool) *Store {
	return NewStoreWithRouter(pool, scheduling.BuiltinV1Router())
}

func NewStoreWithRouter(pool *Pool, router scheduling.Router) *Store {
	return &Store{pool: pool, router: router}
}

// SetRunUpdateNotifier wires a process-local, best-effort wake-up after
// authoritative commits. It is never part of transaction success.
func (store *Store) SetRunUpdateNotifier(value runUpdateNotifier) { store.runUpdates = value }

func (store *Store) notifyRunUpdate(ctx context.Context, runID string) {
	if store.runUpdates == nil || runID == "" {
		return
	}
	store.runUpdates.PublishRunUpdate(ctx, runID)
}
