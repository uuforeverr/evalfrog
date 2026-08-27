package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/uu999/evalfrog/internal/scheduling"
)

// Store implements Access, Managed Resource, and Definition repository ports
// over one PostgreSQL pool. Domain packages never import this adapter.
type Store struct {
	pool           *pgxpool.Pool
	router         scheduling.Router
	runViews       runViewInvalidator
	readyRegistrar scheduling.ReadyRegistrar
}

type runViewInvalidator interface {
	DeleteRunView(context.Context, string)
	PublishRunUpdate(context.Context, string)
}

func NewStore(pool *pgxpool.Pool) *Store {
	return NewStoreWithRouter(pool, scheduling.BuiltinV1Router())
}

func NewStoreWithRouter(pool *pgxpool.Pool, router scheduling.Router) *Store {
	return &Store{pool: pool, router: router}
}

// SetRunViewInvalidator wires a best-effort cache invalidation/notifier at the
// composition root. PostgreSQL commits remain successful if Redis is absent.
func (store *Store) SetRunViewInvalidator(value runViewInvalidator) { store.runViews = value }

// SetReadyRegistrar wires the post-commit Scheduling Redis acceleration path.
// Registration failure is intentionally non-authoritative and is repaired by
// the Scheduler's PostgreSQL reconciliation loop.
func (store *Store) SetReadyRegistrar(value scheduling.ReadyRegistrar) { store.readyRegistrar = value }

func (store *Store) invalidateRunView(ctx context.Context, runID string) {
	if store.runViews == nil || runID == "" {
		return
	}
	store.runViews.DeleteRunView(ctx, runID)
	store.runViews.PublishRunUpdate(ctx, runID)
}

func (store *Store) registerReady(ctx context.Context, candidates []scheduling.Candidate) {
	if store.readyRegistrar == nil || len(candidates) == 0 {
		return
	}
	_ = store.readyRegistrar.RegisterReady(ctx, candidates)
}
