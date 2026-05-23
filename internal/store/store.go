package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is a thin wrapper around pgxpool with the queries this app needs.
//
// Each Store is bound to a specific search_path so the SQL elsewhere in this
// package can reference unqualified table names (e.g. `raw_events` rather
// than `ingest.raw_events`). Callers pick the search_path by choosing one of
// the constructors below.
type Store struct {
	Pool *pgxpool.Pool
}

// NewIngest opens a pool against the ingest database with search_path =
// ingest, so unqualified references resolve to the raw_events log.
func NewIngest(ctx context.Context, databaseURL string) (*Store, error) {
	return newWithSearchPath(ctx, databaseURL, "ingest")
}

// NewProject opens a pool against the project database with search_path =
// project,web — projector + rollup + anomaly + web all live here.
func NewProject(ctx context.Context, databaseURL string) (*Store, error) {
	return newWithSearchPath(ctx, databaseURL, "project,web")
}

func newWithSearchPath(
	ctx context.Context,
	databaseURL string,
	searchPath string,
) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}

	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = searchPath

	cfg.MaxConns = 16
	cfg.MinConns = 2
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &Store{Pool: pool}, nil
}

func (s *Store) Close() {
	s.Pool.Close()
}
