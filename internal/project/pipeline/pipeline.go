package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pipeline drives a batch of decoded events through every stage and
// commits the resulting writes plus the cursor advance in a single tx.
type Pipeline struct {
	pool   *pgxpool.Pool
	log    *slog.Logger
	stages []Stage
}

func New(
	pool *pgxpool.Pool,
	log *slog.Logger,
	stages ...Stage,
) *Pipeline {
	return &Pipeline{
		pool:   pool,
		log:    log,
		stages: stages,
	}
}

// ProcessBatch applies every event to every stage, then flushes every
// stage and advances the projector cursor in one transaction. After
// commit, stages emit any pg_notify side effects and drop in-memory
// state that's now outside their retention window.
//
// cursorName / newCursor identify the cursor row to advance; newCursor
// must equal the highest raw_events.id represented by `events`.
func (p *Pipeline) ProcessBatch(
	ctx context.Context,
	events []Event,
	cursorName string,
	newCursor int64,
) error {
	if newCursor <= 0 {
		return nil
	}

	var hwm time.Time

	for _, e := range events {
		if e.TS.After(hwm) {
			hwm = e.TS
		}

		for _, s := range p.stages {
			err := s.Apply(ctx, e)
			if err != nil {
				return fmt.Errorf("stage %s apply: %w", s.Name(), err)
			}
		}
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}

	defer func(tx pgx.Tx, ctx context.Context) {
		_ = tx.Rollback(ctx)
	}(tx, ctx)

	for _, s := range p.stages {
		err := s.Flush(ctx, tx)
		if err != nil {
			return fmt.Errorf("stage %s flush: %w", s.Name(), err)
		}
	}

	err = saveCursor(ctx, tx, cursorName, newCursor)
	if err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	for _, s := range p.stages {
		err := s.Notify(ctx, p.pool)
		if err != nil {
			p.log.Warn(
				"stage notify failed",
				"stage",
				s.Name(),
				"err",
				err,
			)
		}

		if !hwm.IsZero() {
			s.Evict(hwm)
		}
	}

	return nil
}

// Clear wipes in-memory state in every stage. Called after a reset, before
// the catch-up loop starts replaying raw_events from the beginning.
func (p *Pipeline) Clear() {
	for _, s := range p.stages {
		s.Clear()
	}
}

func saveCursor(
	ctx context.Context,
	tx pgx.Tx,
	name string,
	id int64,
) error {
	_, err := tx.Exec(
		ctx,
		`
		INSERT INTO projector_state
		    (name
		    ,last_event_id
		    ,updated_at)
		VALUES
		    ($1
		    ,$2
		    ,now())
		ON CONFLICT (name) DO UPDATE SET
		     last_event_id = EXCLUDED.last_event_id
		    ,updated_at = now()
		`,
		name,
		id,
	)
	return err
}
