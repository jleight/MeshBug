package store

import (
	"context"
	"fmt"
	"time"
)

// EnsurePartitions makes sure monthly partitions for `now` and the next month
// exist on packet_observations. Safe to call repeatedly.
func (s *Store) EnsurePartitions(ctx context.Context, now time.Time) error {
	for _, t := range []time.Time{now, now.AddDate(0, 1, 0)} {
		if err := s.ensureMonth(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ensureMonth(ctx context.Context, t time.Time) error {
	start := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	name := fmt.Sprintf("packet_observations_%04d_%02d", start.Year(), start.Month())

	// idempotent: skip if we already recorded creating it.
	var exists bool
	err := s.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM _partition_state WHERE partition_name = $1)`, name,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check partition_state: %w", err)
	}
	if exists {
		return nil
	}

	ddl := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s PARTITION OF packet_observations FOR VALUES FROM ('%s') TO ('%s')`,
		name, start.Format(time.RFC3339), end.Format(time.RFC3339),
	)
	if _, err := s.Pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("create partition %s: %w", name, err)
	}
	_, err = s.Pool.Exec(ctx,
		`INSERT INTO _partition_state(partition_name, range_start, range_end) VALUES ($1,$2,$3)
		 ON CONFLICT (partition_name) DO NOTHING`,
		name, start, end,
	)
	if err != nil {
		return fmt.Errorf("record partition: %w", err)
	}
	return nil
}
