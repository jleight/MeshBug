package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// partitionedTables maps a parent table name to the prefix used for its
// monthly partitions. New entries are picked up by EnsurePartitions.
var partitionedTables = map[string]string{
	"packet_observations": "packet_observations",
	"raw_events":          "raw_events",
}

// EnsurePartitions makes sure monthly partitions for `now` and the next month
// exist on every partitioned table. Safe to call repeatedly.
func (s *Store) EnsurePartitions(ctx context.Context, now time.Time) error {
	for _, t := range []time.Time{now, now.AddDate(0, 1, 0)} {
		for parent, prefix := range partitionedTables {
			if err := s.ensureMonth(ctx, parent, prefix, t); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) ensureMonth(ctx context.Context, parent, prefix string, t time.Time) error {
	start := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	name := fmt.Sprintf("%s_%04d_%02d", prefix, start.Year(), start.Month())

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
		`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
		name, parent, start.Format(time.RFC3339), end.Format(time.RFC3339),
	)
	if _, err := s.Pool.Exec(ctx, ddl); err != nil {
		// 42P07 (duplicate_table) can fire under concurrent startup of ingest +
		// project even with CREATE TABLE IF NOT EXISTS, because partition
		// creation isn't atomic with the existence check. Both branches do the
		// same thing — the second one just records state and proceeds.
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42P07" {
			return fmt.Errorf("create partition %s: %w", name, err)
		}
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
