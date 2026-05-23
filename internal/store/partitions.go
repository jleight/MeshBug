package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// EnsurePartitions makes sure monthly partitions for `now` and the next
// month exist on every named table. Safe to call repeatedly.
//
// Each Store owns its own `_partition_state` (resolved via search_path), so
// the ingest and project services maintain their partitions independently
// without coordinating. Pass only tables that live in this store's schema:
//
//	ingest store  → ["raw_events"]
//	project store → ["packet_observations"]
func (s *Store) EnsurePartitions(
	ctx context.Context,
	now time.Time,
	tables ...string,
) error {
	for _, month := range []time.Time{now, now.AddDate(0, 1, 0)} {
		for _, table := range tables {
			if err := s.ensurePartition(ctx, table, month); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) ensurePartition(
	ctx context.Context,
	table string,
	month time.Time,
) error {
	start := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	name := fmt.Sprintf("%s_%04d_%02d", table, start.Year(), start.Month())

	// idempotent: skip if we already recorded creating it.
	var exists bool
	err := s.Pool.
		QueryRow(
			ctx,
			`SELECT EXISTS (SELECT 1 FROM _partition_state WHERE partition_name = $1)`,
			name,
		).
		Scan(&exists)
	if err != nil {
		return fmt.Errorf("check partition_state: %w", err)
	}
	if exists {
		return nil
	}

	ddl := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
		name,
		table,
		start.Format(time.RFC3339),
		end.Format(time.RFC3339),
	)

	if _, err := s.Pool.Exec(ctx, ddl); err != nil {
		// 42P07 (duplicate_table) can still fire under racing partition
		// creations within the same service (e.g. the startup ensure and
		// the daily maintainer overlapping). Either branch lands the
		// partition; the second one just records state and proceeds.
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != "42P07" {
			return fmt.Errorf("create partition %s: %w", name, err)
		}
	}

	_, err = s.Pool.Exec(ctx,
		`INSERT INTO _partition_state(partition_name, range_start, range_end) VALUES ($1,$2,$3) ON CONFLICT (partition_name) DO NOTHING`,
		name,
		start,
		end,
	)
	if err != nil {
		return fmt.Errorf("record partition: %w", err)
	}

	return nil
}
