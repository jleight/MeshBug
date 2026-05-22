// Package rollup aggregates raw packet observations into 1m and 1h tables.
//
// Workers run every minute, aggregating the previous *closed* minute (i.e.
// floor(now) - 1m). 1h aggregation runs every hour for the previous closed
// hour. Each aggregation is idempotent via ON CONFLICT (observer_id, bucket).
package rollup

import (
	"context"
	"log/slog"
	"time"

	"github.com/jleight/meshbug/internal/store"
)

type Worker struct {
	store *store.Store
	log   *slog.Logger
}

func New(s *store.Store, log *slog.Logger) *Worker {
	return &Worker{store: s, log: log}
}

func (w *Worker) Run(ctx context.Context) {
	// One minute after the next minute boundary, so the previous minute is closed.
	now := time.Now().UTC()
	next1m := now.Truncate(time.Minute).Add(time.Minute).Add(2 * time.Second)
	t1m := time.NewTimer(time.Until(next1m))
	defer t1m.Stop()

	next1h := now.Truncate(time.Hour).Add(time.Hour).Add(5 * time.Second)
	t1h := time.NewTimer(time.Until(next1h))
	defer t1h.Stop()

	// Catch up on startup (last 6 hours of 1m, last 7 days of 1h).
	catchupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if err := w.rollup1mRange(catchupCtx, now.Add(-6*time.Hour), now.Truncate(time.Minute)); err != nil {
		w.log.Warn("1m catch-up failed", "err", err)
	}
	if err := w.rollup1hRange(catchupCtx, now.AddDate(0, 0, -7), now.Truncate(time.Hour)); err != nil {
		w.log.Warn("1h catch-up failed", "err", err)
	}
	cancel()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t1m.C:
			closed := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
			if err := w.rollup1m(ctx, closed); err != nil {
				w.log.Warn("1m rollup failed", "bucket", closed, "err", err)
			}
			t1m.Reset(time.Minute)
		case <-t1h.C:
			closed := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
			if err := w.rollup1h(ctx, closed); err != nil {
				w.log.Warn("1h rollup failed", "bucket", closed, "err", err)
			}
			t1h.Reset(time.Hour)
		}
	}
}

func (w *Worker) rollup1m(ctx context.Context, bucket time.Time) error {
	return w.rollup1mRange(ctx, bucket, bucket.Add(time.Minute))
}

func (w *Worker) rollup1h(ctx context.Context, bucket time.Time) error {
	return w.rollup1hRange(ctx, bucket, bucket.Add(time.Hour))
}

func (w *Worker) rollup1mRange(ctx context.Context, from, to time.Time) error {
	_, err := w.store.Pool.Exec(ctx, `
		WITH agg AS (
		  SELECT
		    p.observer_id,
		    date_trunc('minute', p.ts) AS bucket,
		    COUNT(*) AS packets,
		    COUNT(DISTINCT p.packet_hash) AS unique_pkts,
		    COUNT(*) FILTER (WHERE p.route = 'F') AS flood_pkts,
		    COUNT(*) FILTER (WHERE p.route = 'D') AS direct_pkts,
		    AVG(p.rssi)::numeric(6,2) AS avg_rssi,
		    MIN(p.rssi) AS min_rssi,
		    MAX(p.rssi) AS max_rssi,
		    AVG(p.snr)::numeric(6,2) AS avg_snr
		  FROM packet_observations p
		  WHERE p.ts >= $1 AND p.ts < $2
		  GROUP BY p.observer_id, date_trunc('minute', p.ts)
		)
		INSERT INTO rollup_observer_1m (observer_id, bucket, packets, unique_pkts,
		                                flood_pkts, direct_pkts, avg_rssi, min_rssi, max_rssi, avg_snr, noise_floor)
		SELECT a.observer_id, a.bucket, a.packets, a.unique_pkts, a.flood_pkts, a.direct_pkts,
		       a.avg_rssi, a.min_rssi, a.max_rssi, a.avg_snr,
		       (SELECT s.noise_floor FROM observer_status s
		         WHERE s.observer_id = a.observer_id
		           AND s.ts >= a.bucket AND s.ts < a.bucket + interval '1 minute'
		         ORDER BY s.ts DESC LIMIT 1)
		FROM agg a
		ON CONFLICT (observer_id, bucket) DO UPDATE SET
		  packets     = EXCLUDED.packets,
		  unique_pkts = EXCLUDED.unique_pkts,
		  flood_pkts  = EXCLUDED.flood_pkts,
		  direct_pkts = EXCLUDED.direct_pkts,
		  avg_rssi    = EXCLUDED.avg_rssi,
		  min_rssi    = EXCLUDED.min_rssi,
		  max_rssi    = EXCLUDED.max_rssi,
		  avg_snr     = EXCLUDED.avg_snr,
		  noise_floor = COALESCE(EXCLUDED.noise_floor, rollup_observer_1m.noise_floor)
	`, from, to)
	if err != nil {
		return err
	}
	_, err = w.store.Pool.Exec(ctx, `
		INSERT INTO rollup_neighbor_1m (observer_id, source_prefix, bucket, packets, avg_rssi, min_rssi, max_rssi, avg_snr)
		SELECT
		  observer_id, source_prefix,
		  date_trunc('minute', ts) AS bucket,
		  COUNT(*),
		  AVG(rssi)::numeric(6,2), MIN(rssi), MAX(rssi),
		  AVG(snr)::numeric(6,2)
		FROM packet_observations
		WHERE ts >= $1 AND ts < $2 AND source_prefix IS NOT NULL
		GROUP BY observer_id, source_prefix, date_trunc('minute', ts)
		ON CONFLICT (observer_id, source_prefix, bucket) DO UPDATE SET
		  packets  = EXCLUDED.packets,
		  avg_rssi = EXCLUDED.avg_rssi,
		  min_rssi = EXCLUDED.min_rssi,
		  max_rssi = EXCLUDED.max_rssi,
		  avg_snr  = EXCLUDED.avg_snr
	`, from, to)
	return err
}

func (w *Worker) rollup1hRange(ctx context.Context, from, to time.Time) error {
	_, err := w.store.Pool.Exec(ctx, `
		INSERT INTO rollup_observer_1h (observer_id, bucket, packets, unique_pkts,
		                                flood_pkts, direct_pkts, avg_rssi, min_rssi, max_rssi, avg_snr, noise_floor)
		SELECT
		  observer_id,
		  date_trunc('hour', bucket) AS h,
		  SUM(packets), SUM(unique_pkts), SUM(flood_pkts), SUM(direct_pkts),
		  AVG(avg_rssi)::numeric(6,2), MIN(min_rssi), MAX(max_rssi),
		  AVG(avg_snr)::numeric(6,2),
		  AVG(noise_floor)::int
		FROM rollup_observer_1m
		WHERE bucket >= $1 AND bucket < $2
		GROUP BY observer_id, date_trunc('hour', bucket)
		ON CONFLICT (observer_id, bucket) DO UPDATE SET
		  packets     = EXCLUDED.packets,
		  unique_pkts = EXCLUDED.unique_pkts,
		  flood_pkts  = EXCLUDED.flood_pkts,
		  direct_pkts = EXCLUDED.direct_pkts,
		  avg_rssi    = EXCLUDED.avg_rssi,
		  min_rssi    = EXCLUDED.min_rssi,
		  max_rssi    = EXCLUDED.max_rssi,
		  avg_snr     = EXCLUDED.avg_snr,
		  noise_floor = EXCLUDED.noise_floor
	`, from, to)
	return err
}
