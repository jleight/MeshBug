// Package anomaly runs simple rule-based detectors against the rollup tables
// and writes findings to the anomalies table for the UI to surface.
package anomaly

import (
	"context"
	"log/slog"
	"time"

	"github.com/jleight/meshbug/internal/notify"
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
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.tick(ctx)
		}
	}
}

func (w *Worker) tick(ctx context.Context) {
	now := time.Now().UTC()
	checks := []func(context.Context, time.Time){
		w.scoreNonStandard,
		w.observerSilent,
		w.rssiDrop,
		w.trafficSpike,
	}
	for _, c := range checks {
		c(ctx, now)
	}
}

// score != 1000 in the last minute → score_nonstandard anomaly per observer.
func (w *Worker) scoreNonStandard(ctx context.Context, now time.Time) {
	from := now.Add(-time.Minute)
	rows, err := w.store.Pool.Query(ctx, `
		SELECT observer_id, COUNT(*), MIN(score), MAX(score)
		FROM packet_observations
		WHERE ts >= $1 AND ts < $2 AND score IS NOT NULL AND score <> 1000
		GROUP BY observer_id
	`, from, now)
	if err != nil {
		w.log.Warn("scoreNonStandard query", "err", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var obs []byte
		var n, mn, mx int
		if err := rows.Scan(&obs, &n, &mn, &mx); err != nil {
			continue
		}
		w.emit(ctx, "score_nonstandard", obs, "warn", map[string]any{
			"window": "1m", "count": n, "min_score": mn, "max_score": mx,
		})
	}
}

// Observer that historically averages >=1 pkt/min has been silent >10m.
func (w *Worker) observerSilent(ctx context.Context, now time.Time) {
	rows, err := w.store.Pool.Query(ctx, `
		WITH baseline AS (
		  SELECT observer_id, AVG(packets) AS avg_per_min
		  FROM rollup_observer_1m
		  WHERE bucket >= $1::timestamptz - interval '24 hours' AND bucket < $1::timestamptz - interval '15 minutes'
		  GROUP BY observer_id
		  HAVING AVG(packets) >= 1
		), recent AS (
		  SELECT observer_id, COALESCE(SUM(packets),0) AS pkts
		  FROM rollup_observer_1m
		  WHERE bucket >= $1::timestamptz - interval '10 minutes'
		  GROUP BY observer_id
		)
		SELECT b.observer_id, b.avg_per_min, COALESCE(r.pkts, 0)
		FROM baseline b LEFT JOIN recent r USING (observer_id)
		WHERE COALESCE(r.pkts, 0) = 0
	`, now)
	if err != nil {
		w.log.Warn("observerSilent query", "err", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var obs []byte
		var avg float64
		var pkts int
		if err := rows.Scan(&obs, &avg, &pkts); err != nil {
			continue
		}
		w.emit(ctx, "observer_silent", obs, "crit", map[string]any{
			"window": "10m", "baseline_per_min": avg, "recent_packets": pkts,
		})
	}
}

// RSSI for a (observer, source) is ≥6 dB worse than its trailing-24h average.
func (w *Worker) rssiDrop(ctx context.Context, now time.Time) {
	rows, err := w.store.Pool.Query(ctx, `
		WITH base AS (
		  SELECT observer_id, source_prefix, AVG(avg_rssi) AS rssi_24h, COUNT(*) AS samples
		  FROM rollup_neighbor_1m
		  WHERE bucket >= $1::timestamptz - interval '24 hours' AND bucket < $1::timestamptz - interval '15 minutes'
		  GROUP BY observer_id, source_prefix
		  HAVING COUNT(*) >= 20
		), recent AS (
		  SELECT observer_id, source_prefix, AVG(avg_rssi) AS rssi_1h
		  FROM rollup_neighbor_1m
		  WHERE bucket >= $1::timestamptz - interval '1 hour'
		  GROUP BY observer_id, source_prefix
		  HAVING COUNT(*) >= 3
		)
		SELECT b.observer_id, b.source_prefix, b.rssi_24h, r.rssi_1h
		FROM base b JOIN recent r USING (observer_id, source_prefix)
		WHERE b.rssi_24h - r.rssi_1h >= 6
	`, now)
	if err != nil {
		w.log.Warn("rssiDrop query", "err", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var obs, src []byte
		var base, recent float64
		if err := rows.Scan(&obs, &src, &base, &recent); err != nil {
			continue
		}
		w.emit(ctx, "rssi_drop", src, "warn", map[string]any{
			"observer":     obs,
			"source":       src,
			"rssi_24h_avg": base,
			"rssi_1h_avg":  recent,
			"delta_db":     base - recent,
		})
	}
}

// Packet type traffic over the last hour is ≥5x its 24h baseline.
func (w *Worker) trafficSpike(ctx context.Context, now time.Time) {
	rows, err := w.store.Pool.Query(ctx, `
		WITH base AS (
		  SELECT packet_type, COUNT(*)::float / GREATEST(EXTRACT(EPOCH FROM (interval '23 hours'))/3600, 1) AS per_hr
		  FROM packet_observations
		  WHERE ts >= $1::timestamptz - interval '24 hours' AND ts < $1::timestamptz - interval '1 hour'
		  GROUP BY packet_type
		  HAVING COUNT(*) >= 100
		), recent AS (
		  SELECT packet_type, COUNT(*) AS cnt
		  FROM packet_observations
		  WHERE ts >= $1::timestamptz - interval '1 hour'
		  GROUP BY packet_type
		)
		SELECT b.packet_type, b.per_hr, r.cnt
		FROM base b JOIN recent r USING (packet_type)
		WHERE r.cnt >= 5 * b.per_hr
	`, now)
	if err != nil {
		w.log.Warn("trafficSpike query", "err", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var ptype string
		var base float64
		var cnt int
		if err := rows.Scan(&ptype, &base, &cnt); err != nil {
			continue
		}
		w.emit(ctx, "traffic_spike", nil, "info", map[string]any{
			"packet_type": ptype, "baseline_per_hour": base, "recent_1h": cnt,
		})
	}
}

func (w *Worker) emit(ctx context.Context, kind string, subject []byte, severity string, details map[string]any) {
	// Dedupe: don't repeat the same (kind, subject) within the last 15 minutes.
	var exists bool
	if err := w.store.Pool.QueryRow(ctx, `
		SELECT EXISTS(
		  SELECT 1 FROM anomalies
		  WHERE kind = $1
		    AND subject_id IS NOT DISTINCT FROM $2
		    AND ts >= now() - interval '15 minutes'
		    AND resolved_at IS NULL
		)
	`, kind, subject).Scan(&exists); err != nil {
		w.log.Warn("anomaly dedupe", "err", err)
		return
	}
	if exists {
		return
	}
	var id int64
	if err := w.store.Pool.QueryRow(ctx, `
		INSERT INTO anomalies (ts, kind, subject_id, severity, details)
		VALUES (now(), $1, $2, $3, $4)
		RETURNING id
	`, kind, subject, severity, details).Scan(&id); err != nil {
		w.log.Warn("anomaly insert", "err", err)
		return
	}
	if err := notify.Publish(ctx, w.store.Pool, notify.ChannelAnomaly, map[string]any{
		"id": id, "kind": kind, "severity": severity,
	}); err != nil {
		w.log.Warn("notify publish failed", "channel", notify.ChannelAnomaly, "err", err)
	}
}
