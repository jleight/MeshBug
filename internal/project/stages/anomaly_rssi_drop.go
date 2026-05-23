package stages

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// RSSIDrop fires when the average RSSI a given observer hears from a
// given source drops by ThresholdDB or more in the recent window
// (RecentWindow ending at t) compared to the baseline window (the
// BaselineWindow ending where the recent window starts).
//
// Both windows must have at least MinSamples 1-minute rollup rows for
// the comparison to be meaningful — otherwise we'd flag observers we
// haven't seen recently as having "dropped."
type RSSIDrop struct {
	RecentWindow   time.Duration
	BaselineWindow time.Duration
	MinSamples     int
	ThresholdDB    float64
	Severity       string
}

func (d *RSSIDrop) Kind() string {
	return "rssi_drop"
}

func (d *RSSIDrop) Run(
	ctx context.Context,
	tx pgx.Tx,
	t time.Time,
) ([]Finding, error) {
	rows, err := tx.Query(
		ctx,
		`
		WITH base AS (
		    SELECT   observer_id
		            ,source_prefix
		            ,AVG(avg_rssi) AS rssi_prev
		    FROM    rollup_neighbor_1m
		    WHERE   bucket >= $1::timestamptz - $2::interval - $3::interval
		        AND bucket <  $1::timestamptz - $2::interval
		    GROUP BY observer_id, source_prefix
		    HAVING COUNT(*) >= $4
		), curr AS (
		    SELECT   observer_id
		            ,source_prefix
		            ,AVG(avg_rssi) AS rssi_curr
		    FROM    rollup_neighbor_1m
		    WHERE   bucket >= $1::timestamptz - $2::interval
		        AND bucket <  $1::timestamptz
		    GROUP BY observer_id, source_prefix
		    HAVING COUNT(*) >= $4
		)
		SELECT   b.observer_id
		        ,b.source_prefix
		        ,b.rssi_prev
		        ,c.rssi_curr
		FROM    base b
		JOIN    curr c USING (observer_id, source_prefix)
		WHERE   b.rssi_prev - c.rssi_curr >= $5
		`,
		t,
		d.RecentWindow,
		d.BaselineWindow,
		d.MinSamples,
		d.ThresholdDB,
	)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var findings []Finding

	for rows.Next() {
		var (
			observerID []byte
			source     []byte
			rssiPrev   float64
			rssiCurr   float64
		)

		err := rows.Scan(
			&observerID,
			&source,
			&rssiPrev,
			&rssiCurr,
		)
		if err != nil {
			return nil, err
		}

		findings = append(findings, Finding{
			SubjectID: source,
			Severity:  d.Severity,
			Details: map[string]any{
				"observer":        observerID,
				"source":          source,
				"rssi_prev":       rssiPrev,
				"rssi_curr":       rssiCurr,
				"delta_db":        rssiPrev - rssiCurr,
				"recent_window":   d.RecentWindow.String(),
				"baseline_window": d.BaselineWindow.String(),
			},
		})
	}

	return findings, rows.Err()
}
