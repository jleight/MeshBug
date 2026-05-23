package stages

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// ObserverSilent fires when an observer that has historically averaged
// at least BaselineMinPerMin packets per minute over the BaselineWindow
// has heard zero packets in the RecentWindow ending at t. A short
// guard interval just before t excludes the very-latest minute, which
// may still be filling.
type ObserverSilent struct {
	RecentWindow      time.Duration
	BaselineWindow    time.Duration
	BaselineMinPerMin float64
	Severity          string
}

func (d *ObserverSilent) Kind() string {
	return "observer_silent"
}

func (d *ObserverSilent) Run(
	ctx context.Context,
	tx pgx.Tx,
	t time.Time,
) ([]Finding, error) {
	rows, err := tx.Query(
		ctx,
		`
		WITH baseline AS (
		    SELECT   observer_id
		            ,AVG(packets) AS pkts_per_min
		    FROM    rollup_observer_1m
		    WHERE   bucket >= $1::timestamptz - $2::interval
		        AND bucket <  $1::timestamptz - $3::interval
		    GROUP BY observer_id
		    HAVING AVG(packets) >= $4
		), recent AS (
		    SELECT   observer_id
		            ,COALESCE(SUM(packets), 0) AS pkts
		    FROM    rollup_observer_1m
		    WHERE   bucket >= $1::timestamptz - $3::interval
		        AND bucket <  $1::timestamptz
		    GROUP BY observer_id
		)
		SELECT   b.observer_id
		        ,b.pkts_per_min
		        ,COALESCE(r.pkts, 0)
		FROM    baseline b
		LEFT JOIN recent r USING (observer_id)
		WHERE   COALESCE(r.pkts, 0) = 0
		`,
		t,
		d.BaselineWindow,
		d.RecentWindow,
		d.BaselineMinPerMin,
	)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var findings []Finding

	for rows.Next() {
		var (
			observerID []byte
			perMin     float64
			pkts       int64
		)

		err := rows.Scan(&observerID, &perMin, &pkts)
		if err != nil {
			return nil, err
		}

		findings = append(findings, Finding{
			SubjectID: observerID,
			Severity:  d.Severity,
			Details: map[string]any{
				"recent_window":    d.RecentWindow.String(),
				"baseline_window":  d.BaselineWindow.String(),
				"baseline_per_min": perMin,
				"recent_packets":   pkts,
			},
		})
	}

	return findings, rows.Err()
}
