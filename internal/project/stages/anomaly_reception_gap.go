package stages

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// ReceptionGap fires when an observer's recent reception rate from a
// specific source is much lower than that observer's own baseline,
// while peer observers (other observers with a baseline for the same
// source) are still hearing the source at a healthy fraction of their
// own baselines. That asymmetry — this observer dropped, others did
// not — is what distinguishes a reception gap from a fleet-wide drop
// caused by the source itself going quiet.
//
// The detector computes each baselined (observer, source) pair's
// recent/expected ratio, then for each candidate compares it against
// the median ratio of peer observers for the same source. A candidate
// fires when its ratio is below MaxRecentFraction *and* the peer
// median is at or above MinPeerFraction. MinPeers requires at least
// that many peer observers (with their own baseline for the source)
// before the comparison is considered meaningful.
//
// A guard against total observer outages (already covered by
// ObserverSilent) requires the candidate observer to still be hearing
// at least one packet from any source in the recent window — otherwise
// every baselined source would fire for that observer.
type ReceptionGap struct {
	RecentWindow      time.Duration
	BaselineWindow    time.Duration
	MinSamples        int
	BaselineMinPerMin float64
	MaxRecentFraction float64
	MinPeerFraction   float64
	MinPeers          int
	Severity          string
}

func (d *ReceptionGap) Kind() string {
	return "reception_gap"
}

func (d *ReceptionGap) Run(
	ctx context.Context,
	tx pgx.Tx,
	t time.Time,
) ([]Finding, error) {
	rows, err := tx.Query(
		ctx,
		`
		WITH baseline AS (
		    SELECT   observer_id
		            ,source_prefix
		            ,AVG(packets) AS baseline_per_min
		    FROM    rollup_neighbor_1m
		    WHERE   bucket >= $1::timestamptz - $2::interval - $3::interval
		        AND bucket <  $1::timestamptz - $2::interval
		    GROUP BY observer_id, source_prefix
		    HAVING COUNT(*) >= $4
		       AND AVG(packets) >= $5
		), recent AS (
		    SELECT   observer_id
		            ,source_prefix
		            ,SUM(packets) AS recent_pkts
		    FROM    rollup_neighbor_1m
		    WHERE   bucket >= $1::timestamptz - $2::interval
		        AND bucket <  $1::timestamptz
		    GROUP BY observer_id, source_prefix
		), ratios AS (
		    SELECT   b.observer_id
		            ,b.source_prefix
		            ,b.baseline_per_min
		            ,COALESCE(r.recent_pkts, 0) AS recent_pkts
		            ,COALESCE(r.recent_pkts, 0)::float
		             / (b.baseline_per_min
		                * (EXTRACT(EPOCH FROM $2::interval) / 60.0))
		             AS ratio
		    FROM    baseline b
		    LEFT JOIN recent r USING (observer_id, source_prefix)
		), peer_stats AS (
		    SELECT   c.observer_id
		            ,c.source_prefix
		            ,percentile_cont(0.5) WITHIN GROUP (ORDER BY p.ratio)
		                AS peer_median_ratio
		            ,COUNT(*) AS peer_count
		    FROM    ratios c
		    JOIN    ratios p
		         ON p.source_prefix = c.source_prefix
		        AND p.observer_id  <> c.observer_id
		    GROUP BY c.observer_id, c.source_prefix
		), observer_alive AS (
		    SELECT   observer_id
		            ,COALESCE(SUM(packets), 0) AS pkts
		    FROM    rollup_observer_1m
		    WHERE   bucket >= $1::timestamptz - $2::interval
		        AND bucket <  $1::timestamptz
		    GROUP BY observer_id
		)
		SELECT   c.observer_id
		        ,c.source_prefix
		        ,c.baseline_per_min
		        ,c.recent_pkts
		        ,c.ratio
		        ,ps.peer_median_ratio
		        ,ps.peer_count
		FROM    ratios c
		JOIN    peer_stats ps USING (observer_id, source_prefix)
		JOIN    observer_alive oa
		     ON oa.observer_id = c.observer_id
		WHERE   ps.peer_count >= $6
		    AND c.ratio < $7
		    AND ps.peer_median_ratio >= $8
		    AND oa.pkts > 0
		`,
		t,
		d.RecentWindow,
		d.BaselineWindow,
		d.MinSamples,
		d.BaselineMinPerMin,
		d.MinPeers,
		d.MaxRecentFraction,
		d.MinPeerFraction,
	)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var findings []Finding

	for rows.Next() {
		var (
			observerID      []byte
			source          []byte
			baselinePerMin  float64
			recentPkts      int64
			ratio           float64
			peerMedianRatio float64
			peerCount       int
		)

		err := rows.Scan(
			&observerID,
			&source,
			&baselinePerMin,
			&recentPkts,
			&ratio,
			&peerMedianRatio,
			&peerCount,
		)
		if err != nil {
			return nil, err
		}

		expected := baselinePerMin * d.RecentWindow.Minutes()

		findings = append(findings, Finding{
			SubjectID: observerID,
			Severity:  d.Severity,
			Details: map[string]any{
				"observer":          observerID,
				"source":            source,
				"baseline_per_min":  baselinePerMin,
				"expected_pkts":     expected,
				"recent_pkts":       recentPkts,
				"recent_ratio":      ratio,
				"peer_median_ratio": peerMedianRatio,
				"peer_count":        peerCount,
				"recent_window":     d.RecentWindow.String(),
				"baseline_window":   d.BaselineWindow.String(),
			},
		})
	}

	return findings, rows.Err()
}
