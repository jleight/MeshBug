package web

import (
	"context"
	"time"

	"github.com/jleight/meshbug/internal/store"
	"github.com/jleight/meshbug/internal/web/templates"
)

// All read-side queries used by handlers live here. Handlers only call these.

func queryOverview(
	ctx context.Context,
	s *store.Store,
) (templates.OverviewData, error) {
	d := templates.OverviewData{GeneratedAt: time.Now().UTC()}

	err := s.Pool.
		QueryRow(
			ctx,
			`
			SELECT   COUNT(*) FILTER (WHERE last_seen >= now() - interval '5 minutes')
			        ,COUNT(*)
			FROM    observers
			`,
		).
		Scan(&d.ObserversOnline, &d.ObserversTotal)
	if err != nil {
		return d, err
	}

	_ = s.Pool.
		QueryRow(
			ctx,
			`
			SELECT COUNT(*)
			FROM   packet_observations
			WHERE  ts >= now() - interval '1 minute'
			`,
		).
		Scan(&d.PacketsLastMinute)

	_ = s.Pool.
		QueryRow(
			ctx,
			`
			SELECT COUNT(DISTINCT packet_hash)
			FROM   packet_observations
			WHERE  ts >= now() - interval '1 minute'
			   AND packet_hash IS NOT NULL
			`,
		).
		Scan(&d.UniquePktsLastMin)

	_ = s.Pool.
		QueryRow(
			ctx,
			`
			SELECT COUNT(*)
			FROM   anomalies
			WHERE  resolved_at IS NULL
			   AND ts >= now() - interval '24 hours'
			`,
		).
		Scan(&d.OpenAnomalies)

	rows, err := s.Pool.Query(
		ctx,
		`
		SELECT   bucket
		        ,COALESCE(SUM(packets), 0)
		FROM    rollup_observer_1m
		WHERE   bucket >= now() - interval '60 minutes'
		GROUP BY bucket
		ORDER BY bucket
		`,
	)
	if err == nil {
		defer rows.Close()

		for rows.Next() {
			var (
				b time.Time
				v int
			)
			_ = rows.Scan(&b, &v)
			d.MeshSparkline = append(d.MeshSparkline, float64(v))
		}
	}

	// Channel utilization: derive from the latest two status samples per observer.
	chRows, err := s.Pool.Query(
		ctx,
		`
		WITH ranked AS (
		    SELECT   observer_id
		            ,ts
		            ,COALESCE(tx_air_secs, 0) + COALESCE(rx_air_secs, 0) AS air
		            ,noise_floor
		            ,ROW_NUMBER() OVER (PARTITION BY observer_id ORDER BY ts DESC) AS rn
		    FROM    observer_status
		    WHERE   ts >= now() - interval '15 minutes'
		)
		SELECT   r1.observer_id
		        ,o.origin_name
		        ,o.region
		        ,(r1.air - r2.air)::float
		            / NULLIF(EXTRACT(EPOCH FROM (r1.ts - r2.ts)), 0) AS frac
		        ,r1.noise_floor
		FROM    ranked r1
		JOIN    ranked r2
		    ON  r2.observer_id = r1.observer_id
		    AND r2.rn = r1.rn + 1
		JOIN    observers o
		    ON  o.id = r1.observer_id
		WHERE   r1.rn = 1
		`,
	)
	if err == nil {
		defer chRows.Close()

		for chRows.Next() {
			var (
				c    templates.ObserverChannel
				frac *float64
			)

			err := chRows.Scan(
				&c.ID,
				&c.Origin,
				&c.Region,
				&frac,
				&c.NoiseFloor,
			)
			if err != nil {
				continue
			}

			if frac != nil {
				c.PctAir = clamp(*frac*100, 0, 100)
			}

			d.ObserverChannels = append(d.ObserverChannels, c)
		}
	}

	return d, nil
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func queryObservers(
	ctx context.Context,
	s *store.Store,
) ([]templates.ObserverRow, error) {
	rows, err := s.Pool.Query(
		ctx,
		`
		SELECT   o.id
		        ,o.origin_name
		        ,COALESCE(o.region, '')
		        ,COALESCE(o.model, '')
		        ,CASE WHEN o.last_seen >= now() - interval '5 minutes'
		              THEN 'online'
		              ELSE 'offline' END
		        ,(SELECT noise_floor FROM observer_status s
		            WHERE s.observer_id = o.id
		            ORDER BY ts DESC LIMIT 1)
		        ,(SELECT uptime_secs FROM observer_status s
		            WHERE s.observer_id = o.id
		            ORDER BY ts DESC LIMIT 1)
		        ,(SELECT MAX(ts) FROM packet_observations
		            WHERE observer_id = o.id
		              AND ts >= now() - interval '24 hours')
		        ,(SELECT COUNT(*) FROM packet_observations
		            WHERE observer_id = o.id
		              AND ts >= now() - interval '1 minute')
		FROM    observers o
		ORDER BY o.origin_name
		`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []templates.ObserverRow
	for rows.Next() {
		var (
			r    templates.ObserverRow
			last *time.Time
		)

		err := rows.Scan(
			&r.ID,
			&r.Origin,
			&r.Region,
			&r.Model,
			&r.Status,
			&r.NoiseFloor,
			&r.UptimeSecs,
			&last,
			&r.Packets1m,
		)
		if err != nil {
			return nil, err
		}

		if last != nil {
			r.LastPacketTS = *last
		}

		out = append(out, r)
	}

	// Sparkline per observer: 60 minutes.
	for i := range out {
		sp, _ := s.Pool.Query(
			ctx,
			`
			SELECT packets
			FROM   rollup_observer_1m
			WHERE  observer_id = $1
			   AND bucket >= now() - interval '60 minutes'
			ORDER BY bucket
			`,
			out[i].ID,
		)

		for sp.Next() {
			var v int
			_ = sp.Scan(&v)
			out[i].Sparkline = append(out[i].Sparkline, float64(v))
		}
		sp.Close()
	}

	return out, nil
}

func queryObserverDetail(
	ctx context.Context,
	s *store.Store,
	id []byte,
) (templates.ObserverDetail, error) {
	var d templates.ObserverDetail

	err := s.Pool.
		QueryRow(
			ctx,
			`
			SELECT   o.id
			        ,o.origin_name
			        ,COALESCE(o.region, '')
			        ,COALESCE(o.model, '')
			        ,CASE WHEN o.last_seen >= now() - interval '5 minutes'
			              THEN 'online'
			              ELSE 'offline' END
			        ,(SELECT noise_floor FROM observer_status s
			            WHERE s.observer_id = o.id
			            ORDER BY ts DESC LIMIT 1)
			        ,(SELECT uptime_secs FROM observer_status s
			            WHERE s.observer_id = o.id
			            ORDER BY ts DESC LIMIT 1)
			        ,(SELECT MAX(ts) FROM packet_observations
			            WHERE observer_id = o.id
			              AND ts >= now() - interval '24 hours')
			        ,(SELECT COUNT(*) FROM packet_observations
			            WHERE observer_id = o.id
			              AND ts >= now() - interval '1 minute')
			FROM    observers o
			WHERE   o.id = $1
			`,
			id,
		).
		Scan(
			&d.Row.ID,
			&d.Row.Origin,
			&d.Row.Region,
			&d.Row.Model,
			&d.Row.Status,
			&d.Row.NoiseFloor,
			&d.Row.UptimeSecs,
			&d.Row.LastPacketTS,
			&d.Row.Packets1m,
		)
	if err != nil {
		return d, err
	}

	sp, _ := s.Pool.Query(
		ctx,
		`
		SELECT packets
		FROM   rollup_observer_1m
		WHERE  observer_id = $1
		   AND bucket >= now() - interval '60 minutes'
		ORDER BY bucket
		`,
		id,
	)

	for sp.Next() {
		var v int
		_ = sp.Scan(&v)
		d.Row.Sparkline = append(d.Row.Sparkline, float64(v))
	}
	sp.Close()

	nRows, err := s.Pool.Query(
		ctx,
		`
		SELECT   n.source_prefix
		        ,COALESCE(SUM(n.packets), 0)
		        ,AVG(n.avg_rssi)::float
		        ,MIN(n.min_rssi)
		        ,MAX(n.max_rssi)
		        ,AVG(n.avg_snr)::float
		        ,MIN(n.bucket)
		        ,MAX(n.bucket)
		        ,(SELECT COUNT(DISTINCT observer_id)
		          FROM   rollup_neighbor_1m
		          WHERE  source_prefix = n.source_prefix
		             AND bucket >= now() - interval '24 hours')
		FROM    rollup_neighbor_1m n
		WHERE   n.observer_id = $1
		    AND n.bucket >= now() - interval '24 hours'
		GROUP BY n.source_prefix
		ORDER BY SUM(n.packets) DESC
		LIMIT 100
		`,
		id,
	)
	if err == nil {
		defer nRows.Close()

		for nRows.Next() {
			var nr templates.NeighborRow

			err := nRows.Scan(
				&nr.SourcePrefix,
				&nr.Packets,
				&nr.AvgRSSI,
				&nr.MinRSSI,
				&nr.MaxRSSI,
				&nr.AvgSNR,
				&nr.FirstSeen,
				&nr.LastSeen,
				&nr.HeardBy,
			)
			if err == nil {
				d.Neighbors = append(d.Neighbors, nr)
			}
		}
	}

	tRows, err := s.Pool.Query(
		ctx,
		`
		SELECT   COALESCE(NULLIF(packet_type, ''), 'UNKNOWN')
		        ,COUNT(*)
		FROM    packet_observations
		WHERE   observer_id = $1
		    AND ts >= now() - interval '1 hour'
		GROUP BY COALESCE(NULLIF(packet_type, ''), 'UNKNOWN')
		ORDER BY 2 DESC
		`,
		id,
	)
	if err == nil {
		defer tRows.Close()

		for tRows.Next() {
			var t templates.TypeCount
			if err := tRows.Scan(&t.Type, &t.Count); err == nil {
				d.TypeMix = append(d.TypeMix, t)
			}
		}
	}

	return d, nil
}

func queryNeighbors(
	ctx context.Context,
	s *store.Store,
) ([]templates.NeighborRow, error) {
	rows, err := s.Pool.Query(
		ctx,
		`
		SELECT   n.source_prefix
		        ,COALESCE(SUM(n.packets), 0)
		        ,AVG(n.avg_rssi)::float
		        ,MIN(n.min_rssi)
		        ,MAX(n.max_rssi)
		        ,AVG(n.avg_snr)::float
		        ,MIN(n.bucket)
		        ,MAX(n.bucket)
		        ,COUNT(DISTINCT n.observer_id)
		FROM    rollup_neighbor_1m n
		WHERE   n.bucket >= now() - interval '24 hours'
		GROUP BY n.source_prefix
		ORDER BY SUM(n.packets) DESC
		LIMIT 500
		`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []templates.NeighborRow
	for rows.Next() {
		var r templates.NeighborRow

		err := rows.Scan(
			&r.SourcePrefix,
			&r.Packets,
			&r.AvgRSSI,
			&r.MinRSSI,
			&r.MaxRSSI,
			&r.AvgSNR,
			&r.FirstSeen,
			&r.LastSeen,
			&r.HeardBy,
		)
		if err == nil {
			out = append(out, r)
		}
	}

	return out, nil
}

// queryLiveSince returns the rows newer than `since` (or the most recent
// `limit` rows when `since` is the zero time), newest first.
func queryLiveSince(
	ctx context.Context,
	s *store.Store,
	since time.Time,
	limit int,
) ([]templates.LivePacket, error) {
	rows, err := s.Pool.Query(
		ctx,
		`
		SELECT   p.ts
		        ,p.observer_id
		        ,COALESCE(o.origin_name, '')
		        ,p.packet_hash
		        ,COALESCE(NULLIF(p.packet_type, ''), '')
		        ,COALESCE(p.route, '')
		        ,p.rssi
		        ,p.snr
		        ,p.score
		        ,p.source_prefix
		FROM    packet_observations p
		LEFT JOIN observers o ON o.id = p.observer_id
		WHERE   p.ts > $1
		ORDER BY p.ts DESC
		LIMIT $2
		`,
		since,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []templates.LivePacket
	for rows.Next() {
		var p templates.LivePacket

		err := rows.Scan(
			&p.TS,
			&p.ObserverID,
			&p.Origin,
			&p.Hash,
			&p.Type,
			&p.Route,
			&p.RSSI,
			&p.SNR,
			&p.Score,
			&p.Source,
		)
		if err == nil {
			out = append(out, p)
		}
	}

	return out, nil
}

func queryLiveInitial(
	ctx context.Context,
	s *store.Store,
	limit int,
) ([]templates.LivePacket, error) {
	rows, err := s.Pool.Query(
		ctx,
		`
		SELECT   p.ts
		        ,p.observer_id
		        ,COALESCE(o.origin_name, '')
		        ,p.packet_hash
		        ,COALESCE(NULLIF(p.packet_type, ''), '')
		        ,COALESCE(p.route, '')
		        ,p.rssi
		        ,p.snr
		        ,p.score
		        ,p.source_prefix
		FROM    packet_observations p
		LEFT JOIN observers o ON o.id = p.observer_id
		WHERE   p.ts >= now() - interval '10 minutes'
		ORDER BY p.ts DESC
		LIMIT $1
		`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []templates.LivePacket
	for rows.Next() {
		var p templates.LivePacket

		err := rows.Scan(
			&p.TS,
			&p.ObserverID,
			&p.Origin,
			&p.Hash,
			&p.Type,
			&p.Route,
			&p.RSSI,
			&p.SNR,
			&p.Score,
			&p.Source,
		)
		if err == nil {
			out = append(out, p)
		}
	}

	return out, nil
}

func queryAnomalies(
	ctx context.Context,
	s *store.Store,
) ([]templates.AnomalyRow, error) {
	rows, err := s.Pool.Query(
		ctx,
		`
		SELECT   id
		        ,ts
		        ,kind
		        ,severity
		        ,subject_id
		        ,details
		FROM    anomalies
		WHERE   ts >= now() - interval '48 hours'
		ORDER BY ts DESC
		LIMIT 200
		`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []templates.AnomalyRow
	for rows.Next() {
		var r templates.AnomalyRow

		err := rows.Scan(
			&r.ID,
			&r.TS,
			&r.Kind,
			&r.Severity,
			&r.Subject,
			&r.Details,
		)
		if err == nil {
			out = append(out, r)
		}
	}

	return out, nil
}

func queryTopology(
	ctx context.Context,
	s *store.Store,
) (templates.TopologyData, error) {
	var d templates.TopologyData

	oRows, err := s.Pool.Query(
		ctx,
		`SELECT id, origin_name FROM observers ORDER BY origin_name`,
	)
	if err != nil {
		return d, err
	}
	defer oRows.Close()

	for oRows.Next() {
		var (
			id   []byte
			name string
		)

		if err := oRows.Scan(&id, &name); err == nil {
			d.Nodes = append(d.Nodes, templates.TopoNode{
				ID:         "obs:" + templates.HexFull(id),
				Label:      name,
				IsObserver: true,
			})
		}
	}

	sRows, err := s.Pool.Query(
		ctx,
		`
		SELECT DISTINCT source_prefix
		FROM   rollup_neighbor_1m
		WHERE  bucket >= now() - interval '24 hours'
		`,
	)
	if err == nil {
		defer sRows.Close()

		for sRows.Next() {
			var sp []byte
			if err := sRows.Scan(&sp); err == nil {
				d.Nodes = append(d.Nodes, templates.TopoNode{
					ID:         "src:" + templates.HexFull(sp),
					Label:      templates.HexShort(sp),
					IsObserver: false,
				})
			}
		}
	}

	eRows, err := s.Pool.Query(
		ctx,
		`
		SELECT   observer_id
		        ,source_prefix
		        ,SUM(packets)
		        ,AVG(avg_rssi)::float
		FROM    rollup_neighbor_1m
		WHERE   bucket >= now() - interval '24 hours'
		GROUP BY observer_id, source_prefix
		`,
	)
	if err == nil {
		defer eRows.Close()

		for eRows.Next() {
			var (
				obs  []byte
				sp   []byte
				pkts int
				rssi float64
			)

			if err := eRows.Scan(&obs, &sp, &pkts, &rssi); err == nil {
				d.Edges = append(d.Edges, templates.TopoEdge{
					From:    "obs:" + templates.HexFull(obs),
					To:      "src:" + templates.HexFull(sp),
					Packets: pkts,
					AvgRSSI: rssi,
				})
			}
		}
	}

	d.Nodes = templates.LayoutTopology(d.Nodes)

	// Resolve edge endpoints to coordinates so the template doesn't have to.
	pos := map[string]templates.TopoNode{}
	for _, n := range d.Nodes {
		pos[n.ID] = n
	}

	for i, e := range d.Edges {
		if a, ok := pos[e.From]; ok {
			d.Edges[i].FromX, d.Edges[i].FromY = a.X, a.Y
		}
		if b, ok := pos[e.To]; ok {
			d.Edges[i].ToX, d.Edges[i].ToY = b.X, b.Y
		}
	}

	return d, nil
}
