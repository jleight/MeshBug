package stages

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jleight/meshbug/internal/project/pipeline"
)

// RollupObserver owns one of project.rollup_observer_1m or
// project.rollup_observer_1h. The two are structurally identical (same
// columns, same rules); they differ only by bucket size and retention.
//
// Accumulator per (observer, bucket): packet count + per-route counters,
// running sum + count for rssi/snr (avg = sum/n at flush time), running
// min/max, the set of distinct packet hashes (for unique_pkts), and the
// latest status event's noise_floor seen within the bucket.
//
// Eviction: drop buckets whose end time falls outside the retention
// window. A straggler in an evicted bucket triggers a per-bucket reload
// of the existing row on first touch.
type RollupObserver struct {
	table     string
	bucketDur time.Duration
	retention time.Duration

	state map[obsBucketKey]*observerAccum
}

type obsBucketKey struct {
	observerID string
	bucket     int64
}

type observerAccum struct {
	observerID []byte
	bucket     time.Time

	packets     int64
	uniquePkts  map[string]struct{}
	floodPkts   int64
	directPkts  int64

	sumRSSI int64
	nRSSI   int64
	minRSSI *int
	maxRSSI *int

	sumSNR float64
	nSNR   int64

	noiseFloor   *int
	noiseFloorTS time.Time

	loaded bool
	dirty  bool
}

// NewRollupObserver1m returns a stage that maintains
// project.rollup_observer_1m.
func NewRollupObserver1m() *RollupObserver {
	return &RollupObserver{
		table:     "rollup_observer_1m",
		bucketDur: time.Minute,
		retention: time.Hour,
		state:     map[obsBucketKey]*observerAccum{},
	}
}

// NewRollupObserver1h returns a stage that maintains
// project.rollup_observer_1h.
func NewRollupObserver1h() *RollupObserver {
	return &RollupObserver{
		table:     "rollup_observer_1h",
		bucketDur: time.Hour,
		retention: 24 * time.Hour,
		state:     map[obsBucketKey]*observerAccum{},
	}
}

func (s *RollupObserver) Name() string {
	return s.table
}

func (s *RollupObserver) Apply(ctx context.Context, e pipeline.Event) error {
	switch e.Kind {
	case pipeline.KindPacket:
		s.applyPacket(e)

	case pipeline.KindStatus:
		s.applyStatus(e)
	}

	return nil
}

func (s *RollupObserver) applyPacket(e pipeline.Event) {
	bucket := e.TS.UTC().Truncate(s.bucketDur)
	acc := s.touch(e.ObserverID, bucket)

	p := e.Packet
	acc.packets++

	if len(p.PacketHash) > 0 {
		acc.uniquePkts[string(p.PacketHash)] = struct{}{}
	}

	switch p.Route {
	case "F":
		acc.floodPkts++
	case "D":
		acc.directPkts++
	}

	if p.RSSI != nil {
		v := *p.RSSI
		acc.sumRSSI += int64(v)
		acc.nRSSI++

		if acc.minRSSI == nil || v < *acc.minRSSI {
			vv := v
			acc.minRSSI = &vv
		}

		if acc.maxRSSI == nil || v > *acc.maxRSSI {
			vv := v
			acc.maxRSSI = &vv
		}
	}

	if p.SNR != nil {
		acc.sumSNR += *p.SNR
		acc.nSNR++
	}

	acc.dirty = true
}

func (s *RollupObserver) applyStatus(e pipeline.Event) {
	if e.Status.NoiseFloor == nil {
		return
	}

	bucket := e.TS.UTC().Truncate(s.bucketDur)
	acc := s.touch(e.ObserverID, bucket)

	if e.TS.After(acc.noiseFloorTS) {
		acc.noiseFloor = e.Status.NoiseFloor
		acc.noiseFloorTS = e.TS
		acc.dirty = true
	}
}

func (s *RollupObserver) touch(observerID []byte, bucket time.Time) *observerAccum {
	key := obsBucketKey{
		observerID: string(observerID),
		bucket:     bucket.UnixNano(),
	}

	acc, ok := s.state[key]
	if !ok {
		acc = &observerAccum{
			observerID: append([]byte(nil), observerID...),
			bucket:     bucket,
			uniquePkts: map[string]struct{}{},
		}
		s.state[key] = acc
	}

	return acc
}

func (s *RollupObserver) Flush(ctx context.Context, tx pgx.Tx) error {
	err := s.rehydrateUnloaded(ctx, tx)
	if err != nil {
		return err
	}

	for _, acc := range s.state {
		if !acc.dirty {
			continue
		}

		var avgRSSI *float64
		if acc.nRSSI > 0 {
			v := float64(acc.sumRSSI) / float64(acc.nRSSI)
			avgRSSI = &v
		}

		var avgSNR *float64
		if acc.nSNR > 0 {
			v := acc.sumSNR / float64(acc.nSNR)
			avgSNR = &v
		}

		_, err := tx.Exec(
			ctx,
			`
			INSERT INTO `+s.table+`
			    (observer_id
			    ,bucket
			    ,packets
			    ,unique_pkts
			    ,flood_pkts
			    ,direct_pkts
			    ,avg_rssi
			    ,min_rssi
			    ,max_rssi
			    ,avg_snr
			    ,noise_floor)
			VALUES
			    ($1
			    ,$2
			    ,$3
			    ,$4
			    ,$5
			    ,$6
			    ,$7
			    ,$8
			    ,$9
			    ,$10
			    ,$11)
			ON CONFLICT (observer_id, bucket) DO UPDATE SET
			     packets     = EXCLUDED.packets
			    ,unique_pkts = EXCLUDED.unique_pkts
			    ,flood_pkts  = EXCLUDED.flood_pkts
			    ,direct_pkts = EXCLUDED.direct_pkts
			    ,avg_rssi    = EXCLUDED.avg_rssi
			    ,min_rssi    = EXCLUDED.min_rssi
			    ,max_rssi    = EXCLUDED.max_rssi
			    ,avg_snr     = EXCLUDED.avg_snr
			    ,noise_floor = COALESCE(EXCLUDED.noise_floor, `+s.table+`.noise_floor)
			`,
			acc.observerID,
			acc.bucket,
			acc.packets,
			len(acc.uniquePkts),
			acc.floodPkts,
			acc.directPkts,
			avgRSSI,
			acc.minRSSI,
			acc.maxRSSI,
			avgSNR,
			acc.noiseFloor,
		)
		if err != nil {
			return err
		}

		acc.dirty = false
	}

	return nil
}

// rehydrateUnloaded merges historical state into accumulators for
// buckets touched this batch that haven't been loaded yet. After a reset
// the SELECTs return nothing and this is essentially free. The two
// queries are scoped to the pending buckets, not the whole table.
//
// avg_rssi/avg_snr are re-derived as sum ≈ avg * packets, exact up to
// the column's numeric(6,2) precision. unique_pkts is rebuilt by
// pulling the distinct packet hashes for each bucket from
// packet_observations, so set arithmetic stays correct when a straggler
// arrives in a bucket that's already been written.
func (s *RollupObserver) rehydrateUnloaded(ctx context.Context, tx pgx.Tx) error {
	var (
		obsIDs  [][]byte
		buckets []time.Time
		ends    []time.Time
	)

	for _, acc := range s.state {
		if acc.loaded || !acc.dirty {
			continue
		}

		obsIDs = append(obsIDs, acc.observerID)
		buckets = append(buckets, acc.bucket)
		ends = append(ends, acc.bucket.Add(s.bucketDur))
	}

	if len(obsIDs) == 0 {
		return nil
	}

	err := s.loadRowTotals(ctx, tx, obsIDs, buckets)
	if err != nil {
		return err
	}

	err = s.loadHistoricalHashes(ctx, tx, obsIDs, buckets, ends)
	if err != nil {
		return err
	}

	for _, acc := range s.state {
		if !acc.loaded && acc.dirty {
			acc.loaded = true
		}
	}

	return nil
}

func (s *RollupObserver) loadRowTotals(
	ctx context.Context,
	tx pgx.Tx,
	obsIDs [][]byte,
	buckets []time.Time,
) error {
	rows, err := tx.Query(
		ctx,
		`
		SELECT   observer_id
		        ,bucket
		        ,packets
		        ,flood_pkts
		        ,direct_pkts
		        ,avg_rssi
		        ,min_rssi
		        ,max_rssi
		        ,avg_snr
		        ,noise_floor
		FROM    `+s.table+`
		WHERE   (observer_id, bucket) IN (
		            SELECT o, b
		            FROM   unnest($1::bytea[], $2::timestamptz[]) AS u(o, b)
		        )
		`,
		obsIDs,
		buckets,
	)
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var (
			observerID []byte
			bucket     time.Time
			packets    int64
			floodPkts  int64
			directPkts int64
			avgRSSI    *float64
			minRSSI    *int
			maxRSSI    *int
			avgSNR     *float64
			noiseFloor *int
		)

		err := rows.Scan(
			&observerID,
			&bucket,
			&packets,
			&floodPkts,
			&directPkts,
			&avgRSSI,
			&minRSSI,
			&maxRSSI,
			&avgSNR,
			&noiseFloor,
		)
		if err != nil {
			return err
		}

		key := obsBucketKey{
			observerID: string(observerID),
			bucket:     bucket.UnixNano(),
		}

		acc, ok := s.state[key]
		if !ok {
			continue
		}

		acc.packets += packets
		acc.floodPkts += floodPkts
		acc.directPkts += directPkts

		if avgRSSI != nil && packets > 0 {
			acc.sumRSSI += int64(*avgRSSI * float64(packets))
			acc.nRSSI += packets
		}

		if minRSSI != nil && (acc.minRSSI == nil || *minRSSI < *acc.minRSSI) {
			v := *minRSSI
			acc.minRSSI = &v
		}

		if maxRSSI != nil && (acc.maxRSSI == nil || *maxRSSI > *acc.maxRSSI) {
			v := *maxRSSI
			acc.maxRSSI = &v
		}

		if avgSNR != nil && packets > 0 {
			acc.sumSNR += *avgSNR * float64(packets)
			acc.nSNR += packets
		}

		if acc.noiseFloor == nil && noiseFloor != nil {
			v := *noiseFloor
			acc.noiseFloor = &v
		}
	}

	return rows.Err()
}

func (s *RollupObserver) loadHistoricalHashes(
	ctx context.Context,
	tx pgx.Tx,
	obsIDs [][]byte,
	starts []time.Time,
	ends []time.Time,
) error {
	rows, err := tx.Query(
		ctx,
		`
		SELECT   po.observer_id
		        ,po.packet_hash
		        ,po.ts
		FROM    packet_observations po
		JOIN    unnest($1::bytea[], $2::timestamptz[], $3::timestamptz[])
		            AS u(o, s, e)
		    ON  po.observer_id = u.o
		   AND  po.ts >= u.s
		   AND  po.ts <  u.e
		WHERE   po.packet_hash IS NOT NULL
		`,
		obsIDs,
		starts,
		ends,
	)
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var (
			observerID []byte
			hash       []byte
			ts         time.Time
		)

		err := rows.Scan(&observerID, &hash, &ts)
		if err != nil {
			return err
		}

		bucket := ts.UTC().Truncate(s.bucketDur)
		key := obsBucketKey{
			observerID: string(observerID),
			bucket:     bucket.UnixNano(),
		}

		acc, ok := s.state[key]
		if !ok {
			continue
		}

		acc.uniquePkts[string(hash)] = struct{}{}
	}

	return rows.Err()
}

func (s *RollupObserver) Notify(ctx context.Context, pool *pgxpool.Pool) error {
	return nil
}

func (s *RollupObserver) Evict(hwm time.Time) {
	cutoff := hwm.Add(-s.retention)

	for k, acc := range s.state {
		if acc.dirty {
			continue
		}

		bucketEnd := acc.bucket.Add(s.bucketDur)
		if bucketEnd.Before(cutoff) {
			delete(s.state, k)
		}
	}

	for _, acc := range s.state {
		acc.dirty = false
	}
}

func (s *RollupObserver) Clear() {
	s.state = map[obsBucketKey]*observerAccum{}
}

