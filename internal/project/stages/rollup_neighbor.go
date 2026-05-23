package stages

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jleight/meshbug/internal/project/pipeline"
)

// RollupNeighbor1m owns project.rollup_neighbor_1m. One row per
// (observer, source_prefix, minute) summarising packets we heard from a
// given neighbor at a given observer. Only packet events with a non-nil
// source_prefix contribute.
type RollupNeighbor1m struct {
	state map[neighborBucketKey]*neighborAccum
}

type neighborBucketKey struct {
	observerID   string
	sourcePrefix string
	bucket       int64
}

type neighborAccum struct {
	observerID   []byte
	sourcePrefix []byte
	bucket       time.Time

	packets int64

	sumRSSI int64
	nRSSI   int64
	minRSSI *int
	maxRSSI *int

	sumSNR float64
	nSNR   int64

	loaded bool
	dirty  bool
}

func NewRollupNeighbor1m() *RollupNeighbor1m {
	return &RollupNeighbor1m{state: map[neighborBucketKey]*neighborAccum{}}
}

func (s *RollupNeighbor1m) Name() string {
	return "rollup_neighbor_1m"
}

func (s *RollupNeighbor1m) Apply(ctx context.Context, e pipeline.Event) error {
	if e.Kind != pipeline.KindPacket {
		return nil
	}

	p := e.Packet
	if len(p.SourcePrefix) == 0 {
		return nil
	}

	bucket := e.TS.UTC().Truncate(time.Minute)

	key := neighborBucketKey{
		observerID:   string(e.ObserverID),
		sourcePrefix: string(p.SourcePrefix),
		bucket:       bucket.UnixNano(),
	}

	acc, ok := s.state[key]
	if !ok {
		acc = &neighborAccum{
			observerID:   append([]byte(nil), e.ObserverID...),
			sourcePrefix: append([]byte(nil), p.SourcePrefix...),
			bucket:       bucket,
		}
		s.state[key] = acc
	}

	acc.packets++

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
	return nil
}

func (s *RollupNeighbor1m) Flush(ctx context.Context, tx pgx.Tx) error {
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
			INSERT INTO rollup_neighbor_1m
			    (observer_id
			    ,source_prefix
			    ,bucket
			    ,packets
			    ,avg_rssi
			    ,min_rssi
			    ,max_rssi
			    ,avg_snr)
			VALUES
			    ($1
			    ,$2
			    ,$3
			    ,$4
			    ,$5
			    ,$6
			    ,$7
			    ,$8)
			ON CONFLICT (observer_id, source_prefix, bucket) DO UPDATE SET
			     packets  = EXCLUDED.packets
			    ,avg_rssi = EXCLUDED.avg_rssi
			    ,min_rssi = EXCLUDED.min_rssi
			    ,max_rssi = EXCLUDED.max_rssi
			    ,avg_snr  = EXCLUDED.avg_snr
			`,
			acc.observerID,
			acc.sourcePrefix,
			acc.bucket,
			acc.packets,
			avgRSSI,
			acc.minRSSI,
			acc.maxRSSI,
			avgSNR,
		)
		if err != nil {
			return err
		}

		acc.dirty = false
	}

	return nil
}

func (s *RollupNeighbor1m) rehydrateUnloaded(ctx context.Context, tx pgx.Tx) error {
	var (
		obsArr [][]byte
		srcArr [][]byte
		bktArr []time.Time
	)

	for _, acc := range s.state {
		if acc.loaded || !acc.dirty {
			continue
		}

		obsArr = append(obsArr, acc.observerID)
		srcArr = append(srcArr, acc.sourcePrefix)
		bktArr = append(bktArr, acc.bucket)
	}

	if len(obsArr) == 0 {
		return nil
	}

	rows, err := tx.Query(
		ctx,
		`
		SELECT   observer_id
		        ,source_prefix
		        ,bucket
		        ,packets
		        ,avg_rssi
		        ,min_rssi
		        ,max_rssi
		        ,avg_snr
		FROM    rollup_neighbor_1m
		WHERE   (observer_id, source_prefix, bucket) IN (
		            SELECT o, sp, b
		            FROM   unnest($1::bytea[], $2::bytea[], $3::timestamptz[])
		                AS u(o, sp, b)
		        )
		`,
		obsArr,
		srcArr,
		bktArr,
	)
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var (
			observerID []byte
			src        []byte
			bucket     time.Time
			packets    int64
			avgRSSI    *float64
			minRSSI    *int
			maxRSSI    *int
			avgSNR     *float64
		)

		err := rows.Scan(
			&observerID,
			&src,
			&bucket,
			&packets,
			&avgRSSI,
			&minRSSI,
			&maxRSSI,
			&avgSNR,
		)
		if err != nil {
			return err
		}

		key := neighborBucketKey{
			observerID:   string(observerID),
			sourcePrefix: string(src),
			bucket:       bucket.UnixNano(),
		}

		acc, ok := s.state[key]
		if !ok {
			continue
		}

		acc.packets += packets

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
	}

	if err := rows.Err(); err != nil {
		return err
	}

	for _, acc := range s.state {
		if !acc.loaded && acc.dirty {
			acc.loaded = true
		}
	}

	return nil
}

func (s *RollupNeighbor1m) Notify(ctx context.Context, pool *pgxpool.Pool) error {
	return nil
}

func (s *RollupNeighbor1m) Evict(hwm time.Time) {
	cutoff := hwm.Add(-time.Hour)

	for k, acc := range s.state {
		if acc.dirty {
			continue
		}

		bucketEnd := acc.bucket.Add(time.Minute)
		if bucketEnd.Before(cutoff) {
			delete(s.state, k)
		}
	}

	for _, acc := range s.state {
		acc.dirty = false
	}
}

func (s *RollupNeighbor1m) Clear() {
	s.state = map[neighborBucketKey]*neighborAccum{}
}
