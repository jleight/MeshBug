package stages

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jleight/meshbug/internal/project/pipeline"
)

// Nodes owns project.nodes — one row per ed25519 public key seen in an
// ADVERT packet. We update the descriptive fields only from the advert
// with the highest advert.Timestamp we've ever seen for that key, so
// late-arriving stragglers can't overwrite a fresher advert. last_seen
// and advert_count update from every advert.
type Nodes struct {
	touched map[string]*nodeRow
}

type nodeRow struct {
	publicKey []byte
	prefix1   []byte
	prefix4   []byte

	nodeType string

	name string

	hasLatLon bool
	latE6     int32
	lonE6     int32

	hasFeat1 bool
	feat1    uint16

	hasFeat2 bool
	feat2    uint16

	advertTS         time.Time
	advertReceivedAt time.Time
	advertCount      int64

	firstSeen time.Time
	lastSeen  time.Time
}

func NewNodes() *Nodes {
	return &Nodes{touched: map[string]*nodeRow{}}
}

func (s *Nodes) Name() string {
	return "nodes"
}

func (s *Nodes) Apply(_ context.Context, e pipeline.Event) error {
	if e.Kind != pipeline.KindPacket || e.Packet == nil || e.Packet.Advert == nil {
		return nil
	}

	adv := e.Packet.Advert
	if len(adv.PublicKey) == 0 {
		return nil
	}

	key := string(adv.PublicKey)

	row, ok := s.touched[key]
	if !ok {
		row = &nodeRow{
			publicKey: append([]byte(nil), adv.PublicKey...),
			prefix1:   append([]byte(nil), adv.PublicKey[:1]...),
			firstSeen: e.TS,
			lastSeen:  e.TS,
		}

		if len(adv.PublicKey) >= 4 {
			row.prefix4 = append([]byte(nil), adv.PublicKey[:4]...)
		} else {
			row.prefix4 = append([]byte(nil), adv.PublicKey...)
		}

		s.touched[key] = row
	}

	row.advertCount++

	if e.TS.Before(row.firstSeen) {
		row.firstSeen = e.TS
	}

	if e.TS.After(row.lastSeen) {
		row.lastSeen = e.TS
	}

	// Same wire timestamp as the advert we already have — another observer
	// (or a retransmit) reporting the same advert. Pull the earliest
	// reception time forward.
	if adv.Timestamp.Equal(row.advertTS) {
		if row.advertReceivedAt.IsZero() || e.TS.Before(row.advertReceivedAt) {
			row.advertReceivedAt = e.TS
		}

		return nil
	}

	// Only adopt descriptive fields from this advert if it is strictly
	// newer (by the advert's own timestamp) than what we already have.
	if !adv.Timestamp.After(row.advertTS) {
		return nil
	}

	row.advertTS = adv.Timestamp
	row.advertReceivedAt = e.TS
	row.nodeType = adv.NodeType
	row.name = adv.Name

	row.hasLatLon = adv.HasLatLon
	row.latE6 = adv.LatE6
	row.lonE6 = adv.LonE6

	row.hasFeat1 = adv.HasFeat1
	row.feat1 = adv.Feat1

	row.hasFeat2 = adv.HasFeat2
	row.feat2 = adv.Feat2

	return nil
}

func (s *Nodes) Flush(ctx context.Context, tx pgx.Tx) error {
	if len(s.touched) == 0 {
		return nil
	}

	for _, r := range s.touched {
		var (
			latE6 *int32
			lonE6 *int32
			feat1 *int
			feat2 *int
		)

		if r.hasLatLon {
			lat := r.latE6
			lon := r.lonE6
			latE6 = &lat
			lonE6 = &lon
		}

		if r.hasFeat1 {
			v := int(r.feat1)
			feat1 = &v
		}

		if r.hasFeat2 {
			v := int(r.feat2)
			feat2 = &v
		}

		var advertTS *time.Time
		if !r.advertTS.IsZero() {
			ts := r.advertTS
			advertTS = &ts
		}

		var advertReceivedAt *time.Time
		if !r.advertReceivedAt.IsZero() {
			ts := r.advertReceivedAt
			advertReceivedAt = &ts
		}

		_, err := tx.Exec(
			ctx,
			`
			INSERT INTO nodes
			    (public_key
			    ,prefix1
			    ,prefix4
			    ,node_type
			    ,name
			    ,has_lat_lon
			    ,lat_e6
			    ,lon_e6
			    ,has_feat1
			    ,feat1
			    ,has_feat2
			    ,feat2
			    ,advert_timestamp
			    ,advert_received_at
			    ,advert_count
			    ,first_seen
			    ,last_seen)
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
			    ,$11
			    ,$12
			    ,$13
			    ,$14
			    ,$15
			    ,$16
			    ,$17)
			ON CONFLICT (public_key) DO UPDATE SET
			     node_type        = CASE WHEN EXCLUDED.advert_timestamp IS NOT NULL
			                              AND (nodes.advert_timestamp IS NULL
			                                   OR EXCLUDED.advert_timestamp > nodes.advert_timestamp)
			                             THEN EXCLUDED.node_type
			                             ELSE nodes.node_type END
			    ,name             = CASE WHEN EXCLUDED.advert_timestamp IS NOT NULL
			                              AND (nodes.advert_timestamp IS NULL
			                                   OR EXCLUDED.advert_timestamp > nodes.advert_timestamp)
			                             THEN EXCLUDED.name
			                             ELSE nodes.name END
			    ,has_lat_lon      = CASE WHEN EXCLUDED.advert_timestamp IS NOT NULL
			                              AND (nodes.advert_timestamp IS NULL
			                                   OR EXCLUDED.advert_timestamp > nodes.advert_timestamp)
			                             THEN EXCLUDED.has_lat_lon
			                             ELSE nodes.has_lat_lon END
			    ,lat_e6           = CASE WHEN EXCLUDED.advert_timestamp IS NOT NULL
			                              AND (nodes.advert_timestamp IS NULL
			                                   OR EXCLUDED.advert_timestamp > nodes.advert_timestamp)
			                             THEN EXCLUDED.lat_e6
			                             ELSE nodes.lat_e6 END
			    ,lon_e6           = CASE WHEN EXCLUDED.advert_timestamp IS NOT NULL
			                              AND (nodes.advert_timestamp IS NULL
			                                   OR EXCLUDED.advert_timestamp > nodes.advert_timestamp)
			                             THEN EXCLUDED.lon_e6
			                             ELSE nodes.lon_e6 END
			    ,has_feat1        = CASE WHEN EXCLUDED.advert_timestamp IS NOT NULL
			                              AND (nodes.advert_timestamp IS NULL
			                                   OR EXCLUDED.advert_timestamp > nodes.advert_timestamp)
			                             THEN EXCLUDED.has_feat1
			                             ELSE nodes.has_feat1 END
			    ,feat1            = CASE WHEN EXCLUDED.advert_timestamp IS NOT NULL
			                              AND (nodes.advert_timestamp IS NULL
			                                   OR EXCLUDED.advert_timestamp > nodes.advert_timestamp)
			                             THEN EXCLUDED.feat1
			                             ELSE nodes.feat1 END
			    ,has_feat2        = CASE WHEN EXCLUDED.advert_timestamp IS NOT NULL
			                              AND (nodes.advert_timestamp IS NULL
			                                   OR EXCLUDED.advert_timestamp > nodes.advert_timestamp)
			                             THEN EXCLUDED.has_feat2
			                             ELSE nodes.has_feat2 END
			    ,feat2            = CASE WHEN EXCLUDED.advert_timestamp IS NOT NULL
			                              AND (nodes.advert_timestamp IS NULL
			                                   OR EXCLUDED.advert_timestamp > nodes.advert_timestamp)
			                             THEN EXCLUDED.feat2
			                             ELSE nodes.feat2 END
			    ,advert_timestamp   = GREATEST(nodes.advert_timestamp, EXCLUDED.advert_timestamp)
			    ,advert_received_at = CASE
			        WHEN EXCLUDED.advert_timestamp IS NULL THEN nodes.advert_received_at
			        WHEN nodes.advert_timestamp IS NULL
			          OR EXCLUDED.advert_timestamp > nodes.advert_timestamp
			            THEN EXCLUDED.advert_received_at
			        WHEN EXCLUDED.advert_timestamp = nodes.advert_timestamp
			            THEN LEAST(
			                nodes.advert_received_at,
			                EXCLUDED.advert_received_at
			            )
			        ELSE nodes.advert_received_at
			      END
			    ,advert_count    = nodes.advert_count + EXCLUDED.advert_count
			    ,first_seen      = LEAST(nodes.first_seen, EXCLUDED.first_seen)
			    ,last_seen       = GREATEST(nodes.last_seen, EXCLUDED.last_seen)
			`,
			r.publicKey,
			r.prefix1,
			r.prefix4,
			r.nodeType,
			r.name,
			r.hasLatLon,
			latE6,
			lonE6,
			r.hasFeat1,
			feat1,
			r.hasFeat2,
			feat2,
			advertTS,
			advertReceivedAt,
			r.advertCount,
			r.firstSeen,
			r.lastSeen,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Nodes) Notify(_ context.Context, _ *pgxpool.Pool) error {
	return nil
}

func (s *Nodes) Evict(_ time.Time) {
	s.touched = map[string]*nodeRow{}
}

func (s *Nodes) Clear() {
	s.touched = map[string]*nodeRow{}
}
