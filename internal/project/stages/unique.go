package stages

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jleight/meshbug/internal/project/pipeline"
)

// uniqueRetention is how long we keep a hash in memory after its last
// observed timestamp. Longer-running live mode is fine — bounded by the
// number of distinct packets in the rolling window.
const uniqueRetention = time.Hour

// PacketsUnique owns project.packets_unique. The accumulator holds the
// canonical set of observers per hash so observer_count is correct
// regardless of how packets are batched. When a hash is touched after
// eviction, the stage rehydrates from the DB row + a DISTINCT observer_id
// scan of packet_observations.
type PacketsUnique struct {
	state map[string]*uniqueState
}

type uniqueState struct {
	hash         []byte
	firstSeen    time.Time
	lastSeen     time.Time
	packetType   string
	route        string
	decodedType  *int16
	sourcePrefix []byte

	observers map[string]struct{}

	// loaded is true once any historical state for this hash has been
	// merged in. Fresh-allocated entries start false; rehydrate flips
	// them to true. Reset-replay never flips this because the DB is
	// empty.
	loaded bool

	dirty bool
}

func NewPacketsUnique() *PacketsUnique {
	return &PacketsUnique{state: map[string]*uniqueState{}}
}

func (s *PacketsUnique) Name() string {
	return "packets_unique"
}

func (s *PacketsUnique) Apply(ctx context.Context, e pipeline.Event) error {
	if e.Kind != pipeline.KindPacket {
		return nil
	}

	p := e.Packet
	if len(p.PacketHash) == 0 {
		return nil
	}

	key := string(p.PacketHash)

	st, ok := s.state[key]
	if !ok {
		st = &uniqueState{
			hash:      append([]byte(nil), p.PacketHash...),
			firstSeen: e.TS,
			lastSeen:  e.TS,
			observers: map[string]struct{}{},
		}
		s.state[key] = st
	}

	if e.TS.Before(st.firstSeen) {
		st.firstSeen = e.TS
	}

	if e.TS.After(st.lastSeen) {
		st.lastSeen = e.TS
	}

	if p.PacketType != "" {
		st.packetType = p.PacketType
	}

	if p.Route != "" {
		st.route = p.Route
	}

	if p.DecodedType != nil {
		st.decodedType = p.DecodedType
	}

	if len(p.SourcePrefix) > 0 && st.sourcePrefix == nil {
		st.sourcePrefix = p.SourcePrefix
	}

	st.observers[string(e.ObserverID)] = struct{}{}
	st.dirty = true

	return nil
}

func (s *PacketsUnique) Flush(ctx context.Context, tx pgx.Tx) error {
	err := s.rehydrateUnloaded(ctx, tx)
	if err != nil {
		return err
	}

	for _, st := range s.state {
		if !st.dirty {
			continue
		}

		_, err := tx.Exec(
			ctx,
			`
			INSERT INTO packets_unique
			    (packet_hash
			    ,first_seen
			    ,last_seen
			    ,packet_type
			    ,route
			    ,decoded_type
			    ,source_prefix
			    ,observer_count)
			VALUES
			    ($1
			    ,$2
			    ,$3
			    ,$4
			    ,$5
			    ,$6
			    ,$7
			    ,$8)
			ON CONFLICT (packet_hash) DO UPDATE SET
			     first_seen     = LEAST(packets_unique.first_seen, EXCLUDED.first_seen)
			    ,last_seen      = GREATEST(packets_unique.last_seen, EXCLUDED.last_seen)
			    ,packet_type    = COALESCE(NULLIF(EXCLUDED.packet_type,''), packets_unique.packet_type)
			    ,route          = COALESCE(NULLIF(EXCLUDED.route,''), packets_unique.route)
			    ,decoded_type   = COALESCE(EXCLUDED.decoded_type, packets_unique.decoded_type)
			    ,source_prefix  = COALESCE(packets_unique.source_prefix, EXCLUDED.source_prefix)
			    ,observer_count = EXCLUDED.observer_count
			`,
			st.hash,
			st.firstSeen,
			st.lastSeen,
			st.packetType,
			st.route,
			st.decodedType,
			st.sourcePrefix,
			len(st.observers),
		)
		if err != nil {
			return err
		}

		st.dirty = false
	}

	return nil
}

// rehydrateUnloaded bulk-loads historical observer sets for any hashes
// touched this batch that we haven't loaded yet. After a reset the DB is
// empty so the bulk SELECT returns nothing and this is effectively free.
func (s *PacketsUnique) rehydrateUnloaded(ctx context.Context, tx pgx.Tx) error {
	var pending [][]byte

	for _, st := range s.state {
		if st.loaded || !st.dirty {
			continue
		}
		pending = append(pending, st.hash)
	}

	if len(pending) == 0 {
		return nil
	}

	rows, err := tx.Query(
		ctx,
		`
		SELECT   po.packet_hash
		        ,po.observer_id
		        ,pu.first_seen
		FROM    packet_observations po
		JOIN    packets_unique pu USING (packet_hash)
		WHERE   po.packet_hash = ANY($1)
		`,
		pending,
	)
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		var (
			hash      []byte
			obs       []byte
			firstSeen time.Time
		)

		err := rows.Scan(&hash, &obs, &firstSeen)
		if err != nil {
			return err
		}

		st, ok := s.state[string(hash)]
		if !ok {
			continue
		}

		if firstSeen.Before(st.firstSeen) {
			st.firstSeen = firstSeen
		}

		st.observers[string(obs)] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	for _, st := range s.state {
		if !st.loaded && st.dirty {
			st.loaded = true
		}
	}

	return nil
}

func (s *PacketsUnique) Notify(ctx context.Context, pool *pgxpool.Pool) error {
	return nil
}

func (s *PacketsUnique) Evict(hwm time.Time) {
	cutoff := hwm.Add(-uniqueRetention)

	for k, st := range s.state {
		if st.dirty {
			continue
		}

		if st.lastSeen.Before(cutoff) {
			delete(s.state, k)
		}
	}

	for _, st := range s.state {
		st.dirty = false
	}
}

func (s *PacketsUnique) Clear() {
	s.state = map[string]*uniqueState{}
}
