package stages

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jleight/meshbug/internal/notify"
	"github.com/jleight/meshbug/internal/project/pipeline"
)

// PacketObservations owns project.packet_observations. Append-only,
// CopyFrom-batched. Touched observer ids are reported via pg_notify on
// meshbug_packets after commit, so the SSE bridge can fan out updates.
type PacketObservations struct {
	buf     []packetRow
	touched map[string]struct{}
	count   int
}

type packetRow struct {
	ts             time.Time
	observerID     []byte
	packetHash     []byte
	direction      string
	packetType     string
	route          string
	length         *int
	payloadLen     *int
	rssi           *int
	snr            *float64
	score          *int
	durationMS     *int
	raw            []byte
	decodedType    *int16
	sourcePrefix   []byte
	destPrefix     []byte
	transportCodes []byte
}

func NewPacketObservations() *PacketObservations {
	return &PacketObservations{
		touched: map[string]struct{}{},
	}
}

func (s *PacketObservations) Name() string {
	return "packet_observations"
}

func (s *PacketObservations) Apply(ctx context.Context, e pipeline.Event) error {
	if e.Kind != pipeline.KindPacket {
		return nil
	}

	p := e.Packet

	s.buf = append(s.buf, packetRow{
		ts:             e.TS,
		observerID:     append([]byte(nil), e.ObserverID...),
		packetHash:     p.PacketHash,
		direction:      p.Direction,
		packetType:     p.PacketType,
		route:          p.Route,
		length:         p.Len,
		payloadLen:     p.PayloadLen,
		rssi:           p.RSSI,
		snr:            p.SNR,
		score:          p.Score,
		durationMS:     p.DurationMS,
		raw:            p.Raw,
		decodedType:    p.DecodedType,
		sourcePrefix:   p.SourcePrefix,
		destPrefix:     p.DestPrefix,
		transportCodes: p.TransportCodes,
	})

	s.touched[string(e.ObserverID)] = struct{}{}
	return nil
}

func (s *PacketObservations) Flush(ctx context.Context, tx pgx.Tx) error {
	if len(s.buf) == 0 {
		return nil
	}

	ids := make([]int64, len(s.buf))

	err := tx.
		QueryRow(
			ctx,
			`SELECT array_agg(nextval('packet_observations_id_seq')) FROM generate_series(1, $1)`,
			len(s.buf),
		).
		Scan(&ids)
	if err != nil {
		return err
	}

	src := pgx.CopyFromSlice(
		len(s.buf),
		func(i int) ([]any, error) {
			r := s.buf[i]
			return []any{
				r.ts,
				ids[i],
				r.observerID,
				r.packetHash,
				r.direction,
				r.packetType,
				r.route,
				r.length,
				r.payloadLen,
				r.rssi,
				r.snr,
				r.score,
				r.durationMS,
				r.raw,
				r.decodedType,
				r.sourcePrefix,
				r.destPrefix,
				r.transportCodes,
			}, nil
		},
	)

	n, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"packet_observations"},
		[]string{
			"ts",
			"observation_id",
			"observer_id",
			"packet_hash",
			"direction",
			"packet_type",
			"route",
			"len",
			"payload_len",
			"rssi",
			"snr",
			"score",
			"duration_ms",
			"raw",
			"decoded_type",
			"source_prefix",
			"dest_prefix",
			"transport_codes",
		},
		src,
	)
	if err != nil {
		return err
	}

	s.count = int(n)
	return nil
}

func (s *PacketObservations) Notify(ctx context.Context, pool *pgxpool.Pool) error {
	if s.count == 0 {
		return nil
	}

	ids := make([]string, 0, len(s.touched))
	for k := range s.touched {
		ids = append(ids, hex.EncodeToString([]byte(k)))
	}

	_ = notify.Publish(
		ctx,
		pool,
		notify.ChannelPackets,
		map[string]any{
			"count":     s.count,
			"observers": ids,
		},
	)

	return nil
}

func (s *PacketObservations) Evict(hwm time.Time) {
	s.buf = s.buf[:0]
	s.touched = map[string]struct{}{}
	s.count = 0
}

func (s *PacketObservations) Clear() {
	s.buf = nil
	s.touched = map[string]struct{}{}
	s.count = 0
}
