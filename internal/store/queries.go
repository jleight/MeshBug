package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// UpsertObserver inserts or updates an observer row, refreshing last_seen.
func (s *Store) UpsertObserver(ctx context.Context, o ObserverUpsert) error {
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO observers (id, origin_name, region, model, firmware_version, client_version, source,
		                       radio_freq_khz, radio_bw_khz, radio_sf, radio_cr, first_seen, last_seen)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$12)
		ON CONFLICT (id) DO UPDATE SET
		  origin_name      = COALESCE(NULLIF(EXCLUDED.origin_name,''), observers.origin_name),
		  region           = COALESCE(NULLIF(EXCLUDED.region,''), observers.region),
		  model            = COALESCE(NULLIF(EXCLUDED.model,''), observers.model),
		  firmware_version = COALESCE(NULLIF(EXCLUDED.firmware_version,''), observers.firmware_version),
		  client_version   = COALESCE(NULLIF(EXCLUDED.client_version,''), observers.client_version),
		  source           = COALESCE(NULLIF(EXCLUDED.source,''), observers.source),
		  radio_freq_khz   = COALESCE(EXCLUDED.radio_freq_khz, observers.radio_freq_khz),
		  radio_bw_khz     = COALESCE(EXCLUDED.radio_bw_khz, observers.radio_bw_khz),
		  radio_sf         = COALESCE(EXCLUDED.radio_sf, observers.radio_sf),
		  radio_cr         = COALESCE(EXCLUDED.radio_cr, observers.radio_cr),
		  last_seen        = GREATEST(observers.last_seen, EXCLUDED.last_seen)
	`, o.ID, o.OriginName, o.Region, o.Model, o.FirmwareVersion, o.ClientVersion, o.Source,
		o.RadioFreqKHz, o.RadioBWKHz, o.RadioSF, o.RadioCR, o.Seen)
	return err
}

func (s *Store) InsertStatus(ctx context.Context, r StatusRow) error {
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO observer_status (observer_id, ts, status, uptime_secs, battery_mv, queue_len,
		                             noise_floor, tx_air_secs, rx_air_secs, recv_errors,
		                             last_rssi, last_snr, debug_flags)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (observer_id, ts) DO NOTHING
	`, r.ObserverID, r.TS, r.Status, r.UptimeSecs, r.BatteryMV, r.QueueLen,
		r.NoiseFloor, r.TxAirSecs, r.RxAirSecs, r.RecvErrors,
		r.LastRSSI, r.LastSNR, r.DebugFlags)
	return err
}

// InsertPacketBatch writes packet observations using CopyFrom (server-assigned
// observation_id via sequence default). Returns the number of rows written.
func (s *Store) InsertPacketBatch(ctx context.Context, rows []PacketRow) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	// CopyFrom can't use column DEFAULTs, so we pre-assign observation_id from the sequence.
	ids := make([]int64, len(rows))
	if err := s.Pool.QueryRow(ctx,
		`SELECT array_agg(nextval('packet_observations_id_seq')) FROM generate_series(1,$1)`, len(rows),
	).Scan(&ids); err != nil {
		return 0, err
	}
	src := pgx.CopyFromSlice(len(rows), func(i int) ([]any, error) {
		r := rows[i]
		return []any{
			r.TS, ids[i], r.ObserverID, r.PacketHash, r.Direction, r.PacketType, r.Route,
			r.Len, r.PayloadLen, r.RSSI, r.SNR, r.Score, r.DurationMS, r.Raw,
			r.DecodedType, r.SourcePrefix, r.DestPrefix, r.TransportCodes,
		}, nil
	})
	n, err := s.Pool.CopyFrom(ctx,
		pgx.Identifier{"packet_observations"},
		[]string{
			"ts", "observation_id", "observer_id", "packet_hash", "direction", "packet_type", "route",
			"len", "payload_len", "rssi", "snr", "score", "duration_ms", "raw",
			"decoded_type", "source_prefix", "dest_prefix", "transport_codes",
		}, src)
	if err != nil {
		return 0, err
	}

	// Maintain packets_unique. Dedupe by hash *within* this batch first — Postgres
	// can't update the same conflict target twice in one statement, and we want
	// observer_count to bump by exactly one per upsert call no matter how many
	// observers in this batch happened to hear the same packet (the rollup +
	// per-observer table give the per-observer counts anyway).
	type uagg struct {
		first time.Time
		ptype string
		route string
		dtype *int16
		src   []byte
	}
	uniq := map[string]*uagg{}
	for _, r := range rows {
		if len(r.PacketHash) == 0 {
			continue
		}
		key := string(r.PacketHash)
		if e, ok := uniq[key]; ok {
			if r.TS.Before(e.first) {
				e.first = r.TS
			}
			if e.src == nil {
				e.src = r.SourcePrefix
			}
			if e.dtype == nil {
				e.dtype = r.DecodedType
			}
			continue
		}
		uniq[key] = &uagg{first: r.TS, ptype: r.PacketType, route: r.Route, dtype: r.DecodedType, src: r.SourcePrefix}
	}
	hashes := make([][]byte, 0, len(uniq))
	firstSeen := make([]time.Time, 0, len(uniq))
	ptype := make([]string, 0, len(uniq))
	route := make([]string, 0, len(uniq))
	dtype := make([]*int16, 0, len(uniq))
	src2 := make([][]byte, 0, len(uniq))
	for k, v := range uniq {
		hashes = append(hashes, []byte(k))
		firstSeen = append(firstSeen, v.first)
		ptype = append(ptype, v.ptype)
		route = append(route, v.route)
		dtype = append(dtype, v.dtype)
		src2 = append(src2, v.src)
	}
	if len(hashes) > 0 {
		_, err := s.Pool.Exec(ctx, `
			INSERT INTO packets_unique (packet_hash, first_seen, last_seen, packet_type, route, decoded_type, source_prefix, observer_count)
			SELECT h, t, t, pt, rt, dt, sp, 1
			FROM unnest($1::bytea[], $2::timestamptz[], $3::text[], $4::text[], $5::smallint[], $6::bytea[])
			   AS u(h, t, pt, rt, dt, sp)
			ON CONFLICT (packet_hash) DO UPDATE SET
			  last_seen      = GREATEST(packets_unique.last_seen, EXCLUDED.last_seen),
			  observer_count = packets_unique.observer_count + 1,
			  source_prefix  = COALESCE(packets_unique.source_prefix, EXCLUDED.source_prefix),
			  decoded_type   = COALESCE(packets_unique.decoded_type, EXCLUDED.decoded_type)
		`, hashes, firstSeen, ptype, route, dtype, src2)
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
