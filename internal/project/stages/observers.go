// Package stages contains the concrete Stage implementations that own the
// derived tables in the `project` schema. Each file in this package owns
// exactly one table.
package stages

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jleight/meshbug/internal/project/pipeline"
)

// Observers owns project.observers. Only status events create or update
// rows here — packets don't carry the metadata we need (origin name,
// radio config) and observers.origin_name is NOT NULL, so synthesising a
// row from a packet alone would fail the constraint. packet_observations
// doesn't FK to observers, so packets for never-seen observers are fine.
type Observers struct {
	touched map[string]*observerRow
}

type observerRow struct {
	id              []byte
	originName      string
	region          string
	model           string
	firmwareVersion string
	clientVersion   string
	source          string

	radioFreqKHz *int
	radioBWKHz   *float64
	radioSF      *int
	radioCR      *int

	firstSeen time.Time
	lastSeen  time.Time
}

func NewObservers() *Observers {
	return &Observers{touched: map[string]*observerRow{}}
}

func (s *Observers) Name() string {
	return "observers"
}

func (s *Observers) Apply(ctx context.Context, e pipeline.Event) error {
	if e.Kind != pipeline.KindStatus {
		return nil
	}

	key := string(e.ObserverID)

	row, ok := s.touched[key]
	if !ok {
		row = &observerRow{
			id:        append([]byte(nil), e.ObserverID...),
			region:    e.Region,
			firstSeen: e.TS,
			lastSeen:  e.TS,
		}
		s.touched[key] = row
	}

	if e.TS.Before(row.firstSeen) {
		row.firstSeen = e.TS
	}

	if e.TS.After(row.lastSeen) {
		row.lastSeen = e.TS
	}

	if e.Region != "" {
		row.region = e.Region
	}

	p := e.Status

	if p.Origin != "" {
		row.originName = p.Origin
	}

	if p.Model != "" {
		row.model = p.Model
	}

	if p.FirmwareVersion != "" {
		row.firmwareVersion = p.FirmwareVersion
	}

	if p.ClientVersion != "" {
		row.clientVersion = p.ClientVersion
	}

	if p.Source != "" {
		row.source = p.Source
	}

	if p.RadioFreqKHz != nil {
		row.radioFreqKHz = p.RadioFreqKHz
	}

	if p.RadioBWKHz != nil {
		row.radioBWKHz = p.RadioBWKHz
	}

	if p.RadioSF != nil {
		row.radioSF = p.RadioSF
	}

	if p.RadioCR != nil {
		row.radioCR = p.RadioCR
	}

	return nil
}

func (s *Observers) Flush(ctx context.Context, tx pgx.Tx) error {
	if len(s.touched) == 0 {
		return nil
	}

	for _, r := range s.touched {
		_, err := tx.Exec(
			ctx,
			`
			INSERT INTO observers
			    (id
			    ,origin_name
			    ,region
			    ,model
			    ,firmware_version
			    ,client_version
			    ,source
			    ,radio_freq_khz
			    ,radio_bw_khz
			    ,radio_sf
			    ,radio_cr
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
			    ,$13)
			ON CONFLICT (id) DO UPDATE SET
			     origin_name      = COALESCE(NULLIF(EXCLUDED.origin_name,''), observers.origin_name)
			    ,region           = COALESCE(NULLIF(EXCLUDED.region,''), observers.region)
			    ,model            = COALESCE(NULLIF(EXCLUDED.model,''), observers.model)
			    ,firmware_version = COALESCE(NULLIF(EXCLUDED.firmware_version,''), observers.firmware_version)
			    ,client_version   = COALESCE(NULLIF(EXCLUDED.client_version,''), observers.client_version)
			    ,source           = COALESCE(NULLIF(EXCLUDED.source,''), observers.source)
			    ,radio_freq_khz   = COALESCE(EXCLUDED.radio_freq_khz, observers.radio_freq_khz)
			    ,radio_bw_khz     = COALESCE(EXCLUDED.radio_bw_khz, observers.radio_bw_khz)
			    ,radio_sf         = COALESCE(EXCLUDED.radio_sf, observers.radio_sf)
			    ,radio_cr         = COALESCE(EXCLUDED.radio_cr, observers.radio_cr)
			    ,first_seen       = LEAST(observers.first_seen, EXCLUDED.first_seen)
			    ,last_seen        = GREATEST(observers.last_seen, EXCLUDED.last_seen)
			`,
			r.id,
			r.originName,
			r.region,
			r.model,
			r.firmwareVersion,
			r.clientVersion,
			r.source,
			r.radioFreqKHz,
			r.radioBWKHz,
			r.radioSF,
			r.radioCR,
			r.firstSeen,
			r.lastSeen,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Observers) Notify(ctx context.Context, pool *pgxpool.Pool) error {
	return nil
}

// Evict drops the per-batch working set. Observers don't need to live in
// memory across batches — every event for an observer re-touches its row,
// and Flush is idempotent under ON CONFLICT DO UPDATE.
func (s *Observers) Evict(hwm time.Time) {
	s.touched = map[string]*observerRow{}
}

func (s *Observers) Clear() {
	s.touched = map[string]*observerRow{}
}
