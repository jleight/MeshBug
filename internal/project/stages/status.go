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

// ObserverStatus owns project.observer_status. Append-only: each status
// event adds one row keyed by (observer_id, ts). The batch buffer is
// drained on every Flush. Touched observers are reported via pg_notify
// on the meshbug_status channel after commit, so the SSE bridge can
// refresh.
type ObserverStatus struct {
	buf      []statusRow
	touched  map[string]struct{}
}

type statusRow struct {
	observerID []byte
	ts         time.Time
	status     string
	uptimeSecs *int64
	batteryMV  *int
	queueLen   *int
	noiseFloor *int
	txAirSecs  *int64
	rxAirSecs  *int64
	recvErrors *int64
	lastRSSI   *int
	lastSNR    *float64
	debugFlags *int64
}

func NewObserverStatus() *ObserverStatus {
	return &ObserverStatus{
		touched: map[string]struct{}{},
	}
}

func (s *ObserverStatus) Name() string {
	return "observer_status"
}

func (s *ObserverStatus) Apply(ctx context.Context, e pipeline.Event) error {
	if e.Kind != pipeline.KindStatus {
		return nil
	}

	p := e.Status

	s.buf = append(s.buf, statusRow{
		observerID: append([]byte(nil), e.ObserverID...),
		ts:         e.TS,
		status:     p.Status,
		uptimeSecs: p.UptimeSecs,
		batteryMV:  p.BatteryMV,
		queueLen:   p.QueueLen,
		noiseFloor: p.NoiseFloor,
		txAirSecs:  p.TxAirSecs,
		rxAirSecs:  p.RxAirSecs,
		recvErrors: p.RecvErrors,
		lastRSSI:   p.LastRSSI,
		lastSNR:    p.LastSNR,
		debugFlags: p.DebugFlags,
	})

	s.touched[string(e.ObserverID)] = struct{}{}
	return nil
}

func (s *ObserverStatus) Flush(ctx context.Context, tx pgx.Tx) error {
	if len(s.buf) == 0 {
		return nil
	}

	for _, r := range s.buf {
		_, err := tx.Exec(
			ctx,
			`
			INSERT INTO observer_status
			    (observer_id
			    ,ts
			    ,status
			    ,uptime_secs
			    ,battery_mv
			    ,queue_len
			    ,noise_floor
			    ,tx_air_secs
			    ,rx_air_secs
			    ,recv_errors
			    ,last_rssi
			    ,last_snr
			    ,debug_flags)
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
			ON CONFLICT (observer_id, ts) DO NOTHING
			`,
			r.observerID,
			r.ts,
			r.status,
			r.uptimeSecs,
			r.batteryMV,
			r.queueLen,
			r.noiseFloor,
			r.txAirSecs,
			r.rxAirSecs,
			r.recvErrors,
			r.lastRSSI,
			r.lastSNR,
			r.debugFlags,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *ObserverStatus) Notify(ctx context.Context, pool *pgxpool.Pool) error {
	if len(s.touched) == 0 {
		return nil
	}

	for k := range s.touched {
		_ = notify.Publish(
			ctx,
			pool,
			notify.ChannelStatus,
			map[string]any{
				"observer_id": hex.EncodeToString([]byte(k)),
			},
		)
	}

	return nil
}

func (s *ObserverStatus) Evict(hwm time.Time) {
	s.buf = s.buf[:0]
	s.touched = map[string]struct{}{}
}

func (s *ObserverStatus) Clear() {
	s.buf = nil
	s.touched = map[string]struct{}{}
}
