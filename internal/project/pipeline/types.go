// Package pipeline defines the projection pipeline that turns raw MQTT
// events into every derived table in the project schema. The pipeline is
// extensible: each derived table is owned by a Stage, and adding a new
// derivation means writing a Stage and appending it to the pipeline's list.
//
// Each batch of decoded events flows through every stage in order. Stages
// keep an in-memory working set keyed by whatever they aggregate over
// (observer, bucket, packet hash, ...). On Flush, each stage upserts the
// rows it touched into the DB in the same transaction that advances the
// projector cursor. On Evict, stages drop in-memory state for keys whose
// retention window has passed; a later straggler event triggers a per-key
// rehydrate from the DB row.
package pipeline

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EventKind discriminates which payload a decoded event carries.
type EventKind int

const (
	KindStatus EventKind = iota + 1
	KindPacket
)

// Event is one decoded raw_events row, ready for stage consumption. The
// payload fields are populated according to Kind; the unused one is nil.
type Event struct {
	RawID      int64
	TS         time.Time
	ObserverID []byte
	Topic      string
	Region     string
	Kind       EventKind

	Status *StatusPayload
	Packet *PacketPayload
}

// StatusPayload holds the fields lifted out of a /status JSON message.
type StatusPayload struct {
	Status          string
	Origin          string
	Model           string
	FirmwareVersion string
	ClientVersion   string
	Source          string

	RadioFreqKHz *int
	RadioBWKHz   *float64
	RadioSF      *int
	RadioCR      *int

	UptimeSecs *int64
	BatteryMV  *int
	QueueLen   *int
	NoiseFloor *int
	TxAirSecs  *int64
	RxAirSecs  *int64
	RecvErrors *int64
	LastRSSI   *int
	LastSNR    *float64
	DebugFlags *int64
}

// PacketPayload holds the fields lifted out of a /packets JSON message
// plus anything we could decode from the on-wire bytes.
type PacketPayload struct {
	PacketHash []byte
	Direction  string
	PacketType string
	Route      string

	Len        *int
	PayloadLen *int
	RSSI       *int
	SNR        *float64
	Score      *int
	DurationMS *int

	Raw            []byte
	DecodedType    *int16
	SourcePrefix   []byte
	DestPrefix     []byte
	TransportCodes []byte
}

// Stage owns one derived table (or one group of them). The pipeline calls
// Apply for every event in the batch, then Flush once inside the tx that
// advances the cursor, then Notify (post-commit, for pg_notify side
// effects), then Evict to release in-memory state that's now outside the
// retention window. Clear is called on Reset to wipe all state.
//
// Apply takes ctx because a stage may rehydrate state from the DB when it
// sees a key it doesn't have in memory yet (cold-start, or a straggler
// arriving in a previously-evicted bucket).
type Stage interface {
	Name() string

	Apply(ctx context.Context, e Event) error
	Flush(ctx context.Context, tx pgx.Tx) error
	Notify(ctx context.Context, pool *pgxpool.Pool) error
	Evict(hwm time.Time)
	Clear()
}
