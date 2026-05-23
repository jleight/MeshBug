// Package project drives the read-model. It reads `raw_events`, decodes each
// one (MQTT topic + JSON payload + MeshCore wire format), and writes the
// derived rows that the UI queries: observers, observer_status,
// packet_observations, packets_unique. It also publishes pg_notify on the
// channels web subscribes to (meshbug_packets, meshbug_status), so SSE
// streams update in near-real time.
//
// Cursor: a row in projector_state tracks the highest raw_events.id we've
// processed. On startup we resume from there; on each batch we advance the
// cursor only after the derived inserts succeed.
package project

import (
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"

	"github.com/jleight/meshbug/internal/ingest"
	"github.com/jleight/meshbug/internal/meshcore"
	"github.com/jleight/meshbug/internal/notify"
	"github.com/jleight/meshbug/internal/store"
)

const (
	projectorName = "default"
	batchSize     = 500
)

type Projector struct {
	store *store.Store
	log   *slog.Logger
}

func New(s *store.Store, log *slog.Logger) *Projector {
	return &Projector{
		store: s,
		log:   log,
	}
}

// Reset truncates every derived table and rewinds the cursor to 0. Next call
// to Run will rebuild derived state from raw_events.
func (p *Projector) Reset(ctx context.Context) error {
	p.log.Warn("resetting derived state — will rebuild from raw_events on next run")
	return p.store.ResetDerivedState(ctx, projectorName)
}

// Run processes events until ctx is canceled. Catches up from the last
// committed cursor first, then listens for pg_notify('meshbug_raw') and
// processes new batches as they arrive.
func (p *Projector) Run(ctx context.Context, databaseURL string) error {
	cursor, err := p.store.LoadProjectorCursor(ctx, projectorName)
	if err != nil {
		return err
	}

	p.log.Info("project starting", "cursor", cursor)

	// Drain anything queued before we start listening.
	if err := p.catchUp(ctx, &cursor); err != nil && !errors.Is(err, context.Canceled) {
		p.log.Error("initial catch-up failed", "err", err)
	}

	// Stream new events as ingest writes them. The listener is best-effort —
	// if it falls behind or disconnects, the catch-up loop on next wake
	// closes the gap.
	errs := make(chan error, 1)
	go func() {
		channels := []string{ingest.NotifyChannel}

		handle := func(_ notify.Notification) {
			err := p.catchUp(ctx, &cursor)
			if err != nil && !errors.Is(err, context.Canceled) {
				p.log.Error("catch-up failed", "err", err)
			}
		}

		errs <- notify.Listen(ctx, databaseURL, channels, p.log, handle)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errs:
		return err
	}
}

func (p *Projector) catchUp(ctx context.Context, cursor *int64) error {
	for {
		events, err := p.store.FetchRawEventsAfter(ctx, *cursor, batchSize)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}

		highest := *cursor
		obsAffected := map[string]struct{}{}
		statusAffected := map[string]struct{}{}

		var packets []store.PacketRow

		for _, e := range events {
			obs, kind, err := decodeTopic(e.Topic)
			if err != nil {
				highest = e.ID
				continue
			}

			switch kind {
			case "status":
				if err := p.applyStatus(ctx, e, obs); err == nil {
					statusAffected[string(obs)] = struct{}{}
				}

			case "packets":
				row, ok := p.decodePacket(e, obs)
				if ok {
					packets = append(packets, row)
					obsAffected[string(obs)] = struct{}{}
				}
			}

			highest = e.ID
		}

		if len(packets) > 0 {
			if _, err := p.store.InsertPacketBatch(ctx, packets); err != nil {
				p.log.Error(
					"packet batch write failed",
					"n",
					len(packets),
					"err",
					err,
				)
				// Don't advance the cursor on a derived-write failure — we'll retry.
				return err
			}

			ids := make([]string, 0, len(obsAffected))
			for k := range obsAffected {
				ids = append(ids, hex.EncodeToString([]byte(k)))
			}

			_ = notify.Publish(
				ctx,
				p.store.Pool,
				notify.ChannelPackets,
				map[string]any{
					"count":     len(packets),
					"observers": ids,
				},
			)
		}

		for k := range statusAffected {
			_ = notify.Publish(
				ctx,
				p.store.Pool,
				notify.ChannelStatus,
				map[string]any{
					"observer_id": hex.EncodeToString([]byte(k)),
				},
			)
		}

		if err := p.store.SaveProjectorCursor(ctx, projectorName, highest); err != nil {
			return err
		}

		*cursor = highest

		if len(events) < batchSize {
			return nil
		}
	}
}

func decodeTopic(topic string) (obsID []byte, kind string, err error) {
	// The wire prefix is always "meshcore/" — observers we ingest are matched
	// against the broker's TopicPrefix at subscribe time, so by the time we
	// see the topic here it's guaranteed to start with "meshcore/".
	id, _, k, err := parseTopicHash("meshcore/", topic)
	return id, k, err
}

func (p *Projector) applyStatus(
	ctx context.Context,
	e store.RawEvent,
	obsID []byte,
) error {
	m, err := unmarshalStatus(e.Payload)
	if err != nil {
		p.log.Warn(
			"bad status json",
			"id",
			e.ID,
			"err",
			err,
		)
		return err
	}

	region := topicRegion(e.Topic)
	freq, bw, sf, cr := parseRadio(m.Radio)

	up := store.ObserverUpsert{
		ID:              obsID,
		OriginName:      m.Origin,
		Region:          region,
		Model:           m.Model,
		FirmwareVersion: m.FirmwareVersion,
		ClientVersion:   m.ClientVersion,
		Source:          m.Source,
		RadioFreqKHz:    freq,
		RadioBWKHz:      bw,
		RadioSF:         sf,
		RadioCR:         cr,
		Seen:            e.ReceivedAt,
	}
	if err := p.store.UpsertObserver(ctx, up); err != nil {
		p.log.Error("upsert observer failed", "err", err)
		return err
	}

	row := store.StatusRow{
		ObserverID: obsID,
		TS:         e.ReceivedAt,
		Status:     m.Status,
		UptimeSecs: m.Stats.UptimeSecs,
		BatteryMV:  m.Stats.BatteryMV,
		QueueLen:   m.Stats.QueueLen,
		NoiseFloor: m.Stats.NoiseFloor,
		TxAirSecs:  m.Stats.TxAirSecs,
		RxAirSecs:  m.Stats.RxAirSecs,
		RecvErrors: pickErrors(m.Stats.RecvErrors, m.Stats.Errors),
		LastRSSI:   m.Stats.LastRSSI,
		LastSNR:    m.Stats.LastSNR,
		DebugFlags: m.Stats.DebugFlags,
	}
	return p.store.InsertStatus(ctx, row)
}

func pickErrors(a, b *int64) *int64 {
	if a != nil {
		return a
	}
	return b
}

func topicRegion(topic string) string {
	parts := strings.Split(strings.TrimPrefix(topic, "meshcore/"), "/")
	if len(parts) >= 1 {
		return parts[0]
	}
	return ""
}

func (p *Projector) decodePacket(
	e store.RawEvent,
	obsID []byte,
) (store.PacketRow, bool) {
	m, err := unmarshalPacket(e.Payload)
	if err != nil {
		p.log.Warn(
			"bad packet json",
			"id",
			e.ID,
			"err",
			err,
		)
		return store.PacketRow{}, false
	}

	var rawBytes []byte
	if m.Raw != "" {
		rawBytes, _ = hex.DecodeString(strings.TrimSpace(m.Raw))
	}

	var packetHash []byte
	if m.Hash != "" {
		packetHash, _ = hex.DecodeString(strings.TrimSpace(m.Hash))
	}

	row := store.PacketRow{
		TS:         e.ReceivedAt,
		ObserverID: obsID,
		PacketHash: packetHash,
		Direction:  m.Direction,
		PacketType: string(m.PacketType),
		Route:      m.Route,
		Len:        atoiFlex(m.Len),
		PayloadLen: atoiFlex(m.PayloadLen),
		RSSI:       atoiFlex(m.RSSI),
		SNR:        atofFlex(m.SNR),
		Score:      atoiFlex(m.Score),
		DurationMS: atoiFlex(m.Duration),
		Raw:        rawBytes,
	}

	if len(rawBytes) == 0 {
		return row, true
	}

	d, err := meshcore.Decode(rawBytes)
	switch {
	case err == nil:
		t := d.PayloadType
		row.DecodedType = &t
		row.SourcePrefix = d.NeighborSource()
		row.DestPrefix = d.DstHash
		row.TransportCodes = d.TransportCodes
		if row.Route == "" {
			row.Route = meshcore.RouteName(d.Route)
		}
		if row.PacketType == "" {
			row.PacketType = meshcore.PayloadTypeName(t)
		}

	case errors.Is(err, meshcore.ErrDoNotRetransmit):
		// keep the observation, no decoded fields

	default:
		p.log.Debug(
			"decode failed",
			"id",
			e.ID,
			"err",
			err,
		)
	}

	return row, true
}
