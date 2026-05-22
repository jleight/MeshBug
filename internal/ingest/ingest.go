package ingest

import (
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/jleight/meshbug/internal/config"
	"github.com/jleight/meshbug/internal/meshcore"
	"github.com/jleight/meshbug/internal/mqtt"
	"github.com/jleight/meshbug/internal/sse"
	"github.com/jleight/meshbug/internal/store"
)

// Ingester consumes MQTT messages, parses them, decodes packet headers, and
// writes them to the store in batches. After writes, it publishes lightweight
// summaries to the SSE hub for live UI updates.
type Ingester struct {
	cfg        []config.Broker
	store      *store.Store
	hub        *sse.Hub
	in         <-chan mqtt.Message
	log        *slog.Logger
	batchSize  int
	flushEvery time.Duration
}

func New(brokers []config.Broker, s *store.Store, hub *sse.Hub, in <-chan mqtt.Message, log *slog.Logger) *Ingester {
	return &Ingester{
		cfg:        brokers,
		store:      s,
		hub:        hub,
		in:         in,
		log:        log,
		batchSize:  500,
		flushEvery: 250 * time.Millisecond,
	}
}

func (i *Ingester) prefixFor(broker string) string {
	for _, b := range i.cfg {
		if b.Name == broker {
			return b.TopicPrefix
		}
	}
	return "meshcore/"
}

func (i *Ingester) Run(ctx context.Context) error {
	batch := make([]store.PacketRow, 0, i.batchSize)
	t := time.NewTimer(i.flushEvery)
	defer t.Stop()
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if _, err := i.store.InsertPacketBatch(ctx, batch); err != nil {
			i.log.Error("packet batch write failed", "err", err, "n", len(batch))
		} else {
			for _, r := range batch {
				i.hub.Publish(sse.Event{Topic: "live-feed", Payload: r})
				i.hub.Publish(sse.Event{Topic: "observer:" + hex.EncodeToString(r.ObserverID), Payload: r})
			}
			i.hub.Publish(sse.Event{Topic: "overview", Payload: len(batch)})
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return ctx.Err()
		case <-t.C:
			flush()
			t.Reset(i.flushEvery)
		case msg, ok := <-i.in:
			if !ok {
				flush()
				return nil
			}
			i.handle(ctx, msg, &batch)
			if len(batch) >= i.batchSize {
				flush()
				if !t.Stop() {
					select {
					case <-t.C:
					default:
					}
				}
				t.Reset(i.flushEvery)
			}
		}
	}
}

func (i *Ingester) handle(ctx context.Context, msg mqtt.Message, batch *[]store.PacketRow) {
	prefix := i.prefixFor(msg.Broker)
	obsID, region, kind, err := parseTopicHash(prefix, msg.Topic)
	if err != nil {
		i.log.Debug("skip message", "topic", msg.Topic, "err", err)
		return
	}

	switch kind {
	case "status":
		i.handleStatus(ctx, msg, obsID, region)
	case "packets":
		i.handlePacket(msg, obsID, region, batch)
	default:
		// ignore unknown subtopic
	}
}

func (i *Ingester) handleStatus(ctx context.Context, msg mqtt.Message, obsID []byte, region string) {
	m, err := unmarshalStatus(msg.Payload)
	if err != nil {
		i.log.Warn("bad status json", "topic", msg.Topic, "err", err)
		return
	}
	// Observers publish naive timestamps in their local TZ (no offset). For
	// the dashboard we want a single consistent clock, so we use server time
	// as the canonical `ts` everywhere. The reported timestamp is discarded.
	ts := time.Now().UTC()
	_, _ = parseTimestamp(m.Timestamp) // keep tested code reachable; result unused

	freq, bw, sf, cr := parseRadio(m.Radio)
	up := store.ObserverUpsert{
		ID:              obsID,
		OriginName:      m.Origin,
		Region:          region,
		Model:           m.Model,
		FirmwareVersion: m.FirmwareVersion,
		ClientVersion:   m.ClientVersion,
		Source:          m.Source,
		RadioFreqKHz:    freq, RadioBWKHz: bw, RadioSF: sf, RadioCR: cr,
		Seen: ts,
	}
	if err := i.store.UpsertObserver(ctx, up); err != nil {
		i.log.Error("upsert observer failed", "err", err)
		return
	}
	row := store.StatusRow{
		ObserverID: obsID, TS: ts, Status: m.Status,
		UptimeSecs: m.Stats.UptimeSecs, BatteryMV: m.Stats.BatteryMV,
		QueueLen: m.Stats.QueueLen, NoiseFloor: m.Stats.NoiseFloor,
		TxAirSecs: m.Stats.TxAirSecs, RxAirSecs: m.Stats.RxAirSecs,
		RecvErrors: pickErrors(m.Stats.RecvErrors, m.Stats.Errors),
		LastRSSI:   m.Stats.LastRSSI, LastSNR: m.Stats.LastSNR,
		DebugFlags: m.Stats.DebugFlags,
	}
	if err := i.store.InsertStatus(ctx, row); err != nil {
		i.log.Error("insert status failed", "err", err)
		return
	}
	i.hub.Publish(sse.Event{Topic: "observer:" + hex.EncodeToString(obsID), Payload: row})
	i.hub.Publish(sse.Event{Topic: "overview", Payload: "status"})
}

func pickErrors(a, b *int64) *int64 {
	if a != nil {
		return a
	}
	return b
}

func (i *Ingester) handlePacket(msg mqtt.Message, obsID []byte, _ string, batch *[]store.PacketRow) {
	m, err := unmarshalPacket(msg.Payload)
	if err != nil {
		i.log.Warn("bad packet json", "topic", msg.Topic, "err", err)
		return
	}
	ts := time.Now().UTC()
	_, _ = parseTimestamp(m.Timestamp)

	var rawBytes []byte
	if m.Raw != "" {
		rawBytes, err = hex.DecodeString(strings.TrimSpace(m.Raw))
		if err != nil {
			i.log.Debug("bad raw hex", "topic", msg.Topic, "err", err)
			rawBytes = nil
		}
	}

	var packetHash []byte
	if m.Hash != "" {
		packetHash, _ = hex.DecodeString(strings.TrimSpace(m.Hash))
	}

	row := store.PacketRow{
		TS:         ts,
		ObserverID: obsID,
		PacketHash: packetHash,
		Direction:  m.Direction,
		PacketType: m.PacketType,
		Route:      m.Route,
		Len:        atoiFlex(m.Len),
		PayloadLen: atoiFlex(m.PayloadLen),
		RSSI:       atoiFlex(m.RSSI),
		SNR:        atofFlex(m.SNR),
		Score:      atoiFlex(m.Score),
		DurationMS: atoiFlex(m.Duration),
		Raw:        rawBytes,
	}

	if len(rawBytes) > 0 {
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
			// fine — keep the observation, no decoded fields.
		default:
			i.log.Debug("decode failed", "err", err, "raw_len", len(rawBytes))
		}
	}
	*batch = append(*batch, row)
}
