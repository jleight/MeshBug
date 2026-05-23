package project

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"

	"github.com/jleight/meshbug/internal/meshcore"
	"github.com/jleight/meshbug/internal/project/pipeline"
	"github.com/jleight/meshbug/internal/store"
)

// decoder turns store.RawEvent rows into pipeline.Event values. It is the
// only place that knows about MQTT topic shapes, JSON payload shapes, or
// the on-wire meshcore packet format.
type decoder struct {
	log *slog.Logger
}

func newDecoder(log *slog.Logger) *decoder {
	return &decoder{log: log}
}

// Decode parses one raw event. Returns (event, true) when the event is
// usable. Returns (_, false) for events the projector intentionally drops
// (bad topic, bad payload, unknown kind) — the cursor still advances past
// them, they just don't reach any stage.
func (d *decoder) Decode(raw store.RawEvent) (pipeline.Event, bool) {
	obsID, region, kind, err := parseTopicHash("meshcore/", raw.Topic)
	if err != nil {
		return pipeline.Event{}, false
	}

	base := pipeline.Event{
		RawID:      raw.ID,
		TS:         raw.ReceivedAt,
		ObserverID: obsID,
		Topic:      raw.Topic,
		Region:     region,
	}

	switch kind {
	case "status":
		payload, ok := d.decodeStatus(raw)
		if !ok {
			return pipeline.Event{}, false
		}

		base.Kind = pipeline.KindStatus
		base.Status = payload
		return base, true

	case "packets":
		payload, ok := d.decodePacket(raw)
		if !ok {
			return pipeline.Event{}, false
		}

		base.Kind = pipeline.KindPacket
		base.Packet = payload
		return base, true

	default:
		return pipeline.Event{}, false
	}
}

func (d *decoder) decodeStatus(raw store.RawEvent) (*pipeline.StatusPayload, bool) {
	m, err := unmarshalStatus(raw.Payload)
	if err != nil {
		d.log.Warn(
			"bad status json",
			"id",
			raw.ID,
			"err",
			err,
		)
		return nil, false
	}

	freq, bw, sf, cr := parseRadio(m.Radio)

	return &pipeline.StatusPayload{
		Status:          m.Status,
		Origin:          m.Origin,
		Model:           m.Model,
		FirmwareVersion: m.FirmwareVersion,
		ClientVersion:   m.ClientVersion,
		Source:          m.Source,
		RadioFreqKHz:    freq,
		RadioBWKHz:      bw,
		RadioSF:         sf,
		RadioCR:         cr,
		UptimeSecs:      m.Stats.UptimeSecs,
		BatteryMV:       m.Stats.BatteryMV,
		QueueLen:        m.Stats.QueueLen,
		NoiseFloor:      m.Stats.NoiseFloor,
		TxAirSecs:       m.Stats.TxAirSecs,
		RxAirSecs:       m.Stats.RxAirSecs,
		RecvErrors:      pickErrors(m.Stats.RecvErrors, m.Stats.Errors),
		LastRSSI:        m.Stats.LastRSSI,
		LastSNR:         m.Stats.LastSNR,
		DebugFlags:      m.Stats.DebugFlags,
	}, true
}

func (d *decoder) decodePacket(raw store.RawEvent) (*pipeline.PacketPayload, bool) {
	m, err := unmarshalPacket(raw.Payload)
	if err != nil {
		d.log.Warn(
			"bad packet json",
			"id",
			raw.ID,
			"err",
			err,
		)
		return nil, false
	}

	var rawBytes []byte
	if m.Raw != "" {
		rawBytes, _ = hex.DecodeString(strings.TrimSpace(m.Raw))
	}

	var packetHash []byte
	if m.Hash != "" {
		packetHash, _ = hex.DecodeString(strings.TrimSpace(m.Hash))
	}

	p := &pipeline.PacketPayload{
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
		return p, true
	}

	pkt, err := meshcore.Parse(rawBytes)
	switch {
	case err == nil:
		t := int16(pkt.PayloadType())
		p.DecodedType = &t
		p.SourcePrefix = meshcore.NeighborSource(pkt)
		p.DestPrefix = meshcore.PayloadDstHash(pkt)

		if pkt.IsTransport() {
			p.TransportCodes = make([]byte, 4)
			binary.LittleEndian.PutUint16(p.TransportCodes[0:2], pkt.TransportCode1)
			binary.LittleEndian.PutUint16(p.TransportCodes[2:4], pkt.TransportCode2)
		}

		if p.Route == "" {
			p.Route = pkt.RouteTypeString()
		}

		if p.PacketType == "" {
			p.PacketType = pkt.PayloadTypeString()
		}

	case errors.Is(err, meshcore.ErrDoNotRetransmit):
		// keep the observation, no decoded fields

	default:
		d.log.Debug(
			"decode failed",
			"id",
			raw.ID,
			"err",
			err,
		)
	}

	return p, true
}

func pickErrors(a, b *int64) *int64 {
	if a != nil {
		return a
	}
	return b
}
