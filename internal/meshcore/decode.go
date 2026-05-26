// Package meshcore adds MeshBug-specific helpers on top of
// github.com/meshcore-go/meshcore-go.
//
// The upstream library handles wire-format parsing. This package adds:
//
//   - Parse: a wrapper around mc.PacketFromBytes that surfaces the 0xFF
//     "do-not-retransmit" marker as ErrDoNotRetransmit (upstream parses
//     it as a normal header).
//   - VER_1 payload-prefix extraction for the dashboard's "who sent
//     this / who is it for" columns.
package meshcore

import (
	"errors"
	"strings"
	"time"

	mc "github.com/meshcore-go/meshcore-go"
)

var (
	ErrShort           = errors.New("packet truncated")
	ErrDoNotRetransmit = errors.New("do-not-retransmit marker")
)

// Parse decodes one raw MeshCore frame. The special header value 0xFF is
// reported as ErrDoNotRetransmit; callers may choose to ignore that.
func Parse(raw []byte) (*mc.Packet, error) {
	if len(raw) < 1 {
		return nil, ErrShort
	}

	if raw[0] == 0xFF {
		return nil, ErrDoNotRetransmit
	}

	return mc.PacketFromBytes(raw)
}

// PayloadDstHash returns the destination hash carried in the payload
// prefix for VER_1 payload types that have one (or the channel hash for
// group payloads). Returns nil for other payload types or versions.
func PayloadDstHash(pkt *mc.Packet) []byte {
	if pkt.PayloadVer() != 0 {
		return nil
	}

	payload := pkt.Payload

	switch pkt.PayloadType() {
	case mc.PayloadTypeReq,
		mc.PayloadTypeResponse,
		mc.PayloadTypeTxtMsg,
		mc.PayloadTypePath:
		if len(payload) >= 2 {
			return []byte{payload[0]}
		}

	case mc.PayloadTypeGrpTxt, mc.PayloadTypeGrpData, mc.PayloadTypeAnonReq:
		if len(payload) >= 1 {
			return []byte{payload[0]}
		}
	}

	return nil
}

// payloadSrcHash returns the originator hash carried in the payload
// prefix for VER_1 payload types that expose one.
func payloadSrcHash(pkt *mc.Packet) []byte {
	if pkt.PayloadVer() != 0 {
		return nil
	}

	payload := pkt.Payload

	switch pkt.PayloadType() {
	case mc.PayloadTypeReq,
		mc.PayloadTypeResponse,
		mc.PayloadTypeTxtMsg,
		mc.PayloadTypePath:
		if len(payload) >= 2 {
			return []byte{payload[1]}
		}

	case mc.PayloadTypeAdvert:
		// ADVERT payload begins with the 32-byte node public key.
		if len(payload) >= 4 {
			return append([]byte(nil), payload[:4]...)
		}
	}

	return nil
}

// Advert captures the advert fields we care about for the nodes
// projection. Lat/lon and feature flags are reported alongside boolean
// presence flags so callers can distinguish "missing" from "zero".
type Advert struct {
	PublicKey []byte
	Timestamp time.Time
	NodeType  string

	Name string

	HasLatLon bool
	LatE6     int32
	LonE6     int32

	HasFeat1 bool
	Feat1    uint16

	HasFeat2 bool
	Feat2    uint16
}

// DecodeAdvert parses an ADVERT packet's payload (pubkey + timestamp +
// signature + appdata). Returns nil when the payload is not a valid
// advert or carries no app data.
func DecodeAdvert(payload []byte) *Advert {
	a, err := mc.AdvertFromBytes(payload)
	if err != nil {
		return nil
	}

	app := a.AppData()
	flags := a.Flags()

	// Advert names are arbitrary bytes on the wire — coerce to valid UTF-8
	// so Postgres text columns accept them. Also strip NUL bytes, which
	// Postgres rejects in text values regardless of encoding.
	name := strings.ToValidUTF8(app.Name, "�")
	name = strings.ReplaceAll(name, "\x00", "")

	out := &Advert{
		PublicKey: append([]byte(nil), a.PublicKey.PublicKeyBytes()...),
		Timestamp: time.Unix(int64(a.Timestamp), 0).UTC(),
		NodeType:  app.Type,
		Name:      name,
	}

	if flags&mc.AdvertLatLonMask != 0 {
		out.HasLatLon = true
		out.LatE6 = app.Lat
		out.LonE6 = app.Lon
	}

	if flags&mc.AdvertFeat1Mask != 0 {
		out.HasFeat1 = true
		out.Feat1 = app.Feat1
	}

	if flags&mc.AdvertFeat2Mask != 0 {
		out.HasFeat2 = true
		out.Feat2 = app.Feat2
	}

	return out
}

// NeighborSource returns the identifier we use for "which neighbor
// transmitted this packet to the observer". Preference: last hop in the
// path, then the payload src hash, then nil.
func NeighborSource(pkt *mc.Packet) []byte {
	hashes := pkt.PathHashes()
	if len(hashes) > 0 {
		last := hashes[len(hashes)-1]
		return append([]byte(nil), last...)
	}

	return payloadSrcHash(pkt)
}
