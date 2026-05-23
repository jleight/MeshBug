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
