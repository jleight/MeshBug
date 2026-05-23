// Package meshcore decodes the on-the-wire MeshCore packet header.
//
// Wire format (see ripplebiz/MeshCore src/Packet.{h,cpp}):
//
//	byte 0           header (2-bit route, 4-bit payload type, 2-bit version)
//	bytes 1..4       transport_codes (only present when route is *_TRANSPORT_*)
//	byte             path_len_byte: top 2 bits = hash_size-1, bottom 6 bits = hash count
//	N bytes          path hashes (count * size)
//	remaining        payload (type-specific layout, often dst_hash|src_hash|MAC|...)
//
// We do not decrypt payloads. We extract enough to identify the route, payload
// type, the immediate transmitter (last path hash) and the originator hash
// when the payload format exposes one.
package meshcore

import "errors"

const (
	RouteTransportFlood  = 0x00
	RouteFlood           = 0x01
	RouteDirect          = 0x02
	RouteTransportDirect = 0x03

	PayloadTypeREQ       = 0x00
	PayloadTypeRESPONSE  = 0x01
	PayloadTypeTXTMSG    = 0x02
	PayloadTypeACK       = 0x03
	PayloadTypeADVERT    = 0x04
	PayloadTypeGRPTXT    = 0x05
	PayloadTypeGRPDATA   = 0x06
	PayloadTypeANONREQ   = 0x07
	PayloadTypePATH      = 0x08
	PayloadTypeTRACE     = 0x09
	PayloadTypeMULTIPART = 0x0A
	PayloadTypeCONTROL   = 0x0B
	PayloadTypeRAWCUSTOM = 0x0F
)

// PayloadTypeName returns a short human label for a payload type byte.
func PayloadTypeName(t int16) string {
	switch t {
	case PayloadTypeREQ:
		return "REQ"
	case PayloadTypeRESPONSE:
		return "RESPONSE"
	case PayloadTypeTXTMSG:
		return "TXT_MSG"
	case PayloadTypeACK:
		return "ACK"
	case PayloadTypeADVERT:
		return "ADVERT"
	case PayloadTypeGRPTXT:
		return "GRP_TXT"
	case PayloadTypeGRPDATA:
		return "GRP_DATA"
	case PayloadTypeANONREQ:
		return "ANON_REQ"
	case PayloadTypePATH:
		return "PATH"
	case PayloadTypeTRACE:
		return "TRACE"
	case PayloadTypeMULTIPART:
		return "MULTIPART"
	case PayloadTypeCONTROL:
		return "CONTROL"
	case PayloadTypeRAWCUSTOM:
		return "RAW_CUSTOM"
	}
	return ""
}

// RouteName returns "F" (flood) or "D" (direct) for the dashboard.
func RouteName(routeType int) string {
	switch routeType {
	case RouteFlood, RouteTransportFlood:
		return "F"
	case RouteDirect, RouteTransportDirect:
		return "D"
	}
	return ""
}

type DecodedHeader struct {
	Route          int    // RouteFlood / RouteDirect / RouteTransport*
	PayloadType    int16  // PayloadType*
	PayloadVer     int
	TransportCodes []byte // 4 bytes if present, else nil
	Path           [][]byte
	PayloadOffset  int    // index into raw where the payload starts

	// Derived fields used by the dashboard:
	LastHopHash []byte // last entry in Path, or nil
	SrcHash     []byte // src_hash from payload prefix when applicable (1 byte for VER_1)
	DstHash     []byte // dst_hash from payload prefix when applicable
}

var (
	ErrShort           = errors.New("packet truncated")
	ErrBadPath         = errors.New("invalid path length encoding")
	ErrDoNotRetransmit = errors.New("do-not-retransmit marker")
)

// Decode parses the header layout of one MeshCore packet. `raw` is the raw
// frame as emitted by the observer (hex-decoded). Returns an error for
// malformed frames; the special header value 0xFF is reported via
// ErrDoNotRetransmit and callers may choose to ignore that.
func Decode(raw []byte) (*DecodedHeader, error) {
	if len(raw) < 1 {
		return nil, ErrShort
	}

	header := raw[0]
	if header == 0xFF {
		return nil, ErrDoNotRetransmit
	}

	d := &DecodedHeader{
		Route:       int(header & 0x03),
		PayloadType: int16((header >> 2) & 0x0F),
		PayloadVer:  int((header >> 6) & 0x03),
	}

	i := 1
	if d.Route == RouteTransportFlood || d.Route == RouteTransportDirect {
		if len(raw) < i+4 {
			return nil, ErrShort
		}
		d.TransportCodes = append([]byte(nil), raw[i:i+4]...)
		i += 4
	}

	if len(raw) < i+1 {
		return nil, ErrShort
	}

	pathByte := raw[i]
	i++

	hashSize := int(pathByte>>6) + 1
	pathCount := int(pathByte & 0x3F)

	if pathCount > 0 {
		if hashSize == 0 {
			return nil, ErrBadPath
		}

		need := pathCount * hashSize
		if len(raw) < i+need {
			return nil, ErrShort
		}

		d.Path = make([][]byte, pathCount)
		for k := 0; k < pathCount; k++ {
			d.Path[k] = append([]byte(nil), raw[i:i+hashSize]...)
			i += hashSize
		}

		d.LastHopHash = d.Path[pathCount-1]
	}

	d.PayloadOffset = i

	payload := raw[i:]

	// Pull dst/src hashes out of the payload prefix for payload types known
	// to carry them in VER_1 layout (1-byte dst hash, 1-byte src hash,
	// 2-byte MAC).
	if d.PayloadVer == 0 {
		switch d.PayloadType {
		case PayloadTypeREQ,
			PayloadTypeRESPONSE,
			PayloadTypeTXTMSG,
			PayloadTypePATH:
			if len(payload) >= 2 {
				d.DstHash = []byte{payload[0]}
				d.SrcHash = []byte{payload[1]}
			}

		case PayloadTypeGRPTXT, PayloadTypeGRPDATA:
			if len(payload) >= 1 {
				// channel hash; surface in DstHash so the dashboard can group by it.
				d.DstHash = []byte{payload[0]}
			}

		case PayloadTypeANONREQ:
			if len(payload) >= 1 {
				d.DstHash = []byte{payload[0]}
			}

		case PayloadTypeADVERT:
			// ADVERT payload begins with the 32-byte node public key.
			if len(payload) >= 4 {
				d.SrcHash = append([]byte(nil), payload[:4]...) // 4-byte prefix
			}
		}
	}

	return d, nil
}

// NeighborSource returns the identifier we use for "which neighbor
// transmitted this packet to the observer". Preference: last hop in the
// path, then the payload src hash, then nil.
func (d *DecodedHeader) NeighborSource() []byte {
	if len(d.LastHopHash) > 0 {
		return d.LastHopHash
	}

	if len(d.SrcHash) > 0 {
		return d.SrcHash
	}

	return nil
}
