package meshcore

import (
	"bytes"
	"testing"

	mc "github.com/meshcore-go/meshcore-go"
)

func TestParseAdvertFlood(t *testing.T) {
	// header: route=01 (flood), type=04 (ADVERT), ver=0  -> 0b00_0100_01 = 0x11
	// path_len byte = 0 (no path)
	// payload = 32-byte pubkey + filler
	pub := bytes.Repeat([]byte{0xAB}, 32)
	raw := append([]byte{0x11, 0x00}, pub...)

	pkt, err := Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if pkt.RouteType() != mc.RouteTypeFlood {
		t.Errorf("route = %d, want %d", pkt.RouteType(), mc.RouteTypeFlood)
	}

	if pkt.PayloadType() != mc.PayloadTypeAdvert {
		t.Errorf("payload type = %d, want ADVERT", pkt.PayloadType())
	}

	if got := NeighborSource(pkt); !bytes.Equal(got, pub[:4]) {
		t.Errorf("neighbor = %x, want %x", got, pub[:4])
	}

	if got := pkt.RouteTypeString(); got != "FLOOD" {
		t.Errorf("route name = %q, want FLOOD", got)
	}
}

func TestParseTxtMsgDirectWithPath(t *testing.T) {
	// header: route=02 (direct), type=02 (TXT_MSG), ver=0 -> 0b00_0010_10 = 0x0A
	// path_len byte: hash_size=1, count=2 -> top2=00, bottom6=000010 = 0x02
	// path: 2 x 1 byte = [0x11, 0x22]
	// payload: dst(1)=0xAA src(1)=0xBB MAC(2)=0xCC 0xDD + body
	raw := []byte{0x0A, 0x02, 0x11, 0x22, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}

	pkt, err := Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if pkt.RouteType() != mc.RouteTypeDirect {
		t.Errorf("route = %d, want direct", pkt.RouteType())
	}

	if pkt.PayloadType() != mc.PayloadTypeTxtMsg {
		t.Errorf("payload type = %d, want TXT_MSG", pkt.PayloadType())
	}

	hashes := pkt.PathHashes()
	if len(hashes) != 2 {
		t.Fatalf("path len = %d, want 2", len(hashes))
	}

	if !bytes.Equal(hashes[1], []byte{0x22}) {
		t.Errorf("last hop = %x, want 22", hashes[1])
	}

	if got := PayloadDstHash(pkt); !bytes.Equal(got, []byte{0xAA}) {
		t.Errorf("dst = %x, want AA", got)
	}

	if got := NeighborSource(pkt); !bytes.Equal(got, []byte{0x22}) {
		t.Errorf("neighbor = %x, want 22", got)
	}
}

func TestParseTransportFlood(t *testing.T) {
	// header: route=00 (transport_flood), type=03 (ACK), ver=0 -> 0b00_0011_00 = 0x0C
	// transport_codes: 4 bytes
	// path_len: 0
	// payload: 4 ack bytes
	raw := []byte{0x0C, 0x01, 0x02, 0x03, 0x04, 0x00, 0xDE, 0xAD, 0xBE, 0xEF}

	pkt, err := Parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if pkt.RouteType() != mc.RouteTypeTransportFlood {
		t.Errorf("route = %d, want transport flood", pkt.RouteType())
	}

	if pkt.PayloadType() != mc.PayloadTypeAck {
		t.Errorf("type = %d, want ACK", pkt.PayloadType())
	}

	if !pkt.IsTransport() {
		t.Errorf("expected transport route")
	}
}

func TestParseDoNotRetransmit(t *testing.T) {
	_, err := Parse([]byte{0xFF})
	if err != ErrDoNotRetransmit {
		t.Errorf("err = %v, want ErrDoNotRetransmit", err)
	}
}

func TestParseTruncated(t *testing.T) {
	if _, err := Parse(nil); err == nil {
		t.Errorf("expected error for empty input")
	}

	// transport route but no transport bytes
	if _, err := Parse([]byte{0x0C, 0x00}); err == nil {
		t.Errorf("expected error for truncated transport codes")
	}
}
