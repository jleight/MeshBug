package meshcore

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestDecodeAdvertFlood(t *testing.T) {
	// header: route=01 (flood), type=04 (ADVERT), ver=0  -> 0b00_0100_01 = 0x11
	// path_len byte = 0 (no path)
	// payload = 32-byte pubkey + filler
	pub := bytes.Repeat([]byte{0xAB}, 32)
	raw := append([]byte{0x11, 0x00}, pub...)

	d, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if d.Route != RouteFlood {
		t.Errorf("route = %d, want %d", d.Route, RouteFlood)
	}

	if d.PayloadType != PayloadTypeADVERT {
		t.Errorf("payload type = %d, want ADVERT", d.PayloadType)
	}

	if !bytes.Equal(d.SrcHash, pub[:4]) {
		t.Errorf("src hash = %x, want %x", d.SrcHash, pub[:4])
	}

	if RouteName(d.Route) != "F" {
		t.Errorf("route name = %q, want F", RouteName(d.Route))
	}
}

func TestDecodeTxtMsgDirectWithPath(t *testing.T) {
	// header: route=02 (direct), type=02 (TXT_MSG), ver=0 -> 0b00_0010_10 = 0x0A
	// path_len byte: hash_size=1, count=2 -> top2=00, bottom6=000010 = 0x02
	// path: 2 x 1 byte = [0x11, 0x22]
	// payload: dst(1)=0xAA src(1)=0xBB MAC(2)=0xCC 0xDD + body
	raw := []byte{0x0A, 0x02, 0x11, 0x22, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF}

	d, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if d.Route != RouteDirect {
		t.Errorf("route = %d, want direct", d.Route)
	}

	if d.PayloadType != PayloadTypeTXTMSG {
		t.Errorf("payload type = %d, want TXT_MSG", d.PayloadType)
	}

	if len(d.Path) != 2 {
		t.Fatalf("path len = %d, want 2", len(d.Path))
	}

	if !bytes.Equal(d.LastHopHash, []byte{0x22}) {
		t.Errorf("last hop = %x, want 22", d.LastHopHash)
	}

	if !bytes.Equal(d.DstHash, []byte{0xAA}) {
		t.Errorf("dst = %x, want AA", d.DstHash)
	}

	if !bytes.Equal(d.SrcHash, []byte{0xBB}) {
		t.Errorf("src = %x, want BB", d.SrcHash)
	}

	if !bytes.Equal(d.NeighborSource(), []byte{0x22}) {
		t.Errorf("neighbor = %x, want 22", d.NeighborSource())
	}
}

func TestDecodeTransportFlood(t *testing.T) {
	// header: route=00 (transport_flood), type=03 (ACK), ver=0 -> 0b00_0011_00 = 0x0C
	// transport_codes: 4 bytes
	// path_len: 0
	// payload: 4 ack bytes
	raw := []byte{0x0C, 0x01, 0x02, 0x03, 0x04, 0x00, 0xDE, 0xAD, 0xBE, 0xEF}

	d, err := Decode(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if d.Route != RouteTransportFlood {
		t.Errorf("route = %d, want transport flood", d.Route)
	}

	if d.PayloadType != PayloadTypeACK {
		t.Errorf("type = %d, want ACK", d.PayloadType)
	}

	if hex.EncodeToString(d.TransportCodes) != "01020304" {
		t.Errorf("transport codes = %x", d.TransportCodes)
	}

	if d.PayloadOffset != 6 {
		t.Errorf("payload offset = %d, want 6", d.PayloadOffset)
	}
}

func TestDecodeDoNotRetransmit(t *testing.T) {
	_, err := Decode([]byte{0xFF})
	if err != ErrDoNotRetransmit {
		t.Errorf("err = %v, want ErrDoNotRetransmit", err)
	}
}

func TestDecodeTruncated(t *testing.T) {
	if _, err := Decode(nil); err == nil {
		t.Errorf("expected error for empty input")
	}

	// transport route but no transport bytes
	if _, err := Decode([]byte{0x0C, 0x00}); err == nil {
		t.Errorf("expected error for truncated transport codes")
	}
}
