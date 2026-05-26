package project

import (
	"encoding/hex"
	"testing"
)

func TestParseTopicHash(t *testing.T) {
	id, region, kind, err := parseTopicHash(
		"meshcore/",
		"meshcore/BUF/F0924D2FB99A61D5581651F8F65ECDF0474BF7360F2CFC132FC1B62922920E70/status",
	)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if region != "BUF" {
		t.Errorf("region = %q", region)
	}

	if kind != "status" {
		t.Errorf("kind = %q", kind)
	}

	if len(id) != 32 {
		t.Errorf("id len = %d", len(id))
	}

	if hex.EncodeToString(id)[:8] != "f0924d2f" {
		t.Errorf("id prefix = %x", id[:4])
	}
}

func TestUnmarshalStatus(t *testing.T) {
	raw := []byte(`{"status": "online", "timestamp": "2026-05-22T11:33:03.316437", "origin": "mrPeteza HA", "origin_id": "F09...", "source": "meshcore-ha", "client_version": "x:2.6.0", "radio": "910.525,62.5,7,8", "model": "Heltec V4.3 OLED", "firmware_version": "v1.15.0", "stats": {"uptime_secs": 1105592, "battery_mv": 4244, "queue_len": 0, "noise_floor": -84, "tx_air_secs": 87, "rx_air_secs": 6555, "recv_errors": 116}}`)

	m, err := unmarshalStatus(raw)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if m.Status != "online" {
		t.Errorf("status = %q", m.Status)
	}

	if *m.Stats.NoiseFloor != -84 {
		t.Errorf("noise_floor = %d", *m.Stats.NoiseFloor)
	}

	freq, bw, sf, cr := parseRadio(m.Radio)

	if *freq != 910525 {
		t.Errorf("freqKHz = %d", *freq)
	}

	if *bw != 62.5 {
		t.Errorf("bw = %v", *bw)
	}

	if *sf != 7 || *cr != 8 {
		t.Errorf("sf=%d cr=%d", *sf, *cr)
	}
}

func TestUnmarshalStatusFloatStats(t *testing.T) {
	// Some observers publish stat counters as JSON floats (e.g. 0.0) even
	// when the value is whole. They must still decode cleanly.
	raw := []byte(`{"status":"online","stats":{"tx_air_secs":0.0,"rx_air_secs":12.5,"queue_len":3.0,"noise_floor":-84.0,"recv_errors":7.9}}`)

	m, err := unmarshalStatus(raw)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if v := m.Stats.TxAirSecs.Int64Ptr(); v == nil || *v != 0 {
		t.Errorf("tx_air_secs = %v", v)
	}

	if v := m.Stats.RxAirSecs.Int64Ptr(); v == nil || *v != 12 {
		t.Errorf("rx_air_secs = %v", v)
	}

	if v := m.Stats.QueueLen.IntPtr(); v == nil || *v != 3 {
		t.Errorf("queue_len = %v", v)
	}

	if v := m.Stats.NoiseFloor.IntPtr(); v == nil || *v != -84 {
		t.Errorf("noise_floor = %v", v)
	}

	if v := m.Stats.RecvErrors.Int64Ptr(); v == nil || *v != 7 {
		t.Errorf("recv_errors = %v", v)
	}
}

func TestUnmarshalPacket(t *testing.T) {
	raw := []byte(`{"timestamp": "2026-05-22T11:38:03.342542", "origin": "mrPeteza HA", "origin_id": "F0...", "type": "PACKET", "direction": "rx", "time": "11:38:03", "date": "22/5/2026", "len": "0", "packet_type": "", "route": "F", "payload_len": "0", "raw": "", "SNR": "", "RSSI": "", "score": "1000", "duration": "0", "hash": ""}`)

	m, err := unmarshalPacket(raw)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if m.Direction != "rx" || m.Route != "F" {
		t.Errorf("dir=%q route=%q", m.Direction, m.Route)
	}

	if v := atoiFlex(m.Score); v == nil || *v != 1000 {
		t.Errorf("score = %v", v)
	}
}

func TestUnmarshalPacketNumericFields(t *testing.T) {
	// Some observers publish all numeric fields — including packet_type — as
	// JSON numbers rather than strings.
	raw := []byte(`{"timestamp":"2026-05-22T11:38:03.342542","origin":"o","origin_id":"x","type":"PACKET","direction":"rx","len":42,"packet_type":0,"route":"F","payload_len":40,"raw":"abcd","SNR":-5.25,"RSSI":-95,"score":1000,"duration":0,"hash":"deadbeef","path":"F0 B7"}`)

	m, err := unmarshalPacket(raw)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if v := atoiFlex(m.RSSI); v == nil || *v != -95 {
		t.Errorf("RSSI = %v", v)
	}

	if v := atofFlex(m.SNR); v == nil || *v != -5.25 {
		t.Errorf("SNR = %v", v)
	}

	if string(m.PacketType) != "0" {
		t.Errorf("PacketType = %q, want %q", m.PacketType, "0")
	}

	if m.Path != "F0 B7" {
		t.Errorf("Path = %q, want %q", m.Path, "F0 B7")
	}
}
