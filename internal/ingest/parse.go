package ingest

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// statusMessage matches the JSON shape of meshcore/<region>/<hash>/status.
type statusMessage struct {
	Status          string `json:"status"`
	Timestamp       string `json:"timestamp"`
	Origin          string `json:"origin"`
	OriginID        string `json:"origin_id"`
	Source          string `json:"source"`
	ClientVersion   string `json:"client_version"`
	Radio           string `json:"radio"`
	Model           string `json:"model"`
	FirmwareVersion string `json:"firmware_version"`
	Stats           struct {
		UptimeSecs *int64   `json:"uptime_secs,omitempty"`
		BatteryMV  *int     `json:"battery_mv,omitempty"`
		DebugFlags *int64   `json:"debug_flags,omitempty"`
		QueueLen   *int     `json:"queue_len,omitempty"`
		NoiseFloor *int     `json:"noise_floor,omitempty"`
		TxAirSecs  *int64   `json:"tx_air_secs,omitempty"`
		RxAirSecs  *int64   `json:"rx_air_secs,omitempty"`
		RecvErrors *int64   `json:"recv_errors,omitempty"`
		LastRSSI   *int     `json:"last_rssi,omitempty"`
		LastSNR    *float64 `json:"last_snr,omitempty"`
		Errors     *int64   `json:"errors,omitempty"`
	} `json:"stats"`
}

// packetMessage matches the JSON shape of meshcore/<region>/<hash>/packets.
// Different observer flavours publish numbers as either JSON strings or JSON
// numbers; flexStr accepts both transparently.
type packetMessage struct {
	Timestamp  string  `json:"timestamp"`
	Origin     string  `json:"origin"`
	OriginID   string  `json:"origin_id"`
	Type       string  `json:"type"`
	Direction  string  `json:"direction"`
	Len        flexStr `json:"len"`
	PacketType flexStr `json:"packet_type"` // some observers publish this as a JSON number (e.g. 0), others as a string
	Route      string  `json:"route"`
	PayloadLen flexStr `json:"payload_len"`
	Raw        string  `json:"raw"`
	SNR        flexStr `json:"SNR"`
	RSSI       flexStr `json:"RSSI"`
	Score      flexStr `json:"score"`
	Duration   flexStr `json:"duration"`
	Hash       string  `json:"hash"`
	Path       string  `json:"path"` // observers may include the accumulated path as a hex string, e.g. "F0 B7"
}

// flexStr captures a JSON value that may arrive as a string OR a number.
// We always represent it as the string form (number values are reformatted
// via strconv so downstream parsing is uniform).
type flexStr string

func (f *flexStr) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*f = flexStr(s)
		return nil
	}
	// raw JSON number or boolean — keep as-is, trimmed of surrounding whitespace
	*f = flexStr(strings.TrimSpace(string(b)))
	return nil
}

func (f flexStr) String() string { return string(f) }

// parseTopicHash returns the observer hash (32 bytes) and the region label
// found between the prefix and the hash for a topic like
// "<prefix>BUF/F0924D.../packets".
func parseTopicHash(prefix, topic string) (id []byte, region string, kind string, err error) {
	rest := strings.TrimPrefix(topic, prefix)
	parts := strings.Split(rest, "/")
	if len(parts) < 3 {
		return nil, "", "", fmt.Errorf("topic %q has too few segments after prefix", topic)
	}
	region = parts[0]
	hashStr := parts[1]
	kind = parts[len(parts)-1]
	id, err = hex.DecodeString(hashStr)
	if err != nil {
		return nil, "", "", fmt.Errorf("topic hash %q: %w", hashStr, err)
	}
	if len(id) != 32 {
		return nil, "", "", fmt.Errorf("topic hash %q has %d bytes, want 32", hashStr, len(id))
	}
	return id, region, kind, nil
}

func parseTimestamp(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp %q", s)
}

// parseRadio splits the "freq,bw,sf,cr" form into typed pointers (any blank).
func parseRadio(s string) (freqKHz *int, bwKHz *float64, sf *int, cr *int) {
	parts := strings.Split(s, ",")
	if len(parts) >= 1 {
		if mhz, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64); err == nil {
			v := int(mhz * 1000)
			freqKHz = &v
		}
	}
	if len(parts) >= 2 {
		if v, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64); err == nil {
			bwKHz = &v
		}
	}
	if len(parts) >= 3 {
		if v, err := strconv.Atoi(strings.TrimSpace(parts[2])); err == nil {
			sf = &v
		}
	}
	if len(parts) >= 4 {
		if v, err := strconv.Atoi(strings.TrimSpace(parts[3])); err == nil {
			cr = &v
		}
	}
	return
}

func atoiFlex(f flexStr) *int { return atoiPtr(string(f)) }
func atofFlex(f flexStr) *float64 { return atofPtr(string(f)) }

func atoiPtr(s string) *int {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	if v, err := strconv.Atoi(s); err == nil {
		return &v
	}
	return nil
}

func atofPtr(s string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return &v
	}
	return nil
}

func unmarshalStatus(payload []byte) (*statusMessage, error) {
	var m statusMessage
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func unmarshalPacket(payload []byte) (*packetMessage, error) {
	var m packetMessage
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
